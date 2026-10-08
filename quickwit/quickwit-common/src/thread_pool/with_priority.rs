// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::{Future, TryFutureExt};
use tokio::sync::oneshot;

use super::{Panicked, QueuedTask, build_rayon_pool};

/// An executor backed by a thread pool to run CPU-intensive tasks, where
/// high-priority tasks run before pending normal-priority ones.
///
/// Pending tasks wait in a queue owned by this pool and are only handed over to
/// rayon when a thread is available. When a task completes, the worker that ran
/// it schedules the next one, so dispatching never depends on the tokio runtime.
///
/// The underlying rayon pool is deliberately not exposed: rayon runs the tasks
/// dispatched from a worker before anything submitted to it from outside, which
/// would starve such work as long as this pool has a backlog.
///
/// Tasks must not block waiting on other tasks of this pool, directly or through
/// async work that submits to it (e.g. calling `block_on` on a storage read whose
/// download is assembled here). Queued tasks only get a slot when a running one
/// completes, so if every running task waits on a queued one, the pool deadlocks.
#[derive(Clone)]
pub struct ThreadPoolWithPriority {
    inner: Arc<ThreadPoolInner>,
}

struct ThreadPoolInner {
    thread_pool: rayon::ThreadPool,
    name: &'static str,
    max_running_tasks: usize,
    num_running_tasks: AtomicUsize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    high_priority_tasks: VecDeque<Box<dyn PendingTask>>,
    normal_priority_tasks: VecDeque<Box<dyn PendingTask>>,
}

impl State {
    fn pop_next_task(&mut self) -> Option<Box<dyn PendingTask>> {
        self.high_priority_tasks
            .pop_front()
            .or_else(|| self.normal_priority_tasks.pop_front())
    }
}

/// The priority of a task submitted to a [`ThreadPoolWithPriority`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Priority {
    /// The default priority.
    #[default]
    Normal,
    /// A high-priority task is scheduled before normal-priority tasks that are still pending.
    /// This is reserved for short tasks sitting on the critical path of a request (e.g. merging
    /// partial results).
    High,
}

trait PendingTask: Send {
    fn is_cancelled(&self) -> bool;

    fn run(self: Box<Self>);
}

struct CpuIntensiveTask<F, R> {
    cpu_intensive_fn: F,
    tx: oneshot::Sender<R>,
    span: tracing::Span,
    queued_task: QueuedTask,
}

impl<F, R> PendingTask for CpuIntensiveTask<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    fn is_cancelled(&self) -> bool {
        self.tx.is_closed()
    }

    fn run(self: Box<Self>) {
        let CpuIntensiveTask {
            cpu_intensive_fn,
            tx,
            span,
            queued_task,
        } = *self;
        if tx.is_closed() {
            // dropping `queued_task` still records the time it spent queued
            return;
        }
        let _guard = span.enter();
        let running_task = queued_task.start();
        let result = cpu_intensive_fn();
        drop(running_task);
        let _ = tx.send(result);
    }
}

/// A claimed execution slot in the thread pool.
///
/// Dropping this permit decrements the running-task count and re-triggers scheduling so the
/// next queued task can fill the freed slot. It is also dropped when the task panics.
struct Permit(Arc<ThreadPoolInner>);

impl Permit {
    fn new(thread_pool_inner: Arc<ThreadPoolInner>) -> Self {
        thread_pool_inner
            .num_running_tasks
            .fetch_add(1, Ordering::AcqRel);
        Self(thread_pool_inner)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let prev = self.0.num_running_tasks.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "dropped more permits than were acquired");
        ThreadPoolInner::schedule(&self.0);
    }
}

impl ThreadPoolWithPriority {
    pub fn new(name: &'static str, num_threads_opt: Option<usize>) -> ThreadPoolWithPriority {
        let thread_pool = build_rayon_pool(name, num_threads_opt);
        let max_running_tasks = thread_pool.current_num_threads();
        ThreadPoolWithPriority {
            inner: Arc::new(ThreadPoolInner {
                thread_pool,
                name,
                max_running_tasks,
                num_running_tasks: AtomicUsize::new(0),
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Function similar to `tokio::spawn_blocking`.
    ///
    /// Here are two important differences however:
    ///
    /// 1) The task runs on a rayon thread pool managed by Quickwit. This pool is specifically used
    ///    only to run CPU-intensive work.
    ///
    /// 2) Before the task is effectively scheduled, we check that the spawner is still interested
    ///    in its result.
    ///
    /// It is therefore required to `await` the result of this
    /// function to get any work done.
    ///
    /// This is nice because it makes work that has been scheduled
    /// but is not running yet "cancellable".
    pub fn run_cpu_intensive<F, R>(
        &self,
        cpu_intensive_fn: F,
    ) -> impl Future<Output = Result<R, Panicked>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.run_cpu_intensive_with_extra_tags(cpu_intensive_fn, "unknown", "NA")
    }

    /// Same as `run_cpu_intensive` but with a caller identifier recorded in the
    /// metrics.
    pub fn run_cpu_intensive_with_extra_tags<F, R>(
        &self,
        cpu_intensive_fn: F,
        caller: &'static str,
        cost_class: &'static str,
    ) -> impl Future<Output = Result<R, Panicked>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.run_cpu_intensive_with_priority(Priority::Normal, cpu_intensive_fn, caller, cost_class)
    }

    /// Same as `run_cpu_intensive_with_extra_tags` but with an explicit priority.
    pub fn run_cpu_intensive_with_priority<F, R>(
        &self,
        priority: Priority,
        cpu_intensive_fn: F,
        caller: &'static str,
        cost_class: &'static str,
    ) -> impl Future<Output = Result<R, Panicked>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let span = tracing::Span::current();
        let queued_task = QueuedTask::new(self.inner.name, caller, cost_class);
        let (tx, rx) = oneshot::channel();
        let task = CpuIntensiveTask {
            cpu_intensive_fn,
            tx,
            span,
            queued_task,
        };
        self.inner.enqueue(priority, Box::new(task));
        ThreadPoolInner::schedule(&self.inner);
        rx.map_err(|_| Panicked)
    }
}

impl ThreadPoolInner {
    fn enqueue(&self, priority: Priority, task: Box<dyn PendingTask>) {
        let mut state = self.state.lock().unwrap();
        match priority {
            Priority::Normal => state.normal_priority_tasks.push_back(task),
            Priority::High => state.high_priority_tasks.push_back(task),
        }
    }

    fn schedule(inner: &Arc<Self>) {
        // Fast path: skip lock acquisition entirely when already at capacity.
        if inner.num_running_tasks.load(Ordering::Acquire) >= inner.max_running_tasks {
            return;
        }
        // Dropping a cancelled task runs the destructors of whatever its closure captured, which
        // must not happen while holding the lock: they could be slow or submit tasks themselves.
        let mut cancelled_tasks = Vec::new();
        let mut state = inner.state.lock().unwrap();
        while inner.num_running_tasks.load(Ordering::Acquire) < inner.max_running_tasks {
            let Some(task) = state.pop_next_task() else {
                break;
            };
            if task.is_cancelled() {
                cancelled_tasks.push(task);
                continue;
            }
            let permit = Permit::new(inner.clone());
            inner.thread_pool.spawn(move || {
                task.run();
                // We explicitly drop here to force the move of the permit into the closure.
                drop(permit);
            });
        }
        drop(state);
        drop(cancelled_tasks);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn test_run_cpu_intensive() {
        let thread_pool = ThreadPoolWithPriority::new("priority_basic_test", Some(1));
        assert_eq!(thread_pool.run_cpu_intensive(|| 1).await, Ok(1));
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panicks() {
        let thread_pool = ThreadPoolWithPriority::new("priority_panic_basic_test", Some(1));
        assert!(thread_pool.run_cpu_intensive(|| panic!("")).await.is_err());
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panicks_do_not_shrink_thread_pool() {
        let thread_pool = ThreadPoolWithPriority::new("priority_repeated_panic_test", Some(1));
        for _ in 0..100 {
            assert!(thread_pool.run_cpu_intensive(|| panic!("")).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_abort() {
        let thread_pool = ThreadPoolWithPriority::new("priority_abort_test", Some(1));
        let counter: Arc<AtomicU64> = Default::default();
        let mut futures = Vec::new();
        for _ in 0..1_000 {
            let counter_clone = counter.clone();
            let fut = thread_pool.run_cpu_intensive(move || {
                std::thread::sleep(Duration::from_millis(5));
                counter_clone.fetch_add(1, Ordering::SeqCst)
            });
            futures.push(tokio::time::timeout(Duration::from_millis(1), fut));
        }
        futures::future::join_all(futures).await;
        assert!(counter.load(Ordering::SeqCst) < 100);
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_high_priority_runs_before_pending_normal_priority_tasks() {
        let thread_pool = ThreadPoolWithPriority::new("priority_order_test", Some(1));
        let execution_order: Arc<std::sync::Mutex<Vec<u64>>> = Default::default();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let first_task = thread_pool.run_cpu_intensive(move || {
            let _ = started_tx.send(());
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();

        let execution_order_clone = execution_order.clone();
        let normal_task_1 = thread_pool.run_cpu_intensive_with_priority(
            Priority::Normal,
            move || {
                execution_order_clone.lock().unwrap().push(1);
            },
            "test",
            "NA",
        );

        let execution_order_clone = execution_order.clone();
        let normal_task_2 = thread_pool.run_cpu_intensive_with_priority(
            Priority::Normal,
            move || {
                execution_order_clone.lock().unwrap().push(2);
            },
            "test",
            "NA",
        );

        let execution_order_clone = execution_order.clone();
        let high_priority_task = thread_pool.run_cpu_intensive_with_priority(
            Priority::High,
            move || {
                execution_order_clone.lock().unwrap().push(0);
            },
            "test",
            "NA",
        );

        release_tx.send(()).unwrap();
        first_task.await.unwrap();
        high_priority_task.await.unwrap();
        normal_task_1.await.unwrap();
        normal_task_2.await.unwrap();

        assert_eq!(*execution_order.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_with_priority_skips_cancelled_pending_task() {
        let thread_pool = ThreadPoolWithPriority::new("priority_cancellation_test", Some(1));
        let counter: Arc<AtomicU64> = Default::default();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let first_task = thread_pool.run_cpu_intensive(move || {
            let _ = started_tx.send(());
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();

        let counter_clone = counter.clone();
        let cancelled_task = thread_pool.run_cpu_intensive_with_priority(
            Priority::High,
            move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            },
            "test",
            "NA",
        );
        drop(cancelled_task);

        let counter_clone = counter.clone();
        let normal_task = thread_pool.run_cpu_intensive(move || {
            counter_clone.fetch_add(10, Ordering::SeqCst);
        });

        release_tx.send(()).unwrap();
        first_task.await.unwrap();
        normal_task.await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panic_releases_scheduler_slot() {
        let thread_pool = ThreadPoolWithPriority::new("priority_panic_test", Some(1));
        assert!(
            thread_pool
                .run_cpu_intensive_with_priority(
                    Priority::High,
                    || panic!("expected panic"),
                    "test",
                    "NA"
                )
                .await
                .is_err()
        );

        let result =
            tokio::time::timeout(Duration::from_secs(1), thread_pool.run_cpu_intensive(|| 1))
                .await
                .unwrap();
        assert_eq!(result, Ok(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_pending_tasks_are_dispatched_while_the_runtime_is_blocked() {
        let thread_pool = ThreadPoolWithPriority::new("priority_blocked_runtime_test", Some(1));
        let counter: Arc<AtomicU64> = Default::default();
        let mut futures = Vec::new();
        for _ in 0..10 {
            let counter_clone = counter.clone();
            futures.push(thread_pool.run_cpu_intensive(move || {
                std::thread::sleep(Duration::from_millis(1));
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }));
        }
        // Block the only runtime thread: the queue must still be drained, as each completing
        // worker dispatches the next task.
        let deadline = Instant::now() + Duration::from_secs(5);
        while counter.load(Ordering::SeqCst) < 10 {
            assert!(Instant::now() < deadline, "tasks were not dispatched");
            std::thread::sleep(Duration::from_millis(1));
        }
        for result in futures::future::join_all(futures).await {
            result.unwrap();
        }
    }

    /// Submits a task to the pool when dropped.
    struct SubmitOnDrop(ThreadPoolWithPriority);

    impl Drop for SubmitOnDrop {
        fn drop(&mut self) {
            // Dropping the future right away cancels the task.
            drop(self.0.run_cpu_intensive(|| {}));
        }
    }

    #[tokio::test]
    async fn test_dropping_cancelled_task_can_submit_tasks() {
        let thread_pool = ThreadPoolWithPriority::new("priority_cancelled_drop_test", Some(1));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let first_task = thread_pool.run_cpu_intensive(move || {
            let _ = started_tx.send(());
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();

        let submit_on_drop = SubmitOnDrop(thread_pool.clone());
        let cancelled_task = thread_pool.run_cpu_intensive(move || drop(submit_on_drop));
        drop(cancelled_task);

        // The cancelled task gets dropped by the scheduling triggered when the first task
        // completes. That would deadlock if it happened while holding the lock.
        release_tx.send(()).unwrap();
        first_task.await.unwrap();
        // Submitting blocks on the lock if it is never released, hence the separate thread.
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = futures::executor::block_on(thread_pool.run_cpu_intensive(|| 1));
            let _ = result_tx.send(result);
        });
        let result = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("thread pool is deadlocked");
        assert_eq!(result, Ok(1));
    }
}
