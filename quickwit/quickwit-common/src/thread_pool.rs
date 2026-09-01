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

use std::fmt;
use std::sync::Arc;

use futures::{Future, TryFutureExt};
use once_cell::sync::Lazy;
use tokio::sync::oneshot;
use tracing::error;

use crate::metrics::{
    Histogram, HistogramTimer, HistogramVec, IntGauge, IntGaugeVec, OwnedGaugeGuard,
    exponential_buckets, new_gauge_vec, new_histogram_vec,
};

/// An executor backed by a thread pool to run CPU-intensive tasks.
///
/// tokio::spawn_blocking should only used for IO-bound tasks, as it has not limit on its
/// thread count.
#[derive(Clone)]
pub struct ThreadPool {
    thread_pool: Arc<rayon::ThreadPool>,
    name: &'static str,
}

impl ThreadPool {
    pub fn new(name: &'static str, num_threads_opt: Option<usize>) -> ThreadPool {
        let mut rayon_pool_builder = rayon::ThreadPoolBuilder::new()
            .thread_name(move |thread_id| format!("quickwit-{name}-{thread_id}"))
            .panic_handler(move |_my_panic| {
                error!("task running in the quickwit {name} thread pool panicked");
            });
        if let Some(num_threads) = num_threads_opt {
            rayon_pool_builder = rayon_pool_builder.num_threads(num_threads);
        }
        let thread_pool = rayon_pool_builder
            .build()
            .expect("failed to spawn thread pool");
        ThreadPool {
            thread_pool: Arc::new(thread_pool),
            name,
        }
    }

    /// Returns a Tantivy [`tantivy::Executor`] backed by this thread pool.
    ///
    /// Tasks that Tantivy schedules through it are tracked by metrics.
    pub fn get_executor(&self, caller: &'static str) -> tantivy::Executor {
        tantivy::Executor::InstrumentedThreadPool(
            self.thread_pool.clone(),
            Arc::new(ThreadPoolTaskInstrumentation {
                pool_name: self.name,
                caller,
            }),
        )
    }

    /// Same as `run_cpu_intensive` but with a caller identifier recorded in the
    /// metrics.
    pub fn run_cpu_intensive_with_identified_caller<F, R>(
        &self,
        cpu_intensive_fn: F,
        caller: &'static str,
    ) -> impl Future<Output = Result<R, Panicked>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let span = tracing::Span::current();
        let queued_task = QueuedTask::new(self.name, caller);
        let (tx, rx) = oneshot::channel();
        self.thread_pool.spawn(move || {
            if tx.is_closed() {
                // dropping `queued_task` still records the time it spent queued
                return;
            }
            let _guard = span.enter();
            let running_task = queued_task.start();
            let result = cpu_intensive_fn();
            drop(running_task);
            let _ = tx.send(result);
        });
        rx.map_err(|_| Panicked)
    }

    /// Function similar to `tokio::spawn_blocking`.
    ///
    /// Here are two important differences however:
    ///
    /// 1) The task runs on a rayon thread pool managed by Quickwit. This pool is specifically used
    ///    only to run CPU-intensive work and is configured to contain `num_cpus` cores.
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
        self.run_cpu_intensive_with_identified_caller(cpu_intensive_fn, "unknown")
    }
}

/// Tracks a task submitted to a [`ThreadPool`] while it waits in the queue.
///
/// Dropping it without calling [`Self::start`] records the time spent queued but
/// no run time, which is what happens to tasks cancelled before they run.
struct QueuedTask {
    ongoing_tasks: IntGauge,
    run_time: Histogram,
    pending_tasks_guard: OwnedGaugeGuard,
    queue_wait_timer: HistogramTimer,
}

impl QueuedTask {
    /// Must be called when submitting the task, not once a worker picks it up,
    /// for the queue wait time to be measured correctly.
    fn new(pool_name: &'static str, caller: &'static str) -> QueuedTask {
        let labels = [pool_name, caller];
        let mut pending_tasks_guard = OwnedGaugeGuard::from_gauge(
            THREAD_POOL_METRICS.pending_tasks.with_label_values(labels),
        );
        pending_tasks_guard.add(1i64);
        QueuedTask {
            ongoing_tasks: THREAD_POOL_METRICS.ongoing_tasks.with_label_values(labels),
            run_time: THREAD_POOL_METRICS.run_time_secs.with_label_values(labels),
            pending_tasks_guard,
            queue_wait_timer: THREAD_POOL_METRICS
                .queue_wait_time_secs
                .with_label_values(labels)
                .start_timer(),
        }
    }

    fn start(self) -> RunningTaskGuard {
        drop(self.pending_tasks_guard);
        self.queue_wait_timer.observe_duration();
        let mut ongoing_tasks_guard = OwnedGaugeGuard::from_gauge(self.ongoing_tasks);
        ongoing_tasks_guard.add(1i64);
        RunningTaskGuard {
            _ongoing_tasks_guard: ongoing_tasks_guard,
            _run_timer: self.run_time.start_timer(),
        }
    }
}

/// Tracks a task that is running. Dropping it records the run time.
struct RunningTaskGuard {
    _ongoing_tasks_guard: OwnedGaugeGuard,
    _run_timer: HistogramTimer,
}

/// Records tasks that Tantivy schedules on a [`ThreadPool`] into the same
/// metrics as the ones scheduled by Quickwit itself.
struct ThreadPoolTaskInstrumentation {
    pool_name: &'static str,
    caller: &'static str,
}

impl tantivy::TaskInstrumentation for ThreadPoolTaskInstrumentation {
    fn enqueue(&self) -> Box<dyn tantivy::EnqueuedTask> {
        Box::new(QueuedTask::new(self.pool_name, self.caller))
    }
}

impl tantivy::EnqueuedTask for QueuedTask {
    fn run(self: Box<Self>) -> Box<dyn tantivy::RunningTask> {
        Box::new(self.start())
    }
}

impl tantivy::RunningTask for RunningTaskGuard {}

/// Run a small (<200ms) CPU-intensive task on a dedicated thread pool with a few threads.
///
/// When running blocking io (or side-effects in general), prefer using `tokio::spawn_blocking`
/// instead. When running long tasks or a set of tasks that you expect to take more than 33% of
/// your vCPUs, use a dedicated thread/runtime or executor instead.
///
/// Disclaimer: The function will no be executed if the Future is dropped.
#[must_use = "run_cpu_intensive will not run if the future it returns is dropped"]
pub fn run_cpu_intensive<F, R>(cpu_intensive_fn: F) -> impl Future<Output = Result<R, Panicked>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    static SMALL_TASK_EXECUTOR: std::sync::OnceLock<ThreadPool> = std::sync::OnceLock::new();
    SMALL_TASK_EXECUTOR
        .get_or_init(|| {
            let num_threads: usize = (crate::num_cpus() / 3).max(2);
            ThreadPool::new("small_tasks", Some(num_threads))
        })
        .run_cpu_intensive(cpu_intensive_fn)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Panicked;

impl fmt::Display for Panicked {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "scheduled task panicked")
    }
}

impl std::error::Error for Panicked {}

struct ThreadPoolMetrics {
    ongoing_tasks: IntGaugeVec<2>,
    pending_tasks: IntGaugeVec<2>,
    queue_wait_time_secs: HistogramVec<2>,
    run_time_secs: HistogramVec<2>,
}

/// From 1ms to ~32.768s
fn wait_and_run_time_buckets() -> Vec<f64> {
    exponential_buckets(0.001, 2.0, 16).unwrap()
}

impl Default for ThreadPoolMetrics {
    fn default() -> Self {
        ThreadPoolMetrics {
            ongoing_tasks: new_gauge_vec(
                "ongoing_tasks",
                "number of tasks being currently processed by threads in the thread pool",
                "thread_pool",
                &[],
                ["pool", "caller"],
            ),
            pending_tasks: new_gauge_vec(
                "pending_tasks",
                "number of tasks waiting in the queue before being processed by the thread pool",
                "thread_pool",
                &[],
                ["pool", "caller"],
            ),
            queue_wait_time_secs: new_histogram_vec(
                "queue_wait_time_secs",
                "amount of time a task waited in the queue before being picked up by a thread in \
                 the thread pool",
                "thread_pool",
                &[],
                ["pool", "caller"],
                wait_and_run_time_buckets(),
            ),
            run_time_secs: new_histogram_vec(
                "run_time_secs",
                "amount of time spent actually running a task on a thread pool worker, once it \
                 has been picked up from the queue",
                "thread_pool",
                &[],
                ["pool", "caller"],
                wait_and_run_time_buckets(),
            ),
        }
    }
}

static THREAD_POOL_METRICS: Lazy<ThreadPoolMetrics> = Lazy::new(ThreadPoolMetrics::default);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_run_cpu_intensive() {
        assert_eq!(run_cpu_intensive(|| 1).await, Ok(1));
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panicks() {
        assert!(run_cpu_intensive(|| panic!("")).await.is_err());
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panicks_do_not_shrink_thread_pool() {
        for _ in 0..100 {
            assert!(run_cpu_intensive(|| panic!("")).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_abort() {
        let counter: Arc<AtomicU64> = Default::default();
        let mut futures = Vec::new();
        for _ in 0..1_000 {
            let counter_clone = counter.clone();
            let fut = run_cpu_intensive(move || {
                std::thread::sleep(Duration::from_millis(5));
                counter_clone.fetch_add(1, Ordering::SeqCst)
            });
            // The first few num_cores tasks should run, but the other should get cancelled.
            futures.push(tokio::time::timeout(Duration::from_millis(1), fut));
        }
        futures::future::join_all(futures).await;
        assert!(counter.load(Ordering::SeqCst) < 100);
    }
}
