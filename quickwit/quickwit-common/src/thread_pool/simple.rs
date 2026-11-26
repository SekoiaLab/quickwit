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

use std::sync::{Arc, OnceLock};

use futures::{Future, TryFutureExt};
use tokio::sync::oneshot;
use tracing::{info, warn};

use super::{Panicked, QueuedTask, ThreadPoolTaskInstrumentation, build_rayon_pool};

/// An executor backed by a thread pool to run CPU-intensive tasks.
///
/// tokio::spawn_blocking should only used for IO-bound tasks, as it has not limit on its
/// thread count.
struct SimpleThreadPool {
    thread_pool: Arc<rayon::ThreadPool>,
    name: &'static str,
}

impl SimpleThreadPool {
    fn new(name: &'static str, num_threads_opt: Option<usize>) -> SimpleThreadPool {
        SimpleThreadPool {
            thread_pool: Arc::new(build_rayon_pool(name, num_threads_opt)),
            name,
        }
    }

    /// Returns a Tantivy [`tantivy::Executor`] backed by this thread pool.
    ///
    /// Tasks that Tantivy schedules through it are tracked by metrics.
    fn get_executor(&self, caller: &'static str) -> tantivy::Executor {
        tantivy::Executor::InstrumentedThreadPool(
            self.thread_pool.clone(),
            Arc::new(ThreadPoolTaskInstrumentation {
                pool_name: self.name,
                caller,
            }),
        )
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
    fn run_cpu_intensive_with_extra_tags<F, R>(
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
}

/// Computes the number of threads to use for the small tasks thread pool.
///
/// The number of threads is picked, in order of precedence, from:
/// - the `QW_SMALL_TASKS_THREAD_POOL_NUM_CPUS` environment variable, if set
/// - a third of the available CPUs (at least 2), otherwise
fn compute_small_tasks_thread_pool_num_threads() -> usize {
    if let Some(num_cpus) =
        crate::get_from_env_opt::<usize>("QW_SMALL_TASKS_THREAD_POOL_NUM_CPUS", false)
    {
        if num_cpus == 0 {
            warn!("QW_SMALL_TASKS_THREAD_POOL_NUM_CPUS is set to 0, ignoring it");
        } else {
            info!(
                threads = num_cpus,
                "small tasks thread pool configured from QW_SMALL_TASKS_THREAD_POOL_NUM_CPUS"
            );
            return num_cpus;
        }
    }
    let threads = (crate::num_cpus() / 3).max(2);
    info!(
        threads,
        "small tasks thread pool configured with a third of the CPUs"
    );
    threads
}

fn small_task_executor() -> &'static SimpleThreadPool {
    static SMALL_TASK_EXECUTOR: OnceLock<SimpleThreadPool> = OnceLock::new();
    SMALL_TASK_EXECUTOR.get_or_init(|| {
        let num_threads = compute_small_tasks_thread_pool_num_threads();
        SimpleThreadPool::new("small_tasks", Some(num_threads))
    })
}

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
    small_task_executor().run_cpu_intensive_with_extra_tags(cpu_intensive_fn, "unknown")
}

/// Returns a Tantivy [`tantivy::Executor`] backed by the small tasks thread pool used by
/// [`run_cpu_intensive`].
///
/// Tasks that Tantivy schedules through it are tracked by metrics, labeled with `caller`.
pub fn small_tasks_tantivy_executor(caller: &'static str) -> tantivy::Executor {
    small_task_executor().get_executor(caller)
}

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

    // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1, as
    // it mutates a process-wide environment variable. The cases are checked in a single test so
    // that they don't race with each other on it.
    #[test]
    fn test_compute_small_tasks_thread_pool_num_threads() {
        let default_num_threads = (crate::num_cpus() / 3).max(2);
        unsafe { std::env::remove_var("QW_SMALL_TASKS_THREAD_POOL_NUM_CPUS") };
        assert_eq!(
            compute_small_tasks_thread_pool_num_threads(),
            default_num_threads
        );

        unsafe { std::env::set_var("QW_SMALL_TASKS_THREAD_POOL_NUM_CPUS", "3") };
        assert_eq!(compute_small_tasks_thread_pool_num_threads(), 3);

        unsafe { std::env::set_var("QW_SMALL_TASKS_THREAD_POOL_NUM_CPUS", "0") };
        assert_eq!(
            compute_small_tasks_thread_pool_num_threads(),
            default_num_threads
        );
        unsafe { std::env::remove_var("QW_SMALL_TASKS_THREAD_POOL_NUM_CPUS") };
    }
}
