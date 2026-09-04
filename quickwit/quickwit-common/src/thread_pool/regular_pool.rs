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

use futures::Future;
use tracing::error;

use super::ThreadPoolTaskInstrumentation;

/// An executor backed by a thread pool to run CPU-intensive tasks.
///
/// tokio::spawn_blocking should only be used for IO-bound tasks, as it has not
/// limit on its thread count.
///
/// Unlike [`super::SearchThreadPool`], dispatches FIFO straight onto the raw
/// rayon pool, with no per-query priority/fairness layer.
#[derive(Clone)]
pub struct ThreadPool {
    pub(super) rayon_pool: Arc<rayon::ThreadPool>,
    pub(super) name: &'static str,
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
        let rayon_pool = rayon_pool_builder
            .build()
            .expect("failed to spawn thread pool");
        ThreadPool {
            rayon_pool: Arc::new(rayon_pool),
            name,
        }
    }

    /// Returns a Tantivy [`tantivy::Executor`] backed by this thread pool.
    ///
    /// Tasks that Tantivy schedules through it are tracked by metrics.
    pub fn get_executor(
        &self,
        caller: &'static str,
        cost_class: &'static str,
    ) -> tantivy::Executor {
        tantivy::Executor::InstrumentedThreadPool(
            self.rayon_pool.clone(),
            Arc::new(ThreadPoolTaskInstrumentation {
                pool_name: self.name,
                caller,
                cost_class,
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
    pub fn run_cpu_intensive<F, R>(
        &self,
        cpu_intensive_fn: F,
        caller: &'static str,
        cost_class: &'static str,
    ) -> impl Future<Output = Result<R, Panicked>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        super::spawn_traced(self.name, caller, cost_class, cpu_intensive_fn, |job| {
            self.rayon_pool.spawn(job)
        })
    }
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
    static SMALL_TASK_EXECUTOR: std::sync::OnceLock<ThreadPool> = std::sync::OnceLock::new();
    SMALL_TASK_EXECUTOR
        .get_or_init(|| {
            let num_threads: usize = (crate::num_cpus() / 3).max(2);
            ThreadPool::new("small_tasks", Some(num_threads))
        })
        .run_cpu_intensive(cpu_intensive_fn, "unknown", "NA")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Panicked;

impl fmt::Display for Panicked {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "scheduled task panicked")
    }
}

impl std::error::Error for Panicked {}

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
