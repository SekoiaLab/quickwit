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

use std::sync::Arc;

use futures::Future;

use super::scheduler::{Scheduler, SchedulerSplitGuard};
use super::{Panicked, ThreadPool};

/// A [`ThreadPool`] with a per-query fair-share priority scheduler (see
/// [`super::scheduler`]) sitting in front of it, for CPU-intensive tasks that
/// belong to a specific query and must be prioritized/fair-shared against each
/// other.
#[derive(Clone)]
pub struct SearchThreadPool {
    thread_pool: ThreadPool,
    scheduler: Arc<Scheduler>,
}

impl SearchThreadPool {
    pub fn new(name: &'static str, num_threads_opt: Option<usize>) -> SearchThreadPool {
        let thread_pool = ThreadPool::new(name, num_threads_opt);
        let scheduler = Scheduler::new(thread_pool.rayon_pool.clone(), name);
        SearchThreadPool {
            thread_pool,
            scheduler,
        }
    }

    /// Registers a new query for per-query fair-share scheduling. See
    /// [`Scheduler::register_query`].
    pub fn register_query(&self, total_splits: usize) -> Vec<SchedulerSplitGuard> {
        self.scheduler.register_query(total_splits)
    }

    /// Returns a Tantivy [`tantivy::Executor`] backed by this thread pool.
    ///
    /// Tasks that Tantivy schedules through it are tracked by metrics, but --
    /// unlike [`Self::run_cpu_intensive`] -- bypass the per-query
    /// priority scheduler entirely: Tantivy dispatches directly onto the raw
    /// rayon pool.
    pub fn get_executor(
        &self,
        caller: &'static str,
        cost_class: &'static str,
    ) -> tantivy::Executor {
        self.thread_pool.get_executor(caller, cost_class)
    }

    /// Runs a CPU-intensive task ahead of any per-query fair-share task (see
    /// [`super::scheduler`]'s high-priority queue). Meant for short, rare,
    /// one-shot work such as finalizing or merging a query's results, and for
    /// callers with no query to fair-share against at all.
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
        super::spawn_traced(
            self.thread_pool.name,
            caller,
            cost_class,
            cpu_intensive_fn,
            |job| self.scheduler.enqueue_fifo(job),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_externally_submitted_panic_does_not_stop_the_scheduler() {
        crate::setup_logging_for_tests();
        let search_pool = SearchThreadPool::new("test", Some(1));
        let guards = search_pool.register_query(100_000);

        // Keep the worker continuously busy to simulate contention with
        // externally-submitted work.
        let mut futures = Vec::with_capacity(2_000);
        for guard in guards.iter().take(2_000) {
            futures.push(guard.run_cpu_intensive_fair(
                || std::thread::sleep(Duration::from_millis(10)),
                "test",
                "test",
            ));
        }

        // Submitted directly to the raw rayon pool, bypassing the scheduler
        // entirely.
        search_pool.thread_pool.rayon_pool.spawn(|| panic!("boom"));

        // Confirm externally-injected work still gets serviced afterward.
        let (tx, rx) = std::sync::mpsc::channel();
        search_pool
            .thread_pool
            .rayon_pool
            .spawn(move || tx.send(()).unwrap());
        rx.recv_timeout(Duration::from_secs(1))
            .expect("external work starved after the panic");

        // Confirm a task executed on the scheduler (with high prio) also gets
        // serviced.
        search_pool
            .run_cpu_intensive(|| {}, "test", "test")
            .await
            .expect("task should not panic");
    }
}
