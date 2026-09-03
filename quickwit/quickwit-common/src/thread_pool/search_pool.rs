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

use super::scheduler::{QueryId, Scheduler, SchedulerSplitGuard};
use super::{Panicked, ThreadPool};

/// A [`ThreadPool`] with a per-query fair-share priority scheduler (see
/// [`super::scheduler`]) sitting in front of it, for CPU-intensive tasks that
/// belong to a specific query and must be prioritized/fair-shared against
/// each other. The plain [`ThreadPool`] has no notion of query and dispatches
/// FIFO, which is all one-off or query-less CPU work needs.
#[derive(Clone)]
pub struct SearchThreadPool {
    thread_pool: ThreadPool,
    scheduler: Arc<Scheduler>,
}

impl SearchThreadPool {
    pub fn new(name: &'static str, num_threads_opt: Option<usize>) -> SearchThreadPool {
        let thread_pool = ThreadPool::new(name, num_threads_opt);
        let scheduler = Scheduler::new(thread_pool.rayon_pool.clone());
        SearchThreadPool {
            thread_pool,
            scheduler,
        }
    }

    /// Registers a new query for per-query fair-share scheduling. See
    /// [`Scheduler::register_query`].
    pub fn register_query(
        &self,
        query_id: QueryId,
        total_splits: usize,
    ) -> Vec<SchedulerSplitGuard> {
        self.scheduler.register_query(query_id, total_splits)
    }

    /// See [`Scheduler::set_waiting_for_permit`].
    pub fn set_waiting_for_permit(&self, query_id: QueryId, waiting_for_permit: usize) {
        self.scheduler
            .set_waiting_for_permit(query_id, waiting_for_permit);
    }

    /// Returns a Tantivy [`tantivy::Executor`] backed by this thread pool.
    ///
    /// Tasks that Tantivy schedules through it are tracked by metrics, but --
    /// unlike [`Self::run_cpu_intensive_fair`] -- bypass the per-query
    /// priority scheduler entirely: Tantivy dispatches directly onto the raw
    /// rayon pool.
    pub fn get_executor(
        &self,
        caller: &'static str,
        cost_class: &'static str,
    ) -> tantivy::Executor {
        self.thread_pool.get_executor(caller, cost_class)
    }

    /// Runs a CPU-intensive task belonging to `query_id`, subject to the
    /// per-query fair-share scheduling and priority ordering described in
    /// [`super::scheduler`]. `query_id` must already have been registered via
    /// [`Self::register_query`].
    pub fn run_cpu_intensive_fair<F, R>(
        &self,
        cpu_intensive_fn: F,
        query_id: QueryId,
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
            move |job| self.scheduler.enqueue_fair(query_id, job),
        )
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
