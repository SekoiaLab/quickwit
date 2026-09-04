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

use futures::{Future, TryFutureExt};
use once_cell::sync::Lazy;
use tokio::sync::oneshot;

use crate::metrics::{
    Histogram, HistogramTimer, HistogramVec, IntGauge, IntGaugeVec, OwnedGaugeGuard,
    exponential_buckets, new_gauge_vec, new_histogram_vec,
};

pub mod scheduler;

mod regular_pool;
mod search_pool;

pub use regular_pool::{Panicked, ThreadPool, run_cpu_intensive};
pub use search_pool::SearchThreadPool;

/// Wraps `cpu_intensive_fn` with cancellation-awareness and queue/run-time
/// metrics tracking, then hands the resulting job to `dispatch` for actual
/// scheduling (a raw rayon spawn, or one of the scheduler's queues).
fn spawn_traced<F, R>(
    pool_name: &'static str,
    caller: &'static str,
    cost_class: &'static str,
    cpu_intensive_fn: F,
    dispatch: impl FnOnce(Box<dyn FnOnce() + Send>),
) -> impl Future<Output = Result<R, Panicked>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let span = tracing::Span::current();
    let queued_task = QueuedTask::new(pool_name, caller, cost_class);
    let (tx, rx) = oneshot::channel();
    dispatch(Box::new(move || {
        if tx.is_closed() {
            // dropping `queued_task` still records the time it spent queued
            return;
        }
        let _guard = span.enter();
        let running_task = queued_task.start();
        let result = cpu_intensive_fn();
        drop(running_task);
        let _ = tx.send(result);
    }));
    rx.map_err(|_| Panicked)
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
    fn new(pool_name: &'static str, caller: &'static str, cost_class: &'static str) -> QueuedTask {
        let labels = [pool_name, caller, cost_class];
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
    cost_class: &'static str,
}

impl tantivy::TaskInstrumentation for ThreadPoolTaskInstrumentation {
    fn enqueue(&self) -> Box<dyn tantivy::EnqueuedTask> {
        Box::new(QueuedTask::new(
            self.pool_name,
            self.caller,
            self.cost_class,
        ))
    }
}

impl tantivy::EnqueuedTask for QueuedTask {
    fn run(self: Box<Self>) -> Box<dyn tantivy::RunningTask> {
        Box::new(self.start())
    }
}

impl tantivy::RunningTask for RunningTaskGuard {}

struct ThreadPoolMetrics {
    ongoing_tasks: IntGaugeVec<3>,
    pending_tasks: IntGaugeVec<3>,
    queue_wait_time_secs: HistogramVec<3>,
    run_time_secs: HistogramVec<3>,
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
                ["pool", "caller", "cost_class"],
            ),
            pending_tasks: new_gauge_vec(
                "pending_tasks",
                "number of tasks waiting in the queue before being processed by the thread pool",
                "thread_pool",
                &[],
                ["pool", "caller", "cost_class"],
            ),
            queue_wait_time_secs: new_histogram_vec(
                "queue_wait_time_secs",
                "amount of time a task waited in the queue before being picked up by a thread in \
                 the thread pool",
                "thread_pool",
                &[],
                ["pool", "caller", "cost_class"],
                wait_and_run_time_buckets(),
            ),
            run_time_secs: new_histogram_vec(
                "run_time_secs",
                "amount of time spent actually running a task on a thread pool worker, once it \
                 has been picked up from the queue",
                "thread_pool",
                &[],
                ["pool", "caller", "cost_class"],
                wait_and_run_time_buckets(),
            ),
        }
    }
}

static THREAD_POOL_METRICS: Lazy<ThreadPoolMetrics> = Lazy::new(ThreadPoolMetrics::default);
