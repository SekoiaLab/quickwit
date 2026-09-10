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

use once_cell::sync::Lazy;

use crate::metrics::{
    HistogramVec, IntGauge, IntGaugeVec, exponential_buckets, new_gauge, new_gauge_vec,
    new_histogram_vec,
};

pub(super) struct SchedulerMetrics {
    /// Number of queries currently registered with the scheduler.
    pub(super) queries: IntGauge,
}

impl Default for SchedulerMetrics {
    fn default() -> Self {
        SchedulerMetrics {
            queries: new_gauge(
                "scheduler_queries",
                "number of queries currently registered with the CPU scheduler",
                "thread_pool",
                &[],
            ),
        }
    }
}

pub(super) static SCHEDULER_METRICS: Lazy<SchedulerMetrics> = Lazy::new(SchedulerMetrics::default);

pub(super) struct ThreadPoolMetrics {
    pub(super) ongoing_tasks: IntGaugeVec<3>,
    pub(super) pending_tasks: IntGaugeVec<3>,
    pub(super) queue_wait_time_secs: HistogramVec<3>,
    pub(super) run_time_secs: HistogramVec<3>,
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

pub(super) static THREAD_POOL_METRICS: Lazy<ThreadPoolMetrics> =
    Lazy::new(ThreadPoolMetrics::default);
