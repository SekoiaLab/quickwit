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
    Histogram, HistogramVec, IntGauge, IntGaugeVec, exponential_buckets, new_gauge, new_gauge_vec,
    new_histogram, new_histogram_vec,
};

pub(super) struct SchedulerMetrics {
    /// Number of queries currently registered with the scheduler.
    pub(super) queries: IntGauge,
    pub(super) dispatch_latency_secs: HistogramVec<1>,
    pub(super) rayon_pickup_latency_secs: Histogram,
    pub(super) actor_lag_secs: HistogramVec<1>,
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
            dispatch_latency_secs: new_histogram_vec(
                "scheduler_dispatch_latency_secs",
                "amount of time between a task being submitted to the CPU scheduler and the \
                 scheduler actor handing it over to rayon",
                "thread_pool",
                &[],
                ["tier"],
                latency_buckets(),
            ),
            rayon_pickup_latency_secs: new_histogram(
                "scheduler_rayon_pickup_latency_secs",
                "amount of time between the scheduler actor spawning a task on rayon and a rayon \
                 worker starting to run it",
                "thread_pool",
                latency_buckets(),
            ),
            actor_lag_secs: new_histogram_vec(
                "scheduler_actor_lag_secs",
                "amount of time between a task being submitted to the CPU scheduler and the \
                 scheduler actor receiving it. Subtract from dispatch_latency_secs on the same \
                 tier to get the time the task then spent queued",
                "thread_pool",
                &[],
                ["tier"],
                latency_buckets(),
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

/// From 25us to ~52s. The low end matters: dispatch overhead and the shortest
/// tasks (`finalize` runs in tens of microseconds) both live well below 1ms.
fn latency_buckets() -> Vec<f64> {
    exponential_buckets(0.000_025, 2.0, 22).unwrap()
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
                latency_buckets(),
            ),
            run_time_secs: new_histogram_vec(
                "run_time_secs",
                "amount of time spent actually running a task on a thread pool worker, once it \
                 has been picked up from the queue",
                "thread_pool",
                &[],
                ["pool", "caller", "cost_class"],
                latency_buckets(),
            ),
        }
    }
}

pub(super) static THREAD_POOL_METRICS: Lazy<ThreadPoolMetrics> =
    Lazy::new(ThreadPoolMetrics::default);
