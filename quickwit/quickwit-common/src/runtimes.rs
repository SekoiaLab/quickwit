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

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use once_cell::sync::OnceCell;
use prometheus::{Gauge, IntCounter, IntGauge};
use tokio::runtime::Runtime;
use tokio_metrics::{RuntimeMetrics, RuntimeMonitor};

use crate::metrics::{new_counter, new_counter_vec, new_float_gauge, new_gauge};

static RUNTIMES: OnceCell<HashMap<RuntimeType, tokio::runtime::Runtime>> = OnceCell::new();

/// Describes which runtime an actor should run on.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum RuntimeType {
    /// The blocking runtime runs blocking actors.
    /// This runtime is only used as a nice thread pool with
    /// the interface as tokio stasks.
    ///
    /// This runtime should not be used to run tokio
    /// io operations.
    ///
    /// Tasks are allowed to block for an arbitrary amount of time.
    Blocking,

    /// The non-blocking runtime is closer to what one would expect from
    /// a regular tokio runtime.
    ///
    /// Task are expect to yield within 500 micros.
    NonBlocking,
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimesConfig {
    /// Number of worker threads allocated to the non-blocking runtime.
    pub num_threads_non_blocking: usize,
    /// Number of worker threads allocated to the blocking runtime.
    pub num_threads_blocking: usize,
}

impl RuntimesConfig {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn light_for_tests() -> RuntimesConfig {
        RuntimesConfig {
            num_threads_blocking: 1,
            num_threads_non_blocking: 1,
        }
    }

    pub fn with_num_cpus(num_cpus: usize) -> Self {
        // Non blocking task are supposed to be io intensive, and not require many threads.
        // On the other hand the blocking actors are cpu intensive. We allocate
        // almost all of the threads to them.
        match num_cpus {
            0..=3 => {
                // We do not have enough vCPUs to allocate a full thread to
                // non-blocking.
                RuntimesConfig {
                    num_threads_non_blocking: 1,
                    num_threads_blocking: num_cpus,
                }
            }
            4..=6 => RuntimesConfig {
                num_threads_non_blocking: 1,
                num_threads_blocking: num_cpus - 1,
            },
            7.. => RuntimesConfig {
                num_threads_non_blocking: 2,
                num_threads_blocking: num_cpus - 2,
            },
        }
    }
}

impl Default for RuntimesConfig {
    fn default() -> Self {
        let num_cpus = crate::num_cpus();
        Self::with_num_cpus(num_cpus)
    }
}

fn start_runtimes(config: RuntimesConfig) -> HashMap<RuntimeType, Runtime> {
    let mut runtimes = HashMap::with_capacity(2);

    let disable_lifo_slot = crate::get_bool_from_env("QW_DISABLE_TOKIO_LIFO_SLOT", true);

    let mut blocking_runtime_builder = tokio::runtime::Builder::new_multi_thread();
    if disable_lifo_slot {
        blocking_runtime_builder.disable_lifo_slot();
    }
    configure_poll_time_histogram(&mut blocking_runtime_builder);
    let blocking_runtime = blocking_runtime_builder
        .worker_threads(config.num_threads_blocking)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("blocking-{id}")
        })
        .enable_all()
        .build()
        .unwrap();

    scrape_tokio_runtime_metrics(blocking_runtime.handle(), "blocking");
    runtimes.insert(RuntimeType::Blocking, blocking_runtime);

    let mut non_blocking_runtime_builder = tokio::runtime::Builder::new_multi_thread();
    configure_poll_time_histogram(&mut non_blocking_runtime_builder);
    let non_blocking_runtime = non_blocking_runtime_builder
        .worker_threads(config.num_threads_non_blocking)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("non-blocking-{id}")
        })
        .enable_all()
        .build()
        .unwrap();

    scrape_tokio_runtime_metrics(non_blocking_runtime.handle(), "non_blocking");
    runtimes.insert(RuntimeType::NonBlocking, non_blocking_runtime);

    runtimes
}

pub fn initialize_runtimes(runtimes_config: RuntimesConfig) -> anyhow::Result<()> {
    RUNTIMES.get_or_init(|| start_runtimes(runtimes_config));
    Ok(())
}

impl RuntimeType {
    pub fn get_runtime_handle(self) -> tokio::runtime::Handle {
        RUNTIMES
            .get_or_init(|| {
                #[cfg(any(test, feature = "testsuite"))]
                {
                    tracing::warn!("starting Tokio actor runtimes for tests");
                    start_runtimes(RuntimesConfig::light_for_tests())
                }
                #[cfg(not(any(test, feature = "testsuite")))]
                {
                    panic!("Tokio runtimes not initialized. Please, report this issue on GitHub: https://github.com/quickwit-oss/quickwit/issues.");
                }
            })
            .get(&self)
            .unwrap()
            .handle()
            .clone()
    }
}

/// Enables the Tokio poll time histogram on `runtime_builder`, when
/// `QW_TOKIO_POLL_TIME_HISTOGRAM` is set.
///
/// The histogram times every individual task poll. It is the only runtime metric that
/// tells a heavy tail of slow polls apart from a uniform slowdown, but it costs two
/// `Instant::now()` calls per poll, hence the opt-in.
pub fn configure_poll_time_histogram(runtime_builder: &mut tokio::runtime::Builder) {
    if !crate::get_bool_from_env("QW_TOKIO_POLL_TIME_HISTOGRAM", false) {
        return;
    }
    #[cfg(not(tokio_unstable))]
    {
        let _ = runtime_builder;
        tracing::warn!(
            "`QW_TOKIO_POLL_TIME_HISTOGRAM` requires `--cfg tokio_unstable`, ignoring it"
        );
    }
    #[cfg(tokio_unstable)]
    {
        // `precision_exact(0)` makes each bucket twice as wide as the previous one.
        // Tokio rounds the bounds to powers of two, yielding 22 buckets that span ~8us to
        // ~8.6s. Polls longer than that land in the final, unbounded bucket.
        let log_histogram = tokio::runtime::LogHistogram::builder()
            .min_value(Duration::from_micros(10))
            .max_value(Duration::from_secs(8))
            .precision_exact(0)
            .build();
        runtime_builder
            .enable_metrics_poll_time_histogram()
            .metrics_poll_time_histogram_configuration(
                tokio::runtime::HistogramConfiguration::log(log_histogram),
            );
    }
}

/// Upper bounds of the runtime's poll time histogram buckets, formatted as Prometheus `le`
/// label values. Empty when the histogram is disabled.
fn poll_time_bucket_bounds(handle: &tokio::runtime::Handle) -> Vec<String> {
    #[cfg(not(tokio_unstable))]
    {
        let _ = handle;
        Vec::new()
    }
    #[cfg(tokio_unstable)]
    {
        let runtime_metrics = handle.metrics();
        if !runtime_metrics.poll_time_histogram_enabled() {
            return Vec::new();
        }
        let num_buckets = runtime_metrics.poll_time_histogram_num_buckets();
        (0..num_buckets)
            .map(|bucket| {
                if bucket + 1 == num_buckets {
                    // The last bucket stretches to `u64::MAX` nanoseconds.
                    "+Inf".to_string()
                } else {
                    let bucket_end = runtime_metrics.poll_time_histogram_bucket_range(bucket).end;
                    bucket_end.as_secs_f64().to_string()
                }
            })
            .collect()
    }
}

/// Spawns a background task
pub fn scrape_tokio_runtime_metrics(handle: &tokio::runtime::Handle, label: &'static str) {
    let runtime_monitor = RuntimeMonitor::new(handle);
    let poll_time_bucket_bounds = poll_time_bucket_bounds(handle);
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut prometheus_runtime_metrics =
            PrometheusRuntimeMetrics::new(label, &poll_time_bucket_bounds);

        for tokio_runtime_metrics in runtime_monitor.intervals() {
            interval.tick().await;
            prometheus_runtime_metrics.update(&tokio_runtime_metrics);
        }
    });
}

struct PrometheusRuntimeMetrics {
    scheduled_tasks: IntGauge,
    worker_busy_duration_microsecs_total: IntCounter,
    worker_busy_ratio: Gauge,
    /// Cumulative poll duration buckets, ordered by increasing `le`. Empty when the poll
    /// time histogram is disabled.
    poll_duration_seconds_buckets: Vec<IntCounter>,
    worker_polls_total: IntCounter,
    worker_threads: IntGauge,
}

impl PrometheusRuntimeMetrics {
    pub fn new(label: &'static str, poll_time_bucket_bounds: &[String]) -> Self {
        let poll_duration_seconds_buckets = if poll_time_bucket_bounds.is_empty() {
            Vec::new()
        } else {
            let poll_duration_seconds_bucket = new_counter_vec::<1>(
                "tokio_poll_duration_seconds_bucket",
                "Cumulative number of task polls that completed within the bucket's upper bound \
                 `le`, in seconds.",
                "runtime",
                &[("runtime_type", label)],
                ["le"],
            );
            poll_time_bucket_bounds
                .iter()
                .map(|bucket_bound| {
                    poll_duration_seconds_bucket.with_label_values([bucket_bound.as_str()])
                })
                .collect()
        };
        Self {
            scheduled_tasks: new_gauge(
                "tokio_scheduled_tasks",
                "The total number of tasks currently scheduled in workers' local queues.",
                "runtime",
                &[("runtime_type", label)],
            ),
            worker_busy_duration_microsecs_total: new_counter(
                "tokio_worker_busy_duration_microsecs_total",
                "The total amount of time worker threads were busy.",
                "runtime",
                &[("runtime_type", label)],
            ),
            worker_busy_ratio: new_float_gauge(
                "tokio_worker_busy_ratio",
                "The ratio of time worker threads were busy since the last time runtime metrics \
                 were collected.",
                "runtime",
                &[("runtime_type", label)],
            ),
            poll_duration_seconds_buckets,
            #[cfg(tokio_unstable)]
            worker_polls_total: new_counter(
                "tokio_worker_polls_total",
                "The total number of times worker threads polled a task. Divide \
                 `tokio_worker_busy_duration_microsecs_total` by this to obtain the average poll \
                 duration.",
                "runtime",
                &[("runtime_type", label)],
            ),
            worker_threads: new_gauge(
                "tokio_worker_threads",
                "The number of worker threads used by the runtime.",
                "runtime",
                &[("runtime_type", label)],
            ),
        }
    }

    pub fn update(&mut self, runtime_metrics: &RuntimeMetrics) {
        self.scheduled_tasks
            .set(runtime_metrics.total_local_queue_depth as i64);
        self.worker_busy_duration_microsecs_total
            .inc_by(runtime_metrics.total_busy_duration.as_micros() as u64);
        self.worker_busy_ratio.set(runtime_metrics.busy_ratio());
        #[cfg(tokio_unstable)]
        {
            self.worker_polls_total
                .inc_by(runtime_metrics.total_polls_count);
            // `poll_time_histogram` holds this interval's per-bucket counts. Prometheus
            // buckets are cumulative, so each one takes the sum of all buckets up to it.
            let mut cumulative_count = 0;
            for (bucket_counter, bucket_count) in self
                .poll_duration_seconds_buckets
                .iter()
                .zip(&runtime_metrics.poll_time_histogram)
            {
                cumulative_count += *bucket_count;
                bucket_counter.inc_by(cumulative_count);
            }
        }
        self.worker_threads
            .set(runtime_metrics.workers_count as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtimes_config_default() {
        let runtime_default = RuntimesConfig::default();
        assert!(runtime_default.num_threads_non_blocking <= runtime_default.num_threads_blocking);
        assert!(runtime_default.num_threads_non_blocking <= 2);
    }

    #[test]
    fn test_runtimes_with_given_num_cpus_10() {
        let runtime = RuntimesConfig::with_num_cpus(10);
        assert_eq!(runtime.num_threads_blocking, 8);
        assert_eq!(runtime.num_threads_non_blocking, 2);
    }

    #[test]
    fn test_runtimes_with_given_num_cpus_3() {
        let runtime = RuntimesConfig::with_num_cpus(3);
        assert_eq!(runtime.num_threads_blocking, 3);
        assert_eq!(runtime.num_threads_non_blocking, 1);
    }

    #[cfg(tokio_unstable)]
    #[test]
    fn test_poll_time_bucket_bounds() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        // The histogram is opt-in: a runtime built without it exposes no buckets.
        assert!(poll_time_bucket_bounds(runtime.handle()).is_empty());

        let log_histogram = tokio::runtime::LogHistogram::builder()
            .min_value(Duration::from_micros(10))
            .max_value(Duration::from_secs(8))
            .precision_exact(0)
            .build();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .enable_metrics_poll_time_histogram()
            .metrics_poll_time_histogram_configuration(tokio::runtime::HistogramConfiguration::log(
                log_histogram,
            ))
            .build()
            .unwrap();
        let bucket_bounds = poll_time_bucket_bounds(runtime.handle());

        assert_eq!(bucket_bounds.last().unwrap(), "+Inf");
        // Bounds are seconds, strictly increasing, and bracket the range we configured.
        let finite_bounds: Vec<f64> = bucket_bounds[..bucket_bounds.len() - 1]
            .iter()
            .map(|bucket_bound| bucket_bound.parse().unwrap())
            .collect();
        assert!(finite_bounds.windows(2).all(|bounds| bounds[0] < bounds[1]));
        assert!(*finite_bounds.first().unwrap() <= 10e-6);
        assert!(*finite_bounds.last().unwrap() >= 8.0);
    }
}
