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
use quickwit_common::metrics::{HistogramVec, IntCounter, new_counter, new_histogram_vec};

pub struct CliMetrics {
    pub thread_unpark_duration_microseconds: HistogramVec<0>,
    pub thread_unpark_cpu_time_microseconds_total: IntCounter,
}

impl Default for CliMetrics {
    fn default() -> Self {
        CliMetrics {
            thread_unpark_duration_microseconds: new_histogram_vec(
                "thread_unpark_duration_microseconds",
                "Duration for which a thread of the main tokio runtime is unparked.",
                "cli",
                &[],
                [],
                quickwit_common::metrics::exponential_buckets(5.0, 5.0, 5).unwrap(),
            ),
            thread_unpark_cpu_time_microseconds_total: new_counter(
                "thread_unpark_cpu_time_microseconds_total",
                "CPU time consumed by threads of the main tokio runtime while unparked. Compare \
                 with `thread_unpark_duration_microseconds_sum` (wall time over the same \
                 intervals): the gap is time spent runnable but descheduled (CPU starvation, CFS \
                 throttling) or blocked in the kernel.",
                "cli",
                &[],
            ),
        }
    }
}

/// Serve counters exposes a bunch a set of metrics about the request received to quickwit.
pub static CLI_METRICS: Lazy<CliMetrics> = Lazy::new(CliMetrics::default);
