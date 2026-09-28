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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use pin_project::pin_project;
use prometheus::Histogram;

use crate::metrics::{HistogramVec, new_histogram_vec};

/// Duration above which a poll of an instrumented future is logged (rate limited), on
/// top of being recorded in the histogram.
static SLOW_POLL_LOG_THRESHOLD: Lazy<Duration> = Lazy::new(|| {
    Duration::from_millis(crate::get_from_env(
        "QW_SLOW_POLL_LOG_THRESHOLD_MS",
        50u64,
        false,
    ))
});

static POLL_DURATION_HISTOGRAM: Lazy<HistogramVec<1>> = Lazy::new(|| {
    new_histogram_vec(
        "task_poll_duration_seconds",
        "Duration of individual polls of futures instrumented with `detect_slow_poll`. A poll \
         blocks a tokio worker for its entire duration. Nested instrumentation points report \
         inclusive times: `a` includes `a:b`.",
        "runtime",
        &[],
        ["name"],
        vec![0.001, 0.004, 0.016, 0.064, 0.256, 1.024, 4.096],
    )
});

/// Extension trait instrumenting a future to detect polls that block the runtime.
pub trait DetectSlowPollExt: Sized {
    /// Records the duration of every poll of this future in the
    /// `quickwit_runtime_task_poll_duration_seconds` histogram under `name`, and logs a
    /// rate-limited warning when a poll exceeds `QW_SLOW_POLL_LOG_THRESHOLD_MS`
    /// (default: 50ms).
    ///
    /// The log line is emitted from within the poll, so it carries the tracing span
    /// that is entered at that point. For the span to be the task's span, apply this
    /// wrapper *before* `.instrument(...)` / `.in_current_span()`, never after.
    ///
    /// Nested wrappers measure inclusive durations. By convention, an instrumentation
    /// point nested inside another one is named `<outer_name>:<inner_name>`.
    fn detect_slow_poll(self, name: &'static str) -> DetectSlowPoll<Self>;
}

impl<F: Future> DetectSlowPollExt for F {
    fn detect_slow_poll(self, name: &'static str) -> DetectSlowPoll<F> {
        // Resolving the histogram here keeps the per-poll cost down to reading the
        // clock twice and one atomic update.
        let poll_duration_histogram = POLL_DURATION_HISTOGRAM.with_label_values([name]);
        DetectSlowPoll {
            inner: self,
            name,
            poll_duration_histogram,
        }
    }
}

/// Future returned by [`DetectSlowPollExt::detect_slow_poll`].
#[pin_project]
pub struct DetectSlowPoll<F> {
    #[pin]
    inner: F,
    name: &'static str,
    poll_duration_histogram: Histogram,
}

impl<F: Future> Future for DetectSlowPoll<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let start = Instant::now();
        let poll = this.inner.poll(cx);
        let elapsed = start.elapsed();
        this.poll_duration_histogram.observe(elapsed.as_secs_f64());
        if elapsed >= *SLOW_POLL_LOG_THRESHOLD {
            crate::rate_limited_warn!(
                limit_per_min = 10,
                name = this.name,
                elapsed_millis = elapsed.as_millis() as u64,
                "slow poll detected"
            );
        }
        poll
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use super::*;

    #[tokio::test]
    async fn test_detect_slow_poll_records_every_poll() {
        let histogram = POLL_DURATION_HISTOGRAM.with_label_values(["test_wrapper"]);
        let sample_count_before = histogram.get_sample_count();

        let mut polled_once = false;
        let two_poll_future = std::future::poll_fn(move |cx| {
            if polled_once {
                Poll::Ready(42)
            } else {
                polled_once = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        });
        let output = two_poll_future.detect_slow_poll("test_wrapper").await;

        assert_eq!(output, 42);
        assert_eq!(histogram.get_sample_count(), sample_count_before + 2);
    }
}
