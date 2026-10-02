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

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
         inclusive times: `a` includes `a:b`. With task poll attribution enabled, `s3` covers the \
         polls of the tasks spawned by the S3 client (its pooled connections).",
        "runtime",
        &[],
        ["name"],
        vec![0.001, 0.016, 0.256, 4.096],
    )
});

thread_local! {
    /// Whether the tasks spawned from this thread right now do work for S3: set while an S3
    /// request, or a task spawned by one, is being polled.
    static IN_S3: Cell<bool> = const { Cell::new(false) };
}

/// Restores the previous value of [`IN_S3`] on drop, including when the inner poll panics.
struct InS3Guard {
    previous: bool,
}

impl InS3Guard {
    fn enter() -> Self {
        InS3Guard {
            previous: IN_S3.replace(true),
        }
    }
}

impl Drop for InS3Guard {
    fn drop(&mut self) {
        IN_S3.set(self.previous);
    }
}

/// Extension trait marking a future as an S3 request.
pub trait S3ScopeExt: Sized {
    /// With task poll attribution enabled (see [`configure_task_poll_attribution`]), the tasks
    /// spawned while this future is being polled, and their own descendants, are recorded as
    /// `s3`.
    ///
    /// The S3 client's connections run in tasks spawned by the first request that needed
    /// them, then shared by every later request through the pool: this attributes their work
    /// to S3 rather than to whichever caller opened them.
    fn in_s3_scope(self) -> S3Scope<Self>;
}

impl<F: Future> S3ScopeExt for F {
    fn in_s3_scope(self) -> S3Scope<F> {
        S3Scope { inner: self }
    }
}

/// Future returned by [`S3ScopeExt::in_s3_scope`].
#[pin_project]
pub struct S3Scope<F> {
    #[pin]
    inner: F,
}

impl<F: Future> Future for S3Scope<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _in_s3_guard = InS3Guard::enter();
        self.project().inner.poll(cx)
    }
}

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

    /// Like [`Self::detect_slow_poll`], but records each poll under `first_name` or
    /// `second_name` depending on whether [`SlowPollPhase::enter_second_phase`] was called on
    /// `phase` before that poll started.
    ///
    /// This splits a future we don't control at a point observable from the outside, for
    /// instance when it calls back into a closure we provide. The poll during which the switch
    /// happens is recorded under `first_name`.
    fn detect_slow_poll_two_phases(
        self,
        first_name: &'static str,
        second_name: &'static str,
        phase: SlowPollPhase,
    ) -> DetectSlowPollTwoPhases<Self>;
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

    fn detect_slow_poll_two_phases(
        self,
        first_name: &'static str,
        second_name: &'static str,
        phase: SlowPollPhase,
    ) -> DetectSlowPollTwoPhases<F> {
        DetectSlowPollTwoPhases {
            inner: self,
            phase,
            first: (
                first_name,
                POLL_DURATION_HISTOGRAM.with_label_values([first_name]),
            ),
            second: (
                second_name,
                POLL_DURATION_HISTOGRAM.with_label_values([second_name]),
            ),
        }
    }
}

/// Records one poll of an instrumented future.
fn record_poll(name: &'static str, poll_duration_histogram: &Histogram, elapsed: Duration) {
    poll_duration_histogram.observe(elapsed.as_secs_f64());
    if elapsed >= *SLOW_POLL_LOG_THRESHOLD {
        crate::rate_limited_warn!(
            limit_per_min = 10,
            name = name,
            elapsed_millis = elapsed.as_millis() as u64,
            "slow poll detected"
        );
    }
}

/// Phase of a future instrumented with [`DetectSlowPollExt::detect_slow_poll_two_phases`].
/// Clones share the same phase.
#[derive(Clone, Default)]
pub struct SlowPollPhase(Arc<AtomicBool>);

impl SlowPollPhase {
    /// Polls starting after this call are recorded under the second name.
    pub fn enter_second_phase(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    fn is_second_phase(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// Future returned by [`DetectSlowPollExt::detect_slow_poll_two_phases`].
#[pin_project]
pub struct DetectSlowPollTwoPhases<F> {
    #[pin]
    inner: F,
    phase: SlowPollPhase,
    first: (&'static str, Histogram),
    second: (&'static str, Histogram),
}

impl<F: Future> Future for DetectSlowPollTwoPhases<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let (name, poll_duration_histogram) = if this.phase.is_second_phase() {
            this.second
        } else {
            this.first
        };
        let start = Instant::now();
        let poll = this.inner.poll(cx);
        record_poll(name, poll_duration_histogram, start.elapsed());
        poll
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
        record_poll(this.name, this.poll_duration_histogram, start.elapsed());
        poll
    }
}

/// Installs task hooks on `runtime_builder` that record the polls of the tasks spawned by
/// S3 requests (see [`S3ScopeExt::in_s3_scope`]) under `s3`, when
/// `QW_TOKIO_TASK_POLL_ATTRIBUTION` is set.
///
/// These tasks are spawned by the S3 client's HTTP library, so they cannot be wrapped with
/// [`DetectSlowPollExt::detect_slow_poll`] directly. It costs a lookup in a sharded set per
/// task poll, hence the opt-in.
pub fn configure_task_poll_attribution(runtime_builder: &mut tokio::runtime::Builder) {
    if !crate::get_bool_from_env("QW_TOKIO_TASK_POLL_ATTRIBUTION", false) {
        return;
    }
    #[cfg(not(tokio_unstable))]
    {
        let _ = runtime_builder;
        tracing::warn!(
            "`QW_TOKIO_TASK_POLL_ATTRIBUTION` requires `--cfg tokio_unstable`, ignoring it"
        );
    }
    #[cfg(tokio_unstable)]
    s3_task_polls::install(runtime_builder);
}

#[cfg(tokio_unstable)]
mod s3_task_polls {
    use std::collections::HashSet;
    use std::hash::{BuildHasher, RandomState};
    use std::sync::Mutex;

    use tokio::runtime::TaskMeta;
    use tokio::task::Id;

    use super::*;

    const NUM_SHARDS: usize = 64;

    /// Ids of the live tasks spawned by S3 requests.
    struct S3Tasks {
        hasher: RandomState,
        shards: [Mutex<HashSet<Id>>; NUM_SHARDS],
    }

    impl S3Tasks {
        fn shard(&self, task_id: Id) -> &Mutex<HashSet<Id>> {
            &self.shards[self.hasher.hash_one(task_id) as usize % NUM_SHARDS]
        }
    }

    static S3_TASKS: Lazy<S3Tasks> = Lazy::new(|| S3Tasks {
        hasher: RandomState::new(),
        shards: std::array::from_fn(|_| Mutex::new(HashSet::new())),
    });

    static S3_POLL_DURATION_HISTOGRAM: Lazy<Histogram> =
        Lazy::new(|| POLL_DURATION_HISTOGRAM.with_label_values(["s3"]));

    thread_local! {
        /// Start of the S3 task poll in progress on this worker, if any.
        static S3_TASK_POLL_START: Cell<Option<Instant>> = const { Cell::new(None) };
    }

    pub(super) fn install(runtime_builder: &mut tokio::runtime::Builder) {
        runtime_builder
            .on_task_spawn(on_task_spawn)
            .on_task_terminate(on_task_terminate)
            .on_before_task_poll(on_before_task_poll)
            .on_after_task_poll(on_after_task_poll);
    }

    fn on_task_spawn(task_meta: &TaskMeta<'_>) {
        if !IN_S3.get() {
            return;
        }
        let task_id = task_meta.id();
        S3_TASKS.shard(task_id).lock().unwrap().insert(task_id);
    }

    fn on_task_terminate(task_meta: &TaskMeta<'_>) {
        // Also called for `spawn_blocking` tasks, which never went through `on_task_spawn`.
        let task_id = task_meta.id();
        S3_TASKS.shard(task_id).lock().unwrap().remove(&task_id);
    }

    fn on_before_task_poll(task_meta: &TaskMeta<'_>) {
        let task_id = task_meta.id();
        let is_s3_task = S3_TASKS.shard(task_id).lock().unwrap().contains(&task_id);
        // Tasks spawned by an S3 task, e.g. a connection spawned by a background connect, do
        // S3 work too.
        IN_S3.set(is_s3_task);
        S3_TASK_POLL_START.set(is_s3_task.then(Instant::now));
    }

    fn on_after_task_poll(_task_meta: &TaskMeta<'_>) {
        IN_S3.set(false);
        if let Some(start) = S3_TASK_POLL_START.take() {
            S3_POLL_DURATION_HISTOGRAM.observe(start.elapsed().as_secs_f64());
        }
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

    #[tokio::test]
    async fn test_detect_slow_poll_two_phases_switches_histogram() {
        let first_histogram = POLL_DURATION_HISTOGRAM.with_label_values(["test_first_phase"]);
        let second_histogram = POLL_DURATION_HISTOGRAM.with_label_values(["test_second_phase"]);
        let first_count_before = first_histogram.get_sample_count();
        let second_count_before = second_histogram.get_sample_count();

        let phase = SlowPollPhase::default();
        let phase_clone = phase.clone();
        let mut num_polls = 0;
        let three_poll_future = std::future::poll_fn(move |cx| {
            num_polls += 1;
            if num_polls == 3 {
                return Poll::Ready(42);
            }
            // The switch happens during the first poll, which still counts as first phase.
            if num_polls == 1 {
                phase_clone.enter_second_phase();
            }
            cx.waker().wake_by_ref();
            Poll::Pending
        });
        let output = three_poll_future
            .detect_slow_poll_two_phases("test_first_phase", "test_second_phase", phase)
            .await;

        assert_eq!(output, 42);
        assert_eq!(first_histogram.get_sample_count(), first_count_before + 1);
        assert_eq!(second_histogram.get_sample_count(), second_count_before + 2);
    }

    #[cfg(tokio_unstable)]
    #[test]
    fn test_s3_task_polls() {
        fn spin(duration: Duration) {
            let start = Instant::now();
            while start.elapsed() < duration {
                std::hint::black_box(0u64);
            }
        }
        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
        runtime_builder.worker_threads(1).enable_all();
        s3_task_polls::install(&mut runtime_builder);
        let runtime = runtime_builder.build().unwrap();

        // This is the only test installing the hooks, so it is the only one recording `s3`.
        let s3_histogram = POLL_DURATION_HISTOGRAM.with_label_values(["s3"]);
        let s3_sum_before = s3_histogram.get_sample_sum();

        runtime.block_on(async {
            // A task spawned by an S3 request, and the task it spawns in turn, do S3 work.
            // `tokio::spawn` spawns when called, so it must run inside the S3 scope.
            async {
                tokio::spawn(async {
                    spin(Duration::from_millis(10));
                    tokio::spawn(async { spin(Duration::from_millis(10)) })
                        .await
                        .unwrap();
                })
                .await
            }
            .in_s3_scope()
            .await
            .unwrap();

            // Tasks spawned outside of an S3 request don't.
            tokio::spawn(async { spin(Duration::from_millis(100)) })
                .await
                .unwrap();
        });

        let s3_sum = s3_histogram.get_sample_sum() - s3_sum_before;
        assert!(s3_sum >= 0.018, "{s3_sum}");
        assert!(s3_sum < 0.090, "{s3_sum}");
    }
}
