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
         inclusive times: `a` includes `a:b`. With task poll attribution enabled, `a>` covers \
         tasks spawned (transitively) while `a` was being polled, and `unattributed` the other \
         tasks, both excluding the time already reported by an instrumentation point.",
        "runtime",
        &[],
        ["name"],
        vec![0.001, 0.004, 0.016, 0.064, 0.256, 1.024, 4.096],
    )
});

/// What a task spawned from the current thread gets attributed to.
#[derive(Clone, Copy)]
enum SpawnContext {
    /// A future instrumented with `detect_slow_poll(name)` is being polled.
    Instrumented(&'static str),
    /// A task that was itself spawned from an instrumented context is being polled.
    #[cfg_attr(not(tokio_unstable), allow(dead_code))]
    SpawnedFrom(&'static Histogram),
}

thread_local! {
    static SPAWN_CONTEXT: Cell<Option<SpawnContext>> = const { Cell::new(None) };
    /// Number of `DetectSlowPoll` polls currently on this thread's stack.
    static INSTRUMENTED_POLL_DEPTH: Cell<u32> = const { Cell::new(0) };
    /// Time spent in outermost `DetectSlowPoll` polls since the current task poll started.
    static INSTRUMENTED_POLL_NANOS: Cell<u64> = const { Cell::new(0) };
}

/// Publishes the instrumentation point being polled in the thread-local context, and
/// restores the previous context on drop, including when the inner poll panics.
struct InstrumentedPollGuard {
    previous_context: Option<SpawnContext>,
    depth: u32,
    start: Instant,
}

impl InstrumentedPollGuard {
    fn enter(name: &'static str) -> Self {
        let previous_context = SPAWN_CONTEXT.replace(Some(SpawnContext::Instrumented(name)));
        let depth = INSTRUMENTED_POLL_DEPTH.get();
        INSTRUMENTED_POLL_DEPTH.set(depth + 1);
        InstrumentedPollGuard {
            previous_context,
            depth,
            start: Instant::now(),
        }
    }
}

impl Drop for InstrumentedPollGuard {
    fn drop(&mut self) {
        SPAWN_CONTEXT.set(self.previous_context);
        INSTRUMENTED_POLL_DEPTH.set(self.depth);
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
        let guard = InstrumentedPollGuard::enter(this.name);
        let poll = this.inner.poll(cx);
        let elapsed = guard.start.elapsed();
        if guard.depth == 0 {
            // Outermost instrumentation point: task poll attribution must not count this
            // time a second time. Nested points are already covered by their parent.
            INSTRUMENTED_POLL_NANOS.set(INSTRUMENTED_POLL_NANOS.get() + elapsed.as_nanos() as u64);
        }
        drop(guard);
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

/// Installs task hooks on `runtime_builder` that attribute every task poll to an
/// instrumentation point, when `QW_TOKIO_TASK_POLL_ATTRIBUTION` is set.
///
/// A task spawned while `detect_slow_poll(name)` is being polled is labelled `name>`, and
/// so are the tasks it spawns in turn. This reaches tasks spawned by libraries (hyper
/// connections, h2 streams, ...) that cannot be wrapped directly. Tasks spawned outside of
/// any instrumented context are labelled `unattributed`. Polls are recorded in
/// `quickwit_runtime_task_poll_duration_seconds`, minus the time already reported by the
/// outermost instrumentation point polled within them.
///
/// It costs a lookup in a sharded map and two `Instant::now()` calls per task poll, hence
/// the opt-in.
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
    task_poll_attribution::install(runtime_builder);
}

#[cfg(tokio_unstable)]
mod task_poll_attribution {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::hash::{BuildHasher, RandomState};
    use std::sync::Mutex;

    use tokio::runtime::TaskMeta;
    use tokio::task::Id;

    use super::*;

    const NUM_SHARDS: usize = 64;

    /// Remainders shorter than this, left after subtracting the instrumented time from a
    /// task poll, are the overhead of the wrappers themselves: they would only inflate the
    /// poll count.
    const MIN_UNINSTRUMENTED_NANOS: u64 = 1_000;

    /// Label of every live task spawned from an instrumented context.
    struct TaskLabels {
        hasher: RandomState,
        shards: [Mutex<HashMap<Id, &'static Histogram>>; NUM_SHARDS],
    }

    impl TaskLabels {
        fn shard(&self, task_id: Id) -> &Mutex<HashMap<Id, &'static Histogram>> {
            &self.shards[self.hasher.hash_one(task_id) as usize % NUM_SHARDS]
        }
    }

    static TASK_LABELS: Lazy<TaskLabels> = Lazy::new(|| TaskLabels {
        hasher: RandomState::new(),
        shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
    });

    // Histograms are leaked so that the per-poll bookkeeping only copies references. There
    // is one per instrumentation point, so they are bounded like the label values.
    static UNATTRIBUTED_HISTOGRAM: Lazy<&'static Histogram> = Lazy::new(|| {
        Box::leak(Box::new(
            POLL_DURATION_HISTOGRAM.with_label_values(["unattributed"]),
        ))
    });
    static SPAWNED_FROM_HISTOGRAMS: Lazy<Mutex<HashMap<&'static str, &'static Histogram>>> =
        Lazy::new(Default::default);

    thread_local! {
        static SPAWNED_FROM_HISTOGRAMS_CACHE: RefCell<HashMap<&'static str, &'static Histogram>> =
            RefCell::new(HashMap::new());
        /// Start and label of the task poll in progress on this worker.
        static TASK_POLL: Cell<Option<(Instant, &'static Histogram)>> = const { Cell::new(None) };
    }

    /// Histogram of the tasks spawned from the instrumentation point `name`.
    fn spawned_from_histogram(name: &'static str) -> &'static Histogram {
        SPAWNED_FROM_HISTOGRAMS_CACHE.with_borrow_mut(|cache| {
            *cache.entry(name).or_insert_with(|| {
                *SPAWNED_FROM_HISTOGRAMS
                    .lock()
                    .unwrap()
                    .entry(name)
                    .or_insert_with(|| {
                        let label = format!("{name}>");
                        Box::leak(Box::new(
                            POLL_DURATION_HISTOGRAM.with_label_values([&label]),
                        ))
                    })
            })
        })
    }

    pub(super) fn install(runtime_builder: &mut tokio::runtime::Builder) {
        runtime_builder
            .on_task_spawn(on_task_spawn)
            .on_task_terminate(on_task_terminate)
            .on_before_task_poll(on_before_task_poll)
            .on_after_task_poll(on_after_task_poll);
    }

    fn on_task_spawn(task_meta: &TaskMeta<'_>) {
        let histogram = match SPAWN_CONTEXT.get() {
            None => return,
            Some(SpawnContext::Instrumented(name)) => spawned_from_histogram(name),
            Some(SpawnContext::SpawnedFrom(histogram)) => histogram,
        };
        let task_id = task_meta.id();
        TASK_LABELS
            .shard(task_id)
            .lock()
            .unwrap()
            .insert(task_id, histogram);
    }

    fn on_task_terminate(task_meta: &TaskMeta<'_>) {
        // Also called for `spawn_blocking` tasks, which never went through `on_task_spawn`.
        let task_id = task_meta.id();
        TASK_LABELS.shard(task_id).lock().unwrap().remove(&task_id);
    }

    fn on_before_task_poll(task_meta: &TaskMeta<'_>) {
        let task_id = task_meta.id();
        let label_opt: Option<&'static Histogram> = TASK_LABELS
            .shard(task_id)
            .lock()
            .unwrap()
            .get(&task_id)
            .copied();
        SPAWN_CONTEXT.set(label_opt.map(SpawnContext::SpawnedFrom));
        // Reset in case a previous poll panicked in the middle of an instrumented poll.
        INSTRUMENTED_POLL_DEPTH.set(0);
        INSTRUMENTED_POLL_NANOS.set(0);
        let histogram = label_opt.unwrap_or(*UNATTRIBUTED_HISTOGRAM);
        TASK_POLL.set(Some((Instant::now(), histogram)));
    }

    fn on_after_task_poll(_task_meta: &TaskMeta<'_>) {
        let Some((start, histogram)) = TASK_POLL.take() else {
            return;
        };
        let elapsed_nanos = start.elapsed().as_nanos() as u64;
        SPAWN_CONTEXT.set(None);
        let instrumented_nanos = INSTRUMENTED_POLL_NANOS.replace(0);
        let uninstrumented_nanos = elapsed_nanos.saturating_sub(instrumented_nanos);
        if instrumented_nanos == 0 || uninstrumented_nanos >= MIN_UNINSTRUMENTED_NANOS {
            histogram.observe(Duration::from_nanos(uninstrumented_nanos).as_secs_f64());
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

    #[cfg(tokio_unstable)]
    #[test]
    fn test_task_poll_attribution() {
        fn spin(duration: Duration) {
            let start = Instant::now();
            while start.elapsed() < duration {
                std::hint::black_box(0u64);
            }
        }
        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
        runtime_builder.worker_threads(1).enable_all();
        task_poll_attribution::install(&mut runtime_builder);
        let runtime = runtime_builder.build().unwrap();

        // The label values are unique to this test, but `unattributed` is shared: this is the
        // only test installing the hooks, and it only asserts lower bounds on it.
        let parent_histogram = POLL_DURATION_HISTOGRAM.with_label_values(["test_parent"]);
        let spawned_histogram = POLL_DURATION_HISTOGRAM.with_label_values(["test_parent>"]);
        let nested_spawned_histogram =
            POLL_DURATION_HISTOGRAM.with_label_values(["test_parent:nested>"]);
        let unattributed_histogram = POLL_DURATION_HISTOGRAM.with_label_values(["unattributed"]);
        let unattributed_sum_before = unattributed_histogram.get_sample_sum();

        runtime.block_on(async {
            // A task spawned from an instrumented future, which spawns a task in turn: both
            // are attributed to `test_parent>`.
            let parent_task = tokio::spawn(
                async {
                    tokio::spawn(async {
                        spin(Duration::from_millis(20));
                        tokio::spawn(async { spin(Duration::from_millis(20)) })
                            .await
                            .unwrap();
                    })
                    .await
                    .unwrap();
                    async { tokio::spawn(async {}).await.unwrap() }
                        .detect_slow_poll("test_parent:nested")
                        .await;
                }
                .detect_slow_poll("test_parent"),
            );
            parent_task.await.unwrap();

            // An uninstrumented task, whose time lands in `unattributed`.
            tokio::spawn(async { spin(Duration::from_millis(20)) })
                .await
                .unwrap();
        });

        // Both descendants spun 20ms each.
        assert!(spawned_histogram.get_sample_sum() >= 0.035);
        // The parent itself did not spin: its task poll is fully covered by `test_parent`, and
        // must not be reported again in `unattributed`.
        assert!(parent_histogram.get_sample_sum() < 0.010);
        // A task spawned inside a nested instrumentation point is labelled after it.
        assert_eq!(nested_spawned_histogram.get_sample_count(), 1);
        let unattributed_sum = unattributed_histogram.get_sample_sum() - unattributed_sum_before;
        assert!(unattributed_sum >= 0.015, "{unattributed_sum}");
        assert!(unattributed_sum < 0.035, "{unattributed_sum}");
    }
}
