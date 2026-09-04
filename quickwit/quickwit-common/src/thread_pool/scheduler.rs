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

//! Priority scheduler sitting in front of a [`rayon::ThreadPool`].
//!
//! Rayon has no notion of task priority: it dispatches in whatever order tasks
//! land in its own local deques / injector queue. This module keeps its own
//! queue and uses rayon only to own OS threads that continuously drain it (the
//! "pump loop" pattern).
//!
//! Three tiers of priority exist:
//! - High priority: always runs first and processed in strict FIFO order. Meant for short and rarer
//!   tasks such as merging/finalizing a query's results.
//! - Per-query: tries to be fair among queries, with a bias towards queries that are closer to
//!   completion.
//! - External: tasks submitted to the rayon threadpool without going through the scheduler are
//!   executed before all other tasks, but with some latency because they need the pump loops to
//!   yield to be picked up.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use tracing::error;

use crate::metrics::{IntCounter, IntGauge, new_counter, new_gauge};
use crate::rate_limited_error;

/// Identifies a query (leaf search) whose split-processing tasks should be
/// scheduled and fair-shared together. Must be unique among currently active
/// queries: reusing an id while a previous query with that id is still being
/// cleaned up would corrupt its accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct QueryId(u64);

impl QueryId {
    /// Allocates a fresh, never-reused `QueryId`.
    pub fn next() -> QueryId {
        static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
        QueryId(NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// One per split registered via [`Scheduler::register_query`]. Dropping it
/// resolves that split -- via normal completion or via being dropped along
/// with a cancelled future -- so the query's entry can never leak regardless
/// of how its caller ends.
#[must_use = "dropping this immediately resolves the split"]
pub struct SchedulerSplitGuard {
    scheduler: Arc<Scheduler>,
    query_id: QueryId,
}

impl SchedulerSplitGuard {
    pub fn query_id(&self) -> QueryId {
        self.query_id
    }
}

impl Drop for SchedulerSplitGuard {
    fn drop(&mut self) {
        self.scheduler.split_resolved(self.query_id);
    }
}

/// Ensures the running count of the query is decremented after the job
/// completes even if the job itself panics.
struct RunningCountGuard {
    scheduler: Arc<Scheduler>,
    query_id: QueryId,
}

impl Drop for RunningCountGuard {
    fn drop(&mut self) {
        let mut state = self.scheduler.lock_state();
        if let Some(query) = state.queries.get_mut(&self.query_id) {
            query.running_count = query.running_count.saturating_sub(1);
        }
    }
}

type Job = Box<dyn FnOnce() + Send>;

/// How long a pump loop keeps grabbing tasks before handing its worker back to
/// rayon's own scheduler (see [`pump_loop`]).
///
/// Setting this too low adds un-necessary context work, setting this too high
/// adds latency to tasks submitted directly to the rayon threadpool.
const PUMP_LOOP_YIELD_INTERVAL: Duration = Duration::from_millis(100);

/// Per-query scheduling state.
struct QueryState {
    /// Tasks submitted for this query that have not yet been dispatched.
    ready: VecDeque<Job>,
    /// Tasks currently executing on a worker.
    running_count: usize,
    /// Number of this query's splits still waiting on a `SearchPermit`, not
    /// yet admitted into warmup/CPU processing. Kept up to date by the
    /// caller via [`Scheduler::set_waiting_for_permit`].
    waiting_for_permit: usize,
    /// Number of this query's splits not yet resolved through *any* terminal
    /// path (processed, pruned, cache hit, ...). Decremented by
    /// [`Scheduler::split_resolved`], which also cleans up the query once it
    /// reaches zero.
    remaining: usize,
    /// Used as the final tie-break: older queries win, for fairness/liveness
    /// among otherwise-indistinguishable queries.
    created_at: Instant,
}

impl QueryState {
    fn priority_key(&self) -> (usize, usize, Instant) {
        (self.waiting_for_permit, self.remaining, self.created_at)
    }
}

struct SchedulerState {
    /// Always-first, uncapped tasks (e.g. finalize/root_merge).
    high_priority_queue: VecDeque<Job>,
    queries: HashMap<QueryId, QueryState>,
    /// Number of pump loops currently alive, bounded by `num_threads`.
    active_pump_workers: usize,
}

impl SchedulerState {
    /// The maximum number of concurrently running tasks any single query may
    /// have right now, given how many queries are currently competing for the
    /// pool.
    fn current_cap(&self, num_threads: usize) -> usize {
        let competing_queries = self
            .queries
            .values()
            .filter(|query| !query.ready.is_empty())
            .count();
        num_threads.div_ceil(competing_queries.max(1))
    }

    /// Pops the single highest-priority ready and eligible task, if any,
    /// updating `running_count` for its owning query. Called with the lock
    /// already held, by a pump loop looking for its next unit of work.
    ///
    /// Deliberately implemented here rather than on [`Scheduler`]: taking
    /// only `&mut self` (no access to `Scheduler::state`, the `Mutex` this is
    /// always called with already locked) makes it structurally impossible
    /// for this to ever try to re-lock it and deadlock.
    fn pick_next(&mut self, num_threads: usize) -> Option<Job> {
        if let Some(job) = self.high_priority_queue.pop_front() {
            return Some(job);
        }
        let cap = self.current_cap(num_threads);
        let best_query_id = self
            .queries
            .iter()
            .filter(|(_, query)| query.running_count < cap && !query.ready.is_empty())
            .min_by_key(|(_, query)| query.priority_key())
            .map(|(query_id, _)| *query_id)?;
        let query = self
            .queries
            .get_mut(&best_query_id)
            .expect("query looked up right above must still be present");
        let job = query
            .ready
            .pop_front()
            .expect("query was only selected because its ready queue is non-empty");
        query.running_count += 1;
        Some(job)
    }
}

/// A priority scheduler backed by a [`rayon::ThreadPool`]. See the module
/// documentation for the overall design.
pub struct Scheduler {
    rayon_pool: Arc<rayon::ThreadPool>,
    num_threads: usize,
    state: Mutex<SchedulerState>,
}

impl Scheduler {
    pub fn new(rayon_pool: Arc<rayon::ThreadPool>) -> Arc<Scheduler> {
        let num_threads = rayon_pool.current_num_threads();
        Arc::new(Scheduler {
            rayon_pool,
            num_threads,
            state: Mutex::new(SchedulerState {
                high_priority_queue: VecDeque::new(),
                queries: HashMap::new(),
                active_pump_workers: 0,
            }),
        })
    }

    /// Locks `state`, adding how long the calling thread had to wait to
    /// acquire it to [`SchedulerMetrics::lock_wait_time_nanos_total`]. The
    /// lock only ever guards brief in-memory bookkeeping, so a fast-growing
    /// total directly indicates contention.
    fn lock_state(&self) -> MutexGuard<'_, SchedulerState> {
        let wait_start = Instant::now();
        let guard = self.state.lock().unwrap();
        SCHEDULER_METRICS
            .lock_wait_time_nanos_total
            .inc_by(wait_start.elapsed().as_nanos() as u64);
        guard
    }

    /// Registers a new query with its total split count. Must be called exactly
    /// once per query, before any [`Self::enqueue_fair`] or
    /// [`Self::set_waiting_for_permit`] call for that `query_id`.
    ///
    /// Returns one guard per split to track the number of remaining splits for
    /// the query.
    pub fn register_query(
        self: &Arc<Self>,
        query_id: QueryId,
        total_splits: usize,
    ) -> Vec<SchedulerSplitGuard> {
        if total_splits == 0 {
            return Vec::new();
        }
        let mut state = self.lock_state();
        state.queries.insert(
            query_id,
            QueryState {
                ready: VecDeque::new(),
                running_count: 0,
                waiting_for_permit: 0,
                remaining: total_splits,
                created_at: Instant::now(),
            },
        );
        SCHEDULER_METRICS.queries.set(state.queries.len() as i64);
        drop(state);
        (0..total_splits)
            .map(|_| SchedulerSplitGuard {
                scheduler: self.clone(),
                query_id,
            })
            .collect()
    }

    /// Updates how many of `query_id`'s splits are still waiting on a
    /// `SearchPermit`. A query with none left (the common case once
    /// admission is done) is preferred over one still mostly permit-gated.
    pub fn set_waiting_for_permit(&self, query_id: QueryId, waiting_for_permit: usize) {
        let mut state = self.lock_state();
        if let Some(query) = state.queries.get_mut(&query_id) {
            query.waiting_for_permit = waiting_for_permit;
        }
    }

    /// Should be called exactly once per split of `query_id` when we know for
    /// sure that the split won't be submitted again to the fair scheduler.
    fn split_resolved(&self, query_id: QueryId) {
        let mut state = self.lock_state();
        let Some(query) = state.queries.get_mut(&query_id) else {
            return;
        };
        query.remaining = query.remaining.saturating_sub(1);
        if query.remaining == 0 {
            state.queries.remove(&query_id);
            SCHEDULER_METRICS.queries.set(state.queries.len() as i64);
        }
    }

    /// Schedules a high priority task: always dispatched before any per-query
    /// task, and processed FIFO. Long tasks (>100ms) are not recommended.
    pub fn enqueue_fifo<F>(self: &Arc<Self>, job: F)
    where F: FnOnce() + Send + 'static {
        let mut state = self.lock_state();
        state.high_priority_queue.push_back(Box::new(job));
        if state.active_pump_workers < self.num_threads {
            state.active_pump_workers += 1;
            let scheduler = self.clone();
            self.rayon_pool.spawn(move || pump_loop(&scheduler));
        }
    }

    /// Schedules a task belonging to `query_id`. The query is expected to
    /// already have been [`Self::register_query`]-ed.
    pub fn enqueue_fair<F>(self: &Arc<Self>, query_id: QueryId, job: F)
    where F: FnOnce() + Send + 'static {
        let scheduler = self.clone();
        let wrapped: Job = Box::new(move || {
            let _running_guard = RunningCountGuard {
                scheduler,
                query_id,
            };
            job();
        });
        let mut state = self.lock_state();
        match state.queries.get_mut(&query_id) {
            Some(query) => query.ready.push_back(wrapped),
            None => {
                debug_assert!(
                    false,
                    "query must be registered before tasks are enqueued for it"
                );
                rate_limited_error!(
                    limit_per_min = 1,
                    ?query_id,
                    "query not registered on the scheduler, fall back to FIFO"
                );
                state.high_priority_queue.push_back(wrapped);
            }
        }
        if state.active_pump_workers < self.num_threads {
            state.active_pump_workers += 1;
            let scheduler = self.clone();
            self.rayon_pool.spawn(move || pump_loop(&scheduler));
        }
    }
}

/// Runs on a rayon worker thread: repeatedly picks and runs the current best
/// task until none is eligible, then gives up its slot.
///
/// The exit check and the `active_pump_workers` decrement happen in the same
/// critical section as `pick_next`'s "nothing to do" verdict, so a concurrent
/// `enqueue_fifo`/`enqueue_fair` call always sees an up-to-date count and
/// spawns a replacement if this one is exiting right as new work arrives.
///
/// Unfortunately, the rayon pool is also used without the scheduler (as a
/// Tantivy Executor), so the pump loop needs to periodically yield to let rayon
/// schedule those tasks.
fn pump_loop(scheduler: &Arc<Scheduler>) {
    let mut yield_deadline = Instant::now() + PUMP_LOOP_YIELD_INTERVAL;
    loop {
        let job = {
            let mut state = scheduler.lock_state();
            match state.pick_next(scheduler.num_threads) {
                Some(job) => job,
                None => {
                    state.active_pump_workers -= 1;
                    return;
                }
            }
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
            error!("task running in the thread pool scheduler panicked");
        }
        if Instant::now() >= yield_deadline {
            // Drain all currently pending externally-submitted work
            while rayon::yield_now() == Some(rayon::Yield::Executed) {}
            yield_deadline = Instant::now() + PUMP_LOOP_YIELD_INTERVAL;
        }
    }
}

struct SchedulerMetrics {
    /// Number of queries currently registered with the scheduler.
    queries: IntGauge,
    /// Cumulative time (in nanoseconds) callers have spent waiting to
    /// acquire `Scheduler::state`. The lock only ever guards brief in-memory
    /// bookkeeping, so a fast-growing total directly indicates contention.
    lock_wait_time_nanos_total: IntCounter,
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
            lock_wait_time_nanos_total: new_counter(
                "scheduler_lock_wait_time_nanos_total",
                "cumulative time, in nanoseconds, spent waiting to acquire the CPU scheduler's \
                 internal lock",
                "thread_pool",
                &[],
            ),
        }
    }
}

static SCHEDULER_METRICS: Lazy<SchedulerMetrics> = Lazy::new(SchedulerMetrics::default);

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use super::*;

    fn test_scheduler(num_threads: usize) -> Arc<Scheduler> {
        let rayon_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .unwrap(),
        );
        Scheduler::new(rayon_pool)
    }

    // Polls until `condition` is true or the timeout elapses, to avoid flaky
    // sleeps while still bounding worst-case test time.
    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "condition never became true");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn test_level0_runs_before_query_tasks() {
        let scheduler = test_scheduler(1);
        let order: Arc<StdMutex<Vec<&'static str>>> = Arc::new(StdMutex::new(Vec::new()));

        let _guards = scheduler.register_query(QueryId(1), 1);

        // Step 1: Block the single worker so both tasks from step 2 stay in the
        // queue.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        scheduler.enqueue_fair(QueryId(1), move || {
            release_rx.recv().unwrap();
        });

        // Step 2: Add two tasks that remain queued by the scheduler
        let order_clone = order.clone();
        scheduler.enqueue_fair(QueryId(1), move || {
            order_clone.lock().unwrap().push("query")
        });
        let order_clone = order.clone();
        scheduler.enqueue_fifo(move || order_clone.lock().unwrap().push("level0"));

        // Step 3: Release the blocked worker to validate that the priority
        // queue is picked up first
        release_tx.send(()).unwrap();
        wait_until(|| order.lock().unwrap().len() == 2);
        assert_eq!(*order.lock().unwrap(), vec!["level0", "query"]);
    }

    #[test]
    fn test_on_task_complete_runs_even_if_job_panics() {
        let scheduler = test_scheduler(1);
        let _guards = scheduler.register_query(QueryId(1), 1);

        scheduler.enqueue_fair(QueryId(1), || panic!("boom"));
        wait_until(|| match scheduler.lock_state().queries.get(&QueryId(1)) {
            Some(query) => query.running_count == 0,
            None => false,
        });
    }

    #[test]
    fn test_smaller_remaining_runs_first() {
        let scheduler = test_scheduler(1);
        let order: Arc<StdMutex<Vec<QueryId>>> = Arc::new(StdMutex::new(Vec::new()));

        let _guards1 = scheduler.register_query(QueryId(1), 100);
        let _guards2 = scheduler.register_query(QueryId(2), 2);

        // Step 1: Block the single worker so both tasks from step 2 stay in the
        // queue.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        scheduler.enqueue_fair(QueryId(1), move || {
            release_rx.recv().unwrap();
        });

        // Step 2: Add two tasks that remain queued by the scheduler
        let order_clone = order.clone();
        scheduler.enqueue_fair(QueryId(1), move || {
            order_clone.lock().unwrap().push(QueryId(1))
        });
        let order_clone = order.clone();
        scheduler.enqueue_fair(QueryId(2), move || {
            order_clone.lock().unwrap().push(QueryId(2))
        });

        // Step 3: Release the blocked worker to validate that query 2 with
        // fewer remaining splits is picked up first.
        release_tx.send(()).unwrap();
        wait_until(|| order.lock().unwrap().len() == 2);
        assert_eq!(*order.lock().unwrap(), vec![QueryId(2), QueryId(1)]);
    }

    #[test]
    fn test_cap_ignores_queries_with_no_ready_or_running_work() {
        let scheduler = test_scheduler(3);
        // Queries 1 and 2 simulate splits still waiting on a `SearchPermit`:
        // registered, but with nothing enqueued on the CPU scheduler yet.
        let _guards1 = scheduler.register_query(QueryId(1), 100);
        let _guards2 = scheduler.register_query(QueryId(2), 100);
        let _guards3 = scheduler.register_query(QueryId(3), 100);

        let concurrent_3 = Arc::new(AtomicUsize::new(0));
        let max_concurrent_3 = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Arc::new(StdMutex::new(release_rx));
        for _ in 0..10 {
            let concurrent_3 = concurrent_3.clone();
            let max_concurrent_3 = max_concurrent_3.clone();
            let release_rx = release_rx.clone();
            scheduler.enqueue_fair(QueryId(3), move || {
                let current = concurrent_3.fetch_add(1, Ordering::SeqCst) + 1;
                max_concurrent_3.fetch_max(current, Ordering::SeqCst);
                release_rx.lock().unwrap().recv().unwrap();
                concurrent_3.fetch_sub(1, Ordering::SeqCst);
            });
        }

        wait_until(|| concurrent_3.load(Ordering::SeqCst) == 3);
        // 3 queries are registered, but only query 3 has any ready/running
        // work, so it should get the whole pool instead of being capped at
        // usable/3 == 1 while the other two threads sit idle.
        assert_eq!(max_concurrent_3.load(Ordering::SeqCst), 3);

        for _ in 0..10 {
            release_tx.send(()).unwrap();
        }
    }

    #[test]
    fn test_per_query_cap_shares_the_pool() {
        let scheduler = test_scheduler(4);
        let _guards1 = scheduler.register_query(QueryId(1), 100);
        let _guards2 = scheduler.register_query(QueryId(2), 100);

        let concurrent_1 = Arc::new(AtomicUsize::new(0));
        let max_concurrent_1 = Arc::new(AtomicUsize::new(0));
        let (release_tx_1, release_rx_1) = std::sync::mpsc::channel::<()>();
        let release_rx_1 = Arc::new(StdMutex::new(release_rx_1));
        let (release_tx_2, release_rx_2) = std::sync::mpsc::channel::<()>();
        let release_rx_2 = Arc::new(StdMutex::new(release_rx_2));

        // Interleave both queries' backlogs (each with more tasks than the
        // pool could ever run at once for it alone) so both count as
        // competing from the start, splitting cap = 4/2 = 2 between them.
        // Giving one query its whole backlog first would let it alone grab
        // cap = 4/1 = 4 -- the entire pool -- before the other ever gets a
        // chance to compete.
        for _ in 0..10 {
            let concurrent_1 = concurrent_1.clone();
            let max_concurrent_1 = max_concurrent_1.clone();
            let release_rx_1 = release_rx_1.clone();
            scheduler.enqueue_fair(QueryId(1), move || {
                let current = concurrent_1.fetch_add(1, Ordering::SeqCst) + 1;
                max_concurrent_1.fetch_max(current, Ordering::SeqCst);
                release_rx_1.lock().unwrap().recv().unwrap();
                concurrent_1.fetch_sub(1, Ordering::SeqCst);
            });
            let release_rx_2 = release_rx_2.clone();
            scheduler.enqueue_fair(QueryId(2), move || {
                release_rx_2.lock().unwrap().recv().unwrap();
            });
        }

        wait_until(|| concurrent_1.load(Ordering::SeqCst) >= 2);
        // both queries have a backlog, so cap = 4/2 = 2.
        assert!(max_concurrent_1.load(Ordering::SeqCst) <= 2);

        for _ in 0..10 {
            release_tx_1.send(()).unwrap();
        }
        for _ in 0..10 {
            release_tx_2.send(()).unwrap();
        }
    }

    #[test]
    fn test_pump_loop_yields_periodically_for_directly_injected_rayon_work() {
        // A continuous flow of fair-share work keeps pump loops finding more
        // ready tasks (there's always another one queued), so without a
        // periodic yield they would never return control to rayon's own
        // scheduler -- starving anything submitted straight to the same
        // rayon pool outside the scheduler (e.g. Tantivy's own internal
        // parallelism via `ThreadPool::get_executor`), which only gets
        // picked up by a worker that actually returns to rayon's scheduling
        // loop.
        let scheduler = test_scheduler(2);
        let _guards = scheduler.register_query(QueryId(1), 100_000);

        // Keep both workers continuously busy with a long stream of short
        // tasks, well over one yield interval in total.
        for _ in 0..2_000 {
            scheduler.enqueue_fair(QueryId(1), || {
                std::thread::sleep(Duration::from_millis(10));
            });
        }
        wait_until(|| scheduler.lock_state().active_pump_workers >= 1);

        let (tx, rx) = std::sync::mpsc::channel();
        scheduler.rayon_pool.spawn(move || tx.send(()).unwrap());
        rx.recv_timeout(Duration::from_secs(1))
            .expect("directly-injected rayon work starved: pump loops never yielded");
    }

    #[test]
    fn test_query_state_cleaned_up_once_remaining_reaches_zero() {
        let scheduler = test_scheduler(2);
        let mut guards = scheduler.register_query(QueryId(1), 2);
        assert!(scheduler.lock_state().queries.contains_key(&QueryId(1)));

        drop(guards.pop().unwrap());
        assert!(scheduler.lock_state().queries.contains_key(&QueryId(1)));

        drop(guards.pop().unwrap());
        assert!(!scheduler.lock_state().queries.contains_key(&QueryId(1)));
    }

    #[test]
    fn test_pump_loops_drain_and_exit() {
        let scheduler = test_scheduler(2);
        let _guards = scheduler.register_query(QueryId(1), 3);
        let ran = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let ran = ran.clone();
            scheduler.enqueue_fair(QueryId(1), move || {
                ran.fetch_add(1, Ordering::SeqCst);
            });
        }
        wait_until(|| ran.load(Ordering::SeqCst) == 3);
        wait_until(|| scheduler.lock_state().active_pump_workers == 0);
    }

    #[test]
    fn test_more_waiting_for_permit_runs_last() {
        let scheduler = test_scheduler(1);
        let order: Arc<StdMutex<Vec<QueryId>>> = Arc::new(StdMutex::new(Vec::new()));

        let _guards1 = scheduler.register_query(QueryId(1), 20);
        let _guards2 = scheduler.register_query(QueryId(2), 10);
        scheduler.set_waiting_for_permit(QueryId(1), 5);

        // Step 1: Block the single worker so both tasks from step 2 stay in
        // the queue.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        scheduler.enqueue_fair(QueryId(1), move || {
            release_rx.recv().unwrap();
        });

        // Step 2: Add one task per query that remain in the queue.
        let order_clone = order.clone();
        scheduler.enqueue_fair(QueryId(1), move || {
            order_clone.lock().unwrap().push(QueryId(1))
        });
        let order_clone = order.clone();
        scheduler.enqueue_fair(QueryId(2), move || {
            order_clone.lock().unwrap().push(QueryId(2))
        });

        // Step 3: Release the blocked worker to validate that query 2, which
        // has no splits waiting on a permit, is picked up before query 1.
        release_tx.send(()).unwrap();
        wait_until(|| order.lock().unwrap().len() == 2);
        assert_eq!(*order.lock().unwrap(), vec![QueryId(2), QueryId(1)]);
    }

    #[test]
    fn test_register_query_with_zero_splits_returns_no_guards() {
        let scheduler = test_scheduler(1);
        let guards = scheduler.register_query(QueryId(1), 0);
        assert!(guards.is_empty());
        assert!(!scheduler.lock_state().queries.contains_key(&QueryId(1)));
    }
}
