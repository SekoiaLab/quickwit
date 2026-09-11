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
//! land in its own local deques / injector queue. This module keeps a per-query
//! priority queue in a small async actor task, and only ever submits work to
//! rayon one task at a time, directly via `rayon_pool.spawn`, whenever a slot
//! is free. Since the actor itself never runs on a rayon worker, every job it
//! submits lands in rayon's shared injector queue and competes fairly with
//! tasks submitted directly to the same pool from outside the scheduler (e.g.
//! Tantivy's own internal parallelism).
//!
//! Three tiers of priority exist:
//! - High priority: spawned on rayon immediately, without going through the actor or waiting for a
//!   slot. Meant for the short and rare tasks that merge/finalize a query's results: they are far
//!   too short to be worth queueing behind per-query work.
//! - Per-query: tries to be fair among queries, with a bias towards queries that are closer to
//!   completion. Capped at `num_threads + OVERSUBSCRIPTION` tasks in flight.
//! - External: tasks submitted to the rayon threadpool without going through the scheduler are
//!   dispatched by rayon on an equal footing with the scheduler's own tasks.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use tokio::sync::mpsc;
use tracing::{error, info};

use crate::rate_limited_error;
use crate::thread_pool::Panicked;
use crate::thread_pool::metrics::SCHEDULER_METRICS;

/// Number of tasks dispatched to rayon on top of the pool's thread count.
///
/// A worker that just finished a task would otherwise have to wait for a full
/// actor round-trip (completion message -> actor wake-up -> spawn) before
/// getting a new one. Keeping a few extra jobs staged in rayon's injector
/// hides that latency, at the cost of dispatching a few tasks slightly before
/// the priority queue has its final say on them.
static OVERSUBSCRIPTION: LazyLock<usize> = LazyLock::new(|| {
    crate::get_from_env("QW_THREAD_POOL_SCHEDULER_OVERSUBSCRIPTION", 0usize, false)
});

/// Identifies a leaf search query for tasks that need to be fair-shared accross
/// queries. Must be unique among currently active queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct QueryId(u64);

impl QueryId {
    /// Allocates a fresh, never-reused `QueryId`.
    fn next() -> QueryId {
        static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
        QueryId(NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// Tracks the number of remaining splits to be processed for a given query.
#[must_use]
pub struct SchedulerSplitGuard {
    scheduler: Arc<Scheduler>,
    query_id: QueryId,
    waiting_for_permit: bool,
    pool_name: &'static str,
}

impl SchedulerSplitGuard {
    /// Signals the scheduler that this split has obtained a permit.
    pub fn mark_permit_obtained(&mut self) {
        if !self.waiting_for_permit {
            return;
        }
        self.waiting_for_permit = false;
        let _ = self
            .scheduler
            .tx
            .send(ActorMessage::PermitObtained(self.query_id));
    }

    /// Runs a CPU-intensive task, fairly scheduled against other queries.
    pub fn run_cpu_intensive_fair<F, R>(
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
            self.pool_name,
            caller,
            cost_class,
            cpu_intensive_fn,
            move |job| self.scheduler.enqueue_fair(self.query_id, job),
        )
    }
}

impl Drop for SchedulerSplitGuard {
    fn drop(&mut self) {
        let _ = self.scheduler.tx.send(ActorMessage::SplitResolved {
            query_id: self.query_id,
            waiting_for_permit: self.waiting_for_permit,
        });
    }
}

type Job = Box<dyn FnOnce() + Send>;

/// A job waiting in the actor's per-query queues, along with what's needed to
/// attribute the time it spends there.
struct QueuedJob {
    job: Job,
    enqueued_at: Instant,
}

/// Hands a job over to rayon. `capacity_guard` must already account for it in
/// the shared running count.
fn spawn_on_rayon(rayon_pool: &rayon::ThreadPool, job: Job, capacity_guard: CapacityFreedGuard) {
    let spawned_at = Instant::now();
    rayon_pool.spawn(move || {
        SCHEDULER_METRICS
            .rayon_pickup_latency_secs
            .observe(spawned_at.elapsed().as_secs_f64());
        let _capacity_guard = capacity_guard;
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
            error!("task running in the thread pool scheduler panicked");
        }
    });
}

/// Per-query scheduling state, owned exclusively by the [`SchedulerActor`].
struct QueryState {
    /// Tasks submitted for this query that have not yet been dispatched.
    ready: VecDeque<QueuedJob>,
    /// Tasks currently executing on a rayon worker.
    running_count: usize,
    /// Number of this query's splits still waiting on a `SearchPermit`, not yet
    /// admitted into warmup/CPU processing. Kept up to date via
    /// [`SchedulerSplitGuard::mark_permit_obtained`].
    waiting_for_permit: usize,
    /// Number of this query's splits not yet resolved through *any* terminal
    /// path (processed, pruned, cache hit, ...). Used both in the priority
    /// calculation and to determine when a query is fully resolved.
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

/// Messages accepted by the [`SchedulerActor`]. `Scheduler`'s public methods
/// are thin, non-blocking wrappers that just send one of these.
enum ActorMessage {
    EnqueueFair(QueryId, QueuedJob),
    RegisterQuery {
        query_id: QueryId,
        total_splits: usize,
    },
    /// A permit has been obtained, which might change the priority of the
    /// query's tasks.
    PermitObtained(QueryId),
    /// A split's processing has completed.
    SplitResolved {
        query_id: QueryId,
        waiting_for_permit: bool,
    },
    /// A per-query task finished (or panicked).
    QueryTaskFinished(QueryId),
    /// Any task finished (or panicked), so the actor should try to dispatch
    /// more. The count itself is already updated by [`CapacityFreedGuard`].
    CapacityFreed,
    #[cfg(test)]
    Introspect(tokio::sync::oneshot::Sender<DebugState>),
}

/// A priority scheduler backed by a [`rayon::ThreadPool`]. See the module
/// documentation for the overall design.
pub struct Scheduler {
    rayon_pool: Arc<rayon::ThreadPool>,
    /// Shared with the actor: high priority tasks are spawned straight from the
    /// handle, so both sides need to account for them.
    running_count: Arc<AtomicUsize>,
    tx: mpsc::UnboundedSender<ActorMessage>,
    pool_name: &'static str,
}

impl Scheduler {
    /// Spawns a scheduler actor onto the current Tokio runtime. Must therefore
    /// be called from within one.
    pub fn new(rayon_pool: Arc<rayon::ThreadPool>, pool_name: &'static str) -> Arc<Scheduler> {
        Self::new_inner(rayon_pool, pool_name, *OVERSUBSCRIPTION).0
    }

    /// Like [`Self::new`], but more configurable, allowing an explicit
    /// oversubscription to be set and the join handle to be awaited.
    #[cfg(test)]
    fn new_for_test(
        rayon_pool: Arc<rayon::ThreadPool>,
        pool_name: &'static str,
        oversubscription: usize,
    ) -> (Arc<Scheduler>, tokio::task::JoinHandle<()>) {
        Self::new_inner(rayon_pool, pool_name, oversubscription)
    }

    fn new_inner(
        rayon_pool: Arc<rayon::ThreadPool>,
        pool_name: &'static str,
        oversubscription: usize,
    ) -> (Arc<Scheduler>, tokio::task::JoinHandle<()>) {
        let num_threads = rayon_pool.current_num_threads();
        let (tx, rx) = mpsc::unbounded_channel();
        let running_count = Arc::new(AtomicUsize::new(0));
        let actor = SchedulerActor {
            rayon_pool: rayon_pool.clone(),
            dispatch_cap: num_threads + oversubscription,
            running_count: running_count.clone(),
            tx: tx.downgrade(),
            queries: HashMap::new(),
            pool_name,
        };
        let handle = tokio::spawn(actor.run(rx));
        let scheduler = Arc::new(Scheduler {
            rayon_pool,
            running_count,
            tx,
            pool_name,
        });
        (scheduler, handle)
    }

    /// Registers a new query on the scheduler's state. Must be called exactly
    /// once per query.
    ///
    /// Tasks belonging to the query and that need to be scheduled fairly can be
    /// enqueued using the resulting [`SchedulerSplitGuard`]s. This guaranties
    /// that:
    /// - The state of the query is always initialized before it is mutated (RegisterQuery message
    ///   rec).
    pub fn register_query(self: &Arc<Self>, total_splits: usize) -> Vec<SchedulerSplitGuard> {
        let query_id = QueryId::next();
        if total_splits == 0 {
            return Vec::new();
        }
        let _ = self.tx.send(ActorMessage::RegisterQuery {
            query_id,
            total_splits,
        });
        (0..total_splits)
            .map(|_| SchedulerSplitGuard {
                scheduler: self.clone(),
                query_id,
                waiting_for_permit: true,
                pool_name: self.pool_name,
            })
            .collect()
    }

    /// Schedules a high priority task: handed to rayon straight away, without
    /// going through the actor and without waiting for a slot.
    ///
    /// These tasks are short (tens of microseconds) and rare enough that their
    /// contribution to the pool's occupancy is negligible, whereas making them
    /// queue behind per-query tasks -- which run for tens of milliseconds --
    /// would cost them orders of magnitude more than they take to run. Long
    /// tasks (>100ms) are therefore not acceptable here.
    pub fn enqueue_fifo(self: &Arc<Self>, job: Job) {
        self.running_count.fetch_add(1, Ordering::Relaxed);
        let capacity_guard = CapacityFreedGuard {
            tx: self.tx.downgrade(),
            running_count: self.running_count.clone(),
        };
        spawn_on_rayon(&self.rayon_pool, job, capacity_guard);
    }

    /// Schedules a task belonging to `query_id`. The query is expected to
    /// already have been [`Self::register_query`]-ed.
    fn enqueue_fair(self: &Arc<Self>, query_id: QueryId, job: Job) {
        let tx = self.tx.downgrade();
        let wrapped: Job = Box::new(move || {
            let _running_guard = RunningCountGuard { tx, query_id };
            job();
        });
        let queued_job = QueuedJob {
            job: wrapped,
            enqueued_at: Instant::now(),
        };
        let _ = self
            .tx
            .send(ActorMessage::EnqueueFair(query_id, queued_job));
    }

    #[cfg(test)]
    async fn debug_state(&self) -> DebugState {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.tx.send(ActorMessage::Introspect(tx));
        rx.await.expect("scheduler actor must still be running")
    }
}

/// Decrements `query_id`'s `running_count` once its task completes (or
/// panics), even if the job itself panics -- embedded into the job closure
/// by [`Scheduler::enqueue_fair`], at enqueue time.
struct RunningCountGuard {
    tx: mpsc::WeakUnboundedSender<ActorMessage>,
    query_id: QueryId,
}

impl Drop for RunningCountGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.upgrade() {
            let _ = tx.send(ActorMessage::QueryTaskFinished(self.query_id));
        }
    }
}

/// Releases the rayon slot accounted for when the job was handed to rayon,
/// once it completes (or panics), and nudges the actor to dispatch more.
///
/// Holds a weak sender, for the same reason as [`RunningCountGuard`].
struct CapacityFreedGuard {
    tx: mpsc::WeakUnboundedSender<ActorMessage>,
    running_count: Arc<AtomicUsize>,
}

impl Drop for CapacityFreedGuard {
    fn drop(&mut self) {
        self.running_count.fetch_sub(1, Ordering::Relaxed);
        if let Some(tx) = self.tx.upgrade() {
            let _ = tx.send(ActorMessage::CapacityFreed);
        }
    }
}

#[cfg(test)]
struct DebugState {
    running_count: usize,
    query_running_counts: HashMap<QueryId, usize>,
    registered_queries: std::collections::HashSet<QueryId>,
}

/// Owns all scheduling state and priority logic; the only thing that ever
/// calls `rayon_pool.spawn`. Runs as a single, long-lived Tokio task (see
/// [`Self::run`]), processing one [`ActorMessage`] at a time -- so its state
/// needs no lock.
struct SchedulerActor {
    rayon_pool: Arc<rayon::ThreadPool>,
    /// Number of rayon threads plus the configured [`OVERSUBSCRIPTION`].
    dispatch_cap: usize,
    /// Number of tasks currently handed to rayon, including the high priority
    /// ones spawned directly by [`Scheduler::enqueue_fifo`]. Only bounded by
    /// `dispatch_cap` for the tasks the actor itself dispatches.
    running_count: Arc<AtomicUsize>,
    /// Cloned into every dispatched job's [`CapacityFreedGuard`] (and every
    /// enqueued fair job's [`RunningCountGuard`]) so they can report back. Weak
    /// handle to let the actor stop once the scheduler is dropped.
    tx: mpsc::WeakUnboundedSender<ActorMessage>,
    /// Per-query "fairly" scheduled tasks.
    queries: HashMap<QueryId, QueryState>,
    pool_name: &'static str,
}

impl SchedulerActor {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<ActorMessage>) {
        while let Some(message) = rx.recv().await {
            self.handle(message);
            self.fill_capacity();
        }
        info!(pool_name = self.pool_name, "scheduler actor stopped");
    }

    fn handle(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::EnqueueFair(query_id, job) => {
                SCHEDULER_METRICS
                    .actor_lag_secs
                    .observe(job.enqueued_at.elapsed().as_secs_f64());
                match self.queries.get_mut(&query_id) {
                    Some(query) => query.ready.push_back(job),
                    None => {
                        debug_assert!(
                            false,
                            "query must be registered before tasks are enqueued for it"
                        );
                        rate_limited_error!(
                            limit_per_min = 1,
                            ?query_id,
                            "query not registered on the scheduler, running it right away"
                        );
                        self.dispatch(job);
                    }
                }
            }
            ActorMessage::RegisterQuery {
                query_id,
                total_splits,
            } => {
                self.queries.insert(
                    query_id,
                    QueryState {
                        ready: VecDeque::new(),
                        running_count: 0,
                        waiting_for_permit: total_splits,
                        remaining: total_splits,
                        created_at: Instant::now(),
                    },
                );
                SCHEDULER_METRICS.queries.set(self.queries.len() as i64);
            }
            ActorMessage::PermitObtained(query_id) => {
                if let Some(query) = self.queries.get_mut(&query_id) {
                    query.waiting_for_permit = query.waiting_for_permit.saturating_sub(1);
                }
            }
            ActorMessage::SplitResolved {
                query_id,
                waiting_for_permit,
            } => {
                if let Some(query) = self.queries.get_mut(&query_id) {
                    if waiting_for_permit {
                        query.waiting_for_permit = query.waiting_for_permit.saturating_sub(1);
                    }
                    query.remaining = query.remaining.saturating_sub(1);
                    if query.remaining == 0 {
                        self.queries.remove(&query_id);
                        SCHEDULER_METRICS.queries.set(self.queries.len() as i64);
                    }
                }
            }
            ActorMessage::QueryTaskFinished(query_id) => {
                if let Some(query) = self.queries.get_mut(&query_id) {
                    query.running_count = query.running_count.saturating_sub(1);
                }
            }
            ActorMessage::CapacityFreed => {}
            #[cfg(test)]
            ActorMessage::Introspect(reply) => {
                let _ = reply.send(DebugState {
                    running_count: self.running_count.load(Ordering::Relaxed),
                    query_running_counts: self
                        .queries
                        .iter()
                        .map(|(query_id, query)| (*query_id, query.running_count))
                        .collect(),
                    registered_queries: self.queries.keys().copied().collect(),
                });
            }
        }
    }

    /// How long the actor took to pick up this job, before any time it then
    /// spends queued. The rest of its dispatch latency is queue residency.
    fn dispatch(&self, queued_job: QueuedJob) {
        self.running_count.fetch_add(1, Ordering::Relaxed);
        SCHEDULER_METRICS
            .dispatch_latency_secs
            .observe(queued_job.enqueued_at.elapsed().as_secs_f64());
        let capacity_guard = CapacityFreedGuard {
            tx: self.tx.clone(),
            running_count: self.running_count.clone(),
        };
        spawn_on_rayon(&self.rayon_pool, queued_job.job, capacity_guard);
    }

    /// The maximum number of concurrently running tasks any single query may
    /// have right now, given how many queries are currently competing for the
    /// pool.
    fn current_cap(&self) -> usize {
        let competing_queries = self
            .queries
            .values()
            .filter(|query| !query.ready.is_empty())
            .count();
        self.dispatch_cap.div_ceil(competing_queries.max(1))
    }

    /// Pops the single highest-priority ready and eligible task, if any,
    /// updating `running_count` for its owning query.
    fn pick_next(&mut self) -> Option<QueuedJob> {
        let cap = self.current_cap();
        let query = self
            .queries
            .iter_mut()
            .filter(|(_, query)| query.running_count < cap && !query.ready.is_empty())
            .min_by_key(|(_, query)| query.priority_key())
            .map(|(_, query)| query)?;
        let job = query
            .ready
            .pop_front()
            .expect("query was only selected because its ready queue is non-empty");
        query.running_count += 1;
        Some(job)
    }

    /// Tops up the number of per-query tasks handed to rayon to
    /// `dispatch_cap`, dispatching the highest-priority ready task(s) -- called
    /// after every handled message, since any of them could have made more
    /// capacity or work available.
    fn fill_capacity(&mut self) {
        while self.running_count.load(Ordering::Relaxed) < self.dispatch_cap {
            let Some(queued_job) = self.pick_next() else {
                break;
            };
            self.dispatch(queued_job);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use futures::future::join_all;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;

    fn test_scheduler(num_threads: usize) -> Arc<Scheduler> {
        test_scheduler_with_oversubscription(num_threads, 0)
    }

    fn test_scheduler_with_oversubscription(
        num_threads: usize,
        oversubscription: usize,
    ) -> Arc<Scheduler> {
        let rayon_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .unwrap(),
        );
        Scheduler::new_for_test(rayon_pool, "test", oversubscription).0
    }

    // Polls until `condition` is true or the timeout elapses, to avoid flaky
    // sleeps while still bounding worst-case test time.
    async fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "condition never became true");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // Polls the actor's own state (via `Scheduler::debug_state`) until
    // `condition` is true or the timeout elapses.
    async fn wait_until_debug_state(
        scheduler: &Scheduler,
        mut condition: impl FnMut(&DebugState) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let debug_state = scheduler.debug_state().await;
            if condition(&debug_state) {
                return;
            }
            assert!(Instant::now() < deadline, "condition never became true");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn test_fifo_is_dispatched_without_waiting_for_capacity() {
        let scheduler = test_scheduler(1);
        let guards = scheduler.register_query(1);

        // Step 1: saturate the scheduler. With a single thread and no
        // oversubscription, `dispatch_cap` is 1.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let blocker = guards[0].run_cpu_intensive_fair(
            move || {
                release_rx.recv().unwrap();
            },
            "test",
            "test",
        );
        wait_until_debug_state(&scheduler, |debug_state| debug_state.running_count == 1).await;

        // Step 2: a high priority task is handed to rayon straight away, even
        // though the dispatch cap is already reached.
        let (ran_tx, ran_rx) = std::sync::mpsc::channel::<()>();
        scheduler.enqueue_fifo(Box::new(move || ran_tx.send(()).unwrap()));
        wait_until(|| scheduler.running_count.load(Ordering::SeqCst) == 2).await;

        // Step 3: it only has to wait for a free thread, not for a free slot.
        release_tx.send(()).unwrap();
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("high priority task never ran");
        blocker.await.unwrap();
    }

    #[tokio::test]
    async fn test_on_task_complete_runs_even_if_job_panics() {
        let scheduler = test_scheduler(1);
        let guards = scheduler.register_query(1);

        let task = guards[0].run_cpu_intensive_fair(|| panic!("boom"), "test", "test");
        wait_until_debug_state(&scheduler, |debug_state| {
            debug_state
                .query_running_counts
                .values()
                .all(|&count| count == 0)
        })
        .await;
        task.await.unwrap_err();
    }

    #[tokio::test]
    async fn test_smaller_remaining_runs_first() {
        let scheduler = test_scheduler(1);
        let order: Arc<StdMutex<Vec<&'static str>>> = Arc::new(StdMutex::new(Vec::new()));

        let guards1 = scheduler.register_query(100);
        let guards2 = scheduler.register_query(2);

        // Step 1: Block the single worker so both tasks from step 2 stay in the
        // queue.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let blocker = guards1[0].run_cpu_intensive_fair(
            move || {
                release_rx.recv().unwrap();
            },
            "test",
            "test",
        );

        // Step 2: Add two tasks that remain queued by the scheduler
        let order_clone = order.clone();
        let task1 = guards1[0].run_cpu_intensive_fair(
            move || order_clone.lock().unwrap().push("query1"),
            "test",
            "test",
        );
        let order_clone = order.clone();
        let task2 = guards2[0].run_cpu_intensive_fair(
            move || order_clone.lock().unwrap().push("query2"),
            "test",
            "test",
        );

        // Step 3: Release the blocked worker to validate that query 2 with
        // fewer remaining splits is picked up first.
        release_tx.send(()).unwrap();
        wait_until(|| order.lock().unwrap().len() == 2).await;
        assert_eq!(*order.lock().unwrap(), vec!["query2", "query1"]);
        let (blocker_result, task1_result, task2_result) = tokio::join!(blocker, task1, task2);
        blocker_result.unwrap();
        task1_result.unwrap();
        task2_result.unwrap();
    }

    #[tokio::test]
    async fn test_cap_ignores_queries_with_no_ready_work() {
        let scheduler = test_scheduler(3);
        // Queries 1 and 2 simulate splits still waiting on a `SearchPermit`:
        // registered, but with nothing enqueued on the CPU scheduler yet.
        let _guards1 = scheduler.register_query(100);
        let _guards2 = scheduler.register_query(100);
        let guards3 = scheduler.register_query(100);

        let concurrent_3 = Arc::new(AtomicUsize::new(0));
        let max_concurrent_3 = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Arc::new(StdMutex::new(release_rx));
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let concurrent_3 = concurrent_3.clone();
            let max_concurrent_3 = max_concurrent_3.clone();
            let release_rx = release_rx.clone();
            tasks.push(guards3[0].run_cpu_intensive_fair(
                move || {
                    let current = concurrent_3.fetch_add(1, Ordering::SeqCst) + 1;
                    max_concurrent_3.fetch_max(current, Ordering::SeqCst);
                    release_rx.lock().unwrap().recv().unwrap();
                    concurrent_3.fetch_sub(1, Ordering::SeqCst);
                },
                "test",
                "test",
            ));
        }

        wait_until(|| concurrent_3.load(Ordering::SeqCst) == 3).await;
        // 3 queries are registered, but only query 3 has any ready/running
        // work, so it should get the whole pool instead of being capped at
        // usable/3 == 1 while the other two threads sit idle.
        assert_eq!(max_concurrent_3.load(Ordering::SeqCst), 3);

        for _ in 0..10 {
            release_tx.send(()).unwrap();
        }
        join_all(tasks).await;
    }

    #[tokio::test]
    async fn test_per_query_cap_shares_the_pool() {
        let scheduler = test_scheduler(4);
        let guards1 = scheduler.register_query(100);
        let guards2 = scheduler.register_query(100);

        let concurrent_1 = Arc::new(AtomicUsize::new(0));
        let max_concurrent_1 = Arc::new(AtomicUsize::new(0));
        let blocker_1 = Arc::new(AsyncMutex::new(()));
        let _blocker_1_guard = blocker_1.lock().await;
        let (release_tx_2, release_rx_2) = std::sync::mpsc::channel::<()>();
        let release_rx_2 = Arc::new(StdMutex::new(release_rx_2));
        let mut tasks1 = Vec::new();
        let mut tasks2 = Vec::new();

        // Interleave both queries' backlogs (each with more tasks than the
        // pool could ever run at once for it alone) so both count as
        // competing from the start, splitting cap = 4/2 = 2 between them.
        for _ in 0..10 {
            let concurrent_1 = concurrent_1.clone();
            let max_concurrent_1 = max_concurrent_1.clone();
            let blocker_1 = blocker_1.clone();
            tasks1.push(guards1[0].run_cpu_intensive_fair(
                move || {
                    let current = concurrent_1.fetch_add(1, Ordering::SeqCst) + 1;
                    max_concurrent_1.fetch_max(current, Ordering::SeqCst);
                    // tasks for query 1 remain blocked until the end of the test
                    let _unused = blocker_1.blocking_lock();
                },
                "test",
                "test",
            ));
            let release_rx_2 = release_rx_2.clone();
            tasks2.push(guards2[0].run_cpu_intensive_fair(
                move || {
                    release_rx_2.lock().unwrap().recv().unwrap();
                },
                "test",
                "test",
            ));
        }
        wait_until(|| concurrent_1.load(Ordering::SeqCst) >= 2).await;

        // Make sure that even if query 2 is running faster, its capacity is not
        // taken over by query 1.
        for _ in 0..8 {
            release_tx_2.send(()).unwrap();
        }
        assert_eq!(max_concurrent_1.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_directly_injected_rayon_work_runs_promptly_even_under_backlog() {
        // The actor never holds onto a rayon worker thread: it only ever
        // submits one task at a time, directly, whenever a slot is free. So
        // anything submitted straight to the same rayon pool outside the
        // scheduler (e.g. Tantivy's own internal parallelism) always competes
        // on an equal footing in rayon's own queue -- no periodic yielding
        // needed, and no possible starvation by construction.
        let scheduler = test_scheduler(2);
        let guards = scheduler.register_query(100_000);

        // Keep both workers continuously busy with a long stream of short
        // tasks. Held alive (not joined) for the rest of the test: draining
        // the full backlog isn't needed to prove directly-injected work isn't
        // starved, and would needlessly slow the test down.
        let _tasks: Vec<_> = guards
            .iter()
            .take(1000)
            .map(|guard| {
                guard.run_cpu_intensive_fair(
                    || {
                        std::thread::sleep(Duration::from_millis(10));
                    },
                    "test",
                    "test",
                )
            })
            .collect();
        wait_until_debug_state(&scheduler, |debug_state| debug_state.running_count >= 1).await;

        let (tx, rx) = std::sync::mpsc::channel();
        scheduler.rayon_pool.spawn(move || tx.send(()).unwrap());
        rx.recv_timeout(Duration::from_millis(500))
            .expect("directly-injected rayon work starved");
    }

    #[tokio::test]
    async fn test_oversubscription_stages_extra_jobs_in_rayons_queue() {
        let scheduler = test_scheduler_with_oversubscription(1, 2);
        let guards = scheduler.register_query(3);

        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Arc::new(StdMutex::new(release_rx));
        let mut tasks = Vec::new();
        for guard in &guards {
            let release_rx = release_rx.clone();
            tasks.push(guard.run_cpu_intensive_fair(
                move || release_rx.lock().unwrap().recv().unwrap(),
                "test",
                "test",
            ));
        }

        // The single worker can only run one of them, the two others sit in
        // rayon's queue ready to be picked up the instant it frees up.
        wait_until_debug_state(&scheduler, |debug_state| debug_state.running_count == 3).await;

        for _ in 0..3 {
            release_tx.send(()).unwrap();
        }
        join_all(tasks).await;
    }

    #[tokio::test]
    async fn test_query_state_cleaned_up_once_remaining_reaches_zero() {
        let scheduler = test_scheduler(2);
        let mut guards = scheduler.register_query(2);
        wait_until_debug_state(&scheduler, |debug_state| {
            debug_state.registered_queries.len() == 1
        })
        .await;

        drop(guards.pop().unwrap());
        // Give the `SplitResolved` message time to be processed, then check
        // the query is still registered (1 split remains).
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(scheduler.debug_state().await.registered_queries.len(), 1);

        drop(guards.pop().unwrap());
        wait_until_debug_state(&scheduler, |debug_state| {
            debug_state.registered_queries.is_empty()
        })
        .await;
    }

    #[tokio::test]
    async fn test_running_count_drains_back_to_zero_once_all_tasks_complete() {
        let scheduler = test_scheduler(2);
        let guards = scheduler.register_query(3);
        let ran = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..3 {
            let ran = ran.clone();
            tasks.push(guards[0].run_cpu_intensive_fair(
                move || {
                    ran.fetch_add(1, Ordering::SeqCst);
                },
                "test",
                "test",
            ));
        }
        wait_until(|| ran.load(Ordering::SeqCst) == 3).await;
        wait_until_debug_state(&scheduler, |debug_state| debug_state.running_count == 0).await;
        join_all(tasks).await;
    }

    #[tokio::test]
    async fn test_more_waiting_for_permit_runs_last() {
        let scheduler = test_scheduler(1);
        let order: Arc<StdMutex<Vec<&'static str>>> = Arc::new(StdMutex::new(Vec::new()));

        let guards1 = scheduler.register_query(10);
        let mut guards2 = scheduler.register_query(20);
        for guard in guards2.iter_mut().take(15) {
            guard.mark_permit_obtained();
        }

        // Step 1: Block the single worker so both tasks from step 2 stay in
        // the queue.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let blocker = guards1[0].run_cpu_intensive_fair(
            move || {
                release_rx.recv().unwrap();
            },
            "test",
            "test",
        );

        // Step 2: Add one task per query that remain in the queue.
        let order_clone = order.clone();
        let task1 = guards1[0].run_cpu_intensive_fair(
            move || order_clone.lock().unwrap().push("query1"),
            "test",
            "test",
        );
        let order_clone = order.clone();
        let task2 = guards2[0].run_cpu_intensive_fair(
            move || order_clone.lock().unwrap().push("query2"),
            "test",
            "test",
        );

        // Step 3: Release the blocked worker to validate that query 2, which
        // has no splits waiting on a permit, is picked up before query 1.
        release_tx.send(()).unwrap();
        wait_until(|| order.lock().unwrap().len() == 2).await;
        assert_eq!(*order.lock().unwrap(), vec!["query2", "query1"]);
        let (blocker_result, task1_result, task2_result) = tokio::join!(blocker, task1, task2);
        blocker_result.unwrap();
        task1_result.unwrap();
        task2_result.unwrap();
    }

    #[tokio::test]
    async fn test_register_query_with_zero_splits_returns_no_guards() {
        let scheduler = test_scheduler(1);
        let guards = scheduler.register_query(0);
        assert!(guards.is_empty());
        assert!(scheduler.debug_state().await.registered_queries.is_empty());
    }

    #[tokio::test]
    async fn test_actor_stops_once_scheduler_and_guards_are_dropped() {
        let rayon_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        );
        let (scheduler, handle) = Scheduler::new_for_test(rayon_pool, "test", 0);
        let guards = scheduler.register_query(1);

        drop(scheduler);
        // A guard (holding its own `Arc<Scheduler>` clone) is still alive, so
        // the actor must not have stopped yet.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!handle.is_finished());

        drop(guards);
        handle.await.expect("scheduler actor task should not panic");
    }
}
