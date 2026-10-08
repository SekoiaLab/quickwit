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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytesize::ByteSize;
use quickwit_common::metrics::{GaugeGuard, HistogramTimer};
use quickwit_proto::search::SplitIdAndFooterOffsets;
#[cfg(test)]
use tokio::sync::watch;
use tokio::sync::{mpsc, oneshot};

use crate::metrics::SearchTaskMetrics;

/// Distributor of permits to perform split search operation.
///
/// Requests are served by lowest remaining query cost (see [`QueryRemainingCost`]), then in
/// order. Each permit reserves a slot for concurrent
/// search execution and a pessimistic amount of memory. The slot is held for
/// the entire duration of the search. Once the actual memory usage is known,
/// it can be updated via `update_memory_usage()`. When the permit is dropped,
/// both the search slot and memory are released.
#[derive(Clone)]
pub struct SearchPermitProvider {
    message_sender: mpsc::UnboundedSender<SearchPermitMessage>,
    #[cfg(test)]
    actor_stopped: watch::Receiver<bool>,
}

pub enum SearchPermitMessage {
    Request {
        permit_sender: oneshot::Sender<Vec<SearchPermitFuture>>,
        splits: Vec<SplitSearchTaskMetadata>,
        remaining_cost: Arc<QueryRemainingCost>,
    },
    UpdateMemory {
        memory_delta: i64,
    },
    Drop {
        memory_size: u64,
    },
}

/// Resources a split search is expected to need, used to request its permit.
#[derive(Clone, Copy, Debug)]
pub struct SplitSearchTaskMetadata {
    /// Pessimistic estimate of the memory needed, see [`compute_initial_memory_allocation`].
    pub memory_allocation: ByteSize,
    /// Estimated cost of searching the split, see [`crate::cost::compute_split_query_cost`].
    pub job_cost: usize,
}

/// Estimated cost of the splits of a leaf search request that are not done yet.
///
/// It is shared by the permit requests of all the indexes targeted by the leaf
/// search request. It helps assessing the overall remaining cost of the search
/// request, even though the permits are requested by index.
#[derive(Debug)]
pub struct QueryRemainingCost {
    awaiting_permit: AtomicUsize,
    in_progress: AtomicUsize,
}

impl QueryRemainingCost {
    pub fn new(total_cost: usize) -> Arc<Self> {
        Arc::new(QueryRemainingCost {
            awaiting_permit: AtomicUsize::new(total_cost),
            in_progress: AtomicUsize::new(0),
        })
    }

    fn awaiting_permit(&self) -> usize {
        self.awaiting_permit.load(Ordering::Relaxed)
    }

    fn in_progress(&self) -> usize {
        self.in_progress.load(Ordering::Relaxed)
    }

    /// Moves `cost` from awaiting a permit to in progress.
    fn start(&self, cost: usize) {
        saturating_sub(&self.awaiting_permit, cost);
        self.in_progress.fetch_add(cost, Ordering::Relaxed);
    }

    /// Removes `cost` from in progress.
    fn finish(&self, cost: usize) {
        saturating_sub(&self.in_progress, cost);
    }
}

fn saturating_sub(counter: &AtomicUsize, value: usize) {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(value))
        })
        .expect("the update closure always returns Some");
}

/// Makes very pessimistic estimate of the memory allocation required for a split search
///
/// This is refined later on when more data is available about the split.
pub fn compute_initial_memory_allocation(
    split: &SplitIdAndFooterOffsets,
    warmup_single_split_initial_allocation: ByteSize,
) -> ByteSize {
    let split_size = split.split_footer_start;
    // we consider the configured initial allocation to be set for a large split with 10M docs
    const LARGE_SPLIT_NUM_DOCS: u64 = 10_000_000;
    let proportional_allocation =
        warmup_single_split_initial_allocation.as_u64() * split.num_docs / LARGE_SPLIT_NUM_DOCS;
    let size_bytes = [
        split_size,
        proportional_allocation,
        warmup_single_split_initial_allocation.as_u64(),
    ]
    .into_iter()
    .min()
    .unwrap();
    const MINIMUM_ALLOCATION_BYTES: u64 = 10_000_000;
    ByteSize(size_bytes.max(MINIMUM_ALLOCATION_BYTES))
}

impl SearchPermitProvider {
    pub fn new(
        max_num_concurrent_split_searches: usize,
        memory_budget: ByteSize,
        metrics: SearchTaskMetrics,
    ) -> Self {
        let (message_sender, message_receiver) = mpsc::unbounded_channel();
        #[cfg(test)]
        let (state_sender, state_receiver) = watch::channel(false);
        let actor = SearchPermitActor {
            msg_receiver: message_receiver,
            msg_sender: message_sender.downgrade(),
            num_search_slots_available: max_num_concurrent_split_searches,
            total_memory_budget: memory_budget.as_u64(),
            permits_requests: Vec::new(),
            next_permit_request_sequence: 0,
            total_memory_allocated: 0u64,
            #[cfg(test)]
            stopped: state_sender,
            metrics,
        };
        tokio::spawn(actor.run());
        Self {
            message_sender,
            #[cfg(test)]
            actor_stopped: state_receiver,
        }
    }

    /// Returns one permit future for each provided split metadata.
    ///
    /// The permits returned are guaranteed to be resolved in order. Across calls, permits are
    /// granted by lowest `remaining_cost` first, then in the order of the calls. Calls sharing
    /// the same `remaining_cost` are therefore served together.
    ///
    /// The permit memory size is capped by per_permit_initial_memory_allocation.
    pub async fn get_permits(
        &self,
        splits: impl IntoIterator<Item = SplitSearchTaskMetadata>,
        remaining_cost: Arc<QueryRemainingCost>,
    ) -> Vec<SearchPermitFuture> {
        let splits: Vec<SplitSearchTaskMetadata> = splits.into_iter().collect();
        if splits.is_empty() {
            return Vec::new();
        }
        let (permit_sender, permit_receiver) = oneshot::channel();
        self.message_sender
            .send(SearchPermitMessage::Request {
                permit_sender,
                splits,
                remaining_cost,
            })
            .expect("Receiver lives longer than sender");
        permit_receiver
            .await
            .expect("Receiver lives longer than sender")
    }
}

struct SearchPermitActor {
    metrics: SearchTaskMetrics,
    msg_receiver: mpsc::UnboundedReceiver<SearchPermitMessage>,
    msg_sender: mpsc::WeakUnboundedSender<SearchPermitMessage>,
    num_search_slots_available: usize,
    /// Note it is possible for memory_allocated to exceed memory_budget temporarily,
    /// if and only if a split leaf search task ended up using more than `initial_allocation`.
    /// When it happens, new permits will not be assigned until the memory is freed.
    total_memory_budget: u64,
    total_memory_allocated: u64,
    /// Pending requests, served by lowest remaining cost then by sequence. Use
    /// a plain Vec because the cost is shared with subqueries to other indexes.
    /// We can go back to a heap when the permit request is moved up to the
    /// multi index query step.
    permits_requests: Vec<LeafPermitRequest>,
    next_permit_request_sequence: u64,
    #[cfg(test)]
    stopped: watch::Sender<bool>,
}

struct SingleSplitPermitRequest {
    permit_sender: oneshot::Sender<SearchPermit>,
    permit_size: u64,
    job_cost: usize,
}

struct LeafPermitRequest {
    /// Single split permit requests for this leaf search.
    single_split_permit_requests: std::vec::IntoIter<SingleSplitPermitRequest>,
    /// Remaining cost of the search request, possibly shared with other leaf permit requests.
    remaining_cost: Arc<QueryRemainingCost>,
    /// Arrival order, used to break ties between requests with the same remaining cost.
    sequence: u64,
}

impl LeafPermitRequest {
    fn from_estimated_costs(
        splits: Vec<SplitSearchTaskMetadata>,
        remaining_cost: Arc<QueryRemainingCost>,
        sequence: u64,
    ) -> (Self, Vec<SearchPermitFuture>) {
        let mut permits = Vec::with_capacity(splits.len());
        let mut single_split_permit_requests = Vec::with_capacity(splits.len());
        let wait_histogram = &crate::metrics::SEARCH_METRICS.leaf_search_permit_wait_duration_secs;
        for split in splits {
            let (tx, rx) = oneshot::channel();
            // we keep our internal list of permits and the returned wait handles in the
            // same order to make sure we emit each permit in the right order. Doing otherwise
            // may cause deadlocks
            single_split_permit_requests.push(SingleSplitPermitRequest {
                permit_sender: tx,
                permit_size: split.memory_allocation.as_u64(),
                job_cost: split.job_cost,
            });
            permits.push(SearchPermitFuture {
                receiver: rx,
                wait_timer: Some(wait_histogram.start_timer()),
            });
        }
        (
            LeafPermitRequest {
                single_split_permit_requests: single_split_permit_requests.into_iter(),
                remaining_cost,
                sequence,
            },
            permits,
        )
    }

    fn pop_if_smaller_than(&mut self, max_size: u64) -> Option<SingleSplitPermitRequest> {
        // IntoIter::as_slice() allows us to peek at the next element without consuming it
        match self.single_split_permit_requests.as_slice().first() {
            Some(request) if request.permit_size <= max_size => {
                self.single_split_permit_requests.next()
            }
            _ => None,
        }
    }

    fn is_empty(&self) -> bool {
        self.single_split_permit_requests.as_slice().is_empty()
    }
}

impl SearchPermitActor {
    async fn run(mut self) {
        // Stops when the last clone of SearchPermitProvider is dropped.
        while let Some(msg) = self.msg_receiver.recv().await {
            self.handle_message(msg);
        }
        #[cfg(test)]
        self.stopped.send(true).ok();
    }

    fn handle_message(&mut self, msg: SearchPermitMessage) {
        match msg {
            SearchPermitMessage::Request {
                splits,
                permit_sender,
                remaining_cost,
            } => {
                assert_ne!(
                    splits.len(),
                    0,
                    "empty permit request would lead to deadlock"
                );
                let sequence = self.next_permit_request_sequence;
                self.next_permit_request_sequence += 1;
                let (leaf_permit_request, permits) =
                    LeafPermitRequest::from_estimated_costs(splits, remaining_cost, sequence);
                self.permits_requests.push(leaf_permit_request);
                self.assign_available_permits();
                // The receiver could be dropped in the (unlikely) situation
                // where the future requesting these permits is cancelled before
                // this message is processed.
                let _ = permit_sender.send(permits);
            }
            SearchPermitMessage::UpdateMemory { memory_delta } => {
                if self.total_memory_allocated as i64 + memory_delta < 0 {
                    panic!("More memory released than allocated, should never happen.")
                }
                self.total_memory_allocated =
                    (self.total_memory_allocated as i64 + memory_delta) as u64;
                self.assign_available_permits();
            }
            SearchPermitMessage::Drop { memory_size } => {
                self.num_search_slots_available += 1;
                self.total_memory_allocated = self
                    .total_memory_allocated
                    .checked_sub(memory_size)
                    .expect("More memory released than allocated, should never happen.");
                self.assign_available_permits();
            }
        }
    }

    /// Pops the next single split permit request of the leaf permit request with the lowest
    /// remaining cost, if there are enough resources to serve it, and deducts its cost from the
    /// remaining cost of its search request.
    ///
    /// Also returns the remaining cost of its search request.
    fn pop_next_request_if_serviceable(
        &mut self,
    ) -> Option<(SingleSplitPermitRequest, Arc<QueryRemainingCost>)> {
        if self.num_search_slots_available == 0 {
            return None;
        }
        let available_memory = self
            .total_memory_budget
            .checked_sub(self.total_memory_allocated)?;
        let (next_idx, _) =
            self.permits_requests
                .iter()
                .enumerate()
                .min_by_key(|(_, leaf_req)| {
                    (leaf_req.remaining_cost.awaiting_permit(), leaf_req.sequence)
                })?;
        let leaf_req = &mut self.permits_requests[next_idx];
        let permit_request = leaf_req.pop_if_smaller_than(available_memory)?;
        leaf_req.remaining_cost.start(permit_request.job_cost);
        let remaining_cost = leaf_req.remaining_cost.clone();
        if leaf_req.is_empty() {
            // the order of the remaining requests is given by their sequence, not their position
            self.permits_requests.swap_remove(next_idx);
        }
        Some((permit_request, remaining_cost))
    }

    fn assign_available_permits(&mut self) {
        while let Some((permit_request, remaining_query_cost)) =
            self.pop_next_request_if_serviceable()
        {
            let ongoing_tasks_metric = self.metrics.ongoing_tasks;
            let mut ongoing_gauge_guard = GaugeGuard::from_gauge(ongoing_tasks_metric);
            ongoing_gauge_guard.add(1);
            self.total_memory_allocated += permit_request.permit_size;
            self.num_search_slots_available -= 1;
            permit_request
                .permit_sender
                .send(SearchPermit {
                    _ongoing_gauge_guard: ongoing_gauge_guard,
                    msg_sender: self.msg_sender.clone(),
                    memory_allocation: permit_request.permit_size,
                    job_cost: permit_request.job_cost,
                    remaining_query_cost,
                })
                // if the requester dropped its receiver, we drop the newly
                // created SearchPermit which releases the resources
                .ok();
        }
        let pending_tasks = self
            .permits_requests
            .iter()
            .map(|leaf_req| leaf_req.single_split_permit_requests.as_slice().len() as i64)
            .sum();
        self.metrics.pending_tasks.set(pending_tasks);
    }
}

pub struct SearchPermit {
    _ongoing_gauge_guard: GaugeGuard<'static>,
    msg_sender: mpsc::WeakUnboundedSender<SearchPermitMessage>,
    memory_allocation: u64,
    /// Cost of the split this permit was granted to, counted in progress until it is dropped.
    job_cost: usize,
    /// Remaining cost of the search request, still updated as other permits are granted or
    /// dropped.
    remaining_query_cost: Arc<QueryRemainingCost>,
}

impl SearchPermit {
    /// Update the memory usage attached to this permit.
    ///
    /// This will increase or decrease the available memory in the [`SearchPermitProvider`].
    pub fn update_memory_usage(&mut self, new_memory_usage: ByteSize) {
        let new_usage_bytes = new_memory_usage.as_u64();
        let memory_delta = new_usage_bytes as i64 - self.memory_allocation as i64;
        self.memory_allocation = new_usage_bytes;
        self.send_if_still_running(SearchPermitMessage::UpdateMemory { memory_delta });
    }

    pub fn memory_allocation(&self) -> ByteSize {
        ByteSize(self.memory_allocation)
    }

    /// Current estimated cost of the splits of the search request this permit belongs to that
    /// are still waiting for a permit.
    ///
    /// It decreases as permits are granted to the request, including after this one was.
    pub fn remaining_cost_awaiting_permit(&self) -> usize {
        self.remaining_query_cost.awaiting_permit()
    }

    /// Current estimated cost of the splits of the search request this permit belongs to that
    /// were granted a permit that is not dropped yet, including this one.
    pub fn remaining_cost_in_progress(&self) -> usize {
        self.remaining_query_cost.in_progress()
    }

    fn send_if_still_running(&self, msg: SearchPermitMessage) {
        if let Some(sender) = self.msg_sender.upgrade() {
            sender
                .send(msg)
                // Receiver instance in the event loop is never dropped or
                // closed as long as there is a strong sender reference.
                .expect("Receiver should live longer than sender");
        }
    }
}

impl Drop for SearchPermit {
    fn drop(&mut self) {
        self.remaining_query_cost.finish(self.job_cost);
        self.send_if_still_running(SearchPermitMessage::Drop {
            memory_size: self.memory_allocation,
        });
    }
}

pub struct SearchPermitFuture {
    receiver: oneshot::Receiver<SearchPermit>,
    /// Records the time spent queuing for this permit
    wait_timer: Option<HistogramTimer>,
}

impl Future for SearchPermitFuture {
    type Output = SearchPermit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let receiver = Pin::new(&mut this.receiver);
        match receiver.poll(cx) {
            Poll::Ready(Ok(search_permit)) => {
                // Record now rather than on drop, so that the measure doesn't depend on how long
                // the caller holds onto this future once it has resolved.
                if let Some(wait_timer) = this.wait_timer.take() {
                    wait_timer.observe_duration();
                }
                Poll::Ready(search_permit)
            }
            Poll::Ready(Err(_)) => panic!(
                "Failed to acquire permit. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."
            ),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use rand::seq::SliceRandom;
    use tokio::task::JoinSet;

    use super::*;
    use crate::metrics::SEARCH_METRICS;

    fn test_metrics() -> SearchTaskMetrics {
        SEARCH_METRICS.search_task_metrics()
    }

    /// `count` splits of `memory_mb`, each with the given `job_cost`.
    fn splits(memory_mb: u64, count: usize, job_cost: usize) -> Vec<SplitSearchTaskMetadata> {
        let split = SplitSearchTaskMetadata {
            memory_allocation: ByteSize::mb(memory_mb),
            job_cost,
        };
        vec![split; count]
    }

    /// Requests permits for `count` splits of `memory_mb` belonging to a new query, each costing
    /// the same.
    async fn get_permits_for_new_query(
        permit_provider: &SearchPermitProvider,
        memory_mb: u64,
        count: usize,
    ) -> Vec<SearchPermitFuture> {
        const JOB_COST: usize = 5;
        permit_provider
            .get_permits(
                splits(memory_mb, count, JOB_COST),
                QueryRemainingCost::new(JOB_COST * count),
            )
            .await
    }

    #[tokio::test]
    async fn test_get_permits_empty() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100), test_metrics());
        let permits = get_permits_for_new_query(&permit_provider, 10, 0).await;
        assert!(permits.is_empty());

        // Subsequent non-empty requests must still be served normally.
        let permits = get_permits_for_new_query(&permit_provider, 10, 1).await;
        assert_eq!(permits.len(), 1);
        let _permit = permits.into_iter().next().unwrap().await;
    }

    /// A wait that ends in a cancellation must still be reported, otherwise the metric hides the
    /// longest waits.
    #[tokio::test]
    async fn test_abandoned_permit_wait_is_reported() {
        let samples_before = SEARCH_METRICS
            .leaf_search_permit_wait_duration_secs
            .get_sample_count();

        // A single search slot, so the second permit stays queued. Neither future is ever polled,
        // so both waits can only be reported by `Drop`.
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100), test_metrics());
        let permit_futs = get_permits_for_new_query(&permit_provider, 10, 2).await;
        assert_eq!(permit_futs.len(), 2);
        drop(permit_futs);

        // `SEARCH_METRICS` is process wide, so other tests may concurrently add samples of their
        // own: only the two we are responsible for are guaranteed.
        assert!(
            SEARCH_METRICS
                .leaf_search_permit_wait_duration_secs
                .get_sample_count()
                >= samples_before + 2
        );
    }

    #[tokio::test]
    async fn test_search_permit_order() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100), test_metrics());
        let mut all_futures = Vec::new();
        let first_batch_of_permits = get_permits_for_new_query(&permit_provider, 10, 10).await;
        assert_eq!(first_batch_of_permits.len(), 10);
        all_futures.extend(
            first_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((1, i), fut)),
        );

        let second_batch_of_permits = get_permits_for_new_query(&permit_provider, 10, 10).await;
        assert_eq!(second_batch_of_permits.len(), 10);
        all_futures.extend(
            second_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((2, i), fut)),
        );

        // not super useful, considering what join set does, but still a tiny bit more sound.
        all_futures.shuffle(&mut rand::rng());

        let mut join_set = JoinSet::new();
        for (res, fut) in all_futures {
            join_set.spawn(async move {
                let permit = fut.await;
                (res, permit)
            });
        }
        let mut ordered_result: Vec<(usize, usize)> = Vec::with_capacity(20);
        while let Some(Ok(((batch_id, order), _permit))) = join_set.join_next().await {
            ordered_result.push((batch_id, order));
        }

        assert_eq!(ordered_result.len(), 20);
        for (i, res) in ordered_result[0..10].iter().enumerate() {
            assert_eq!(res, &(1, i));
        }
        for (i, res) in ordered_result[10..20].iter().enumerate() {
            assert_eq!(res, &(2, i));
        }
    }

    #[tokio::test]
    async fn test_search_permit_order_with_concurrent_search() {
        let permit_provider = SearchPermitProvider::new(4, ByteSize::mb(100), test_metrics());
        let mut all_futures = Vec::new();
        let first_batch_of_permits = get_permits_for_new_query(&permit_provider, 10, 8).await;
        assert_eq!(first_batch_of_permits.len(), 8);
        all_futures.extend(
            first_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((1, i), fut)),
        );

        let second_batch_of_permits = get_permits_for_new_query(&permit_provider, 10, 2).await;
        all_futures.extend(
            second_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((2, i), fut)),
        );

        let third_batch_of_permits = get_permits_for_new_query(&permit_provider, 10, 6).await;
        all_futures.extend(
            third_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((3, i), fut)),
        );

        // not super useful, considering what join set does, but still a tiny bit more sound.
        all_futures.shuffle(&mut rand::rng());

        let mut join_set = JoinSet::new();
        for (res, fut) in all_futures {
            join_set.spawn(async move {
                let permit = fut.await;
                (res, permit)
            });
        }
        let mut ordered_result: Vec<(usize, usize)> = Vec::with_capacity(20);
        while let Some(Ok(((batch_id, order), _permit))) = join_set.join_next().await {
            ordered_result.push((batch_id, order));
        }

        let mut counters = [0; 4];
        let expected_result: Vec<(usize, usize)> = [
            1, 1, 1, 1, // initial 4 permits
            2, 2, 1, 1, 1, 1, 3, 3, 3, 3, 3, 3,
        ]
        .into_iter()
        .map(|batch_id| {
            let order = counters[batch_id];
            counters[batch_id] += 1;
            (batch_id, order)
        })
        .collect();

        // for the first 4 permits, the order is not well defined as they are all granted at once,
        // and we poll futures in a random order. We sort them to fix that artifact
        ordered_result[..4].sort();
        assert_eq!(ordered_result, expected_result);
    }

    #[tokio::test]
    async fn test_search_permit_order_by_shared_remaining_cost() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100), test_metrics());
        let blocker = get_permits_for_new_query(&permit_provider, 10, 1)
            .await
            .pop()
            .unwrap()
            .await;

        // Query A targets two indexes: its permit requests share a remaining cost of 40, higher
        // than query B's 30, even though each of them is cheaper than query B.
        let query_a_remaining_cost = QueryRemainingCost::new(40);
        let query_a_index_1 = permit_provider
            .get_permits(splits(10, 2, 10), query_a_remaining_cost.clone())
            .await;
        let query_a_index_2 = permit_provider
            .get_permits(splits(10, 2, 10), query_a_remaining_cost)
            .await;
        let query_b = permit_provider
            .get_permits(splits(10, 3, 10), QueryRemainingCost::new(30))
            .await;

        let mut join_set = JoinSet::new();
        for (request, permit_futures) in [
            ("a1", query_a_index_1),
            ("a2", query_a_index_2),
            ("b", query_b),
        ] {
            for (split_idx, permit_future) in permit_futures.into_iter().enumerate() {
                join_set.spawn(async move {
                    let permit = permit_future.await;
                    (request, split_idx, permit)
                });
            }
        }
        drop(blocker);
        let mut execution_order = Vec::new();
        // Only one permit is granted at a time: each one is dropped before getting the next.
        while let Some(result) = join_set.join_next().await {
            let (request, split_idx, permit) = result.unwrap();
            execution_order.push((request, split_idx, permit.remaining_cost_awaiting_permit()));
        }
        assert_eq!(
            execution_order,
            vec![
                ("b", 0, 20),
                ("b", 1, 10),
                ("b", 2, 0),
                ("a1", 0, 30),
                ("a1", 1, 20),
                ("a2", 0, 10),
                ("a2", 1, 0),
            ]
        );
    }

    #[tokio::test]
    async fn test_permit_remaining_query_cost_follows_later_grants() {
        let permit_provider = SearchPermitProvider::new(2, ByteSize::mb(100), test_metrics());
        let remaining_cost = QueryRemainingCost::new(30);
        let mut permit_futures = permit_provider
            .get_permits(splits(10, 3, 10), remaining_cost.clone())
            .await
            .into_iter();
        // The first two permits are granted together: both see the cost left after that batch.
        let first_permit = permit_futures.next().unwrap().await;
        let second_permit = permit_futures.next().unwrap().await;
        assert_eq!(first_permit.remaining_cost_awaiting_permit(), 10);
        assert_eq!(second_permit.remaining_cost_awaiting_permit(), 10);
        assert_eq!(first_permit.remaining_cost_in_progress(), 20);

        // Granting the last permit is visible from the permits granted before it.
        drop(first_permit);
        let third_permit = permit_futures.next().unwrap().await;
        assert_eq!(second_permit.remaining_cost_awaiting_permit(), 0);
        assert_eq!(third_permit.remaining_cost_awaiting_permit(), 0);
        assert_eq!(second_permit.remaining_cost_in_progress(), 20);

        // Dropping the permits completes the request.
        drop(second_permit);
        drop(third_permit);
        assert_eq!(remaining_cost.awaiting_permit(), 0);
        assert_eq!(remaining_cost.in_progress(), 0);
    }

    #[tokio::test]
    async fn test_search_permit_early_drops() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100), test_metrics());
        let permit_fut1 = get_permits_for_new_query(&permit_provider, 10, 1)
            .await
            .into_iter()
            .next()
            .unwrap();
        let permit_fut2 = get_permits_for_new_query(&permit_provider, 10, 1)
            .await
            .into_iter()
            .next()
            .unwrap();
        drop(permit_fut1);
        let permit = permit_fut2.await;
        assert_eq!(permit.memory_allocation, ByteSize::mb(10).as_u64());
        assert_eq!(*permit_provider.actor_stopped.borrow(), false);

        let _permit_fut3 = get_permits_for_new_query(&permit_provider, 10, 1)
            .await
            .into_iter()
            .next()
            .unwrap();
        let mut actor_stopped = permit_provider.actor_stopped.clone();
        drop(permit_provider);
        {
            actor_stopped.changed().await.unwrap();
            assert!(*actor_stopped.borrow());
        }
    }

    /// Tries to wait for a permit
    async fn try_get(permit_fut: SearchPermitFuture) -> anyhow::Result<SearchPermit> {
        // using a short timeout is a bit flaky, but it should be enough for these tests
        let permit = tokio::time::timeout(Duration::from_millis(20), permit_fut).await?;
        Ok(permit)
    }

    #[tokio::test]
    async fn test_memory_budget() {
        let permit_provider = SearchPermitProvider::new(100, ByteSize::mb(100), test_metrics());
        let mut permit_futs = get_permits_for_new_query(&permit_provider, 10, 14).await;
        let mut remaining_permit_futs = permit_futs.split_off(10).into_iter();
        assert_eq!(remaining_permit_futs.len(), 4);
        // we should be able to obtain 10 permits right away (100MB / 10MB)
        let mut permits: Vec<SearchPermit> = futures::stream::iter(permit_futs.into_iter())
            .buffered(1)
            .collect()
            .await;
        // the next permit is blocked by the memory budget
        let next_blocked_permit_fut = remaining_permit_futs.next().unwrap();
        try_get(next_blocked_permit_fut).await.err().unwrap();
        // if we drop one of the permits, we can get a new one
        permits.drain(0..1);
        let next_permit_fut = remaining_permit_futs.next().unwrap();
        let _new_permit = try_get(next_permit_fut).await.unwrap();
        // the next permit is blocked again by the memory budget
        let next_blocked_permit_fut = remaining_permit_futs.next().unwrap();
        try_get(next_blocked_permit_fut).await.err().unwrap();
        // by setting a more accurate memory usage after a completed warmup, we can get more permits
        permits[0].update_memory_usage(ByteSize::mb(4));
        permits[1].update_memory_usage(ByteSize::mb(6));
        let next_permit_fut = remaining_permit_futs.next().unwrap();
        try_get(next_permit_fut).await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_search_slots() {
        let permit_provider = SearchPermitProvider::new(10, ByteSize::mb(100), test_metrics());
        let mut permit_futs = get_permits_for_new_query(&permit_provider, 1, 16).await;
        let mut remaining_permit_futs = permit_futs.split_off(10).into_iter();
        assert_eq!(remaining_permit_futs.len(), 6);
        // we should be able to obtain 10 permits right away
        let mut permits: Vec<SearchPermit> = futures::stream::iter(permit_futs.into_iter())
            .buffered(1)
            .collect()
            .await;
        // the next permit is blocked by the concurrent search slots
        let next_blocked_permit_fut = remaining_permit_futs.next().unwrap();
        try_get(next_blocked_permit_fut).await.err().unwrap();
        // if we drop one of the permits, we can get a new one
        permits.drain(0..1);
        let next_permit_fut = remaining_permit_futs.next().unwrap();
        permits.push(try_get(next_permit_fut).await.unwrap());
        // the next permit is blocked again by the concurrent search slots
        let next_blocked_permit_fut = remaining_permit_futs.next().unwrap();
        try_get(next_blocked_permit_fut).await.err().unwrap();
        // dropping a permit frees up a slot
        permits.drain(0..1);
        let next_permit_fut = remaining_permit_futs.next().unwrap();
        permits.push(try_get(next_permit_fut).await.unwrap());
    }
}
