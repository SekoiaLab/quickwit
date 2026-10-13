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

//! Temporary per-community object storage metrics.
//!
//! The community is extracted from the object key (`events-<community_uuid>/<split_id>.split`).
//! On top of plain counters, we expose the max 1-second value observed over the last
//! [`WINDOW_SECS`] seconds, because the average rate computed from counters over a 15s/30s
//! scrape interval hides the real peaks.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, new_counter_vec, new_gauge_vec,
};

use crate::metrics_wrappers::ActionLabel;

const WINDOW_SECS: usize = 30;
const UNKNOWN_COMMUNITY: &str = "unknown";

struct CommunityMetrics {
    requests_total: IntCounterVec<2>,
    bytes_total: IntCounterVec<2>,
    peak_requests_per_sec: IntGaugeVec<2>,
    peak_bytes_per_sec: IntGaugeVec<2>,
}

static COMMUNITY_METRICS: Lazy<CommunityMetrics> = Lazy::new(|| CommunityMetrics {
    requests_total: new_counter_vec(
        "object_storage_community_requests_total",
        "Number of requests (including retries) to the object store, by community and action.",
        "storage",
        &[],
        ["community", "action"],
    ),
    bytes_total: new_counter_vec(
        "object_storage_community_bytes_total",
        "Number of bytes exchanged with the object store, by community and direction.",
        "storage",
        &[],
        ["community", "direction"],
    ),
    peak_requests_per_sec: new_gauge_vec(
        "object_storage_community_peak_requests_per_sec",
        "Max number of requests to the object store within one second over the last 30 seconds, \
         by community and action.",
        "storage",
        &[],
        ["community", "action"],
    ),
    peak_bytes_per_sec: new_gauge_vec(
        "object_storage_community_peak_bytes_per_sec",
        "Max number of bytes exchanged with the object store within one second over the last 30 \
         seconds, by community and direction.",
        "storage",
        &[],
        ["community", "direction"],
    ),
});

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Kind {
    Requests(&'static str),
    Bytes(&'static str),
}

struct Slot {
    current_sec: u64,
    window: VecDeque<u64>,
    total_counter: IntCounter,
    peak_gauge: IntGauge,
}

impl Slot {
    fn new(community: &str, kind: Kind) -> Self {
        let (total_counter, peak_gauge) = match kind {
            Kind::Requests(action) => (
                COMMUNITY_METRICS
                    .requests_total
                    .with_label_values([community, action]),
                COMMUNITY_METRICS
                    .peak_requests_per_sec
                    .with_label_values([community, action]),
            ),
            Kind::Bytes(direction) => (
                COMMUNITY_METRICS
                    .bytes_total
                    .with_label_values([community, direction]),
                COMMUNITY_METRICS
                    .peak_bytes_per_sec
                    .with_label_values([community, direction]),
            ),
        };
        Slot {
            current_sec: 0,
            window: VecDeque::with_capacity(WINDOW_SECS + 1),
            total_counter,
            peak_gauge,
        }
    }
}

#[derive(Default)]
struct Slots {
    per_community: HashMap<String, HashMap<Kind, Slot>>,
}

impl Slots {
    fn record(&mut self, community: &str, kind: Kind, value: u64) {
        let community_slots = match self.per_community.get_mut(community) {
            Some(community_slots) => community_slots,
            None => self.per_community.entry(community.to_string()).or_default(),
        };
        let slot = community_slots
            .entry(kind)
            .or_insert_with(|| Slot::new(community, kind));
        slot.current_sec += value;
        slot.total_counter.inc_by(value);
    }

    /// Closes the current one-second bucket of every slot and refreshes the peak gauges.
    fn tick(&mut self) {
        for slot in self
            .per_community
            .values_mut()
            .flat_map(HashMap::values_mut)
        {
            slot.window.push_back(slot.current_sec);
            if slot.window.len() > WINDOW_SECS {
                slot.window.pop_front();
            }
            slot.current_sec = 0;
            let peak = slot.window.iter().copied().max().unwrap_or(0);
            slot.peak_gauge.set(peak as i64);
        }
    }
}

static SLOTS: Lazy<Mutex<Slots>> = Lazy::new(|| {
    std::thread::Builder::new()
        .name("community-metrics".to_string())
        .spawn(|| {
            let mut next_tick = Instant::now();
            loop {
                next_tick += Duration::from_secs(1);
                std::thread::sleep(next_tick.saturating_duration_since(Instant::now()));
                SLOTS.lock().unwrap().tick();
            }
        })
        .expect("failed to spawn community metrics thread");
    Mutex::new(Slots::default())
});

/// Extracts the community UUID from an object key such as
/// `events-<community_uuid>/<split_id>.split`.
fn extract_community(key: &str) -> &str {
    let Some(parent) = key.rsplit('/').nth(1) else {
        return UNKNOWN_COMMUNITY;
    };
    let Some(community) = parent
        .len()
        .checked_sub(36)
        .and_then(|start| parent.get(start..))
    else {
        return UNKNOWN_COMMUNITY;
    };
    let is_uuid = community.bytes().enumerate().all(|(i, b)| match i {
        8 | 13 | 18 | 23 => b == b'-',
        _ => b.is_ascii_hexdigit(),
    });
    if is_uuid {
        community
    } else {
        UNKNOWN_COMMUNITY
    }
}

pub(crate) fn record_request(key: &str, action: ActionLabel) {
    let community = extract_community(key);
    SLOTS
        .lock()
        .unwrap()
        .record(community, Kind::Requests(action.as_str()), 1);
}

pub(crate) fn record_bytes(key: &str, direction: &'static str, num_bytes: u64) {
    let community = extract_community(key);
    SLOTS
        .lock()
        .unwrap()
        .record(community, Kind::Bytes(direction), num_bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_community() {
        assert_eq!(
            extract_community(
                "events-49cca94b-5ad3-4857-b644-1348a1ab5ea6/01M1PGRCR6JSW021GKK3Z4V5BW.split"
            ),
            "49cca94b-5ad3-4857-b644-1348a1ab5ea6"
        );
        assert_eq!(
            extract_community(
                "indexes/events-49cca94b-5ad3-4857-b644-1348a1ab5ea6/01M1PGRCR6JSW021GKK3Z4V5BW.\
                 split"
            ),
            "49cca94b-5ad3-4857-b644-1348a1ab5ea6"
        );
        assert_eq!(extract_community("metastore.json"), UNKNOWN_COMMUNITY);
        assert_eq!(
            extract_community("my-index/01M1PGRCR6JSW021GKK3Z4V5BW.split"),
            UNKNOWN_COMMUNITY
        );
        assert_eq!(
            extract_community("events-49cca94b_5ad3-4857-b644-1348a1ab5ea6/split"),
            UNKNOWN_COMMUNITY
        );
        assert_eq!(extract_community("é/split"), UNKNOWN_COMMUNITY);
    }

    #[test]
    fn test_slots_peak() {
        let community = "test-community-peak";
        let kind = Kind::Requests("get_object");
        let mut slots = Slots::default();
        for _ in 0..5 {
            slots.record(community, kind, 1);
        }
        slots.tick();
        let slot = &slots.per_community[community][&kind];
        assert_eq!(slot.peak_gauge.get(), 5);
        assert_eq!(slot.total_counter.get(), 5);

        slots.record(community, kind, 2);
        slots.tick();
        assert_eq!(slots.per_community[community][&kind].peak_gauge.get(), 5);

        for _ in 0..WINDOW_SECS - 1 {
            slots.tick();
        }
        // The 5 has left the window, the 2 is still in it.
        assert_eq!(slots.per_community[community][&kind].peak_gauge.get(), 2);
        slots.tick();
        assert_eq!(slots.per_community[community][&kind].peak_gauge.get(), 0);
        assert_eq!(slots.per_community[community][&kind].total_counter.get(), 7);
    }
}
