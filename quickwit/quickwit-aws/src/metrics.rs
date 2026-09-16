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
use quickwit_common::metrics::{Histogram, IntCounterVec, new_counter_vec, new_histogram};

use crate::dns::DnsLookupOutcome;

/// Metrics for the DNS resolution performed when opening a connection to an object store.
///
/// They live in the `storage` subsystem, next to the other object storage metrics, because
/// that is the only thing this resolver serves. They are declared here rather than in
/// `quickwit-storage` because `quickwit-storage` depends on `quickwit-aws`, not the reverse.
pub struct DnsMetrics {
    pub lookups_total: IntCounterVec<1>,
    pub resolve_duration_seconds: Histogram,
}

impl Default for DnsMetrics {
    fn default() -> Self {
        DnsMetrics {
            lookups_total: new_counter_vec(
                "dns_lookups_total",
                "Number of DNS lookups performed when connecting to object storage, by outcome. \
                 `hit` was served from the cache without blocking, every other outcome either \
                 blocked the caller or is a background refresh.",
                "storage",
                &[],
                ["outcome"],
            ),
            resolve_duration_seconds: new_histogram(
                "dns_resolve_duration_seconds",
                "Time spent in a `getaddrinfo` call resolving an object storage endpoint. Only \
                 recorded for the lookups that are actually performed, not for the cache hits \
                 they spare.",
                "storage",
                // getaddrinfo against an in-cluster resolver is expected to land in the low
                // milliseconds; the upper buckets are there to catch a resolver in trouble.
                vec![
                    0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
                ],
            ),
        }
    }
}

impl DnsMetrics {
    pub(crate) fn record_lookup(&self, outcome: DnsLookupOutcome) {
        self.lookups_total
            .with_label_values([outcome.as_str()])
            .inc();
    }

    #[cfg(test)]
    pub(crate) fn lookup_count(&self, outcome: DnsLookupOutcome) -> u64 {
        self.lookups_total
            .with_label_values([outcome.as_str()])
            .get()
    }
}

pub static DNS_METRICS: Lazy<DnsMetrics> = Lazy::new(DnsMetrics::default);
