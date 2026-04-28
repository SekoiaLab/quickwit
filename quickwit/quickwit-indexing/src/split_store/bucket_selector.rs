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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use quickwit_common::uri::Uri;

/// Selects which storage bucket to write the next split to.
///
/// Implementations own the list of available URIs and encapsulate the
/// selection algorithm. This keeps the `IndexingSplitStore` simple — it
/// just calls `select()` and writes to the returned URI.
pub trait BucketSelector: Send + Sync + 'static {
    /// Returns the storage URI to use for writing the next split.
    fn select(&self) -> &Uri;
}

/// Round-robin bucket selector that cycles through the configured URIs.
pub struct RoundRobinBucketSelector {
    uris: Vec<Uri>,
    counter: AtomicU64,
}

impl RoundRobinBucketSelector {
    /// Creates a new round-robin selector.
    ///
    /// # Panics
    ///
    /// Panics if `uris` is empty.
    pub fn new(uris: Vec<Uri>) -> Self {
        assert!(!uris.is_empty(), "BucketSelector requires at least one URI");
        Self {
            uris,
            counter: AtomicU64::new(0),
        }
    }
}

impl BucketSelector for RoundRobinBucketSelector {
    fn select(&self) -> &Uri {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) as usize % self.uris.len();
        &self.uris[idx]
    }
}

/// Creates the default bucket selector (round-robin) from a list of URIs.
pub fn default_bucket_selector(uris: Vec<Uri>) -> Arc<dyn BucketSelector> {
    Arc::new(RoundRobinBucketSelector::new(uris))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_single_uri() {
        let uri = Uri::for_test("s3://bucket-a/index");
        let selector = RoundRobinBucketSelector::new(vec![uri.clone()]);
        for _ in 0..10 {
            assert_eq!(selector.select(), &uri);
        }
    }

    #[test]
    fn test_round_robin_multiple_uris() {
        let uri_a = Uri::for_test("s3://bucket-a/index");
        let uri_b = Uri::for_test("s3://bucket-b/index");
        let uri_c = Uri::for_test("s3://bucket-c/index");
        let selector =
            RoundRobinBucketSelector::new(vec![uri_a.clone(), uri_b.clone(), uri_c.clone()]);

        assert_eq!(selector.select(), &uri_a);
        assert_eq!(selector.select(), &uri_b);
        assert_eq!(selector.select(), &uri_c);
        assert_eq!(selector.select(), &uri_a);
        assert_eq!(selector.select(), &uri_b);
        assert_eq!(selector.select(), &uri_c);
    }

    #[test]
    #[should_panic(expected = "BucketSelector requires at least one URI")]
    fn test_round_robin_empty_uris_panics() {
        RoundRobinBucketSelector::new(Vec::new());
    }

    #[test]
    fn test_default_bucket_selector() {
        let uri = Uri::for_test("s3://bucket-a/index");
        let selector = default_bucket_selector(vec![uri.clone()]);
        assert_eq!(selector.select(), &uri);
    }
}
