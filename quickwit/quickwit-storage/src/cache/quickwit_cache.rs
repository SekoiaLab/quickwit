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

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_config::CacheConfig;

use crate::OwnedBytes;
use crate::cache::{MemorySizedCache, StorageCache};
use crate::metrics::ComponentCacheMetrics;

const FULL_SLICE: Range<usize> = 0..usize::MAX;

/// Quickwit storage cache with a size limit.
///
/// Every route currently shares a single underlying cache instance, so the components listed
/// in [`QuickwitCache::new`] compete for one budget.
pub struct QuickwitCache {
    router: Vec<(&'static str, Arc<dyn StorageCache>)>,
}

impl From<Vec<(&'static str, Arc<dyn StorageCache>)>> for QuickwitCache {
    fn from(router: Vec<(&'static str, Arc<dyn StorageCache>)>) -> Self {
        QuickwitCache { router }
    }
}

impl QuickwitCache {
    /// Creates a [`QuickwitCache`] built from `cache_config`, shared by all the routed
    /// tantivy components.
    ///
    /// All routes point at the same underlying cache, so a single eviction policy arbitrates
    /// between components instead of each one getting a fixed budget. Each route is wrapped in
    /// a [`MeteredRoute`] so the shared pool's behaviour stays attributable per component.
    pub fn new(cache_config: &CacheConfig) -> Self {
        let mut quickwit_cache = QuickwitCache::empty();
        let cache: Arc<dyn StorageCache> = Arc::new(SimpleCache::from_config(
            cache_config,
            &crate::STORAGE_METRICS.fast_field_cache,
        ));
        for suffix in [".fast", ".term", ".idx", ".pos", ".fieldnorm"] {
            quickwit_cache.add_route(
                suffix,
                Arc::new(MeteredRoute {
                    suffix,
                    inner: cache.clone(),
                }),
            );
        }
        quickwit_cache
    }

    /// Empties cache.
    pub fn empty() -> QuickwitCache {
        QuickwitCache::from(Vec::new())
    }

    /// Adds a caching route defined by a path suffix. All elements with a path matching
    /// this suffix will be cached.
    pub fn add_route(&mut self, path_suffix: &'static str, route_cache: Arc<dyn StorageCache>) {
        self.router.push((path_suffix, route_cache));
    }

    fn get_relevant_cache(&self, path: &Path) -> Option<&dyn StorageCache> {
        for (suffix, cache) in &self.router {
            if path.to_string_lossy().ends_with(suffix) {
                return Some(cache.as_ref());
            }
        }
        None
    }
}

#[async_trait]
impl StorageCache for QuickwitCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        // We don't check for the presence of the entire file in the
        // cache.
        // That's voluntary to avoid messing with the cache miss counts.
        if let Some(cache) = self.get_relevant_cache(path) {
            return cache.get(path, byte_range).await;
        }
        None
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        if let Some(cache) = self.get_relevant_cache(path) {
            return cache.get_all(path).await;
        }
        None
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        if let Some(cache) = self.get_relevant_cache(&path) {
            cache.put(path, byte_range, bytes).await;
        }
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        if let Some(cache) = self.get_relevant_cache(&path) {
            cache.put(path, FULL_SLICE, bytes).await;
        }
    }
}

/// Records per-suffix hit/miss metrics around a shared underlying cache.
///
/// The shared pool reports a single aggregate hit rate under its own component name, which
/// cannot tell us whether a given component regressed or which one is winning the shared
/// space. These counters are recorded *in addition* to the pool's own metrics, so summing the
/// two would double count.
///
/// Puts are not metered: a put only ever follows a miss, which is already counted.
struct MeteredRoute {
    suffix: &'static str,
    inner: Arc<dyn StorageCache>,
}

impl MeteredRoute {
    fn record(&self, result: Option<&OwnedBytes>) {
        let metrics = &crate::STORAGE_METRICS;
        let outcome = if let Some(bytes) = result {
            metrics
                .cache_route_hit_bytes
                .with_label_values([self.suffix])
                .inc_by(bytes.len() as u64);
            "hit"
        } else {
            "miss"
        };
        metrics
            .cache_route_events
            .with_label_values([self.suffix, outcome])
            .inc();
    }
}

#[async_trait]
impl StorageCache for MeteredRoute {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let result = self.inner.get(path, byte_range).await;
        self.record(result.as_ref());
        result
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        let result = self.inner.get_all(path).await;
        self.record(result.as_ref());
        result
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        self.inner.put(path, byte_range, bytes).await;
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        self.inner.put_all(path, bytes).await;
    }
}

/// The Quickwit cache logic is very simple for the moment.
///
/// It stores slices of the routed files in a size-bounded cache.
///
/// HACK! We use `0..usize::MAX` to signify the "entire file".
/// TODO fixme
struct SimpleCache {
    slice_cache: MemorySizedCache,
}

impl SimpleCache {
    fn from_config(
        cache_config: &CacheConfig,
        cache_counters: &'static ComponentCacheMetrics,
    ) -> Self {
        SimpleCache {
            slice_cache: MemorySizedCache::from_config(cache_config, cache_counters),
        }
    }
}

#[async_trait]
impl StorageCache for SimpleCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        if let Some(bytes) = self.slice_cache.get_slice(path, byte_range) {
            return Some(bytes);
        }
        None
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        self.slice_cache.put_slice(path, byte_range, bytes);
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        self.slice_cache.get_slice(path, FULL_SLICE)
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        self.slice_cache.put_slice(path, FULL_SLICE.clone(), bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::QuickwitCache;
    use crate::cache::StorageCache;
    use crate::{MockStorageCache, OwnedBytes};

    #[tokio::test]
    async fn test_quickwit_cache_get_all() {
        let mock_cache_hotcache = MockStorageCache::default();
        let mut mock_cache_fast = MockStorageCache::default();
        mock_cache_fast
            .expect_get_all()
            .times(1)
            .withf(|path| path == Path::new("bubu/toto.fast"))
            .returning(|_| Some(OwnedBytes::new(&b"aaaa"[..])));
        let mut quickwit_cache = QuickwitCache::empty();
        quickwit_cache.add_route("hotcache", Arc::new(mock_cache_hotcache));
        quickwit_cache.add_route("fast", Arc::new(mock_cache_fast));
        quickwit_cache.get_all(Path::new("bubu/toto.fast")).await;
    }

    #[tokio::test]
    async fn test_quickwit_cache_get() {
        let mock_cache_hotcache = MockStorageCache::default();
        let mut mock_cache = MockStorageCache::default();
        mock_cache
            .expect_get()
            .times(1)
            .withf(|path, _| path == Path::new("bubu/toto.fast"))
            .returning(|_, _| Some(OwnedBytes::new(&b"aaaaa"[..])));
        let mut quickwit_cache = QuickwitCache::empty();
        quickwit_cache.add_route("hotcache", Arc::new(mock_cache_hotcache));
        quickwit_cache.add_route("fast", Arc::new(mock_cache));
        quickwit_cache.get(Path::new("bubu/toto.fast"), 5..10).await;
    }

    #[tokio::test]
    async fn test_quickwit_cache_priority() {
        let mut mock_cache_ast = MockStorageCache::default();
        mock_cache_ast
            .expect_get()
            .times(1)
            .withf(|path, _| path == Path::new("bubu/toto.fast"))
            .returning(|_, _| Some(OwnedBytes::new(&b"aaaaa"[..])));
        let mock_cache_fast = MockStorageCache::default();
        let mut quickwit_cache = QuickwitCache::empty();
        quickwit_cache.add_route("ast", Arc::new(mock_cache_ast));
        quickwit_cache.add_route("fast", Arc::new(mock_cache_fast));
        assert_eq!(
            quickwit_cache
                .get(Path::new("bubu/toto.fast"), 5..10)
                .await
                .unwrap(),
            &b"aaaaa"[..]
        );
    }
}
