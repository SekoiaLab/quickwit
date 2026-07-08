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

use std::fmt::Display;
use std::hash::Hash;

use crate::OwnedBytes;
use crate::cache::disk_sized_cache::DiskSizedCache;
use crate::cache::memory_sized_cache::MemorySizedCache;

/// A two-tier, size-bounded cache combining a fast in-memory tier (L1) with an optional
/// disk-backed tier (L2).
///
/// Lookups check memory first, then disk; a disk hit is promoted back into memory. Writes
/// populate both tiers. When no disk tier is configured this behaves exactly like the
/// underlying [`MemorySizedCache`], which makes it a drop-in, opt-in replacement.
pub struct TieredSizedCache<K: Hash + Eq = String> {
    memory: MemorySizedCache<K>,
    disk: Option<DiskSizedCache<K>>,
}

impl<K: Hash + Eq + Clone + Display> TieredSizedCache<K> {
    /// Creates a tiered cache from an in-memory tier and an optional disk tier.
    pub fn new(memory: MemorySizedCache<K>, disk: Option<DiskSizedCache<K>>) -> Self {
        TieredSizedCache { memory, disk }
    }

    /// Returns the cached payload for the given key, checking memory first, then disk.
    ///
    /// A disk hit is promoted into the memory tier before being returned.
    pub fn get(&self, key: &K) -> Option<OwnedBytes> {
        if let Some(bytes) = self.memory.get(key) {
            return Some(bytes);
        }
        let bytes = self.disk.as_ref()?.get(key)?;
        self.memory.put(key.clone(), bytes.clone());
        Some(bytes)
    }

    /// Stores the given payload in both the memory tier and, if configured, the disk tier.
    pub fn put(&self, key: K, bytes: OwnedBytes) {
        if let Some(disk) = &self.disk {
            disk.put(key.clone(), bytes.clone());
        }
        self.memory.put(key, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::CACHE_METRICS_FOR_TESTS;

    fn memory_cache() -> MemorySizedCache<String> {
        MemorySizedCache::with_capacity_in_bytes(1_000, &CACHE_METRICS_FOR_TESTS)
    }

    #[test]
    fn test_memory_only() {
        let cache = TieredSizedCache::new(memory_cache(), None);
        assert!(cache.get(&"a".to_string()).is_none());
        cache.put("a".to_string(), OwnedBytes::new(&b"hello"[..]));
        assert_eq!(cache.get(&"a".to_string()).unwrap(), &b"hello"[..]);
    }

    #[test]
    fn test_put_writes_to_both_tiers() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let disk = DiskSizedCache::open(
            tmp_dir.path().to_path_buf(),
            1_000,
            &CACHE_METRICS_FOR_TESTS,
        )
        .unwrap();
        let cache = TieredSizedCache::new(memory_cache(), Some(disk));
        cache.put("a".to_string(), OwnedBytes::new(&b"hello"[..]));
        assert_eq!(cache.get(&"a".to_string()).unwrap(), &b"hello"[..]);
        // The payload must have been persisted on disk as well.
        assert!(tmp_dir.path().join("a").exists());
    }

    #[test]
    fn test_disk_hit_is_promoted_to_memory() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let disk = DiskSizedCache::open(
                tmp_dir.path().to_path_buf(),
                1_000,
                &CACHE_METRICS_FOR_TESTS,
            )
            .unwrap();
            let cache = TieredSizedCache::new(memory_cache(), Some(disk));
            cache.put("a".to_string(), OwnedBytes::new(&b"hello"[..]));
        }
        // Simulate a fresh process: only the disk tier still holds the data.
        let disk = DiskSizedCache::open(
            tmp_dir.path().to_path_buf(),
            1_000,
            &CACHE_METRICS_FOR_TESTS,
        )
        .unwrap();
        let memory = memory_cache();
        let cache = TieredSizedCache::new(memory, Some(disk));

        assert_eq!(cache.get(&"a".to_string()).unwrap(), &b"hello"[..]);
        // After a disk hit, deleting the file must not lose the value: it lives in memory now.
        std::fs::remove_file(tmp_dir.path().join("a")).unwrap();
        assert_eq!(cache.get(&"a".to_string()).unwrap(), &b"hello"[..]);
    }
}
