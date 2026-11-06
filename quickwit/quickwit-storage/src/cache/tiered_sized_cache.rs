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
pub struct TieredSizedCache<K: Hash + Eq + Send + Sync + 'static = String> {
    memory: MemorySizedCache<K>,
    disk: Option<DiskSizedCache<K>>,
}

impl<K: Hash + Eq + Clone + Display + Send + Sync + 'static> TieredSizedCache<K> {
    /// Creates a tiered cache from an in-memory tier and an optional disk tier.
    pub fn new(memory: MemorySizedCache<K>, disk: Option<DiskSizedCache<K>>) -> Self {
        TieredSizedCache { memory, disk }
    }

    /// Returns the cached payload for the given key, checking memory first, then disk.
    ///
    /// A disk hit is promoted into the memory tier before being returned. Only the disk tier
    /// performs (off-thread) I/O, so an in-memory hit stays fully synchronous and cheap.
    pub async fn get(&self, key: &K) -> Option<OwnedBytes> {
        if let Some(bytes) = self.memory.get(key) {
            // Propagate the access down to keep L2 in sync.
            if let Some(disk) = &self.disk {
                disk.touch(key);
            }
            return Some(bytes);
        }
        let bytes = self.disk.as_ref()?.get(key).await?;
        self.memory.put(key.clone(), bytes.clone());
        Some(bytes)
    }

    /// Stores the given payload in both the memory tier and, if configured, the disk tier.
    pub async fn put(&self, key: K, bytes: OwnedBytes) {
        // Populate L1 first so callers benefit immediately even if the disk tier is slow.
        self.memory.put(key.clone(), bytes.clone());
        if let Some(disk) = &self.disk {
            disk.put(key, bytes).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use bytesize::ByteSize;

    use super::*;
    use crate::cache::disk_sized_cache::path_for;
    use crate::metrics::CACHE_METRICS_FOR_TESTS;

    fn memory_cache() -> MemorySizedCache<String> {
        MemorySizedCache::from_config(&ByteSize::b(1_000).into(), &CACHE_METRICS_FOR_TESTS)
    }

    async fn open_disk(root_path: PathBuf, capacity_in_bytes: u64) -> DiskSizedCache<String> {
        // Zero debounce so accesses deterministically refresh the on-disk recency in tests.
        DiskSizedCache::open_with_interval(
            root_path,
            capacity_in_bytes,
            Duration::ZERO,
            &CACHE_METRICS_FOR_TESTS,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_memory_only() {
        let cache = TieredSizedCache::new(memory_cache(), None);
        assert!(cache.get(&"a".to_string()).await.is_none());
        cache
            .put("a".to_string(), OwnedBytes::new(&b"hello"[..]))
            .await;
        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"hello"[..]);
    }

    #[tokio::test]
    async fn test_put_writes_to_both_tiers() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let disk = DiskSizedCache::open(
            tmp_dir.path().to_path_buf(),
            1_000,
            &CACHE_METRICS_FOR_TESTS,
        )
        .await
        .unwrap();
        let cache = TieredSizedCache::new(memory_cache(), Some(disk));
        cache
            .put("a".to_string(), OwnedBytes::new(&b"hello"[..]))
            .await;
        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"hello"[..]);
        // The payload must have been persisted on disk as well.
        assert!(path_for(tmp_dir.path(), "a").try_exists().unwrap());
    }

    #[tokio::test]
    async fn test_disk_hit_is_promoted_to_memory() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let disk = DiskSizedCache::open(
                tmp_dir.path().to_path_buf(),
                1_000,
                &CACHE_METRICS_FOR_TESTS,
            )
            .await
            .unwrap();
            let cache = TieredSizedCache::new(memory_cache(), Some(disk));
            cache
                .put("a".to_string(), OwnedBytes::new(&b"hello"[..]))
                .await;
        }
        // Simulate a fresh process: only the disk tier still holds the data.
        let disk = DiskSizedCache::open(
            tmp_dir.path().to_path_buf(),
            1_000,
            &CACHE_METRICS_FOR_TESTS,
        )
        .await
        .unwrap();
        let memory = memory_cache();
        let cache = TieredSizedCache::new(memory, Some(disk));

        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"hello"[..]);
        // After a disk hit, deleting the file must not lose the value: it lives in memory now.
        std::fs::remove_file(path_for(tmp_dir.path(), "a")).unwrap();
        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"hello"[..]);
    }

    #[tokio::test]
    async fn test_memory_hit_refreshes_disk_recency_across_reopen() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let disk = open_disk(tmp_dir.path().to_path_buf(), 6).await;
            let cache = TieredSizedCache::new(memory_cache(), Some(disk));
            // Both entries land in memory (L1) and on disk (L2). By first-write time "a" is older.
            cache
                .put("a".to_string(), OwnedBytes::new(&b"aaa"[..]))
                .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            cache
                .put("b".to_string(), OwnedBytes::new(&b"bbb"[..]))
                .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            // "a" is served purely from the in-memory tier (it never re-reads disk), yet this must
            // still refresh its disk recency so it outranks the more recently written "b".
            assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"aaa"[..]);
            // The propagated mtime refresh happens off-thread; let it land before reopening.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Reopen the disk tier alone (simulating a restart) with room for a single entry: the
        // memory-hot "a" must have survived over "b", which a naive write-time ordering would
        // evict.
        let disk = open_disk(tmp_dir.path().to_path_buf(), 3).await;
        assert!(disk.get(&"a".to_string()).await.is_some());
        assert!(disk.get(&"b".to_string()).await.is_none());
    }
}
