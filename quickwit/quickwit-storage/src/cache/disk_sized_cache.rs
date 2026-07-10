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
use std::hash::Hasher;
use std::io;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use fnv::FnvHasher;
use lru::LruCache;
use tracing::{info, warn};

use crate::OwnedBytes;
use crate::metrics::CacheMetrics;

/// Substring used to mark files that are being written.
const TEMP_MARKER: &str = ".tmp";

/// Global counter used to build unique temporary file names.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct DiskCacheIndex {
    /// Maps the on-disk file name to the size of its content.
    /// The LRU order tracks the recency of accesses for eviction.
    lru_cache: LruCache<String, u64>,
    num_bytes: u64,
    capacity_in_bytes: u64,
    cache_counters: &'static CacheMetrics,
}

impl DiskCacheIndex {
    fn record_item(&mut self, num_bytes: u64) {
        self.num_bytes += num_bytes;
        self.cache_counters.in_cache_count.inc();
        self.cache_counters.in_cache_num_bytes.add(num_bytes as i64);
    }

    fn drop_item(&mut self, num_bytes: u64) {
        self.num_bytes -= num_bytes;
        self.cache_counters.in_cache_count.dec();
        self.cache_counters.in_cache_num_bytes.sub(num_bytes as i64);
        self.cache_counters.evict_num_items.inc();
        self.cache_counters.evict_num_bytes.inc_by(num_bytes);
    }

    /// Evicts the least recently used entries until `incoming` extra bytes would fit
    /// under the capacity. Returns the file names that must be deleted from disk.
    fn evict_to_fit(&mut self, incoming: u64) -> Vec<String> {
        let mut victims = Vec::new();
        while self.num_bytes + incoming > self.capacity_in_bytes {
            if let Some((file_name, num_bytes)) = self.lru_cache.pop_lru() {
                self.drop_item(num_bytes);
                victims.push(file_name);
            } else {
                break;
            }
        }
        victims
    }
}

impl Drop for DiskCacheIndex {
    fn drop(&mut self) {
        // We don't count this toward evicted entries, as we are clearing the whole index.
        self.cache_counters
            .in_cache_count
            .sub(self.lru_cache.len() as i64);
        self.cache_counters
            .in_cache_num_bytes
            .sub(self.num_bytes as i64);
    }
}

/// A size-bounded cache that persists its entries on disk.
///
/// This is the on-disk counterpart to [`MemorySizedCache`](super::MemorySizedCache): entries
/// are evicted following an LRU policy once the configured capacity (in bytes) is exceeded.
///
/// Each entry is stored as a single file named after `key.to_string()`. Keys must therefore
/// produce filesystem-safe, collision-free strings.
pub struct DiskSizedCache<K = String> {
    root_path: PathBuf,
    index: Mutex<DiskCacheIndex>,
    _phantom: PhantomData<fn() -> K>,
}

impl<K: Display> DiskSizedCache<K> {
    /// Opens a disk cache rooted at `root_path`.
    pub async fn open(
        root_path: PathBuf,
        capacity_in_bytes: u64,
        cache_counters: &'static CacheMetrics,
    ) -> io::Result<Self>
    where
        K: 'static,
    {
        tokio::task::spawn_blocking(move || {
            Self::open_blocking(root_path, capacity_in_bytes, cache_counters)
        })
        .await
        .map_err(io::Error::other)?
    }

    fn open_blocking(
        root_path: PathBuf,
        capacity_in_bytes: u64,
        cache_counters: &'static CacheMetrics,
    ) -> io::Result<Self> {
        let start = Instant::now();
        std::fs::create_dir_all(&root_path)?;

        let mut entries: Vec<(String, u64, SystemTime)> = Vec::new();
        for shard_entry_res in std::fs::read_dir(&root_path)? {
            let shard_entry = shard_entry_res?;
            // Entries live inside shard sub-directories; only recurse into those.
            match shard_entry.file_type() {
                Ok(file_type) if file_type.is_dir() => {}
                _ => continue,
            }
            let Ok(shard_dir_iter) = std::fs::read_dir(shard_entry.path()) else {
                continue;
            };

            for dir_entry_res in shard_dir_iter {
                let Ok(dir_entry) = dir_entry_res else {
                    continue;
                };

                if let Ok(file_type) = dir_entry.file_type()
                    && !file_type.is_file()
                {
                    continue;
                }
                let Ok(file_name) = dir_entry.file_name().into_string() else {
                    continue;
                };

                if file_name.contains(TEMP_MARKER) {
                    // Leftover temporary file from an interrupted write: clean it up.
                    let _ = std::fs::remove_file(dir_entry.path());
                    continue;
                }
                let Ok(metadata) = dir_entry.metadata() else {
                    continue;
                };
                if !metadata.is_file() {
                    continue;
                }
                let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                entries.push((file_name, metadata.len(), modified));
            }
        }
        entries.sort_by_key(|(_, _, modified)| *modified);

        let mut index = DiskCacheIndex {
            lru_cache: LruCache::unbounded(),
            num_bytes: 0,
            capacity_in_bytes,
            cache_counters,
        };
        for (file_name, num_bytes, _) in entries {
            index.record_item(num_bytes);
            index.lru_cache.put(file_name, num_bytes);
        }
        let victims = index.evict_to_fit(0);

        let cache = DiskSizedCache {
            root_path,
            index: Mutex::new(index),
            _phantom: PhantomData,
        };
        remove_files(&cache.root_path, &victims);
        let num_entries = {
            let index = cache.index.lock().unwrap();
            index.lru_cache.len()
        };
        info!(
            root_path = %cache.root_path.display(),
            num_entries,
            num_evicted = victims.len(),
            elapsed_millis = start.elapsed().as_millis(),
            "opened disk cache"
        );
        Ok(cache)
    }

    /// Returns the cached payload for the given key, if present on disk.
    pub async fn get(&self, key: &K) -> Option<OwnedBytes> {
        let file_name = key.to_string();
        {
            let mut index = self.index.lock().unwrap();
            // `LruCache::get` refreshes the recency of the entry.
            if index.lru_cache.get(&file_name).is_none() {
                index.cache_counters.misses_num_items.inc();
                return None;
            }
        }
        // Offload the blocking read so we don't stall the async runtime worker.
        let path = path_for(&self.root_path, &file_name);
        let read_res = tokio::task::spawn_blocking(move || std::fs::read(path)).await;
        match read_res {
            Ok(Ok(buffer)) => {
                let index = self.index.lock().unwrap();
                index.cache_counters.hits_num_items.inc();
                index
                    .cache_counters
                    .hits_num_bytes
                    .inc_by(buffer.len() as u64);
                Some(OwnedBytes::new(buffer))
            }
            Ok(Err(_)) => {
                // The file vanished (e.g. concurrent eviction or manual deletion): drop the
                // stale index entry and report a miss.
                let mut index = self.index.lock().unwrap();
                if let Some(num_bytes) = index.lru_cache.pop(&file_name) {
                    index.num_bytes -= num_bytes;
                    index.cache_counters.in_cache_count.dec();
                    index
                        .cache_counters
                        .in_cache_num_bytes
                        .sub(num_bytes as i64);
                }
                index.cache_counters.misses_num_items.inc();
                None
            }
            Err(_join_error) => {
                // The blocking read task failed unexpectedly. Keep the index entry (the file is
                // likely still valid) and just report a miss.
                let index = self.index.lock().unwrap();
                index.cache_counters.misses_num_items.inc();
                None
            }
        }
    }

    /// Stores the given payload on disk under the given key.
    ///
    /// This silently does nothing if the payload is larger than the whole cache capacity. If an
    /// entry already exists for the key, it is kept as-is (payloads are assumed immutable) and its
    /// recency is refreshed.
    pub async fn put(&self, key: K, bytes: OwnedBytes) {
        let num_bytes = bytes.len() as u64;
        let file_name = key.to_string();
        {
            let mut index = self.index.lock().unwrap();
            if index.capacity_in_bytes < num_bytes {
                if index.capacity_in_bytes != 0 {
                    warn!(
                        capacity_in_bytes = index.capacity_in_bytes,
                        len = num_bytes,
                        "payload larger than the disk cache capacity, not caching it on disk"
                    );
                }
                return;
            }
            if index.lru_cache.get(&file_name).is_some() {
                // Already cached: payloads are immutable, just keep the refreshed recency.
                return;
            }
        }

        // We write the new file *before* evicting on purpose: if the write fails we return early
        // without having deleted any valid entry, and the write stays outside the index lock. The
        // tradeoff is that on-disk usage may transiently exceed the capacity by roughly one entry.
        // The blocking write is offloaded so we don't stall the async runtime worker.
        let write_res = {
            let root_path = self.root_path.clone();
            let write_name = file_name.clone();
            tokio::task::spawn_blocking(move || write_file(&root_path, &write_name, &bytes)).await
        };
        match write_res {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%error, file_name, "failed to persist entry to disk cache");
                return;
            }
            Err(_join_error) => return,
        }

        let victims = {
            let mut index = self.index.lock().unwrap();
            if index.lru_cache.get(&file_name).is_some() {
                // Lost a race with a concurrent put of the same (immutable) payload.
                return;
            }
            let victims = index.evict_to_fit(num_bytes);
            index.record_item(num_bytes);
            index.lru_cache.put(file_name, num_bytes);
            victims
        };
        if !victims.is_empty() {
            let root_path = self.root_path.clone();
            let _ = tokio::task::spawn_blocking(move || remove_files(&root_path, &victims)).await;
        }
    }
}

/// Returns the shard sub-directory name a file belongs to.
fn shard_dir(file_name: &str) -> String {
    // Number of shard sub-directories files are spread across.
    const NUM_SHARDS: u64 = 256;

    let mut hasher = FnvHasher::default();
    hasher.write(file_name.as_bytes());
    format!("{:02x}", hasher.finish() % NUM_SHARDS)
}

/// Returns the full on-disk path of an entry, including its shard sub-directory.
pub(crate) fn path_for(root_path: &Path, file_name: &str) -> PathBuf {
    root_path.join(shard_dir(file_name)).join(file_name)
}

fn write_file(root_path: &Path, file_name: &str, bytes: &[u8]) -> io::Result<()> {
    let shard_path = root_path.join(shard_dir(file_name));
    std::fs::create_dir_all(&shard_path)?;
    // Rely on a counter to guarantee uniqueness of the temporary file name.
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = shard_path.join(format!("{file_name}{TEMP_MARKER}{counter}"));
    std::fs::write(&tmp_path, bytes)?;
    std::fs::rename(&tmp_path, shard_path.join(file_name))
}

fn remove_files(root_path: &Path, file_names: &[String]) {
    for file_name in file_names {
        let _ = std::fs::remove_file(path_for(root_path, file_name));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::CACHE_METRICS_FOR_TESTS;

    async fn open_cache(root_path: PathBuf, capacity_in_bytes: u64) -> DiskSizedCache<String> {
        DiskSizedCache::open(root_path, capacity_in_bytes, &CACHE_METRICS_FOR_TESTS)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_put_get() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cache = open_cache(tmp_dir.path().to_path_buf(), 1_000).await;
        assert!(cache.get(&"missing".to_string()).await.is_none());

        cache
            .put("a".to_string(), OwnedBytes::new(&b"hello"[..]))
            .await;
        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"hello"[..]);
        // A file should have been written on disk, inside its shard sub-directory.
        assert!(path_for(tmp_dir.path(), "a").try_exists().unwrap());
    }

    #[tokio::test]
    async fn test_persists_across_reopen() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let cache = open_cache(tmp_dir.path().to_path_buf(), 1_000).await;
            cache
                .put("a".to_string(), OwnedBytes::new(&b"hello"[..]))
                .await;
            cache
                .put("b".to_string(), OwnedBytes::new(&b"world"[..]))
                .await;
        }
        // Re-opening the cache should recover the previously stored entries.
        let cache = open_cache(tmp_dir.path().to_path_buf(), 1_000).await;
        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"hello"[..]);
        assert_eq!(cache.get(&"b".to_string()).await.unwrap(), &b"world"[..]);
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cache = open_cache(tmp_dir.path().to_path_buf(), 6).await;
        cache
            .put("a".to_string(), OwnedBytes::new(&b"aaa"[..]))
            .await;
        cache
            .put("b".to_string(), OwnedBytes::new(&b"bbb"[..]))
            .await;
        // Access "a" so that "b" becomes the least recently used entry.
        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"aaa"[..]);
        // Inserting a third entry must evict "b".
        cache
            .put("c".to_string(), OwnedBytes::new(&b"ccc"[..]))
            .await;

        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"aaa"[..]);
        assert!(cache.get(&"b".to_string()).await.is_none());
        assert_eq!(cache.get(&"c".to_string()).await.unwrap(), &b"ccc"[..]);
        // The evicted entry's file must be gone.
        assert!(!path_for(tmp_dir.path(), "b").try_exists().unwrap());
    }

    #[tokio::test]
    async fn test_payload_larger_than_capacity_is_ignored() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cache = open_cache(tmp_dir.path().to_path_buf(), 3).await;
        cache
            .put("big".to_string(), OwnedBytes::new(&b"toolarge"[..]))
            .await;
        assert!(cache.get(&"big".to_string()).await.is_none());
        assert!(!path_for(tmp_dir.path(), "big").try_exists().unwrap());
    }

    #[tokio::test]
    async fn test_put_same_key_twice_keeps_single_entry() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cache = open_cache(tmp_dir.path().to_path_buf(), 6).await;
        cache
            .put("a".to_string(), OwnedBytes::new(&b"aaa"[..]))
            .await;
        // Immutable payload: putting the same key again is a no-op and must not evict others.
        cache
            .put("a".to_string(), OwnedBytes::new(&b"aaa"[..]))
            .await;
        cache
            .put("b".to_string(), OwnedBytes::new(&b"bbb"[..]))
            .await;
        assert_eq!(cache.get(&"a".to_string()).await.unwrap(), &b"aaa"[..]);
        assert_eq!(cache.get(&"b".to_string()).await.unwrap(), &b"bbb"[..]);
    }

    #[tokio::test]
    async fn test_get_after_manual_file_deletion() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cache = open_cache(tmp_dir.path().to_path_buf(), 1_000).await;
        cache
            .put("a".to_string(), OwnedBytes::new(&b"hello"[..]))
            .await;
        std::fs::remove_file(path_for(tmp_dir.path(), "a")).unwrap();
        // The stale entry should be detected and reported as a miss.
        assert!(cache.get(&"a".to_string()).await.is_none());
    }

    #[tokio::test]
    async fn test_open_evicts_when_over_capacity() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let cache = open_cache(tmp_dir.path().to_path_buf(), 1_000).await;
            cache
                .put("a".to_string(), OwnedBytes::new(&b"aaa"[..]))
                .await;
            cache
                .put("b".to_string(), OwnedBytes::new(&b"bbb"[..]))
                .await;
        }
        // Re-open with a capacity that can only hold one of the two entries.
        let cache = open_cache(tmp_dir.path().to_path_buf(), 3).await;
        let a = cache.get(&"a".to_string()).await;
        let b = cache.get(&"b".to_string()).await;
        // Exactly one entry should have survived.
        assert_ne!(a.is_some(), b.is_some());
    }

    #[tokio::test]
    async fn test_open_cleans_up_temp_files() {
        let tmp_dir = tempfile::tempdir().unwrap();
        // A leftover temp file lives inside the shard directory of the entry it belongs to.
        let shard_path = tmp_dir.path().join(shard_dir("a"));
        std::fs::create_dir_all(&shard_path).unwrap();
        let leftover = shard_path.join("a.tmp42");
        std::fs::write(&leftover, b"partial").unwrap();
        let cache = open_cache(tmp_dir.path().to_path_buf(), 1_000).await;
        assert!(!leftover.try_exists().unwrap());
        assert!(cache.get(&"a".to_string()).await.is_none());
    }

    #[tokio::test]
    async fn test_entries_are_sharded_and_survive_reopen() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cache = open_cache(tmp_dir.path().to_path_buf(), 100_000).await;
        // Insert enough entries that at least two distinct shard directories get used.
        for i in 0..64 {
            let key = format!("key-{i}");
            cache.put(key, OwnedBytes::new(&b"payload"[..])).await;
        }
        // Files must be nested under shard sub-directories, not directly in the root.
        let mut shard_dirs = 0;
        for entry in std::fs::read_dir(tmp_dir.path()).unwrap() {
            let entry = entry.unwrap();
            assert!(
                entry.file_type().unwrap().is_dir(),
                "root should only contain shard directories"
            );
            shard_dirs += 1;
        }
        assert!(
            shard_dirs > 1,
            "entries should be spread over several shards"
        );

        // Every entry is still retrievable, including after a reopen (which walks the shards).
        let cache = open_cache(tmp_dir.path().to_path_buf(), 100_000).await;
        for i in 0..64 {
            let key = format!("key-{i}");
            assert_eq!(cache.get(&key).await.unwrap(), &b"payload"[..]);
        }
    }
}
