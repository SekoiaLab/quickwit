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
use std::{fmt, io};

use async_trait::async_trait;
use quickwit_storage::{OwnedBytes, Storage};
use tantivy::directory::FileHandle;
use tantivy::directory::error::OpenReadError;
use tantivy::{Directory, HasLen};
use tokio::runtime::Handle;

/// Read-only directory backed by a quickwit `Storage`, that also serves synchronous reads by
/// blocking on the storage's async calls.
///
/// [`StorageDirectory`](crate::StorageDirectory) only supports async reads, which is what search
/// needs: everything is warmed up beforehand. Code that cannot warm up, like tantivy's
/// `space_usage`, needs synchronous reads instead.
///
/// Synchronous reads block the calling thread on the given runtime: they must only happen on a
/// thread where blocking is allowed, e.g. inside `tokio::task::spawn_blocking`. Calling them from
/// an async task panics.
#[derive(Clone)]
pub struct BlockingStorageDirectory {
    storage: Arc<dyn Storage>,
    runtime_handle: Handle,
}

impl fmt::Debug for BlockingStorageDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BlockingStorageDirectory({:?})", self.storage.uri())
    }
}

impl BlockingStorageDirectory {
    /// Creates a directory reading from `storage`, that blocks on `runtime_handle` for synchronous
    /// reads.
    pub fn new(storage: Arc<dyn Storage>, runtime_handle: Handle) -> Self {
        Self {
            storage,
            runtime_handle,
        }
    }
}

struct BlockingStorageFileHandle {
    directory: BlockingStorageDirectory,
    path: PathBuf,
    num_bytes: usize,
}

impl fmt::Debug for BlockingStorageFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BlockingStorageFileHandle({:?}, dir={:?})",
            self.path, self.directory
        )
    }
}

impl HasLen for BlockingStorageFileHandle {
    fn len(&self) -> usize {
        self.num_bytes
    }
}

#[async_trait]
impl FileHandle for BlockingStorageFileHandle {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if byte_range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        self.directory
            .runtime_handle
            .block_on(self.read_bytes_async(byte_range))
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if byte_range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let bytes = self
            .directory
            .storage
            .get_slice(&self.path, byte_range)
            .await?;
        Ok(bytes)
    }
}

impl Directory for BlockingStorageDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let num_bytes = self
            .runtime_handle
            .block_on(self.storage.file_num_bytes(path))
            .map_err(|error| OpenReadError::wrap_io_error(error.into(), path.to_path_buf()))?;
        Ok(Arc::new(BlockingStorageFileHandle {
            directory: self.clone(),
            path: path.to_path_buf(),
            num_bytes: num_bytes as usize,
        }))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let bytes = self
            .runtime_handle
            .block_on(self.storage.get_all(path))
            .map_err(|error| OpenReadError::wrap_io_error(error.into(), path.to_path_buf()))?;
        Ok(bytes.to_vec())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.runtime_handle
            .block_on(self.storage.exists(path))
            .map_err(|error| OpenReadError::wrap_io_error(error.into(), path.to_path_buf()))
    }

    crate::read_only_directory!();
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use quickwit_storage::{PutPayload, RamStorage};

    use super::*;

    #[test]
    fn test_blocking_storage_directory_sync_reads() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let storage = Arc::new(RamStorage::default());
        let payload: Box<dyn PutPayload> = Box::new(b"hello world".to_vec());
        runtime
            .block_on(storage.put(Path::new("file"), payload))
            .unwrap();
        let directory = BlockingStorageDirectory::new(storage, runtime.handle().clone());

        assert!(directory.exists(Path::new("file")).unwrap());
        assert!(!directory.exists(Path::new("missing")).unwrap());
        assert_eq!(
            directory.atomic_read(Path::new("file")).unwrap(),
            b"hello world"
        );
        let file_handle = directory.get_file_handle(Path::new("file")).unwrap();
        assert_eq!(file_handle.len(), 11);
        assert_eq!(file_handle.read_bytes(6..11).unwrap().as_slice(), b"world");
        assert!(file_handle.read_bytes(3..3).unwrap().is_empty());
    }
}
