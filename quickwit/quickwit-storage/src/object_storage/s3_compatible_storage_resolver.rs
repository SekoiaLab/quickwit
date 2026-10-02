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

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use quickwit_common::thread_pool::with_priority::ThreadPoolWithPriority;
use quickwit_common::uri::Uri;
use quickwit_config::{S3StorageConfig, StorageBackend};
use tokio::sync::OnceCell;

use super::s3_compatible_storage::create_s3_client;
use crate::{
    DebouncedStorage, S3CompatibleObjectStorage, Storage, StorageFactory, StorageResolverError,
};

/// S3 compatible object storage resolver.
pub struct S3CompatibleObjectStorageFactory {
    storage_config: S3StorageConfig,
    // we cache the S3Client so we don't rebuild one every time we build a new Storage (for
    // every search query).
    // We don't build it in advance because we don't know if this factory is one that will
    // end up being used, or if something like azure, gcs, or even local files, will be used
    // instead.
    s3_client: OnceCell<S3Client>,
    /// See [`S3CompatibleObjectStorage::with_assembly_thread_pool`].
    assembly_thread_pool: Option<ThreadPoolWithPriority>,
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3-compatible storage factory.
    pub fn new(storage_config: S3StorageConfig) -> Self {
        Self {
            storage_config,
            s3_client: OnceCell::new(),
            assembly_thread_pool: None,
        }
    }

    /// Every storage resolved by this factory assembles large multi-segment response bodies on
    /// `thread_pool`. See [`S3CompatibleObjectStorage::with_assembly_thread_pool`].
    pub fn with_assembly_thread_pool(self, thread_pool: ThreadPoolWithPriority) -> Self {
        Self {
            assembly_thread_pool: Some(thread_pool),
            ..self
        }
    }
}

#[async_trait]
impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::S3
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let s3_client = self
            .s3_client
            .get_or_init(|| create_s3_client(&self.storage_config))
            .await
            .clone();
        let mut storage =
            S3CompatibleObjectStorage::from_uri_and_client(&self.storage_config, uri, s3_client)
                .await?;
        if let Some(thread_pool) = &self.assembly_thread_pool {
            storage = storage.with_assembly_thread_pool(thread_pool.clone());
        }
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}
