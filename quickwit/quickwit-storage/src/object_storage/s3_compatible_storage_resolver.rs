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
use quickwit_config::{S3StorageConfig, StorageBackend, StorageTimeoutPolicy};
use tokio::sync::OnceCell;

use super::s3_compatible_storage::create_s3_client;
use crate::{
    DebouncedStorage, S3CompatibleObjectStorage, Storage, StorageFactory, StorageResolverError,
};

/// Settings of the S3 storages that belong to the node building them rather than to the S3
/// storage config.
#[derive(Clone, Default)]
pub struct S3NodeOptions {
    /// See [`S3CompatibleObjectStorage::with_assembly_thread_pool`].
    pub assembly_thread_pool: Option<ThreadPoolWithPriority>,
    /// See [`S3CompatibleObjectStorage::with_timeout_policy`].
    pub timeout_policy: Option<StorageTimeoutPolicy>,
}

impl S3NodeOptions {
    /// Whether no option is set.
    pub fn is_empty(&self) -> bool {
        self.assembly_thread_pool.is_none() && self.timeout_policy.is_none()
    }
}

/// S3 compatible object storage resolver.
pub struct S3CompatibleObjectStorageFactory {
    storage_config: S3StorageConfig,
    // we cache the S3Client so we don't rebuild one every time we build a new Storage (for
    // every search query).
    // We don't build it in advance because we don't know if this factory is one that will
    // end up being used, or if something like azure, gcs, or even local files, will be used
    // instead.
    s3_client: OnceCell<S3Client>,
    node_options: S3NodeOptions,
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3-compatible storage factory.
    pub fn new(storage_config: S3StorageConfig) -> Self {
        Self {
            storage_config,
            s3_client: OnceCell::new(),
            node_options: S3NodeOptions::default(),
        }
    }

    /// Applies `node_options` to every storage resolved by this factory.
    pub fn with_node_options(self, node_options: S3NodeOptions) -> Self {
        Self {
            node_options,
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
        if let Some(thread_pool) = &self.node_options.assembly_thread_pool {
            storage = storage.with_assembly_thread_pool(thread_pool.clone());
        }
        if let Some(timeout_policy) = &self.node_options.timeout_policy {
            storage = storage.with_timeout_policy(timeout_policy.clone());
        }
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}
