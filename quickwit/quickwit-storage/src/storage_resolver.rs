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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use once_cell::sync::Lazy;
use quickwit_common::thread_pool::with_priority::ThreadPoolWithPriority;
use quickwit_common::uri::{Protocol, Uri};
use quickwit_config::{StorageBackend, StorageConfigs};

#[cfg(feature = "azure")]
use crate::AzureBlobStorageFactory;
#[cfg(feature = "gcs")]
use crate::GoogleCloudStorageFactory;
use crate::local_file_storage::LocalFileStorageFactory;
use crate::ram_storage::RamStorageFactory;
use crate::{S3CompatibleObjectStorageFactory, Storage, StorageFactory, StorageResolverError};

/// Returns the [`Storage`] instance associated with the protocol of a URI. The actual creation of
/// storage objects is delegated to pre-registered [`StorageFactory`]. The resolver is only
/// responsible for dispatching to the appropriate factory.
#[derive(Clone)]
pub struct StorageResolver {
    per_backend_factories: Arc<HashMap<StorageBackend, Box<dyn StorageFactory>>>,
}

impl fmt::Debug for StorageResolver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StorageResolver").finish()
    }
}

impl StorageResolver {
    /// Creates an empty [`StorageResolverBuilder`].
    pub fn builder() -> StorageResolverBuilder {
        StorageResolverBuilder::default()
    }

    /// Resolves the given URI.
    pub async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let backend = match uri.protocol() {
            Protocol::Azure => StorageBackend::Azure,
            Protocol::File => StorageBackend::File,
            Protocol::Ram => StorageBackend::Ram,
            Protocol::S3 => StorageBackend::S3,
            Protocol::Google => StorageBackend::Google,
            _ => {
                let message = format!(
                    "Quickwit does not support {} as a storage backend",
                    uri.protocol()
                );
                return Err(StorageResolverError::UnsupportedBackend(message));
            }
        };
        let storage_factory = self.per_backend_factories.get(&backend).ok_or({
            let message = format!("no storage factory is registered for {}", uri.protocol());
            StorageResolverError::UnsupportedBackend(message)
        })?;
        let storage = storage_factory.resolve(uri).await?;
        Ok(storage)
    }

    /// Creates and returns a default [`StorageResolver`] with the default storage configuration for
    /// each backend. Note that if the environment (env vars, instance metadata, ...) fails to
    /// provide the necessary credentials, the default Azure or S3 storage returned by this
    /// resolver will not work.
    pub fn unconfigured() -> Self {
        static STORAGE_RESOLVER: Lazy<StorageResolver> = Lazy::new(|| {
            let storage_configs = StorageConfigs::default();
            StorageResolver::configured(&storage_configs)
        });
        STORAGE_RESOLVER.clone()
    }

    /// Creates and returns a [`StorageResolver`].
    pub fn configured(storage_configs: &StorageConfigs) -> Self {
        Self::configured_with_options(storage_configs, None)
    }

    /// Same as [`StorageResolver::configured`], but S3 storages assemble large multi-segment
    /// downloads on `s3_assembly_thread_pool` instead of the tokio runtime. This is a property
    /// of the node building the resolver, not of the storage configs: searcher-only nodes hand
    /// over their search thread pool.
    pub fn configured_with_s3_assembly_thread_pool(
        storage_configs: &StorageConfigs,
        s3_assembly_thread_pool: ThreadPoolWithPriority,
    ) -> Self {
        Self::configured_with_options(storage_configs, Some(s3_assembly_thread_pool))
    }

    fn configured_with_options(
        storage_configs: &StorageConfigs,
        s3_assembly_thread_pool_opt: Option<ThreadPoolWithPriority>,
    ) -> Self {
        let mut s3_storage_factory = S3CompatibleObjectStorageFactory::new(
            storage_configs.find_s3().cloned().unwrap_or_default(),
        );
        if let Some(thread_pool) = s3_assembly_thread_pool_opt {
            s3_storage_factory = s3_storage_factory.with_assembly_thread_pool(thread_pool);
        }
        let mut builder = StorageResolver::builder()
            .register(LocalFileStorageFactory)
            .register(RamStorageFactory::default())
            .register(s3_storage_factory);
        #[cfg(feature = "azure")]
        {
            builder = builder.register(AzureBlobStorageFactory::new(
                storage_configs.find_azure().cloned().unwrap_or_default(),
            ));
        }
        #[cfg(not(feature = "azure"))]
        {
            use crate::storage_factory::UnsupportedStorage;

            builder = builder.register(UnsupportedStorage::new(
                StorageBackend::Azure,
                "Quickwit was compiled without the `azure` feature",
            ))
        }
        #[cfg(feature = "gcs")]
        {
            builder = builder.register(GoogleCloudStorageFactory::new(
                storage_configs.find_google().cloned().unwrap_or_default(),
            ));
        }
        #[cfg(not(feature = "gcs"))]
        {
            use crate::storage_factory::UnsupportedStorage;

            builder = builder.register(UnsupportedStorage::new(
                StorageBackend::Google,
                "Quickwit was compiled without the `gcs` feature",
            ))
        }
        builder
            .build()
            .expect("storage factory and config backends should match")
    }

    /// Returns a [`StorageResolver`] for testing purposes. Unlike
    /// [`StorageResolver::unconfigured`], this resolver does not return a singleton.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        StorageResolver::builder()
            .register(RamStorageFactory::default())
            .register(LocalFileStorageFactory)
            .build()
            .expect("storage factory and config backends should match")
    }
}

#[derive(Default)]
pub struct StorageResolverBuilder {
    per_backend_factories: HashMap<StorageBackend, Box<dyn StorageFactory>>,
}

impl StorageResolverBuilder {
    /// Registers a [`StorageFactory`].
    pub fn register<S: StorageFactory>(mut self, storage_factory: S) -> Self {
        self.per_backend_factories
            .insert(storage_factory.backend(), Box::new(storage_factory));
        self
    }

    /// Builds the [`StorageResolver`].
    pub fn build(self) -> anyhow::Result<StorageResolver> {
        let storage_resolver = StorageResolver {
            per_backend_factories: Arc::new(self.per_backend_factories),
        };
        Ok(storage_resolver)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::{MockStorageFactory, RamStorage};

    #[tokio::test]
    async fn test_storage_resolver_simple() -> anyhow::Result<()> {
        let mut file_storage_factory = MockStorageFactory::new();
        file_storage_factory
            .expect_backend()
            .returning(|| StorageBackend::File);

        let mut ram_storage_factory = MockStorageFactory::new();
        ram_storage_factory
            .expect_backend()
            .returning(|| StorageBackend::Ram);
        ram_storage_factory.expect_resolve().returning(|_uri| {
            Ok(Arc::new(
                RamStorage::builder()
                    .put("hello", b"hello_content_second")
                    .build(),
            ))
        });
        let storage_resolver = StorageResolver::builder()
            .register(file_storage_factory)
            .register(ram_storage_factory)
            .build()
            .unwrap();
        let storage = storage_resolver.resolve(&Uri::for_test("ram:///")).await?;
        let data = storage.get_all(Path::new("hello")).await?;
        assert_eq!(&data[..], b"hello_content_second");
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_resolver_override() -> anyhow::Result<()> {
        let mut first_ram_storage_factory = MockStorageFactory::new();
        first_ram_storage_factory
            .expect_backend()
            .returning(|| StorageBackend::Ram);

        let mut second_ram_storage_factory = MockStorageFactory::new();
        second_ram_storage_factory
            .expect_backend()
            .returning(|| StorageBackend::Ram);
        second_ram_storage_factory
            .expect_resolve()
            .returning(|uri| {
                assert_eq!(uri.as_str(), "ram:///home");
                Ok(Arc::new(
                    RamStorage::builder()
                        .put("hello", b"hello_content_second")
                        .build(),
                ))
            });
        let storage_resolver = StorageResolver::builder()
            .register(first_ram_storage_factory)
            .register(second_ram_storage_factory)
            .build()
            .unwrap();
        let storage = storage_resolver
            .resolve(&Uri::for_test("ram:///home"))
            .await?;
        let data = storage.get_all(Path::new("hello")).await?;
        assert_eq!(&data[..], b"hello_content_second");
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_resolver_unsupported_protocol() {
        let storage_resolver = StorageResolver::unconfigured();
        let storage_uri = Uri::for_test("postgresql://localhost:5432/metastore");
        let resolver_error = storage_resolver.resolve(&storage_uri).await.unwrap_err();
        assert!(matches!(
            resolver_error,
            StorageResolverError::UnsupportedBackend(_)
        ));
    }
}
