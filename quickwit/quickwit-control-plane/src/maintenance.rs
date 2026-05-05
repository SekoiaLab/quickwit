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

//! Maintenance mode management for the Quickwit control plane.
//!
//! When maintenance mode is enabled:
//! - Metadata mutations (index/source CRUD) are rejected with a `MaintenanceMode` error.
//! - The indexing plan is frozen: it is not rebuilt when indexers join or leave.
//! - Shard scaling (up/down) and rebalancing are paused.
//! - The frozen plan and maintenance metadata are persisted to the metastore `kv` table so they
//!   survive control plane restarts.
//!
//! # Persistence
//!
//! One key is stored in the metastore `kv` table:
//! - `maintenance_state`: postcard-serialized (then base64-encoded) [`MaintenancePersistedState`]
//!   (contains both metadata and frozen plan)
//!
//! # Integration
//!
//! Persistence is abstracted behind the [`MaintenancePersistence`] trait. The production
//! implementation ([`MetastoreKvPersistence`]) uses the metastore's `GetKv`/`SetKv`/`DeleteKv`
//! RPCs (which read/write to the PostgreSQL `kv` table). Tests can use [`InMemoryPersistence`].

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine as _;
use quickwit_proto::metastore::{
    DeleteKvRequest, GetKvRequest, MetastoreService, MetastoreServiceClient, SetKvRequest,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::info;

use crate::indexing_plan::PhysicalIndexingPlan;

/// Key in the metastore `kv` table for the combined maintenance state.
pub const KV_KEY_MAINTENANCE_STATE: &str = "maintenance_state";

/// Metadata persisted alongside the maintenance mode flag.
///
/// The `enabled_at` field stores a human-readable RFC 3339 datetime string
/// (e.g., `"2024-06-15T14:30:00Z"`), making it easy to inspect directly in the database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MaintenanceModeMetadata {
    /// RFC 3339 formatted UTC datetime when maintenance mode was enabled.
    pub enabled_at: String,
}

impl MaintenanceModeMetadata {
    /// Creates a new metadata instance with `enabled_at` set to the current UTC time.
    pub fn new_now() -> Self {
        Self {
            enabled_at: now_rfc3339(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MaintenancePersistedState {
    pub metadata: MaintenanceModeMetadata,
    pub frozen_plan: PhysicalIndexingPlan,
}

/// In-memory maintenance mode state for the control plane.
#[derive(Debug, Clone, Default)]
pub struct MaintenanceState {
    /// If `Some`, maintenance mode is active with the given metadata.
    metadata: Option<MaintenanceModeMetadata>,
}

impl MaintenanceState {
    /// Returns `true` if maintenance mode is currently active.
    pub fn is_active(&self) -> bool {
        self.metadata.is_some()
    }

    /// Returns the metadata if maintenance mode is active.
    pub fn metadata(&self) -> Option<&MaintenanceModeMetadata> {
        self.metadata.as_ref()
    }

    /// Enables maintenance mode.
    /// Returns the metadata that was set.
    pub fn enable(&mut self) -> MaintenanceModeMetadata {
        let enabled_at = now_rfc3339();
        let metadata = MaintenanceModeMetadata { enabled_at };
        self.metadata = Some(metadata.clone());
        info!(
            enabled_at = %metadata.enabled_at,
            "maintenance mode enabled"
        );
        metadata
    }

    /// Disables maintenance mode.
    /// Returns `true` if it was previously active.
    pub fn disable(&mut self) -> bool {
        let was_active = self.metadata.is_some();
        self.metadata = None;
        if was_active {
            info!("maintenance mode disabled");
        }
        was_active
    }

    /// Loads maintenance state from persisted metadata.
    pub fn load_from_metadata(&mut self, metadata: MaintenanceModeMetadata) {
        info!(
            enabled_at = %metadata.enabled_at,
            "loaded maintenance mode from persisted state"
        );
        self.metadata = Some(metadata);
    }
}

// -- Persistence Trait --

/// Persistence abstraction for maintenance mode state.
#[async_trait]
pub trait MaintenancePersistence: Send + Sync + std::fmt::Debug + 'static {
    /// Loads the maintenance state from persistent storage.
    /// Returns `None` if no maintenance state is persisted.
    async fn load(&self) -> Option<MaintenancePersistedState>;

    /// Persists the maintenance metadata and frozen plan atomically.
    async fn save(
        &self,
        metadata: &MaintenanceModeMetadata,
        frozen_plan: &PhysicalIndexingPlan,
    ) -> anyhow::Result<()>;

    /// Clears all persisted maintenance state.
    async fn clear(&self) -> anyhow::Result<()>;
}

/// In-memory implementation of [`MaintenancePersistence`] for tests.
///
/// This implementation stores raw postcard bytes in a thread-safe `Option<Vec<u8>>` and does not
/// persist across process restarts.
#[derive(Debug, Clone, Default)]
pub struct InMemoryPersistence {
    state: Arc<Mutex<Option<Vec<u8>>>>,
}

impl InMemoryPersistence {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl MaintenancePersistence for InMemoryPersistence {
    async fn load(&self) -> Option<MaintenancePersistedState> {
        let state = self.state.lock().unwrap();
        match state.as_deref() {
            Some(bytes) => {
                let persisted: MaintenancePersistedState = postcard::from_bytes(bytes)
                    .expect("failed to deserialize maintenance state from in-memory bytes");
                Some(persisted)
            }
            None => None,
        }
    }

    async fn save(
        &self,
        metadata: &MaintenanceModeMetadata,
        frozen_plan: &PhysicalIndexingPlan,
    ) -> anyhow::Result<()> {
        let persisted = MaintenancePersistedState {
            metadata: metadata.clone(),
            frozen_plan: frozen_plan.clone(),
        };
        let bytes = postcard::to_allocvec(&persisted)
            .map_err(|err| anyhow::anyhow!("failed to serialize maintenance state: {err}"))?;
        let mut state = self.state.lock().unwrap();
        *state = Some(bytes);
        Ok(())
    }

    async fn clear(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        *state = None;
        Ok(())
    }
}

// -- Metastore-backed persistence --

/// Production implementation of [`MaintenancePersistence`] that uses the metastore's
/// `GetKv`/`SetKv`/`DeleteKv` RPCs to persist maintenance state in the PostgreSQL `kv` table.
#[derive(Debug, Clone)]
pub struct MetastoreKvPersistence {
    metastore: MetastoreServiceClient,
}

impl MetastoreKvPersistence {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        Self { metastore }
    }
}

#[async_trait]
impl MaintenancePersistence for MetastoreKvPersistence {
    async fn load(&self) -> Option<MaintenancePersistedState> {
        let response = self
            .metastore
            .clone()
            .get_kv(GetKvRequest {
                key: KV_KEY_MAINTENANCE_STATE.to_string(),
            })
            .await
            .expect("failed to get maintenance state from metastore");
        match response.value {
            Some(encoded) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(&encoded)
                    .expect("maintenance state in metastore should always be valid base64");
                let persisted: MaintenancePersistedState = postcard::from_bytes(&decoded)
                    .expect("failed to deserialize maintenance state from metastore bytes");
                Some(persisted)
            }
            None => None,
        }
    }

    async fn save(
        &self,
        metadata: &MaintenanceModeMetadata,
        frozen_plan: &PhysicalIndexingPlan,
    ) -> anyhow::Result<()> {
        let persisted = MaintenancePersistedState {
            metadata: metadata.clone(),
            frozen_plan: frozen_plan.clone(),
        };
        let bytes = postcard::to_allocvec(&persisted)
            .map_err(|err| anyhow::anyhow!("failed to serialize maintenance state: {err}"))?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
        self.metastore
            .clone()
            .set_kv(SetKvRequest {
                key: KV_KEY_MAINTENANCE_STATE.to_string(),
                value: encoded,
            })
            .await?;
        Ok(())
    }

    async fn clear(&self) -> anyhow::Result<()> {
        self.metastore
            .clone()
            .delete_kv(DeleteKvRequest {
                key: KV_KEY_MAINTENANCE_STATE.to_string(),
            })
            .await?;
        Ok(())
    }
}

// -- Helper functions --

/// Serializes a `PhysicalIndexingPlan` to a JSON string for use in API responses.
pub fn serialize_frozen_plan(plan: &PhysicalIndexingPlan) -> serde_json::Result<String> {
    serde_json::to_string(plan)
}

/// Returns the current UTC time formatted as an RFC 3339 string.
fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("formatting OffsetDateTime as RFC 3339 should never fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_maintenance_state_default_is_inactive() {
        let state = MaintenanceState::default();
        assert!(!state.is_active());
        assert!(state.metadata().is_none());
    }

    #[test]
    fn test_maintenance_state_enable_disable() {
        let mut state = MaintenanceState::default();

        // Enable
        let metadata = state.enable();
        assert!(state.is_active());
        assert!(!metadata.enabled_at.is_empty());
        // Should be a valid RFC 3339 datetime
        assert!(
            OffsetDateTime::parse(&metadata.enabled_at, &Rfc3339).is_ok(),
            "enabled_at should be valid RFC 3339: {}",
            metadata.enabled_at
        );

        // Disable
        let was_active = state.disable();
        assert!(was_active);
        assert!(!state.is_active());

        // Disable again is a no-op
        let was_active = state.disable();
        assert!(!was_active);
    }

    #[test]
    fn test_maintenance_metadata_serde_round_trip() {
        let metadata = MaintenanceModeMetadata {
            enabled_at: "2024-06-15T14:30:00Z".to_string(),
        };
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("2024-06-15T14:30:00Z"));
        let deserialized: MaintenanceModeMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_maintenance_persisted_state() {
        let metadata = MaintenanceModeMetadata {
            enabled_at: "2024-06-15T14:30:00Z".to_string(),
        };
        let plan = PhysicalIndexingPlan::with_indexer_ids(&[
            "indexer-1".to_string(),
            "indexer-2".to_string(),
        ]);
        let state = MaintenancePersistedState {
            metadata: metadata.clone(),
            frozen_plan: plan.clone(),
        };
        let bytes = postcard::to_allocvec(&state).unwrap();
        let deserialized: MaintenancePersistedState = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, state);
    }

    #[test]
    fn test_deserialize_maintenance_persisted_state_invalid_bytes() {
        let result: Result<MaintenancePersistedState, _> =
            postcard::from_bytes(b"not valid postcard");
        assert!(result.is_err());
    }

    /// Validates that a hardcoded postcard serialization of [`MaintenancePersistedState`] can
    /// always be deserialized without errors.
    ///
    /// If this test fails, it means a breaking change was introduced in the binary serialization
    /// format of [`PhysicalIndexingPlan`] or one of its dependencies. Any such change would
    /// corrupt persisted maintenance state in existing deployments.
    ///
    /// The bytes encode the following value:
    /// ```text
    /// MaintenancePersistedState {
    ///     metadata: MaintenanceModeMetadata { enabled_at: "2024-06-15T14:30:00Z" },
    ///     frozen_plan: PhysicalIndexingPlan::with_indexer_ids(&["indexer-1"]),
    /// }
    /// ```
    #[test]
    fn test_postcard_deserialization_stability() {
        // Layout (postcard wire format):
        //   varint(20) + b"2024-06-15T14:30:00Z"   -- metadata.enabled_at
        //   varint(1)                               -- map length (1 entry)
        //   varint(9)  + b"indexer-1"              -- map key
        //   varint(0)                               -- map value (empty Vec<IndexingTask>)
        const HARDCODED_BYTES: &[u8] = &[
            0x14, b'2', b'0', b'2', b'4', b'-', b'0', b'6', b'-', b'1', b'5', b'T', b'1', b'4',
            b':', b'3', b'0', b':', b'0', b'0',
            b'Z', // metadata.enabled_at: "2024-06-15T14:30:00Z"
            0x01, // 1 entry in the map
            0x09, b'i', b'n', b'd', b'e', b'x', b'e', b'r', b'-', b'1', // key: "indexer-1"
            0x00, // empty Vec<IndexingTask>
        ];

        let state: MaintenancePersistedState = postcard::from_bytes(HARDCODED_BYTES)
            .expect("hardcoded bytes must deserialize without errors");

        assert_eq!(state.metadata.enabled_at, "2024-06-15T14:30:00Z");
        assert_eq!(state.frozen_plan.num_indexers(), 1);
        assert!(
            state.frozen_plan.indexer("indexer-1").is_some(),
            "expected 'indexer-1' in the frozen plan"
        );
    }

    #[tokio::test]
    async fn test_in_memory_persistence_save_and_load() {
        let persistence = InMemoryPersistence::new();

        // Initially empty
        let loaded = persistence.load().await;
        assert!(loaded.is_none());

        // Save
        let metadata = MaintenanceModeMetadata {
            enabled_at: "2024-01-15T10:00:00Z".to_string(),
        };
        let plan = PhysicalIndexingPlan::with_indexer_ids(&["indexer-1".to_string()]);
        persistence.save(&metadata, &plan).await.unwrap();

        // Load
        let loaded = persistence.load().await.unwrap();
        assert_eq!(loaded.metadata, metadata);
        assert_eq!(loaded.frozen_plan, plan);

        // Clear
        persistence.clear().await.unwrap();
        let loaded = persistence.load().await;
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_persistence_overwrite() {
        let persistence = InMemoryPersistence::new();

        let metadata1 = MaintenanceModeMetadata {
            enabled_at: "2024-01-01T00:00:00Z".to_string(),
        };
        let plan1 = PhysicalIndexingPlan::with_indexer_ids(&["a".to_string()]);
        persistence.save(&metadata1, &plan1).await.unwrap();

        let metadata2 = MaintenanceModeMetadata {
            enabled_at: "2024-06-01T12:00:00Z".to_string(),
        };
        let plan2 = PhysicalIndexingPlan::with_indexer_ids(&["b".to_string()]);
        persistence.save(&metadata2, &plan2).await.unwrap();

        let loaded = persistence.load().await.unwrap();
        assert_eq!(loaded.metadata, metadata2);
        assert_eq!(loaded.frozen_plan, plan2);
    }
}
