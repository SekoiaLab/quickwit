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

use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::SplitState;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::ListSplitsQueryParams;
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ClusterSandboxBuilder, ingest};

/// Tests the main multi-bucket split sharding use case end-to-end:
///
/// 1. Creates an index with `index_uri` + two `extra_index_uris` (three ram:// buckets).
/// 2. Ingests enough documents to produce multiple splits.
/// 3. Verifies that published splits have `storage_uri` set and that at least two distinct URIs
///    were used (round-robin distributes across buckets).
/// 4. Verifies that search returns correct results across all buckets.
#[tokio::test]
async fn test_multi_bucket_ingest_and_search() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;

    let index_id = "test-multi-bucket";
    let index_config = format!(
        r#"
        version: 0.8
        index_id: {index_id}
        index_uri: ram:///multi-bucket-test/bucket-a/{index_id}
        extra_index_uris:
          - ram:///multi-bucket-test/bucket-b/{index_id}
          - ram:///multi-bucket-test/bucket-c/{index_id}
        doc_mapping:
            field_mappings:
            - name: body
              type: text
            - name: ts
              type: datetime
              fast: true
            timestamp_field: ts
        indexing_settings:
            commit_timeout_secs: 1
        "#
    );

    // Create the index with three storage URIs.
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // Ingest a first batch and let it commit into a split.
    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "first record", "ts": 1735689600}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    // Ingest a second batch so we get a second split.
    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "second record", "ts": 1735689601}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 2)
        .await
        .unwrap();

    // Ingest a third batch for a third split.
    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "third record", "ts": 1735689602}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 3)
        .await
        .unwrap();

    // Verify that splits were distributed across multiple buckets.
    let splits = sandbox
        .rest_client(QuickwitService::Metastore)
        .splits(index_id)
        .list(ListSplitsQueryParams {
            split_states: Some(vec![SplitState::Published]),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(splits.len(), 3, "expected exactly 3 published splits");

    let storage_uris: Vec<String> = splits
        .iter()
        .map(|split| {
            split
                .split_metadata
                .storage_uri
                .as_ref()
                .expect("every new split must have storage_uri set")
                .to_string()
        })
        .collect();

    // With round-robin over 3 buckets and 3 splits, each bucket should get
    // exactly one split. Verify at least 2 distinct URIs were used (in case
    // pipeline restarts reset the counter).
    let distinct_uris: std::collections::HashSet<&str> =
        storage_uris.iter().map(|s| s.as_str()).collect();
    assert!(
        distinct_uris.len() >= 2,
        "expected splits across at least 2 distinct storage URIs, got: {storage_uris:?}"
    );

    // All URIs must be one of the three configured buckets.
    let expected_uris = [
        format!("ram:///multi-bucket-test/bucket-a/{index_id}"),
        format!("ram:///multi-bucket-test/bucket-b/{index_id}"),
        format!("ram:///multi-bucket-test/bucket-c/{index_id}"),
    ];
    for uri in &storage_uris {
        assert!(
            expected_uris.contains(uri),
            "unexpected storage_uri {uri}, expected one of {expected_uris:?}"
        );
    }

    // Search must return all 3 documents across all buckets.
    sandbox.assert_hit_count(index_id, "body:record", 3).await;

    // Targeted searches should also work.
    sandbox.assert_hit_count(index_id, "body:first", 1).await;
    sandbox.assert_hit_count(index_id, "body:second", 1).await;
    sandbox.assert_hit_count(index_id, "body:third", 1).await;

    // Cleanup.
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}
