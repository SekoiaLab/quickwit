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

use std::time::Duration;

use quickwit_common::rand::append_random_suffix;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_indexing::actors::ObservePipelines;
use quickwit_metastore::SplitState;
use quickwit_rest_client::rest_client::QuickwitClient;
use quickwit_serve::ListSplitsQueryParams;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder, STANDALONE_NODE_NAME};

fn create_kafka_admin_client() -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("broker.address.family", "v4")
        .create()
        .unwrap()
}

async fn create_topic(
    admin_client: &AdminClient<DefaultClientContext>,
    topic: &str,
    num_partitions: i32,
) -> anyhow::Result<()> {
    admin_client
        .create_topics(
            &[NewTopic::new(
                topic,
                num_partitions,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|(topic, err_code)| {
            anyhow::anyhow!(
                "failed to create topic `{}`. error code: `{}`",
                topic,
                err_code
            )
        })?;
    Ok(())
}

async fn populate_topic(topic: &str, partition: Option<i32>) -> anyhow::Result<()> {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "30000")
        .create()?;

    let message = format!(
        r#"{{"message":"test","partition":"{}"}}"#,
        partition.map(|p| p.to_string()).unwrap_or_default()
    );

    println!("message to send: {}", message);

    producer
        .send(
            FutureRecord {
                topic,
                partition,
                timestamp: None,
                key: None::<&[u8]>,
                payload: Some(message.as_bytes()),
                headers: None,
            },
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    Ok(())
}

async fn setup_index_with_kafka_source(
    client: &QuickwitClient,
    index_id: &str,
    source_id: &str,
    topic: &str,
    num_pipelines: i32,
) {
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
                field_mappings:
                - name: message
                  type: text
                - name: id
                  type: i64
            indexing_settings:
                commit_timeout_secs: 1
            "#
    );

    client
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    let source_config = format!(
        r#"
            version: 0.9
            source_id: {source_id}
            num_pipelines: {num_pipelines}
            source_type: kafka
            params:
                topic: {topic}
                client_params:
                    bootstrap.servers: localhost:9092
                    broker.address.family: v4
                    auto.offset.reset: earliest
                    enable.auto.commit: false
            input_format: json
        "#
    );

    client
        .sources(index_id)
        .create(source_config, ConfigFormat::Yaml)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_kafka_source() {
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = append_random_suffix("test-kafka-source");
    let topic = append_random_suffix("test-kafka-source-topic");

    let kafka_admin_client = create_kafka_admin_client();
    create_topic(&kafka_admin_client, &topic, 1).await.unwrap();

    setup_index_with_kafka_source(
        &sandbox.rest_client(STANDALONE_NODE_NAME),
        &index_id,
        "test-kafka-source",
        &topic,
        1,
    )
    .await;

    populate_topic(&topic, None).await.unwrap();

    wait_until_predicate(
        || async {
            let splits_query_params = ListSplitsQueryParams {
                split_states: Some(vec![SplitState::Published]),
                ..Default::default()
            };
            sandbox
                .rest_client(STANDALONE_NODE_NAME)
                .splits(&index_id)
                .list(splits_query_params)
                .await
                .map(|splits| !splits.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    sandbox.assert_hit_count(&index_id, "", 1).await;

    sandbox
        .rest_client(STANDALONE_NODE_NAME)
        .indexes()
        .delete(&index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_kafka_source_with_indexing_settings_override() {
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = append_random_suffix("test-kafka-indexing-settings-override");
    let topic = append_random_suffix("test-kafka-indexing-settings-override-topic");

    let admin_client = create_kafka_admin_client();
    create_topic(&admin_client, &topic, 1).await.unwrap();

    // Create index with high commit_timeout (300 seconds)
    // This would normally mean splits take 5 minutes to commit
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
                field_mappings:
                - name: message
                  type: text
                - name: id
                  type: i64
            indexing_settings:
                commit_timeout_secs: 300
            "#
    );

    sandbox
        .rest_client(STANDALONE_NODE_NAME)
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // Create Kafka source with indexing_settings override to lower commit_timeout to 3 seconds
    // This tests that the source-level override works correctly
    let source_id = "test-kafka-source";
    let source_config = format!(
        r#"
            version: 0.7
            source_id: {source_id}
            desired_num_pipelines: 1
            max_num_pipelines_per_indexer: 1
            source_type: kafka
            params:
                topic: {topic}
                client_params:
                    bootstrap.servers: localhost:9092
                    broker.address.family: v4
                    auto.offset.reset: earliest
                    enable.auto.commit: false
                    indexing_settings:
                        commit_timeout_secs: 3
            input_format: json
        "#
    );

    sandbox
        .rest_client(STANDALONE_NODE_NAME)
        .sources(&index_id)
        .create(source_config, ConfigFormat::Yaml)
        .await
        .unwrap();

    populate_topic(&topic, None).await.unwrap();

    wait_until_predicate(
        || async {
            let splits_query_params = ListSplitsQueryParams {
                split_states: Some(vec![SplitState::Published]),
                ..Default::default()
            };
            sandbox
                .rest_client(STANDALONE_NODE_NAME)
                .splits(&index_id)
                .list(splits_query_params)
                .await
                .map(|splits| !splits.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    sandbox.assert_hit_count(&index_id, "", 1).await;

    sandbox
        .rest_client(STANDALONE_NODE_NAME)
        .indexes()
        .delete(&index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

/// Collects all partitions assigned accross all pipelines of a source that are
/// on the given indexer
async fn get_partition_assignment(
    sandbox: &ClusterSandbox,
    indexer_name: &str,
    index_id: &str,
    source_id: &str,
) -> Vec<i64> {
    let pipelines = sandbox
        .rest_client(indexer_name)
        .node_stats()
        .indexing_pipelines(ObservePipelines {
            source_id: Some(source_id.to_string()),
            index_id: Some(index_id.to_string()),
        })
        .await
        .unwrap();
    pipelines
        .indexing_pipelines
        .iter()
        .flat_map(|pipeline| {
            pipeline
                .source_observation
                .get("assigned_partitions")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|val| val.as_i64().unwrap())
        })
        .collect()
}

#[tokio::test]
async fn test_kafka_indexer_restart() {
    quickwit_common::setup_logging_for_tests();

    // Build a cluster with two indexers and other necessary services
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node("indexer-1", [QuickwitService::Indexer])
        .add_node("indexer-2", [QuickwitService::Indexer])
        .add_node("control-plane", [QuickwitService::ControlPlane])
        .add_node("metastore", [QuickwitService::Metastore])
        .add_node("searcher", [QuickwitService::Searcher])
        .add_node("janitor", [QuickwitService::Janitor])
        .build_and_start()
        .await;

    let index_id = append_random_suffix("test-kafka-indexer-restart");
    let source_id = "test-kafka-source";
    let topic = append_random_suffix("test-kafka-indexer-restart-topic");
    let num_partitions = 4;

    let kafka_admin_client = create_kafka_admin_client();
    // Create topic with 2 partitions to ensure we get 2 pipelines
    create_topic(&kafka_admin_client, &topic, num_partitions)
        .await
        .unwrap();

    let metastore_client = sandbox.rest_client("metastore");
    setup_index_with_kafka_source(&metastore_client, &index_id, source_id, &topic, 2).await;

    // Wait for the partitions to be balanced
    wait_until_predicate(
        || async {
            let partitions_indexer_1 =
                get_partition_assignment(&sandbox, "indexer-1", &index_id, source_id).await;
            let partitions_indexer_2 =
                get_partition_assignment(&sandbox, "indexer-2", &index_id, source_id).await;
            partitions_indexer_1.len() == num_partitions as usize / 2
                && partitions_indexer_2.len() == num_partitions as usize / 2
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    for partition in 0..num_partitions {
        populate_topic(&topic, Some(partition)).await.unwrap();
    }
    // Expect one split per pipeline. They will not be merged because they come
    // from different nodes
    sandbox.wait_for_splits(&index_id, None, 2).await.unwrap();

    let indexer_1_partitions_before_shutdown =
        get_partition_assignment(&sandbox, "indexer-1", &index_id, source_id).await;
    assert!(
        !indexer_1_partitions_before_shutdown.is_empty(),
        "indexer-1 should have at least 1 partition assigned before shutdown"
    );

    sandbox.shutdown_nodes(["indexer-1"]).await.unwrap();

    // Make sure the plan is not recalculated unnecessarily
    tokio::time::sleep(Duration::from_secs(10)).await;
    for partition in 0..num_partitions {
        populate_topic(&topic, Some(partition)).await.unwrap();
    }

    let stats_after_shutdown = sandbox
        .rest_client("indexer-2")
        .node_stats()
        .indexing()
        .await
        .unwrap();
    assert_eq!(stats_after_shutdown.num_running_pipelines, 2);
    let partitions_after_shutdown_indexer_2 =
        get_partition_assignment(&sandbox, "indexer-2", &index_id, source_id).await;
    assert_eq!(
        partitions_after_shutdown_indexer_2.len(),
        num_partitions as usize / 2
    );

    tracing::info!("starting indexer-1");
    sandbox
        .add_node("indexer-1", [QuickwitService::Indexer])
        .await
        .unwrap();

    // Wait for indexer-1 to be assigned its partitions back
    wait_until_predicate(
        || async {
            let partitions_indexer_1 =
                get_partition_assignment(&sandbox, "indexer-1", &index_id, source_id).await;
            partitions_indexer_1.len() == num_partitions as usize / 2
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    sandbox.wait_for_splits(&index_id, None, 4).await.unwrap();

    let indexer_1_partitions_after_shutdown =
        get_partition_assignment(&sandbox, "indexer-1", &index_id, source_id).await;
    assert_eq!(
        indexer_1_partitions_before_shutdown,
        indexer_1_partitions_after_shutdown,
    );

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_kafka_control_plane_restart() {
    quickwit_common::setup_logging_for_tests();

    // Build a cluster with two indexers and other necessary services
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node("indexer-1", [QuickwitService::Indexer])
        .add_node("indexer-2", [QuickwitService::Indexer])
        .add_node("control-plane", [QuickwitService::ControlPlane])
        .add_node("metastore", [QuickwitService::Metastore])
        .add_node("searcher", [QuickwitService::Searcher])
        .add_node("janitor", [QuickwitService::Janitor])
        .build_and_start()
        .await;

    let index_id = append_random_suffix("test-kafka-control-plane-restart");
    let source_id = "test-kafka-source";
    let topic = append_random_suffix("test-kafka-control-plane-restart-topic");
    let num_partitions = 4;

    let kafka_admin_client = create_kafka_admin_client();
    // Create topic with 2 partitions to ensure we get 2 pipelines
    create_topic(&kafka_admin_client, &topic, num_partitions)
        .await
        .unwrap();

    let metastore_client = sandbox.rest_client("metastore");
    setup_index_with_kafka_source(&metastore_client, &index_id, source_id, &topic, 2).await;

    wait_until_predicate(
        || async {
            let partitions_indexer_1 =
                get_partition_assignment(&sandbox, "indexer-1", &index_id, source_id).await;
            let partitions_indexer_2 =
                get_partition_assignment(&sandbox, "indexer-2", &index_id, source_id).await;
            partitions_indexer_1.len() == num_partitions as usize / 2
                && partitions_indexer_2.len() == num_partitions as usize / 2
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    let pipelines = sandbox
        .rest_client("indexer-1")
        .node_stats()
        .indexing_pipelines(ObservePipelines {
            source_id: Some(source_id.to_string()),
            index_id: Some(index_id.clone()),
        })
        .await
        .unwrap();
    let indexer_1_pipeline_uid_before_shutdown = pipelines.indexing_pipelines[0].pipeline_uid;

    sandbox.shutdown_nodes(["control-plane"]).await.unwrap();

    // Indexers should live on without control plane
    for partition in 0..num_partitions {
        populate_topic(&topic, Some(partition)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(10)).await;

    let stats_after_shutdown = sandbox
        .rest_client("indexer-2")
        .node_stats()
        .indexing()
        .await
        .unwrap();
    assert_eq!(stats_after_shutdown.num_running_pipelines, 2);

    tracing::info!("starting new control plane");
    sandbox
        .add_node("control-plane-reborn", [QuickwitService::ControlPlane])
        .await
        .unwrap();

    // Here, we want to make sure that the pipelines were not changed after the control plane
    // recomputed the plan. The only way to know for sure whether the plan was recomputed is to
    // create a new index and wait for it to be scheduled.
    let index_id_2 = append_random_suffix("test-kafka-control-plane-restart-2");
    let source_id_2 = "test-kafka-source-2";
    setup_index_with_kafka_source(&metastore_client, &index_id_2, source_id_2, &topic, 2).await;

    wait_until_predicate(
        || async {
            let pipelines = sandbox
                .rest_client("indexer-1")
                .node_stats()
                .indexing_pipelines(ObservePipelines {
                    source_id: Some(source_id_2.to_string()),
                    index_id: Some(index_id_2.to_string()),
                })
                .await
                .unwrap();
            !pipelines.indexing_pipelines.is_empty()
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    // now that we know for sure that the plan was recomputed, if the pipeline
    // UID for the original source is unchanged it means that the pipeline
    // survived the control plane restart
    let pipelines = sandbox
        .rest_client("indexer-1")
        .node_stats()
        .indexing_pipelines(ObservePipelines {
            source_id: Some(source_id.to_string()),
            index_id: Some(index_id.clone()),
        })
        .await
        .unwrap();
    let indexer_1_pipeline_uid_after_shutdown = pipelines.indexing_pipelines[0].pipeline_uid;
    assert_eq!(
        indexer_1_pipeline_uid_before_shutdown,
        indexer_1_pipeline_uid_after_shutdown,
    );

    sandbox.shutdown().await.unwrap();
}
