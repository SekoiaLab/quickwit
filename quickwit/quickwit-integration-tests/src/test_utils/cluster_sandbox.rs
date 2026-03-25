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

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use futures_util::future;
use itertools::Itertools;
use quickwit_actors::ActorExitStatus;
use quickwit_cli::tool::{LocalIngestDocsArgs, local_ingest_docs_cli};
use quickwit_common::new_coolid;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::{MetastoreResolver, SplitState};
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_client::SpanReaderPluginClient;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_client::TraceServiceClient;
use quickwit_proto::types::NodeId;
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::{
    CommitType, DEFAULT_BASE_URL, QuickwitClient, QuickwitClientBuilder,
};
use quickwit_serve::tcp_listener::for_tests::TestTcpListenerResolver;
use quickwit_serve::{
    ListSplitsQueryParams, RestIngestResponse, SearchRequestQueryString, serve_quickwit,
};
use quickwit_storage::StorageResolver;
use reqwest::Url;
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tracing::debug;

use super::shutdown::NodeShutdownHandle;
use crate::test_utils::STANDALONE_NODE_NAME;

struct ClusterNode {
    node_name: String,
    config: NodeConfig,
    shutdown_handle: NodeShutdownHandle,
}

pub struct SandboxNodeConfig {
    pub node_name: String,
    pub services: HashSet<QuickwitService>,
    pub enable_otlp: bool,
}

pub struct ClusterSandboxBuilder {
    temp_dir: TempDir,
    node_configs: Vec<SandboxNodeConfig>,
}

impl Default for ClusterSandboxBuilder {
    fn default() -> Self {
        Self {
            temp_dir: tempfile::tempdir().unwrap(),
            node_configs: Vec::new(),
        }
    }
}

struct SandboxCommonConfigs {
    root_data_dir: PathBuf,
    metastore_uri: QuickwitUri,
    default_index_root_uri: QuickwitUri,
    cluster_id: String,
}

impl SandboxCommonConfigs {
    fn new(tmp_dir: &TempDir) -> Self {
        let unique_ram_dir_name = new_coolid("test-ram-dir");
        let metastore_uri =
            QuickwitUri::from_str(&format!("ram:///{}/metastore", unique_ram_dir_name)).unwrap();
        let default_index_root_uri =
            QuickwitUri::from_str(&format!("ram:///{}/indexes", unique_ram_dir_name)).unwrap();
        let cluster_id = new_coolid("test-cluster");
        Self {
            root_data_dir: tmp_dir.path().to_path_buf(),
            metastore_uri,
            default_index_root_uri,
            cluster_id,
        }
    }
}

/// Creates a NodeConfig from sandbox configs.
///
/// Peer seeds still need to be filled later.
fn assemble_node_config(
    common_configs: &SandboxCommonConfigs,
    test_node_config: SandboxNodeConfig,
    rest_port: u16,
    grpc_port: u16,
) -> NodeConfig {
    let mut config = NodeConfig::for_test_from_ports(rest_port, grpc_port);
    config.indexer_config.enable_otlp_endpoint = test_node_config.enable_otlp;
    config
        .enabled_services
        .clone_from(&test_node_config.services);
    config.jaeger_config.enable_endpoint = true;
    config.cluster_id.clone_from(&common_configs.cluster_id);
    config.node_id = NodeId::new(test_node_config.node_name.clone());
    config.data_dir_path = common_configs.root_data_dir.join(config.node_id.as_str());
    config.metastore_uri = common_configs.metastore_uri.clone();
    config.default_index_root_uri = common_configs.default_index_root_uri.clone();
    config
}

impl ClusterSandboxBuilder {
    pub fn add_node(
        mut self,
        node_name: impl Into<String>,
        services: impl IntoIterator<Item = QuickwitService>,
    ) -> Self {
        self.node_configs.push(SandboxNodeConfig {
            node_name: node_name.into(),
            services: HashSet::from_iter(services),
            enable_otlp: false,
        });
        self
    }

    pub fn add_node_with_otlp(
        mut self,
        node_name: impl Into<String>,
        services: impl IntoIterator<Item = QuickwitService>,
    ) -> Self {
        self.node_configs.push(SandboxNodeConfig {
            node_name: node_name.into(),
            services: HashSet::from_iter(services),
            enable_otlp: true,
        });
        self
    }

    /// Builds a list of of [`NodeConfig`] from the node definitions added to
    /// builder. For each node, a [`NodeConfig`] is built with the right
    /// parameters such that we will be able to run `quickwit_serve` on them and
    /// form a Quickwit cluster.
    pub async fn build_config(self) -> ResolvedClusterConfig {
        let common_configs = SandboxCommonConfigs::new(&self.temp_dir);
        let mut resolved_node_configs = Vec::new();
        let mut peers: Vec<String> = Vec::new();
        let tcp_listener_resolver = TestTcpListenerResolver::default();
        for node_builder in self.node_configs {
            let socket: SocketAddr = ([127, 0, 0, 1], 0u16).into();
            let rest_tcp_listener = TcpListener::bind(socket).await.unwrap();
            let grpc_tcp_listener = TcpListener::bind(socket).await.unwrap();
            let config = assemble_node_config(
                &common_configs,
                node_builder,
                rest_tcp_listener.local_addr().unwrap().port(),
                grpc_tcp_listener.local_addr().unwrap().port(),
            );
            tcp_listener_resolver.add_listener(rest_tcp_listener).await;
            tcp_listener_resolver.add_listener(grpc_tcp_listener).await;
            peers.push(config.gossip_advertise_addr.to_string());
            resolved_node_configs.push(config);
        }
        for node_config in resolved_node_configs.iter_mut() {
            node_config.peer_seeds = peers
                .clone()
                .into_iter()
                .filter(|seed| *seed != node_config.gossip_advertise_addr.to_string())
                .collect_vec();
        }
        ResolvedClusterConfig {
            temp_dir: self.temp_dir,
            node_configs: resolved_node_configs,
            common_sandbox_configs: common_configs,
            tcp_listener_resolver,
        }
    }

    /// Builds the cluster config, starts the nodes and waits for them to be ready
    pub async fn build_and_start(self) -> ClusterSandbox {
        self.build_config().await.start().await
    }

    pub async fn build_and_start_standalone() -> ClusterSandbox {
        ClusterSandboxBuilder::default()
            .add_node(STANDALONE_NODE_NAME, QuickwitService::supported_services())
            .build_config()
            .await
            .start()
            .await
    }
}

/// Intermediate state where the ports of all the test cluster nodes have
/// been reserved and the configurations have been generated.
pub struct ResolvedClusterConfig {
    temp_dir: TempDir,
    pub node_configs: Vec<NodeConfig>,
    common_sandbox_configs: SandboxCommonConfigs,
    tcp_listener_resolver: TestTcpListenerResolver,
}

impl ResolvedClusterConfig {
    /// Start a cluster using this config and waits for the nodes to be ready
    pub async fn start(self) -> ClusterSandbox {
        quickwit_cli::install_default_crypto_ring_provider();
        let runtimes_config = RuntimesConfig::light_for_tests();
        let storage_resolver = StorageResolver::unconfigured();
        let metastore_resolver = MetastoreResolver::unconfigured();
        let mut nodes = Vec::with_capacity(self.node_configs.len());
        for node_config in self.node_configs {
            let mut shutdown_handle = NodeShutdownHandle::new();
            let shutdown_signal = shutdown_handle.shutdown_signal();
            let join_handle = tokio::spawn({
                let node_config = node_config.clone();
                let node_id = node_config.node_id.clone();
                let services = node_config.enabled_services.clone();
                let metastore_resolver = metastore_resolver.clone();
                let storage_resolver = storage_resolver.clone();
                let tcp_listener_resolver = self.tcp_listener_resolver.clone();

                async move {
                    let result = serve_quickwit(
                        node_config,
                        runtimes_config,
                        metastore_resolver,
                        storage_resolver,
                        tcp_listener_resolver,
                        shutdown_signal,
                        quickwit_serve::do_nothing_env_filter_reload_fn(),
                    )
                    .await?;
                    debug!("{node_id} stopped successfully ({:?})", services);
                    Result::<_, anyhow::Error>::Ok(result)
                }
            });
            shutdown_handle.set_node_join_handle(join_handle);
            nodes.push(ClusterNode {
                node_name: node_config.node_id.as_str().to_string(),
                config: node_config,
                shutdown_handle,
            });
        }

        let sandbox = ClusterSandbox {
            nodes,
            common_sandbox_configs: self.common_sandbox_configs,
            storage_resolver,
            metastore_resolver,
            _temp_dir: self.temp_dir,
        };
        sandbox
            .wait_for_cluster_num_ready_nodes(sandbox.nodes.len())
            .await
            .unwrap();
        sandbox
    }
}

fn transport_url(addr: SocketAddr, tls: bool) -> Url {
    let mut url = Url::parse(DEFAULT_BASE_URL).unwrap();
    url.set_ip_host(addr.ip()).unwrap();
    url.set_port(Some(addr.port())).unwrap();
    if tls {
        url.set_scheme("https").unwrap();
    }
    url
}

#[macro_export]
macro_rules! ingest_json {
    ($($json:tt)+) => {
        quickwit_rest_client::models::IngestSource::Str(json!($($json)+).to_string())
    };
}

pub(crate) async fn ingest(
    client: &QuickwitClient,
    index_id: &str,
    ingest_source: IngestSource,
    commit_type: CommitType,
) -> anyhow::Result<RestIngestResponse> {
    let resp = client
        .ingest(index_id, ingest_source, None, None, commit_type)
        .await?;
    Ok(resp)
}

/// A test environment where you can start a Quickwit cluster and use the gRPC
/// or REST clients to test it.
pub struct ClusterSandbox {
    nodes: Vec<ClusterNode>,
    common_sandbox_configs: SandboxCommonConfigs,
    storage_resolver: StorageResolver,
    metastore_resolver: MetastoreResolver,
    _temp_dir: TempDir,
}

impl ClusterSandbox {
    /// Returns the node configurations (useful for tests that need to access node settings)
    pub fn node_configs(&self) -> impl Iterator<Item = &NodeConfig> {
        self.nodes.iter().map(|node| &node.config)
    }

    pub fn find_node_for_service(&self, service: QuickwitService) -> NodeConfig {
        self.nodes
            .iter()
            .find(|node| node.config.is_service_enabled(service))
            .unwrap_or_else(|| panic!("No {service:?} node"))
            .config
            .clone()
    }

    fn channel(&self, service: QuickwitService) -> tonic::transport::Channel {
        let node_config = self.find_node_for_service(service);
        let endpoint = format!("http://{}", node_config.grpc_listen_addr);
        tonic::transport::Channel::from_shared(endpoint)
            .unwrap()
            .connect_lazy()
    }

    /// Returns a client to one of the nodes that runs the specified service
    pub fn rest_client(&self, node_name: &str) -> QuickwitClient {
        let node_config = self
            .nodes
            .iter()
            .find(|node| node.node_name == node_name)
            .unwrap_or_else(|| panic!("No node named {node_name}"))
            .config
            .clone();

        let certificate = if let Some(tls_conf) = &node_config.rest_config.tls {
            let cert_bytes = std::fs::read(&tls_conf.ca_path).unwrap();
            Some(reqwest::tls::Certificate::from_pem(&cert_bytes).unwrap())
        } else {
            None
        };

        QuickwitClientBuilder::new(transport_url(
            node_config.rest_config.listen_addr,
            certificate.is_some(),
        ))
        .set_tls_ca(certificate)
        .build()
    }

    pub fn jaeger_client(&self) -> SpanReaderPluginClient<tonic::transport::Channel> {
        SpanReaderPluginClient::new(self.channel(QuickwitService::Searcher))
    }

    pub fn logs_client(&self) -> LogsServiceClient<tonic::transport::Channel> {
        LogsServiceClient::new(self.channel(QuickwitService::Indexer))
    }

    pub fn trace_client(&self) -> TraceServiceClient<tonic::transport::Channel> {
        TraceServiceClient::new(self.channel(QuickwitService::Indexer))
    }

    async fn wait_for_cluster_num_ready_nodes(
        &self,
        expected_num_ready_nodes: usize,
    ) -> anyhow::Result<()> {
        let metastore_node_id = self
            .find_node_for_service(QuickwitService::Metastore)
            .node_id;
        wait_until_predicate(
            || async {
                match self
                    .rest_client(metastore_node_id.as_str())
                    .cluster()
                    .snapshot()
                    .await
                {
                    Ok(result) => {
                        if result.ready_nodes.len() != expected_num_ready_nodes {
                            debug!(
                                "wait_for_cluster_num_ready_nodes expected {} ready nodes, got {}",
                                expected_num_ready_nodes,
                                result.live_nodes.len()
                            );
                            false
                        } else {
                            true
                        }
                    }
                    Err(err) => {
                        debug!("wait_for_cluster_num_ready_nodes error {err}");
                        false
                    }
                }
            },
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await?;
        Ok(())
    }

    /// Waits for the needed number of indexing pipeline to start on the given indexer.
    pub async fn wait_for_indexing_pipelines(
        &self,
        node_name: &str,
        required_pipeline_num: usize,
    ) -> anyhow::Result<()> {
        let node_config = self
            .node_configs()
            .find(|conf| conf.node_id.as_str() == node_name)
            .unwrap();
        assert!(
            node_config.is_service_enabled(QuickwitService::Indexer),
            "{node_name} is not an indexer"
        );
        wait_until_predicate(
            || async {
                match self.rest_client(node_name).node_stats().indexing().await {
                    Ok(result) => {
                        if result.num_running_pipelines != required_pipeline_num {
                            debug!(
                                "wait_for_indexing_pipelines expected {} pipelines, got {}",
                                required_pipeline_num, result.num_running_pipelines
                            );
                            false
                        } else {
                            true
                        }
                    }
                    Err(err) => {
                        debug!("wait_for_indexing_pipelines error {err}");
                        false
                    }
                }
            },
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await?;
        Ok(())
    }

    // Waits for the needed number of indexing pipeline to start.
    pub async fn wait_for_splits(
        &self,
        index_id: &str,
        split_states_filter: Option<Vec<SplitState>>,
        required_splits_num: usize,
    ) -> anyhow::Result<()> {
        let metastore_node_id = self
            .find_node_for_service(QuickwitService::Metastore)
            .node_id;
        wait_until_predicate(
            || {
                let splits_query_params = ListSplitsQueryParams {
                    split_states: split_states_filter.clone(),
                    ..Default::default()
                };
                async {
                    match self
                        .rest_client(metastore_node_id.as_str())
                        .splits(index_id)
                        .list(splits_query_params)
                        .await
                    {
                        Ok(result) => {
                            if result.len() != required_splits_num {
                                debug!(
                                    "wait_for_splits expected {} splits, got {}",
                                    required_splits_num,
                                    result.len()
                                );
                                false
                            } else {
                                true
                            }
                        }
                        Err(err) => {
                            debug!("wait_for_splits error {err}");
                            false
                        }
                    }
                }
            },
            Duration::from_secs(15),
            Duration::from_millis(500),
        )
        .await?;
        Ok(())
    }

    pub async fn local_ingest(&self, index_id: &str, json_data: &[Value]) -> anyhow::Result<()> {
        let test_node = self
            .nodes
            .iter()
            .find(|node| node.config.is_service_enabled(QuickwitService::Indexer))
            .ok_or(anyhow::anyhow!("No indexer node found"))?;
        // NodeConfig cannot be serialized, we write our own simplified config
        let mut tmp_config_file = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        // we suffix data_dir with a random slug to save us from multiple local ingestion trying to
        // concurrently do something, and cleanup the directory to start a new ingestion.
        let data_dir = test_node
            .config
            .data_dir_path
            .join(rand::random::<u64>().to_string());
        tokio::fs::create_dir(&data_dir).await?;
        let node_config = format!(
            r#"
                version: 0.8
                metastore_uri: {}
                data_dir: {:?}
                "#,
            test_node.config.metastore_uri, data_dir
        );
        tmp_config_file.write_all(node_config.as_bytes())?;
        tmp_config_file.flush()?;

        let mut tmp_data_file = tempfile::NamedTempFile::new().unwrap();
        for line in json_data {
            serde_json::to_writer(&mut tmp_data_file, line)?;
            tmp_data_file.write_all(b"\n")?;
        }
        tmp_data_file.flush()?;

        local_ingest_docs_cli(LocalIngestDocsArgs {
            clear_cache: false,
            config_uri: QuickwitUri::from_str(tmp_config_file.path().to_str().unwrap())?,
            index_id: index_id.to_string(),
            input_format: quickwit_config::SourceInputFormat::Json,
            overwrite: false,
            vrl_script: None,
            input_path_opt: Some(QuickwitUri::from_str(
                tmp_data_file
                    .path()
                    .to_str()
                    .context("temp path could not be converted to URI")?,
            )?),
        })
        .await?;
        Ok(())
    }

    pub async fn assert_hit_count(&self, index_id: &str, query: &str, expected_num_hits: u64) {
        let searcher_node_id = self
            .find_node_for_service(QuickwitService::Searcher)
            .node_id;
        let search_response = self
            .rest_client(searcher_node_id.as_str())
            .search(
                index_id,
                SearchRequestQueryString {
                    query: query.to_string(),
                    max_hits: 10,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        debug!(
            "search response for query {} on index {index_id}: {:?}",
            query, search_response
        );
        assert_eq!(
            search_response.num_hits, expected_num_hits,
            "unexpected num_hits for query {query}"
        );
    }

    /// Shutdown nodes by name
    pub async fn shutdown_nodes(
        &mut self,
        node_names: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Vec<HashMap<String, ActorExitStatus>>, anyhow::Error> {
        // We need to drop rest clients first because reqwest can hold connections open
        // preventing rest server's graceful shutdown.
        let mut indexer_shutdown_futures = Vec::new();
        let mut other_shutdown_futures = Vec::new();
        let mut shutdown_node_info = HashMap::new();
        let node_names_set: HashSet<String> = node_names
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        let mut i = 0;
        while i < self.nodes.len() {
            let node_name = &self.nodes[i].node_name;
            if !node_names_set.contains(node_name) {
                i += 1;
                continue;
            }
            let node_to_shutdown = self.nodes.remove(i);
            shutdown_node_info.insert(
                node_to_shutdown.config.node_id.clone(),
                node_to_shutdown.config.enabled_services.clone(),
            );
            if node_to_shutdown
                .config
                .is_service_enabled(QuickwitService::Indexer)
            {
                indexer_shutdown_futures.push(node_to_shutdown.shutdown_handle.shutdown());
            } else {
                other_shutdown_futures.push(node_to_shutdown.shutdown_handle.shutdown());
            }
        }
        debug!("shutting down {:?}", shutdown_node_info);
        // We must decommision the indexer nodes first and independently from the other nodes.
        let indexer_shutdown_results = future::join_all(indexer_shutdown_futures).await;
        let other_shutdown_results = future::join_all(other_shutdown_futures).await;
        let exit_statuses = indexer_shutdown_results
            .into_iter()
            .chain(other_shutdown_results)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(exit_statuses)
    }

    pub async fn shutdown(
        mut self,
    ) -> Result<Vec<HashMap<String, ActorExitStatus>>, anyhow::Error> {
        let all_node_names: Vec<String> = self.nodes.iter().map(|n| n.node_name.clone()).collect();
        self.shutdown_nodes(all_node_names).await
    }

    /// Adds a new node to the existing cluster
    pub async fn add_node(
        &mut self,
        node_name: impl Into<String>,
        services: impl IntoIterator<Item = QuickwitService>,
    ) -> anyhow::Result<()> {
        let node_name = node_name.into();
        let services: HashSet<QuickwitService> = HashSet::from_iter(services);

        // Collect peer seeds from existing nodes
        let peer_seeds: Vec<String> = self
            .nodes
            .iter()
            .map(|node| node.config.gossip_advertise_addr.to_string())
            .collect();

        // Create TCP listeners for the new node
        let socket: SocketAddr = ([127, 0, 0, 1], 0u16).into();
        let rest_tcp_listener = TcpListener::bind(socket).await?;
        let grpc_tcp_listener = TcpListener::bind(socket).await?;

        let rest_port = rest_tcp_listener.local_addr()?.port();
        let grpc_port = grpc_tcp_listener.local_addr()?.port();

        let tcp_listener_resolver = TestTcpListenerResolver::default();
        tcp_listener_resolver.add_listener(rest_tcp_listener).await;
        tcp_listener_resolver.add_listener(grpc_tcp_listener).await;

        // Build the node configuration using common configs
        let sandbox_node_config = SandboxNodeConfig {
            node_name: node_name.clone(),
            services: services.clone(),
            enable_otlp: false,
        };
        let mut config = assemble_node_config(
            &self.common_sandbox_configs,
            sandbox_node_config,
            rest_port,
            grpc_port,
        );
        config.peer_seeds = peer_seeds;

        // Start the node
        let runtimes_config = RuntimesConfig::light_for_tests();
        let mut shutdown_handle = NodeShutdownHandle::new();
        let shutdown_signal = shutdown_handle.shutdown_signal();

        let join_handle = tokio::spawn({
            let node_config = config.clone();
            let node_id = node_config.node_id.clone();
            let node_services = node_config.enabled_services.clone();
            let metastore_resolver = self.metastore_resolver.clone();
            let storage_resolver = self.storage_resolver.clone();

            async move {
                let result = serve_quickwit(
                    node_config,
                    runtimes_config,
                    metastore_resolver,
                    storage_resolver,
                    tcp_listener_resolver,
                    shutdown_signal,
                    quickwit_serve::do_nothing_env_filter_reload_fn(),
                )
                .await?;
                debug!("{node_id} stopped successfully ({:?})", node_services);
                Result::<_, anyhow::Error>::Ok(result)
            }
        });

        shutdown_handle.set_node_join_handle(join_handle);

        // Add the node to the cluster
        self.nodes.push(ClusterNode {
            node_name: node_name.clone(),
            config,
            shutdown_handle,
        });

        // Wait for the newly added node to become ready
        // Give extra time for gossip propagation and metastore connectivity
        self.wait_for_cluster_num_ready_nodes(self.nodes.len())
            .await?;

        Ok(())
    }
}
