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

use std::convert::Infallible;

use quickwit_actors::{AskError, Mailbox, Observe};
use quickwit_indexing::actors::{
    IndexingService, IndexingServiceCounters, ObservePipelines, PipelineObservation,
};
use quickwit_indexing::models::IndexingStatistics;
use quickwit_proto::types::{IndexId, PipelineUid, SourceId};
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::require;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;

#[derive(utoipa::OpenApi)]
#[openapi(paths(indexing_endpoint, indexing_pipelines_endpoint))]
pub struct IndexingApi;

#[utoipa::path(
    get,
    tag = "Indexing",
    path = "/indexing",
    responses(
        (status = 200, description = "Successfully observed indexing pipelines.", body = IndexingStatistics)
    ),
)]
/// Observe Indexing Pipeline
async fn indexing_endpoint(
    indexing_service_mailbox: Mailbox<IndexingService>,
) -> Result<IndexingServiceCounters, AskError<Infallible>> {
    let counters = indexing_service_mailbox.ask(Observe).await?;
    indexing_service_mailbox.ask(Observe).await?;
    Ok(counters)
}

fn indexing_get_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("indexing").and(warp::get())
}

pub fn indexing_get_handler(
    indexing_service_mailbox_opt: Option<Mailbox<IndexingService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    indexing_get_filter()
        .and(require(indexing_service_mailbox_opt))
        .then(indexing_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .recover(recover_fn)
        .boxed()
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct IndexingPipelineResponse {
    pub index_id: IndexId,
    pub source_id: SourceId,
    pub pipeline_uid: PipelineUid,
    pub indexing_statistics: IndexingStatistics,
    pub source_observation: serde_json::Value,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct IndexingPipelinesResponse {
    pub indexing_pipelines: Vec<IndexingPipelineResponse>,
}

#[utoipa::path(
    get,
    tag = "Indexing",
    path = "/indexing/pipelines",
    responses(
        (status = 200, description = "Successfully queried indexing pipelines.", body = IndexingPipelinesResponse)
    ),
)]
/// Query Indexing Pipelines
async fn indexing_pipelines_endpoint(
    query: ObservePipelines,
    indexing_service_mailbox: Mailbox<IndexingService>,
) -> Result<IndexingPipelinesResponse, AskError<Infallible>> {
    let response = indexing_service_mailbox.ask(query).await?;
    let futures = response
        .indexing_pipelines
        .into_iter()
        .map(|pipeline_observation| {
            let PipelineObservation {
                index_id,
                source_id,
                pipeline_uid,
                source_observation_fut,
                indexing_statistics,
            } = pipeline_observation;
            async move {
                let (source_observation, error) = match source_observation_fut.await {
                    Ok(source_observation) => (source_observation, None),
                    Err(err) => (serde_json::json!({}), Some(err.to_string())),
                };
                IndexingPipelineResponse {
                    index_id,
                    source_id,
                    pipeline_uid,
                    source_observation,
                    indexing_statistics,
                    error,
                }
            }
        });
    let indexing_pipelines = futures::future::join_all(futures).await;
    Ok(IndexingPipelinesResponse { indexing_pipelines })
}

fn indexing_pipelines_filter()
-> impl Filter<Extract = (ObservePipelines,), Error = Rejection> + Clone {
    warp::path!("indexing" / "pipelines")
        .and(warp::get())
        .and(warp::query::<ObservePipelines>())
}

pub fn indexing_pipelines_handler(
    indexing_service_mailbox_opt: Option<Mailbox<IndexingService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    indexing_pipelines_filter()
        .and(require(indexing_service_mailbox_opt))
        .then(indexing_pipelines_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .recover(recover_fn)
        .boxed()
}
