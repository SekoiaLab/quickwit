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

//! Developer endpoints to inspect a split stored in object storage:
//! - `GET /api/developer/splits/{index_id}/{split_id}/space-usage` returns the space used by each
//!   field and each data structure of the split.
//! - `GET /api/developer/splits/{index_id}/{split_id}/download` streams the split file, throttled
//!   to `max_bytes_per_sec` (1 Gbps by default). warp cannot stream a response body, so this one is
//!   served by [`SplitDownloadLayer`], a tower layer in front of the warp service.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::{Stream, TryStreamExt};
use http::header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderValue, Method, StatusCode};
use http_body::Frame;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use quickwit_common::split_file;
use quickwit_directories::{BlockingStorageDirectory, HotDirectory};
use quickwit_metastore::{
    IndexMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, SplitMetadata,
};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListSplitsRequest, MetastoreError, MetastoreService,
    MetastoreServiceClient,
};
use quickwit_storage::{BundleStorage, Storage, StorageError, StorageErrorKind, StorageResolver};
use serde::{Deserialize, Serialize};
use tantivy::directory::FileSlice;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::{Index, ReloadPolicy};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::runtime::Handle;
use tokio::time::Instant;
use tower::{BoxError, Layer, Service};
use warp::{Filter, Rejection, Reply};

use crate::with_arg;

/// 1 Gbps.
const DEFAULT_DOWNLOAD_MAX_BYTES_PER_SEC: u64 = 125_000_000;

/// Size of the chunks the split download is streamed and throttled with.
const DOWNLOAD_CHUNK_NUM_BYTES: usize = 1 << 20;

#[derive(Debug)]
enum SplitApiError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl SplitApiError {
    fn into_response(self) -> warp::reply::Response {
        let (status_code, message) = match self {
            SplitApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            SplitApiError::NotFound(message) => (StatusCode::NOT_FOUND, message),
            SplitApiError::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
        };
        warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "message": message })),
            status_code,
        )
        .into_response()
    }
}

impl From<MetastoreError> for SplitApiError {
    fn from(error: MetastoreError) -> Self {
        match error {
            MetastoreError::NotFound(entity) => SplitApiError::NotFound(entity.to_string()),
            other => SplitApiError::Internal(other.to_string()),
        }
    }
}

/// A split listed in the metastore can be missing from the storage, e.g. once the janitor has
/// deleted it.
fn storage_error(context: &str) -> impl FnOnce(StorageError) -> SplitApiError + '_ {
    move |error| {
        let message = format!("{context}: {error}");
        if error.kind() == StorageErrorKind::NotFound {
            SplitApiError::NotFound(message)
        } else {
            SplitApiError::Internal(message)
        }
    }
}

fn internal_error(context: &str) -> impl FnOnce(anyhow::Error) -> SplitApiError + '_ {
    move |error| SplitApiError::Internal(format!("{context}: {error:#}"))
}

/// Returns the storage of the index and the metadata of the split.
async fn resolve_split(
    metastore: &MetastoreServiceClient,
    storage_resolver: &StorageResolver,
    index_id: &str,
    split_id: &str,
) -> Result<(Arc<dyn Storage>, SplitMetadata), SplitApiError> {
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await?
        .deserialize_index_metadata()?;
    let list_splits_query = ListSplitsQuery::for_index(index_metadata.index_uid.clone())
        .with_split_ids(vec![split_id.to_string()]);
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;
    let split_metadata = metastore
        .list_splits(list_splits_request)
        .await?
        .collect_splits_metadata()
        .await?
        .into_iter()
        .find(|split_metadata| split_metadata.split_id == split_id)
        .ok_or_else(|| {
            SplitApiError::NotFound(format!(
                "split `{split_id}` not found in index `{index_id}`"
            ))
        })?;
    let storage = storage_resolver
        .resolve(index_metadata.index_uri())
        .await
        .map_err(|error| {
            SplitApiError::Internal(format!("failed to resolve the index storage: {error}"))
        })?;
    Ok((storage, split_metadata))
}

/// Space used by one field of the split, summed over its segments, in bytes.
#[derive(Debug, Default, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FieldSpaceUsage {
    pub field: String,
    pub termdict: u64,
    pub postings: u64,
    pub positions: u64,
    pub fieldnorms: u64,
    /// Fast field column and dictionary, plus the column metadata.
    pub fast_field: u64,
    /// Values (dictionary ordinals for text fields) of the fast field column.
    pub fast_field_column: u64,
    /// Dictionary of the fast field, for text and bytes fields.
    pub fast_field_dictionary: u64,
    pub total: u64,
}

/// Space used by each data structure of the split, in bytes.
#[derive(Debug, Default, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SplitComponentsSpaceUsage {
    pub termdict: u64,
    pub postings: u64,
    pub positions: u64,
    pub fast_fields: u64,
    pub fieldnorms: u64,
    pub store: u64,
    pub deletes: u64,
    /// Sum of the components above: the split file also holds the hotcache and the footer.
    pub total: u64,
}

/// Space usage of a split. Per field sizes do not include the doc store, which is shared by all
/// the fields.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SplitSpaceUsage {
    pub index_id: String,
    pub split_id: String,
    pub num_docs: u64,
    pub num_segments: usize,
    /// Size of the split file.
    pub split_num_bytes: u64,
    pub components: SplitComponentsSpaceUsage,
    /// Fields, largest first.
    pub fields: Vec<FieldSpaceUsage>,
}

fn summarize_space_usage(
    index_id: String,
    split_id: String,
    split_num_bytes: u64,
    space_usage: &SearcherSpaceUsage,
) -> SplitSpaceUsage {
    let mut components = SplitComponentsSpaceUsage::default();
    let mut fields: BTreeMap<String, FieldSpaceUsage> = BTreeMap::new();
    let mut num_docs = 0u64;

    for segment in space_usage.segments() {
        num_docs += u64::from(segment.num_docs());
        components.termdict += segment.termdict().total().get_bytes();
        components.postings += segment.postings().total().get_bytes();
        components.positions += segment.positions().total().get_bytes();
        components.fast_fields += segment.fast_fields().total().get_bytes();
        components.fieldnorms += segment.fieldnorms().total().get_bytes();
        components.store += segment.store().total().get_bytes();
        components.deletes += segment.deletes().get_bytes();
        components.total += segment.total().get_bytes();

        for field_usage in segment.termdict().fields() {
            field_space_usage(&mut fields, field_usage.field_name()).termdict +=
                field_usage.total().get_bytes();
        }
        for field_usage in segment.postings().fields() {
            field_space_usage(&mut fields, field_usage.field_name()).postings +=
                field_usage.total().get_bytes();
        }
        for field_usage in segment.positions().fields() {
            field_space_usage(&mut fields, field_usage.field_name()).positions +=
                field_usage.total().get_bytes();
        }
        for field_usage in segment.fieldnorms().fields() {
            field_space_usage(&mut fields, field_usage.field_name()).fieldnorms +=
                field_usage.total().get_bytes();
        }
        for field_usage in segment.fast_fields().fields() {
            let field = field_space_usage(&mut fields, field_usage.field_name());
            field.fast_field += field_usage.total().get_bytes();
            if let Some(column_num_bytes) = field_usage.column_num_bytes() {
                field.fast_field_column += column_num_bytes.get_bytes();
            }
            if let Some(dictionary_num_bytes) = field_usage.dictionary_num_bytes() {
                field.fast_field_dictionary += dictionary_num_bytes.get_bytes();
            }
        }
    }
    let mut fields: Vec<FieldSpaceUsage> = fields
        .into_values()
        .map(|mut field| {
            field.total = field.termdict
                + field.postings
                + field.positions
                + field.fieldnorms
                + field.fast_field;
            field
        })
        .collect();
    fields.sort_by(|left, right| {
        right
            .total
            .cmp(&left.total)
            .then_with(|| left.field.cmp(&right.field))
    });
    SplitSpaceUsage {
        index_id,
        split_id,
        num_docs,
        num_segments: space_usage.segments().len(),
        split_num_bytes,
        components,
        fields,
    }
}

fn field_space_usage<'a>(
    fields: &'a mut BTreeMap<String, FieldSpaceUsage>,
    field_name: &str,
) -> &'a mut FieldSpaceUsage {
    fields
        .entry(field_name.to_string())
        .or_insert_with(|| FieldSpaceUsage {
            field: field_name.to_string(),
            ..Default::default()
        })
}

async fn split_space_usage(
    index_id: String,
    split_id: String,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> Result<SplitSpaceUsage, SplitApiError> {
    let (storage, split_metadata) =
        resolve_split(&metastore, &storage_resolver, &index_id, &split_id).await?;
    let split_path = PathBuf::from(split_file(&split_id));
    let footer_offsets = &split_metadata.footer_offsets;
    let footer_bytes = storage
        .get_slice(
            &split_path,
            footer_offsets.start as usize..footer_offsets.end as usize,
        )
        .await
        .map_err(storage_error("failed to fetch the split footer"))?;
    let (hotcache, bundle_storage) = BundleStorage::open_from_split_data(
        storage,
        split_path,
        FileSlice::new(Arc::new(footer_bytes)),
    )
    .map_err(internal_error("failed to open the split bundle"))?;

    // tantivy computes the space usage synchronously: the reads that the hotcache does not answer
    // block on the storage, on a thread where blocking is allowed.
    let runtime_handle = Handle::current();
    let space_usage = tokio::task::spawn_blocking(move || -> anyhow::Result<SearcherSpaceUsage> {
        let directory = BlockingStorageDirectory::new(Arc::new(bundle_storage), runtime_handle);
        let hot_directory = HotDirectory::open(directory, hotcache.read_bytes()?)?;
        let index = Index::open(hot_directory)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(reader.searcher().space_usage()?)
    })
    .await
    .map_err(|error| SplitApiError::Internal(format!("space usage task failed: {error}")))?
    .map_err(internal_error("failed to compute the split space usage"))?;

    Ok(summarize_space_usage(
        index_id,
        split_id,
        footer_offsets.end,
        &space_usage,
    ))
}

#[utoipa::path(
    get,
    tag = "Debug",
    path = "/splits/{index_id}/{split_id}/space-usage",
    responses(
        (status = 200, description = "Space used by each field and data structure of the split.", body = SplitSpaceUsage),
        (status = 404, description = "The index or the split does not exist."),
    ),
    params(
        ("index_id" = String, Path, description = "The index ID."),
        ("split_id" = String, Path, description = "The split ID."),
    )
)]
/// Get the space used by each field and data structure of a split.
///
/// Only the hotcache and the metadata of the split are read from the object storage, not the
/// whole split.
pub(super) fn split_space_usage_handler(
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("splits" / String / String / "space-usage")
        .and(warp::get())
        .and(with_arg(metastore))
        .and(with_arg(storage_resolver))
        .then(
            |index_id: String,
             split_id: String,
             metastore: MetastoreServiceClient,
             storage_resolver: StorageResolver| async move {
                match split_space_usage(index_id, split_id, metastore, storage_resolver).await {
                    Ok(space_usage) => warp::reply::json(&space_usage).into_response(),
                    Err(error) => error.into_response(),
                }
            },
        )
}

#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
struct SplitDownloadQueryParams {
    /// Maximum download rate in bytes per second. Defaults to 125000000 (1 Gbps).
    max_bytes_per_sec: Option<u64>,
}

/// Paces a stream to an average rate.
struct Throttle {
    started_at: Instant,
    max_bytes_per_sec: u64,
    num_bytes: u64,
}

impl Throttle {
    fn new(max_bytes_per_sec: u64) -> Self {
        Self {
            started_at: Instant::now(),
            max_bytes_per_sec,
            num_bytes: 0,
        }
    }

    /// Waits until `num_bytes` more bytes can be sent without exceeding the rate.
    async fn wait_for(&mut self, num_bytes: usize) {
        self.num_bytes += num_bytes as u64;
        let target_elapsed =
            Duration::from_secs_f64(self.num_bytes as f64 / self.max_bytes_per_sec as f64);
        let elapsed = self.started_at.elapsed();
        if target_elapsed > elapsed {
            tokio::time::sleep(target_elapsed - elapsed).await;
        }
    }
}

/// Streams `reader` in chunks, at most `max_bytes_per_sec` on average.
fn throttled_stream(
    reader: Box<dyn AsyncRead + Send + Unpin>,
    max_bytes_per_sec: u64,
) -> impl Stream<Item = io::Result<Bytes>> + Send + 'static {
    // Chunks hold at most one second of data, so that slow rates still stream steadily.
    let chunk_num_bytes = DOWNLOAD_CHUNK_NUM_BYTES.min(max_bytes_per_sec as usize);
    let throttle = Throttle::new(max_bytes_per_sec);
    futures::stream::try_unfold(
        (reader, throttle),
        move |(mut reader, mut throttle)| async move {
            let mut chunk = BytesMut::with_capacity(chunk_num_bytes);
            while chunk.len() < chunk_num_bytes {
                if reader.read_buf(&mut chunk).await? == 0 {
                    break;
                }
            }
            if chunk.is_empty() {
                return Ok(None);
            }
            throttle.wait_for(chunk.len()).await;
            Ok(Some((chunk.freeze(), (reader, throttle))))
        },
    )
}

/// Body of the responses of the REST server.
///
/// warp cannot stream a response body: the split download is served by
/// [`SplitDownloadLayer`], in front of warp, and every response is boxed into this type.
pub(crate) type RestBody = UnsyncBoxBody<Bytes, BoxError>;

fn full_body(bytes: impl Into<Bytes>) -> RestBody {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

fn error_response(error: SplitApiError) -> http::Response<RestBody> {
    let (status_code, message) = match error {
        SplitApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
        SplitApiError::NotFound(message) => (StatusCode::NOT_FOUND, message),
        SplitApiError::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
    };
    let body = serde_json::to_vec(&serde_json::json!({ "message": message }))
        .expect("a JSON object should serialize");
    let mut response = http::Response::new(full_body(body));
    *response.status_mut() = status_code;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// Returns the index ID and the split ID of a split download path:
/// `/api/developer/splits/{index_id}/{split_id}/download`.
fn parse_split_download_path(path: &str) -> Option<(&str, &str)> {
    let mut segments = path
        .strip_prefix("/api/developer/splits/")?
        .strip_suffix("/download")?
        .split('/');
    let index_id = segments.next().filter(|index_id| !index_id.is_empty())?;
    let split_id = segments.next().filter(|split_id| !split_id.is_empty())?;
    if segments.next().is_some() {
        return None;
    }
    Some((index_id, split_id))
}

async fn download_split(
    index_id: String,
    split_id: String,
    query: Option<String>,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> Result<http::Response<RestBody>, SplitApiError> {
    let query_params: SplitDownloadQueryParams = serde_qs::from_str(query.as_deref().unwrap_or(""))
        .map_err(|error| SplitApiError::BadRequest(format!("invalid query parameters: {error}")))?;
    let max_bytes_per_sec = query_params
        .max_bytes_per_sec
        .unwrap_or(DEFAULT_DOWNLOAD_MAX_BYTES_PER_SEC);
    if max_bytes_per_sec == 0 {
        return Err(SplitApiError::BadRequest(
            "`max_bytes_per_sec` must be greater than 0".to_string(),
        ));
    }
    let (storage, split_metadata) =
        resolve_split(&metastore, &storage_resolver, &index_id, &split_id).await?;
    let split_file_name = split_file(&split_id);
    let split_num_bytes = split_metadata.footer_offsets.end;
    let reader = storage
        .get_slice_stream(
            &PathBuf::from(&split_file_name),
            0..split_num_bytes as usize,
        )
        .await
        .map_err(storage_error("failed to read the split"))?;
    let frames = throttled_stream(reader, max_bytes_per_sec)
        .map_ok(Frame::data)
        .map_err(BoxError::from);
    let mut response = http::Response::new(StreamBody::new(frames).boxed_unsync());
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(CONTENT_LENGTH, HeaderValue::from(split_num_bytes));
    let content_disposition = format!("attachment; filename=\"{split_file_name}\"");
    if let Ok(content_disposition) = HeaderValue::from_str(&content_disposition) {
        headers.insert(CONTENT_DISPOSITION, content_disposition);
    }
    Ok(response)
}

/// Download a split file from the object storage, throttled to `max_bytes_per_sec`.
///
/// Served by [`SplitDownloadLayer`]: this function only documents the endpoint.
#[utoipa::path(
    get,
    tag = "Debug",
    path = "/splits/{index_id}/{split_id}/download",
    responses(
        (status = 200, description = "The split file, as `application/octet-stream`."),
        (status = 400, description = "Invalid query parameters."),
        (status = 404, description = "The index or the split does not exist."),
    ),
    params(
        ("index_id" = String, Path, description = "The index ID."),
        ("split_id" = String, Path, description = "The split ID."),
        SplitDownloadQueryParams,
    )
)]
#[allow(dead_code)]
pub(super) fn split_download_handler() {}

/// Tower layer serving `GET /api/developer/splits/{index_id}/{split_id}/download` with a streamed
/// body, and passing every other request to the inner service.
///
/// It goes outside the compression layer: the split file is already compressed, and its
/// `Content-Length` must stay exact.
#[derive(Clone)]
pub(crate) struct SplitDownloadLayer {
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
}

impl SplitDownloadLayer {
    pub(crate) fn new(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
    ) -> Self {
        Self {
            metastore,
            storage_resolver,
        }
    }
}

impl<S> Layer<S> for SplitDownloadLayer {
    type Service = SplitDownloadService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SplitDownloadService {
            inner,
            metastore: self.metastore.clone(),
            storage_resolver: self.storage_resolver.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SplitDownloadService<S> {
    inner: S,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for SplitDownloadService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>, Error = Infallible>,
    S::Future: Send + 'static,
    ResBody: http_body::Body<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<BoxError>,
{
    type Response = http::Response<RestBody>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        if request.method() == Method::GET
            && let Some((index_id, split_id)) = parse_split_download_path(request.uri().path())
        {
            let index_id = index_id.to_string();
            let split_id = split_id.to_string();
            let query = request.uri().query().map(str::to_string);
            let metastore = self.metastore.clone();
            let storage_resolver = self.storage_resolver.clone();
            return Box::pin(async move {
                let response =
                    download_split(index_id, split_id, query, metastore, storage_resolver)
                        .await
                        .unwrap_or_else(error_response);
                Ok(response)
            });
        }
        let response_future = self.inner.call(request);
        Box::pin(async move {
            let response = response_future.await?;
            Ok(response.map(|body| body.map_err(Into::into).boxed_unsync()))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::path::Path;

    use quickwit_common::ServiceStream;
    use quickwit_common::uri::Uri;
    use quickwit_directories::write_hotcache;
    use quickwit_metastore::{
        IndexMetadata, IndexMetadataResponseExt, ListSplitsResponseExt, Split, SplitState,
    };
    use quickwit_proto::metastore::{
        IndexMetadataResponse, ListSplitsResponse, MetastoreServiceClient, MockMetastoreService,
    };
    use quickwit_storage::SplitPayloadBuilder;
    use tantivy::directory::MmapDirectory;
    use tantivy::schema::{FAST, STORED, STRING, Schema, TEXT};
    use tantivy::{IndexWriter, doc};
    use tower::ServiceExt;

    use super::*;

    const INDEX_URI: &str = "ram:///indexes/test-index";

    /// Builds a split of `num_docs` documents, writes it to the index storage of `resolver`, and
    /// returns its footer offsets.
    async fn put_test_split(
        storage_resolver: &StorageResolver,
        split_id: &str,
        num_docs: usize,
    ) -> Range<u64> {
        let tempdir = tempfile::tempdir().unwrap();
        let mut schema_builder = Schema::builder();
        let event_id = schema_builder.add_text_field("event.id", STRING | FAST | STORED);
        let message = schema_builder.add_text_field("message", TEXT | STORED);
        let index = Index::create_in_dir(tempdir.path(), schema_builder.build()).unwrap();
        let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
        for doc_id in 0..num_docs {
            index_writer
                .add_document(doc!(
                    event_id => format!("event-{doc_id:08}"),
                    message => format!("message number {doc_id}"),
                ))
                .unwrap();
        }
        index_writer.commit().unwrap();
        index_writer.wait_merging_threads().unwrap();

        let mut hotcache = Vec::new();
        write_hotcache(MmapDirectory::open(tempdir.path()).unwrap(), &mut hotcache).unwrap();
        let split_files: Vec<PathBuf> = std::fs::read_dir(tempdir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| !path.file_name().unwrap().to_string_lossy().starts_with('.'))
            .collect();
        let split_payload =
            SplitPayloadBuilder::get_split_payload(&split_files, &[], &hotcache).unwrap();
        let footer_range = split_payload.footer_range.clone();
        let storage = storage_resolver
            .resolve(&Uri::for_test(INDEX_URI))
            .await
            .unwrap();
        storage
            .put(Path::new(&split_file(split_id)), Box::new(split_payload))
            .await
            .unwrap();
        footer_range
    }

    /// Metastore holding the test index and, if given, one split.
    fn test_metastore(split_opt: Option<(&str, Range<u64>)>) -> MetastoreServiceClient {
        let index_metadata = IndexMetadata::for_test("test-index", INDEX_URI);
        let index_uid = index_metadata.index_uid.clone();
        let splits: Vec<Split> = split_opt
            .into_iter()
            .map(|(split_id, footer_offsets)| Split {
                split_state: SplitState::Published,
                split_metadata: SplitMetadata {
                    index_uid: index_uid.clone(),
                    split_id: split_id.to_string(),
                    footer_offsets,
                    ..Default::default()
                },
                update_timestamp: 0,
                publish_timestamp: None,
            })
            .collect();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        mock_metastore.expect_list_splits().returning(move |_| {
            let response = ListSplitsResponse::try_from_splits(splits.clone()).unwrap();
            Ok(ServiceStream::from(vec![Ok(response)]))
        });
        MetastoreServiceClient::from_mock(mock_metastore)
    }

    #[test]
    fn test_parse_split_download_path() {
        assert_eq!(
            parse_split_download_path("/api/developer/splits/my-index/01ABC/download"),
            Some(("my-index", "01ABC"))
        );
        assert_eq!(
            parse_split_download_path("/api/developer/splits/my-index/01ABC/space-usage"),
            None
        );
        assert_eq!(
            parse_split_download_path("/api/developer/splits/my-index/download"),
            None
        );
        assert_eq!(
            parse_split_download_path("/api/developer/splits//01ABC/download"),
            None
        );
        assert_eq!(
            parse_split_download_path("/api/developer/splits/a/b/c/download"),
            None
        );
    }

    #[tokio::test]
    async fn test_throttled_stream() {
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let reader: Box<dyn AsyncRead + Send + Unpin> =
            Box::new(std::io::Cursor::new(data.clone()));
        let started_at = std::time::Instant::now();
        let chunks: Vec<Bytes> = throttled_stream(reader, 1_000_000)
            .try_collect()
            .await
            .unwrap();
        let elapsed = started_at.elapsed();

        assert_eq!(chunks.concat(), data);
        // 300 KB at 1 MB/s.
        assert!(
            elapsed >= Duration::from_millis(280),
            "elapsed: {elapsed:?}"
        );
        assert!(elapsed < Duration::from_secs(2), "elapsed: {elapsed:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_split_space_usage_handler() {
        let storage_resolver = StorageResolver::for_test();
        let footer_offsets = put_test_split(&storage_resolver, "split-1", 1_000).await;
        let metastore = test_metastore(Some(("split-1", footer_offsets.clone())));
        let handler = split_space_usage_handler(metastore, storage_resolver);

        let response = warp::test::request()
            .path("/splits/test-index/split-1/space-usage")
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 200, "{:?}", response.body());
        let space_usage: SplitSpaceUsage = serde_json::from_slice(response.body()).unwrap();

        assert_eq!(space_usage.index_id, "test-index");
        assert_eq!(space_usage.split_id, "split-1");
        assert_eq!(space_usage.num_docs, 1_000);
        assert_eq!(space_usage.split_num_bytes, footer_offsets.end);
        assert!(space_usage.components.store > 0);
        assert!(space_usage.components.total < space_usage.split_num_bytes);

        let event_id = space_usage
            .fields
            .iter()
            .find(|field| field.field == "event.id")
            .unwrap();
        assert!(event_id.termdict > 0);
        assert!(event_id.postings > 0);
        assert!(event_id.fast_field_dictionary > 0);
        assert_eq!(
            event_id.total,
            event_id.termdict
                + event_id.postings
                + event_id.positions
                + event_id.fieldnorms
                + event_id.fast_field
        );
        let message = space_usage
            .fields
            .iter()
            .find(|field| field.field == "message")
            .unwrap();
        assert!(message.positions > 0);
        assert_eq!(message.fast_field, 0);
    }

    #[tokio::test]
    async fn test_split_space_usage_handler_split_not_found() {
        let handler = split_space_usage_handler(test_metastore(None), StorageResolver::for_test());
        let response = warp::test::request()
            .path("/splits/test-index/missing/space-usage")
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 404);
    }

    fn download_service(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
    ) -> SplitDownloadService<
        impl Service<
            http::Request<String>,
            Response = http::Response<Full<Bytes>>,
            Error = Infallible,
            Future = impl Send + 'static,
        > + Clone,
    > {
        let inner = tower::service_fn(|_request: http::Request<String>| async {
            Ok::<_, Infallible>(http::Response::new(Full::new(Bytes::from_static(b"inner"))))
        });
        SplitDownloadLayer::new(metastore, storage_resolver).layer(inner)
    }

    async fn call(
        service: impl Service<
            http::Request<String>,
            Response = http::Response<RestBody>,
            Error = Infallible,
        >,
        uri: &str,
    ) -> (http::Response<()>, Bytes) {
        let request = http::Request::get(uri).body(String::new()).unwrap();
        let response = service.oneshot(request).await.unwrap();
        let (parts, body) = response.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        (http::Response::from_parts(parts, ()), bytes)
    }

    #[tokio::test]
    async fn test_split_download_layer() {
        let storage_resolver = StorageResolver::for_test();
        let footer_offsets = put_test_split(&storage_resolver, "split-1", 100).await;
        let metastore = test_metastore(Some(("split-1", footer_offsets.clone())));
        let storage = storage_resolver
            .resolve(&Uri::for_test(INDEX_URI))
            .await
            .unwrap();
        let split_bytes = storage
            .get_all(Path::new(&split_file("split-1")))
            .await
            .unwrap();

        let (response, body) = call(
            download_service(metastore.clone(), storage_resolver.clone()),
            "/api/developer/splits/test-index/split-1/download?max_bytes_per_sec=1000000000",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            footer_offsets.end.to_string()
        );
        assert_eq!(
            response.headers()[CONTENT_DISPOSITION],
            "attachment; filename=\"split-1.split\""
        );
        assert_eq!(body.as_ref(), split_bytes.as_slice());

        // Default rate.
        let (response, body) = call(
            download_service(metastore.clone(), storage_resolver.clone()),
            "/api/developer/splits/test-index/split-1/download",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body.as_ref(), split_bytes.as_slice());

        let (response, _) = call(
            download_service(metastore.clone(), storage_resolver.clone()),
            "/api/developer/splits/test-index/split-1/download?max_bytes_per_sec=0",
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let (response, _) = call(
            download_service(metastore.clone(), storage_resolver.clone()),
            "/api/developer/splits/test-index/split-1/download?unknown=1",
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let (response, _) = call(
            download_service(metastore.clone(), storage_resolver.clone()),
            "/api/developer/splits/test-index/missing/download",
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Other requests go to the inner service.
        let (response, body) = call(
            download_service(metastore, storage_resolver),
            "/api/developer/splits/test-index/split-1/space-usage",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body.as_ref(), b"inner");
    }
}
