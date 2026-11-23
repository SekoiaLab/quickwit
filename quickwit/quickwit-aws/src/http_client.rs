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

//! HTTP client wrapper attributing the background work of the S3 client's connection pool.
//!
//! The SDK's hyper client runs every connection (socket reads, TLS, HTTP parsing) in a task
//! spawned by the first request that needed it, and then shares it with every later request
//! through its pool. Running requests in [`S3ScopeExt::in_s3_scope`] records those tasks as
//! `s3` instead of attributing them to whichever caller opened the connection.
//!
//! Also holds [`S3RemoteIpInterceptor`], which breaks the S3 response bytes down by remote IP.

use std::collections::HashMap;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};
use std::task::{Context, Poll, ready};

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::connection::CaptureSmithyConnection;
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeDeserializationInterceptorContextMut;
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use quickwit_common::metrics::IntCounter;
use quickwit_common::slow_poll::S3ScopeExt;

use crate::metrics::S3_METRICS;

/// Wraps the HTTP client of the S3 client so that the tasks spawned while serving its
/// requests are recorded as `s3` by S3 task poll attribution.
pub fn s3_http_client(http_client: SharedHttpClient) -> SharedHttpClient {
    SharedHttpClient::new(S3HttpClient { inner: http_client })
}

#[derive(Debug)]
struct S3HttpClient {
    inner: SharedHttpClient,
}

impl HttpClient for S3HttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        // The inner client caches its connectors, only the thin wrapper is created per call.
        SharedHttpConnector::new(S3HttpConnector {
            inner: self.inner.http_connector(settings, components),
        })
    }

    fn validate_base_client_config(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.inner
            .validate_base_client_config(runtime_components, cfg)
    }

    fn validate_final_config(
        &self,
        runtime_components: &RuntimeComponents,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.inner.validate_final_config(runtime_components, cfg)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.inner.connector_metadata()
    }
}

#[derive(Debug)]
struct S3HttpConnector {
    inner: SharedHttpConnector,
}

impl HttpConnector for S3HttpConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        // `call` itself runs inside the S3 scope too, in case it spawns eagerly.
        let inner = self.inner.clone();
        HttpConnectorFuture::new(async move { inner.call(request).await }.in_s3_scope())
    }
}

/// Maximum number of distinct `remote_ip` label values per process. S3 endpoints that rotate
/// through a large pool of addresses would otherwise grow the number of series without bound.
const MAX_REMOTE_IP_LABELS: usize = 256;

/// Returns the counter of response body bytes for `remote_ip_opt`.
fn response_body_counter(remote_ip_opt: Option<IpAddr>) -> IntCounter {
    static COUNTERS: LazyLock<Mutex<HashMap<IpAddr, IntCounter>>> = LazyLock::new(Default::default);

    let metric = &S3_METRICS.response_body_num_bytes;
    let Some(remote_ip) = remote_ip_opt else {
        return metric.with_label_values(["unknown"]);
    };
    let mut counters = COUNTERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(counter) = counters.get(&remote_ip) {
        return counter.clone();
    }
    if counters.len() >= MAX_REMOTE_IP_LABELS {
        return metric.with_label_values(["other"]);
    }
    let counter = metric.with_label_values([&remote_ip.to_string()]);
    counters.insert(remote_ip, counter.clone());
    counter
}

/// S3 client interceptor counting the response body bytes by the remote IP of the connection
/// that served them, see `object_storage_response_body_num_bytes_by_ip`.
///
/// It relies on the [`CaptureSmithyConnection`] that the SDK's connection poisoning interceptor
/// stores for every attempt, which the HTTP client fills with the connection's metadata.
#[derive(Debug, Default)]
pub struct S3RemoteIpInterceptor;

impl Intercept for S3RemoteIpInterceptor {
    fn name(&self) -> &'static str {
        "S3RemoteIpInterceptor"
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let remote_ip_opt = cfg
            .load::<CaptureSmithyConnection>()
            .and_then(CaptureSmithyConnection::get)
            .and_then(|connection| connection.remote_addr())
            .map(|socket_addr| socket_addr.ip());
        let counter = response_body_counter(remote_ip_opt);
        let response = context.response_mut();
        let body = response.take_body();
        *response.body_mut() = SdkBody::from_body_1_x(CountingBody {
            inner: body,
            counter,
        });
        Ok(())
    }
}

/// Response body adding the size of every data frame it yields to `counter`.
struct CountingBody {
    inner: SdkBody,
    counter: IntCounter,
}

impl http_body::Body for CountingBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let frame_opt = ready!(Pin::new(&mut self.inner).poll_frame(cx));
        if let Some(Ok(frame)) = &frame_opt
            && let Some(data) = frame.data_ref()
        {
            self.counter.inc_by(data.len() as u64);
        }
        Poll::Ready(frame_opt)
    }

    fn is_end_stream(&self) -> bool {
        http_body::Body::is_end_stream(&self.inner)
    }

    fn size_hint(&self) -> SizeHint {
        http_body::Body::size_hint(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::config::{Credentials, Region};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    const BODY: &[u8] = b"hello world";

    /// Serves `BODY` to every HTTP request received on `listener`.
    async fn serve_body(listener: TcpListener) {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let num_bytes = stream.read(&mut buffer).await.unwrap();
                    if num_bytes == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..num_bytes]);
                }
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: \
                     application/octet-stream\r\n\r\n",
                    BODY.len()
                );
                stream.write_all(headers.as_bytes()).await.unwrap();
                stream.write_all(BODY).await.unwrap();
            });
        }
    }

    #[tokio::test]
    async fn test_s3_remote_ip_interceptor_counts_body_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve_body(listener));

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(crate::aws_behavior_version())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("id", "secret", None, None, "test"))
            .endpoint_url(format!("http://127.0.0.1:{port}"))
            .force_path_style(true)
            .http_client(s3_http_client(crate::build_http_client()))
            .interceptor(S3RemoteIpInterceptor)
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let counter = S3_METRICS
            .response_body_num_bytes
            .with_label_values(["127.0.0.1"]);
        let num_bytes_before = counter.get();
        let output = s3_client
            .get_object()
            .bucket("bucket")
            .key("key")
            .send()
            .await
            .unwrap();
        let body = output.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], BODY);
        assert_eq!(counter.get() - num_bytes_before, BODY.len() as u64);
    }
}
