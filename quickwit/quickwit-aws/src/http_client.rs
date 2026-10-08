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

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::config_bag::ConfigBag;
use quickwit_common::slow_poll::S3ScopeExt;

/// Wraps the HTTP client of the S3 client so that the tasks spawned while serving its
/// requests are recorded as `s3` (see
/// [`quickwit_common::slow_poll::configure_task_poll_attribution`]).
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
