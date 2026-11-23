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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    Histogram, IntCounter, IntCounterVec, new_counter, new_counter_vec, new_histogram,
};

/// Metrics for the DNS resolution performed when opening a connection to an AWS endpoint.
///
/// The resolver is installed on the process-wide [`crate::get_aws_config`], so this covers
/// every AWS client built from it -- S3, but also SQS and Kinesis on an indexer, and the
/// credential providers (STS, SSO, IMDS) -- not just object storage.
pub struct DnsMetrics {
    pub resolve_duration_seconds: Histogram,
    pub connection_attempts_total: IntCounter,
}

impl Default for DnsMetrics {
    fn default() -> Self {
        DnsMetrics {
            resolve_duration_seconds: new_histogram(
                "dns_resolve_duration_seconds",
                "Time spent in a `getaddrinfo` call resolving an AWS endpoint, for every AWS \
                 client in the process. Cache hits are not recorded.",
                "storage",
                vec![
                    0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ],
            ),
            connection_attempts_total: new_counter(
                "aws_connection_attempts_total",
                "Number of new TCP connections the AWS SDK HTTP client tried to open, for every \
                 AWS client in the process. Hyper asks the resolver for addresses once per new \
                 connection, so this counts resolver calls.",
                "storage",
                &[],
            ),
        }
    }
}

pub static DNS_METRICS: Lazy<DnsMetrics> = Lazy::new(DnsMetrics::default);

/// Metrics of the S3 client only, recorded by [`crate::http_client::S3RemoteIpInterceptor`].
pub struct S3Metrics {
    pub response_body_num_bytes: IntCounterVec<1>,
}

impl Default for S3Metrics {
    fn default() -> Self {
        S3Metrics {
            response_body_num_bytes: new_counter_vec(
                "object_storage_response_body_num_bytes_by_ip",
                "Bytes of S3 response bodies read by Quickwit, by remote IP of the connection \
                 that served them. Covers every S3 operation (GetObject bodies dominate). Bytes \
                 are counted as they are read, so a download dropped midway only counts what was \
                 read. Beyond a fixed number of distinct IPs per process, new IPs are reported as \
                 `other`; `unknown` means the connection address was not available.",
                "storage",
                &[],
                ["remote_ip"],
            ),
        }
    }
}

pub static S3_METRICS: Lazy<S3Metrics> = Lazy::new(S3Metrics::default);
