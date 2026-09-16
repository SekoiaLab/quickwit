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

//! Instrumented DNS resolver used by the AWS/S3 HTTP client.
//!
//! The AWS SDK's default HTTP client performs a blocking `getaddrinfo` lookup on
//! every new connection, without any caching, and reports nothing about it.
//!
//! [`DirectDnsResolver`] resolves names exactly the same way -- through
//! [`tokio::net::lookup_host`], which goes through `getaddrinfo` and therefore
//! honors every NSS source configured on the host: DNS, `/etc/hosts`, mDNS,
//! LDAP, etc -- and only adds metrics. It exists to answer, on a real
//! deployment, how many of those blocking lookups actually happen and what each
//! one costs. Caching them is only worth doing if those two numbers say so.

use std::net::IpAddr;

use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};

use crate::metrics::DNS_METRICS;

/// Resolves a host through `getaddrinfo`, recording what the call cost and how it went.
async fn resolve_host(host: &str) -> Result<Vec<IpAddr>, std::io::Error> {
    let _timer = DNS_METRICS.resolve_duration_seconds.start_timer();
    let socket_addrs = tokio::net::lookup_host((host, 0)).await.inspect_err(|_| {
        DNS_METRICS.record_lookup(DnsLookupOutcome::Error);
    })?;
    let ip_addresses: Vec<IpAddr> = socket_addrs.map(|socket_addr| socket_addr.ip()).collect();
    if ip_addresses.is_empty() {
        DNS_METRICS.record_lookup(DnsLookupOutcome::Empty);
    } else {
        DNS_METRICS.record_lookup(DnsLookupOutcome::Resolved);
    }
    Ok(ip_addresses)
}

/// A [`ResolveDns`] implementation that measures lookups without changing them.
///
/// It resolves like the SDK's default resolver -- a blocking `getaddrinfo` on every new
/// connection -- and only adds the metrics.
#[derive(Debug, Clone, Default)]
pub struct DirectDnsResolver;

impl ResolveDns for DirectDnsResolver {
    fn resolve_dns<'a>(&'a self, host: &'a str) -> DnsFuture<'a> {
        let host = host.to_string();
        DnsFuture::new(async move {
            let ip_addresses = resolve_host(&host).await.map_err(ResolveDnsError::new)?;
            Ok(ip_addresses)
        })
    }
}

/// How a DNS lookup went.
#[derive(Clone, Copy, Debug)]
pub enum DnsLookupOutcome {
    /// A blocking `getaddrinfo` call resolved the host.
    Resolved,
    /// A blocking `getaddrinfo` call failed.
    Error,
    /// A blocking `getaddrinfo` call returned no address at all.
    Empty,
}

impl DnsLookupOutcome {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            DnsLookupOutcome::Resolved => "resolved",
            DnsLookupOutcome::Error => "error",
            DnsLookupOutcome::Empty => "empty",
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    #[tokio::test]
    #[serial(dns_metrics)]
    async fn test_direct_dns_resolver_resolves_localhost() {
        let resolver = DirectDnsResolver;
        let ip_addresses = resolver
            .resolve_dns("localhost")
            .await
            .expect("localhost should resolve");
        assert!(
            ip_addresses
                .iter()
                .any(|ip_address| ip_address.is_loopback()),
            "expected a loopback address, got {ip_addresses:?}"
        );
    }

    #[tokio::test]
    #[serial(dns_metrics)]
    async fn test_direct_dns_resolver_counts_every_lookup() {
        let resolver = DirectDnsResolver;
        let resolved_before = DNS_METRICS.lookup_count(DnsLookupOutcome::Resolved);

        for _ in 0..2 {
            resolver
                .resolve_dns("localhost")
                .await
                .expect("localhost should resolve");
        }
        // Nothing is cached, so every call pays for a lookup. This is the baseline any
        // future cache would be measured against.
        assert_eq!(
            DNS_METRICS.lookup_count(DnsLookupOutcome::Resolved),
            resolved_before + 2
        );
    }

    #[tokio::test]
    #[serial(dns_metrics)]
    async fn test_direct_dns_resolver_counts_failed_lookups() {
        let resolver = DirectDnsResolver;
        let errors_before = DNS_METRICS.lookup_count(DnsLookupOutcome::Error);

        resolver
            .resolve_dns("quickwit.invalid.")
            .await
            .expect_err("the `.invalid.` TLD is reserved and must not resolve");

        assert_eq!(
            DNS_METRICS.lookup_count(DnsLookupOutcome::Error),
            errors_before + 1
        );
    }
}
