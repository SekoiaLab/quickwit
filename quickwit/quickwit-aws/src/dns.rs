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

//! Caching DNS resolver used by the AWS/S3 HTTP client.
//!
//! The AWS SDK's default HTTP client performs a blocking `getaddrinfo`
//! lookup on every new connection without any caching.
//!
//! [`CachingDnsResolver`] resolves names the same way (via
//! [`tokio::net::lookup_host`], which goes through `getaddrinfo` and
//! therefore honors every NSS source configured on the host: DNS,
//! `/etc/hosts`, mDNS, LDAP, etc), but keeps an in-memory cache of the
//! results so repeated lookups for the same host don't each pay for a
//! blocking `getaddrinfo` call.
//!
//! Cached entries never expire outright: once a host has been resolved at
//! least once, lookups always return the cached (possibly stale) addresses
//! immediately. Entries older than [`DNS_REFRESH_COOLDOWN`] instead trigger a
//! background refresh that updates the cache in place once it completes, so
//! callers are never blocked waiting on a fresh `getaddrinfo` call for a
//! host they already have an answer for.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use mini_moka::sync::Cache;
use rand::Rng;
use tokio::sync::watch;

use crate::metrics::DNS_METRICS;

// We refresh once every 5 seconds. S3's DNS varies very rapidly.
const DNS_REFRESH_COOLDOWN: Duration = Duration::from_secs(5);

// On the first call, we do not have any answer to give back,
// so we wait up to DNS_TIMEOUT seconds for an entry to have been populated
// by the background task.
const DNS_TIMEOUT: Duration = Duration::from_secs(1);

/// Maximum number of cached hostnames.
/// In practise this cache should only contain a few entries to connect to s3's endpoints.
const DNS_CACHE_SIZE: u64 = 1_024;

// An instant expressed as duration elapsed from a reference time, expressed in millisecs.
type IInstant = u64;

fn now() -> IInstant {
    static REFERENCE_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);
    Instant::now().duration_since(*REFERENCE_TIME).as_millis() as u64
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IpFamily {
    V4,
    V6,
}

/// The addresses a host resolved to, split by family.
#[derive(Debug, Default)]
struct ResolvedIps {
    ipv4s: Vec<Ipv4Addr>,
    ipv6s: Vec<Ipv6Addr>,
    /// The family of each address, in the order returned by `getaddrinfo`.
    family_order: Vec<IpFamily>,
}

impl ResolvedIps {
    fn is_empty(&self) -> bool {
        self.family_order.is_empty()
    }

    /// Returns the addresses of each family rotated by a random offset, keeping the family of
    /// each position.
    ///
    /// Hyper connects to the first address that accepts the connection, so returning the
    /// addresses in the same order every time would send every new connection to the same IP
    /// until the next refresh. The family order is preserved because hyper's happy eyeballs
    /// prefers the family of the first address.
    fn new_random_order(&self) -> Vec<IpAddr> {
        let mut rng = rand::rng();
        let mut random_offset = |len: usize| {
            if len == 0 {
                0
            } else {
                rng.random_range(0..len)
            }
        };
        let (ipv4s_head, ipv4s_tail) = self.ipv4s.split_at(random_offset(self.ipv4s.len()));
        let (ipv6s_head, ipv6s_tail) = self.ipv6s.split_at(random_offset(self.ipv6s.len()));
        let mut ipv4s = ipv4s_tail.iter().chain(ipv4s_head).copied().map(IpAddr::V4);
        let mut ipv6s = ipv6s_tail.iter().chain(ipv6s_head).copied().map(IpAddr::V6);
        self.family_order
            .iter()
            .map(|family| {
                let ip_opt = match family {
                    IpFamily::V4 => ipv4s.next(),
                    IpFamily::V6 => ipv6s.next(),
                };
                ip_opt.expect("family order should match the number of ips of each family")
            })
            .collect()
    }
}

impl FromIterator<IpAddr> for ResolvedIps {
    fn from_iter<I: IntoIterator<Item = IpAddr>>(ips: I) -> Self {
        let mut resolved_ips = ResolvedIps::default();
        for ip in ips {
            match ip {
                IpAddr::V4(ipv4) => {
                    resolved_ips.ipv4s.push(ipv4);
                    resolved_ips.family_order.push(IpFamily::V4);
                }
                IpAddr::V6(ipv6) => {
                    resolved_ips.ipv6s.push(ipv6);
                    resolved_ips.family_order.push(IpFamily::V6);
                }
            }
        }
        resolved_ips
    }
}

/// A cached DNS entry.
#[derive(Debug)]
struct DnsEntry {
    ip_addresses_rx: watch::Receiver<ResolvedIps>,
    ip_addresses_tx: watch::Sender<ResolvedIps>,
    next_resolve_attempt: AtomicU64,
}

impl DnsEntry {
    fn new() -> Self {
        let (ip_addresses_tx, ip_addresses_rx) =
            tokio::sync::watch::channel(ResolvedIps::default());
        DnsEntry {
            ip_addresses_rx,
            ip_addresses_tx,
            next_resolve_attempt: AtomicU64::new(0u64),
        }
    }

    fn should_resolve(&self) -> bool {
        let next_resolve_attempt: u64 = self.next_resolve_attempt.load(Ordering::Relaxed);
        let now = now();
        if now >= next_resolve_attempt {
            // Our current entry is stale. Let's see if we want to trigger
            // the refresh by atomically updating the next_resolve_attempt instant.
            //
            // Relaxed suffices on both arms: this CAS only elects a single winner.
            self.next_resolve_attempt
                .compare_exchange(
                    next_resolve_attempt,
                    now + DNS_REFRESH_COOLDOWN.as_millis() as u64,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        } else {
            false
        }
    }
}

/// A [`ResolveDns`] implementation that caches `getaddrinfo` lookups.
#[derive(Debug, Clone)]
pub struct CachingDnsResolver {
    cache: Arc<Cache<String, Arc<DnsEntry>>>,
}

impl Default for CachingDnsResolver {
    fn default() -> Self {
        let cache = Cache::builder().max_capacity(DNS_CACHE_SIZE).build();
        CachingDnsResolver {
            cache: Arc::new(cache),
        }
    }
}

fn spawn_refresh_dns(dns_entry: Arc<DnsEntry>, host: String) {
    tokio::task::spawn(async move {
        let timer = DNS_METRICS.resolve_duration_seconds.start_timer();
        let lookup_res = tokio::net::lookup_host((host.as_str(), 0)).await;
        timer.observe_duration();
        let Ok(socket_addrs) = lookup_res else {
            quickwit_common::rate_limited_error!(limit_per_min = 10, %host, "failed to refresh DNS");
            dns_entry.next_resolve_attempt.store(0, Ordering::Relaxed);
            return;
        };
        let ips: ResolvedIps = socket_addrs.map(|socket_addr| socket_addr.ip()).collect();
        if ips.is_empty() {
            dns_entry.next_resolve_attempt.store(0, Ordering::Relaxed);
            return;
        }
        let _ = dns_entry.ip_addresses_tx.send(ips);
    });
}

impl ResolveDns for CachingDnsResolver {
    fn resolve_dns<'a>(&'a self, host: &'a str) -> DnsFuture<'a> {
        // Hyper's `HttpConnector` resolves the host on every new connection, and only then.
        DNS_METRICS.connection_attempts_total.inc();
        let cache = self.cache.clone();
        let host = host.to_string();
        let dns_entry_opt: Option<Arc<DnsEntry>> = cache.get(&host);
        // First insertion CAN trigger several DNS call, but this is not a problem.
        let dns_entry: Arc<DnsEntry> = if let Some(dns_entry) = dns_entry_opt {
            dns_entry
        } else {
            let dns_entry = Arc::new(DnsEntry::new());
            cache.insert(host.clone(), dns_entry.clone());
            dns_entry
        };
        if dns_entry.should_resolve() {
            spawn_refresh_dns(dns_entry.clone(), host.clone());
        }
        DnsFuture::new(async move {
            {
                // Happy path! If we have ips, we return directly.
                let resolved_ips = dns_entry.ip_addresses_rx.borrow();
                if !resolved_ips.is_empty() {
                    return Ok(resolved_ips.new_random_order());
                }
            }
            let mut ip_addresses_rx = dns_entry.ip_addresses_rx.clone();
            let ips: Vec<IpAddr> = tokio::time::timeout(DNS_TIMEOUT, async move {
                ip_addresses_rx
                    .wait_for(|resolved_ips| !resolved_ips.is_empty())
                    .await
                    .map_err(ResolveDnsError::new)
                    .map(|resolved_ips| resolved_ips.new_random_order())
            })
            .await
            .map_err(ResolveDnsError::new)??;
            Ok(ips)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_caching_dns_resolver_resolves_localhost() {
        let resolver = CachingDnsResolver::default();
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
    async fn test_caching_dns_resolver_caches_lookups() {
        let resolver = CachingDnsResolver::default();
        let first = resolver
            .resolve_dns("localhost")
            .await
            .expect("localhost should resolve");
        assert!(resolver.cache.get(&"localhost".to_string()).is_some());
        let mut second = resolver
            .resolve_dns("localhost")
            .await
            .expect("localhost should resolve from cache");
        // The order is shuffled on every lookup.
        let mut first = first;
        first.sort();
        second.sort();
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn test_caching_dns_resolver_shuffles_cached_ips() {
        let resolver = CachingDnsResolver::default();
        let cached_ips: Vec<IpAddr> = (1..=12).map(|i| IpAddr::from([10, 0, 0, i])).collect();
        let dns_entry = DnsEntry::new();
        dns_entry
            .ip_addresses_tx
            .send(cached_ips.iter().copied().collect())
            .unwrap();
        // Prevents a background refresh from overwriting the cached ips.
        dns_entry
            .next_resolve_attempt
            .store(u64::MAX, Ordering::Relaxed);
        resolver
            .cache
            .insert("s3.test".to_string(), Arc::new(dns_entry));

        let mut first_ips = std::collections::HashSet::new();
        for _ in 0..100 {
            let mut ips = resolver.resolve_dns("s3.test").await.unwrap();
            first_ips.insert(ips[0]);
            ips.sort();
            assert_eq!(ips, cached_ips);
        }
        // With 12 ips and 100 lookups, the probability of seeing fewer than 6 distinct first ips
        // is negligible.
        assert!(first_ips.len() >= 6, "{first_ips:?}");
    }

    #[test]
    fn test_resolved_ips_new_random_order_keeps_family_order() {
        let ipv6 = |i: u16| IpAddr::from([0x2001, 0xdb8, 0, 0, 0, 0, 0, i]);
        let ipv4 = |i: u8| IpAddr::from([10, 0, 0, i]);
        let original_ips: Vec<IpAddr> = vec![
            ipv6(1),
            ipv6(2),
            ipv6(3),
            ipv4(1),
            ipv6(4),
            ipv4(2),
            ipv4(3),
            ipv4(4),
        ];
        let resolved_ips: ResolvedIps = original_ips.iter().copied().collect();
        let mut sorted_original_ips = original_ips.clone();
        sorted_original_ips.sort();

        let mut first_ips = std::collections::HashSet::new();
        for _ in 0..100 {
            let mut ips = resolved_ips.new_random_order();
            first_ips.insert(ips[0]);
            for (ip, original_ip) in ips.iter().zip(&original_ips) {
                assert_eq!(ip.is_ipv4(), original_ip.is_ipv4(), "{ips:?}");
            }
            ips.sort();
            assert_eq!(ips, sorted_original_ips);
        }
        // The first position is always IPv6, shuffled among the 4 IPv6 addresses.
        assert!(first_ips.iter().all(|ip| ip.is_ipv6()));
        assert_eq!(first_ips.len(), 4);
    }

    #[test]
    fn test_resolved_ips_empty() {
        let resolved_ips = ResolvedIps::default();
        assert!(resolved_ips.is_empty());
        assert!(resolved_ips.new_random_order().is_empty());
    }
}
