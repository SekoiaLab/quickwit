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

//! Process-wide limits on S3 downloads: the number of GetObject requests in flight and the
//! download bandwidth. Both are shared by all buckets and all S3 storages of the process, and
//! both serve requests in FIFO order.
//!
//! A GetObject request is admitted ([`admit_get_object`]) once it holds a connection permit and,
//! when the bandwidth is limited, the first window of its response, in that order: the limiter
//! only grants bandwidth to requests that can use it right away. The body of the response then
//! holds the permit until it ends or is dropped ([`GetObjectAdmission::into_body`]).
//!
//! The bandwidth is limited by a single token bucket ([`DownloadLimiter`]).
//!
//! Where the bytes are charged matters. Delaying the reads of a response body only slows its
//! sender down once the socket receive buffer of the connection is full: up to a receive window
//! of data crosses the network at full speed whether or not the body is read. Many byte ranges
//! fetched by searches are smaller than that. So every GET is charged in two parts:
//! - before the request is sent, the bytes that will arrive regardless of reads, at most
//!   [`FIRST_WINDOW_NUM_BYTES`]. They are refunded if no response arrives;
//! - the rest of the body as it is read, [`READ_QUANTUM_NUM_BYTES`] at a time. Past the first
//!   window, TCP flow control slows the sender down to the rate at which the body is read.
//!
//! When a body ends, the bytes acquired for it but not received are refunded. When it is dropped
//! midway, they are not: they may be sitting in the socket buffer already.
//!
//! The bucket can go into debt: an acquisition only waits for the balance to be non-negative, then
//! subtracts its whole size. A request larger than the burst is therefore admitted as soon as the
//! previous debt is repaid, and the long-run rate is exact.
//!
//! Configured with `QW_S3_MAX_GET_OBJECT_CONNECTIONS` and `QW_S3_MAX_DOWNLOAD_BYTES_PER_SEC` (for
//! instance `4GB`), both unlimited when unset.

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use aws_smithy_types::body::{Error as BoxError, SdkBody};
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;
use bytesize::ByteSize;
use http_body::{Frame, SizeHint};
use once_cell::sync::Lazy;
use quickwit_common::metrics::{GaugeGuard, OwnedGaugeGuard};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::Instant;
use tracing::info;

use crate::metrics::STORAGE_METRICS;

const MAX_GET_OBJECT_CONNECTIONS_ENV_KEY: &str = "QW_S3_MAX_GET_OBJECT_CONNECTIONS";

const MAX_DOWNLOAD_BYTES_PER_SEC_ENV_KEY: &str = "QW_S3_MAX_DOWNLOAD_BYTES_PER_SEC";

/// Number of bytes that can accumulate while no download is running, as a duration at the
/// configured rate. It must exceed the resolution of the tokio timer (1ms): a waiter that
/// oversleeps would otherwise lose the bytes refilled meanwhile.
const BURST_DURATION: Duration = Duration::from_millis(10);

/// Number of bytes of a response that can arrive before its body is read: the receive window of
/// the connection, plus the read-ahead buffer of the HTTP client (about 400KB).
///
/// With TCP autotuning, the socket receive buffer grows up to `net.ipv4.tcp_rmem[2]` (6MiB by
/// default) as long as the application keeps up, and the window advertised to the sender is only
/// a fraction of it, as part of the buffer goes to per-packet overhead. Overestimating this only
/// charges bytes a little earlier than they arrive; underestimating it lets bytes through
/// uncharged.
const FIRST_WINDOW_NUM_BYTES: u64 = 4 << 20;

/// Number of bytes acquired at once while a body is read past its first window. Larger quanta
/// mean fewer acquisitions, but more bytes acquired ahead of being read.
///
/// Unlike [`FIRST_WINDOW_NUM_BYTES`], set by the kernel, this is only a pacing step: past the
/// first window, every byte read lets one more byte in.
const READ_QUANTUM_NUM_BYTES: u64 = 1 << 20;

/// Maximum number of GetObject attempts in flight at any time. Unlimited when
/// `QW_S3_MAX_GET_OBJECT_CONNECTIONS` is not set. An invalid value is reported as an error
/// when the storage is resolved, see [`validate_download_limits`].
static MAX_GET_OBJECT_CONNECTIONS: Lazy<Result<usize, String>> = Lazy::new(|| {
    let value_opt = std::env::var(MAX_GET_OBJECT_CONNECTIONS_ENV_KEY).ok();
    let max_connections_res = parse_max_get_object_connections(value_opt.as_deref());
    if let (Some(_), Ok(max_connections)) = (&value_opt, &max_connections_res) {
        info!(
            max_connections,
            "using environment variable `{MAX_GET_OBJECT_CONNECTIONS_ENV_KEY}` value"
        );
    }
    max_connections_res
});

fn parse_max_get_object_connections(value_opt: Option<&str>) -> Result<usize, String> {
    let Some(value_str) = value_opt else {
        return Ok(Semaphore::MAX_PERMITS);
    };
    match value_str.trim().parse::<usize>() {
        Ok(max_connections) if (1..=Semaphore::MAX_PERMITS).contains(&max_connections) => {
            Ok(max_connections)
        }
        _ => Err(format!(
            "environment variable `{MAX_GET_OBJECT_CONNECTIONS_ENV_KEY}` must be an integer \
             between 1 and {}, got `{value_str}`",
            Semaphore::MAX_PERMITS
        )),
    }
}

/// Semaphore limiting the number of GetObject attempts in flight at any time.
static GET_OBJECT_CONNECTION_SEMAPHORE: Lazy<Semaphore> = Lazy::new(|| {
    let num_permits = MAX_GET_OBJECT_CONNECTIONS
        .clone()
        .expect("the GetObject connection limit should be validated when resolving the storage");
    Semaphore::new(num_permits)
});

/// The process-wide download limiter, `None` when the bandwidth is not limited. An invalid
/// configuration is reported as an error when the storage is resolved, see
/// [`validate_download_limits`].
static DOWNLOAD_LIMITER: Lazy<Result<Option<DownloadLimiter>, String>> = Lazy::new(|| {
    let value_opt = std::env::var(MAX_DOWNLOAD_BYTES_PER_SEC_ENV_KEY).ok();
    let bytes_per_sec_opt = parse_max_download_bytes_per_sec(value_opt.as_deref())?;
    if let Some(bytes_per_sec) = bytes_per_sec_opt {
        info!(
            bytes_per_sec,
            "using environment variable `{MAX_DOWNLOAD_BYTES_PER_SEC_ENV_KEY}` value"
        );
    }
    Ok(bytes_per_sec_opt.map(DownloadLimiter::new))
});

fn parse_max_download_bytes_per_sec(value_opt: Option<&str>) -> Result<Option<u64>, String> {
    let Some(value_str) = value_opt else {
        return Ok(None);
    };
    match value_str.trim().parse::<ByteSize>() {
        Ok(bytes_per_sec) if bytes_per_sec.as_u64() > 0 => Ok(Some(bytes_per_sec.as_u64())),
        _ => Err(format!(
            "environment variable `{MAX_DOWNLOAD_BYTES_PER_SEC_ENV_KEY}` must be a positive \
             number of bytes per second, for instance `4GB`, got `{value_str}`"
        )),
    }
}

/// Checks the configuration of the download limits, so that an invalid value fails the
/// resolution of the storage rather than its first download.
pub(crate) fn validate_download_limits() -> Result<(), String> {
    MAX_GET_OBJECT_CONNECTIONS.as_ref().map_err(String::clone)?;
    DOWNLOAD_LIMITER.as_ref().map_err(String::clone)?;
    Ok(())
}

/// Admits a GetObject request to `bucket` for `range_opt` (`None` for a whole object): acquires
/// a connection permit, then, when the bandwidth is limited, reserves the first window of the
/// response.
///
/// The permit comes first so that the limiter only grants bandwidth to requests that can use it
/// right away. Reserving first, requests stuck waiting for a permit would hold bandwidth that
/// the bodies being read, which hold the permits, then wait for.
///
/// The permits are shared by all buckets; the gauges are labelled by bucket to tell which ones
/// hold or wait for them. A request only counts as in use once it is admitted.
pub(crate) async fn admit_get_object(
    bucket: &str,
    range_opt: Option<&Range<usize>>,
) -> GetObjectAdmission {
    let mut waiting_guard = OwnedGaugeGuard::from_gauge(
        STORAGE_METRICS
            .object_storage_get_object_connections_waiting
            .with_label_values([bucket]),
    );
    waiting_guard.add(1);
    let permit = GET_OBJECT_CONNECTION_SEMAPHORE
        .acquire()
        .await
        .expect("the semaphore should never be closed");
    drop(waiting_guard);
    let limiter_opt = DOWNLOAD_LIMITER.as_ref().ok().and_then(Option::as_ref);
    let reservation_opt = match limiter_opt {
        Some(limiter) => {
            let num_bytes_opt = range_opt.map(|range| range.len() as u64);
            Some(limiter.reserve(num_bytes_opt).await)
        }
        None => None,
    };
    let mut in_use_guard = OwnedGaugeGuard::from_gauge(
        STORAGE_METRICS
            .object_storage_get_object_connections_in_use
            .with_label_values([bucket]),
    );
    in_use_guard.add(1);
    GetObjectAdmission {
        connection_permit: ConnectionPermit {
            _permit: permit,
            _in_use_guard: in_use_guard,
        },
        reservation_opt,
    }
}

/// An admitted GetObject request, see [`admit_get_object`]. Dropping it before the response
/// arrives releases the connection permit and gives back the reserved bytes.
pub(crate) struct GetObjectAdmission {
    connection_permit: ConnectionPermit,
    reservation_opt: Option<DownloadReservation>,
}

impl GetObjectAdmission {
    /// The response has arrived: returns its body, which holds the connection permit until it
    /// ends or is dropped and, when the bandwidth is limited, charges the bytes it yields.
    pub(crate) fn into_body(self, byte_stream: ByteStream) -> ByteStream {
        let throttle_opt = self.reservation_opt.map(DownloadReservation::into_throttle);
        AdmittedBody::new_byte_stream(byte_stream, Some(self.connection_permit), throttle_opt)
    }
}

/// A permit from `GET_OBJECT_CONNECTION_SEMAPHORE`, counted as in use for its bucket.
struct ConnectionPermit {
    _permit: SemaphorePermit<'static>,
    _in_use_guard: OwnedGaugeGuard,
}

/// A token bucket counting bytes, whose waiters are served in FIFO order.
struct DownloadLimiter {
    bytes_per_sec: u64,
    burst_num_bytes: i64,
    /// Serializes the acquisitions: the holder is the head of the queue and the only one waiting
    /// for the balance to become non-negative. The tokio mutex is fair, so the others are served
    /// in the order they arrived.
    queue: tokio::sync::Mutex<()>,
    /// Never held across an await point, so refunds can be made from `Drop`.
    balance: Mutex<Balance>,
}

struct Balance {
    num_bytes: i64,
    refilled_at: Instant,
}

impl DownloadLimiter {
    fn new(bytes_per_sec: u64) -> Self {
        let burst_num_bytes = (bytes_per_sec as u128 * BURST_DURATION.as_nanos() / 1_000_000_000)
            .clamp(1, i64::MAX as u128) as i64;
        DownloadLimiter {
            bytes_per_sec,
            burst_num_bytes,
            queue: tokio::sync::Mutex::new(()),
            balance: Mutex::new(Balance {
                num_bytes: burst_num_bytes,
                refilled_at: Instant::now(),
            }),
        }
    }

    fn lock_balance(&self) -> std::sync::MutexGuard<'_, Balance> {
        self.balance
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Charges `num_bytes` if the balance is non-negative. Otherwise, returns how long to wait
    /// before it will be.
    fn try_charge(&self, num_bytes: u64) -> Result<(), Duration> {
        let now = Instant::now();
        let mut balance = self.lock_balance();
        let elapsed_nanos = now
            .saturating_duration_since(balance.refilled_at)
            .as_nanos();
        let refill = (elapsed_nanos * self.bytes_per_sec as u128 / 1_000_000_000)
            .min(i64::MAX as u128) as i64;
        balance.num_bytes = balance
            .num_bytes
            .saturating_add(refill)
            .min(self.burst_num_bytes);
        balance.refilled_at = now;
        if balance.num_bytes >= 0 {
            balance.num_bytes = balance
                .num_bytes
                .saturating_sub(num_bytes.min(i64::MAX as u64) as i64);
            return Ok(());
        }
        let deficit = balance.num_bytes.unsigned_abs() as u128;
        let wait_nanos = (deficit * 1_000_000_000).div_ceil(self.bytes_per_sec as u128);
        Err(Duration::from_nanos(wait_nanos.min(u64::MAX as u128) as u64))
    }

    /// Adds `num_bytes` to the balance, without waiting: positive to give back bytes acquired
    /// but not downloaded, negative to charge bytes downloaded but not acquired.
    fn adjust(&self, num_bytes: i64) {
        let mut balance = self.lock_balance();
        balance.num_bytes = balance
            .num_bytes
            .saturating_add(num_bytes)
            .min(self.burst_num_bytes);
    }

    /// Waits for the turn of this acquisition and for the balance to be non-negative, then
    /// charges `num_bytes`. Dropping the future before it completes charges nothing.
    async fn acquire(&self, num_bytes: u64) {
        // Fast path: nobody is waiting and the balance is non-negative.
        if let Ok(_queue_guard) = self.queue.try_lock()
            && self.try_charge(num_bytes).is_ok()
        {
            return;
        }
        let start = Instant::now();
        let mut waiting_guard =
            GaugeGuard::from_gauge(&STORAGE_METRICS.object_storage_download_limiter_waiting);
        waiting_guard.add(1);
        let _queue_guard = self.queue.lock().await;
        while let Err(wait) = self.try_charge(num_bytes) {
            tokio::time::sleep(wait).await;
        }
        STORAGE_METRICS
            .object_storage_download_limiter_wait_seconds
            .observe(start.elapsed().as_secs_f64());
    }

    /// Acquires the first window of a GET before it is sent: [`FIRST_WINDOW_NUM_BYTES`], or the
    /// size of the response if it is known to be smaller. The bytes are refunded if the
    /// returned reservation is dropped before a response arrives, for instance when the request
    /// fails or is cancelled.
    async fn reserve(&'static self, num_bytes_opt: Option<u64>) -> DownloadReservation {
        let num_bytes = num_bytes_opt
            .unwrap_or(FIRST_WINDOW_NUM_BYTES)
            .min(FIRST_WINDOW_NUM_BYTES);
        self.acquire(num_bytes).await;
        DownloadReservation {
            limiter: self,
            num_bytes,
        }
    }
}

/// Bytes acquired for a GET that has not received a response yet, see
/// [`DownloadLimiter::reserve`].
struct DownloadReservation {
    limiter: &'static DownloadLimiter,
    num_bytes: u64,
}

impl DownloadReservation {
    /// The response has arrived: its body is charged from the reserved bytes, then as it is read.
    fn into_throttle(mut self) -> Throttle {
        let credit = std::mem::take(&mut self.num_bytes).min(i64::MAX as u64) as i64;
        Throttle {
            limiter: self.limiter,
            credit,
            acquire_fut_opt: None,
        }
    }
}

impl Drop for DownloadReservation {
    fn drop(&mut self) {
        if self.num_bytes > 0 {
            self.limiter
                .adjust(self.num_bytes.min(i64::MAX as u64) as i64);
        }
    }
}

type AcquireFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

/// Charges the bytes of a response body to a [`DownloadLimiter`].
struct Throttle {
    limiter: &'static DownloadLimiter,
    /// Bytes acquired and not received yet. A frame can be larger than the credit, in which case
    /// it goes negative and the next acquisition pays back the difference.
    credit: i64,
    /// The acquisition in progress and its number of bytes.
    acquire_fut_opt: Option<(AcquireFuture, u64)>,
}

impl Throttle {
    /// Waits for more bytes from the limiter if the credit is exhausted.
    ///
    /// A credit of exactly zero does not acquire anything: it is the common case of a body that
    /// fit in its reservation, whose end has not been polled yet. Acquiring then would make it
    /// wait in the queue only to give the bytes back at the end of the body. If more bytes come,
    /// the credit goes negative and the next acquisition pays them back.
    fn poll_acquire(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.credit >= 0 {
            return Poll::Ready(());
        }
        let limiter = self.limiter;
        let num_bytes = READ_QUANTUM_NUM_BYTES + self.credit.unsigned_abs();
        let (acquire_fut, num_bytes) = self.acquire_fut_opt.get_or_insert_with(|| {
            let acquire_fut: AcquireFuture =
                Box::pin(async move { limiter.acquire(num_bytes).await });
            (acquire_fut, num_bytes)
        });
        ready!(acquire_fut.as_mut().poll(cx));
        self.credit = self.credit.saturating_add(*num_bytes as i64);
        self.acquire_fut_opt = None;
        Poll::Ready(())
    }

    fn consume(&mut self, num_bytes: usize) {
        self.credit = self.credit.saturating_sub(num_bytes as i64);
    }

    /// The body has ended: gives back the bytes acquired but not received, or charges the bytes
    /// received but not acquired.
    fn settle(mut self) {
        self.limiter.adjust(std::mem::take(&mut self.credit));
    }
}

impl Drop for Throttle {
    fn drop(&mut self) {
        // Dropped midway: the bytes received but not acquired are charged. Those acquired but not
        // received are not given back, they may be in the socket buffer already. An acquisition in
        // progress is dropped with `acquire_fut_opt` and charges nothing.
        if self.credit < 0 {
            self.limiter.adjust(self.credit);
        }
    }
}

/// Body of an admitted GetObject response, see [`GetObjectAdmission::into_body`].
struct AdmittedBody {
    inner: SdkBody,
    /// Released at the end of the body.
    connection_permit_opt: Option<ConnectionPermit>,
    /// Settled at the end of the body. `None` when the bandwidth is not limited.
    throttle_opt: Option<Throttle>,
}

impl AdmittedBody {
    fn new_byte_stream(
        byte_stream: ByteStream,
        connection_permit_opt: Option<ConnectionPermit>,
        throttle_opt: Option<Throttle>,
    ) -> ByteStream {
        let body = AdmittedBody {
            inner: byte_stream.into_inner(),
            connection_permit_opt,
            throttle_opt,
        };
        ByteStream::new(SdkBody::from_body_1_x(body))
    }

    fn end(&mut self) {
        if let Some(throttle) = self.throttle_opt.take() {
            throttle.settle();
        }
        self.connection_permit_opt = None;
    }
}

impl http_body::Body for AdmittedBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if let Some(throttle) = &mut this.throttle_opt {
            ready!(throttle.poll_acquire(cx));
        }
        let frame_opt = ready!(Pin::new(&mut this.inner).poll_frame(cx));
        if let Some(throttle) = &mut this.throttle_opt
            && let Some(Ok(frame)) = &frame_opt
            && let Some(data) = frame.data_ref()
        {
            throttle.consume(data.len());
        }
        if frame_opt.is_none() || http_body::Body::is_end_stream(&this.inner) {
            this.end();
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// Returns a body yielding `num_bytes` in frames of `frame_num_bytes`, which does not know
    /// it has ended until its last frame has been polled, like a chunked response.
    fn streamed_body(num_bytes: usize, frame_num_bytes: usize) -> ByteStream {
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = (0..num_bytes)
            .step_by(frame_num_bytes)
            .map(|start| {
                let end = (start + frame_num_bytes).min(num_bytes);
                Ok(Frame::data(Bytes::from(vec![7u8; end - start])))
            })
            .collect();
        let body = http_body_util::StreamBody::new(futures::stream::iter(frames));
        ByteStream::new(SdkBody::from_body_1_x(body))
    }

    /// Returns the body of a response admitted with `reservation`, without a connection permit.
    fn throttle(reservation: DownloadReservation, byte_stream: ByteStream) -> ByteStream {
        AdmittedBody::new_byte_stream(byte_stream, None, Some(reservation.into_throttle()))
    }

    fn leak(limiter: DownloadLimiter) -> &'static DownloadLimiter {
        Box::leak(Box::new(limiter))
    }

    #[test]
    fn test_parse_max_get_object_connections() {
        assert_eq!(
            parse_max_get_object_connections(None),
            Ok(Semaphore::MAX_PERMITS)
        );
        assert_eq!(parse_max_get_object_connections(Some("2048")), Ok(2048));
        assert_eq!(parse_max_get_object_connections(Some(" 1 ")), Ok(1));
        let max_permits = Semaphore::MAX_PERMITS.to_string();
        assert_eq!(
            parse_max_get_object_connections(Some(&max_permits)),
            Ok(Semaphore::MAX_PERMITS)
        );
        let too_many_permits = (Semaphore::MAX_PERMITS + 1).to_string();
        for invalid_value in ["0", "-1", "", "abc", "1.5", too_many_permits.as_str()] {
            parse_max_get_object_connections(Some(invalid_value)).unwrap_err();
        }
    }

    #[tokio::test]
    async fn test_connection_permit_held_until_end_of_body() {
        // Buckets of their own: the gauges are shared with the other tests.
        let in_use = |bucket| {
            STORAGE_METRICS
                .object_storage_get_object_connections_in_use
                .with_label_values([bucket])
                .get()
        };
        // Released when the admission is dropped before a response arrives.
        let admission = admit_get_object("test-permit-gauges-a", None).await;
        assert_eq!(in_use("test-permit-gauges-a"), 1);
        assert_eq!(in_use("test-permit-gauges-b"), 0);
        drop(admission);
        assert_eq!(in_use("test-permit-gauges-a"), 0);

        // Released at the end of a body reporting its end, though the body is still alive.
        let admission = admit_get_object("test-permit-gauges-a", None).await;
        let mut body = admission.into_body(ByteStream::from(vec![7u8; 1_000]));
        assert_eq!(in_use("test-permit-gauges-a"), 1);
        body.next().await.unwrap().unwrap();
        assert_eq!(in_use("test-permit-gauges-a"), 0);

        // Released at the end of a streamed body, once its end is polled.
        let admission = admit_get_object("test-permit-gauges-a", None).await;
        let mut body = admission.into_body(streamed_body(2_000, 1_000));
        body.next().await.unwrap().unwrap();
        body.next().await.unwrap().unwrap();
        assert_eq!(in_use("test-permit-gauges-a"), 1);
        assert!(body.next().await.is_none());
        assert_eq!(in_use("test-permit-gauges-a"), 0);

        // Released when the body is dropped midway.
        let admission = admit_get_object("test-permit-gauges-a", None).await;
        let mut body = admission.into_body(streamed_body(2_000, 1_000));
        body.next().await.unwrap().unwrap();
        drop(body);
        assert_eq!(in_use("test-permit-gauges-a"), 0);
    }

    #[test]
    fn test_parse_max_download_bytes_per_sec() {
        assert_eq!(parse_max_download_bytes_per_sec(None), Ok(None));
        assert_eq!(
            parse_max_download_bytes_per_sec(Some("4GB")),
            Ok(Some(4_000_000_000))
        );
        assert_eq!(
            parse_max_download_bytes_per_sec(Some(" 1000 ")),
            Ok(Some(1000))
        );
        assert!(parse_max_download_bytes_per_sec(Some("0")).is_err());
        assert!(parse_max_download_bytes_per_sec(Some("fast")).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_enforces_rate() {
        // 100MB/s, so a 1MB burst.
        let limiter = DownloadLimiter::new(100_000_000);
        let start = Instant::now();
        // The burst, then 99MB at 100MB/s.
        for _ in 0..100 {
            limiter.acquire(1_000_000).await;
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(980), "{elapsed:?}");
        assert!(elapsed <= Duration::from_millis(1_000), "{elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_admits_request_larger_than_burst() {
        let limiter = DownloadLimiter::new(100_000_000);
        let start = Instant::now();
        // Larger than the burst: admitted right away, then repaid by the next acquisition.
        limiter.acquire(50_000_000).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        limiter.acquire(1).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(490), "{elapsed:?}");
        assert!(elapsed <= Duration::from_millis(500), "{elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_serves_waiters_in_order() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        // Puts the bucket 1s in debt.
        limiter.acquire(1_010_000).await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for task_id in 0..10 {
            let order = order.clone();
            handles.push(tokio::spawn(async move {
                limiter.acquire(10_000).await;
                order.lock().unwrap().push(task_id);
            }));
            // Lets the task enqueue before the next one.
            tokio::task::yield_now().await;
        }
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), (0..10).collect::<Vec<_>>());
    }

    /// Returns how long an acquisition of 1 byte waits, which is how long the debt of the limiter
    /// takes to be repaid.
    async fn debt_duration(limiter: &DownloadLimiter) -> Duration {
        let start = Instant::now();
        limiter.acquire(1).await;
        start.elapsed()
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_reservation_first_window() {
        // 1MB/s, so a 10KB burst.
        let limiter = leak(DownloadLimiter::new(1_000_000));
        // A 50MB range only reserves its first window.
        let reservation = limiter.reserve(Some(50_000_000)).await;
        let _body = throttle(reservation, ByteStream::default());
        let debt = debt_duration(limiter).await;
        let expected = Duration::from_secs_f64((FIRST_WINDOW_NUM_BYTES - 10_000) as f64 / 1e6);
        assert!(
            debt.abs_diff(expected) <= Duration::from_millis(1),
            "{debt:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_reservation_refunded_without_response() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        drop(limiter.reserve(None).await);
        assert_eq!(debt_duration(limiter).await, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_cancelled_acquisition_charges_nothing() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        limiter.acquire(2_000_000).await;
        // Waits for the 2s debt, cancelled after 1s.
        let acquire_res =
            tokio::time::timeout(Duration::from_secs(1), limiter.acquire(5_000_000)).await;
        assert!(acquire_res.is_err());
        let start = Instant::now();
        limiter.acquire(1).await;
        let elapsed = start.elapsed();
        assert!(elapsed <= Duration::from_millis(1_001), "{elapsed:?}");
    }

    /// Reads `body` with a reservation for a response of `num_bytes_opt`, then checks that
    /// exactly `body_num_bytes` were charged at 10MB/s: the next acquisition goes through once
    /// they are paid, minus the 100KB burst.
    async fn assert_body_charged(
        num_bytes_opt: Option<u64>,
        body: ByteStream,
        body_num_bytes: usize,
    ) {
        let limiter = leak(DownloadLimiter::new(10_000_000));
        let start = Instant::now();
        let reservation = limiter.reserve(num_bytes_opt).await;
        let bytes = throttle(reservation, body)
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(bytes.len(), body_num_bytes);
        debt_duration(limiter).await;
        let elapsed = start.elapsed();
        let expected =
            Duration::from_secs_f64(body_num_bytes.saturating_sub(100_000) as f64 / 10e6);
        assert!(
            elapsed.abs_diff(expected) <= Duration::from_millis(2),
            "{body_num_bytes}: {elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttled_body_charges_streamed_body() {
        // Past the first window, paced 1MiB at a time.
        assert_body_charged(None, streamed_body(30_000_000, 64_000), 30_000_000).await;
        // Smaller than the first window: the unused part is given back.
        assert_body_charged(None, streamed_body(5_000, 1_000), 5_000).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttled_body_charges_single_frame_body() {
        // A single frame overdrawing the credit, settled when the body reports its end.
        let body = ByteStream::from(vec![7u8; 50_000_000]);
        assert_body_charged(Some(50_000_000), body, 50_000_000).await;
        // A range that fits in its reservation: no acquisition beyond it.
        let body = ByteStream::from(vec![7u8; 1_000_000]);
        assert_body_charged(Some(1_000_000), body, 1_000_000).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttled_body_paces_reads() {
        // 10MB/s, so a 100KB burst.
        let limiter = leak(DownloadLimiter::new(10_000_000));
        let start = Instant::now();
        let reservation = limiter.reserve(None).await;
        let mut body = throttle(reservation, streamed_body(30_000_000, 64_000));
        // The first window comes without waiting.
        let mut num_bytes_read = 0;
        while num_bytes_read < FIRST_WINDOW_NUM_BYTES as usize {
            num_bytes_read += body.next().await.unwrap().unwrap().len();
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
        // The rest is paced: reading 10MB more ends once everything read so far, first window
        // included, is paid, minus the 100KB burst and give or take the quantum acquired ahead.
        while num_bytes_read < FIRST_WINDOW_NUM_BYTES as usize + 10_000_000 {
            num_bytes_read += body.next().await.unwrap().unwrap().len();
        }
        let elapsed = start.elapsed();
        let paid_num_bytes = (num_bytes_read - 100_000) as f64;
        let min_elapsed =
            Duration::from_secs_f64((paid_num_bytes - READ_QUANTUM_NUM_BYTES as f64) / 10e6);
        let max_elapsed = Duration::from_secs_f64(paid_num_bytes / 10e6);
        assert!(elapsed >= min_elapsed, "{elapsed:?}");
        assert!(elapsed <= max_elapsed, "{elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttled_body_dropped_midway_keeps_charge() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        let reservation = limiter.reserve(None).await;
        let mut body = throttle(reservation, streamed_body(5_000, 1_000));
        body.next().await.unwrap().unwrap();
        drop(body);
        // The reserved first window stays charged.
        let first_window = Duration::from_secs_f64(FIRST_WINDOW_NUM_BYTES as f64 / 1e6);
        assert!(debt_duration(limiter).await >= first_window - Duration::from_millis(10));
    }

    /// Measures the CPU cost of the limiter. Run with:
    /// `cargo test --release -p quickwit-storage download_limiter_cpu_cost -- --ignored
    /// --nocapture`
    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn download_limiter_cpu_cost() {
        const NUM_TASKS: u64 = 4_000;
        const ACQUISITIONS_PER_TASK: u64 = 250;

        for (label, bytes_per_sec, chunk_num_bytes) in [
            ("unthrottled", u64::MAX / 4, 1_000_000),
            // 5GB/s of 256KB requests: 20k acquisitions/s, all waiting their turn.
            ("throttled", 5_000_000_000, 256_000),
        ] {
            let limiter = leak(DownloadLimiter::new(bytes_per_sec));
            let total_num_bytes = Arc::new(AtomicU64::new(0));
            let cpu_start = cpu_time();
            let start = std::time::Instant::now();
            let handles: Vec<_> = (0..NUM_TASKS)
                .map(|_| {
                    let total_num_bytes = total_num_bytes.clone();
                    tokio::spawn(async move {
                        for _ in 0..ACQUISITIONS_PER_TASK {
                            limiter.acquire(chunk_num_bytes).await;
                            total_num_bytes.fetch_add(chunk_num_bytes, Ordering::Relaxed);
                        }
                    })
                })
                .collect();
            for handle in handles {
                handle.await.unwrap();
            }
            let elapsed = start.elapsed();
            let cpu = cpu_time() - cpu_start;
            let num_acquisitions = NUM_TASKS * ACQUISITIONS_PER_TASK;
            println!(
                "{label}: {num_acquisitions} acquisitions in {elapsed:?} ({:.0}/s, {:.2} GB/s), \
                 CPU {cpu:?} = {:.0} ns/acquisition, {:.3} cores",
                num_acquisitions as f64 / elapsed.as_secs_f64(),
                total_num_bytes.load(Ordering::Relaxed) as f64 / elapsed.as_secs_f64() / 1e9,
                cpu.as_nanos() as f64 / num_acquisitions as f64,
                cpu.as_secs_f64() / elapsed.as_secs_f64(),
            );
        }
    }

    fn cpu_time() -> Duration {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: `getrusage` fills the struct it is given.
        let usage = unsafe {
            libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
            usage.assume_init()
        };
        let to_duration = |time: libc::timeval| {
            Duration::from_secs(time.tv_sec as u64) + Duration::from_micros(time.tv_usec as u64)
        };
        to_duration(usage.ru_utime) + to_duration(usage.ru_stime)
    }
}
