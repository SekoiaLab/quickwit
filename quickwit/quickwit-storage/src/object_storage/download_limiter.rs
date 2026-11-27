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

//! Process-wide limits on S3 downloads, shared by all buckets: the number of GetObject requests
//! in flight (`QW_S3_MAX_GET_OBJECT_CONNECTIONS`) and the download bandwidth
//! (`QW_S3_MAX_DOWNLOAD_BYTES_PER_SEC`, for instance `4GB`), both unlimited when unset.
//!
//! - Before a GetObject request is sent, [`admit_get_object`] waits for a connection permit, then
//!   reserves the first window of the response. The first window is the part of the response that
//!   arrives whether or not the body is read, at most [`FIRST_WINDOW_NUM_BYTES`]. The reservation
//!   is given back if no response arrives.
//! - The response body ([`GetObjectAdmission::into_body`]) keeps the connection permit until the
//!   body ends or is dropped. The bytes beyond the first window are acquired as the body is read,
//!   [`READ_QUANTUM_NUM_BYTES`] at a time: a frame that is not covered yet is held back until it
//!   is. When the body ends, the bytes acquired but not received are given back.
//! - The bandwidth limit is a token bucket ([`DownloadLimiter`]) built on a semaphore with one
//!   permit per byte. It is refilled according to the elapsed time, up to a burst of
//!   [`BURST_DURATION`]. Waiting acquisitions are served in FIFO order, and each refill wakes all
//!   the acquisitions it satisfies at once. While acquisitions are waiting, one of them (the refill
//!   leader) refills the bucket every [`REFILL_PERIOD`].

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Number of bytes that can accumulate while downloads use less than the configured rate, as a
/// duration at that rate. It must exceed [`REFILL_PERIOD`] by a wide margin: refills arriving late
/// would otherwise be capped. The burst is also at least [`FIRST_WINDOW_NUM_BYTES`], so that a
/// request does not wait for its own first window when bandwidth is available, and a refunded
/// first window always fits.
///
/// After a period below the rate, downloads can exceed it by up to the burst: 10ms of traffic, or
/// 1% over a second.
const BURST_DURATION: Duration = Duration::from_millis(10);

/// How often the waiting acquisitions get the bytes refilled since the last refill.
const REFILL_PERIOD: Duration = Duration::from_millis(1);

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

/// A token bucket counting bytes: a semaphore holding one permit per byte, refilled at the
/// configured rate, up to a burst.
///
/// See the module documentation for how acquisitions wait and how the bucket is refilled.
struct DownloadLimiter {
    bytes_per_sec: u64,
    /// See [`BURST_DURATION`].
    burst_num_bytes: u64,
    /// The bytes available, and the FIFO queue of the acquisitions waiting for more.
    permits: Semaphore,
    /// Time from which bytes are earned at the configured rate.
    clock_start: Instant,
    /// Bytes earned since `clock_start` that have been added to the bucket, or discarded because
    /// it was full.
    num_refilled_bytes: AtomicU64,
    /// Held by the refill leader: the waiting acquisition that refills the bucket periodically on
    /// behalf of all the waiting acquisitions. The others queue on it to take over.
    refill_leader: tokio::sync::Mutex<()>,
}

impl DownloadLimiter {
    fn new(bytes_per_sec: u64) -> Self {
        let burst_num_bytes = (bytes_per_sec as u128 * BURST_DURATION.as_nanos() / 1_000_000_000)
            .max(FIRST_WINDOW_NUM_BYTES as u128)
            .min(Semaphore::MAX_PERMITS as u128) as u64;
        DownloadLimiter {
            bytes_per_sec,
            burst_num_bytes,
            permits: Semaphore::new(burst_num_bytes as usize),
            clock_start: Instant::now(),
            num_refilled_bytes: AtomicU64::new(0),
            refill_leader: tokio::sync::Mutex::new(()),
        }
    }

    /// Adds the bytes earned since the last refill.
    ///
    /// The bytes earned since `clock_start` are a pure function of the time. A refill raises
    /// `num_refilled_bytes` to that number and adds the difference: concurrent refills add
    /// disjoint amounts, and since nothing is rounded but the total, nothing is lost however often
    /// the bucket is refilled.
    fn refill(&self) {
        let elapsed_nanos = self.clock_start.elapsed().as_nanos();
        let num_earned_bytes = (elapsed_nanos * self.bytes_per_sec as u128 / 1_000_000_000)
            .min(u64::MAX as u128) as u64;
        let num_refilled_bytes = self
            .num_refilled_bytes
            .fetch_max(num_earned_bytes, Ordering::AcqRel);
        if num_earned_bytes > num_refilled_bytes {
            self.add(num_earned_bytes - num_refilled_bytes);
        }
    }

    /// Adds `num_bytes` to the bucket, up to the burst. The semaphore hands them to the
    /// acquisitions at the front of its queue, if any.
    ///
    /// The cap does not shortchange the waiting acquisitions: while some are waiting, the
    /// semaphore has handed every permit to them, so none is available and the whole burst can be
    /// added at once, ten times what a refill period earns.
    fn add(&self, num_bytes: u64) {
        let num_available = self.permits.available_permits() as u64;
        let num_added = num_bytes.min(self.burst_num_bytes.saturating_sub(num_available));
        if num_added > 0 {
            self.permits.add_permits(num_added as usize);
        }
    }

    fn try_acquire(&self, num_permits: u32) -> bool {
        match self.permits.try_acquire_many(num_permits) {
            Ok(permit) => {
                permit.forget();
                true
            }
            Err(_) => false,
        }
    }

    /// Waits for `num_bytes`, at most `u32::MAX`. Dropping the future before it completes charges
    /// nothing.
    async fn acquire(&self, num_bytes: u64) {
        // The largest acquisitions are a first window or an oversized frame plus a quantum.
        let num_permits = num_bytes.min(u32::MAX as u64) as u32;
        if num_permits == 0 || self.try_acquire(num_permits) {
            return;
        }
        self.refill();
        if self.try_acquire(num_permits) {
            return;
        }
        let start = Instant::now();
        let mut waiting_guard =
            GaugeGuard::from_gauge(&STORAGE_METRICS.object_storage_download_limiter_waiting);
        waiting_guard.add(1);
        let acquire = self.permits.acquire_many(num_permits);
        tokio::pin!(acquire);
        tokio::select! {
            biased;
            permit_res = &mut acquire => {
                permit_res.expect("the semaphore should never be closed").forget();
            }
            never = self.lead_refills() => match never {},
        }
        STORAGE_METRICS
            .object_storage_download_limiter_wait_seconds
            .observe(start.elapsed().as_secs_f64());
    }

    /// Waits to become the refill leader, then refills the bucket every [`REFILL_PERIOD`]. Never
    /// returns: it is dropped when its acquisition completes, which hands the leadership over to
    /// the next waiting acquisition.
    async fn lead_refills(&self) -> std::convert::Infallible {
        let _leader_guard = self.refill_leader.lock().await;
        loop {
            tokio::time::sleep(REFILL_PERIOD).await;
            self.refill();
        }
    }

    /// Acquires the first window of a GET before it is sent: [`FIRST_WINDOW_NUM_BYTES`], or the
    /// size of the response if it is known to be smaller. The bytes are given back if the
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
        Throttle {
            limiter: self.limiter,
            credit: std::mem::take(&mut self.num_bytes),
            held_frame_opt: None,
        }
    }
}

impl Drop for DownloadReservation {
    fn drop(&mut self) {
        self.limiter.add(self.num_bytes);
    }
}

type AcquireFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

/// A frame larger than the credit, held until the bytes it lacks are acquired.
struct HeldFrame {
    frame: Frame<Bytes>,
    num_bytes: u64,
    num_acquired_bytes: u64,
    acquire_fut: AcquireFuture,
}

/// Charges the frames of a response body to a [`DownloadLimiter`].
struct Throttle {
    limiter: &'static DownloadLimiter,
    /// Bytes acquired and not received yet.
    credit: u64,
    held_frame_opt: Option<HeldFrame>,
}

impl Throttle {
    /// Charges `frame` to the credit. Returns it if the credit covers it. Otherwise, holds it and
    /// starts acquiring the bytes it lacks, plus [`READ_QUANTUM_NUM_BYTES`] for the next frames:
    /// see [`Self::poll_held_frame`].
    fn charge(&mut self, frame: Frame<Bytes>) -> Option<Frame<Bytes>> {
        let num_bytes = frame.data_ref().map(|data| data.len() as u64).unwrap_or(0);
        if num_bytes <= self.credit {
            self.credit -= num_bytes;
            return Some(frame);
        }
        let limiter = self.limiter;
        let num_acquired_bytes = num_bytes - self.credit + READ_QUANTUM_NUM_BYTES;
        let acquire_fut: AcquireFuture =
            Box::pin(async move { limiter.acquire(num_acquired_bytes).await });
        self.held_frame_opt = Some(HeldFrame {
            frame,
            num_bytes,
            num_acquired_bytes,
            acquire_fut,
        });
        None
    }

    /// Returns the held frame, if any, once the bytes it lacked are acquired.
    fn poll_held_frame(&mut self, cx: &mut Context<'_>) -> Poll<Option<Frame<Bytes>>> {
        let Some(held_frame) = &mut self.held_frame_opt else {
            return Poll::Ready(None);
        };
        ready!(held_frame.acquire_fut.as_mut().poll(cx));
        let held_frame = self
            .held_frame_opt
            .take()
            .expect("the frame should be held");
        self.credit = self.credit + held_frame.num_acquired_bytes - held_frame.num_bytes;
        Poll::Ready(Some(held_frame.frame))
    }

    /// The body has ended: gives back the bytes acquired but not received.
    fn settle(self) {
        self.limiter.add(self.credit);
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

    /// Releases the admission if the inner body has ended. Called after every frame, since a
    /// consumer may stop polling once `is_end_stream` returns true.
    fn end_if_ended(&mut self, frame_opt: &Option<Result<Frame<Bytes>, BoxError>>) {
        if frame_opt.is_none() || http_body::Body::is_end_stream(&self.inner) {
            if let Some(throttle) = self.throttle_opt.take() {
                throttle.settle();
            }
            self.connection_permit_opt = None;
        }
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
        let frame_opt = match &mut this.throttle_opt {
            // A frame held until its bytes are acquired is yielded before polling the next one:
            // meanwhile, the connection is not read, and TCP flow control slows the sender down.
            Some(throttle) if throttle.held_frame_opt.is_some() => {
                ready!(throttle.poll_held_frame(cx)).map(Ok)
            }
            Some(throttle) => match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
                Some(Ok(frame)) => match throttle.charge(frame) {
                    Some(frame) => Some(Ok(frame)),
                    // Polls the acquisition right away, so that it registers the waker.
                    None => ready!(throttle.poll_held_frame(cx)).map(Ok),
                },
                frame_or_error_opt => frame_or_error_opt,
            },
            None => ready!(Pin::new(&mut this.inner).poll_frame(cx)),
        };
        this.end_if_ended(&frame_opt);
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

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

    /// The burst of the limiters of the tests: their rates are low enough for the burst to be a
    /// first window.
    const BURST: u64 = FIRST_WINDOW_NUM_BYTES;

    /// Returns the time to wait for `num_bytes` at `bytes_per_sec`.
    fn duration_of(num_bytes: u64, bytes_per_sec: u64) -> Duration {
        Duration::from_secs_f64(num_bytes as f64 / bytes_per_sec as f64)
    }

    fn assert_around(elapsed: Duration, expected: Duration) {
        assert!(
            elapsed.abs_diff(expected) <= Duration::from_millis(2),
            "elapsed {elapsed:?}, expected {expected:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_enforces_rate() {
        let limiter = DownloadLimiter::new(100_000_000);
        let start = Instant::now();
        // The burst, then the rest at 100MB/s.
        for _ in 0..100 {
            limiter.acquire(1_000_000).await;
        }
        assert_around(
            start.elapsed(),
            duration_of(100_000_000 - BURST, 100_000_000),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_request_larger_than_burst() {
        let limiter = DownloadLimiter::new(100_000_000);
        let start = Instant::now();
        // Waits for the bytes beyond the burst.
        limiter.acquire(50_000_000).await;
        assert_around(
            start.elapsed(),
            duration_of(50_000_000 - BURST, 100_000_000),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_refills_while_idle_up_to_burst() {
        let limiter = DownloadLimiter::new(1_000_000);
        limiter.acquire(BURST).await;
        // Idle for longer than needed to refill the burst: only the burst accumulates.
        tokio::time::sleep(Duration::from_secs(60)).await;
        let start = Instant::now();
        limiter.acquire(BURST).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        limiter.acquire(1_000_000).await;
        assert_around(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_serves_waiters_in_order() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        limiter.acquire(BURST).await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for task_id in 0..10 {
            let order = order.clone();
            // Decreasing sizes: served by size rather than by arrival, they would complete in
            // reverse order.
            let num_bytes = 100_000 - task_id * 5_000;
            handles.push(tokio::spawn(async move {
                limiter.acquire(num_bytes).await;
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

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_grants_waiters_together() {
        // 100 waiters of 10KB at 1MB/s: each refill of 1ms covers one waiter, so they complete
        // 1ms apart, the last one after 1s.
        let limiter = leak(DownloadLimiter::new(1_000_000));
        limiter.acquire(BURST).await;
        let start = Instant::now();
        let handles: Vec<_> = (0..100)
            .map(|_| tokio::spawn(async move { limiter.acquire(10_000).await }))
            .collect();
        for handle in handles {
            handle.await.unwrap();
        }
        assert_around(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_refills_in_whole_bytes_without_loss() {
        // 1 byte every 3 microseconds: refilling every microsecond earns a fraction of a byte
        // each time, which must not be lost.
        let limiter = DownloadLimiter::new(333_333);
        limiter.acquire(BURST).await;
        let start = Instant::now();
        for _ in 0..30_000 {
            tokio::time::advance(Duration::from_micros(1)).await;
            limiter.refill();
        }
        // 30ms earned 10_000 bytes.
        assert_eq!(limiter.permits.available_permits(), 9_999);
        assert_eq!(start.elapsed(), Duration::from_millis(30));
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttled_body_holds_frame_until_acquired() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        limiter.acquire(BURST).await;
        let reservation = limiter.reserve(Some(1_000)).await;
        let start = Instant::now();
        // A single 2MB frame, 1KB of which is covered by the reservation: it is yielded once the
        // bytes it lacks are acquired, plus a quantum.
        let mut body = throttle(reservation, ByteStream::from(vec![7u8; 2_000_000]));
        let frame = body.next().await.unwrap().unwrap();
        assert_eq!(frame.len(), 2_000_000);
        assert_around(
            start.elapsed(),
            duration_of(2_000_000 - 1_000 + READ_QUANTUM_NUM_BYTES, 1_000_000),
        );
        // The body has ended: the quantum was given back.
        assert!(body.next().await.is_none());
        let start = Instant::now();
        limiter.acquire(READ_QUANTUM_NUM_BYTES).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_limiter_cancelled_acquisition_charges_nothing() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        limiter.acquire(BURST).await;
        // Cancelled after 1s, halfway through.
        let acquire_res =
            tokio::time::timeout(Duration::from_secs(1), limiter.acquire(2_000_000)).await;
        assert!(acquire_res.is_err());
        // The bytes refilled for the cancelled acquisition are available.
        let start = Instant::now();
        limiter.acquire(900_000).await;
        assert!(start.elapsed() <= Duration::from_millis(1));
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_reservation_first_window() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        let start = Instant::now();
        // A 50MB range only reserves its first window, which the burst covers.
        let reservation = limiter.reserve(Some(50_000_000)).await;
        let _body = throttle(reservation, ByteStream::default());
        assert_eq!(start.elapsed(), Duration::ZERO);
        limiter.acquire(1_000_000).await;
        assert_around(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn test_download_reservation_refunded_without_response() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        drop(limiter.reserve(None).await);
        let start = Instant::now();
        limiter.acquire(BURST).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    /// Reads `body` with a reservation for a response of `num_bytes_opt` at 10MB/s, then checks
    /// that the bytes charged are `body_num_bytes`: the next acquisition goes through once they
    /// are paid, minus the burst, give or take the quantum acquired ahead and given back at the
    /// end of the body.
    async fn assert_body_charged(
        num_bytes_opt: Option<u64>,
        body: ByteStream,
        body_num_bytes: u64,
    ) {
        let limiter = leak(DownloadLimiter::new(10_000_000));
        let start = Instant::now();
        let reservation = limiter.reserve(num_bytes_opt).await;
        let bytes = throttle(reservation, body)
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(bytes.len() as u64, body_num_bytes);
        limiter.acquire(1).await;
        let elapsed = start.elapsed();
        let min_elapsed = duration_of(body_num_bytes.saturating_sub(BURST), 10_000_000);
        let max_elapsed = duration_of(
            (body_num_bytes + READ_QUANTUM_NUM_BYTES).saturating_sub(BURST),
            10_000_000,
        ) + Duration::from_millis(2);
        assert!(
            elapsed >= min_elapsed && elapsed <= max_elapsed,
            "{body_num_bytes}: {elapsed:?} not in {min_elapsed:?}..{max_elapsed:?}"
        );
        // The unused part of the reservation was given back.
        if body_num_bytes < BURST {
            let start = Instant::now();
            limiter.acquire(BURST - body_num_bytes - 1).await;
            assert_eq!(start.elapsed(), Duration::ZERO);
        }
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
        // A single frame overdrawing the credit, charged as debt when the body reports its end.
        let body = ByteStream::from(vec![7u8; 50_000_000]);
        assert_body_charged(Some(50_000_000), body, 50_000_000).await;
        // A range that fits in its reservation: no acquisition beyond it.
        let body = ByteStream::from(vec![7u8; 1_000_000]);
        assert_body_charged(Some(1_000_000), body, 1_000_000).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttled_body_paces_reads() {
        let limiter = leak(DownloadLimiter::new(10_000_000));
        let start = Instant::now();
        let reservation = limiter.reserve(None).await;
        let mut body = throttle(reservation, streamed_body(30_000_000, 64_000));
        // The frames within the first window come without waiting.
        let mut num_bytes_read = 0;
        while num_bytes_read + 64_000 <= FIRST_WINDOW_NUM_BYTES {
            num_bytes_read += body.next().await.unwrap().unwrap().len() as u64;
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
        // The rest is paced: reading 10MB more takes about 1s, give or take the quantum acquired
        // ahead.
        while num_bytes_read < FIRST_WINDOW_NUM_BYTES + 10_000_000 {
            num_bytes_read += body.next().await.unwrap().unwrap().len() as u64;
        }
        let elapsed = start.elapsed();
        let min_elapsed = duration_of(num_bytes_read - BURST - READ_QUANTUM_NUM_BYTES, 10_000_000);
        let max_elapsed = duration_of(num_bytes_read - BURST + READ_QUANTUM_NUM_BYTES, 10_000_000);
        assert!(
            elapsed >= min_elapsed && elapsed <= max_elapsed,
            "{elapsed:?} not in {min_elapsed:?}..{max_elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttled_body_dropped_midway_keeps_charge() {
        let limiter = leak(DownloadLimiter::new(1_000_000));
        let reservation = limiter.reserve(None).await;
        let mut body = throttle(reservation, streamed_body(5_000, 1_000));
        body.next().await.unwrap().unwrap();
        drop(body);
        // The reserved first window stays charged: the bucket is empty.
        let start = Instant::now();
        limiter.acquire(1_000_000).await;
        assert_around(start.elapsed(), Duration::from_secs(1));
    }

    /// Measures how many acquisitions per second the limiter serves on a busy runtime when the
    /// bandwidth is not the bottleneck: busy tasks keep the workers occupied, so woken tasks wait
    /// to be scheduled, as on a loaded searcher. Run with:
    /// `cargo test --release -p quickwit-storage --lib download_limiter_loaded_runtime --
    /// --ignored --nocapture`
    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn download_limiter_loaded_runtime() {
        const NUM_BUSY_TASKS: usize = 16;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        for _ in 0..NUM_BUSY_TASKS {
            let stop = stop.clone();
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    spin(std::time::Duration::from_micros(200));
                    tokio::task::yield_now().await;
                }
            });
        }
        // Never binding: 1TB/s.
        let limiter = leak(DownloadLimiter::new(1_000_000_000_000));
        for (label, limiter_opt) in [("no limiter", None), ("limiter", Some(limiter))] {
            run_requests(label, limiter_opt).await;
        }
        stop.store(true, Ordering::Relaxed);
    }

    fn spin(duration: std::time::Duration) {
        let spin_start = std::time::Instant::now();
        while spin_start.elapsed() < duration {
            std::hint::spin_loop();
        }
    }

    async fn run_requests(label: &str, limiter_opt: Option<&'static DownloadLimiter>) {
        const NUM_REQUESTS: usize = 2_000;
        const ACQUISITIONS_PER_REQUEST: usize = 20;

        let max_waiting = Arc::new(AtomicU64::new(0));
        let start = std::time::Instant::now();
        let handles: Vec<_> = (0..NUM_REQUESTS)
            .map(|_| {
                let max_waiting = max_waiting.clone();
                tokio::spawn(async move {
                    for _ in 0..ACQUISITIONS_PER_REQUEST {
                        if let Some(limiter) = limiter_opt {
                            limiter.acquire(256_000).await;
                        }
                        let waiting = STORAGE_METRICS
                            .object_storage_download_limiter_waiting
                            .get();
                        max_waiting.fetch_max(waiting.max(0) as u64, Ordering::Relaxed);
                        // Sending the request continues the same poll: a waiter woken by the
                        // acquisition runs on this worker once the poll ends at the earliest.
                        spin(std::time::Duration::from_micros(50));
                        // The download itself.
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.await.unwrap();
        }
        let elapsed = start.elapsed();
        let num_acquisitions = NUM_REQUESTS * ACQUISITIONS_PER_REQUEST;
        println!(
            "{label}: {num_acquisitions} requests in {elapsed:?} = {:.0}/s, max waiting {}",
            num_acquisitions as f64 / elapsed.as_secs_f64(),
            max_waiting.load(Ordering::Relaxed),
        );
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
