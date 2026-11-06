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

use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use futures::Future;
use rand::Rng;
use tracing::{debug, warn};

pub trait Retryable {
    fn is_retryable(&self) -> bool {
        false
    }

    /// Whether the error signals that the service is throttling requests. Such errors are retried
    /// after longer delays, see [`RetryParams::compute_throttling_delay`].
    fn is_throttling(&self) -> bool {
        false
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Retry<E> {
    Permanent(E),
    Transient(E),
    Throttled(E),
}

impl<E> Retry<E> {
    pub fn into_inner(self) -> E {
        match self {
            Self::Transient(error) => error,
            Self::Permanent(error) => error,
            Self::Throttled(error) => error,
        }
    }
}

impl<E> Retryable for Retry<E> {
    fn is_retryable(&self) -> bool {
        match self {
            Retry::Permanent(_) => false,
            Retry::Transient(_) | Retry::Throttled(_) => true,
        }
    }

    fn is_throttling(&self) -> bool {
        matches!(self, Retry::Throttled(_))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RetryParams {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub max_attempts: usize,
    pub throttling_base_delay: Duration,
    pub throttling_max_delay: Duration,
}

impl RetryParams {
    /// Creates a new [`RetryParams`] instance using the same settings as the standard retry policy
    /// defined in the AWS SDK for Rust.
    pub fn standard() -> Self {
        Self {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(20),
            max_attempts: 3,
            throttling_base_delay: Duration::from_secs(1),
            throttling_max_delay: Duration::from_secs(20),
        }
    }

    /// Creates a new [`RetryParams`] instance using settings that are more aggressive than those of
    /// the standard policy for services that are more resilient to retries, usually managed
    /// cloud services.
    ///
    /// Throttled requests are retried later: a throttled request
    /// is often throttled again if retried right away.
    pub fn aggressive() -> Self {
        Self {
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(20),
            max_attempts: 5,
            throttling_base_delay: Duration::from_millis(750),
            throttling_max_delay: Duration::from_millis(1_800),
        }
    }

    /// Creates a new [`RetryParams`] instance that does not perform any retries.
    pub fn no_retries() -> Self {
        Self {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_attempts: 1,
            throttling_base_delay: Duration::ZERO,
            throttling_max_delay: Duration::ZERO,
        }
    }

    /// Computes the delay after which a new attempt should be performed. The randomized delay
    /// increases after each attempt (exponential backoff and full jitter). Implementation and
    /// default values originate from the Java SDK. See also: <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>.
    ///
    /// The caller should pass the number of attempts that have been performed so far. Not to be
    /// confused with the number of retries, which is one less than the number of attempts.
    ///
    /// # Panics
    ///
    /// Panics if `num_attempts` is zero.
    pub fn compute_delay(&self, num_attempts: usize) -> Duration {
        exponential_backoff_delay(self.base_delay, self.max_delay, num_attempts)
    }

    /// Computes the delay before a new attempt after a throttling error: same as
    /// [`Self::compute_delay`], with the throttling base and maximum delays.
    ///
    /// # Panics
    ///
    /// Panics if `num_attempts` is zero.
    pub fn compute_throttling_delay(&self, num_attempts: usize) -> Duration {
        exponential_backoff_delay(
            self.throttling_base_delay,
            self.throttling_max_delay,
            num_attempts,
        )
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        Self {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            max_attempts: 3,
            throttling_base_delay: Duration::from_millis(1),
            throttling_max_delay: Duration::from_millis(2),
        }
    }
}

/// Delay of an exponential backoff with full jitter: between half and all of
/// `base_delay * 2^(num_attempts - 1)`, capped at `max_delay`. See [`RetryParams::compute_delay`].
fn exponential_backoff_delay(
    base_delay: Duration,
    max_delay: Duration,
    num_attempts: usize,
) -> Duration {
    assert!(num_attempts > 0, "num_attempts should be greater than zero");
    let num_attempts = num_attempts.min(32);
    let delay_ms = (base_delay.as_millis() as u64)
        .saturating_mul(2u64.saturating_pow(num_attempts as u32 - 1));
    let capped_delay_ms = delay_ms.min(max_delay.as_millis() as u64);
    let half_delay_ms = capped_delay_ms.div_ceil(2);
    let jitter_range = half_delay_ms..capped_delay_ms + 1;
    let jittered_delay_ms = rand::rng().random_range(jitter_range);
    Duration::from_millis(jittered_delay_ms)
}

#[async_trait]
pub trait MockableSleep {
    async fn sleep(&self, duration: Duration);
}

pub struct TokioSleep;

#[async_trait]
impl MockableSleep for TokioSleep {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

pub async fn retry_with_mockable_sleep<U, E, Fut>(
    retry_params: &RetryParams,
    f: impl Fn() -> Fut,
    mockable_sleep: impl MockableSleep,
) -> Result<U, E>
where
    Fut: Future<Output = Result<U, E>>,
    E: Retryable + Debug + 'static,
{
    let mut num_attempts = 0;

    loop {
        let response = f().await;

        let error = match response {
            Ok(response) => {
                return Ok(response);
            }
            Err(error) => error,
        };
        if !error.is_retryable() {
            return Err(error);
        }
        num_attempts += 1;

        if num_attempts >= retry_params.max_attempts {
            warn!(
                num_attempts=%num_attempts,
                "request failed"
            );
            return Err(error);
        }
        let delay = if error.is_throttling() {
            retry_params.compute_throttling_delay(num_attempts)
        } else {
            retry_params.compute_delay(num_attempts)
        };
        debug!(
            num_attempts=%num_attempts,
            delay_ms=%delay.as_millis(),
            error=?error,
            "request failed, retrying"
        );
        mockable_sleep.sleep(delay).await;
    }
}

pub async fn retry<U, E, Fut>(retry_params: &RetryParams, f: impl Fn() -> Fut) -> Result<U, E>
where
    Fut: Future<Output = Result<U, E>>,
    E: Retryable + Debug + 'static,
{
    retry_with_mockable_sleep(retry_params, f, TokioSleep).await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    use futures::future::ready;

    use super::{MockableSleep, RetryParams, Retryable, retry_with_mockable_sleep};

    #[derive(Debug, Eq, PartialEq)]
    pub enum Retry<E> {
        Permanent(E),
        Transient(E),
    }

    impl<E> Retryable for Retry<E> {
        fn is_retryable(&self) -> bool {
            match self {
                Retry::Permanent(_) => false,
                Retry::Transient(_) => true,
            }
        }
    }

    struct NoopSleep;

    #[async_trait::async_trait]
    impl MockableSleep for NoopSleep {
        async fn sleep(&self, _duration: Duration) {
            // This is a no-op implementation, so we do nothing here.
        }
    }

    async fn simulate_retries<T>(values: Vec<Result<T, Retry<usize>>>) -> Result<T, Retry<usize>> {
        let noop_mock = NoopSleep;
        let values_it = RwLock::new(values.into_iter());
        retry_with_mockable_sleep(
            &RetryParams {
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
                max_attempts: 30,
                throttling_base_delay: Duration::from_millis(1),
                throttling_max_delay: Duration::from_millis(2),
            },
            || ready(values_it.write().unwrap().next().unwrap()),
            noop_mock,
        )
        .await
    }

    #[tokio::test]
    async fn test_retry_accepts_ok() {
        assert_eq!(simulate_retries(vec![Ok(())]).await, Ok(()));
    }

    #[tokio::test]
    async fn test_retry_does_retry() {
        assert_eq!(
            simulate_retries(vec![Err(Retry::Transient(1)), Ok(())]).await,
            Ok(())
        );
    }

    #[tokio::test]
    async fn test_retry_stops_retrying_on_non_retryable_error() {
        assert_eq!(
            simulate_retries(vec![Err(Retry::Permanent(1)), Ok(())]).await,
            Err(Retry::Permanent(1))
        );
    }

    #[tokio::test]
    async fn test_retry_retries_up_at_most_attempts_times() {
        let retry_sequence: Vec<_> = (0..30)
            .map(|retry_id| Err(Retry::Transient(retry_id)))
            .chain(Some(Ok(())))
            .collect();
        assert_eq!(
            simulate_retries(retry_sequence).await,
            Err(Retry::Transient(29))
        );
    }

    #[derive(Clone, Default)]
    struct RecordingSleep(Arc<Mutex<Vec<Duration>>>);

    #[async_trait::async_trait]
    impl MockableSleep for RecordingSleep {
        async fn sleep(&self, duration: Duration) {
            self.0.lock().unwrap().push(duration);
        }
    }

    /// Returns the delays slept before each attempt of an operation that fails with `errors`, in
    /// order, then succeeds.
    async fn record_delays(errors: Vec<super::Retry<()>>) -> Vec<u128> {
        let retry_params = RetryParams {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            max_attempts: 10,
            throttling_base_delay: Duration::from_millis(300),
            throttling_max_delay: Duration::from_millis(800),
        };
        let attempts = RwLock::new(errors.into_iter().map(Err).chain([Ok(())]));
        let recording_sleep = RecordingSleep::default();
        retry_with_mockable_sleep(
            &retry_params,
            || ready(attempts.write().unwrap().next().unwrap()),
            recording_sleep.clone(),
        )
        .await
        .unwrap();
        let delays = recording_sleep.0.lock().unwrap();
        delays.iter().map(|delay| delay.as_millis()).collect()
    }

    #[tokio::test]
    async fn test_retry_waits_longer_after_throttling_errors() {
        let delays = record_delays((0..3).map(|_| super::Retry::Throttled(())).collect()).await;
        // Jittered delays: 300ms, then 600ms, then capped at 800ms.
        assert!((150..=300).contains(&delays[0]), "{delays:?}");
        assert!((300..=600).contains(&delays[1]), "{delays:?}");
        assert!((400..=800).contains(&delays[2]), "{delays:?}");

        let delays = record_delays(vec![super::Retry::Transient(())]).await;
        assert!((50..=100).contains(&delays[0]), "{delays:?}");
    }

    #[tokio::test]
    async fn test_retry_retries_up_to_max_attempts_times() {
        let retry_sequence: Vec<_> = (0..29)
            .map(|retry_id| Err(Retry::Transient(retry_id)))
            .chain(Some(Ok(())))
            .collect();
        assert_eq!(simulate_retries(retry_sequence).await, Ok(()));
    }

    fn test_retry_delay_does_not_overflow_aux(retry_params: RetryParams) {
        for i in 1..100 {
            let delay = retry_params.compute_delay(i);
            assert!(delay <= retry_params.max_delay);
            if retry_params.base_delay <= retry_params.max_delay {
                assert!(delay * 2 >= retry_params.base_delay);
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn test_retry_delay_does_not_overflow(
            max_attempts in 1..1_000usize,
            base_delay in 0..1_000u64,
            max_delay in 0..60_000u64,
        ) {
            let retry_params = RetryParams {
                max_attempts,
                base_delay: Duration::from_millis(base_delay),
                max_delay: Duration::from_millis(max_delay),
                throttling_base_delay: Duration::from_millis(base_delay),
                throttling_max_delay: Duration::from_millis(max_delay),
            };
            test_retry_delay_does_not_overflow_aux(retry_params);
        }
    }
}
