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

//! Error reporting to Sentry.
//!
//! The client is enabled only when `SENTRY_DSN` is set. `SENTRY_ENVIRONMENT` sets the
//! environment and `SENTRY_SAMPLE_RATE` (0.0 to 1.0, default 1.0) the share of events sent.
//! Panics and `ERROR` events are sent, except the expected ones in `IGNORED_MESSAGES`; `WARN`
//! events are kept as breadcrumbs. Performance tracing stays off (the SDK default).

use std::sync::Arc;
use std::time::Duration;

use quickwit_serve::BuildInfo;
use sentry::integrations::tracing::{EventFilter, SentryLayer};
use tracing::{Level, Metadata};
use tracing_subscriber::registry::LookupSpan;

const FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

const SAMPLE_RATE_ENV_KEY: &str = "SENTRY_SAMPLE_RATE";

/// Errors that are expected and can come in bursts, so they stay in the logs only:
/// - a partial success, e.g. when the object store answers `SlowDown`;
/// - a planning failure, e.g. when the query sent to Quickwit is invalid.
const IGNORED_MESSAGES: &[&str] = &["root search partial success", "root search planning failed"];

/// Initializes the Sentry client. Keep the returned guard alive until the process exits:
/// dropping it flushes pending events.
pub fn init_sentry(build_info: &BuildInfo) -> sentry::ClientInitGuard {
    let sample_rate_opt = quickwit_common::get_from_env_opt(SAMPLE_RATE_ENV_KEY, false);
    let mut options = sentry::ClientOptions::new().sample_rate(sample_rate(sample_rate_opt));
    options.release = Some(release_name(build_info).into());
    options.send_default_pii = false;
    options.attach_stacktrace = false;
    options.before_send = Some(Arc::new(|event| (!is_ignored(&event)).then_some(event)));
    sentry::init(options)
}

/// `SENTRY_SAMPLE_RATE`, clamped to `[0.0, 1.0]` (the SDK panics outside it); every event is sent
/// when it is unset or invalid.
fn sample_rate(value_opt: Option<f32>) -> f32 {
    match value_opt {
        Some(value) if value.is_finite() => value.clamp(0.0, 1.0),
        _ => 1.0,
    }
}

fn is_ignored(event: &sentry::protocol::Event<'static>) -> bool {
    match event.message.as_deref() {
        Some(message) => IGNORED_MESSAGES.contains(&message),
        None => false,
    }
}

/// Sends pending events. Needed before `std::process::exit`, which skips the guard's drop.
pub fn flush_sentry() {
    if let Some(client) = sentry::Hub::main().client() {
        client.flush(Some(FLUSH_TIMEOUT));
    }
}

fn release_name(build_info: &BuildInfo) -> String {
    format!(
        "quickwit@{}+{}",
        build_info.version, build_info.commit_short_hash
    )
}

/// Tracing layer forwarding events to Sentry. Spans are ignored: tracing is exported with
/// OpenTelemetry, and creating a Sentry span for each tracing span would cost on hot paths.
pub fn sentry_layer<S>() -> SentryLayer<S>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    sentry::integrations::tracing::layer()
        .event_filter(event_filter)
        .span_filter(|_| false)
}

/// The Sentry layer, or `None` when the client is disabled (no `SENTRY_DSN`), so that a disabled
/// Sentry adds no work to each event. `init_sentry` must run first.
pub fn sentry_layer_if_enabled<S>() -> Option<SentryLayer<S>>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    let is_enabled = match sentry::Hub::main().client() {
        Some(client) => client.is_enabled(),
        None => false,
    };
    is_enabled.then(sentry_layer)
}

fn event_filter(metadata: &Metadata<'_>) -> EventFilter {
    match *metadata.level() {
        Level::ERROR => EventFilter::Event,
        Level::WARN => EventFilter::Breadcrumb,
        _ => EventFilter::Ignore,
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn test_release_name() {
        let build_info = BuildInfo::get();
        let release = release_name(build_info);
        assert!(release.starts_with("quickwit@"));
        assert!(release.ends_with(build_info.commit_short_hash));
    }

    #[test]
    fn test_sample_rate() {
        assert_eq!(sample_rate(None), 1.0);
        assert_eq!(sample_rate(Some(0.25)), 0.25);
        assert_eq!(sample_rate(Some(2.0)), 1.0);
        assert_eq!(sample_rate(Some(-1.0)), 0.0);
        assert_eq!(sample_rate(Some(f32::NAN)), 1.0);
    }

    #[test]
    fn test_is_ignored() {
        let event = |message: &str| sentry::protocol::Event {
            message: Some(message.to_string()),
            ..Default::default()
        };
        assert!(is_ignored(&event("root search partial success")));
        assert!(is_ignored(&event("root search planning failed")));
        assert!(!is_ignored(&event("root search failed")));
        assert!(!is_ignored(&sentry::protocol::Event::default()));
    }

    #[test]
    fn test_sentry_layer_if_enabled_without_client() {
        // No `init_sentry` in tests: the main hub has no client.
        assert!(sentry_layer_if_enabled::<tracing_subscriber::Registry>().is_none());
    }

    #[test]
    fn test_sentry_layer_sends_errors_only() {
        let subscriber = tracing_subscriber::registry().with(sentry_layer());
        let events = tracing::subscriber::with_default(subscriber, || {
            sentry::test::with_captured_events(|| {
                let _span = tracing::info_span!("some_span").entered();
                tracing::info!("some info");
                tracing::warn!("some warning");
                tracing::error!("some error");
            })
        });
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.level, sentry::Level::Error);
        assert_eq!(event.message.as_deref(), Some("some error"));
        let breadcrumbs: Vec<&str> = event
            .breadcrumbs
            .iter()
            .filter_map(|breadcrumb| breadcrumb.message.as_deref())
            .collect();
        assert_eq!(breadcrumbs, ["some warning"]);
    }
}
