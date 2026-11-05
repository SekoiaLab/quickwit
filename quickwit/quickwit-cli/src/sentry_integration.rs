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
//! environment. Panics and `ERROR` events are sent; `WARN` events are kept as breadcrumbs.

use std::time::Duration;

use quickwit_serve::BuildInfo;
use sentry::integrations::tracing::{EventFilter, SentryLayer};
use tracing::{Level, Metadata};
use tracing_subscriber::registry::LookupSpan;

const FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

/// Initializes the Sentry client. Keep the returned guard alive until the process exits:
/// dropping it flushes pending events.
pub fn init_sentry(build_info: &BuildInfo) -> sentry::ClientInitGuard {
    let mut options = sentry::ClientOptions::new().traces_sample_rate(0.0);
    options.release = Some(release_name(build_info).into());
    options.send_default_pii = false;
    options.attach_stacktrace = false;
    sentry::init(options)
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
where S: tracing::Subscriber + for<'a> LookupSpan<'a> {
    sentry::integrations::tracing::layer()
        .event_filter(event_filter)
        .span_filter(|_| false)
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
