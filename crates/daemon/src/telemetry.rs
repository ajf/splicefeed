//! Metrics (milestone 7): one OpenTelemetry registry, two ways out.
//!
//! The Prometheus reader always feeds the `/metrics` route (pull —
//! nothing leaves the box); an OTLP `PeriodicReader` pushes to a
//! collector only when `[telemetry.otlp]` is configured. Both hang off
//! the same `SdkMeterProvider`, so there is exactly one source of truth.
//!
//! The boundary rule keeps every `opentelemetry` crate out of the
//! library tree, so the libraries never *record* metrics directly:
//! the sync engine broadcasts domain events (milestone 6) and
//! [`pump_events`] translates them into counters here in the binary;
//! HTTP latency is measured by axum middleware in the server module.
//!
//! Decisions within the design's leeway: metrics only — traces stay on
//! stderr via `tracing` (a `tracing-opentelemetry` layer can bolt onto
//! the same provider later), and `tokio-metrics` was skipped (its
//! runtime instrumentation needs an unstable tokio cfg).

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, MeterProvider as _};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use splicefeed::ipc::KnownEvent;
use splicefeed::{Config, Library};
use tokio::sync::watch;

/// The daemon's metrics handle. Cheap to clone.
#[derive(Clone)]
pub struct Metrics {
    registry: prometheus::Registry,
    // Keeps the readers alive; dropping the provider stops export.
    _provider: SdkMeterProvider,
    polls: Counter<u64>,
    discovered: Counter<u64>,
    downloads: Counter<u64>,
    pruned_episodes: Counter<u64>,
    pruned_bytes: Counter<u64>,
    quarantined: Counter<u64>,
    /// `http.server.request.duration` per OTel semantic conventions.
    pub http_request_duration: Histogram<f64>,
}

/// Build the registry: Prometheus reader always, OTLP reader when
/// configured. Failure to construct the OTLP exporter is an error —
/// the operator asked for it explicitly.
pub fn init(config: &Config) -> anyhow::Result<Metrics> {
    let registry = prometheus::Registry::new();
    let prometheus_reader = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()?;

    let mut provider = SdkMeterProvider::builder().with_reader(prometheus_reader);
    if let Some(otlp) = config.telemetry().otlp() {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(otlp.endpoint().as_str())
            .with_headers(otlp.headers().clone())
            .build()?;
        let reader = PeriodicReader::builder(exporter)
            .with_interval(otlp.interval())
            .build();
        provider = provider.with_reader(reader);
        tracing::info!(
            endpoint = %otlp.endpoint(),
            interval = ?otlp.interval(),
            "OTLP metric export enabled"
        );
    }
    let provider = provider.build();

    let meter = provider.meter("splicefeed");
    Ok(Metrics {
        registry,
        polls: meter
            .u64_counter("splicefeed.polls")
            .with_description("Provider polls, by show and outcome")
            .build(),
        discovered: meter
            .u64_counter("splicefeed.episodes.discovered")
            .with_description("Episodes newly discovered, by show")
            .build(),
        downloads: meter
            .u64_counter("splicefeed.downloads")
            .with_description("Finished downloads, by show and result")
            .build(),
        pruned_episodes: meter
            .u64_counter("splicefeed.pruned.episodes")
            .with_description("Episodes removed by retention, by show")
            .build(),
        pruned_bytes: meter
            .u64_counter("splicefeed.pruned.bytes")
            .with_description("Bytes freed by retention, by show")
            .with_unit("By")
            .build(),
        quarantined: meter
            .u64_counter("splicefeed.quarantined")
            .with_description("Unparseable provider payloads quarantined, by provider")
            .build(),
        http_request_duration: meter
            .f64_histogram("http.server.request.duration")
            .with_description("HTTP request duration")
            .with_unit("s")
            .build(),
        _provider: provider,
    })
}

impl Metrics {
    /// The Prometheus exposition text for `/metrics`.
    pub fn scrape(&self) -> String {
        prometheus::TextEncoder::new()
            .encode_to_string(&self.registry.gather())
            .unwrap_or_else(|err| {
                tracing::error!(error = %err, "metrics encoding failed");
                String::new()
            })
    }

    fn record(&self, event: &KnownEvent) {
        let show = |slug: &splicefeed::ShowSlug| KeyValue::new("show", slug.to_string());
        match event {
            KnownEvent::PollStarted { .. } => {}
            KnownEvent::PollFinished { show: slug, ok, .. } => {
                self.polls.add(1, &[show(slug), KeyValue::new("ok", *ok)])
            }
            KnownEvent::EpisodeDiscovered { show: slug, .. } => {
                self.discovered.add(1, &[show(slug)]);
            }
            KnownEvent::DownloadFinished {
                show: slug, error, ..
            } => {
                let result = error.map_or("ok".to_owned(), |class| class.to_string());
                self.downloads
                    .add(1, &[show(slug), KeyValue::new("result", result)]);
            }
            KnownEvent::Pruned {
                show: slug,
                episodes,
                bytes_freed,
            } => {
                self.pruned_episodes
                    .add(u64::from(*episodes), &[show(slug)]);
                self.pruned_bytes.add(*bytes_freed, &[show(slug)]);
            }
            KnownEvent::Quarantined { provider, .. } => {
                self.quarantined
                    .add(1, &[KeyValue::new("provider", provider.clone())]);
            }
            _ => {}
        }
    }
}

/// Translate the library's event stream into counters, forever. The
/// subscription survives reloads (the broadcast sender is shared across
/// `Library` generations); the task ends when the sender is dropped at
/// shutdown.
pub async fn pump_events(library: watch::Receiver<std::sync::Arc<Library>>, metrics: Metrics) {
    let mut events = library.borrow().subscribe();
    loop {
        match events.recv().await {
            Ok(event) => metrics.record(&event),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                tracing::warn!(missed, "metrics pump lagged behind the event stream");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}
