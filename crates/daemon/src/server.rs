//! The axum HTTP server: feeds, range-served media, artwork, health, and
//! the `/debug` state dump.
//!
//! No TLS and no auth by design — this binds loopback by default and a
//! reverse proxy (Caddy) fronts anything wider. Nothing here ever emits
//! an upstream credential: feeds reference our own `/media` routes, and
//! media files are served straight from the data directory.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use splicefeed::{Library, LibraryError, ShowSlug};
use tokio::sync::watch;
use tower_http::services::ServeDir;

use crate::report;

/// The server's handle to the current [`Library`]. A reload (SIGHUP)
/// swaps the value; handlers read it per request.
pub type LibraryHandle = watch::Receiver<Arc<Library>>;

/// All routes over the current library. The `/media` and `/artwork`
/// roots are captured from the initial config — `data_dir` is a
/// restart-only setting, enforced by the reload guard. Every request
/// bumps `vitals` for the control socket's snapshot and lands in the
/// `http.server.request.duration` histogram.
pub fn router(
    library: LibraryHandle,
    vitals: crate::control::Vitals,
    metrics: crate::telemetry::Metrics,
) -> Router {
    let data_dir = library.borrow().config().data_dir().to_owned();
    let scraped = metrics.clone();
    Router::new()
        .route("/feeds/{feed}", get(feed))
        .route("/subscriptions.opml", get(subscriptions))
        .route("/healthz", get(healthz))
        .route("/debug", get(debug))
        .route(
            "/metrics",
            get(move || {
                let metrics = scraped.clone();
                async move {
                    (
                        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                        metrics.scrape(),
                    )
                }
            }),
        )
        // ServeDir brings range requests (podcast apps scrub), sane
        // Content-Type from extensions, and path sanitization.
        .nest_service("/media", ServeDir::new(data_dir.join("media")))
        .nest_service("/artwork", ServeDir::new(data_dir.join("artwork")))
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                vitals
                    .http_requests
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let method = request.method().as_str().to_owned();
                let metrics = metrics.clone();
                let started = std::time::Instant::now();
                async move {
                    let response = next.run(request).await;
                    metrics.http_request_duration.record(
                        started.elapsed().as_secs_f64(),
                        &[
                            opentelemetry::KeyValue::new("http.request.method", method),
                            opentelemetry::KeyValue::new(
                                "http.response.status_code",
                                i64::from(response.status().as_u16()),
                            ),
                        ],
                    );
                    response
                }
            },
        ))
        .with_state(library)
}

/// Bind the configured address and serve until `shutdown` resolves.
pub async fn serve(
    library: LibraryHandle,
    vitals: crate::control::Vitals,
    metrics: crate::telemetry::Metrics,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let (bind, external) = {
        let current = library.borrow();
        (
            current.config().bind(),
            current.config().external_base_url(),
        )
    };
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, %external, "HTTP server listening");
    axum::serve(listener, router(library, vitals, metrics))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

/// `GET /feeds/<slug>.xml` — the show's podcast RSS.
async fn feed(State(handle): State<LibraryHandle>, Path(name): Path<String>) -> Response {
    let library = handle.borrow().clone();
    let Some(slug) = name.strip_suffix(".xml") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(slug) = slug.parse::<ShowSlug>() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut xml = Vec::new();
    match library.write_feed(&slug, &mut xml).await {
        Ok(()) => (
            [(header::CONTENT_TYPE, "application/rss+xml; charset=utf-8")],
            xml,
        )
            .into_response(),
        Err(LibraryError::UnknownShow(_) | LibraryError::NotSynced(_)) => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(err) => {
            tracing::error!(show = %slug, error = %err, "feed generation failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `GET /subscriptions.opml` — every servable feed as one importable
/// subscription list.
async fn subscriptions(State(handle): State<LibraryHandle>) -> Response {
    let library = handle.borrow().clone();
    let mut xml = Vec::new();
    match library.write_opml(&mut xml).await {
        Ok(()) => ([(header::CONTENT_TYPE, "text/x-opml; charset=utf-8")], xml).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "opml generation failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(serde::Serialize)]
struct Health {
    status: &'static str,
    shows: Vec<ShowHealth>,
}

#[derive(serde::Serialize)]
struct ShowHealth {
    slug: ShowSlug,
    last_poll_at: Option<jiff::Timestamp>,
    last_poll_ok: Option<bool>,
    last_error: Option<String>,
}

/// `GET /healthz` — liveness plus last-poll health per show.
async fn healthz(State(handle): State<LibraryHandle>) -> Response {
    let library = handle.borrow().clone();
    match library.show_records().await {
        Ok(records) => Json(Health {
            status: "ok",
            shows: records
                .into_iter()
                .map(|record| ShowHealth {
                    slug: record.slug,
                    last_poll_at: record.last_poll_at,
                    last_poll_ok: record.last_poll_ok,
                    last_error: record.last_error,
                })
                .collect(),
        })
        .into_response(),
        Err(err) => {
            tracing::error!(error = %err, "healthz storage read failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `GET /debug` — the same report the `status` CLI command shows, as
/// pretty-printed JSON.
async fn debug(State(handle): State<LibraryHandle>) -> Response {
    let library = handle.borrow().clone();
    match report::status_report(&library).await {
        Ok(report) => match serde_json::to_string_pretty(&report) {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(err) => {
                tracing::error!(error = %err, "debug report serialization failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
        Err(err) => {
            tracing::error!(error = %err, "debug report assembly failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
