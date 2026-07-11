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
use tower_http::services::ServeDir;

use crate::report;

/// All routes over a shared [`Library`].
pub fn router(library: Arc<Library>) -> Router {
    let data_dir = library.config().data_dir();
    Router::new()
        .route("/feeds/{feed}", get(feed))
        .route("/healthz", get(healthz))
        .route("/debug", get(debug))
        // ServeDir brings range requests (podcast apps scrub), sane
        // Content-Type from extensions, and path sanitization.
        .nest_service("/media", ServeDir::new(data_dir.join("media")))
        .nest_service("/artwork", ServeDir::new(data_dir.join("artwork")))
        .with_state(library)
}

/// Bind the configured address and serve until `shutdown` resolves.
pub async fn serve(
    library: Arc<Library>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let bind = library.config().bind();
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(
        %bind,
        external = %library.config().external_base_url(),
        "HTTP server listening"
    );
    axum::serve(listener, router(library))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

/// `GET /feeds/<slug>.xml` — the show's podcast RSS.
async fn feed(State(library): State<Arc<Library>>, Path(name): Path<String>) -> Response {
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
async fn healthz(State(library): State<Arc<Library>>) -> Response {
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
async fn debug(State(library): State<Arc<Library>>) -> Response {
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
