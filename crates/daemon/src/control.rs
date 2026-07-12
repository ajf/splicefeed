//! The unix control socket (milestone 6): NDJSON request/response plus a
//! subscription stream, serving the daemon's in-process view to
//! `splicefeed status --watch` and anything else on the box.
//!
//! Debuggable by design: `socat - UNIX-CONNECT:$XDG_RUNTIME_DIR/splicefeed.sock`
//! then type `{"request":"snapshot"}`. The socket is chmod 0600 — same
//! trust boundary as the database.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use splicefeed::{EpisodeState, Library, ipc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

/// Shared daemon vitals the HTTP server contributes to.
#[derive(Clone, Default)]
pub struct Vitals {
    /// HTTP requests served since start.
    pub http_requests: Arc<AtomicU64>,
}

/// Everything a connection needs to answer requests.
#[derive(Clone)]
struct Control {
    library: watch::Receiver<Arc<Library>>,
    vitals: Vitals,
    started: Instant,
    /// Last observed (bytes_done, at) per in-flight episode, for
    /// throughput between consecutive snapshots.
    rates: Arc<std::sync::Mutex<std::collections::HashMap<String, (u64, Instant)>>>,
}

/// Bind the socket (replacing any stale file) and serve until the
/// library sender closes.
pub async fn serve(
    path: PathBuf,
    library: watch::Receiver<Arc<Library>>,
    vitals: Vitals,
) -> anyhow::Result<()> {
    // A leftover socket file from an unclean exit would fail the bind;
    // a *live* second daemon is an operator error we can't detect here.
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    let listener = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    tracing::info!(socket = %path.display(), "control socket listening");

    let control = Control {
        library: library.clone(),
        vitals,
        started: Instant::now(),
        rates: Arc::default(),
    };
    let mut library = library;
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    tokio::spawn(connection(stream, control.clone()));
                }
                Err(err) => tracing::warn!(error = %err, "control accept failed"),
            },
            changed = library.changed() => {
                if changed.is_err() {
                    // Daemon shutting down: remove the socket file.
                    std::fs::remove_file(&path).ok();
                    tracing::info!("control socket closed");
                    return Ok(());
                }
            }
        }
    }
}

async fn connection(stream: UnixStream, control: Control) {
    if let Err(err) = talk(stream, control).await {
        tracing::debug!(error = %err, "control connection ended with error");
    }
}

async fn talk(stream: UnixStream, control: Control) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    send(
        &mut writer,
        &ipc::Hello {
            protocol_version: ipc::PROTOCOL_VERSION,
            daemon_version: env!("CARGO_PKG_VERSION").to_owned(),
        },
    )
    .await?;

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<ipc::Request>(&line) {
            Ok(ipc::Request::Snapshot) => {
                let response = match snapshot(&control).await {
                    Ok(snapshot) => ipc::Response::Snapshot(snapshot),
                    Err(err) => ipc::Response::Error {
                        message: err.to_string(),
                    },
                };
                send(&mut writer, &response).await?;
            }
            Ok(ipc::Request::Subscribe) => {
                let response = match snapshot(&control).await {
                    Ok(snapshot) => ipc::Response::Snapshot(snapshot),
                    Err(err) => ipc::Response::Error {
                        message: err.to_string(),
                    },
                };
                send(&mut writer, &response).await?;
                return stream_events(writer, control).await;
            }
            // Includes verbs from a newer client: answer, don't drop.
            Ok(other) => {
                send(
                    &mut writer,
                    &ipc::Response::Error {
                        message: format!("unsupported request: {other:?}"),
                    },
                )
                .await?;
            }
            Err(err) => {
                send(
                    &mut writer,
                    &ipc::Response::Error {
                        message: format!("unparseable request: {err}"),
                    },
                )
                .await?;
            }
        }
    }
    Ok(())
}

/// Forward library events as NDJSON until the client hangs up.
async fn stream_events(
    mut writer: tokio::net::unix::OwnedWriteHalf,
    control: Control,
) -> anyhow::Result<()> {
    let mut events = control.library.borrow().subscribe();
    loop {
        match events.recv().await {
            Ok(event) => send(&mut writer, &ipc::Event::Known(event)).await?,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                tracing::debug!(missed, "control subscriber lagged; continuing");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}

async fn send<T: serde::Serialize>(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    message: &T,
) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(message)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    Ok(())
}

/// Assemble the point-in-time snapshot from storage plus daemon vitals.
async fn snapshot(control: &Control) -> Result<ipc::Snapshot, splicefeed::LibraryError> {
    let library = control.library.borrow().clone();
    let records = library.show_records().await?;

    let mut shows = Vec::with_capacity(records.len());
    let mut downloads = Vec::new();
    for record in records {
        let episodes = library.episode_records(&record.slug).await?;
        let cached: Vec<&splicefeed::EpisodeRecord> = episodes
            .iter()
            .filter(|ep| matches!(ep.state, EpisodeState::Cached))
            .collect();
        downloads.extend(
            episodes
                .iter()
                .filter(|ep| matches!(ep.state, EpisodeState::Downloading))
                .map(|ep| ipc::DownloadStatus {
                    show: record.slug.clone(),
                    episode: ep.id.clone(),
                    bytes_done: ep.bytes_done.unwrap_or(0),
                    bytes_total: ep.bytes_total,
                    bytes_per_sec: rate(control, &record.slug, ep),
                }),
        );
        shows.push(ipc::ShowStatus {
            slug: record.slug,
            provider: record.provider,
            last_poll_at: record.last_poll_at.map(|t| t.as_second()),
            last_poll_ok: record.last_poll_ok,
            // The scheduler's next fire is jittered per cycle; exposing
            // it lands with scheduler introspection (not yet plumbed).
            next_poll_at: None,
            episodes_cached: cached.len() as u64,
            cache_bytes: cached.iter().filter_map(|ep| ep.bytes).sum(),
            last_error: record.last_error,
        });
    }

    Ok(ipc::Snapshot {
        uptime_secs: control.started.elapsed().as_secs(),
        shows,
        downloads,
        data_dir_bytes: dir_bytes(library.config().data_dir().to_owned()).await,
        http_requests: control.vitals.http_requests.load(Ordering::Relaxed),
    })
}

/// Throughput from the delta against this episode's previous snapshot
/// observation; 0 until a second observation exists.
fn rate(control: &Control, show: &splicefeed::ShowSlug, ep: &splicefeed::EpisodeRecord) -> u64 {
    let key = format!("{show}/{}", ep.id);
    let done = ep.bytes_done.unwrap_or(0);
    let now = Instant::now();
    let mut rates = control
        .rates
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous = rates.insert(key, (done, now));
    match previous {
        Some((before, at)) if done > before && now > at => {
            ((done - before) as f64 / (now - at).as_secs_f64()) as u64
        }
        _ => 0,
    }
}

/// Total size of the data directory, walked on the blocking pool.
async fn dir_bytes(dir: PathBuf) -> u64 {
    tokio::task::spawn_blocking(move || walk(&dir))
        .await
        .unwrap_or(0)
}

fn walk(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                walk(&path)
            } else {
                entry.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}
