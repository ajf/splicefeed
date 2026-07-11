//! SIGHUP config reload.
//!
//! The serving daemon holds its [`Library`] behind a
//! `tokio::sync::watch` channel: HTTP handlers read the current value
//! per request, and a reload swaps in a whole new `Library` built from a
//! freshly loaded config. A failed reload — unparseable TOML, invalid
//! config, or a restart-only change — logs loudly and leaves the old
//! configuration serving; an operator typo must never take down the
//! feeds. (The milestone-5 scheduler subscribes to the same channel to
//! pick up interval and show-list changes.)
//!
//! Restart-only settings: `bind` (the TCP listener is already bound) and
//! `data_dir` (the media/artwork routes and the shared storage handle
//! captured it at startup).

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, bail};
use splicefeed::{Config, Library};
use tokio::sync::watch;

/// Load the config from `config_path` (same resolution as startup),
/// build a new [`Library`] reusing the current one's storage, and swap
/// it in. Returns the new library so the caller can kick a sync; on any
/// error the channel is untouched.
pub async fn apply(
    tx: &watch::Sender<Arc<Library>>,
    config_path: Option<&Path>,
) -> anyhow::Result<Arc<Library>> {
    let old = tx.borrow().clone();
    let config = Config::load(config_path).context("reload aborted; keeping previous config")?;
    guard(old.config(), &config)?;

    let library = Arc::new(old.reload(config).await?);
    log_diff(old.config(), library.config());
    tx.send(Arc::clone(&library))
        .context("no receivers left for the reloaded library")?;
    Ok(library)
}

/// Reject changes that need a restart to take effect. Rejecting the
/// whole reload (rather than partially applying it) keeps the running
/// state equal to *some* config the operator actually wrote.
fn guard(old: &Config, new: &Config) -> anyhow::Result<()> {
    if old.bind() != new.bind() {
        bail!(
            "`bind` changed ({} -> {}): restart required; reload aborted",
            old.bind(),
            new.bind()
        );
    }
    if old.data_dir() != new.data_dir() {
        bail!(
            "`data_dir` changed ({} -> {}): restart required; reload aborted",
            old.data_dir().display(),
            new.data_dir().display()
        );
    }
    Ok(())
}

fn log_diff(old: &Config, new: &Config) {
    let slugs = |config: &Config| -> BTreeSet<String> {
        config
            .shows()
            .iter()
            .map(|show| show.slug().to_string())
            .collect()
    };
    let (old_shows, new_shows) = (slugs(old), slugs(new));
    let added: Vec<&String> = new_shows.difference(&old_shows).collect();
    let removed: Vec<&String> = old_shows.difference(&new_shows).collect();
    tracing::info!(
        shows = new_shows.len(),
        ?added,
        ?removed,
        external_base_url = %new.external_base_url(),
        download_concurrency = new.download_concurrency(),
        "configuration reloaded"
    );
}

/// Listen for SIGHUP and reload on each one, syncing any newly added
/// shows afterwards (until the scheduler exists, a reload is the only
/// moment an added show would ever sync).
#[cfg(unix)]
pub async fn on_sighup(tx: watch::Sender<Arc<Library>>, config_path: Option<std::path::PathBuf>) {
    let mut hangups = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        Ok(stream) => stream,
        Err(err) => {
            tracing::error!(error = %err, "cannot install SIGHUP handler; reload disabled");
            return;
        }
    };
    while hangups.recv().await.is_some() {
        tracing::info!("SIGHUP received: reloading configuration");
        match apply(&tx, config_path.as_deref()).await {
            Ok(library) => {
                if let Err(err) = crate::ops::sync_all_once(&library).await {
                    tracing::error!(error = %err, "post-reload sync failed");
                }
            }
            Err(err) => {
                tracing::error!(error = %err, "reload failed; previous configuration stays live");
            }
        }
    }
}
