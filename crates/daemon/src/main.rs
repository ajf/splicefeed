//! splicefeed daemon binary: everything that serves or renders.
//!
//! All backend logic lives in the `splicefeed` library; this crate adds the
//! axum HTTP server, the unix-socket control server, the ratatui status
//! TUI, telemetry exporter wiring, CLI parsing, and daemon lifecycle
//! (milestones 4–7).

use std::path::PathBuf;

use anyhow::bail;
use clap::{Parser, Subcommand};
use splicefeed::{Config, Library, Mode};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "splicefeed",
    version,
    about = "DI.FM-to-podcast-RSS proxy daemon"
)]
struct Cli {
    /// Path to config.toml (default: ~/.config/splicefeed/config.toml).
    #[arg(long, global = true, env = "SPLICEFEED_CONFIG", value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon: poll shows on schedule and serve feeds over HTTP.
    Run {
        /// Poll every show once, write feeds, and exit (cron-style).
        #[arg(long)]
        once: bool,
    },
    /// Live daemon status TUI (connects to the control socket).
    Status,
    /// Hit the live provider API for one show and report what parsed —
    /// the early-warning system for upstream API drift.
    Probe {
        /// Show slug to probe, e.g. `melodik-revolution`.
        slug: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Run { once } => {
            run(
                cli.config.as_deref(),
                if once { Mode::Once } else { Mode::Serve },
            )
            .await
        }
        Command::Status => bail!("`status` lands in milestone 6 (IPC + TUI)"),
        Command::Probe { slug } => probe(cli.config.as_deref(), &slug).await,
    }
}

/// Hit the live provider API and report what parsed — the early-warning
/// system for upstream schema drift. Unparseable payloads are quarantined
/// (see warnings) rather than crashing anything.
async fn probe(config_path: Option<&std::path::Path>, slug: &str) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    let slug: splicefeed::ShowSlug = slug.parse()?;

    let provider_name = config
        .shows()
        .iter()
        .find(|s| s.slug() == &slug)
        .map(|s| s.provider().to_owned())
        .unwrap_or_else(|| "difm".to_owned());
    let provider = splicefeed::ProviderRegistry::create(&config, &provider_name)?;
    println!("probing `{slug}` via provider `{provider_name}`");

    match provider.show(&slug).await {
        Ok(meta) => {
            println!("show:      OK  title={:?}", meta.title);
            println!(
                "           description: {}",
                meta.description
                    .as_deref()
                    .map_or("MISSING".into(), |d| format!("{} chars", d.len()))
            );
            println!(
                "           artwork: {}",
                meta.artwork
                    .as_ref()
                    .map_or("MISSING".into(), |u| u.to_string())
            );
        }
        Err(err) => println!("show:      FAILED  {err}"),
    }

    let episodes = match provider.episodes(&slug).await {
        Ok(episodes) => {
            println!(
                "episodes:  OK  {} parsed (drifted entries, if any, are quarantined and warned above)",
                episodes.len()
            );
            for episode in episodes.iter().take(5) {
                println!(
                    "           {:>8}  {:60}  {}  {}",
                    episode.id.to_string(),
                    format!("{:?}", episode.title),
                    episode
                        .published_at
                        .map_or("pubdate MISSING".into(), |t| t.to_string()),
                    episode
                        .duration_secs
                        .map_or("duration MISSING".into(), |d| format!("{d}s")),
                );
            }
            episodes
        }
        Err(err) => {
            println!("episodes:  FAILED  {err}");
            Vec::new()
        }
    };

    if let Some(first) = episodes.first() {
        match provider.resolve_audio(&slug, &first.id).await {
            Ok(audio) => {
                println!(
                    "audio:     OK  {} ({}, {})",
                    splicefeed::redacted(&audio.url),
                    audio.mime.as_deref().unwrap_or("mime unknown"),
                    audio
                        .bytes
                        .map_or("size unknown".into(), |b| format!("{b} bytes")),
                );
            }
            Err(err) => println!("audio:     FAILED  {err}"),
        }
    }
    Ok(())
}

async fn run(config_path: Option<&std::path::Path>, mode: Mode) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    let library = Library::open(config).await?;

    let config = library.config();
    tracing::info!(
        shows = config.shows().len(),
        bind = %config.bind(),
        external_base_url = %config.external_base_url(),
        data_dir = %config.data_dir().display(),
        ?mode,
        "configuration loaded and validated"
    );
    for show in config.shows() {
        tracing::info!(
            show = %show.slug(),
            provider = show.provider(),
            interval = ?show.poll_interval(config.poll_interval()),
            "following"
        );
    }

    match mode {
        Mode::Once => sync_all_once(&library).await,
        Mode::Serve => bail!("the serve loop lands in milestones 4–5 (server + scheduler)"),
    }
}

/// Cron-style operation: poll every configured show once, then exit.
/// One failing show never stops the others; any failure makes the exit
/// status non-zero so cron/systemd notice.
async fn sync_all_once(library: &Library) -> anyhow::Result<()> {
    let mut failed = Vec::new();
    for show in library.config().shows() {
        let slug = show.slug();
        match library.sync(slug).await {
            Ok(report) => tracing::info!(
                show = %slug,
                discovered = report.discovered,
                downloaded = report.downloaded,
                pruned = report.pruned,
                "sync complete"
            ),
            Err(err) => {
                tracing::error!(show = %slug, error = %err, "sync failed");
                failed.push(slug.to_string());
            }
        }
    }
    if !failed.is_empty() {
        bail!("sync failed for {}", failed.join(", "));
    }
    Ok(())
}
