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
        Command::Probe { slug } => {
            bail!("`probe {slug}` lands in milestone 2 (provider + live API confirmation)")
        }
    }
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
        Mode::Once => bail!("--once sync lands in milestone 3 (storage + downloader)"),
        Mode::Serve => bail!("the serve loop lands in milestones 4–5 (server + scheduler)"),
    }
}
