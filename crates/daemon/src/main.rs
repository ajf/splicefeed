//! splicefeed daemon binary: everything that serves or renders.
//!
//! All backend logic lives in the `splicefeed` library; this crate adds the
//! axum HTTP server, the unix-socket control server, the ratatui status
//! TUI, telemetry exporter wiring, CLI parsing, and daemon lifecycle
//! (milestones 4–7).

use std::path::PathBuf;

use anyhow::bail;
use clap::{Parser, Subcommand};
use colored::Colorize;
use splicefeed::{Config, EpisodeState, Library, Mode};
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
    /// Print the library's state from the database: cached files with
    /// locations and hashes, per-show poll health, total space used.
    /// (The live TUI over the control socket lands in milestone 6.)
    Status {
        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,
    },
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
        Command::Status { format } => status(cli.config.as_deref(), format).await,
        Command::Probe { slug } => probe(cli.config.as_deref(), &slug).await,
    }
}

/// How `status` renders its report.
#[derive(Clone, Copy, clap::ValueEnum)]
enum OutputFormat {
    /// Human-readable text.
    Text,
    /// Machine-readable JSON.
    Json,
}

/// Snapshot of the library assembled for `status`. The JSON output *is*
/// this struct, and the text renderer reads from it too, so the two
/// formats can never drift apart.
#[derive(serde::Serialize)]
struct StatusReport {
    shows: Vec<ShowStatus>,
    configured_never_synced: Vec<splicefeed::ShowSlug>,
    total_files: usize,
    total_bytes: u64,
    state_db: std::path::PathBuf,
    data_dir: std::path::PathBuf,
}

#[derive(serde::Serialize)]
struct ShowStatus {
    slug: splicefeed::ShowSlug,
    title: String,
    provider: String,
    last_poll_at: Option<jiff::Timestamp>,
    last_poll_ok: Option<bool>,
    last_error: Option<String>,
    cached_bytes: u64,
    episodes: Vec<EpisodeStatus>,
}

#[derive(serde::Serialize)]
struct EpisodeStatus {
    id: splicefeed::EpisodeId,
    state: EpisodeState,
    bytes: Option<u64>,
    mime: Option<splicefeed::AudioMime>,
    duration_secs: Option<u32>,
    blake3: Option<String>,
    file_path: Option<std::path::PathBuf>,
    downloaded_at: Option<jiff::Timestamp>,
}

/// Report the library's state straight from the database — no daemon or
/// socket involved, safe to run alongside one (WAL + busy timeout).
async fn status(config_path: Option<&std::path::Path>, format: OutputFormat) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    let library = Library::open(config).await?;

    let shows = library.show_records().await?;
    let show_reports =
        futures_util::future::try_join_all(shows.iter().map(|show| show_status(&library, show)))
            .await?;
    let configured_never_synced: Vec<splicefeed::ShowSlug> = library
        .config()
        .shows()
        .iter()
        .map(|show| show.slug())
        .filter(|slug| !shows.iter().any(|record| &record.slug == *slug))
        .cloned()
        .collect();

    let report = StatusReport {
        total_files: show_reports
            .iter()
            .flat_map(|show| &show.episodes)
            .filter(|episode| matches!(episode.state, EpisodeState::Cached))
            .count(),
        total_bytes: show_reports.iter().map(|show| show.cached_bytes).sum(),
        state_db: library.config().data_dir().join("splicefeed.db"),
        data_dir: library.config().data_dir().to_owned(),
        shows: show_reports,
        configured_never_synced,
    };

    match format {
        OutputFormat::Text => print_text(&report),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
    }
    Ok(())
}

async fn show_status(
    library: &Library,
    show: &splicefeed::ShowRecord,
) -> Result<ShowStatus, splicefeed::LibraryError> {
    let episodes = library.episode_records(&show.slug).await?;
    Ok(ShowStatus {
        slug: show.slug.clone(),
        title: show.title.clone(),
        provider: show.provider.clone(),
        last_poll_at: show.last_poll_at,
        last_poll_ok: show.last_poll_ok,
        last_error: show.last_error.clone(),
        cached_bytes: episodes
            .iter()
            .filter(|episode| matches!(episode.state, EpisodeState::Cached))
            .filter_map(|episode| episode.bytes)
            .sum(),
        episodes: episodes
            .into_iter()
            .map(|episode| EpisodeStatus {
                id: episode.id,
                state: episode.state,
                bytes: episode.bytes,
                mime: episode.mime,
                duration_secs: episode.duration_secs,
                blake3: episode.blake3.map(|hash| hash.to_hex().to_string()),
                file_path: episode.file_path,
                downloaded_at: episode.downloaded_at,
            })
            .collect(),
    })
}

/// Width of the horizontal rules separating shows.
const RULE_WIDTH: usize = 72;

fn rule(glyph: &str) -> colored::ColoredString {
    glyph.repeat(RULE_WIDTH).dimmed()
}

fn print_text(report: &StatusReport) {
    if report.shows.is_empty() {
        println!(
            "{}",
            "no shows in storage yet — run `splicefeed run --once` first".yellow()
        );
        println!();
    }
    report.shows.iter().for_each(print_show_text);
    report.configured_never_synced.iter().for_each(|slug| {
        println!("{}", rule("─"));
        println!(
            "{} — {}",
            slug.to_string().bold(),
            "configured, never synced".yellow()
        );
        println!();
    });

    println!("{}", rule("═"));
    println!(
        "{}    {} file(s) on disk · {}",
        "total".bold(),
        report.total_files,
        humansize::format_size(report.total_bytes, humansize::DECIMAL)
            .bold()
            .green(),
    );
    println!(
        "{} {}",
        "state db".dimmed(),
        report.state_db.display().to_string().dimmed()
    );
    println!(
        "{} {}",
        "data dir".dimmed(),
        report.data_dir.display().to_string().dimmed()
    );
}

fn print_show_text(show: &ShowStatus) {
    println!("{}", rule("─"));
    println!(
        "{} — {} {}",
        show.slug.to_string().bold().cyan(),
        show.title.bold(),
        format!("[{}]", show.provider).dimmed(),
    );
    println!();

    match (&show.last_poll_at, show.last_poll_ok) {
        (Some(at), Some(true)) => {
            println!("  last poll {} {}", stamp(at).dimmed(), "(ok)".green());
        }
        (Some(at), Some(false)) => println!(
            "  last poll {} {}",
            stamp(at).dimmed(),
            format!(
                "(FAILED: {})",
                show.last_error.as_deref().unwrap_or("unknown error")
            )
            .red()
            .bold(),
        ),
        _ => println!("  {}", "never polled".yellow()),
    }

    let of_state = |wanted: fn(&EpisodeState) -> bool| {
        show.episodes
            .iter()
            .filter(move |episode| wanted(&episode.state))
    };
    let cached: Vec<&EpisodeStatus> =
        of_state(|state| matches!(state, EpisodeState::Cached)).collect();
    let failed: Vec<String> = show
        .episodes
        .iter()
        .filter_map(|episode| match episode.state {
            EpisodeState::Failed(class) => Some(format!("{} ({class})", episode.id)),
            _ => None,
        })
        .collect();
    let downloading = of_state(|state| matches!(state, EpisodeState::Downloading)).count();

    let cached_part = {
        let text = format!(
            "{} cached ({})",
            cached.len(),
            humansize::format_size(show.cached_bytes, humansize::DECIMAL)
        );
        if cached.is_empty() {
            text.dimmed()
        } else {
            text.green().bold()
        }
    };
    let mut parts = vec![
        cached_part.to_string(),
        count_part(
            of_state(|state| matches!(state, EpisodeState::Discovered)).count(),
            "discovered",
            |text| text.normal(),
        ),
        count_part(failed.len(), "failed", |text| text.red().bold()),
        count_part(
            of_state(|state| matches!(state, EpisodeState::Pruned)).count(),
            "pruned",
            |text| text.normal(),
        ),
    ];
    if downloading > 0 {
        // Only visible while a daemon is mid-download (or died there).
        parts.push(format!("{downloading} downloading").yellow().to_string());
    }
    println!("  {}", parts.join(&format!(" {} ", "·".dimmed())));
    println!();

    cached.iter().for_each(|episode| {
        // Styled columns are pre-padded (width specs count ANSI escape
        // bytes); unstyled ones take their widths here directly.
        println!(
            "  {}  {:>10}  {}  {:>9}  {}",
            format!("{:>8}", episode.id).bold(),
            episode
                .bytes
                .map_or("?".into(), |b| humansize::format_size(
                    b,
                    humansize::DECIMAL
                )),
            format!(
                "{:<11}",
                episode
                    .mime
                    .as_ref()
                    .map_or("mime?".into(), ToString::to_string)
            )
            .dimmed(),
            episode.duration_secs.map_or("?".into(), |secs| {
                humantime::format_duration(std::time::Duration::from_secs(secs.into())).to_string()
            }),
            format!(
                "downloaded {}",
                episode.downloaded_at.as_ref().map_or("?".into(), stamp)
            )
            .dimmed(),
        );
        println!(
            "           {}",
            format!("blake3 {}", episode.blake3.as_deref().unwrap_or("?")).dimmed()
        );
        println!(
            "           {}",
            episode
                .file_path
                .as_deref()
                .map_or("<file path missing>".into(), |p| p.display().to_string())
                .cyan(),
        );
        println!();
    });
    if !failed.is_empty() {
        println!("  {} {}", "failed:".red().bold(), failed.join(", ").red());
        println!();
    }
}

/// A `"N label"` summary fragment: dimmed when zero, `highlight`ed when
/// something is there to see.
fn count_part(
    count: usize,
    label: &str,
    highlight: impl FnOnce(&str) -> colored::ColoredString,
) -> String {
    let text = format!("{count} {label}");
    if count == 0 {
        text.dimmed().to_string()
    } else {
        highlight(&text).to_string()
    }
}

fn stamp(at: &jiff::Timestamp) -> String {
    at.strftime("%Y-%m-%d %H:%M:%SZ").to_string()
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

    // The probe always looks at the provider's full natural window —
    // it diagnoses upstream, not the fetch_last config.
    let episodes = match provider.episodes(&slug, None).await {
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
                    audio
                        .mime
                        .as_ref()
                        .map_or("mime unknown".into(), ToString::to_string),
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
    let mut failed: Vec<&splicefeed::ShowSlug> = Vec::new();
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
                failed.push(slug);
            }
        }
    }
    if !failed.is_empty() {
        bail!(
            "sync failed for {}",
            failed
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}
