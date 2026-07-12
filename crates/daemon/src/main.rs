//! splicefeed daemon binary: everything that serves or renders.
//!
//! All backend logic lives in the `splicefeed` library; this crate adds the
//! axum HTTP server, the unix-socket control server, the ratatui status
//! TUI, telemetry exporter wiring, CLI parsing, and daemon lifecycle
//! (milestones 4–7).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::bail;
use clap::{Parser, Subcommand};
use colored::Colorize;
use splicefeed::{Config, EpisodeState, Library, Mode};
use splicefeed_daemon::{control, ops, reload, report, scheduler, server};
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
        /// Live-updating TUI view instead of a one-shot report.
        #[arg(long, conflicts_with = "format")]
        watch: bool,
    },
    /// Check cached audio files against the database: existence, size,
    /// and blake3 hash.
    Verify {
        /// Show slug; verifies every configured show when omitted.
        slug: Option<String>,
        /// Re-download files that are missing or corrupt.
        #[arg(long)]
        fix: bool,
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
        Command::Status { format, watch } => status(cli.config.as_deref(), format, watch).await,
        Command::Verify { slug, fix, format } => {
            verify(cli.config.as_deref(), slug.as_deref(), fix, format).await
        }
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

/// Report the library's state straight from the database — no daemon or
/// socket involved, safe to run alongside one (WAL + busy timeout).
async fn status(
    config_path: Option<&std::path::Path>,
    format: OutputFormat,
    watch: bool,
) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    let library = Library::open(config).await?;
    if watch {
        let socket = library.config().control_socket_path();
        return splicefeed_daemon::tui::watch(&library, &socket).await;
    }

    let report = report::status_report(&library).await?;

    match format {
        OutputFormat::Text => print_text(&report),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
    }
    Ok(())
}

/// Width of the horizontal rules separating shows.
const RULE_WIDTH: usize = 72;

fn rule(glyph: &str) -> colored::ColoredString {
    glyph.repeat(RULE_WIDTH).dimmed()
}

fn print_text(report: &report::StatusReport) {
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

fn print_show_text(show: &report::ShowStatus) {
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
    let cached: Vec<&report::EpisodeStatus> =
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
            episode.bytes.map_or("?".into(), |b| humansize::format_size(
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
        if let Some(description) = episode.description.as_deref() {
            println!("           {}", description.italic().dimmed());
        }
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

/// What `verify` reports: one entry per show checked.
#[derive(serde::Serialize)]
struct VerifyRun {
    shows: Vec<ShowVerify>,
}

#[derive(serde::Serialize)]
struct ShowVerify {
    slug: splicefeed::ShowSlug,
    #[serde(flatten)]
    report: splicefeed::VerifyReport,
}

/// Check cached files against the database, optionally re-downloading
/// damage. Exits non-zero when problems remain, so cron jobs notice.
async fn verify(
    config_path: Option<&std::path::Path>,
    slug: Option<&str>,
    fix: bool,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    let library = Library::open(config).await?;

    let slugs: Vec<splicefeed::ShowSlug> = match slug {
        Some(raw) => vec![raw.parse()?],
        None => library
            .config()
            .shows()
            .iter()
            .map(|show| show.slug().clone())
            .collect(),
    };
    let reports =
        futures_util::future::try_join_all(slugs.iter().map(|slug| library.verify(slug, fix)))
            .await?;
    let run = VerifyRun {
        shows: slugs
            .into_iter()
            .zip(reports)
            .map(|(slug, report)| ShowVerify { slug, report })
            .collect(),
    };

    match format {
        OutputFormat::Text => print_verify_text(&run, fix),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&run)?),
    }

    let unfixed = run
        .shows
        .iter()
        .flat_map(|show| &show.report.problems)
        .filter(|problem| !problem.fixed)
        .count();
    if unfixed > 0 {
        bail!(
            "{unfixed} file(s) failed verification{}",
            if fix {
                " and could not be fixed"
            } else {
                " (run with --fix to re-download)"
            }
        );
    }
    Ok(())
}

fn print_verify_text(run: &VerifyRun, fix: bool) {
    run.shows.iter().for_each(|show| {
        println!("{}", rule("─"));
        let intact = format!("{} intact", show.report.intact);
        println!(
            "{} — {} checked · {}",
            show.slug.to_string().bold().cyan(),
            show.report.checked,
            if show.report.problems.is_empty() {
                intact.green()
            } else {
                intact.normal()
            },
        );
        show.report.problems.iter().for_each(|outcome| {
            let verdict = if outcome.fixed {
                "fixed ✓".green().bold()
            } else if fix {
                "NOT FIXED".red().bold()
            } else {
                "run --fix to re-download".yellow()
            };
            println!(
                "  {}  {}  {}",
                format!("{:>8}", outcome.id).bold(),
                outcome.problem.to_string().red(),
                verdict,
            );
        });
        println!();
    });
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
    let episodes = match provider.episodes(&slug, None, None).await {
        Ok(splicefeed::EpisodeListing::NotModified) => {
            println!("episodes:  unexpected 304 without a validator");
            Vec::new()
        }
        Ok(splicefeed::EpisodeListing::Modified { episodes, .. }) => {
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
        Mode::Once => ops::sync_all_once(&library).await,
        Mode::Serve => {
            let socket_path = library.config().control_socket_path();
            let (tx, rx) = tokio::sync::watch::channel(Arc::new(library));
            let vitals = control::Vitals::default();
            // Converge once at startup; the scheduler takes it from there.
            tokio::spawn(initial_sync(tx.borrow().clone()));
            tokio::spawn(scheduler::run(rx.clone()));
            tokio::spawn(control_serve(
                socket_path.clone(),
                rx.clone(),
                vitals.clone(),
            ));
            #[cfg(unix)]
            tokio::spawn(reload::on_sighup(
                tx,
                config_path.map(std::path::Path::to_path_buf),
            ));
            let served = server::serve(rx, vitals, shutdown_signal()).await;
            // The control task can't see process exit; tidy its socket
            // here (startup tolerates a stale file regardless).
            std::fs::remove_file(&socket_path).ok();
            served
        }
    }
}

async fn initial_sync(library: Arc<Library>) {
    if let Err(err) = ops::sync_all_once(&library).await {
        tracing::error!(error = %err, "initial sync failed");
    }
}

async fn control_serve(
    path: std::path::PathBuf,
    library: server::LibraryHandle,
    vitals: control::Vitals,
) {
    if let Err(err) = control::serve(path, library, vitals).await {
        tracing::error!(error = %err, "control socket failed; status --watch falls back to the database");
    }
}

async fn shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => tracing::info!("shutdown signal received"),
        Err(err) => {
            // No signal handler means no clean way to stop; serve on.
            tracing::error!(error = %err, "failed to install ctrl-c handler");
            std::future::pending::<()>().await
        }
    }
}
