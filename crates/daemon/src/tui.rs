//! Live status TUI (`splicefeed status --watch`).
//!
//! Renders the same [`StatusReport`] the plain `status` command and the
//! `/debug` route use, refreshed from the database every couple of
//! seconds — so it works whether or not a daemon is running (WAL + busy
//! timeout), and "daemon not running" is simply a quieter screen, never
//! a panic. When the daemon's control socket is reachable, the view
//! additionally gains live vitals in the header (uptime, HTTP request
//! count), a rolling event log, and event-driven refreshes that beat
//! the 2-second tick.

use std::time::Duration;

use ratatui::Frame;
use ratatui::crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use splicefeed::{EpisodeState, Library};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::report::{self, ShowStatus, StatusReport};

const REFRESH: Duration = Duration::from_secs(2);

/// Everything the renderer needs; kept separate from I/O so tests can
/// draw it into a [`ratatui::backend::TestBackend`].
pub struct App {
    /// The current snapshot.
    pub report: StatusReport,
    /// Index of the selected show.
    pub selected: usize,
    /// Daemon vitals from the control socket; `None` = no daemon
    /// running (the tables still work — they read the database).
    pub vitals: Option<splicefeed::ipc::Snapshot>,
    /// Rolling event log from the control socket, newest last.
    pub events: std::collections::VecDeque<String>,
}

/// Rolling event log capacity.
const EVENT_LOG: usize = 100;

impl App {
    fn clamp(&mut self) {
        self.selected = self.selected.min(self.report.shows.len().saturating_sub(1));
    }

    fn select_next(&mut self) {
        self.selected = (self.selected + 1).min(self.report.shows.len().saturating_sub(1));
    }

    fn select_prev(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }
}

/// Run the watch loop until the user quits (`q`, `Esc`, or ctrl-c).
/// `socket` is the daemon's control socket: when connectable, the view
/// gains live vitals and an event stream; when not, everything still
/// renders from the database.
pub async fn watch(library: &Library, socket: &std::path::Path) -> anyhow::Result<()> {
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, library, socket).await;
    ratatui::restore();
    result
}

/// What the control-socket reader task feeds the render loop.
enum Live {
    Snapshot(splicefeed::ipc::Snapshot),
    Event(splicefeed::ipc::KnownEvent),
    /// Connection ended (daemon stopped, or never ran).
    Gone,
}

/// Connect, subscribe, and forward the daemon's stream. Exits (sending
/// [`Live::Gone`]) on any error — the TUI then shows DB-only state.
async fn follow_control_socket(path: std::path::PathBuf, tx: tokio::sync::mpsc::Sender<Live>) {
    let Ok(stream) = tokio::net::UnixStream::connect(&path).await else {
        tx.send(Live::Gone).await.ok();
        return;
    };
    let (reader, mut writer) = stream.into_split();
    let mut lines = tokio::io::BufReader::new(reader).lines();

    // Hello, then subscribe.
    let hello = lines.next_line().await.ok().flatten();
    let compatible = hello
        .as_deref()
        .and_then(|line| serde_json::from_str::<splicefeed::ipc::Hello>(line).ok())
        .is_some_and(|hello| hello.protocol_version == splicefeed::ipc::PROTOCOL_VERSION);
    if !compatible
        || writer
            .write_all(b"{\"request\":\"subscribe\"}\n")
            .await
            .is_err()
    {
        tx.send(Live::Gone).await.ok();
        return;
    }

    while let Ok(Some(line)) = lines.next_line().await {
        // First reply is the snapshot; everything after is events.
        if let Ok(splicefeed::ipc::Response::Snapshot(snapshot)) = serde_json::from_str(&line) {
            if tx.send(Live::Snapshot(snapshot)).await.is_err() {
                return;
            }
        } else if let Ok(splicefeed::ipc::Event::Known(event)) = serde_json::from_str(&line)
            && tx.send(Live::Event(event)).await.is_err()
        {
            return;
        }
    }
    tx.send(Live::Gone).await.ok();
}

async fn run_loop(
    terminal: &mut ratatui::DefaultTerminal,
    library: &Library,
    socket: &std::path::Path,
) -> anyhow::Result<()> {
    // Crossterm event reads are blocking; a dedicated thread feeds them
    // into the async loop. The thread ends when the receiver drops.
    let (tx, mut events) = tokio::sync::mpsc::channel(16);
    std::thread::spawn(move || {
        loop {
            let ready = ratatui::crossterm::event::poll(Duration::from_millis(200));
            if ready.unwrap_or(false) {
                let Ok(event) = ratatui::crossterm::event::read() else {
                    break;
                };
                if tx.blocking_send(event).is_err() {
                    break;
                }
            } else if tx.is_closed() {
                break;
            }
        }
    });

    let (live_tx, mut live) = tokio::sync::mpsc::channel(64);
    tokio::spawn(follow_control_socket(socket.to_path_buf(), live_tx));

    let mut app = App {
        report: report::status_report(library).await?,
        selected: 0,
        vitals: None,
        events: std::collections::VecDeque::new(),
    };
    let mut ticker = tokio::time::interval(REFRESH);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        terminal.draw(|frame| draw(frame, &app))?;
        tokio::select! {
            _ = ticker.tick() => {
                app.report = report::status_report(library).await?;
                app.clamp();
            }
            update = live.recv() => match update {
                Some(Live::Snapshot(snapshot)) => app.vitals = Some(snapshot),
                Some(Live::Event(event)) => {
                    if app.events.len() == EVENT_LOG {
                        app.events.pop_front();
                    }
                    app.events.push_back(describe(&event));
                    // Events mean state changed: refresh the tables now
                    // instead of waiting out the tick.
                    app.report = report::status_report(library).await?;
                    app.clamp();
                }
                Some(Live::Gone) => app.vitals = None,
                None => {}
            },
            event = events.recv() => match event {
                Some(Event::Key(key)) if key.kind == KeyEventKind::Press => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        return Ok(());
                    }
                    KeyCode::Down | KeyCode::Char('j') => app.select_next(),
                    KeyCode::Up | KeyCode::Char('k') => app.select_prev(),
                    KeyCode::Char('r') => {
                        app.report = report::status_report(library).await?;
                        app.clamp();
                    }
                    _ => {}
                },
                Some(_) => {} // resize etc.: redrawn on the next loop pass
                None => return Ok(()),
            }
        }
    }
}

/// Render one frame. Pure over [`App`], so tests can assert the buffer.
pub fn draw(frame: &mut Frame, app: &App) {
    let active = active_downloads(&app.report);
    let downloads_height = if active.is_empty() {
        0
    } else {
        (active.len() as u16 + 2).min(6)
    };
    let log_height = if app.vitals.is_some() || !app.events.is_empty() {
        7
    } else {
        0
    };
    let [header, downloads, shows, episodes, log, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(downloads_height),
        Constraint::Length((app.report.shows.len() as u16 + 3).min(12)),
        Constraint::Min(4),
        Constraint::Length(log_height),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    let vitals = match &app.vitals {
        Some(snapshot) => format!(
            " · daemon up {} · {} http req",
            humantime::format_duration(Duration::from_secs(snapshot.uptime_secs)),
            snapshot.http_requests,
        ),
        None => " · daemon offline".into(),
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            " splicefeed ".bold().reversed(),
            format!(
                " {} file(s) · {} · {} download slot(s){vitals}",
                app.report.total_files,
                humansize::format_size(app.report.total_bytes, humansize::DECIMAL),
                app.report.download_concurrency,
            )
            .into(),
        ])),
        header,
    );

    if !active.is_empty() {
        draw_downloads(frame, app, &active, downloads);
    }
    draw_shows(frame, app, shows);
    if let Some(show) = app.report.shows.get(app.selected) {
        draw_episodes(frame, show, episodes);
    } else {
        frame.render_widget(
            Paragraph::new("no shows in storage yet — run `splicefeed run --once` first")
                .style(Style::new().fg(Color::Yellow))
                .block(Block::new().borders(Borders::ALL).title("episodes")),
            episodes,
        );
    }

    if log_height > 0 {
        let recent: Vec<Line> = app
            .events
            .iter()
            .rev()
            .take(usize::from(log_height.saturating_sub(2)))
            .rev()
            .map(|entry| Line::from(entry.as_str()))
            .collect();
        frame.render_widget(
            Paragraph::new(recent).block(Block::new().borders(Borders::ALL).title("events")),
            log,
        );
    }

    frame.render_widget(
        Paragraph::new(" ↑/↓ select · r refresh · q quit").dim(),
        footer,
    );
}

/// One line of the rolling event log.
fn describe(event: &splicefeed::ipc::KnownEvent) -> String {
    use splicefeed::ipc::KnownEvent;
    let stamp = jiff::Timestamp::now().strftime("%H:%M:%S");
    let what = match event {
        KnownEvent::PollStarted { show } => format!("poll started: {show}"),
        KnownEvent::PollFinished {
            show,
            ok: true,
            new_episodes,
        } => format!("poll ok: {show} ({new_episodes} new)"),
        KnownEvent::PollFinished {
            show, ok: false, ..
        } => format!("poll FAILED: {show}"),
        KnownEvent::EpisodeDiscovered { show, episode } => {
            format!("discovered: {show}/{episode}")
        }
        KnownEvent::DownloadFinished {
            show,
            episode,
            error: None,
        } => format!("downloaded: {show}/{episode}"),
        KnownEvent::DownloadFinished {
            show,
            episode,
            error: Some(class),
        } => format!("download FAILED ({class}): {show}/{episode}"),
        KnownEvent::Pruned {
            show,
            episodes,
            bytes_freed,
        } => format!(
            "pruned: {show} ({episodes} episode(s), {} freed)",
            humansize::format_size(*bytes_freed, humansize::DECIMAL)
        ),
        KnownEvent::Quarantined { provider, path } => format!(
            "QUARANTINED ({provider}): {}",
            path.as_deref()
                .map_or("<write failed>".into(), |p| p.display().to_string())
        ),
        other => format!("{other:?}"),
    };
    format!("{stamp}  {what}")
}

/// One in-flight download as the TUI shows it.
struct ActiveDownload<'a> {
    show: &'a splicefeed::ShowSlug,
    episode: &'a crate::report::EpisodeStatus,
}

fn active_downloads(report: &StatusReport) -> Vec<ActiveDownload<'_>> {
    report
        .shows
        .iter()
        .flat_map(|show| {
            show.episodes
                .iter()
                .filter(|episode| matches!(episode.state, EpisodeState::Downloading))
                .map(move |episode| ActiveDownload {
                    show: &show.slug,
                    episode,
                })
        })
        .collect()
}

/// Progress written more than this long ago counts as stalled (writes
/// come every ~1s while bytes flow).
const STALL_AFTER: Duration = Duration::from_secs(10);

fn draw_downloads(frame: &mut Frame, app: &App, active: &[ActiveDownload<'_>], area: Rect) {
    let now = jiff::Timestamp::now();
    let rows = active.iter().map(|dl| {
        let done = dl.episode.bytes_done;
        let total = dl.episode.bytes_total;
        let percent = match (done, total) {
            (Some(done), Some(total)) if total > 0 => {
                format!("{:>3.0}%", done as f64 / total as f64 * 100.0)
            }
            _ => "  ?%".into(),
        };
        let bytes = format!(
            "{} / {}",
            done.map_or("?".into(), |b| humansize::format_size(
                b,
                humansize::DECIMAL
            )),
            total.map_or("?".into(), |b| humansize::format_size(
                b,
                humansize::DECIMAL
            )),
        );
        let stalled = dl.episode.progress_at.is_none_or(|at| {
            now.since(at)
                .map(|span| span.get_seconds() >= STALL_AFTER.as_secs() as i64)
                .unwrap_or(true)
        });
        let health = if stalled {
            Cell::from("stalled").style(Style::new().fg(Color::Red).add_modifier(Modifier::BOLD))
        } else {
            Cell::from("active").style(Style::new().fg(Color::Green))
        };
        Row::new(vec![
            Cell::from(format!("{}/{}", dl.show, dl.episode.id)),
            Cell::from(percent).style(Style::new().fg(Color::Yellow)),
            Cell::from(bytes),
            health,
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Min(24),
            Constraint::Length(5),
            Constraint::Length(24),
            Constraint::Length(8),
        ],
    )
    .block(Block::new().borders(Borders::ALL).title(format!(
        "downloads ({}/{} slots)",
        active.len().min(app.report.download_concurrency),
        app.report.download_concurrency
    )));
    frame.render_widget(table, area);
}

fn draw_shows(frame: &mut Frame, app: &App, area: Rect) {
    let rows = app.report.shows.iter().enumerate().map(|(i, show)| {
        let (cached, failed) = count_states(show);
        let poll = match (&show.last_poll_at, show.last_poll_ok) {
            (Some(at), Some(true)) => Cell::from(stamp(at)).style(Style::new().fg(Color::Green)),
            (Some(at), Some(false)) => Cell::from(format!("{} FAILED", stamp(at)))
                .style(Style::new().fg(Color::Red).add_modifier(Modifier::BOLD)),
            _ => Cell::from("never polled").style(Style::new().fg(Color::Yellow)),
        };
        let row = Row::new(vec![
            Cell::from(show.slug.to_string()),
            poll,
            Cell::from(format!(
                "{cached} ({})",
                humansize::format_size(show.cached_bytes, humansize::DECIMAL)
            ))
            .style(Style::new().fg(Color::Green)),
            Cell::from(failed.to_string()).style(if failed > 0 {
                Style::new().fg(Color::Red)
            } else {
                Style::new().dim()
            }),
        ]);
        if i == app.selected {
            row.style(Style::new().add_modifier(Modifier::REVERSED))
        } else {
            row
        }
    });

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(28),
            Constraint::Length(18),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec!["show", "last poll", "cached", "failed"])
            .style(Style::new().add_modifier(Modifier::BOLD)),
    )
    .block(Block::new().borders(Borders::ALL).title("shows"));
    frame.render_widget(table, area);
}

fn draw_episodes(frame: &mut Frame, show: &ShowStatus, area: Rect) {
    let rows = show.episodes.iter().map(|episode| {
        let (state, style) = match episode.state {
            EpisodeState::Cached => ("cached", Style::new().fg(Color::Green)),
            EpisodeState::Discovered => ("discovered", Style::new().dim()),
            EpisodeState::Downloading => ("downloading", Style::new().fg(Color::Yellow)),
            EpisodeState::Pruned => ("pruned", Style::new().dim()),
            EpisodeState::Failed(_) => ("failed", Style::new().fg(Color::Red)),
        };
        Row::new(vec![
            Cell::from(episode.id.to_string()),
            Cell::from(state).style(style),
            Cell::from(episode.bytes.map_or("—".into(), |b| {
                humansize::format_size(b, humansize::DECIMAL)
            })),
            Cell::from(episode.duration_secs.map_or("—".into(), |secs| {
                humantime::format_duration(Duration::from_secs(secs.into())).to_string()
            })),
            Cell::from(episode.downloaded_at.as_ref().map_or("—".into(), stamp)),
            Cell::from(episode.description.as_deref().unwrap_or("—").to_owned())
                .style(Style::new().dim()),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(17),
            Constraint::Min(16),
        ],
    )
    .header(
        Row::new(vec![
            "episode",
            "state",
            "size",
            "length",
            "downloaded",
            "description",
        ])
        .style(Style::new().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::new()
            .borders(Borders::ALL)
            .title(format!("episodes — {}", show.slug)),
    );
    frame.render_widget(table, area);
}

fn count_states(show: &ShowStatus) -> (usize, usize) {
    (
        show.episodes
            .iter()
            .filter(|e| matches!(e.state, EpisodeState::Cached))
            .count(),
        show.episodes
            .iter()
            .filter(|e| matches!(e.state, EpisodeState::Failed(_)))
            .count(),
    )
}

fn stamp(at: &jiff::Timestamp) -> String {
    at.strftime("%Y-%m-%d %H:%M").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::EpisodeStatus;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn fake_report() -> StatusReport {
        StatusReport {
            shows: vec![ShowStatus {
                slug: "melodik-revolution".parse().expect("valid slug"),
                title: "Melodik Revolution".into(),
                provider: "difm".into(),
                last_poll_at: Some("2026-07-11T22:14:29Z".parse().expect("valid ts")),
                last_poll_ok: Some(true),
                last_error: None,
                cached_bytes: 288_111_664,
                episodes: vec![
                    EpisodeStatus {
                        id: "162".parse().expect("valid id"),
                        description: Some("with Mark Pledger".into()),
                        state: EpisodeState::Cached,
                        bytes: Some(288_111_664),
                        mime: Some(splicefeed::AudioMime::Mpeg),
                        duration_secs: Some(7200),
                        blake3: Some("ab".repeat(32)),
                        file_path: Some("/data/media/x/162.mp3".into()),
                        downloaded_at: Some("2026-07-11T22:14:29Z".parse().expect("valid ts")),
                        bytes_done: None,
                        bytes_total: None,
                        progress_at: None,
                    },
                    EpisodeStatus {
                        id: "161".parse().expect("valid id"),
                        description: None,
                        state: EpisodeState::Downloading,
                        bytes: None,
                        mime: None,
                        duration_secs: None,
                        blake3: None,
                        file_path: None,
                        downloaded_at: None,
                        bytes_done: Some(144_000_000),
                        bytes_total: Some(288_000_000),
                        progress_at: Some(jiff::Timestamp::now()),
                    },
                    EpisodeStatus {
                        id: "160".parse().expect("valid id"),
                        description: None,
                        state: EpisodeState::Failed(splicefeed::ErrorClass::Network),
                        bytes: None,
                        mime: None,
                        duration_secs: None,
                        blake3: None,
                        file_path: None,
                        downloaded_at: None,
                        bytes_done: None,
                        bytes_total: None,
                        progress_at: None,
                    },
                ],
            }],
            configured_never_synced: vec![],
            total_files: 1,
            total_bytes: 288_111_664,
            download_concurrency: 2,
            state_db: "/data/splicefeed.db".into(),
            data_dir: "/data".into(),
        }
    }

    fn rendered(app: &App) -> String {
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        terminal.draw(|frame| draw(frame, app)).expect("draws");
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    #[test]
    fn renders_shows_and_selected_episodes() {
        let content = rendered(&App {
            report: fake_report(),
            vitals: None,
            events: std::collections::VecDeque::new(),
            selected: 0,
        });
        assert!(content.contains("melodik-revolution"));
        assert!(content.contains("cached"));
        assert!(content.contains("162"));
        assert!(content.contains("failed"));
        assert!(content.contains("288.11 MB"));
        assert!(content.contains("q quit"));
        assert!(
            content.contains("with Mark Pledger"),
            "episode description is rendered"
        );
    }

    #[test]
    fn active_download_panel_shows_progress_and_health() {
        let content = rendered(&App {
            report: fake_report(),
            vitals: None,
            events: std::collections::VecDeque::new(),
            selected: 0,
        });
        assert!(content.contains("downloads (1/2 slots)"));
        assert!(content.contains("melodik-revolution/161"));
        assert!(content.contains("50%"));
        assert!(content.contains("144 MB / 288 MB"));
        assert!(content.contains("active"));
    }

    #[test]
    fn stale_progress_reads_as_stalled() {
        let mut report = fake_report();
        report.shows[0].episodes[1].progress_at =
            Some("2026-07-11T00:00:00Z".parse().expect("valid ts"));
        let content = rendered(&App {
            report,
            selected: 0,
            vitals: None,
            events: std::collections::VecDeque::new(),
        });
        assert!(content.contains("stalled"));
    }

    #[test]
    fn empty_library_renders_a_hint_not_a_panic() {
        let content = rendered(&App {
            report: StatusReport {
                shows: vec![],
                configured_never_synced: vec![],
                total_files: 0,
                total_bytes: 0,
                download_concurrency: 2,
                state_db: "/data/splicefeed.db".into(),
                data_dir: "/data".into(),
            },
            selected: 0,
            vitals: None,
            events: std::collections::VecDeque::new(),
        });
        assert!(content.contains("no shows in storage yet"));
    }

    #[test]
    fn selection_stays_in_bounds() {
        let mut app = App {
            report: fake_report(),
            vitals: None,
            events: std::collections::VecDeque::new(),
            selected: 5,
        };
        app.clamp();
        assert_eq!(app.selected, 0);
        app.select_prev();
        assert_eq!(app.selected, 0);
        app.select_next();
        assert_eq!(app.selected, 0, "single show cannot select past the end");
    }
}
