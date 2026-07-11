//! Live status TUI (`splicefeed status --watch`).
//!
//! Milestone-6 precursor: renders the same [`StatusReport`] the plain
//! `status` command and `/debug` route use, refreshed from the database
//! every couple of seconds. When the scheduler and control socket land,
//! this view switches to the daemon's in-process state (in-flight
//! downloads, live events) over IPC — the rendering stays.
//!
//! Reading the database directly means it works whether or not a daemon
//! is running (WAL + busy timeout), so "daemon not running" is simply a
//! quieter screen, never a panic.

use std::time::Duration;

use ratatui::Frame;
use ratatui::crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use splicefeed::{EpisodeState, Library};

use crate::report::{self, ShowStatus, StatusReport};

const REFRESH: Duration = Duration::from_secs(2);

/// Everything the renderer needs; kept separate from I/O so tests can
/// draw it into a [`ratatui::backend::TestBackend`].
pub struct App {
    /// The current snapshot.
    pub report: StatusReport,
    /// Index of the selected show.
    pub selected: usize,
}

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
pub async fn watch(library: &Library) -> anyhow::Result<()> {
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, library).await;
    ratatui::restore();
    result
}

async fn run_loop(
    terminal: &mut ratatui::DefaultTerminal,
    library: &Library,
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

    let mut app = App {
        report: report::status_report(library).await?,
        selected: 0,
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
    let [header, downloads, shows, episodes, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(downloads_height),
        Constraint::Length((app.report.shows.len() as u16 + 3).min(12)),
        Constraint::Min(4),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            " splicefeed ".bold().reversed(),
            format!(
                " {} file(s) · {} · {} download slot(s) · {}",
                app.report.total_files,
                humansize::format_size(app.report.total_bytes, humansize::DECIMAL),
                app.report.download_concurrency,
                app.report.data_dir.display(),
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

    frame.render_widget(
        Paragraph::new(" ↑/↓ select · r refresh · q quit").dim(),
        footer,
    );
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
        });
        assert!(content.contains("no shows in storage yet"));
    }

    #[test]
    fn selection_stays_in_bounds() {
        let mut app = App {
            report: fake_report(),
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
