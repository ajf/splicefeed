//! Assembly of the library-state snapshot shared by the `status` CLI
//! command and the HTTP `/debug` route. The JSON output of both *is*
//! [`StatusReport`], and the CLI's text renderer reads from it too, so
//! the formats can never drift apart.

use splicefeed::{EpisodeState, Library, LibraryError};

/// Snapshot of the whole library.
#[derive(serde::Serialize)]
pub struct StatusReport {
    /// Per-show state, ordered by slug.
    pub shows: Vec<ShowStatus>,
    /// Shows in the configuration that storage has never seen.
    pub configured_never_synced: Vec<splicefeed::ShowSlug>,
    /// Cached files across all shows.
    pub total_files: usize,
    /// Bytes on disk across all shows.
    pub total_bytes: u64,
    /// The SQLite database file.
    pub state_db: std::path::PathBuf,
    /// The data directory.
    pub data_dir: std::path::PathBuf,
}

/// One show's state.
#[derive(serde::Serialize)]
pub struct ShowStatus {
    /// The show.
    pub slug: splicefeed::ShowSlug,
    /// Provider-reported title.
    pub title: String,
    /// Provider registry name.
    pub provider: String,
    /// When the show was last polled, if ever.
    pub last_poll_at: Option<jiff::Timestamp>,
    /// Whether that poll succeeded.
    pub last_poll_ok: Option<bool>,
    /// Error message of the last failed poll.
    pub last_error: Option<String>,
    /// Bytes on disk for this show.
    pub cached_bytes: u64,
    /// Every episode row, newest first.
    pub episodes: Vec<EpisodeStatus>,
}

/// One episode's state.
#[derive(serde::Serialize)]
pub struct EpisodeStatus {
    /// The episode.
    pub id: splicefeed::EpisodeId,
    /// Lifecycle state.
    pub state: EpisodeState,
    /// File size, when downloaded.
    pub bytes: Option<u64>,
    /// Audio MIME type, when known.
    pub mime: Option<splicefeed::AudioMime>,
    /// Duration in seconds, when known.
    pub duration_secs: Option<u32>,
    /// blake3 of the file (hex), when downloaded.
    pub blake3: Option<String>,
    /// Where the audio lives, while cached.
    pub file_path: Option<std::path::PathBuf>,
    /// When the download completed.
    pub downloaded_at: Option<jiff::Timestamp>,
}

/// Build the full report from storage.
pub async fn status_report(library: &Library) -> Result<StatusReport, LibraryError> {
    let shows = library.show_records().await?;
    let show_reports =
        futures_util::future::try_join_all(shows.iter().map(|show| show_status(library, show)))
            .await?;
    let configured_never_synced: Vec<splicefeed::ShowSlug> = library
        .config()
        .shows()
        .iter()
        .map(|show| show.slug())
        .filter(|slug| !shows.iter().any(|record| &record.slug == *slug))
        .cloned()
        .collect();

    Ok(StatusReport {
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
    })
}

async fn show_status(
    library: &Library,
    show: &splicefeed::ShowRecord,
) -> Result<ShowStatus, LibraryError> {
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
