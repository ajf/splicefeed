//! SQLite-backed state: shows, episodes, and the episode lifecycle.
//!
//! One database file in the data directory, accessed through `rusqlite`
//! (bundled). All access goes through [`Storage`], which owns the single
//! connection behind a mutex and runs every query on the blocking thread
//! pool — write volume here is tiny (see DESIGN.md "Decisions").
//!
//! Lifecycle transitions are enforced in this layer with
//! [`EpisodeState::can_transition_to`]; an illegal transition is a bug in
//! the caller and surfaces as [`StorageError::IllegalTransition`], never as
//! silently corrupted state. Pruned rows are kept as tombstones so a pruned
//! episode is never re-discovered as "new".

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};

use crate::domain::{
    AudioMime, EpisodeId, EpisodeMeta, EpisodeState, ErrorClass, ShowMeta, ShowSlug,
};

/// Errors surfaced by [`Storage`] operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// The data directory could not be created or accessed.
    #[error("data directory unusable at {path}: {source}")]
    DataDir {
        /// The directory that failed.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The database rejected an operation.
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// A stored value no longer parses as its domain type — the database
    /// was edited out from under us or written by an incompatible version.
    #[error("corrupt row in `{table}`: {reason}")]
    Corrupt {
        /// Table holding the bad row.
        table: &'static str,
        /// What failed to parse.
        reason: String,
    },
    /// The caller asked for a lifecycle transition the state machine
    /// forbids (see [`EpisodeState::can_transition_to`]).
    #[error("illegal transition for episode `{episode}` of `{show}`: {from:?} → {to:?}")]
    IllegalTransition {
        /// The show.
        show: ShowSlug,
        /// The episode.
        episode: EpisodeId,
        /// State recorded in the database.
        from: EpisodeState,
        /// State the caller asked for.
        to: EpisodeState,
    },
    /// The episode is not in the database at all.
    #[error("episode `{episode}` of `{show}` is not in storage")]
    EpisodeNotFound {
        /// The show.
        show: ShowSlug,
        /// The episode.
        episode: EpisodeId,
    },
    /// The blocking storage task was cancelled or panicked.
    #[error("storage task failed to complete")]
    TaskFailed,
}

/// A show row.
#[derive(Debug, Clone)]
pub struct ShowRecord {
    /// The show's slug.
    pub slug: ShowSlug,
    /// Registry name of the provider serving it.
    pub provider: String,
    /// Title as reported by the provider (feed-level overrides apply at
    /// RSS generation, not here).
    pub title: String,
    /// Provider description, if any.
    pub description: Option<String>,
    /// Cached artwork file, if fetched (milestone 4).
    pub artwork_path: Option<PathBuf>,
    /// When the show was last polled, if ever.
    pub last_poll_at: Option<jiff::Timestamp>,
    /// Whether the last poll succeeded.
    pub last_poll_ok: Option<bool>,
    /// Error message of the last failed poll, cleared on success.
    pub last_error: Option<String>,
}

/// An episode row.
#[derive(Debug, Clone)]
pub struct EpisodeRecord {
    /// The show this episode belongs to.
    pub show: ShowSlug,
    /// Provider-assigned identifier; feed GUIDs derive from this.
    pub id: EpisodeId,
    /// Episode title.
    pub title: String,
    /// Episode description, if any.
    pub description: Option<String>,
    /// Publication time, if the provider supplied one.
    pub published_at: Option<jiff::Timestamp>,
    /// Duration in seconds, from the provider or probed from the file.
    pub duration_secs: Option<u32>,
    /// Lifecycle state.
    pub state: EpisodeState,
    /// Audio file on disk while `Cached`; `None` otherwise.
    pub file_path: Option<PathBuf>,
    /// Size of the downloaded file in bytes.
    pub bytes: Option<u64>,
    /// blake3 hash of the downloaded file.
    pub blake3: Option<blake3::Hash>,
    /// MIME type of the audio.
    pub mime: Option<AudioMime>,
    /// When this episode was first seen.
    pub discovered_at: jiff::Timestamp,
    /// When the download last completed.
    pub downloaded_at: Option<jiff::Timestamp>,
    /// Bytes received so far, while `Downloading`.
    pub bytes_done: Option<u64>,
    /// Expected total bytes, while `Downloading` (when upstream said).
    pub bytes_total: Option<u64>,
    /// When progress was last reported — lets readers spot a stalled or
    /// abandoned download.
    pub progress_at: Option<jiff::Timestamp>,
}

/// Everything recorded about a finished download when an episode becomes
/// [`EpisodeState::Cached`].
#[derive(Debug, Clone)]
pub struct CachedFile {
    /// Where the audio landed.
    pub file_path: PathBuf,
    /// Verified size in bytes.
    pub bytes: u64,
    /// blake3 hash computed while streaming.
    pub blake3: blake3::Hash,
    /// MIME type, if known.
    pub mime: Option<AudioMime>,
    /// Duration probed from the file, used when the provider gave none.
    pub duration_secs: Option<u32>,
}

const SCHEMA_V1: &str = "
CREATE TABLE shows (
    slug          TEXT PRIMARY KEY,
    provider      TEXT NOT NULL,
    title         TEXT NOT NULL,
    description   TEXT,
    artwork_path  TEXT,
    last_poll_at  TEXT,
    last_poll_ok  INTEGER,
    last_error    TEXT
) STRICT;

CREATE TABLE episodes (
    show_slug           TEXT NOT NULL REFERENCES shows(slug),
    provider_episode_id TEXT NOT NULL,
    title               TEXT NOT NULL,
    description         TEXT,
    published_at        TEXT,
    duration_secs       INTEGER,
    state               TEXT NOT NULL,
    error_class         TEXT,
    file_path           TEXT,
    bytes               INTEGER,
    blake3              TEXT,
    mime                TEXT,
    discovered_at       TEXT NOT NULL,
    downloaded_at       TEXT,
    PRIMARY KEY (show_slug, provider_episode_id)
) STRICT;

CREATE INDEX episodes_by_show_state ON episodes (show_slug, state);
";

/// v2: in-flight download progress, written (throttled) by the download
/// engine and read by `status --watch` / `/debug` across processes.
const SCHEMA_V2: &str = "
ALTER TABLE episodes ADD COLUMN bytes_done INTEGER;
ALTER TABLE episodes ADD COLUMN bytes_total INTEGER;
ALTER TABLE episodes ADD COLUMN progress_at TEXT;
";

/// Handle to the SQLite state. Cheap to clone; all clones share one
/// connection.
#[derive(Clone)]
pub struct Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Storage {
    /// Open (creating if needed) the database at
    /// `<data_dir>/splicefeed.db` and bring the schema up to date.
    pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let data_dir = data_dir.as_ref().to_owned();
        run_blocking(move || {
            std::fs::create_dir_all(&data_dir).map_err(|source| StorageError::DataDir {
                path: data_dir.clone(),
                source,
            })?;
            let conn = Connection::open(data_dir.join("splicefeed.db"))?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.busy_timeout(Duration::from_secs(5))?;
            migrate(&conn)?;
            Ok(Self {
                conn: Arc::new(Mutex::new(conn)),
            })
        })
        .await
    }

    /// Insert or refresh a show's provider metadata. Poll bookkeeping and
    /// artwork are managed separately.
    pub async fn upsert_show(&self, meta: &ShowMeta, provider: &str) -> Result<(), StorageError> {
        let (meta, provider) = (meta.clone(), provider.to_owned());
        self.with(move |conn| {
            conn.execute(
                "INSERT INTO shows (slug, provider, title, description)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (slug) DO UPDATE
                 SET provider = excluded.provider,
                     title = excluded.title,
                     description = excluded.description",
                params![meta.slug.as_str(), provider, meta.title, meta.description],
            )?;
            Ok(())
        })
        .await
    }

    /// Record the outcome of a poll. A no-op when the show row does not
    /// exist yet (a poll that failed before the show was ever stored).
    pub async fn record_poll(
        &self,
        show: &ShowSlug,
        error: Option<String>,
    ) -> Result<(), StorageError> {
        let show = show.clone();
        self.with(move |conn| {
            conn.execute(
                "UPDATE shows
                 SET last_poll_at = ?2, last_poll_ok = ?3, last_error = ?4
                 WHERE slug = ?1",
                params![
                    show.as_str(),
                    jiff::Timestamp::now().to_string(),
                    error.is_none(),
                    error
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// All shows recorded in storage, ordered by slug. May include shows
    /// that are no longer configured — storage outlives config edits.
    pub async fn shows(&self) -> Result<Vec<ShowRecord>, StorageError> {
        self.with(|conn| {
            let mut stmt = conn.prepare(
                "SELECT slug, provider, title, description, artwork_path,
                        last_poll_at, last_poll_ok, last_error
                 FROM shows ORDER BY slug",
            )?;
            let rows = stmt.query_map([], decode_show)?;
            rows.map(|row| row?).collect()
        })
        .await
    }

    /// Record where a show's cached artwork lives.
    pub async fn set_artwork_path(&self, slug: &ShowSlug, path: &Path) -> Result<(), StorageError> {
        let (slug, path) = (slug.clone(), path.to_owned());
        self.with(move |conn| {
            conn.execute(
                "UPDATE shows SET artwork_path = ?2 WHERE slug = ?1",
                params![slug.as_str(), path.to_string_lossy()],
            )?;
            Ok(())
        })
        .await
    }

    /// Fetch one show row.
    pub async fn show(&self, slug: &ShowSlug) -> Result<Option<ShowRecord>, StorageError> {
        let slug = slug.clone();
        self.with(move |conn| {
            conn.query_row(
                "SELECT slug, provider, title, description, artwork_path,
                        last_poll_at, last_poll_ok, last_error
                 FROM shows WHERE slug = ?1",
                params![slug.as_str()],
                decode_show,
            )
            .optional()?
            .transpose()
        })
        .await
    }

    /// Record newly listed episodes: unknown ones are inserted as
    /// [`EpisodeState::Discovered`], known ones (any state, including
    /// pruned tombstones) get their provider metadata refreshed and their
    /// state left alone. Returns how many were new.
    pub async fn discover(
        &self,
        show: &ShowSlug,
        episodes: &[EpisodeMeta],
    ) -> Result<u32, StorageError> {
        let show = show.clone();
        let episodes = episodes.to_vec();
        self.with(move |conn| {
            let tx = conn.transaction()?;
            let mut new = 0;
            for episode in &episodes {
                let updated = tx.execute(
                    "UPDATE episodes
                     SET title = ?3, description = ?4, published_at = ?5,
                         duration_secs = COALESCE(?6, duration_secs)
                     WHERE show_slug = ?1 AND provider_episode_id = ?2",
                    params![
                        show.as_str(),
                        episode.id.as_str(),
                        episode.title,
                        episode.description,
                        episode.published_at.map(|t| t.to_string()),
                        episode.duration_secs,
                    ],
                )?;
                if updated == 0 {
                    tx.execute(
                        "INSERT INTO episodes
                         (show_slug, provider_episode_id, title, description,
                          published_at, duration_secs, state, discovered_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'discovered', ?7)",
                        params![
                            show.as_str(),
                            episode.id.as_str(),
                            episode.title,
                            episode.description,
                            episode.published_at.map(|t| t.to_string()),
                            episode.duration_secs,
                            jiff::Timestamp::now().to_string(),
                        ],
                    )?;
                    new += 1;
                }
            }
            tx.commit()?;
            Ok(new)
        })
        .await
    }

    /// All episodes of a show, newest first (`published_at` DESC with the
    /// episode id as a total-order tiebreak — the same order feeds use).
    pub async fn episodes(&self, show: &ShowSlug) -> Result<Vec<EpisodeRecord>, StorageError> {
        let show = show.clone();
        self.with(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT show_slug, provider_episode_id, title, description,
                        published_at, duration_secs, state, error_class,
                        file_path, bytes, blake3, mime, discovered_at,
                        downloaded_at, bytes_done, bytes_total, progress_at
                 FROM episodes WHERE show_slug = ?1
                 ORDER BY published_at DESC, provider_episode_id DESC",
            )?;
            stmt.query_map(params![show.as_str()], decode_episode)?
                .map(|row| row?)
                .collect()
        })
        .await
    }

    /// Move an episode into [`EpisodeState::Downloading`].
    pub async fn mark_downloading(
        &self,
        show: &ShowSlug,
        episode: &EpisodeId,
    ) -> Result<(), StorageError> {
        self.transition(show, episode, EpisodeState::Downloading, |tx, show, id| {
            tx.execute(
                "UPDATE episodes SET state = 'downloading', error_class = NULL,
                        bytes_done = NULL, bytes_total = NULL, progress_at = NULL
                 WHERE show_slug = ?1 AND provider_episode_id = ?2",
                params![show, id],
            )
        })
        .await
    }

    /// Record in-flight download progress. A no-op unless the episode is
    /// currently [`EpisodeState::Downloading`], so a racing completion
    /// never gets stale progress written over it.
    pub async fn set_progress(
        &self,
        show: &ShowSlug,
        episode: &EpisodeId,
        bytes_done: u64,
        bytes_total: Option<u64>,
    ) -> Result<(), StorageError> {
        let (show, episode) = (show.clone(), episode.clone());
        self.with(move |conn| {
            conn.execute(
                "UPDATE episodes
                 SET bytes_done = ?3, bytes_total = ?4, progress_at = ?5
                 WHERE show_slug = ?1 AND provider_episode_id = ?2
                   AND state = 'downloading'",
                params![
                    show.as_str(),
                    episode.as_str(),
                    i64::try_from(bytes_done).unwrap_or(i64::MAX),
                    bytes_total.map(|b| i64::try_from(b).unwrap_or(i64::MAX)),
                    jiff::Timestamp::now().to_string(),
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Move an episode into [`EpisodeState::Cached`], recording the file.
    pub async fn mark_cached(
        &self,
        show: &ShowSlug,
        episode: &EpisodeId,
        file: CachedFile,
    ) -> Result<(), StorageError> {
        self.transition(show, episode, EpisodeState::Cached, move |tx, show, id| {
            tx.execute(
                "UPDATE episodes
                 SET state = 'cached', error_class = NULL, file_path = ?3,
                     bytes = ?4, blake3 = ?5, mime = ?6,
                     duration_secs = COALESCE(duration_secs, ?7),
                     downloaded_at = ?8,
                     bytes_done = NULL, bytes_total = NULL, progress_at = NULL
                 WHERE show_slug = ?1 AND provider_episode_id = ?2",
                params![
                    show,
                    id,
                    file.file_path.to_string_lossy(),
                    i64::try_from(file.bytes).unwrap_or(i64::MAX),
                    file.blake3.to_hex().as_str(),
                    file.mime.as_ref().map(ToString::to_string),
                    file.duration_secs,
                    jiff::Timestamp::now().to_string(),
                ],
            )
        })
        .await
    }

    /// Move an episode into [`EpisodeState::Failed`] with a class.
    pub async fn mark_failed(
        &self,
        show: &ShowSlug,
        episode: &EpisodeId,
        class: ErrorClass,
    ) -> Result<(), StorageError> {
        self.transition(
            show,
            episode,
            EpisodeState::Failed(class),
            move |tx, show, id| {
                tx.execute(
                    "UPDATE episodes SET state = 'failed', error_class = ?3,
                            bytes_done = NULL, bytes_total = NULL,
                            progress_at = NULL
                     WHERE show_slug = ?1 AND provider_episode_id = ?2",
                    params![show, id, class.to_string()],
                )
            },
        )
        .await
    }

    /// Move an episode into [`EpisodeState::Pruned`]: the tombstone keeps
    /// the metadata and hash but forgets the file path.
    pub async fn mark_pruned(
        &self,
        show: &ShowSlug,
        episode: &EpisodeId,
    ) -> Result<(), StorageError> {
        self.transition(show, episode, EpisodeState::Pruned, |tx, show, id| {
            tx.execute(
                "UPDATE episodes SET state = 'pruned', file_path = NULL
                 WHERE show_slug = ?1 AND provider_episode_id = ?2",
                params![show, id],
            )
        })
        .await
    }

    /// Run a lifecycle transition atomically: read the current state,
    /// check legality, apply `update` — all in one transaction.
    async fn transition<F>(
        &self,
        show: &ShowSlug,
        episode: &EpisodeId,
        to: EpisodeState,
        update: F,
    ) -> Result<(), StorageError>
    where
        F: FnOnce(&rusqlite::Transaction<'_>, &str, &str) -> rusqlite::Result<usize>
            + Send
            + 'static,
    {
        let (show, episode) = (show.clone(), episode.clone());
        self.with(move |conn| {
            let tx = conn.transaction()?;
            let from: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT state, error_class FROM episodes
                     WHERE show_slug = ?1 AND provider_episode_id = ?2",
                    params![show.as_str(), episode.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((state, class)) = from else {
                return Err(StorageError::EpisodeNotFound { show, episode });
            };
            let from = decode_state(&state, class.as_deref())?;
            if !from.can_transition_to(to) {
                return Err(StorageError::IllegalTransition {
                    show,
                    episode,
                    from,
                    to,
                });
            }
            update(&tx, show.as_str(), episode.as_str())?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Run `f` against the connection on the blocking pool.
    async fn with<T, F>(&self, f: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StorageError> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        run_blocking(move || {
            let mut conn = conn.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            f(&mut conn)
        })
        .await
    }
}

async fn run_blocking<T, F>(f: F) -> Result<T, StorageError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, StorageError> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| StorageError::TaskFailed)?
}

fn migrate(conn: &Connection) -> Result<(), StorageError> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version < 1 {
        conn.execute_batch(SCHEMA_V1)?;
    }
    if version < 2 {
        conn.execute_batch(SCHEMA_V2)?;
    }
    conn.pragma_update(None, "user_version", 2)?;
    Ok(())
}

fn decode_show(row: &rusqlite::Row<'_>) -> rusqlite::Result<Result<ShowRecord, StorageError>> {
    let slug: String = row.get(0)?;
    let description: Option<String> = row.get(3)?;
    let artwork_path: Option<String> = row.get(4)?;
    let last_poll_at: Option<String> = row.get(5)?;
    Ok((|| {
        Ok(ShowRecord {
            slug: parse_col("shows", "slug", &slug)?,
            provider: row_get(row, 1)?,
            title: row_get(row, 2)?,
            description,
            artwork_path: artwork_path.map(PathBuf::from),
            last_poll_at: last_poll_at
                .as_deref()
                .map(|t| parse_col("shows", "last_poll_at", t))
                .transpose()?,
            last_poll_ok: row_get(row, 6)?,
            last_error: row_get(row, 7)?,
        })
    })())
}

fn decode_episode(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<EpisodeRecord, StorageError>> {
    let show: String = row.get(0)?;
    let id: String = row.get(1)?;
    let published_at: Option<String> = row.get(4)?;
    let state: String = row.get(6)?;
    let error_class: Option<String> = row.get(7)?;
    let file_path: Option<String> = row.get(8)?;
    let bytes: Option<i64> = row.get(9)?;
    let blake3: Option<String> = row.get(10)?;
    let mime: Option<String> = row.get(11)?;
    let discovered_at: String = row.get(12)?;
    let downloaded_at: Option<String> = row.get(13)?;
    let bytes_done: Option<i64> = row.get(14)?;
    let bytes_total: Option<i64> = row.get(15)?;
    let progress_at: Option<String> = row.get(16)?;
    Ok((|| {
        Ok(EpisodeRecord {
            show: parse_col("episodes", "show_slug", &show)?,
            id: parse_col("episodes", "provider_episode_id", &id)?,
            title: row_get(row, 2)?,
            description: row_get(row, 3)?,
            published_at: published_at
                .as_deref()
                .map(|t| parse_col("episodes", "published_at", t))
                .transpose()?,
            duration_secs: row_get(row, 5)?,
            state: decode_state(&state, error_class.as_deref())?,
            file_path: file_path.map(PathBuf::from),
            bytes: bytes.map(|b| u64::try_from(b).unwrap_or(0)),
            blake3: blake3
                .as_deref()
                .map(|hex| {
                    blake3::Hash::from_hex(hex).map_err(|e| StorageError::Corrupt {
                        table: "episodes",
                        reason: format!("column `blake3` value {hex:?}: {e}"),
                    })
                })
                .transpose()?,
            mime: mime.as_deref().map(AudioMime::from),
            discovered_at: parse_col("episodes", "discovered_at", &discovered_at)?,
            downloaded_at: downloaded_at
                .as_deref()
                .map(|t| parse_col("episodes", "downloaded_at", t))
                .transpose()?,
            bytes_done: bytes_done.map(|b| u64::try_from(b).unwrap_or(0)),
            bytes_total: bytes_total.map(|b| u64::try_from(b).unwrap_or(0)),
            progress_at: progress_at
                .as_deref()
                .map(|t| parse_col("episodes", "progress_at", t))
                .transpose()?,
        })
    })())
}

fn row_get<T: rusqlite::types::FromSql>(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> Result<T, StorageError> {
    row.get(index).map_err(Into::into)
}

fn parse_col<T>(table: &'static str, column: &str, raw: &str) -> Result<T, StorageError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    raw.parse().map_err(|e| StorageError::Corrupt {
        table,
        reason: format!("column `{column}` value {raw:?}: {e}"),
    })
}

fn decode_state(state: &str, class: Option<&str>) -> Result<EpisodeState, StorageError> {
    match (state, class) {
        ("discovered", _) => Ok(EpisodeState::Discovered),
        ("downloading", _) => Ok(EpisodeState::Downloading),
        ("cached", _) => Ok(EpisodeState::Cached),
        ("pruned", _) => Ok(EpisodeState::Pruned),
        ("failed", Some(class)) => Ok(EpisodeState::Failed(parse_col(
            "episodes",
            "error_class",
            class,
        )?)),
        ("failed", None) => Err(StorageError::Corrupt {
            table: "episodes",
            reason: "state is `failed` but error_class is NULL".into(),
        }),
        (other, _) => Err(StorageError::Corrupt {
            table: "episodes",
            reason: format!("unknown state {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: &str, published_at: &str) -> EpisodeMeta {
        EpisodeMeta {
            id: id.parse().expect("valid id"),
            title: format!("Episode {id}"),
            description: None,
            published_at: Some(published_at.parse().expect("valid timestamp")),
            duration_secs: Some(7200),
        }
    }

    fn show_meta(slug: &ShowSlug) -> ShowMeta {
        ShowMeta {
            slug: slug.clone(),
            title: "Test Show".into(),
            description: Some("about".into()),
            artwork: None,
        }
    }

    async fn open_temp() -> (tempfile::TempDir, Storage, ShowSlug) {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = Storage::open(dir.path()).await.expect("open");
        let slug: ShowSlug = "test-show".parse().expect("valid slug");
        storage
            .upsert_show(&show_meta(&slug), "difm")
            .await
            .expect("upsert show");
        (dir, storage, slug)
    }

    #[tokio::test]
    async fn discover_is_idempotent_and_counts_new() {
        let (_dir, storage, slug) = open_temp().await;
        let episodes = [
            meta("161", "2026-06-07T18:00:00Z"),
            meta("162", "2026-07-05T18:00:00Z"),
        ];
        assert_eq!(storage.discover(&slug, &episodes).await.unwrap(), 2);
        assert_eq!(storage.discover(&slug, &episodes).await.unwrap(), 0);

        let rows = storage.episodes(&slug).await.unwrap();
        assert_eq!(rows.len(), 2);
        // Newest first.
        assert_eq!(rows[0].id.as_str(), "162");
        assert_eq!(rows[0].state, EpisodeState::Discovered);
    }

    #[tokio::test]
    async fn full_lifecycle_roundtrip() {
        let (_dir, storage, slug) = open_temp().await;
        storage
            .discover(&slug, &[meta("162", "2026-07-05T18:00:00Z")])
            .await
            .unwrap();
        let id: EpisodeId = "162".parse().unwrap();

        let hash = blake3::hash(b"episode-162-audio");
        storage.mark_downloading(&slug, &id).await.unwrap();
        storage
            .mark_cached(
                &slug,
                &id,
                CachedFile {
                    file_path: "/data/media/test-show/162.m4a".into(),
                    bytes: 123_456_789,
                    blake3: hash,
                    mime: Some(AudioMime::Mp4),
                    duration_secs: None,
                },
            )
            .await
            .unwrap();

        let row = &storage.episodes(&slug).await.unwrap()[0];
        assert_eq!(row.state, EpisodeState::Cached);
        assert_eq!(row.bytes, Some(123_456_789));
        assert_eq!(row.duration_secs, Some(7200)); // provider value kept
        assert_eq!(row.mime, Some(AudioMime::Mp4));
        assert!(row.downloaded_at.is_some());

        storage.mark_pruned(&slug, &id).await.unwrap();
        let row = &storage.episodes(&slug).await.unwrap()[0];
        assert_eq!(row.state, EpisodeState::Pruned);
        assert_eq!(row.file_path, None);
        assert_eq!(row.blake3, Some(hash)); // tombstone keeps hash
    }

    #[tokio::test]
    async fn pruned_tombstone_is_not_rediscovered() {
        let (_dir, storage, slug) = open_temp().await;
        let episodes = [meta("162", "2026-07-05T18:00:00Z")];
        storage.discover(&slug, &episodes).await.unwrap();
        let id: EpisodeId = "162".parse().unwrap();
        storage.mark_downloading(&slug, &id).await.unwrap();
        storage
            .mark_cached(
                &slug,
                &id,
                CachedFile {
                    file_path: "/x/162.m4a".into(),
                    bytes: 1,
                    blake3: blake3::hash(b"x"),
                    mime: None,
                    duration_secs: None,
                },
            )
            .await
            .unwrap();
        storage.mark_pruned(&slug, &id).await.unwrap();

        // The next poll lists the same episode again: no new row, state
        // stays pruned.
        assert_eq!(storage.discover(&slug, &episodes).await.unwrap(), 0);
        assert_eq!(
            storage.episodes(&slug).await.unwrap()[0].state,
            EpisodeState::Pruned
        );
    }

    #[tokio::test]
    async fn illegal_transitions_are_rejected() {
        let (_dir, storage, slug) = open_temp().await;
        storage
            .discover(&slug, &[meta("162", "2026-07-05T18:00:00Z")])
            .await
            .unwrap();
        let id: EpisodeId = "162".parse().unwrap();

        // Discovered → Cached skips Downloading.
        let err = storage
            .mark_cached(
                &slug,
                &id,
                CachedFile {
                    file_path: "/x".into(),
                    bytes: 1,
                    blake3: blake3::hash(b"x"),
                    mime: None,
                    duration_secs: None,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::IllegalTransition { .. }));

        // Failed downloads are retryable.
        storage.mark_downloading(&slug, &id).await.unwrap();
        storage
            .mark_failed(&slug, &id, ErrorClass::Network)
            .await
            .unwrap();
        assert_eq!(
            storage.episodes(&slug).await.unwrap()[0].state,
            EpisodeState::Failed(ErrorClass::Network)
        );
        storage.mark_downloading(&slug, &id).await.unwrap();
    }

    #[tokio::test]
    async fn poll_bookkeeping_roundtrips() {
        let (_dir, storage, slug) = open_temp().await;
        storage
            .record_poll(&slug, Some("boom".into()))
            .await
            .unwrap();
        let show = storage.show(&slug).await.unwrap().expect("show exists");
        assert_eq!(show.last_poll_ok, Some(false));
        assert_eq!(show.last_error.as_deref(), Some("boom"));

        storage.record_poll(&slug, None).await.unwrap();
        let show = storage.show(&slug).await.unwrap().expect("show exists");
        assert_eq!(show.last_poll_ok, Some(true));
        assert_eq!(show.last_error, None);
        assert!(show.last_poll_at.is_some());

        // Unknown show: silently a no-op, never an error.
        let ghost: ShowSlug = "ghost".parse().unwrap();
        storage.record_poll(&ghost, None).await.unwrap();
        assert!(storage.show(&ghost).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn v1_databases_migrate_in_place() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Lay down a genuine v1 database with a row in it.
        {
            let conn = Connection::open(dir.path().join("splicefeed.db")).expect("open");
            conn.execute_batch(SCHEMA_V1).expect("v1 schema");
            conn.pragma_update(None, "user_version", 1)
                .expect("version");
            conn.execute(
                "INSERT INTO shows (slug, provider, title) VALUES ('test-show', 'difm', 'T')",
                [],
            )
            .expect("seed show");
            conn.execute(
                "INSERT INTO episodes (show_slug, provider_episode_id, title, state,
                 discovered_at) VALUES ('test-show', '162', 'E', 'discovered',
                 '2026-07-11T00:00:00Z')",
                [],
            )
            .expect("seed episode");
        }

        let storage = Storage::open(dir.path()).await.expect("migrates");
        let slug: ShowSlug = "test-show".parse().unwrap();
        let rows = storage.episodes(&slug).await.expect("reads post-migration");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bytes_done, None, "new columns default to NULL");
    }

    #[tokio::test]
    async fn progress_only_sticks_while_downloading() {
        let (_dir, storage, slug) = open_temp().await;
        storage
            .discover(&slug, &[meta("162", "2026-07-05T18:00:00Z")])
            .await
            .unwrap();
        let id: EpisodeId = "162".parse().unwrap();

        // Not downloading yet: the write is a guarded no-op.
        storage
            .set_progress(&slug, &id, 10, Some(100))
            .await
            .unwrap();
        assert_eq!(storage.episodes(&slug).await.unwrap()[0].bytes_done, None);

        storage.mark_downloading(&slug, &id).await.unwrap();
        storage
            .set_progress(&slug, &id, 10, Some(100))
            .await
            .unwrap();
        let row = &storage.episodes(&slug).await.unwrap()[0];
        assert_eq!(row.bytes_done, Some(10));
        assert_eq!(row.bytes_total, Some(100));
        assert!(row.progress_at.is_some());

        // Completion clears the transient progress fields.
        storage
            .mark_cached(
                &slug,
                &id,
                CachedFile {
                    file_path: "/x/162.m4a".into(),
                    bytes: 100,
                    blake3: blake3::hash(b"x"),
                    mime: None,
                    duration_secs: None,
                },
            )
            .await
            .unwrap();
        let row = &storage.episodes(&slug).await.unwrap()[0];
        assert_eq!(row.bytes_done, None);
        assert_eq!(row.progress_at, None);
    }

    #[tokio::test]
    async fn shows_lists_all_rows_ordered() {
        let (_dir, storage, slug) = open_temp().await;
        let other: ShowSlug = "anything-melodic".parse().unwrap();
        storage
            .upsert_show(&show_meta(&other), "difm")
            .await
            .unwrap();

        let shows = storage.shows().await.unwrap();
        assert_eq!(shows.len(), 2);
        assert_eq!(shows[0].slug, other); // "anything-melodic" < "test-show"
        assert_eq!(shows[1].slug, slug);
    }
}
