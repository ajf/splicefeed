//! splicefeed — turn DI.FM premium radio shows into standard podcast RSS
//! feeds you host yourself.
//!
//! This facade crate is the library a downstream user depends on. It has no
//! HTTP server, TUI, or CLI in its dependency tree; the `splicefeed-daemon`
//! binary is a thin shell over it. The embedded/cron use case is:
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let config = splicefeed::Config::load(None)?;
//! let library = splicefeed::Library::open(config).await?;
//! let slug: splicefeed::ShowSlug = "melodik-revolution".parse()?;
//! library.sync(&slug).await?;
//! let mut feed = Vec::new();
//! library.write_feed(&slug, &mut feed)?;
//! # Ok(()) }
//! ```
//!
//! `examples/sync_once.rs` is the compile-tested contract of this API.

#![deny(missing_docs)]

use std::io::Write;
use std::path::PathBuf;

use futures_util::StreamExt;
use splicefeed_core::download::{Downloader, blake3_of_file, probe_duration};
use splicefeed_core::retention;
use splicefeed_core::storage::{CachedFile, Storage};

pub use splicefeed_core::config::{ArtworkOverride, Config, ConfigError, Retention, ShowConfig};
pub use splicefeed_core::domain::{
    ApiKey, AudioMime, AudioSource, EpisodeId, EpisodeMeta, EpisodeState, ErrorClass, ListenKey,
    Mode, RedactedUrl, ShowMeta, ShowSlug, redacted,
};
pub use splicefeed_core::download::DownloadError;
pub use splicefeed_core::ipc;
pub use splicefeed_core::storage::{EpisodeRecord, ShowRecord, StorageError};
pub use splicefeed_providers::{Provider, ProviderError, ProviderRegistry};

/// Errors surfaced by [`Library`] operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LibraryError {
    /// Configuration failed to load or validate.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// A provider operation failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// The SQLite state failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The download engine could not be constructed.
    #[error(transparent)]
    Download(#[from] DownloadError),
    /// The named show is not in the configuration.
    #[error("show `{0}` is not configured")]
    UnknownShow(ShowSlug),
}

/// What [`Library::verify`] found for one show.
#[derive(Debug, Clone, serde::Serialize)]
pub struct VerifyReport {
    /// Cached episodes examined.
    pub checked: u32,
    /// Episodes whose file matched its record exactly.
    pub intact: u32,
    /// Episodes whose file did not (empty = everything checks out).
    pub problems: Vec<VerifyOutcome>,
}

/// One episode that failed verification.
#[derive(Debug, Clone, serde::Serialize)]
pub struct VerifyOutcome {
    /// The episode.
    pub id: EpisodeId,
    /// What was wrong with its file.
    pub problem: FileProblem,
    /// Whether a re-download restored it (always `false` without
    /// `fix`; when `false` under `fix`, the episode is now recorded as
    /// [`EpisodeState::Failed`] and retried by later syncs).
    pub fixed: bool,
}

/// How a cached episode's file can disagree with its database record.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileProblem {
    /// The file is gone (or the record never stored a path).
    Missing,
    /// The file exists but its size differs from the record.
    SizeMismatch {
        /// Bytes the record promises.
        expected: u64,
        /// Bytes on disk.
        actual: u64,
    },
    /// Right size, wrong content: the blake3 does not match.
    HashMismatch,
    /// The file could not be read at all.
    Unreadable {
        /// The underlying I/O error.
        reason: String,
    },
}

impl std::fmt::Display for FileProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => f.write_str("missing"),
            Self::SizeMismatch { expected, actual } => {
                write!(f, "size mismatch (expected {expected} bytes, got {actual})")
            }
            Self::HashMismatch => f.write_str("hash mismatch"),
            Self::Unreadable { reason } => write!(f, "unreadable: {reason}"),
        }
    }
}

/// What a [`Library::sync`] run did for one show.
#[derive(Debug, Clone, Copy, Default)]
pub struct SyncReport {
    /// Episodes newly discovered by this sync.
    pub discovered: u32,
    /// Episodes downloaded to disk.
    pub downloaded: u32,
    /// Episodes removed by retention.
    pub pruned: u32,
}

/// Handle over the whole backend: providers, storage, downloads, retention,
/// and feed generation — everything except serving.
pub struct Library {
    config: Config,
    providers: ProviderRegistry,
    storage: Storage,
    downloader: Downloader,
}

impl Library {
    /// Open the library: instantiate the providers required by `config`,
    /// the SQLite state in the data directory, and the download engine.
    pub async fn open(config: Config) -> Result<Self, LibraryError> {
        let providers = ProviderRegistry::from_config(&config)?;
        let storage = Storage::open(config.data_dir()).await?;
        let downloader = Downloader::new(config.download_concurrency())?;
        Ok(Self {
            config,
            providers,
            storage,
            downloader,
        })
    }

    /// The validated configuration this library was opened with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// All shows recorded in local storage, ordered by slug, with poll
    /// bookkeeping. May include shows no longer in the configuration —
    /// storage outlives config edits.
    pub async fn show_records(&self) -> Result<Vec<ShowRecord>, LibraryError> {
        Ok(self.storage.shows().await?)
    }

    /// Everything storage knows about one show's episodes, newest first:
    /// lifecycle state, file location, size, hash, MIME, timestamps.
    pub async fn episode_records(
        &self,
        slug: &ShowSlug,
    ) -> Result<Vec<EpisodeRecord>, LibraryError> {
        Ok(self.storage.episodes(slug).await?)
    }

    /// Poll one show: discover new episodes, download them (bounded
    /// concurrency, one failure never aborts the rest), apply retention.
    ///
    /// Individual episode failures are recorded as
    /// [`EpisodeState::Failed`] and retried on the next sync; only a
    /// failure to *list* the show (provider down, show gone) is an error.
    pub async fn sync(&self, slug: &ShowSlug) -> Result<SyncReport, LibraryError> {
        let show = self.show_config(slug)?;
        let provider = self.providers.get(show.provider())?;
        let limit = show.fetch_last(self.config.fetch_last());

        let listing = async {
            let meta = provider.show(slug).await?;
            let episodes = provider.episodes(slug, limit).await?;
            Ok::<_, ProviderError>((meta, episodes))
        }
        .await;
        let (meta, episodes) = match listing {
            Ok(ok) => ok,
            Err(err) => {
                self.storage
                    .record_poll(slug, Some(err.to_string()))
                    .await?;
                return Err(err.into());
            }
        };
        self.storage.upsert_show(&meta, show.provider()).await?;
        let discovered = self.storage.discover(slug, &episodes).await?;

        // Only episodes in the current (possibly fetch_last-bounded)
        // listing are downloaded or retried: older Discovered/Failed rows
        // stay put instead of being fetched outside the window — and a
        // Failed episode that upstream dropped can't retry forever.
        let listed: std::collections::HashSet<&EpisodeId> =
            episodes.iter().map(|ep| &ep.id).collect();
        let pending: Vec<EpisodeRecord> = self
            .storage
            .episodes(slug)
            .await?
            .into_iter()
            .filter(|ep| {
                listed.contains(&ep.id)
                    && matches!(ep.state, EpisodeState::Discovered | EpisodeState::Failed(_))
            })
            .collect();
        let downloaded = futures_util::stream::iter(
            pending
                .iter()
                .map(|ep| self.download_episode(provider.as_ref(), slug, ep)),
        )
        .buffer_unordered(self.config.download_concurrency().get())
        .fold(0_u32, |done, ok| async move { done + u32::from(ok) })
        .await;

        let policy = show.retention(self.config.retention());
        let pruned = self.apply_retention(slug, &policy).await?;

        self.storage.record_poll(slug, None).await?;
        Ok(SyncReport {
            discovered,
            downloaded,
            pruned,
        })
    }

    /// Check every cached episode of a show against its database record:
    /// the file must exist, match the recorded size, and match the
    /// recorded blake3. With `fix`, damaged episodes are re-downloaded on
    /// the spot (fresh audio URL, atomic replace, record updated); a
    /// failed fix leaves the episode [`EpisodeState::Failed`] for later
    /// syncs to retry.
    ///
    /// Checking needs only storage; fixing requires the show to be
    /// configured (its provider supplies the new download).
    pub async fn verify(&self, slug: &ShowSlug, fix: bool) -> Result<VerifyReport, LibraryError> {
        let provider = if fix {
            Some(self.providers.get(self.show_config(slug)?.provider())?)
        } else {
            None
        };
        let cached: Vec<EpisodeRecord> = self
            .storage
            .episodes(slug)
            .await?
            .into_iter()
            .filter(|ep| matches!(ep.state, EpisodeState::Cached))
            .collect();

        let problems: Vec<VerifyOutcome> = futures_util::stream::iter(
            cached
                .iter()
                .map(|ep| self.verify_episode(provider.map(|p| p.as_ref()), slug, ep)),
        )
        .buffer_unordered(self.config.download_concurrency().get())
        .filter_map(|outcome| async move { outcome })
        .collect()
        .await;

        let checked = cached.len() as u32;
        Ok(VerifyReport {
            checked,
            intact: checked - problems.len() as u32,
            problems,
        })
    }

    /// `None` when the episode's file matches its record; otherwise the
    /// problem, after an optional fix attempt.
    async fn verify_episode(
        &self,
        provider: Option<&dyn Provider>,
        slug: &ShowSlug,
        episode: &EpisodeRecord,
    ) -> Option<VerifyOutcome> {
        let problem = check_file(episode).await?;
        tracing::warn!(show = %slug, episode = %episode.id, %problem,
            "cached file failed verification");
        let fixed = match provider {
            Some(provider) => self.download_episode(provider, slug, episode).await,
            None => false,
        };
        Some(VerifyOutcome {
            id: episode.id.clone(),
            problem,
            fixed,
        })
    }

    /// Write the show's podcast RSS feed. Byte-identical across calls when
    /// no episodes changed.
    pub fn write_feed<W: Write>(&self, slug: &ShowSlug, _out: &mut W) -> Result<(), LibraryError> {
        let _show = self.show_config(slug)?;
        todo!("milestone 4: deterministic RSS generation")
    }

    fn show_config(&self, slug: &ShowSlug) -> Result<&ShowConfig, LibraryError> {
        self.config
            .shows()
            .iter()
            .find(|s| s.slug() == slug)
            .ok_or_else(|| LibraryError::UnknownShow(slug.clone()))
    }

    /// Download one episode end to end, recording the outcome. Returns
    /// whether the episode is now cached; never propagates the failure —
    /// it is stored, logged, and retried next sync.
    async fn download_episode(
        &self,
        provider: &dyn Provider,
        slug: &ShowSlug,
        episode: &EpisodeRecord,
    ) -> bool {
        match self.try_download(provider, slug, episode).await {
            Ok(()) => {
                tracing::info!(show = %slug, episode = %episode.id, "episode cached");
                true
            }
            Err(err) => {
                tracing::warn!(show = %slug, episode = %episode.id, error = %err,
                    "episode download failed");
                if let Err(err) = self
                    .storage
                    .mark_failed(slug, &episode.id, err.class())
                    .await
                {
                    tracing::error!(show = %slug, episode = %episode.id, error = %err,
                        "could not record download failure");
                }
                false
            }
        }
    }

    async fn try_download(
        &self,
        provider: &dyn Provider,
        slug: &ShowSlug,
        episode: &EpisodeRecord,
    ) -> Result<(), EpisodeSyncError> {
        self.storage.mark_downloading(slug, &episode.id).await?;
        let source = provider.resolve_audio(slug, &episode.id).await?;
        let dest = self.media_path(slug, &episode.id, &source);
        let got = self.downloader.fetch(&source, &dest).await?;
        // Only probe the file when the provider's listing had no duration.
        let duration_secs = match episode.duration_secs {
            Some(_) => None,
            None => probe_duration(&dest).await,
        };
        self.storage
            .mark_cached(
                slug,
                &episode.id,
                CachedFile {
                    file_path: dest,
                    bytes: got.bytes,
                    blake3: got.blake3,
                    mime: got.mime,
                    duration_secs,
                },
            )
            .await?;
        Ok(())
    }

    async fn apply_retention(
        &self,
        slug: &ShowSlug,
        policy: &Retention,
    ) -> Result<u32, LibraryError> {
        let records = self.storage.episodes(slug).await?;
        let cached: Vec<retention::Candidate> = records
            .iter()
            .filter(|ep| ep.state == EpisodeState::Cached)
            .map(|ep| retention::Candidate {
                id: ep.id.clone(),
                bytes: ep.bytes.unwrap_or(0),
            })
            .collect();

        let mut pruned = 0;
        for id in retention::plan(policy, &cached) {
            let file = records
                .iter()
                .find(|ep| ep.id == id)
                .and_then(|ep| ep.file_path.clone());
            if let Some(path) = file {
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        tracing::warn!(show = %slug, episode = %id, path = %path.display(),
                            error = %e, "could not delete pruned file; keeping episode cached");
                        continue;
                    }
                }
            }
            self.storage.mark_pruned(slug, &id).await?;
            tracing::info!(show = %slug, episode = %id, "episode pruned by retention");
            pruned += 1;
        }
        Ok(pruned)
    }

    /// Where an episode's audio lives:
    /// `<data_dir>/media/<show>/<episode>.<ext>`. The episode id is
    /// sanitized for use as a file name — GUIDs never derive from paths,
    /// so this is purely cosmetic.
    fn media_path(&self, slug: &ShowSlug, id: &EpisodeId, source: &AudioSource) -> PathBuf {
        let stem: String = id
            .as_str()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.config
            .data_dir()
            .join("media")
            .join(slug.as_str())
            .join(format!("{stem}.{}", extension_for(source)))
    }
}

/// How a cached episode's file disagrees with its record, if it does.
async fn check_file(episode: &EpisodeRecord) -> Option<FileProblem> {
    let Some(path) = episode.file_path.as_deref() else {
        return Some(FileProblem::Missing);
    };
    let meta = match tokio::fs::metadata(path).await {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Some(FileProblem::Missing),
        Err(e) => {
            return Some(FileProblem::Unreadable {
                reason: e.to_string(),
            });
        }
    };
    if let Some(expected) = episode.bytes
        && meta.len() != expected
    {
        return Some(FileProblem::SizeMismatch {
            expected,
            actual: meta.len(),
        });
    }
    // A cached row without a hash has nothing further to check against.
    let expected = episode.blake3?;
    match blake3_of_file(path).await {
        Ok(actual) if actual == expected => None,
        Ok(_) => Some(FileProblem::HashMismatch),
        Err(e) => Some(FileProblem::Unreadable {
            reason: e.to_string(),
        }),
    }
}

/// File extension for an audio source, from its MIME type when known,
/// else from the URL path, else a neutral fallback.
fn extension_for(source: &AudioSource) -> &str {
    if let Some(ext) = source.mime.as_ref().and_then(AudioMime::extension) {
        return ext;
    }
    match source.url.path().rsplit('.').next() {
        Some(ext)
            if !ext.contains('/')
                && (1..=4).contains(&ext.len())
                && ext.chars().all(|c| c.is_ascii_alphanumeric()) =>
        {
            ext
        }
        _ => "bin",
    }
}

/// A failure while syncing one episode, unified across the provider,
/// download, and storage layers so it can be classified and recorded.
#[derive(Debug, thiserror::Error)]
enum EpisodeSyncError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl EpisodeSyncError {
    fn class(&self) -> ErrorClass {
        match self {
            Self::Provider(err) => match err {
                ProviderError::Http(_) => ErrorClass::Network,
                ProviderError::Status { .. } | ProviderError::ShowNotFound(_) => {
                    ErrorClass::HttpStatus
                }
                ProviderError::Parse { .. } | ProviderError::NoAudioAsset { .. } => {
                    ErrorClass::Parse
                }
                _ => ErrorClass::Network,
            },
            Self::Download(err) => err.class(),
            Self::Storage(_) => ErrorClass::Disk,
        }
    }
}
