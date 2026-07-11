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
//! library.write_feed(&slug, &mut feed).await?;
//! # Ok(()) }
//! ```
//!
//! `examples/sync_once.rs` is the compile-tested contract of this API.

#![deny(missing_docs)]

use std::io::Write;
use std::path::PathBuf;

use futures_util::StreamExt;
use splicefeed_core::download::{Downloader, blake3_of_file, probe_duration};
use splicefeed_core::storage::{CachedFile, Storage};
use splicefeed_core::{retention, rss};
use url::Url;

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
    /// The show is configured but has never been synced — there is
    /// nothing in storage to build a feed from.
    #[error("show `{0}` has not been synced yet")]
    NotSynced(ShowSlug),
    /// Writing the feed to the output sink failed.
    #[error("failed to write feed: {0}")]
    Feed(#[from] std::io::Error),
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

    /// A new `Library` over `config`, sharing this one's storage handle
    /// (and its download engine, unless `download_concurrency` changed —
    /// sharing keeps in-flight transfers counted against the one true
    /// limit across a reload). Providers are rebuilt, so credential
    /// changes take effect.
    ///
    /// The caller must not change `data_dir` between the two configs:
    /// storage is shared, so the new config's data_dir is assumed to be
    /// the old one's. The daemon enforces this before calling.
    pub async fn reload(&self, config: Config) -> Result<Library, LibraryError> {
        let providers = ProviderRegistry::from_config(&config)?;
        let downloader = if config.download_concurrency() == self.config.download_concurrency() {
            self.downloader.clone()
        } else {
            Downloader::new(config.download_concurrency())?
        };
        Ok(Library {
            config,
            providers,
            storage: self.storage.clone(),
            downloader,
        })
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

    /// Poll one show: discover new episodes, download the ones retention
    /// will keep (bounded concurrency, one failure never aborts the
    /// rest), apply retention.
    ///
    /// Retention is planned before downloading, so an episode that would
    /// be pruned immediately is never fetched — and a pruned tombstone
    /// that fits a widened retention window is revived and re-downloaded.
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
        self.cache_artwork(show, &meta, slug).await;
        let discovered = self.storage.discover(slug, &episodes).await?;

        // Plan retention over the listing *before* downloading: an
        // episode the policy would prune right back out is never fetched
        // (keep_last = 1 against a 25-episode listing downloads one
        // file, not 25), and a tombstone that fits a widened retention
        // window is revived and re-downloaded. Projected sizes use
        // recorded bytes where known — tombstones keep theirs — and 0
        // for the never-downloaded, so an optimistic fetch under max_gb
        // is corrected (and remembered) after one cycle at most.
        //
        // Only episodes in the current (possibly fetch_last-bounded)
        // listing are considered at all: rows outside it stay put, and a
        // Failed episode upstream dropped can't retry forever.
        let policy = show.retention(self.config.retention());
        let records = self.storage.episodes(slug).await?;
        let by_id: std::collections::HashMap<&EpisodeId, &EpisodeRecord> =
            records.iter().map(|record| (&record.id, record)).collect();
        let candidates: Vec<retention::Candidate> = episodes
            .iter()
            .map(|ep| retention::Candidate {
                id: ep.id.clone(),
                bytes: by_id
                    .get(&ep.id)
                    .and_then(|record| record.bytes)
                    .unwrap_or(0),
            })
            .collect();
        let want: std::collections::HashSet<EpisodeId> = retention::split(&policy, &candidates)
            .0
            .into_iter()
            .collect();

        let pending: Vec<&EpisodeRecord> = episodes
            .iter()
            .filter(|ep| want.contains(&ep.id))
            .filter_map(|ep| by_id.get(&ep.id).copied())
            .filter(|record| {
                matches!(
                    record.state,
                    EpisodeState::Discovered | EpisodeState::Failed(_) | EpisodeState::Pruned
                )
            })
            .collect();
        // Futures built eagerly: a lazy `.map` here leaves a
        // higher-ranked closure bound the compiler cannot discharge once
        // this future crosses a `tokio::spawn` (`'static`) boundary.
        let downloads: Vec<_> = pending
            .iter()
            .copied()
            .map(|ep| self.download_episode(provider.as_ref(), slug, ep))
            .collect();
        let downloaded = futures_util::stream::iter(downloads)
            .buffer_unordered(self.config.download_concurrency().get())
            .fold(0_u32, |done, ok| async move { done + u32::from(ok) })
            .await;

        let pruned = self.apply_retention(slug, &policy).await?;

        self.storage.record_poll(slug, None).await?;
        Ok(SyncReport {
            discovered,
            downloaded,
            pruned,
        })
    }

    /// Cache the show's artwork to `<data_dir>/artwork/<slug>.<ext>`,
    /// once. The config override (local path or URL) beats provider
    /// artwork. Best-effort by design: failure is logged and the feed
    /// simply omits its `itunes:image` — never fatal to a sync.
    async fn cache_artwork(&self, show: &ShowConfig, meta: &ShowMeta, slug: &ShowSlug) {
        enum Source {
            Fetch(Url),
            Copy(PathBuf),
        }
        let existing = match self.storage.show(slug).await {
            Ok(record) => record.and_then(|r| r.artwork_path),
            Err(err) => {
                tracing::warn!(show = %slug, error = %err, "artwork: storage lookup failed");
                return;
            }
        };
        if let Some(path) = &existing
            && tokio::fs::try_exists(path).await.unwrap_or(false)
        {
            return;
        }
        let source = match show.artwork() {
            Some(ArtworkOverride::Url(url)) => Some(Source::Fetch(url.clone())),
            Some(ArtworkOverride::Path(path)) => Some(Source::Copy(path.clone())),
            None => meta.artwork.clone().map(Source::Fetch),
        };
        let Some(source) = source else { return };

        let ext = match &source {
            Source::Fetch(url) => url_extension(url).unwrap_or("img").to_owned(),
            Source::Copy(path) => path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("img")
                .to_owned(),
        };
        let dest = self
            .config
            .data_dir()
            .join("artwork")
            .join(format!("{slug}.{ext}"));
        let fetched = match source {
            Source::Fetch(url) => self
                .downloader
                .fetch(
                    &AudioSource {
                        url,
                        mime: None,
                        bytes: None,
                    },
                    &dest,
                    None,
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
            Source::Copy(from) => {
                async {
                    if let Some(parent) = dest.parent() {
                        tokio::fs::create_dir_all(parent)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    tokio::fs::copy(&from, &dest)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
                .await
            }
        };
        match fetched {
            Ok(()) => {
                if let Err(err) = self.storage.set_artwork_path(slug, &dest).await {
                    tracing::warn!(show = %slug, error = %err, "artwork: could not record path");
                } else {
                    tracing::info!(show = %slug, path = %dest.display(), "artwork cached");
                }
            }
            Err(reason) => {
                tracing::warn!(show = %slug, %reason, "artwork caching failed; feed will omit it");
            }
        }
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

    /// Write the show's podcast RSS feed. Byte-identical across calls
    /// when nothing changed. Enclosure and artwork URLs are built from
    /// the configured external base URL — never the bind address — and
    /// point at this daemon's `/media` and `/artwork` routes, so no
    /// upstream credential can ever appear in a feed.
    pub async fn write_feed<W: Write>(
        &self,
        slug: &ShowSlug,
        out: &mut W,
    ) -> Result<(), LibraryError> {
        let show = self.show_config(slug)?;
        let record = self
            .storage
            .show(slug)
            .await?
            .ok_or_else(|| LibraryError::NotSynced(slug.clone()))?;
        let base = self.config.external_base_url();

        let title = show
            .title()
            .map_or_else(|| record.title.clone(), str::to_owned);
        let items = self
            .episode_records(slug)
            .await?
            .into_iter()
            .filter(|ep| matches!(ep.state, EpisodeState::Cached))
            .filter_map(|ep| {
                let file = ep.file_path.as_deref()?.file_name()?.to_str()?.to_owned();
                Some(rss::Item {
                    guid: format!("{}/{}/{}", show.provider(), slug, ep.id),
                    title: ep.title,
                    description: ep.description,
                    published_at: ep.published_at,
                    enclosure_url: under(&base, &["media", slug.as_str(), &file])?,
                    enclosure_bytes: ep.bytes?,
                    enclosure_mime: ep
                        .mime
                        .map_or_else(|| "application/octet-stream".to_owned(), |m| m.to_string()),
                    duration_secs: ep.duration_secs,
                })
            })
            .collect();

        let feed = rss::Feed {
            link: base.clone(),
            description: record.description.unwrap_or_else(|| title.clone()),
            title,
            artwork: record
                .artwork_path
                .as_deref()
                .and_then(|p| p.file_name()?.to_str())
                .and_then(|name| under(&base, &["artwork", name])),
            items,
        };
        Ok(rss::write(&feed, out)?)
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
        let progress = progress_writer(self.storage.clone(), slug.clone(), episode.id.clone());
        let got = self
            .downloader
            .fetch(&source, &dest, Some(&progress))
            .await?;
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

/// A URL under `base`, extending its path — correct whether or not the
/// base has a trailing slash (`Url::join` would swallow a last segment).
fn under(base: &Url, segments: &[&str]) -> Option<Url> {
    let mut url = base.clone();
    url.path_segments_mut()
        .ok()?
        .pop_if_empty()
        .extend(segments);
    Some(url)
}

/// A throttled progress callback for one episode: at most one storage
/// write per second, spawned off the transfer loop so the download never
/// waits on SQLite. The write is guarded by `state = 'downloading'`, so a
/// racing completion wins.
fn progress_writer(
    storage: Storage,
    slug: ShowSlug,
    id: EpisodeId,
) -> impl Fn(u64, Option<u64>) + Send + Sync {
    use std::sync::atomic::{AtomicU64, Ordering};
    let started = std::time::Instant::now();
    let last_write_ms = AtomicU64::new(u64::MAX); // force an immediate first write
    move |done, total| {
        let now_ms = started.elapsed().as_millis() as u64;
        let prev = last_write_ms.load(Ordering::Relaxed);
        if prev != u64::MAX && now_ms.saturating_sub(prev) < 1_000 {
            return;
        }
        if last_write_ms
            .compare_exchange(prev, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return; // another chunk's callback just wrote
        }
        let (storage, slug, id) = (storage.clone(), slug.clone(), id.clone());
        tokio::spawn(async move {
            if let Err(err) = storage.set_progress(&slug, &id, done, total).await {
                tracing::debug!(show = %slug, episode = %id, error = %err,
                    "progress write failed");
            }
        });
    }
}

/// File extension for an audio source, from its MIME type when known,
/// else from the URL path, else a neutral fallback.
fn extension_for(source: &AudioSource) -> &str {
    if let Some(ext) = source.mime.as_ref().and_then(AudioMime::extension) {
        return ext;
    }
    url_extension(&source.url).unwrap_or("bin")
}

/// A plausible file extension from a URL path, if it has one.
fn url_extension(url: &Url) -> Option<&str> {
    url.path().rsplit('.').next().filter(|ext| {
        !ext.contains('/')
            && (1..=4).contains(&ext.len())
            && ext.chars().all(|c| c.is_ascii_alphanumeric())
    })
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
