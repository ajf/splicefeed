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

pub use splicefeed_core::config::{ArtworkOverride, Config, ConfigError, Retention, ShowConfig};
pub use splicefeed_core::domain::{
    AudioSource, EpisodeId, EpisodeMeta, EpisodeState, ErrorClass, ListenKey, Mode, ShowMeta,
    ShowSlug,
};
pub use splicefeed_core::ipc;
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
    /// The named show is not in the configuration.
    #[error("show `{0}` is not configured")]
    UnknownShow(ShowSlug),
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
    #[allow(dead_code)] // consumed by sync() from milestone 3
    providers: ProviderRegistry,
}

impl Library {
    /// Open the library: instantiate the providers required by `config` and
    /// (from milestone 3) the SQLite state in the data directory.
    pub async fn open(config: Config) -> Result<Self, LibraryError> {
        let providers = ProviderRegistry::from_config(&config)?;
        Ok(Self { config, providers })
    }

    /// The validated configuration this library was opened with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Poll one show: discover new episodes, download them, apply retention.
    pub async fn sync(&self, slug: &ShowSlug) -> Result<SyncReport, LibraryError> {
        let _show = self.show_config(slug)?;
        todo!("milestone 3: episode sync engine (storage + downloader)")
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
}
