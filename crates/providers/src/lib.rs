//! The [`Provider`] abstraction and its implementations.
//!
//! Adding a provider requires a trait impl plus one registration entry in
//! [`ProviderRegistry::from_config`] — scheduler, downloader, storage, RSS,
//! and server code never change.

#![deny(missing_docs)]

pub mod difm;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use splicefeed_core::config::Config;
use splicefeed_core::domain::{AudioSource, EpisodeId, EpisodeMeta, ShowMeta, ShowSlug};
use url::Url;

/// Errors surfaced by providers.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProviderError {
    /// No provider is registered under this name.
    #[error("no provider registered as `{0}`")]
    UnknownProvider(String),
    /// The provider's required credentials are missing from config.
    #[error("provider `{0}` requires credentials that are not configured")]
    MissingCredentials(&'static str),
    /// The provider does not know this show.
    #[error("show `{0}` not found upstream")]
    ShowNotFound(ShowSlug),
    /// Transport-level failure talking to the upstream API.
    #[error("upstream request failed: {0}")]
    Http(String),
    /// The response arrived but could not be parsed; the payload was
    /// quarantined at the given path.
    #[error("unparseable upstream response (quarantined at {quarantine_path})")]
    Parse {
        /// Where the raw payload was written for inspection.
        quarantine_path: String,
    },
}

/// A source of shows and episodes (DI.FM/AudioAddict is the first impl).
///
/// Implementations are `Send + Sync` and shared as `Arc<dyn Provider>`;
/// methods borrow their inputs and return owned domain types at the
/// boundary. (`#[async_trait]` because `async fn` in traits is not
/// object-safe and the registry is `dyn`.)
#[async_trait]
pub trait Provider: Send + Sync {
    /// Fetch show-level metadata.
    async fn show(&self, slug: &ShowSlug) -> Result<ShowMeta, ProviderError>;

    /// List episodes for a show, newest first.
    async fn episodes(&self, slug: &ShowSlug) -> Result<Vec<EpisodeMeta>, ProviderError>;

    /// Resolve the downloadable audio URL for an episode (credentials
    /// included — sensitive, redact before logging).
    async fn resolve_audio(&self, episode: &EpisodeId) -> Result<AudioSource, ProviderError>;

    /// Fetch the show's artwork URL, if it has one.
    async fn artwork(&self, slug: &ShowSlug) -> Result<Option<Url>, ProviderError>;
}

/// Builds a [`Provider`] from its section of the daemon [`Config`].
///
/// Each provider owns its config/auth shape; the registry only knows names.
pub trait ProviderFactory {
    /// The TOML `provider = "..."` string this factory answers to.
    fn name(&self) -> &'static str;

    /// Construct the provider, failing if its credentials are absent.
    fn create(&self, config: &Config) -> Result<Arc<dyn Provider>, ProviderError>;
}

/// Instantiated providers, keyed by their TOML registry name.
pub struct ProviderRegistry {
    providers: HashMap<&'static str, Arc<dyn Provider>>,
}

impl ProviderRegistry {
    /// Instantiate every provider referenced by the configured shows.
    ///
    /// New providers are registered by adding one factory to the list here.
    pub fn from_config(config: &Config) -> Result<Self, ProviderError> {
        let factories: &[&dyn ProviderFactory] = &[&difm::DifmFactory];

        let mut providers = HashMap::new();
        for show in config.shows() {
            let name = show.provider();
            if providers.contains_key(name) {
                continue;
            }
            let factory = factories
                .iter()
                .find(|f| f.name() == name)
                .ok_or_else(|| ProviderError::UnknownProvider(name.to_owned()))?;
            providers.insert(factory.name(), factory.create(config)?);
        }
        Ok(Self { providers })
    }

    /// Look up a provider by registry name.
    pub fn get(&self, name: &str) -> Result<&Arc<dyn Provider>, ProviderError> {
        self.providers
            .get(name)
            .ok_or_else(|| ProviderError::UnknownProvider(name.to_owned()))
    }
}
