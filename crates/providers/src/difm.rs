//! DI.FM / AudioAddict provider.
//!
//! The upstream API is undocumented and treated as reverse-engineered and
//! fragile. Endpoints are confirmed empirically against the live API before
//! anything is hardcoded (milestone 2); representative responses are
//! captured into `tests/fixtures/`, and all wire → domain parsing lives in
//! a versioned parser layer with quarantine-on-failure.
//!
//! The trait method bodies are milestone-2 stubs.

use std::sync::Arc;

use async_trait::async_trait;
use splicefeed_core::config::Config;
use splicefeed_core::domain::{AudioSource, EpisodeId, EpisodeMeta, ListenKey, ShowMeta, ShowSlug};
use url::Url;

use crate::{Provider, ProviderError, ProviderFactory};

/// Provider for DI.FM premium shows (and, eventually, the other
/// AudioAddict networks: RadioTunes, JazzRadio, RockRadio, ClassicalRadio).
pub struct DifmProvider {
    // Read from milestone 2: appended to resolved audio URLs, sent to the
    // AudioAddict API. Never logged; `ListenKey` redacts itself.
    #[allow(dead_code)]
    listen_key: ListenKey,
}

impl DifmProvider {
    /// Build a provider from a premium listen key.
    pub fn new(listen_key: ListenKey) -> Self {
        Self { listen_key }
    }
}

#[async_trait]
impl Provider for DifmProvider {
    async fn show(&self, _slug: &ShowSlug) -> Result<ShowMeta, ProviderError> {
        todo!("milestone 2: confirm DI.FM/AudioAddict show endpoint empirically")
    }

    async fn episodes(&self, _slug: &ShowSlug) -> Result<Vec<EpisodeMeta>, ProviderError> {
        todo!("milestone 2: confirm episode listing endpoint and pagination empirically")
    }

    async fn resolve_audio(&self, _episode: &EpisodeId) -> Result<AudioSource, ProviderError> {
        todo!("milestone 2: confirm audio asset URL shape (listen_key query param) empirically")
    }

    async fn artwork(&self, _slug: &ShowSlug) -> Result<Option<Url>, ProviderError> {
        todo!("milestone 2: confirm artwork source empirically")
    }
}

/// Factory registering [`DifmProvider`] under the name `difm`.
pub struct DifmFactory;

impl ProviderFactory for DifmFactory {
    fn name(&self) -> &'static str {
        "difm"
    }

    fn create(&self, config: &Config) -> Result<Arc<dyn Provider>, ProviderError> {
        let key = config
            .difm_listen_key()
            .ok_or(ProviderError::MissingCredentials("difm"))?;
        Ok(Arc::new(DifmProvider::new(key.clone())))
    }
}
