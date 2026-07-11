//! DI.FM / AudioAddict provider.
//!
//! Endpoints confirmed empirically 2026-07-11 (see DESIGN.md and
//! `tests/fixtures/audioaddict/`):
//!
//! - `GET {base}/shows/<slug>` — show metadata (no auth required)
//! - `GET {base}/shows/<slug>/episodes?page=N&per_page=M` — episode
//!   listing, newest first; RFC 5988 `Link` headers carry pagination
//! - `GET {base}/shows/<slug>/episodes/<episode-slug>` — single episode
//!
//! with `base = https://api.audioaddict.com/v1/di/`. AudioAddict is the
//! shared platform behind DI.FM, RadioTunes, JazzRadio, RockRadio, and
//! ClassicalRadio, so `di` in the base path is the network name.
//!
//! Unauthenticated, `tracks[].content` is empty and `tracks[].asset_url`
//! points at artwork. The authenticated audio-asset shape is UNCONFIRMED:
//! [`DifmProvider::resolve_audio`] sends the listen key as a `listen_key`
//! query parameter (the historically known mechanism) and fails loudly
//! with a hint when no audio asset appears. `splicefeed probe` is the way
//! to verify against the live API.

mod v1;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use splicefeed_core::config::Config;
use splicefeed_core::domain::{AudioSource, EpisodeId, EpisodeMeta, ListenKey, ShowMeta, ShowSlug};
use url::Url;

use crate::quarantine::Quarantine;
use crate::{Provider, ProviderError, ProviderFactory, redacted};

/// Production API base for the DI.FM network.
pub const DEFAULT_BASE_URL: &str = "https://api.audioaddict.com/v1/di/";

/// Episodes fetched per listing call (first page only for now; retention
/// defaults keep fewer than upstream's 25-per-show on-demand window).
const DEFAULT_PER_PAGE: u32 = 25;

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Provider for DI.FM premium shows via the AudioAddict API.
pub struct DifmProvider {
    http: reqwest::Client,
    base_url: Url,
    listen_key: ListenKey,
    quarantine: Quarantine,
    per_page: u32,
}

/// Builder for [`DifmProvider`].
pub struct DifmProviderBuilder {
    listen_key: ListenKey,
    base_url: Option<Url>,
    quarantine_dir: Option<PathBuf>,
    per_page: Option<u32>,
}

impl DifmProviderBuilder {
    /// Override the API base URL (tests point this at a mock server).
    pub fn base_url(mut self, base_url: Url) -> Self {
        self.base_url = Some(base_url);
        self
    }

    /// Where unparseable payloads are written (default: alongside the
    /// current directory under `quarantine/difm`).
    pub fn quarantine_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.quarantine_dir = Some(dir.into());
        self
    }

    /// Episodes requested per listing call.
    pub fn per_page(mut self, per_page: u32) -> Self {
        self.per_page = Some(per_page);
        self
    }

    /// Build the provider.
    pub fn build(self) -> Result<DifmProvider, ProviderError> {
        let base_url = match self.base_url {
            Some(url) => url,
            None => DEFAULT_BASE_URL
                .parse()
                .unwrap_or_else(|_| unreachable!("default base URL is valid")),
        };
        let http = reqwest::Client::builder()
            .user_agent(concat!("splicefeed/", env!("CARGO_PKG_VERSION")))
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| ProviderError::Http(e.to_string()))?;
        Ok(DifmProvider {
            http,
            base_url,
            listen_key: self.listen_key,
            quarantine: Quarantine::new(
                self.quarantine_dir
                    .unwrap_or_else(|| PathBuf::from("quarantine").join("difm")),
            ),
            per_page: self.per_page.unwrap_or(DEFAULT_PER_PAGE),
        })
    }
}

impl DifmProvider {
    /// Start building a provider from the premium listen key.
    pub fn builder(listen_key: ListenKey) -> DifmProviderBuilder {
        DifmProviderBuilder {
            listen_key,
            base_url: None,
            quarantine_dir: None,
            per_page: None,
        }
    }

    fn endpoint(&self, segments: &[&str], query: &[(&str, &str)]) -> Result<Url, ProviderError> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|()| ProviderError::Http("base URL cannot hold a path".into()))?
            .pop_if_empty()
            .extend(segments);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        Ok(url)
    }

    /// Fetch a URL, mapping transport and status failures. Never lets the
    /// listen key into an error message.
    async fn get_text(&self, url: Url) -> Result<String, ProviderError> {
        let shown = redacted(&url);
        tracing::debug!(url = %shown, "difm: GET");
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| ProviderError::Http(e.without_url().to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ProviderError::Status {
                status: status.as_u16(),
                url: shown,
            });
        }
        response
            .text()
            .await
            .map_err(|e| ProviderError::Http(e.without_url().to_string()))
    }

    /// Parse a payload, quarantining it on failure.
    fn parse<T: serde::de::DeserializeOwned>(
        &self,
        payload: &str,
        label: &str,
    ) -> Result<T, ProviderError> {
        serde_json::from_str(payload).map_err(|e| ProviderError::Parse {
            reason: e.to_string(),
            quarantine_path: self.quarantine.write_or_note(label, payload),
        })
    }

    fn not_found_means_no_show(err: ProviderError, slug: &ShowSlug) -> ProviderError {
        match err {
            ProviderError::Status { status: 404, .. } => ProviderError::ShowNotFound(slug.clone()),
            other => other,
        }
    }
}

#[async_trait]
impl Provider for DifmProvider {
    async fn show(&self, slug: &ShowSlug) -> Result<ShowMeta, ProviderError> {
        let url = self.endpoint(&["shows", slug.as_str()], &[])?;
        let payload = self
            .get_text(url)
            .await
            .map_err(|e| Self::not_found_means_no_show(e, slug))?;
        let show: v1::Show = self.parse(&payload, &format!("show-{slug}"))?;
        Ok(show.into_meta(slug))
    }

    async fn episodes(&self, slug: &ShowSlug) -> Result<Vec<EpisodeMeta>, ProviderError> {
        let per_page = self.per_page.to_string();
        let url = self.endpoint(
            &["shows", slug.as_str(), "episodes"],
            &[("page", "1"), ("per_page", per_page.as_str())],
        )?;
        let payload = self
            .get_text(url)
            .await
            .map_err(|e| Self::not_found_means_no_show(e, slug))?;

        // Parse the array shell first, then each entry on its own, so one
        // drifted episode quarantines that entry instead of the whole poll.
        let entries: Vec<serde_json::Value> = self.parse(&payload, &format!("episodes-{slug}"))?;
        let mut episodes = Vec::with_capacity(entries.len());
        for entry in entries {
            let raw = entry.to_string();
            let parsed = serde_json::from_value::<v1::Episode>(entry)
                .map_err(|e| e.to_string())
                .and_then(|wire| EpisodeMeta::try_from(wire).map_err(|e| e.to_string()));
            match parsed {
                Ok(meta) => episodes.push(meta),
                Err(reason) => {
                    let path = self
                        .quarantine
                        .write_or_note(&format!("episode-entry-{slug}"), &raw);
                    tracing::warn!(show = %slug, %reason, quarantined = %path,
                        "difm: skipping unparseable episode entry");
                }
            }
        }
        episodes.sort_by_key(|e| std::cmp::Reverse(e.published_at));
        Ok(episodes)
    }

    async fn resolve_audio(
        &self,
        show: &ShowSlug,
        episode: &EpisodeId,
    ) -> Result<AudioSource, ProviderError> {
        // UNCONFIRMED auth mechanism: listen_key as a query parameter on
        // the single-episode endpoint. `splicefeed probe` verifies this
        // against the live API; failures land in NoAudioAsset with a hint.
        let url = self.endpoint(
            &["shows", show.as_str(), "episodes", episode.as_str()],
            &[("listen_key", self.listen_key.expose())],
        )?;
        let payload = self
            .get_text(url)
            .await
            .map_err(|e| Self::not_found_means_no_show(e, show))?;
        let wire: v1::Episode = self.parse(&payload, &format!("episode-{show}-{episode}"))?;

        let Some(mut audio_url) = wire.audio_url() else {
            return Err(ProviderError::NoAudioAsset {
                episode: episode.clone(),
                hint: "the response held no audio asset; either the listen key is not being \
                       accepted (auth mechanism unconfirmed — run `splicefeed probe`) or the \
                       upstream schema drifted"
                    .into(),
            });
        };
        if !audio_url.query_pairs().any(|(k, _)| k == "listen_key") {
            audio_url
                .query_pairs_mut()
                .append_pair("listen_key", self.listen_key.expose());
        }
        let mime = v1::mime_for(&audio_url).map(str::to_owned);
        Ok(AudioSource {
            url: audio_url,
            mime,
            bytes: None,
        })
    }

    async fn artwork(&self, slug: &ShowSlug) -> Result<Option<Url>, ProviderError> {
        Ok(self.show(slug).await?.artwork)
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
        let provider = DifmProvider::builder(key.clone())
            .quarantine_dir(config.data_dir().join("quarantine").join("difm"))
            .build()?;
        Ok(Arc::new(provider))
    }
}
