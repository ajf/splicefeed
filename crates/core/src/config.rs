//! Configuration: a single TOML file plus environment overrides.
//!
//! Resolution order for the file path: explicit `--config` argument, then
//! `SPLICEFEED_CONFIG`, then `~/.config/splicefeed/config.toml`. The DI.FM
//! listen key may come from the file or `DIFM_LISTEN_KEY`; the env var wins.
//! Loading refuses to succeed if a configured show needs credentials that
//! are absent.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use etcetera::BaseStrategy;
use figment::Figment;
use figment::providers::{Format, Toml};
use serde::Deserialize;
use url::Url;

use crate::domain::{ListenKey, ShowSlug};

/// Name of the env var holding the DI.FM premium listen key.
pub const DIFM_LISTEN_KEY_ENV: &str = "DIFM_LISTEN_KEY";
/// Name of the env var overriding the config file path.
pub const CONFIG_PATH_ENV: &str = "SPLICEFEED_CONFIG";

/// Errors produced while locating, parsing, or validating configuration.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// No config file exists at the resolved path.
    #[error("config file not found at {0} (create it or pass --config / set {CONFIG_PATH_ENV})")]
    NotFound(PathBuf),
    /// The file could not be read or did not match the schema.
    #[error("failed to load config: {0}")]
    Load(#[from] Box<figment::Error>),
    /// A show uses a provider whose required credentials are absent.
    #[error(
        "show `{show}` uses provider `{provider}`, which requires a listen key; \
         set [auth.{provider}] listen_key in the config or the {DIFM_LISTEN_KEY_ENV} env var"
    )]
    MissingListenKey {
        /// The show whose provider needs credentials.
        show: ShowSlug,
        /// The provider name.
        provider: String,
    },
    /// Two `[[shows]]` entries share a slug.
    #[error("duplicate show slug `{0}`")]
    DuplicateShow(ShowSlug),
    /// The platform's standard directories could not be determined.
    #[error("could not determine platform directories: {0}")]
    Dirs(String),
}

/// Top-level daemon configuration. Construct via [`Config::load`].
#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "defaults::bind")]
    bind: SocketAddr,
    #[serde(default)]
    external_base_url: Option<Url>,
    #[serde(default)]
    data_dir: Option<PathBuf>,
    #[serde(default = "defaults::poll_interval", with = "humantime_serde")]
    poll_interval: Duration,
    #[serde(default = "defaults::download_concurrency")]
    download_concurrency: std::num::NonZeroUsize,
    #[serde(default)]
    retention: Retention,
    #[serde(default)]
    auth: Auth,
    #[serde(default)]
    shows: Vec<ShowConfig>,
}

impl Config {
    /// Load, layer, and validate configuration.
    ///
    /// `explicit_path` (from `--config`) beats `SPLICEFEED_CONFIG`, which
    /// beats the platform default. `DIFM_LISTEN_KEY` overrides any key in
    /// the file.
    pub fn load(explicit_path: Option<&Path>) -> Result<Self, ConfigError> {
        let path = match explicit_path {
            Some(p) => p.to_owned(),
            None => match std::env::var_os(CONFIG_PATH_ENV) {
                Some(p) => PathBuf::from(p),
                None => default_config_path()?,
            },
        };
        if !path.is_file() {
            return Err(ConfigError::NotFound(path));
        }

        let mut config: Config = Figment::new()
            .merge(Toml::file(&path))
            .extract()
            .map_err(Box::new)?;

        if let Ok(key) = std::env::var(DIFM_LISTEN_KEY_ENV) {
            config
                .auth
                .difm
                .get_or_insert_with(DifmAuth::default)
                .listen_key = Some(ListenKey::new(key));
        }
        if config.data_dir.is_none() {
            config.data_dir = Some(default_data_dir()?);
        }

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let mut seen = std::collections::HashSet::new();
        for show in &self.shows {
            if !seen.insert(show.slug.clone()) {
                return Err(ConfigError::DuplicateShow(show.slug.clone()));
            }
            if show.provider == "difm" && self.difm_listen_key().is_none() {
                return Err(ConfigError::MissingListenKey {
                    show: show.slug.clone(),
                    provider: show.provider.clone(),
                });
            }
        }
        Ok(())
    }

    /// Address the HTTP server binds to (loopback by default; exposing
    /// wider is an explicit choice).
    pub fn bind(&self) -> SocketAddr {
        self.bind
    }

    /// Base URL used in generated enclosure/artwork URLs. Falls back to
    /// `http://<bind>` when unset.
    pub fn external_base_url(&self) -> Url {
        match &self.external_base_url {
            Some(url) => url.clone(),
            // The default bind address always forms a valid http URL.
            None => Url::parse(&format!("http://{}", self.bind))
                .unwrap_or_else(|_| unreachable!("SocketAddr always forms a valid URL host")),
        }
    }

    /// Directory holding the SQLite state, downloaded audio, artwork cache,
    /// and parse quarantine.
    pub fn data_dir(&self) -> &Path {
        self.data_dir
            .as_deref()
            .unwrap_or_else(|| unreachable!("data_dir is resolved during load()"))
    }

    /// Default poll interval for shows without an override.
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// How many episode downloads may run at once, across all shows.
    pub fn download_concurrency(&self) -> std::num::NonZeroUsize {
        self.download_concurrency
    }

    /// Global retention policy (per-show overrides layer on top).
    pub fn retention(&self) -> &Retention {
        &self.retention
    }

    /// The configured shows.
    pub fn shows(&self) -> &[ShowConfig] {
        &self.shows
    }

    /// The DI.FM premium listen key, if configured.
    pub fn difm_listen_key(&self) -> Option<&ListenKey> {
        self.auth.difm.as_ref()?.listen_key.as_ref()
    }

    /// Override for the AudioAddict API base URL — for sibling networks
    /// (RadioTunes, JazzRadio, …) or tests. `None` means the DI.FM
    /// production API.
    pub fn difm_base_url(&self) -> Option<&Url> {
        self.auth.difm.as_ref()?.base_url.as_ref()
    }
}

/// Retention policy: both limits may apply; pruning satisfies whichever is
/// stricter. Absent fields mean "no limit at this level".
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct Retention {
    #[serde(default)]
    keep_last: Option<u32>,
    #[serde(default)]
    max_gb: Option<f64>,
}

impl Retention {
    /// Keep at most this many episodes per show.
    pub fn keep_last(&self) -> Option<u32> {
        self.keep_last
    }

    /// Keep at most this many bytes per show.
    pub fn max_bytes(&self) -> Option<u64> {
        self.max_gb.map(|gb| (gb * 1e9) as u64)
    }

    /// This policy with any unset field filled from `fallback` — used to
    /// layer a per-show override over the global policy.
    pub fn layered_over(&self, fallback: &Retention) -> Retention {
        Retention {
            keep_last: self.keep_last.or(fallback.keep_last),
            max_gb: self.max_gb.or(fallback.max_gb),
        }
    }
}

/// One `[[shows]]` entry.
#[derive(Debug, Deserialize)]
pub struct ShowConfig {
    #[serde(default = "defaults::provider")]
    provider: String,
    slug: ShowSlug,
    #[serde(default)]
    title: Option<String>,
    #[serde(default, with = "humantime_serde")]
    poll_interval: Option<Duration>,
    #[serde(default)]
    artwork: Option<ArtworkOverride>,
    #[serde(default)]
    retention: Option<Retention>,
}

impl ShowConfig {
    /// Registry name of the provider serving this show (default `difm`).
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// The show's slug.
    pub fn slug(&self) -> &ShowSlug {
        &self.slug
    }

    /// Feed title override, if any (otherwise provider metadata is used).
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Effective poll interval given the global default.
    pub fn poll_interval(&self, global_default: Duration) -> Duration {
        self.poll_interval.unwrap_or(global_default)
    }

    /// Artwork override, if any (beats provider artwork).
    pub fn artwork(&self) -> Option<&ArtworkOverride> {
        self.artwork.as_ref()
    }

    /// Effective retention given the global policy.
    pub fn retention(&self, global: &Retention) -> Retention {
        match &self.retention {
            Some(own) => own.layered_over(global),
            None => *global,
        }
    }
}

/// A show artwork override: a URL for `http(s)://` values, a local file
/// path otherwise.
#[derive(Debug, Clone)]
pub enum ArtworkOverride {
    /// Fetch (and cache) from this URL.
    Url(Url),
    /// Serve this local file.
    Path(PathBuf),
}

impl FromStr for ArtworkOverride {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("http://") || s.starts_with("https://") {
            Ok(Self::Url(s.parse()?))
        } else {
            Ok(Self::Path(PathBuf::from(s)))
        }
    }
}

impl<'de> Deserialize<'de> for ArtworkOverride {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Default, Deserialize)]
struct Auth {
    #[serde(default)]
    difm: Option<DifmAuth>,
}

#[derive(Debug, Default, Deserialize)]
struct DifmAuth {
    #[serde(default)]
    listen_key: Option<ListenKey>,
    #[serde(default)]
    base_url: Option<Url>,
}

mod defaults {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    pub(super) fn bind() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8380)
    }

    pub(super) fn poll_interval() -> Duration {
        Duration::from_secs(30 * 60)
    }

    pub(super) fn provider() -> String {
        "difm".to_owned()
    }

    pub(super) fn download_concurrency() -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(2).unwrap_or_else(|| unreachable!("2 is nonzero"))
    }
}

fn default_config_path() -> Result<PathBuf, ConfigError> {
    let strategy =
        etcetera::choose_base_strategy().map_err(|e| ConfigError::Dirs(e.to_string()))?;
    Ok(strategy.config_dir().join("splicefeed").join("config.toml"))
}

fn default_data_dir() -> Result<PathBuf, ConfigError> {
    let strategy =
        etcetera::choose_base_strategy().map_err(|e| ConfigError::Dirs(e.to_string()))?;
    Ok(strategy.data_dir().join("splicefeed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml: &str) -> Config {
        Figment::new()
            .merge(Toml::string(toml))
            .extract()
            .expect("test config parses")
    }

    #[test]
    fn minimal_config_gets_defaults() {
        let config = parse(
            r#"
            [auth.difm]
            listen_key = "abc123"

            [[shows]]
            slug = "melodik-revolution"
            "#,
        );

        assert_eq!(config.bind().port(), 8380);
        assert!(config.bind().ip().is_loopback());
        assert_eq!(config.poll_interval(), Duration::from_secs(1800));
        assert_eq!(config.shows().len(), 1);
        assert_eq!(config.shows()[0].provider(), "difm");
        assert!(config.difm_listen_key().is_some());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn difm_show_without_key_is_rejected() {
        let config = parse(
            r#"
            [[shows]]
            slug = "anything-melodic"
            "#,
        );
        assert!(matches!(
            config.validate(),
            Err(ConfigError::MissingListenKey { .. })
        ));
    }

    #[test]
    fn duplicate_slugs_are_rejected() {
        let config = parse(
            r#"
            [auth.difm]
            listen_key = "abc"

            [[shows]]
            slug = "x"
            [[shows]]
            slug = "x"
            "#,
        );
        assert!(matches!(
            config.validate(),
            Err(ConfigError::DuplicateShow(_))
        ));
    }

    #[test]
    fn overrides_and_layering() {
        let config = parse(
            r#"
            bind = "0.0.0.0:9000"
            external_base_url = "http://nas.lan:9000"
            poll_interval = "1h"

            [retention]
            keep_last = 50
            max_gb = 20.0

            [auth.difm]
            listen_key = "abc"

            [[shows]]
            slug = "melodik-revolution"
            poll_interval = "15m"
            artwork = "https://example.com/art.jpg"
            [shows.retention]
            keep_last = 5
            "#,
        );

        assert_eq!(config.external_base_url().as_str(), "http://nas.lan:9000/");
        let show = &config.shows()[0];
        assert_eq!(
            show.poll_interval(config.poll_interval()),
            Duration::from_secs(900)
        );
        assert!(matches!(show.artwork(), Some(ArtworkOverride::Url(_))));
        let effective = show.retention(config.retention());
        assert_eq!(effective.keep_last(), Some(5));
        assert_eq!(effective.max_bytes(), Some(20_000_000_000));
    }

    #[test]
    fn default_external_base_url_derives_from_bind() {
        let config = parse(
            r#"
            [auth.difm]
            listen_key = "abc"
            "#,
        );
        assert_eq!(
            config.external_base_url().as_str(),
            "http://127.0.0.1:8380/"
        );
    }
}
