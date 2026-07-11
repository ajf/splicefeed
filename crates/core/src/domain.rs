//! Domain types: identifiers (newtypes, never bare `String`s), the episode
//! lifecycle state machine, and provider-boundary metadata types.

use std::fmt;
use std::str::FromStr;

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use url::Url;

/// Error returned when a string fails validation as a domain identifier.
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid {kind}: {reason}")]
pub struct InvalidId {
    kind: &'static str,
    reason: &'static str,
}

/// Identifier of a show as used by a provider, e.g. `melodik-revolution`.
///
/// Slugs are non-empty and restricted to `[A-Za-z0-9._-]` so they are safe
/// to embed in URLs and file names without escaping.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ShowSlug(String);

impl ShowSlug {
    /// The slug as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ShowSlug {
    type Err = InvalidId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidId {
                kind: "show slug",
                reason: "must not be empty",
            });
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        {
            return Err(InvalidId {
                kind: "show slug",
                reason: "only ASCII letters, digits, `-`, `_`, and `.` are allowed",
            });
        }
        Ok(Self(s.to_owned()))
    }
}

impl TryFrom<String> for ShowSlug {
    type Error = InvalidId;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<ShowSlug> for String {
    fn from(slug: ShowSlug) -> Self {
        slug.0
    }
}

impl fmt::Display for ShowSlug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ShowSlug {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Provider-assigned identifier of an episode.
///
/// Opaque: feeds derive their GUIDs from this value, never from file paths.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EpisodeId(String);

impl EpisodeId {
    /// The identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for EpisodeId {
    type Err = InvalidId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidId {
                kind: "episode id",
                reason: "must not be empty",
            });
        }
        Ok(Self(s.to_owned()))
    }
}

impl TryFrom<String> for EpisodeId {
    type Error = InvalidId;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<EpisodeId> for String {
    fn from(id: EpisodeId) -> Self {
        id.0
    }
}

impl fmt::Display for EpisodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A DI.FM/AudioAddict premium listen key.
///
/// Wraps [`SecretString`]: the key never appears in `Debug` output, has no
/// `Display` impl, and is only readable through [`ListenKey::expose`] at the
/// point a request URL is built. URLs containing it must go through a
/// redaction helper before logging.
#[derive(Clone)]
pub struct ListenKey(SecretString);

impl ListenKey {
    /// Wrap a raw key.
    pub fn new(key: String) -> Self {
        Self(key.into())
    }

    /// Reveal the key. Call only where the value is actually sent upstream.
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for ListenKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ListenKey(<redacted>)")
    }
}

impl<'de> Deserialize<'de> for ListenKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::new)
    }
}

/// An AudioAddict member API key.
///
/// A distinct credential from the [`ListenKey`]: the listen key authorizes
/// premium *stream hosts*, while the member API key authorizes the API
/// itself (confirmed empirically 2026-07-11 — episode audio assets do not
/// appear for any placement of the listen key). Same secrecy rules as
/// [`ListenKey`].
#[derive(Clone)]
pub struct ApiKey(SecretString);

impl ApiKey {
    /// Wrap a raw key.
    pub fn new(key: String) -> Self {
        Self(key.into())
    }

    /// Reveal the key. Call only where the value is actually sent upstream.
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
}

impl<'de> Deserialize<'de> for ApiKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::new)
    }
}

/// Classification of a failure, shared between episode state, logs, and the
/// `download errors` metric labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorClass {
    /// Connection, DNS, or timeout failures.
    Network,
    /// Upstream responded with a non-success status.
    HttpStatus,
    /// Response received but could not be parsed (quarantined).
    Parse,
    /// Local filesystem failures.
    Disk,
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Network => "network",
            Self::HttpStatus => "http-status",
            Self::Parse => "parse",
            Self::Disk => "disk",
        })
    }
}

impl FromStr for ErrorClass {
    type Err = InvalidId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "network" => Ok(Self::Network),
            "http-status" => Ok(Self::HttpStatus),
            "parse" => Ok(Self::Parse),
            "disk" => Ok(Self::Disk),
            _ => Err(InvalidId {
                kind: "error class",
                reason: "not one of network/http-status/parse/disk",
            }),
        }
    }
}

/// Lifecycle of an episode: `Discovered → Downloading → Cached → Pruned`,
/// with `Failed` as a retryable detour out of `Downloading`,
/// `Cached → Downloading` for re-downloading a file that failed
/// verification (`splicefeed verify --fix`), and `Pruned → Downloading`
/// for reviving a tombstone that fits a widened retention window.
///
/// Pruned rows stay in storage so a pruned episode is never re-discovered
/// as "new" — discovery leaves tombstones alone; only the sync engine's
/// retention planner revives them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EpisodeState {
    /// Known from a provider listing; no local file yet.
    Discovered,
    /// A download is in flight.
    Downloading,
    /// Audio is on disk, verified, and served in the feed.
    Cached,
    /// Removed by retention; kept in storage as a tombstone.
    Pruned,
    /// The last download attempt failed; eligible for retry.
    Failed(ErrorClass),
}

impl EpisodeState {
    /// Whether the transition `self → next` is legal.
    pub fn can_transition_to(self, next: Self) -> bool {
        match (self, next) {
            (Self::Discovered, Self::Downloading) => true,
            (Self::Downloading, Self::Cached | Self::Failed(_)) => true,
            (Self::Failed(_), Self::Downloading) => true,
            // Downloading out of Cached = re-fetch after a failed
            // file verification.
            (Self::Cached, Self::Downloading | Self::Pruned) => true,
            // Downloading out of Pruned = revival: the retention window
            // widened and the tombstone fits it again.
            (Self::Pruned, Self::Downloading) => true,
            (
                Self::Discovered
                | Self::Downloading
                | Self::Cached
                | Self::Pruned
                | Self::Failed(_),
                _,
            ) => false,
        }
    }

    /// Whether this state has a playable file on disk.
    pub fn is_on_disk(self) -> bool {
        match self {
            Self::Cached => true,
            Self::Discovered | Self::Downloading | Self::Pruned | Self::Failed(_) => false,
        }
    }
}

/// How the daemon was asked to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Poll every show once, then exit (`--once`; cron-style operation).
    Once,
    /// Long-running daemon: scheduler plus HTTP and IPC servers.
    Serve,
}

/// Show-level metadata returned by a provider.
#[derive(Debug, Clone)]
pub struct ShowMeta {
    /// The show's slug.
    pub slug: ShowSlug,
    /// Human-readable title.
    pub title: String,
    /// Long-form description, if the provider supplies one.
    pub description: Option<String>,
    /// Artwork location, if the provider supplies one.
    pub artwork: Option<Url>,
}

/// Episode-level metadata returned by a provider listing.
#[derive(Debug, Clone)]
pub struct EpisodeMeta {
    /// Provider-assigned identifier; the feed GUID derives from this.
    pub id: EpisodeId,
    /// Episode title.
    pub title: String,
    /// Episode description/notes, if any.
    pub description: Option<String>,
    /// Publication time, if the provider supplies one.
    pub published_at: Option<jiff::Timestamp>,
    /// Duration in seconds, if the provider supplies one (otherwise probed
    /// from the downloaded file with `lofty`).
    pub duration_secs: Option<u32>,
}

/// A resolved, downloadable audio asset for one episode.
///
/// The URL typically embeds the listen key — treat as sensitive and redact
/// before logging.
#[derive(Debug, Clone)]
pub struct AudioSource {
    /// Fully resolved URL, credentials included.
    pub url: Url,
    /// MIME type, if known ahead of the download.
    pub mime: Option<AudioMime>,
    /// Size in bytes, if known ahead of the download.
    pub bytes: Option<u64>,
}

/// Query parameters that carry credentials and must never reach a log.
const SECRET_PARAMS: [&str; 3] = ["listen_key", "api_key", "audio_token"];

/// A URL rendered safe for logs and error messages: the value of any
/// credential query parameter (`listen_key`, `api_key`, `audio_token`) is
/// replaced with `REDACTED`.
///
/// The type makes "already sanitized" part of a signature instead of a
/// comment: anything that logs or stores a displayable URL carries this,
/// never a bare `String`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactedUrl(Url);

impl From<&Url> for RedactedUrl {
    fn from(url: &Url) -> Self {
        if !url
            .query_pairs()
            .any(|(k, _)| SECRET_PARAMS.contains(&k.as_ref()))
        {
            return Self(url.clone());
        }
        let mut clean = url.clone();
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| {
                let v = if SECRET_PARAMS.contains(&k.as_ref()) {
                    "REDACTED".into()
                } else {
                    v
                };
                (k.into_owned(), v.into_owned())
            })
            .collect();
        clean
            .query_pairs_mut()
            .clear()
            .extend_pairs(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        Self(clean)
    }
}

impl fmt::Display for RedactedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// [`RedactedUrl`] as a free function, for call sites that read better
/// with a verb.
pub fn redacted(url: &Url) -> RedactedUrl {
    RedactedUrl::from(url)
}

/// MIME type of an episode's audio.
///
/// The formats a podcast feed realistically carries are a known set —
/// which buys exhaustive matching and one authoritative mime→extension
/// mapping — while [`AudioMime::Other`] keeps upstream's open set from
/// ever becoming a parse failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AudioMime {
    /// `audio/mpeg` (MP3).
    Mpeg,
    /// `audio/mp4` (M4A/AAC in MP4).
    Mp4,
    /// `audio/aac` (raw AAC).
    Aac,
    /// `audio/ogg` (Vorbis/Opus).
    Ogg,
    /// Anything upstream sends that we don't recognize; passed through
    /// verbatim to feed enclosures.
    Other(String),
}

impl AudioMime {
    /// Canonical file extension, when this is a format we know.
    pub fn extension(&self) -> Option<&'static str> {
        match self {
            Self::Mpeg => Some("mp3"),
            Self::Mp4 => Some("m4a"),
            Self::Aac => Some("aac"),
            Self::Ogg => Some("ogg"),
            Self::Other(_) => None,
        }
    }
}

impl From<&str> for AudioMime {
    fn from(raw: &str) -> Self {
        match raw {
            "audio/mpeg" | "audio/mp3" => Self::Mpeg,
            "audio/mp4" | "audio/x-m4a" => Self::Mp4,
            "audio/aac" => Self::Aac,
            "audio/ogg" => Self::Ogg,
            other => Self::Other(other.to_owned()),
        }
    }
}

impl fmt::Display for AudioMime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Mpeg => "audio/mpeg",
            Self::Mp4 => "audio/mp4",
            Self::Aac => "audio/aac",
            Self::Ogg => "audio/ogg",
            Self::Other(raw) => raw,
        })
    }
}

impl Serialize for AudioMime {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_rejects_junk() {
        assert!("melodik-revolution".parse::<ShowSlug>().is_ok());
        assert!("a/b".parse::<ShowSlug>().is_err());
        assert!("".parse::<ShowSlug>().is_err());
        assert!("with space".parse::<ShowSlug>().is_err());
    }

    #[test]
    fn listen_key_debug_is_redacted() {
        let key = ListenKey::new("super-secret".into());
        assert_eq!(format!("{key:?}"), "ListenKey(<redacted>)");
    }

    #[test]
    fn redaction_hides_listen_key() {
        let url: Url = "https://prem2.di.fm/shows/x/ep.mp4?foo=1&listen_key=sekrit"
            .parse()
            .expect("valid url");
        let shown = redacted(&url).to_string();
        assert!(!shown.contains("sekrit"));
        assert!(shown.contains("listen_key=REDACTED"));
        assert!(shown.contains("foo=1"));
    }

    #[test]
    fn redaction_hides_api_key_too() {
        let url: Url = "https://api.audioaddict.com/v1/di/shows/x/episodes/1?api_key=sekrit"
            .parse()
            .expect("valid url");
        let shown = redacted(&url).to_string();
        assert!(!shown.contains("sekrit"));
        assert!(shown.contains("api_key=REDACTED"));
    }

    #[test]
    fn redaction_leaves_clean_urls_alone() {
        let url: Url = "https://api.audioaddict.com/v1/di/shows/x"
            .parse()
            .expect("valid url");
        assert_eq!(redacted(&url).to_string(), url.to_string());
    }

    #[test]
    fn audio_mime_roundtrip_and_extensions() {
        assert_eq!(AudioMime::from("audio/mpeg"), AudioMime::Mpeg);
        assert_eq!(AudioMime::from("audio/x-m4a"), AudioMime::Mp4);
        assert_eq!(AudioMime::Mp4.extension(), Some("m4a"));
        assert_eq!(AudioMime::Mp4.to_string(), "audio/mp4");
        let odd = AudioMime::from("audio/flac");
        assert_eq!(odd, AudioMime::Other("audio/flac".into()));
        assert_eq!(odd.extension(), None);
        assert_eq!(odd.to_string(), "audio/flac");
    }

    #[test]
    fn lifecycle_transitions() {
        use EpisodeState::*;
        assert!(Discovered.can_transition_to(Downloading));
        assert!(Downloading.can_transition_to(Cached));
        assert!(Downloading.can_transition_to(Failed(ErrorClass::Network)));
        assert!(Failed(ErrorClass::Disk).can_transition_to(Downloading));
        assert!(Cached.can_transition_to(Pruned));
        assert!(Cached.can_transition_to(Downloading)); // verify --fix
        assert!(Pruned.can_transition_to(Downloading)); // retention revival
        assert!(!Pruned.can_transition_to(Cached));
        assert!(!Discovered.can_transition_to(Cached));
    }
}
