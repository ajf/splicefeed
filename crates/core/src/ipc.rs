//! Versioned IPC protocol between the daemon's unix control socket and the
//! `splicefeed status` TUI (and future control clients).
//!
//! Framing is newline-delimited JSON — debuggable with `socat`/`jq`. Both
//! request and event enums are `#[non_exhaustive]` so verbs like
//! `poll-now <show>` can be added without breaking older clients, and the
//! event stream carries an untagged [`Event::Unknown`] tail so a newer
//! daemon never breaks an older TUI (`#[serde(other)]` only works on unit
//! variants, hence the wrapper).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::domain::{EpisodeId, ErrorClass, ShowSlug};

/// Protocol version, exchanged in [`Hello`] so mismatches fail clearly.
pub const PROTOCOL_VERSION: u32 = 1;

/// First message sent by the daemon on every new connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    /// The daemon's [`PROTOCOL_VERSION`].
    pub protocol_version: u32,
    /// Daemon version string (`CARGO_PKG_VERSION`).
    pub daemon_version: String,
}

/// A client request. One JSON object per line.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Request {
    /// Ask for a one-shot [`Snapshot`] of daemon state.
    Snapshot,
    /// Ask for a snapshot followed by a live [`Event`] stream.
    Subscribe,
}

/// A daemon response to a [`Request`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "response", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Response {
    /// Current daemon state.
    Snapshot(Snapshot),
    /// The request could not be served.
    Error {
        /// Human-readable reason.
        message: String,
    },
}

/// Point-in-time view of daemon state, rendered by the TUI's tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Seconds since the daemon started.
    pub uptime_secs: u64,
    /// Per-show status, in config order.
    pub shows: Vec<ShowStatus>,
    /// In-flight downloads.
    pub downloads: Vec<DownloadStatus>,
    /// Total bytes used in the data directory.
    pub data_dir_bytes: u64,
    /// HTTP requests served since start.
    pub http_requests: u64,
}

/// Status of one followed show.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShowStatus {
    /// The show.
    pub slug: ShowSlug,
    /// Provider registry name.
    pub provider: String,
    /// Unix seconds of the last poll attempt, if any.
    pub last_poll_at: Option<i64>,
    /// Whether the last poll succeeded.
    pub last_poll_ok: Option<bool>,
    /// Unix seconds of the next scheduled poll.
    pub next_poll_at: Option<i64>,
    /// Episodes currently cached on disk.
    pub episodes_cached: u64,
    /// Bytes on disk for this show.
    pub cache_bytes: u64,
    /// Most recent error, if the show is unhealthy.
    pub last_error: Option<String>,
}

/// Progress of one in-flight download.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadStatus {
    /// The show the episode belongs to.
    pub show: ShowSlug,
    /// The episode being fetched.
    pub episode: EpisodeId,
    /// Bytes written so far.
    pub bytes_done: u64,
    /// Total bytes, when upstream provided a length.
    pub bytes_total: Option<u64>,
    /// Recent throughput in bytes/second.
    pub bytes_per_sec: u64,
}

/// One event on a subscription stream.
///
/// Deserialization tries [`KnownEvent`] first and falls back to
/// [`Event::Unknown`], so clients skip event kinds they don't understand
/// instead of erroring out.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event {
    /// An event this client's protocol version understands.
    Known(KnownEvent),
    /// An event from a newer daemon; ignore, never fail.
    Unknown(serde_json::Value),
}

/// Events the daemon emits while running.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
#[non_exhaustive]
pub enum KnownEvent {
    /// A poll of a show started.
    PollStarted {
        /// The show being polled.
        show: ShowSlug,
    },
    /// A poll finished.
    PollFinished {
        /// The show that was polled.
        show: ShowSlug,
        /// Whether the poll succeeded.
        ok: bool,
        /// Episodes newly discovered by this poll.
        new_episodes: u32,
    },
    /// A new episode was recorded.
    EpisodeDiscovered {
        /// The show it belongs to.
        show: ShowSlug,
        /// The new episode.
        episode: EpisodeId,
    },
    /// A download finished (successfully or not).
    DownloadFinished {
        /// The show it belongs to.
        show: ShowSlug,
        /// The episode.
        episode: EpisodeId,
        /// Failure classification; `None` means success.
        error: Option<ErrorClass>,
    },
    /// Retention pruned episodes from a show.
    Pruned {
        /// The show that was pruned.
        show: ShowSlug,
        /// Episodes removed.
        episodes: u32,
        /// Bytes freed.
        bytes_freed: u64,
    },
    /// A provider response failed to parse and was quarantined.
    Quarantined {
        /// Provider registry name.
        provider: String,
        /// Path of the quarantined payload, when the write succeeded.
        path: Option<PathBuf>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_events_do_not_fail() {
        let line = r#"{"event":"warp_core_breach","dilithium":42}"#;
        let event: Event = serde_json::from_str(line).expect("unknown event still parses");
        assert!(matches!(event, Event::Unknown(_)));
    }

    #[test]
    fn known_events_round_trip() {
        let event = Event::Known(KnownEvent::PollFinished {
            show: "melodik-revolution".parse().expect("valid slug"),
            ok: true,
            new_episodes: 2,
        });
        let json = serde_json::to_string(&event).expect("serializes");
        let back: Event = serde_json::from_str(&json).expect("parses");
        assert!(matches!(
            back,
            Event::Known(KnownEvent::PollFinished {
                new_episodes: 2,
                ..
            })
        ));
    }
}
