//! Versioned parser layer for the AudioAddict API, as observed 2026-07-11
//! against `https://api.audioaddict.com/v1/di/` (fixtures in
//! `tests/fixtures/audioaddict/`).
//!
//! The API is undocumented; these types are deliberately maximally
//! tolerant: unknown fields are ignored, everything non-essential is
//! `Option`, and wire → domain conversion is the only place that decides
//! what is actually required. When conversion fails the caller quarantines
//! the raw payload — a schema change must never take down the feeds.

use serde::Deserialize;
use splicefeed_core::domain::{EpisodeMeta, ShowMeta, ShowSlug};
use url::Url;

/// A show, from `GET shows/<slug>`.
#[derive(Debug, Deserialize)]
pub struct Show {
    /// Upstream slug (echoed back; the requested slug is authoritative).
    pub slug: Option<String>,
    /// Display name.
    pub name: Option<String>,
    /// Long-form description.
    pub description: Option<String>,
    /// Artwork variants; values are protocol-relative URI templates.
    #[serde(default)]
    pub images: Images,
}

/// Artwork variants on shows and episodes. Other variants (`compact`,
/// `vertical`, …) exist upstream; declare them if and when needed.
#[derive(Debug, Default, Deserialize)]
pub struct Images {
    /// The default/horizontal image — what feeds use.
    pub default: Option<String>,
}

/// An episode, from `GET shows/<slug>/episodes` (array) or
/// `GET shows/<slug>/episodes/<episode-slug>` (single).
#[derive(Debug, Deserialize)]
pub struct Episode {
    /// Episode slug, unique within the show — our `EpisodeId`.
    pub slug: Option<String>,
    /// Short name (often just the episode number).
    pub name: Option<String>,
    /// Publication time, RFC 3339 with offset.
    pub start_at: Option<String>,
    /// Long-form description (single-episode responses only).
    pub description: Option<String>,
    /// Audio tracks; observed as exactly one per episode.
    #[serde(default)]
    pub tracks: Vec<Track>,
}

/// One audio track of an episode.
#[derive(Debug, Deserialize)]
pub struct Track {
    /// Duration in seconds.
    pub length: Option<i64>,
    /// Human title, e.g. `Melodik Revolution 162` — best feed title.
    pub display_title: Option<String>,
    /// Playable content. Observed empty (`{}`) unauthenticated; the
    /// authenticated shape is still UNCONFIRMED (verify via `probe` with a
    /// listen key before trusting).
    pub content: Option<Content>,
    /// Unauthenticated this held an *image* URL, not audio — do not treat
    /// as an audio source without an audio-looking extension.
    pub asset_url: Option<String>,
}

/// Playable content attached to a track. Shape UNCONFIRMED until probed
/// with premium auth; kept maximally tolerant.
#[derive(Debug, Default, Deserialize)]
pub struct Content {
    /// Candidate audio assets.
    pub assets: Option<Vec<Asset>>,
    /// Sometimes APIs inline a single URL instead of an asset list.
    pub url: Option<String>,
}

/// One candidate audio asset.
#[derive(Debug, Deserialize)]
pub struct Asset {
    /// Asset URL (typically protocol-relative, premium content host).
    pub url: Option<String>,
}

/// Why a wire value could not become a domain value.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WireError {
    /// A field the domain cannot do without was absent or empty.
    #[error("missing required field `{0}`")]
    Missing(&'static str),
    /// A field was present but unusable.
    #[error("invalid field `{field}`: {reason}")]
    Invalid {
        /// The offending field.
        field: &'static str,
        /// What was wrong with it.
        reason: String,
    },
}

impl Show {
    /// Convert to domain metadata. `requested` supplies the slug when the
    /// wire object omits or mangles its own.
    pub fn into_meta(self, requested: &ShowSlug) -> ShowMeta {
        let artwork = self.images.default.as_deref().and_then(normalize_image_url);
        ShowMeta {
            slug: requested.clone(),
            title: self
                .name
                .unwrap_or_else(|| self.slug.unwrap_or_else(|| requested.to_string())),
            description: self.description.filter(|d| !d.is_empty()),
            artwork,
        }
    }
}

impl TryFrom<Episode> for EpisodeMeta {
    type Error = WireError;

    fn try_from(episode: Episode) -> Result<Self, Self::Error> {
        let slug = episode
            .slug
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or(WireError::Missing("slug"))?;
        let id = slug.parse().map_err(|e| WireError::Invalid {
            field: "slug",
            reason: format!("{e}"),
        })?;

        let published_at = match &episode.start_at {
            Some(raw) => Some(
                raw.parse::<jiff::Timestamp>()
                    .map_err(|e| WireError::Invalid {
                        field: "start_at",
                        reason: format!("{e}"),
                    })?,
            ),
            None => None,
        };

        let track = episode.tracks.first();
        let title = track
            .and_then(|t| t.display_title.clone())
            .or(episode.name)
            .unwrap_or_else(|| slug.to_owned());
        let duration_secs = track
            .and_then(|t| t.length)
            .and_then(|len| u32::try_from(len).ok())
            .filter(|len| *len > 0);

        Ok(EpisodeMeta {
            id,
            title,
            description: episode.description.filter(|d| !d.is_empty()),
            published_at,
            duration_secs,
        })
    }
}

impl Episode {
    /// The first plausible audio URL in this episode's tracks, normalized
    /// to absolute. Checks `content.assets[].url` and `content.url`;
    /// `asset_url` is only trusted when it has an audio-looking extension
    /// (unauthenticated it points at artwork).
    pub fn audio_url(&self) -> Option<Url> {
        for track in &self.tracks {
            if let Some(content) = &track.content {
                let from_assets = content
                    .assets
                    .iter()
                    .flatten()
                    .find_map(|asset| asset.url.as_deref());
                if let Some(url) = from_assets.or(content.url.as_deref())
                    && let Some(url) = normalize_url(url)
                {
                    return Some(url);
                }
            }
            if let Some(raw) = track.asset_url.as_deref()
                && has_audio_extension(raw)
                && let Some(url) = normalize_url(raw)
            {
                return Some(url);
            }
        }
        None
    }
}

/// MIME type for an audio URL, judged by extension.
pub fn mime_for(url: &Url) -> Option<&'static str> {
    match url.path().rsplit('.').next()? {
        "mp3" => Some("audio/mpeg"),
        "mp4" | "m4a" => Some("audio/mp4"),
        "aac" => Some("audio/aac"),
        "ogg" | "oga" => Some("audio/ogg"),
        _ => None,
    }
}

fn has_audio_extension(raw: &str) -> bool {
    let path = raw.split(['?', '#']).next().unwrap_or(raw);
    matches!(
        path.rsplit('.').next(),
        Some("mp3" | "mp4" | "m4a" | "aac" | "ogg" | "oga")
    )
}

/// Image URLs arrive protocol-relative with an RFC 6570 template suffix:
/// `//cdn-images.audioaddict.com/…/x.png{?size,height,width,quality,pad}`.
/// Strip the template, force https.
fn normalize_image_url(raw: &str) -> Option<Url> {
    normalize_url(raw.split('{').next()?)
}

fn normalize_url(raw: &str) -> Option<Url> {
    let absolute = if let Some(rest) = raw.strip_prefix("//") {
        format!("https://{rest}")
    } else {
        raw.to_owned()
    };
    absolute.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_urls_are_normalized() {
        let url = normalize_image_url(
            "//cdn-images.audioaddict.com/4/5/8/8.png{?size,height,width,quality,pad}",
        )
        .expect("normalizes");
        assert_eq!(
            url.as_str(),
            "https://cdn-images.audioaddict.com/4/5/8/8.png"
        );
    }

    #[test]
    fn artwork_asset_url_is_not_mistaken_for_audio() {
        let episode = Episode {
            slug: Some("162".into()),
            name: None,
            start_at: None,
            description: None,
            tracks: vec![Track {
                length: Some(7200),
                display_title: None,
                content: Some(Content::default()),
                asset_url: Some("//cdn-images.audioaddict.com/f/a/a.png".into()),
            }],
        };
        assert!(episode.audio_url().is_none());
    }

    #[test]
    fn asset_urls_are_found_and_normalized() {
        let episode = Episode {
            slug: Some("162".into()),
            name: None,
            start_at: None,
            description: None,
            tracks: vec![Track {
                length: None,
                display_title: None,
                content: Some(Content {
                    assets: Some(vec![Asset {
                        url: Some("//prem2.di.fm/shows/x/ep162.mp4".into()),
                    }]),
                    url: None,
                }),
                asset_url: None,
            }],
        };
        let url = episode.audio_url().expect("finds asset");
        assert_eq!(url.as_str(), "https://prem2.di.fm/shows/x/ep162.mp4");
        assert_eq!(mime_for(&url), Some("audio/mp4"));
    }
}
