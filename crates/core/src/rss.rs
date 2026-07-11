//! Deterministic podcast RSS generation.
//!
//! Hand-written via the `quick-xml` writer — one of the design's two
//! sanctioned hand-rolls: the spec demands byte-identical regeneration
//! when nothing changed, and owning every byte (element order, attribute
//! order, indentation, **no `lastBuildDate`**) is simpler than auditing a
//! feed crate's output stability across versions.
//!
//! Determinism rules:
//! - no `lastBuildDate`, no generator timestamps — nothing time-of-render;
//! - fixed element and attribute emission order;
//! - items arrive pre-sorted from storage (`published_at` DESC with the
//!   episode id as total-order tiebreak) and are written as given;
//! - `pubDate` is RFC 2822 (UTC), rendered through jiff.
//!
//! The caller (the facade) resolves everything URL-shaped against the
//! configured external base URL — never the bind address — and the
//! listen key can never appear because enclosure URLs point at our own
//! `/media/...` routes.

use std::io::Write;

use quick_xml::Writer;
use quick_xml::events::{BytesDecl, BytesText, Event};
use url::Url;

/// A fully resolved feed, ready to serialize. Pure data: building one is
/// the facade's job, writing one is [`write`]'s.
#[derive(Debug, Clone)]
pub struct Feed {
    /// Channel title.
    pub title: String,
    /// Channel link (the external base URL).
    pub link: Url,
    /// Channel description.
    pub description: String,
    /// Cover art URL, if the show has artwork.
    pub artwork: Option<Url>,
    /// Items, newest first (written in the given order).
    pub items: Vec<Item>,
}

/// One episode of a [`Feed`].
#[derive(Debug, Clone)]
pub struct Item {
    /// Episode title.
    pub title: String,
    /// Stable GUID (`<provider>/<show>/<episode>`), never a file path.
    pub guid: String,
    /// Episode notes, if any.
    pub description: Option<String>,
    /// Publication time, if known.
    pub published_at: Option<jiff::Timestamp>,
    /// Where the audio is served (external base + `/media/...`).
    pub enclosure_url: Url,
    /// Exact byte length of the audio file.
    pub enclosure_bytes: u64,
    /// MIME type of the audio.
    pub enclosure_mime: String,
    /// Duration in seconds, if known.
    pub duration_secs: Option<u32>,
}

const ITUNES_NS: &str = "http://www.itunes.com/dtds/podcast-1.0.dtd";

/// Serialize `feed` as podcast RSS 2.0. Byte-identical for identical
/// input, by construction. The only failure mode is sink I/O — escaping
/// and structure cannot fail.
pub fn write<W: Write>(feed: &Feed, out: &mut W) -> std::io::Result<()> {
    let mut w = Writer::new_with_indent(out, b' ', 2);
    w.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

    w.create_element("rss")
        .with_attribute(("version", "2.0"))
        .with_attribute(("xmlns:itunes", ITUNES_NS))
        .write_inner_content(|w| {
            w.create_element("channel").write_inner_content(|w| {
                text_element(w, "title", &feed.title)?;
                text_element(w, "link", feed.link.as_str())?;
                text_element(w, "description", &feed.description)?;
                if let Some(artwork) = &feed.artwork {
                    w.create_element("itunes:image")
                        .with_attribute(("href", artwork.as_str()))
                        .write_empty()?;
                }
                feed.items.iter().try_for_each(|item| write_item(w, item))
            })?;
            Ok(())
        })?;
    Ok(())
}

fn write_item<W: Write>(w: &mut Writer<W>, item: &Item) -> std::io::Result<()> {
    w.create_element("item").write_inner_content(|w| {
        text_element(w, "title", &item.title)?;
        w.create_element("guid")
            .with_attribute(("isPermaLink", "false"))
            .write_text_content(BytesText::new(&item.guid))?;
        if let Some(at) = item.published_at {
            text_element(w, "pubDate", &rfc2822(at))?;
        }
        if let Some(description) = &item.description {
            text_element(w, "description", description)?;
        }
        w.create_element("enclosure")
            .with_attribute(("url", item.enclosure_url.as_str()))
            .with_attribute(("length", item.enclosure_bytes.to_string().as_str()))
            .with_attribute(("type", item.enclosure_mime.as_str()))
            .write_empty()?;
        if let Some(secs) = item.duration_secs {
            text_element(w, "itunes:duration", &hms(secs))?;
        }
        Ok(())
    })?;
    Ok(())
}

fn text_element<W: Write>(w: &mut Writer<W>, name: &str, text: &str) -> std::io::Result<()> {
    w.create_element(name)
        .write_text_content(BytesText::new(text))?;
    Ok(())
}

/// RFC 2822 in UTC, as podcast `pubDate`s want.
fn rfc2822(at: jiff::Timestamp) -> String {
    jiff::fmt::rfc2822::to_string(&at.to_zoned(jiff::tz::TimeZone::UTC))
        .unwrap_or_else(|_| unreachable!("UTC timestamps are always RFC 2822-representable"))
}

/// `HH:MM:SS`, the least ambiguous `itunes:duration` form.
fn hms(secs: u32) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed() -> Feed {
        Feed {
            title: "Melodik Revolution".into(),
            link: "http://nas.lan:8380/".parse().expect("valid url"),
            description: "Monthly trance & <melody>".into(),
            artwork: Some(
                "http://nas.lan:8380/artwork/melodik-revolution.png"
                    .parse()
                    .expect("valid url"),
            ),
            items: vec![
                Item {
                    title: "Melodik Revolution 162".into(),
                    guid: "difm/melodik-revolution/162".into(),
                    description: None,
                    published_at: Some("2026-07-05T18:00:00Z".parse().expect("valid ts")),
                    enclosure_url: "http://nas.lan:8380/media/melodik-revolution/162.mp3"
                        .parse()
                        .expect("valid url"),
                    enclosure_bytes: 288_111_664,
                    enclosure_mime: "audio/mpeg".into(),
                    duration_secs: Some(7236),
                },
                Item {
                    title: "Melodik Revolution 161".into(),
                    guid: "difm/melodik-revolution/161".into(),
                    description: Some("older & wiser".into()),
                    published_at: Some("2026-06-07T18:00:00Z".parse().expect("valid ts")),
                    enclosure_url: "http://nas.lan:8380/media/melodik-revolution/161.mp3"
                        .parse()
                        .expect("valid url"),
                    enclosure_bytes: 100,
                    enclosure_mime: "audio/mpeg".into(),
                    duration_secs: None,
                },
            ],
        }
    }

    fn render(feed: &Feed) -> Vec<u8> {
        let mut out = Vec::new();
        write(feed, &mut out).expect("serializes");
        out
    }

    #[test]
    fn regeneration_is_byte_identical() {
        assert_eq!(render(&feed()), render(&feed()));
    }

    #[test]
    fn no_time_of_render_leaks_in() {
        let xml = String::from_utf8(render(&feed())).expect("utf-8");
        assert!(!xml.contains("lastBuildDate"));
        assert!(!xml.contains("generator"));
    }

    #[test]
    fn a_strict_parser_accepts_it() {
        let parsed = feed_rs::parser::parse(render(&feed()).as_slice()).expect("valid feed");
        assert_eq!(parsed.title.expect("title").content, "Melodik Revolution");
        assert_eq!(parsed.entries.len(), 2);

        let newest = &parsed.entries[0];
        assert_eq!(newest.id, "difm/melodik-revolution/162");
        let media = newest.media.first().expect("enclosure");
        let content = media.content.first().expect("content");
        assert_eq!(
            content.url.as_ref().expect("url").as_str(),
            "http://nas.lan:8380/media/melodik-revolution/162.mp3"
        );
        assert_eq!(content.size, Some(288_111_664));
        assert_eq!(
            content.content_type.as_ref().expect("mime").to_string(),
            "audio/mpeg"
        );
        assert_eq!(
            media.duration,
            Some(std::time::Duration::from_secs(7236)),
            "itunes:duration must parse as HH:MM:SS"
        );
        assert!(newest.published.is_some(), "pubDate must parse");
    }

    #[test]
    fn xml_escaping_is_handled() {
        let xml = String::from_utf8(render(&feed())).expect("utf-8");
        assert!(xml.contains("Monthly trance &amp; &lt;melody&gt;"));
    }

    #[test]
    fn duration_renders_as_hms() {
        assert_eq!(hms(7236), "02:00:36");
        assert_eq!(hms(59), "00:00:59");
    }
}
