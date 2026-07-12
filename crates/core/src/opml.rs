//! OPML 2.0 subscription-list generation: one import subscribes a
//! podcast app to every feed this daemon serves.
//!
//! Written with the `quick-xml` writer under the same determinism rules
//! as the RSS module: fixed element/attribute order, no timestamps
//! (`dateCreated` deliberately omitted), byte-identical output for
//! identical input.

use std::io::Write;

use quick_xml::Writer;
use quick_xml::events::{BytesDecl, BytesText, Event};
use url::Url;

/// One feed in the subscription list.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Human-readable show title.
    pub title: String,
    /// Absolute feed URL (built from the external base URL).
    pub feed_url: Url,
}

/// Serialize an OPML 2.0 document titled `title` over `subscriptions`,
/// in the given order.
pub fn write<W: Write>(
    title: &str,
    subscriptions: &[Subscription],
    out: &mut W,
) -> std::io::Result<()> {
    let mut w = Writer::new_with_indent(out, b' ', 2);
    w.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

    w.create_element("opml")
        .with_attribute(("version", "2.0"))
        .write_inner_content(|w| {
            w.create_element("head").write_inner_content(|w| {
                w.create_element("title")
                    .write_text_content(BytesText::new(title))?;
                Ok(())
            })?;
            w.create_element("body").write_inner_content(|w| {
                subscriptions.iter().try_for_each(|subscription| {
                    w.create_element("outline")
                        .with_attribute(("type", "rss"))
                        .with_attribute(("text", subscription.title.as_str()))
                        .with_attribute(("title", subscription.title.as_str()))
                        .with_attribute(("xmlUrl", subscription.feed_url.as_str()))
                        .write_empty()?;
                    Ok(())
                })
            })?;
            Ok(())
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriptions() -> Vec<Subscription> {
        vec![
            Subscription {
                title: "Melodik Revolution".into(),
                feed_url: "http://nas.lan:8380/feeds/melodik-revolution.xml"
                    .parse()
                    .expect("valid url"),
            },
            Subscription {
                title: "Trance & <Friends>".into(),
                feed_url: "http://nas.lan:8380/feeds/trance-friends.xml"
                    .parse()
                    .expect("valid url"),
            },
        ]
    }

    fn render(subscriptions: &[Subscription]) -> String {
        let mut out = Vec::new();
        write("splicefeed", subscriptions, &mut out).expect("serializes");
        String::from_utf8(out).expect("utf-8")
    }

    #[test]
    fn regeneration_is_byte_identical_and_well_formed() {
        let first = render(&subscriptions());
        assert_eq!(first, render(&subscriptions()));

        // Well-formed XML with the expected shape.
        let mut reader = quick_xml::Reader::from_str(&first);
        let mut outlines = 0;
        loop {
            match reader.read_event().expect("well-formed") {
                quick_xml::events::Event::Empty(e) if e.name().as_ref() == b"outline" => {
                    outlines += 1;
                }
                quick_xml::events::Event::Eof => break,
                _ => {}
            }
        }
        assert_eq!(outlines, 2);
    }

    #[test]
    fn attributes_carry_urls_and_escaped_titles() {
        let xml = render(&subscriptions());
        assert!(xml.contains(r#"<opml version="2.0">"#));
        assert!(xml.contains(r#"type="rss""#));
        assert!(xml.contains(r#"xmlUrl="http://nas.lan:8380/feeds/melodik-revolution.xml""#));
        assert!(
            xml.contains("Trance &amp; &lt;Friends&gt;"),
            "titles must be escaped: {xml}"
        );
        assert!(!xml.contains("dateCreated"), "no time-of-render output");
    }
}
