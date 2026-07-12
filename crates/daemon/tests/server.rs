//! HTTP server integration tests: a mock AudioAddict API upstream, a real
//! `Library` in a temp dir, and the axum router served on an ephemeral
//! port, hit with a real HTTP client.

use std::sync::Arc;

use serde_json::json;
use splicefeed::{Config, Library, ShowSlug};
use splicefeed_daemon::server;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const AUDIO_NEW: &[u8] = b"new-episode-audio-bytes";
const AUDIO_OLD: &[u8] = b"old-episode-audio-bytes";
const ART: &[u8] = b"png-not-really";
const EXTERNAL: &str = "http://nas.lan:8380";

fn episode_json(slug: &str, start_at: &str, asset_base: Option<&str>) -> serde_json::Value {
    let content = match asset_base {
        Some(base) => json!({ "assets": [{ "url": format!("{base}/audio/{slug}.mp4") }] }),
        None => json!({}),
    };
    json!({
        "slug": slug,
        "name": slug,
        "start_at": start_at,
        "tracks": [{
            "length": 7200,
            "display_title": format!("Test Show {slug}"),
            "content": content,
        }],
    })
}

async fn mount_api(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/shows/test-show"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "slug": "test-show",
            "name": "Test Show",
            "description": "a show for tests",
            "images": { "default": format!("{}/art.png{{?size,height}}", server.uri()) },
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/art.png"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/png")
                .set_body_bytes(ART),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/shows/test-show/episodes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            episode_json("162", "2026-07-05T18:00:00Z", None),
            episode_json("161", "2026-06-07T18:00:00Z", None),
        ])))
        .mount(server)
        .await;
    for (slug, start_at, body) in [
        ("162", "2026-07-05T18:00:00Z", AUDIO_NEW),
        ("161", "2026-06-07T18:00:00Z", AUDIO_OLD),
    ] {
        Mock::given(method("GET"))
            .and(path(format!("/shows/test-show/episodes/{slug}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(episode_json(
                slug,
                start_at,
                Some(&server.uri()),
            )))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/audio/{slug}.mp4")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "audio/mp4")
                    .set_body_bytes(body),
            )
            .mount(server)
            .await;
    }
}

/// A synced library served on an ephemeral port. Returns the base URL of
/// the running server (kept alive by the spawned task for the test's
/// lifetime) and the tempdir guard.
async fn served_library(api: &MockServer) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = Config::from_toml_str(&format!(
        r#"
        data_dir = "{data}"
        external_base_url = "{EXTERNAL}"

        [auth.difm]
        api_key = "member-key"
        base_url = "{base}/"

        [[shows]]
        slug = "test-show"
        "#,
        data = dir.path().display(),
        base = api.uri(),
    ))
    .expect("config parses");
    let library = Arc::new(Library::open(config).await.expect("library opens"));
    let slug: ShowSlug = "test-show".parse().expect("valid slug");
    library.sync(&slug).await.expect("sync succeeds");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral bind");
    let addr = listener.local_addr().expect("bound addr");
    let (_tx, rx) = tokio::sync::watch::channel(library);
    tokio::spawn(async move {
        let vitals = splicefeed_daemon::control::Vitals::default();
        axum::serve(listener, server::router(rx, vitals))
            .await
            .expect("server runs");
    });
    (format!("http://{addr}"), dir)
}

#[tokio::test]
async fn feed_endpoint_serves_valid_rss_with_external_urls() {
    let api = MockServer::start().await;
    mount_api(&api).await;
    let (base, _dir) = served_library(&api).await;

    let response = reqwest::get(format!("{base}/feeds/test-show.xml"))
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 200);
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("application/rss+xml"))
    );

    let body = response.bytes().await.expect("body");
    let parsed = feed_rs::parser::parse(body.as_ref()).expect("strict parser accepts the feed");
    assert_eq!(parsed.title.expect("title").content, "Test Show");
    assert_eq!(parsed.entries.len(), 2);
    let enclosure_url = parsed.entries[0]
        .media
        .first()
        .and_then(|m| m.content.first())
        .and_then(|c| c.url.as_ref())
        .expect("enclosure url")
        .to_string();
    assert_eq!(
        enclosure_url, "http://nas.lan:8380/media/test-show/162.m4a",
        "enclosures use the external base URL, never the bind address"
    );
    let icon = parsed.logo.expect("itunes:image");
    assert!(
        icon.uri
            .starts_with("http://nas.lan:8380/artwork/test-show-")
            && icon.uri.ends_with(".png"),
        "artwork under the external base, source-hashed name: {}",
        icon.uri
    );
}

#[tokio::test]
async fn feed_regeneration_is_byte_identical_over_http() {
    let api = MockServer::start().await;
    mount_api(&api).await;
    let (base, _dir) = served_library(&api).await;

    let url = format!("{base}/feeds/test-show.xml");
    let first = reqwest::get(&url)
        .await
        .expect("first")
        .bytes()
        .await
        .expect("body");
    let second = reqwest::get(&url)
        .await
        .expect("second")
        .bytes()
        .await
        .expect("body");
    assert_eq!(first, second);
}

#[tokio::test]
async fn unknown_and_malformed_feed_paths_are_404() {
    let api = MockServer::start().await;
    mount_api(&api).await;
    let (base, _dir) = served_library(&api).await;

    for path in [
        "/feeds/not-a-show.xml",
        "/feeds/test-show",
        "/feeds/a b.xml",
    ] {
        let status = reqwest::get(format!("{base}{path}"))
            .await
            .expect("request succeeds")
            .status();
        assert_eq!(status, 404, "{path} must 404");
    }
}

#[tokio::test]
async fn media_supports_range_requests() {
    let api = MockServer::start().await;
    mount_api(&api).await;
    let (base, _dir) = served_library(&api).await;
    let url = format!("{base}/media/test-show/162.m4a");

    let full = reqwest::get(&url).await.expect("full GET");
    assert_eq!(full.status(), 200);
    assert!(full.headers().contains_key("accept-ranges"));
    assert_eq!(full.bytes().await.expect("body").as_ref(), AUDIO_NEW);

    let partial = reqwest::Client::new()
        .get(&url)
        .header("range", "bytes=0-3")
        .send()
        .await
        .expect("range GET");
    assert_eq!(partial.status(), 206, "podcast apps need scrubbing");
    assert!(partial.headers().contains_key("content-range"));
    assert_eq!(
        partial.bytes().await.expect("body").as_ref(),
        &AUDIO_NEW[..4]
    );
}

#[tokio::test]
async fn path_traversal_out_of_media_is_blocked() {
    let api = MockServer::start().await;
    mount_api(&api).await;
    let (base, _dir) = served_library(&api).await;

    // The state database lives one level above the media dir.
    let status = reqwest::get(format!("{base}/media/%2e%2e/splicefeed.db"))
        .await
        .expect("request succeeds")
        .status();
    assert_ne!(status, 200, "encoded traversal must not leak files");
}

#[tokio::test]
async fn debug_serves_the_status_report_as_json() {
    let api = MockServer::start().await;
    mount_api(&api).await;
    let (base, _dir) = served_library(&api).await;

    let report: serde_json::Value = reqwest::get(format!("{base}/debug"))
        .await
        .expect("request succeeds")
        .json()
        .await
        .expect("valid json");
    assert_eq!(report["shows"][0]["slug"], "test-show");
    let episode = &report["shows"][0]["episodes"][0];
    assert_eq!(episode["state"], "cached");
    assert!(
        episode["blake3"].as_str().is_some_and(|h| h.len() == 64),
        "debug carries the full hash"
    );
    assert!(report["total_bytes"].as_u64().is_some_and(|b| b > 0));
}

#[tokio::test]
async fn healthz_reports_poll_health() {
    let api = MockServer::start().await;
    mount_api(&api).await;
    let (base, _dir) = served_library(&api).await;

    let health: serde_json::Value = reqwest::get(format!("{base}/healthz"))
        .await
        .expect("request succeeds")
        .json()
        .await
        .expect("valid json");
    assert_eq!(health["status"], "ok");
    assert_eq!(health["shows"][0]["slug"], "test-show");
    assert_eq!(health["shows"][0]["last_poll_ok"], true);
}
