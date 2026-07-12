//! End-to-end sync: a mock AudioAddict API (metadata + audio bytes) on one
//! side, `Library::sync` on the other. Exercises discovery, download
//! (hash, atomic write), retention pruning, and tombstone behavior.

use serde_json::json;
use splicefeed::{Config, FileProblem, Library, ShowSlug};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const AUDIO_NEW: &[u8] = b"new-episode-audio-bytes";
const AUDIO_OLD: &[u8] = b"old-episode-audio-bytes";

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

/// Mock the AudioAddict endpoints the sync engine hits. Asset URLs in the
/// single-episode responses are relative to the mock server, so downloads
/// come back to it too.
async fn mount_api(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/shows/test-show"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "slug": "test-show",
            "name": "Test Show",
            "description": "a show for tests",
        })))
        .mount(server)
        .await;

    // Listings carry no assets (matches production: audio appears only on
    // the authenticated single-episode endpoint).
    Mock::given(method("GET"))
        .and(path("/shows/test-show/episodes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            episode_json("162", "2026-07-05T18:00:00Z", None),
            episode_json("161", "2026-06-07T18:00:00Z", None),
        ])))
        .mount(server)
        .await;

    for slug in ["161", "162"] {
        let start_at = if slug == "162" {
            "2026-07-05T18:00:00Z"
        } else {
            "2026-06-07T18:00:00Z"
        };
        Mock::given(method("GET"))
            .and(path(format!("/shows/test-show/episodes/{slug}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(episode_json(
                slug,
                start_at,
                Some(&server.uri()),
            )))
            .mount(server)
            .await;
    }

    Mock::given(method("GET"))
        .and(path("/audio/162.mp4"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "audio/mp4")
                .set_body_bytes(AUDIO_NEW),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/audio/161.mp4"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "audio/mp4")
                .set_body_bytes(AUDIO_OLD),
        )
        .mount(server)
        .await;
}

async fn open_library(server: &MockServer, data_dir: &std::path::Path) -> Library {
    open_library_with(server, data_dir, "").await
}

async fn open_library_with(
    server: &MockServer,
    data_dir: &std::path::Path,
    extra_toml: &str,
) -> Library {
    let config = Config::from_toml_str(&format!(
        r#"
        data_dir = "{data}"
        {extra_toml}

        [auth.difm]
        api_key = "member-key"
        base_url = "{base}/"

        [[shows]]
        slug = "test-show"
        "#,
        data = data_dir.display(),
        base = server.uri(),
    ))
    .expect("config parses");
    Library::open(config).await.expect("library opens")
}

#[tokio::test]
async fn sync_downloads_only_what_retention_keeps() {
    let server = MockServer::start().await;
    mount_api(&server).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library_with(&server, dir.path(), "[retention]\nkeep_last = 1").await;
    let slug: ShowSlug = "test-show".parse().expect("valid slug");

    let report = library.sync(&slug).await.expect("sync succeeds");
    assert_eq!(report.discovered, 2);
    // keep_last = 1: retention is planned before downloading, so the
    // older episode is never fetched — not downloaded-then-pruned.
    assert_eq!(report.downloaded, 1);
    assert_eq!(report.pruned, 0);

    let media = dir.path().join("media").join("test-show");
    assert_eq!(
        std::fs::read(media.join("162.m4a")).expect("newest episode on disk"),
        AUDIO_NEW
    );
    assert!(!media.join("161.m4a").exists(), "older is never fetched");

    // Second sync: nothing new, nothing re-fetched.
    let report = library.sync(&slug).await.expect("second sync succeeds");
    assert_eq!(report.discovered, 0);
    assert_eq!(report.downloaded, 0);
    assert_eq!(report.pruned, 0);
    assert!(media.join("162.m4a").exists());
    assert!(!media.join("161.m4a").exists());
}

#[tokio::test]
async fn widened_retention_revives_pruned_episodes() {
    let server = MockServer::start().await;
    mount_api(&server).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let slug: ShowSlug = "test-show".parse().expect("valid slug");
    let media = dir.path().join("media").join("test-show");

    // Cache both episodes.
    let library = open_library_with(&server, dir.path(), "[retention]\nkeep_last = 2").await;
    let report = library.sync(&slug).await.expect("sync succeeds");
    assert_eq!(report.downloaded, 2);
    drop(library);

    // Tighten retention: the older episode is pruned to a tombstone.
    let library = open_library_with(&server, dir.path(), "[retention]\nkeep_last = 1").await;
    let report = library.sync(&slug).await.expect("sync succeeds");
    assert_eq!(report.downloaded, 0);
    assert_eq!(report.pruned, 1);
    assert!(!media.join("161.m4a").exists(), "pruned file deleted");
    drop(library);

    // Widen it again: the tombstone fits the window and is revived.
    let library = open_library_with(&server, dir.path(), "[retention]\nkeep_last = 2").await;
    let report = library.sync(&slug).await.expect("sync succeeds");
    assert_eq!(report.discovered, 0, "revival is not re-discovery");
    assert_eq!(report.downloaded, 1, "tombstone re-downloaded");
    assert_eq!(report.pruned, 0);
    assert_eq!(
        std::fs::read(media.join("161.m4a")).expect("revived file on disk"),
        AUDIO_OLD
    );
}

#[tokio::test]
async fn fetch_last_bounds_discovery_and_download() {
    let server = MockServer::start().await;
    mount_api(&server).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library_with(
        &server,
        dir.path(),
        "fetch_last = 1\n[retention]\nkeep_last = 1",
    )
    .await;
    let slug: ShowSlug = "test-show".parse().expect("valid slug");

    let report = library.sync(&slug).await.expect("sync succeeds");
    // Upstream lists two episodes; only the newest is even discovered,
    // so nothing needs pruning afterwards.
    assert_eq!(report.discovered, 1);
    assert_eq!(report.downloaded, 1);
    assert_eq!(report.pruned, 0);

    let media = dir.path().join("media").join("test-show");
    assert!(media.join("162.m4a").exists(), "newest downloaded");
    assert!(!media.join("161.m4a").exists(), "older never fetched");
}

#[tokio::test]
async fn unchanged_listing_polls_via_304() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/shows/test-show"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "slug": "test-show", "name": "Test Show",
        })))
        .mount(&server)
        .await;
    // The listing serves one body with a validator, then only answers
    // 304 to the conditional request; a non-conditional second poll
    // hits the 500 fallback and fails the test loudly.
    Mock::given(method("GET"))
        .and(path("/shows/test-show/episodes"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "W/\"v1\"")
                .set_body_json(json!([episode_json("162", "2026-07-05T18:00:00Z", None)])),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/shows/test-show/episodes"))
        .and(wiremock::matchers::header("if-none-match", "W/\"v1\""))
        .respond_with(ResponseTemplate::new(304))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/shows/test-show/episodes"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/shows/test-show/episodes/162"))
        .respond_with(ResponseTemplate::new(200).set_body_json(episode_json(
            "162",
            "2026-07-05T18:00:00Z",
            Some(&server.uri()),
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/audio/162.mp4"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "audio/mp4")
                .set_body_bytes(AUDIO_NEW),
        )
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library(&server, dir.path()).await;
    let slug: ShowSlug = "test-show".parse().expect("valid slug");

    let report = library.sync(&slug).await.expect("first sync");
    assert_eq!(report.discovered, 1);
    assert_eq!(report.downloaded, 1);

    // Second poll: 304 — nothing new, nothing re-fetched, poll healthy.
    let report = library.sync(&slug).await.expect("conditional sync");
    assert_eq!(report.discovered, 0);
    assert_eq!(report.downloaded, 0);
    let records = library.show_records().await.expect("records");
    assert_eq!(records[0].last_poll_ok, Some(true));
    assert_eq!(records[0].episodes_etag.as_deref(), Some("W/\"v1\""));
}

#[tokio::test]
async fn feed_is_deterministic_valid_and_external() {
    let server = MockServer::start().await;
    mount_api(&server).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library_with(
        &server,
        dir.path(),
        "external_base_url = \"http://nas.lan:8380\"",
    )
    .await;
    let slug: ShowSlug = "test-show".parse().expect("valid slug");

    // Before any sync there is nothing to build a feed from.
    let mut out = Vec::new();
    assert!(matches!(
        library.write_feed(&slug, &mut out).await,
        Err(splicefeed::LibraryError::NotSynced(_))
    ));

    library.sync(&slug).await.expect("sync succeeds");
    let mut first = Vec::new();
    library
        .write_feed(&slug, &mut first)
        .await
        .expect("feed writes");
    let mut second = Vec::new();
    library
        .write_feed(&slug, &mut second)
        .await
        .expect("feed writes again");
    assert_eq!(first, second, "byte-identical regeneration");

    let parsed = feed_rs::parser::parse(first.as_slice()).expect("valid feed");
    assert_eq!(parsed.entries.len(), 2);
    assert_eq!(parsed.entries[0].id, "difm/test-show/162");
    let enclosure = parsed.entries[0]
        .media
        .first()
        .and_then(|m| m.content.first())
        .and_then(|c| c.url.as_ref())
        .expect("enclosure")
        .to_string();
    assert!(
        enclosure.starts_with("http://nas.lan:8380/media/test-show/"),
        "enclosures come from external_base_url, got {enclosure}"
    );

    // An unknown show is rejected, not an empty feed.
    let ghost: ShowSlug = "ghost".parse().expect("valid slug");
    assert!(matches!(
        library.write_feed(&ghost, &mut Vec::new()).await,
        Err(splicefeed::LibraryError::UnknownShow(_))
    ));
}

#[tokio::test]
async fn verify_detects_and_fixes_damage() {
    let server = MockServer::start().await;
    mount_api(&server).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library_with(
        &server,
        dir.path(),
        "fetch_last = 1\n[retention]\nkeep_last = 1",
    )
    .await;
    let slug: ShowSlug = "test-show".parse().expect("valid slug");
    library.sync(&slug).await.expect("sync succeeds");
    let media = dir.path().join("media").join("test-show").join("162.m4a");

    // Pristine cache: nothing to report.
    let report = library.verify(&slug, false).await.expect("verify runs");
    assert_eq!(report.checked, 1);
    assert_eq!(report.intact, 1);
    assert!(report.problems.is_empty());

    // Same size, different content: only the hash notices.
    std::fs::write(&media, vec![0_u8; AUDIO_NEW.len()]).expect("corrupt file");
    let report = library.verify(&slug, false).await.expect("verify runs");
    assert_eq!(report.problems[0].problem, FileProblem::HashMismatch);
    assert!(!report.problems[0].fixed, "no --fix, no download");

    // --fix restores the original bytes.
    let report = library.verify(&slug, true).await.expect("verify fixes");
    assert!(report.problems[0].fixed);
    assert_eq!(std::fs::read(&media).expect("file back"), AUDIO_NEW);

    // Truncation is caught by size before hashing.
    std::fs::write(&media, b"short").expect("truncate file");
    let report = library.verify(&slug, false).await.expect("verify runs");
    assert!(matches!(
        report.problems[0].problem,
        FileProblem::SizeMismatch { actual: 5, .. }
    ));

    // Deletion reads as missing, and --fix recovers that too.
    std::fs::remove_file(&media).expect("delete file");
    let report = library.verify(&slug, true).await.expect("verify fixes");
    assert_eq!(report.problems[0].problem, FileProblem::Missing);
    assert!(report.problems[0].fixed);
    assert_eq!(std::fs::read(&media).expect("file back"), AUDIO_NEW);

    // And the library is clean again afterwards.
    let report = library.verify(&slug, false).await.expect("verify runs");
    assert_eq!(report.intact, 1);
}

#[tokio::test]
async fn listing_failure_is_an_error_and_recorded() {
    let server = MockServer::start().await;
    // No mocks mounted: every request 404s, so the show lookup fails.
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library(&server, dir.path()).await;
    let slug: ShowSlug = "test-show".parse().expect("valid slug");

    library.sync(&slug).await.expect_err("sync fails");
}

#[tokio::test]
async fn unknown_show_is_rejected() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library(&server, dir.path()).await;
    let slug: ShowSlug = "not-configured".parse().expect("valid slug");

    assert!(matches!(
        library.sync(&slug).await,
        Err(splicefeed::LibraryError::UnknownShow(_))
    ));
}
