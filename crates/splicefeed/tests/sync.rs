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

        [retention]
        keep_last = 1

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
async fn sync_discovers_downloads_and_prunes() {
    let server = MockServer::start().await;
    mount_api(&server).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library(&server, dir.path()).await;
    let slug: ShowSlug = "test-show".parse().expect("valid slug");

    let report = library.sync(&slug).await.expect("sync succeeds");
    assert_eq!(report.discovered, 2);
    assert_eq!(report.downloaded, 2);
    // keep_last = 1: the older episode is pruned in the same pass.
    assert_eq!(report.pruned, 1);

    let media = dir.path().join("media").join("test-show");
    assert_eq!(
        std::fs::read(media.join("162.m4a")).expect("newest episode on disk"),
        AUDIO_NEW
    );
    assert!(!media.join("161.m4a").exists(), "pruned file is deleted");

    // Second sync: nothing new, and the pruned tombstone is not
    // re-downloaded or re-pruned.
    let report = library.sync(&slug).await.expect("second sync succeeds");
    assert_eq!(report.discovered, 0);
    assert_eq!(report.downloaded, 0);
    assert_eq!(report.pruned, 0);
    assert!(media.join("162.m4a").exists());
    assert!(!media.join("161.m4a").exists());
}

#[tokio::test]
async fn fetch_last_bounds_discovery_and_download() {
    let server = MockServer::start().await;
    mount_api(&server).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library_with(&server, dir.path(), "fetch_last = 1").await;
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
async fn verify_detects_and_fixes_damage() {
    let server = MockServer::start().await;
    mount_api(&server).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let library = open_library_with(&server, dir.path(), "fetch_last = 1").await;
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
