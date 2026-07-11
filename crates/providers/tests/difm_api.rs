//! Provider tests against captured AudioAddict fixtures served by
//! wiremock. Fixtures were captured live on 2026-07-11; `splicefeed probe`
//! is the tool for noticing when the live API drifts away from them.

use splicefeed_core::domain::{ListenKey, ShowSlug};
use splicefeed_providers::difm::DifmProvider;
use splicefeed_providers::{Provider, ProviderError};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn fixture(name: &str) -> String {
    let path = format!(
        "{}/tests/fixtures/audioaddict/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("fixture {path}: {e}"))
}

fn provider_for(server: &MockServer, quarantine: &std::path::Path) -> DifmProvider {
    DifmProvider::builder(ListenKey::new("test-key".into()))
        .base_url(server.uri().parse().expect("mock uri parses"))
        .quarantine_dir(quarantine)
        .build()
        .expect("provider builds")
}

fn slug(s: &str) -> ShowSlug {
    s.parse().expect("valid slug")
}

#[tokio::test]
async fn show_metadata_parses_from_fixture() {
    let server = MockServer::start().await;
    let tmp = tempdir();
    Mock::given(method("GET"))
        .and(path("/shows/melodik-revolution"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("show.json")))
        .mount(&server)
        .await;

    let provider = provider_for(&server, &tmp);
    let meta = provider
        .show(&slug("melodik-revolution"))
        .await
        .expect("parses");

    assert_eq!(meta.title, "Melodik Revolution");
    assert_eq!(meta.slug.as_str(), "melodik-revolution");
    assert!(
        meta.description
            .as_deref()
            .is_some_and(|d| d.contains("Trance"))
    );
    let artwork = meta.artwork.expect("has artwork");
    assert!(
        artwork
            .as_str()
            .starts_with("https://cdn-images.audioaddict.com/")
    );
    assert!(
        !artwork.as_str().contains('{'),
        "template suffix must be stripped"
    );
    cleanup(tmp);
}

#[tokio::test]
async fn episode_listing_parses_newest_first() {
    let server = MockServer::start().await;
    let tmp = tempdir();
    Mock::given(method("GET"))
        .and(path("/shows/melodik-revolution/episodes"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("episodes_page1.json")))
        .mount(&server)
        .await;

    let provider = provider_for(&server, &tmp);
    let episodes = provider
        .episodes(&slug("melodik-revolution"))
        .await
        .expect("parses");

    assert_eq!(episodes.len(), 2);
    let first = &episodes[0];
    assert_eq!(first.id.as_str(), "162");
    assert_eq!(first.title, "Melodik Revolution 162");
    assert_eq!(first.duration_secs, Some(7200));
    let published = first.published_at.expect("has pubdate");
    assert_eq!(published.to_string(), "2026-07-05T18:00:00Z");
    assert!(
        episodes[0].published_at >= episodes[1].published_at,
        "must be newest first"
    );
    cleanup(tmp);
}

#[tokio::test]
async fn one_drifted_entry_is_quarantined_not_fatal() {
    let server = MockServer::start().await;
    let tmp = tempdir();
    // Second entry has no slug — unconvertible — first stays usable.
    let body = r#"[
        {"slug": "162", "name": "162", "start_at": "2026-07-05T14:00:00-04:00",
         "tracks": [{"length": 7200, "display_title": "Melodik Revolution 162"}]},
        {"name": "the api changed", "tracks": "surprise, a string"}
    ]"#;
    Mock::given(method("GET"))
        .and(path("/shows/melodik-revolution/episodes"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let provider = provider_for(&server, &tmp);
    let episodes = provider
        .episodes(&slug("melodik-revolution"))
        .await
        .expect("not fatal");

    assert_eq!(episodes.len(), 1, "good entry survives");
    assert_eq!(episodes[0].id.as_str(), "162");
    let quarantined: Vec<_> = std::fs::read_dir(&tmp)
        .expect("quarantine dir exists")
        .collect();
    assert_eq!(quarantined.len(), 1, "bad entry quarantined");
    cleanup(tmp);
}

#[tokio::test]
async fn garbage_payload_is_quarantined_and_errors() {
    let server = MockServer::start().await;
    let tmp = tempdir();
    Mock::given(method("GET"))
        .and(path("/shows/melodik-revolution"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>maintenance</html>"))
        .mount(&server)
        .await;

    let provider = provider_for(&server, &tmp);
    let err = provider
        .show(&slug("melodik-revolution"))
        .await
        .expect_err("must fail");

    let ProviderError::Parse {
        quarantine_path, ..
    } = &err
    else {
        panic!("expected Parse error, got: {err}");
    };
    assert!(std::path::Path::new(quarantine_path).is_file());
    cleanup(tmp);
}

#[tokio::test]
async fn unknown_show_maps_to_show_not_found() {
    let server = MockServer::start().await;
    let tmp = tempdir();
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let provider = provider_for(&server, &tmp);
    let err = provider.show(&slug("nope")).await.expect_err("404");
    assert!(matches!(err, ProviderError::ShowNotFound(_)));
    cleanup(tmp);
}

#[tokio::test]
async fn resolve_audio_without_asset_fails_with_hint_and_sends_key() {
    let server = MockServer::start().await;
    let tmp = tempdir();
    // Unauthenticated-shaped single episode: content empty, asset_url is art.
    Mock::given(method("GET"))
        .and(path("/shows/melodik-revolution/episodes/162"))
        .and(query_param("listen_key", "test-key"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(fixture("episode_162_unauth.json")),
        )
        .mount(&server)
        .await;

    let provider = provider_for(&server, &tmp);
    let err = provider
        .resolve_audio(
            &slug("melodik-revolution"),
            &"162".parse().expect("valid id"),
        )
        .await
        .expect_err("no asset in unauth response");
    assert!(matches!(err, ProviderError::NoAudioAsset { .. }));
    cleanup(tmp);
}

#[tokio::test]
async fn resolve_audio_appends_listen_key_to_asset() {
    let server = MockServer::start().await;
    let tmp = tempdir();
    let body = r#"{"slug": "162", "tracks": [
        {"length": 7200, "content": {"assets": [{"url": "//prem2.di.fm/shows/mr/ep162.mp4"}]}}
    ]}"#;
    Mock::given(method("GET"))
        .and(path("/shows/melodik-revolution/episodes/162"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let provider = provider_for(&server, &tmp);
    let audio = provider
        .resolve_audio(
            &slug("melodik-revolution"),
            &"162".parse().expect("valid id"),
        )
        .await
        .expect("resolves");

    assert_eq!(audio.url.host_str(), Some("prem2.di.fm"));
    assert!(
        audio
            .url
            .query()
            .is_some_and(|q| q.contains("listen_key=test-key"))
    );
    assert_eq!(audio.mime.as_deref(), Some("audio/mp4"));
    assert!(
        !splicefeed_providers::redacted(&audio.url).contains("test-key"),
        "redaction must hide the key"
    );
    cleanup(tmp);
}

fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splicefeed-difm-test-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp quarantine dir");
    dir
}

fn cleanup(dir: std::path::PathBuf) {
    std::fs::remove_dir_all(dir).ok();
}
