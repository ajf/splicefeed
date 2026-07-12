//! Telemetry integration: sync-engine events land as Prometheus
//! counters, HTTP requests land in the duration histogram, and the
//! `/metrics` route serves it all in exposition format.

use std::sync::Arc;

use serde_json::json;
use splicefeed::{Config, Library, ShowSlug};
use splicefeed_daemon::{control, server, telemetry};
use tokio::sync::watch;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn mount_show(server: &MockServer, slug: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/shows/{slug}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "slug": slug, "name": format!("Show {slug}"),
        })))
        .mount(server)
        .await;
    let episode = json!({
        "slug": "1",
        "start_at": "2026-07-05T18:00:00Z",
        "tracks": [{
            "length": 60,
            "display_title": format!("{slug} 1"),
            "content": { "assets": [{ "url": format!("{}/audio/{slug}.mp4", server.uri()) }] },
        }],
    });
    Mock::given(method("GET"))
        .and(path(format!("/shows/{slug}/episodes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([episode])))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/shows/{slug}/episodes/1")))
        .respond_with(ResponseTemplate::new(200).set_body_json(episode))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/audio/{slug}.mp4")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "audio/mp4")
                .set_body_bytes(b"telemetry-test-audio".as_slice()),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn events_and_requests_show_up_in_the_scrape() {
    let api = MockServer::start().await;
    mount_show(&api, "test-show").await;

    let dir = tempfile::tempdir().expect("tempdir");
    let config = Config::from_toml_str(&format!(
        r#"
        data_dir = "{data}"

        [retention]
        keep_last = 0

        [auth.difm]
        api_key = "k"
        base_url = "{base}/"

        [[shows]]
        slug = "test-show"
        "#,
        data = dir.path().display(),
        base = api.uri(),
    ))
    .expect("config parses");
    let metrics = telemetry::init(&config).expect("metrics init");
    let library = Arc::new(Library::open(config).await.expect("library opens"));

    let (_tx, rx) = watch::channel(Arc::clone(&library));
    tokio::spawn(telemetry::pump_events(rx.clone(), metrics.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral bind");
    let addr = listener.local_addr().expect("bound addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            server::router(rx, control::Vitals::default(), metrics),
        )
        .await
        .expect("server runs");
    });

    // A sync produces poll + discovery events; keep_last = 0 means no
    // download, but the counters must still move.
    let slug: ShowSlug = "test-show".parse().expect("valid slug");
    library.sync(&slug).await.expect("sync succeeds");
    // One HTTP request for the histogram.
    reqwest::get(format!("http://{addr}/healthz"))
        .await
        .expect("healthz");

    // The pump is async; give it a moment to drain.
    let mut scrape = String::new();
    for _ in 0..50 {
        scrape = reqwest::get(format!("http://{addr}/metrics"))
            .await
            .expect("scrape")
            .text()
            .await
            .expect("body");
        if scrape.contains("splicefeed_polls") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        scrape.contains("splicefeed_polls_total")
            && scrape.contains("show=\"test-show\"")
            && scrape.contains("ok=\"true\""),
        "poll counter with labels missing from scrape:\n{scrape}"
    );
    assert!(
        scrape.contains("splicefeed_episodes_discovered_total"),
        "discovery counter missing:\n{scrape}"
    );
    assert!(
        scrape.contains("http_server_request_duration"),
        "http histogram missing:\n{scrape}"
    );
}
