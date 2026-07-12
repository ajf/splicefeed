//! Scheduler integration: short real intervals against a mock API,
//! asserting that shows get polled repeatedly and that a reload swaps
//! the poll set without restarting the daemon.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use splicefeed::{Config, Library};
use splicefeed_daemon::{reload, scheduler};
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
    // Empty listings keep the polls fast — this test is about cadence,
    // not downloads.
    Mock::given(method("GET"))
        .and(path(format!("/shows/{slug}/episodes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(server)
        .await;
}

fn write_config(
    config_path: &std::path::Path,
    data_dir: &std::path::Path,
    api: &str,
    slugs: &[&str],
) {
    let shows: String = slugs
        .iter()
        .map(|slug| format!("[[shows]]\nslug = \"{slug}\"\n"))
        .collect();
    std::fs::write(
        config_path,
        format!(
            "data_dir = \"{}\"\npoll_interval = \"1s\"\n[auth.difm]\napi_key = \"k\"\nbase_url = \"{api}/\"\n{shows}",
            data_dir.display(),
        ),
    )
    .expect("config written");
}

/// Wait until `predicate` holds, or fail after ~5s.
async fn eventually<F: AsyncFn() -> bool>(what: &str, predicate: F) {
    for _ in 0..50 {
        if predicate().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn shows_are_polled_repeatedly_and_reload_swaps_the_set() {
    let api = MockServer::start().await;
    mount_show(&api, "first-show").await;
    mount_show(&api, "second-show").await;

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("config.toml");
    write_config(&config_path, dir.path(), &api.uri(), &["first-show"]);
    let config = Config::load(Some(&config_path)).expect("config loads");
    let library = Arc::new(Library::open(config).await.expect("library opens"));

    let (tx, rx) = watch::channel(Arc::clone(&library));
    let handle = tokio::spawn(scheduler::run(rx));

    // The scheduler (not any manual sync) polls the show — repeatedly.
    let polled = |slug: &'static str, lib: Arc<Library>| {
        move || {
            let lib = Arc::clone(&lib);
            async move {
                lib.show_records()
                    .await
                    .expect("records")
                    .iter()
                    .any(|record| record.slug.as_str() == slug && record.last_poll_ok == Some(true))
            }
        }
    };
    eventually("first poll of first-show", {
        let check = polled("first-show", Arc::clone(&library));
        move || check()
    })
    .await;
    let first_poll_at = library.show_records().await.expect("records")[0]
        .last_poll_at
        .expect("polled");
    eventually("a second poll of first-show", || {
        let lib = Arc::clone(&library);
        async move {
            lib.show_records().await.expect("records")[0]
                .last_poll_at
                .is_some_and(|at| at > first_poll_at)
        }
    })
    .await;

    // Reload with a second show: the scheduler generation swaps and the
    // new show starts getting polled without any manual sync.
    write_config(
        &config_path,
        dir.path(),
        &api.uri(),
        &["first-show", "second-show"],
    );
    let reloaded = reload::apply(&tx, Some(&config_path))
        .await
        .expect("reload applies");
    eventually("first poll of second-show", {
        let check = polled("second-show", Arc::clone(&reloaded));
        move || check()
    })
    .await;

    // Sender drop stops the scheduler (daemon shutdown path).
    drop(tx);
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("scheduler stops after sender drop")
        .expect("scheduler task completes");
}
