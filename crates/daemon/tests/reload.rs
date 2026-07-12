//! Config reload: the `reload::apply` path the SIGHUP handler drives.
//! A mock AudioAddict API upstream, a real config file on disk that the
//! tests rewrite between reloads, and the axum router reading through
//! the watch channel.

use std::path::Path;
use std::sync::Arc;

use serde_json::json;
use splicefeed::{Config, Library, ShowSlug};
use splicefeed_daemon::{reload, server};
use tokio::sync::watch;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const AUDIO: &[u8] = b"reload-test-audio-bytes";

/// Mount show/episodes/audio mocks for one slug.
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
                .set_body_bytes(AUDIO),
        )
        .mount(server)
        .await;
}

fn write_config(
    config_path: &Path,
    data_dir: &Path,
    api_base: &str,
    external: &str,
    slugs: &[&str],
) {
    let shows: String = slugs
        .iter()
        .map(|slug| format!("[[shows]]\nslug = \"{slug}\"\n"))
        .collect();
    std::fs::write(
        config_path,
        format!(
            r#"
            data_dir = "{data}"
            external_base_url = "{external}"

            [auth.difm]
            api_key = "member-key"
            base_url = "{api_base}/"

            {shows}
            "#,
            data = data_dir.display(),
        ),
    )
    .expect("config written");
}

struct Rig {
    tx: watch::Sender<Arc<Library>>,
    base: String,
    dir: tempfile::TempDir,
    config_path: std::path::PathBuf,
}

/// Boot a library from a config file, sync it, and serve it through a
/// watch channel — the same wiring `run` uses.
async fn rig(api: &MockServer, slugs: &[&str], external: &str) -> Rig {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("config.toml");
    write_config(&config_path, dir.path(), &api.uri(), external, slugs);

    let config = Config::load(Some(&config_path)).expect("config loads");
    let library = Arc::new(Library::open(config).await.expect("library opens"));
    for slug in slugs {
        let slug: ShowSlug = slug.parse().expect("valid slug");
        library.sync(&slug).await.expect("sync succeeds");
    }

    let (tx, rx) = watch::channel(library);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral bind");
    let addr = listener.local_addr().expect("bound addr");
    let metrics = splicefeed_daemon::telemetry::init(rx.borrow().config()).expect("metrics init");
    tokio::spawn(async move {
        axum::serve(
            listener,
            server::router(rx, splicefeed_daemon::control::Vitals::default(), metrics),
        )
        .await
        .expect("server runs");
    });
    Rig {
        tx,
        base: format!("http://{addr}"),
        dir,
        config_path,
    }
}

async fn get_status(base: &str, path: &str) -> u16 {
    reqwest::get(format!("{base}{path}"))
        .await
        .expect("request succeeds")
        .status()
        .as_u16()
}

#[tokio::test]
async fn reload_picks_up_an_added_show() {
    let api = MockServer::start().await;
    mount_show(&api, "first-show").await;
    mount_show(&api, "second-show").await;
    let rig = rig(&api, &["first-show"], "http://nas.lan:8380").await;

    assert_eq!(get_status(&rig.base, "/feeds/first-show.xml").await, 200);
    assert_eq!(get_status(&rig.base, "/feeds/second-show.xml").await, 404);

    write_config(
        &rig.config_path,
        rig.dir.path(),
        &api.uri(),
        "http://nas.lan:8380",
        &["first-show", "second-show"],
    );
    let library = reload::apply(&rig.tx, Some(&rig.config_path))
        .await
        .expect("reload applies");
    // The SIGHUP handler follows a successful swap with a sync.
    library
        .sync(&"second-show".parse().expect("valid slug"))
        .await
        .expect("new show syncs");

    assert_eq!(get_status(&rig.base, "/feeds/second-show.xml").await, 200);
    assert_eq!(get_status(&rig.base, "/feeds/first-show.xml").await, 200);
}

#[tokio::test]
async fn broken_config_keeps_the_old_one_serving() {
    let api = MockServer::start().await;
    mount_show(&api, "first-show").await;
    let rig = rig(&api, &["first-show"], "http://nas.lan:8380").await;

    std::fs::write(&rig.config_path, "this is not [toml").expect("write garbage");
    reload::apply(&rig.tx, Some(&rig.config_path))
        .await
        .err()
        .expect("garbage config rejected");

    // A config that parses but fails validation is rejected too.
    std::fs::write(
        &rig.config_path,
        "[[shows]]\nslug = \"first-show\"\n", // no api_key
    )
    .expect("write invalid");
    reload::apply(&rig.tx, Some(&rig.config_path))
        .await
        .err()
        .expect("invalid config rejected");

    assert_eq!(
        get_status(&rig.base, "/feeds/first-show.xml").await,
        200,
        "old config still serving after both failed reloads"
    );
}

#[tokio::test]
async fn bind_and_data_dir_changes_are_restart_only() {
    let api = MockServer::start().await;
    mount_show(&api, "first-show").await;
    let rig = rig(&api, &["first-show"], "http://nas.lan:8380").await;

    let rewrite = |extra: &str| {
        let shows = "[[shows]]\nslug = \"first-show\"\n";
        std::fs::write(
            &rig.config_path,
            format!(
                "{extra}\ndata_dir = \"{}\"\n[auth.difm]\napi_key = \"k\"\nbase_url = \"{}/\"\n{shows}",
                rig.dir.path().display(),
                api.uri(),
            ),
        )
        .expect("config written");
    };

    rewrite("bind = \"127.0.0.1:19999\"");
    let err = reload::apply(&rig.tx, Some(&rig.config_path))
        .await
        .err()
        .expect("bind change rejected");
    assert!(err.to_string().contains("restart required"), "{err}");

    // data_dir change: write a config whose data_dir points elsewhere.
    let other = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        &rig.config_path,
        format!(
            "data_dir = \"{}\"\n[auth.difm]\napi_key = \"k\"\nbase_url = \"{}/\"\n[[shows]]\nslug = \"first-show\"\n",
            other.path().display(),
            api.uri(),
        ),
    )
    .expect("config written");
    let err = reload::apply(&rig.tx, Some(&rig.config_path))
        .await
        .err()
        .expect("data_dir change rejected");
    assert!(err.to_string().contains("restart required"), "{err}");
}

#[tokio::test]
async fn external_base_url_change_shows_up_in_the_next_feed() {
    let api = MockServer::start().await;
    mount_show(&api, "first-show").await;
    let rig = rig(&api, &["first-show"], "http://old.lan:8380").await;

    let body = reqwest::get(format!("{}/feeds/first-show.xml", rig.base))
        .await
        .expect("request")
        .text()
        .await
        .expect("body");
    assert!(body.contains("http://old.lan:8380/media/"));

    write_config(
        &rig.config_path,
        rig.dir.path(),
        &api.uri(),
        "http://new.lan:9999",
        &["first-show"],
    );
    reload::apply(&rig.tx, Some(&rig.config_path))
        .await
        .expect("reload applies");

    let body = reqwest::get(format!("{}/feeds/first-show.xml", rig.base))
        .await
        .expect("request")
        .text()
        .await
        .expect("body");
    assert!(
        body.contains("http://new.lan:9999/media/"),
        "enclosures follow the reloaded external_base_url"
    );
    assert!(!body.contains("http://old.lan:8380"));
}
