//! Control socket integration: a real unix socket in a temp dir, spoken
//! to with raw NDJSON exactly as a client (or `socat`) would.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use serde_json::json;
use splicefeed::{Config, Library, ShowSlug, ipc};
use splicefeed_daemon::control;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
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
                .set_body_bytes(b"control-test-audio".as_slice()),
        )
        .mount(server)
        .await;
}

struct Rig {
    library: Arc<Library>,
    socket: std::path::PathBuf,
    _tx: watch::Sender<Arc<Library>>,
    _dir: tempfile::TempDir,
}

async fn rig(api: &MockServer) -> Rig {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("control.sock");
    let config = Config::from_toml_str(&format!(
        r#"
        data_dir = "{data}"
        control_socket = "{sock}"

        [auth.difm]
        api_key = "k"
        base_url = "{base}/"

        [[shows]]
        slug = "test-show"
        "#,
        data = dir.path().display(),
        sock = socket.display(),
        base = api.uri(),
    ))
    .expect("config parses");
    let library = Arc::new(Library::open(config).await.expect("library opens"));

    let (tx, rx) = watch::channel(Arc::clone(&library));
    tokio::spawn(control::serve(
        socket.clone(),
        rx,
        control::Vitals::default(),
    ));
    // Wait for the socket to exist.
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Rig {
        library,
        socket,
        _tx: tx,
        _dir: dir,
    }
}

async fn connect(
    rig: &Rig,
) -> (
    tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    tokio::net::unix::OwnedWriteHalf,
) {
    let stream = UnixStream::connect(&rig.socket).await.expect("connects");
    let (reader, writer) = stream.into_split();
    (BufReader::new(reader).lines(), writer)
}

#[tokio::test]
async fn hello_snapshot_and_errors_over_raw_ndjson() {
    let api = MockServer::start().await;
    mount_show(&api, "test-show").await;
    let rig = rig(&api).await;
    rig.library
        .sync(&"test-show".parse::<ShowSlug>().expect("valid slug"))
        .await
        .expect("sync succeeds");

    // Socket is private to the user.
    let mode = std::fs::metadata(&rig.socket)
        .expect("socket exists")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "control socket must be 0600");

    let (mut lines, mut writer) = connect(&rig).await;
    let hello: ipc::Hello =
        serde_json::from_str(&lines.next_line().await.expect("io").expect("hello line"))
            .expect("hello parses");
    assert_eq!(hello.protocol_version, ipc::PROTOCOL_VERSION);

    // Garbage in: an error line out, connection stays usable.
    writer.write_all(b"not json\n").await.expect("writes");
    let reply = lines.next_line().await.expect("io").expect("error line");
    assert!(
        matches!(
            serde_json::from_str(&reply),
            Ok(ipc::Response::Error { .. })
        ),
        "unparseable request answered with an error: {reply}"
    );

    writer
        .write_all(b"{\"request\":\"snapshot\"}\n")
        .await
        .expect("writes");
    let reply = lines.next_line().await.expect("io").expect("snapshot line");
    let ipc::Response::Snapshot(snapshot) = serde_json::from_str(&reply).expect("parses") else {
        panic!("expected snapshot, got: {reply}");
    };
    assert_eq!(snapshot.shows.len(), 1);
    assert_eq!(snapshot.shows[0].slug.as_str(), "test-show");
    assert_eq!(snapshot.shows[0].last_poll_ok, Some(true));
    assert_eq!(snapshot.shows[0].episodes_cached, 1);
    assert!(snapshot.shows[0].cache_bytes > 0);
    assert!(snapshot.data_dir_bytes > 0, "data dir walked");
    assert!(snapshot.downloads.is_empty(), "nothing in flight");
}

#[tokio::test]
async fn subscribe_streams_sync_events() {
    let api = MockServer::start().await;
    mount_show(&api, "test-show").await;
    let rig = rig(&api).await;

    let (mut lines, mut writer) = connect(&rig).await;
    lines.next_line().await.expect("io").expect("hello");
    writer
        .write_all(b"{\"request\":\"subscribe\"}\n")
        .await
        .expect("writes");
    let first = lines.next_line().await.expect("io").expect("snapshot");
    assert!(
        matches!(serde_json::from_str(&first), Ok(ipc::Response::Snapshot(_))),
        "subscribe opens with a snapshot: {first}"
    );

    // A sync on the library side must flow to the subscriber.
    rig.library
        .sync(&"test-show".parse::<ShowSlug>().expect("valid slug"))
        .await
        .expect("sync succeeds");

    let mut kinds = Vec::new();
    while kinds.len() < 4 {
        let line = tokio::time::timeout(std::time::Duration::from_secs(5), lines.next_line())
            .await
            .expect("event arrives in time")
            .expect("io")
            .expect("stream open");
        if let Ok(ipc::Event::Known(event)) = serde_json::from_str(&line) {
            kinds.push(match event {
                ipc::KnownEvent::PollStarted { .. } => "poll_started",
                ipc::KnownEvent::PollFinished { ok: true, .. } => "poll_ok",
                ipc::KnownEvent::PollFinished { ok: false, .. } => "poll_failed",
                ipc::KnownEvent::EpisodeDiscovered { .. } => "discovered",
                ipc::KnownEvent::DownloadFinished { error: None, .. } => "downloaded",
                ipc::KnownEvent::DownloadFinished { error: Some(_), .. } => "download_failed",
                _ => "other",
            });
        }
    }
    assert_eq!(
        kinds,
        ["poll_started", "discovered", "downloaded", "poll_ok"],
        "the full sync story arrives in order"
    );
}
