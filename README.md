# splicefeed

Self-hosted daemon that turns [DI.FM](https://www.di.fm) premium radio shows
into standard podcast RSS feeds any podcast app can subscribe to. Strictly a
personal-use LAN daemon for content you already pay for — see
[DESIGN.md](DESIGN.md) for architecture and decisions.

**Status: feature-complete.** Feeds validated end to end in Apple
Podcasts.

## How it works

The daemon polls each followed show on a jittered interval, downloads new
episodes (streamed to disk, blake3-verified, atomically renamed), applies
retention, and serves deterministic podcast RSS plus range-served media over
HTTP. State lives in one SQLite file; unparseable upstream responses are
quarantined so an API change never takes the feeds down.

## Quickstart

```sh
cargo build --workspace --release
mkdir -p ~/.config/splicefeed
cp config.example.toml ~/.config/splicefeed/config.toml   # edit: api_key + shows
target/release/splicefeed run --once   # first sync, watch it work
target/release/splicefeed run          # then serve for real
```

Subscribe to `http://<external_base_url>/feeds/<slug>.xml` in your podcast
app.

### Credentials

The **member API key** is required (`[auth.difm] api_key`, or the
`DIFM_API_KEY` env var): it authorizes the AudioAddict API, including
episode audio. Find it while logged in at di.fm — view the page source and
search for `api_key`. The premium *listen key* is optional and only kept
for legacy unsigned stream URLs.

## Commands

| Command | What it does |
|---|---|
| `run` | Daemon: jittered per-show polling + HTTP server, until SIGINT |
| `run --once` | Poll every show once and exit (cron-style; non-zero exit on failure) |
| `status [--format json]` | Library state from the database: files, hashes, sizes, poll health |
| `status --watch` | Live TUI; with a running daemon: vitals + event stream over the control socket |
| `verify [SLUG] [--fix]` | Check every cached file (existence, size, blake3); `--fix` re-downloads damage |
| `probe SLUG` | Hit the live API and report what parsed — the drift early-warning system |
| `completions SHELL` | Print a completion script for `fish`, `zsh`, `bash` (and friends) |
| `manpage [--out DIR]` | Write `splicefeed.1` plus a page per subcommand |

`kill -HUP <pid>` reloads the config without dropping the server: shows are
added/removed live, credentials rotate, intervals change. `bind`,
`data_dir`, and `control_socket` need a restart. A broken config is
rejected and the old one keeps serving.

## Config reference

Default path `~/.config/splicefeed/config.toml`; override with `--config`
or `SPLICEFEED_CONFIG`. See [config.example.toml](config.example.toml) for
a commented copy.

| Key | Default | Meaning |
|---|---|---|
| `bind` | `127.0.0.1:8380` | HTTP listen address. Exposing beyond loopback is your explicit choice. |
| `external_base_url` | `http://<bind>` | How your podcast app reaches the daemon; written into every feed URL. Set this. |
| `data_dir` | `~/.local/share/splicefeed` | SQLite state, audio, artwork, parse quarantine. |
| `poll_interval` | `30m` | Default per-show poll cadence (±10% jitter per cycle). |
| `download_concurrency` | `2` | Max simultaneous episode downloads, globally. |
| `fetch_last` | provider window (25) | Only consider each show's newest N episodes. Bounds what comes *in*. |
| `control_socket` | `$XDG_RUNTIME_DIR/splicefeed.sock` | NDJSON control socket for `status --watch` (0600). |
| `[retention] keep_last / max_gb` | unlimited | What *stays*; stricter wins. Widening later revives pruned episodes. |
| `[auth.difm] api_key` | — | **Required** member API key (`DIFM_API_KEY` overrides). |
| `[auth.difm] listen_key` | — | Optional legacy stream-host credential (`DIFM_LISTEN_KEY` overrides). |
| `[auth.difm] base_url` | DI.FM | Sibling AudioAddict networks (RadioTunes, JazzRadio, …). |
| `[telemetry.otlp] endpoint/interval/headers` | off | Push metrics to an OTLP collector. `/metrics` (Prometheus) is always served, pull-only. |
| `[[shows]] slug` | — | One block per show. Optional per-show: `title`, `poll_interval`, `fetch_last`, `artwork` (path or URL), `[shows.retention]`. |

## HTTP surface

`/feeds/<slug>.xml` · `/media/…` (range-served) · `/artwork/…` ·
`/healthz` · `/debug` (status report as JSON) · `/metrics` (Prometheus).

No TLS, no auth, by design: bind to loopback and put
[Caddy](https://caddyserver.com/) in front if you need either.

## Deployment

Ready-to-use units live in [packaging/](packaging/), install notes in each
file's header:

- **systemd**: `packaging/splicefeed.service` — hardened, dedicated user,
  `systemctl reload` = SIGHUP config reload.
- **Podman quadlet**: `packaging/Containerfile` +
  `packaging/splicefeed.container` — rootless container under systemd.
- **launchd (macOS)**: `packaging/io.splicefeed.daemon.plist`.

Shell completions and man pages come from the binary itself:

```sh
splicefeed completions fish > ~/.config/fish/completions/splicefeed.fish
splicefeed completions zsh  > "${fpath[1]}/_splicefeed"
splicefeed completions bash > /etc/bash_completion.d/splicefeed
splicefeed manpage --out /usr/local/share/man/man1 && mandb
```

## When DI.FM changes their API

The provider treats upstream as reverse-engineered and fragile; drift is
expected and survivable:

1. **Feeds keep serving.** Parse failures never take the daemon down —
   already-cached episodes stay in the feed.
2. **Look at the evidence.** `splicefeed probe <slug>` reports what still
   parses against the live API. Unparseable payloads are written to
   `<data_dir>/quarantine/difm/<timestamp>-<label>.json` — the raw JSON
   that broke, ready to diff against
   `crates/providers/tests/fixtures/audioaddict/`.
3. **Fix the wire types.** The tolerant parser lives in
   `crates/providers/src/difm/v1.rs`; every non-essential field is
   `Option`, unknown fields are ignored. Usually drift means one new
   field shape — update the type, add the quarantined payload as a
   fixture, and the wiremock tests in `crates/providers/tests/` lock it
   in.
4. **Auth changes**: `resolve_audio` fails loudly with a hint naming the
   credential it suspects. The API key and listen key are separate
   credentials (see DESIGN.md "DI.FM specifics" for the confirmed auth
   model and the experiments that established it).

## Development

```sh
just build   # or: cargo build --workspace
just test
just ci      # fmt check + clippy -D warnings + tests + library-boundary check
```

Layout: `crates/core` (domain, config, storage, download, retention, RSS,
IPC types) · `crates/providers` (Provider trait + DI.FM/AudioAddict) ·
`crates/splicefeed` (embeddable facade; `examples/sync_once.rs` is the
compile-tested API contract) · `crates/daemon` (the binary: axum server,
control socket, TUI, telemetry, CLI).
