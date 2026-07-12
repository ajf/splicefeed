# splicefeed

Self-hosted daemon that turns [DI.FM](https://www.di.fm) premium radio shows
into podcast RSS feeds for your LAN. It polls shows on a jittered schedule,
downloads and verifies episodes, applies retention, and serves feeds +
range-served media over HTTP. Personal use, for content you already pay
for. Architecture and decisions: [DESIGN.md](DESIGN.md).

## Quickstart

```sh
cargo build --workspace --release
mkdir -p ~/.config/splicefeed
cp config.example.toml ~/.config/splicefeed/config.toml   # edit: api_key + shows
target/release/splicefeed run
```

Subscribe to `http://<external_base_url>/feeds/<slug>.xml`, or import
`/subscriptions.opml` to add every show at once.

The one tricky bit is the **member API key** (not the di.fm "listen key"):
log in at di.fm, view page source, search for `api_key`.
[config.example.toml](config.example.toml) documents every setting and
walks through this step by step; verify with `splicefeed probe <slug>`.

## Commands

| Command | What it does |
|---|---|
| `run [--once]` | Daemon (poll + serve); `--once` polls everything and exits |
| `status [--watch] [--format json]` | Library state; `--watch` is a live TUI |
| `verify [SLUG] [--fix]` | Check cached files (size, blake3); `--fix` re-downloads |
| `probe SLUG` | Does the upstream API still parse? |
| `opml` | OPML subscription list to stdout |
| `completions SHELL` / `manpage` | Shell completions, man pages |

`kill -HUP` reloads the config live (`systemctl reload`); a broken config
is rejected and the old one keeps serving. `bind`, `data_dir`, and
`control_socket` need a restart.

## HTTP surface

`/feeds/<slug>.xml` · `/subscriptions.opml` · `/media/…` · `/artwork/…` ·
`/healthz` · `/debug` · `/metrics` — no TLS or auth; bind loopback and
front with Caddy if you need either.

## Deployment

Units in [packaging/](packaging/), install notes in each header: systemd
(hardened; `reload` = SIGHUP), Podman quadlet, launchd. Or run the
CI-published multi-arch container: `podman pull ghcr.io/ajf/splicefeed`.
Completions and man pages come from the binary:
`splicefeed completions fish > ~/.config/fish/completions/splicefeed.fish`.

## When DI.FM changes their API

Feeds keep serving — parse failures never take the daemon down. Run
`splicefeed probe <slug>`; broken payloads are quarantined under
`<data_dir>/quarantine/difm/`. Fix the tolerant wire types in
`crates/providers/src/difm/v1.rs` and add the payload as a test fixture.
Auth details live in DESIGN.md "DI.FM specifics".

## Development

`just ci` — fmt + clippy `-D warnings` + tests + library-boundary check.
Crates: `core` (domain/storage/RSS), `providers` (DI.FM), `splicefeed`
(embeddable facade), `daemon` (the binary).

## License

Apache-2.0 — see [LICENSE](LICENSE). Copyright 2026 Andrew Forgue.
