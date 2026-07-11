# splicefeed

Self-hosted daemon that turns [DI.FM](https://www.di.fm) premium radio shows
into standard podcast RSS feeds any podcast app can subscribe to. Strictly a
personal-use LAN daemon for content you already pay for — see
[DESIGN.md](DESIGN.md) for architecture and decisions.

**Status: skeleton (milestone 1 of 8).** Workspace, config schema, domain
types, provider abstraction, IPC protocol types, and CLI shell exist and are
tested; provider/network code lands in milestone 2. See DESIGN.md
"Milestones".

## Layout

- `crates/core` — domain types, config, IPC protocol (later: storage, RSS)
- `crates/providers` — `Provider` trait + DI.FM/AudioAddict implementation
- `crates/splicefeed` — the embeddable library facade
  (`examples/sync_once.rs` is the API contract)
- `crates/daemon` — the `splicefeed` binary: HTTP server, control socket,
  status TUI, CLI

## Quickstart (once functional)

```sh
cp config.example.toml ~/.config/splicefeed/config.toml   # edit it
splicefeed run            # daemon: poll on schedule, serve feeds
splicefeed run --once     # cron-style: poll everything once, exit
splicefeed status         # live TUI over the control socket
splicefeed probe <slug>   # check the upstream API still parses
```

Subscribe to `http://<external_base_url>/feeds/<slug>.xml`.

## Development

```sh
just build   # or: cargo build --workspace
just test
just ci      # lint + test + library-boundary check
```

A full config reference, deployment guides (systemd, Podman quadlet,
launchd), and the "when DI.FM changes their API" troubleshooting section
land with milestone 8.
