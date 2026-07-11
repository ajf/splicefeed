# splicefeed — Design

A self-hosted daemon that turns DI.FM premium radio shows into standard podcast
RSS feeds served on the local network. Personal use only: it consumes content
the operator already pays for and rehosts it on their own LAN.

## Data flow

```
                 poll (scheduler, per-show interval + jitter)
                          │
  DI.FM / AudioAddict API │  tolerant parse ──► quarantine dir on failure
                          ▼
                    Provider (trait)
                          │  new episodes
                          ▼
                    Downloader ── streams to disk (tmp + rename), blake3
                          │
                          ▼
                 SQLite state + data dir ◄── retention/pruning
                          │
            ┌─────────────┼──────────────┐
            ▼             ▼              ▼
      RSS generator   axum HTTP      unix socket IPC
      (deterministic) /feeds /media  (NDJSON) ──► `splicefeed status` TUI
                      /artwork
                      /healthz /metrics
```

## Workspace layout

| Crate | Kind | Contents |
|---|---|---|
| `crates/core` | lib | domain types, config, storage (SQLite), download engine, retention, RSS generation, artwork cache, IPC message types, instrumentation facades |
| `crates/providers` | lib | `Provider` trait, `difm` (AudioAddict) implementation, tolerant parsing + quarantine |
| `crates/splicefeed` | lib (facade) | re-exports the public API of `core` + `providers`; the crate downstream users depend on; ships `examples/sync_once.rs` |
| `crates/daemon` | bin | axum server, unix-socket control server, ratatui `status` TUI, OTLP/Prometheus wiring, clap CLI, signal handling |

**Boundary rule (enforced in CI):** the library crates must build with no
`axum`, `ratatui`, `crossterm`, `clap`, or exporter crates in their dependency
tree (`cargo tree -p splicefeed` is checked against a denylist, and the facade
is built in isolation). The libs *record* telemetry via `tracing` and metrics
facades; only the binary *exports*.

The binary output is a single self-contained `splicefeed` executable
(rusqlite `bundled`, rustls — no system libs). Targets: macOS aarch64, Linux
x86_64/aarch64, plus optional `x86_64-unknown-linux-musl` static build.

## Decisions

Recorded per the spec's "when unsure, ask" rule; all four contested choices
were reviewed and approved before implementation.

| Decision | Choice | Rationale |
|---|---|---|
| SQLite driver | `rusqlite` (bundled) | Write volume is tiny; sqlx's async adds a runtime dance and compile cost for nothing here. Access is wrapped in a small `spawn_blocking` layer inside `core::storage`. Friendlier to the musl static build. |
| Dates | `jiff` | Actively developed, sane API, RFC 2822 formatting built in (`jiff::fmt::rfc2822`) which is exactly what feeds need. |
| RSS XML | hand-written via `quick-xml` writer | **Deliberate hand-roll.** The spec demands byte-identical regeneration; owning every byte (element order, attribute order, no `lastBuildDate`) is simpler than auditing the `rss` crate's output stability across versions. The itunes namespace surface we emit is small. This is one of the two sanctioned hand-rolls (see below). |
| IPC framing | newline-delimited JSON | Debuggable with `socat`/`jq`; framing efficiency is irrelevant on a local control socket. `postcard` rejected. |
| Metrics | `opentelemetry`/`opentelemetry_sdk` as the single registry; OTLP via `PeriodicReader` (off by default), Prometheus `/metrics` via the `opentelemetry-prometheus` reader | One source of truth, as required. Known risk: the prometheus bridge crate has historically lagged the SDK; versions are pinned together in the workspace and telemetry lands in the last milestone, so if the bridge is incompatible at that point we re-evaluate (fallback: OTel SDK + a thin manual encoder over its in-memory reader). The TUI reads from the same in-process state/instrumentation layer — no parallel bookkeeping. |
| Config | `figment` (TOML file + `SPLICEFEED_CONFIG`/`DIFM_LISTEN_KEY` env overrides, env wins) | Exactly the layering semantics required, without hand-rolling precedence. |
| HTTP client | `reqwest` (rustls, streaming) | Standard. |
| Retries/backoff | `backon` | Small, maintained, async-native. |
| Paths | `etcetera` | XDG + macOS conventions. |
| Duration probing | `lofty` | Read duration/tags from downloaded files when the API doesn't provide them; never parse MP3 frames by hand. |
| Secrets | `secrecy` for the listen key; `ListenKey` newtype wraps `SecretString` with a redacting `Debug`/`Display` | Key must never appear in logs; URL logging goes through a redaction helper. |
| Checksums | `blake3` | Hash while streaming to disk; stored in SQLite. Upstream gives us nothing better than `Content-Length`, so our own hash is the integrity anchor for retention checks and re-download decisions. |
| Atomic writes | `tempfile` in the destination directory + `persist()` | Same-filesystem rename. |

**Sanctioned hand-rolls** (no maintained crate fits): the DI.FM/AudioAddict
wire types, the IPC protocol enums, and the RSS writer (rationale above).
Everything else comes from the crates listed in the spec's mapping.

## Domain model (`crates/core`)

- Newtypes: `ShowSlug(String)`, `EpisodeId(String)`, `ListenKey(SecretString)`.
  Conversions via `FromStr`/`TryFrom`/`Display` impls, not helper functions.
- Episode lifecycle is an enum with methods, matched exhaustively:
  `Discovered → Downloading → Cached → Pruned` (+ `Failed { class }`).
  No boolean flag clusters.
- Daemon mode: `enum Mode { Once, Serve }`.
- Wire types (provider crate) convert into domain types via `TryFrom` at the
  provider boundary; domain types are owned; APIs borrow
  (`&self`, `&ShowSlug`, `impl AsRef<Path>`).

## Provider abstraction (`crates/providers`)

```rust
trait Provider: Send + Sync {
    async fn show(&self, slug: &ShowSlug) -> Result<ShowMeta, ProviderError>;
    async fn episodes(&self, slug: &ShowSlug) -> Result<Vec<EpisodeMeta>, ProviderError>;
    async fn resolve_audio(&self, show: &ShowSlug, ep: &EpisodeId)
        -> Result<AudioSource, ProviderError>;
    async fn artwork(&self, slug: &ShowSlug) -> Result<Option<Url>, ProviderError>;
}
```

(`resolve_audio` takes the show too: AudioAddict addresses episodes by
`<show-slug>/<episode-slug>`, and the sync engine always knows the show.
`EpisodeId` wraps the provider's episode slug; feed GUIDs are
`difm/<show>/<episode-slug>`.)

- `async fn` in trait is not object-safe, and the registry holds
  `Arc<dyn Provider>` keyed by the TOML `provider = "..."` string — so the
  trait uses `#[async_trait]`.
- Construction goes through a `ProviderFactory` trait (provider builds itself
  from its own config/auth block); the registry maps name → factory → instance.
- Adding a provider = new trait impl + one registry entry. Scheduler,
  downloader, storage, RSS, and server code never change.

### DI.FM specifics — treated as reverse-engineered and fragile

Confirmed empirically 2026-07-11 against the live API (fixtures captured in
`crates/providers/tests/fixtures/audioaddict/`, exercised by `wiremock`
tests):

- Base: `https://api.audioaddict.com/v1/di/` — the AudioAddict API is used,
  not the di.fm website (`www.di.fm/shows/<slug>` 403s/404s for non-browser
  clients; the API serves JSON cleanly and generalizes across AudioAddict
  networks via the `/v1/<network>/` path segment).
- `GET shows/<slug>` — show metadata, **no auth required**. Image URLs are
  protocol-relative RFC 6570 templates
  (`//cdn-images…/x.png{?size,height,…}`) — template stripped, https forced.
- `GET shows/<slug>/episodes?page=N&per_page=M` — newest first; pagination
  via RFC 5988 `Link` headers (`rel="next"/"last"`). No auth required.
  `tracks[0].length` is the duration in seconds; `start_at` is RFC 3339
  with offset; the episode `slug` (e.g. `162`) is the addressable id.
- `GET shows/<slug>/episodes/<episode-slug>` — single episode.
- **Still UNCONFIRMED (needs a real listen key):** the audio asset shape.
  Unauthenticated, `tracks[].content` is `{}` and `tracks[].asset_url`
  points at *artwork*, not audio. `resolve_audio` currently sends
  `?listen_key=` on the single-episode endpoint (the historically known
  mechanism) and fails loudly with a hint if no asset appears; `probe`
  against the live API with a real key is the verification step, and the
  parser must not trust `asset_url` without an audio-looking extension.

### Resilience to drift (first-class)

- serde: `deny_unknown_fields` never used; every non-essential field is
  `Option`; custom deserializers for inconsistent formats (dates especially).
  `#[serde(borrow)]` on hot paths where the payload lifetime allows.
- A versioned parser layer (`difm::parse::v1`) isolates wire → domain
  conversion. Parse failure ⇒ raw payload written to
  `<data_dir>/quarantine/<provider>/<timestamp>.json` + structured error log +
  daemon keeps running and keeps serving previously fetched episodes. A schema
  change must never take down the feeds.
- `splicefeed probe <slug>` hits the live API, reports what parsed and what
  didn't, and diffs against fixture expectations — the early-warning system.

## Storage (`core::storage`)

SQLite via rusqlite (bundled), one DB in the data dir. Sketch:

```sql
shows    (slug PK, provider, title, artwork_path, last_poll_at, last_poll_ok, last_error)
episodes (provider_episode_id PK, show_slug FK, title, description,
          published_at, duration_secs, state, file_path, bytes, blake3,
          mime, discovered_at, downloaded_at)
```

GUIDs derive from `provider_episode_id` — never file paths. Retention
(keep-last-N and/or max-GB, global default + per-show override) prunes files
and flips state to `Pruned`; pruned rows are kept so episodes don't reappear
as "new".

## Downloader

- Polls per-show on its interval **with jitter**, conditional requests
  (`If-Modified-Since`/`ETag`) where upstream supports them, and a global
  polite rate limit — this is someone else's infrastructure.
- Streams response body straight to disk (`AsyncWrite`), hashing with blake3
  on the way through; episodes are 250+ MB and are never buffered in memory.
  `bytes::Bytes` chunks are shared between writer and hasher.
- Temp file + rename (atomic), verified against `Content-Length`, recorded in
  SQLite with hash.
- Bounded concurrency (config), queue depth and per-download progress exposed
  through the shared state layer the IPC/TUI reads.

## RSS (`core::rss`)

Podcast RSS 2.0 + `itunes:` namespace, written with `quick-xml` into a
reusable buffer. Determinism rules:

- **No `lastBuildDate`** — the spec's byte-identical requirement quietly
  forbids it; omitted deliberately.
- Fixed element/attribute emission order; episodes sorted by
  (`published_at` DESC, `provider_episode_id`) as a total order.
- `pubDate` RFC 2822 via jiff; `enclosure` carries exact byte length and MIME
  from storage; `itunes:duration` when derivable (API or lofty probe).
- Enclosure URLs are built from the configured **external base URL**, never
  the bind address. The listen key never appears anywhere in a feed.
- Tests validate output with a strict feed parser and assert byte-identical
  regeneration.

## HTTP server (binary, axum)

- `/feeds/<show>.xml` — generated from storage, deterministic
- `/media/<show>/<file>` — via `tower-http` `ServeFile`-style streamed
  responses: correct `Content-Type`/`Content-Length` and **range support**
  (podcast apps require it for scrubbing); never read-into-memory
- `/artwork/<show>.<ext>` — cached to disk at sync time; TOML override
  (local path or URL) beats provider artwork
- `/healthz` — liveness + last-successful-poll per show
- `/metrics` — Prometheus scrape (same OTel registry)

Binds loopback by default; exposing wider is an explicit config choice.
Optional single static bearer token / secret path prefix when LAN-bound.
No TLS (Caddy in front if needed), no privileged ports, no root.

## IPC + status TUI

- Unix socket at `$XDG_RUNTIME_DIR/splicefeed.sock` (macOS: under the data
  dir), mode `0600`, path configurable.
- Versioned, typed protocol: NDJSON-framed request/response for snapshots plus
  a subscription stream for events. Message enums live in `crates/core::ipc`
  so daemon and TUI cannot drift.
- Forward compatibility: `#[serde(other)]` only works on unit variants, so
  data-carrying event enums get an explicit
  `#[serde(untagged)] Unknown(serde_json::Value)` tail variant — unknown
  events from a newer daemon are ignored, never fatal. Request verbs are
  designed so `poll-now <show>` / `prune <show>` can be added without breaking
  older clients.
- TUI (`ratatui` + `crossterm`): shows table (last/next poll, episode count,
  cache size, last error), active downloads (progress/throughput/ETA,
  concurrency vs limit, queue depth), rolling event log, daemon vitals.
  Daemon not running ⇒ clear message, not a panic.
- All numbers come from the same in-process instrumentation/state layer the
  metrics use — one source of truth.

## Telemetry

Disabled by default; nothing leaves the box unless configured. OTLP endpoint/
headers/interval in TOML. Counters, histograms, and gauges exactly as listed
in the spec, labeled by `show`/`provider`; OTel semantic conventions where
they exist (`http.server.request.duration`), `splicefeed.*` namespace
otherwise. `tracing` + `tracing-subscriber` (env-filter) + optional
`tracing-opentelemetry`. `tokio-metrics` for runtime vitals if practical.

## Error handling

`thiserror` in the libraries (typed errors per module: `ProviderError`,
`StorageError`, …), `anyhow` at the binary boundary. No `unwrap()` outside
tests. Download errors classified (`network` / `http-status` / `parse` /
`disk`) — the same classes label the error metrics.

## Public API contract

The facade crate makes the cron/embedded use case one-liner-ish, with no HTTP
server involved:

```rust
let config = splicefeed::Config::load(path)?;
let lib = splicefeed::Library::open(&config).await?;
lib.sync(&show_slug).await?;
lib.write_feed(&show_slug, &mut out)?;
```

`examples/sync_once.rs` demonstrates exactly this and is compile-tested in CI
as the contract of the public API. `#![deny(missing_docs)]` on all lib crates;
`pub` only what the binary and examples need.

## Milestones

1. **Skeleton** — workspace, config schema, domain types, CLI shell. *(done)*
2. **Provider** — confirm DI.FM/AudioAddict endpoints empirically (needs
   listen key), capture fixtures, implement `difm` + quarantine + `probe`.
3. **Storage + downloader** — SQLite state, streaming downloads, retention.
4. **RSS + server** — feed generation, axum routes, range-served media.
   **← usable milestone: feeds work in a real podcast app here.**
5. **Scheduler + daemon** — jittered polling, `--once`, graceful shutdown.
6. **IPC + TUI** — socket protocol, `splicefeed status`.
7. **Telemetry** — OTel/OTLP/Prometheus wiring (bridge risk re-checked here).
8. **Packaging** — systemd unit, Podman quadlet, launchd plist, musl build,
   README (config reference, deployment, "when DI.FM changes their API").

## Non-goals

No multi-user, no accounts, no TLS termination, no transcoding, no cloud
anything, no HTML scraping where a JSON endpoint exists.
