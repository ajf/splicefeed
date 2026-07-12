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
| Config | `figment` (TOML file + `SPLICEFEED_CONFIG`/`DIFM_API_KEY`/`DIFM_LISTEN_KEY` env overrides, env wins) | Exactly the layering semantics required, without hand-rolling precedence. |
| HTTP client | `reqwest` (rustls, streaming) | Standard. |
| Retries/backoff | `backon` | Small, maintained, async-native. |
| Paths | `etcetera` | XDG + macOS conventions. |
| Duration probing | `lofty` | Read duration/tags from downloaded files when the API doesn't provide them; never parse MP3 frames by hand. |
| Secrets | `secrecy` for the member API key (required) and listen key (optional); `ApiKey`/`ListenKey` newtypes wrap `SecretString` with redacting `Debug` | Keys must never appear in logs; URL logging goes through a redaction helper that also masks `api_key`/`audio_token`/`listen_key` query params. |
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
  Variants measured live 2026-07-12: `images.default` aliases the
  *horizontal banner* (940×374) — wrong shape for podcast cover art;
  `images.compact` is the 1400×1400 square (Apple's minimum spec) and is
  what feeds use, `default` as fallback. Cached artwork file names are
  keyed to the source URL so a variant/upstream change refetches instead
  of skipping as already-cached.
- `GET shows/<slug>/episodes?page=N&per_page=M` — newest first; pagination
  via RFC 5988 `Link` headers (`rel="next"/"last"`). No auth required.
  `tracks[0].length` is the duration in seconds; `start_at` is RFC 3339
  with offset; the episode `slug` (e.g. `162`) is the addressable id.
- `GET shows/<slug>/episodes/<episode-slug>` — single episode.
- **Confirmed 2026-07-11 with a real premium listen key: the listen key
  does NOT unlock audio assets in the API.** Tested against the live
  single-episode and `tracks/<id>` endpoints: `?listen_key=` is silently
  ignored (200, `tracks[].content` stays `{}`), and HTTP basic auth (all
  arrangements) and an `X-Listen-Key` header do no better. `?api_key=` is
  a recognized parameter — bogus values get 403 "Invalid API Key" — and
  takes the *member API key*, a separate credential (found in the
  logged-in di.fm page source; `[auth.difm] api_key` / `DIFM_API_KEY`).
  The API key is therefore the **required** difm credential; the listen
  key is optional and only ever appended to a legacy unsigned stream-host
  audio URL (none observed since the signed playback URLs).
- **Confirmed 2026-07-11 with a real member API key:** an authenticated
  episode carries `tracks[].content.assets[].url` — a signed, short-lived
  playback URL on `content.audioaddict.com` (`audio_token`, `member_id`,
  `exp` ~24h out, and an `auth` HMAC over the query string; MP3,
  range-served, HTTP 206). It authorizes itself, so `resolve_audio` does
  **not** append the listen key — that invalidates the signature (verified:
  403). The append survives only for a bare stream-host URL with no
  signature of its own. Since the URL expires, audio is resolved right
  before each download, never cached. The tolerant parser
  (`content.assets[].url`, `content.url`, extension-gated `asset_url`)
  needed no change — the confirmed shape is the one it already handled.

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
shows    (slug PK, provider, title, description, artwork_path,
          last_poll_at, last_poll_ok, last_error)
episodes (show_slug FK + provider_episode_id → composite PK, title,
          description, published_at, duration_secs, state, error_class,
          file_path, bytes, blake3, mime, discovered_at, downloaded_at)
```

(The composite key is deliberate: AudioAddict episode slugs like `162` are
only unique within a show.) GUIDs derive from `provider_episode_id` —
never file paths. Retention
(keep-last-N and/or max-GB, global default + per-show override) prunes files
and flips state to `Pruned`; pruned rows are kept so episodes don't reappear
as "new" — discovery never resurrects a tombstone. Widening retention does:
the sync engine plans retention over the listing *before* downloading
(projected sizes: recorded bytes where known, tombstones keep theirs), so
an episode that would be pruned right back out is never fetched, and a
tombstone that fits the widened window is revived (`Pruned → Downloading`)
and re-downloaded. The byte cap never evicts the newest cached episode on
its own — one oversized file must not leave a feed empty or churn through
download-then-prune every poll.

## Downloader

- Discovery is bounded by `fetch_last` (global default + per-show
  override, optional): only the newest N episodes are listed — the limit
  is passed upstream as `per_page`, so a small window is also a small
  request — and only episodes present in the current listing are
  downloaded or retried. Old rows stay put, and an episode upstream
  dropped can't retry forever. Within the listing, only episodes the
  retention policy will keep are fetched at all (see Storage). Distinct
  from retention: `fetch_last` bounds what comes in, `keep_last`/`max_gb`
  bound what stays.
- Polls per-show on its interval **with ±10% jitter** per cycle, and a
  global one-permit semaphore serializes syncs — at most one poll runs
  at any moment: the polite rate limit; this is someone else's
  infrastructure. A stale `downloading` row (no progress for 10+
  minutes — a crashed process's leftover) is healed to retryable
  `failed` at the start of each sync.
- Conditional listings: the previous poll's `ETag` is stored per show
  (schema v3) and sent as `If-None-Match`; a 304 skips discovery and the
  show-metadata request entirely — the stored window still drives
  retries and retention. **Reality check (measured 2026-07-12):**
  AudioAddict sends weak ETags but identical requests return different
  bodies (volatile vote-tracking fields) and therefore different
  validators, so upstream never actually answers 304. The mechanism is
  tested, costs one header, and stays for the day upstream fixes it or
  a provider that honors validators.
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
- `/debug` — the `status` CLI report as JSON (operator request)
- `/metrics` — Prometheus scrape (same OTel registry; milestone 7)

Binds loopback by default; exposing wider is an explicit config choice.
No TLS and no auth of any kind — operator decision 2026-07-11: Caddy
fronts anything that needs either. No privileged ports, no root.

**SIGHUP reloads the config.** The serving daemon holds its `Library`
behind a `tokio::sync::watch` channel; a reload builds a fresh one
(storage handle shared, download engine shared unless the concurrency
changed, providers rebuilt) and swaps it in, then syncs so added shows
materialize. A failed reload — bad TOML, invalid config, or a
restart-only change — logs and leaves the old config serving; `bind`
and `data_dir` are restart-only (listener and media routes captured
them at startup). The milestone-5 scheduler subscribes to the same
channel. systemd gets `ExecReload=kill -HUP $MAINPID` in milestone 8.

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
lib.write_feed(&show_slug, &mut out).await?;
```

`examples/sync_once.rs` demonstrates exactly this and is compile-tested in CI
as the contract of the public API. Embedders whose settings don't live in a
file the library can read use `Config::from_toml_str` — identical env
layering, defaults, and validation, no filesystem involved. `#![deny(missing_docs)]` on all lib crates;
`pub` only what the binary and examples need.

## Milestones

1. **Skeleton** — workspace, config schema, domain types, CLI shell. *(done)*
2. **Provider** — confirm DI.FM/AudioAddict endpoints empirically (needs
   listen key), capture fixtures, implement `difm` + quarantine + `probe`.
   *(done — except the audio-asset shape, still blocked on a real listen
   key; failed episodes are recorded and retried each sync, so it can be
   confirmed with `probe` at any time without further code changes)*
3. **Storage + downloader** — SQLite state, streaming downloads, retention.
   *(done — `run --once` works end to end)*
4. **RSS + server** — feed generation, axum routes, range-served media.
   **← usable milestone: feeds work in a real podcast app here.**
   *(done — validated end to end in Apple Podcasts, 2026-07-12;
   /feeds, /media (range-served), /artwork, /healthz, /debug;
   artwork cached at sync time. `run` currently syncs once at startup
   then serves; the jittered scheduler is milestone 5. `write_feed`
   became `async` — it reads storage.)*
5. **Scheduler + daemon** — jittered polling, `--once`, graceful shutdown.
   *(done — per-show poll loops with ±10% jitter, serialized as the
   polite rate limit, subscribed to the reload channel so SIGHUP swaps
   the poll set live; conditional listings with per-show ETags; stale
   `downloading` rows healed at sync start. `--once`, graceful
   shutdown, and SIGHUP reload had shipped earlier.)*
6. **IPC + TUI** — socket protocol, live `splicefeed status` TUI. (Much
   of this shipped early: plain-text/JSON `status` reading the database
   directly (milestone 3), `verify [SLUG] [--fix]` — existence, size,
   and blake3 of every cached file, re-downloading damage on `--fix` —
   and a ratatui `status --watch` view (alongside milestone 4) that
   re-reads the database every 2s: shows table, per-show episode table,
   poll health. Milestone 6 proper is the control socket: it feeds this
   same view the daemon's in-process state — in-flight downloads,
   throughput, live events — which does not exist until the scheduler
   runs in-daemon.)
7. **Telemetry** — OTel/OTLP/Prometheus wiring (bridge risk re-checked here).
8. **Packaging** — systemd unit, Podman quadlet, launchd plist, musl build,
   README (config reference, deployment, "when DI.FM changes their API").

## Non-goals

No multi-user, no accounts, no TLS termination, no transcoding, no cloud
anything, no HTML scraping where a JSON endpoint exists.
