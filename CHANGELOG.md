# Changelog

## 0.1.0

First release — DI.FM premium shows as self-hosted podcast RSS feeds.
Validated in Apple Podcasts.

### Highlights

- Jittered per-show polling with conditional requests and a global rate limit
- Streaming, blake3-verified, atomically-written downloads
- Retention (keep-last-N / max-GB, global + per-show); pruned episodes revive on widen
- Deterministic RSS + range-served media; `/subscriptions.opml` export
- `SIGHUP` config reload with zero downtime
- `status --watch` live TUI over a unix control socket
- `verify [--fix]` cached-file integrity checks
- OpenTelemetry metrics: `/metrics` + optional OTLP
- Quarantine + `probe` for graceful upstream API drift
- Multi-arch container (amd64 + arm64), plus systemd / quadlet / launchd units

### Install

```sh
podman pull ghcr.io/ajf/splicefeed:0.1.0
```

Needs a DI.FM member API key — see `config.example.toml`. Apache-2.0.
