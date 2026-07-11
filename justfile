# splicefeed build/dev tasks. https://github.com/casey/just

default: build

# Debug build of the whole workspace.
build:
    cargo build --workspace

# Run all tests.
test:
    cargo test --workspace

# Format check + clippy with warnings denied.
lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings

# Enforce the library/binary boundary (DESIGN.md "Workspace layout"):
# the facade lib must build in isolation and must not pull in server/TUI/
# CLI/exporter crates. Run in CI.
check-boundary:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -p splicefeed
    banned='axum|ratatui|crossterm|clap|opentelemetry'
    if cargo tree -p splicefeed --edges normal | grep -E "^\S*($banned)"; then
        echo "ERROR: banned crate in the splicefeed library dependency tree" >&2
        exit 1
    fi
    echo "boundary OK: no banned crates in the library tree"

# Release build for the host platform (single self-contained binary).
release:
    cargo build --workspace --release

# Static Linux binary (requires: rustup target add x86_64-unknown-linux-musl).
release-musl:
    cargo build -p splicefeed-daemon --release --target x86_64-unknown-linux-musl

# Release build for Apple Silicon; run on a macOS host.
release-macos:
    cargo build -p splicefeed-daemon --release --target aarch64-apple-darwin

# Everything CI runs.
ci: lint test check-boundary
