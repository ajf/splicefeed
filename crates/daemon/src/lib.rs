//! Library half of the daemon crate: the HTTP server and the shared
//! status-report assembly, split out of the binary so integration tests
//! can drive them. The facade (`splicefeed`) stays free of `axum` — the
//! boundary rule applies to the library crates, and this one is the
//! binary's private support crate, not part of the public API.

pub mod report;
pub mod server;
