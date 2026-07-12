//! Core library for splicefeed: domain types, configuration, and the IPC
//! protocol shared between the daemon and the status TUI.
//!
//! This crate must never depend on `axum`, `ratatui`, `crossterm`, `clap`,
//! or telemetry exporter crates — it records via facades, the binary
//! exports. See `DESIGN.md` ("Workspace layout") for the boundary rule.
//!
#![deny(missing_docs)]

pub mod config;
pub mod domain;
pub mod download;
pub mod ipc;
pub mod opml;
pub mod retention;
pub mod rss;
pub mod storage;
