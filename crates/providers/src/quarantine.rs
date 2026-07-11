//! Quarantine for unparseable upstream payloads.
//!
//! When a provider response fails to parse, the raw payload is written
//! here with a clear name and the daemon keeps running on previously
//! fetched data — a schema change must never take down the feeds. The
//! `splicefeed probe` subcommand and the quarantine directory together are
//! the early-warning system for upstream API drift.

use std::io;
use std::path::{Path, PathBuf};

/// Writes raw payloads into a per-provider quarantine directory.
#[derive(Debug, Clone)]
pub struct Quarantine {
    dir: PathBuf,
}

impl Quarantine {
    /// A quarantine rooted at `dir` (created lazily on first write).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// The quarantine directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Write one payload under `<dir>/<timestamp>-<label>.json` and return
    /// the path. `label` says what was being parsed (e.g. `episodes-page1`).
    pub fn write(&self, label: &str, payload: &str) -> io::Result<PathBuf> {
        std::fs::create_dir_all(&self.dir)?;
        let timestamp = jiff::Timestamp::now().strftime("%Y%m%dT%H%M%S%.3fZ");
        let path = self.dir.join(format!("{timestamp}-{label}.json"));
        std::fs::write(&path, payload)?;
        Ok(path)
    }

    /// Like [`write`](Self::write), but downgrades I/O failures to a
    /// logged `None` — quarantining must never introduce a second failure
    /// mode into the parse path.
    pub fn write_or_note(&self, label: &str, payload: &str) -> Option<PathBuf> {
        match self.write(label, payload) {
            Ok(path) => Some(path),
            Err(err) => {
                tracing::error!(%err, label, "failed to write quarantine file");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_payload_with_label() {
        let dir =
            std::env::temp_dir().join(format!("splicefeed-quarantine-{}", std::process::id()));
        let quarantine = Quarantine::new(&dir);
        let path = quarantine
            .write("episodes-test", r#"{"weird": true}"#)
            .expect("write succeeds");
        assert!(
            path.file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with("-episodes-test.json"))
        );
        assert_eq!(
            std::fs::read_to_string(&path).expect("readable"),
            r#"{"weird": true}"#
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
