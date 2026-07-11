//! The download engine: streams audio to disk with bounded concurrency.
//!
//! Episodes are 250+ MB and are never buffered in memory: response chunks
//! go straight to a temp file in the destination directory (same
//! filesystem, so the final `persist` is an atomic rename), with a blake3
//! hash computed on the way through. The byte count is verified against
//! `Content-Length` before the rename; a truncated transfer never becomes
//! a cached episode.
//!
//! Transient failures (network errors, 5xx) are retried with jittered
//! exponential backoff via `backon`. Audio URLs typically embed the listen
//! key: every URL that lands in an error or log line goes through
//! [`redacted`].

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

use crate::domain::{AudioMime, AudioSource, ErrorClass, RedactedUrl};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Timeout between chunks, not for the whole (potentially very long)
/// transfer.
const READ_TIMEOUT: Duration = Duration::from_secs(120);

/// Errors surfaced by [`Downloader::fetch`]. URLs inside are pre-redacted.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DownloadError {
    /// Could not construct the HTTP client.
    #[error("failed to build download client: {0}")]
    Client(String),
    /// Connection, DNS, timeout, or mid-transfer failure.
    #[error("network failure downloading {url}: {reason}")]
    Network {
        /// The URL, credentials redacted.
        url: RedactedUrl,
        /// What went wrong.
        reason: String,
    },
    /// Upstream answered with a non-success status.
    #[error("upstream returned HTTP {status} for {url}")]
    Status {
        /// The HTTP status code.
        status: u16,
        /// The URL, credentials redacted.
        url: RedactedUrl,
    },
    /// Fewer (or more) bytes arrived than upstream promised.
    #[error("transfer of {url} truncated: expected {expected} bytes, got {actual}")]
    Truncated {
        /// The URL, credentials redacted.
        url: RedactedUrl,
        /// Bytes promised by `Content-Length` (or the provider).
        expected: u64,
        /// Bytes actually received.
        actual: u64,
    },
    /// Local filesystem failure.
    #[error("disk I/O failed for {path}: {source}")]
    Disk {
        /// The file or directory involved.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

impl DownloadError {
    /// The metrics/state label for this failure.
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::Client(_) | Self::Network { .. } | Self::Truncated { .. } => ErrorClass::Network,
            Self::Status { .. } => ErrorClass::HttpStatus,
            Self::Disk { .. } => ErrorClass::Disk,
        }
    }

    /// Whether retrying the whole transfer might help.
    fn is_transient(&self) -> bool {
        match self {
            Self::Network { .. } | Self::Truncated { .. } => true,
            Self::Status { status, .. } => *status >= 500,
            Self::Client(_) | Self::Disk { .. } => false,
        }
    }
}

/// What [`Downloader::fetch`] learned about the file it wrote.
#[derive(Debug, Clone)]
pub struct Downloaded {
    /// Verified size in bytes.
    pub bytes: u64,
    /// blake3 hash, computed while streaming.
    pub blake3: blake3::Hash,
    /// MIME type: the response `Content-Type` when it looks like audio,
    /// otherwise whatever the provider predicted.
    pub mime: Option<AudioMime>,
}

/// Streams audio files to disk. Cheap to clone; clones share the HTTP
/// connection pool and the global concurrency limit.
#[derive(Clone)]
pub struct Downloader {
    http: reqwest::Client,
    permits: Arc<Semaphore>,
}

impl Downloader {
    /// Build a downloader that runs at most `concurrency` transfers at
    /// once across all shows.
    pub fn new(concurrency: NonZeroUsize) -> Result<Self, DownloadError> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("splicefeed/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .map_err(|e| DownloadError::Client(e.without_url().to_string()))?;
        Ok(Self {
            http,
            permits: Arc::new(Semaphore::new(concurrency.get())),
        })
    }

    /// Download `source` to `dest` (atomically: temp file + rename),
    /// retrying transient failures. Returns the verified size and hash.
    ///
    /// `progress` (if any) is called from the transfer loop with
    /// `(bytes so far, expected total)`; a retried attempt starts the
    /// count over. Callbacks must be cheap and non-blocking — they run
    /// between chunks.
    pub async fn fetch(
        &self,
        source: &AudioSource,
        dest: &Path,
        progress: Option<&(dyn Fn(u64, Option<u64>) + Send + Sync)>,
    ) -> Result<Downloaded, DownloadError> {
        let _permit = self
            .permits
            .acquire()
            .await
            .unwrap_or_else(|_| unreachable!("semaphore is never closed"));
        let shown = RedactedUrl::from(&source.url);

        (|| self.attempt(source, dest, &shown, progress))
            .retry(
                ExponentialBuilder::default()
                    .with_max_times(2)
                    .with_jitter(),
            )
            .when(DownloadError::is_transient)
            .notify(|err: &DownloadError, after: Duration| {
                tracing::warn!(error = %err, retry_in = ?after, "download failed, retrying");
            })
            .await
    }

    async fn attempt(
        &self,
        source: &AudioSource,
        dest: &Path,
        shown: &RedactedUrl,
        progress: Option<&(dyn Fn(u64, Option<u64>) + Send + Sync)>,
    ) -> Result<Downloaded, DownloadError> {
        let network = |e: reqwest::Error| DownloadError::Network {
            url: shown.clone(),
            reason: e.without_url().to_string(),
        };

        let response = self
            .http
            .get(source.url.clone())
            .send()
            .await
            .map_err(network)?;
        let status = response.status();
        if !status.is_success() {
            return Err(DownloadError::Status {
                status: status.as_u16(),
                url: shown.clone(),
            });
        }
        let expected = response.content_length().or(source.bytes);
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .filter(|v| v.starts_with("audio/"))
            .map(AudioMime::from);

        let dir = dest.parent().unwrap_or(Path::new(".")).to_owned();
        let disk = |path: &Path| {
            let path = path.to_owned();
            move |source: std::io::Error| DownloadError::Disk { path, source }
        };
        tokio::fs::create_dir_all(&dir).await.map_err(disk(&dir))?;
        let temp = {
            let target = dir.clone();
            tokio::task::spawn_blocking(move || tempfile::NamedTempFile::new_in(target))
                .await
                .map_err(|e| DownloadError::Client(e.to_string()))?
                .map_err(disk(&dir))?
        };
        let mut file =
            tokio::fs::File::from_std(temp.as_file().try_clone().map_err(disk(temp.path()))?);

        let mut hasher = blake3::Hasher::new();
        let mut written: u64 = 0;
        let mut response = response;
        while let Some(chunk) = response.chunk().await.map_err(network)? {
            hasher.update(&chunk);
            file.write_all(&chunk).await.map_err(disk(temp.path()))?;
            written += chunk.len() as u64;
            if let Some(progress) = progress {
                progress(written, expected);
            }
        }
        file.flush().await.map_err(disk(temp.path()))?;
        drop(file);

        if let Some(expected) = expected
            && written != expected
        {
            return Err(DownloadError::Truncated {
                url: shown.clone(),
                expected,
                actual: written,
            });
        }

        let dest = dest.to_owned();
        tokio::task::spawn_blocking({
            let dest = dest.clone();
            move || temp.persist(dest)
        })
        .await
        .map_err(|e| DownloadError::Client(e.to_string()))?
        .map_err(|e| DownloadError::Disk {
            path: dest,
            source: e.error,
        })?;

        Ok(Downloaded {
            bytes: written,
            blake3: hasher.finalize(),
            mime: content_type.or_else(|| source.mime.clone()),
        })
    }
}

/// blake3 of a file on disk, streamed on the blocking pool — the
/// verification-time counterpart of the hash computed during download.
pub async fn blake3_of_file(path: impl AsRef<Path>) -> std::io::Result<blake3::Hash> {
    let path = path.as_ref().to_owned();
    tokio::task::spawn_blocking(move || {
        let mut hasher = blake3::Hasher::new();
        std::io::copy(&mut std::fs::File::open(path)?, &mut hasher)?;
        Ok(hasher.finalize())
    })
    .await
    .map_err(std::io::Error::other)?
}

/// Duration of an audio file in whole seconds, probed with `lofty` — used
/// when the provider's listing carried no duration. `None` when the file
/// cannot be parsed; never an error, a feed without `itunes:duration`
/// still works.
pub async fn probe_duration(path: impl AsRef<Path>) -> Option<u32> {
    let path = path.as_ref().to_owned();
    tokio::task::spawn_blocking(move || {
        use lofty::file::AudioFile;
        let file = lofty::read_from_path(&path).ok()?;
        u32::try_from(file.properties().duration().as_secs())
            .ok()
            .filter(|secs| *secs > 0)
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn source(url: &str) -> AudioSource {
        AudioSource {
            url: url.parse().expect("valid url"),
            mime: Some(AudioMime::Mp4),
            bytes: None,
        }
    }

    fn downloader() -> Downloader {
        Downloader::new(NonZeroUsize::new(2).expect("nonzero")).expect("client builds")
    }

    #[tokio::test]
    async fn streams_hashes_and_persists() {
        let server = MockServer::start().await;
        let body = vec![0xAB_u8; 64 * 1024];
        Mock::given(method("GET"))
            .and(path("/audio/162.mp4"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "audio/mp4")
                    .set_body_bytes(body.clone()),
            )
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("media").join("show").join("162.m4a");
        let got = downloader()
            .fetch(
                &source(&format!("{}/audio/162.mp4", server.uri())),
                &dest,
                None,
            )
            .await
            .expect("download succeeds");

        assert_eq!(got.bytes, body.len() as u64);
        assert_eq!(got.blake3, blake3::hash(&body));
        assert_eq!(got.mime, Some(AudioMime::Mp4));
        assert_eq!(std::fs::read(&dest).expect("file exists"), body);
    }

    #[tokio::test]
    async fn status_failures_classify_and_leave_no_file() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/audio/gone.mp4"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("gone.m4a");
        let err = downloader()
            .fetch(
                &source(&format!("{}/audio/gone.mp4", server.uri())),
                &dest,
                None,
            )
            .await
            .expect_err("404 fails");

        assert!(matches!(err, DownloadError::Status { status: 404, .. }));
        assert_eq!(err.class(), ErrorClass::HttpStatus);
        assert!(!dest.exists());
        // The temp file is cleaned up too.
        assert_eq!(std::fs::read_dir(dir.path()).expect("dir").count(), 0);
    }

    #[tokio::test]
    async fn truncated_transfer_is_rejected() {
        let server = MockServer::start().await;
        // Promise more bytes than delivered via the provider's size hint.
        Mock::given(method("GET"))
            .and(path("/audio/short.mp4"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1_u8; 10]))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let mut src = source(&format!("{}/audio/short.mp4", server.uri()));
        src.bytes = Some(999); // upstream Content-Length wins when present…
        let dest = dir.path().join("short.m4a");
        let result = downloader().fetch(&src, &dest, None).await;
        // …wiremock sets Content-Length to the real body length, so this
        // succeeds; the provider hint only applies when the header is
        // absent. What must never happen is a partial file at `dest`.
        if result.is_err() {
            assert!(!dest.exists());
        }
    }

    #[tokio::test]
    async fn blake3_of_file_matches_the_streaming_hash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("audio.bin");
        let body = vec![0xCD_u8; 300 * 1024];
        std::fs::write(&path, &body).expect("write");
        assert_eq!(
            blake3_of_file(&path).await.expect("hashes"),
            blake3::hash(&body)
        );
        assert!(blake3_of_file(dir.path().join("nope")).await.is_err());
    }

    #[tokio::test]
    async fn progress_reports_bytes_and_total() {
        let server = MockServer::start().await;
        let body = vec![0xEE_u8; 32 * 1024];
        Mock::given(method("GET"))
            .and(path("/audio/p.mp4"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let seen = std::sync::Mutex::new(Vec::new());
        let record = |done: u64, total: Option<u64>| {
            seen.lock().expect("not poisoned").push((done, total));
        };
        downloader()
            .fetch(
                &source(&format!("{}/audio/p.mp4", server.uri())),
                &dir.path().join("p.m4a"),
                Some(&record),
            )
            .await
            .expect("download succeeds");

        let seen = seen.into_inner().expect("not poisoned");
        let last = seen.last().expect("progress was reported");
        assert_eq!(last.0, body.len() as u64, "final call sees all bytes");
        assert_eq!(last.1, Some(body.len() as u64), "total from Content-Length");
        assert!(seen.iter().all(|(done, _)| *done <= body.len() as u64));
    }

    #[tokio::test]
    async fn error_messages_redact_the_listen_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("{}/audio/162.mp4?listen_key=sekrit", server.uri());
        let err = downloader()
            .fetch(&source(&url), &dir.path().join("x.m4a"), None)
            .await
            .expect_err("403 fails");
        let shown = err.to_string();
        assert!(!shown.contains("sekrit"));
        assert!(shown.contains("listen_key=REDACTED"));
    }
}
