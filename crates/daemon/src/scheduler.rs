//! The poll scheduler: one jittered loop per show.
//!
//! Each configured show sleeps its own interval (per-show override or
//! the global default), ±10% jitter per cycle so polls never align into
//! a thundering herd against upstream. A global one-permit semaphore
//! serializes syncs — combined with the jitter, at most one poll runs at
//! any moment: the polite rate limit the design requires.
//!
//! The scheduler subscribes to the same watch channel a SIGHUP reload
//! swaps: on a new `Library` (or the sender closing at shutdown), show
//! loops finish their in-flight sync — never aborted mid-download — and
//! exit; the manager then rebuilds the loop set from the new config, so
//! added shows get polled, removed shows stop, and interval changes take
//! effect from the next cycle.

use std::sync::Arc;
use std::time::Duration;

use splicefeed::{Library, ShowSlug};
use tokio::sync::{Semaphore, watch};

/// Run poll loops until the library sender closes (daemon shutdown).
pub async fn run(mut library: watch::Receiver<Arc<Library>>) {
    loop {
        let current = library.borrow_and_update().clone();
        let polls = Arc::new(Semaphore::new(1));
        let loops: Vec<tokio::task::JoinHandle<()>> = current
            .config()
            .shows()
            .iter()
            .map(|show| {
                tokio::spawn(show_loop(
                    Arc::clone(&current),
                    show.slug().clone(),
                    show.poll_interval(current.config().poll_interval()),
                    Arc::clone(&polls),
                    library.clone(),
                ))
            })
            .collect();

        let closed = library.changed().await.is_err();
        // Loops notice the same change and exit after any in-flight
        // sync; waiting here guarantees one generation at a time.
        for handle in loops {
            handle.await.ok();
        }
        if closed {
            tracing::info!("scheduler stopped");
            return;
        }
        tracing::info!("scheduler restarted with reloaded configuration");
    }
}

async fn show_loop(
    library: Arc<Library>,
    slug: ShowSlug,
    interval: Duration,
    polls: Arc<Semaphore>,
    mut generation: watch::Receiver<Arc<Library>>,
) {
    tracing::debug!(show = %slug, ?interval, "poll loop started");
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval)) => {
                let _permit = polls
                    .acquire()
                    .await
                    .unwrap_or_else(|_| unreachable!("semaphore is never closed"));
                match library.sync(&slug).await {
                    Ok(report) => tracing::info!(
                        show = %slug,
                        discovered = report.discovered,
                        downloaded = report.downloaded,
                        pruned = report.pruned,
                        "scheduled poll complete"
                    ),
                    Err(err) => {
                        // Recorded in storage by sync(); the next cycle
                        // retries.
                        tracing::error!(show = %slug, error = %err, "scheduled poll failed");
                    }
                }
            }
            _ = generation.changed() => break,
        }
    }
    tracing::debug!(show = %slug, "poll loop stopped");
}

/// The interval with ±10% uniform jitter applied.
fn jittered(interval: Duration) -> Duration {
    interval.mul_f64(0.9 + fastrand::f64() * 0.2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_ten_percent() {
        let interval = Duration::from_secs(1800);
        let (lo, hi) = (interval.mul_f64(0.9), interval.mul_f64(1.1));
        let samples: Vec<Duration> = (0..1000).map(|_| jittered(interval)).collect();
        assert!(samples.iter().all(|d| (lo..=hi).contains(d)));
        // And it actually varies — a constant "jitter" defeats the point.
        assert!(samples.iter().any(|d| d != &samples[0]));
    }
}
