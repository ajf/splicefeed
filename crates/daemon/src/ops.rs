//! Operations shared between CLI modes and the signal handlers.

use anyhow::bail;
use splicefeed::Library;

/// Poll every configured show once. One failing show never stops the
/// others; any failure makes the result an error so `run --once` exits
/// non-zero for cron/systemd.
pub async fn sync_all_once(library: &Library) -> anyhow::Result<()> {
    let mut failed: Vec<&splicefeed::ShowSlug> = Vec::new();
    for show in library.config().shows() {
        let slug = show.slug();
        match library.sync(slug).await {
            Ok(report) => tracing::info!(
                show = %slug,
                discovered = report.discovered,
                downloaded = report.downloaded,
                pruned = report.pruned,
                "sync complete"
            ),
            Err(err) => {
                tracing::error!(show = %slug, error = %err, "sync failed");
                failed.push(slug);
            }
        }
    }
    if !failed.is_empty() {
        bail!(
            "sync failed for {}",
            failed
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}
