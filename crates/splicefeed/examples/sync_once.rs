//! Cron-style one-shot sync with no HTTP server: load config, sync every
//! configured show, write each feed to a file in the data directory.
//!
//! This example is the compile-tested contract of the public API
//! (DESIGN.md, "Public API contract"). Run with:
//!
//! ```sh
//! cargo run --example sync_once -- [path/to/config.toml]
//! ```

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use splicefeed::{Config, Library};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args().nth(1).map(PathBuf::from);
    let config = Config::load(config_path.as_deref())?;
    let library = Library::open(config).await?;

    let slugs: Vec<_> = library
        .config()
        .shows()
        .iter()
        .map(|show| show.slug().clone())
        .collect();
    let feed_dir = library.config().data_dir().join("feeds");
    std::fs::create_dir_all(&feed_dir)?;

    for slug in &slugs {
        let report = library.sync(slug).await?;
        println!(
            "{slug}: {} discovered, {} downloaded, {} pruned",
            report.discovered, report.downloaded, report.pruned
        );

        let path = feed_dir.join(format!("{slug}.xml"));
        let mut out = BufWriter::new(File::create(&path)?);
        library.write_feed(slug, &mut out).await?;
        println!("{slug}: feed written to {}", path.display());
    }
    Ok(())
}
