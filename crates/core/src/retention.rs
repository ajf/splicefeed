//! Retention planning: which cached episodes to prune under a policy.
//!
//! Pure decision logic — the caller (the sync engine) deletes files and
//! flips states. Both limits of a [`Retention`] policy may be set; walking
//! newest → oldest, an episode is kept only while it satisfies *both*, so
//! the stricter limit wins.

use crate::config::Retention;
use crate::domain::EpisodeId;

/// One cached episode as retention sees it.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// The episode.
    pub id: EpisodeId,
    /// Size of its file on disk.
    pub bytes: u64,
}

/// Given the cached episodes of one show, **newest first** (the storage
/// ordering), return the ids to prune, oldest-first.
pub fn plan(policy: &Retention, newest_first: &[Candidate]) -> Vec<EpisodeId> {
    let mut kept: u32 = 0;
    let mut kept_bytes: u64 = 0;
    let mut doomed = Vec::new();
    for candidate in newest_first {
        let over_count = policy.keep_last().is_some_and(|n| kept >= n);
        // The byte cap never evicts the newest kept episode on its own:
        // one oversized file must not leave the feed empty (or churn
        // through download-then-prune every poll).
        let over_bytes = kept > 0
            && policy
                .max_bytes()
                .is_some_and(|max| kept_bytes + candidate.bytes > max);
        if over_count || over_bytes {
            doomed.push(candidate.id.clone());
        } else {
            kept += 1;
            kept_bytes += candidate.bytes;
        }
    }
    doomed.reverse();
    doomed
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Figment;
    use figment::providers::{Format, Toml};

    fn policy(toml: &str) -> Retention {
        Figment::new()
            .merge(Toml::string(toml))
            .extract()
            .expect("valid retention")
    }

    fn candidates(sizes: &[(u32, u64)]) -> Vec<Candidate> {
        sizes
            .iter()
            .map(|(id, bytes)| Candidate {
                id: id.to_string().parse().expect("valid id"),
                bytes: *bytes,
            })
            .collect()
    }

    fn ids(doomed: &[EpisodeId]) -> Vec<&str> {
        doomed.iter().map(|id| id.as_str()).collect()
    }

    #[test]
    fn no_limits_prunes_nothing() {
        let cached = candidates(&[(162, 100), (161, 100)]);
        assert!(plan(&policy(""), &cached).is_empty());
    }

    #[test]
    fn keep_last_keeps_the_newest() {
        let cached = candidates(&[(162, 100), (161, 100), (160, 100)]);
        assert_eq!(
            ids(&plan(&policy("keep_last = 2"), &cached)),
            ["160"] // oldest goes
        );
    }

    #[test]
    fn max_bytes_accumulates_from_the_newest() {
        // 0.25 GB fits exactly two 100 MB episodes (max_gb is decimal GB).
        let cached = candidates(&[(162, 100_000_000), (161, 100_000_000), (160, 100_000_000)]);
        assert_eq!(ids(&plan(&policy("max_gb = 0.25"), &cached)), ["160"]);
    }

    #[test]
    fn stricter_limit_wins() {
        let cached = candidates(&[(162, 100_000_000), (161, 100_000_000), (160, 100_000_000)]);
        let both = policy("keep_last = 3\nmax_gb = 0.15");
        assert_eq!(ids(&plan(&both, &cached)), ["160", "161"]);
    }

    #[test]
    fn oversized_newest_episode_still_counts_against_bytes() {
        // The newest alone exceeds the cap: it is kept (never serve an
        // empty feed because one file is big), everything older goes.
        let cached = candidates(&[(162, 400_000_000), (161, 1)]);
        assert_eq!(ids(&plan(&policy("max_gb = 0.3"), &cached)), ["161"]);
    }
}
