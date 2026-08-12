//! Published logical commit catalog projection for DBNext archive consumers.

use super::DB;
use crate::error::{Error, Result};
use crate::storage::format::CommitId;

impl DB {
    /// Return every published logical commit boundary in this history,
    /// including the initial empty root. Maintenance generations that retain
    /// the same logical commit are returned only once. The manifest-history
    /// sidecar is the authoritative catalog; callers must still retain each
    /// returned commit before reading it.
    pub fn published_commits(&self) -> Result<Vec<CommitId>> {
        let mut commits = Vec::new();
        for manifest in self.manifest_history.manifests() {
            let commit = manifest.commit_id;
            if commits.last().copied() == Some(commit) {
                continue;
            }
            if commits
                .last()
                .is_some_and(|previous| commit.get() != previous.get().saturating_add(1))
                || (commits.is_empty() && commit.get() != 0)
            {
                return Err(Error::SnapshotUnavailable(
                    "complete commit history is no longer retained".into(),
                ));
            }
            commits.push(commit);
        }
        Ok(commits)
    }
}
