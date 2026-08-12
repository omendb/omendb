//! Copy-backed snapshot handle lifecycle.

use super::retention_state::RetentionLease;
use super::{DB, DurabilityStatus, Error, Result, VerificationReport};
use crate::storage::format::SnapshotId;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// An owned, read-only snapshot view backed by an independently verified
/// directory copy.
///
/// The copy-backed implementation is intentionally conservative: source page
/// reclamation cannot invalidate the snapshot, and dropping or releasing the
/// handle removes its temporary directory. A future shared-page snapshot can
/// preserve this API while replacing the copy mechanism.
pub struct Snapshot {
    pub(super) db: Option<DB>,
    pub(super) path: PathBuf,
    pub(super) released: bool,
}

/// An owned retained snapshot backed by a verified read-only copy and a
/// durable root-generation retention lease.
///
/// The copy is the current read implementation; the lease is the physical
/// safety boundary. Retained page versions are not reused while the lease is
/// live, so the eventual in-history reader can replace the copy without
/// changing the reclamation contract.
pub struct RetainedSnapshot {
    pub(super) snapshot: Option<Snapshot>,
    pub(super) lease: Option<RetentionLease>,
}

impl Snapshot {
    /// Return the snapshot directory path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get a value from the retained snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .get(key)
    }

    /// Scan a range in the retained snapshot.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .range(start, end)
    }

    /// Verify the retained snapshot independently.
    pub fn verify(&mut self) -> Result<VerificationReport> {
        self.db
            .as_mut()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .verify()
    }

    /// Return the durable identity captured by this snapshot.
    pub fn durability_status(&self) -> Result<DurabilityStatus> {
        Ok(self
            .db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .durability_status())
    }

    /// Release the snapshot directory immediately.
    pub fn release(mut self) -> Result<()> {
        self.db.take();
        fs::remove_dir_all(&self.path)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.db.take();
        if !self.released {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

impl RetainedSnapshot {
    /// Return the durable retention identifier.
    pub fn snapshot_id(&self) -> SnapshotId {
        self.lease
            .as_ref()
            .map(|lease| lease.snapshot_id)
            .unwrap_or_default()
    }

    /// Return the path of the conservative read copy while it is live.
    pub fn path(&self) -> Option<&Path> {
        self.snapshot.as_ref().map(Snapshot::path)
    }

    /// Read a value from the retained snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .get(key)
    }

    /// Scan a range in the retained snapshot.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .range(start, end)
    }

    /// Verify the retained snapshot independently.
    pub fn verify(&mut self) -> Result<VerificationReport> {
        self.snapshot
            .as_mut()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .verify()
    }

    /// Return the durable identity captured by this retained snapshot.
    pub fn durability_status(&self) -> Result<DurabilityStatus> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .durability_status()
    }

    /// Release the physical root-retention lease and temporary read copy.
    pub fn release(mut self) -> Result<()> {
        let lease_result = self.lease.as_mut().map_or(Ok(()), RetentionLease::release);
        let snapshot_result = self.snapshot.take().map_or(Ok(()), Snapshot::release);
        if lease_result.is_ok() {
            self.lease.take();
        }
        lease_result.and(snapshot_result)
    }
}

impl DB {
    /// Create an owned read-only snapshot handle.
    ///
    /// The handle owns a verified temporary directory and removes it on
    /// `release()` or `Drop`. Use [`DB::snapshot`] when the archive should
    /// survive independently of this process.
    pub fn begin_snapshot(&mut self) -> Result<Snapshot> {
        self.check_writable()?;
        let destination = self.next_snapshot_path()?;
        self.snapshot(&destination)?;
        let db = match DB::open(&destination, self.options.clone()) {
            Ok(db) => db,
            Err(error) => {
                let _ = fs::remove_dir_all(&destination);
                return Err(error);
            }
        };
        Ok(Snapshot {
            db: Some(db),
            path: destination,
            released: false,
        })
    }

    /// Retain the current root generation with a durable physical lease.
    ///
    /// The retained root is registered durably before its pages can be
    /// reclaimed. Historical reads use the source device and the retained
    /// PMT; the independently verified copy remains available for callers
    /// that need an isolated archive-style handle.
    pub fn retain_current(&mut self) -> Result<RetainedSnapshot> {
        self.check_writable()?;
        let snapshot = self.begin_snapshot()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        // Each owned handle needs its own durable lease. Reusing a commit's
        // existing root here would let releasing one handle unprotect the
        // pages still needed by another handle.
        let snapshot_id = self.register_retained_manifest(manifest, false)?;

        Ok(RetainedSnapshot {
            snapshot: Some(snapshot),
            lease: Some(RetentionLease {
                state: Arc::clone(&self.retention),
                snapshot_id,
                reclamation_dirty: self.engine.reclamation_dirty_handle(),
                released: false,
            }),
        })
    }
}
