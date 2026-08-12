//! Snapshot, read-view, and retention lifecycle ownership.
//!
//! `DB` remains the authority for selecting and publishing manifests. This
//! module owns the handles, registry state, leases, and cleanup behavior that
//! protect a selected root while callers read it.

use super::{
    BlobReadView, DB, DurabilityStatus, Error, Result, VerificationReport, atomic_write,
    retained_blob_path, sync_directory,
};
use crate::btree::LookupResult;
use crate::storage::StorageReadView;
use crate::storage::format::{CommitId, Manifest, RetainedRoot, RetentionRegistry, SnapshotId};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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

pub(super) struct RetentionState {
    path: PathBuf,
    root_path: PathBuf,
    registry: RetentionRegistry,
    /// Process-local transaction roots. These intentionally do not enter the
    /// durable named-snapshot registry: a crashed process must not leave a
    /// short-lived transaction pin blocking reclamation after reopen.
    ephemeral_roots: BTreeMap<SnapshotId, RetainedRoot>,
    next_ephemeral_snapshot_id: SnapshotId,
    pub(super) protected_offsets: Arc<Mutex<HashSet<u64>>>,
    offsets_by_snapshot: BTreeMap<SnapshotId, HashSet<u64>>,
}

pub(super) struct RetentionLease {
    pub(super) state: Arc<Mutex<RetentionState>>,
    pub(super) snapshot_id: SnapshotId,
    pub(super) reclamation_dirty: Arc<AtomicBool>,
    pub(super) released: bool,
}

/// A cheap immutable read handle bound to one published SeerDB generation.
///
/// The handle owns a process-local retention lease and independent page/blob
/// descriptors. It does not copy the PMT, serialize blob bytes, or write a
/// sidecar at creation time. Writers continue to publish newer generations;
/// this view remains on the root and physical files it captured.
pub struct ReadView {
    pub(super) storage: StorageReadView,
    pub(super) blobs: BlobReadView,
    pub(super) lease: Option<RetentionLease>,
    pub(super) durability: DurabilityStatus,
}

impl ReadView {
    /// Return the generation captured by this view.
    #[must_use]
    pub fn commit_id(&self) -> CommitId {
        self.durability.commit_id
    }

    /// Return the durability state captured by this view.
    #[must_use]
    pub fn durability_status(&self) -> DurabilityStatus {
        self.durability
    }

    /// Read a key from the captured generation.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.storage.lookup(key)? {
            LookupResult::Found(value) => Ok(Some(value)),
            LookupResult::Blob(pointer) => self.blobs.read(&pointer).map(Some),
            LookupResult::Deleted | LookupResult::NotFound => Ok(None),
        }
    }

    /// Read a range from the captured generation.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.storage
            .range(start, end)?
            .into_iter()
            .filter_map(|(key, result)| match result {
                LookupResult::Found(value) => Some(Ok((key, value))),
                LookupResult::Blob(pointer) => {
                    Some(self.blobs.read(&pointer).map(|value| (key, value)))
                }
                LookupResult::Deleted | LookupResult::NotFound => None,
            })
            .collect()
    }

    /// Release the view's root lease before dropping the handle.
    pub fn release(mut self) -> Result<()> {
        if let Some(mut lease) = self.lease.take() {
            lease.release()?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for ReadView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadView")
            .field("durability", &self.durability)
            .finish_non_exhaustive()
    }
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

impl RetentionState {
    pub(super) fn load(path: PathBuf, protected_offsets: Arc<Mutex<HashSet<u64>>>) -> Result<Self> {
        let registry = if path.exists() {
            let bytes = fs::read(&path)?;
            RetentionRegistry::from_bytes(&bytes)
                .map_err(|message| Error::Corruption(format!("retention registry {message}")))?
        } else {
            RetentionRegistry::new()
        };
        Ok(Self {
            root_path: path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            path,
            registry,
            ephemeral_roots: BTreeMap::new(),
            next_ephemeral_snapshot_id: SnapshotId::new(u64::MAX),
            protected_offsets,
            offsets_by_snapshot: BTreeMap::new(),
        })
    }

    fn persist(&self, registry: &RetentionRegistry) -> Result<()> {
        if registry.is_empty() {
            if self.path.exists() {
                fs::remove_file(&self.path)?;
                sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
            }
            return Ok(());
        }
        let bytes = registry
            .to_bytes()
            .ok_or_else(|| Error::Wal("retention registry is too large".into()))?;
        atomic_write(&self.path, &bytes)
    }

    fn replace_protected_offsets(&self) -> Result<()> {
        let mut protected = HashSet::new();
        for offsets in self.offsets_by_snapshot.values() {
            protected.extend(offsets);
        }
        *self
            .protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))? =
            protected;
        Ok(())
    }

    pub(super) fn install_offsets(
        &mut self,
        offsets_by_snapshot: BTreeMap<SnapshotId, HashSet<u64>>,
    ) -> Result<()> {
        self.offsets_by_snapshot = offsets_by_snapshot;
        self.replace_protected_offsets()
    }

    pub(super) fn insert(
        &mut self,
        manifest: Manifest,
        offsets: HashSet<u64>,
    ) -> Result<SnapshotId> {
        let mut candidate = self.registry.clone();
        let snapshot_id = candidate
            .insert(manifest)
            .ok_or_else(|| Error::Wal("snapshot ID overflow".into()))?;
        self.persist(&candidate)?;
        self.registry = candidate;
        self.offsets_by_snapshot.insert(snapshot_id, offsets);
        self.replace_protected_offsets()?;
        Ok(snapshot_id)
    }

    pub(super) fn remove(&mut self, snapshot_id: SnapshotId) -> Result<()> {
        if let Some(root) = self.ephemeral_roots.remove(&snapshot_id) {
            self.offsets_by_snapshot.remove(&snapshot_id);
            self.replace_protected_offsets()?;
            let blob_path = retained_blob_path(&self.root_path, root.snapshot_id);
            if blob_path.exists() {
                fs::remove_file(blob_path)?;
                sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
            }
            return Ok(());
        }
        let mut candidate = self.registry.clone();
        if candidate.remove(snapshot_id).is_none() {
            return Err(Error::InvalidArgument(format!(
                "unknown retained snapshot {}",
                snapshot_id.get()
            )));
        }
        self.persist(&candidate)?;
        self.registry = candidate;
        self.offsets_by_snapshot.remove(&snapshot_id);
        self.replace_protected_offsets()?;
        let blob_path = retained_blob_path(&self.root_path, snapshot_id);
        if blob_path.exists() {
            fs::remove_file(blob_path)?;
            sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
        }
        Ok(())
    }

    pub(super) fn roots(&self) -> &[RetainedRoot] {
        self.registry.roots()
    }

    pub(super) fn all_roots(&self) -> impl Iterator<Item = &RetainedRoot> {
        self.registry
            .roots()
            .iter()
            .chain(self.ephemeral_roots.values())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.registry.is_empty() && self.ephemeral_roots.is_empty()
    }

    pub(super) fn next_snapshot_id(&self) -> SnapshotId {
        self.registry.next_snapshot_id()
    }

    pub(super) fn next_ephemeral_snapshot_id(&self) -> SnapshotId {
        self.next_ephemeral_snapshot_id
    }

    pub(super) fn insert_ephemeral(
        &mut self,
        manifest: Manifest,
        offsets: HashSet<u64>,
    ) -> Result<SnapshotId> {
        let snapshot_id = self.next_ephemeral_snapshot_id;
        if snapshot_id.get() == 0
            || self
                .registry
                .roots()
                .iter()
                .any(|root| root.snapshot_id == snapshot_id)
        {
            return Err(Error::Wal("ephemeral snapshot ID overflow".into()));
        }
        self.next_ephemeral_snapshot_id = SnapshotId::new(snapshot_id.get() - 1);
        self.ephemeral_roots.insert(
            snapshot_id,
            RetainedRoot {
                snapshot_id,
                manifest,
            },
        );
        self.offsets_by_snapshot.insert(snapshot_id, offsets);
        self.replace_protected_offsets()?;
        Ok(snapshot_id)
    }
}

impl RetentionLease {
    pub(super) fn release(&mut self) -> Result<()> {
        if !self.released {
            self.state
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?
                .remove(self.snapshot_id)?;
            self.reclamation_dirty.store(true, Ordering::Release);
            self.released = true;
        }
        Ok(())
    }
}

impl Drop for RetentionLease {
    fn drop(&mut self) {
        if !self.released {
            if let Ok(mut state) = self.state.lock() {
                let _ = state.remove(self.snapshot_id);
            }
            self.reclamation_dirty.store(true, Ordering::Release);
            self.released = true;
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
