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
use crate::mvcc::PMT;
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

    /// Retain an arbitrary published commit for shared historical reads.
    ///
    /// The returned ID is stable across reopen because the commit-to-root
    /// descriptor is recorded in the durable retention registry. Callers can
    /// use [`DB::get_at`] and [`DB::range_at`] with that ID without copying the
    /// database directory.
    pub fn retain_commit(&mut self, commit_id: CommitId) -> Result<SnapshotId> {
        self.check_writable()?;
        self.flush()?;
        if let Some(snapshot_id) = self.retained_snapshot_id(commit_id) {
            return Ok(snapshot_id);
        }
        let manifest = self
            .manifest_history
            .find_commit(commit_id)
            .ok_or_else(|| {
                Error::SnapshotUnavailable(format!("commit {} is not retained", commit_id.get()))
            })?;
        self.register_retained_manifest(manifest, true)
    }

    /// Pin the active root for a short-lived transaction without rewalking the
    /// entire B-tree on every begin.
    ///
    /// The durable blob image and physical page offsets are still copied and
    /// registered before this returns, so later reclamation cannot overwrite
    /// the pinned root. Full graph and blob-target validation remains the
    /// responsibility of [`DB::retain_commit`], [`DB::check`], and
    /// [`DB::verify`]; reads through this pin validate pages and blob records
    /// at their access boundaries and fail closed on corruption.
    pub fn retain_current_commit(&mut self) -> Result<SnapshotId> {
        self.check_writable()?;
        self.flush()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        if manifest.commit_id != self.commit_id || manifest.generation_id != self.generation_id {
            return Err(Error::Corruption(
                "active manifest does not match the database frontier".into(),
            ));
        }
        self.register_current_transaction_manifest(manifest)
    }

    /// Begin a cheap immutable view over the active published generation.
    ///
    /// Unlike historical retention, this path does not serialize a blob
    /// sidecar or copy the PMT. The view pins the immutable PMT and opens the
    /// current page/blob files before returning; the process-local lease keeps
    /// reclamation from reusing pages or deleting old blob segments while it
    /// is live.
    pub fn begin_read_view(&mut self) -> Result<ReadView> {
        self.check_readable()?;
        self.check_maintenance_idle()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        if manifest.commit_id != self.commit_id || manifest.generation_id != self.generation_id {
            return Err(Error::Corruption(
                "active manifest does not match the database frontier".into(),
            ));
        }

        let snapshot_id = self.register_read_view_manifest(manifest)?;
        let mut lease = RetentionLease {
            state: Arc::clone(&self.retention),
            snapshot_id,
            reclamation_dirty: self.engine.reclamation_dirty_handle(),
            released: false,
        };
        let storage = match self.engine.read_view(manifest.root_page_id) {
            Ok(storage) => storage,
            Err(error) => {
                let _ = lease.release();
                return Err(error);
            }
        };
        let blobs = match BlobReadView::open(&self.path, &self.blobs) {
            Ok(blobs) => blobs,
            Err(error) => {
                let _ = lease.release();
                return Err(error);
            }
        };
        Ok(ReadView {
            storage,
            blobs,
            lease: Some(lease),
            durability: self.durability_status(),
        })
    }

    /// Return the active retention ID for a commit, if one exists.
    pub fn retained_snapshot_id(&self, commit_id: CommitId) -> Option<SnapshotId> {
        self.retention
            .lock()
            .ok()?
            .roots()
            .iter()
            .find_map(|root| (root.manifest.commit_id == commit_id).then_some(root.snapshot_id))
    }

    /// Release a durable historical retention lease by ID.
    pub fn release_snapshot(&mut self, snapshot_id: SnapshotId) -> Result<()> {
        self.check_writable()?;
        self.retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?
            .remove(snapshot_id)?;
        self.engine
            .reclamation_dirty_handle()
            .store(true, Ordering::Release);
        Ok(())
    }

    fn register_retained_manifest(
        &mut self,
        manifest: Manifest,
        deduplicate_commit: bool,
    ) -> Result<SnapshotId> {
        self.register_manifest(manifest, deduplicate_commit, false, true)
    }

    fn register_current_transaction_manifest(&mut self, manifest: Manifest) -> Result<SnapshotId> {
        self.register_manifest(manifest, false, true, false)
    }

    fn register_read_view_manifest(&mut self, manifest: Manifest) -> Result<SnapshotId> {
        let snapshot_id = {
            let state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            state.next_ephemeral_snapshot_id()
        };
        {
            let mut state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            state.insert_ephemeral(manifest, HashSet::new())?;
            let protected = state
                .protected_offsets
                .lock()
                .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
                .clone();
            self.engine.set_protected_offsets(protected)?;
        }
        Ok(snapshot_id)
    }

    pub(super) fn register_ephemeral_manifest(&mut self, manifest: Manifest) -> Result<SnapshotId> {
        self.register_manifest(manifest, false, true, true)
    }

    fn register_manifest(
        &mut self,
        manifest: Manifest,
        deduplicate_commit: bool,
        ephemeral: bool,
        validate_tree: bool,
    ) -> Result<SnapshotId> {
        if deduplicate_commit
            && let Some(snapshot_id) = self.retained_snapshot_id(manifest.commit_id)
        {
            return Ok(snapshot_id);
        }
        let snapshot_id = {
            let state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            if ephemeral {
                state.next_ephemeral_snapshot_id()
            } else {
                state.next_snapshot_id()
            }
        };
        let retained_blob = retained_blob_path(&self.path, snapshot_id);
        // Build the immutable retention sidecar from the verified in-memory
        // manager. Segmented stores use whole-image sidecars for now; this
        // keeps historical reads independent of active segment cleanup while
        // the active publication path avoids rewriting those segments.
        let mut retained_blobs = self.blobs.clone();
        // Deletion markers describe the active root. An older retained root
        // may still legitimately reference a value that a later commit
        // replaced, so its immutable sidecar must preserve the append-only
        // record bytes while omitting active-root deletion metadata.
        retained_blobs.clear_deletion_metadata();
        let blob_bytes = retained_blobs.to_bytes();
        if let Err(error) = atomic_write(&retained_blob, &blob_bytes) {
            let _ = fs::remove_file(&retained_blob);
            return Err(error);
        }
        if manifest.pmt_checkpoint_id.get() != 0 {
            let checkpoint = self
                .path
                .join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
            let (pmt, _) = match Self::load_meta(&checkpoint) {
                Ok(meta) => meta,
                Err(error) => {
                    let _ = fs::remove_file(&retained_blob);
                    return Err(error);
                }
            };
            if let Err(error) = self.validate_historical_page_liveness(manifest, &pmt) {
                let _ = fs::remove_file(&retained_blob);
                return Err(error);
            }
            if validate_tree {
                let pointers = match self.engine.verify_tree_at(manifest.root_page_id, &pmt) {
                    Ok(pointers) => pointers,
                    Err(error) => {
                        let _ = fs::remove_file(&retained_blob);
                        return Err(error);
                    }
                };
                if pointers
                    .iter()
                    .any(|pointer| retained_blobs.read(pointer).is_none())
                {
                    let _ = fs::remove_file(&retained_blob);
                    return Err(Error::SnapshotUnavailable(format!(
                        "commit {} has no complete historical blob image",
                        manifest.commit_id.get()
                    )));
                }
            }
        }
        let offsets = match Self::load_manifest_offsets(&self.path, manifest, snapshot_id) {
            Ok(offsets) => offsets,
            Err(error) => {
                let _ = fs::remove_file(&retained_blob);
                return Err(error);
            }
        };
        let snapshot_id = {
            let mut state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            let result = if ephemeral {
                state.insert_ephemeral(manifest, offsets)
            } else {
                state.insert(manifest, offsets)
            };
            if let Err(error) = result {
                let _ = fs::remove_file(&retained_blob);
                return Err(error);
            }
            snapshot_id
        };
        let protected = {
            let state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            state
                .protected_offsets
                .lock()
                .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
                .clone()
        };
        if let Err(error) = self.engine.set_protected_offsets(protected) {
            self.write_fenced = true;
            return Err(error);
        }
        Ok(snapshot_id)
    }

    /// Refuse a historical retention request once a later published
    /// generation has reused one of the target root's physical page slots.
    ///
    /// Published manifest history and the durable pre-reuse ledger together
    /// establish whether a later generation may have overwritten the target
    /// bytes. Retention must fail closed rather than treating a different,
    /// structurally valid page as the requested historical value.
    fn validate_historical_page_liveness(&self, target: Manifest, target_pmt: &PMT) -> Result<()> {
        let target_by_offset: BTreeMap<_, _> = target_pmt
            .iter()
            .map(|(_, mapping)| (mapping.offset, *mapping))
            .collect();
        if target_by_offset.is_empty() {
            return Ok(());
        }

        for attempt in self
            .reuse_ledger
            .attempts()
            .iter()
            .filter(|attempt| attempt.generation_id > target.generation_id)
        {
            if attempt
                .offsets
                .iter()
                .any(|offset| target_by_offset.contains_key(offset))
            {
                return Err(Error::SnapshotUnavailable(format!(
                    "commit {} has physical pages reused by an uncertain generation {}",
                    target.commit_id.get(),
                    attempt.generation_id.get()
                )));
            }
        }

        for later in self
            .manifest_history
            .manifests()
            .iter()
            .filter(|manifest| manifest.generation_id > target.generation_id)
        {
            if later.pmt_checkpoint_id.get() == 0 {
                continue;
            }
            let checkpoint = self
                .path
                .join(format!("seerdb.meta.{}", later.pmt_checkpoint_id.get()));
            let (later_pmt, _) = Self::load_meta(&checkpoint).map_err(|error| {
                Error::SnapshotUnavailable(format!(
                    "commit {} cannot establish page liveness through generation {}: {error}",
                    target.commit_id.get(),
                    later.generation_id.get()
                ))
            })?;
            if later_pmt.iter().any(|(_, mapping)| {
                target_by_offset
                    .get(&mapping.offset)
                    .is_some_and(|target_mapping| *target_mapping != *mapping)
            }) {
                return Err(Error::SnapshotUnavailable(format!(
                    "commit {} has physical pages reused by a later generation",
                    target.commit_id.get()
                )));
            }
        }
        Ok(())
    }
}
