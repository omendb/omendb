//! DB retention registration and historical page-liveness policy.
//!
//! `DB` remains the authority for selecting and publishing manifests. Handle
//! and registry lifecycles live in [`super::snapshot`],
//! [`super::read_view`], and [`super::retention_state`]; this module owns the
//! policy that turns a selected manifest into a protected historical root.

use super::retention_state::RetentionState;
use super::{
    BLOB_FILE, DATA_FILE, DB, Error, Result, atomic_write, retained_blob_path, sync_directory,
};
use crate::blob::BlobManager;
use crate::btree::PAGE_SIZE;
use crate::mvcc::PMT;
use crate::storage::format::{CommitId, DatabaseId, HistoryId, Manifest, SnapshotId};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

impl DB {
    pub(super) fn cleanup_orphaned_retained_blobs(
        path: &Path,
        retention: &Arc<Mutex<RetentionState>>,
    ) -> Result<()> {
        let state = retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        let retained_ids = state
            .roots()
            .iter()
            .map(|root| root.snapshot_id)
            .collect::<HashSet<_>>();
        drop(state);

        let prefix = format!("{BLOB_FILE}.retained.");
        let mut removed = false;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(id) = name
                .strip_prefix(&prefix)
                .and_then(|suffix| suffix.parse::<u64>().ok())
            else {
                continue;
            };
            if !retained_ids.contains(&SnapshotId::new(id)) {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
        if removed {
            sync_directory(path)?;
        }
        Ok(())
    }

    pub(super) fn load_retained_offset_map(
        path: &Path,
        state: &RetentionState,
        database_id: DatabaseId,
        history_id: HistoryId,
    ) -> Result<BTreeMap<SnapshotId, HashSet<u64>>> {
        let mut offsets_by_snapshot = BTreeMap::new();
        for root in state.roots() {
            if root.manifest.database_id != database_id {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} belongs to another database",
                    root.snapshot_id.get()
                )));
            }
            if root.manifest.history_id != history_id {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} belongs to another history",
                    root.snapshot_id.get()
                )));
            }
            if root.manifest.page_size as usize != PAGE_SIZE {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has page size {}",
                    root.snapshot_id.get(),
                    root.manifest.page_size
                )));
            }
            let blob_path = retained_blob_path(path, root.snapshot_id);
            let blob_bytes = fs::read(&blob_path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Error::Corruption(format!(
                        "retained snapshot {} is missing its blob image",
                        root.snapshot_id.get()
                    ))
                } else {
                    error.into()
                }
            })?;
            if BlobManager::from_bytes(&blob_bytes).is_none() {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has an invalid blob image",
                    root.snapshot_id.get()
                )));
            }
            let protected = Self::load_manifest_offsets(path, root.manifest, root.snapshot_id)?;
            offsets_by_snapshot.insert(root.snapshot_id, protected);
        }
        Ok(offsets_by_snapshot)
    }

    pub(super) fn load_manifest_offsets(
        path: &Path,
        manifest: Manifest,
        snapshot_id: SnapshotId,
    ) -> Result<HashSet<u64>> {
        if manifest.pmt_checkpoint_id.get() == 0 {
            if manifest.root_page_id != 0 {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has a root without a checkpoint",
                    snapshot_id.get()
                )));
            }
            return Ok(HashSet::new());
        }

        let checkpoint = path.join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
        let (pmt, _) = Self::load_meta(&checkpoint)?;
        if !pmt.contains(manifest.root_page_id) {
            return Err(Error::Corruption(format!(
                "retained snapshot {} names a root missing from its checkpoint",
                snapshot_id.get()
            )));
        }
        let mut protected = HashSet::new();
        let data_bytes = fs::metadata(path.join(DATA_FILE))?.len();
        for (_, mapping) in pmt.iter() {
            if mapping.file_id != 0 || !mapping.offset.is_multiple_of(PAGE_SIZE as u64) {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} names an invalid page mapping",
                    snapshot_id.get()
                )));
            }
            let end = mapping
                .offset
                .checked_add(PAGE_SIZE as u64)
                .ok_or_else(|| {
                    Error::Corruption(format!(
                        "retained snapshot {} has an overflowing page mapping",
                        snapshot_id.get()
                    ))
                })?;
            if end > data_bytes {
                return Err(Error::SnapshotUnavailable(format!(
                    "retained snapshot {} names pages beyond the data file",
                    snapshot_id.get()
                )));
            }
            protected.insert(mapping.offset);
        }
        Ok(protected)
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

    pub(super) fn register_retained_manifest(
        &mut self,
        manifest: Manifest,
        deduplicate_commit: bool,
    ) -> Result<SnapshotId> {
        self.register_manifest(manifest, deduplicate_commit, false, true)
    }

    fn register_current_transaction_manifest(&mut self, manifest: Manifest) -> Result<SnapshotId> {
        self.register_manifest(manifest, false, true, false)
    }

    pub(super) fn register_read_view_manifest(&mut self, manifest: Manifest) -> Result<SnapshotId> {
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
