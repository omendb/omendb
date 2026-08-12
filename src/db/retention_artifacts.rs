//! Durable retained-root artifact bootstrap and protected-page reconstruction.
//!
//! This module owns filesystem cleanup and validation needed to install the
//! retained-root protection view during open. Runtime retention registration,
//! sidecar creation, and historical page-liveness policy remain in
//! [`super::retention`].

use super::retention_state::RetentionState;
use super::{BLOB_FILE, DATA_FILE, DB, Error, Result, retained_blob_path, sync_directory};
use crate::blob::BlobManager;
use crate::btree::PAGE_SIZE;
use crate::storage::format::{DatabaseId, HistoryId, Manifest, SnapshotId};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;
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
}
