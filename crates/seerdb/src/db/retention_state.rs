//! Durable retained-root registry and process-local retention leases.

use super::{Error, Result, atomic_write, retained_blob_path, sync_directory};
use crate::storage::format::{Manifest, RetainedRoot, RetentionRegistry, SnapshotId};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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
