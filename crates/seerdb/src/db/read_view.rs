//! Generation-bound read-view lifecycle.

use super::retention_state::RetentionLease;
use super::{BlobReadView, DB, DurabilityStatus, Error, Result};
use crate::btree::LookupResult;
use crate::storage::StorageReadView;
use crate::storage::format::{CommitId, CommitPosition};
use std::sync::Arc;

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

    /// Return the logical and durable position captured by this view.
    #[must_use]
    pub fn commit_position(&self) -> CommitPosition {
        self.durability.commit_position
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

impl DB {
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
            .manifest_history
            .latest()
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
}
