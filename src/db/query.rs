//! Public point/range reads and retained-root query resolution.
//!
//! `DB` remains the mutable state and publication authority. This module owns
//! the read-side policy that turns B-tree lookup results into user values,
//! including blob-pointer validation and retained-root PMT/blob selection.
//! Generation-bound `ReadView` has its own resource lifecycle in
//! [`super::read_view`]; this module covers the ordinary DB API and durable
//! historical IDs.

use super::retained_blob_path;
use super::{BlobManager, DB, Error, LookupResult, Manifest, PMT, Result, SnapshotId};
use std::fs;

impl DB {
    /// Get a value by key.
    ///
    /// Read path:
    /// 1. Lookup key in B-tree
    /// 2. If value is inline, return it
    /// 3. If value is blob pointer, read from blob file
    /// 4. If deleted (tombstone), return None
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_readable()?;

        match self.engine.lookup(key)? {
            LookupResult::Found(value) => Ok(Some(value)),
            LookupResult::Blob(ptr) => match self.blobs.read(&ptr) {
                Some(data) => Ok(Some(data.to_vec())),
                None => Err(Error::Corruption("blob pointer invalid".into())),
            },
            LookupResult::Deleted | LookupResult::NotFound => Ok(None),
        }
    }

    /// Get a value from a retained historical root.
    ///
    /// IDs returned by [`DB::retain_commit`] are durable across reopen. IDs
    /// owned by [`DB::begin_batch_transaction`] are process-local and expire
    /// when the transaction or process ends.
    pub fn get_at(&self, snapshot_id: SnapshotId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_readable()?;
        let manifest = self.retained_manifest(snapshot_id)?;
        let pmt = self.retained_pmt(manifest)?;
        let result = self.engine.lookup_at(manifest.root_page_id, &pmt, key)?;
        self.lookup_result_value(result, snapshot_id)
    }

    /// Range scan over [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_readable()?;
        self.engine
            .range(start, end)?
            .into_iter()
            .filter_map(|(key, value)| match value {
                LookupResult::Found(value) => Some(Ok((key, value))),
                LookupResult::Blob(pointer) => Some(
                    self.blobs
                        .read(&pointer)
                        .map(|value| (key, value.to_vec()))
                        .ok_or_else(|| Error::Corruption("blob pointer invalid".into())),
                ),
                LookupResult::Deleted | LookupResult::NotFound => None,
            })
            .collect()
    }

    /// Scan a range from a retained historical root.
    pub fn range_at(
        &self,
        snapshot_id: SnapshotId,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_readable()?;
        let manifest = self.retained_manifest(snapshot_id)?;
        let pmt = self.retained_pmt(manifest)?;
        let blob_path = retained_blob_path(&self.path, snapshot_id);
        let blob_bytes = fs::read(&blob_path)?;
        let blobs = BlobManager::from_bytes(&blob_bytes).ok_or_else(|| {
            Error::Corruption(format!(
                "retained snapshot {} has an invalid blob image",
                snapshot_id.get()
            ))
        })?;
        self.engine
            .range_at(manifest.root_page_id, &pmt, start, end)?
            .into_iter()
            .filter_map(|(key, value)| match value {
                LookupResult::Found(value) => Some(Ok((key, value))),
                LookupResult::Blob(pointer) => Some(
                    blobs
                        .read(&pointer)
                        .map(|value| (key, value.to_vec()))
                        .ok_or_else(|| {
                            Error::Corruption(format!(
                                "retained snapshot {} has an invalid blob pointer",
                                snapshot_id.get()
                            ))
                        }),
                ),
                LookupResult::Deleted | LookupResult::NotFound => None,
            })
            .collect()
    }

    fn retained_manifest(&self, snapshot_id: SnapshotId) -> Result<Manifest> {
        let state = self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        state
            .all_roots()
            .find(|root| root.snapshot_id == snapshot_id)
            .map(|root| root.manifest)
            .ok_or_else(|| {
                Error::SnapshotUnavailable(format!(
                    "retained root {} is not active",
                    snapshot_id.get()
                ))
            })
    }

    fn retained_pmt(&self, manifest: Manifest) -> Result<PMT> {
        if manifest.pmt_checkpoint_id.get() == 0 {
            return Ok(PMT::new());
        }
        let checkpoint = self
            .path
            .join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
        Self::load_meta(&checkpoint).map(|(pmt, _)| pmt)
    }

    fn lookup_result_value(
        &self,
        result: LookupResult,
        snapshot_id: SnapshotId,
    ) -> Result<Option<Vec<u8>>> {
        match result {
            LookupResult::Found(value) => Ok(Some(value)),
            LookupResult::Blob(pointer) => {
                let blob_path = retained_blob_path(&self.path, snapshot_id);
                let blob_bytes = fs::read(&blob_path)?;
                let blobs = BlobManager::from_bytes(&blob_bytes).ok_or_else(|| {
                    Error::Corruption(format!(
                        "retained snapshot {} has an invalid blob image",
                        snapshot_id.get()
                    ))
                })?;
                blobs
                    .read(&pointer)
                    .map(|value| Some(value.to_vec()))
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "retained snapshot {} has an invalid blob pointer",
                            snapshot_id.get()
                        ))
                    })
            }
            LookupResult::Deleted | LookupResult::NotFound => Ok(None),
        }
    }
}
