//! Capacity admission and reservation for `DB` publications.
//!
//! This module owns conservative blob, page, metadata, history, and ledger
//! footprint calculations before mutation or maintenance writes begin. It
//! does not publish artifacts or mutate the authoritative DB state; durable
//! artifact persistence remains in `durability.rs` and ordering remains in
//! `publication.rs`.

use super::metadata_codec::{META_DELTA_CHECKSUM_SIZE, META_DELTA_HEADER_SIZE, META_MAGIC};
use super::{
    BLOB_RESERVATION_FILE, DB, Error, META_FILE, PUBLICATION_CAPACITY_SAFETY_BYTES, Result,
};
use crate::allocator::PageAllocator;
use crate::blob::BlobManager;
use crate::btree::{BTree, BlobPointer, PAGE_SIZE};
use crate::mvcc::PageMapping;
use crate::space::reserve_file;
use crate::storage::format::{
    CommitId, CommitSeq, FORMAT_VERSION, GenerationId, Manifest, ManifestHistory, PmtCheckpointId,
};
use std::fs::{self, OpenOptions};

impl DB {
    /// Reserve the next blob image before a mutation changes memory state.
    ///
    /// Linux and macOS retain the physical reservation in a sidecar that is
    /// consumed by the next atomic blob publication. Other platforms use a
    /// best-effort filesystem-space check and keep the final write fallback.
    pub(super) fn admit_blob_image(
        &self,
        retired: Option<&BlobPointer>,
        appended_value_len: Option<usize>,
    ) -> Result<()> {
        let required = if self.blobs.is_segmented() {
            self.blobs
                .projected_segment_write_size(retired, appended_value_len)
        } else {
            self.blobs
                .projected_serialized_size(retired, appended_value_len)
        }
        .ok_or_else(|| Error::InvalidArgument("blob image size overflows".into()))?;
        self.engine.check_artifact_capacity(required)?;
        if self.blobs.is_segmented() {
            return Ok(());
        }
        self.reserve_blob_image(required)
    }

    pub(super) fn blob_publication_size(blobs: &BlobManager) -> Result<u64> {
        if blobs.is_segmented() {
            blobs
                .segment_write_size()
                .ok_or_else(|| Error::InvalidArgument("blob catalog size overflows".into()))
        } else {
            blobs
                .serialized_size()
                .ok_or_else(|| Error::InvalidArgument("blob image size overflows".into()))
        }
    }

    pub(super) fn reserve_blob_image(&self, required: u64) -> Result<()> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let reservation_path = self.path.join(BLOB_RESERVATION_FILE);
            let file = match OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&reservation_path)
            {
                Ok(file) => file,
                Err(error) => {
                    let _ = fs::remove_file(&reservation_path);
                    return Err(error.into());
                }
            };
            if let Err(error) = reserve_file(&file, required) {
                drop(file);
                let _ = fs::remove_file(&reservation_path);
                return Err(error.into());
            }
            file.set_len(required)?;
            file.sync_data()?;
            super::artifact_io::sync_directory(&self.path)?;
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            if required > fs2::available_space(&self.path)? {
                return Err(Error::DiskFull);
            }
        }

        Ok(())
    }

    /// Admit the complete candidate publication before the first page write.
    ///
    /// Individual atomic artifacts reserve their own temporary files later in
    /// publication. A filesystem can therefore report ENOSPC after the data
    /// page generation has already reached the device unless their aggregate
    /// footprint is checked first. This is a conservative same-filesystem
    /// guard; a concurrent external consumer can still force the final
    /// write-time DiskFull/recovery path.
    pub(super) fn preflight_publication_capacity(&mut self) -> Result<()> {
        let dirty_page_count = self
            .engine
            .btree()
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| self.engine.btree().node(*page_id).is_some())
            .count() as u64;
        let reused_page_count = self.engine.pending_reuse_offsets().len() as u64;
        let new_page_count = dirty_page_count.saturating_sub(reused_page_count);

        let pmt_bytes = (self.engine.pmt().serialized_len() as u64)
            .checked_add(
                dirty_page_count
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let allocator_bytes = (self.engine.allocator().serialized_len() as u64)
            .checked_add(dirty_page_count.checked_mul(8).ok_or(Error::DiskFull)?)
            .ok_or(Error::DiskFull)?;
        let full_meta_bytes = (META_MAGIC.len() as u64)
            .checked_add(4 + 4 + 4 + 4)
            .and_then(|size| size.checked_add(pmt_bytes))
            .and_then(|size| size.checked_add(allocator_bytes))
            .ok_or(Error::DiskFull)?;
        let parent = self
            .manifest_history
            .latest()
            .unwrap_or_else(|| self.bootstrap_manifest());
        let (checkpoint_meta_bytes, _) =
            self.generation_meta_bytes(parent.pmt_checkpoint_id.get(), dirty_page_count as usize)?;
        let legacy_meta_bytes = if self.path.join(META_FILE).is_file() {
            0
        } else {
            full_meta_bytes
        };
        let blob_bytes = Self::blob_publication_size(&self.blobs)?;
        let history_entry_bytes = ManifestHistory::entry_bytes(Manifest {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: GenerationId::new(0),
            commit_id: CommitId::new(0),
            commit_seq: CommitSeq::new(0),
            page_size: PAGE_SIZE as u32,
            root_page_id: 0,
            pmt_checkpoint_id: PmtCheckpointId::new(0),
            wal_segment: 0,
            wal_offset: 0,
            mutation_count: 0,
            digest: 0,
            format_version: FORMAT_VERSION,
        })
        .len() as u64;
        let history_length = fs::metadata(DB::metadata_log_path(&self.path))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let history_bytes = history_length
            .checked_add(history_entry_bytes)
            .ok_or(Error::DiskFull)?;
        let required = new_page_count
            .checked_mul(PAGE_SIZE as u64)
            .and_then(|size| size.checked_add(checkpoint_meta_bytes))
            .and_then(|size| size.checked_add(legacy_meta_bytes))
            .and_then(|size| size.checked_add(blob_bytes))
            .and_then(|size| size.checked_add(history_bytes))
            .and_then(|size| size.checked_add(PUBLICATION_CAPACITY_SAFETY_BYTES))
            .ok_or(Error::DiskFull)?;

        if fs2::available_space(&self.path)? < required {
            return Err(Error::CapacityPreflight);
        }
        Ok(())
    }

    /// Admit a maintenance generation before it writes new data pages.
    ///
    /// Maintenance publications do not carry a WAL reservation, so account
    /// for their new data extent and all atomic sidecar artifacts directly.
    /// This is conservative: the metadata argument may be a full checkpoint
    /// even when the selected generation will use a smaller delta.
    pub(super) fn preflight_maintenance_capacity(
        &self,
        data_bytes: u64,
        metadata_bytes: u64,
        blob_bytes: u64,
    ) -> Result<()> {
        let history_entry_bytes =
            ManifestHistory::entry_bytes(self.bootstrap_manifest()).len() as u64;
        let history_length = fs::metadata(DB::metadata_log_path(&self.path))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let history_bytes = history_length
            .checked_add(history_entry_bytes)
            .ok_or(Error::DiskFull)?;
        let required = data_bytes
            .checked_add(metadata_bytes)
            .and_then(|size| size.checked_add(blob_bytes))
            .and_then(|size| size.checked_add(history_bytes))
            .and_then(|size| size.checked_add(PUBLICATION_CAPACITY_SAFETY_BYTES))
            .ok_or(Error::DiskFull)?;
        if fs2::available_space(&self.path)? < required {
            return Err(Error::CapacityPreflight);
        }
        Ok(())
    }

    pub(super) fn full_metadata_bytes_for_candidate(candidate: &BTree) -> Result<u64> {
        let page_count = u64::try_from(candidate.node_count()).map_err(|_| Error::DiskFull)?;
        let pmt_bytes = 4u64
            .checked_add(
                page_count
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let allocator_bytes = candidate.page_allocator().serialized_len() as u64;
        (META_MAGIC.len() as u64)
            .checked_add(4 + 4 + 4 + 4)
            .and_then(|size| size.checked_add(pmt_bytes))
            .and_then(|size| size.checked_add(allocator_bytes))
            .ok_or(Error::DiskFull)
    }

    /// Bound a metadata delta that may update every current mapping.
    ///
    /// Interior relocation changes existing PMT mappings after the normal
    /// dirty-page admission point. Passing zero dirty pages to
    /// `generation_meta_bytes` would therefore under-account the sidecar that
    /// is written after the relocation. The relocation path does not change
    /// the logical page set, so the current PMT count bounds both its mapping
    /// updates and any conservative removal allowance.
    pub(super) fn max_metadata_delta_bytes(
        page_count: usize,
        allocator: &PageAllocator,
    ) -> Result<u64> {
        let page_count = u64::try_from(page_count).map_err(|_| Error::DiskFull)?;
        let update_bytes = page_count
            .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
            .ok_or(Error::DiskFull)?;
        let removal_bytes = page_count.checked_mul(8).ok_or(Error::DiskFull)?;
        (META_DELTA_HEADER_SIZE as u64)
            .checked_add(update_bytes)
            .and_then(|size| size.checked_add(removal_bytes))
            .and_then(|size| size.checked_add(allocator.serialized_len() as u64))
            .and_then(|size| size.checked_add(META_DELTA_CHECKSUM_SIZE as u64))
            .ok_or(Error::DiskFull)
    }
}
