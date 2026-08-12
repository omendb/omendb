//! Durable artifact admission and persistence helpers for `DB`.
//!
//! This module owns WAL journaling and reservation, blob reservation,
//! publication-capacity preflight, manifest-history and reuse-ledger artifact
//! persistence, and manifest mirroring. `DB` remains the mutable state owner;
//! `publication.rs` remains the publication-ordering authority.

use super::metadata::{META_DELTA_CHECKSUM_SIZE, META_DELTA_HEADER_SIZE, META_MAGIC};
use super::wal_recovery::extend_digest;
use super::{
    BLOB_RESERVATION_FILE, DB, Error, MANIFEST_HISTORY_FILE, META_FILE,
    PUBLICATION_CAPACITY_SAFETY_BYTES, REUSE_LEDGER_FILE, Result, WAL_COMMIT_RECORD_BYTES,
    WAL_FILE, WAL_RESERVATION_SEGMENT_BYTES, atomic_write, atomic_write_without_directory_sync,
    atomic_write_without_fault_injection, elapsed_nanos, sync_directory,
};
#[cfg(any(test, feature = "fault-injection"))]
use super::{
    FAIL_NEXT_WAL_AFTER_SYNC, FAIL_NEXT_WAL_AFTER_WRITE, FAIL_NEXT_WAL_SYNC, FAIL_NEXT_WAL_WRITE,
};
use crate::allocator::PageAllocator;
use crate::blob::BlobManager;
use crate::btree::{BTree, BlobPointer, PAGE_SIZE};
use crate::mvcc::PageMapping;
use crate::recovery::{SyncPolicy, WalRecord};
use crate::space::reserve_file;
use crate::storage::format::{
    CommitId, FORMAT_VERSION, GenerationId, Manifest, ManifestHistory, PmtCheckpointId, ReuseLedger,
};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Instant;

impl DB {
    /// Write buffered WAL records to disk and optionally force the prefix.
    pub(super) fn write_wal_to_disk(&mut self, force_sync: bool) -> Result<()> {
        let started = Instant::now();
        let result = self.write_wal_to_disk_inner(force_sync);
        self.publication_timing.wal_write_ns = self
            .publication_timing
            .wal_write_ns
            .saturating_add(elapsed_nanos(started));
        result
    }

    fn write_wal_to_disk_inner(&mut self, force_sync: bool) -> Result<()> {
        let mut wal_buf = Vec::new();
        self.wal.flush(&mut wal_buf)?;
        let should_sync = force_sync || self.wal.sync_policy() != SyncPolicy::None;
        if !wal_buf.is_empty() || should_sync {
            let wal_path = self.path.join(WAL_FILE);
            let mut file = OpenOptions::new()
                .create(true)
                .append(!wal_buf.is_empty())
                .read(should_sync)
                .write(!wal_buf.is_empty() || should_sync)
                .open(&wal_path)?;
            if !wal_buf.is_empty() {
                // Append to WAL file (not overwrite).
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_WRITE.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected WAL append failure").into());
                }
                use std::io::Write;
                file.write_all(&wal_buf)?;
                self.publication.wal_bytes_written = self
                    .publication
                    .wal_bytes_written
                    .saturating_add(wal_buf.len() as u64);
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_AFTER_WRITE.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected post-append WAL failure").into());
                }
            }
            if should_sync {
                // The commit boundary and any configured per-mutation policy
                // force the WAL before dependent page publication.
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_SYNC.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected WAL sync failure").into());
                }
                match self.wal.sync_policy() {
                    SyncPolicy::SyncAll => file.sync_all()?,
                    SyncPolicy::FDataSync | SyncPolicy::None => file.sync_data()?,
                }
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_AFTER_SYNC.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected post-WAL-sync failure").into());
                }
            }
        }
        Ok(())
    }

    /// Journal a mutation after it has successfully changed memory state.
    pub(super) fn journal_mutation(&mut self, record: WalRecord) -> Result<()> {
        self.wal.append(&record);
        let sync_mutation = self.wal.sync_policy() != SyncPolicy::None;
        if let Err(error) = self.write_wal_to_disk(sync_mutation) {
            self.write_fenced = true;
            return Err(error);
        }
        self.pending_mutations = self
            .pending_mutations
            .checked_add(1)
            .ok_or_else(|| Error::Wal("mutation count overflow".into()))?;
        self.pending_wal_bytes = self
            .pending_wal_bytes
            .checked_add(record.to_bytes().len() as u64)
            .ok_or_else(|| Error::Wal("WAL byte count overflow".into()))?;
        self.pending_digest = extend_digest(self.pending_digest, &record);
        Ok(())
    }

    /// Reserve enough logical WAL budget for one mutation and the commit that
    /// closes its pending generation. This runs before any tree or blob state
    /// changes, so retryable backpressure cannot leave a partial mutation.
    pub(super) fn admit_wal_record(&mut self, record: &WalRecord) -> Result<()> {
        let used = self.pending_wal_bytes;
        let required = (record.to_bytes().len() as u64)
            .checked_add(WAL_COMMIT_RECORD_BYTES)
            .ok_or(Error::DiskFull)?;
        let available = self.options.max_wal_bytes.saturating_sub(used);
        if required > available {
            self.wal_admission_failures = self.wal_admission_failures.saturating_add(1);
            return Err(Error::Backpressure {
                required,
                available,
            });
        }

        self.ensure_wal_reservation()?;
        Ok(())
    }

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
            sync_directory(&self.path)?;
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            if required > fs2::available_space(&self.path)? {
                return Err(Error::DiskFull);
            }
        }

        Ok(())
    }

    /// Ensure the database owns a fixed-size WAL reservation extent before a
    /// mutation changes tree or blob state. The extent is rounded to fixed
    /// segments so future WAL growth has a bounded, stable admission domain;
    /// the logical WAL remains separately length-delimited and checksummed.
    pub(super) fn ensure_wal_reservation(&mut self) -> Result<u64> {
        let target = self.wal_reservation_target()?;
        if target == 0 {
            self.wal_reserved_extent = 0;
            return Ok(0);
        }
        if self.wal_reserved_extent >= target {
            return Ok(self.wal_reserved_extent);
        }

        // Reserve the physical extent on the file that will receive WAL
        // bytes. A separate sidecar would consume capacity without protecting
        // the actual append path.
        let path = self.path.join(WAL_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let current = file.metadata()?.len();
        if current < target {
            let physically_reserved = reserve_file(&file, target)?;
            if !physically_reserved
                && fs2::available_space(&self.path)? < target.saturating_sub(current)
            {
                return Err(Error::DiskFull);
            }
            file.sync_data()?;
            sync_directory(&self.path)?;
        }
        self.wal_reserved_extent = target;
        Ok(current.max(target))
    }

    fn wal_reservation_target(&self) -> Result<u64> {
        if self.options.max_wal_bytes == 0 {
            return Ok(0);
        }

        let remainder = self.options.max_wal_bytes % WAL_RESERVATION_SEGMENT_BYTES;
        self.options
            .max_wal_bytes
            .checked_add(
                (WAL_RESERVATION_SEGMENT_BYTES - remainder) % WAL_RESERVATION_SEGMENT_BYTES,
            )
            .ok_or(Error::DiskFull)
    }

    pub(super) fn persist_manifest_history(&self, history: &ManifestHistory) -> Result<()> {
        self.persist_manifest_history_with_directory_sync(history, true)
    }

    pub(super) fn persist_manifest_history_without_directory_sync(
        &self,
        history: &ManifestHistory,
    ) -> Result<()> {
        self.persist_manifest_history_with_directory_sync(history, false)
    }

    fn persist_manifest_history_with_directory_sync(
        &self,
        history: &ManifestHistory,
        sync_parent: bool,
    ) -> Result<()> {
        let bytes = history
            .to_bytes()
            .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
        if sync_parent {
            atomic_write(&self.path.join(MANIFEST_HISTORY_FILE), &bytes)
        } else {
            atomic_write_without_directory_sync(&self.path.join(MANIFEST_HISTORY_FILE), &bytes)
        }
    }

    pub(super) fn persist_reuse_ledger(&self) -> Result<()> {
        Self::persist_reuse_ledger_at(&self.path, &self.reuse_ledger)
    }

    pub(super) fn persist_reuse_ledger_at(path: &Path, ledger: &ReuseLedger) -> Result<()> {
        let ledger_path = path.join(REUSE_LEDGER_FILE);
        if ledger.attempts().is_empty() {
            if ledger_path.exists() {
                fs::remove_file(ledger_path)?;
                sync_directory(path)?;
            }
            return Ok(());
        }
        let bytes = ledger
            .to_bytes()
            .ok_or_else(|| Error::Wal("reuse ledger is too large".into()))?;
        atomic_write_without_fault_injection(&ledger_path, &bytes)
    }

    /// Admit the complete candidate publication before the first page write.
    ///
    /// Individual atomic artifacts reserve their own temporary files later in
    /// publication. A filesystem can therefore report ENOSPC after the data
    /// page generation has already reached the device unless their aggregate
    /// footprint is checked first. This is a conservative same-filesystem
    /// guard; a concurrent external consumer can still force the final
    /// write-time DiskFull/recovery path.
    pub(super) fn preflight_publication_capacity(&self) -> Result<()> {
        let dirty_page_count = self
            .engine
            .btree()
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| self.engine.btree().node(*page_id).is_some())
            .count() as u64;
        let reused_page_count = self.engine.pending_reuse_offsets().len() as u64;
        let new_page_count = dirty_page_count.saturating_sub(reused_page_count);

        let pmt_bytes = (self.engine.pmt().to_bytes().len() as u64)
            .checked_add(
                dirty_page_count
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let allocator_bytes = (self.engine.allocator().to_bytes().len() as u64)
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
            self.generation_meta_bytes(parent, dirty_page_count as usize)?;
        let legacy_meta_bytes = if self.path.join(META_FILE).is_file() {
            0
        } else {
            full_meta_bytes
        };
        let blob_bytes = Self::blob_publication_size(&self.blobs)?;
        let ledger_bytes = self
            .reuse_ledger
            .to_bytes()
            .ok_or_else(|| Error::Wal("reuse ledger is too large".into()))?
            .len() as u64;
        let history_entry_bytes = ManifestHistory::entry_bytes(Manifest {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: GenerationId::new(0),
            commit_id: CommitId::new(0),
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
        let history_length = match fs::metadata(self.path.join(MANIFEST_HISTORY_FILE)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => self
                .manifest_history
                .to_bytes()
                .map_or(0, |bytes| bytes.len() as u64),
            Err(error) => return Err(error.into()),
        };
        let history_bytes = history_length
            .checked_add(history_entry_bytes)
            .ok_or(Error::DiskFull)?;
        let required = new_page_count
            .checked_mul(PAGE_SIZE as u64)
            .and_then(|size| size.checked_add(checkpoint_meta_bytes))
            .and_then(|size| size.checked_add(legacy_meta_bytes))
            .and_then(|size| size.checked_add(blob_bytes))
            .and_then(|size| size.checked_add(ledger_bytes))
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
        let ledger_bytes = self
            .reuse_ledger
            .to_bytes()
            .ok_or_else(|| Error::Wal("reuse ledger is too large".into()))?
            .len() as u64;
        let history_entry_bytes =
            ManifestHistory::entry_bytes(self.bootstrap_manifest()).len() as u64;
        let history_length = match fs::metadata(self.path.join(MANIFEST_HISTORY_FILE)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => self
                .manifest_history
                .to_bytes()
                .map_or(0, |bytes| bytes.len() as u64),
            Err(error) => return Err(error.into()),
        };
        let history_bytes = history_length
            .checked_add(history_entry_bytes)
            .ok_or(Error::DiskFull)?;
        let required = data_bytes
            .checked_add(metadata_bytes)
            .and_then(|size| size.checked_add(blob_bytes))
            .and_then(|size| size.checked_add(ledger_bytes))
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
        let allocator_bytes = candidate.page_allocator().to_bytes().len() as u64;
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
            .and_then(|size| size.checked_add(allocator.to_bytes().len() as u64))
            .and_then(|size| size.checked_add(META_DELTA_CHECKSUM_SIZE as u64))
            .ok_or(Error::DiskFull)
    }

    pub(super) fn append_manifest_history(&self, manifest: Manifest) -> Result<u64> {
        self.append_manifest_history_with_directory_sync(manifest, true)
    }

    pub(super) fn append_manifest_history_without_directory_sync(
        &self,
        manifest: Manifest,
    ) -> Result<u64> {
        self.append_manifest_history_with_directory_sync(manifest, false)
    }

    fn append_manifest_history_with_directory_sync(
        &self,
        manifest: Manifest,
        sync_parent: bool,
    ) -> Result<u64> {
        let path = self.path.join(MANIFEST_HISTORY_FILE);
        let existing_len = fs::metadata(&path)?.len();
        let header_len = u64::try_from(ManifestHistory::header_bytes().len())
            .map_err(|_| Error::Corruption("manifest history header is too large".into()))?;
        let entry_len = u64::try_from(ManifestHistory::entry_bytes(manifest).len())
            .map_err(|_| Error::Corruption("manifest history entry is too large".into()))?;
        if existing_len < header_len {
            return Err(Error::Corruption("manifest history is truncated".into()));
        }

        // A crash may leave a partial final frame. Remove only that tail before
        // appending so recovery never has to scan through a misaligned frame.
        let complete_len = header_len + (existing_len - header_len) / entry_len * entry_len;
        let mut bytes_written = 0u64;
        if complete_len != existing_len {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(complete_len)?;
            file.sync_all()?;
            if sync_parent {
                sync_directory(&self.path)?;
            }
        }

        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        let entry = ManifestHistory::entry_bytes(manifest);
        file.write_all(&entry)?;
        bytes_written = bytes_written.saturating_add(entry.len() as u64);
        file.flush()?;
        file.sync_all()?;
        if sync_parent {
            sync_directory(&self.path)?;
        } else {
            // The caller owns the final publication-directory barrier.
        }
        Ok(bytes_written)
    }

    /// Make both manifest slots name the latest durable generation before a
    /// new generation may reuse pages from older slots.
    pub(super) fn mirror_current_manifest(&mut self) -> Result<()> {
        if let Some(current) = self.manifest.load_latest()? {
            self.manifest.publish_mirrored(current)?;
        }
        Ok(())
    }
}
