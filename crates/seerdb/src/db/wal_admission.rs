//! WAL journaling and physical reservation helpers for `DB`.
//!
//! This module owns the WAL-side admission and journaling lifecycle. `DB`
//! remains the mutable state owner; publication artifact admission and
//! persistence remain in `durability.rs`, and publication ordering remains in
//! `publication.rs`.

use super::artifact_io::sync_directory;
use super::wal_recovery::extend_digest;
use super::{
    DB, Error, Result, WAL_COMMIT_RECORD_BYTES, WAL_FILE, WAL_RESERVATION_SEGMENT_BYTES,
    elapsed_nanos,
};
#[cfg(any(test, feature = "fault-injection"))]
use super::{
    FAIL_NEXT_WAL_AFTER_SYNC, FAIL_NEXT_WAL_AFTER_WRITE, FAIL_NEXT_WAL_SYNC, FAIL_NEXT_WAL_WRITE,
};
use crate::recovery::{SyncPolicy, WalRecord};
use crate::space::reserve_file;
use std::fs::{File, OpenOptions};
use std::io::Write;
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
            if !wal_buf.is_empty() {
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_WRITE.with(|failure| failure.replace(false)) {
                    self.invalidate_wal_handle();
                    return Err(std::io::Error::other("injected WAL append failure").into());
                }
                let appended = (|| {
                    let file = self.wal_append_handle()?;
                    let mut file = file;
                    file.write_all(&wal_buf)
                })();
                if let Err(error) = appended {
                    self.invalidate_wal_handle();
                    return Err(error.into());
                }
                self.publication.wal_bytes_written = self
                    .publication
                    .wal_bytes_written
                    .saturating_add(wal_buf.len() as u64);
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_AFTER_WRITE.with(|failure| failure.replace(false)) {
                    self.invalidate_wal_handle();
                    return Err(std::io::Error::other("injected post-append WAL failure").into());
                }
            }
            if should_sync {
                // Explicit sync policy and requested force boundaries make
                // the WAL durable. The default out-of-place publication keeps
                // its commit prefix buffered until the authority frame.
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_SYNC.with(|failure| failure.replace(false)) {
                    self.invalidate_wal_handle();
                    return Err(std::io::Error::other("injected WAL sync failure").into());
                }
                let synced = (|| {
                    let policy = self.wal.sync_policy();
                    let file = self.wal_append_handle()?;
                    match policy {
                        SyncPolicy::SyncAll => file.sync_all(),
                        SyncPolicy::FDataSync | SyncPolicy::None => file.sync_data(),
                    }
                })();
                if let Err(error) = synced {
                    self.invalidate_wal_handle();
                    return Err(error.into());
                }
                crate::storage::record_durability_sync();
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_AFTER_SYNC.with(|failure| failure.replace(false)) {
                    self.invalidate_wal_handle();
                    return Err(std::io::Error::other("injected post-WAL-sync failure").into());
                }
            }
        }
        Ok(())
    }

    /// Return the cached WAL append handle, opening the file on first use.
    /// The handle is cached across publications because open-per-append
    /// syscalls dominate the per-record cost of the WAL path.
    fn wal_append_handle(&mut self) -> std::io::Result<&File> {
        if self.wal_handle.is_none() {
            let wal_path = self.path.join(WAL_FILE);
            self.wal_handle = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&wal_path)?,
            );
        }
        Ok(self.wal_handle.as_ref().expect("wal handle just set"))
    }

    /// Drop the cached WAL handle so the next append reopens the file.
    /// Required after reclaim removes the file and after any I/O error that
    /// may have left the descriptor in an unreliable state.
    pub(super) fn invalidate_wal_handle(&mut self) {
        self.wal_handle = None;
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
            crate::storage::record_durability_sync();
            sync_directory(&self.path)?;
        }
        if current == 0 {
            // A retained WAL file inherits its segment from the authority
            // manifest. A newly created file must carry its own segment so a
            // crash after WAL recreation can recover an LSN newer than the
            // previous manifest even when the old WAL file was reclaimed.
            self.wal.append(&WalRecord::wal_segment(self.wal_segment));
            self.write_wal_to_disk(true)?;
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
}
