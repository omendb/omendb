//! Durable artifact persistence helpers for `DB`.
//!
//! This module owns manifest-history and reuse-ledger artifact persistence,
//! manifest mirroring, and their directory synchronization. Capacity and
//! reservation policy lives in `capacity.rs`; WAL admission and journaling
//! live in `wal_admission.rs`. `DB` remains the mutable state owner and
//! `publication.rs` remains the publication-ordering authority.

use super::artifact_io::{
    atomic_write, atomic_write_without_directory_sync, atomic_write_without_fault_injection,
    sync_directory,
};
use super::{DB, Error, MANIFEST_HISTORY_FILE, REUSE_LEDGER_FILE, Result};
use crate::storage::format::{Manifest, ManifestHistory, ReuseLedger};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

impl DB {
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
