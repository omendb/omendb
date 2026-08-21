//! Durable artifact persistence helpers for `DB`.
//!
//! This module owns authority-frame publication and reuse-ledger persistence.
//! Capacity and reservation policy lives in `capacity.rs`; WAL admission and
//! journaling live in `wal_admission.rs`. `DB` remains the mutable state
//! owner and `publication.rs` remains the publication-ordering authority.

use super::artifact_io::{atomic_write_without_fault_injection, sync_directory};
use super::{DB, Error, REUSE_LEDGER_FILE, Result};
use crate::storage::format::{Manifest, ReuseLedger};
use std::fs;
use std::path::Path;

impl DB {
    /// Append and sync the authority frame for a maintenance generation.
    ///
    /// The caller has already synced every page and blob byte the manifest
    /// names; this frame is the visibility barrier that selects them.
    pub(super) fn publish_authority_frame(&mut self, manifest: Manifest) -> Result<()> {
        let parent = self
            .manifest_history
            .latest()
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let bytes = manifest.to_bytes();
        let (_frame_bytes, meta_log_created) = self.append_generation_meta(
            manifest.generation_id.get(),
            parent.pmt_checkpoint_id.get(),
            &bytes,
        )?;
        if meta_log_created {
            sync_directory(&self.path)?;
        }
        let mut history = self.manifest_history.clone();
        history
            .push(manifest)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        self.manifest_history = history;
        Ok(())
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
}
