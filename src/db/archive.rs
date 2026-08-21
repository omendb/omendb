//! Archive, snapshot, and repair lifecycle ownership.
//!
//! This module owns directory-copy workflows that validate a source,
//! materialize a private destination, reopen and verify it, and publish the
//! result with an atomic rename. `DB` remains the authority for mutable state,
//! manifest publication, and WAL recovery; this module only coordinates those
//! existing boundaries for archive-style operations.

use super::{
    ARCHIVE_MARKER_FILE, BLOB_DELTA_FILE, BLOB_FILE, BLOB_SEGMENT_PREFIX, DATA_FILE, DB, Error,
    LOCK_FILE, META_FILE, Options, REUSE_LEDGER_FILE, RepairAction, RepairReport, RestoreReport,
    Result, SnapshotReport, WAL_FILE, sync_directory,
};
use crate::storage::format::{GenerationId, HistoryId, Manifest, PmtCheckpointId};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

impl DB {
    /// Flush and create an atomically published, independently verified
    /// snapshot in a new directory without mutating the source directory.
    pub fn snapshot<P: AsRef<Path>>(&mut self, destination: P) -> Result<SnapshotReport> {
        self.check_writable()?;
        self.flush()?;
        let source_report = self.verify()?;
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot destination already exists: {}",
                destination.display()
            )));
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = destination.with_extension("seerdb.snapshot.tmp");
        if temporary.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot temporary path already exists: {}",
                temporary.display()
            )));
        }

        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_snapshot_artifacts(&self.path, &temporary)?;
            let marker_path = temporary.join(ARCHIVE_MARKER_FILE);
            fs::write(&marker_path, b"SEERDB-ARCHIVE-V1\n")?;
            File::open(&marker_path)?.sync_all()?;
            sync_directory(&temporary)?;

            let mut restored = DB::open(&temporary, self.options.clone())?;
            let restored_report = restored.verify()?;
            if restored_report.durability != source_report.durability
                || restored_report.verified_pages != source_report.verified_pages
            {
                return Err(Error::Corruption(
                    "snapshot verification does not match source durability state".into(),
                ));
            }
            let destination_status = restored_report.durability;
            drop(restored);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(SnapshotReport {
                source: source_report.durability,
                destination: destination_status,
                copied_files,
                verified_pages: restored_report.verified_pages,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Restore an immutable archive into a new writable history.
    ///
    /// The archive is verified before copying. The destination receives a new
    /// `HistoryId` while preserving the source's database identity and
    /// durable root, so it can advance independently without sharing future
    /// history IDs with the archive.
    pub fn restore<P: AsRef<Path>, Q: AsRef<Path>>(
        archive: P,
        destination: Q,
        options: Options,
    ) -> Result<RestoreReport> {
        let archive = archive.as_ref().to_path_buf();
        if !archive.join(ARCHIVE_MARKER_FILE).is_file() {
            return Err(Error::InvalidArgument(
                "restore source is not an immutable SeerDB archive".into(),
            ));
        }
        let mut source = DB::open(&archive, options.clone())?;
        let source_report = source.verify()?;
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "restore destination already exists: {}",
                destination.display()
            )));
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = Self::next_derived_path(&destination, "restore")?;
        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_snapshot_artifacts(&archive, &temporary)?;
            sync_directory(&temporary)?;

            let mut restored = DB::open(&temporary, options.clone())?;
            // Forking the history publishes one new physical generation past
            // the restored frontier (which reserved IDs may advance beyond
            // the archive root); the logical root must match the archive's.
            let forked_from = restored.durability_status().generation_id;
            restored.fork_history()?;
            let restored_report = restored.verify()?;
            if restored_report.durability.database_id != source_report.durability.database_id
                || restored_report.durability.commit_id != source_report.durability.commit_id
                || restored_report.durability.generation_id.get() <= forked_from.get()
                || restored_report.verified_pages != source_report.verified_pages
            {
                return Err(Error::Corruption(
                    "restored history does not match archive root".into(),
                ));
            }
            let destination_status = restored_report.durability;
            drop(restored);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(RestoreReport {
                source: source_report.durability,
                destination: destination_status,
                copied_files,
                verified_pages: restored_report.verified_pages,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Rebuild a checked database into a new writable history without
    /// mutating the source directory.
    ///
    /// Unlike [`DB::restore`], this operation copies the source WAL as well as
    /// the durable generation. The destination opens normally, so committed
    /// WAL prefixes are reconciled (and replayed when they advance the
    /// manifest), while uncommitted or torn suffixes are reconciled there. The
    /// source is held under a shared advisory lock when its writer lock exists;
    /// an active writer therefore receives [`Error::DatabaseBusy`] instead of
    /// being copied concurrently.
    pub fn repair<P: AsRef<Path>, Q: AsRef<Path>>(
        source: P,
        destination: Q,
        options: Options,
    ) -> Result<RepairReport> {
        let source = source.as_ref().to_path_buf();
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "repair destination already exists: {}",
                destination.display()
            )));
        }

        let _source_lock = Self::acquire_source_shared_lock(&source)?;
        let source_check = DB::check(&source, options.clone())?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = Self::next_derived_path(&destination, "repair")?;
        let action = match source_check.wal_status {
            super::WalCheckStatus::Clean => RepairAction::NoRepair,
            super::WalCheckStatus::Pending => RepairAction::DiscardedUncommittedWal,
            super::WalCheckStatus::NeedsRecovery => RepairAction::ReconciledCommittedWal,
            super::WalCheckStatus::Incomplete => RepairAction::ReconciledIncompleteWal,
        };

        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_repair_artifacts(&source, &temporary)?;
            sync_directory(&temporary)?;

            let mut repaired = DB::open(&temporary, options.clone())?;
            repaired.fork_history()?;
            let repaired_report = repaired.verify()?;
            if repaired_report.durability.database_id
                != source_check.verification.durability.database_id
            {
                return Err(Error::Corruption(
                    "repaired history changed the database identity".into(),
                ));
            }
            let destination_status = repaired_report.durability;
            drop(repaired);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(RepairReport {
                source: source_check.verification.durability,
                source_wal_status: source_check.wal_status,
                destination: destination_status,
                copied_files,
                verified_pages: repaired_report.verified_pages,
                action,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    pub(super) fn next_snapshot_path(&self) -> Result<PathBuf> {
        Self::next_derived_path(&self.path, "snapshot")
    }

    fn next_derived_path(path: &Path, kind: &str) -> Result<PathBuf> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("seerdb");
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let id = NEXT_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
        let destination = parent.join(format!(
            ".{name}.{kind}-{}-{timestamp}-{id}",
            std::process::id()
        ));
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot destination already exists: {}",
                destination.display()
            )));
        }
        Ok(destination)
    }

    fn fork_history(&mut self) -> Result<()> {
        self.check_writable()?;
        let current = self
            .manifest_history
            .latest()
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let history_id = HistoryId::new(
            current
                .history_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("history ID overflow".into()))?,
        );
        // The forked history needs a fresh generation so its authority frame
        // has a unique checkpoint ID in the copied log.
        let generation_id = self.next_generation_id;
        let forked = Manifest {
            history_id,
            generation_id,
            pmt_checkpoint_id: PmtCheckpointId::new(generation_id.get()),
            ..current
        };
        self.publish_authority_frame(forked)?;
        self.history_id = history_id;
        self.generation_id = generation_id;
        self.next_generation_id = GenerationId::new(
            generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        Ok(())
    }
}

fn copy_snapshot_artifacts(source: &Path, destination: &Path) -> Result<u32> {
    copy_artifacts(source, destination, false)
}

fn copy_repair_artifacts(source: &Path, destination: &Path) -> Result<u32> {
    copy_artifacts(source, destination, true)
}

fn copy_artifacts(source: &Path, destination: &Path, include_wal: bool) -> Result<u32> {
    let mut copied_files = 0u32;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".tmp")
            || name == LOCK_FILE
            || name == ARCHIVE_MARKER_FILE
            || !(name == REUSE_LEDGER_FILE
                || name == DATA_FILE
                || name == BLOB_FILE
                || name == BLOB_DELTA_FILE
                || name.starts_with(BLOB_SEGMENT_PREFIX)
                || name == META_FILE
                || name.starts_with("seerdb.meta.")
                || (include_wal && name == WAL_FILE))
        {
            continue;
        }

        let destination_file = destination.join(name.as_ref());
        fs::copy(entry.path(), &destination_file)?;
        OpenOptions::new()
            .read(true)
            .open(destination_file)?
            .sync_all()?;
        copied_files = copied_files
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("too many snapshot artifacts".into()))?;
    }

    Ok(copied_files)
}
