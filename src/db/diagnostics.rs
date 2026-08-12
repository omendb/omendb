//! DB-level integrity diagnostics.
//!
//! This module owns the read-only verification and offline-check lifecycle:
//! it validates the open database identity against its manifest, delegates
//! page and tree checks to `StorageEngine`, validates DB-owned blob/checkpoint
//! artifacts, and classifies the WAL frontier. Public report types remain in
//! `db.rs`; lower-level page and graph verification remains in
//! `src/storage/verification.rs`.

use super::*;

#[derive(Debug)]
struct VerificationFailure {
    kind: CheckFailureKind,
    message: String,
}

impl VerificationFailure {
    fn from_error(kind: CheckFailureKind, error: Error) -> Self {
        Self {
            kind,
            message: error_message(error),
        }
    }

    fn into_error(self) -> Error {
        Error::Check {
            kind: self.kind,
            message: self.message,
        }
    }
}

impl DB {
    /// Check an existing database without taking writer ownership or
    /// replaying, truncating, or publishing its WAL.
    pub fn check<P: AsRef<Path>>(path: P, options: Options) -> Result<CheckReport> {
        let mut db = Self::open_with_mode(path, options, OpenMode::Check)
            .map_err(Self::map_check_open_error)?;
        let verification = db.verify_inner().map_err(VerificationFailure::into_error)?;
        let wal_status = db
            .wal_check_status()
            .map_err(|error| Self::map_check_error(CheckFailureKind::Wal, error))?;
        Ok(CheckReport {
            verification,
            wal_status,
        })
    }

    fn map_check_open_error(error: Error) -> Error {
        Self::map_check_error(CheckFailureKind::Format, error)
    }

    pub(super) fn map_check_error(default_kind: CheckFailureKind, error: Error) -> Error {
        match error {
            Error::Check { .. } => error,
            Error::InvalidArgument(message) => Error::Check {
                kind: CheckFailureKind::Target,
                message,
            },
            Error::Io(error) => Error::Check {
                kind: CheckFailureKind::Io,
                message: error.to_string(),
            },
            Error::NeedsRecovery(message) => Error::Check {
                kind: CheckFailureKind::Wal,
                message,
            },
            Error::Corruption(message) => Error::Check {
                kind: default_kind,
                message,
            },
            other => other,
        }
    }

    pub(super) fn map_checkpoint_check_error(error: Error) -> Error {
        match error {
            Error::Corruption(message) if message.contains("unsupported meta format version") => {
                Error::Check {
                    kind: CheckFailureKind::Format,
                    message,
                }
            }
            other => Self::map_check_error(CheckFailureKind::Checkpoint, other),
        }
    }

    /// Verify the active manifest, checkpoint, pages, blob file, and WAL.
    ///
    /// This pass does not mutate logical state and is intended for DBNext
    /// check/repair tooling and pre-snapshot validation.
    pub fn verify(&mut self) -> Result<VerificationReport> {
        self.check_readable()?;
        self.verify_inner()
            .map_err(|failure| Error::Corruption(failure.message))
    }

    fn verify_inner(&mut self) -> std::result::Result<VerificationReport, VerificationFailure> {
        self.engine.validate_runtime_state().map_err(|error| {
            VerificationFailure::from_error(CheckFailureKind::Checkpoint, error)
        })?;
        let manifest = self
            .manifest
            .load_latest()
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Manifest, error))?
            .ok_or_else(|| VerificationFailure {
                kind: CheckFailureKind::Manifest,
                message: "database has no valid manifest".into(),
            })?;
        if manifest.database_id != self.database_id
            || manifest.history_id != self.history_id
            || manifest.generation_id != self.generation_id
            || manifest.commit_id != self.commit_id
        {
            return Err(VerificationFailure {
                kind: CheckFailureKind::Manifest,
                message: "manifest identity does not match the open database".into(),
            });
        }

        let (verified_pages, data_bytes) = self
            .engine
            .verify_pages(manifest.root_page_id)
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::DataPage, error))?;
        let blob_pointers = self
            .engine
            .verify_tree(manifest.root_page_id)
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Structure, error))?;
        for pointer in blob_pointers {
            if self.blobs.read(&pointer).is_none() {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Blob,
                    message: format!(
                        "blob pointer target is missing: file {}, offset {}, length {}",
                        pointer.file_id, pointer.offset, pointer.length
                    ),
                });
            }
        }

        if manifest.pmt_checkpoint_id.get() != 0 {
            let checkpoint_path = self
                .path
                .join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
            let (checkpoint_pmt, checkpoint_allocator) = Self::load_meta(&checkpoint_path)
                .map_err(|error| {
                    VerificationFailure::from_error(CheckFailureKind::Checkpoint, error)
                })?;
            if checkpoint_pmt.to_bytes() != self.engine.pmt().to_bytes()
                || checkpoint_allocator.to_bytes() != self.engine.allocator().to_bytes()
            {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Checkpoint,
                    message: "manifest checkpoint does not match active PMT or allocator".into(),
                });
            }
        }

        let blob_path = self.path.join(BLOB_FILE);
        let blob_bytes = if blob_path.exists() {
            let bytes = fs::read(&blob_path).map_err(|error| {
                VerificationFailure::from_error(CheckFailureKind::Blob, error.into())
            })?;
            parse_blob_catalog(&self.path, &bytes, Some(self.generation_id.get()))
                .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Blob, error))?
                .ok_or_else(|| VerificationFailure {
                    kind: CheckFailureKind::Blob,
                    message: "blob catalog failed integrity verification".into(),
                })?;
            blob_storage_size(&self.path)
                .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Blob, error))?
        } else {
            0
        };

        let wal_path = self.path.join(WAL_FILE);
        let wal_bytes = if wal_path.exists() {
            let bytes = fs::read(&wal_path).map_err(|error| {
                VerificationFailure::from_error(CheckFailureKind::Wal, error.into())
            })?;
            let (_, status) = WalManager::parse_records_with_status(&bytes);
            if status == ParseStatus::Corrupt
                || (!self.check_only && status != ParseStatus::Complete)
            {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Wal,
                    message: format!("WAL integrity status is {status:?}"),
                });
            }
            bytes.len() as u64
        } else {
            0
        };

        Ok(VerificationReport {
            durability: self.durability_status(),
            verified_pages,
            data_bytes,
            blob_bytes,
            wal_bytes,
            reclaimable_pages: self.engine.reclaimable_page_count() as u64,
        })
    }

    fn wal_check_status(&mut self) -> Result<WalCheckStatus> {
        let wal_path = self.path.join(WAL_FILE);
        if !wal_path.exists() {
            return Ok(WalCheckStatus::Clean);
        }

        let bytes = fs::read(wal_path)?;
        if bytes.is_empty() {
            return Ok(WalCheckStatus::Clean);
        }

        let (records, status) = WalManager::parse_records_with_status(&bytes);
        if status == ParseStatus::Corrupt {
            return Err(Error::Corruption(
                "offline check found a corrupt WAL record".into(),
            ));
        }

        let current_manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        let mut pending = Vec::new();
        let mut saw_commit = false;
        for record in &records {
            match record.record_type {
                RecordType::Put => {
                    decode_put_payload(false, &record.payload)?;
                    pending.push(record);
                }
                RecordType::PutV2 => {
                    decode_put_payload(true, &record.payload)?;
                    pending.push(record);
                }
                RecordType::Delete => {
                    decode_delete_payload(false, &record.payload)?;
                    pending.push(record);
                }
                RecordType::DeleteV2 => {
                    decode_delete_payload(true, &record.payload)?;
                    pending.push(record);
                }
                RecordType::Commit => {
                    let commit = record
                        .commit_record()
                        .ok_or_else(|| Error::Corruption("invalid WAL commit envelope".into()))?;
                    if commit.mutation_count != pending.len() as u64
                        || commit.digest != digest_records(&pending)
                    {
                        return Err(Error::Corruption(
                            "WAL commit does not match its mutation prefix".into(),
                        ));
                    }

                    match commit
                        .generation_id
                        .get()
                        .cmp(&current_manifest.generation_id.get())
                    {
                        std::cmp::Ordering::Less
                            if commit.commit_id > current_manifest.commit_id =>
                        {
                            return Err(Error::Corruption(
                                "WAL commit frontier is inconsistent with manifest".into(),
                            ));
                        }
                        std::cmp::Ordering::Equal => {
                            if commit.commit_id != current_manifest.commit_id
                                || commit.root_page_id != current_manifest.root_page_id
                                || commit.mutation_count != current_manifest.mutation_count
                                || commit.digest != current_manifest.digest
                            {
                                return Err(Error::Corruption(
                                    "WAL commit disagrees with authoritative manifest".into(),
                                ));
                            }
                        }
                        std::cmp::Ordering::Greater
                            if commit.commit_id <= current_manifest.commit_id =>
                        {
                            return Err(Error::Corruption(
                                "WAL commit frontier is inconsistent with manifest".into(),
                            ));
                        }
                        _ => {}
                    }
                    saw_commit = true;
                    pending.clear();
                }
                _ => {}
            }
        }

        match status {
            ParseStatus::Incomplete => Ok(WalCheckStatus::Incomplete),
            ParseStatus::Complete if records.is_empty() => Ok(WalCheckStatus::Clean),
            ParseStatus::Complete if saw_commit => Ok(WalCheckStatus::NeedsRecovery),
            ParseStatus::Complete => Ok(WalCheckStatus::Pending),
            ParseStatus::Corrupt => unreachable!("corrupt WAL status returned above"),
        }
    }
}

fn error_message(error: Error) -> String {
    match error {
        Error::Corruption(message) => message,
        Error::Check { message, .. } => message,
        other => other.to_string(),
    }
}
