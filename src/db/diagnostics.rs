//! DB-level integrity diagnostics.
//!
//! This module owns the read-only verification and offline-check lifecycle:
//! it validates the open database identity against its manifest, delegates
//! page and tree checks to `StorageEngine`, validates DB-owned blob/checkpoint
//! artifacts, and classifies the WAL frontier. Public report types remain in
//! `db.rs`; lower-level page and graph verification remains in
//! `src/storage/verification.rs`.

use super::blob_layout::blob_storage_size;
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
            Error::Wal(message) => Error::Check {
                kind: CheckFailureKind::Wal,
                message,
            },
            Error::Blob(error) => Error::Check {
                kind: CheckFailureKind::Blob,
                message: error.to_string(),
            },
            Error::Buffer(message) => Error::Check {
                kind: CheckFailureKind::Runtime,
                message,
            },
            Error::BTree(message) => Error::Check {
                kind: CheckFailureKind::Structure,
                message,
            },
            Error::SnapshotUnavailable(message) => Error::Check {
                kind: CheckFailureKind::Checkpoint,
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
        let manifest = self.load_verified_manifest()?;
        let (verified_pages, data_bytes, blob_pointers) =
            self.verify_active_generation(&manifest)?;
        self.verify_blob_pointers(&blob_pointers)?;
        self.verify_checkpoint_artifact(&manifest)?;
        let blob_bytes = self.verify_blob_artifact()?;
        let wal_bytes = self.verify_wal_artifact()?;

        Ok(VerificationReport {
            durability: self.durability_status(),
            verified_pages,
            data_bytes,
            blob_bytes,
            wal_bytes,
            reclaimable_pages: self.engine.reclaimable_page_count() as u64,
        })
    }

    fn load_verified_manifest(&mut self) -> std::result::Result<Manifest, VerificationFailure> {
        let manifest = self
            .manifest_history
            .latest()
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
        Ok(manifest)
    }

    fn verify_active_generation(
        &mut self,
        manifest: &Manifest,
    ) -> std::result::Result<(u64, u64, Vec<BlobPointer>), VerificationFailure> {
        let (verified_pages, data_bytes) = self
            .engine
            .verify_pages(manifest.root_page_id)
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::DataPage, error))?;
        // Check the coordinator relationships after the durable page boundary.
        // A truncated data file can make the derived allocation frontier
        // unaligned; the missing page is the actionable failure for check and
        // repair callers, while runtime validation still classifies unrelated
        // in-memory ownership and reclamation defects.
        self.engine
            .validate_runtime_state()
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Runtime, error))?;
        let blob_pointers = self
            .engine
            .verify_tree(manifest.root_page_id)
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Structure, error))?;
        Ok((verified_pages, data_bytes, blob_pointers))
    }

    fn verify_blob_pointers(
        &self,
        blob_pointers: &[BlobPointer],
    ) -> std::result::Result<(), VerificationFailure> {
        for pointer in blob_pointers {
            if self.blobs.read(pointer).is_none() {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Blob,
                    message: format!(
                        "blob pointer target is missing: file {}, offset {}, length {}",
                        pointer.file_id, pointer.offset, pointer.length
                    ),
                });
            }
        }
        Ok(())
    }

    fn verify_checkpoint_artifact(
        &self,
        manifest: &Manifest,
    ) -> std::result::Result<(), VerificationFailure> {
        if manifest.pmt_checkpoint_id.get() == 0 {
            return Ok(());
        }

        let (checkpoint_pmt, checkpoint_allocator, _) = self
            .load_meta_by_id(manifest.pmt_checkpoint_id.get())
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
        Ok(())
    }

    fn verify_blob_artifact(&self) -> std::result::Result<u64, VerificationFailure> {
        let blob_path = self.path.join(BLOB_FILE);
        if !blob_path.exists() {
            return Ok(0);
        }

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
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Blob, error))
    }

    fn verify_wal_artifact(&self) -> std::result::Result<u64, VerificationFailure> {
        let wal_path = self.path.join(WAL_FILE);
        if !wal_path.exists() {
            return Ok(0);
        }

        let bytes = fs::read(&wal_path).map_err(|error| {
            VerificationFailure::from_error(CheckFailureKind::Wal, error.into())
        })?;
        let (_, status) = WalManager::parse_records_with_status(&bytes);
        if status == ParseStatus::Corrupt {
            // Once a valid authority frame exists, an unsynced WAL suffix is
            // non-authoritative. Validate the complete prefix/frontier, but
            // let writable open reconcile the corrupt suffix and let check
            // report the authoritative database instead of rejecting it.
            if let Some(current) = self.manifest_history.latest() {
                super::wal_recovery::analyze_wal_bytes(&bytes, Some(current)).map_err(|error| {
                    VerificationFailure::from_error(CheckFailureKind::Wal, error)
                })?;
                return Ok(bytes.len() as u64);
            }
        }
        if !self.check_only && status != ParseStatus::Complete {
            return Err(VerificationFailure {
                kind: CheckFailureKind::Wal,
                message: format!("WAL integrity status is {status:?}"),
            });
        }
        if status == ParseStatus::Corrupt {
            return Err(VerificationFailure {
                kind: CheckFailureKind::Wal,
                message: format!("WAL integrity status is {status:?}"),
            });
        }
        Ok(bytes.len() as u64)
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

        let current_manifest = self
            .manifest_history
            .latest()
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        if status == ParseStatus::Corrupt {
            let analysis = super::wal_recovery::analyze_wal_bytes(&bytes, Some(current_manifest))?;
            return Ok(if analysis.has_unpublished_commit {
                WalCheckStatus::NeedsRecovery
            } else {
                WalCheckStatus::Incomplete
            });
        }

        let mut pending = Vec::new();
        let mut saw_unpublished_commit = false;
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
                        // Published generations are retained by design; only
                        // commits ahead of the authority would replay. Older
                        // generations with consistent frontiers are inert.
                        std::cmp::Ordering::Greater => {
                            saw_unpublished_commit = true;
                        }
                        std::cmp::Ordering::Less => {}
                    }
                    pending.clear();
                }
                _ => {}
            }
        }

        // A trailing mutation prefix without a commit envelope is pending
        // work regardless of published records retained before it.
        let has_trailing_pending = !pending.is_empty();
        match status {
            ParseStatus::Incomplete => Ok(WalCheckStatus::Incomplete),
            ParseStatus::Complete if records.is_empty() => Ok(WalCheckStatus::Clean),
            ParseStatus::Complete if saw_unpublished_commit => Ok(WalCheckStatus::NeedsRecovery),
            ParseStatus::Complete if has_trailing_pending => Ok(WalCheckStatus::Pending),
            ParseStatus::Complete => Ok(WalCheckStatus::Clean),
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
