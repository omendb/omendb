use super::open::OpenPaths;
use super::*;

pub(super) struct OpenCatalog {
    pub(super) read_only: bool,
    pub(super) check_only: bool,
    pub(super) lock_file: Option<File>,
    pub(super) current_manifest: Option<Manifest>,
    pub(super) manifest_history: ManifestHistory,
    pub(super) reuse_ledger: ReuseLedger,
    pub(super) database_id: DatabaseId,
    pub(super) history_id: HistoryId,
    pub(super) generation_id: GenerationId,
    pub(super) commit_id: CommitId,
    pub(super) next_commit_id: CommitId,
    pub(super) next_generation_id: GenerationId,
    pub(super) retention: Arc<Mutex<RetentionState>>,
    pub(super) protected_offsets: Arc<Mutex<HashSet<u64>>>,
}

struct OpenRetention {
    state: Arc<Mutex<RetentionState>>,
    protected_offsets: Arc<Mutex<HashSet<u64>>>,
}

impl DB {
    pub(super) fn load_open_catalog(paths: &OpenPaths, mode: OpenMode) -> Result<OpenCatalog> {
        let check_only = mode == OpenMode::Check;
        let archive = paths.path.join(ARCHIVE_MARKER_FILE).is_file();
        let read_only = check_only || archive;

        if mode == OpenMode::Normal
            && paths.path_preexisted
            && !paths.meta_log.is_file()
            && !paths.data.is_file()
            && !paths.wal.is_file()
            && !paths.meta.is_file()
        {
            return Err(Error::Corruption(format!(
                "existing database path has no authoritative storage artifacts: {}",
                paths.path.display()
            )));
        }
        if check_only && (!paths.meta_log.is_file() || !paths.data.is_file()) {
            return Err(Error::Check {
                kind: CheckFailureKind::Target,
                message: "check target is missing required manifest or data artifacts".into(),
            });
        }
        if archive && !check_only {
            if !paths.meta_log.is_file() || !paths.data.is_file() {
                return Err(Error::Corruption(
                    "read-only archive is missing required artifacts".into(),
                ));
            }
            if paths.wal.exists() {
                return Err(Error::NeedsRecovery(
                    "read-only archive contains a pending WAL".into(),
                ));
            }
        }

        let lock_file = if read_only {
            None
        } else {
            Some(Self::acquire_writer_lock(&paths.path.join(LOCK_FILE))?)
        };
        if !read_only {
            cleanup_orphaned_temporary_artifacts(&paths.path)?;
            clear_blob_reservation(&paths.path)?;
            clear_wal_reservation(&paths.path)?;
        }

        if check_only && !paths.meta_log.is_file() {
            return Err(Error::Check {
                kind: CheckFailureKind::Manifest,
                message: "metadata log is missing".into(),
            });
        }
        let parsed_meta_log = DB::read_meta_log(&paths.path).map_err(|error| {
            if check_only {
                Self::map_check_error(CheckFailureKind::Manifest, error)
            } else {
                error
            }
        })?;
        // A log whose valid prefix contains no publication frames is either
        // header-only (never published) or corrupt past its header.
        if let Some(parsed) = parsed_meta_log.as_ref()
            && parsed.frames.is_empty()
            && !parsed.complete
        {
            return Err(Error::Corruption(
                "metadata log has no valid publication frames".into(),
            ));
        }
        if paths.data.is_file()
            && parsed_meta_log
                .as_ref()
                .is_none_or(|parsed| parsed.frames.is_empty())
            && !paths.wal.is_file()
        {
            // Page data without any authority frame or WAL must never be
            // reinterpreted as a fresh database.
            return Err(Error::Corruption(
                "data file has no authoritative metadata log or WAL".into(),
            ));
        }
        let current_manifest = match parsed_meta_log.as_ref() {
            Some(parsed) => DB::select_authority_manifest(parsed).map_err(|error| {
                if check_only {
                    Self::map_check_error(CheckFailureKind::Manifest, error)
                } else {
                    error
                }
            })?,
            None => None,
        };
        Self::validate_selected_artifacts(paths, current_manifest, check_only)?;
        if check_only && current_manifest.is_none() {
            return Err(Error::Check {
                kind: CheckFailureKind::Manifest,
                message: "check target has no valid manifest generation".into(),
            });
        }

        let (database_id, history_id, generation_id, commit_id) =
            Self::open_identities(&paths.path, current_manifest)?;
        // The authority log is the only history source: every valid
        // publication frame contributes its manifest.
        let mut manifest_history = parsed_meta_log
            .as_ref()
            .map(DB::publication_manifests)
            .unwrap_or_default()
            .into_iter()
            .fold(ManifestHistory::new(), |mut history, manifest| {
                let _ = history.push(manifest);
                history
            });
        if let Some(current) = current_manifest {
            manifest_history
                .reconcile_current(current)
                .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        }
        let mut reuse_ledger = Self::load_reuse_ledger(paths, current_manifest, check_only)?;
        let pruned_reuse_attempts = reuse_ledger.prune_published(&manifest_history);
        if pruned_reuse_attempts > 0 && !read_only && !check_only {
            Self::persist_reuse_ledger_at(&paths.path, &reuse_ledger)?;
        }
        let (next_commit_id, next_generation_id) =
            Self::next_open_identities(commit_id, generation_id, &reuse_ledger)?;
        let open_retention =
            Self::load_retention(paths, database_id, history_id, read_only, check_only)?;

        Ok(OpenCatalog {
            read_only,
            check_only,
            lock_file,
            current_manifest,
            manifest_history,
            reuse_ledger,
            database_id,
            history_id,
            generation_id,
            commit_id,
            next_commit_id,
            next_generation_id,
            retention: open_retention.state,
            protected_offsets: open_retention.protected_offsets,
        })
    }

    fn validate_selected_artifacts(
        paths: &OpenPaths,
        current_manifest: Option<Manifest>,
        check_only: bool,
    ) -> Result<()> {
        let Some(current) = current_manifest else {
            return Ok(());
        };
        if !paths.data.is_file() {
            let error = Error::Corruption(format!(
                "manifest generation {} is missing the data file",
                current.generation_id.get()
            ));
            return Err(if check_only {
                Self::map_check_error(CheckFailureKind::Target, error)
            } else {
                error
            });
        }
        Ok(())
    }

    fn open_identities(
        path: &Path,
        current_manifest: Option<Manifest>,
    ) -> Result<(DatabaseId, HistoryId, GenerationId, CommitId)> {
        if let Some(current) = current_manifest {
            if current.page_size as usize != PAGE_SIZE {
                return Err(Error::Corruption(format!(
                    "manifest page size {} does not match build page size {PAGE_SIZE}",
                    current.page_size
                )));
            }
            return Ok((
                current.database_id,
                current.history_id,
                current.generation_id,
                current.commit_id,
            ));
        }
        Ok((
            Self::new_database_id(path),
            HistoryId::new(1),
            GenerationId::new(0),
            CommitId::new(0),
        ))
    }

    fn load_reuse_ledger(
        paths: &OpenPaths,
        current_manifest: Option<Manifest>,
        check_only: bool,
    ) -> Result<ReuseLedger> {
        let ledger_path = paths.path.join(REUSE_LEDGER_FILE);
        let ledger = if ledger_path.is_file() {
            let bytes = fs::read(&ledger_path)?;
            ReuseLedger::scan_latest(&bytes).map_err(|message| {
                let error = Error::Corruption(format!("reuse ledger {message}"));
                if check_only {
                    Self::map_check_error(CheckFailureKind::Format, error)
                } else {
                    error
                }
            })?
        } else {
            ReuseLedger::new()
        };
        if current_manifest.is_none() && !ledger.attempts().is_empty() {
            return Err(Error::Corruption(
                "reuse ledger exists without an authoritative manifest".into(),
            ));
        }
        Ok(ledger)
    }

    fn next_open_identities(
        commit_id: CommitId,
        generation_id: GenerationId,
        reuse_ledger: &ReuseLedger,
    ) -> Result<(CommitId, GenerationId)> {
        let mut next_commit_id = CommitId::new(
            commit_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
        );
        let mut next_generation_id = GenerationId::new(
            generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        for attempt in reuse_ledger.attempts() {
            let reserved_commit = CommitId::new(
                attempt
                    .commit_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
            );
            let reserved_generation = GenerationId::new(
                attempt
                    .generation_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
            );
            next_commit_id = next_commit_id.max(reserved_commit);
            next_generation_id = next_generation_id.max(reserved_generation);
        }
        Ok((next_commit_id, next_generation_id))
    }

    fn load_retention(
        paths: &OpenPaths,
        database_id: DatabaseId,
        history_id: HistoryId,
        read_only: bool,
        check_only: bool,
    ) -> Result<OpenRetention> {
        let protected_offsets = Arc::new(Mutex::new(HashSet::new()));
        let retention = Arc::new(Mutex::new(
            RetentionState::load(
                paths.path.join(RETENTION_FILE),
                Arc::clone(&protected_offsets),
            )
            .map_err(|error| {
                if check_only {
                    Self::map_check_error(CheckFailureKind::Format, error)
                } else {
                    error
                }
            })?,
        ));
        if !read_only && !check_only {
            Self::cleanup_orphaned_retained_blobs(&paths.path, &retention)?;
        }
        let offsets = {
            let state = retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            Self::load_retained_offset_map(&paths.path, &state, database_id, history_id).map_err(
                |error| {
                    if check_only {
                        Self::map_check_error(CheckFailureKind::Checkpoint, error)
                    } else {
                        error
                    }
                },
            )?
        };
        retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?
            .install_offsets(offsets)
            .map_err(|error| {
                if check_only {
                    Self::map_check_error(CheckFailureKind::Checkpoint, error)
                } else {
                    error
                }
            })?;
        Ok(OpenRetention {
            state: retention,
            protected_offsets,
        })
    }
}
