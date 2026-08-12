use super::*;

struct OpenPaths {
    path: PathBuf,
    path_preexisted: bool,
    data: PathBuf,
    wal: PathBuf,
    meta: PathBuf,
    manifest: PathBuf,
}

struct OpenCatalog {
    read_only: bool,
    check_only: bool,
    lock_file: Option<File>,
    manifest: ManifestStore,
    current_manifest: Option<Manifest>,
    manifest_history: ManifestHistory,
    reuse_ledger: ReuseLedger,
    database_id: DatabaseId,
    history_id: HistoryId,
    generation_id: GenerationId,
    commit_id: CommitId,
    next_commit_id: CommitId,
    next_generation_id: GenerationId,
    retention: Arc<Mutex<RetentionState>>,
    protected_offsets: Arc<Mutex<HashSet<u64>>>,
}

struct OpenComponents {
    engine: StorageEngine,
    wal: WalManager,
    blobs: BlobManager,
    recovery: Option<RecoverySummary>,
}

struct OpenRetention {
    state: Arc<Mutex<RetentionState>>,
    protected_offsets: Arc<Mutex<HashSet<u64>>>,
}

impl OpenPaths {
    fn prepare<P: AsRef<Path>>(path: P, mode: OpenMode) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let path_preexisted = path.exists();

        match mode {
            OpenMode::Check if !path_preexisted => {
                return Err(Error::InvalidArgument(format!(
                    "check path does not exist: {}",
                    path.display()
                )));
            }
            OpenMode::Create => {
                if path_preexisted {
                    return Err(Error::InvalidArgument(format!(
                        "database path already exists: {}",
                        path.display()
                    )));
                }
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    fs::create_dir_all(parent)?;
                }
                fs::create_dir(&path)?;
                sync_directory_chain(path.parent().unwrap_or_else(|| Path::new(".")))?;
            }
            OpenMode::Normal if !path_preexisted => {
                fs::create_dir_all(&path)?;
                sync_directory_chain(path.parent().unwrap_or_else(|| Path::new(".")))?;
            }
            OpenMode::Check | OpenMode::Normal => {}
        }

        Ok(Self {
            data: path.join(DATA_FILE),
            wal: path.join(WAL_FILE),
            meta: path.join(META_FILE),
            manifest: path.join(MANIFEST_FILE),
            path,
            path_preexisted,
        })
    }
}

impl DB {
    fn load_open_catalog(paths: &OpenPaths, mode: OpenMode) -> Result<OpenCatalog> {
        let check_only = mode == OpenMode::Check;
        let archive = paths.path.join(ARCHIVE_MARKER_FILE).is_file();
        let read_only = check_only || archive;

        if mode == OpenMode::Normal
            && paths.path_preexisted
            && !paths.manifest.is_file()
            && !paths.data.is_file()
            && !paths.wal.is_file()
            && !paths.meta.is_file()
        {
            return Err(Error::Corruption(format!(
                "existing database path has no authoritative storage artifacts: {}",
                paths.path.display()
            )));
        }
        if check_only && (!paths.manifest.is_file() || !paths.data.is_file()) {
            return Err(Error::Check {
                kind: CheckFailureKind::Target,
                message: "check target is missing required manifest or data artifacts".into(),
            });
        }
        if archive && !check_only {
            if !paths.manifest.is_file() || !paths.data.is_file() {
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

        let mut manifest = if check_only {
            ManifestStore::open_read_only(&paths.manifest)
                .map_err(|error| Self::map_check_error(CheckFailureKind::Manifest, error))?
        } else {
            ManifestStore::open(&paths.manifest)?
        };
        let current_manifest = manifest.load_latest().map_err(|error| {
            if check_only {
                Self::map_check_error(CheckFailureKind::Manifest, error)
            } else {
                error
            }
        })?;
        Self::validate_selected_artifacts(paths, current_manifest, check_only)?;
        if check_only && current_manifest.is_none() {
            return Err(Error::Check {
                kind: CheckFailureKind::Manifest,
                message: "check target has no valid manifest generation".into(),
            });
        }

        let (database_id, history_id, generation_id, commit_id) =
            Self::open_identities(&paths.path, current_manifest)?;
        let manifest_history = Self::load_manifest_history(paths, current_manifest, read_only)?;
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
            manifest,
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
        if current.pmt_checkpoint_id.get() != 0 {
            let checkpoint_path = paths
                .path
                .join(format!("seerdb.meta.{}", current.pmt_checkpoint_id.get()));
            if !checkpoint_path.is_file() {
                let error = Error::Corruption(format!(
                    "manifest generation {} is missing checkpoint {}",
                    current.generation_id.get(),
                    current.pmt_checkpoint_id.get()
                ));
                return Err(if check_only {
                    Self::map_check_error(CheckFailureKind::Checkpoint, error)
                } else {
                    error
                });
            }
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

    fn load_manifest_history(
        paths: &OpenPaths,
        current_manifest: Option<Manifest>,
        read_only: bool,
    ) -> Result<ManifestHistory> {
        let history_path = paths.path.join(MANIFEST_HISTORY_FILE);
        let mut history = if history_path.exists() {
            let bytes = fs::read(&history_path)?;
            ManifestHistory::from_bytes(&bytes)
                .map_err(|message| Error::Corruption(format!("manifest history {message}")))?
        } else {
            ManifestHistory::new()
        };
        if current_manifest.is_none() && history.latest().is_some() {
            return Err(Error::Corruption(
                "manifest history exists without an authoritative manifest".into(),
            ));
        }
        if let Some(current) = current_manifest {
            history
                .reconcile_current(current)
                .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
            if !read_only {
                let bytes = history
                    .to_bytes()
                    .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
                atomic_write(&history_path, &bytes)?;
            }
        }
        Ok(history)
    }

    fn load_reuse_ledger(
        paths: &OpenPaths,
        current_manifest: Option<Manifest>,
        check_only: bool,
    ) -> Result<ReuseLedger> {
        let ledger_path = paths.path.join(REUSE_LEDGER_FILE);
        let ledger = if ledger_path.is_file() {
            let bytes = fs::read(&ledger_path)?;
            ReuseLedger::from_bytes(&bytes).map_err(|message| {
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

    fn build_open_components(
        paths: &OpenPaths,
        options: &Options,
        catalog: &OpenCatalog,
    ) -> Result<OpenComponents> {
        let device_opts = DeviceOptions {
            use_odirect: options.use_odirect,
            sync_writes: options.sync_writes,
            create: !catalog.read_only,
        };
        let device = if catalog.check_only {
            Device::open_read_only(&paths.data, &device_opts)?
        } else {
            Device::open(&paths.data, &device_opts)?
        };
        let buffer = BufferManager::new(options.buffer_pool_size);
        let sync_policy = if options.sync_writes {
            SyncPolicy::FDataSync
        } else {
            SyncPolicy::None
        };
        let wal = WalManager::new(sync_policy);
        let recovered_blob_bytes = Self::recover_blob_rewrite_backup(
            &paths.path,
            catalog.current_manifest,
            catalog.read_only,
        )?;
        let blob_path = paths.path.join(BLOB_FILE);
        let mut blobs = if blob_path.exists() {
            let blob_data = recovered_blob_bytes
                .as_deref()
                .map_or_else(|| fs::read(&blob_path), |bytes| Ok(bytes.to_vec()))?;
            match parse_blob_catalog(
                &paths.path,
                &blob_data,
                catalog
                    .current_manifest
                    .map(|manifest| manifest.generation_id.get()),
            )? {
                Some(blobs) => blobs,
                None if catalog.check_only => {
                    return Err(Error::Check {
                        kind: CheckFailureKind::Blob,
                        message: "blob catalog or segment is invalid".into(),
                    });
                }
                None => {
                    return Err(Error::Corruption(
                        "blob catalog or segment is invalid".into(),
                    ));
                }
            }
        } else {
            BlobManager::with_threshold_and_mode(
                options.blob_threshold,
                options.blob_storage == BlobStorageMode::Segmented,
            )
        };
        if catalog
            .current_manifest
            .is_some_and(|current| blobs.generation_id() != current.generation_id.get())
        {
            blobs.clear_deletion_metadata();
        }

        let (pmt, allocator) = Self::load_open_metadata(paths, catalog)?;
        let mut engine = StorageEngine::new_with_protected_offsets(
            BTree::new(),
            buffer,
            pmt,
            allocator,
            device,
            Arc::clone(&catalog.protected_offsets),
        );
        let retained_offsets = catalog
            .protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
            .clone();
        engine.set_protected_offsets(retained_offsets)?;
        if let Some(current) = catalog.current_manifest {
            engine.load_from_manifest(current.root_page_id)?;
        } else if !paths.wal.exists() {
            engine.load_from_disk()?;
        }
        if paths.wal.exists() && !catalog.check_only {
            engine.ensure_materialized()?;
        }
        let recovery = if paths.wal.exists() && !catalog.check_only {
            Some(wal_recovery::recover_from_wal(
                &paths.wal,
                catalog.current_manifest,
                engine.btree_mut(),
                &mut blobs,
            )?)
        } else {
            None
        };
        Ok(OpenComponents {
            engine,
            wal,
            blobs,
            recovery,
        })
    }

    fn load_open_metadata(
        paths: &OpenPaths,
        catalog: &OpenCatalog,
    ) -> Result<(PMT, PageAllocator)> {
        if let Some(current) = catalog.current_manifest {
            if current.pmt_checkpoint_id.get() == 0 {
                return Ok((PMT::new(), PageAllocator::new()));
            }
            let checkpoint_path = paths
                .path
                .join(format!("seerdb.meta.{}", current.pmt_checkpoint_id.get()));
            return Self::load_meta(&checkpoint_path).map_err(|error| {
                if catalog.check_only {
                    Self::map_checkpoint_check_error(error)
                } else {
                    error
                }
            });
        }
        if paths.meta.exists() {
            return Self::load_meta(&paths.meta).map_err(|error| {
                if catalog.check_only {
                    Self::map_checkpoint_check_error(error)
                } else {
                    error
                }
            });
        }
        Ok((PMT::new(), PageAllocator::new()))
    }
}

impl DB {
    pub(super) fn open_with_mode<P: AsRef<Path>>(
        path: P,
        options: Options,
        mode: OpenMode,
    ) -> Result<Self> {
        options.validate()?;
        let paths = OpenPaths::prepare(path, mode)?;
        let catalog = Self::load_open_catalog(&paths, mode)?;
        let components = Self::build_open_components(&paths, &options, &catalog)?;

        let current_manifest = catalog.current_manifest;
        let recovery = components.recovery;
        let mut db = Self {
            path: paths.path.clone(),
            options,
            engine: components.engine,
            wal: components.wal,
            blobs: components.blobs,
            vacuum: None,
            retention: catalog.retention,
            txn_manager: TransactionManager::new(),
            manifest: catalog.manifest,
            manifest_history: catalog.manifest_history,
            reuse_ledger: catalog.reuse_ledger,
            database_id: catalog.database_id,
            history_id: catalog.history_id,
            generation_id: catalog.generation_id,
            commit_id: catalog.commit_id,
            next_commit_id: catalog.next_commit_id,
            next_generation_id: catalog.next_generation_id,
            pending_mutations: 0,
            pending_wal_bytes: 0,
            wal_reserved_extent: 0,
            pending_digest: 0,
            pending_blob_changes: recovery
                .as_ref()
                .is_some_and(|summary| summary.blob_changed),
            is_open: true,
            write_fenced: false,
            read_only: catalog.read_only,
            check_only: catalog.check_only,
            lock_file: catalog.lock_file,
            wal_admission_failures: 0,
            publication: PublicationMetrics::default(),
            publication_timing: PublicationTimingMetrics::default(),
        };

        if !catalog.check_only
            && current_manifest.is_none()
            && !paths.wal.exists()
            && !paths.meta.exists()
        {
            let initial = Manifest {
                database_id: db.database_id,
                history_id: db.history_id,
                generation_id: GenerationId::new(0),
                commit_id: CommitId::new(0),
                page_size: PAGE_SIZE as u32,
                root_page_id: db.engine.btree().root_id() as u64,
                pmt_checkpoint_id: PmtCheckpointId::new(0),
                wal_segment: 0,
                wal_offset: 0,
                mutation_count: 0,
                digest: 0,
                format_version: FORMAT_VERSION,
            };
            db.manifest_history.reset(initial);
            db.persist_manifest_history(&db.manifest_history)?;
            db.manifest.publish(initial)?;
        }

        if let Some(recovery) = recovery {
            if let Some(commit) = recovery.last_commit {
                db.publish_recovered(commit, recovery.last_commit_offset)?;
            } else {
                // Complete mutations without a commit envelope are not
                // visible in the durable protocol and may be discarded.
                fs::remove_file(&paths.wal)?;
            }
        }

        Ok(db)
    }
}
