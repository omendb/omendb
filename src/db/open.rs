use super::*;

impl DB {
    pub(super) fn open_with_mode<P: AsRef<Path>>(
        path: P,
        options: Options,
        mode: OpenMode,
    ) -> Result<Self> {
        options.validate()?;
        // Page size is part of the compiled page, buffer, and on-disk
        // format. `Options` intentionally does not expose a second page-size
        // choice that could drift from those owners.
        let path = path.as_ref().to_path_buf();
        let path_preexisted = path.exists();
        let check_only = mode == OpenMode::Check;

        match mode {
            OpenMode::Check if !path.exists() => {
                return Err(Error::InvalidArgument(format!(
                    "check path does not exist: {}",
                    path.display()
                )));
            }
            OpenMode::Create => {
                if path.exists() {
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
                // The directory entry itself is part of the acknowledged
                // create boundary. Sync the parent chain before publishing
                // any manifest/data artifacts so a power loss cannot lose the
                // newly created database directory while retaining its files.
                sync_directory_chain(path.parent().unwrap_or_else(|| Path::new(".")))?;
            }
            OpenMode::Normal if !path.exists() => fs::create_dir_all(&path)?,
            OpenMode::Check | OpenMode::Normal => {}
        }

        let data_path = path.join(DATA_FILE);
        let wal_path = path.join(WAL_FILE);
        let meta_path = path.join(META_FILE);
        let manifest_path = path.join(MANIFEST_FILE);
        let archive = path.join(ARCHIVE_MARKER_FILE).is_file();
        let read_only = check_only || archive;
        if mode == OpenMode::Normal
            && path_preexisted
            && !manifest_path.is_file()
            && !data_path.is_file()
            && !wal_path.is_file()
            && !meta_path.is_file()
        {
            return Err(Error::Corruption(format!(
                "existing database path has no authoritative storage artifacts: {}",
                path.display()
            )));
        }
        if check_only && (!manifest_path.is_file() || !data_path.is_file()) {
            return Err(Error::Check {
                kind: CheckFailureKind::Target,
                message: "check target is missing required manifest or data artifacts".into(),
            });
        }
        if archive && !check_only {
            if !manifest_path.is_file() || !data_path.is_file() {
                return Err(Error::Corruption(
                    "read-only archive is missing required artifacts".into(),
                ));
            }
            if wal_path.exists() {
                return Err(Error::NeedsRecovery(
                    "read-only archive contains a pending WAL".into(),
                ));
            }
        }
        let lock_file = if read_only {
            None
        } else {
            Some(Self::acquire_writer_lock(&path.join(LOCK_FILE))?)
        };
        if !read_only {
            cleanup_orphaned_temporary_artifacts(&path)?;
            clear_blob_reservation(&path)?;
            clear_wal_reservation(&path)?;
        }

        let mut manifest = if check_only {
            ManifestStore::open_read_only(&manifest_path)
                .map_err(|error| Self::map_check_error(CheckFailureKind::Manifest, error))?
        } else {
            ManifestStore::open(&manifest_path)?
        };
        let current_manifest = manifest.load_latest().map_err(|error| {
            if check_only {
                Self::map_check_error(CheckFailureKind::Manifest, error)
            } else {
                error
            }
        })?;
        if let Some(current) = current_manifest {
            if !data_path.is_file() {
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
                let checkpoint_path =
                    path.join(format!("seerdb.meta.{}", current.pmt_checkpoint_id.get()));
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
        }
        if check_only && current_manifest.is_none() {
            return Err(Error::Check {
                kind: CheckFailureKind::Manifest,
                message: "check target has no valid manifest generation".into(),
            });
        }
        let (database_id, history_id, generation_id, commit_id) =
            if let Some(current) = current_manifest {
                if current.page_size as usize != PAGE_SIZE {
                    return Err(Error::Corruption(format!(
                        "manifest page size {} does not match build page size {PAGE_SIZE}",
                        current.page_size
                    )));
                }
                (
                    current.database_id,
                    current.history_id,
                    current.generation_id,
                    current.commit_id,
                )
            } else {
                (
                    Self::new_database_id(&path),
                    HistoryId::new(1),
                    GenerationId::new(0),
                    CommitId::new(0),
                )
            };

        let manifest_history_path = path.join(MANIFEST_HISTORY_FILE);
        let mut manifest_history = if manifest_history_path.exists() {
            let bytes = fs::read(&manifest_history_path)?;
            ManifestHistory::from_bytes(&bytes)
                .map_err(|message| Error::Corruption(format!("manifest history {message}")))?
        } else {
            ManifestHistory::new()
        };
        if let Some(current) = current_manifest {
            manifest_history
                .reconcile_current(current)
                .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
            if !read_only {
                let bytes = manifest_history
                    .to_bytes()
                    .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
                // Rewrite only at open/reconciliation boundaries. Normal
                // commits append one checksummed frame below.
                atomic_write(&manifest_history_path, &bytes)?;
            }
        } else if manifest_history.latest().is_some() {
            return Err(Error::Corruption(
                "manifest history exists without an authoritative manifest".into(),
            ));
        }

        let reuse_ledger_path = path.join(REUSE_LEDGER_FILE);
        let mut reuse_ledger = if reuse_ledger_path.is_file() {
            let bytes = fs::read(&reuse_ledger_path)?;
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
        let pruned_reuse_attempts = reuse_ledger.prune_published(&manifest_history);
        if pruned_reuse_attempts > 0 && !read_only && !check_only {
            Self::persist_reuse_ledger_at(&path, &reuse_ledger)?;
        }
        if current_manifest.is_none() && !reuse_ledger.attempts().is_empty() {
            return Err(Error::Corruption(
                "reuse ledger exists without an authoritative manifest".into(),
            ));
        }

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
            if reserved_commit > next_commit_id {
                next_commit_id = reserved_commit;
            }
            if reserved_generation > next_generation_id {
                next_generation_id = reserved_generation;
            }
        }

        let protected_offsets = Arc::new(Mutex::new(HashSet::new()));
        let retention_path = path.join(RETENTION_FILE);
        let retention = Arc::new(Mutex::new(
            RetentionState::load(retention_path, Arc::clone(&protected_offsets)).map_err(
                |error| {
                    if check_only {
                        Self::map_check_error(CheckFailureKind::Format, error)
                    } else {
                        error
                    }
                },
            )?,
        ));
        if !read_only && !check_only {
            Self::cleanup_orphaned_retained_blobs(&path, &retention)?;
        }
        {
            let mut state = retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            let offsets = Self::load_retained_offset_map(&path, &state, database_id, history_id)
                .map_err(|error| {
                    if check_only {
                        Self::map_check_error(CheckFailureKind::Checkpoint, error)
                    } else {
                        error
                    }
                })?;
            state.install_offsets(offsets).map_err(|error| {
                if check_only {
                    Self::map_check_error(CheckFailureKind::Checkpoint, error)
                } else {
                    error
                }
            })?;
        }

        // Open the data file.
        let device_opts = DeviceOptions {
            use_odirect: options.use_odirect,
            sync_writes: options.sync_writes,
            create: !read_only,
        };
        let device = if check_only {
            Device::open_read_only(&data_path, &device_opts)?
        } else {
            Device::open(&data_path, &device_opts)?
        };

        // Create buffer manager.
        let buffer = BufferManager::new(options.buffer_pool_size);

        // Create WAL manager.
        let sync_policy = if options.sync_writes {
            SyncPolicy::FDataSync
        } else {
            SyncPolicy::None
        };
        let wal = WalManager::new(sync_policy);

        // Recover an interrupted mixed-blob rewrite before loading the blob
        // catalog. A rewrite keeps the previous image under a side filename
        // until its maintenance manifest is authoritative.
        let recovered_blob_bytes =
            Self::recover_blob_rewrite_backup(&path, current_manifest, read_only)?;

        // Create blob manager.
        let blob_path = path.join(BLOB_FILE);
        let mut blobs = if blob_path.exists() {
            // Load blob files from disk.
            let blob_data = recovered_blob_bytes
                .as_deref()
                .map_or_else(|| fs::read(&blob_path), |bytes| Ok(bytes.to_vec()))?;
            match parse_blob_catalog(
                &path,
                &blob_data,
                current_manifest.map(|manifest| manifest.generation_id.get()),
            )? {
                Some(blobs) => blobs,
                None if check_only => {
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
        if current_manifest
            .is_some_and(|current| blobs.generation_id() != current.generation_id.get())
        {
            // A blob image is written before its manifest. If publication
            // stopped between those boundaries, deletion marks from the
            // newer image must not make pages referenced by the older
            // manifest reclaimable.
            blobs.clear_deletion_metadata();
        }

        // A published manifest selects an immutable PMT checkpoint. Never
        // pair an older manifest with a newer mutable metadata file.
        let (pmt, allocator) = if let Some(current) = current_manifest {
            if current.pmt_checkpoint_id.get() == 0 {
                (PMT::new(), PageAllocator::new())
            } else {
                let checkpoint_path =
                    path.join(format!("seerdb.meta.{}", current.pmt_checkpoint_id.get()));
                Self::load_meta(&checkpoint_path).map_err(|error| {
                    if check_only {
                        Self::map_checkpoint_check_error(error)
                    } else {
                        error
                    }
                })?
            }
        } else if meta_path.exists() {
            Self::load_meta(&meta_path).map_err(|error| {
                if check_only {
                    Self::map_checkpoint_check_error(error)
                } else {
                    error
                }
            })?
        } else {
            (PMT::new(), PageAllocator::new())
        };

        // Create storage engine.
        let mut engine = StorageEngine::new_with_protected_offsets(
            BTree::new(),
            buffer,
            pmt,
            allocator,
            device,
            Arc::clone(&protected_offsets),
        );
        let retained_offsets = protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
            .clone();
        engine.set_protected_offsets(retained_offsets)?;

        // A published manifest selects the PMT locations for the latest
        // generation. Without one, retain the legacy scan as a migration path.
        if let Some(current) = current_manifest {
            engine.load_from_manifest(current.root_page_id)?;
        } else if !wal_path.exists() {
            engine.load_from_disk()?;
        }

        // WAL replay mutates the logical tree, so materialize a lazily opened
        // generation before applying a committed recovery prefix. A clean
        // reopen remains lazy and serves reads directly through the PMT.
        if wal_path.exists() && !check_only {
            engine.ensure_materialized()?;
        }

        let recovery = if wal_path.exists() && !check_only {
            Some(wal_recovery::recover_from_wal(
                &wal_path,
                current_manifest,
                engine.btree_mut(),
                &mut blobs,
            )?)
        } else {
            None
        };

        let mut db = Self {
            path,
            options,
            engine,
            wal,
            blobs,
            vacuum: None,
            retention,
            txn_manager: TransactionManager::new(),
            manifest,
            manifest_history,
            reuse_ledger,
            database_id,
            history_id,
            generation_id,
            commit_id,
            next_commit_id,
            next_generation_id,
            pending_mutations: 0,
            pending_wal_bytes: 0,
            wal_reserved_extent: 0,
            pending_digest: 0,
            pending_blob_changes: recovery
                .as_ref()
                .is_some_and(|summary| summary.blob_changed),
            is_open: true,
            write_fenced: false,
            read_only,
            check_only,
            lock_file,
            wal_admission_failures: 0,
            publication: PublicationMetrics::default(),
            publication_timing: PublicationTimingMetrics::default(),
        };

        if !check_only && current_manifest.is_none() && !wal_path.exists() && !meta_path.exists() {
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
                fs::remove_file(&wal_path)?;
            }
        }

        Ok(db)
    }
}
