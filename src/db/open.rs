use super::*;

pub(super) struct OpenPaths {
    pub(super) path: PathBuf,
    pub(super) path_preexisted: bool,
    pub(super) data: PathBuf,
    pub(super) wal: PathBuf,
    pub(super) meta: PathBuf,
    pub(super) manifest: PathBuf,
}

impl OpenPaths {
    pub(super) fn prepare<P: AsRef<Path>>(path: P, mode: OpenMode) -> Result<Self> {
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
    pub(super) fn open_with_mode<P: AsRef<Path>>(
        path: P,
        options: Options,
        mode: OpenMode,
    ) -> Result<Self> {
        options.validate()?;
        let paths = OpenPaths::prepare(path, mode)?;
        let catalog = Self::load_open_catalog(&paths, mode)?;
        let components = open_components::build(&paths, &options, &catalog)?;

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
