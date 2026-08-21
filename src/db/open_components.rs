//! Runtime component construction for database startup.
//!
//! This module owns the startup lifetime from durable artifact selection to a
//! ready `StorageEngine`, `WalManager`, and `BlobManager`, including WAL
//! replay. Path creation, manifest/identity reconciliation, retention state,
//! and final `DB` assembly remain in `open.rs`.

use super::open::OpenPaths;
use super::open_catalog::OpenCatalog;
use super::*;

pub(super) struct OpenComponents {
    pub(super) engine: StorageEngine,
    pub(super) wal: WalManager,
    pub(super) blobs: BlobManager,
    pub(super) recovery: Option<RecoverySummary>,
}

pub(super) fn build(
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
    let recovered_blob_bytes =
        DB::recover_blob_rewrite_backup(&paths.path, catalog.current_manifest, catalog.read_only)?;
    let mut blobs = load_blobs(paths, options, catalog, recovered_blob_bytes)?;
    let (pmt, allocator) = load_metadata(paths, catalog)?;
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

fn load_blobs(
    paths: &OpenPaths,
    options: &Options,
    catalog: &OpenCatalog,
    recovered_blob_bytes: Option<Vec<u8>>,
) -> Result<BlobManager> {
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
    Ok(blobs)
}

fn load_metadata(paths: &OpenPaths, catalog: &OpenCatalog) -> Result<(PMT, PageAllocator)> {
    if let Some(current) = catalog.current_manifest {
        if current.pmt_checkpoint_id.get() == 0 {
            return Ok((PMT::new(), PageAllocator::new()));
        }
        let parsed = DB::read_meta_log(&paths.path)?.ok_or_else(|| {
            Error::Corruption(format!(
                "manifest generation {} names checkpoint {} but the metadata log is missing",
                current.generation_id.get(),
                current.pmt_checkpoint_id.get()
            ))
        })?;
        let resolved =
            DB::resolve_meta_log(&parsed, current.pmt_checkpoint_id.get()).map_err(|error| {
                if catalog.check_only {
                    DB::map_checkpoint_check_error(error)
                } else {
                    error
                }
            })?;
        // A torn tail behind the selected frame is an abandoned append from a
        // crash; a writable open durably removes it before any later append
        // can land behind the torn boundary.
        let log_path = DB::metadata_log_path(&paths.path);
        let log_len = fs::metadata(&log_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if !catalog.read_only && !catalog.check_only && (parsed.valid_len as u64) < log_len {
            let file = fs::OpenOptions::new().write(true).open(&log_path)?;
            file.set_len(parsed.valid_len as u64)?;
            file.sync_all()?;
        }
        return Ok((resolved.0, resolved.1));
    }
    if paths.meta.exists() {
        return DB::load_meta(&paths.meta).map_err(|error| {
            if catalog.check_only {
                DB::map_checkpoint_check_error(error)
            } else {
                error
            }
        });
    }
    Ok((PMT::new(), PageAllocator::new()))
}
