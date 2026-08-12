//! Blob file-layout and catalog-loading helpers.
//!
//! This module owns the mapping between DB paths and blob artifacts, plus the
//! read-only decoding needed to bootstrap a BlobManager. DB retains
//! publication ordering, mutable blob state, and maintenance policy.

use super::{BlobManager, Error, Result};
use crate::storage::format::SnapshotId;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

pub(super) const BLOB_FILE: &str = "seerdb.blob";
pub(super) const BLOB_DELTA_FILE: &str = "seerdb.blob.delta";
pub(super) const BLOB_SEGMENT_PREFIX: &str = "seerdb.blob.segment.";
pub(super) const BLOB_RESERVATION_FILE: &str = "seerdb.blob.reserve";
pub(super) const BLOB_REWRITE_BACKUP_FILE: &str = "seerdb.blob.rewrite-old";

/// Maximum accumulated deletion offsets before explicit catalog
/// consolidation is requested by DB::gc().
pub(super) const MAX_SEGMENTED_CATALOG_DELETED_ENTRIES: usize = 4096;

pub(super) fn retained_blob_path(path: &Path, snapshot_id: SnapshotId) -> PathBuf {
    path.join(format!("{BLOB_FILE}.retained.{}", snapshot_id.get()))
}

pub(super) fn blob_segment_path(path: &Path, file_id: u32) -> PathBuf {
    path.join(format!("{BLOB_SEGMENT_PREFIX}{file_id:010}"))
}

fn read_blob_segments(path: &Path) -> Result<HashMap<u32, Vec<u8>>> {
    let mut segments = HashMap::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(suffix) = name
            .to_str()
            .and_then(|name| name.strip_prefix(BLOB_SEGMENT_PREFIX))
        else {
            continue;
        };
        let file_id = suffix
            .parse::<u32>()
            .map_err(|_| Error::Corruption("blob segment has an invalid file ID".into()))?;
        if file_id == 0
            || file_id == u32::MAX
            || segments.insert(file_id, fs::read(entry.path())?).is_some()
        {
            return Err(Error::Corruption(
                "blob segment IDs are invalid or duplicated".into(),
            ));
        }
    }
    Ok(segments)
}

pub(super) fn parse_blob_catalog(
    path: &Path,
    bytes: &[u8],
    target_generation: Option<u64>,
) -> Result<Option<BlobManager>> {
    if BlobManager::is_segment_catalog(bytes) {
        let segments = read_blob_segments(path)?;
        let delta_log = match fs::read(path.join(BLOB_DELTA_FILE)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let parsed = BlobManager::from_segment_catalog_with_delta_log(
            bytes,
            &segments,
            &delta_log,
            target_generation,
        );
        Ok(parsed)
    } else {
        Ok(BlobManager::from_bytes(bytes))
    }
}

pub(super) fn blob_storage_size(path: &Path) -> Result<u64> {
    let mut total = match fs::metadata(path.join(BLOB_FILE)) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry
                .file_name()
                .to_string_lossy()
                .starts_with(BLOB_SEGMENT_PREFIX)
        {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    if let Ok(metadata) = fs::metadata(path.join(BLOB_DELTA_FILE)) {
        total = total.saturating_add(metadata.len());
    }
    Ok(total)
}

pub(super) fn segmented_catalog_needs_consolidation(blobs: &BlobManager) -> bool {
    blobs.is_segmented()
        && (blobs.total_deleted_entries() > MAX_SEGMENTED_CATALOG_DELETED_ENTRIES
            || blobs.catalog_needs_consolidation())
}
