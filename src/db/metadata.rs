//! PMT/allocator checkpoint and bounded metadata-delta lifecycle.
//!
//! `DB` owns which checkpoint becomes authoritative during publication. This
//! module owns the checkpoint representation, delta-chain validation, retained
//! offset validation, and metadata footprint calculations used by that state
//! machine.

use super::retention_state::RetentionState;
use super::{
    BLOB_FILE, DATA_FILE, DB, atomic_write, atomic_write_without_directory_sync,
    retained_blob_path, sync_directory,
};
use crate::allocator::PageAllocator;
use crate::blob::BlobManager;
use crate::btree::PAGE_SIZE;
use crate::error::{Error, Result};
use crate::mvcc::{PMT, PageMapping};
use crate::storage::format::{DatabaseId, FORMAT_VERSION, HistoryId, Manifest, SnapshotId};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub(super) const META_MAGIC: [u8; 8] = *b"SEERMET1";
pub(super) const META_DELTA_MAGIC: [u8; 8] = *b"SEERMDL1";
const META_DELTA_VERSION: u32 = 1;
pub(super) const META_DELTA_HEADER_SIZE: usize = 8 + 4 + 8 + 4 + 4 + 4;
pub(super) const META_DELTA_CHECKSUM_SIZE: usize = 4;
pub(super) const MAX_META_DELTA_CHAIN: usize = 64;

/// A decoded incremental PMT/allocator checkpoint.
struct MetaDelta {
    parent_checkpoint_id: u64,
    updates: Vec<(u64, PageMapping)>,
    removals: Vec<u64>,
    allocator: PageAllocator,
}

impl DB {
    /// Load PMT and allocator from meta file.
    pub(super) fn load_meta(path: &Path) -> Result<(PMT, PageAllocator)> {
        Self::load_meta_with_depth(path).map(|(pmt, allocator, _)| (pmt, allocator))
    }

    /// Load a full checkpoint or a bounded metadata-delta chain.
    pub(super) fn load_meta_with_depth(path: &Path) -> Result<(PMT, PageAllocator, usize)> {
        let mut current_path = path.to_path_buf();
        let mut deltas = Vec::new();
        let mut visited = HashSet::new();
        let (mut pmt, mut allocator) = loop {
            let data = fs::read(&current_path)?;
            if data.len() >= META_DELTA_MAGIC.len()
                && data[..META_DELTA_MAGIC.len()] == META_DELTA_MAGIC
            {
                let delta = Self::load_meta_delta(&data)?;
                if delta.parent_checkpoint_id != 0 && !visited.insert(delta.parent_checkpoint_id) {
                    return Err(Error::Corruption(
                        "metadata delta chain contains a cycle".into(),
                    ));
                }
                deltas.push(delta);
                if deltas.len() > MAX_META_DELTA_CHAIN {
                    return Err(Error::Corruption(format!(
                        "metadata delta chain exceeds maximum length {MAX_META_DELTA_CHAIN}"
                    )));
                }
                let parent = deltas
                    .last()
                    .map(|delta| delta.parent_checkpoint_id)
                    .ok_or_else(|| Error::Corruption("metadata delta disappeared".into()))?;
                if parent == 0 {
                    return Err(Error::Corruption(
                        "metadata delta has no full checkpoint parent".into(),
                    ));
                }
                current_path = current_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(format!("seerdb.meta.{parent}"));
                continue;
            }

            break if data.len() >= META_MAGIC.len() && data[..META_MAGIC.len()] == META_MAGIC {
                Self::load_versioned_meta(&data)?
            } else {
                Self::load_legacy_meta(&data)?
            };
        };

        for delta in deltas.iter().rev() {
            for page_id in &delta.removals {
                if pmt.remove(*page_id).is_none() {
                    return Err(Error::Corruption(format!(
                        "metadata delta removes unknown page {page_id}"
                    )));
                }
            }
            for (page_id, mapping) in &delta.updates {
                pmt.insert_persisted(*page_id, *mapping);
            }
            allocator = delta.allocator.clone();
        }

        Ok((pmt, allocator, deltas.len()))
    }

    fn load_meta_delta(data: &[u8]) -> Result<MetaDelta> {
        if data.len() < META_DELTA_HEADER_SIZE + META_DELTA_CHECKSUM_SIZE {
            return Err(Error::Corruption("metadata delta is truncated".into()));
        }
        let version = u32::from_le_bytes(
            data[8..12]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta version is truncated".into()))?,
        );
        if version != META_DELTA_VERSION {
            return Err(Error::Corruption(format!(
                "unsupported metadata delta version {version}"
            )));
        }

        let checksum_offset = data
            .len()
            .checked_sub(META_DELTA_CHECKSUM_SIZE)
            .ok_or_else(|| Error::Corruption("metadata delta checksum is truncated".into()))?;
        let expected = u32::from_le_bytes(
            data[checksum_offset..]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta checksum is truncated".into()))?,
        );
        if crc32c::crc32c(&data[..checksum_offset]) != expected {
            return Err(Error::Corruption("metadata delta checksum mismatch".into()));
        }

        let parent_checkpoint_id = u64::from_le_bytes(
            data[12..20]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta parent is truncated".into()))?,
        );
        let update_count =
            u32::from_le_bytes(data[20..24].try_into().map_err(|_| {
                Error::Corruption("metadata delta update count is truncated".into())
            })?) as usize;
        let removal_count =
            u32::from_le_bytes(data[24..28].try_into().map_err(|_| {
                Error::Corruption("metadata delta removal count is truncated".into())
            })?) as usize;
        let allocator_len = u32::from_le_bytes(data[28..32].try_into().map_err(|_| {
            Error::Corruption("metadata delta allocator length is truncated".into())
        })?) as usize;

        let update_bytes = update_count
            .checked_mul(8 + PageMapping::SERIALIZED_SIZE)
            .ok_or_else(|| Error::Corruption("metadata delta updates overflow".into()))?;
        let removal_bytes = removal_count
            .checked_mul(8)
            .ok_or_else(|| Error::Corruption("metadata delta removals overflow".into()))?;
        let allocator_start = META_DELTA_HEADER_SIZE
            .checked_add(update_bytes)
            .and_then(|offset| offset.checked_add(removal_bytes))
            .ok_or_else(|| Error::Corruption("metadata delta layout overflows".into()))?;
        let checksum_expected_end = allocator_start
            .checked_add(allocator_len)
            .and_then(|offset| offset.checked_add(META_DELTA_CHECKSUM_SIZE))
            .ok_or_else(|| Error::Corruption("metadata delta layout overflows".into()))?;
        if checksum_expected_end != data.len() {
            return Err(Error::Corruption(
                "metadata delta has trailing or truncated bytes".into(),
            ));
        }

        let mut updates = Vec::with_capacity(update_count);
        let mut offset = META_DELTA_HEADER_SIZE;
        let mut previous_page = None;
        for _ in 0..update_count {
            let page_id =
                u64::from_le_bytes(data[offset..offset + 8].try_into().map_err(|_| {
                    Error::Corruption("metadata delta page ID is truncated".into())
                })?);
            if previous_page.is_some_and(|previous| page_id <= previous) {
                return Err(Error::Corruption(
                    "metadata delta updates are not strictly sorted".into(),
                ));
            }
            previous_page = Some(page_id);
            offset += 8;
            let mapping_end = offset + PageMapping::SERIALIZED_SIZE;
            let mapping =
                PageMapping::from_bytes(data[offset..mapping_end].try_into().map_err(|_| {
                    Error::Corruption("metadata delta mapping is truncated".into())
                })?);
            if mapping.version == u64::MAX {
                return Err(Error::Corruption(
                    "metadata delta mapping version is exhausted".into(),
                ));
            }
            updates.push((page_id, mapping));
            offset = mapping_end;
        }

        let mut removals = Vec::with_capacity(removal_count);
        let mut previous_page = None;
        for _ in 0..removal_count {
            let page_id =
                u64::from_le_bytes(data[offset..offset + 8].try_into().map_err(|_| {
                    Error::Corruption("metadata delta removal is truncated".into())
                })?);
            if previous_page.is_some_and(|previous| page_id <= previous) {
                return Err(Error::Corruption(
                    "metadata delta removals are not strictly sorted".into(),
                ));
            }
            previous_page = Some(page_id);
            removals.push(page_id);
            offset += 8;
        }

        if updates
            .iter()
            .any(|(page_id, _)| removals.binary_search(page_id).is_ok())
        {
            return Err(Error::Corruption(
                "metadata delta updates and removals overlap".into(),
            ));
        }

        let allocator =
            PageAllocator::from_bytes(&data[allocator_start..allocator_start + allocator_len])
                .ok_or_else(|| Error::Corruption("metadata delta allocator is invalid".into()))?;
        Ok(MetaDelta {
            parent_checkpoint_id,
            updates,
            removals,
            allocator,
        })
    }

    pub(super) fn cleanup_orphaned_retained_blobs(
        path: &Path,
        retention: &Arc<Mutex<RetentionState>>,
    ) -> Result<()> {
        let state = retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        let retained_ids = state
            .roots()
            .iter()
            .map(|root| root.snapshot_id)
            .collect::<HashSet<_>>();
        drop(state);

        let prefix = format!("{BLOB_FILE}.retained.");
        let mut removed = false;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(id) = name
                .strip_prefix(&prefix)
                .and_then(|suffix| suffix.parse::<u64>().ok())
            else {
                continue;
            };
            if !retained_ids.contains(&SnapshotId::new(id)) {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
        if removed {
            sync_directory(path)?;
        }
        Ok(())
    }

    pub(super) fn load_retained_offset_map(
        path: &Path,
        state: &RetentionState,
        database_id: DatabaseId,
        history_id: HistoryId,
    ) -> Result<BTreeMap<SnapshotId, HashSet<u64>>> {
        let mut offsets_by_snapshot = BTreeMap::new();
        for root in state.roots() {
            if root.manifest.database_id != database_id {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} belongs to another database",
                    root.snapshot_id.get()
                )));
            }
            if root.manifest.history_id != history_id {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} belongs to another history",
                    root.snapshot_id.get()
                )));
            }
            if root.manifest.page_size as usize != PAGE_SIZE {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has page size {}",
                    root.snapshot_id.get(),
                    root.manifest.page_size
                )));
            }
            let blob_path = retained_blob_path(path, root.snapshot_id);
            let blob_bytes = fs::read(&blob_path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Error::Corruption(format!(
                        "retained snapshot {} is missing its blob image",
                        root.snapshot_id.get()
                    ))
                } else {
                    error.into()
                }
            })?;
            if BlobManager::from_bytes(&blob_bytes).is_none() {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has an invalid blob image",
                    root.snapshot_id.get()
                )));
            }
            let protected = Self::load_manifest_offsets(path, root.manifest, root.snapshot_id)?;
            offsets_by_snapshot.insert(root.snapshot_id, protected);
        }
        Ok(offsets_by_snapshot)
    }

    pub(super) fn load_manifest_offsets(
        path: &Path,
        manifest: Manifest,
        snapshot_id: SnapshotId,
    ) -> Result<HashSet<u64>> {
        if manifest.pmt_checkpoint_id.get() == 0 {
            if manifest.root_page_id != 0 {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has a root without a checkpoint",
                    snapshot_id.get()
                )));
            }
            return Ok(HashSet::new());
        }

        let checkpoint = path.join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
        let (pmt, _) = Self::load_meta(&checkpoint)?;
        if !pmt.contains(manifest.root_page_id) {
            return Err(Error::Corruption(format!(
                "retained snapshot {} names a root missing from its checkpoint",
                snapshot_id.get()
            )));
        }
        let mut protected = HashSet::new();
        let data_bytes = fs::metadata(path.join(DATA_FILE))?.len();
        for (_, mapping) in pmt.iter() {
            if mapping.file_id != 0 || !mapping.offset.is_multiple_of(PAGE_SIZE as u64) {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} names an invalid page mapping",
                    snapshot_id.get()
                )));
            }
            let end = mapping
                .offset
                .checked_add(PAGE_SIZE as u64)
                .ok_or_else(|| {
                    Error::Corruption(format!(
                        "retained snapshot {} has an overflowing page mapping",
                        snapshot_id.get()
                    ))
                })?;
            if end > data_bytes {
                return Err(Error::SnapshotUnavailable(format!(
                    "retained snapshot {} names pages beyond the data file",
                    snapshot_id.get()
                )));
            }
            protected.insert(mapping.offset);
        }
        Ok(protected)
    }

    fn load_versioned_meta(data: &[u8]) -> Result<(PMT, PageAllocator)> {
        const HEADER_SIZE: usize = META_MAGIC.len() + 4;
        const CHECKSUM_SIZE: usize = 4;
        if data.len() < HEADER_SIZE + CHECKSUM_SIZE {
            return Err(Error::Corruption("meta file is truncated".into()));
        }

        let version = u32::from_le_bytes(
            data[META_MAGIC.len()..HEADER_SIZE]
                .try_into()
                .map_err(|_| Error::Corruption("meta version is truncated".into()))?,
        );
        if version != FORMAT_VERSION {
            return Err(Error::Corruption(format!(
                "unsupported meta format version {version}"
            )));
        }

        let checksum_offset = data.len() - CHECKSUM_SIZE;
        let expected = u32::from_le_bytes(
            data[checksum_offset..]
                .try_into()
                .map_err(|_| Error::Corruption("meta checksum is truncated".into()))?,
        );
        let actual = crc32c::crc32c(&data[..checksum_offset]);
        if expected != actual {
            return Err(Error::Corruption("meta checksum mismatch".into()));
        }

        Self::load_legacy_meta(&data[HEADER_SIZE..checksum_offset])
    }

    fn load_legacy_meta(data: &[u8]) -> Result<(PMT, PageAllocator)> {
        if data.len() < 4 {
            return Err(Error::Corruption("meta file too small".into()));
        }

        let pmt_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        let pmt_end = 4usize
            .checked_add(pmt_len)
            .ok_or_else(|| Error::Corruption("meta PMT length overflows".into()))?;
        let alloc_len_start = pmt_end;
        let alloc_len_end = alloc_len_start
            .checked_add(4)
            .ok_or_else(|| Error::Corruption("meta allocator length overflows".into()))?;
        if data.len() < alloc_len_end {
            return Err(Error::Corruption("meta file truncated".into()));
        }

        let pmt = PMT::from_bytes(&data[4..pmt_end])
            .ok_or_else(|| Error::Corruption("invalid PMT data".into()))?;

        let alloc_offset = alloc_len_start;
        let alloc_len = u32::from_le_bytes([
            data[alloc_offset],
            data[alloc_offset + 1],
            data[alloc_offset + 2],
            data[alloc_offset + 3],
        ]) as usize;

        let alloc_end = alloc_len_end
            .checked_add(alloc_len)
            .ok_or_else(|| Error::Corruption("meta allocator length overflows".into()))?;
        if data.len() != alloc_end {
            return Err(Error::Corruption(
                if data.len() < alloc_end {
                    "meta allocator data is truncated"
                } else {
                    "meta file has trailing bytes"
                }
                .into(),
            ));
        }

        let alloc_data = &data[alloc_len_end..alloc_end];
        let allocator = PageAllocator::from_bytes(alloc_data)
            .ok_or_else(|| Error::Corruption("invalid allocator data".into()))?;

        Ok((pmt, allocator))
    }

    /// Save PMT and allocator to meta file.
    pub(super) fn save_meta(path: &Path, pmt: &PMT, allocator: &PageAllocator) -> Result<u64> {
        Self::save_meta_with_directory_sync(path, pmt, allocator, true)
    }

    pub(super) fn save_meta_without_directory_sync(
        path: &Path,
        pmt: &PMT,
        allocator: &PageAllocator,
    ) -> Result<u64> {
        Self::save_meta_with_directory_sync(path, pmt, allocator, false)
    }

    fn save_meta_with_directory_sync(
        path: &Path,
        pmt: &PMT,
        allocator: &PageAllocator,
        sync_parent: bool,
    ) -> Result<u64> {
        let pmt_bytes = pmt.to_bytes();
        let alloc_bytes = allocator.to_bytes();

        let pmt_len = u32::try_from(pmt_bytes.len())
            .map_err(|_| Error::InvalidArgument("PMT checkpoint is too large".into()))?;
        let alloc_len = u32::try_from(alloc_bytes.len())
            .map_err(|_| Error::InvalidArgument("allocator checkpoint is too large".into()))?;

        let mut buf = Vec::with_capacity(
            META_MAGIC.len() + 4 + 4 + pmt_bytes.len() + 4 + alloc_bytes.len() + 4,
        );
        buf.extend_from_slice(&META_MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&pmt_len.to_le_bytes());
        buf.extend_from_slice(&pmt_bytes);
        buf.extend_from_slice(&alloc_len.to_le_bytes());
        buf.extend_from_slice(&alloc_bytes);
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        if sync_parent {
            atomic_write(path, &buf)?;
        } else {
            atomic_write_without_directory_sync(path, &buf)?;
        }
        Ok(buf.len() as u64)
    }

    fn save_meta_delta_without_directory_sync(
        path: &Path,
        parent_checkpoint_id: u64,
        parent_pmt: &PMT,
        pmt: &PMT,
        allocator: &PageAllocator,
    ) -> Result<u64> {
        let mut updates = pmt
            .iter()
            .filter_map(|(page_id, mapping)| {
                (parent_pmt.get(page_id) != Some(mapping)).then_some((page_id, *mapping))
            })
            .collect::<Vec<_>>();
        updates.sort_unstable_by_key(|(page_id, _)| *page_id);
        let mut removals = parent_pmt
            .iter()
            .filter_map(|(page_id, _)| (!pmt.contains(page_id)).then_some(page_id))
            .collect::<Vec<_>>();
        removals.sort_unstable();

        let update_count = u32::try_from(updates.len())
            .map_err(|_| Error::InvalidArgument("metadata delta has too many updates".into()))?;
        let removal_count = u32::try_from(removals.len())
            .map_err(|_| Error::InvalidArgument("metadata delta has too many removals".into()))?;
        let allocator_bytes = allocator.to_bytes();
        let allocator_len = u32::try_from(allocator_bytes.len())
            .map_err(|_| Error::InvalidArgument("metadata delta allocator is too large".into()))?;

        let total_len = META_DELTA_HEADER_SIZE
            .checked_add(
                updates
                    .len()
                    .checked_mul(8 + PageMapping::SERIALIZED_SIZE)
                    .ok_or(Error::DiskFull)?,
            )
            .and_then(|size| size.checked_add(removals.len().checked_mul(8)?))
            .and_then(|size| size.checked_add(allocator_bytes.len()))
            .and_then(|size| size.checked_add(META_DELTA_CHECKSUM_SIZE))
            .ok_or(Error::DiskFull)?;
        let mut buf = Vec::with_capacity(total_len);
        buf.extend_from_slice(&META_DELTA_MAGIC);
        buf.extend_from_slice(&META_DELTA_VERSION.to_le_bytes());
        buf.extend_from_slice(&parent_checkpoint_id.to_le_bytes());
        buf.extend_from_slice(&update_count.to_le_bytes());
        buf.extend_from_slice(&removal_count.to_le_bytes());
        buf.extend_from_slice(&allocator_len.to_le_bytes());
        for (page_id, mapping) in updates {
            buf.extend_from_slice(&page_id.to_le_bytes());
            buf.extend_from_slice(&mapping.to_bytes());
        }
        for page_id in removals {
            buf.extend_from_slice(&page_id.to_le_bytes());
        }
        buf.extend_from_slice(&allocator_bytes);
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        atomic_write_without_directory_sync(path, &buf)?;
        Ok(buf.len() as u64)
    }

    pub(super) fn load_meta_ancestors(path: &Path, checkpoint_id: u64) -> Result<BTreeSet<u64>> {
        let mut ancestors = BTreeSet::new();
        let mut current_id = checkpoint_id;
        for _ in 0..=MAX_META_DELTA_CHAIN {
            if !ancestors.insert(current_id) {
                return Err(Error::Corruption(
                    "metadata delta chain contains a cycle".into(),
                ));
            }
            let current_path = path.join(format!("seerdb.meta.{current_id}"));
            let data = fs::read(&current_path)?;
            if data.len() < META_DELTA_MAGIC.len()
                || data[..META_DELTA_MAGIC.len()] != META_DELTA_MAGIC
            {
                return Ok(ancestors);
            }
            let delta = Self::load_meta_delta(&data)?;
            if delta.parent_checkpoint_id == 0 {
                return Err(Error::Corruption(
                    "metadata delta has no full checkpoint parent".into(),
                ));
            }
            current_id = delta.parent_checkpoint_id;
        }
        Err(Error::Corruption(format!(
            "metadata delta chain exceeds maximum length {MAX_META_DELTA_CHAIN}"
        )))
    }

    pub(super) fn generation_meta_bytes(
        &self,
        parent: Manifest,
        dirty_page_count: usize,
    ) -> Result<(u64, bool)> {
        let pmt_bytes = (self.engine.pmt().to_bytes().len() as u64)
            .checked_add(
                (dirty_page_count as u64)
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let allocator_bytes = (self.engine.allocator().to_bytes().len() as u64)
            .checked_add(
                (dirty_page_count as u64)
                    .checked_mul(8)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let full_bytes = (META_MAGIC.len() as u64)
            .checked_add(4 + 4 + 4 + 4)
            .and_then(|size| size.checked_add(pmt_bytes))
            .and_then(|size| size.checked_add(allocator_bytes))
            .ok_or(Error::DiskFull)?;
        if parent.pmt_checkpoint_id.get() == 0 {
            return Ok((full_bytes, true));
        }

        let checkpoint = self
            .path
            .join(format!("seerdb.meta.{}", parent.pmt_checkpoint_id.get()));
        let (_, _, depth) = Self::load_meta_with_depth(&checkpoint)?;
        if depth >= MAX_META_DELTA_CHAIN {
            return Ok((full_bytes, true));
        }
        let delta_bytes = ((META_DELTA_HEADER_SIZE + META_DELTA_CHECKSUM_SIZE) as u64)
            .checked_add(
                (dirty_page_count as u64)
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE + 8) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .and_then(|size| size.checked_add(self.engine.allocator().to_bytes().len() as u64))
            .ok_or(Error::DiskFull)?;
        Ok((delta_bytes, false))
    }

    pub(super) fn save_generation_meta(&self, path: &Path, parent: Manifest) -> Result<u64> {
        if parent.pmt_checkpoint_id.get() == 0 {
            return Self::save_meta_without_directory_sync(
                path,
                self.engine.pmt(),
                self.engine.allocator(),
            );
        }
        let parent_path = self
            .path
            .join(format!("seerdb.meta.{}", parent.pmt_checkpoint_id.get()));
        let (parent_pmt, _, depth) = Self::load_meta_with_depth(&parent_path)?;
        if depth >= MAX_META_DELTA_CHAIN {
            Self::save_meta_without_directory_sync(path, self.engine.pmt(), self.engine.allocator())
        } else {
            Self::save_meta_delta_without_directory_sync(
                path,
                parent.pmt_checkpoint_id.get(),
                &parent_pmt,
                self.engine.pmt(),
                self.engine.allocator(),
            )
        }
    }
}
