//! PMT/allocator checkpoint and bounded metadata-delta lifecycle.
//!
//! `DB` owns which checkpoint becomes authoritative during publication. This
//! module owns the checkpoint representation, delta-chain validation, retained
//! offset validation, and metadata footprint calculations used by that state
//! machine.

use super::metadata_codec::{
    MAX_META_DELTA_CHAIN, META_DELTA_CHECKSUM_SIZE, META_DELTA_HEADER_SIZE, META_DELTA_MAGIC,
    META_MAGIC, decode_checkpoint, decode_delta, decode_legacy_checkpoint, encode_checkpoint,
    encode_delta,
};
use super::{DB, atomic_write, atomic_write_without_directory_sync};
use crate::allocator::PageAllocator;
use crate::error::{Error, Result};
use crate::mvcc::{PMT, PageMapping};
use crate::storage::format::Manifest;
use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::Path;

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
                let delta = decode_delta(&data)?;
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
                decode_checkpoint(&data)?
            } else {
                decode_legacy_checkpoint(&data)?
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
        let buf = encode_checkpoint(pmt, allocator)?;

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
        let buf = encode_delta(parent_checkpoint_id, parent_pmt, pmt, allocator)?;
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
            let delta = decode_delta(&data)?;
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
