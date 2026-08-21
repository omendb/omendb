//! PMT/allocator checkpoint and bounded metadata-delta lifecycle.
//!
//! `DB` owns which checkpoint becomes authoritative during publication. This
//! module owns the checkpoint representation, the append-only metadata log
//! that durably orders checkpoints and deltas before the manifest barrier,
//! delta-chain validation, retained offset validation, and metadata footprint
//! calculations used by that state machine. The legacy whole-image
//! `seerdb.meta` file remains the bootstrap fallback for databases without a
//! published manifest.

#[cfg(any(test, feature = "fault-injection"))]
use super::faults::{FAIL_NEXT_META_LOG_SYNC, FAIL_NEXT_META_LOG_WRITE};
use super::metadata_codec::{
    MAX_META_DELTA_CHAIN, META_DELTA_CHECKSUM_SIZE, META_DELTA_HEADER_SIZE, META_DELTA_MAGIC,
    META_LOG_FRAME_HEADER_SIZE, META_LOG_HEADER_SIZE, META_MAGIC, MetaLogEntry, ParsedMetaLog,
    decode_checkpoint, decode_legacy_checkpoint, encode_checkpoint, encode_delta,
    encode_meta_log_frame, meta_log_header_bytes, parse_meta_log,
};
use super::{DB, META_LOG_FILE, atomic_write, atomic_write_without_directory_sync};
use crate::allocator::PageAllocator;
use crate::error::{Error, Result};
use crate::mvcc::{PMT, PageMapping};
use crate::storage::format::Manifest;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

impl DB {
    /// Load PMT and allocator from the legacy bootstrap meta file.
    pub(super) fn load_meta(path: &Path) -> Result<(PMT, PageAllocator)> {
        Self::load_meta_with_depth(path).map(|(pmt, allocator, _)| (pmt, allocator))
    }

    /// Load a legacy full checkpoint file. Pre-manifest databases only ever
    /// store whole-image checkpoints, so no delta-chain walk is needed.
    fn load_meta_with_depth(path: &Path) -> Result<(PMT, PageAllocator, usize)> {
        let data = fs::read(path)?;
        if data.len() >= META_DELTA_MAGIC.len()
            && data[..META_DELTA_MAGIC.len()] == META_DELTA_MAGIC
        {
            return Err(Error::Corruption(
                "legacy metadata file cannot be a delta".into(),
            ));
        }
        let (pmt, allocator) =
            if data.len() >= META_MAGIC.len() && data[..META_MAGIC.len()] == META_MAGIC {
                decode_checkpoint(&data)?
            } else {
                decode_legacy_checkpoint(&data)?
            };
        Ok((pmt, allocator, 0))
    }

    /// Save PMT and allocator to the legacy bootstrap meta file.
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

    /// Return the parsed valid prefix of this database's metadata log.
    pub(super) fn read_meta_log(path: &Path) -> Result<Option<ParsedMetaLog>> {
        let log_path = Self::metadata_log_path(path);
        let bytes = match fs::read(&log_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        parse_meta_log(&bytes).map(Some)
    }

    pub(super) fn metadata_log_path(path: &Path) -> PathBuf {
        path.join(META_LOG_FILE)
    }

    /// Resolve a checkpoint ID against a parsed metadata log by walking its
    /// delta parents back to a full checkpoint and applying the deltas.
    pub(super) fn resolve_meta_log(
        parsed: &ParsedMetaLog,
        checkpoint_id: u64,
    ) -> Result<(PMT, PageAllocator, usize)> {
        let frames: HashMap<u64, &MetaLogEntry> = parsed
            .frames
            .iter()
            .map(|frame| (frame.checkpoint_id, &frame.entry))
            .collect();
        let mut deltas = Vec::new();
        let mut visited = HashSet::new();
        let mut current_id = checkpoint_id;
        let (mut pmt, mut allocator) = loop {
            let entry = frames.get(&current_id).ok_or_else(|| {
                Error::Corruption(format!(
                    "metadata checkpoint {current_id} is missing from the log{}",
                    if parsed.complete {
                        String::new()
                    } else {
                        " (the log ends at a torn or corrupt frame)".to_string()
                    }
                ))
            })?;
            match entry {
                MetaLogEntry::Checkpoint(pmt, allocator) => break (pmt.clone(), allocator.clone()),
                MetaLogEntry::Delta(delta) => {
                    if !visited.insert(delta.parent_checkpoint_id) {
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
                    if delta.parent_checkpoint_id == 0 {
                        return Err(Error::Corruption(
                            "metadata delta has no full checkpoint parent".into(),
                        ));
                    }
                    current_id = delta.parent_checkpoint_id;
                }
            }
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

    /// Return the checkpoint ID and every delta ancestor needed to resolve it.
    pub(super) fn meta_log_ancestors(
        parsed: &ParsedMetaLog,
        checkpoint_id: u64,
    ) -> Result<BTreeSet<u64>> {
        let frames: HashMap<u64, u64> = parsed
            .frames
            .iter()
            .map(|frame| match &frame.entry {
                MetaLogEntry::Delta(delta) => (frame.checkpoint_id, delta.parent_checkpoint_id),
                MetaLogEntry::Checkpoint(..) => (frame.checkpoint_id, 0),
            })
            .collect();
        let mut ancestors = BTreeSet::new();
        let mut current_id = checkpoint_id;
        for _ in 0..=MAX_META_DELTA_CHAIN {
            if !ancestors.insert(current_id) {
                return Err(Error::Corruption(
                    "metadata delta chain contains a cycle".into(),
                ));
            }
            let Some(&parent) = frames.get(&current_id) else {
                return Err(Error::Corruption(format!(
                    "metadata checkpoint {current_id} is missing from the log"
                )));
            };
            if parent == 0 {
                return Ok(ancestors);
            }
            current_id = parent;
        }
        Err(Error::Corruption(format!(
            "metadata delta chain exceeds maximum length {MAX_META_DELTA_CHAIN}"
        )))
    }

    /// Load a checkpoint chain from the metadata log at `path`.
    pub(super) fn load_meta_by_id_path(
        path: &Path,
        checkpoint_id: u64,
    ) -> Result<(PMT, PageAllocator, usize)> {
        let parsed = Self::read_meta_log(path)?
            .ok_or_else(|| Error::Corruption("metadata log is missing".into()))?;
        Self::resolve_meta_log(&parsed, checkpoint_id)
    }

    /// Load a checkpoint chain from this database's metadata log.
    pub(super) fn load_meta_by_id(
        &self,
        checkpoint_id: u64,
    ) -> Result<(PMT, PageAllocator, usize)> {
        let parsed = Self::read_meta_log(&self.path)?
            .ok_or_else(|| Error::Corruption("metadata log is missing".into()))?;
        Self::resolve_meta_log(&parsed, checkpoint_id)
    }

    /// Append the durable checkpoint or delta frame for `checkpoint_id`.
    ///
    /// The frame is fully written and synced before the caller may publish a
    /// manifest naming this checkpoint; that ordering replaces the per-file
    /// create plus directory barrier of the per-generation checkpoint files.
    /// Returns the bytes written and whether this call created the log file
    /// (the caller must then make the new directory entry durable before the
    /// manifest barrier).
    pub(super) fn append_generation_meta(
        &self,
        checkpoint_id: u64,
        parent: Manifest,
    ) -> Result<(u64, bool)> {
        let log_path = Self::metadata_log_path(&self.path);
        let existed = log_path.exists();
        let bytes = if existed {
            fs::read(&log_path)?
        } else {
            Vec::new()
        };
        let parsed = if existed {
            Some(parse_meta_log(&bytes)?)
        } else {
            None
        };
        if let Some(parsed) = &parsed
            && parsed.valid_len < bytes.len()
        {
            // An abandoned torn tail from a crash during append must be
            // durably removed before new frames land behind it, or a later
            // crash would leave the new frame unreachable behind the torn
            // boundary.
            let file = OpenOptions::new().write(true).open(&log_path)?;
            file.set_len(parsed.valid_len as u64)?;
            file.sync_all()?;
        }

        let payload = match (parent.pmt_checkpoint_id.get(), parsed.as_ref()) {
            (0, _) => encode_checkpoint(self.engine.pmt(), self.engine.allocator())?,
            (parent_id, Some(parsed)) => {
                let (parent_pmt, _, depth) = Self::resolve_meta_log(parsed, parent_id)?;
                if depth >= MAX_META_DELTA_CHAIN {
                    encode_checkpoint(self.engine.pmt(), self.engine.allocator())?
                } else {
                    encode_delta(
                        parent_id,
                        &parent_pmt,
                        self.engine.pmt(),
                        self.engine.allocator(),
                    )?
                }
            }
            (parent_id, None) => {
                return Err(Error::Corruption(format!(
                    "metadata log is missing parent checkpoint {parent_id}"
                )));
            }
        };
        let frame = encode_meta_log_frame(checkpoint_id, &payload)?;

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_META_LOG_WRITE.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected metadata log write failure").into());
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        if !existed {
            file.write_all(&meta_log_header_bytes())?;
        }
        file.write_all(&frame)?;
        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_META_LOG_SYNC.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected metadata log sync failure").into());
        }
        file.sync_all()?;

        let header_bytes = u64::from(!existed) * META_LOG_HEADER_SIZE as u64;
        Ok((header_bytes + frame.len() as u64, !existed))
    }

    /// Rewrite the metadata log keeping only the retained checkpoint IDs.
    ///
    /// Retained sets are ancestor-closed, so keeping exactly the retained
    /// frames preserves every retained delta chain. Returns the number of
    /// dropped frames and the bytes they occupied.
    pub(super) fn compact_metadata_log(&self, retained: &BTreeSet<u64>) -> Result<(u64, u64)> {
        let Some(parsed) = Self::read_meta_log(&self.path)? else {
            return Ok((0, 0));
        };
        let removed_frames = parsed
            .frames
            .iter()
            .filter(|frame| !retained.contains(&frame.checkpoint_id))
            .count() as u64;
        if removed_frames == 0 {
            return Ok((0, 0));
        }
        let reclaimed_bytes = parsed
            .frames
            .iter()
            .filter(|frame| !retained.contains(&frame.checkpoint_id))
            .map(|frame| frame.raw.len() as u64)
            .sum();
        let mut bytes = Vec::with_capacity(parsed.valid_len);
        bytes.extend_from_slice(&meta_log_header_bytes());
        for frame in &parsed.frames {
            if retained.contains(&frame.checkpoint_id) {
                bytes.extend_from_slice(&frame.raw);
            }
        }
        atomic_write(&Self::metadata_log_path(&self.path), &bytes)?;
        Ok((removed_frames, reclaimed_bytes))
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
        let frame_overhead = META_LOG_FRAME_HEADER_SIZE as u64;
        let full_bytes = (META_MAGIC.len() as u64)
            .checked_add(4 + 4 + 4 + 4)
            .and_then(|size| size.checked_add(pmt_bytes))
            .and_then(|size| size.checked_add(allocator_bytes))
            .and_then(|size| size.checked_add(frame_overhead))
            .ok_or(Error::DiskFull)?;
        if parent.pmt_checkpoint_id.get() == 0 {
            return Ok((full_bytes, true));
        }

        let parsed = Self::read_meta_log(&self.path)?
            .ok_or_else(|| Error::Corruption("metadata log is missing".into()))?;
        let (_, _, depth) = Self::resolve_meta_log(&parsed, parent.pmt_checkpoint_id.get())?;
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
            .and_then(|size| size.checked_add(frame_overhead))
            .ok_or(Error::DiskFull)?;
        Ok((delta_bytes, false))
    }
}
