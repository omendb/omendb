use super::*;

use crate::memtable::Entry;
use crate::types::InternalKey;
use crate::vlog::{VLog, ValuePointer};
use block::Block;
use bytes::Bytes;
use quick_cache::sync::Cache;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ============================================================================
// SSTableIterator - Iterator over all entries
// ============================================================================

pub struct SSTableIterator {
    pub(super) entries: std::vec::IntoIter<(Bytes, Bytes)>,
}

impl Iterator for SSTableIterator {
    type Item = Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(Ok)
    }
}

impl DoubleEndedIterator for SSTableIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.entries.next_back().map(Ok)
    }
}

// ============================================================================
// SSTableRangeIterator - Lazy iterator over a key range
// ============================================================================

/// Lazy iterator that loads blocks on-demand during range scans
pub struct SSTableRangeIterator {
    file: Arc<Mutex<File>>, // Reuse file handle from parent SSTable
    block_cache: Arc<Cache<u64, Block>>,
    vlog: Option<Arc<Mutex<VLog>>>,
    top_level_index: Vec<TopLevelIndexEntry>,
    start_key: Bytes,
    end_key: Option<Bytes>,
    // Cache performance metrics (shared with parent SSTable)
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,

    // Iteration state
    top_idx: usize,
    current_index_block: Option<Block>,
    index_block_entries: Vec<(u64, u32)>, // (offset, size) pairs for data blocks
    index_entry_idx: usize,
    // Store entries from current data block (avoids lifetime issues with iterator)
    current_block_entries: Vec<(Bytes, Bytes)>,
    current_entry_idx: usize,

    // Read-ahead prefetching
    readahead_size: usize,

    // Key-only iteration (BadgerDB pattern)
    read_values: bool,
}

impl SSTableRangeIterator {
    pub(super) fn new(
        file: Arc<Mutex<File>>,
        block_cache: Arc<Cache<u64, Block>>,
        vlog: Option<Arc<Mutex<VLog>>>,
        top_level_index: Vec<TopLevelIndexEntry>,
        start_key: &[u8],
        end_key: Option<&[u8]>,
        cache_hits: Arc<AtomicU64>,
        cache_misses: Arc<AtomicU64>,
        read_values: bool,
    ) -> Self {
        Self {
            file,
            block_cache,
            vlog,
            top_level_index,
            start_key: Bytes::copy_from_slice(start_key),
            end_key: end_key.map(Bytes::copy_from_slice),
            cache_hits,
            cache_misses,
            top_idx: 0,
            current_index_block: None,
            index_block_entries: Vec::new(),
            index_entry_idx: 0,
            current_block_entries: Vec::new(),
            current_entry_idx: 0,
            readahead_size: 2,
            read_values,
        }
    }

    fn load_block(&self, offset: u64, size: u32) -> Result<Block> {
        // Check cache first (quick_cache is lock-free!)
        if let Some(block) = self.block_cache.get(&offset) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(block);
        }

        // Cache miss - record and load from disk
        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        let mut file = self.file.lock().expect("file mutex poisoned");
        file.seek(SeekFrom::Start(offset))?;

        let mut buf = vec![0u8; size as usize];
        file.read_exact(&mut buf)?;
        let block_data = Bytes::from(buf);
        drop(file);

        let block = Block::from_bytes(block_data)?;
        self.block_cache.insert(offset, block.clone());

        Ok(block)
    }

    fn prefetch_data_blocks(&self) {
        for i in 1..=self.readahead_size {
            let next_idx = self.index_entry_idx + i;
            if next_idx < self.index_block_entries.len() {
                let (offset, size) = self.index_block_entries[next_idx];
                if self.block_cache.get(&offset).is_none() {
                    let _ = self.load_block(offset, size);
                }
            }
        }
    }

    fn advance_to_next_index_block(&mut self) -> Result<bool> {
        // Find next relevant top-level index block
        while self.top_idx < self.top_level_index.len() {
            let top_entry = &self.top_level_index[self.top_idx];

            // Skip blocks that end before our start key
            if top_entry.last_key.as_ref() < self.start_key.as_ref() {
                self.top_idx += 1;
                continue;
            }

            // Stop if we've gone past end_key
            if let Some(ref end) = self.end_key {
                if top_entry.last_key.as_ref() >= end.as_ref() && self.current_index_block.is_some()
                {
                    return Ok(false);
                }
            }

            // Load this index block
            let index_block = self.load_block(top_entry.offset, top_entry.size)?;

            // Extract data block offsets/sizes from index block
            self.index_block_entries.clear();
            for entry_result in index_block.iter() {
                let (_key, value) = entry_result?;

                if value.len() < 12 {
                    continue;
                }

                let value_len = value.len();
                let mut offset_bytes = [0u8; 8];
                let mut size_bytes = [0u8; 4];
                offset_bytes.copy_from_slice(&value[value_len - 12..value_len - 4]);
                size_bytes.copy_from_slice(&value[value_len - 4..]);

                let offset = u64::from_le_bytes(offset_bytes);
                let size = u32::from_le_bytes(size_bytes);

                self.index_block_entries.push((offset, size));
            }

            self.current_index_block = Some(index_block);
            self.index_entry_idx = 0;
            self.top_idx += 1;

            return Ok(true);
        }

        Ok(false)
    }

    #[inline]
    fn advance_to_next_data_block(&mut self) -> Result<bool> {
        if self.index_entry_idx >= self.index_block_entries.len()
            && !self.advance_to_next_index_block()?
        {
            return Ok(false);
        }

        if self.index_entry_idx < self.index_block_entries.len() {
            let (offset, size) = self.index_block_entries[self.index_entry_idx];
            let data_block = self.load_block(offset, size)?;

            self.current_block_entries.clear();
            for entry_result in data_block.iter() {
                let (key, value) = entry_result?;
                self.current_block_entries.push((key, value));
            }

            self.current_entry_idx = 0;
            self.index_entry_idx += 1;

            self.prefetch_data_blocks();

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Iterator for SSTableRangeIterator {
    type Item = Result<(Bytes, Entry)>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get next entry from current data block
            while self.current_entry_idx < self.current_block_entries.len() {
                let (key, entry_value) = &self.current_block_entries[self.current_entry_idx];
                self.current_entry_idx += 1;

                // Check if key is in range
                if key.as_ref() < self.start_key.as_ref() {
                    continue;
                }
                if let Some(ref end) = self.end_key {
                    if key.as_ref() >= end.as_ref() {
                        return None; // Past end of range
                    }
                }

                // Extract user key from encoded InternalKey (handles both MVCC and legacy formats)
                let user_key = InternalKey::extract_user_key(key);

                if !self.read_values {
                    return Some(Ok((user_key, Entry::Value(Bytes::new()))));
                }

                if entry_value.is_empty() {
                    continue;
                }

                let flag = entry_value[0];
                let data = entry_value.slice(1..);

                let entry = match flag {
                    FLAG_INLINE => Entry::Value(data),
                    FLAG_POINTER => {
                        if data.len() < 12 {
                            continue;
                        }

                        let offset = u64::from_le_bytes([
                            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                        ]);
                        let length = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);

                        if let Some(ref vlog) = self.vlog {
                            let mut vlog_guard = vlog.lock().expect("vlog mutex poisoned");
                            let pointer = ValuePointer { offset, length };
                            match vlog_guard.read(pointer) {
                                Ok(value) => Entry::Value(value),
                                Err(e) => return Some(Err(SSTableError::VLog(e.to_string()))),
                            }
                        } else {
                            return Some(Err(SSTableError::VLog("VLog not attached".to_string())));
                        }
                    }
                    FLAG_TOMBSTONE => Entry::Tombstone,
                    FLAG_MERGE => Entry::Merge(vec![data]),
                    _ => continue,
                };

                return Some(Ok((user_key, entry)));
            }

            // Need to advance to next data block
            match self.advance_to_next_data_block() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
