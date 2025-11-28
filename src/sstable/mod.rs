// Public API takes Bytes by value intentionally - O(1) clone, convenient for callers
#![allow(clippy::needless_pass_by_value)]

pub mod block;
mod builder;
mod error;
mod iter;

pub use builder::SSTableBuilder;
pub use error::{Result, SSTableError};
pub use iter::{SSTableIterator, SSTableRangeIterator};

use crate::alex::AlexTree;
use crate::bloom::BloomFilter;
use crate::buffer::{BufferPool, PageId};
use crate::types::{InternalKey, ValueType};
use crate::vlog::{VLog, ValuePointer};
use block::Block;
pub use block::CompressionType;
use bytes::Bytes;
use quick_cache::sync::Cache;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Magic number for `SSTable` format: "SSTB"
const MAGIC: u32 = 0x5353_5442;
const VERSION: u32 = 0x0000_0002; // v2: Added prefix bloom filter

/// Entry value type flags
pub const FLAG_INLINE: u8 = 0x00;
pub const FLAG_POINTER: u8 = 0x01;
pub const FLAG_TOMBSTONE: u8 = 0x02;
pub const FLAG_MERGE: u8 = 0x03;

/// Default prefix length for prefix bloom filter
pub const DEFAULT_PREFIX_LEN: usize = 3;

/// Top-level index entry (loaded into RAM)
#[derive(Debug, Clone)]
struct TopLevelIndexEntry {
    last_key: Bytes,
    offset: u64,
    size: u32,
}

/// Convert bytes key to i64 for ALEX index
/// Uses big-endian to preserve lexicographic ordering
fn bytes_to_i64(bytes: &[u8]) -> i64 {
    // Convert key bytes to i64 while preserving lexicographic ordering
    // Bug #11 fix: Previous implementation only used first 8 bytes, causing collisions
    // when keys share the same prefix (e.g., "key_0000000000" and "key_0000000100" both
    // had "key_0000" as first 8 bytes, hashing to the same value)
    //
    // New approach: Use bytes at strategic positions to capture differences
    // Position 0, 2, 4, 6, 8, 10, 12, len-1 gives good spread while maintaining some ordering

    let len = bytes.len();
    if len <= 8 {
        // Short keys: use all bytes with padding
        let mut buf = [0u8; 8];
        buf[..len].copy_from_slice(&bytes[..len]);
        i64::from_be_bytes(buf)
    } else {
        // Long keys: sample bytes at multiple positions to capture structure
        // This provides better collision resistance than first/last only
        let positions = [
            0,
            2,
            4,
            6,
            8.min(len - 1),
            10.min(len - 1),
            12.min(len - 1),
            len - 1,
        ];
        let mut buf = [0u8; 8];
        for (i, &pos) in positions.iter().enumerate() {
            if pos < len {
                buf[i] = bytes[pos];
            }
        }
        i64::from_be_bytes(buf)
    }
}

// ============================================================================
// SSTable - Read SSTables with lazy loading
// ============================================================================

/// `SSTable` reader with lazy block loading
///
/// File handle optimization: Keeps file open for the lifetime of the `SSTable` to eliminate
/// repeated open/close overhead. Each `SSTable` maintains 1 file descriptor, bounded by the
/// number of `SSTables` in the LSM tree (typically 70-350 across all levels).
pub struct SSTable {
    file: Arc<Mutex<File>>, // File handle kept open for zero-overhead reads
    path: PathBuf,
    top_level_index: Vec<TopLevelIndexEntry>,
    #[allow(dead_code)]
    // Disabled due to key collision issues (Bug #9), binary search used instead
    alex_index: Option<AlexTree>, // ALEX learned index for faster lookups
    bloom: BloomFilter,
    prefix_bloom: Option<BloomFilter>,
    prefix_len: usize,
    num_entries: u64,
    vlog: Option<Arc<Mutex<VLog>>>,
    block_cache: Arc<Cache<u64, Block>>, // LRU cache with size limits
    min_key: Option<Bytes>,
    max_key: Option<Bytes>,
    /// Maximum sequence number in this `SSTable`
    /// Used to coordinate flush and compaction to prevent live key deletion
    max_sequence: u64,
    // Cache performance metrics (Arc for sharing with iterators)
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,
    /// Optional global block cache shared across all `SSTables`
    /// Key: (`path_hash`, `block_offset`), Value: raw block data
    global_cache: Option<Arc<Cache<(u64, u64), Bytes>>>,
    /// Optional `LeanStore` `BufferPool` (L2 Raw Page Cache)
    /// Replaces `global_cache/OS` cache usage when enabled
    buffer_pool: Option<Arc<BufferPool>>,
    /// Hash of this `SSTable`'s path for global cache key
    path_hash: u64,
}

impl SSTable {
    /// Hash a path to a u64 for use as cache key
    fn hash_path(path: &Path) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        hasher.finish()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, None, None)
    }

    /// Open `SSTable` with optional global block cache
    ///
    /// When a global cache is provided, blocks are cached there with key (`path_hash`, offset).
    /// This allows hot blocks to be shared across all `SSTables`, improving cache hit rates.
    pub fn open_with_global_cache(
        path: impl AsRef<Path>,
        global_cache: Option<Arc<Cache<(u64, u64), Bytes>>>,
    ) -> Result<Self> {
        Self::open_with_options(path, global_cache, None)
    }

    pub fn open_with_buffer_pool(
        path: impl AsRef<Path>,
        buffer_pool: Option<Arc<BufferPool>>,
    ) -> Result<Self> {
        Self::open_with_options(path, None, buffer_pool)
    }

    fn open_with_options(
        path: impl AsRef<Path>,
        global_cache: Option<Arc<Cache<(u64, u64), Bytes>>>,
        buffer_pool: Option<Arc<BufferPool>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let path_hash = Self::hash_path(&path);

        // Open file once and keep handle for the lifetime of this SSTable
        // This eliminates repeated open/close overhead (7.1x faster than opening per read)
        let mut file = File::open(&path)?;

        let (num_entries, max_sequence, prefix_len) = Self::read_header(&mut file)?;
        let (top_level_offset, bloom_offset, prefix_bloom_offset, metadata_offset) =
            Self::read_footer(&mut file)?;
        let top_level_index = Self::load_top_level_index(&mut file, top_level_offset)?;
        let bloom = Self::load_bloom_filter(&mut file, bloom_offset)?;
        let prefix_bloom = if prefix_bloom_offset > 0 {
            Some(Self::load_bloom_filter(&mut file, prefix_bloom_offset)?)
        } else {
            None
        };
        let (min_key, max_key) = Self::load_metadata(&mut file, metadata_offset)?;

        // Build ALEX learned index for faster top-level index lookups
        let alex_index = if !top_level_index.is_empty() {
            let mut alex = AlexTree::new();
            for (idx, entry) in top_level_index.iter().enumerate() {
                let key_i64 = bytes_to_i64(&entry.last_key);
                // Store index position as value (encoded as bytes)
                let value = (idx as u64).to_le_bytes().to_vec();
                if alex.insert(key_i64, value).is_err() {
                    // ALEX insert failed - fall back to binary search
                    break;
                }
            }
            Some(alex)
        } else {
            None
        };

        // Create LRU block cache with capacity for 10,000 blocks (~40MB at 4KB/block)
        // This is a local fallback cache if global cache is not provided
        let block_cache = Arc::new(Cache::new(10_000));

        Ok(Self {
            file: Arc::new(Mutex::new(file)), // Keep file handle for reuse
            path,
            top_level_index,
            alex_index,
            bloom,
            prefix_bloom,
            prefix_len,
            num_entries,
            vlog: None,
            block_cache,
            min_key,
            max_key,
            max_sequence,
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            global_cache,
            buffer_pool,
            path_hash,
        })
    }

    pub fn with_vlog(mut self, vlog: VLog) -> Self {
        self.vlog = Some(Arc::new(Mutex::new(vlog)));
        self
    }

    /// Get maximum sequence number in this `SSTable`
    pub const fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    /// Get the minimum key in this `SSTable` (for range filtering)
    pub const fn min_key(&self) -> Option<&Bytes> {
        self.min_key.as_ref()
    }

    /// Get the maximum key in this `SSTable` (for range filtering)
    pub const fn max_key(&self) -> Option<&Bytes> {
        self.max_key.as_ref()
    }

    /// Get the configured prefix length for this `SSTable`
    pub const fn prefix_len(&self) -> usize {
        self.prefix_len
    }

    /// Check if this `SSTable`'s key range overlaps with [`start_key`, `end_key`)
    pub fn overlaps_range(&self, start_key: &[u8], end_key: Option<&[u8]>) -> bool {
        // If we don't have metadata, assume it overlaps (conservative)
        let (Some(min), Some(max)) = (&self.min_key, &self.max_key) else {
            return true;
        };

        // Check if ranges overlap
        // Range [min, max] overlaps with [start_key, end_key) if:
        // max >= start_key AND (end_key is None OR min < end_key)
        if max.as_ref() < start_key {
            return false; // SSTable range is entirely before query range
        }

        if let Some(end) = end_key {
            if min.as_ref() >= end {
                return false; // SSTable range is entirely after query range
            }
        }

        true // Ranges overlap
    }

    /// Check if key might be in this `SSTable` (bloom filter check)
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.contains(key)
    }

    /// Check if a prefix might be in this `SSTable` (prefix bloom filter check)
    /// Returns true if the prefix might exist, false if definitely not.
    /// If prefix filter is not present or prefix is too short, returns true (conservative).
    #[inline]
    pub fn may_contain_prefix(&self, prefix: &[u8]) -> bool {
        if let Some(ref pb) = self.prefix_bloom {
            if prefix.len() >= self.prefix_len {
                return pb.contains(&prefix[..self.prefix_len]);
            }
        }
        true
    }

    /// Get block cache statistics
    pub fn cache_stats(&self) -> (u64, u64, f64) {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };
        (hits, misses, hit_rate)
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        match self.get_entry(key)? {
            Some((data, flag)) => {
                if flag == FLAG_TOMBSTONE {
                    Ok(None)
                } else if flag == FLAG_MERGE {
                    // Legacy get() cannot handle merge operands without an operator
                    // For now, treat as "Found" but return data (caller beware)
                    // Ideally DB should use get_entry() to handle merges
                    Ok(Some(data))
                } else {
                    Ok(Some(data))
                }
            }
            None => Ok(None),
        }
    }

    /// Get entry with type flag
    pub fn get_entry(&mut self, key: &[u8]) -> Result<Option<(Bytes, u8)>> {
        if !self.bloom.contains(key) {
            return Ok(None);
        }

        let Some((index_block_offset, index_block_size)) = self.find_index_block(key) else {
            return Ok(None);
        };

        let index_block = self.load_block(index_block_offset, index_block_size)?;

        let Some((data_block_offset, data_block_size)) =
            self.find_in_index_block(&index_block, key)?
        else {
            return Ok(None);
        };

        let data_block = self.load_block(data_block_offset, data_block_size)?;

        self.find_in_data_block(&data_block, key)
    }

    /// Check if a key exists in this `SSTable` (even if it's a tombstone)
    /// Returns true if the key is present (value or tombstone), false otherwise
    pub fn contains(&mut self, key: &[u8]) -> Result<bool> {
        if !self.bloom.contains(key) {
            return Ok(false);
        }

        let Some((index_block_offset, index_block_size)) = self.find_index_block(key) else {
            return Ok(false);
        };

        let index_block = self.load_block(index_block_offset, index_block_size)?;

        let Some((data_block_offset, data_block_size)) =
            self.find_in_index_block(&index_block, key)?
        else {
            return Ok(false);
        };

        let data_block = self.load_block(data_block_offset, data_block_size)?;

        // Check if key exists in data block
        Ok(data_block.find_exact(key).is_some())
    }

    // ========================================================================
    // MVCC-aware get methods: Search for user_key with seq <= snapshot_seq
    // ========================================================================

    /// Get value for `user_key` visible at `snapshot_seq` (MVCC-aware)
    ///
    /// Searches for the first version of `user_key` with seq <= `snapshot_seq`.
    /// Returns:
    /// - Ok(Some(value)) if a live value is found
    /// - Ok(None) if key not found or deleted (tombstone)
    /// - Err if I/O or format error
    ///
    /// IMPORTANT: Only works on `SSTables` built with `add_internal()` methods.
    pub fn get_mvcc(&mut self, user_key: &[u8], snapshot_seq: u64) -> Result<Option<Bytes>> {
        match self.get_entry_mvcc(user_key, snapshot_seq)? {
            Some((data, flag)) => {
                if flag == FLAG_TOMBSTONE {
                    Ok(None)
                } else {
                    Ok(Some(data))
                }
            }
            None => Ok(None),
        }
    }

    /// Get entry with type flag for `user_key` visible at `snapshot_seq` (MVCC-aware)
    ///
    /// Returns (`value_data`, flag) where flag indicates the entry type.
    pub fn get_entry_mvcc(
        &mut self,
        user_key: &[u8],
        snapshot_seq: u64,
    ) -> Result<Option<(Bytes, u8)>> {
        // Check bloom filter with user_key (bloom stores user keys, not encoded InternalKeys)
        if !self.bloom.contains(user_key) {
            return Ok(None);
        }

        // Create search key: looking for first entry with seq <= snapshot_seq
        // Since encoded keys sort by (user_key ASC, seq DESC), searching for
        // InternalKey(user_key, snapshot_seq) with find_lower_bound gives us
        // the first version <= snapshot_seq
        let search_key = InternalKey::new(
            Bytes::copy_from_slice(user_key),
            snapshot_seq,
            ValueType::Value,
        );
        let encoded_search_key = search_key.encode();

        // Find the index block containing this key
        let Some((index_block_offset, index_block_size)) =
            self.find_index_block(&encoded_search_key)
        else {
            return Ok(None);
        };

        let index_block = self.load_block(index_block_offset, index_block_size)?;

        // Find the data block containing this key
        let Some((data_block_offset, data_block_size)) =
            self.find_in_index_block(&index_block, &encoded_search_key)?
        else {
            return Ok(None);
        };

        let data_block = self.load_block(data_block_offset, data_block_size)?;

        // Search for the entry in the data block
        self.find_in_data_block_mvcc(&data_block, user_key, &encoded_search_key)
    }

    /// Find entry in data block using MVCC semantics
    ///
    /// Uses `find_mvcc` to locate the first visible version of the target `user_key`.
    /// This handles the `InternalKey` encoding correctly when one `user_key` is a
    /// prefix of another (e.g., "key1" vs "key10").
    #[inline]
    fn find_in_data_block_mvcc(
        &self,
        data_block: &Block,
        user_key: &[u8],
        encoded_search_key: &[u8],
    ) -> Result<Option<(Bytes, u8)>> {
        // find_mvcc scans forward from the lower bound to find matching user_key
        let Some((_found_key, entry_value)) = data_block.find_mvcc(encoded_search_key, user_key)
        else {
            return Ok(None);
        };

        // Found a matching entry - decode the value
        if entry_value.is_empty() {
            return Err(SSTableError::InvalidFormat);
        }

        let flag = entry_value[0];
        let data = entry_value.slice(1..);

        match flag {
            FLAG_INLINE | FLAG_MERGE => Ok(Some((data, flag))),
            FLAG_POINTER => {
                if data.len() < 12 {
                    return Err(SSTableError::InvalidFormat);
                }

                let offset = u64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                let length = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);

                if let Some(ref vlog) = self.vlog {
                    let mut vlog_guard = vlog.lock().expect("vlog mutex poisoned");
                    let pointer = ValuePointer { offset, length };
                    let value = vlog_guard
                        .read(pointer)
                        .map_err(|e| SSTableError::VLog(e.to_string()))?;
                    Ok(Some((value, flag)))
                } else {
                    Err(SSTableError::VLog("VLog not attached".to_string()))
                }
            }
            FLAG_TOMBSTONE => Ok(Some((Bytes::new(), FLAG_TOMBSTONE))),
            _ => Err(SSTableError::InvalidFormat),
        }
    }

    /// Get the raw entry (encoded key + value) for the latest version of a user key.
    ///
    /// Used by transactions for OCC conflict detection - returns the encoded
    /// `InternalKey` so the sequence number can be extracted.
    pub fn get_raw_entry(&mut self, user_key: &[u8]) -> Result<Option<(Bytes, Bytes)>> {
        // Check bloom filter
        if !self.bloom.contains(user_key) {
            return Ok(None);
        }

        // Search for the latest version (seq = u64::MAX)
        let search_key =
            InternalKey::new(Bytes::copy_from_slice(user_key), u64::MAX, ValueType::Value);
        let encoded_search_key = search_key.encode();

        // Find the index block
        let Some((index_block_offset, index_block_size)) =
            self.find_index_block(&encoded_search_key)
        else {
            return Ok(None);
        };

        let index_block = self.load_block(index_block_offset, index_block_size)?;

        // Find the data block
        let Some((data_block_offset, data_block_size)) =
            self.find_in_index_block(&index_block, &encoded_search_key)?
        else {
            return Ok(None);
        };

        let data_block = self.load_block(data_block_offset, data_block_size)?;

        // Get raw entry from data block using MVCC-aware lookup
        let Some((found_key, entry_value)) = data_block.find_mvcc(&encoded_search_key, user_key)
        else {
            return Ok(None);
        };

        Ok(Some((found_key, entry_value)))
    }

    #[inline]
    fn find_index_block(&self, key: &[u8]) -> Option<(u64, u32)> {
        // CRITICAL FIX (Bug #11): Disable ALEX for top-level index lookup
        // ALEX learned index cannot correctly handle keys with shared prefixes
        // (e.g., "key_0000000000" and "key_0000000100" produce non-monotonic i64 values)
        // The partition_point binary search is correct and fast (O(log N) where N is typically 2-10)
        let idx = self
            .top_level_index
            .partition_point(|entry| entry.last_key.as_ref() < key);

        if idx < self.top_level_index.len() {
            Some((
                self.top_level_index[idx].offset,
                self.top_level_index[idx].size,
            ))
        } else {
            self.top_level_index.last().map(|e| (e.offset, e.size))
        }
    }

    #[inline]
    fn find_in_index_block(&self, index_block: &Block, key: &[u8]) -> Result<Option<(u64, u32)>> {
        // Binary search for first entry where entry_key >= key
        let Some((_entry_key, entry_value)) = index_block.find_lower_bound(key) else {
            return Ok(None);
        };
        let value_len = entry_value.len();

        if value_len < 12 {
            return Err(SSTableError::InvalidFormat);
        }

        let mut offset_bytes = [0u8; 8];
        let mut size_bytes = [0u8; 4];
        offset_bytes.copy_from_slice(&entry_value[value_len - 12..value_len - 4]);
        size_bytes.copy_from_slice(&entry_value[value_len - 4..]);

        let offset = u64::from_le_bytes(offset_bytes);
        let size = u32::from_le_bytes(size_bytes);

        Ok(Some((offset, size)))
    }

    #[inline]
    fn find_in_data_block(&self, data_block: &Block, key: &[u8]) -> Result<Option<(Bytes, u8)>> {
        // Binary search for exact key match
        let Some((_entry_key, entry_value)) = data_block.find_exact(key) else {
            return Ok(None);
        };

        if entry_value.is_empty() {
            return Err(SSTableError::InvalidFormat);
        }

        let flag = entry_value[0];
        let data = entry_value.slice(1..);

        match flag {
            FLAG_INLINE | FLAG_MERGE => Ok(Some((data, flag))),
            FLAG_POINTER => {
                if data.len() < 12 {
                    return Err(SSTableError::InvalidFormat);
                }

                let offset = u64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                let length = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);

                if let Some(ref vlog) = self.vlog {
                    let mut vlog_guard = vlog.lock().expect("vlog mutex poisoned");
                    let pointer = ValuePointer { offset, length };
                    let value = vlog_guard
                        .read(pointer)
                        .map_err(|e| SSTableError::VLog(e.to_string()))?;
                    Ok(Some((value, flag)))
                } else {
                    Err(SSTableError::VLog("VLog not attached".to_string()))
                }
            }
            FLAG_TOMBSTONE => Ok(Some((Bytes::new(), FLAG_TOMBSTONE))),
            _ => Err(SSTableError::InvalidFormat),
        }
    }

    fn load_block(&self, offset: u64, size: u32) -> Result<Block> {
        // Fast path 1: Check global cache first (shared across all SSTables)
        if let Some(ref global) = self.global_cache {
            let cache_key = (self.path_hash, offset);
            if let Some(block_data) = global.get(&cache_key) {
                // Global cache hit!
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                // Parse the cached bytes into a Block (no CRC check needed - already verified)
                return Block::from_bytes(block_data)
                    .map_err(|e| SSTableError::Io(std::io::Error::other(e)));
            }
        }

        // Fast path 2: Check local cache (per-SSTable fallback)
        if let Some(block) = self.block_cache.get(&offset) {
            // Local cache hit!
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(block);
        }

        // Cache miss - record and load from disk
        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        if let Some(ref pool) = self.buffer_pool {
            // LeanStore Path: Use BufferPool for L2 caching
            let page_id = PageId {
                file_id: self.path_hash,
                offset,
            };

            let frame_ref = pool.get_page(page_id, |buf| -> Result<()> {
                // Load from disk via shared file handle
                let mut file = self.file.lock().expect("file mutex poisoned");
                file.seek(SeekFrom::Start(offset))?;

                // Resize buffer to fit data
                // This reuses memory if capacity is sufficient
                // If size > capacity, it will reallocate, but next time capacity will be larger
                buf.resize(size as usize, 0);

                file.read_exact(buf)?;
                Ok(())
            })?;

            // Zero-Copy: Create Block viewing the Frame
            // We use BlockData::Borrowed to avoid copying 4KB
            let block = Block::new(block::BlockData::Borrowed(frame_ref))?;

            // NOTE: We do NOT cache Borrowed blocks in block_cache (L1)
            // This avoids pinning frames in L2 indefinitely.
            // This implements "Option A: Ephemeral Blocks" from Phase 3 Design.

            return Ok(block);
        }

        // Standard Path: Direct File IO (OS Cache)
        let mut file = self.file.lock().expect("file mutex poisoned");
        file.seek(SeekFrom::Start(offset))?;

        let mut buf = vec![0u8; size as usize];
        file.read_exact(&mut buf)?;
        drop(file); // Release lock before CRC verification
        let block_data = Bytes::from(buf);

        // Parse and verify block (CRC check happens here)
        let block = Block::from_bytes(block_data.clone())?;

        // Cache the verified block in global cache (if available)
        if let Some(ref global) = self.global_cache {
            let cache_key = (self.path_hash, offset);
            global.insert(cache_key, block_data);
        }

        // Also cache in local cache (automatic LRU eviction when full)
        self.block_cache.insert(offset, block.clone());

        Ok(block)
    }

    fn read_header(file: &mut File) -> Result<(u64, u64, usize)> {
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 32];
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let version = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

        if magic != MAGIC {
            return Err(SSTableError::InvalidFormat);
        }
        // Backward compatibility for v1
        let prefix_len = if version >= 2 {
            u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize
        } else {
            0
        };

        let num_entries = u64::from_le_bytes([
            header[16], header[17], header[18], header[19], header[20], header[21], header[22],
            header[23],
        ]);

        let max_sequence = u64::from_le_bytes([
            header[24], header[25], header[26], header[27], header[28], header[29], header[30],
            header[31],
        ]);

        Ok((num_entries, max_sequence, prefix_len))
    }

    fn read_footer(file: &mut File) -> Result<(u64, u64, u64, u64)> {
        let file_size = file.metadata()?.len();

        // Read last 48 bytes first (common minimal footer size)
        // v1: 48 bytes. Magic at offset 36 (from start of 48 byte buffer).
        // v2: 56 bytes. Magic at offset 44 (from start of 56 byte buffer).
        //
        // Magic is always 12 bytes from the end.
        // Version is always 8 bytes from the end.

        if file_size < 48 {
            return Err(SSTableError::InvalidFormat);
        }

        // Check Magic and Version at the end of file
        file.seek(SeekFrom::Start(file_size - 12))?;
        let mut tail_buf = [0u8; 12];
        file.read_exact(&mut tail_buf)?;

        let magic = u32::from_le_bytes(tail_buf[0..4].try_into().expect("slice length is correct"));
        let version =
            u32::from_le_bytes(tail_buf[4..8].try_into().expect("slice length is correct"));
        // Last 4 bytes are padding

        if magic != MAGIC {
            return Err(SSTableError::InvalidFormat);
        }

        // Determine footer size based on version
        let footer_size = if version >= 2 { 56 } else { 48 };

        if file_size < footer_size {
            return Err(SSTableError::InvalidFormat);
        }

        file.seek(SeekFrom::Start(file_size - footer_size))?;
        let mut footer = vec![0u8; footer_size as usize];
        file.read_exact(&mut footer)?;

        // Common fields (24 bytes)
        let _index_blocks_offset =
            u64::from_le_bytes(footer[0..8].try_into().expect("footer size guaranteed"));
        let top_level_offset =
            u64::from_le_bytes(footer[8..16].try_into().expect("footer size guaranteed"));
        let bloom_offset =
            u64::from_le_bytes(footer[16..24].try_into().expect("footer size guaranteed"));

        let (prefix_bloom_offset, metadata_offset, stored_checksum) = if version >= 2 {
            let prefix_bloom =
                u64::from_le_bytes(footer[24..32].try_into().expect("footer size guaranteed"));
            let metadata =
                u64::from_le_bytes(footer[32..40].try_into().expect("footer size guaranteed"));
            let checksum =
                u32::from_le_bytes(footer[40..44].try_into().expect("footer size guaranteed"));
            (prefix_bloom, metadata, checksum)
        } else {
            let metadata =
                u64::from_le_bytes(footer[24..32].try_into().expect("footer size guaranteed"));
            let checksum =
                u32::from_le_bytes(footer[32..36].try_into().expect("footer size guaranteed"));
            (0, metadata, checksum)
        };

        // Validate checksum
        let footer_start = file_size - footer_size;
        file.seek(SeekFrom::Start(0))?;
        let mut computed_checksum = 0u32;
        let mut buf = vec![0u8; 4096];
        let mut remaining = footer_start;

        while remaining > 0 {
            let to_read = remaining.min(4096) as usize;
            file.read_exact(&mut buf[..to_read])?;
            computed_checksum = crc32c::crc32c_append(computed_checksum, &buf[..to_read]);
            remaining -= to_read as u64;
        }

        if computed_checksum != stored_checksum {
            return Err(SSTableError::Corruption {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        Ok((
            top_level_offset,
            bloom_offset,
            prefix_bloom_offset,
            metadata_offset,
        ))
    }

    fn load_metadata(file: &mut File, offset: u64) -> Result<(Option<Bytes>, Option<Bytes>)> {
        file.seek(SeekFrom::Start(offset))?;

        // Read min_key
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let min_key_len = u32::from_le_bytes(len_buf) as usize;
        let min_key = if min_key_len > 0 {
            let mut key_buf = vec![0u8; min_key_len];
            file.read_exact(&mut key_buf)?;
            Some(Bytes::from(key_buf))
        } else {
            None
        };

        // Read max_key
        file.read_exact(&mut len_buf)?;
        let max_key_len = u32::from_le_bytes(len_buf) as usize;
        let max_key = if max_key_len > 0 {
            let mut key_buf = vec![0u8; max_key_len];
            file.read_exact(&mut key_buf)?;
            Some(Bytes::from(key_buf))
        } else {
            None
        };

        Ok((min_key, max_key))
    }

    fn load_top_level_index(file: &mut File, offset: u64) -> Result<Vec<TopLevelIndexEntry>> {
        file.seek(SeekFrom::Start(offset))?;

        let mut num_entries_buf = [0u8; 4];
        file.read_exact(&mut num_entries_buf)?;
        let num_entries = u32::from_le_bytes(num_entries_buf) as usize;

        let mut entries = Vec::with_capacity(num_entries);

        for _ in 0..num_entries {
            let mut key_len_buf = [0u8; 4];
            file.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            let mut key = vec![0u8; key_len];
            file.read_exact(&mut key)?;

            let mut offset_buf = [0u8; 8];
            file.read_exact(&mut offset_buf)?;
            let block_offset = u64::from_le_bytes(offset_buf);

            let mut size_buf = [0u8; 4];
            file.read_exact(&mut size_buf)?;
            let block_size = u32::from_le_bytes(size_buf);

            entries.push(TopLevelIndexEntry {
                last_key: Bytes::from(key),
                offset: block_offset,
                size: block_size,
            });
        }

        Ok(entries)
    }

    fn load_bloom_filter(file: &mut File, offset: u64) -> Result<BloomFilter> {
        file.seek(SeekFrom::Start(offset))?;

        let mut len_buf = [0u8; 8];
        file.read_exact(&mut len_buf)?;
        let bloom_len = u64::from_le_bytes(len_buf) as usize;

        let mut bloom_bytes = vec![0u8; bloom_len];
        file.read_exact(&mut bloom_bytes)?;

        BloomFilter::from_bytes(&bloom_bytes).ok_or(SSTableError::InvalidFormat)
    }

    pub const fn len(&self) -> usize {
        self.num_entries as usize
    }

    pub const fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Validate all blocks in the `SSTable` by loading and checking checksums
    /// This is expensive but useful for corruption detection
    pub fn validate(&mut self) -> Result<()> {
        let file_size = std::fs::metadata(&self.path)?.len();

        for top_entry in &self.top_level_index {
            if top_entry.offset + (top_entry.size as u64) > file_size {
                return Err(SSTableError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Block extends past end of file: offset={}, size={}, file_size={}",
                        top_entry.offset, top_entry.size, file_size
                    ),
                )));
            }

            let index_block = self.load_block(top_entry.offset, top_entry.size)?;

            for result in index_block.iter() {
                let (_key_bytes, value_bytes) = result?;

                // Index entry format: [key_len: 4][key: variable][offset: 8][size: 4]
                if value_bytes.len() < 16 {
                    continue;
                }

                let key_len =
                    u32::from_le_bytes(value_bytes[..4].try_into().expect("slice length checked"))
                        as usize;
                let offset_start = 4 + key_len;

                if value_bytes.len() < offset_start + 12 {
                    continue;
                }

                let offset = u64::from_le_bytes(
                    value_bytes[offset_start..offset_start + 8]
                        .try_into()
                        .expect("slice length checked"),
                );
                let size = u32::from_le_bytes(
                    value_bytes[offset_start + 8..offset_start + 12]
                        .try_into()
                        .expect("slice length checked"),
                );

                if offset + (size as u64) > file_size {
                    return Err(SSTableError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Data block extends past end of file: offset={offset}, size={size}, file_size={file_size}"
                        ),
                    )));
                }

                let _data_block = self.load_block(offset, size)?;
            }
        }

        Ok(())
    }

    /// Verify `SSTable` integrity by checking all block checksums
    ///
    /// Reads and validates the CRC32C checksum of every data block in the `SSTable`.
    /// Returns the number of blocks verified and total bytes read.
    ///
    /// # Errors
    ///
    /// Returns an error if any block has a checksum mismatch or I/O error.
    pub fn verify(&mut self) -> Result<SSTableVerifyResult> {
        let mut blocks_verified = 0u64;
        let mut bytes_verified = 0u64;

        for top_entry in &self.top_level_index {
            // Load and verify index block
            let index_block = self.load_block(top_entry.offset, top_entry.size)?;
            blocks_verified += 1;
            bytes_verified += top_entry.size as u64;

            // Verify all data blocks referenced by this index block
            for result in index_block.iter() {
                let (_key_bytes, value_bytes) = result?;

                // Index entry format: [key_len: 4][key: variable][offset: 8][size: 4]
                if value_bytes.len() < 16 {
                    continue;
                }

                let key_len =
                    u32::from_le_bytes(value_bytes[..4].try_into().expect("slice length checked"))
                        as usize;
                let offset_start = 4 + key_len;

                if value_bytes.len() < offset_start + 12 {
                    continue;
                }

                let offset = u64::from_le_bytes(
                    value_bytes[offset_start..offset_start + 8]
                        .try_into()
                        .expect("slice length checked"),
                );
                let size = u32::from_le_bytes(
                    value_bytes[offset_start + 8..offset_start + 12]
                        .try_into()
                        .expect("slice length checked"),
                );

                // Load block - this validates the CRC32C checksum
                let _data_block = self.load_block(offset, size)?;
                blocks_verified += 1;
                bytes_verified += size as u64;
            }
        }

        Ok(SSTableVerifyResult {
            blocks_verified,
            bytes_verified,
        })
    }
}

/// Result of `SSTable` verification
#[derive(Debug, Clone, Default)]
pub struct SSTableVerifyResult {
    /// Number of blocks verified
    pub blocks_verified: u64,
    /// Total bytes read and verified
    pub bytes_verified: u64,
}

impl SSTable {
    pub fn iter(&mut self) -> Result<SSTableIterator> {
        let mut entries = Vec::new();

        for top_entry in &self.top_level_index {
            let index_block = self.load_block(top_entry.offset, top_entry.size)?;

            for idx_entry in index_block.iter() {
                let (_index_key, index_value) = idx_entry?;

                let value_len = index_value.len();
                if value_len < 12 {
                    continue;
                }

                let mut offset_bytes = [0u8; 8];
                let mut size_bytes = [0u8; 4];
                offset_bytes.copy_from_slice(&index_value[value_len - 12..value_len - 4]);
                size_bytes.copy_from_slice(&index_value[value_len - 4..]);

                let data_offset = u64::from_le_bytes(offset_bytes);
                let data_size = u32::from_le_bytes(size_bytes);

                let data_block = self.load_block(data_offset, data_size)?;

                for data_entry in data_block.iter() {
                    let (key, entry_value) = data_entry?;

                    if entry_value.is_empty() {
                        continue;
                    }

                    let flag = entry_value[0];
                    let data = entry_value.slice(1..);

                    let value = match flag {
                        FLAG_INLINE => {
                            if self.vlog.is_some() {
                                // vlog attached - return decoded value
                                data
                            } else {
                                // No vlog attached (compaction) - return full entry with FLAG
                                entry_value
                            }
                        }
                        FLAG_POINTER => {
                            if data.len() < 12 {
                                continue;
                            }

                            let offset = u64::from_le_bytes([
                                data[0], data[1], data[2], data[3], data[4], data[5], data[6],
                                data[7],
                            ]);
                            let length = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);

                            if let Some(ref vlog) = self.vlog {
                                let mut vlog_guard = vlog.lock().expect("vlog mutex poisoned");
                                let pointer = ValuePointer { offset, length };
                                vlog_guard
                                    .read(pointer)
                                    .map_err(|e| SSTableError::VLog(e.to_string()))?
                            } else {
                                // No vlog attached (e.g., during compaction)
                                // Return the full entry including FLAG_POINTER + pointer bytes
                                // so compaction can preserve vlog pointers
                                entry_value
                            }
                        }
                        FLAG_TOMBSTONE => {
                            // CRITICAL FIX (Bug #7): Preserve tombstones during compaction
                            // Tombstones MUST be copied to output SSTables to prevent deleted keys
                            // from resurrecting from older levels after compaction completes.
                            if self.vlog.is_some() {
                                // User-facing read: filter out tombstones (deleted keys)
                                continue;
                            }
                            // Compaction: preserve tombstones in output SSTable
                            entry_value
                        }
                        _ => continue,
                    };

                    entries.push((key, value));
                }
            }
        }

        Ok(SSTableIterator {
            entries: entries.into_iter(),
        })
    }

    /// Return an iterator over all entries in reverse sorted order
    pub fn iter_rev(&mut self) -> Result<impl Iterator<Item = Result<(Bytes, Bytes)>>> {
        // Since SSTableIterator implements DoubleEndedIterator, we can just reverse it
        Ok(self.iter()?.rev())
    }

    /// Scan a range of keys from this `SSTable` using lazy iteration
    ///
    /// Returns an iterator that yields (key, `Option<value>`) where None indicates a tombstone.
    /// Blocks are loaded on-demand as the iterator is consumed, avoiding upfront materialization.
    pub fn scan_range(&self, start_key: &[u8], end_key: Option<&[u8]>) -> SSTableRangeIterator {
        self.scan_range_with_options(start_key, end_key, true)
    }

    pub fn scan_range_keys_only(
        &self,
        start_key: &[u8],
        end_key: Option<&[u8]>,
    ) -> SSTableRangeIterator {
        self.scan_range_with_options(start_key, end_key, false)
    }

    fn scan_range_with_options(
        &self,
        start_key: &[u8],
        end_key: Option<&[u8]>,
        read_values: bool,
    ) -> SSTableRangeIterator {
        SSTableRangeIterator::new(
            Arc::clone(&self.file),
            Arc::clone(&self.block_cache),
            self.vlog.as_ref().map(Arc::clone),
            self.top_level_index.clone(),
            start_key,
            end_key,
            Arc::clone(&self.cache_hits),
            Arc::clone(&self.cache_misses),
            read_values,
        )
    }
}

#[cfg(test)]
mod tests;
