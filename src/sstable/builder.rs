use super::*;

use crate::types::{InternalKey, ValueType};
use crate::vlog::VLog;
use block::{BlockBuilder, DEFAULT_BLOCK_SIZE};
use bytes::{Bytes, BytesMut};
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Helper: Handle vLog separation logic (shared by `SSTableBuilder` implementations)
///
/// Returns (`encoded_value`, flag):
/// - If value is large (>threshold), appends to vLog and returns (`pointer_bytes`, `FLAG_POINTER`)
/// - Otherwise, returns (value, `FLAG_INLINE`)
pub(super) fn handle_vlog_value(
    key: &Bytes,
    value: Bytes,
    vlog: &mut VLog,
    threshold: Option<usize>,
) -> Result<(Bytes, u8)> {
    if value.len() > threshold.unwrap_or(usize::MAX) {
        // Large value: store in vLog and return pointer
        let pointer = vlog
            .append(key, &value)
            .map_err(|e| SSTableError::VLog(format!("Failed to append to vLog: {e}")))?;
        Ok((pointer.to_bytes(), FLAG_POINTER))
    } else {
        // Small value: store inline
        Ok((value, FLAG_INLINE))
    }
}

/// `SSTable` builder with block-based format
///
/// Generic over any writer that implements Read + Write + Seek.
/// - Use `SSTableBuilder<File>` for direct-to-disk (low memory)
/// - Use `SSTableBuilder<Cursor<Vec<u8>>>` for buffered (cloud storage)
pub struct SSTableBuilder<W> {
    writer: W,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    top_level_index: Vec<TopLevelIndexEntry>,
    bloom: BloomFilter,
    prefix_bloom: BloomFilter,
    prefix_len: usize,
    vlog_threshold: Option<usize>,
    num_entries: u64,
    current_offset: u64,
    index_blocks_start: u64,
    min_key: Option<Bytes>,
    max_key: Option<Bytes>,
    /// Maximum sequence number in this `SSTable`
    /// Used to coordinate flush and compaction to prevent live key deletion
    max_sequence: u64,
    /// Compression algorithm for blocks
    compression_type: CompressionType,
}

impl SSTableBuilder<File> {
    /// Create a new `SSTable` builder writing directly to a file
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let header = Self::create_header(DEFAULT_PREFIX_LEN);
        file.write_all(&header)?;
        let header_size = header.len() as u64;

        Ok(Self {
            writer: file,
            data_block: BlockBuilder::with_capacity(DEFAULT_BLOCK_SIZE),
            index_block: BlockBuilder::with_capacity(DEFAULT_BLOCK_SIZE),
            top_level_index: Vec::new(),
            bloom: BloomFilter::new(10000, 0.01),
            prefix_bloom: BloomFilter::new(10000, 0.01),
            prefix_len: DEFAULT_PREFIX_LEN,
            vlog_threshold: None,
            num_entries: 0,
            current_offset: header_size,
            index_blocks_start: 0,
            min_key: None,
            max_key: None,
            max_sequence: 0,
            compression_type: CompressionType::Lz4,
        })
    }

    /// Finish building and ensure data is synced to disk
    pub fn finish(self) -> Result<()> {
        let file = self.finish_internal()?;
        file.sync_all()?;
        Ok(())
    }
}

impl SSTableBuilder<Cursor<Vec<u8>>> {
    /// Create a new buffered `SSTable` builder (in-memory)
    #[must_use]
    pub fn new_buffered() -> Self {
        let header = Self::create_header(DEFAULT_PREFIX_LEN);
        let header_size = header.len() as u64;

        // Pre-allocate 64KB
        let mut buffer = Vec::with_capacity(64 * 1024);
        buffer.extend_from_slice(&header);
        let mut writer = Cursor::new(buffer);
        writer.set_position(header_size);

        Self {
            writer,
            data_block: BlockBuilder::with_capacity(DEFAULT_BLOCK_SIZE),
            index_block: BlockBuilder::with_capacity(DEFAULT_BLOCK_SIZE),
            top_level_index: Vec::new(),
            bloom: BloomFilter::new(10000, 0.01),
            prefix_bloom: BloomFilter::new(10000, 0.01),
            prefix_len: DEFAULT_PREFIX_LEN,
            vlog_threshold: None,
            num_entries: 0,
            current_offset: header_size,
            index_blocks_start: 0,
            min_key: None,
            max_key: None,
            max_sequence: 0,
            compression_type: CompressionType::Lz4,
        }
    }

    /// Finish building and return the complete `SSTable` as bytes
    pub fn finish_to_bytes(self) -> Result<Bytes> {
        let writer = self.finish_internal()?;
        Ok(Bytes::from(writer.into_inner()))
    }

    /// Finish building and write to a file (helper for tests)
    pub fn finish_to_file(self, path: impl AsRef<Path>) -> Result<()> {
        let bytes = self.finish_to_bytes()?;
        std::fs::write(path, &bytes)?;
        Ok(())
    }
}

impl<W: Read + Write + Seek> SSTableBuilder<W> {
    /// Create header (static helper)
    fn create_header(prefix_len: usize) -> Vec<u8> {
        let mut header = Vec::with_capacity(32);
        header.extend_from_slice(&MAGIC.to_le_bytes()); // 4 bytes: magic
        header.extend_from_slice(&VERSION.to_le_bytes()); // 4 bytes: version
                                                          // Store prefix_len in reserved field (4 bytes prefix_len + 4 bytes reserved)
        header.extend_from_slice(&(prefix_len as u32).to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        header.extend_from_slice(&0u64.to_le_bytes()); // 8 bytes: num_entries (filled in finish())
        header.extend_from_slice(&0u64.to_le_bytes()); // 8 bytes: max_sequence (filled in finish())
        header
    }

    pub const fn with_vlog_threshold(mut self, threshold: usize) -> Self {
        self.vlog_threshold = Some(threshold);
        self
    }

    pub fn with_prefix_len(mut self, len: usize) -> Self {
        self.prefix_len = len;
        // Re-create header with correct prefix_len
        let header = Self::create_header(len);
        let _ = self.writer.seek(SeekFrom::Start(0));
        let _ = self.writer.write_all(&header);
        let _ = self.writer.seek(SeekFrom::Start(self.current_offset));
        self
    }

    /// Set the maximum sequence number for this `SSTable`
    pub const fn with_max_sequence(mut self, seq: u64) -> Self {
        self.max_sequence = seq;
        self
    }

    /// Set the compression algorithm for `SSTable` blocks
    ///
    /// Controls the compression used for data and index blocks.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use seerdb::sstable::{SSTableBuilder, CompressionType};
    ///
    /// let builder = SSTableBuilder::new_buffered()
    ///     .with_compression(CompressionType::Zstd);
    /// ```
    pub fn with_compression(mut self, compression_type: CompressionType) -> Self {
        self.compression_type = compression_type;
        // Apply to existing block builders
        self.data_block.set_compression_type(compression_type);
        self.index_block.set_compression_type(compression_type);
        self
    }

    /// Check if the builder is empty (no entries added yet)
    pub const fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// Get the number of entries added so far
    pub const fn num_entries(&self) -> u64 {
        self.num_entries
    }

    #[inline]
    pub fn add(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        let encoded_value = self.encode_entry(&key, FLAG_INLINE, &value);
        self.add_raw(key, encoded_value)
    }

    #[inline]
    pub fn add_raw(&mut self, key: Bytes, encoded_value: Bytes) -> Result<()> {
        // Track min/max keys for range filtering
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key.clone());

        // Insert key into bloom filter as-is (for non-MVCC add() calls)
        self.bloom.insert(&key);
        if self.prefix_len > 0 && key.len() >= self.prefix_len {
            self.prefix_bloom.insert(&key[..self.prefix_len]);
        }

        if !self.data_block.add(&key, &encoded_value) {
            self.flush_data_block()?;
            if !self.data_block.add(&key, &encoded_value) {
                // Entry too large for default block - create custom-sized block
                let entry_size = key.len() + encoded_value.len() + 8;
                let custom_size = (entry_size * 2).max(DEFAULT_BLOCK_SIZE * 2);
                self.data_block = BlockBuilder::with_capacity(custom_size);

                if !self.data_block.add(&key, &encoded_value) {
                    return Err(SSTableError::InvalidFormat);
                }
            }
        }

        self.num_entries += 1;
        Ok(())
    }

    /// Add a raw entry from MVCC-encoded `SSTable` (for compaction)
    ///
    /// Similar to `add_raw()` but extracts user key from encoded `InternalKey` for bloom filter.
    /// This ensures `get_mvcc()` can find keys in compacted `SSTables`.
    #[inline]
    pub fn add_raw_mvcc(&mut self, key: Bytes, encoded_value: Bytes) -> Result<()> {
        // Track min/max keys for range filtering
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key.clone());

        // Extract user key from encoded InternalKey for bloom filter
        // This allows get_mvcc(user_key) to find the entry
        let user_key = InternalKey::extract_user_key(&key);
        self.bloom.insert(&user_key);
        if self.prefix_len > 0 && user_key.len() >= self.prefix_len {
            self.prefix_bloom.insert(&user_key[..self.prefix_len]);
        }

        if !self.data_block.add(&key, &encoded_value) {
            self.flush_data_block()?;
            if !self.data_block.add(&key, &encoded_value) {
                // Entry too large for default block - create custom-sized block
                let entry_size = key.len() + encoded_value.len() + 8;
                let custom_size = (entry_size * 2).max(DEFAULT_BLOCK_SIZE * 2);
                self.data_block = BlockBuilder::with_capacity(custom_size);

                if !self.data_block.add(&key, &encoded_value) {
                    return Err(SSTableError::InvalidFormat);
                }
            }
        }

        self.num_entries += 1;
        Ok(())
    }

    pub fn add_with_vlog(&mut self, key: Bytes, value: Bytes, vlog: &mut VLog) -> Result<()> {
        self.bloom.insert(&key);
        if self.prefix_len > 0 && key.len() >= self.prefix_len {
            self.prefix_bloom.insert(&key[..self.prefix_len]);
        }

        // Use shared helper for vLog handling
        let (data, flag) = handle_vlog_value(&key, value, vlog, self.vlog_threshold)?;

        let entry = self.encode_entry(&key, flag, &data);

        if !self.data_block.add(&key, &entry) {
            self.flush_data_block()?;
            if !self.data_block.add(&key, &entry) {
                let entry_size = key.len() + entry.len() + 8;
                let custom_size = (entry_size * 2).max(DEFAULT_BLOCK_SIZE * 2);
                self.data_block = BlockBuilder::with_capacity(custom_size);

                if !self.data_block.add(&key, &entry) {
                    return Err(SSTableError::InvalidFormat);
                }
            }
        }

        self.num_entries += 1;
        Ok(())
    }

    pub fn add_tombstone(&mut self, key: Bytes) -> Result<()> {
        // Extract user key from encoded InternalKey for bloom filter
        // This allows get_mvcc(user_key) to find the tombstone
        let user_key = InternalKey::extract_user_key(&key);
        self.bloom.insert(&user_key);
        if self.prefix_len > 0 && user_key.len() >= self.prefix_len {
            self.prefix_bloom.insert(&user_key[..self.prefix_len]);
        }
        let entry = self.encode_entry(&key, FLAG_TOMBSTONE, &[]);

        if !self.data_block.add(&key, &entry) {
            self.flush_data_block()?;
            if !self.data_block.add(&key, &entry) {
                let entry_size = key.len() + entry.len() + 8;
                let custom_size = (entry_size * 2).max(DEFAULT_BLOCK_SIZE * 2);
                self.data_block = BlockBuilder::with_capacity(custom_size);

                if !self.data_block.add(&key, &entry) {
                    return Err(SSTableError::InvalidFormat);
                }
            }
        }

        self.num_entries += 1;
        Ok(())
    }

    pub fn add_merge(&mut self, key: Bytes, operand: Bytes) -> Result<()> {
        // Extract user key from encoded InternalKey for bloom filter
        // This allows get_mvcc(user_key) to find the merge operand
        let user_key = InternalKey::extract_user_key(&key);
        self.bloom.insert(&user_key);
        if self.prefix_len > 0 && user_key.len() >= self.prefix_len {
            self.prefix_bloom.insert(&user_key[..self.prefix_len]);
        }
        let entry = self.encode_entry(&key, FLAG_MERGE, &operand);

        if !self.data_block.add(&key, &entry) {
            self.flush_data_block()?;
            if !self.data_block.add(&key, &entry) {
                let entry_size = key.len() + entry.len() + 8;
                let custom_size = (entry_size * 2).max(DEFAULT_BLOCK_SIZE * 2);
                self.data_block = BlockBuilder::with_capacity(custom_size);

                if !self.data_block.add(&key, &entry) {
                    return Err(SSTableError::InvalidFormat);
                }
            }
        }

        self.num_entries += 1;
        Ok(())
    }

    // ========================================================================
    // MVCC-aware methods: Store encoded InternalKeys, bloom filter uses user keys
    // ========================================================================

    /// Add an entry with `InternalKey` (MVCC-aware)
    ///
    /// - Encodes `InternalKey` for storage (sorted by `user_key` ASC, seq DESC)
    /// - Uses `user_key` for bloom filter (so `get_mvcc` can check bloom)
    /// - Tracks max sequence number
    pub fn add_internal(&mut self, ikey: &InternalKey, value: Bytes) -> Result<()> {
        // Update max sequence for this SSTable
        if ikey.seq > self.max_sequence {
            self.max_sequence = ikey.seq;
        }

        // Bloom filter uses user_key (not encoded InternalKey)
        // This allows get_mvcc(user_key) to check bloom correctly
        self.bloom.insert(&ikey.user_key);
        if self.prefix_len > 0 && ikey.user_key.len() >= self.prefix_len {
            self.prefix_bloom.insert(&ikey.user_key[..self.prefix_len]);
        }

        // Encode the InternalKey for storage
        let encoded_key = ikey.encode();

        // Track min/max using encoded keys (for range filtering)
        if self.min_key.is_none() {
            self.min_key = Some(encoded_key.clone());
        }
        self.max_key = Some(encoded_key.clone());

        // Encode value with appropriate flag based on ValueType
        let flag = match ikey.kind {
            ValueType::Value | ValueType::Log => FLAG_INLINE, // Log treated as inline value
            ValueType::Deletion => FLAG_TOMBSTONE,
            ValueType::Merge => FLAG_MERGE,
        };
        let encoded_value = self.encode_entry(&encoded_key, flag, &value);

        // Add to data block
        if !self.data_block.add(&encoded_key, &encoded_value) {
            self.flush_data_block()?;
            if !self.data_block.add(&encoded_key, &encoded_value) {
                let entry_size = encoded_key.len() + encoded_value.len() + 8;
                let custom_size = (entry_size * 2).max(DEFAULT_BLOCK_SIZE * 2);
                self.data_block = BlockBuilder::with_capacity(custom_size);

                if !self.data_block.add(&encoded_key, &encoded_value) {
                    return Err(SSTableError::InvalidFormat);
                }
            }
        }

        self.num_entries += 1;
        Ok(())
    }

    /// Add an entry with `InternalKey` and vLog support (MVCC-aware)
    pub fn add_internal_with_vlog(
        &mut self,
        ikey: &InternalKey,
        value: Bytes,
        vlog: &mut VLog,
    ) -> Result<()> {
        // Update max sequence
        if ikey.seq > self.max_sequence {
            self.max_sequence = ikey.seq;
        }

        // Bloom filter uses user_key
        self.bloom.insert(&ikey.user_key);
        if self.prefix_len > 0 && ikey.user_key.len() >= self.prefix_len {
            self.prefix_bloom.insert(&ikey.user_key[..self.prefix_len]);
        }

        // Encode the InternalKey
        let encoded_key = ikey.encode();

        // Track min/max
        if self.min_key.is_none() {
            self.min_key = Some(encoded_key.clone());
        }
        self.max_key = Some(encoded_key.clone());

        // Handle vLog for large values (only for Value type)
        let (data, flag) = if matches!(ikey.kind, ValueType::Value) {
            handle_vlog_value(&ikey.user_key, value, vlog, self.vlog_threshold)?
        } else {
            // Tombstone/Merge don't use vLog
            let flag = match ikey.kind {
                ValueType::Deletion => FLAG_TOMBSTONE,
                ValueType::Merge => FLAG_MERGE,
                _ => FLAG_INLINE,
            };
            (value, flag)
        };

        let encoded_value = self.encode_entry(&encoded_key, flag, &data);

        // Add to data block
        if !self.data_block.add(&encoded_key, &encoded_value) {
            self.flush_data_block()?;
            if !self.data_block.add(&encoded_key, &encoded_value) {
                let entry_size = encoded_key.len() + encoded_value.len() + 8;
                let custom_size = (entry_size * 2).max(DEFAULT_BLOCK_SIZE * 2);
                self.data_block = BlockBuilder::with_capacity(custom_size);

                if !self.data_block.add(&encoded_key, &encoded_value) {
                    return Err(SSTableError::InvalidFormat);
                }
            }
        }

        self.num_entries += 1;
        Ok(())
    }

    #[inline]
    fn encode_entry(&self, _key: &[u8], flag: u8, data: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + data.len());
        buf.extend_from_slice(&[flag]);
        buf.extend_from_slice(data);
        buf.freeze()
    }

    /// Create a new `BlockBuilder` with the configured compression type
    fn new_block_builder(&self) -> BlockBuilder {
        let mut builder = BlockBuilder::with_capacity(DEFAULT_BLOCK_SIZE);
        builder.set_compression_type(self.compression_type);
        builder
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }

        let last_key = Bytes::copy_from_slice(self.data_block.last_key());
        let block_offset = self.current_offset;

        let new_block = self.new_block_builder();
        let old_block = std::mem::replace(&mut self.data_block, new_block);
        let block_data = old_block.finish();
        let block_size = block_data.len() as u32;
        self.writer.write_all(&block_data)?;
        self.current_offset += block_data.len() as u64;

        let mut index_entry = BytesMut::with_capacity(4 + last_key.len() + 8 + 4);
        index_entry.extend_from_slice(&(last_key.len() as u32).to_le_bytes());
        index_entry.extend_from_slice(&last_key);
        index_entry.extend_from_slice(&block_offset.to_le_bytes());
        index_entry.extend_from_slice(&block_size.to_le_bytes());
        let index_entry_bytes = index_entry.freeze();

        if !self.index_block.add(&last_key, &index_entry_bytes) {
            self.flush_index_block()?;

            let mut index_entry2 = BytesMut::with_capacity(4 + last_key.len() + 8 + 4);
            index_entry2.extend_from_slice(&(last_key.len() as u32).to_le_bytes());
            index_entry2.extend_from_slice(&last_key);
            index_entry2.extend_from_slice(&block_offset.to_le_bytes());
            index_entry2.extend_from_slice(&block_size.to_le_bytes());

            if !self.index_block.add(&last_key, &index_entry2.freeze()) {
                return Err(SSTableError::InvalidFormat);
            }
        }

        Ok(())
    }

    fn flush_index_block(&mut self) -> Result<()> {
        if self.index_block.is_empty() {
            return Ok(());
        }

        if self.index_blocks_start == 0 {
            self.index_blocks_start = self.current_offset;
        }

        let last_key = Bytes::copy_from_slice(self.index_block.last_key());
        let block_offset = self.current_offset;

        let new_block = self.new_block_builder();
        let old_block = std::mem::replace(&mut self.index_block, new_block);
        let block_data = old_block.finish();
        let block_size = block_data.len() as u32;
        self.writer.write_all(&block_data)?;
        self.current_offset += block_data.len() as u64;

        self.top_level_index.push(TopLevelIndexEntry {
            last_key,
            offset: block_offset,
            size: block_size,
        });

        Ok(())
    }

    pub fn finish_internal(mut self) -> Result<W> {
        self.flush_data_block()?;
        self.flush_index_block()?;

        let top_level_offset = self.current_offset;
        self.write_top_level_index()?;

        let bloom_offset = self.current_offset;
        let bloom_bytes = self.bloom.to_bytes();
        self.writer
            .write_all(&(bloom_bytes.len() as u64).to_le_bytes())?;
        self.writer.write_all(&bloom_bytes)?;
        self.current_offset += 8 + bloom_bytes.len() as u64;

        let prefix_bloom_offset = self.current_offset;
        let prefix_bloom_bytes = self.prefix_bloom.to_bytes();
        self.writer
            .write_all(&(prefix_bloom_bytes.len() as u64).to_le_bytes())?;
        self.writer.write_all(&prefix_bloom_bytes)?;
        self.current_offset += 8 + prefix_bloom_bytes.len() as u64;

        // Write min_key/max_key metadata
        let metadata_offset = self.current_offset;
        self.write_metadata()?;

        // Write num_entries and max_sequence to header BEFORE computing footer checksum
        // This ensures the checksum includes these header fields
        let footer_offset = self.current_offset;
        self.writer.seek(SeekFrom::Start(16))?; // Skip magic (4) + version (4) + reserved (8)
        self.writer.write_all(&self.num_entries.to_le_bytes())?; // Offset 16-23
        self.writer.write_all(&self.max_sequence.to_le_bytes())?; // Offset 24-31
        self.writer.seek(SeekFrom::Start(footer_offset))?; // Return to footer position

        self.write_footer(
            top_level_offset,
            bloom_offset,
            prefix_bloom_offset,
            metadata_offset,
        )?;

        Ok(self.writer)
    }

    fn write_top_level_index(&mut self) -> Result<()> {
        // OPTIMIZATION: Batch all index entries into single buffer to reduce syscalls
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(self.top_level_index.len() as u32).to_le_bytes());

        for entry in &self.top_level_index {
            buffer.extend_from_slice(&(entry.last_key.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&entry.last_key);
            buffer.extend_from_slice(&entry.offset.to_le_bytes());
            buffer.extend_from_slice(&entry.size.to_le_bytes());
        }

        // Single syscall instead of N syscalls
        self.writer.write_all(&buffer)?;
        self.current_offset += buffer.len() as u64;

        Ok(())
    }

    fn write_metadata(&mut self) -> Result<()> {
        // OPTIMIZATION: Batch metadata writes into single buffer
        let mut buffer = Vec::new();

        // Write min_key
        let min_key = self.min_key.as_ref().map_or(&[][..], AsRef::as_ref);
        buffer.extend_from_slice(&(min_key.len() as u32).to_le_bytes());
        buffer.extend_from_slice(min_key);

        // Write max_key
        let max_key = self.max_key.as_ref().map_or(&[][..], AsRef::as_ref);
        buffer.extend_from_slice(&(max_key.len() as u32).to_le_bytes());
        buffer.extend_from_slice(max_key);

        // Single syscall instead of 4 syscalls
        self.writer.write_all(&buffer)?;
        self.current_offset += buffer.len() as u64;

        Ok(())
    }

    fn write_footer(
        &mut self,
        top_level_offset: u64,
        bloom_offset: u64,
        prefix_bloom_offset: u64,
        metadata_offset: u64,
    ) -> Result<()> {
        let footer_start = self.current_offset;

        self.writer.seek(SeekFrom::Start(0))?;
        let mut checksum = 0u32;
        let mut buf = vec![0u8; 4096];
        let mut remaining = footer_start;

        while remaining > 0 {
            let to_read = remaining.min(4096) as usize;
            self.writer.read_exact(&mut buf[..to_read])?;
            checksum = crc32c::crc32c_append(checksum, &buf[..to_read]);
            remaining -= to_read as u64;
        }
        self.writer.seek(SeekFrom::Start(footer_start))?;

        // OPTIMIZATION: Batch footer writes into single buffer (8 syscalls → 1 syscall)
        let mut footer_buffer = Vec::with_capacity(56);
        footer_buffer.extend_from_slice(&self.index_blocks_start.to_le_bytes());
        footer_buffer.extend_from_slice(&top_level_offset.to_le_bytes());
        footer_buffer.extend_from_slice(&bloom_offset.to_le_bytes());
        footer_buffer.extend_from_slice(&prefix_bloom_offset.to_le_bytes());
        footer_buffer.extend_from_slice(&metadata_offset.to_le_bytes());
        footer_buffer.extend_from_slice(&checksum.to_le_bytes());
        footer_buffer.extend_from_slice(&MAGIC.to_le_bytes());
        footer_buffer.extend_from_slice(&VERSION.to_le_bytes());
        footer_buffer.extend_from_slice(&0u32.to_le_bytes()); // Padding

        self.writer.write_all(&footer_buffer)?;

        Ok(())
    }
}
