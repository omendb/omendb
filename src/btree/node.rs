//! B-tree node format.
//!
//! Each node is a fixed-size page (4KB default) with a slotted page layout:
//!
//! ```text
//! [Header: 32 bytes] [Slot 0..N] [...free...] [Entry N] ... [Entry 0]
//! ```
//!
//! - **Header**: page metadata
//! - **Slot Array**: fixed-size entries pointing to variable-length data (grows forward)
//! - **Entries**: self-contained live-mutation keys packed from the end (grows backward)
//!
//! The decoder remains compatible with the older prefix-compressed v2 entry
//! form. New mutations intentionally use self-contained keys until a bounded
//! restart-point compression scheme is implemented and benchmarked.

/// Page size in bytes (4KB).
pub const PAGE_SIZE: usize = 4096;

/// Maximum key length that fits in both a leaf entry and a promoted internal
/// separator, including one slot and the fixed page header.
pub const MAX_KEY_SIZE: usize = PAGE_SIZE - 40 - 4 - 4 - 8;

/// Header size in bytes.
const HEADER_SIZE: usize = 40;

/// Magic number identifying seerdb pages.
const MAGIC: u32 = 0x5345_4552; // "SEER"

/// Current page format version.
const PAGE_VERSION: u32 = 2;

/// Size of each slot entry (offset: u16, key_len: u16).
const SLOT_SIZE: usize = 4;

/// Blob pointer size: file_id(4) + offset(8) + length(4) = 16 bytes.
pub const BLOB_POINTER_SIZE: usize = 16;

/// Page type discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PageType {
    /// Internal node: keys + child page IDs.
    Internal = 1,
    /// Leaf node: keys + values.
    Leaf = 2,
}

/// Value storage type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    /// Value stored inline in the page.
    Inline = 0x00,
    /// Value stored in a blob file; page contains a 16-byte pointer.
    BlobPointer = 0x01,
    /// Key has been deleted (tombstone).
    Tombstone = 0x02,
}

/// Marker for a deleted key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tombstone;

/// Blob reference for values stored externally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobPointer {
    pub file_id: u32,
    pub offset: u64,
    pub length: u32,
}

impl BlobPointer {
    /// Size of serialized blob pointer (file_id:4 + offset:8 + length:4 = 16 bytes).
    pub const SERIALIZED_SIZE: usize = 16;

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[0..4].copy_from_slice(&self.file_id.to_le_bytes());
        buf[4..12].copy_from_slice(&self.offset.to_le_bytes());
        buf[12..16].copy_from_slice(&self.length.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(buf: &[u8; Self::SERIALIZED_SIZE]) -> Self {
        let file_id = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let offset = u64::from_le_bytes([
            buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
        ]);
        let length = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        Self {
            file_id,
            offset,
            length,
        }
    }
}

/// Fixed-size page header (40 bytes).
///
/// Layout: magic(4) | version(4) | page_type(4) | count(4) | free_space(4) | checksum(8) | parent_id(4) | leftmost_child(8)
#[derive(Debug, Clone)]
pub struct NodeHeader {
    /// Magic number for page identification.
    pub magic: u32,
    /// Page format version.
    pub version: u32,
    /// Type of node (internal or leaf).
    pub page_type: PageType,
    /// Number of key-value pairs in this node.
    pub count: u32,
    /// Free space in bytes (between slot array and entries).
    pub free_space: u32,
    /// CRC32C checksum of the page contents (excluding this field).
    pub checksum: u64,
    /// Parent page ID (0 if root).
    pub parent_id: u32,
    /// Leftmost child page ID for internal nodes.
    pub leftmost_child: u64,
}

impl NodeHeader {
    /// Size of the serialized header.
    pub const SIZE: usize = HEADER_SIZE;

    /// Serialize header to bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&(self.page_type as u32).to_le_bytes());
        buf[12..16].copy_from_slice(&self.count.to_le_bytes());
        buf[16..20].copy_from_slice(&self.free_space.to_le_bytes());
        buf[20..28].copy_from_slice(&self.checksum.to_le_bytes());
        buf[28..32].copy_from_slice(&self.parent_id.to_le_bytes());
        buf[32..40].copy_from_slice(&self.leftmost_child.to_le_bytes());
        buf
    }

    /// Deserialize header from bytes.
    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Self {
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let page_type_raw = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let page_type = match page_type_raw {
            1 => PageType::Internal,
            2 => PageType::Leaf,
            _ => PageType::Leaf,
        };
        let count = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let free_space = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
        let checksum = u64::from_le_bytes([
            buf[20], buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27],
        ]);
        let parent_id = u32::from_le_bytes([buf[28], buf[29], buf[30], buf[31]]);
        let leftmost_child = u64::from_le_bytes([
            buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
        ]);
        Self {
            magic,
            version,
            page_type,
            count,
            free_space,
            checksum,
            parent_id,
            leftmost_child,
        }
    }

    /// Deserialize a header while rejecting unknown page types.
    fn try_from_bytes(buf: &[u8; HEADER_SIZE]) -> Option<Self> {
        let page_type_raw = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        if !matches!(page_type_raw, 1 | 2) {
            return None;
        }

        let header = Self::from_bytes(buf);
        header.is_valid().then_some(header)
    }

    /// Create a fresh header for the given page type.
    fn new(page_type: PageType) -> Self {
        Self {
            magic: MAGIC,
            version: PAGE_VERSION,
            page_type,
            count: 0,
            free_space: (PAGE_SIZE - HEADER_SIZE) as u32,
            checksum: 0,
            parent_id: 0,
            leftmost_child: 0,
        }
    }

    /// Validate the header magic and version.
    pub fn is_valid(&self) -> bool {
        self.magic == MAGIC && self.version == PAGE_VERSION
    }
}

/// A B-tree node backed by a fixed-size page buffer.
///
/// The node manages its own memory layout using a slotted page design:
/// - Slots (fixed 4 bytes each) grow forward from the header
/// - Entries (variable-length) grow backward from the end of the page
/// - Free space sits in the middle
#[derive(Clone)]
pub struct Node {
    /// Raw page buffer (always PAGE_SIZE bytes).
    data: Box<[u8; PAGE_SIZE]>,
}

impl Node {
    /// Create a new empty leaf node.
    pub fn new_leaf() -> Self {
        Self::new(PageType::Leaf)
    }

    /// Create a new empty internal node.
    pub fn new_internal() -> Self {
        Self::new(PageType::Internal)
    }

    fn new(page_type: PageType) -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let header = NodeHeader::new(page_type);
        data[..HEADER_SIZE].copy_from_slice(&header.to_bytes());
        Self { data }
    }

    /// Wrap an existing page buffer (e.g., read from disk).
    ///
    /// Returns `None` if the header or slotted-page layout is invalid.
    pub fn from_bytes(data: Box<[u8; PAGE_SIZE]>) -> Option<Self> {
        let node = Self { data };
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&node.data[..HEADER_SIZE]);
        NodeHeader::try_from_bytes(&header_bytes)?;
        node.validate_layout().then_some(node)
    }

    /// Access the raw page bytes.
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Consume the node and return the raw page buffer.
    pub fn into_bytes(self) -> Box<[u8; PAGE_SIZE]> {
        self.data
    }

    /// Set the leftmost child ID (for internal nodes).
    pub fn set_leftmost_child(&mut self, child_id: u64) {
        let mut header = self.header();
        header.leftmost_child = child_id;
        self.set_header(&header);
    }

    /// Get the leftmost child ID (for internal nodes).
    pub fn leftmost_child(&self) -> u64 {
        self.header().leftmost_child
    }

    /// Read the page header.
    pub fn header(&self) -> NodeHeader {
        let mut buf = [0u8; HEADER_SIZE];
        buf.copy_from_slice(&self.data[..HEADER_SIZE]);
        NodeHeader::from_bytes(&buf)
    }

    /// Validate all bounds and typed payloads in the slotted-page layout.
    fn validate_layout(&self) -> bool {
        let header = self.header();
        let count = header.count as usize;
        let slot_end = HEADER_SIZE.saturating_add(count.saturating_mul(SLOT_SIZE));
        if count > (PAGE_SIZE - HEADER_SIZE) / SLOT_SIZE || slot_end > PAGE_SIZE {
            return false;
        }

        let mut offsets = Vec::with_capacity(count);
        for index in 0..count {
            let offset = self.slot_offset(index);
            if offset < slot_end || offset >= PAGE_SIZE {
                return false;
            }
            offsets.push(offset);
        }

        let mut sorted_offsets = offsets.clone();
        sorted_offsets.sort_unstable();
        if sorted_offsets
            .windows(2)
            .any(|window| window[0] == window[1])
        {
            return false;
        }

        let min_entry = offsets.iter().copied().min().unwrap_or(PAGE_SIZE);
        if header.free_space as usize != min_entry.saturating_sub(slot_end) {
            return false;
        }

        let mut previous_key = Vec::new();
        for (index, &offset) in offsets.iter().enumerate() {
            let sorted_index = match sorted_offsets.binary_search(&offset) {
                Ok(index) => index,
                Err(_) => return false,
            };
            let entry_end = sorted_offsets
                .get(sorted_index + 1)
                .copied()
                .unwrap_or(PAGE_SIZE);
            if entry_end <= offset || entry_end > PAGE_SIZE || offset + 4 > entry_end {
                return false;
            }

            let prefix_len = self.entry_prefix_len(index) as usize;
            let suffix_len = self.entry_suffix_len(index) as usize;
            let suffix_end = match offset.checked_add(4 + suffix_len) {
                Some(end) if end <= entry_end => end,
                _ => return false,
            };
            let key_len = match prefix_len.checked_add(suffix_len) {
                Some(length) => length,
                None => return false,
            };
            if self.slot_key_len(index) as usize != key_len {
                return false;
            }

            if prefix_len > previous_key.len() {
                return false;
            }
            let key_start = offset + 4;
            let key_suffix = &self.data[key_start..suffix_end];
            let mut key = previous_key[..prefix_len].to_vec();
            key.extend_from_slice(key_suffix);
            if previous_key.as_slice() > key.as_slice() {
                return false;
            }
            previous_key = key;

            if self.is_internal() {
                if suffix_end.checked_add(8).is_none_or(|end| end > entry_end) {
                    return false;
                }
            } else {
                if suffix_end >= entry_end {
                    return false;
                }
                let value_type = self.data[suffix_end];
                let value_start = suffix_end + 1;
                match value_type {
                    0x00 | 0x02 => {}
                    0x01 if value_start
                        .checked_add(BLOB_POINTER_SIZE)
                        .is_some_and(|end| end <= entry_end) => {}
                    _ => return false,
                }
            }
        }

        true
    }

    /// Write the page header.
    fn set_header(&mut self, header: &NodeHeader) {
        self.data[..HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    /// Whether this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        self.header().page_type == PageType::Leaf
    }

    /// Whether this is an internal node.
    pub fn is_internal(&self) -> bool {
        self.header().page_type == PageType::Internal
    }

    /// Number of key-value pairs in this node.
    pub fn count(&self) -> usize {
        self.header().count as usize
    }

    /// Set the cached parent page ID.
    ///
    /// Forward child edges are authoritative for the durable tree. The hint
    /// may be stale for an unloaded page after an out-of-place internal split.
    pub fn set_parent_id(&mut self, parent_id: u32) {
        let mut header = self.header();
        header.parent_id = parent_id;
        self.set_header(&header);
    }

    /// Cached parent page ID (0 if root in a fully resident tree).
    pub fn parent_id(&self) -> u32 {
        self.header().parent_id
    }

    // -- Slot array helpers --

    /// Offset where the slot array starts (right after header).
    const fn slot_array_start() -> usize {
        HEADER_SIZE
    }

    /// Read the entry offset for slot `index`.
    fn slot_offset(&self, index: usize) -> usize {
        let start = Self::slot_array_start() + index * SLOT_SIZE;
        u16::from_le_bytes([self.data[start], self.data[start + 1]]) as usize
    }

    /// Write the entry offset for slot `index`.
    fn set_slot_offset(&mut self, index: usize, offset: u16) {
        let start = Self::slot_array_start() + index * SLOT_SIZE;
        self.data[start..start + 2].copy_from_slice(&offset.to_le_bytes());
    }

    /// Read the key length stored in the slot for `index`.
    fn slot_key_len(&self, index: usize) -> u16 {
        let start = Self::slot_array_start() + index * SLOT_SIZE + 2;
        u16::from_le_bytes([self.data[start], self.data[start + 1]])
    }

    /// Write the key length for slot `index`.
    fn set_slot_key_len(&mut self, index: usize, key_len: u16) {
        let start = Self::slot_array_start() + index * SLOT_SIZE + 2;
        self.data[start..start + 2].copy_from_slice(&key_len.to_le_bytes());
    }

    // -- Entry layout --
    //
    // Each entry stored at the slot's offset:
    //   [prefix_len: u16] [suffix_len: u16] [suffix: bytes] [value_type: u8] [value: bytes]
    //
    // For leaf nodes: value is inline bytes or a 16-byte blob pointer.
    // For internal nodes: value is the child page ID (u64).

    /// Read an entry's prefix length at the given slot.
    fn entry_prefix_len(&self, index: usize) -> u16 {
        let off = self.slot_offset(index);
        u16::from_le_bytes([self.data[off], self.data[off + 1]])
    }

    /// Read an entry's suffix length at the given slot.
    fn entry_suffix_len(&self, index: usize) -> u16 {
        let off = self.slot_offset(index) + 2;
        u16::from_le_bytes([self.data[off], self.data[off + 1]])
    }

    fn has_prefix_compression(&self) -> bool {
        (0..self.count()).any(|index| self.entry_prefix_len(index) != 0)
    }

    /// Reconstruct the full key at slot `index`.
    ///
    /// For index 0, the prefix is always empty (no previous key).
    /// For index > 0, we read the prefix from the previous key.
    pub fn key(&self, index: usize) -> Option<Vec<u8>> {
        if index >= self.count() {
            return None;
        }

        let prefix_len = self.entry_prefix_len(index) as usize;
        let suffix_len = self.entry_suffix_len(index) as usize;
        let entry_off = self.slot_offset(index);
        let suffix_start = entry_off + 4; // after prefix_len + suffix_len

        let suffix = &self.data[suffix_start..suffix_start + suffix_len];

        if index == 0 || prefix_len == 0 {
            return Some(suffix.to_vec());
        }

        // Reconstruct by prepending prefix from previous key.
        let prev_key = self.key(index - 1)?;
        if prefix_len > prev_key.len() {
            return None; // corruption
        }
        let mut full_key = prev_key[..prefix_len].to_vec();
        full_key.extend_from_slice(suffix);
        Some(full_key)
    }

    /// Read the value at slot `index` in a leaf node.
    ///
    /// Returns `None` if out of bounds or if this is an internal node.
    pub fn value(&self, index: usize) -> Option<ValueRef<'_>> {
        if index >= self.count() || !self.is_leaf() {
            return None;
        }

        let entry_off = self.slot_offset(index);
        let suffix_len = self.entry_suffix_len(index) as usize;
        // value_type is right after prefix_len(2) + suffix_len(2) + suffix
        let vt_off = entry_off + 4 + suffix_len;
        let value_type = self.data[vt_off];

        match value_type {
            0x00 => {
                // Inline value: find end by looking at all other entries
                let val_start = vt_off + 1;
                let this_offset = entry_off;
                // End is the minimum offset of all entries with higher offset, or PAGE_SIZE
                let val_end = (0..self.count())
                    .filter(|&i| i != index)
                    .map(|i| self.slot_offset(i))
                    .filter(|&off| off > this_offset)
                    .min()
                    .unwrap_or(PAGE_SIZE);
                Some(ValueRef::Inline(&self.data[val_start..val_end]))
            }
            0x01 => {
                // Blob pointer: 16 bytes
                let ptr_start = vt_off + 1;
                let ptr = &self.data[ptr_start..ptr_start + BLOB_POINTER_SIZE];
                let file_id = u32::from_le_bytes([ptr[0], ptr[1], ptr[2], ptr[3]]);
                let offset = u64::from_le_bytes([
                    ptr[4], ptr[5], ptr[6], ptr[7], ptr[8], ptr[9], ptr[10], ptr[11],
                ]);
                let length = u32::from_le_bytes([ptr[12], ptr[13], ptr[14], ptr[15]]);
                Some(ValueRef::Blob(BlobPointer {
                    file_id,
                    offset,
                    length,
                }))
            }
            0x02 => Some(ValueRef::Tombstone),
            _ => None, // corrupt
        }
    }

    /// Read the child page ID at slot `index` in an internal node.
    ///
    /// Internal nodes store child pointers as the "value" part.
    pub fn child_id(&self, index: usize) -> Option<u64> {
        if index >= self.count() || !self.is_internal() {
            return None;
        }

        let entry_off = self.slot_offset(index);
        let suffix_len = self.entry_suffix_len(index) as usize;
        let child_off = entry_off + 4 + suffix_len;
        if child_off + 8 > PAGE_SIZE {
            return None;
        }
        Some(u64::from_le_bytes([
            self.data[child_off],
            self.data[child_off + 1],
            self.data[child_off + 2],
            self.data[child_off + 3],
            self.data[child_off + 4],
            self.data[child_off + 5],
            self.data[child_off + 6],
            self.data[child_off + 7],
        ]))
    }

    /// Binary search for a key in this node.
    ///
    /// Returns `Ok(index)` of the FIRST occurrence if found, or `Err(index)`
    /// where `index` is the insertion point (the first slot with key >= the search key).
    pub fn search(&self, key: &[u8]) -> Result<usize, usize> {
        let count = self.count();
        if count == 0 {
            return Err(0);
        }

        // Find the first occurrence using binary search.
        let mut lo = 0;
        let mut hi = count;
        let mut result = None;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key(mid) {
                Some(mid_key) => match mid_key.as_slice().cmp(key) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Equal => {
                        result = Some(mid);
                        hi = mid; // continue searching left for first occurrence
                    }
                    std::cmp::Ordering::Greater => hi = mid,
                },
                None => return Err(mid), // corruption
            }
        }

        match result {
            Some(idx) => Ok(idx),
            None => Err(lo),
        }
    }

    /// Calculate the offset where a new entry should be placed.
    ///
    /// Entries are packed from the end of the page.
    /// We always place new entries at the lowest available offset.
    fn new_entry_offset(&self, entry_size: usize) -> Option<usize> {
        let count = self.count();
        if count == 0 {
            // First entry goes at the end of the page.
            return Some(PAGE_SIZE - entry_size);
        }

        // Find the minimum offset among existing entries.
        // New entry goes just before it.
        let min_offset = (0..count)
            .map(|i| self.slot_offset(i))
            .min()
            .unwrap_or(PAGE_SIZE);

        if min_offset < entry_size {
            None
        } else {
            Some(min_offset - entry_size)
        }
    }

    /// Insert a key-value pair into this leaf node.
    ///
    /// Returns `Err` if the entry doesn't fit.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }

        if let Ok(idx) = self.search(key) {
            return Err(InsertError::DuplicateKey(idx));
        }

        let insertion_point = self.search(key).unwrap_err();
        self.insert_leaf_value(key, ValueType::Inline, value, insertion_point)
    }

    /// Insert a leaf value at an already selected slot position.
    ///
    /// The caller controls duplicate ordering. Public mutation APIs reject
    /// duplicate live keys, while internal rebuilds use the upper bound so
    /// tombstone/version history is preserved in its original order.
    fn insert_leaf_value(
        &mut self,
        key: &[u8],
        value_type: ValueType,
        value: &[u8],
        insertion_point: usize,
    ) -> Result<(), InsertError> {
        if !self.is_leaf() || insertion_point > self.count() {
            return Err(InsertError::WrongNodeType);
        }

        // Prefix compression is relative to the preceding key. Inserting in
        // the middle changes that predecessor for every following entry, so
        // retain the logical entries and rebuild their encoded forms instead
        // of leaving stale prefix lengths in place.
        if insertion_point < self.count() && self.has_prefix_compression() {
            let parent_id = self.parent_id();
            let mut entries = Vec::with_capacity(self.count() + 1);
            for index in 0..self.count() {
                let entry_key = self.key(index).ok_or(InsertError::WrongNodeType)?;
                let (entry_type, entry_value) =
                    match self.value(index).ok_or(InsertError::WrongNodeType)? {
                        ValueRef::Inline(data) => (ValueType::Inline, data.to_vec()),
                        ValueRef::Blob(pointer) => {
                            (ValueType::BlobPointer, pointer.to_bytes().to_vec())
                        }
                        ValueRef::Tombstone => (ValueType::Tombstone, Vec::new()),
                    };
                entries.push((entry_key, entry_type, entry_value));
            }
            entries.insert(insertion_point, (key.to_vec(), value_type, value.to_vec()));

            let mut replacement = Self::new_leaf();
            replacement.set_parent_id(parent_id);
            for (entry_key, entry_type, entry_value) in entries {
                let append_at = replacement.count();
                replacement.insert_leaf_value_raw(
                    &entry_key,
                    entry_type,
                    &entry_value,
                    append_at,
                )?;
            }
            *self = replacement;
            return Ok(());
        }

        self.insert_leaf_value_raw(key, value_type, value, insertion_point)
    }

    fn insert_leaf_value_raw(
        &mut self,
        key: &[u8],
        value_type: ValueType,
        value: &[u8],
        insertion_point: usize,
    ) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }

        // New mutations use self-contained keys. This keeps middle inserts
        // O(1) and avoids changing the decode context of following entries;
        // existing compressed pages are normalized by the wrapper above when
        // a middle mutation first touches them.
        let prefix_len = 0_u16;
        let suffix = key;

        // Build entry: prefix_len(2) + suffix_len(2) + suffix + value_type(1) + value
        let entry_size = 4 + suffix.len() + 1 + value.len();
        if entry_size > PAGE_SIZE - HEADER_SIZE - SLOT_SIZE {
            return Err(InsertError::EntryTooLarge);
        }
        let count = self.count();

        // Check if there's room: need space for new slot + entry
        let slot_array_end = Self::slot_array_start() + (count + 1) * SLOT_SIZE;
        let entry_offset = self
            .new_entry_offset(entry_size)
            .ok_or(InsertError::PageFull)?;

        // Ensure slot array and entry don't overlap
        if slot_array_end + SLOT_SIZE > entry_offset {
            return Err(InsertError::PageFull);
        }

        // Write entry data at the new offset.
        let mut pos = entry_offset;
        self.data[pos..pos + 2].copy_from_slice(&prefix_len.to_le_bytes());
        pos += 2;
        self.data[pos..pos + 2].copy_from_slice(&(suffix.len() as u16).to_le_bytes());
        pos += 2;
        self.data[pos..pos + suffix.len()].copy_from_slice(suffix);
        pos += suffix.len();
        self.data[pos] = value_type as u8;
        pos += 1;
        self.data[pos..pos + value.len()].copy_from_slice(value);

        // Shift existing slots to make room at insertion_point.
        for i in (insertion_point..count).rev() {
            let old_offset = self.slot_offset(i) as u16;
            let old_key_len = self.slot_key_len(i);
            self.set_slot_offset(i + 1, old_offset);
            self.set_slot_key_len(i + 1, old_key_len);
        }

        // Write new slot.
        self.set_slot_offset(insertion_point, entry_offset as u16);
        self.set_slot_key_len(insertion_point, prefix_len + suffix.len() as u16);

        // Update header.
        let mut header = self.header();
        header.count += 1;
        let new_slot_array_end = Self::slot_array_start() + header.count as usize * SLOT_SIZE;
        let min_entry = (0..header.count as usize)
            .map(|i| self.slot_offset(i))
            .min()
            .unwrap_or(PAGE_SIZE);
        header.free_space = (min_entry - new_slot_array_end) as u32;
        self.set_header(&header);

        Ok(())
    }

    /// Return the insertion point after all existing versions of `key`.
    fn upper_bound(&self, key: &[u8]) -> usize {
        let mut index = match self.search(key) {
            Ok(index) | Err(index) => index,
        };
        while index < self.count() && self.key(index).as_deref() == Some(key) {
            index += 1;
        }
        index
    }

    /// Replace the value at a given index (in-place update).
    ///
    /// The new value must be the same size as the old value.
    /// This is used for upsert when the value size doesn't change.
    pub fn replace_value(&mut self, index: usize, new_value: &[u8]) {
        let entry_off = self.slot_offset(index);
        let suffix_len = self.entry_suffix_len(index) as usize;
        let vt_off = entry_off + 4 + suffix_len;
        let val_start = vt_off + 1;

        // Verify the new value is the same size.
        let old_value_type = self.data[vt_off];
        if old_value_type == ValueType::Inline as u8 {
            // Find the old value size by looking at neighboring entries.
            let val_end = (0..self.count())
                .filter(|&i| i != index)
                .map(|i| self.slot_offset(i))
                .filter(|&off| off > entry_off)
                .min()
                .unwrap_or(PAGE_SIZE);
            let old_size = val_end - val_start;
            assert_eq!(new_value.len(), old_size, "replace_value: size mismatch");
        } else {
            // For tombstone/blob, just overwrite with new inline value.
            // This is a simplification — in production, we'd handle this more carefully.
        }

        self.data[val_start..val_start + new_value.len()].copy_from_slice(new_value);
        self.data[vt_off] = ValueType::Inline as u8;
    }

    /// Replace an entry with a value of any size, rebuilding the leaf when
    /// an in-place replacement is not possible.
    ///
    /// The original node remains unchanged when the rebuilt page cannot fit.
    pub fn replace_value_resized(
        &mut self,
        index: usize,
        new_value: &[u8],
    ) -> Result<(), InsertError> {
        if !self.is_leaf() || index >= self.count() {
            return Err(InsertError::WrongNodeType);
        }

        let key = self.key(index).ok_or(InsertError::WrongNodeType)?;
        let original = self.clone();
        self.remove_entry(index)?;
        while let Ok(duplicate_index) = self.search(&key) {
            self.remove_entry(duplicate_index)?;
        }
        match self.insert(&key, new_value) {
            Ok(()) => Ok(()),
            Err(error) => {
                *self = original;
                Err(error)
            }
        }
    }

    /// Remove one leaf entry and compact the remaining entries.
    pub fn remove_entry(&mut self, index: usize) -> Result<(), InsertError> {
        if !self.is_leaf() || index >= self.count() {
            return Err(InsertError::WrongNodeType);
        }

        enum OwnedValue {
            Inline(Vec<u8>),
            Blob(BlobPointer),
            Tombstone,
        }

        let parent_id = self.parent_id();
        let mut entries = Vec::with_capacity(self.count() - 1);
        for entry_index in 0..self.count() {
            if entry_index == index {
                continue;
            }
            let key = self.key(entry_index).ok_or(InsertError::WrongNodeType)?;
            let value = match self.value(entry_index).ok_or(InsertError::WrongNodeType)? {
                ValueRef::Inline(value) => OwnedValue::Inline(value.to_vec()),
                ValueRef::Blob(pointer) => OwnedValue::Blob(pointer),
                ValueRef::Tombstone => OwnedValue::Tombstone,
            };
            entries.push((key, value));
        }

        let mut replacement = Self::new_leaf();
        replacement.set_parent_id(parent_id);
        for (key, value) in entries {
            let insertion_point = replacement.upper_bound(&key);
            let result = match value {
                OwnedValue::Inline(value) => {
                    replacement.insert_leaf_value(&key, ValueType::Inline, &value, insertion_point)
                }
                OwnedValue::Blob(pointer) => {
                    let bytes = pointer.to_bytes();
                    replacement.insert_leaf_value(
                        &key,
                        ValueType::BlobPointer,
                        &bytes,
                        insertion_point,
                    )
                }
                OwnedValue::Tombstone => {
                    replacement.insert_leaf_value(&key, ValueType::Tombstone, &[], insertion_point)
                }
            };
            result?;
        }

        *self = replacement;
        Ok(())
    }

    /// Insert a key with a tombstone marker (delete).
    pub fn insert_tombstone(&mut self, key: &[u8]) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }

        let insertion_point = match self.search(key) {
            Ok(index) => {
                self.remove_entry(index)?;
                self.search(key).unwrap_or_else(|idx| idx)
            }
            Err(index) => index,
        };
        self.insert_leaf_value(key, ValueType::Tombstone, &[], insertion_point)
    }

    /// Insert a key with a blob pointer value.
    pub fn insert_blob(&mut self, key: &[u8], ptr: BlobPointer) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }

        if let Ok(idx) = self.search(key) {
            return Err(InsertError::DuplicateKey(idx));
        }

        let insertion_point = self.search(key).unwrap_err();
        let bytes = ptr.to_bytes();
        self.insert_leaf_value(key, ValueType::BlobPointer, &bytes, insertion_point)
    }

    /// Insert a child pointer (for internal nodes).
    ///
    /// Internal nodes store: key → child_page_id.
    /// The leftmost child (before the first key) is stored separately via
    /// `set_leftmost_child` / `leftmost_child`.
    pub fn insert_child(&mut self, key: &[u8], child_id: u64) -> Result<(), InsertError> {
        if !self.is_internal() {
            return Err(InsertError::WrongNodeType);
        }

        let insertion_point = match self.search(key) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        // Internal keys use the same predecessor-relative compression as
        // leaf keys. Rebuild when inserting into the middle so subsequent
        // separators remain decodable and keep their routing bounds.
        if insertion_point < self.count() && self.has_prefix_compression() {
            let parent_id = self.parent_id();
            let leftmost_child = self.leftmost_child();
            let mut entries = Vec::with_capacity(self.count() + 1);
            for index in 0..self.count() {
                let entry_key = self.key(index).ok_or(InsertError::WrongNodeType)?;
                let entry_child = self.child_id(index).ok_or(InsertError::WrongNodeType)?;
                entries.push((entry_key, entry_child));
            }
            entries.insert(insertion_point, (key.to_vec(), child_id));

            let mut replacement = Self::new_internal();
            replacement.set_parent_id(parent_id);
            replacement.set_leftmost_child(leftmost_child);
            for (entry_key, entry_child) in entries {
                let append_at = replacement.count();
                replacement.insert_child_raw(&entry_key, entry_child, append_at)?;
            }
            *self = replacement;
            return Ok(());
        }

        self.insert_child_raw(key, child_id, insertion_point)
    }

    fn insert_child_raw(
        &mut self,
        key: &[u8],
        child_id: u64,
        insertion_point: usize,
    ) -> Result<(), InsertError> {
        if !self.is_internal() || insertion_point > self.count() {
            return Err(InsertError::WrongNodeType);
        }

        // Keep new internal entries self-contained for stable middle inserts.
        let prefix_len = 0_u16;
        let suffix = key;

        // entry = prefix_len(2) + suffix_len(2) + suffix + child_id(8)
        let entry_size = 4 + suffix.len() + 8;
        if entry_size > PAGE_SIZE - HEADER_SIZE - SLOT_SIZE {
            return Err(InsertError::EntryTooLarge);
        }
        let count = self.count();

        let slot_array_end = Self::slot_array_start() + (count + 1) * SLOT_SIZE;
        let entry_offset = self
            .new_entry_offset(entry_size)
            .ok_or(InsertError::PageFull)?;

        if slot_array_end + SLOT_SIZE > entry_offset {
            return Err(InsertError::PageFull);
        }

        let mut pos = entry_offset;
        self.data[pos..pos + 2].copy_from_slice(&prefix_len.to_le_bytes());
        pos += 2;
        self.data[pos..pos + 2].copy_from_slice(&(suffix.len() as u16).to_le_bytes());
        pos += 2;
        self.data[pos..pos + suffix.len()].copy_from_slice(suffix);
        pos += suffix.len();
        self.data[pos..pos + 8].copy_from_slice(&child_id.to_le_bytes());

        for i in (insertion_point..count).rev() {
            let old_offset = self.slot_offset(i) as u16;
            let old_key_len = self.slot_key_len(i);
            self.set_slot_offset(i + 1, old_offset);
            self.set_slot_key_len(i + 1, old_key_len);
        }

        self.set_slot_offset(insertion_point, entry_offset as u16);
        self.set_slot_key_len(insertion_point, prefix_len + suffix.len() as u16);

        let mut header = self.header();
        header.count += 1;
        let new_slot_array_end = Self::slot_array_start() + header.count as usize * SLOT_SIZE;
        let min_entry = (0..header.count as usize)
            .map(|i| self.slot_offset(i))
            .min()
            .unwrap_or(PAGE_SIZE);
        header.free_space = (min_entry - new_slot_array_end) as u32;
        self.set_header(&header);

        Ok(())
    }

    /// Compute the checksum of the page (excluding the checksum field itself).
    pub fn compute_checksum(&self) -> u64 {
        // Checksum everything except bytes 20..28 (the checksum field).
        let mut hasher = crc32c::crc32c(&self.data[..20]);
        hasher = crc32c::crc32c_combine(hasher, crc32c::crc32c(&self.data[28..]), PAGE_SIZE - 28);
        hasher as u64
    }

    /// Update the stored checksum.
    pub fn update_checksum(&mut self) {
        let checksum = self.compute_checksum();
        let mut header = self.header();
        header.checksum = checksum;
        self.set_header(&header);
    }

    /// Verify the stored checksum matches the computed one.
    pub fn verify_checksum(&self) -> bool {
        let header = self.header();
        header.checksum == self.compute_checksum()
    }

    /// Iterate over keys in sorted order.
    pub fn keys(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        (0..self.count()).filter_map(move |i| self.key(i))
    }

    /// Split this node, returning a new right sibling.
    ///
    /// The median key is returned separately for insertion into the parent.
    ///
    /// For leaf nodes: median key and all keys >= median go to the right sibling.
    /// For internal nodes: median key goes to the parent; keys > median go right.
    pub fn split(&mut self) -> Result<(Vec<u8>, Node), SplitError> {
        let count = self.count();
        if count < 2 {
            return Err(SplitError::TooFewKeys);
        }

        let mid = count / 2;
        let median_key = self.key(mid).ok_or(SplitError::Corruption)?;

        let mut right = if self.is_leaf() {
            Node::new_leaf()
        } else {
            Node::new_internal()
        };

        // Copy keys from mid..count to the right node.
        if self.is_leaf() {
            for i in mid..count {
                let k = self.key(i).ok_or(SplitError::Corruption)?;
                let v = self.value(i).ok_or(SplitError::Corruption)?;
                let insertion_point = right.upper_bound(&k);
                match v {
                    ValueRef::Inline(data) => {
                        right
                            .insert_leaf_value(&k, ValueType::Inline, data, insertion_point)
                            .map_err(|_| SplitError::InsertFailed)?;
                    }
                    ValueRef::Blob(ptr) => {
                        let bytes = ptr.to_bytes();
                        right
                            .insert_leaf_value(&k, ValueType::BlobPointer, &bytes, insertion_point)
                            .map_err(|_| SplitError::InsertFailed)?;
                    }
                    ValueRef::Tombstone => {
                        right
                            .insert_leaf_value(&k, ValueType::Tombstone, &[], insertion_point)
                            .map_err(|_| SplitError::InsertFailed)?;
                    }
                }
            }
        } else {
            // Internal: copy keys > median (skip mid itself — it goes to parent).
            // The right node's leftmost_child is the child AFTER the median key.
            let right_leftmost = self.child_id(mid).unwrap_or(0);
            right.set_leftmost_child(right_leftmost);

            for i in (mid + 1)..count {
                let k = self.key(i).ok_or(SplitError::Corruption)?;
                let c = self.child_id(i).ok_or(SplitError::Corruption)?;
                right
                    .insert_child(&k, c)
                    .map_err(|_| SplitError::InsertFailed)?;
            }
        }

        // Rebuild the left leaf so truncation also reclaims the old right-half
        // entry bytes. Merely reducing the slot count would leave stale low
        // offsets consuming the page and eventually report false PageFull
        // errors after repeated left-side splits.
        if self.is_leaf() {
            let parent_id = self.parent_id();
            let mut left = Node::new_leaf();
            left.set_parent_id(parent_id);
            for i in 0..mid {
                let key = self.key(i).ok_or(SplitError::Corruption)?;
                let value = self.value(i).ok_or(SplitError::Corruption)?;
                let insertion_point = left.upper_bound(&key);
                match value {
                    ValueRef::Inline(data) => {
                        left.insert_leaf_value(&key, ValueType::Inline, data, insertion_point)
                            .map_err(|_| SplitError::InsertFailed)?;
                    }
                    ValueRef::Blob(pointer) => {
                        let bytes = pointer.to_bytes();
                        left.insert_leaf_value(
                            &key,
                            ValueType::BlobPointer,
                            &bytes,
                            insertion_point,
                        )
                        .map_err(|_| SplitError::InsertFailed)?;
                    }
                    ValueRef::Tombstone => {
                        left.insert_leaf_value(&key, ValueType::Tombstone, &[], insertion_point)
                            .map_err(|_| SplitError::InsertFailed)?;
                    }
                }
            }
            *self = left;
        } else {
            // Internal entries can be physically interleaved with the half
            // being removed when separators arrived out of order. Rebuilding
            // the retained left half is required to reclaim those holes;
            // merely truncating the slot array can leave two logical entries
            // with almost no reported free space and make the next split
            // fail with PageFull.
            let parent_id = self.parent_id();
            let leftmost_child = self.leftmost_child();
            let mut left = Node::new_internal();
            left.set_parent_id(parent_id);
            left.set_leftmost_child(leftmost_child);
            for index in 0..mid {
                let key = self.key(index).ok_or(SplitError::Corruption)?;
                let child = self.child_id(index).ok_or(SplitError::Corruption)?;
                left.insert_child(&key, child)
                    .map_err(|_| SplitError::InsertFailed)?;
            }
            *self = left;
        }

        Ok((median_key, right))
    }
}

/// Reference to a value in a leaf node.
#[derive(Debug)]
pub enum ValueRef<'a> {
    /// Value stored inline in the page.
    Inline(&'a [u8]),
    /// Value stored in a blob file.
    Blob(BlobPointer),
    /// Key has been deleted.
    Tombstone,
}

/// Error from inserting into a node.
#[derive(Debug, Clone, thiserror::Error)]
pub enum InsertError {
    #[error("page is full")]
    PageFull,
    #[error("entry is too large for a page")]
    EntryTooLarge,
    #[error("wrong node type for this operation")]
    WrongNodeType,
    #[error("duplicate key at index {0}")]
    DuplicateKey(usize),
}

/// Error from splitting a node.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SplitError {
    #[error("node has too few keys to split")]
    TooFewKeys,
    #[error("node data corruption")]
    Corruption,
    #[error("failed to insert into new node")]
    InsertFailed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_node() {
        let node = Node::new_leaf();
        assert_eq!(node.count(), 0);
        assert!(node.is_leaf());
        assert!(!node.is_internal());
        assert!(node.header().is_valid());
    }

    #[test]
    fn test_internal_node() {
        let node = Node::new_internal();
        assert!(node.is_internal());
        assert_eq!(node.count(), 0);
    }

    #[test]
    fn test_insert_and_lookup() {
        let mut node = Node::new_leaf();
        node.insert(b"hello", b"world").unwrap();
        node.insert(b"foo", b"bar").unwrap();
        node.insert(b"aaa", b"bbb").unwrap();

        assert_eq!(node.count(), 3);

        // Keys should be sorted.
        assert_eq!(node.key(0), Some(b"aaa".to_vec()));
        assert_eq!(node.key(1), Some(b"foo".to_vec()));
        assert_eq!(node.key(2), Some(b"hello".to_vec()));

        // Values should be retrievable.
        assert!(matches!(node.value(0), Some(ValueRef::Inline(b"bbb"))));
        assert!(matches!(node.value(1), Some(ValueRef::Inline(b"bar"))));
        assert!(matches!(node.value(2), Some(ValueRef::Inline(b"world"))));
    }

    #[test]
    fn test_search() {
        let mut node = Node::new_leaf();
        node.insert(b"a", b"1").unwrap();
        node.insert(b"c", b"3").unwrap();
        node.insert(b"e", b"5").unwrap();

        assert_eq!(node.search(b"a"), Ok(0));
        assert_eq!(node.search(b"c"), Ok(1));
        assert_eq!(node.search(b"e"), Ok(2));
        assert_eq!(node.search(b"b"), Err(1)); // between a and c
        assert_eq!(node.search(b"d"), Err(2)); // between c and e
        assert_eq!(node.search(b"z"), Err(3)); // after all
    }

    #[test]
    fn test_shared_prefix_keys_roundtrip() {
        let mut node = Node::new_leaf();
        // Shared-prefix keys must remain lossless under the self-contained
        // live-mutation encoding.
        node.insert(b"key_001", b"v1").unwrap();
        node.insert(b"key_002", b"v2").unwrap();
        node.insert(b"key_003", b"v3").unwrap();

        assert_eq!(node.key(0), Some(b"key_001".to_vec()));
        assert_eq!(node.key(1), Some(b"key_002".to_vec()));
        assert_eq!(node.key(2), Some(b"key_003".to_vec()));
        assert_eq!(node.entry_prefix_len(1), 0);
        assert_eq!(node.entry_prefix_len(2), 0);
    }

    #[test]
    fn test_entry_too_large_is_typed() {
        let mut node = Node::new_leaf();
        let key = vec![0xA5; PAGE_SIZE];
        assert!(matches!(
            node.insert(&key, b"value"),
            Err(InsertError::EntryTooLarge)
        ));
    }

    #[test]
    fn test_tombstone() {
        let mut node = Node::new_leaf();
        node.insert(b"alive", b"yes").unwrap();
        node.insert_tombstone(b"dead").unwrap();

        assert_eq!(node.count(), 2);
        assert!(matches!(node.value(0), Some(ValueRef::Inline(_))));
        assert!(matches!(node.value(1), Some(ValueRef::Tombstone)));
    }

    #[test]
    fn test_tombstone_same_key() {
        let mut node = Node::new_leaf();
        node.insert(b"key", b"value").unwrap();

        node.insert_tombstone(b"key").unwrap();
        assert_eq!(node.count(), 1);
        assert_eq!(node.search(b"key"), Ok(0));
        assert!(matches!(node.value(0), Some(ValueRef::Tombstone)));
    }

    #[test]
    fn test_blob_pointer() {
        let mut node = Node::new_leaf();
        node.insert(b"aaa", b"inline").unwrap();
        node.insert_blob(
            b"zzz",
            BlobPointer {
                file_id: 1,
                offset: 4096,
                length: 1024,
            },
        )
        .unwrap();

        assert_eq!(node.count(), 2);
        assert!(matches!(node.value(0), Some(ValueRef::Inline(_))));
        assert!(matches!(
            node.value(1),
            Some(ValueRef::Blob(BlobPointer {
                file_id: 1,
                offset: 4096,
                length: 1024
            }))
        ));
    }

    #[test]
    fn test_internal_node_children() {
        let mut node = Node::new_internal();
        node.set_leftmost_child(7);
        node.insert_child(b"b", 10).unwrap();
        node.insert_child(b"d", 20).unwrap();
        node.insert_child(b"f", 30).unwrap();

        assert_eq!(node.child_id(0), Some(10));
        assert_eq!(node.child_id(1), Some(20));
        assert_eq!(node.child_id(2), Some(30));

        let restored = Node::from_bytes(node.into_bytes()).unwrap();
        assert_eq!(restored.leftmost_child(), 7);
        assert_eq!(restored.child_id(0), Some(10));
    }

    #[test]
    fn test_duplicate_key_rejected() {
        let mut node = Node::new_leaf();
        node.insert(b"key", b"val1").unwrap();
        assert!(matches!(
            node.insert(b"key", b"val2"),
            Err(InsertError::DuplicateKey(_))
        ));
    }

    #[test]
    fn test_split_leaf() {
        let mut node = Node::new_leaf();
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let val = format!("val_{:03}", i);
            node.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let (median, right) = node.split().unwrap();
        assert_eq!(median, b"key_005");
        assert_eq!(node.count(), 5);
        assert_eq!(right.count(), 5);

        // Left has first 5 keys.
        assert_eq!(node.key(0), Some(b"key_000".to_vec()));
        assert_eq!(node.key(4), Some(b"key_004".to_vec()));

        // Right has last 5 keys.
        assert_eq!(right.key(0), Some(b"key_005".to_vec()));
        assert_eq!(right.key(4), Some(b"key_009".to_vec()));
    }

    #[test]
    fn test_checksum_roundtrip() {
        let mut node = Node::new_leaf();
        node.insert(b"key", b"value").unwrap();
        node.update_checksum();
        assert!(node.verify_checksum());
    }

    #[test]
    fn test_from_bytes_roundtrip() {
        let mut node = Node::new_leaf();
        node.insert(b"hello", b"world").unwrap();

        let bytes = node.into_bytes();
        let restored = Node::from_bytes(bytes).unwrap();
        assert_eq!(restored.count(), 1);
        assert_eq!(restored.key(0), Some(b"hello".to_vec()));
    }

    #[test]
    fn test_invalid_magic() {
        let data = Box::new([0u8; PAGE_SIZE]);
        assert!(Node::from_bytes(data).is_none());
    }

    #[test]
    fn test_rejects_unknown_page_type() {
        let mut node = Node::new_leaf().into_bytes();
        node[8..12].copy_from_slice(&99u32.to_le_bytes());
        assert!(Node::from_bytes(node).is_none());
    }

    #[test]
    fn test_rejects_invalid_slot_bounds() {
        let mut node = Node::new_leaf();
        node.insert(b"key", b"value").unwrap();
        let mut bytes = node.into_bytes();
        bytes[40..42].copy_from_slice(&1u16.to_le_bytes());
        assert!(Node::from_bytes(bytes).is_none());
    }

    #[test]
    fn test_keys_iterator() {
        let mut node = Node::new_leaf();
        node.insert(b"c", b"3").unwrap();
        node.insert(b"a", b"1").unwrap();
        node.insert(b"b", b"2").unwrap();

        let keys: Vec<_> = node.keys().collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_many_inserts() {
        let mut node = Node::new_leaf();
        let mut inserted = Vec::new();

        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            match node.insert(key.as_bytes(), val.as_bytes()) {
                Ok(()) => inserted.push(key),
                Err(InsertError::PageFull) => break,
                Err(e) => panic!("unexpected error: {}", e),
            }
        }

        // Should fit many entries in 4KB.
        assert!(
            inserted.len() > 50,
            "expected >50 entries, got {}",
            inserted.len()
        );

        // Verify all inserted keys are searchable.
        for key in &inserted {
            assert!(node.search(key.as_bytes()).is_ok(), "key {} not found", key);
        }
    }

    #[test]
    fn test_parent_id() {
        let mut node = Node::new_leaf();
        assert_eq!(node.parent_id(), 0);
        node.set_parent_id(42);
        assert_eq!(node.parent_id(), 42);
    }

    #[test]
    fn test_header_roundtrip() {
        let header = NodeHeader {
            magic: MAGIC,
            version: PAGE_VERSION,
            page_type: PageType::Leaf,
            count: 5,
            free_space: 3000,
            checksum: 0xDEAD_BEEF_CAFE_BABE,
            parent_id: 123,
            leftmost_child: 456,
        };
        let bytes = header.to_bytes();
        let restored = NodeHeader::from_bytes(&bytes);
        assert_eq!(restored.magic, header.magic);
        assert_eq!(restored.version, header.version);
        assert_eq!(restored.page_type, header.page_type);
        assert_eq!(restored.count, header.count);
        assert_eq!(restored.free_space, header.free_space);
        assert_eq!(restored.checksum, header.checksum);
        assert_eq!(restored.parent_id, header.parent_id);
        assert_eq!(restored.leftmost_child, header.leftmost_child);
    }

    #[test]
    fn test_tombstone_blob_layout_roundtrip() {
        let mut node = Node::new_leaf();
        for key in [b"key1".as_slice(), b"key2".as_slice(), b"key3".as_slice()] {
            node.insert_blob(
                key,
                BlobPointer {
                    file_id: 1,
                    offset: 4096,
                    length: 2000,
                },
            )
            .unwrap();
        }
        node.insert_tombstone(b"key1").unwrap();
        node.insert_tombstone(b"key2").unwrap();

        assert!(Node::from_bytes(node.into_bytes()).is_some());
    }
}
