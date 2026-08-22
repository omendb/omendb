//! Fixed-page encoding, validation, and checksum ownership for B-tree nodes.

use super::{Node, PAGE_SIZE};

/// Header size in bytes.
pub(super) const HEADER_SIZE: usize = 48;

/// Size of each slot entry (offset: u16, key_len: u16).
pub(super) const SLOT_SIZE: usize = 4;

/// Blob pointer size: file_id(4) + offset(8) + length(4) = 16 bytes.
pub const BLOB_POINTER_SIZE: usize = 16;

pub(super) const MAGIC: u32 = 0x5345_4552; // "SEER"
pub(super) const PAGE_VERSION: u32 = 3;

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
    /// Size of serialized blob pointer (file_id:4 + offset:8 + length:4).
    pub const SERIALIZED_SIZE: usize = BLOB_POINTER_SIZE;

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

/// Fixed-size page header (48 bytes).
///
/// Layout: magic(4) | version(4) | page_type(4) | count(4) |
/// free_space(4) | checksum(8) | parent_id(4) | leftmost_child(8) |
/// write_generation(8)
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
    /// Generation whose publication wrote this page image. Snapshot
    /// creation compares this against a historical root's generation to
    /// detect that the mapped bytes were rewritten.
    pub write_generation: u64,
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
        buf[40..48].copy_from_slice(&self.write_generation.to_le_bytes());
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
        let write_generation = u64::from_le_bytes([
            buf[40], buf[41], buf[42], buf[43], buf[44], buf[45], buf[46], buf[47],
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
            write_generation,
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
            write_generation: 0,
        }
    }

    /// Validate the header magic and version.
    pub fn is_valid(&self) -> bool {
        self.magic == MAGIC && self.version == PAGE_VERSION
    }
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
    pub(super) fn set_header(&mut self, header: &NodeHeader) {
        self.data[..HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    /// Set the cached parent page ID.
    pub fn set_parent_id(&mut self, parent_id: u32) {
        let mut header = self.header();
        header.parent_id = parent_id;
        self.set_header(&header);
    }

    /// Cached parent page ID (0 if root in a fully resident tree).
    pub fn parent_id(&self) -> u32 {
        self.header().parent_id
    }

    /// Generation whose publication wrote this page image.
    pub fn write_generation(&self) -> u64 {
        self.header().write_generation
    }

    /// Stamp the page with the generation publishing this image. Must be
    /// called before `update_checksum` so the stamp is covered by the
    /// checksum.
    pub fn set_write_generation(&mut self, generation: u64) {
        let mut header = self.header();
        header.write_generation = generation;
        self.set_header(&header);
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
}
