//! Blob-record wire format.
//!
//! [`super::BlobFile`] owns append ordering, offsets, deletion metadata, and
//! record-stream lifecycle. This module owns the bytes and checksum for one
//! record within that stream.

/// A blob record stored in a blob file.
#[derive(Debug, Clone)]
pub(crate) struct BlobRecord {
    /// First 8 bytes of the key (for identification during GC).
    pub(crate) key_prefix: [u8; 8],
    /// The value data.
    pub(crate) value: Vec<u8>,
}

impl BlobRecord {
    /// Size of the serialized record (excluding value data).
    pub(crate) const HEADER_SIZE: usize = 8 + 4; // key_prefix + length
    pub(crate) const FOOTER_SIZE: usize = 4; // crc32c
    pub(crate) const OVERHEAD_SIZE: usize = Self::HEADER_SIZE + Self::FOOTER_SIZE;

    /// Create a new blob record.
    pub(crate) fn new(key_prefix: [u8; 8], value: Vec<u8>) -> Self {
        Self { key_prefix, value }
    }

    /// Total serialized size.
    pub(crate) fn serialized_size(&self) -> usize {
        Self::OVERHEAD_SIZE + self.value.len()
    }

    /// Serialize to bytes.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.serialized_size());
        buf.extend_from_slice(&self.key_prefix);
        buf.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.value);
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Deserialize the first complete record in a byte slice.
    pub(crate) fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::OVERHEAD_SIZE {
            return None;
        }

        let mut key_prefix = [0u8; 8];
        key_prefix.copy_from_slice(&buf[0..8]);

        let length = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
        let total_size = Self::OVERHEAD_SIZE.checked_add(length)?;

        if buf.len() < total_size {
            return None;
        }

        let stored_crc = u32::from_le_bytes([
            buf[total_size - 4],
            buf[total_size - 3],
            buf[total_size - 2],
            buf[total_size - 1],
        ]);
        let computed_crc = crc32c::crc32c(&buf[0..total_size - 4]);
        if stored_crc != computed_crc {
            return None;
        }

        let value = buf[12..12 + length].to_vec();
        Some(Self { key_prefix, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_record_roundtrip() {
        let record = BlobRecord::new([1, 2, 3, 4, 5, 6, 7, 8], vec![10, 20, 30]);
        let bytes = record.to_bytes();
        let restored = BlobRecord::from_bytes(&bytes).unwrap();

        assert_eq!(restored.key_prefix, record.key_prefix);
        assert_eq!(restored.value, record.value);
    }

    #[test]
    fn test_blob_record_crc() {
        let record = BlobRecord::new([0; 8], vec![1, 2, 3]);
        let mut bytes = record.to_bytes();

        let len = bytes.len();
        bytes[len - 5] ^= 0xFF;

        assert!(BlobRecord::from_bytes(&bytes).is_none());
    }
}
