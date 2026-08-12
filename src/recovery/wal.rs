//! Write-Ahead Log (WAL) implementation.
//!
//! The WAL records all mutations before they are applied to the database.
//! Each record is checksummed with CRC32C for integrity.
//!
//! # Record Format
//!
//! ```text
//! [length: u32] [type: u8] [payload: bytes] [crc32c: u4]
//! ```
//!
//! # Sync Policies
//!
//! - `SyncAll`: fsync after every commit (safest, slowest)
//! - `FDataSync`: fdatasync after every commit (good balance)
//! - `None`: no sync (fastest, risk of data loss on crash)

use std::io::{self, Write};

use crate::storage::format::CommitRecord;

/// Sync policy for the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// fsync after every commit.
    SyncAll,
    /// fdatasync after every commit.
    FDataSync,
    /// No sync (fastest, risk of data loss).
    None,
}

/// WAL record types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// PMT update: page_id → (file_id, offset).
    PmtUpdate = 1,
    /// Page allocation.
    PageAlloc = 2,
    /// Page deallocation.
    PageDealloc = 3,
    /// Blob append.
    BlobAppend = 4,
    /// Transaction commit.
    TxnCommit = 5,
    /// Transaction abort.
    TxnAbort = 6,
    /// Checkpoint marker.
    Checkpoint = 7,
    /// Legacy Put: key_len(u16) + key + value_len(u16) + value.
    Put = 8,
    /// Legacy Delete: key_len(u16) + key.
    Delete = 9,
    /// Durable commit envelope with commit/generation/root/count/digest.
    Commit = 10,
    /// Current Put: key_len(u32) + key + value_len(u32) + value.
    PutV2 = 11,
    /// Current Delete: key_len(u32) + key.
    DeleteV2 = 12,
}

/// Result of parsing a WAL prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseStatus {
    /// The entire input contains complete valid records.
    Complete,
    /// The final record is incomplete and may be a torn write.
    Incomplete,
    /// A complete record has an invalid type or checksum.
    Corrupt,
}

/// A WAL record.
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// Record type.
    pub record_type: RecordType,
    /// Record payload (variable-length).
    pub payload: Vec<u8>,
}

impl WalRecord {
    /// Create a new WAL record.
    pub fn new(record_type: RecordType, payload: Vec<u8>) -> Self {
        Self {
            record_type,
            payload,
        }
    }

    /// Create a PMT update record.
    pub fn pmt_update(page_id: u64, file_id: u32, offset: u64) -> Self {
        let mut payload = Vec::with_capacity(20);
        payload.extend_from_slice(&page_id.to_le_bytes());
        payload.extend_from_slice(&file_id.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());
        Self::new(RecordType::PmtUpdate, payload)
    }

    /// Create a page allocation record.
    pub fn page_alloc(page_id: u64, file_id: u32) -> Self {
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&page_id.to_le_bytes());
        payload.extend_from_slice(&file_id.to_le_bytes());
        Self::new(RecordType::PageAlloc, payload)
    }

    /// Create a page deallocation record.
    pub fn page_dealloc(page_id: u64) -> Self {
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&page_id.to_le_bytes());
        Self::new(RecordType::PageDealloc, payload)
    }

    /// Create a transaction commit record.
    pub fn txn_commit(txn_id: u64) -> Self {
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&txn_id.to_le_bytes());
        Self::new(RecordType::TxnCommit, payload)
    }

    /// Create a transaction abort record.
    pub fn txn_abort(txn_id: u64) -> Self {
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&txn_id.to_le_bytes());
        Self::new(RecordType::TxnAbort, payload)
    }

    /// Create a durable commit envelope record.
    pub fn commit(commit: CommitRecord) -> Self {
        Self::new(RecordType::Commit, commit.to_bytes().to_vec())
    }

    /// Decode this record as a durable commit envelope.
    pub fn commit_record(&self) -> Option<CommitRecord> {
        (self.record_type == RecordType::Commit)
            .then(|| CommitRecord::from_bytes(&self.payload))
            .flatten()
    }

    /// Create a Put record (for crash recovery of B-tree data).
    pub fn put(key: &[u8], value: &[u8]) -> Self {
        let mut payload = Vec::with_capacity(4 + key.len() + 4 + value.len());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(value);
        Self::new(RecordType::PutV2, payload)
    }

    /// Create a Delete record (for crash recovery of B-tree data).
    pub fn delete(key: &[u8]) -> Self {
        let mut payload = Vec::with_capacity(4 + key.len());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        Self::new(RecordType::DeleteV2, payload)
    }

    /// Serialize the record to bytes (for writing to the WAL file).
    ///
    /// Format: length(u32) + type(u8) + payload + crc32c(u32)
    pub fn to_bytes(&self) -> Vec<u8> {
        let payload_len = self.payload.len();
        let total_len = 4 + 1 + payload_len + 4; // length + type + payload + crc
        let mut buf = Vec::with_capacity(total_len);

        // Length field (includes type + payload + crc).
        let length = (1 + payload_len + 4) as u32;
        buf.extend_from_slice(&length.to_le_bytes());

        // Record type.
        buf.push(self.record_type as u8);

        // Payload.
        buf.extend_from_slice(&self.payload);

        // CRC32C checksum over type + payload.
        let crc = crc32c::crc32c(&buf[4..]);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize a record from bytes.
    ///
    /// Returns the record and the number of bytes consumed.
    pub fn from_bytes(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 4 {
            return None;
        }

        let length = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if length < 5 {
            return None;
        }
        let total_len = 4 + length;

        if buf.len() < total_len {
            return None;
        }

        let record_type = match buf[4] {
            1 => RecordType::PmtUpdate,
            2 => RecordType::PageAlloc,
            3 => RecordType::PageDealloc,
            4 => RecordType::BlobAppend,
            5 => RecordType::TxnCommit,
            6 => RecordType::TxnAbort,
            7 => RecordType::Checkpoint,
            8 => RecordType::Put,
            9 => RecordType::Delete,
            10 => RecordType::Commit,
            11 => RecordType::PutV2,
            12 => RecordType::DeleteV2,
            _ => return None, // unknown type
        };

        let payload = buf[5..total_len - 4].to_vec();

        // Verify CRC.
        let stored_crc = u32::from_le_bytes([
            buf[total_len - 4],
            buf[total_len - 3],
            buf[total_len - 2],
            buf[total_len - 1],
        ]);
        let computed_crc = crc32c::crc32c(&buf[4..total_len - 4]);
        if stored_crc != computed_crc {
            return None; // CRC mismatch
        }

        Some((
            Self {
                record_type,
                payload,
            },
            total_len,
        ))
    }
}

/// WAL manager for writing and reading WAL records.
pub struct WalManager {
    /// Buffer for accumulating records before flush.
    buffer: Vec<u8>,
    /// Sync policy.
    sync_policy: SyncPolicy,
    /// Total bytes written.
    bytes_written: u64,
    /// Number of records written.
    records_written: u64,
}

impl WalManager {
    /// Create a new WAL manager with the given sync policy.
    pub fn new(sync_policy: SyncPolicy) -> Self {
        Self {
            buffer: Vec::with_capacity(64 * 1024), // 64KB buffer
            sync_policy,
            bytes_written: 0,
            records_written: 0,
        }
    }

    /// Get the sync policy.
    pub fn sync_policy(&self) -> SyncPolicy {
        self.sync_policy
    }

    /// Get total bytes written.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Get number of records written.
    pub fn records_written(&self) -> u64 {
        self.records_written
    }

    /// Append a record to the WAL buffer.
    pub fn append(&mut self, record: &WalRecord) {
        let bytes = record.to_bytes();
        self.buffer.extend_from_slice(&bytes);
        self.bytes_written += bytes.len() as u64;
        self.records_written += 1;
    }

    /// Flush the buffer to the provided writer.
    pub fn flush<W: Write>(&mut self, writer: &mut W) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        writer.write_all(&self.buffer)?;
        writer.flush()?;
        self.buffer.clear();
        Ok(())
    }

    /// Parse all records from a buffer.
    pub fn parse_records(buf: &[u8]) -> Vec<WalRecord> {
        Self::parse_records_with_status(buf).0
    }

    /// Parse a WAL prefix and classify an incomplete or corrupt suffix.
    pub fn parse_records_with_status(buf: &[u8]) -> (Vec<WalRecord>, ParseStatus) {
        let mut records = Vec::new();
        let mut pos = 0;

        while pos < buf.len() {
            if buf.len() - pos < 4 {
                return (records, ParseStatus::Incomplete);
            }
            let length =
                u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
            // A fixed WAL reservation may leave an all-zero unused tail after
            // the last valid record. Treat only that exact suffix as an
            // unused/torn tail; non-zero malformed bytes remain corruption.
            if length == 0 {
                return if buf[pos..].iter().all(|byte| *byte == 0) {
                    (records, ParseStatus::Incomplete)
                } else {
                    (records, ParseStatus::Corrupt)
                };
            }
            if length < 5 {
                return (records, ParseStatus::Corrupt);
            }
            let total_len = match 4usize.checked_add(length) {
                Some(total_len) => total_len,
                None => return (records, ParseStatus::Corrupt),
            };
            if buf.len() - pos < total_len {
                return (records, ParseStatus::Incomplete);
            }
            match WalRecord::from_bytes(&buf[pos..]) {
                Some((record, consumed)) => {
                    records.push(record);
                    pos += consumed;
                }
                None => return (records, ParseStatus::Corrupt),
            }
        }

        (records, ParseStatus::Complete)
    }
}

#[cfg(test)]
#[path = "wal_tests.rs"]
mod tests;
