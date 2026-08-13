//! Buffered WAL manager.
//!
//! The WAL is durable before page generations are published. On crash
//! recovery, only mutation prefixes closed by a valid commit envelope are
//! replayed.
//!
//! # Sync Policies
//!
//! - `SyncAll`: fsync after every commit (safest, slowest)
//! - `FDataSync`: fdatasync after every commit (good balance)
//! - `None`: no sync (fastest, risk of data loss)

use std::io::{self, Write};

use super::record::{ParseStatus, WalRecord};

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
        super::record::parse_records(buf)
    }

    /// Parse a WAL prefix and classify an incomplete or corrupt suffix.
    pub fn parse_records_with_status(buf: &[u8]) -> (Vec<WalRecord>, ParseStatus) {
        super::record::parse_records_with_status(buf)
    }
}

#[cfg(test)]
#[path = "wal_tests.rs"]
mod tests;
