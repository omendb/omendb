//! Unit tests for the buffered WAL manager.

use super::*;
use crate::recovery::RecordType;

#[test]
fn test_wal_manager() {
    let mut wal = WalManager::new(SyncPolicy::FDataSync);

    wal.append(&WalRecord::pmt_update(1, 0, 4096));
    wal.append(&WalRecord::txn_commit(1));

    assert_eq!(wal.records_written(), 2);
    assert!(wal.bytes_written() > 0);
}

#[test]
fn test_wal_flush() {
    let mut wal = WalManager::new(SyncPolicy::None);
    wal.append(&WalRecord::pmt_update(1, 0, 4096));

    let mut buf = Vec::new();
    wal.flush(&mut buf).unwrap();

    assert!(!buf.is_empty());

    let records = WalManager::parse_records(&buf);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_type, RecordType::PmtUpdate);
}

#[test]
fn test_parse_multiple_records() {
    let mut wal = WalManager::new(SyncPolicy::None);
    wal.append(&WalRecord::pmt_update(1, 0, 100));
    wal.append(&WalRecord::pmt_update(2, 0, 200));
    wal.append(&WalRecord::txn_commit(1));

    let mut buf = Vec::new();
    wal.flush(&mut buf).unwrap();

    let records = WalManager::parse_records(&buf);
    assert_eq!(records.len(), 3);
}
