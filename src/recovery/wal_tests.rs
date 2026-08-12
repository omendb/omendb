//! Unit tests for the WAL record codec and buffered manager.

use super::*;

#[test]
fn test_record_roundtrip() {
    let record = WalRecord::pmt_update(42, 0, 4096);
    let bytes = record.to_bytes();
    let (restored, consumed) = WalRecord::from_bytes(&bytes).unwrap();

    assert_eq!(consumed, bytes.len());
    assert_eq!(restored.record_type, RecordType::PmtUpdate);
    assert_eq!(restored.payload, record.payload);
}

#[test]
fn test_record_types() {
    let records = vec![
        WalRecord::pmt_update(1, 0, 100),
        WalRecord::page_alloc(2, 1),
        WalRecord::page_dealloc(3),
        WalRecord::txn_commit(100),
        WalRecord::txn_abort(101),
        WalRecord::commit(CommitRecord {
            commit_id: crate::storage::format::CommitId::new(1),
            generation_id: crate::storage::format::GenerationId::new(1),
            root_page_id: 0,
            mutation_count: 2,
            digest: 3,
        }),
    ];

    for record in records {
        let bytes = record.to_bytes();
        let (restored, _) = WalRecord::from_bytes(&bytes).unwrap();
        assert_eq!(restored.record_type, record.record_type);
    }
}

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
fn test_crc_validation() {
    let record = WalRecord::pmt_update(1, 0, 4096);
    let mut bytes = record.to_bytes();

    let len = bytes.len();
    bytes[len - 1] ^= 0xFF;

    assert!(WalRecord::from_bytes(&bytes).is_none());
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

#[test]
fn test_parse_status_distinguishes_torn_and_corrupt_suffixes() {
    let record = WalRecord::put(b"key", b"value");
    let bytes = record.to_bytes();
    assert_eq!(
        WalManager::parse_records_with_status(&bytes).1,
        ParseStatus::Complete
    );

    let torn = &bytes[..bytes.len() - 1];
    assert_eq!(
        WalManager::parse_records_with_status(torn).1,
        ParseStatus::Incomplete
    );

    let mut corrupt = bytes;
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    assert_eq!(
        WalManager::parse_records_with_status(&corrupt).1,
        ParseStatus::Corrupt
    );

    let mut reserved_tail = record.to_bytes();
    reserved_tail.extend_from_slice(&[0; 4096]);
    let (records, status) = WalManager::parse_records_with_status(&reserved_tail);
    assert_eq!(records.len(), 1);
    assert_eq!(status, ParseStatus::Incomplete);
}

#[test]
fn test_commit_record_roundtrip() {
    let expected = CommitRecord {
        commit_id: crate::storage::format::CommitId::new(8),
        generation_id: crate::storage::format::GenerationId::new(9),
        root_page_id: 10,
        mutation_count: 11,
        digest: 12,
    };
    let record = WalRecord::commit(expected);
    assert_eq!(record.commit_record(), Some(expected));
}

#[test]
fn test_large_mutation_payload_roundtrip() {
    let value = vec![0xA5; 70_000];
    let record = WalRecord::put(b"large-key", &value);
    let (restored, consumed) = WalRecord::from_bytes(&record.to_bytes()).unwrap();

    assert_eq!(consumed, record.to_bytes().len());
    assert_eq!(restored.payload, record.payload);
}
