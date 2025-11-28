use super::*;
use tempfile::tempdir;

#[test]
fn test_buffered_builder_basic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // Build SSTable with buffered builder
    let mut builder = SSTableBuilder::new_buffered();
    builder
        .add(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    builder
        .add(Bytes::from("key2"), Bytes::from("value2"))
        .unwrap();
    builder
        .add(Bytes::from("key3"), Bytes::from("value3"))
        .unwrap();

    // Finish to file
    builder.finish_to_file(&path).unwrap();

    // Read back with SSTable
    let mut sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.num_entries, 3);
    assert_eq!(sst.get(b"key1").unwrap().unwrap(), Bytes::from("value1"));
    assert_eq!(sst.get(b"key2").unwrap().unwrap(), Bytes::from("value2"));
    assert_eq!(sst.get(b"key3").unwrap().unwrap(), Bytes::from("value3"));
}

#[test]
fn test_buffered_builder_to_bytes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // Build SSTable and get bytes
    let mut builder = SSTableBuilder::new_buffered();
    builder.add(Bytes::from("aaa"), Bytes::from("111")).unwrap();
    builder.add(Bytes::from("bbb"), Bytes::from("222")).unwrap();

    let bytes = builder.finish_to_bytes().unwrap();

    // Write bytes to file manually
    std::fs::write(&path, &bytes).unwrap();

    // Read back
    let mut sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.num_entries, 2);
    assert_eq!(sst.get(b"aaa").unwrap().unwrap(), Bytes::from("111"));
    assert_eq!(sst.get(b"bbb").unwrap().unwrap(), Bytes::from("222"));
}

#[test]
fn test_buffered_builder_empty() {
    let builder = SSTableBuilder::new_buffered();
    assert!(builder.is_empty());
    assert_eq!(builder.num_entries(), 0);

    // Even empty builder should produce valid SSTable bytes
    let bytes = builder.finish_to_bytes().unwrap();
    assert!(bytes.len() > 0);

    // Should contain header (32 bytes) + footer (48 bytes) minimum
    assert!(bytes.len() >= 80);
}

#[test]
fn test_buffered_builder_tombstone() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new_buffered();
    builder
        .add(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    builder.add_tombstone(Bytes::from("key2")).unwrap();
    builder
        .add(Bytes::from("key3"), Bytes::from("value3"))
        .unwrap();

    builder.finish_to_file(&path).unwrap();

    let mut sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.num_entries, 3);
    assert_eq!(sst.get(b"key1").unwrap().unwrap(), Bytes::from("value1"));
    // key2 is a tombstone - should return None
    assert!(sst.get(b"key2").unwrap().is_none());
    assert_eq!(sst.get(b"key3").unwrap().unwrap(), Bytes::from("value3"));
}

#[test]
fn test_buffered_builder_max_sequence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new_buffered().with_max_sequence(12345);
    builder
        .add(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();

    builder.finish_to_file(&path).unwrap();

    let sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.max_sequence(), 12345);
}

#[test]
fn test_buffered_builder_many_entries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new_buffered();

    // Add 1000 entries to force multiple data blocks
    for i in 0..1000 {
        let key = format!("key{:06}", i);
        let value = format!("value{:06}", i);
        builder.add(Bytes::from(key), Bytes::from(value)).unwrap();
    }

    assert_eq!(builder.num_entries(), 1000);

    builder.finish_to_file(&path).unwrap();

    let mut sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.num_entries, 1000);

    // Spot check
    assert_eq!(
        sst.get(b"key000000").unwrap().unwrap(),
        Bytes::from("value000000")
    );
    assert_eq!(
        sst.get(b"key000500").unwrap().unwrap(),
        Bytes::from("value000500")
    );
    assert_eq!(
        sst.get(b"key000999").unwrap().unwrap(),
        Bytes::from("value000999")
    );
}

#[test]
fn test_buffered_builder_checksum_valid() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new_buffered();
    builder
        .add(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();

    let bytes = builder.finish_to_bytes().unwrap();
    std::fs::write(&path, &bytes).unwrap();

    // Opening should validate checksum
    let sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.num_entries, 1);
}

#[test]
fn test_buffered_vs_file_builder_equivalence() {
    let dir = tempdir().unwrap();
    let path_buffered = dir.path().join("buffered.sst");
    let path_file = dir.path().join("file.sst");

    // Build with buffered builder
    let mut buffered = SSTableBuilder::new_buffered().with_max_sequence(100);
    buffered
        .add(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    buffered
        .add(Bytes::from("key2"), Bytes::from("value2"))
        .unwrap();
    buffered.finish_to_file(&path_buffered).unwrap();

    // Build with file-based builder
    let mut file_builder = SSTableBuilder::create(&path_file)
        .unwrap()
        .with_max_sequence(100);
    file_builder
        .add(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    file_builder
        .add(Bytes::from("key2"), Bytes::from("value2"))
        .unwrap();
    file_builder.finish().unwrap();

    // Both should be readable and contain same data
    let mut sst_buffered = SSTable::open(&path_buffered).unwrap();
    let mut sst_file = SSTable::open(&path_file).unwrap();

    assert_eq!(sst_buffered.num_entries, sst_file.num_entries);
    assert_eq!(sst_buffered.max_sequence(), sst_file.max_sequence());
    assert_eq!(
        sst_buffered.get(b"key1").unwrap(),
        sst_file.get(b"key1").unwrap()
    );
    assert_eq!(
        sst_buffered.get(b"key2").unwrap(),
        sst_file.get(b"key2").unwrap()
    );
}

#[test]
fn test_sstable_iter_rev() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("rev.sst");

    let mut builder = SSTableBuilder::new_buffered();
    builder
        .add(Bytes::from("key1"), Bytes::from("val1"))
        .unwrap();
    builder
        .add(Bytes::from("key2"), Bytes::from("val2"))
        .unwrap();
    builder
        .add(Bytes::from("key3"), Bytes::from("val3"))
        .unwrap();

    builder.finish_to_file(&path).unwrap();

    let mut sst = SSTable::open(&path).unwrap();
    let entries: Vec<_> = sst.iter_rev().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].0, Bytes::from("key3"));
    assert_eq!(entries[1].0, Bytes::from("key2"));
    assert_eq!(entries[2].0, Bytes::from("key1"));
}

#[test]
fn test_buffered_builder_large_value() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new_buffered();
    let large_value = vec![0xABu8; 100_000]; // 100KB value

    builder
        .add(Bytes::from("large"), Bytes::from(large_value.clone()))
        .unwrap();

    builder.finish_to_file(&path).unwrap();

    let mut sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.get(b"large").unwrap().unwrap().as_ref(), &large_value);
}

// ========================================================================
// MVCC Tests
// ========================================================================

#[test]
fn test_mvcc_add_internal_basic() {
    use crate::types::{InternalKey, ValueType};

    let dir = tempdir().unwrap();
    let path = dir.path().join("mvcc.sst");

    let mut builder = SSTableBuilder::new_buffered();

    // Add entries with different sequence numbers for same key
    // Higher seq = newer version, but sorted FIRST due to encoding
    let ikey1 = InternalKey::new(Bytes::from("key1"), 100, ValueType::Value);
    let ikey2 = InternalKey::new(Bytes::from("key1"), 200, ValueType::Value);
    let ikey3 = InternalKey::new(Bytes::from("key2"), 100, ValueType::Value);

    // Add in sorted order (required for SSTable builder)
    // key1@200 < key1@100 < key2@100 (user_key ASC, seq DESC)
    builder.add_internal(&ikey2, Bytes::from("v1_200")).unwrap();
    builder.add_internal(&ikey1, Bytes::from("v1_100")).unwrap();
    builder.add_internal(&ikey3, Bytes::from("v2_100")).unwrap();

    builder.finish_to_file(&path).unwrap();

    let mut sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.num_entries, 3);
    assert_eq!(sst.max_sequence(), 200);

    // MVCC lookup: snapshot_seq=200 should see v1_200
    let result = sst.get_mvcc(b"key1", 200).unwrap();
    assert_eq!(result, Some(Bytes::from("v1_200")));

    // MVCC lookup: snapshot_seq=150 should see v1_100 (newest <= 150)
    let result = sst.get_mvcc(b"key1", 150).unwrap();
    assert_eq!(result, Some(Bytes::from("v1_100")));

    // MVCC lookup: snapshot_seq=50 should see nothing (no version <= 50)
    let result = sst.get_mvcc(b"key1", 50).unwrap();
    assert_eq!(result, None);

    // key2 lookup
    let result = sst.get_mvcc(b"key2", 200).unwrap();
    assert_eq!(result, Some(Bytes::from("v2_100")));
}

#[test]
fn test_mvcc_tombstone() {
    use crate::types::{InternalKey, ValueType};

    let dir = tempdir().unwrap();
    let path = dir.path().join("mvcc_tomb.sst");

    let mut builder = SSTableBuilder::new_buffered();

    // key1: value at seq 100, tombstone at seq 200
    let ikey1_tomb = InternalKey::new(Bytes::from("key1"), 200, ValueType::Deletion);
    let ikey1_val = InternalKey::new(Bytes::from("key1"), 100, ValueType::Value);

    builder.add_internal(&ikey1_tomb, Bytes::new()).unwrap();
    builder
        .add_internal(&ikey1_val, Bytes::from("value"))
        .unwrap();

    builder.finish_to_file(&path).unwrap();

    let mut sst = SSTable::open(&path).unwrap();

    // snapshot_seq=200 should see tombstone (deleted)
    let result = sst.get_mvcc(b"key1", 200).unwrap();
    assert_eq!(result, None);

    // snapshot_seq=150 should see the value (tombstone not visible)
    let result = sst.get_mvcc(b"key1", 150).unwrap();
    assert_eq!(result, Some(Bytes::from("value")));
}

#[test]
fn test_mvcc_many_versions() {
    use crate::types::{InternalKey, ValueType};

    let dir = tempdir().unwrap();
    let path = dir.path().join("mvcc_many.sst");

    let mut builder = SSTableBuilder::new_buffered();

    // Add 100 versions of the same key
    for seq in (1..=100).rev() {
        let ikey = InternalKey::new(Bytes::from("key"), seq * 10, ValueType::Value);
        let value = format!("value_seq{}", seq * 10);
        builder.add_internal(&ikey, Bytes::from(value)).unwrap();
    }

    builder.finish_to_file(&path).unwrap();

    let mut sst = SSTable::open(&path).unwrap();
    assert_eq!(sst.num_entries, 100);

    // Latest version
    let result = sst.get_mvcc(b"key", 1000).unwrap();
    assert_eq!(result, Some(Bytes::from("value_seq1000")));

    // Middle version
    let result = sst.get_mvcc(b"key", 505).unwrap();
    assert_eq!(result, Some(Bytes::from("value_seq500")));

    // Oldest version
    let result = sst.get_mvcc(b"key", 10).unwrap();
    assert_eq!(result, Some(Bytes::from("value_seq10")));

    // Too old
    let result = sst.get_mvcc(b"key", 5).unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_mvcc_bloom_filter_uses_user_key() {
    use crate::types::{InternalKey, ValueType};

    let dir = tempdir().unwrap();
    let path = dir.path().join("mvcc_bloom.sst");

    let mut builder = SSTableBuilder::new_buffered();

    let ikey = InternalKey::new(Bytes::from("testkey"), 100, ValueType::Value);
    builder
        .add_internal(&ikey, Bytes::from("testvalue"))
        .unwrap();

    builder.finish_to_file(&path).unwrap();

    let sst = SSTable::open(&path).unwrap();

    // Bloom filter should be indexed by user_key, not encoded InternalKey
    assert!(sst.may_contain(b"testkey"));

    // A key that doesn't exist should (probably) return false
    // (Bloom filters can have false positives but should not for completely different keys)
    // This tests that the bloom filter was built with user_key
    assert!(!sst.may_contain(b"nonexistent_key_that_is_very_different"));
}
