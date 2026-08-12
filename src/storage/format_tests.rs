use super::*;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

fn database_id() -> DatabaseId {
    DatabaseId::new([7; 16])
}

fn manifest(generation_id: u64, commit_id: u64) -> Manifest {
    Manifest {
        database_id: database_id(),
        history_id: HistoryId::new(2),
        generation_id: GenerationId::new(generation_id),
        commit_id: CommitId::new(commit_id),
        page_size: 4096,
        root_page_id: 42,
        pmt_checkpoint_id: PmtCheckpointId::new(9),
        wal_segment: 3,
        wal_offset: 8192,
        mutation_count: 4,
        digest: 0x1234_5678,
        format_version: FORMAT_VERSION,
    }
}

#[test]
fn superblock_roundtrip_and_checksum_validation() {
    let superblock = Superblock::new(database_id(), HistoryId::new(11), 4096).unwrap();
    let bytes = superblock.to_bytes();
    assert_eq!(Superblock::from_bytes(&bytes), Some(superblock));

    let mut corrupt = bytes;
    corrupt[32] ^= 1;
    assert_eq!(Superblock::from_bytes(&corrupt), None);
}

#[test]
fn commit_record_roundtrip() {
    let commit = CommitRecord {
        commit_id: CommitId::new(8),
        generation_id: GenerationId::new(9),
        root_page_id: 10,
        mutation_count: 11,
        digest: 12,
    };
    assert_eq!(CommitRecord::from_bytes(&commit.to_bytes()), Some(commit));
}

#[test]
fn manifest_roundtrip_and_checksum_validation() {
    let expected = manifest(1, 7);
    let bytes = expected.to_bytes();
    assert_eq!(Manifest::from_bytes(&bytes), Ok(Some(expected)));

    let mut corrupt = bytes;
    corrupt[88] ^= 1;
    assert!(Manifest::from_bytes(&corrupt).is_err());
}

#[test]
fn manifest_history_validates_frames_and_ignores_partial_tail() {
    let first = manifest(1, 1);
    let second = manifest(2, 2);
    let mut history = ManifestHistory::new();
    history.push(first).unwrap();
    history.push(second).unwrap();
    let bytes = history.to_bytes().unwrap();
    assert_eq!(ManifestHistory::from_bytes(&bytes).unwrap(), history);

    let mut partial = bytes.clone();
    partial.extend_from_slice(&[0xA5; 17]);
    assert_eq!(ManifestHistory::from_bytes(&partial).unwrap(), history);

    let mut corrupt = bytes;
    corrupt[ManifestHistory::header_bytes().len()] ^= 1;
    assert_eq!(
        ManifestHistory::from_bytes(&corrupt),
        Err("manifest history checksum mismatch")
    );
}

#[test]
fn reuse_ledger_roundtrips_and_rejects_corruption() {
    let mut ledger = ReuseLedger::new();
    ledger
        .push(ReuseAttempt {
            commit_id: CommitId::new(3),
            generation_id: GenerationId::new(3),
            offsets: vec![0, 4096],
        })
        .unwrap();
    ledger
        .push(ReuseAttempt {
            commit_id: CommitId::new(4),
            generation_id: GenerationId::new(4),
            offsets: Vec::new(),
        })
        .unwrap();
    let bytes = ledger.to_bytes().unwrap();
    assert_eq!(ReuseLedger::from_bytes(&bytes).unwrap(), ledger);

    let mut corrupt = bytes;
    corrupt[24] ^= 1;
    assert_eq!(
        ReuseLedger::from_bytes(&corrupt),
        Err("reuse ledger checksum mismatch")
    );
}

#[test]
fn reuse_ledger_prunes_published_attempts_only() {
    let mut ledger = ReuseLedger::new();
    ledger
        .push(ReuseAttempt {
            commit_id: CommitId::new(2),
            generation_id: GenerationId::new(2),
            offsets: vec![0],
        })
        .unwrap();
    ledger
        .push(ReuseAttempt {
            commit_id: CommitId::new(4),
            generation_id: GenerationId::new(4),
            offsets: vec![4096],
        })
        .unwrap();
    let mut history = ManifestHistory::new();
    history.push(manifest(2, 2)).unwrap();
    assert_eq!(ledger.prune_published(&history), 1);
    assert_eq!(ledger.attempts()[0].generation_id, GenerationId::new(4));
}

#[test]
fn reuse_ledger_prunes_superseded_empty_reservations() {
    let mut ledger = ReuseLedger::new();
    ledger
        .push(ReuseAttempt {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            offsets: Vec::new(),
        })
        .unwrap();
    let mut history = ManifestHistory::new();
    history.push(manifest(2, 2)).unwrap();
    assert_eq!(ledger.prune_published(&history), 1);
    assert!(ledger.attempts().is_empty());
}

#[test]
fn manifest_store_publishes_and_selects_newest_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("MANIFEST");
    let mut store = ManifestStore::open(&path).unwrap();
    assert_eq!(store.load_latest().unwrap(), None);

    store.publish(manifest(1, 1)).unwrap();
    store.publish(manifest(2, 2)).unwrap();
    assert_eq!(store.load_latest().unwrap(), Some(manifest(2, 2)));
}

#[cfg(feature = "fault-injection")]
#[test]
fn manifest_sync_faults_distinguish_candidate_and_safety_mirror() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("MANIFEST");
    let mut store = ManifestStore::open(&path).unwrap();
    let first = manifest(1, 1);
    let second = manifest(2, 2);

    store.publish(first).unwrap();
    store.inject_sync_failure();
    store.publish_mirrored(first).unwrap();
    assert!(store.publish(second).is_err());

    store.inject_mirror_sync_failure();
    assert!(store.publish_mirrored(first).is_err());
    store.publish(second).unwrap();
}

#[test]
fn manifest_store_falls_back_after_torn_inactive_slot() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("MANIFEST");
    let mut store = ManifestStore::open(&path).unwrap();
    store.publish(manifest(1, 1)).unwrap();
    store.publish(manifest(2, 2)).unwrap();
    drop(store);

    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
        .unwrap();
    file.write_all(&[0xA5; 32]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let mut reopened = ManifestStore::open(&path).unwrap();
    assert_eq!(reopened.load_latest().unwrap(), Some(manifest(1, 1)));
}

#[test]
fn retention_registry_roundtrip_and_exact_validation() {
    let mut registry = RetentionRegistry::new();
    let first = registry.insert(manifest(3, 3)).unwrap();
    let second = registry.insert(manifest(4, 4)).unwrap();
    let bytes = registry.to_bytes().unwrap();
    assert_eq!(RetentionRegistry::from_bytes(&bytes).unwrap(), registry);

    let removed = registry.remove(first).unwrap();
    assert_eq!(removed.snapshot_id, first);
    assert!(registry.remove(first).is_none());
    assert_eq!(registry.roots()[0].snapshot_id, second);

    let mut truncated = bytes.clone();
    truncated.pop();
    assert_eq!(
        RetentionRegistry::from_bytes(&truncated),
        Err("retention registry is truncated")
    );

    let mut torn = bytes;
    torn[24] ^= 1;
    assert_eq!(
        RetentionRegistry::from_bytes(&torn),
        Err("retention registry checksum mismatch")
    );
}

#[test]
fn retention_registry_rejects_duplicate_and_future_ids() {
    let mut registry = RetentionRegistry::new();
    registry.insert(manifest(1, 1)).unwrap();
    registry.insert(manifest(2, 2)).unwrap();
    let mut bytes = registry.to_bytes().unwrap();
    // The second entry ID starts after the first ID and manifest.
    let second_id_offset = 24 + 8 + MANIFEST_SLOT_SIZE;
    bytes[second_id_offset..second_id_offset + 8].copy_from_slice(&1u64.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..bytes.len() - 4]);
    let checksum_offset = bytes.len() - 4;
    bytes[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    assert_eq!(
        RetentionRegistry::from_bytes(&bytes),
        Err("retention registry contains a duplicate or invalid ID")
    );

    let mut future = registry.to_bytes().unwrap();
    future[12..20].copy_from_slice(&1u64.to_le_bytes());
    let checksum = crc32c::crc32c(&future[..future.len() - 4]);
    let checksum_offset = future.len() - 4;
    future[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    assert_eq!(
        RetentionRegistry::from_bytes(&future),
        Err("retention registry next ID is not beyond retained IDs")
    );
}
