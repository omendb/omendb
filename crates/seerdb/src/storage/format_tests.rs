use super::*;
use crate::allocator::PageAllocator;
use crate::db::metadata_codec::{
    encode_checkpoint, encode_meta_log_frame, encode_publication_payload, meta_log_header_bytes,
    parse_meta_log,
};
use crate::error::Error;
use crate::{Options, db::DB};

use crate::mvcc::PMT;
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
fn manifest_store_publishes_and_selects_newest_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("authority.log");
    let mut frames = Vec::new();
    for mut m in [manifest(1, 1), manifest(2, 2)] {
        m.pmt_checkpoint_id = PmtCheckpointId::new(m.generation_id.get());
        let payload = encode_publication_payload(
            &m.to_bytes(),
            &encode_checkpoint(&PMT::new(), &PageAllocator::new()).unwrap(),
        )
        .unwrap();
        frames.extend(encode_meta_log_frame(m.generation_id.get(), &payload).unwrap());
    }
    std::fs::write(
        &path,
        meta_log_header_bytes()
            .into_iter()
            .chain(frames)
            .collect::<Vec<u8>>(),
    )
    .unwrap();
    let parsed = parse_meta_log(&std::fs::read(&path).unwrap()).unwrap();
    let mut expected = manifest(2, 2);
    expected.pmt_checkpoint_id = PmtCheckpointId::new(2);
    assert_eq!(
        DB::select_authority_manifest(&parsed).unwrap(),
        Some(expected)
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn manifest_sync_faults_distinguish_candidate_and_safety_mirror() {
    // The mirror no longer exists; the former mirror seam now targets the
    // metadata-log write boundary. Both public seams must stay injectable.
    let db_path = tempdir().unwrap();
    let mut db = DB::open(db_path.path().join("seams.db"), Options::default()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.inject_manifest_sync_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    drop(db);
    assert!(DB::open(db_path.path().join("seams.db"), Options::default()).is_ok());
}

#[test]
fn manifest_store_falls_back_after_torn_inactive_slot() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("authority.log");
    let mut frames = Vec::new();
    for mut m in [manifest(1, 1), manifest(2, 2)] {
        m.pmt_checkpoint_id = PmtCheckpointId::new(m.generation_id.get());
        let payload = encode_publication_payload(
            &m.to_bytes(),
            &encode_checkpoint(&PMT::new(), &PageAllocator::new()).unwrap(),
        )
        .unwrap();
        frames.extend(encode_meta_log_frame(m.generation_id.get(), &payload).unwrap());
    }
    // Torn newest frame: clobber generation 2's payload in place so the
    // checksum fails and selection falls back to generation 1.
    let mut bytes: Vec<u8> = meta_log_header_bytes().into_iter().chain(frames).collect();
    let tail_start = bytes.len() - 16;
    bytes[tail_start..].copy_from_slice(&[0xA5; 16]);
    std::fs::write(&path, &bytes).unwrap();
    let parsed = parse_meta_log(&std::fs::read(&path).unwrap()).unwrap();
    let mut expected = manifest(1, 1);
    expected.pmt_checkpoint_id = PmtCheckpointId::new(1);
    assert_eq!(
        DB::select_authority_manifest(&parsed).unwrap(),
        Some(expected)
    );
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
