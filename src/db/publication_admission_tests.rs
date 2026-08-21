//! Publication-admission, metadata-delta, and checkpoint recovery tests.

use super::metadata_codec::MetaLogEntry;
use super::*;
use std::fs;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::MetadataExt;
use tempfile::tempdir;

#[test]
fn test_db_meta_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create and populate.
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    // Meta file should exist.
    assert!(DB::metadata_log_path(&path).is_file());
}

#[test]
fn test_db_metrics_attribute_page_work_and_lazy_reads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metrics.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();

        let metrics = db.metrics().unwrap();
        assert_eq!(metrics.storage.physical_page_writes, 1);
        assert_eq!(metrics.storage.page_bytes_written, PAGE_SIZE as u64);
        assert_eq!(metrics.storage.generation_flushes, 1);
        assert_eq!(metrics.storage.syncs, 1);
        assert_eq!(metrics.data_bytes, PAGE_SIZE as u64);
        assert_eq!(metrics.wal_bytes, 0);
        assert!(metrics.publication.wal_bytes_written > 0);
        assert!(metrics.publication.metadata_bytes_written > 0);
        assert_eq!(metrics.publication.blob_bytes_written, 0);
        assert_eq!(metrics.publication.history_bytes_written, 0);
        assert_eq!(metrics.publication.manifest_bytes_written, 0);
    }

    let reopened = DB::open(&path, Options::default()).unwrap();
    let before = reopened.metrics().unwrap();
    assert_eq!(before.storage.logical_page_reads, 0);
    assert_eq!(before.storage.physical_page_reads, 0);
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    let after = reopened.metrics().unwrap();
    assert_eq!(after.storage.logical_page_reads, 2);
    assert_eq!(after.storage.physical_page_reads, 1);
    assert_eq!(after.storage.page_bytes_read, PAGE_SIZE as u64);
    assert_eq!(after.buffer.reads, 1);
}

#[test]
fn test_db_metadata_delta_reopens_and_preserves_parent_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata-delta.db");

    let snapshot_id;
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..200 {
            db.put(
                format!("key-{index:04}").as_bytes(),
                format!("value-{index:04}").as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap();
        let first = db.durability_status();
        snapshot_id = db.retain_commit(first.commit_id).unwrap();
        db.put(b"key-0000", b"updated-value").unwrap();
        db.flush().unwrap();
        assert_eq!(
            db.get(b"key-0000").unwrap(),
            Some(b"updated-value".to_vec())
        );
        assert_eq!(
            db.get_at(snapshot_id, b"key-0000").unwrap(),
            Some(b"value-0000".to_vec())
        );
    }

    let parsed = DB::read_meta_log(&path).unwrap().expect("metadata log");
    // Frame 0 is the generation-0 bootstrap checkpoint written at open.
    let entry = |index: usize| match &parsed.frames[index].entry {
        MetaLogEntry::Publication { entry, .. } => &**entry,
        other => other,
    };
    let first_frame = parsed
        .frames
        .iter()
        .find(|frame| frame.checkpoint_id == 1)
        .expect("generation 1 frame");
    assert!(matches!(
        &first_frame.entry,
        MetaLogEntry::Publication { entry, .. } if matches!(&**entry, MetaLogEntry::Checkpoint(..))
    ));
    let _ = entry;
    let second_frame = parsed
        .frames
        .iter()
        .find(|frame| frame.checkpoint_id == 2)
        .expect("generation 2 frame");
    assert!(matches!(
        &second_frame.entry,
        MetaLogEntry::Publication { entry, .. } if matches!(&**entry, MetaLogEntry::Delta(..))
    ));
    assert!(second_frame.raw.len() < first_frame.raw.len());

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"key-0000").unwrap(),
        Some(b"updated-value".to_vec())
    );
    assert_eq!(
        reopened.get_at(snapshot_id, b"key-0000").unwrap(),
        Some(b"value-0000".to_vec())
    );
    reopened.release_snapshot(snapshot_id).unwrap();
    reopened.prune_history().unwrap();
    let pruned = DB::read_meta_log(&path).unwrap().unwrap();
    let retained_ids: Vec<u64> = pruned.frames.iter().map(|f| f.checkpoint_id).collect();
    assert!(retained_ids.contains(&1));
    assert!(retained_ids.contains(&2));
}

#[test]
fn test_db_metadata_delta_corruption_falls_back_to_resolvable_root() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("corrupt-metadata-delta.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    drop(db);

    let metadata_log = DB::metadata_log_path(&path);
    let mut bytes = fs::read(&metadata_log).unwrap();
    // Locate generation 2's frame payload and corrupt one payload byte so
    // the frame checksum no longer matches.
    let mut cursor = 12usize;
    let mut target = None;
    while cursor + 16 <= bytes.len() {
        let payload_len =
            u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        let frame_id = u64::from_le_bytes(bytes[cursor + 8..cursor + 16].try_into().unwrap());
        let end = cursor + 16 + payload_len;
        if frame_id == 2 {
            target = Some(cursor + 16 + payload_len / 2);
            break;
        }
        cursor = end;
    }
    let corrupt_at = target.expect("generation 2 frame");
    bytes[corrupt_at] ^= 0xA5;
    fs::write(&metadata_log, &bytes).unwrap();
    // The corrupt newest frame no longer hides the database: authority
    // selection falls back to the previous resolvable publication frame.
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    drop(reopened);

    // Rewrite generation 2's delta with a zeroed parent while fixing both
    // the payload checksum and the frame checksum, exercising the bounded
    // anchorless-delta decode refusal.
    let mut cursor = 12usize;
    let mut frame_range = None;
    while cursor + 16 <= bytes.len() {
        let payload_len =
            u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        let frame_id = u64::from_le_bytes(bytes[cursor + 8..cursor + 16].try_into().unwrap());
        let end = cursor + 16 + payload_len;
        if frame_id == 2 {
            frame_range = Some((cursor, end));
            break;
        }
        cursor = end;
    }
    let (frame_start, frame_end) = frame_range.expect("generation 2 frame");
    let mut anchorless = bytes[frame_start + 16..frame_end].to_vec();
    anchorless[12..20].fill(0);
    let checksum = crc32c::crc32c(&anchorless[..anchorless.len() - 4]);
    let checksum_offset = anchorless.len() - 4;
    anchorless[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());

    let mut frame = Vec::new();
    frame.extend_from_slice(&u32::try_from(anchorless.len()).unwrap().to_le_bytes());
    let mut checksum_input = Vec::with_capacity(8 + anchorless.len());
    checksum_input.extend_from_slice(&bytes[frame_start + 8..frame_start + 16]);
    checksum_input.extend_from_slice(&anchorless);
    frame.extend_from_slice(&crc32c::crc32c(&checksum_input).to_le_bytes());
    frame.extend_from_slice(&bytes[frame_start + 8..frame_start + 16]);
    frame.extend_from_slice(&anchorless);
    let mut rewritten = bytes[..frame_start].to_vec();
    rewritten.extend_from_slice(&frame);
    fs::write(&metadata_log, &rewritten).unwrap();
    // The anchorless delta is refused as an authority candidate; selection
    // falls back to the previous resolvable publication frame.
    let mut reopened = match DB::open(&path, Options::default()) {
        Ok(db) => db,
        Err(error) => panic!("anchorless delta should fall back, not fail: {error:?}"),
    };
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    reopened.verify().unwrap();
    reopened.close().unwrap();
}

#[test]
fn test_db_metadata_delta_chain_consolidates_at_hard_limit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata-delta-chain.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-0").unwrap();
    db.flush().unwrap();

    for revision in 1..=MAX_META_DELTA_CHAIN + 1 {
        db.put(b"key", format!("value-{revision}").as_bytes())
            .unwrap();
        db.flush().unwrap();
    }
    let consolidation_generation = MAX_META_DELTA_CHAIN as u64 + 2;
    let parsed = DB::read_meta_log(&path).unwrap().unwrap();
    let consolidated = parsed
        .frames
        .iter()
        .find(|frame| frame.checkpoint_id == consolidation_generation)
        .expect("consolidation frame");
    assert!(matches!(
        &consolidated.entry,
        MetaLogEntry::Publication { entry, .. } if matches!(&**entry, MetaLogEntry::Checkpoint(..))
    ));
    // The inactive slot still names the immediately previous delta
    // frontier. Publish one more generation so that fallback advances
    // beyond the consolidation before pruning the old chain.
    db.put(b"key", b"value-66").unwrap();
    db.flush().unwrap();
    let report = db.prune_history().unwrap();
    // +1 consolidation boundary, +1 the generation-0 bootstrap frame.
    assert_eq!(report.removed_checkpoints, MAX_META_DELTA_CHAIN as u64 + 2);
    let pruned = DB::read_meta_log(&path).unwrap().unwrap();
    assert!(
        pruned
            .frames
            .iter()
            .any(|frame| frame.checkpoint_id == consolidation_generation)
    );
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-66".to_vec()));
    db.close().unwrap();
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-66".to_vec()));
}

#[test]
fn test_compaction_after_metadata_delta_admits_relocation_sidecar() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata-delta-compaction.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    let parsed = DB::read_meta_log(&path).unwrap().unwrap();
    assert!(matches!(
        &parsed.frames[2].entry,
        MetaLogEntry::Publication { entry, .. } if matches!(&**entry, MetaLogEntry::Delta(..))
    ));

    let report = db.compact().unwrap();
    assert_eq!(report.relocated_pages, 1);
    let parsed = DB::read_meta_log(&path).unwrap().unwrap();
    assert!(matches!(
        &parsed.frames[3].entry,
        MetaLogEntry::Publication { entry, .. } if matches!(&**entry, MetaLogEntry::Delta(..))
    ));
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    reopened.verify().unwrap();
}

#[test]
fn test_compaction_final_write_disk_full_reopens_old_root() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compaction-final-disk-full.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();

    db.inject_final_write_disk_full();
    assert!(matches!(db.compact(), Err(Error::DiskFull)));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    reopened.verify().unwrap();
}

#[test]
fn test_db_wal_admission_rejects_before_blob_or_tree_mutation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-admission.db");
    let value = vec![0xA5; 2_000];
    let record_bytes = WalRecord::put(b"key", &value).to_bytes().len() as u64;
    let exact_budget = record_bytes + WAL_COMMIT_RECORD_BYTES;
    let mut options = Options::for_test();
    options.max_wal_bytes = exact_budget - 1;

    let mut db = DB::open(&path, options).unwrap();
    let error = db.put(b"key", &value).unwrap_err();
    assert!(matches!(
        error,
        Error::Backpressure { required, available }
            if required == exact_budget && available == exact_budget - 1
    ));
    assert_eq!(db.get(b"key").unwrap(), None);
    assert_eq!(db.blob_stats().total_valid, 0);
    assert!(!path.join(WAL_FILE).exists());
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.metrics().unwrap().wal_admission_failures, 1);

    db.options.max_wal_bytes = exact_budget;
    db.put(b"key", &value).unwrap();
    assert!(path.join(WAL_FILE).is_file());
    assert!(!path.join(WAL_RESERVATION_FILE).exists());
    assert!(fs::metadata(path.join(WAL_FILE)).unwrap().len() < WAL_RESERVATION_SEGMENT_BYTES);
    assert!(path.join(BLOB_RESERVATION_FILE).is_file());
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert!(
        fs::metadata(path.join(BLOB_RESERVATION_FILE))
            .unwrap()
            .blocks()
            > 0,
        "blob reservation should own physical blocks on this platform"
    );
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert!(
        fs::metadata(path.join(WAL_FILE)).unwrap().blocks() > 0,
        "WAL file should own physical blocks on this platform"
    );
    assert_eq!(
        db.metrics().unwrap().wal_reserved_bytes,
        WAL_RESERVATION_SEGMENT_BYTES
    );
    db.flush().unwrap();
    assert!(!path.join(BLOB_RESERVATION_FILE).exists());
    assert!(!path.join(WAL_FILE).exists());
    assert_eq!(db.metrics().unwrap().wal_reserved_bytes, 0);
    drop(db);

    let reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(value));
}

#[test]
fn test_db_removes_legacy_wal_reservation_sidecar_on_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy-wal-reservation.db");
    let db = DB::open(&path, Options::default()).unwrap();
    drop(db);
    fs::write(path.join(WAL_RESERVATION_FILE), [0xA5; 4096]).unwrap();

    let db = DB::open(&path, Options::default()).unwrap();
    assert!(!path.join(WAL_RESERVATION_FILE).exists());
    assert_eq!(db.metrics().unwrap().wal_reserved_bytes, 0);
}

#[test]
fn test_db_blob_admission_rejects_before_blob_or_tree_mutation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("blob-admission.db");
    let value = vec![0x5A; 2_000];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.inject_capacity_limit(0);

    assert!(matches!(db.put(b"key", &value), Err(Error::DiskFull)));
    assert_eq!(db.get(b"key").unwrap(), None);
    assert_eq!(db.blob_stats().total_valid, 0);
    assert_eq!(db.durability_status().pending_mutations, 0);
    assert!(!db.durability_status().write_fenced);

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}
