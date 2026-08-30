//! DBNext R0 blob reclamation, pointer retirement, and publication-fence tests.

use super::*;

#[test]
fn dbnext_r0_blob_reclamation_survives_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-reclamation.db");
    let value = vec![0x6Du8; 2_048];
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"large-a", &value).unwrap();
        db.put(b"large-b", &value).unwrap();
        db.flush().unwrap();
        db.delete(b"large-a").unwrap();
        db.delete(b"large-b").unwrap();
        db.flush().unwrap();
    }

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.blob_stats().total_valid, 0);
    assert_eq!(reopened.blob_stats().total_deleted, 2);
    assert_eq!(reopened.gc().unwrap(), 2);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
    assert_eq!(reopened.verify().unwrap().blob_bytes, 44);
}

#[test]
fn dbnext_r0_gc_rewrites_mixed_blob_files_and_reclaims_old_pointers() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-mixed-reclamation.db");
    let first = vec![0x61u8; 2_048];
    let second = vec![0x62u8; 2_048];
    let third = vec![0x63u8; 2_048];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"large-a", &first).unwrap();
    db.put(b"large-b", &second).unwrap();
    db.put(b"large-c", &third).unwrap();
    db.flush().unwrap();
    db.delete(b"large-a").unwrap();
    db.delete(b"large-b").unwrap();
    db.flush().unwrap();

    let before = db.blob_stats();
    assert_eq!(before.total_valid, 1);
    assert_eq!(before.total_deleted, 2);
    assert_eq!(before.files_needing_gc, 1);

    assert_eq!(db.gc().unwrap(), 3);
    assert_eq!(db.get(b"large-a").unwrap(), None);
    assert_eq!(db.get(b"large-b").unwrap(), None);
    assert_eq!(db.get(b"large-c").unwrap(), Some(third.clone()));
    assert_eq!(db.blob_stats().total_valid, 1);
    assert_eq!(db.blob_stats().total_deleted, 0);
    assert_eq!(db.blob_stats().files_needing_gc, 0);
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"large-a").unwrap(), None);
    assert_eq!(reopened.get(b"large-b").unwrap(), None);
    assert_eq!(reopened.get(b"large-c").unwrap(), Some(third));
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
}

#[test]
fn dbnext_r0_gc_mixed_blob_rewrite_failure_reopens_old_root() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-mixed-reclamation-fault.db");
    let first = vec![0x71u8; 2_048];
    let second = vec![0x72u8; 2_048];
    let third = vec![0x73u8; 2_048];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"large-a", &first).unwrap();
    db.put(b"large-b", &second).unwrap();
    db.put(b"large-c", &third).unwrap();
    db.flush().unwrap();
    db.delete(b"large-a").unwrap();
    db.delete(b"large-b").unwrap();
    db.flush().unwrap();

    db.inject_after_blob_rewrite_image_failure();
    let error = db.gc().unwrap_err();
    assert!(matches!(error, Error::Io(_)));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"large-a").unwrap(), None);
    assert_eq!(reopened.get(b"large-b").unwrap(), None);
    assert_eq!(reopened.get(b"large-c").unwrap(), Some(third));
    assert_eq!(reopened.blob_stats().total_valid, 1);
    assert_eq!(reopened.blob_stats().total_deleted, 2);
    assert_eq!(reopened.gc().unwrap(), 3);
    assert!(!reopened.durability_status().write_fenced);
}

#[test]
fn dbnext_r0_gc_capacity_refusal_preserves_blob_catalog() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-gc-capacity.db");
    let value = vec![0x6Du8; 2_048];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"large", &value).unwrap();
    db.flush().unwrap();
    db.delete(b"large").unwrap();
    db.flush().unwrap();

    let before_bytes = fs::metadata(path.join("seerdb.blob")).unwrap().len();
    let before = db.blob_stats();
    assert_eq!(before.total_valid, 0);
    assert_eq!(before.total_deleted, 1);
    assert_eq!(before.files_needing_gc, 1);

    db.inject_capacity_limit(0);
    assert!(matches!(db.gc(), Err(Error::DiskFull)));
    assert_eq!(
        fs::metadata(path.join("seerdb.blob")).unwrap().len(),
        before_bytes
    );
    let after_failure = db.blob_stats();
    assert_eq!(after_failure.total_valid, before.total_valid);
    assert_eq!(after_failure.total_deleted, before.total_deleted);
    assert_eq!(after_failure.files_needing_gc, before.files_needing_gc);
    assert!(!db.durability_status().write_fenced);

    db.inject_capacity_limit(u64::MAX);
    assert_eq!(db.gc().unwrap(), 1);
    assert_eq!(db.blob_stats().files_needing_gc, 0);
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"large").unwrap(), None);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
}

#[test]
fn dbnext_r0_gc_publication_failure_preserves_previous_blob_image() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-gc-fault.db");
    let value = vec![0x6Du8; 2_048];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"large", &value).unwrap();
    db.flush().unwrap();
    db.delete(b"large").unwrap();
    db.flush().unwrap();

    db.inject_atomic_rename_failure();
    assert!(matches!(db.gc(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    assert!(path.join("seerdb.blob.reserve").is_file());
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert!(!path.join("seerdb.blob.reserve").exists());
    assert_eq!(reopened.get(b"large").unwrap(), None);
    assert_eq!(reopened.blob_stats().total_deleted, 1);
    assert_eq!(reopened.blob_stats().files_needing_gc, 1);
    assert_eq!(reopened.gc().unwrap(), 1);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
}

#[test]
fn dbnext_r0_blob_to_inline_retires_old_value() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-inline-replacement.db");
    let large = vec![0xC3; 2_048];

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", &large).unwrap();
        db.flush().unwrap();
        db.put(b"key", b"inline-value").unwrap();
        db.flush().unwrap();

        let stats = db.blob_stats();
        assert_eq!(stats.total_valid, 0);
        assert_eq!(stats.total_deleted, 1);
        assert_eq!(db.get(b"key").unwrap(), Some(b"inline-value".to_vec()));
        assert_eq!(db.gc().unwrap(), 1);
        let stats = db.blob_stats();
        assert_eq!(stats.files_needing_gc, 0);
        assert_eq!(stats.total_valid, 0);
        assert_eq!(stats.total_deleted, 0);
    }

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap(),
        Some(b"inline-value".to_vec())
    );
    let stats = reopened.blob_stats();
    assert_eq!(stats.files_needing_gc, 0);
    assert_eq!(stats.total_valid, 0);
    assert_eq!(stats.total_deleted, 0);
}

#[test]
fn dbnext_r0_recovery_retires_blob_on_inline_wal_replacement() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-inline-recovery.db");
    let large = vec![0xC3; 2_048];
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", &large).unwrap();
        db.flush().unwrap();
    }

    let current = active_manifest(&path);
    let record = WalRecord::put(b"key", b"inline-after-recovery");
    let mut wal_bytes = fs::read(path.join("seerdb.wal")).unwrap();
    let wal_end = wal_bytes
        .len()
        .saturating_add(record.to_bytes().len())
        .saturating_add(4 + 1 + CommitRecord::SERIALIZED_SIZE + 4);
    let commit = CommitRecord {
        commit_id: CommitId::new(current.commit_id.get() + 1),
        commit_seq: CommitSeq::new(current.commit_seq.get() + 1),
        lsn: Lsn::from_wal_position(current.wal_segment, wal_end as u64).unwrap(),
        generation_id: GenerationId::new(current.generation_id.get() + 1),
        root_page_id: current.root_page_id,
        mutation_count: 1,
        digest: wal_digest(&record),
    };
    wal_bytes.extend_from_slice(&record.to_bytes());
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::write(path.join("seerdb.wal"), wal_bytes).unwrap();

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap(),
        Some(b"inline-after-recovery".to_vec())
    );
    assert_eq!(reopened.blob_stats().total_valid, 0);
    assert_eq!(reopened.blob_stats().total_deleted, 1);
    assert_eq!(reopened.gc().unwrap(), 1);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
}

#[test]
fn dbnext_r0_newer_blob_image_cannot_reclaim_manifest_value() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-publication-fence.db");
    let old_value = vec![0x11u8; 2_048];
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"large", &old_value).unwrap();
        db.flush().unwrap();
    }

    // Simulate a blob image written for a newer generation whose manifest
    // publication did not complete. The old manifest still owns offset 0.
    let mut newer = BlobManager::new();
    let old_pointer = newer.append(b"large", old_value.clone()).unwrap();
    newer.append(b"large", vec![0x22; 2_048]).unwrap();
    assert!(newer.mark_deleted(&old_pointer));
    let mut bytes = newer.to_bytes();
    bytes[20..28].copy_from_slice(&2u64.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..bytes.len() - 4]);
    let checksum_offset = bytes.len() - 4;
    bytes[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    fs::write(path.join("seerdb.blob"), bytes).unwrap();

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"large").unwrap(), Some(old_value));
    assert_eq!(reopened.blob_stats().total_deleted, 0);
    assert_eq!(reopened.gc().unwrap(), 0);
    assert_eq!(reopened.get(b"large").unwrap(), Some(vec![0x11; 2_048]));
}
