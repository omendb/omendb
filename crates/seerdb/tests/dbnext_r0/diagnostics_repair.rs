//! DBNext R0 verification, offline-check, and repair tests.

use super::*;

#[test]
fn dbnext_r0_verify_rejects_dangling_blob_pointer() {
    let root = tempdir().unwrap();
    let path = root.path().join("dangling-blob.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"large", &vec![0x7Fu8; 2_048]).unwrap();
        db.flush().unwrap();
    }
    fs::remove_file(path.join("seerdb.blob")).unwrap();

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert!(matches!(
        reopened.verify(),
        Err(Error::Corruption(message)) if message.contains("blob pointer target")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check { kind: CheckFailureKind::Blob, message })
            if message.contains("blob pointer target")
    ));
}

#[test]
fn dbnext_r0_offline_check_is_read_only_and_reports_wal_state() {
    let root = tempdir().unwrap();
    let path = root.path().join("offline-check.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"pending", b"value").unwrap();

    // The writer remains open. A real offline check must not contend on the
    // writer lock or reconcile the WAL in the source directory.
    let pending = DB::check(&path, Options::default()).unwrap();
    assert_eq!(pending.wal_status, WalCheckStatus::Pending);
    assert!(path.join("seerdb.wal").is_file());
    assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));

    db.flush().unwrap();
    let clean = DB::check(&path, Options::default()).unwrap();
    assert_eq!(clean.wal_status, WalCheckStatus::Clean);
    assert_eq!(
        clean.verification.wal_bytes,
        fs::metadata(path.join("seerdb.wal")).unwrap().len()
    );
}

#[test]
fn dbnext_r0_repair_discards_uncommitted_wal_without_mutating_source() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("repair-pending-source.db");
    let destination_path = root.path().join("repair-pending-destination.db");
    let mut db = DB::open(&source_path, Options::default()).unwrap();
    db.put(b"stable", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"pending", b"uncommitted").unwrap();
    assert!(matches!(
        DB::repair(&source_path, &destination_path, Options::default()),
        Err(Error::DatabaseBusy)
    ));
    drop(db);

    let source_check = DB::check(&source_path, Options::default()).unwrap();
    assert_eq!(source_check.wal_status, WalCheckStatus::Pending);
    let report = DB::repair(&source_path, &destination_path, Options::default()).unwrap();
    assert_eq!(report.action, RepairAction::DiscardedUncommittedWal);
    assert_eq!(report.source_wal_status, WalCheckStatus::Pending);
    assert!(source_path.join("seerdb.wal").is_file());
    assert_eq!(
        DB::check(&source_path, Options::default())
            .unwrap()
            .wal_status,
        WalCheckStatus::Pending
    );

    let repaired = DB::open(&destination_path, Options::default()).unwrap();
    assert_eq!(repaired.get(b"stable").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(repaired.get(b"pending").unwrap(), None);
    assert_ne!(
        repaired.durability_status().history_id,
        report.source.history_id
    );
}

#[test]
fn dbnext_r0_repair_replays_committed_wal_into_new_location() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("repair-committed-source.db");
    let destination_path = root.path().join("repair-committed-destination.db");
    {
        let mut db = DB::open(&source_path, Options::default()).unwrap();
        db.put(b"stable", b"value-1").unwrap();
        db.flush().unwrap();
    }

    let current = active_manifest(&source_path);
    let mutation = WalRecord::put(b"replayed", b"value-2");
    let mut wal = fs::read(source_path.join("seerdb.wal")).unwrap();
    let wal_end = wal
        .len()
        .saturating_add(mutation.to_bytes().len())
        .saturating_add((4 + 1 + CommitRecord::SERIALIZED_SIZE + 4) as usize);
    let commit = CommitRecord {
        commit_id: CommitId::new(current.commit_id.get() + 1),
        commit_seq: CommitSeq::new(current.commit_seq.get() + 1),
        lsn: Lsn::from_wal_position(current.wal_segment, wal_end as u64).unwrap(),
        generation_id: GenerationId::new(current.generation_id.get() + 1),
        root_page_id: current.root_page_id,
        mutation_count: 1,
        digest: wal_digest(&mutation),
    };
    wal.extend_from_slice(&mutation.to_bytes());
    wal.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::write(source_path.join("seerdb.wal"), wal).unwrap();

    let source_check = DB::check(&source_path, Options::default()).unwrap();
    assert_eq!(source_check.wal_status, WalCheckStatus::NeedsRecovery);
    let report = DB::repair(&source_path, &destination_path, Options::default()).unwrap();
    assert_eq!(report.action, RepairAction::ReconciledCommittedWal);
    assert_eq!(report.source_wal_status, WalCheckStatus::NeedsRecovery);
    assert!(source_path.join("seerdb.wal").is_file());

    let repaired = DB::open(&destination_path, Options::default()).unwrap();
    assert_eq!(repaired.get(b"stable").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(
        repaired.get(b"replayed").unwrap(),
        Some(b"value-2".to_vec())
    );
    // The replayed WAL publishes its authority frame (source generation + 1)
    // and the forked destination history publishes one more, so the
    // destination lands exactly two generations past the source root.
    assert_eq!(
        repaired.durability_status().generation_id.get(),
        current.generation_id.get() + 2
    );
    assert_ne!(
        repaired.durability_status().history_id,
        report.source.history_id
    );
}

#[test]
fn dbnext_r0_repair_reconciles_torn_wal_in_destination_only() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("repair-torn-source.db");
    let destination_path = root.path().join("repair-torn-destination.db");
    {
        let mut db = DB::open(&source_path, Options::default()).unwrap();
        db.put(b"stable", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"pending", b"uncommitted").unwrap();
    }
    let wal_path = source_path.join("seerdb.wal");
    let before = fs::read(&wal_path).unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(&[0xA5, 0x5A])
        .unwrap();

    let source_check = DB::check(&source_path, Options::default()).unwrap();
    assert_eq!(source_check.wal_status, WalCheckStatus::Incomplete);
    let report = DB::repair(&source_path, &destination_path, Options::default()).unwrap();
    assert_eq!(report.action, RepairAction::ReconciledIncompleteWal);
    assert_eq!(report.source_wal_status, WalCheckStatus::Incomplete);
    assert_ne!(before, fs::read(&wal_path).unwrap());

    let repaired = DB::open(&destination_path, Options::default()).unwrap();
    assert_eq!(repaired.get(b"stable").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(repaired.get(b"pending").unwrap(), None);
}

#[test]
fn dbnext_r0_unrecoverable_repair_refuses_truncated_data() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("repair-truncated-source.db");
    let destination_path = root.path().join("repair-truncated-destination.db");
    {
        let mut db = DB::open(&source_path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let data_path = source_path.join("seerdb.data");
    let original_length = fs::metadata(&data_path).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&data_path)
        .unwrap()
        .set_len(original_length - 1)
        .unwrap();

    let check = DB::check(&source_path, Options::default());
    assert!(
        matches!(
            &check,
            Err(Error::Check {
                kind: CheckFailureKind::DataPage,
                ..
            })
        ),
        "truncated data check returned {check:?}"
    );
    assert!(matches!(
        DB::repair(&source_path, &destination_path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::DataPage,
            ..
        })
    ));
    assert!(!destination_path.exists());
    assert_eq!(fs::metadata(&data_path).unwrap().len(), original_length - 1);
}
