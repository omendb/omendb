//! Measures committed-WAL replay throughput to calibrate the checkpoint
//! cadence that bounds reopen cost (tk-x9ez / tk-gy55 prerequisite).

use super::wal_recovery::{digest_records, recover_from_wal};
use crate::recovery::{ParseStatus, RecordType, WalManager, WalRecord};
use crate::storage::format::{CommitId, CommitRecord, GenerationId};
use std::time::Instant;

fn synthetic_committed_wal(generations: usize, ops_per_gen: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut commit_id = 1u64;
    for generation in 0..generations as u64 {
        let mut pending = Vec::with_capacity(ops_per_gen);
        for op in 0..ops_per_gen {
            let key = format!("gen{generation}-op{op}");
            pending.push(WalRecord::put(
                key.as_bytes(),
                b"value-128-bytes-padding-padding-padding",
            ));
        }
        let digest = digest_records(&pending.iter().collect::<Vec<_>>());
        for record in &pending {
            out.extend_from_slice(&record.to_bytes());
        }
        let commit = CommitRecord {
            commit_id: CommitId::new(commit_id),
            generation_id: GenerationId::new(generation + 1),
            root_page_id: 1,
            mutation_count: pending.len() as u64,
            digest,
        };
        commit_id += 1;
        out.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    }
    out
}

#[test]
#[ignore]
fn measure_recover_from_wal_scaling() {
    for generations in [250usize, 500, 1000, 2000] {
        let ops = generations * 16;
        let dir = std::env::temp_dir().join(format!(
            "seerdb-replay-scale-{}-{generations}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = dir.join("seerdb.wal");
        std::fs::write(&wal_path, synthetic_committed_wal(generations, 16)).unwrap();

        let mut btree = super::BTree::new();
        let mut blobs = super::BlobManager::new();
        let start = Instant::now();
        let summary = recover_from_wal(&wal_path, None, &mut btree, &mut blobs).unwrap();
        let elapsed = start.elapsed();
        assert!(summary.last_commit.is_some());

        println!(
            "generations={generations} ops={ops} wal_bytes={} replay_us_total={:.0} replay_us_per_op={:.3}",
            std::fs::metadata(&wal_path).unwrap().len(),
            elapsed.as_secs_f64() * 1e6,
            elapsed.as_secs_f64() * 1e6 / ops as f64,
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[test]
#[ignore]
fn measure_replay_cost_breakdown() {
    let generations = 2000;
    let ops_per_gen = 16;
    let ops = generations * ops_per_gen;

    // Parse + CRC only.
    let wal_bytes = synthetic_committed_wal(generations, ops_per_gen);
    let start = Instant::now();
    let (records, status) = WalManager::parse_records_with_status(&wal_bytes);
    assert_eq!(status, ParseStatus::Complete);
    let parse = start.elapsed();

    // Parse + digest verification per commit envelope.
    let start = Instant::now();
    let mut pending: Vec<&WalRecord> = Vec::new();
    let mut digest_ns_total = 0u128;
    for record in &records {
        match record.record_type {
            RecordType::Commit => {
                let d = digest_records(&pending);
                pending.clear();
                let _ = d;
            }
            _ => pending.push(record),
        }
        digest_ns_total = start.elapsed().as_nanos();
    }
    let parse_digest = start.elapsed();
    let _ = digest_ns_total;

    // Full replay (parse + digest + tree apply), from the scaling test.
    let dir = std::env::temp_dir().join(format!("seerdb-replay-breakdown-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("seerdb.wal");
    std::fs::write(&wal_path, &wal_bytes).unwrap();
    let mut btree = super::BTree::new();
    let mut blobs = super::BlobManager::new();
    let start = Instant::now();
    recover_from_wal(&wal_path, None, &mut btree, &mut blobs).unwrap();
    let full = start.elapsed();
    std::fs::remove_dir_all(&dir).unwrap();

    println!(
        "ops={ops} parse_us={:.0} ({:.3}/op) parse+digest_us={:.0} ({:.3}/op) full_replay_us={:.0} ({:.3}/op)",
        parse.as_secs_f64() * 1e6,
        parse.as_secs_f64() * 1e6 / ops as f64,
        parse_digest.as_secs_f64() * 1e6,
        parse_digest.as_secs_f64() * 1e6 / ops as f64,
        full.as_secs_f64() * 1e6,
        full.as_secs_f64() * 1e6 / ops as f64,
    );
}

#[test]
#[ignore]
fn probe_journal_path_wall_time() {
    use super::DB;
    use crate::Options;
    let dir = std::env::temp_dir().join(format!(
        "seerdb-wal-probe-{}-{:?}",
        std::process::id(),
        std::time::Instant::now()
    ));
    let mut db = DB::open(&dir, Options::default()).unwrap();
    let value = vec![0x42u8; 128];
    for i in 0..200 {
        db.put(format!("warm{i}").as_bytes(), &value).unwrap();
    }
    let start = Instant::now();
    for i in 0..10_000 {
        db.put(format!("key{i}").as_bytes(), &value).unwrap();
    }
    let journal = start.elapsed();
    let start = Instant::now();
    db.flush().unwrap();
    let publish = start.elapsed();
    println!(
        "puts=10000 journal_us_per_put={:.3} publish_us_total={:.0}",
        journal.as_secs_f64() * 1e6 / 10_000.0,
        publish.as_secs_f64() * 1e6,
    );
    db.close().unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
