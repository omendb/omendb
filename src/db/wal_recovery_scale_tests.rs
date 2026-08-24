//! Measures committed-WAL replay throughput to calibrate the checkpoint
//! cadence that bounds reopen cost (tk-x9ez / tk-gy55 prerequisite).

use super::wal_recovery::{digest_records, recover_from_wal};
use crate::recovery::WalRecord;
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
