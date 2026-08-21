//! Reproduction for unbounded data-file growth under churn.

use super::*;
use crate::btree::PAGE_SIZE;
use std::fs;
use tempfile::tempdir;

#[test]
fn data_file_size_plateaus_under_churn() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut options = Options::for_test();
    options.max_wal_bytes = 8 * 1024 * 1024;
    let mut db = DB::create(&path, options.clone()).unwrap();

    let value = vec![0xABu8; 128];
    let key = |index: usize| format!("soak-key-{index:08}").into_bytes();

    // Seed a live set.
    let seed: Vec<BatchMutation> = (0..512)
        .map(|index| BatchMutation::Put {
            key: key(index),
            value: value.clone(),
        })
        .collect();
    db.commit_batch(&seed).unwrap();
    db.flush().unwrap();
    let size_after_seed =
        fs::metadata(path.join("seerdb.data")).unwrap().len();

    // Churn: overwrite the same region repeatedly.
    for round in 0..20 {
        let mutations: Vec<BatchMutation> = (0..16)
            .map(|slot| BatchMutation::Put {
                key: key((round * 16 + slot) % 512),
                value: value.clone(),
            })
            .collect();
        db.commit_batch(&mutations).unwrap();
    }
    let size_after_churn =
        fs::metadata(path.join("seerdb.data")).unwrap().len();

    eprintln!(
        "seed_size={} churn_size={} growth_pages={}",
        size_after_seed,
        size_after_churn,
        (size_after_churn - size_after_seed) / PAGE_SIZE as u64
    );
    assert!(
        size_after_churn <= size_after_seed + PAGE_SIZE as u64 * 4,
        "data file grew {} pages under same-region churn; free-list reuse is not happening",
        (size_after_churn - size_after_seed) / PAGE_SIZE as u64
    );
}

#[test]
fn data_file_size_plateaus_under_scattered_put_delete_soak() {
    // Mirrors examples/retention_soak.rs: golden-ratio scatter over a
    // 4096-key space, even rounds put / odd rounds delete, maintenance
    // every 10 rounds.
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut options = Options::for_test();
    options.max_wal_bytes = 8 * 1024 * 1024;
    let mut db = DB::create(&path, options.clone()).unwrap();

    let value = vec![0xABu8; 128];
    let key = |index: usize| format!("soak-key-{index:08}").into_bytes();
    let scatter = |round: usize, slot: usize| {
        ((round as u64 * 16 + slot as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            % 512) as usize
    };

    let mut sizes = Vec::new();
    for round in 0..160 {
        let mutations: Vec<BatchMutation> = (0..16)
            .map(|slot| {
                let index = scatter(round, slot);
                if round % 2 == 0 {
                    BatchMutation::Put {
                        key: key(index),
                        value: value.clone(),
                    }
                } else {
                    BatchMutation::Delete { key: key(index) }
                }
            })
            .collect();
        db.commit_batch(&mutations).unwrap();
        if (round + 1) % 10 == 0 {
            db.prune_history().unwrap();
            db.vacuum().unwrap();
            db.gc().unwrap();
        }
        if (round + 1) % 20 == 0 {
            let m = db.metrics().unwrap().storage;
            sizes.push(fs::metadata(path.join("seerdb.data")).unwrap().len());
            eprintln!(
                "sample {}: {} KB | page_writes={} reclaimed={} flushes={} | nodes={} next_offset={} free={} pending={} protected={} leased_pmts={}",
                sizes.len(),
                sizes.last().unwrap() / 1024,
                m.physical_page_writes,
                m.reclaimed_pages,
                m.generation_flushes,
                db.engine.reclamation_probe().0,
                db.engine.reclamation_probe().1,
                db.engine.reclamation_probe().2,
                db.engine.reclamation_probe().3,
                db.engine.protection_probe().0,
                db.engine.protection_probe().1,
            );
        }
    }
    // The 512-key space fills within the first ~64 rounds; after that the
    // live set only oscillates and the file must stop growing.
    let mid = sizes[sizes.len() / 2];
    let last = sizes.last().copied().unwrap();
    assert!(
        last <= mid.saturating_mul(3) / 2,
        "data file kept growing after the key space was full: {} -> {} bytes",
        mid,
        last
    );
}
