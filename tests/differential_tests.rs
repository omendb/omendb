//! Differential testing: Compare seerdb behavior against RocksDB
//!
//! Runs identical operations against both databases and verifies results match.
//! Catches semantic bugs that single-implementation tests miss.
//!
//! Run with: `cargo test --features baseline-benchmarks differential`

#![cfg(feature = "baseline-benchmarks")]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rocksdb::{Options, DB as RocksDB};
use seerdb::{DBOptions, DB as SeerDB};
use std::collections::BTreeMap;
use tempfile::TempDir;

/// Operation types for differential testing
#[derive(Debug, Clone)]
enum Op {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Generate random operations with a given seed for reproducibility
fn generate_ops(seed: u64, count: usize, key_space: usize) -> Vec<Op> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut ops = Vec::with_capacity(count);

    for _ in 0..count {
        let key_num = rng.gen_range(0..key_space);
        let key = format!("key_{:08}", key_num).into_bytes();

        let op_type: u8 = rng.gen_range(0..10);
        let op = match op_type {
            0..=5 => {
                // 60% puts
                let value_len = rng.gen_range(10..200);
                let value: Vec<u8> = (0..value_len).map(|_| rng.gen()).collect();
                Op::Put { key, value }
            }
            6..=8 => {
                // 30% gets
                Op::Get { key }
            }
            _ => {
                // 10% deletes
                Op::Delete { key }
            }
        };
        ops.push(op);
    }

    ops
}

/// Run operations against seerdb, return final state
fn run_seerdb(ops: &[Op], temp_dir: &TempDir) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        background_compaction: false,
        background_flush: false,
        ..Default::default()
    };
    let db = SeerDB::open(opts).expect("Failed to open seerdb");

    let mut results = BTreeMap::new();

    for op in ops {
        match op {
            Op::Put { key, value } => {
                db.put(key, value).expect("seerdb put failed");
            }
            Op::Get { key } => {
                let result = db.get(key).expect("seerdb get failed");
                results.insert(key.clone(), result.map(|b| b.to_vec()));
            }
            Op::Delete { key } => {
                db.delete(key).expect("seerdb delete failed");
            }
        }
    }

    // Flush to ensure data is persisted
    db.flush().expect("seerdb flush failed");

    results
}

/// Run operations against RocksDB, return final state
fn run_rocksdb(ops: &[Op], temp_dir: &TempDir) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);

    let db = RocksDB::open(&opts, temp_dir.path()).expect("Failed to open RocksDB");

    let mut results = BTreeMap::new();

    for op in ops {
        match op {
            Op::Put { key, value } => {
                db.put(key, value).expect("RocksDB put failed");
            }
            Op::Get { key } => {
                let result = db.get(key).expect("RocksDB get failed");
                results.insert(key.clone(), result);
            }
            Op::Delete { key } => {
                db.delete(key).expect("RocksDB delete failed");
            }
        }
    }

    results
}

/// Compare final state of both databases
fn compare_final_state(seerdb_dir: &TempDir, rocksdb_dir: &TempDir, keys: &[Vec<u8>]) {
    // Reopen seerdb
    let seer_opts = DBOptions {
        data_dir: seerdb_dir.path().to_path_buf(),
        background_compaction: false,
        background_flush: false,
        ..Default::default()
    };
    let seerdb = SeerDB::open(seer_opts).expect("Failed to reopen seerdb");

    // Reopen RocksDB
    let mut rocks_opts = Options::default();
    rocks_opts.create_if_missing(true);
    let rocksdb = RocksDB::open(&rocks_opts, rocksdb_dir.path()).expect("Failed to reopen RocksDB");

    // Compare all keys
    for key in keys {
        let seer_val = seerdb.get(key).expect("seerdb get failed");
        let rocks_val = rocksdb.get(key).expect("RocksDB get failed");

        assert_eq!(
            seer_val.as_ref().map(|b| b.as_ref()),
            rocks_val.as_deref(),
            "Mismatch for key {:?}: seerdb={:?}, rocksdb={:?}",
            String::from_utf8_lossy(key),
            seer_val,
            rocks_val
        );
    }
}

/// Basic differential test with random operations
#[test]
fn test_differential_random_ops() {
    let seed = 12345u64;
    let ops = generate_ops(seed, 1000, 100); // 1000 ops, 100 unique keys

    let seerdb_dir = TempDir::new().unwrap();
    let rocksdb_dir = TempDir::new().unwrap();

    // Run same ops against both
    let seer_results = run_seerdb(&ops, &seerdb_dir);
    let rocks_results = run_rocksdb(&ops, &rocksdb_dir);

    // Compare intermediate get results
    assert_eq!(
        seer_results, rocks_results,
        "Intermediate results differ between seerdb and RocksDB"
    );

    // Compare final state
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key_{:08}", i).into_bytes())
        .collect();
    compare_final_state(&seerdb_dir, &rocksdb_dir, &keys);
}

/// Test with many overwrites to same keys
#[test]
fn test_differential_overwrite_heavy() {
    let seed = 67890u64;
    let mut rng = StdRng::seed_from_u64(seed);

    // Generate ops that heavily overwrite the same keys
    let mut ops = Vec::new();
    for _ in 0..500 {
        let key_num = rng.gen_range(0..10); // Only 10 keys
        let key = format!("key_{:08}", key_num).into_bytes();
        let value_len = rng.gen_range(10..100);
        let value: Vec<u8> = (0..value_len).map(|_| rng.gen()).collect();
        ops.push(Op::Put { key, value });
    }

    let seerdb_dir = TempDir::new().unwrap();
    let rocksdb_dir = TempDir::new().unwrap();

    run_seerdb(&ops, &seerdb_dir);
    run_rocksdb(&ops, &rocksdb_dir);

    let keys: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("key_{:08}", i).into_bytes())
        .collect();
    compare_final_state(&seerdb_dir, &rocksdb_dir, &keys);
}

/// Test with delete-heavy workload
#[test]
fn test_differential_delete_heavy() {
    let seed = 11111u64;
    let mut rng = StdRng::seed_from_u64(seed);

    let mut ops = Vec::new();

    // First, insert all keys
    for i in 0..50 {
        let key = format!("key_{:08}", i).into_bytes();
        let value = format!("value_{}", i).into_bytes();
        ops.push(Op::Put { key, value });
    }

    // Then randomly delete and re-insert
    for _ in 0..200 {
        let key_num = rng.gen_range(0..50);
        let key = format!("key_{:08}", key_num).into_bytes();

        if rng.gen_bool(0.5) {
            ops.push(Op::Delete { key });
        } else {
            let value = format!("updated_{}", rng.gen::<u32>()).into_bytes();
            ops.push(Op::Put { key, value });
        }
    }

    let seerdb_dir = TempDir::new().unwrap();
    let rocksdb_dir = TempDir::new().unwrap();

    run_seerdb(&ops, &seerdb_dir);
    run_rocksdb(&ops, &rocksdb_dir);

    let keys: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("key_{:08}", i).into_bytes())
        .collect();
    compare_final_state(&seerdb_dir, &rocksdb_dir, &keys);
}

/// Test with empty values
#[test]
fn test_differential_empty_values() {
    let ops = vec![
        Op::Put {
            key: b"key1".to_vec(),
            value: vec![],
        },
        Op::Put {
            key: b"key2".to_vec(),
            value: b"nonempty".to_vec(),
        },
        Op::Put {
            key: b"key3".to_vec(),
            value: vec![],
        },
        Op::Get {
            key: b"key1".to_vec(),
        },
        Op::Get {
            key: b"key2".to_vec(),
        },
        Op::Get {
            key: b"key3".to_vec(),
        },
    ];

    let seerdb_dir = TempDir::new().unwrap();
    let rocksdb_dir = TempDir::new().unwrap();

    let seer_results = run_seerdb(&ops, &seerdb_dir);
    let rocks_results = run_rocksdb(&ops, &rocksdb_dir);

    assert_eq!(seer_results, rocks_results);
}

/// Test with binary keys (non-UTF8)
#[test]
fn test_differential_binary_keys() {
    let ops = vec![
        Op::Put {
            key: vec![0x00, 0x01, 0x02],
            value: b"value1".to_vec(),
        },
        Op::Put {
            key: vec![0xFF, 0xFE, 0xFD],
            value: b"value2".to_vec(),
        },
        Op::Put {
            key: vec![0x00, 0x00, 0x00],
            value: b"value3".to_vec(),
        },
        Op::Get {
            key: vec![0x00, 0x01, 0x02],
        },
        Op::Get {
            key: vec![0xFF, 0xFE, 0xFD],
        },
        Op::Get {
            key: vec![0x00, 0x00, 0x00],
        },
        Op::Get {
            key: vec![0x01, 0x02, 0x03],
        }, // Non-existent
    ];

    let seerdb_dir = TempDir::new().unwrap();
    let rocksdb_dir = TempDir::new().unwrap();

    let seer_results = run_seerdb(&ops, &seerdb_dir);
    let rocks_results = run_rocksdb(&ops, &rocksdb_dir);

    assert_eq!(seer_results, rocks_results);
}

/// Test persistence: write, close, reopen, verify
#[test]
fn test_differential_persistence() {
    let seed = 99999u64;
    let ops = generate_ops(seed, 200, 50);

    let seerdb_dir = TempDir::new().unwrap();
    let rocksdb_dir = TempDir::new().unwrap();

    // Phase 1: Write data
    {
        run_seerdb(&ops, &seerdb_dir);
        run_rocksdb(&ops, &rocksdb_dir);
    }

    // Phase 2: Reopen and compare
    let keys: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("key_{:08}", i).into_bytes())
        .collect();
    compare_final_state(&seerdb_dir, &rocksdb_dir, &keys);
}

/// Longer random test for more coverage
#[test]
fn test_differential_extended() {
    let seed = 42424242u64;
    let ops = generate_ops(seed, 5000, 500); // 5000 ops, 500 unique keys

    let seerdb_dir = TempDir::new().unwrap();
    let rocksdb_dir = TempDir::new().unwrap();

    let seer_results = run_seerdb(&ops, &seerdb_dir);
    let rocks_results = run_rocksdb(&ops, &rocksdb_dir);

    assert_eq!(
        seer_results, rocks_results,
        "Extended test: intermediate results differ"
    );

    let keys: Vec<Vec<u8>> = (0..500)
        .map(|i| format!("key_{:08}", i).into_bytes())
        .collect();
    compare_final_state(&seerdb_dir, &rocksdb_dir, &keys);
}
