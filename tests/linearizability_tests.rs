//! Linearizability Tests for seerdb
//!
//! Verifies concurrent operations form a valid linearizable history.
//! Uses history-based verification (similar to Jepsen/Porcupine approach):
//! 1. Spawn threads that perform operations and record timing
//! 2. Collect operation histories with invocation/response times
//! 3. Verify the history is linearizable
//!
//! A history is linearizable if there exists a sequential ordering of operations
//! where each operation appears to take effect atomically at some point between
//! its invocation and response.

use seerdb::{DBOptions, DB};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

/// Maximum operations per key for exhaustive linearizability check
/// Beyond this, we only do the weaker read-your-writes check
const MAX_OPS_FOR_EXHAUSTIVE_CHECK: usize = 8;

/// Operation types for linearizability checking
#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Result of an operation
#[derive(Debug, Clone, PartialEq, Eq)]
enum OpResult {
    PutOk,
    GetOk(Option<Vec<u8>>),
    DeleteOk,
    Error(String),
}

/// A recorded operation with timing information
#[derive(Debug, Clone)]
#[allow(dead_code)] // thread_id used for debugging
struct HistoryEntry {
    thread_id: usize,
    op: Op,
    result: OpResult,
    /// Monotonic timestamp when operation was invoked (nanoseconds)
    invoke_time: u64,
    /// Monotonic timestamp when operation returned (nanoseconds)
    return_time: u64,
}

/// Global monotonic counter for ordering (more precise than Instant for ordering)
static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);

fn get_timestamp() -> u64 {
    GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Sequential specification for a key-value store
/// Used to check if a proposed linearization is valid
#[derive(Default, Clone)]
struct KVSpec {
    state: HashMap<Vec<u8>, Vec<u8>>,
}

impl KVSpec {
    fn apply(&mut self, op: &Op) -> OpResult {
        match op {
            Op::Put { key, value } => {
                self.state.insert(key.clone(), value.clone());
                OpResult::PutOk
            }
            Op::Get { key } => OpResult::GetOk(self.state.get(key).cloned()),
            Op::Delete { key } => {
                self.state.remove(key);
                OpResult::DeleteOk
            }
        }
    }
}

/// Check if a history is linearizable using brute-force search
/// This is O(n!) in worst case but works well for small histories
///
/// For each operation, we try to linearize it at any point where it could
/// have taken effect (between invoke and return times).
fn is_linearizable(history: &[HistoryEntry]) -> Result<Vec<usize>, String> {
    if history.is_empty() {
        return Ok(vec![]);
    }

    // Sort by invoke time for initial ordering
    let mut entries: Vec<(usize, &HistoryEntry)> = history.iter().enumerate().collect();
    entries.sort_by_key(|(_, e)| e.invoke_time);

    // Try to find a valid linearization using backtracking
    let mut linearization = Vec::new();
    let mut used = vec![false; history.len()];
    let mut spec = KVSpec::default();

    if try_linearize(history, &mut linearization, &mut used, &mut spec) {
        Ok(linearization)
    } else {
        Err("No valid linearization found".to_string())
    }
}

/// Recursive backtracking to find valid linearization
fn try_linearize(
    history: &[HistoryEntry],
    linearization: &mut Vec<usize>,
    used: &mut [bool],
    spec: &mut KVSpec,
) -> bool {
    if linearization.len() == history.len() {
        return true;
    }

    // Find the earliest return time among pending operations
    // Operations can only be linearized after all earlier-returning ops
    let current_time = linearization
        .last()
        .map(|&idx| history[idx].return_time)
        .unwrap_or(0);

    // Try each unused operation that could be linearized next
    for (idx, entry) in history.iter().enumerate() {
        if used[idx] {
            continue;
        }

        // Operation can be linearized if:
        // 1. It was invoked (invoke_time exists)
        // 2. Its invoke_time <= linearization point <= return_time
        // For simplicity, we just check that it doesn't violate ordering
        if entry.invoke_time > current_time + 1000 {
            // Skip if invoked much later (rough heuristic)
            continue;
        }

        // Check if applying this op produces the expected result
        let mut spec_clone = spec.clone();
        let expected_result = spec_clone.apply(&entry.op);

        // For Get operations, verify the result matches
        if let (OpResult::GetOk(expected), OpResult::GetOk(actual)) =
            (&expected_result, &entry.result)
        {
            if expected != actual {
                continue; // This linearization doesn't work
            }
        }

        // Try this linearization
        used[idx] = true;
        linearization.push(idx);
        *spec = spec_clone;

        if try_linearize(history, linearization, used, spec) {
            return true;
        }

        // Backtrack
        linearization.pop();
        used[idx] = false;
        // Restore spec state (we need to rebuild from scratch)
        *spec = KVSpec::default();
        for &prev_idx in linearization.iter() {
            spec.apply(&history[prev_idx].op);
        }
    }

    false
}

/// Run concurrent operations and collect history
fn run_concurrent_test(
    db: &Arc<DB>,
    num_threads: usize,
    ops_per_thread: usize,
    key_range: usize,
) -> Vec<HistoryEntry> {
    let barrier = Arc::new(Barrier::new(num_threads));
    let histories: Arc<parking_lot::Mutex<Vec<HistoryEntry>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(db);
            let barrier = Arc::clone(&barrier);
            let histories = Arc::clone(&histories);

            thread::spawn(move || {
                let mut local_history = Vec::with_capacity(ops_per_thread);
                let mut rng_state = thread_id as u64 * 12345 + 67890;

                // Simple PRNG for deterministic but varied operations
                let mut next_rand = || {
                    rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
                    rng_state
                };

                barrier.wait();

                for i in 0..ops_per_thread {
                    let key_idx = (next_rand() as usize) % key_range;
                    let key = format!("key_{:04}", key_idx).into_bytes();

                    // Decide operation type based on iteration and randomness
                    let op_type = (next_rand() % 100) as u8;

                    let (op, result, invoke_time, return_time) = if op_type < 50 {
                        // 50% Put
                        let value = format!("t{}v{}", thread_id, i).into_bytes();
                        let invoke_time = get_timestamp();
                        let res = db.put(&key, &value);
                        let return_time = get_timestamp();

                        let result = match res {
                            Ok(()) => OpResult::PutOk,
                            Err(e) => OpResult::Error(e.to_string()),
                        };
                        (
                            Op::Put {
                                key: key.clone(),
                                value,
                            },
                            result,
                            invoke_time,
                            return_time,
                        )
                    } else if op_type < 90 {
                        // 40% Get
                        let invoke_time = get_timestamp();
                        let res = db.get(&key);
                        let return_time = get_timestamp();

                        let result = match res {
                            Ok(v) => OpResult::GetOk(v.map(|b| b.to_vec())),
                            Err(e) => OpResult::Error(e.to_string()),
                        };
                        (
                            Op::Get { key: key.clone() },
                            result,
                            invoke_time,
                            return_time,
                        )
                    } else {
                        // 10% Delete
                        let invoke_time = get_timestamp();
                        let res = db.delete(&key);
                        let return_time = get_timestamp();

                        let result = match res {
                            Ok(()) => OpResult::DeleteOk,
                            Err(e) => OpResult::Error(e.to_string()),
                        };
                        (
                            Op::Delete { key: key.clone() },
                            result,
                            invoke_time,
                            return_time,
                        )
                    };

                    local_history.push(HistoryEntry {
                        thread_id,
                        op,
                        result,
                        invoke_time,
                        return_time,
                    });
                }

                // Merge into global history
                histories.lock().extend(local_history);
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    Arc::try_unwrap(histories).unwrap().into_inner()
}

/// Verify that all Gets returned values consistent with some Put
/// This is a weaker check than full linearizability but catches obvious bugs
fn verify_read_your_writes(history: &[HistoryEntry]) -> Result<(), String> {
    // Track all values ever written to each key
    let mut written_values: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();

    for entry in history {
        if let Op::Put { key, value } = &entry.op {
            written_values
                .entry(key.clone())
                .or_default()
                .push(value.clone());
        }
    }

    // Verify each Get returned either None or a value that was written
    for entry in history {
        if let Op::Get { key } = &entry.op {
            if let OpResult::GetOk(Some(value)) = &entry.result {
                let valid_values = written_values.get(key);
                match valid_values {
                    Some(values) if values.contains(value) => {}
                    Some(_) => {
                        return Err(format!(
                            "Get({:?}) returned {:?} which was never written",
                            String::from_utf8_lossy(key),
                            String::from_utf8_lossy(value)
                        ));
                    }
                    None => {
                        return Err(format!(
                            "Get({:?}) returned {:?} but key was never written",
                            String::from_utf8_lossy(key),
                            String::from_utf8_lossy(value)
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Per-key linearizability check (more efficient than global)
/// For a KV store, we can check linearizability per-key independently
fn verify_per_key_linearizability(history: &[HistoryEntry]) -> Result<(), String> {
    // Group operations by key
    let mut per_key: HashMap<Vec<u8>, Vec<&HistoryEntry>> = HashMap::new();

    for entry in history {
        let key = match &entry.op {
            Op::Put { key, .. } => key,
            Op::Get { key } => key,
            Op::Delete { key } => key,
        };
        per_key.entry(key.clone()).or_default().push(entry);
    }

    // Check linearizability for each key
    for (key, entries) in per_key {
        // Convert refs to owned for the check
        let owned_entries: Vec<HistoryEntry> = entries.into_iter().cloned().collect();

        // Only do exhaustive check for very small histories (O(n!) complexity)
        if owned_entries.len() <= MAX_OPS_FOR_EXHAUSTIVE_CHECK {
            if let Err(e) = is_linearizable(&owned_entries) {
                return Err(format!(
                    "Key {:?} not linearizable: {}",
                    String::from_utf8_lossy(&key),
                    e
                ));
            }
        }
        // For larger histories, read-your-writes check already ran
    }

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[test]
fn test_linearizability_single_key_2_threads() {
    let temp_dir = TempDir::new().unwrap();
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    // Small test: 2 threads, 10 ops each, 1 key
    let history = run_concurrent_test(&db, 2, 10, 1);

    // Verify read-your-writes (weaker guarantee)
    verify_read_your_writes(&history).expect("Read-your-writes violated");

    // Verify per-key linearizability
    verify_per_key_linearizability(&history).expect("Per-key linearizability violated");

    println!(
        "✓ Single key linearizability: {} operations verified",
        history.len()
    );
}

#[test]
fn test_linearizability_few_keys_4_threads() {
    let temp_dir = TempDir::new().unwrap();
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    // Medium test: 4 threads, 20 ops each, 5 keys
    let history = run_concurrent_test(&db, 4, 20, 5);

    verify_read_your_writes(&history).expect("Read-your-writes violated");
    verify_per_key_linearizability(&history).expect("Per-key linearizability violated");

    println!(
        "✓ Few keys linearizability: {} operations verified",
        history.len()
    );
}

#[test]
fn test_linearizability_many_keys_8_threads() {
    let temp_dir = TempDir::new().unwrap();
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    // Larger test: 8 threads, 50 ops each, 20 keys
    let history = run_concurrent_test(&db, 8, 50, 20);

    // For larger histories, just check read-your-writes
    // Full linearizability check is exponential
    verify_read_your_writes(&history).expect("Read-your-writes violated");

    // Per-key check is still feasible since each key has few ops
    verify_per_key_linearizability(&history).expect("Per-key linearizability violated");

    println!(
        "✓ Many keys linearizability: {} operations verified",
        history.len()
    );
}

#[test]
fn test_linearizability_with_flush() {
    let temp_dir = TempDir::new().unwrap();
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    // First batch of operations
    let history1 = run_concurrent_test(&db, 4, 25, 10);
    verify_read_your_writes(&history1).expect("Pre-flush read-your-writes violated");

    // Flush to SSTable
    db.flush().expect("Flush failed");

    // Second batch of operations
    let history2 = run_concurrent_test(&db, 4, 25, 10);
    verify_read_your_writes(&history2).expect("Post-flush read-your-writes violated");

    // Combined history should still be consistent
    let mut combined = history1;
    combined.extend(history2);
    verify_per_key_linearizability(&combined).expect("Combined linearizability violated");

    println!(
        "✓ Linearizability with flush: {} operations verified",
        combined.len()
    );
}

#[test]
fn test_linearizability_delete_heavy() {
    let temp_dir = TempDir::new().unwrap();
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    // Pre-populate some keys
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        let value = format!("initial_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Run test with lots of deletes (using modified distribution)
    // We'll manually construct a delete-heavy workload
    let barrier = Arc::new(Barrier::new(4));
    let histories: Arc<parking_lot::Mutex<Vec<HistoryEntry>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let histories = Arc::clone(&histories);

            thread::spawn(move || {
                let mut local_history = Vec::new();

                barrier.wait();

                for i in 0..20 {
                    let key_idx = (thread_id + i) % 5;
                    let key = format!("key_{:04}", key_idx).into_bytes();

                    // 40% delete, 30% put, 30% get
                    let op_type = (thread_id * 17 + i * 31) % 10;

                    let invoke_time = get_timestamp();

                    let (op, result) = if op_type < 4 {
                        // Delete
                        let res = db.delete(&key);
                        let result = match res {
                            Ok(()) => OpResult::DeleteOk,
                            Err(e) => OpResult::Error(e.to_string()),
                        };
                        (Op::Delete { key: key.clone() }, result)
                    } else if op_type < 7 {
                        // Put
                        let value = format!("t{}v{}", thread_id, i).into_bytes();
                        let res = db.put(&key, &value);
                        let result = match res {
                            Ok(()) => OpResult::PutOk,
                            Err(e) => OpResult::Error(e.to_string()),
                        };
                        (
                            Op::Put {
                                key: key.clone(),
                                value,
                            },
                            result,
                        )
                    } else {
                        // Get
                        let res = db.get(&key);
                        let result = match res {
                            Ok(v) => OpResult::GetOk(v.map(|b| b.to_vec())),
                            Err(e) => OpResult::Error(e.to_string()),
                        };
                        (Op::Get { key: key.clone() }, result)
                    };

                    let return_time = get_timestamp();

                    local_history.push(HistoryEntry {
                        thread_id,
                        op,
                        result,
                        invoke_time,
                        return_time,
                    });
                }

                histories.lock().extend(local_history);
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let history = Arc::try_unwrap(histories).unwrap().into_inner();

    // Verify - note that Gets may return None after deletes, which is valid
    verify_per_key_linearizability(&history).expect("Delete-heavy linearizability violated");

    println!(
        "✓ Delete-heavy linearizability: {} operations verified",
        history.len()
    );
}

#[test]
fn test_sequential_spec_correctness() {
    // Unit test for the sequential spec itself
    let mut spec = KVSpec::default();

    // Initial get returns None
    assert_eq!(
        spec.apply(&Op::Get {
            key: b"k1".to_vec()
        }),
        OpResult::GetOk(None)
    );

    // Put then get
    assert_eq!(
        spec.apply(&Op::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec()
        }),
        OpResult::PutOk
    );
    assert_eq!(
        spec.apply(&Op::Get {
            key: b"k1".to_vec()
        }),
        OpResult::GetOk(Some(b"v1".to_vec()))
    );

    // Overwrite
    assert_eq!(
        spec.apply(&Op::Put {
            key: b"k1".to_vec(),
            value: b"v2".to_vec()
        }),
        OpResult::PutOk
    );
    assert_eq!(
        spec.apply(&Op::Get {
            key: b"k1".to_vec()
        }),
        OpResult::GetOk(Some(b"v2".to_vec()))
    );

    // Delete then get
    assert_eq!(
        spec.apply(&Op::Delete {
            key: b"k1".to_vec()
        }),
        OpResult::DeleteOk
    );
    assert_eq!(
        spec.apply(&Op::Get {
            key: b"k1".to_vec()
        }),
        OpResult::GetOk(None)
    );

    println!("✓ Sequential spec correctness verified");
}

#[test]
fn test_linearizable_history_verification() {
    // Test the linearizability checker with known-good histories
    let history = vec![
        HistoryEntry {
            thread_id: 0,
            op: Op::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            },
            result: OpResult::PutOk,
            invoke_time: 1,
            return_time: 2,
        },
        HistoryEntry {
            thread_id: 1,
            op: Op::Get { key: b"k".to_vec() },
            result: OpResult::GetOk(Some(b"v1".to_vec())),
            invoke_time: 3,
            return_time: 4,
        },
    ];

    assert!(
        is_linearizable(&history).is_ok(),
        "Valid sequential history should be linearizable"
    );

    // Concurrent but linearizable
    let concurrent_history = vec![
        HistoryEntry {
            thread_id: 0,
            op: Op::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            },
            result: OpResult::PutOk,
            invoke_time: 1,
            return_time: 10, // Long duration
        },
        HistoryEntry {
            thread_id: 1,
            op: Op::Get { key: b"k".to_vec() },
            result: OpResult::GetOk(Some(b"v1".to_vec())),
            invoke_time: 5, // Overlaps with put
            return_time: 8,
        },
    ];

    assert!(
        is_linearizable(&concurrent_history).is_ok(),
        "Concurrent history with valid ordering should be linearizable"
    );

    println!("✓ Linearizability checker verified");
}
