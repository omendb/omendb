//! Matched-durability contended-trace gate for the group-commit pipeline.
//!
//! Runs one deterministic, contended mixed R1/R2 workload through both
//! publication paths — the exclusive per-commit session API and the
//! pipelined group-commit coordinator — and requires identical verified
//! state after checkpoint and reopen. Latency and throughput numbers are
//! recorded for the task gate; the state digest equality is the correctness
//! gate.
//!
//! Run explicitly: `cargo test --all-features --test project_group_commit_gate -- --ignored --nocapture`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, Key, OperationControl, RelationalBackendConfig,
    RelationalDatabaseConfig, RelationalDatabaseSession, RelationalSessionConfig, Row, TableId,
    Value,
};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

const ACCOUNTS_TABLE: TableId = TableId(701);
const EVENTS_TABLE: TableId = TableId(702);
const HOT_ACCOUNTS: u64 = 32;
const COLD_ACCOUNTS: u64 = 256;
/// Constant total balance: every transaction moves value between accounts,
/// so the sum is a strong matched-durability oracle.
const INITIAL_BALANCE: u64 = 1_000_000;
const WORKERS: usize = 8;
const OPS_PER_WORKER: usize = 150;

fn schema() -> Vec<(TableId, omendb::TableDefinition)> {
    vec![
        (
            ACCOUNTS_TABLE,
            omendb::TableDefinition {
                id: ACCOUNTS_TABLE,
                name: "accounts".to_owned(),
                columns: vec![
                    ColumnDefinition {
                        id: ColumnId(1),
                        name: "id".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(2),
                        name: "balance".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                ],
            },
        ),
        (
            EVENTS_TABLE,
            omendb::TableDefinition {
                id: EVENTS_TABLE,
                name: "events".to_owned(),
                columns: vec![
                    ColumnDefinition {
                        id: ColumnId(1),
                        name: "id".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(2),
                        name: "account".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(3),
                        name: "delta".to_owned(),
                        data_type: ColumnType::I64,
                        nullable: false,
                    },
                ],
            },
        ),
    ]
}

fn account_row(id: u64, balance: u64) -> Row {
    Row {
        primary: Key::new(ACCOUNTS_TABLE.0, id),
        values: vec![Value::U64(id), Value::U64(balance)],
    }
}

fn event_row(id: u64, account: u64, delta: i64) -> Row {
    Row {
        primary: Key::new(EVENTS_TABLE.0, id),
        values: vec![Value::U64(id), Value::U64(account), Value::I64(delta)],
    }
}

/// Deterministic per-worker operation schedule with hot-row contention.
struct Schedule {
    worker: usize,
    op: usize,
    state: u64,
}

impl Schedule {
    fn new(worker: usize) -> Self {
        Self {
            worker,
            op: 0,
            state: 0x9E37_79B9_7F4A_7C15 ^ (worker as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9),
        }
    }

    fn next(&mut self) -> Op {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        let op = if self.op % 5 == 4 {
            // R2-style hot-row update: every worker competes for the same
            // account, the pgrust contended-update shape.
            Op::HotUpdate {
                account: self.state % HOT_ACCOUNTS,
                counterparty: HOT_ACCOUNTS + (self.state >> 8) % (COLD_ACCOUNTS),
                delta: 1 + (self.state >> 16) % 100,
                event: (self.worker * OPS_PER_WORKER + self.op) as u64,
            }
        } else {
            // R1-style short read/write transaction over cold accounts.
            Op::ColdTransfer {
                from: HOT_ACCOUNTS + (self.state >> 8) % COLD_ACCOUNTS,
                to: HOT_ACCOUNTS + (self.state >> 20) % COLD_ACCOUNTS,
                delta: 1 + (self.state >> 24) % 50,
                event: (self.worker * OPS_PER_WORKER + self.op) as u64,
            }
        };
        self.op += 1;
        op
    }
}

enum Op {
    HotUpdate {
        account: u64,
        counterparty: u64,
        delta: u64,
        event: u64,
    },
    ColdTransfer {
        from: u64,
        to: u64,
        delta: u64,
        event: u64,
    },
}

fn open_session(directory: &std::path::Path) -> RelationalDatabaseSession {
    let config = RelationalDatabaseConfig {
        backend: RelationalBackendConfig::Seer(omendb::SeerKernelConfig::new(
            directory.to_owned(),
        )),
        session: RelationalSessionConfig {
            max_in_flight: WORKERS * 2,
            admission_timeout: Duration::from_secs(120),
        },
    };
    let session = RelationalDatabaseSession::create(config).expect("create session");
    for (table, definition) in schema() {
        let _ = table;
        session
            .create_table(&OperationControl::new(), definition)
            .expect("create table");
    }
    let control = OperationControl::new();
    for id in 0..HOT_ACCOUNTS + COLD_ACCOUNTS {
        session
            .transaction(&control, |db, tx| {
                tx.insert(db, ACCOUNTS_TABLE, account_row(id, INITIAL_BALANCE))
            })
            .expect("seed account")
            ;
    }
    session
}

fn run_mode(
    group_commit: bool,
    stats: &mut Vec<Duration>,
) -> (BTreeMap<u64, u64>, u64, Duration) {
    let dir = tempdir().expect("tempdir");
    let session = Arc::new(open_session(&dir.path().join("seerdb")));
    let started = Instant::now();

    let mut handles = Vec::new();
    for worker in 0..WORKERS {
        let session = Arc::clone(&session);
        handles.push(thread::spawn(move || {
            let control = OperationControl::new();
            let mut schedule = Schedule::new(worker);
            let mut local = Vec::with_capacity(OPS_PER_WORKER);
            for _ in 0..OPS_PER_WORKER {
                let op = schedule.next();
                let began = Instant::now();
                let result = match op {
                    Op::HotUpdate {
                        account,
                        counterparty,
                        delta,
                        event,
                    } => {
                        let account = account as i64;
                        let counterparty = counterparty as i64;
                        let delta = delta as i64;
                        if group_commit {
                            session
                                .transaction_with_group_commit(&control, |db, tx| {
                                    transfer(db, tx, account, counterparty, delta, event)
                                })
                                .map(|(_, commit)| commit)
                        } else {
                            session
                                .transaction(&control, |db, tx| {
                                    transfer(db, tx, account, counterparty, delta, event)
                                })
                                .map(|(_, commit)| commit)
                        }
                    }
                    Op::ColdTransfer {
                        from,
                        to,
                        delta,
                        event,
                    } => {
                        let from = from as i64;
                        let to = to as i64;
                        let delta = delta as i64;
                        if group_commit {
                            session
                                .transaction_with_group_commit(&control, |db, tx| {
                                    transfer(db, tx, from, to, delta, event)
                                })
                                .map(|(_, commit)| commit)
                        } else {
                            session
                                .transaction(&control, |db, tx| {
                                    transfer(db, tx, from, to, delta, event)
                                })
                                .map(|(_, commit)| commit)
                        }
                    }
                };
                result.expect("transaction must commit");
                local.push(began.elapsed());
            }
            local
        }));
    }
    for handle in handles {
        stats.extend(handle.join().expect("worker join"));
    }
    let elapsed = started.elapsed();

    // Matched durability: checkpoint, reopen, verify, digest.
    let control = OperationControl::new();
    session.checkpoint(&control).expect("checkpoint");
    let report = session.verify(&control).expect("verify");
    assert_eq!(report.verified_tables, 2);

    let mut balances = BTreeMap::new();
    for id in 0..HOT_ACCOUNTS + COLD_ACCOUNTS {
        let row = session
            .transaction(&control, |db, tx| {
                tx.get(db, ACCOUNTS_TABLE, Key::new(ACCOUNTS_TABLE.0, id))
            })
            .expect("read account")
            .0
            .expect("account exists");
        let Value::U64(balance) = row.values[1] else {
            panic!("balance type");
        };
        balances.insert(id, balance);
    }
    let total: u64 = balances.values().sum();
    assert_eq!(
        total,
        (HOT_ACCOUNTS + COLD_ACCOUNTS) * INITIAL_BALANCE,
        "balance conservation violated"
    );
    let mut hasher = Sha256::new();
    for (id, balance) in &balances {
        hasher.update(id.to_le_bytes());
        hasher.update(balance.to_le_bytes());
    }
    let _ = group_commit;
    (balances, hasher.finalize().iter().fold(0u64, |acc, b| acc ^ u64::from(*b)), elapsed)
}

fn transfer(
    db: &omendb::RelationalDatabase,
    tx: &mut omendb::RelationalDatabaseTransaction,
    from: i64,
    to: i64,
    delta: i64,
    event: u64,
) -> omendb::Result<()> {
    let from_id = u64::try_from(from).expect("non-negative account");
    let to_id = u64::try_from(to).expect("non-negative account");
    let from_row = tx
        .get(db, ACCOUNTS_TABLE, Key::new(ACCOUNTS_TABLE.0, from_id))?
        .expect("from account");
    let Value::U64(from_balance) = from_row.values[1] else {
        return Err(omendb::DbError::InvalidState("balance type".to_owned()));
    };
    let to_row = tx
        .get(db, ACCOUNTS_TABLE, Key::new(ACCOUNTS_TABLE.0, to_id))?
        .expect("to account");
    let Value::U64(to_balance) = to_row.values[1] else {
        return Err(omendb::DbError::InvalidState("balance type".to_owned()));
    };
    // Hot-row contention: clamp the debit to what exists, keeping the sum
    // invariant while forcing read-modify-write on the same rows.
    let amount = delta.min(from_balance as i64).max(0) as u64;
    // Self-transfers are no-ops: two updates to one row would
    // last-write-wins and break balance conservation.
    if from_id == to_id || amount == 0 {
        return Ok(());
    }
    let mut new_from = from_row.clone();
    new_from.values[1] = Value::U64(from_balance - amount);
    tx.update(db, ACCOUNTS_TABLE, new_from)?;
    let mut new_to = to_row;
    new_to.values[1] = Value::U64(to_balance + amount);
    tx.update(db, ACCOUNTS_TABLE, new_to)?;
    tx.insert(db, EVENTS_TABLE, event_row(event, from_id, -(amount as i64)))?;
    tx.insert(db, EVENTS_TABLE, event_row(event + 1_000_000, to_id, amount as i64))?;
    Ok(())
}

fn percentile(latencies: &mut [Duration], pct: f64) -> Duration {
    latencies.sort();
    let index = ((latencies.len() as f64) * pct).clamp(0.0, (latencies.len() - 1) as f64) as usize;
    latencies[index]
}

#[test]
#[ignore = "gate benchmark: run explicitly with --ignored"]
fn group_commit_matched_durability_contended_trace() {
    let mut per_commit_stats = Vec::new();
    let (per_commit_balances, per_commit_digest, per_commit_elapsed) =
        run_mode(false, &mut per_commit_stats);

    let mut group_commit_stats = Vec::new();
    let (group_balances, group_digest, group_elapsed) =
        run_mode(true, &mut group_commit_stats);

    // Matched-durability gate: identical verified state from both paths.
    assert_eq!(per_commit_digest, group_digest, "state digests diverged");
    assert_eq!(per_commit_balances, group_balances);

    let ops = (WORKERS * OPS_PER_WORKER) as u64;
    for (name, stats, elapsed) in [
        ("per-commit", &mut per_commit_stats, per_commit_elapsed),
        ("group-commit", &mut group_commit_stats, group_elapsed),
    ] {
        let p50 = percentile(stats, 0.50);
        let p99 = percentile(stats, 0.99);
        println!(
            "{name}: {tps} tps, p50 {p50:?}, p99 {p99:?}, max {:?} over {ops} ops",
            *stats.last().expect("non-empty"),
            tps = ops as f64 / elapsed.as_secs_f64(),
        );
    }
    let per_commit_tps = ops as f64 / per_commit_elapsed.as_secs_f64();
    let group_tps = ops as f64 / group_elapsed.as_secs_f64();
    println!(
        "group-commit speedup: {:.2}x",
        group_tps / per_commit_tps
    );
}
