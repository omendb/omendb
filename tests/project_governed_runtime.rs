use std::path::Path;

use omendb::{
    OverloadPolicy,
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, IndexDefinition, IndexId, Key,
    RelationalBackendConfig, RelationalBackendKind, RelationalCompactionBudget, RelationalDatabase,
    Row, SeerKernelConfig, TableDefinition, TableId, Value,
    GovernorConfig, Reactor, ReactorConfig, WorkClass,
};
use tempfile::tempdir;

const INVENTORY_TABLE: TableId = TableId(40);
const ORDERS_TABLE: TableId = TableId(41);
const ORDER_STATUS_INDEX: IndexId = IndexId(40);

fn inventory_table() -> TableDefinition {
    TableDefinition {
        id: INVENTORY_TABLE,
        name: "governed_inventory".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "item_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "quantity".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}
fn orders_table() -> TableDefinition {
    TableDefinition {
        id: ORDERS_TABLE,
        name: "governed_orders".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "order_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "status".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn inventory_row(item_id: u64, quantity: u64) -> Row {
    Row {
        primary: Key::new(INVENTORY_TABLE.0, item_id),
        values: vec![Value::U64(item_id), Value::U64(quantity)],
    }
}

fn order_row(order_id: u64, status: &str) -> Row {
    Row {
        primary: Key::new(ORDERS_TABLE.0, order_id),
        values: vec![Value::U64(order_id), Value::Text(status.to_owned())],
    }
}

fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    }
}

enum DatabaseTask {
    OltpUpdate { item_id: u64, decrement: u64 },
    ScanOrders { status: String },
    ReclaimCompaction { units: usize },
    SchemaAddColumn,
    WalCheckpoint,
}

fn exercise_governed_database_workload(kind: RelationalBackendKind, directory: &Path) {
    let db_config = config(kind, directory);
    let mut database = RelationalDatabase::create(db_config.clone()).expect("create database");

    database.create_table(inventory_table()).expect("create inventory");
    database.create_table(orders_table()).expect("create orders");
    database
        .create_index(IndexDefinition {
            id: ORDER_STATUS_INDEX,
            table: ORDERS_TABLE,
            columns: vec![ColumnId(2)],
            unique: false,
        })
        .expect("create order index");

    // Initial seed
    let initial_items = 20;
    for i in 1..=initial_items {
        database.insert(INVENTORY_TABLE, inventory_row(i, 1000)).expect("seed inventory");
        database.insert(ORDERS_TABLE, order_row(i, "pending")).expect("seed order");
    }

    let workers = 4;
    let mut reactor = Reactor::new(ReactorConfig {
        workers,
        governor: GovernorConfig {
            capacity: 32,
            protected_reserve: 8,
            max_queue_per_class: 32,
            max_in_flight: workers,
            overload_policy: OverloadPolicy::default(),
        },
    demotion_after: None,
    })
    .expect("create reactor");

    let mut task_map = std::collections::BTreeMap::new();
    let mut now: u64 = 0;
    let mut completed_oltp = 0;
    let mut completed_scans = 0;
    let mut completed_reclaim = 0;
    let mut schema_added = false;

    // Schedule mixed batch of tasks
    for i in 1..=30 {
        now += 1;
        // 1. OLTP work (point inventory updates)
        let oltp_work_id = reactor
            .submit(WorkClass::OlTp, 2, Some(now + 20))
            .expect("submit oltp");
        task_map.insert(
            oltp_work_id,
            DatabaseTask::OltpUpdate {
                item_id: (i % initial_items) + 1,
                decrement: 5,
            },
        );

        // 2. Scan work (range/index scans)
        if i % 3 == 0
            && let Ok(scan_id) = reactor.submit(WorkClass::Scan, 4, Some(now + 40))
        {
            task_map.insert(
                scan_id,
                DatabaseTask::ScanOrders {
                    status: "pending".to_owned(),
                },
            );
        }

        // 3. Reclaim compaction
        if i % 6 == 0
            && let Ok(reclaim_id) = reactor.submit(WorkClass::Reclaim, 4, Some(now + 30))
        {
            task_map.insert(
                reclaim_id,
                DatabaseTask::ReclaimCompaction { units: 5 },
            );
        }

        // 4. Schema modification
        if i == 15
            && let Ok(schema_id) = reactor.submit(WorkClass::Schema, 6, Some(now + 50))
        {
            task_map.insert(schema_id, DatabaseTask::SchemaAddColumn);
        }

        // 5. WAL checkpoint
        if i % 10 == 0
            && let Ok(wal_id) = reactor.submit(WorkClass::Wal, 4, Some(now + 30))
        {
            task_map.insert(wal_id, DatabaseTask::WalCheckpoint);
        }

        // Dispatch and execute ready workers
        let dispatches = reactor.dispatch_batch(now);
        for dispatch in dispatches {
            let task = task_map.remove(&dispatch.work.id).expect("task found");
            match task {
                DatabaseTask::OltpUpdate { item_id, decrement } => {
                    let key = Key::new(INVENTORY_TABLE.0, item_id);
                    let commit = database.commit_id();
                    let existing = database.get(INVENTORY_TABLE, commit, key).expect("get item").expect("item exists");
                    let current_qty = match existing.values.get(1) {
                        Some(Value::U64(q)) => *q,
                        _ => 0,
                    };
                    database
                        .update(INVENTORY_TABLE, inventory_row(item_id, current_qty.saturating_sub(decrement)))
                        .expect("update inventory");
                    completed_oltp += 1;
                }
                DatabaseTask::ScanOrders { status } => {
                    let commit = database.commit_id();
                    let matching = database
                        .index_get(ORDERS_TABLE, commit, ORDER_STATUS_INDEX, &[Value::Text(status)])
                        .expect("index get orders");
                    assert!(!matching.is_empty());
                    completed_scans += 1;
                }
                DatabaseTask::ReclaimCompaction { units } => {
                    let report = database
                        .compact_with_budget(RelationalCompactionBudget::new(units))
                        .expect("compact with budget");
                    assert_eq!(report.before.backend, kind);
                    completed_reclaim += 1;
                }
                DatabaseTask::SchemaAddColumn => {
                    database
                        .add_nullable_column(
                            ORDERS_TABLE,
                            ColumnDefinition {
                                id: ColumnId(3),
                                name: "notes".to_owned(),
                                data_type: ColumnType::Text,
                                nullable: true,
                            },
                        )
                        .expect("add notes column");
                    schema_added = true;
                }
                DatabaseTask::WalCheckpoint => {
                    database.checkpoint().expect("checkpoint");
                }
            }
            reactor.complete(dispatch.worker).expect("complete worker");
        }
    }

    // Drain remaining queued work
    while reactor.stats().queued > 0 {
        now += 1;
        let dispatches = reactor.dispatch_batch(now);
        for dispatch in dispatches {
            let task = task_map.remove(&dispatch.work.id).expect("task found");
            match task {
                DatabaseTask::OltpUpdate { item_id, decrement } => {
                    let key = Key::new(INVENTORY_TABLE.0, item_id);
                    let commit = database.commit_id();
                    let existing = database.get(INVENTORY_TABLE, commit, key).expect("get item").expect("item exists");
                    let current_qty = match existing.values.get(1) {
                        Some(Value::U64(q)) => *q,
                        _ => 0,
                    };
                    database
                        .update(INVENTORY_TABLE, inventory_row(item_id, current_qty.saturating_sub(decrement)))
                        .expect("update inventory");
                    completed_oltp += 1;
                }
                DatabaseTask::ScanOrders { status } => {
                    let commit = database.commit_id();
                    let matching = database
                        .index_get(ORDERS_TABLE, commit, ORDER_STATUS_INDEX, &[Value::Text(status)])
                        .expect("index get orders");
                    assert!(!matching.is_empty());
                    completed_scans += 1;
                }
                DatabaseTask::ReclaimCompaction { units } => {
                    let _ = database
                        .compact_with_budget(RelationalCompactionBudget::new(units))
                        .expect("compact with budget");
                    completed_reclaim += 1;
                }
                DatabaseTask::SchemaAddColumn => {
                    if !schema_added {
                        database
                            .add_nullable_column(
                                ORDERS_TABLE,
                                ColumnDefinition {
                                id: ColumnId(3),
                                name: "notes".to_owned(),
                                data_type: ColumnType::Text,
                                nullable: true,
                            },
                        )
                        .expect("add notes column");
                        schema_added = true;
                    }
                }
                DatabaseTask::WalCheckpoint => {
                    database.checkpoint().expect("checkpoint");
                }
            }
            reactor.complete(dispatch.worker).expect("complete worker");
        }
    }

    assert_eq!(completed_oltp, 30);
    assert!(completed_scans > 0);
    assert!(completed_reclaim > 0);
    assert!(schema_added);

    // Verify governor invariants
    let stats = reactor.stats();
    assert_eq!(stats.accounted_cost, 0);
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(stats.expired, 0);

    database.verify().expect("verify database");
    database.checkpoint().expect("checkpoint final");
    database.close().expect("close database");

    // Reopen and re-verify
    let mut reopened = RelationalDatabase::open(db_config).expect("reopen");
    reopened.verify().expect("verify reopened");
    reopened.close().expect("close reopened");
}

#[test]
fn mixed_reactor_governor_executes_database_workload_across_backends() {
    let temporary = tempdir().expect("temporary directory");
    exercise_governed_database_workload(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise_governed_database_workload(
        RelationalBackendKind::Seer,
        &seer.path().join("seer"),
    );
}
