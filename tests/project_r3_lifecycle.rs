use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, CommitId, IndexDefinition, IndexId, Key,
    RelationalArchive, RelationalArchiveMode, RelationalBackendKind, RelationalDatabase,
    RelationalSnapshotCaptureOptions, Row, TableDefinition, TableId, Value,
};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

#[allow(dead_code)]
mod support;

use support::config;

const ORDERS_TABLE: TableId = TableId(30);
const ORDER_STATUS_INDEX: IndexId = IndexId(30);
const ORDER_PRIORITY_INDEX: IndexId = IndexId(31);

fn orders_table() -> TableDefinition {
    TableDefinition {
        id: ORDERS_TABLE,
        name: "r3_orders".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "order_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "status".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "amount".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn order_key(tenant_id: u64, order_id: u64) -> Key {
    assert!(tenant_id <= u64::from(u32::MAX));
    assert!(order_id <= u64::from(u32::MAX));
    Key::new(ORDERS_TABLE.0, (tenant_id << 32) | order_id)
}

fn order_row(tenant_id: u64, order_id: u64, status: &str, amount: u64) -> Row {
    Row {
        primary: order_key(tenant_id, order_id),
        values: vec![
            Value::U64(tenant_id),
            Value::U64(order_id),
            Value::Text(status.to_owned()),
            Value::U64(amount),
        ],
    }
}

fn order_row_with_priority(
    tenant_id: u64,
    order_id: u64,
    status: &str,
    amount: u64,
    priority: Value,
) -> Row {
    Row {
        primary: order_key(tenant_id, order_id),
        values: vec![
            Value::U64(tenant_id),
            Value::U64(order_id),
            Value::Text(status.to_owned()),
            Value::U64(amount),
            priority,
        ],
    }
}

fn digest_database(database: &RelationalDatabase, snapshot: CommitId) -> String {
    let mut canonical = String::new();
    let rows = database
        .scan(ORDERS_TABLE, snapshot, usize::MAX)
        .expect("scan orders");
    for row in rows {
        let tenant = match row.values.first() {
            Some(Value::U64(v)) => *v,
            _ => panic!("bad tenant"),
        };
        let order = match row.values.get(1) {
            Some(Value::U64(v)) => *v,
            _ => panic!("bad order"),
        };
        let status = match row.values.get(2) {
            Some(Value::Text(s)) => s.as_str(),
            _ => panic!("bad status"),
        };
        let amount = match row.values.get(3) {
            Some(Value::U64(v)) => *v,
            _ => panic!("bad amount"),
        };
        let priority = match row.values.get(4) {
            Some(Value::Text(p)) => p.as_str(),
            Some(Value::Null) | None => "null",
            _ => panic!("bad priority"),
        };
        canonical.push_str(&format!("{tenant}|{order}|{status}|{amount}|{priority}\n"));
    }
    let digest = Sha256::digest(canonical.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn exercise_r3_operational_lifecycle(kind: RelationalBackendKind, directory: &Path) {
    let db_config = config(kind, directory);
    let mut database = RelationalDatabase::create(db_config.clone()).expect("create database");

    // 1. Seed initial schema and data
    database
        .create_table(orders_table())
        .expect("create orders table");
    database
        .create_index(IndexDefinition {
            id: ORDER_STATUS_INDEX,
            table: ORDERS_TABLE,
            columns: vec![ColumnId(1), ColumnId(3)],
            unique: false,
        })
        .expect("create status index");

    let num_tenants: u64 = 10;
    let orders_per_tenant: u64 = 20;
    let (_, seed_commit) = database
        .transaction(|db, tx| {
            for tenant in 1..=num_tenants {
                for order in 1..=orders_per_tenant {
                    tx.insert(
                        db,
                        ORDERS_TABLE,
                        order_row(tenant, order, "pending", order * 100),
                    )?;
                }
            }
            Ok(())
        })
        .expect("seed initial orders");

    let pre_cutover_lease = database
        .retain(seed_commit)
        .expect("retain pre-cutover snapshot");
    let initial_digest = digest_database(&database, seed_commit);

    // 2. Active traffic (updates & reads)
    let (_, traffic_commit) = database
        .transaction(|db, tx| {
            for tenant in 1..=num_tenants {
                tx.update(db, ORDERS_TABLE, order_row(tenant, 1, "processing", 150))?;
            }
            Ok(())
        })
        .expect("traffic updates");
    assert!(traffic_commit.0 > seed_commit.0);

    // 3. Schema submit: add nullable 'priority' column
    let schema_commit = database
        .add_nullable_column(
            ORDERS_TABLE,
            ColumnDefinition {
                id: ColumnId(5),
                name: "priority".to_owned(),
                data_type: ColumnType::Text,
                nullable: true,
            },
        )
        .expect("add priority column");
    assert!(schema_commit.0 > traffic_commit.0);

    // Historical reader on seed_commit sees 4 columns and initial row state
    let historical_row = database
        .get(ORDERS_TABLE, seed_commit, order_key(1, 1))
        .expect("get historical row")
        .expect("historical row exists");
    assert_eq!(historical_row.values.len(), 4);
    assert_eq!(
        historical_row.values.get(2),
        Some(&Value::Text("pending".to_owned()))
    );

    // Current reader sees 5 columns with Value::Null for un-backfilled rows
    let current_row = database
        .get(ORDERS_TABLE, schema_commit, order_key(1, 2))
        .expect("get current row")
        .expect("current row exists");
    assert_eq!(current_row.values.len(), 5);
    assert_eq!(current_row.values.get(4), Some(&Value::Null));

    // 4. Schema run: backfill priority in batches
    for tenant in 1..=num_tenants {
        database
            .transaction(|db, tx| {
                for order in 1..=orders_per_tenant {
                    let key = order_key(tenant, order);
                    let existing = tx.get(db, ORDERS_TABLE, key)?.expect("existing order");
                    let status = match existing.values.get(2) {
                        Some(Value::Text(s)) => s.clone(),
                        _ => "pending".to_owned(),
                    };
                    let amount = match existing.values.get(3) {
                        Some(Value::U64(a)) => *a,
                        _ => 100,
                    };
                    let priority_val = if order % 5 == 0 { "high" } else { "normal" };
                    tx.update(
                        db,
                        ORDERS_TABLE,
                        order_row_with_priority(
                            tenant,
                            order,
                            &status,
                            amount,
                            Value::Text(priority_val.to_owned()),
                        ),
                    )?;
                }
                Ok(())
            })
            .expect("backfill batch");
    }

    // 5. Schema run: add and validate secondary index on (tenant_id, priority)
    database
        .create_index(IndexDefinition {
            id: ORDER_PRIORITY_INDEX,
            table: ORDERS_TABLE,
            columns: vec![ColumnId(1), ColumnId(5)],
            unique: false,
        })
        .expect("create priority index");

    // 6. Schema cutover verification
    let cutover_commit = database.commit_id();
    let post_cutover_lease = database
        .retain(cutover_commit)
        .expect("retain post-cutover");

    let high_priority_orders = database
        .index_get(
            ORDERS_TABLE,
            cutover_commit,
            ORDER_PRIORITY_INDEX,
            &[Value::U64(1), Value::Text("high".to_owned())],
        )
        .expect("query high priority index");
    assert_eq!(high_priority_orders.len(), (orders_per_tenant / 5) as usize);

    let cutover_digest = digest_database(&database, cutover_commit);

    // 7. Snapshot clone / archive capture
    let archive_path = directory.join("r3_cutover.archive");
    let capture = database
        .capture_selected_snapshots(
            &[seed_commit, cutover_commit],
            RelationalSnapshotCaptureOptions::new(10000),
        )
        .expect("capture selected snapshots");

    let archive =
        RelationalArchive::from_capture(capture, RelationalArchiveMode::RetainedSnapshots)
            .expect("create archive");
    archive.write(&archive_path).expect("write archive");

    // 8. Restore into fresh database (clone/restore qualification)
    let restore_dir = directory.join("restored_clone");
    let target_config = config(kind, &restore_dir);

    let read_archive = RelationalArchive::read(&archive_path).expect("read archive");
    let (mut restored_db, restore_report) = read_archive
        .restore(target_config)
        .expect("restore archive into target clone");

    assert_eq!(restore_report.mappings.len(), 2);
    let target_cutover_commit = restore_report
        .mappings
        .iter()
        .find(|m| m.source == cutover_commit)
        .map(|m| m.target)
        .expect("target cutover commit mapping");

    assert_eq!(restored_db.commit_id(), target_cutover_commit);
    let restored_digest = digest_database(&restored_db, target_cutover_commit);
    assert_eq!(restored_digest, cutover_digest);

    // Verify index works on restored database
    let restored_high_priority = restored_db
        .index_get(
            ORDERS_TABLE,
            target_cutover_commit,
            ORDER_PRIORITY_INDEX,
            &[Value::U64(1), Value::Text("high".to_owned())],
        )
        .expect("query restored index");
    assert_eq!(
        restored_high_priority.len(),
        (orders_per_tenant / 5) as usize
    );

    restored_db.verify().expect("verify restored database");
    restored_db.close().expect("close restored clone");

    // 9. Rollback check: Historical pre-cutover state is completely preserved
    let pre_cutover_digest_now = digest_database(&database, seed_commit);
    assert_eq!(pre_cutover_digest_now, initial_digest);

    database
        .release(pre_cutover_lease)
        .expect("release pre-cutover");
    database
        .release(post_cutover_lease)
        .expect("release post-cutover");
    database.verify().expect("verify source database");
    database.checkpoint().expect("checkpoint source");
    database.close().expect("close source database");

    // Reopen source and verify integrity
    let mut reopened_db = RelationalDatabase::open(db_config).expect("reopen source");
    assert_eq!(reopened_db.commit_id(), cutover_commit);
    assert_eq!(
        digest_database(&reopened_db, cutover_commit),
        cutover_digest
    );
    reopened_db.verify().expect("verify reopened source");
    reopened_db.close().expect("close reopened source");
}

#[test]
fn public_facade_replays_r3_operational_lifecycle_across_selected_backends() {
    let temporary = tempdir().expect("temporary directory");
    exercise_r3_operational_lifecycle(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise_r3_operational_lifecycle(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
