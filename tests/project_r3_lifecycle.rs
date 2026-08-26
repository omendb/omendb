use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, IndexDefinition, IndexId, Key, RelationalDatabase, Row,
    TableDefinition, TableId, Value,
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

fn digest_database(database: &RelationalDatabase) -> String {
    let mut canonical = String::new();
    let rows = database
        .scan(ORDERS_TABLE, usize::MAX)
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

fn exercise_r3_operational_lifecycle(directory: &Path) {
    let db_config = config(directory);
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
    database
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

    // 2. Active traffic (updates & reads)
    let traffic_commit = database
        .transaction(|db, tx| {
            for tenant in 1..=num_tenants {
                tx.update(db, ORDERS_TABLE, order_row(tenant, 1, "processing", 150))?;
            }
            Ok(())
        })
        .expect("traffic updates")
        .1;

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

    // Current reader sees 5 columns with Value::Null for un-backfilled rows
    let current_row = database
        .get(ORDERS_TABLE, order_key(1, 2))
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
    let high_priority_orders = database
        .index_get(
            ORDERS_TABLE,
            ORDER_PRIORITY_INDEX,
            &[Value::U64(1), Value::Text("high".to_owned())],
        )
        .expect("query high priority index");
    assert_eq!(high_priority_orders.len(), (orders_per_tenant / 5) as usize);

    let cutover_digest = digest_database(&database);

    database.close().expect("close source database");

    // Reopen source and verify integrity
    let reopened_db = RelationalDatabase::open(db_config).expect("reopen source");
    assert_eq!(reopened_db.commit_id(), cutover_commit);
    assert_eq!(digest_database(&reopened_db), cutover_digest);
    reopened_db.close().expect("close reopened source");
}

#[test]
fn public_facade_replays_r3_operational_lifecycle() {
    let temporary = tempdir().expect("temporary directory");
    exercise_r3_operational_lifecycle(&temporary.path().join("temporary"));
}
