use std::collections::BTreeMap;
use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, IndexDefinition, IndexId, Key, NamedIndexDefinition,
    RelationalDatabase, RelationalSchemaDefinition, Row, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

#[allow(dead_code)]
mod support;

use support::config;

const ITEMS: TableId = TableId(30);
const STATE_INDEX: IndexId = IndexId(30);
const VERSION_INDEX: IndexId = IndexId(31);
const TENANTS: u64 = 4;
const INITIAL_ENTRIES: u64 = 32;
const BATCHES: usize = 24;
const OPERATIONS_PER_BATCH: usize = 6;
const REOPEN_INTERVAL: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Item {
    state: String,
    version: u64,
}

type Model = BTreeMap<(u64, u64), Item>;

fn table() -> TableDefinition {
    TableDefinition {
        id: ITEMS,
        name: "composite_items".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "entry_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "state".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "version".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn schema() -> RelationalSchemaDefinition {
    RelationalSchemaDefinition {
        indexes: vec![
            NamedIndexDefinition {
                definition: IndexDefinition {
                    id: STATE_INDEX,
                    table: ITEMS,
                    columns: vec![ColumnId(3)],
                    unique: false,
                },
                name: Some("composite_item_state".to_owned()),
            },
            NamedIndexDefinition {
                definition: IndexDefinition {
                    id: VERSION_INDEX,
                    table: ITEMS,
                    columns: vec![ColumnId(1), ColumnId(4)],
                    unique: false,
                },
                name: Some("composite_item_version".to_owned()),
            },
        ],
        foreign_keys: Vec::new(),
    }
}

fn row(identity: (u64, u64), item: &Item) -> Row {
    Row {
        primary: Key::new(ITEMS.0, identity.1),
        values: vec![
            Value::U64(identity.0),
            Value::U64(identity.1),
            Value::Text(item.state.clone()),
            Value::U64(item.version),
        ],
    }
}

fn row_identity(row: &Row) -> (u64, u64) {
    match (&row.values[0], &row.values[1]) {
        (Value::U64(tenant), Value::U64(entry)) => (*tenant, *entry),
        values => panic!("invalid composite identity values: {values:?}"),
    }
}

fn u64_value(row: &Row, position: usize) -> u64 {
    match row.values.get(position) {
        Some(Value::U64(value)) => *value,
        value => panic!("expected U64 at column {position}, got {value:?}"),
    }
}

fn sorted_rows(model: &Model) -> Vec<Row> {
    model
        .iter()
        .map(|(identity, item)| row(*identity, item))
        .collect()
}

fn sorted_actual(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort_by_key(row_identity);
    rows
}

fn logical_signature(row: &Row) -> (u64, u64, String, u64) {
    (
        u64_value(row, 0),
        u64_value(row, 1),
        match &row.values[2] {
            Value::Text(value) => value.clone(),
            value => panic!("expected Text at column 2, got {value:?}"),
        },
        u64_value(row, 3),
    )
}

fn expected_signatures(model: &Model) -> Vec<(u64, u64, String, u64)> {
    sorted_rows(model).iter().map(logical_signature).collect()
}

fn actual_signatures(rows: Vec<Row>) -> Vec<(u64, u64, String, u64)> {
    sorted_actual(rows).iter().map(logical_signature).collect()
}

fn assert_matches(database: &RelationalDatabase, model: &Model) {
    assert_eq!(
        actual_signatures(
            database
                .scan(ITEMS, usize::MAX)
                .expect("scan composite rows"),
        ),
        expected_signatures(model)
    );

    for state in ["active", "review", "archived", "blocked"] {
        let mut expected = model
            .iter()
            .filter(|(_, item)| item.state == state)
            .map(|(identity, item)| row(*identity, item))
            .collect::<Vec<_>>();
        expected.sort_by_key(row_identity);
        assert_eq!(
            actual_signatures(
                database
                    .index_get(ITEMS, STATE_INDEX, &[Value::Text(state.to_owned())],)
                    .expect("read composite state index"),
            ),
            expected.iter().map(logical_signature).collect::<Vec<_>>(),
            "state index {state}"
        );
    }

    let mut expected = sorted_rows(model);
    expected.sort_by_key(|row| (u64_value(row, 0), u64_value(row, 3), row_identity(row)));
    let mut actual = database
        .index_scan(ITEMS, VERSION_INDEX)
        .expect("scan composite version index");
    actual.sort_by_key(|row| (u64_value(row, 0), u64_value(row, 3), row_identity(row)));
    assert_eq!(
        actual.iter().map(logical_signature).collect::<Vec<_>>(),
        expected.iter().map(logical_signature).collect::<Vec<_>>(),
        "version index membership"
    );
}

fn exercise(directory: &Path) -> Vec<(u64, u64, String, u64)> {
    let config = config(directory);
    let mut database = RelationalDatabase::create(config.clone()).expect("create composite DB");
    database
        .create_table_with_schema_and_primary_key(
            table(),
            Some(vec![ColumnId(1), ColumnId(2)]),
            schema(),
        )
        .expect("create composite schema");

    let mut model = Model::new();
    let _seed_commit = database
        .transaction(|database, transaction| {
            for tenant in 0..TENANTS {
                for entry in 0..INITIAL_ENTRIES {
                    let item = Item {
                        state: "active".to_owned(),
                        version: 0,
                    };
                    transaction.insert(database, ITEMS, row((tenant, entry), &item))?;
                    model.insert((tenant, entry), item);
                }
            }
            Ok::<_, omendb::DbError>(())
        })
        .expect("seed composite rows");

    for batch in 0..BATCHES {
        let mut transaction = database.begin().expect("begin composite batch");
        let mut candidate = model.clone();

        for offset in 0..OPERATIONS_PER_BATCH {
            let identity = (
                ((batch + offset) as u64) % TENANTS,
                ((batch * 7 + offset * 5) as u64) % INITIAL_ENTRIES,
            );
            if let Some(item) = candidate.get_mut(&identity) {
                item.state = match (batch + offset) % 4 {
                    0 => "active",
                    1 => "review",
                    2 => "archived",
                    _ => "blocked",
                }
                .to_owned();
                item.version += 1;
                transaction
                    .update(&database, ITEMS, row(identity, item))
                    .expect("stage composite update");
            }
        }

        let inserted_identity = ((batch as u64) % TENANTS, INITIAL_ENTRIES + batch as u64);
        let inserted = Item {
            state: "review".to_owned(),
            version: 1,
        };
        transaction
            .insert(&database, ITEMS, row(inserted_identity, &inserted))
            .expect("stage composite insert");
        candidate.insert(inserted_identity, inserted);

        let delete_position = (batch * 11) % candidate.len();
        let delete_identity = *candidate
            .keys()
            .nth(delete_position)
            .expect("composite delete target");
        let deleted = candidate
            .remove(&delete_identity)
            .expect("remove composite model row");
        transaction
            .delete_row(&database, ITEMS, row(delete_identity, &deleted))
            .expect("stage composite delete");

        transaction.commit().expect("commit composite batch");
        model = candidate;

        let current = database.commit_id();
        if (batch + 1) % 4 == 0 {
            assert_matches(&database, &model);
        }

        if (batch + 1) % REOPEN_INTERVAL == 0 {
            database.close().expect("close composite history");
            database = RelationalDatabase::open(config.clone()).expect("reopen composite history");
            assert_eq!(database.commit_id(), current);
            assert_matches(&database, &model);
        }
    }

    let current = database.commit_id();
    assert_matches(&database, &model);
    database.close().expect("close final composite history");

    let reopened = RelationalDatabase::open(config).expect("reopen final composite history");
    assert_eq!(reopened.commit_id(), current);
    assert_matches(&reopened, &model);
    let rows = actual_signatures(
        reopened
            .scan(ITEMS, usize::MAX)
            .expect("read final composite rows"),
    );
    reopened.close().expect("close reopened composite history");
    rows
}

#[test]
fn public_facade_preserves_composite_lifecycle() {
    let temporary = tempdir().expect("temporary composite directory");
    exercise(&temporary.path().join("temporary"));
}
