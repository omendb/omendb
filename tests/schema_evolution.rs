use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DbError, IndexDefinition, IndexId, Key,
    RelationalBackendConfig, RelationalDatabase, Row, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

const ITEMS: TableId = TableId(1);
const VALUE_INDEX: IndexId = IndexId(1);

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn table() -> TableDefinition {
    TableDefinition {
        id: ITEMS,
        name: "items".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "value".to_owned(),
            data_type: ColumnType::Text,
            nullable: false,
        }],
    }
}

fn row(id: u64, value: &str) -> Row {
    Row {
        primary: Key::new(ITEMS.0, id),
        values: vec![Value::Text(value.to_owned())],
    }
}

fn expanded_row(id: u64, value: &str, note: Value) -> Row {
    Row {
        primary: Key::new(ITEMS.0, id),
        values: vec![Value::Text(value.to_owned()), note],
    }
}

fn exercise(directory: &Path) {
    let database_config = config(directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    database.create_table(table()).expect("table");
    database
        .create_index(IndexDefinition {
            id: VALUE_INDEX,
            table: ITEMS,
            columns: vec![ColumnId(1)],
            unique: false,
        })
        .expect("value index");
    let seed = database.insert(ITEMS, row(1, "before")).expect("seed");
    database
        .insert(ITEMS, row(2, "also-before"))
        .expect("second seed");
    let schema_commit = database
        .add_nullable_column(
            ITEMS,
            ColumnDefinition {
                id: ColumnId(2),
                name: "note".to_owned(),
                data_type: ColumnType::Text,
                nullable: true,
            },
        )
        .expect("add nullable column");
    assert!(schema_commit.0 > seed.0);
    assert_eq!(
        database
            .catalog()
            .table(ITEMS)
            .expect("current table")
            .columns
            .len(),
        2
    );
    assert_eq!(
        database
            .get(ITEMS, Key::new(ITEMS.0, 1))
            .expect("expanded row"),
        Some(expanded_row(1, "before", Value::Null))
    );
    assert_eq!(
        database
            .index_get(ITEMS, VALUE_INDEX, &[Value::Text("before".to_owned())])
            .expect("existing index"),
        vec![expanded_row(1, "before", Value::Null)]
    );

    let note_index = IndexId(2);
    database
        .create_index(IndexDefinition {
            id: note_index,
            table: ITEMS,
            columns: vec![ColumnId(2)],
            unique: false,
        })
        .expect("index on appended column");
    assert!(
        database
            .index_get(ITEMS, note_index, &[Value::Text("memo".to_owned())],)
            .expect("empty appended-column index")
            .is_empty()
    );

    database
        .update(
            ITEMS,
            expanded_row(1, "before", Value::Text("memo".to_owned())),
        )
        .expect("write new column");
    assert_eq!(
        database
            .index_get(ITEMS, note_index, &[Value::Text("memo".to_owned())],)
            .expect("updated appended-column index"),
        vec![expanded_row(1, "before", Value::Text("memo".to_owned()))]
    );
    let before_invalid = database.commit_id();
    assert!(matches!(
        database.add_nullable_column(
            ITEMS,
            ColumnDefinition {
                id: ColumnId(3),
                name: "required".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            }
        ),
        Err(DbError::InvalidState(message)) if message.contains("nullable")
    ));
    assert!(matches!(
        database.add_nullable_column(
            ITEMS,
            ColumnDefinition {
                id: ColumnId(2),
                name: "duplicate".to_owned(),
                data_type: ColumnType::Text,
                nullable: true,
            }
        ),
        Err(DbError::InvalidState(message)) if message.contains("already exists")
    ));
    assert_eq!(database.commit_id(), before_invalid);

    database.close().expect("close");

    let reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_eq!(
        reopened
            .catalog()
            .table(ITEMS)
            .expect("reopened table")
            .columns
            .len(),
        2
    );
    assert_eq!(
        reopened
            .get(ITEMS, Key::new(ITEMS.0, 1))
            .expect("reopened row"),
        Some(expanded_row(1, "before", Value::Text("memo".to_owned())))
    );
    assert_eq!(
        reopened
            .index_get(ITEMS, note_index, &[Value::Text("memo".to_owned())],)
            .expect("reopened appended-column index"),
        vec![expanded_row(1, "before", Value::Text("memo".to_owned()))]
    );
    reopened.close().expect("close reopened");
}

#[test]
fn nullable_column_evolution_preserves_history_and_reopen_on_each_backend() {
    let root = tempdir().expect("root");
    exercise(&root.path().join("temporary"));
}
