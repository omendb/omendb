//! Typed and analytical operations must resolve metadata from their data snapshot.
use omendb::{
    AggregateKind, AnalyticalExecutor, AnalyticalQuery, ColumnDefinition, ColumnId, ColumnType,
    DbError, Key, OperationControl, RelationalBackendConfig, RelationalDatabase, Row, RowIdentity,
    SchemaMutation, TableDefinition, TableId, TransactionErrorClass, Value,
};

fn database(directory: &std::path::Path) -> RelationalDatabase {
    RelationalDatabase::create(RelationalBackendConfig::new(directory.join("db"))).unwrap()
}

fn table_id(database: &RelationalDatabase, name: &str) -> TableId {
    database
        .catalog()
        .tables()
        .find(|table| table.name == name)
        .unwrap()
        .id
}

#[test]
fn analytical_metadata_and_positions_follow_the_retained_layout() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = database(directory.path());
    db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, discarded BIGINT, amount BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO items VALUES (1, 100, 7), (2, 200, 11)")
        .unwrap();
    let table = table_id(&db, "items");
    let mut tx = db.begin().unwrap();
    db.apply_schema_mutations(&[
        SchemaMutation::DropColumn {
            table,
            column: ColumnId(2),
        },
        SchemaMutation::RenameColumn {
            table,
            column: ColumnId(3),
            new_name: "funds".to_owned(),
        },
    ])
    .unwrap();
    let query = AnalyticalQuery::new(table)
        .with_morsel_size(1)
        .with_aggregate(AggregateKind::Sum, Some(ColumnId(3)), "total");
    let result =
        AnalyticalExecutor::execute(&db, &mut tx, &query, &OperationControl::default()).unwrap();
    assert_eq!(result.rows, vec![vec![Value::I64(18)]]);
    let grouped = query.with_group_by(ColumnId(3));
    let result =
        AnalyticalExecutor::execute(&db, &mut tx, &grouped, &OperationControl::default()).unwrap();
    assert_eq!(result.column_names, vec!["amount", "total"]);
    assert_eq!(
        result.rows,
        vec![
            vec![Value::I64(7), Value::I64(7)],
            vec![Value::I64(11), Value::I64(11)]
        ]
    );
    assert_eq!(
        db.execute_sql("SELECT SUM(funds) FROM items").unwrap().rows,
        vec![vec![Value::I64(18)]]
    );
    assert_eq!(tx.catalog().unwrap().table(table).unwrap().columns.len(), 3);
    // SQL has a conservative generation fence, not a second retained binder.
    assert_eq!(
        tx.execute_sql(&db, "SELECT SUM(funds) FROM items")
            .unwrap_err()
            .transaction_class(),
        TransactionErrorClass::SerializationRetry
    );
    assert!(matches!(tx.catalog(), Err(DbError::TransactionAborted)));
}

#[test]
fn typed_row_delete_uses_the_retained_primary_key_position() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = database(directory.path());
    db.execute_sql("CREATE TABLE items (discarded BIGINT, id BIGINT PRIMARY KEY, amount BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO items VALUES (99, 1, 7)")
        .unwrap();
    let table = table_id(&db, "items");
    let mut tx = db.begin().unwrap();
    let row = tx.scan(&db, table, 1).unwrap().pop().unwrap();
    db.execute_sql("ALTER TABLE items DROP COLUMN discarded")
        .unwrap();
    tx.delete_row(&db, table, row).unwrap();
    assert!(tx.scan(&db, table, 1).unwrap().is_empty());
    // A row rewrite or the schema dependency may reject publication first.
    assert_eq!(
        tx.commit().unwrap_err().transaction_class(),
        TransactionErrorClass::SerializationRetry
    );
    assert_eq!(
        db.execute_sql("SELECT id, amount FROM items").unwrap().rows,
        vec![vec![Value::I64(1), Value::I64(7)]]
    );
}

#[test]
fn composite_lookup_retains_its_identity_and_table_incarnation() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = database(directory.path());
    db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, amount BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO items VALUES (1, 7)").unwrap();
    let table = table_id(&db, "items");
    let definition = db.catalog().table(table).unwrap().clone();
    let identity = RowIdentity::new(table, vec![ColumnId(1)], vec![Value::I64(1)]).unwrap();
    let mut tx = db.begin().unwrap();
    let expected = tx.get_by_identity(&db, table, &identity).unwrap().unwrap();
    db.apply_schema_mutations(&[SchemaMutation::DropTable { table }])
        .unwrap();
    // Same ID and names, but the new incarnation uses legacy key identity.
    db.create_table(definition).unwrap();
    db.insert(
        table,
        Row {
            primary: Key::new(table.0, 1),
            values: vec![Value::I64(1), Value::I64(700)],
        },
    )
    .unwrap();
    assert_eq!(
        tx.get_by_identity(&db, table, &identity).unwrap(),
        Some(expected)
    );
    assert_eq!(
        db.get(table, Key::new(table.0, 1)).unwrap().unwrap().values[1],
        Value::I64(700)
    );
}

#[test]
fn legacy_lookup_and_delete_do_not_switch_to_a_recreated_primary_key() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = database(directory.path());
    let table = TableId(9);
    let definition = TableDefinition {
        id: table,
        name: "items".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "id".to_owned(),
            data_type: ColumnType::I64,
            nullable: false,
        }],
    };
    db.create_table(definition.clone()).unwrap();
    let row = Row {
        primary: Key::new(table.0, 1),
        values: vec![Value::I64(1)],
    };
    db.insert(table, row.clone()).unwrap();
    let mut tx = db.begin().unwrap();
    db.apply_schema_mutations(&[SchemaMutation::DropTable { table }])
        .unwrap();
    db.create_table_with_schema_and_primary_key(
        definition,
        Some(vec![ColumnId(1)]),
        Default::default(),
    )
    .unwrap();
    assert_eq!(tx.get(&db, table, row.primary).unwrap(), Some(row.clone()));
    tx.delete(&db, table, row.primary).unwrap();
    assert!(tx.get(&db, table, row.primary).unwrap().is_none());
    assert_eq!(
        tx.commit().unwrap_err().transaction_class(),
        TransactionErrorClass::SerializationRetry
    );
    assert!(db.scan(table, 1).unwrap().is_empty());
}
