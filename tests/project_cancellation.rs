use std::cell::Cell;
use std::path::Path;

use omendb::{
    CancellationToken, ColumnDefinition, ColumnId, ColumnType, DbError, Key,
    RelationalBackendConfig, RelationalDatabase, Row, TableDefinition, TableId,
    TransactionErrorClass, Value,
};
use tempfile::tempdir;

const TABLE: TableId = TableId(1);

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn table() -> TableDefinition {
    TableDefinition {
        id: TABLE,
        name: "items".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "value".to_owned(),
            data_type: ColumnType::Text,
            nullable: false,
        }],
    }
}

fn row(primary: u64, value: &str) -> Row {
    Row {
        primary: Key::new(TABLE.0, primary),
        values: vec![Value::Text(value.to_owned())],
    }
}

fn exercise(directory: &Path) {
    let config = config(directory);
    let mut database = RelationalDatabase::create(config).expect("create");
    let schema_commit = database.create_table(table()).expect("table");

    let admission_token = CancellationToken::new();
    admission_token.cancel();
    let called = Cell::new(false);
    let result =
        database.transaction_with_cancellation(&admission_token, |_database, _transaction| {
            called.set(true);
            Ok(())
        });
    let error = result.expect_err("cancelled transaction");
    assert!(matches!(&error, DbError::Cancelled));
    assert_eq!(error.transaction_class(), TransactionErrorClass::Cancelled);
    assert!(!called.get());
    assert_eq!(database.commit_id(), schema_commit);

    let publication_token = CancellationToken::new();
    let result =
        database.transaction_with_cancellation(&publication_token, |database, transaction| {
            transaction.insert(database, TABLE, row(1, "not durable"))?;
            publication_token.cancel();
            Ok(())
        });
    assert!(matches!(result, Err(DbError::Cancelled)));
    assert_eq!(database.commit_id(), schema_commit);
    assert_eq!(
        database
            .get(TABLE, Key::new(TABLE.0, 1))
            .expect("cancelled row lookup"),
        None
    );

    let read_token = CancellationToken::new();
    let result = database.transaction_with_cancellation(&read_token, |database, transaction| {
        assert_eq!(
            transaction.get(database, TABLE, Key::new(TABLE.0, 1))?,
            None
        );
        read_token.cancel();
        Ok(())
    });
    assert!(matches!(result, Err(DbError::Cancelled)));

    database.close().expect("close");
}

#[test]
fn public_transactions_cancel_before_publication() {
    let temporary = tempdir().expect("temporary directory");
    exercise(&temporary.path().join("temporary"));
}
