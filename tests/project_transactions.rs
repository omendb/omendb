use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DbError, Key, RelationalBackendConfig,
    RelationalDatabase, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

const ACCOUNTS: TableId = TableId(1);

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn accounts_table() -> TableDefinition {
    TableDefinition {
        id: ACCOUNTS,
        name: "accounts".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "balance".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "state".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn account(id: u64, balance: u64, state: &str) -> omendb::Row {
    omendb::Row {
        primary: Key::new(ACCOUNTS.0, id),
        values: vec![Value::U64(balance), Value::Text(state.to_owned())],
    }
}

fn exercise_transaction_contract(directory: &Path) {
    let mut database = RelationalDatabase::create(config(directory)).expect("create");
    database
        .create_table(accounts_table())
        .expect("accounts table");
    let seed = database
        .commit_batch([
            omendb::RelationalMutation::Insert {
                table: ACCOUNTS,
                row: account(1, 100, "open"),
            },
            omendb::RelationalMutation::Insert {
                table: ACCOUNTS,
                row: account(2, 100, "open"),
            },
        ])
        .expect("seed accounts");

    // A read-only transaction keeps one snapshot even when a later commit
    // advances the database head. It returns that snapshot without a no-op
    // publication.
    let mut reader = database.begin().expect("begin reader");
    assert_eq!(
        reader.scan(&database, ACCOUNTS, 10).expect("initial scan"),
        vec![account(1, 100, "open"), account(2, 100, "open")]
    );
    let update = database
        .update(ACCOUNTS, account(1, 90, "open"))
        .expect("advance head");
    assert_eq!(
        reader
            .get(&database, ACCOUNTS, Key::new(ACCOUNTS.0, 1))
            .expect("repeatable point read"),
        Some(account(1, 100, "open"))
    );
    assert_eq!(reader.commit().expect("read-only commit"), seed);
    assert_eq!(database.commit_id(), update);

    // A closure failure drops all staged mutations. No partial row may be
    // visible at the current head.
    let aborted = database.transaction(|database, transaction| {
        transaction.insert(database, ACCOUNTS, account(3, 50, "aborted"))?;
        Err::<(), _>(DbError::InvalidState(
            "intentional transaction abort".to_owned(),
        ))
    });
    assert!(matches!(
        aborted,
        Err(DbError::InvalidState(reason)) if reason == "intentional transaction abort"
    ));
    assert_eq!(
        database
            .get(ACCOUNTS, Key::new(ACCOUNTS.0, 3))
            .expect("aborted row lookup"),
        None
    );

    database.close().expect("close");
}

#[test]
fn project_facing_transactions_match_the_fixed_snapshot_profile() {
    let temporary = tempdir().expect("temporary directory");
    exercise_transaction_contract(&temporary.path().join("temporary"));
}
