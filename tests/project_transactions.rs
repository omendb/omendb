use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, DbError, Key, RelationalBackendConfig,
    RelationalBackendKind, RelationalDatabase, SeerKernelConfig, TableDefinition, TableId,
    TransactionProfile, Value,
};
use tempfile::tempdir;

const ACCOUNTS: TableId = TableId(1);

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

fn exercise_transaction_contract(kind: RelationalBackendKind, directory: &Path) {
    let mut database = RelationalDatabase::create(config(kind, directory)).expect("create");
    assert_eq!(
        database.transaction_profile(),
        TransactionProfile::FixedSnapshotSerializedWriter
    );
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
    assert_eq!(
        reader.commit(&mut database).expect("read-only commit"),
        seed
    );
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
            .get(ACCOUNTS, database.commit_id(), Key::new(ACCOUNTS.0, 3))
            .expect("aborted row lookup"),
        None
    );

    // Both writers observe the same invariant and update disjoint rows. The
    // first commit succeeds; the stale second writer is rejected rather than
    // creating a backend-dependent write-skew history.
    let mut first = database.begin().expect("begin first writer");
    let mut second = database.begin().expect("begin second writer");
    assert_eq!(first.snapshot(), second.snapshot());
    assert_eq!(
        first
            .scan(&database, ACCOUNTS, 10)
            .expect("first invariant read"),
        vec![account(1, 90, "open"), account(2, 100, "open")]
    );
    assert_eq!(
        second
            .scan(&database, ACCOUNTS, 10)
            .expect("second invariant read"),
        vec![account(1, 90, "open"), account(2, 100, "open")]
    );
    first
        .update(&database, ACCOUNTS, account(1, 80, "open"))
        .expect("first disjoint update");
    second
        .update(&database, ACCOUNTS, account(2, 90, "open"))
        .expect("second disjoint update");
    let first_commit = first.commit(&mut database).expect("first writer commit");
    assert!(matches!(
        second.commit(&mut database),
        Err(DbError::SerializationConflict { snapshot, current })
            if snapshot == first_commit.0 - 1 && current == first_commit.0
    ));
    assert_eq!(
        database
            .scan(ACCOUNTS, first_commit, 10)
            .expect("committed state"),
        vec![account(1, 80, "open"), account(2, 100, "open")]
    );

    database.close().expect("close");
}

#[test]
fn project_facing_transactions_match_the_fixed_snapshot_profile() {
    let temporary = tempdir().expect("temporary directory");
    exercise_transaction_contract(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise_transaction_contract(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
