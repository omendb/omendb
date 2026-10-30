//! Constraint timing at real native-operation and SQL-statement boundaries.

use omendb::{
    ConstraintId, ConstraintTiming, DbError, ForeignKeyDefinition, Key, ReferentialAction,
    RelationalBackendConfig, RelationalDatabase, RelationalMutation, Row, TableId, Value,
};
use tempfile::{TempDir, tempdir};

struct Fixture {
    _directory: TempDir,
    database: RelationalDatabase,
    parents: TableId,
    children: TableId,
}

impl Fixture {
    fn new(timing: ConstraintTiming) -> Self {
        let directory = tempdir().unwrap();
        let mut database =
            RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
                .unwrap();
        database
            .execute_sql("CREATE TABLE parents (id BIGINT PRIMARY KEY)")
            .unwrap();
        database
            .execute_sql("CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT)")
            .unwrap();
        let parents = database
            .catalog()
            .tables()
            .find(|table| table.name == "parents")
            .unwrap()
            .id;
        let children = database
            .catalog()
            .tables()
            .find(|table| table.name == "children")
            .unwrap()
            .id;
        database
            .create_foreign_key(ForeignKeyDefinition {
                id: ConstraintId(1),
                table: children,
                columns: vec![database.catalog().table(children).unwrap().columns[1].id],
                referenced_table: parents,
                referenced_columns: vec![database.catalog().table(parents).unwrap().columns[0].id],
                on_delete: ReferentialAction::Restrict,
                timing,
            })
            .unwrap();
        Self {
            _directory: directory,
            database,
            parents,
            children,
        }
    }
}

fn parent(table: TableId, id: u64) -> Row {
    Row {
        primary: Key::new(table.0, id),
        values: vec![Value::I64(i64::try_from(id).unwrap())],
    }
}

fn values(rows: Vec<Row>) -> Vec<Vec<Value>> {
    rows.into_iter().map(|row| row.values).collect()
}

fn child(table: TableId, id: u64, parent_id: i64) -> Row {
    let mut row = parent(table, id);
    row.values.push(Value::I64(parent_id));
    row
}

#[test]
fn immediate_native_operation_rejects_orphans_and_irreversibly_aborts_prior_work() {
    let fixture = Fixture::new(ConstraintTiming::Immediate);
    let database = &fixture.database;
    let mut transaction = database.begin().unwrap();
    transaction
        .insert(database, fixture.parents, parent(fixture.parents, 1))
        .unwrap();
    assert!(matches!(
        transaction.insert(database, fixture.children, child(fixture.children, 10, 7)),
        Err(DbError::ForeignKeyViolation { constraint: 1, .. })
    ));
    assert!(matches!(
        transaction.scan(database, fixture.parents, 10),
        Err(DbError::TransactionAborted)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(DbError::TransactionAborted)
    ));
    assert!(database.scan(fixture.parents, 10).unwrap().is_empty());
    assert!(database.scan(fixture.children, 10).unwrap().is_empty());
    fixture.database.close().unwrap();
}

#[test]
fn immediate_sql_checks_insert_update_and_delete_before_statement_success() {
    for statement in [
        "INSERT INTO children VALUES (11, 7)",
        "UPDATE children SET parent_id = 7 WHERE id = 10",
        "DELETE FROM parents WHERE id = 1",
    ] {
        let mut fixture = Fixture::new(ConstraintTiming::Immediate);
        fixture
            .database
            .execute_sql("INSERT INTO parents VALUES (1)")
            .unwrap();
        fixture
            .database
            .execute_sql("INSERT INTO children VALUES (10, 1)")
            .unwrap();
        let database = &fixture.database;
        let mut transaction = database.begin().unwrap();
        transaction
            .execute_sql(database, "INSERT INTO parents VALUES (2)")
            .unwrap();
        assert!(
            matches!(
                transaction.execute_sql(database, statement),
                Err(DbError::ForeignKeyViolation { .. })
            ),
            "{statement}"
        );
        assert!(matches!(
            transaction.execute_sql(database, "INSERT INTO parents VALUES (7)"),
            Err(DbError::TransactionAborted)
        ));
        assert!(matches!(
            transaction.commit(),
            Err(DbError::TransactionAborted)
        ));
        assert_eq!(
            values(database.scan(fixture.parents, 10).unwrap()),
            [vec![Value::I64(1)]]
        );
        assert_eq!(
            values(database.scan(fixture.children, 10).unwrap()),
            [vec![Value::I64(10), Value::I64(1)]]
        );
        fixture.database.close().unwrap();
    }
}

#[test]
fn sql_batch_cannot_repair_an_immediate_violation_in_a_later_statement() {
    let mut fixture = Fixture::new(ConstraintTiming::Immediate);
    assert!(matches!(
        fixture.database.execute_sql_batch(&[
            "INSERT INTO children VALUES (10, 7)",
            "INSERT INTO parents VALUES (7)",
        ]),
        Err(DbError::ForeignKeyViolation { .. })
    ));
    assert!(
        fixture
            .database
            .scan(fixture.parents, 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .database
            .scan(fixture.children, 10)
            .unwrap()
            .is_empty()
    );
    fixture.database.close().unwrap();
}

#[test]
fn native_mutation_batch_checks_its_complete_atomic_delta() {
    let mut fixture = Fixture::new(ConstraintTiming::Immediate);
    fixture
        .database
        .commit_batch([
            RelationalMutation::Insert {
                table: fixture.children,
                row: child(fixture.children, 10, 7),
            },
            RelationalMutation::Insert {
                table: fixture.parents,
                row: parent(fixture.parents, 7),
            },
        ])
        .unwrap();
    assert_eq!(
        values(fixture.database.scan(fixture.children, 10).unwrap()),
        [vec![Value::I64(10), Value::I64(7)]]
    );
    fixture.database.close().unwrap();
}

#[test]
fn deferred_constraints_allow_intermediate_orphans_but_validate_before_publication() {
    for repair in [false, true] {
        let mut fixture = Fixture::new(ConstraintTiming::DeferredToPublication);
        let database = &fixture.database;
        let mut transaction = database.begin().unwrap();
        transaction
            .execute_sql(database, "INSERT INTO children VALUES (10, 7)")
            .unwrap();
        assert_eq!(
            values(transaction.scan(database, fixture.children, 10).unwrap()),
            [vec![Value::I64(10), Value::I64(7)]]
        );
        if repair {
            transaction
                .execute_sql(database, "INSERT INTO parents VALUES (7)")
                .unwrap();
            transaction.commit().unwrap();
            assert_eq!(
                fixture
                    .database
                    .execute_sql("SELECT id FROM children")
                    .unwrap()
                    .rows,
                [vec![Value::I64(10)]]
            );
        } else {
            assert!(matches!(
                transaction.commit(),
                Err(DbError::ForeignKeyViolation { .. })
            ));
            assert!(database.scan(fixture.children, 10).unwrap().is_empty());
        }
        fixture.database.close().unwrap();
    }
}

#[test]
fn immediate_checks_do_not_resolve_other_deferred_obligations() {
    let mut fixture = Fixture::new(ConstraintTiming::DeferredToPublication);
    fixture.database.execute_sql(
        "CREATE TABLE immediate_children (id BIGINT PRIMARY KEY, parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES parents(id))",
    ).unwrap();
    let database = &fixture.database;
    let mut transaction = database.begin().unwrap();
    transaction
        .execute_sql(database, "INSERT INTO children VALUES (10, 7)")
        .unwrap();
    transaction
        .execute_sql(database, "INSERT INTO parents VALUES (1)")
        .unwrap();
    transaction
        .execute_sql(database, "INSERT INTO immediate_children VALUES (20, 1)")
        .unwrap();
    assert!(matches!(
        transaction.execute_sql(database, "INSERT INTO immediate_children VALUES (21, 99)"),
        Err(DbError::ForeignKeyViolation { constraint: 2, .. })
    ));
    assert!(matches!(
        transaction.commit(),
        Err(DbError::TransactionAborted)
    ));
    assert!(database.scan(fixture.parents, 10).unwrap().is_empty());
    assert!(database.scan(fixture.children, 10).unwrap().is_empty());
    fixture.database.close().unwrap();
}

#[test]
fn multi_row_self_references_are_checked_after_the_complete_sql_statement() {
    let directory = tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    database.execute_sql("CREATE TABLE nodes (id BIGINT PRIMARY KEY, code BIGINT, link BIGINT, UNIQUE (code), FOREIGN KEY (link) REFERENCES nodes (code))").unwrap();
    let mut transaction = database.begin().unwrap();
    for statement in [
        "INSERT INTO nodes VALUES (1, 1, 2), (2, 2, 1)",
        "UPDATE nodes SET code = code + 10, link = link + 10",
        "DELETE FROM nodes",
    ] {
        assert_eq!(
            transaction
                .execute_sql(&database, statement)
                .unwrap()
                .affected_rows,
            2,
            "{statement}"
        );
    }
    transaction.commit().unwrap();
    assert!(
        database
            .execute_sql("SELECT id FROM nodes")
            .unwrap()
            .rows
            .is_empty()
    );
    database.close().unwrap();
}
