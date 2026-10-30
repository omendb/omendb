//! SQL-tier schema evolution: ALTER TABLE operations, CREATE/DROP INDEX,
//! and DROP TABLE, with reopen-based durability checks. These tests pin
//! the atomic publication contract: a failed multi-operation ALTER TABLE
//! must leave no partial schema change behind.

// Only `config` is used here; the schema constants are consumed by the
// other suites that share this support module.
#[allow(dead_code)]
mod support;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, ConstraintTiming, DbError, F64,
    ForeignKeyDefinition, IndexDefinition, IndexId, IndexKeyPart, NamedForeignKeyDefinition,
    NamedIndexDefinition, ReferentialAction, RelationalDatabase, RelationalSchemaDefinition,
    SchemaMutation, TableDefinition, TableId, Value,
};
use support::config;
use tempfile::tempdir;

fn setup(directory: &std::path::Path) -> RelationalDatabase {
    // create() requires a path that does not exist yet.
    let path = directory.join("evolution-db");
    let mut database = RelationalDatabase::create(config(&path)).expect("create");
    database
        .execute_sql(
            "CREATE TABLE accounts (
                id BIGINT PRIMARY KEY,
                balance BIGINT,
                state TEXT,
                opened DATE
            )",
        )
        .expect("create table");
    for (id, balance, state) in [(1, 100, "open"), (2, 250, "closed"), (3, 75, "open")] {
        database
            .execute_sql(&format!(
                "INSERT INTO accounts (id, balance, state, opened) VALUES ({id}, {balance}, '{state}', '2026-01-15')"
            ))
            .expect("seed row");
    }
    database
}

fn text_columns_database(directory: &std::path::Path) -> RelationalDatabase {
    let mut database = RelationalDatabase::create(config(&directory.join("text-db"))).unwrap();
    database
        .execute_sql(
            "CREATE TABLE changes (id BIGINT PRIMARY KEY, first TEXT, kept TEXT, amount TEXT)",
        )
        .unwrap();
    database
        .execute_sql("INSERT INTO changes VALUES (1, '101', 'keep', '0007'), (2, NULL, NULL, NULL)")
        .unwrap();
    database
}

#[test]
fn combined_column_changes_preserve_values_and_nullable_tails_after_reopen() {
    for (alter, projection, expected_first) in [
        (
            "DROP COLUMN first, ADD COLUMN extra TEXT",
            "id, kept, amount, extra",
            vec![
                Value::I64(1),
                Value::Text("keep".into()),
                Value::Text("0007".into()),
                Value::Null,
            ],
        ),
        (
            "DROP COLUMN first, DROP COLUMN amount",
            "id, kept",
            vec![Value::I64(1), Value::Text("keep".into())],
        ),
    ] {
        let directory = tempdir().unwrap();
        let mut database = text_columns_database(directory.path());
        database
            .execute_sql(&format!("ALTER TABLE changes {alter}"))
            .unwrap();
        let query = format!("SELECT {projection} FROM changes ORDER BY id");
        let mut null_row = vec![Value::Null; expected_first.len()];
        null_row[0] = Value::I64(2);
        let expected = vec![expected_first, null_row];
        assert_eq!(
            database.execute_sql(&query).unwrap().rows,
            expected,
            "{alter}"
        );
        database.close().unwrap();
        let mut reopened =
            RelationalDatabase::open(config(&directory.path().join("text-db"))).unwrap();
        assert_eq!(
            reopened.execute_sql(&query).unwrap().rows,
            expected,
            "{alter}: reopen"
        );
        reopened.close().unwrap();
    }
}

#[test]
fn type_changes_mixed_with_column_adds_or_drops_preserve_values() {
    for (alter, projection, expected_first) in [
        (
            "ALTER COLUMN amount TYPE DOUBLE PRECISION, DROP COLUMN discarded",
            "id, amount",
            vec![Value::I64(1), Value::Float64(F64::new(7.0))],
        ),
        (
            "ADD COLUMN extra TEXT, ALTER COLUMN amount TYPE DOUBLE PRECISION",
            "id, discarded, amount, extra",
            vec![
                Value::I64(1),
                Value::Text("gone".into()),
                Value::Float64(F64::new(7.0)),
                Value::Null,
            ],
        ),
    ] {
        let directory = tempdir().unwrap();
        let path = directory.path().join("numbers-db");
        let mut database = RelationalDatabase::create(config(&path)).unwrap();
        database
            .execute_sql(
                "CREATE TABLE numbers (id BIGINT PRIMARY KEY, discarded TEXT, amount BIGINT)",
            )
            .unwrap();
        database
            .execute_sql("INSERT INTO numbers VALUES (1, 'gone', 7), (2, NULL, NULL)")
            .unwrap();
        database
            .execute_sql(&format!("ALTER TABLE numbers {alter}"))
            .unwrap();
        let query = format!("SELECT {projection} FROM numbers ORDER BY id");
        let mut null_row = vec![Value::Null; expected_first.len()];
        null_row[0] = Value::I64(2);
        let expected = vec![expected_first, null_row];
        assert_eq!(
            database.execute_sql(&query).unwrap().rows,
            expected,
            "{alter}"
        );
        database.close().unwrap();
        let mut reopened = RelationalDatabase::open(config(&path)).unwrap();
        assert_eq!(
            reopened.execute_sql(&query).unwrap().rows,
            expected,
            "{alter}: reopen"
        );
        reopened.close().unwrap();
    }
}

#[test]
fn native_column_changes_validate_each_intermediate_declaration() {
    let directory = tempdir().unwrap();
    let mut database = text_columns_database(directory.path());
    let table = database
        .catalog()
        .tables()
        .find(|table| table.name == "changes")
        .unwrap();
    let column = table
        .columns
        .iter()
        .find(|column| column.name == "amount")
        .unwrap()
        .id;
    let table = table.id;
    let catalog = database.catalog().clone();
    let commit = database.commit_id();
    // The final TEXT declaration cannot erase an invalid intermediate DATE.
    let result = database.apply_schema_mutations(&[
        SchemaMutation::AlterColumnType {
            table,
            column,
            data_type: ColumnType::Date,
            nullable: true,
        },
        SchemaMutation::AlterColumnType {
            table,
            column,
            data_type: ColumnType::Text,
            nullable: true,
        },
    ]);
    assert!(result.is_err());
    assert_eq!(database.catalog(), &catalog);
    assert_eq!(database.commit_id(), commit);
    let expected = vec![vec![Value::Text("0007".into())], vec![Value::Null]];
    assert_eq!(
        database
            .execute_sql("SELECT amount FROM changes ORDER BY id")
            .unwrap()
            .rows,
        expected
    );
    database.close().unwrap();
    let mut reopened = RelationalDatabase::open(config(&directory.path().join("text-db"))).unwrap();
    assert_eq!(
        reopened
            .execute_sql("SELECT amount FROM changes ORDER BY id")
            .unwrap()
            .rows,
        expected
    );
    reopened.close().unwrap();
}

fn kept_index(database: &RelationalDatabase) -> IndexDefinition {
    let table = database
        .catalog()
        .tables()
        .find(|table| table.name == "changes")
        .unwrap();
    let column = table
        .columns
        .iter()
        .find(|column| column.name == "kept")
        .unwrap()
        .id;
    IndexDefinition {
        id: IndexId(100),
        table: table.id,
        parts: vec![IndexKeyPart::Column(column)],
        unique: false,
        predicate: None,
    }
}

#[test]
fn index_builds_use_the_row_layout_at_their_mutation_step() {
    for build_first in [true, false] {
        let directory = tempdir().unwrap();
        let mut database = text_columns_database(directory.path());
        let index = kept_index(&database);
        let table = index.table;
        let column = database
            .catalog()
            .table(table)
            .unwrap()
            .columns
            .iter()
            .find(|column| column.name == "first")
            .unwrap()
            .id;
        let mut mutations = vec![
            SchemaMutation::AddIndex {
                index,
                name: Some("changes_kept".into()),
            },
            SchemaMutation::DropColumn { table, column },
        ];
        if !build_first {
            mutations.reverse();
        }
        database.apply_schema_mutations(&mutations).unwrap();
        let expected = vec![
            Value::I64(1),
            Value::Text("keep".into()),
            Value::Text("0007".into()),
        ];
        let rows = database
            .index_get(table, IndexId(100), &[Value::Text("keep".into())])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, expected);
        database.close().unwrap();
        let reopened = RelationalDatabase::open(config(&directory.path().join("text-db"))).unwrap();
        let rows = reopened
            .index_get(table, IndexId(100), &[Value::Text("keep".into())])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, expected);
        reopened.close().unwrap();
    }
}

#[test]
fn anonymous_index_registration_is_idempotent_without_losing_recreation() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("text-db");
    let mut database = text_columns_database(directory.path());
    let index = kept_index(&database);
    let add = SchemaMutation::AddIndex {
        index: index.clone(),
        name: None,
    };
    database
        .apply_schema_mutations(&[add.clone(), add.clone()])
        .unwrap();
    database.close().unwrap();
    let mut reopened = RelationalDatabase::open(config(&path)).unwrap();
    // An existing registration is a no-op; a genuine drop must still
    // allow a replacement to be built in this same publication.
    reopened
        .apply_schema_mutations(&[
            add.clone(),
            SchemaMutation::DropIndex { index: index.id },
            add,
        ])
        .unwrap();
    reopened.close().unwrap();
    let reopened = RelationalDatabase::open(config(&path)).unwrap();
    let rows = reopened
        .index_get(index.table, index.id, &[Value::Text("keep".into())])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values,
        vec![
            Value::I64(1),
            Value::Text("101".into()),
            Value::Text("keep".into()),
            Value::Text("0007".into())
        ]
    );
    reopened.close().unwrap();
}

fn new_table_definition() -> TableDefinition {
    TableDefinition {
        id: TableId(200),
        name: "new_table".into(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "id".into(),
            data_type: ColumnType::I64,
            nullable: false,
        }],
    }
}

#[test]
fn new_table_index_definitions_allocate_one_tree_per_catalog_object() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("new-table-db");
    let mut database = RelationalDatabase::create(config(&path)).unwrap();
    let table = new_table_definition();
    let index = NamedIndexDefinition {
        definition: IndexDefinition {
            id: IndexId(100),
            table: table.id,
            parts: vec![IndexKeyPart::Column(ColumnId(1))],
            unique: false,
            predicate: None,
        },
        name: None,
    };
    database
        .create_table_with_schema(
            table,
            RelationalSchemaDefinition {
                indexes: vec![index.clone(), index],
                foreign_keys: vec![],
            },
        )
        .unwrap();
    database.close().unwrap();
    let reopened = RelationalDatabase::open(config(&path)).unwrap();
    assert_eq!(reopened.catalog().indexes().count(), 1);
    reopened.close().unwrap();
}

#[test]
fn new_table_schema_refuses_indexes_owned_by_existing_tables() {
    assert_new_table_schema_owner(false);
}

#[test]
fn new_table_schema_refuses_foreign_keys_owned_by_existing_tables() {
    assert_new_table_schema_owner(true);
}

fn assert_new_table_schema_owner(attach_foreign_key: bool) {
    let directory = tempdir().unwrap();
    let mut database = text_columns_database(directory.path());
    database
        .execute_sql("CREATE TABLE codes (code TEXT PRIMARY KEY)")
        .unwrap();
    let index = kept_index(&database);
    let schema = if attach_foreign_key {
        let parent = database
            .catalog()
            .tables()
            .find(|table| table.name == "codes")
            .unwrap();
        let child = database.catalog().table(index.table).unwrap();
        let column = child
            .columns
            .iter()
            .find(|column| column.name == "amount")
            .unwrap()
            .id;
        // Existing references have no parent: new-table creation must
        // not bypass activation's existing-row validation.
        RelationalSchemaDefinition {
            indexes: vec![],
            foreign_keys: vec![NamedForeignKeyDefinition {
                definition: ForeignKeyDefinition {
                    id: ConstraintId(100),
                    table: child.id,
                    columns: vec![column],
                    referenced_table: parent.id,
                    referenced_columns: vec![parent.columns[0].id],
                    on_delete: ReferentialAction::Restrict,
                    timing: ConstraintTiming::Immediate,
                },
                name: None,
            }],
        }
    } else {
        // A populated table's index cannot be published as empty on
        // the assumption that its owner is the newly created table.
        RelationalSchemaDefinition {
            indexes: vec![NamedIndexDefinition {
                definition: index,
                name: None,
            }],
            foreign_keys: vec![],
        }
    };
    let catalog = database.catalog().clone();
    let commit = database.commit_id();
    assert!(
        database
            .create_table_with_schema(new_table_definition(), schema)
            .is_err()
    );
    assert_eq!(database.catalog(), &catalog);
    assert_eq!(database.commit_id(), commit);
    database.close().unwrap();
    let reopened = RelationalDatabase::open(config(&directory.path().join("text-db"))).unwrap();
    assert_eq!(reopened.catalog(), &catalog);
    reopened.close().unwrap();
}

#[test]
fn dropping_a_table_removes_indexes_added_earlier_in_the_same_batch() {
    let directory = tempdir().unwrap();
    let mut database = text_columns_database(directory.path());
    let index = kept_index(&database);
    let table = index.table;
    database
        .apply_schema_mutations(&[
            SchemaMutation::AddIndex {
                index,
                name: Some("changes_kept".into()),
            },
            SchemaMutation::DropTable { table },
        ])
        .unwrap();
    assert_eq!(database.catalog().tables().count(), 0);
    assert_eq!(database.catalog().indexes().count(), 0);
    database.close().unwrap();
    let reopened = RelationalDatabase::open(config(&directory.path().join("text-db"))).unwrap();
    assert_eq!(reopened.catalog().tables().count(), 0);
    assert_eq!(reopened.catalog().indexes().count(), 0);
    reopened.close().unwrap();
}

#[test]
fn late_conversion_failure_discards_earlier_schema_rewrites() {
    let directory = tempdir().unwrap();
    let mut database = text_columns_database(directory.path());
    let catalog = database.catalog().clone();
    let commit = database.commit_id();
    assert!(
        database
            .execute_sql(
                "ALTER TABLE changes ALTER COLUMN amount TYPE NUMERIC, ALTER COLUMN kept TYPE DATE"
            )
            .is_err()
    );
    assert_eq!(database.catalog(), &catalog);
    assert_eq!(database.commit_id(), commit);
    let expected = vec![
        vec![Value::Text("keep".into()), Value::Text("0007".into())],
        vec![Value::Null, Value::Null],
    ];
    assert_eq!(
        database
            .execute_sql("SELECT kept, amount FROM changes ORDER BY id")
            .unwrap()
            .rows,
        expected
    );
    database.close().unwrap();
    let mut reopened = RelationalDatabase::open(config(&directory.path().join("text-db"))).unwrap();
    assert_eq!(reopened.catalog(), &catalog);
    assert_eq!(
        reopened
            .execute_sql("SELECT kept, amount FROM changes ORDER BY id")
            .unwrap()
            .rows,
        expected
    );
    reopened.close().unwrap();
}

#[test]
fn self_referencing_table_drops_remove_owned_foreign_keys_atomically() {
    for native in [false, true] {
        let directory = tempdir().unwrap();
        let path = directory.path().join("self-drop-db");
        let mut database = RelationalDatabase::create(config(&path)).unwrap();
        database.execute_sql("CREATE TABLE nodes (id BIGINT PRIMARY KEY, parent BIGINT, FOREIGN KEY (parent) REFERENCES nodes(id))").unwrap();
        database
            .execute_sql("INSERT INTO nodes VALUES (1, 1)")
            .unwrap();
        if native {
            let table = database.catalog().tables().next().unwrap().id;
            database
                .apply_schema_mutations(&[SchemaMutation::DropTable { table }])
                .unwrap();
        } else {
            database.execute_sql("DROP TABLE nodes").unwrap();
        }
        assert_eq!(database.catalog().tables().count(), 0);
        assert_eq!(database.catalog().foreign_keys().count(), 0);
        database.close().unwrap();
        let reopened = RelationalDatabase::open(config(&path)).unwrap();
        assert_eq!(reopened.catalog().tables().count(), 0);
        assert_eq!(reopened.catalog().foreign_keys().count(), 0);
        reopened.close().unwrap();
    }
}

#[test]
fn alter_table_renames_columns_and_tables_durably() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("ALTER TABLE accounts RENAME COLUMN balance TO funds")
        .expect("rename column");
    database
        .execute_sql("ALTER TABLE accounts RENAME TO ledger")
        .expect("rename table");

    let rows = database
        .execute_sql("SELECT id, funds FROM ledger WHERE id = 2")
        .expect("query renamed schema");
    assert_eq!(rows.rows, vec![vec![Value::I64(2), Value::I64(250)]]);

    drop(database);
    let mut reopened =
        RelationalDatabase::open(config(&directory.path().join("evolution-db"))).expect("reopen");
    let rows = reopened
        .execute_sql("SELECT funds FROM ledger WHERE id = 2")
        .expect("query after reopen");
    assert_eq!(rows.rows, vec![vec![Value::I64(250)]]);
    // The old names are gone.
    assert!(reopened.execute_sql("SELECT balance FROM ledger").is_err());
}

#[test]
fn alter_table_adds_columns_and_drops_them_durably() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("ALTER TABLE accounts ADD COLUMN note TEXT")
        .expect("add column");
    database
        .execute_sql("UPDATE accounts SET note = 'watch' WHERE id = 1")
        .expect("write new column");
    database
        .execute_sql("ALTER TABLE accounts DROP COLUMN note")
        .expect("drop column");
    // The dropped column is gone from the projection.
    assert!(
        database
            .execute_sql("SELECT note FROM accounts WHERE id = 1")
            .is_err()
    );
    // Row content is intact with the remaining columns.
    let rows = database
        .execute_sql("SELECT id, balance FROM accounts WHERE id = 1")
        .expect("row after drop");
    assert_eq!(rows.rows, vec![vec![Value::I64(1), Value::I64(100)]]);

    drop(database);
    let mut reopened =
        RelationalDatabase::open(config(&directory.path().join("evolution-db"))).expect("reopen");
    let rows = reopened
        .execute_sql("SELECT id, balance, state, opened FROM accounts WHERE id = 1")
        .expect("full row after reopen");
    assert_eq!(rows.rows[0].len(), 4);
}

#[test]
fn alter_table_changes_types_with_value_conversion() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    // balance: BIGINT -> DOUBLE PRECISION rewrites every stored value.
    database
        .execute_sql("ALTER TABLE accounts ALTER COLUMN balance SET DATA TYPE DOUBLE PRECISION")
        .expect("alter type");
    let rows = database
        .execute_sql("SELECT balance FROM accounts WHERE id = 2")
        .expect("converted value");
    assert_eq!(rows.rows, vec![vec![Value::Float64(F64::new(250.0))]]);

    drop(database);
    let mut reopened =
        RelationalDatabase::open(config(&directory.path().join("evolution-db"))).expect("reopen");
    let rows = reopened
        .execute_sql("SELECT balance FROM accounts WHERE id = 2")
        .expect("converted value after reopen");
    assert_eq!(rows.rows, vec![vec![Value::Float64(F64::new(250.0))]]);
}

#[test]
fn alter_table_type_conversion_failure_is_atomic() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    // state holds 'open', which is not a valid date: the conversion must
    // fail and leave the original schema fully intact.
    let before = database.commit_id();
    assert!(
        database
            .execute_sql("ALTER TABLE accounts ALTER COLUMN state SET DATA TYPE DATE")
            .is_err()
    );
    assert_eq!(database.commit_id(), before);
    let rows = database
        .execute_sql("SELECT state FROM accounts WHERE id = 1")
        .expect("original column intact");
    assert_eq!(rows.rows, vec![vec![Value::Text("open".to_owned())]]);
}

#[test]
fn multi_operation_alter_table_is_all_or_nothing() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    // ADD COLUMN works; DROP COLUMN on the primary key fails. Both are in
    // ONE statement, so neither may take effect.
    let before = database.commit_id();
    assert!(
        database
            .execute_sql("ALTER TABLE accounts ADD COLUMN memo TEXT, DROP COLUMN id")
            .is_err()
    );
    assert_eq!(database.commit_id(), before);
    // No memo column was created.
    assert!(database.execute_sql("SELECT memo FROM accounts").is_err());
    // The primary key is intact.
    let rows = database
        .execute_sql("SELECT id FROM accounts WHERE id = 1")
        .expect("primary key intact");
    assert_eq!(rows.rows, vec![vec![Value::I64(1)]]);
}

#[test]
fn alter_table_refuses_constrained_columns() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("CREATE INDEX accounts_state_idx ON accounts (state)")
        .expect("create index");
    // state is now indexed: rename/drop/type-change must all refuse.
    for statement in [
        "ALTER TABLE accounts RENAME COLUMN state TO status",
        "ALTER TABLE accounts DROP COLUMN state",
        "ALTER TABLE accounts ALTER COLUMN state SET DATA TYPE TEXT",
    ] {
        assert!(
            database.execute_sql(statement).is_err(),
            "indexed column change must refuse: {statement}"
        );
    }
    // The primary key is constrained the same way.
    assert!(
        database
            .execute_sql("ALTER TABLE accounts DROP COLUMN id")
            .is_err()
    );
    // Dropping the index frees the column again.
    database
        .execute_sql("DROP INDEX accounts_state_idx")
        .expect("drop index");
    database
        .execute_sql("ALTER TABLE accounts RENAME COLUMN state TO status")
        .expect("rename after index drop");
}

#[test]
fn drop_index_and_drop_table_remove_objects_durably() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("CREATE INDEX accounts_state_idx ON accounts (state)")
        .expect("create index");
    // DROP INDEX IF EXISTS on a missing name is a no-op.
    database
        .execute_sql("DROP INDEX IF EXISTS missing_idx")
        .expect("no-op drop");
    database
        .execute_sql("DROP INDEX accounts_state_idx")
        .expect("drop index");
    assert!(matches!(
        database.execute_sql("DROP INDEX accounts_state_idx"),
        Err(DbError::InvalidState(_))
    ));

    database
        .execute_sql("DROP TABLE accounts")
        .expect("drop table");
    assert!(matches!(
        database.execute_sql("SELECT id FROM accounts"),
        Err(DbError::SqlUndefinedTable { .. })
    ));
    // The durable catalog no longer knows the table.
    drop(database);
    let mut reopened =
        RelationalDatabase::open(config(&directory.path().join("evolution-db"))).expect("reopen");
    assert!(reopened.execute_sql("SELECT id FROM accounts").is_err());
    // And the name is free again.
    reopened
        .execute_sql("CREATE TABLE accounts (id BIGINT PRIMARY KEY, note TEXT)")
        .expect("recreate after drop");
}

#[test]
fn drop_table_refuses_referenced_foreign_keys() {
    let directory = tempdir().expect("tempdir");
    let mut database =
        RelationalDatabase::create(config(&directory.path().join("evolution-db"))).expect("create");
    database
        .execute_sql(
            "CREATE TABLE groups (
                id BIGINT PRIMARY KEY,
                name TEXT
            )",
        )
        .expect("create groups");
    database
        .execute_sql(
            "CREATE TABLE members (
                id BIGINT PRIMARY KEY,
                group_id BIGINT,
                FOREIGN KEY (group_id) REFERENCES groups (id)
            )",
        )
        .expect("create members");
    // The referenced unique index on groups(id) backs members' FK; the
    // drop must refuse while the reference exists.
    assert!(matches!(
        database.execute_sql("DROP TABLE groups"),
        Err(DbError::InvalidState(reason)) if reason.contains("foreign key")
    ));
    // The native path must enforce the same dependency before publication.
    let table = database
        .catalog()
        .tables()
        .find(|table| table.name == "groups")
        .unwrap()
        .id;
    let catalog = database.catalog().clone();
    let commit = database.commit_id();
    assert!(matches!(
        database.apply_schema_mutations(&[SchemaMutation::DropTable { table }]),
        Err(DbError::InvalidState(_))
    ));
    assert_eq!(database.catalog(), &catalog);
    assert_eq!(database.commit_id(), commit);
    // Dropping the referencing table first frees the referenced one.
    database
        .execute_sql("DROP TABLE members")
        .expect("drop referencing table");
    database
        .execute_sql("DROP TABLE groups")
        .expect("drop after reference removed");
}

#[test]
fn alter_table_drop_not_null_relaxes_the_declaration() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("ALTER TABLE accounts ALTER COLUMN state DROP NOT NULL")
        .expect("drop not null");
    // NULL is now accepted.
    database
        .execute_sql("UPDATE accounts SET state = NULL WHERE id = 1")
        .expect("null state");
    let rows = database
        .execute_sql("SELECT id FROM accounts WHERE state IS NULL")
        .expect("null predicate");
    assert_eq!(rows.rows, vec![vec![Value::I64(1)]]);
}
