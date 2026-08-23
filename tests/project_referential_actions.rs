//! Referential-action conformance across both backends:
//! `CONSTRAINT_TIMING_CONTRACT.md` — ON DELETE {Restrict, Cascade, SetNull},
//! eager cascade staging, bounded depth, and deferred resolution at
//! publication validation.

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, ForeignKeyDefinition, IndexDefinition,
    RelationalBackendConfig, RelationalBackendKind, RelationalDatabase, Row, TableDefinition,
    TableId, Value,
};
use tempfile::tempdir;

const PARENTS: TableId = TableId(60);
const CHILDREN: TableId = TableId(61);
const GRANDCHILDREN: TableId = TableId(62);

fn id_table(id: TableId, name: &str, nullable_link: bool) -> TableDefinition {
    TableDefinition {
        id,
        name: name.to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "parent_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: nullable_link,
            },
        ],
    }
}

fn key_of(table: TableId, id: u64) -> omendb::Key {
    omendb::Key::new(table.0, id)
}

fn row(table: TableId, id: u64, parent: Option<u64>) -> Row {
    Row {
        primary: key_of(table, id),
        values: vec![
            Value::U64(id),
            parent.map(Value::U64).unwrap_or(Value::Null),
        ],
    }
}

fn setup(kind: RelationalBackendKind) -> (tempfile::TempDir, RelationalDatabase) {
    let directory = tempdir().expect("tempdir");
    let config = match kind {
        RelationalBackendKind::Temporary => {
            RelationalBackendConfig::Temporary(omendb::DatabaseConfig {
                directory: directory.path().join("db"),
            })
        }
        RelationalBackendKind::Seer => RelationalBackendConfig::Seer(
            omendb::SeerKernelConfig::new(directory.path().join("db")),
        ),
    };
    let mut database = RelationalDatabase::create(config).expect("create database");
    database
        .create_table(id_table(PARENTS, "parents", false))
        .expect("parents table");
    database
        .create_table(id_table(CHILDREN, "children", false))
        .expect("children table");
    database
        .create_table(id_table(GRANDCHILDREN, "grandchildren", true))
        .expect("grandchildren table");
    for table in [PARENTS, CHILDREN, GRANDCHILDREN] {
        database
            .create_index(IndexDefinition {
                id: omendb::IndexId(table.0),
                table,
                columns: vec![ColumnId(1)],
                unique: true,
            })
            .expect("primary index");
    }
    (directory, database)
}

fn fk(
    id: u64,
    child: TableId,
    parent: TableId,
    on_delete: omendb::ReferentialAction,
) -> ForeignKeyDefinition {
    ForeignKeyDefinition {
        id: ConstraintId(id),
        table: child,
        columns: vec![ColumnId(2)],
        referenced_table: parent,
        referenced_columns: vec![ColumnId(1)],
        on_delete,
        timing: omendb::ConstraintTiming::default(),
    }
}

fn seed(database: &mut RelationalDatabase) {
    database
        .insert(PARENTS, row(PARENTS, 1, Some(0)))
        .expect("parent");
    database
        .insert(CHILDREN, row(CHILDREN, 11, Some(1)))
        .expect("child");
    database
        .insert(GRANDCHILDREN, row(GRANDCHILDREN, 21, Some(11)))
        .expect("grandchild");
}

#[test]
fn restrict_still_rejects_parent_delete_on_both_backends() {
    for kind in [
        RelationalBackendKind::Temporary,
        RelationalBackendKind::Seer,
    ] {
        let (_dir, mut database) = setup(kind);
        seed(&mut database);
        database
            .create_foreign_key(fk(
                1,
                CHILDREN,
                PARENTS,
                omendb::ReferentialAction::Restrict,
            ))
            .expect("restrict fk");
        assert!(matches!(
            database.delete(PARENTS, key_of(PARENTS, 1)),
            Err(omendb::DbError::ForeignKeyViolation { constraint: 1, .. })
        ));
        assert!(
            database
                .get(PARENTS, database.commit_id(), key_of(PARENTS, 1))
                .expect("parent read")
                .is_some()
        );
    }
}

#[test]
fn cascade_deletes_children_and_descendants_on_both_backends() {
    for kind in [
        RelationalBackendKind::Temporary,
        RelationalBackendKind::Seer,
    ] {
        let (_dir, mut database) = setup(kind);
        seed(&mut database);
        database
            .create_foreign_key(fk(1, CHILDREN, PARENTS, omendb::ReferentialAction::Cascade))
            .expect("cascade fk");
        database
            .create_foreign_key(fk(
                2,
                GRANDCHILDREN,
                CHILDREN,
                omendb::ReferentialAction::Cascade,
            ))
            .expect("cascade fk chain");
        database
            .delete(PARENTS, key_of(PARENTS, 1))
            .expect("delete");
        let commit = database.commit_id();
        assert!(
            database
                .scan(PARENTS, commit, 10)
                .expect("parents")
                .is_empty()
        );
        assert!(
            database
                .scan(CHILDREN, commit, 10)
                .expect("children")
                .is_empty()
        );
        assert!(
            database
                .scan(GRANDCHILDREN, commit, 10)
                .expect("grandchildren")
                .is_empty()
        );
    }
}

#[test]
fn set_null_preserves_child_and_clears_reference_on_both_backends() {
    for kind in [
        RelationalBackendKind::Temporary,
        RelationalBackendKind::Seer,
    ] {
        let (_dir, mut database) = setup(kind);
        seed(&mut database);
        // Grandchildren's link column is nullable; children's is not.
        database
            .create_foreign_key(fk(
                1,
                GRANDCHILDREN,
                CHILDREN,
                omendb::ReferentialAction::SetNull,
            ))
            .expect("set null fk");
        database
            .delete(CHILDREN, key_of(CHILDREN, 11))
            .expect("delete child");
        let commit = database.commit_id();
        assert_eq!(
            database
                .get(GRANDCHILDREN, commit, key_of(GRANDCHILDREN, 21))
                .expect("grandchild survives"),
            Some(row(GRANDCHILDREN, 21, None))
        );
    }
}

#[test]
fn set_null_activation_refuses_non_nullable_fk_column() {
    let (_dir, mut database) = setup(RelationalBackendKind::Temporary);
    assert!(matches!(
        database.create_foreign_key(fk(
            1,
            CHILDREN,
            PARENTS,
            omendb::ReferentialAction::SetNull
        )),
        Err(omendb::DbError::InvalidState(reason)) if reason.contains("non-nullable")
    ));
}

#[test]
fn staged_cascade_resolves_when_transaction_reinserts_the_child() {
    for kind in [
        RelationalBackendKind::Temporary,
        RelationalBackendKind::Seer,
    ] {
        let (_dir, mut database) = setup(kind);
        seed(&mut database);
        database
            .create_foreign_key(fk(1, CHILDREN, PARENTS, omendb::ReferentialAction::Cascade))
            .expect("cascade fk");
        let (_, _commit) = database
            .transaction(|database, transaction| {
                transaction.delete(database, PARENTS, key_of(PARENTS, 1))?;
                // The cascaded delete of the child is already staged; the
                // publication fixpoint must accept a re-insert that leaves a
                // consistent final state. Grandchildren's link column is
                // nullable, so a parentless insert is representable.
                transaction.insert(database, GRANDCHILDREN, row(GRANDCHILDREN, 31, None))?;
                Ok(())
            })
            .expect("transaction with staged cascade resolves");
        let commit = database.commit_id();
        assert!(
            database
                .scan(PARENTS, commit, 10)
                .expect("parents")
                .is_empty()
        );
        assert!(
            database
                .scan(CHILDREN, commit, 10)
                .expect("children")
                .is_empty()
        );
        // Only the CHILDREN->PARENTS cascade exists here; grandchild 21 keeps
        // its (now dangling) reference because no constraint governs it.
        assert_eq!(
            database
                .scan(GRANDCHILDREN, commit, 10)
                .expect("grandchildren"),
            vec![
                row(GRANDCHILDREN, 21, Some(11)),
                row(GRANDCHILDREN, 31, None)
            ]
        );
    }
}

#[test]
fn cascade_depth_bound_rejects_deeper_than_max_chains() {
    for kind in [
        RelationalBackendKind::Temporary,
        RelationalBackendKind::Seer,
    ] {
        let (_dir, mut database) = setup(kind);
        // Self-referential chain on one table: row N references N-1.
        const CHAIN: TableId = TableId(70);
        database
            .create_table(TableDefinition {
                id: CHAIN,
                name: "chain".to_owned(),
                columns: vec![
                    ColumnDefinition {
                        id: ColumnId(1),
                        name: "id".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(2),
                        name: "prev".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: true,
                    },
                ],
            })
            .expect("chain table");
        database
            .create_index(IndexDefinition {
                id: omendb::IndexId(70),
                table: CHAIN,
                columns: vec![ColumnId(1)],
                unique: true,
            })
            .expect("chain index");
        database
            .create_foreign_key(ForeignKeyDefinition {
                id: ConstraintId(9),
                table: CHAIN,
                columns: vec![ColumnId(2)],
                referenced_table: CHAIN,
                referenced_columns: vec![ColumnId(1)],
                on_delete: omendb::ReferentialAction::Cascade,
                timing: omendb::ConstraintTiming::default(),
            })
            .expect("self cascade fk");
        for id in 1..=70_u64 {
            // Row 1 anchors the chain with a NULL predecessor; each later row
            // references its predecessor, so every per-commit FK check passes.
            let prev = if id == 1 { None } else { Some(id - 1) };
            database
                .insert(CHAIN, row(CHAIN, id, prev))
                .expect("chain row");
        }
        // Depth 70 exceeds the bound of 64: the delete fails closed and nothing
        // durable changes.
        let before = database.commit_id();
        assert!(matches!(
            database.delete(CHAIN, key_of(CHAIN, 1)),
            Err(omendb::DbError::CascadeDepthExceeded { constraint: 9, .. })
        ));
        assert_eq!(database.commit_id(), before);
        assert_eq!(
            database
                .scan(CHAIN, before, usize::MAX)
                .expect("chain intact")
                .len(),
            70
        );
    }
}
