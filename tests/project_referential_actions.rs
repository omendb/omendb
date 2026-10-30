//! Direct-adapter referential actions, bounded cascade staging and FK
//! certification. Statement timing has separate coverage in
//! project_constraint_timing.rs; these tests do not qualify vNext.

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, ForeignKeyDefinition, IndexDefinition,
    IndexKeyPart, RelationalBackendConfig, RelationalDatabase, Row, TableDefinition, TableId,
    Value,
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

fn setup() -> (tempfile::TempDir, RelationalDatabase) {
    let directory = tempdir().expect("tempdir");
    let config = RelationalBackendConfig::new(directory.path().join("db"));
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
                parts: vec![IndexKeyPart::Column(ColumnId(1))],
                predicate: None,
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
fn composite_foreign_key_maps_values_into_supporting_index_order() {
    // Existing coverage uses the same column order on both sides. Exercise
    // reversed references during both constraint creation and later DML.
    for seed_before_constraint in [false, true] {
        let directory = tempdir().expect("tempdir");
        let config = RelationalBackendConfig::new(directory.path().join("db"));
        let mut database = RelationalDatabase::create(config.clone()).expect("create");
        database
            .execute_sql("CREATE TABLE parents (a BIGINT, b BIGINT, PRIMARY KEY (a, b))")
            .expect("parents");
        database
            .execute_sql("CREATE TABLE children (id BIGINT PRIMARY KEY, x BIGINT, y BIGINT)")
            .expect("children");
        database
            .execute_sql("INSERT INTO parents VALUES (1, 2)")
            .expect("parent");
        if seed_before_constraint {
            database
                .execute_sql("INSERT INTO children VALUES (1, 2, 1)")
                .expect("unconstrained child");
        }
        database
            .execute_sql(
                "ALTER TABLE children ADD CONSTRAINT reversed_parent \
                 FOREIGN KEY (x, y) REFERENCES parents (b, a)",
            )
            .expect("valid reversed reference");
        if !seed_before_constraint {
            database
                .execute_sql("INSERT INTO children VALUES (1, 2, 1)")
                .expect("matching parent in referenced-column order");
        }
        assert!(matches!(
            database.execute_sql("INSERT INTO children VALUES (2, 1, 2)"),
            Err(omendb::DbError::ForeignKeyViolation { .. })
        ));
        database.close().expect("close");
        let mut reopened = RelationalDatabase::open(config).expect("reopen");
        assert_eq!(
            reopened
                .execute_sql("SELECT id, x, y FROM children")
                .expect("retained valid child")
                .rows,
            vec![vec![Value::I64(1), Value::I64(2), Value::I64(1)]]
        );
        assert!(matches!(
            reopened.execute_sql("INSERT INTO children VALUES (2, 1, 2)"),
            Err(omendb::DbError::ForeignKeyViolation { .. })
        ));
        reopened.close().expect("close reopened");
    }
}

#[test]
fn dropping_the_last_fk_supporting_index_is_refused_atomically() {
    let directory = tempdir().expect("tempdir");
    let config = RelationalBackendConfig::new(directory.path().join("db"));
    let mut database = RelationalDatabase::create(config.clone()).expect("create");
    for sql in [
        "CREATE TABLE parents (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
        "CREATE UNIQUE INDEX parent_key ON parents (a, b)",
        "CREATE UNIQUE INDEX parent_backup ON parents (b, a)",
        "CREATE TABLE children (id BIGINT PRIMARY KEY, x BIGINT, y BIGINT, \
         FOREIGN KEY (x, y) REFERENCES parents (b, a))",
        "INSERT INTO parents VALUES (1, 1, 2)",
        "INSERT INTO children VALUES (1, 2, 1)",
        "DROP INDEX parent_key",
    ] {
        database.execute_sql(sql).expect(sql);
    }
    let before = database.commit_id();
    assert!(database.execute_sql("DROP INDEX parent_backup").is_err());
    assert_eq!(database.commit_id(), before);
    database.close().expect("close");
    let mut reopened = RelationalDatabase::open(config).expect("reopen");
    reopened
        .execute_sql("INSERT INTO parents VALUES (2, 5, 6)")
        .expect("parent insert after refused drop");
    reopened
        .execute_sql("INSERT INTO children VALUES (2, 6, 5)")
        .expect("remaining supporting index enforces valid references");
    assert!(matches!(
        reopened.execute_sql("INSERT INTO children VALUES (3, 5, 6)"),
        Err(omendb::DbError::ForeignKeyViolation { .. })
    ));
    reopened.close().expect("close reopened");
}

#[test]
fn restrict_still_rejects_parent_delete_on_both_backends() {
    let (_dir, mut database) = setup();
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
            .get(PARENTS, key_of(PARENTS, 1))
            .expect("parent read")
            .is_some()
    );
}

#[test]
fn cascade_deletes_children_and_descendants_on_both_backends() {
    let (_dir, mut database) = setup();
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
    assert!(database.scan(PARENTS, 10).expect("parents").is_empty());
    assert!(database.scan(CHILDREN, 10).expect("children").is_empty());
    assert!(
        database
            .scan(GRANDCHILDREN, 10)
            .expect("grandchildren")
            .is_empty()
    );
}

#[test]
fn set_null_preserves_child_and_clears_reference_on_both_backends() {
    let (_dir, mut database) = setup();
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
    assert_eq!(
        database
            .get(GRANDCHILDREN, key_of(GRANDCHILDREN, 21))
            .expect("grandchild survives"),
        Some(row(GRANDCHILDREN, 21, None))
    );
}

#[test]
fn set_null_activation_refuses_non_nullable_fk_column() {
    let (_dir, mut database) = setup();
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
fn cascade_effects_are_readable_before_transaction_commit() {
    let (_dir, mut database) = setup();
    seed(&mut database);
    database
        .create_foreign_key(fk(1, CHILDREN, PARENTS, omendb::ReferentialAction::Cascade))
        .expect("cascade fk");
    let (_, _commit) = database
        .transaction(|database, transaction| {
            transaction.delete(database, PARENTS, key_of(PARENTS, 1))?;
            // The complete delete, including its cascade, passed immediate
            // validation. Later operations see the staged child deletion.
            assert!(transaction.scan(database, CHILDREN, 10)?.is_empty());
            // No FK governs grandchildren here; a parentless insert is valid.
            transaction.insert(database, GRANDCHILDREN, row(GRANDCHILDREN, 31, None))?;
            Ok(())
        })
        .expect("transaction with staged cascade");
    assert!(database.scan(PARENTS, 10).expect("parents").is_empty());
    assert!(database.scan(CHILDREN, 10).expect("children").is_empty());
    // Only the CHILDREN->PARENTS cascade exists here; grandchild 21 keeps
    // its (now dangling) reference because no constraint governs it.
    assert_eq!(
        database.scan(GRANDCHILDREN, 10).expect("grandchildren"),
        vec![
            row(GRANDCHILDREN, 21, Some(11)),
            row(GRANDCHILDREN, 31, None)
        ]
    );
}

#[test]
fn cascade_depth_bound_rejects_deeper_than_max_chains() {
    let (_dir, mut database) = setup();
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
            parts: vec![IndexKeyPart::Column(ColumnId(1))],
            predicate: None,
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
            .scan(CHAIN, usize::MAX)
            .expect("chain intact")
            .len(),
        70
    );

    // Both native delete entry points must retain the operation failure, not
    // merely rely on a later FK check to catch the partly staged cascade.
    for legacy_key in [true, false] {
        let mut transaction = database.begin().expect("begin native delete");
        let result = if legacy_key {
            transaction.delete(&database, CHAIN, key_of(CHAIN, 1))
        } else {
            transaction.delete_row(&database, CHAIN, row(CHAIN, 1, None))
        };
        assert!(matches!(
            result,
            Err(omendb::DbError::CascadeDepthExceeded { constraint: 9, .. })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(omendb::DbError::TransactionAborted)
        ));
        assert_eq!(database.commit_id(), before);
        assert_eq!(database.scan(CHAIN, usize::MAX).unwrap().len(), 70);
    }
}

#[test]
fn overlapping_parent_delete_and_child_insert_cannot_both_commit() {
    fn database_with_parent_and_restrict_fk() -> (tempfile::TempDir, RelationalDatabase) {
        let (directory, mut database) = setup();
        database
            .insert(PARENTS, row(PARENTS, 1, Some(0)))
            .expect("parent");
        database
            .create_foreign_key(fk(
                1,
                CHILDREN,
                PARENTS,
                omendb::ReferentialAction::Restrict,
            ))
            .expect("restrict fk");
        (directory, database)
    }

    // Child publishes first. Parent deletion validated an older child
    // snapshot, so the child-table range dependency must reject it.
    {
        let (_directory, database) = database_with_parent_and_restrict_fk();
        let mut child = database.begin().expect("child transaction");
        let mut parent = database.begin().expect("parent transaction");
        child
            .insert(&database, CHILDREN, row(CHILDREN, 12, Some(1)))
            .expect("stage child");
        parent
            .delete(&database, PARENTS, key_of(PARENTS, 1))
            .expect("stage parent delete");

        child.commit().expect("child commits");
        assert!(matches!(
            parent.commit(),
            Err(omendb::DbError::SerializationConflict { .. })
        ));
        assert!(
            database
                .get(PARENTS, key_of(PARENTS, 1))
                .expect("parent read")
                .is_some()
        );
        assert!(
            database
                .get(CHILDREN, key_of(CHILDREN, 12))
                .expect("child read")
                .is_some()
        );
    }

    // Parent publishes first. Child insertion validated an older
    // referenced-index snapshot, so that range dependency must reject it.
    {
        let (_directory, database) = database_with_parent_and_restrict_fk();
        let mut child = database.begin().expect("child transaction");
        let mut parent = database.begin().expect("parent transaction");
        child
            .insert(&database, CHILDREN, row(CHILDREN, 12, Some(1)))
            .expect("stage child");
        parent
            .delete(&database, PARENTS, key_of(PARENTS, 1))
            .expect("stage parent delete");

        parent.commit().expect("parent commits");
        assert!(matches!(
            child.commit(),
            Err(omendb::DbError::SerializationConflict { .. })
        ));
        assert!(
            database
                .get(PARENTS, key_of(PARENTS, 1))
                .expect("parent read")
                .is_none()
        );
        assert!(
            database
                .get(CHILDREN, key_of(CHILDREN, 12))
                .expect("child read")
                .is_none()
        );
    }
}

#[test]
fn foreign_key_activation_and_overlapping_writes_cannot_both_commit() {
    let (_directory, mut database) = setup();
    database.insert(PARENTS, row(PARENTS, 1, Some(0))).unwrap();
    let mut seed = database.begin().unwrap();
    // Enough references to overlap a real validation scan without test hooks
    // or timing assumptions. Either publication order is valid, never both.
    for id in 1..=500 {
        seed.insert(&database, CHILDREN, row(CHILDREN, id, Some(1)))
            .unwrap();
    }
    seed.commit().unwrap();
    for named in [false, true] {
        for delete_parent in [false, true] {
            for _ in 0..8 {
                let mut writer = database.begin().unwrap();
                if delete_parent {
                    writer
                        .delete(&database, PARENTS, key_of(PARENTS, 1))
                        .unwrap();
                } else {
                    writer
                        .insert(&database, CHILDREN, row(CHILDREN, 1000, Some(2)))
                        .unwrap();
                }
                let barrier = std::sync::Barrier::new(2);
                let (activation, writing) = std::thread::scope(|scope| {
                    let writer = scope.spawn(|| {
                        barrier.wait();
                        writer.commit()
                    });
                    barrier.wait();
                    let definition = fk(1, CHILDREN, PARENTS, omendb::ReferentialAction::Restrict);
                    let activation = if named {
                        database.create_named_foreign_key(definition, "child_parent".into())
                    } else {
                        database.create_foreign_key(definition)
                    };
                    (activation, writer.join().unwrap())
                });
                assert!(
                    activation.is_err() || writing.is_err(),
                    "activation and invalidating write both committed (named={named}, delete_parent={delete_parent})"
                );
                if let Err(error) = &activation {
                    assert!(
                        matches!(
                            error,
                            omendb::DbError::ForeignKeyViolation { .. }
                                | omendb::DbError::SerializationConflict { .. }
                        ),
                        "unexpected activation failure: {error:?}"
                    );
                }
                if let Err(error) = &writing {
                    assert!(
                        matches!(error, omendb::DbError::SerializationConflict { .. }),
                        "unexpected writer failure: {error:?}"
                    );
                }
                // Restore valid, unconstrained state regardless of which
                // operation won, before trying another interleaving.
                if writing.is_ok() {
                    if delete_parent {
                        database.insert(PARENTS, row(PARENTS, 1, Some(0))).unwrap();
                    } else {
                        database.delete(CHILDREN, key_of(CHILDREN, 1000)).unwrap();
                    }
                }
                if activation.is_ok() {
                    database
                        .apply_schema_mutations(&[omendb::SchemaMutation::DropForeignKey {
                            constraint: ConstraintId(1),
                        }])
                        .unwrap();
                }
            }
        }
    }
}

#[test]
fn transaction_started_before_fk_activation_cannot_publish_dangling_reference() {
    let (_directory, mut database) = setup();
    seed(&mut database);

    let mut stale = database.begin().expect("stale transaction");
    stale
        .delete(&database, PARENTS, key_of(PARENTS, 1))
        .expect("stage parent delete");

    database
        .create_foreign_key(fk(
            1,
            CHILDREN,
            PARENTS,
            omendb::ReferentialAction::Restrict,
        ))
        .expect("activate restrict fk");

    assert!(matches!(
        stale.commit(),
        Err(omendb::DbError::SerializationConflict { .. })
    ));
    assert!(
        database
            .get(PARENTS, key_of(PARENTS, 1))
            .expect("parent read")
            .is_some()
    );
    assert!(
        database
            .get(CHILDREN, key_of(CHILDREN, 11))
            .expect("child read")
            .is_some()
    );
}
