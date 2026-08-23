use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, DatabaseConfig, DbError,
    ForeignKeyDefinition, IndexDefinition, IndexId, Key, NamedForeignKeyDefinition,
    NamedIndexDefinition, OperationControl, RelationalBackendConfig, RelationalBackendKind,
    RelationalDatabaseConfig, RelationalDatabaseSession, RelationalSchemaDefinition,
    RelationalSessionConfig, RowIdentity, SeerKernelConfig, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

const PARENTS: TableId = TableId(40);
const CHILDREN: TableId = TableId(41);
const SHIPMENTS: TableId = TableId(42);
const PARENT_ID_INDEX: IndexId = IndexId(40);
const CHILD_STATE_INDEX: IndexId = IndexId(41);
const CHILD_ITEM_INDEX: IndexId = IndexId(42);
const CHILD_PARENT_FK: ConstraintId = ConstraintId(40);
const SHIPMENT_PARENT_FK: ConstraintId = ConstraintId(41);

type SchemaObservation = (
    Vec<ColumnId>,
    Vec<Option<String>>,
    Vec<omendb::Row>,
    Vec<omendb::Row>,
    Option<omendb::Row>,
);

fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalDatabaseConfig {
    let backend = match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    };
    RelationalDatabaseConfig::new(backend).with_session_config(RelationalSessionConfig {
        max_in_flight: 2,
        ..RelationalSessionConfig::default()
    })
}

fn parents_table() -> TableDefinition {
    TableDefinition {
        id: PARENTS,
        name: "parents".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "parent_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "label".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn children_table() -> TableDefinition {
    TableDefinition {
        id: CHILDREN,
        name: "children".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "parent_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "item_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "state".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn shipments_table() -> TableDefinition {
    TableDefinition {
        id: SHIPMENTS,
        name: "shipments".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "shipment_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "parent_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn parents_schema() -> RelationalSchemaDefinition {
    RelationalSchemaDefinition {
        indexes: vec![NamedIndexDefinition {
            definition: IndexDefinition {
                id: PARENT_ID_INDEX,
                table: PARENTS,
                columns: vec![ColumnId(1)],
                unique: true,
            },
            name: Some("parents_parent_id".to_owned()),
        }],
        foreign_keys: Vec::new(),
    }
}

fn children_schema() -> RelationalSchemaDefinition {
    RelationalSchemaDefinition {
        indexes: vec![NamedIndexDefinition {
            definition: IndexDefinition {
                id: CHILD_STATE_INDEX,
                table: CHILDREN,
                columns: vec![ColumnId(3)],
                unique: false,
            },
            name: Some("children_state".to_owned()),
        }],
        foreign_keys: vec![NamedForeignKeyDefinition {
            definition: ForeignKeyDefinition {
                id: CHILD_PARENT_FK,
                table: CHILDREN,
                columns: vec![ColumnId(1)],
                referenced_table: PARENTS,
                referenced_columns: vec![ColumnId(1)],
                on_delete: omendb::ReferentialAction::default(),
                timing: omendb::ConstraintTiming::default(),
            },
            name: Some("children_parent".to_owned()),
        }],
    }
}

fn parent_row() -> omendb::Row {
    omendb::Row {
        primary: Key::new(PARENTS.0, 7),
        values: vec![Value::U64(7), Value::Text("root".to_owned())],
    }
}

fn child_row() -> omendb::Row {
    omendb::Row {
        primary: Key::new(CHILDREN.0, 700),
        values: vec![
            Value::U64(7),
            Value::U64(700),
            Value::Text("available".to_owned()),
        ],
    }
}

fn shipment_row() -> omendb::Row {
    omendb::Row {
        primary: Key::new(SHIPMENTS.0, 900),
        values: vec![Value::U64(900), Value::U64(7)],
    }
}

fn child_identity() -> RowIdentity {
    RowIdentity::new(
        CHILDREN,
        vec![ColumnId(1), ColumnId(2)],
        vec![Value::U64(7), Value::U64(700)],
    )
    .expect("child identity")
}

fn observe(session: &RelationalDatabaseSession, control: &OperationControl) -> SchemaObservation {
    let metadata = session
        .read(control, |database| {
            let catalog = database.catalog();
            Ok((
                catalog
                    .primary_key(CHILDREN)
                    .map_or_else(Vec::new, <[ColumnId]>::to_vec),
                vec![
                    catalog.index_name(PARENT_ID_INDEX).map(str::to_owned),
                    catalog.index_name(CHILD_STATE_INDEX).map(str::to_owned),
                    catalog.index_name(CHILD_ITEM_INDEX).map(str::to_owned),
                    catalog.foreign_key_name(CHILD_PARENT_FK).map(str::to_owned),
                    catalog
                        .foreign_key_name(SHIPMENT_PARENT_FK)
                        .map(str::to_owned),
                ],
            ))
        })
        .expect("read schema metadata");
    let snapshot = session.commit_id(control).expect("read commit");
    let child_rows = session
        .scan(control, CHILDREN, snapshot, usize::MAX)
        .expect("scan children");
    let available_rows = session
        .index_get(
            control,
            CHILDREN,
            snapshot,
            CHILD_STATE_INDEX,
            &[Value::Text("available".to_owned())],
        )
        .expect("read child state index");
    let identity_row = session
        .get_by_identity(control, CHILDREN, snapshot, &child_identity())
        .expect("read child identity");
    (
        metadata.0,
        metadata.1,
        child_rows,
        available_rows,
        identity_row,
    )
}

fn exercise(kind: RelationalBackendKind, directory: &Path) -> SchemaObservation {
    let database_config = config(kind, directory);
    let session = RelationalDatabaseSession::create(database_config.clone()).expect("create");
    let control = OperationControl::default();

    session
        .create_table_with_schema(&control, parents_table(), parents_schema())
        .expect("create parent schema");
    session
        .create_table_with_schema_and_primary_key(
            &control,
            children_table(),
            Some(vec![ColumnId(1), ColumnId(2)]),
            children_schema(),
        )
        .expect("create child schema");
    session
        .create_table(&control, shipments_table())
        .expect("create shipment table");
    session
        .create_named_index(
            &control,
            IndexDefinition {
                id: CHILD_ITEM_INDEX,
                table: CHILDREN,
                columns: vec![ColumnId(2)],
                unique: false,
            },
            "children_item_id".to_owned(),
        )
        .expect("create named child index");
    session
        .create_named_foreign_key(
            &control,
            ForeignKeyDefinition {
                id: SHIPMENT_PARENT_FK,
                table: SHIPMENTS,
                columns: vec![ColumnId(2)],
                referenced_table: PARENTS,
                referenced_columns: vec![ColumnId(1)],
                on_delete: omendb::ReferentialAction::default(),
                timing: omendb::ConstraintTiming::default(),
            },
            "shipments_parent".to_owned(),
        )
        .expect("create named shipment foreign key");

    session
        .transaction(&control, |database, transaction| {
            transaction.insert(database, PARENTS, parent_row())?;
            transaction.insert(database, CHILDREN, child_row())?;
            transaction.insert(database, SHIPMENTS, shipment_row())?;

            assert!(
                transaction
                    .get_by_identity(database, CHILDREN, &child_identity())?
                    .is_some()
            );
            transaction.delete_row(database, CHILDREN, child_row())?;
            assert_eq!(
                transaction.get_by_identity(database, CHILDREN, &child_identity())?,
                None
            );
            transaction.insert(database, CHILDREN, child_row())?;
            Ok::<_, omendb::DbError>(())
        })
        .expect("insert related rows");

    let before_reopen = observe(&session, &control);
    assert!(matches!(
        session.get_by_identity(
            &control,
            CHILDREN,
            session.commit_id(&control).expect("read commit"),
            &RowIdentity::new(PARENTS, vec![ColumnId(1)], vec![Value::U64(7)])
                .expect("wrong-table identity"),
        ),
        Err(DbError::InvalidState(_))
    ));
    session.close().expect("close");

    let reopened = RelationalDatabaseSession::open(database_config).expect("reopen");
    let after_reopen = observe(&reopened, &control);
    reopened.close().expect("close reopened");
    assert_eq!(before_reopen, after_reopen);
    after_reopen
}

#[test]
fn public_session_schema_publication_matches_across_backends_and_reopens() {
    let temporary = tempdir().expect("temporary directory");
    let temporary_observation = exercise(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    let seer_observation = exercise(RelationalBackendKind::Seer, &seer.path().join("seer"));

    assert_eq!(temporary_observation, seer_observation);
}
