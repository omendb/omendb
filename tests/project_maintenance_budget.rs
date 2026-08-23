use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, IndexDefinition, IndexId, Key,
    OperationControl, RelationalBackendConfig, RelationalBackendKind, RelationalCompactionBudget,
    RelationalDatabaseConfig, RelationalDatabaseSession, RelationalSessionConfig, Row,
    SeerKernelConfig, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

const TABLE: TableId = TableId(80);
const VALUE_INDEX: IndexId = IndexId(80);

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

fn table() -> TableDefinition {
    TableDefinition {
        id: TABLE,
        name: "maintenance_items".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "value".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn row(id: u64, value: u64) -> Row {
    Row {
        primary: Key::new(TABLE.0, id),
        values: vec![Value::U64(id), Value::U64(value)],
    }
}

fn exercise(kind: RelationalBackendKind, directory: &Path) {
    let session = RelationalDatabaseSession::create(
        RelationalDatabaseConfig::new(config(kind, directory)).with_session_config(
            RelationalSessionConfig {
                max_in_flight: 2,
                ..RelationalSessionConfig::default()
            },
        ),
    )
    .expect("create session");
    let control = OperationControl::default();
    session.create_table(&control, table()).expect("table");
    session
        .create_index(
            &control,
            IndexDefinition {
                id: VALUE_INDEX,
                table: TABLE,
                columns: vec![ColumnId(1), ColumnId(2)],
                unique: false,
            },
        )
        .expect("index");
    for id in 0..4 {
        session.insert(&control, TABLE, row(id, 0)).expect("insert");
        session.update(&control, TABLE, row(id, 1)).expect("update");
    }

    let budget = RelationalCompactionBudget::new(1);
    let report = session
        .compact_with_budget(&control, budget)
        .expect("bounded compaction");
    assert_eq!(report.budget, budget);
    assert!(report.work_units_consumed <= 1);
    assert_eq!(report.before.backend, kind);
    assert_eq!(report.after.backend, kind);
    if kind == RelationalBackendKind::Temporary {
        assert_eq!(
            report.row_keys_considered.unwrap_or_default()
                + report.index_keys_considered.unwrap_or_default(),
            report.work_units_consumed
        );
    } else {
        assert_eq!(
            report.relocated_pages.unwrap_or_default(),
            report.work_units_consumed
        );
    }

    session
        .verify(&control)
        .expect("verify after bounded compaction");
    session.close().expect("close");
}

#[test]
fn bounded_compaction_is_backend_neutral_at_the_project_session() {
    let temporary = tempdir().expect("temporary directory");
    exercise(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
