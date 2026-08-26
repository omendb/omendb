use std::collections::BTreeMap;
use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, CommitId, IndexDefinition, IndexId, Key,
    RelationalDatabase, RelationalDatabaseTransaction, Row, TableDefinition, TableId, Value,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

#[allow(dead_code)]
mod support;

use support::config;

const DOCUMENTS_TABLE: TableId = TableId(10);
const STATUS_INDEX: IndexId = IndexId(10);
const OWNER_INDEX: IndexId = IndexId(11);
const UPDATED_INDEX: IndexId = IndexId(12);
const EXPECTED_R2_SEED_DIGEST: &str =
    "5e2197b32592239ce5f3aa52367eee287ad88c546a74c865493436bbfdc57cac";
const EXPECTED_R2_FINAL_DIGEST: &str =
    "58cb02f07491bc9868ec28d7c88ba15e8566dbd11d5be58672c4699aa78e9b07";
const R2_TRACE: &str = include_str!("fixtures/r2-update-lifecycle-trace.jsonl");

#[derive(Clone, Debug, Eq, PartialEq)]
struct Document {
    status: String,
    owner_id: u64,
    version: u64,
}

type Model = BTreeMap<u64, Document>;

#[derive(Debug, Deserialize)]
struct TraceEvent {
    seq: u64,
    kind: String,
    rows: Option<u64>,
    operations: Option<Vec<Operation>>,
}

#[derive(Debug, Deserialize)]
struct Operation {
    op: String,
    document_id: u64,
    status: Option<String>,
    owner_id: Option<u64>,
}

fn documents_table() -> TableDefinition {
    TableDefinition {
        id: DOCUMENTS_TABLE,
        name: "r2_documents".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "document_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "status".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "owner_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(5),
                name: "payload".to_owned(),
                data_type: ColumnType::Bytes,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(6),
                name: "version".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn install_schema(database: &mut RelationalDatabase) {
    database
        .create_table(documents_table())
        .expect("documents table");
    for index in [
        IndexDefinition {
            id: STATUS_INDEX,
            table: DOCUMENTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(3)],
            unique: false,
        },
        IndexDefinition {
            id: OWNER_INDEX,
            table: DOCUMENTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(4)],
            unique: false,
        },
        IndexDefinition {
            id: UPDATED_INDEX,
            table: DOCUMENTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(6)],
            unique: false,
        },
    ] {
        database.create_index(index).expect("document index");
    }
}

fn document_key(document_id: u64) -> Key {
    Key::new(DOCUMENTS_TABLE.0, document_id)
}

fn document_row(document_id: u64, document: &Document) -> Row {
    Row {
        primary: document_key(document_id),
        values: vec![
            Value::U64(1),
            Value::U64(document_id),
            Value::Text(document.status.clone()),
            Value::U64(document.owner_id),
            Value::Bytes(format!("payload-{document_id}").into_bytes()),
            Value::U64(document.version),
        ],
    }
}

fn seed(database: &mut RelationalDatabase, rows: u64) -> (Model, CommitId) {
    let mut model = Model::new();
    let (_, commit) = database
        .transaction(|database, transaction| {
            for document_id in 0..rows {
                let document = Document {
                    status: "active".to_owned(),
                    owner_id: document_id % 10,
                    version: 0,
                };
                transaction.insert(
                    database,
                    DOCUMENTS_TABLE,
                    document_row(document_id, &document),
                )?;
                model.insert(document_id, document);
            }
            Ok(())
        })
        .expect("seed R2 documents");
    (model, commit)
}

fn stage_operation(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    model: &mut Model,
    operation: &Operation,
) {
    match operation.op.as_str() {
        "update" => {
            let document = model
                .get_mut(&operation.document_id)
                .expect("update target");
            document.status = operation.status.clone().expect("update status");
            document.owner_id = operation.owner_id.expect("update owner");
            document.version += 1;
            transaction
                .update(
                    database,
                    DOCUMENTS_TABLE,
                    document_row(operation.document_id, document),
                )
                .expect("stage update");
        }
        "delete" => {
            assert!(model.remove(&operation.document_id).is_some());
            transaction
                .delete(
                    database,
                    DOCUMENTS_TABLE,
                    document_key(operation.document_id),
                )
                .expect("stage delete");
        }
        operation => panic!("unsupported R2 operation: {operation}"),
    }
}

fn digest_model(model: &Model) -> String {
    let mut canonical = String::new();
    for (document_id, document) in model {
        canonical.push_str(&format!(
            "{document_id}|{}|{}|{}\n",
            document.status, document.owner_id, document.version
        ));
    }
    digest_bytes(canonical.as_bytes())
}

fn digest_database(database: &RelationalDatabase) -> String {
    let mut canonical = String::new();
    for row in database
        .scan(DOCUMENTS_TABLE, usize::MAX)
        .expect("scan documents for digest")
    {
        canonical.push_str(&format!(
            "{}|{}|{}|{}\n",
            value_u64(&row, 1),
            value_text(&row, 2),
            value_u64(&row, 3),
            value_u64(&row, 5),
        ));
    }
    digest_bytes(canonical.as_bytes())
}

fn digest_bytes(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn value_u64(row: &Row, position: usize) -> u64 {
    match row.values.get(position) {
        Some(Value::U64(value)) => *value,
        other => panic!("expected U64 at column {position}, got {other:?}"),
    }
}

fn value_text(row: &Row, position: usize) -> &str {
    match row.values.get(position) {
        Some(Value::Text(value)) => value,
        other => panic!("expected Text at column {position}, got {other:?}"),
    }
}

fn assert_database_matches(database: &RelationalDatabase, model: &Model) {
    let expected = model
        .iter()
        .map(|(document_id, document)| document_row(*document_id, document))
        .collect::<Vec<_>>();
    assert_eq!(
        database
            .scan(DOCUMENTS_TABLE, usize::MAX)
            .expect("scan current R2 state"),
        expected
    );
    assert_eq!(digest_database(database), digest_model(model));

    for index in [STATUS_INDEX, OWNER_INDEX, UPDATED_INDEX] {
        let mut actual = database
            .index_scan(DOCUMENTS_TABLE, index)
            .expect("scan R2 index")
            .into_iter()
            .map(|row| value_u64(&row, 1))
            .collect::<Vec<_>>();
        let mut expected = model.keys().copied().collect::<Vec<_>>();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected, "index {index:?} membership");
    }
}

fn exercise_public_r2(directory: &Path) {
    let config = config(directory);
    let mut database = RelationalDatabase::create(config.clone()).expect("create R2 database");
    install_schema(&mut database);
    let (mut model, _seed_commit) = seed(&mut database, 100);
    assert_eq!(digest_model(&model), EXPECTED_R2_SEED_DIGEST);

    let mut expected_seq = 1;
    for line in R2_TRACE.lines().filter(|line| !line.trim().is_empty()) {
        let event: TraceEvent = serde_json::from_str(line).expect("parse R2 trace event");
        assert_eq!(event.seq, expected_seq);
        expected_seq += 1;
        match event.kind.as_str() {
            "seed" => assert_eq!(event.rows, Some(100)),
            // Retained-snapshot capture is no longer part of the facade.
            "retain" => {}
            "commit" => {
                let mut transaction = database.begin().expect("begin R2 transaction");
                let mut candidate = model.clone();
                for operation in event.operations.as_ref().expect("R2 operations") {
                    stage_operation(&database, &mut transaction, &mut candidate, operation);
                }
                transaction.commit().expect("commit R2 event");
                model = candidate;
            }
            // Historical snapshot scans were removed with the kernel seam.
            "scan" => {}
            // Compaction and lease release moved into SeerDB internals.
            "maintenance" | "release" => {}
            kind => panic!("unsupported R2 event kind: {kind}"),
        }
    }
    assert_eq!(expected_seq, 8);

    let winner = Document {
        status: "winner".to_owned(),
        owner_id: 88,
        version: model.get(&1).expect("winner target").version + 1,
    };
    database
        .update(DOCUMENTS_TABLE, document_row(1, &winner))
        .expect("commit winner");
    database
        .update(DOCUMENTS_TABLE, document_row(1, &winner))
        .expect("commit winner");
    model.insert(1, winner);
    assert_eq!(digest_model(&model), EXPECTED_R2_FINAL_DIGEST);
    assert_database_matches(&database, &model);

    let current = database.commit_id();
    database.close().expect("close R2 database");

    let reopened = RelationalDatabase::open(config).expect("reopen R2 database");
    assert_eq!(reopened.commit_id(), current);
    assert_database_matches(&reopened, &model);
    reopened.close().expect("close reopened R2 database");
}

#[test]
fn public_facade_replays_r2_lifecycle() {
    let temporary = tempdir().expect("temporary R2 directory");
    exercise_public_r2(&temporary.path().join("temporary"));
}
