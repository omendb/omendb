use std::collections::BTreeMap;
use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, CommitId, DbError, IndexDefinition, IndexId, Key,
    RelationalBackendKind, RelationalDatabase, RelationalDatabaseTransaction, Row, TableDefinition,
    TableId, TransactionErrorClass, Value,
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
const CHURN_ROWS: u64 = 256;
const CHURN_BATCHES: usize = 100;
const CHURN_BATCH_SIZE: usize = 100;
const CHURN_REOPEN_INTERVAL: usize = 10;
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
    snapshot: Option<String>,
    name: Option<String>,
    expect: Option<String>,
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

fn digest_database(database: &RelationalDatabase, snapshot: CommitId) -> String {
    let mut canonical = String::new();
    for row in database
        .scan(DOCUMENTS_TABLE, snapshot, usize::MAX)
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

fn assert_database_matches(database: &RelationalDatabase, model: &Model, snapshot: CommitId) {
    let expected = model
        .iter()
        .map(|(document_id, document)| document_row(*document_id, document))
        .collect::<Vec<_>>();
    assert_eq!(
        database
            .scan(DOCUMENTS_TABLE, snapshot, usize::MAX)
            .expect("scan current R2 state"),
        expected
    );
    assert_eq!(digest_database(database, snapshot), digest_model(model));

    for index in [STATUS_INDEX, OWNER_INDEX, UPDATED_INDEX] {
        let mut actual = database
            .index_scan(DOCUMENTS_TABLE, snapshot, index, None, None, usize::MAX)
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

fn exercise_public_r2(kind: RelationalBackendKind, directory: &Path) {
    let config = config(kind, directory);
    let mut database = RelationalDatabase::create(config.clone()).expect("create R2 database");
    install_schema(&mut database);
    let (mut model, seed_commit) = seed(&mut database, 100);
    assert_eq!(digest_model(&model), EXPECTED_R2_SEED_DIGEST);
    let retained_model = model.clone();
    let retained_lease = database.retain(seed_commit).expect("retain R2 seed");

    let mut expected_seq = 1;
    let mut retained_lease = Some(retained_lease);
    for line in R2_TRACE.lines().filter(|line| !line.trim().is_empty()) {
        let event: TraceEvent = serde_json::from_str(line).expect("parse R2 trace event");
        assert_eq!(event.seq, expected_seq);
        expected_seq += 1;
        match event.kind.as_str() {
            "seed" => assert_eq!(event.rows, Some(100)),
            "retain" => {
                assert_eq!(event.snapshot.as_deref(), Some("current"));
                assert_eq!(event.name.as_deref(), Some("r2-snap-1"));
            }
            "commit" => {
                let mut transaction = database.begin().expect("begin R2 transaction");
                let mut candidate = model.clone();
                for operation in event.operations.as_ref().expect("R2 operations") {
                    stage_operation(&database, &mut transaction, &mut candidate, operation);
                }
                transaction.commit(&mut database).expect("commit R2 event");
                model = candidate;
            }
            "scan" => {
                assert_eq!(event.expect.as_deref(), Some("state-digest-0"));
                assert_eq!(
                    digest_database(&database, seed_commit),
                    digest_model(&retained_model)
                );
            }
            "maintenance" => {
                let metrics_before = database.metrics().expect("R2 metrics before compact");
                let report = database.compact().expect("compact R2 state");
                let metrics_after = database.metrics().expect("R2 metrics after compact");
                assert_eq!(report.before.commit, report.after.commit);
                assert_eq!(report.before.commit, database.commit_id());
                assert_eq!(metrics_after.commit, database.commit_id());
                assert_eq!(metrics_after.backend, kind);
                match kind {
                    RelationalBackendKind::Temporary => {
                        assert!(report.row_keys_considered.is_some());
                        assert!(report.index_keys_considered.is_some());
                    }
                    RelationalBackendKind::Seer => {
                        assert!(report.data_bytes_before.is_some());
                        assert!(report.data_bytes_after.is_some());
                        assert!(metrics_after.syncs >= metrics_before.syncs);
                    }
                }
                assert_database_matches(&database, &model, database.commit_id());
                assert_eq!(
                    digest_database(&database, seed_commit),
                    digest_model(&retained_model)
                );
            }
            "release" => {
                assert_eq!(event.snapshot.as_deref(), Some("r2-snap-1"));
                database
                    .release(retained_lease.take().expect("R2 retained lease"))
                    .expect("release R2 snapshot");
                assert!(
                    database
                        .scan(DOCUMENTS_TABLE, seed_commit, usize::MAX)
                        .is_err()
                );
            }
            kind => panic!("unsupported R2 event kind: {kind}"),
        }
    }
    assert_eq!(expected_seq, 8);

    let mut stale = database.begin().expect("begin stale R2 writer");
    let stale_document = Document {
        status: "stale".to_owned(),
        owner_id: 99,
        version: model.get(&2).expect("stale target").version + 1,
    };
    stale
        .update(&database, DOCUMENTS_TABLE, document_row(2, &stale_document))
        .expect("stage stale disjoint write");
    let winner = Document {
        status: "winner".to_owned(),
        owner_id: 88,
        version: model.get(&1).expect("winner target").version + 1,
    };
    database
        .update(DOCUMENTS_TABLE, document_row(1, &winner))
        .expect("commit winner");
    model.insert(1, winner);
    let error = stale
        .commit(&mut database)
        .expect_err("stale disjoint writer must be rejected");
    assert!(matches!(error, DbError::SerializationConflict { .. }));
    assert_eq!(
        error.transaction_class(),
        TransactionErrorClass::SerializationRetry
    );
    assert_eq!(digest_model(&model), EXPECTED_R2_FINAL_DIGEST);
    assert_database_matches(&database, &model, database.commit_id());

    database.verify().expect("verify R2 state");
    database.checkpoint().expect("checkpoint R2 state");
    let current = database.commit_id();
    database.close().expect("close R2 database");

    let mut reopened = RelationalDatabase::open(config).expect("reopen R2 database");
    assert_eq!(reopened.commit_id(), current);
    assert_database_matches(&reopened, &model, current);
    reopened.verify().expect("verify reopened R2 state");
    reopened.close().expect("close reopened R2 database");
}

fn exercise_public_r2_churn(kind: RelationalBackendKind, directory: &Path) {
    let config = config(kind, directory);
    let mut database = RelationalDatabase::create(config.clone()).expect("create churn database");
    install_schema(&mut database);
    let (mut model, seed_commit) = seed(&mut database, CHURN_ROWS);
    let seed_model = model.clone();
    let mut seed_lease = Some(
        database
            .retain(seed_commit)
            .expect("retain churn seed snapshot"),
    );

    for batch in 0..CHURN_BATCHES {
        let mut transaction = database.begin().expect("begin churn batch");
        let mut candidate = model.clone();
        for offset in 0..CHURN_BATCH_SIZE {
            let operation = batch * CHURN_BATCH_SIZE + offset;
            let document_id = ((operation * 37 + 11) % CHURN_ROWS as usize) as u64;
            let document = candidate
                .get_mut(&document_id)
                .expect("churn target exists");
            document.status = match (operation + batch) % 4 {
                0 => "active",
                1 => "review",
                2 => "archived",
                _ => "blocked",
            }
            .to_owned();
            document.owner_id = ((operation * 11 + batch) % 32) as u64;
            document.version += 1;
            transaction
                .update(
                    &database,
                    DOCUMENTS_TABLE,
                    document_row(document_id, document),
                )
                .expect("stage churn update");
        }
        transaction
            .commit(&mut database)
            .expect("commit churn batch");
        model = candidate;

        if (batch + 1) % CHURN_REOPEN_INTERVAL == 0 {
            let current_commit = database.commit_id();
            let (retained_commit, retained_model, lease) = if let Some(lease) = seed_lease.take() {
                (seed_commit, seed_model.clone(), lease)
            } else {
                let lease = database
                    .retain(current_commit)
                    .expect("retain churn checkpoint");
                (current_commit, model.clone(), lease)
            };
            let before = database.status().expect("churn status before compact");
            let report = database.compact().expect("compact churn checkpoint");
            assert_eq!(report.before.commit, current_commit);
            assert_eq!(report.after.commit, current_commit);
            assert_eq!(report.before, before);
            assert_database_matches(&database, &model, current_commit);
            assert_database_matches(&database, &retained_model, retained_commit);
            database.verify().expect("verify churn checkpoint");
            database.release(lease).expect("release churn checkpoint");
            database.checkpoint().expect("checkpoint churn state");
            database.close().expect("close churn checkpoint");

            database = RelationalDatabase::open(config.clone()).expect("reopen churn checkpoint");
            assert_eq!(database.commit_id(), current_commit);
            assert_database_matches(&database, &model, current_commit);
            database.verify().expect("verify reopened churn checkpoint");
        }
    }

    assert_database_matches(&database, &model, database.commit_id());
    let final_digest = digest_model(&model);
    assert_eq!(
        final_digest,
        "feeb36dbc0cc8a7c4758d49078f3678225ae02929dafc6d53f8ec970fe234376"
    );
    database.verify().expect("verify final churn state");
    database.checkpoint().expect("checkpoint final churn state");
    let final_commit = database.commit_id();
    database.close().expect("close final churn state");

    let mut reopened = RelationalDatabase::open(config).expect("reopen final churn state");
    assert_eq!(reopened.commit_id(), final_commit);
    assert_database_matches(&reopened, &model, final_commit);
    assert_eq!(digest_database(&reopened, final_commit), final_digest);
    reopened
        .verify()
        .expect("verify final reopened churn state");
    reopened.close().expect("close final reopened churn state");
}

#[test]
fn public_facade_replays_r2_lifecycle_across_selected_backends() {
    let temporary = tempdir().expect("temporary R2 directory");
    exercise_public_r2(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("SeerDB R2 directory");
    exercise_public_r2(RelationalBackendKind::Seer, &seer.path().join("seer"));
}

#[test]
fn public_facade_replays_sustained_r2_churn_with_reopen() {
    let temporary = tempdir().expect("temporary churn directory");
    exercise_public_r2_churn(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("SeerDB churn directory");
    exercise_public_r2_churn(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
