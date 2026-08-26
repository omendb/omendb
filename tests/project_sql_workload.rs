use std::collections::BTreeMap;
use std::path::Path;

use omendb::{DbError, RelationalBackendConfig, RelationalDatabase, Value};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkItem {
    tenant_id: u64,
    title: String,
    state: String,
    priority: i64,
}

type Model = BTreeMap<u64, WorkItem>;

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn item_values(id: u64, item: &WorkItem) -> Vec<Value> {
    vec![
        Value::I64(id as i64),
        Value::I64(item.tenant_id as i64),
        Value::Text(item.title.clone()),
        Value::Text(item.state.clone()),
        Value::I64(item.priority),
    ]
}

fn model_rows(model: &Model) -> Vec<Vec<Value>> {
    model
        .iter()
        .map(|(id, item)| item_values(*id, item))
        .collect()
}

fn model_digest(model: &Model) -> [u8; 32] {
    let mut digest = Sha256::new();
    for (id, item) in model {
        digest.update(format!(
            "{}|{}|{}|{}|{}\n",
            id, item.tenant_id, item.title, item.state, item.priority
        ));
    }
    digest.finalize().into()
}

fn assert_state(database: &mut RelationalDatabase, model: &Model) {
    let result = database
        .execute_sql("SELECT id, tenant_id, title, state, priority FROM work_items")
        .expect("read work-item state");
    assert_eq!(result.rows, model_rows(model));
    assert_eq!(model_digest(model), model_digest_from_rows(&result.rows));
}

fn model_digest_from_rows(rows: &[Vec<Value>]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for row in rows {
        let [id, tenant_id, title, state, priority] = row.as_slice() else {
            panic!("unexpected work-item row shape: {row:?}");
        };
        let (
            Value::I64(id),
            Value::I64(tenant_id),
            Value::Text(title),
            Value::Text(state),
            Value::I64(priority),
        ) = (id, tenant_id, title, state, priority)
        else {
            panic!("unexpected work-item value types: {row:?}");
        };
        digest.update(format!("{id}|{tenant_id}|{title}|{state}|{priority}\n"));
    }
    digest.finalize().into()
}

fn exercise_workload(directory: &Path) {
    let database_config = config(directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    database
        .execute_sql(
            "CREATE TABLE work_items (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL, title TEXT NOT NULL, state TEXT NOT NULL, priority BIGINT NOT NULL)",
        )
        .expect("create work-item schema");

    let seed = [
        (
            1,
            WorkItem {
                tenant_id: 1,
                title: "first".to_owned(),
                state: "open".to_owned(),
                priority: 10,
            },
        ),
        (
            2,
            WorkItem {
                tenant_id: 1,
                title: "second".to_owned(),
                state: "open".to_owned(),
                priority: 20,
            },
        ),
        (
            3,
            WorkItem {
                tenant_id: 1,
                title: "finished".to_owned(),
                state: "closed".to_owned(),
                priority: 5,
            },
        ),
        (
            4,
            WorkItem {
                tenant_id: 2,
                title: "tenant-two".to_owned(),
                state: "open".to_owned(),
                priority: 15,
            },
        ),
        (
            5,
            WorkItem {
                tenant_id: 2,
                title: "old".to_owned(),
                state: "closed".to_owned(),
                priority: 3,
            },
        ),
    ];
    let mut model = Model::new();
    database
        .transaction(|database, transaction| {
            for (id, item) in &seed {
                transaction.execute_sql_with_params(
                    database,
                    "INSERT INTO work_items VALUES ($1, $2, $3, $4, $5)",
                    &[
                        Value::I64(*id as i64),
                        Value::I64(item.tenant_id as i64),
                        Value::Text(item.title.clone()),
                        Value::Text(item.state.clone()),
                        Value::I64(item.priority),
                    ],
                )?;
            }
            Ok::<_, DbError>(())
        })
        .expect("seed transaction");
    for (id, item) in &seed {
        model.insert(*id, item.clone());
    }
    assert_state(&mut database, &model);

    for tenant_id in [1_i64, 2_i64] {
        let expected = model
            .iter()
            .filter(|(_, item)| item.tenant_id == tenant_id as u64 && item.state == "open")
            .map(|(id, item)| item_values(*id, item))
            .collect::<Vec<_>>();
        let actual = database
            .execute_sql_with_params(
                "SELECT id, tenant_id, title, state, priority FROM work_items WHERE tenant_id = $1 AND state = $2",
                &[Value::I64(tenant_id), Value::Text("open".to_owned())],
            )
            .expect("tenant/state query")
            .rows;
        assert_eq!(actual, expected);
    }

    let updated = database
        .execute_sql_with_params(
            "UPDATE work_items SET state = $1, priority = $2 WHERE tenant_id = $3 AND state = $4",
            &[
                Value::Text("running".to_owned()),
                Value::I64(99),
                Value::I64(1),
                Value::Text("open".to_owned()),
            ],
        )
        .expect("tenant update");
    assert_eq!(updated.affected_rows, 2);
    for item in model.values_mut() {
        if item.tenant_id == 1 && item.state == "open" {
            item.state = "running".to_owned();
            item.priority = 99;
        }
    }
    assert_state(&mut database, &model);

    database
        .transaction(|database, transaction| {
            transaction.execute_sql_with_params(
                database,
                "INSERT INTO work_items VALUES ($1, $2, $3, $4, $5)",
                &[
                    Value::I64(6),
                    Value::I64(1),
                    Value::Text("transactional".to_owned()),
                    Value::Text("open".to_owned()),
                    Value::I64(7),
                ],
            )?;
            transaction.execute_sql_with_params(
                database,
                "UPDATE work_items SET state = $1, priority = $2 WHERE id = $3",
                &[
                    Value::Text("running".to_owned()),
                    Value::I64(8),
                    Value::I64(6),
                ],
            )?;
            let row = transaction
                .execute_sql_with_params(
                    database,
                    "SELECT id, tenant_id, title, state, priority FROM work_items WHERE id = $1",
                    &[Value::I64(6)],
                )?
                .rows;
            assert_eq!(
                row,
                vec![vec![
                    Value::I64(6),
                    Value::I64(1),
                    Value::Text("transactional".to_owned()),
                    Value::Text("running".to_owned()),
                    Value::I64(8),
                ]]
            );
            Ok::<_, DbError>(())
        })
        .expect("multi-statement SQL transaction");
    model.insert(
        6,
        WorkItem {
            tenant_id: 1,
            title: "transactional".to_owned(),
            state: "running".to_owned(),
            priority: 8,
        },
    );
    assert_state(&mut database, &model);

    let before_abort = model.clone();
    let aborted = database.transaction(|database, transaction| {
        transaction.execute_sql_with_params(
            database,
            "INSERT INTO work_items VALUES ($1, $2, $3, $4, $5)",
            &[
                Value::I64(7),
                Value::I64(1),
                Value::Text("aborted".to_owned()),
                Value::Text("open".to_owned()),
                Value::I64(1),
            ],
        )?;
        transaction.execute_sql_with_params(
            database,
            "INSERT INTO work_items VALUES ($1, $2, $3, $4, $5)",
            &[
                Value::I64(8),
                Value::I64(1),
                Value::Text("invalid".to_owned()),
                Value::Null,
                Value::I64(1),
            ],
        )?;
        Ok::<_, DbError>(())
    });
    assert!(matches!(aborted, Err(DbError::InvalidState(_))));
    assert_eq!(model, before_abort);
    assert_state(&mut database, &model);

    let deleted = database
        .execute_sql_with_params(
            "DELETE FROM work_items WHERE tenant_id = $1 AND state = $2",
            &[Value::I64(2), Value::Text("closed".to_owned())],
        )
        .expect("tenant cleanup");
    assert_eq!(deleted.affected_rows, 1);
    model.retain(|_, item| !(item.tenant_id == 2 && item.state == "closed"));
    assert_state(&mut database, &model);

    database.close().expect("close");
    let mut reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_state(&mut reopened, &model);
    reopened.close().expect("close reopened");
}

#[test]
fn named_sql_ordinary_workload_matches_across_backends_and_reopens() {
    let temporary = tempdir().expect("temporary directory");
    exercise_workload(&temporary.path().join("temporary"));
}
