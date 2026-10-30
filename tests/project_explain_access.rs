//! EXPLAIN must name an executable strategy, not a partially bound candidate.
use omendb::{RelationalBackendConfig, RelationalDatabase, Value};

fn database(expression_index: bool) -> (tempfile::TempDir, RelationalDatabase) {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    db.execute_sql(
        "CREATE TABLE positions (id BIGINT PRIMARY KEY, x BIGINT, y BIGINT, bucket BIGINT)",
    )
    .unwrap();
    db.execute_sql("INSERT INTO positions VALUES (1, 3, 4, 1), (2, 10, 20, 2)")
        .unwrap();
    db.execute_sql("CREATE INDEX bucket_key ON positions (bucket)")
        .unwrap();
    if expression_index {
        db.execute_sql("CREATE INDEX expression_key ON positions (x + y)")
            .unwrap();
    }
    (directory, db)
}

fn assert_plan(db: &mut RelationalDatabase, predicate: &str, expected: &str) {
    for statement in [
        format!("SELECT id FROM positions WHERE {predicate}"),
        format!("UPDATE positions SET bucket = 9 WHERE {predicate}"),
        format!("DELETE FROM positions WHERE {predicate}"),
    ] {
        let result = db.execute_sql(&format!("EXPLAIN {statement}")).unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Value::Text(expected.into())]],
            "{statement}"
        );
    }
}

#[test]
fn expression_index_candidates_explain_the_actual_table_scan() {
    let (_directory, mut db) = database(true);
    for predicate in [
        "x + y = 7",
        "bucket = 1 AND x + y = 7",
        "x + y = 7 AND bucket = 1",
        "id = 1 AND x + y = 7",
    ] {
        assert_plan(&mut db, predicate, "Seq Scan on positions");
    }
    // Describing UPDATE/DELETE did not execute them.
    assert_eq!(
        db.execute_sql("SELECT id, bucket FROM positions ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::I64(1), Value::I64(1)],
            vec![Value::I64(2), Value::I64(2)]
        ]
    );
    assert_eq!(
        db.execute_sql("SELECT id FROM positions WHERE x + y = 7")
            .unwrap()
            .rows,
        vec![vec![Value::I64(1)]]
    );
    assert_eq!(
        db.execute_sql("UPDATE positions SET bucket = 9 WHERE x + y = 7")
            .unwrap()
            .affected_rows,
        1
    );
    assert_eq!(
        db.execute_sql("DELETE FROM positions WHERE x + y = 7")
            .unwrap()
            .affected_rows,
        1
    );
    assert_eq!(
        db.execute_sql("SELECT id, bucket FROM positions")
            .unwrap()
            .rows,
        vec![vec![Value::I64(2), Value::I64(2)]]
    );
    db.close().unwrap();
}

#[test]
fn partial_column_bindings_do_not_advertise_plain_indexes_or_primary_keys() {
    let (_directory, mut db) = database(false);
    for predicate in ["bucket = 1 AND x + y = 7", "id = 1 AND x + y = 7"] {
        assert_plan(&mut db, predicate, "Seq Scan on positions");
    }
    db.close().unwrap();
}

#[test]
fn fully_modeled_equalities_keep_their_lookup_and_empty_plans() {
    let (_directory, mut db) = database(true);
    for (predicate, expected) in [
        ("id = 1", "Primary Key Lookup on positions"),
        (
            "bucket = 1",
            "Index Scan using bucket_key on positions (bucket)",
        ),
        (
            "bucket = NULL",
            "Result (empty) on positions (NULL equality)",
        ),
    ] {
        assert_plan(&mut db, predicate, expected);
    }
    assert_eq!(
        db.execute_sql("SELECT id FROM positions WHERE bucket = 1")
            .unwrap()
            .rows,
        vec![vec![Value::I64(1)]]
    );
    assert_eq!(
        db.execute_sql("UPDATE positions SET bucket = 9 WHERE id = 1")
            .unwrap()
            .affected_rows,
        1
    );
    assert_eq!(
        db.execute_sql("DELETE FROM positions WHERE bucket = 9")
            .unwrap()
            .affected_rows,
        1
    );
    assert!(
        db.execute_sql("SELECT id FROM positions WHERE bucket = NULL")
            .unwrap()
            .rows
            .is_empty()
    );
    db.close().unwrap();
}
