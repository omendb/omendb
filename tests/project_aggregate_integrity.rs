//! SUM failures must not publish saturated values or preceding writes.

#[allow(dead_code)]
#[path = "common/aggregate_sum.rs"]
mod aggregate_sum;
#[allow(dead_code)]
mod support;

use omendb::{AggregateAccumulator, AggregateKind, ColumnType, DbError, RelationalDatabase, Value};
use support::config;
use tempfile::tempdir;

fn database(path: &std::path::Path) -> RelationalDatabase {
    let mut database = RelationalDatabase::create(config(path)).unwrap();
    for sql in aggregate_sum::SCHEMA {
        database.execute_sql(sql).unwrap();
    }
    database
}

fn assert_overflow_aborts(query: &str) {
    for (first, second) in [(i64::MAX, 1), (i64::MIN, -1)] {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let mut database = database(&path);
        database
            .execute_sql_with_params(
                "INSERT INTO aggregate_items VALUES (1, 1, $1), (2, 1, $2)",
                &[Value::I64(first), Value::I64(second)],
            )
            .unwrap();
        let commit = database.commit_id();
        let mut transaction = database.begin().unwrap();
        transaction
            .execute_sql(&database, "INSERT INTO aggregate_items VALUES (3, 2, 7)")
            .unwrap();
        assert!(
            matches!(
                transaction.execute_sql(&database, query),
                Err(DbError::SqlNumericValueOutOfRange(_))
            ),
            "{query}"
        );
        assert!(transaction.execute_sql(&database, "SELECT 1").is_err());
        assert!(transaction.commit().is_err());
        assert_eq!(database.commit_id(), commit);
        let expected = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        assert_eq!(
            database
                .execute_sql("SELECT id FROM aggregate_items ORDER BY id")
                .unwrap()
                .rows,
            expected
        );
        database.close().unwrap();
        let mut reopened = RelationalDatabase::open(config(&path)).unwrap();
        assert_eq!(
            reopened
                .execute_sql("SELECT id FROM aggregate_items ORDER BY id")
                .unwrap()
                .rows,
            expected
        );
        reopened.close().unwrap();
    }
}

#[test]
fn global_integer_sum_overflow_aborts_prior_work() {
    assert_overflow_aborts(aggregate_sum::GLOBAL);
}
#[test]
fn grouped_integer_sum_overflow_aborts_prior_work() {
    assert_overflow_aborts(aggregate_sum::GROUPED);
}
#[test]
fn joined_integer_sum_overflow_aborts_prior_work() {
    assert_overflow_aborts(aggregate_sum::JOINED);
}
#[test]
fn joined_grouped_integer_sum_overflow_aborts_prior_work() {
    assert_overflow_aborts(aggregate_sum::JOINED_GROUPED);
}
#[test]
fn window_integer_sum_overflow_aborts_prior_work() {
    assert_overflow_aborts(aggregate_sum::WINDOW);
}

#[test]
fn sum_without_nonnull_input_is_null_while_count_remains_zero() {
    for all_null in [false, true] {
        let directory = tempdir().unwrap();
        let mut database = database(&directory.path().join("db"));
        if all_null {
            database
                .execute_sql("INSERT INTO aggregate_items VALUES (1, 1, NULL), (2, 1, NULL)")
                .unwrap();
        }
        let expected = vec![vec![
            Value::U64(if all_null { 2 } else { 0 }),
            Value::U64(0),
            Value::Null,
            Value::Null,
            Value::Null,
        ]];
        for query in [
            "SELECT count(*), count(amount), sum(amount), min(amount), max(amount) FROM aggregate_items",
            "SELECT count(*), count(i.amount), sum(i.amount), min(i.amount), max(i.amount) FROM aggregate_items i JOIN aggregate_groups g ON i.group_id = g.id",
        ] {
            assert_eq!(
                database.execute_sql(query).unwrap().rows,
                expected,
                "{query}; all_null={all_null}"
            );
        }
        database.close().unwrap();
    }
}

#[test]
fn null_groups_and_window_frames_are_not_integer_zero() {
    let directory = tempdir().unwrap();
    let mut database = database(&directory.path().join("db"));
    database
        .execute_sql("INSERT INTO aggregate_items VALUES (1, 1, NULL), (2, 1, NULL)")
        .unwrap();
    assert_eq!(
        database.execute_sql(aggregate_sum::GROUPED).unwrap().rows,
        vec![vec![Value::I64(1), Value::Null]]
    );
    for query in [
        aggregate_sum::WINDOW,
        "SELECT sum(amount) OVER () FROM aggregate_items",
    ] {
        assert_eq!(
            database.execute_sql(query).unwrap().rows,
            vec![vec![Value::Null], vec![Value::Null]]
        );
    }
    database
        .execute_sql("UPDATE aggregate_items SET amount = 0")
        .unwrap();
    let result = database.execute_sql(aggregate_sum::GLOBAL).unwrap();
    assert_eq!(result.columns[0].column_type, Some(ColumnType::I64));
    assert_eq!(result.rows, vec![vec![Value::I64(0)]]);
    database.close().unwrap();
}

#[test]
fn insert_select_cannot_publish_a_saturated_sum() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let mut database = database(&path);
    database
        .execute_sql_with_params(
            "INSERT INTO aggregate_items VALUES (1, 1, $1), (2, 1, 1)",
            &[Value::I64(i64::MAX)],
        )
        .unwrap();
    let commit = database.commit_id();
    assert!(matches!(
        database.execute_sql("INSERT INTO aggregate_saved SELECT sum(amount) FROM aggregate_items"),
        Err(DbError::SqlNumericValueOutOfRange(_))
    ));
    assert_eq!(database.commit_id(), commit);
    assert!(
        database
            .execute_sql("SELECT amount FROM aggregate_saved")
            .unwrap()
            .rows
            .is_empty()
    );
    database.close().unwrap();
    let mut reopened = RelationalDatabase::open(config(&path)).unwrap();
    assert!(
        reopened
            .execute_sql("SELECT amount FROM aggregate_saved")
            .unwrap()
            .rows
            .is_empty()
    );
    reopened.close().unwrap();
}

#[test]
fn native_mixed_integer_sums_do_not_truncate_operands() {
    for (first, second) in [
        (Value::I64(i64::MIN), Value::U64(u64::MAX)),
        (Value::U64(u64::MAX), Value::I64(i64::MIN)),
    ] {
        let mut sum = AggregateAccumulator::new(AggregateKind::Sum);
        sum.update(Some(&first)).unwrap();
        sum.update(Some(&second)).unwrap();
        assert_eq!(sum.finalize(), Value::I64(i64::MAX));
    }
}

#[test]
fn native_integer_sum_overflow_preserves_the_last_valid_accumulator() {
    for (first, second) in [
        (Value::I64(i64::MAX), Value::I64(1)),
        (Value::I64(i64::MIN), Value::I64(-1)),
        (Value::U64(u64::MAX), Value::U64(1)),
        (Value::I64(0), Value::U64(u64::MAX)),
        (Value::U64(u64::MAX), Value::I64(1)),
        (Value::U64(u64::MAX), Value::I64(-1)),
    ] {
        let mut sum = AggregateAccumulator::new(AggregateKind::Sum);
        sum.update(Some(&first)).unwrap();
        assert!(matches!(
            sum.update(Some(&second)),
            Err(DbError::SqlNumericValueOutOfRange(_))
        ));
        assert_eq!(sum.finalize(), first);
    }
}
