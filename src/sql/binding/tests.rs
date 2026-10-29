use crate::{ColumnType as T, DbError, RelationalBackendConfig, RelationalDatabase};

use super::{StatementEffect, bind};

fn database() -> (tempfile::TempDir, RelationalDatabase) {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    database.execute_sql("CREATE TABLE a (id BIGINT PRIMARY KEY, label TEXT, amount DOUBLE PRECISION, d DATE, ts TIMESTAMP, dec NUMERIC)").unwrap();
    database
        .execute_sql("CREATE TABLE b (id BIGINT PRIMARY KEY, aid BIGINT, label TEXT)")
        .unwrap();
    (directory, database)
}

fn types(database: &RelationalDatabase, sql: &str) -> Vec<T> {
    bind(database, sql, &[])
        .unwrap()
        .columns
        .into_iter()
        .map(|column| column.column_type.unwrap())
        .collect()
}

#[test]
fn validates_all_branches_before_empty_or_limit_zero_execution() {
    let (_directory, database) = database();
    for sql in [
        "SELECT id FROM a WHERE missing = 1 LIMIT 0",
        "SELECT CASE WHEN id = 1 THEN 1 ELSE missing END FROM a LIMIT 0",
        "SELECT coalesce(id, missing) FROM a LIMIT 0",
        "UPDATE a SET id = missing WHERE id = 100",
        "SELECT id FROM a WHERE label + 1 = 2",
    ] {
        assert!(bind(&database, sql, &[]).is_err(), "{sql}");
    }
}

#[test]
fn infer_parameters_across_contexts_and_reject_conflicts() {
    let (_directory, database) = database();
    let bound = bind(
        &database,
        "SELECT $1 + $2 FROM a WHERE $2 = id AND label = $3 LIMIT $4 OFFSET $5",
        &[],
    )
    .unwrap();
    assert_eq!(
        bound.parameter_types,
        vec![
            Some(T::I64),
            Some(T::I64),
            Some(T::Text),
            Some(T::I64),
            Some(T::I64)
        ]
    );
    assert!(matches!(
        bind(
            &database,
            "SELECT $1 FROM a WHERE id = $1 AND label = $1",
            &[]
        ),
        Err(DbError::SqlDatatypeMismatch { .. })
    ));
    assert!(bind(&database, "SELECT id FROM a WHERE id = $1", &[Some(T::U64)]).is_ok());
    let chained = bind(&database, "SELECT $1 FROM a WHERE $1=$2 AND $2=id", &[]).unwrap();
    assert_eq!(chained.parameter_types, vec![Some(T::I64), Some(T::I64)]);
    assert_eq!(chained.columns[0].column_type, Some(T::I64));
    assert_eq!(
        bind(&database, "SELECT -$1, floor($2)", &[])
            .unwrap()
            .parameter_types,
        vec![Some(T::I64), Some(T::I64)]
    );
    for sql in [
        "SELECT $1 FROM a WHERE id=$1 AND amount=$1",
        "SELECT $1 FROM a WHERE amount=$1 AND id=$1",
    ] {
        assert_eq!(
            bind(&database, sql, &[]).unwrap().parameter_types,
            vec![Some(T::Float64)]
        );
        assert_eq!(
            bind(&database, sql, &[Some(T::I64)])
                .unwrap()
                .parameter_types,
            vec![Some(T::I64)]
        );
    }
}

#[test]
fn full_domain_case_coalesce_and_set_outputs() {
    let (_directory, database) = database();
    assert_eq!(
        types(
            &database,
            "SELECT CASE WHEN id = 1 THEN NULL ELSE amount END, coalesce(NULL, dec, amount), coalesce(NULL, NULL) FROM a"
        ),
        vec![T::Float64, T::Float64, T::Text]
    );
    assert_eq!(
        types(
            &database,
            "SELECT NULL AS x UNION ALL SELECT 1 UNION ALL SELECT 1.5"
        ),
        vec![T::Decimal]
    );
    assert!(bind(&database, "SELECT label FROM a UNION SELECT id FROM a", &[]).is_err());
}

#[test]
fn binds_subqueries_and_sources_into_one_access_authority() {
    let (_directory, database) = database();
    let bound = bind(&database, "SELECT (SELECT count(*) FROM b) FROM a WHERE id IN (SELECT aid FROM b) AND EXISTS (SELECT id FROM b)", &[]).unwrap();
    assert_eq!(bound.access.read.len(), 2);
    assert!(bound.access.write.is_empty());
    assert_eq!(bound.columns[0].column_type, Some(T::I64));
    let bound = bind(
        &database,
        "INSERT INTO a (id, label) SELECT id, label FROM b RETURNING id",
        &[],
    )
    .unwrap();
    assert_eq!(bound.effect, StatementEffect::Write);
    assert_eq!(bound.access.write.len(), 1);
    assert_eq!(bound.access.read.len(), 1);
    assert_ne!(bound.access.read, bound.access.write);
}

#[test]
fn qualified_ambiguous_and_joined_output_order() {
    let (_directory, database) = database();
    assert!(bind(&database, "SELECT id FROM a JOIN b ON a.id = b.aid", &[]).is_err());
    assert!(bind(&database, "SELECT wrong.id FROM a", &[]).is_err());
    assert!(bind(&database, "SELECT a.id FROM a AS x", &[]).is_err());
    let bound = bind(
        &database,
        "SELECT count(*) AS n, x.label AS name FROM a x JOIN b y ON x.id = y.aid GROUP BY x.label",
        &[],
    )
    .unwrap();
    assert_eq!(
        bound
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["n", "name"]
    );
    assert_eq!(
        bound
            .columns
            .iter()
            .map(|c| c.column_type)
            .collect::<Vec<_>>(),
        vec![Some(T::I64), Some(T::Text)]
    );
    let bound = bind(&database, "SELECT count(*) AS n FROM a JOIN b ON a.id=b.aid GROUP BY a.label HAVING label='open' ORDER BY label", &[]).unwrap();
    assert_eq!(bound.columns.len(), 1);
    assert_eq!(bound.columns[0].name, "n");
}

#[test]
fn temporal_and_window_metadata_is_fixed_without_evaluation() {
    let (_directory, database) = database();
    assert_eq!(
        types(
            &database,
            "SELECT date_part(label, ts), EXTRACT(YEAR FROM ts), count(*) OVER (), avg(id) OVER (), lag(label) OVER (ORDER BY id) FROM a"
        ),
        vec![T::Float64, T::Decimal, T::I64, T::Float64, T::Text]
    );
    assert_eq!(
        types(
            &database,
            "SELECT now(), current_date, version(), coalesce(NULL, NULL)"
        ),
        vec![T::Timestamp, T::Date, T::Text, T::Text]
    );
    // A binder cannot evaluate this: division by zero is a runtime error.
    assert_eq!(types(&database, "SELECT id / 0 FROM a"), vec![T::I64]);
}

#[test]
fn unsupported_shapes_fail_closed_and_controls_are_not_admin() {
    let (_directory, database) = database();
    for sql in [
        "SELECT id FROM a WHERE id LIKE '1'",
        "SELECT 1 + 2",
        "SELECT CASE WHEN true THEN 1 END",
        "SELECT sum(id) FILTER (WHERE id > 1) FROM a",
        "SELECT count(DISTINCT id) FROM a",
        "SELECT id FROM a FOR UPDATE",
        "SELECT * FROM (SELECT id FROM a) x",
        "SHOW unknown_setting",
        "BEGIN ISOLATION LEVEL READ COMMITTED",
        "UPDATE a SET id = id * 2",
        "DELETE FROM a RETURNING id AS x",
        "UPDATE a SET id = (id + 1) + 1",
        "SELECT CASE WHEN id = 1 THEN now() ELSE ts END FROM a",
        "SELECT id FROM a LIMIT NULL",
        "SELECT date_part('dow', ts) FROM a",
        "EXPLAIN SHOW server_version",
        "CREATE TABLE bad (id VARCHARHACK)",
        "SELECT CAST($1 AS VARCHARHACK)",
        "SELECT amount % 2 FROM a",
        "SELECT dec % 2 FROM a",
        "SELECT now() FROM a",
        "SELECT upper('x') FROM a JOIN b ON a.id=b.aid",
    ] {
        assert!(bind(&database, sql, &[]).is_err(), "{sql}");
    }
    let bound = bind(&database, "BEGIN", &[]).unwrap();
    assert!(matches!(bound.effect, StatementEffect::Control(_)));
    assert!(!bound.access.requires_admin);
    assert_eq!(types(&database, "SHOW server_version"), vec![T::Text]);
    let bound = bind(&database, "EXPLAIN SELECT id FROM a", &[]).unwrap();
    assert_eq!(bound.access.read.len(), 1);
    assert_eq!(bound.columns[0].column_type, Some(T::Text));
    assert_eq!(bound.catalog_stamp, Some(database.catalog_stamp()));
    assert!(matches!(
        bound.statement,
        sqlparser::ast::Statement::Explain { .. }
    ));
}

#[test]
fn controls_bind_without_catalog_and_reject_incomplete_or_extended_controls() {
    for source in [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "ABORT",
        "-- leading comment\nCOMMIT;",
    ] {
        let bound = super::bind_control(source).unwrap().unwrap();
        assert_eq!(bound.catalog_stamp, None);
        assert!(matches!(bound.effect, StatementEffect::Control(_)));
        assert!(bound.parameter_types.is_empty() && bound.columns.is_empty());
        assert!(
            bound.access.read.is_empty()
                && bound.access.write.is_empty()
                && !bound.access.requires_admin
        );
    }
    assert!(super::bind_control("SELECT 1").unwrap().is_none());
    for source in [
        "COMMIT AND CHAIN",
        "ROLLBACK TO SAVEPOINT x",
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
        "COMMIT; SELECT 1",
        "BEGIN; END; COMMIT",
    ] {
        assert!(super::bind_control(source).is_err(), "{source}");
    }
}

#[test]
fn preferred_output_parameters_do_not_default_ambiguous_predicates_or_gaps() {
    let (_directory, database) = database();
    for sql in [
        "SELECT $1",
        "SELECT coalesce($1, NULL)",
        "SELECT CASE WHEN id = 1 THEN $1 ELSE NULL END FROM a",
        "SELECT (SELECT $1) FROM a",
        "SELECT id FROM a WHERE EXISTS(SELECT $1)",
    ] {
        assert_eq!(
            bind(&database, sql, &[]).unwrap().parameter_types,
            vec![Some(T::Text)],
            "{sql}"
        );
    }
    assert_eq!(
        bind(&database, "SELECT $3 FROM a WHERE $1 = $2", &[])
            .unwrap()
            .parameter_types,
        vec![None, None, Some(T::Text)]
    );
    assert_eq!(
        bind(
            &database,
            "SELECT coalesce($1, NULL) UNION SELECT id FROM a",
            &[]
        )
        .unwrap()
        .parameter_types,
        vec![Some(T::I64)]
    );
    assert_eq!(
        bind(
            &database,
            "SELECT coalesce($1, '1') UNION SELECT id FROM a",
            &[]
        )
        .unwrap()
        .parameter_types,
        vec![Some(T::I64)]
    );
    assert_eq!(
        bind(
            &database,
            "SELECT $1 FROM a WHERE $1 IS NULL",
            &[Some(T::Date)]
        )
        .unwrap()
        .columns[0]
            .column_type,
        Some(T::Date)
    );
    assert!(bind(&database, "SELECT $65536", &[]).is_err());
    assert!(bind(&database, "SELECT $0", &[]).is_err());
    assert!(bind(&database, "SELECT ?", &[]).is_err());
    assert_eq!(
        bind(&database, "SELECT $65535", &[])
            .unwrap()
            .parameter_types
            .len(),
        65535
    );
}

#[test]
fn rewritten_parameters_preserve_names_numbers_and_declared_domains() {
    use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
    let (_directory, database) = database();
    let bound = bind(
        &database,
        "SELECT $1 AS x, $2 FROM a WHERE id = $1",
        &[Some(T::U64), Some(T::Date)],
    )
    .unwrap();
    assert_eq!(bound.columns[0].name, "x");
    assert_eq!(bound.columns[1].name, "$2");
    let Statement::Query(query) = bound.statement else {
        panic!("query");
    };
    let SetExpr::Select(select) = *query.body else {
        panic!("select");
    };
    let SelectItem::ExprWithAlias {
        expr: Expr::Cast {
            expr, data_type, ..
        },
        ..
    } = &select.projection[0]
    else {
        panic!("typed placeholder");
    };
    assert_eq!(*data_type, sqlparser::ast::DataType::UInt64);
    assert_eq!(expr.to_string(), "$1");
    let SelectItem::UnnamedExpr(Expr::Cast {
        expr, data_type, ..
    }) = &select.projection[1]
    else {
        panic!("typed date placeholder");
    };
    assert_eq!(*data_type, sqlparser::ast::DataType::Date);
    assert_eq!(expr.to_string(), "$2");
}

#[test]
fn contextual_string_casts_are_scoped_by_occurrence_not_literal_text() {
    use sqlparser::ast::{DataType, Expr, visit_expressions};
    use std::ops::ControlFlow;
    let (_directory, database) = database();
    let sql =
        "SELECT coalesce('1', id), coalesce('1', label), (SELECT coalesce('1', dec) FROM a) FROM a";
    let bound = bind(&database, sql, &[]).unwrap();
    let mut cast_types = Vec::new();
    let result: ControlFlow<()> = visit_expressions(&bound.statement, |expr| {
        if let Expr::Cast {
            expr, data_type, ..
        } = expr
            && matches!(expr.as_ref(), Expr::Value(value) if value.value.clone().into_string().as_deref() == Some("1"))
        {
            cast_types.push(super::sql_type(data_type).unwrap());
            assert!(!matches!(data_type, DataType::Custom(..)));
        }
        ControlFlow::Continue(())
    });
    assert!(result.is_continue());
    assert_eq!(cast_types, vec![T::I64, T::Text, T::Decimal]);
    assert_eq!(bound.columns[0].name, "coalesce('1', id)");
    assert_eq!(
        bound
            .columns
            .iter()
            .map(|column| column.column_type)
            .collect::<Vec<_>>(),
        vec![Some(T::I64), Some(T::Text), Some(T::Decimal)]
    );
    // Parsing is cached, but neither the source AST nor another bind's literal
    // decisions may be contaminated by the first rewrite.
    let again = bind(&database, sql, &[]).unwrap();
    assert_eq!(bound.statement, again.statement);
    let numeric = bind(&database, "SELECT coalesce('1', id) + amount FROM a", &[]).unwrap();
    assert!(
        numeric
            .statement
            .to_string()
            .contains("CAST('1' AS DOUBLE PRECISION)")
    );
}

#[test]
fn using_common_domains_do_not_retype_qualified_source_columns() {
    let (_directory, mut database) = database();
    database
        .execute_sql("CREATE TABLE c (id DOUBLE PRECISION)")
        .unwrap();
    assert_eq!(
        types(&database, "SELECT id, a.id, c.id FROM a JOIN c USING (id)"),
        vec![T::Float64, T::I64, T::Float64]
    );
    let bound = bind(&database, "SELECT * FROM a NATURAL JOIN b", &[]).unwrap();
    assert_eq!(bound.access.read.len(), 2);
    assert_eq!(bound.columns.len(), 7);
}

#[test]
fn common_domains_preserve_unknowns_and_promote_all_numeric_arms() {
    for (left, right, result) in [
        (None, None, None),
        (None, Some(T::Date), Some(T::Date)),
        (Some(T::I64), Some(T::U64), Some(T::I64)),
        (Some(T::I64), Some(T::Decimal), Some(T::Decimal)),
        (Some(T::Decimal), Some(T::Float64), Some(T::Float64)),
    ] {
        assert_eq!(super::common_type(left, right).unwrap(), result);
        assert_eq!(super::common_type(right, left).unwrap(), result);
    }
    assert!(super::common_type(Some(T::Text), Some(T::I64)).is_err());
}

#[test]
fn dml_sources_and_static_assignment_domains() {
    let (_directory, database) = database();
    for sql in [
        "UPDATE a SET id = id + $1 FROM b WHERE a.id = b.aid RETURNING id",
        "DELETE FROM a USING b WHERE a.id = b.aid RETURNING *",
    ] {
        let bound = bind(&database, sql, &[]).unwrap();
        assert_eq!(bound.access.read.len(), 1, "{sql}");
        assert_eq!(bound.access.write.len(), 1, "{sql}");
    }
    assert!(bind(&database, "INSERT INTO a(id) VALUES (lower('oops'))", &[]).is_err());
    assert!(bind(&database, "INSERT INTO a(id) VALUES (1), ('oops')", &[]).is_err());
    assert!(bind(&database, "UPDATE a SET id = label", &[]).is_err());
    assert!(
        bind(
            &database,
            "UPDATE a SET id = 1 WHERE EXISTS(SELECT id FROM b)",
            &[]
        )
        .is_err()
    );
    assert!(
        bind(
            &database,
            "SELECT id FROM a WHERE id='1' AND d='2026-01-01'",
            &[]
        )
        .is_ok()
    );
    assert!(bind(&database, "SELECT id FROM a WHERE id='oops'", &[]).is_err());
    assert!(bind(&database, "SELECT id FROM a WHERE d='not-a-date'", &[]).is_err());
}

#[test]
fn set_inference_visits_each_body_once_per_pass_and_reaches_both_arms() {
    use std::collections::BTreeMap;
    let (_directory, database) = database();
    let mut arms = vec!["SELECT NULL"; 25];
    arms[0] = "SELECT coalesce($1, '1')";
    arms[1] = "SELECT $2";
    arms[24] = "SELECT amount FROM a";
    for reverse in [false, true] {
        if reverse {
            arms.reverse();
        }
        let sql = arms.join(" UNION ALL ");
        let statement = crate::sql::parse_statement(&sql).unwrap();
        let mut parameters = vec![None, None];
        let mut contexts = BTreeMap::new();
        let mut binder = super::Binder {
            database: &database,
            parameters: &mut parameters,
            hints: &[],
            access: Default::default(),
            prefer_outputs: false,
            literal_casts: BTreeMap::new(),
            output_contexts: &mut contexts,
            outputs_changed: false,
            set_visits: 0,
        };
        binder.statement(&statement).unwrap();
        assert_eq!(binder.set_visits, 49);
        // The real entry point must also converge and retain literal coercions.
        let start = std::time::Instant::now();
        let bound = bind(&database, &sql, &[]).unwrap();
        eprintln!("25-arm bind (reverse={reverse}): {:?}", start.elapsed());
        assert_eq!(bound.parameter_types, vec![Some(T::Float64); 2]);
        assert_eq!(bound.columns[0].column_type, Some(T::Float64));
        assert!(
            bound
                .statement
                .to_string()
                .contains("CAST('1' AS DOUBLE PRECISION)")
        );
    }
    // With no parameters, output constraints alone must keep inference running.
    let bound = bind(
        &database,
        "SELECT '2026-01-01' UNION SELECT NULL UNION SELECT d FROM a",
        &[],
    )
    .unwrap();
    assert_eq!(bound.columns[0].column_type, Some(T::Date));
    assert!(
        bound
            .statement
            .to_string()
            .contains("CAST('2026-01-01' AS DATE)")
    );
    assert!(bound.statement.to_string().contains("CAST(NULL AS DATE)"));
    assert!(matches!(
        bind(&database, "SELECT 'not-a-date' UNION SELECT d FROM a", &[]),
        Err(DbError::SqlDatatypeMismatch { .. })
    ));
}

#[test]
fn nested_query_contexts_do_not_rebind_descendants_within_a_pass() {
    use std::collections::BTreeMap;
    let (_directory, database) = database();
    for scalar in [false, true] {
        let mut sql = "SELECT coalesce($1, '1')".to_owned();
        for _ in 0..12 {
            sql = if scalar {
                format!("SELECT ({sql}) FROM a")
            } else {
                format!("SELECT $1 FROM a WHERE $1 IN ({sql})")
            };
        }
        sql.push_str(" UNION ALL SELECT amount FROM a");
        let statement = crate::sql::parse_statement(&sql).unwrap();
        let mut parameters = vec![None];
        let mut contexts = BTreeMap::new();
        let mut binder = super::Binder {
            database: &database,
            parameters: &mut parameters,
            hints: &[],
            access: Default::default(),
            prefer_outputs: true,
            literal_casts: BTreeMap::new(),
            output_contexts: &mut contexts,
            outputs_changed: false,
            set_visits: 0,
        };
        binder.statement(&statement).unwrap();
        assert_eq!(binder.set_visits, 15);
        let bound = bind(&database, &sql, &[]).unwrap();
        assert_eq!(bound.parameter_types, vec![Some(T::Float64)]);
        assert_eq!(bound.columns[0].column_type, Some(T::Float64));
        assert!(
            bound
                .statement
                .to_string()
                .contains("CAST('1' AS DOUBLE PRECISION)")
        );
    }
    for sql in [
        "SELECT (SELECT coalesce($1, '2026-01-01')) FROM a UNION SELECT d FROM a",
        "SELECT d FROM a UNION SELECT (SELECT coalesce($1, '2026-01-01')) FROM a",
        "SELECT id FROM a WHERE d IN (SELECT coalesce($1, '2026-01-01'))",
    ] {
        assert_eq!(
            bind(&database, sql, &[]).unwrap().parameter_types,
            vec![Some(T::Date)],
            "{sql}"
        );
    }
}

#[test]
fn numeric_nulls_keep_their_domain_in_metadata_and_retained_ast() {
    use sqlparser::ast::{Expr, Value, visit_expressions};
    use std::ops::ControlFlow;
    let (_directory, database) = database();
    for sql in [
        "SELECT abs(NULL)",
        "SELECT round(NULL), floor(NULL), ceil(NULL), -NULL",
        "SELECT NULL + NULL, NULL * NULL, NULL / NULL FROM a",
        "SELECT abs(NULL) UNION SELECT id FROM a",
    ] {
        let bound = bind(&database, sql, &[]).unwrap();
        assert!(
            bound
                .columns
                .iter()
                .all(|column| column.column_type == Some(T::I64)),
            "{sql}"
        );
        let mut null_casts = 0;
        let mut nulls = 0;
        let result: ControlFlow<()> = visit_expressions(&bound.statement, |expr| {
            if matches!(expr, Expr::Value(value) if value.value == Value::Null) {
                nulls += 1;
            }
            if let Expr::Cast {
                expr, data_type, ..
            } = expr
                && matches!(expr.as_ref(), Expr::Value(value) if value.value == Value::Null)
            {
                assert_eq!(super::sql_type(data_type).unwrap(), T::I64, "{sql}");
                null_casts += 1;
            }
            ControlFlow::Continue(())
        });
        assert!(result.is_continue());
        assert!(null_casts > 0, "{sql}");
        assert_eq!(
            null_casts, nulls,
            "every NULL must retain its domain: {sql}"
        );
    }
    for sql in [
        "SELECT abs(NULL) UNION SELECT label FROM a",
        "SELECT label FROM a UNION SELECT abs(NULL)",
        "SELECT NULL + NULL FROM a UNION SELECT label FROM a",
    ] {
        assert!(
            matches!(
                bind(&database, sql, &[]),
                Err(DbError::SqlDatatypeMismatch { .. })
            ),
            "{sql}"
        );
    }
    for sql in [
        "SELECT NULL + amount FROM a",
        "SELECT abs(NULL) UNION SELECT amount FROM a",
    ] {
        let bound = bind(&database, sql, &[]).unwrap();
        assert_eq!(bound.columns[0].column_type, Some(T::Float64));
        assert!(
            bound
                .statement
                .to_string()
                .contains("CAST(NULL AS DOUBLE PRECISION)")
        );
    }
}

#[test]
fn partial_index_boolean_columns_are_resolved_and_type_checked() {
    let (_directory, mut database) = database();
    database
        .execute_sql("CREATE TABLE flags (id BIGINT, active BOOLEAN)")
        .unwrap();
    for predicate in [
        "active",
        "(active)",
        "active AND id = 1",
        "id = 1 AND active",
    ] {
        let sql = format!("CREATE UNIQUE INDEX ix ON flags(id) WHERE {predicate}");
        let bound = bind(&database, &sql, &[]).unwrap();
        assert_eq!(bound.effect, StatementEffect::Schema);
        assert!(bound.access.requires_admin);
    }
    assert!(matches!(
        bind(
            &database,
            "CREATE UNIQUE INDEX ix ON flags(id) WHERE id",
            &[]
        ),
        Err(DbError::SqlDatatypeMismatch { .. })
    ));
    assert!(
        bind(
            &database,
            "CREATE UNIQUE INDEX ix ON flags(id) WHERE missing",
            &[]
        )
        .is_err()
    );
}

#[test]
fn unsupported_modifiers_and_window_diagnostics_fail_closed() {
    let (_directory, database) = database();
    for (sql, diagnostic) in [
        (
            "SELECT a.id FROM a GLOBAL JOIN b ON a.id=b.aid",
            "GLOBAL JOIN",
        ),
        ("EXPLAIN QUERY PLAN SELECT id FROM a", "EXPLAIN"),
        (
            "SELECT count(*) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM a",
            "window frames",
        ),
        ("SELECT count(*) OVER w FROM a", "named windows"),
        ("SELECT count(*) OVER (w) FROM a", "named windows"),
    ] {
        let error = bind(&database, sql, &[]).unwrap_err();
        assert!(
            matches!(error, DbError::SqlUnsupported { .. }),
            "{sql}: {error}"
        );
        assert!(error.to_string().contains(diagnostic), "{sql}: {error}");
    }
}

#[test]
fn supplied_hints_without_parameters_preserve_the_native_diagnostic() {
    let (_directory, database) = database();
    for sql in ["SELECT 1", "SELECT id FROM a", "BEGIN"] {
        assert!(
            matches!(
                bind(&database, sql, &[Some(T::I64)]),
                Err(DbError::SqlParameter(message)) if message == "statement does not reference supplied parameters"
            ),
            "{sql}"
        );
    }
    assert!(matches!(
        bind(&database, "SELECT $1", &[Some(T::I64), Some(T::Text)]),
        Err(DbError::SqlParameter(message)) if message == "more declared types than parameter positions"
    ));
    assert_eq!(
        bind(&database, "SELECT $2", &[Some(T::Date)])
            .unwrap()
            .parameter_types,
        vec![Some(T::Date), Some(T::Text)]
    );
}

#[test]
fn foreign_key_actions_share_binding_and_publication_validation() {
    use crate::ReferentialAction::{Cascade, Restrict, SetNull};

    for (clause, expected) in [
        ("", Some(Restrict)),
        ("ON DELETE RESTRICT", Some(Restrict)),
        ("ON DELETE CASCADE", Some(Cascade)),
        ("ON DELETE SET NULL", Some(SetNull)),
        ("ON DELETE NO ACTION", None),
        ("ON DELETE SET DEFAULT", None),
        ("ON UPDATE CASCADE", None),
        ("MATCH FULL", None),
        ("DEFERRABLE INITIALLY DEFERRED", None),
    ] {
        let (_directory, mut database) = database();
        for sql in [
            format!(
                "CREATE TABLE child (parent_id BIGINT, FOREIGN KEY(parent_id) REFERENCES a(id) {clause})"
            ),
            format!("ALTER TABLE b ADD FOREIGN KEY(aid) REFERENCES a(id) {clause}"),
        ] {
            let before = database.catalog().clone();
            let commit = database.commit_id();
            let bound = bind(&database, &sql, &[]);
            assert_eq!(
                database.catalog(),
                &before,
                "binding mutated catalog: {sql}"
            );
            assert_eq!(database.commit_id(), commit, "binding published: {sql}");
            if expected.is_some() {
                let bound = bound.unwrap();
                assert_eq!(bound.effect, StatementEffect::Schema);
                assert!(bound.access.requires_admin);
                assert_eq!(bound.access.read.len(), 1);
                database.execute_sql(&sql).unwrap();
                assert!(
                    database
                        .catalog()
                        .foreign_keys()
                        .all(|fk| Some(fk.on_delete) == expected)
                );
            } else {
                assert!(
                    matches!(bound, Err(DbError::SqlUnsupported { .. })),
                    "{sql}"
                );
                assert!(
                    matches!(
                        database.execute_sql(&sql),
                        Err(DbError::SqlUnsupported { .. })
                    ),
                    "{sql}"
                );
                assert_eq!(database.catalog(), &before);
                assert_eq!(database.commit_id(), commit);
            }
        }
    }
}

#[test]
fn ddl_resolves_references_without_scanning() {
    let (_directory, database) = database();
    let bound = bind(
        &database,
        "CREATE TABLE child (id BIGINT, FOREIGN KEY(id) REFERENCES a(id))",
        &[],
    )
    .unwrap();
    assert_eq!(bound.effect, StatementEffect::Schema);
    assert!(bound.access.requires_admin);
    assert_eq!(bound.access.read.len(), 1);
    assert!(
        bind(
            &database,
            "CREATE TABLE child (id BIGINT, FOREIGN KEY(id) REFERENCES missing(id))",
            &[]
        )
        .is_err()
    );
    assert!(
        bind(
            &database,
            "ALTER TABLE a ALTER COLUMN id TYPE TEXT USING (SELECT label FROM b)",
            &[]
        )
        .is_err()
    );
    assert!(
        bind(
            &database,
            "CREATE INDEX x ON a(id) WHERE id = (SELECT aid FROM b)",
            &[]
        )
        .is_err()
    );
}
