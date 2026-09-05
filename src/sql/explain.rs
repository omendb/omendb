//! `EXPLAIN <statement>`: report the row-access strategy the executor
//! will actually use, without running the statement.
//!
//! OmenDB has no cost-based planner; the executor's decision is a
//! deterministic function of the catalog (primary key, secondary
//! indexes) and the predicate shape. This module resolves the same
//! [`PredicatePlan`] the executor does, so `EXPLAIN` output never
//! disagrees with execution: whatever access path it names is the one
//! taken.

use sqlparser::ast::Statement;

use crate::{DbError, Result, Value};

use super::query::{self, PredicatePlan};
use super::{statement_kind, table_from_join, unsupported};

/// One access path, named the way PostgreSQL names nodes.
#[derive(Debug, PartialEq)]
pub(crate) enum AccessPath {
    /// One row fetched through its exact primary-key identity.
    PrimaryKeyLookup { table: String },
    /// Rows fetched through one secondary index whose full column list
    /// the predicate binds.
    IndexScan {
        table: String,
        index: String,
        columns: Vec<String>,
    },
    /// The predicate pins a primary-key column to NULL: nothing can
    /// match and the executor short-circuits to no rows.
    NullShortCircuit { table: String },
    /// Every table row is read and filtered.
    SeqScan { table: String },
}

impl AccessPath {
    fn plan_line(&self) -> String {
        match self {
            AccessPath::PrimaryKeyLookup { table } => {
                format!("Primary Key Lookup on {table}")
            }
            AccessPath::IndexScan {
                table,
                index,
                columns,
            } => format!(
                "Index Scan using {index} on {table} ({})",
                columns.join(", ")
            ),
            AccessPath::NullShortCircuit { table } => {
                format!("Result (empty) on {table} (NULL equality)")
            }
            AccessPath::SeqScan { table } => format!("Seq Scan on {table}"),
        }
    }
}

/// The EXPLAIN result: plan lines as one text column.
pub(crate) struct SqlExplainResult {
    pub(crate) columns: Vec<crate::SqlColumn>,
    pub(crate) rows: Vec<Vec<Value>>,
}

/// Analyze one supported statement's row-access strategy. `EXPLAIN`
/// accepts SELECT, UPDATE, and DELETE (the three row-reading
/// statements) and reports INSERT's target and row count; anything
/// else is refused with the same statement kind the embedded tier
/// reports.
pub(crate) fn explain_statement(
    database: &crate::RelationalDatabase,
    statement: &Statement,
    params: &[Value],
) -> Result<SqlExplainResult> {
    match statement {
        Statement::Query(query) => {
            let (table, selection) = query::query_table_and_selection(database.catalog(), query)?;
            explain_table_predicate(database, &table, selection.as_ref(), params)
        }
        Statement::Update(update) => {
            let table = table_from_join(database.catalog(), &update.table)?;
            explain_table_predicate(database, table, update.selection.as_ref(), params)
        }
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            if tables.len() != 1 {
                return Err(unsupported("DELETE", "only one table is supported"));
            }
            let table = table_from_join(database.catalog(), &tables[0])?;
            explain_table_predicate(database, table, delete.selection.as_ref(), params)
        }
        Statement::Insert(insert) => {
            let name = query::table_name_from_insert(database.catalog(), insert)?;
            let rows = insert_rows_per_statement(insert);
            Ok(plan_result(format!(
                "Insert into {name} ({rows} row{} per statement)",
                if rows == 1 { "" } else { "s" },
            )))
        }
        other => Err(DbError::SqlUnsupported {
            statement: statement_kind(other),
            reason: "EXPLAIN supports SELECT, INSERT, UPDATE, and DELETE".to_owned(),
        }),
    }
}

fn explain_table_predicate(
    database: &crate::RelationalDatabase,
    table: &crate::TableDefinition,
    selection: Option<&sqlparser::ast::Expr>,
    params: &[Value],
) -> Result<SqlExplainResult> {
    let plan = query::resolve_predicate_plan(database.catalog(), table, selection, params)?;
    let path = match plan {
        Some(PredicatePlan::PrimaryKeyLookup { .. }) => AccessPath::PrimaryKeyLookup {
            table: table.name.clone(),
        },
        Some(PredicatePlan::NullShortCircuit) => AccessPath::NullShortCircuit {
            table: table.name.clone(),
        },
        Some(PredicatePlan::IndexScan { index_id, .. }) => {
            let definition = database
                .catalog()
                .index(index_id)
                .ok_or_else(|| DbError::InvalidState("index is missing".to_owned()))?;
            AccessPath::IndexScan {
                table: table.name.clone(),
                index: database
                    .catalog()
                    .index_name(index_id)
                    .unwrap_or("unnamed")
                    .to_owned(),
                columns: definition
                    .columns
                    .iter()
                    .map(|column| {
                        table
                            .columns
                            .iter()
                            .find(|candidate| candidate.id == *column)
                            .map(|candidate| candidate.name.clone())
                            .unwrap_or_else(|| format!("column {}", column.0))
                    })
                    .collect(),
            }
        }
        None => AccessPath::SeqScan {
            table: table.name.clone(),
        },
    };
    Ok(plan_result(path.plan_line()))
}

fn plan_result(line: String) -> SqlExplainResult {
    SqlExplainResult {
        columns: vec![crate::SqlColumn::typed(
            "Query Plan",
            crate::ColumnType::Text,
        )],
        rows: vec![vec![Value::Text(line)]],
    }
}

/// Rows the INSERT writes per execution (VALUES arity; SELECT sources
/// report one row per source statement for planning purposes).
fn insert_rows_per_statement(insert: &sqlparser::ast::Insert) -> usize {
    let Some(source) = &insert.source else {
        return 1;
    };
    match source.body.as_ref() {
        sqlparser::ast::SetExpr::Values(values) => values.rows.len(),
        _ => 1,
    }
}
