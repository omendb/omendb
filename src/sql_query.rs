use std::cmp::Ordering;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, LimitClause, OrderBy, OrderByKind, Query, Select,
    SelectItem, SetExpr,
};

use crate::{
    DbError, RelationalDatabase, RelationalDatabaseTransaction, Result, Row, SqlColumn, SqlResult,
    TableDefinition, Value,
};

use super::values::literal_value;
use super::{column_position, find_table, simple_object_name, table_from_join, unsupported};

pub(super) fn execute_query(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    query: &Query,
    params: &[Value],
) -> Result<SqlResult> {
    validate_query_shape(query)?;
    if matches!(query.body.as_ref(), SetExpr::SetOperation { .. }) {
        return execute_set_operation(query, query.body.as_ref(), database, transaction, params);
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(unsupported(
            "SELECT",
            "only SELECT bodies are supported here",
        ));
    };
    let select = select.as_ref();
    if select.from.is_empty() {
        if select.selection.is_some() || select.projection.is_empty() || query.order_by.is_some() {
            return Err(unsupported("SELECT", "literal SELECT has no WHERE clause"));
        }
        let (offset, limit) = query_window(query, params)?;
        if offset != 0 || limit == 0 {
            let columns = select
                .projection
                .iter()
                .map(|item| {
                    let (expression, alias) = projection_expression(item)?;
                    Ok(SqlColumn {
                        name: alias.unwrap_or_else(|| expression.to_string()),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(SqlResult::rows(columns, Vec::new()));
        }
        let mut columns = Vec::with_capacity(select.projection.len());
        let mut row = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            let (expression, alias) = projection_expression(item)?;
            let value = literal_value(expression, params)?;
            columns.push(SqlColumn {
                name: alias.unwrap_or_else(|| expression.to_string()),
            });
            row.push(value);
        }
        return Ok(SqlResult::rows(columns, vec![row]));
    }
    if select.from.len() != 1 {
        return Err(unsupported("SELECT", "only one table in FROM is supported"));
    }
    if !select.from[0].joins.is_empty() {
        return execute_join_query(database, transaction, query, select, params);
    }
    let table = table_from_join(database.catalog(), &select.from[0])?;
    if is_aggregate_query(select) {
        return execute_aggregate_query(database, transaction, query, select, table, params);
    }
    if select.distinct.is_some()
        && !matches!(
            select.distinct.as_ref(),
            Some(sqlparser::ast::Distinct::Distinct)
        )
    {
        return Err(unsupported("SELECT", "only plain DISTINCT is supported"));
    }
    let scalars = resolve_scalar_subqueries(database, transaction, &select.projection, params)?;
    let projection = projection_plan_for_select(select, table, params, &scalars)?;
    let order = order_plan(query.order_by.as_ref(), table)?;
    let (offset, limit) = query_window(query, params)?;
    if limit == 0 {
        return Ok(SqlResult::rows(projection.columns, Vec::new()));
    }
    let subqueries = resolve_subqueries(database, transaction, select.selection.as_ref(), params)?;
    let rows = transaction.scan(database, table.id, usize::MAX)?;
    let required_rows = offset.saturating_add(limit);
    let can_stop_after_window = order.is_empty() && select.distinct.is_none();
    let mut matching_rows = Vec::new();
    for row in rows {
        if let Some(selection) = &select.selection
            && predicate(selection, &row, table, params, &subqueries)? != Truth::True
        {
            continue;
        }
        matching_rows.push(row);
        if can_stop_after_window && matching_rows.len() >= required_rows {
            break;
        }
    }
    if !order.is_empty() {
        validate_order_rows(&matching_rows, table, &order)?;
        matching_rows.sort_by(|left, right| compare_rows(left, right, &order));
    }
    let projected_rows = matching_rows
        .into_iter()
        .map(|row| project_row(&projection, &row))
        .collect::<Result<Vec<_>>>()?;
    let projected_rows = if select.distinct.is_some() {
        apply_distinct(projected_rows)
    } else {
        projected_rows
    };
    let result_rows = projected_rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();
    Ok(SqlResult::rows(projection.columns, result_rows))
}

#[derive(Default)]
pub(super) struct ResolvedSubqueries {
    /// IN (SELECT ...): candidate values by canonical subquery text.
    pub(super) in_lists: std::collections::HashMap<String, Vec<Value>>,
    /// EXISTS (...): row-presence by canonical subquery text.
    pub(super) exists: std::collections::HashMap<String, bool>,
}

impl ResolvedSubqueries {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

/// Pre-execute every uncorrelated subquery in the predicate exactly once,
/// keyed by canonical text for per-row lookup.
fn resolve_subqueries(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    selection: Option<&Expr>,
    params: &[Value],
) -> Result<ResolvedSubqueries> {
    let mut resolved = ResolvedSubqueries::default();
    let Some(selection) = selection else {
        return Ok(resolved);
    };
    let mut found = Vec::new();
    collect_subqueries(selection, &mut found);
    for expression in found {
        let (key, query, kind) = match expression {
            Expr::InSubquery { subquery, .. } => (subquery.to_string(), subquery, "IN"),
            Expr::Exists { subquery, .. } => (subquery.to_string(), subquery, "EXISTS"),
            _ => unreachable!("collector only pushes subquery forms"),
        };
        if resolved.in_lists.contains_key(&key) || resolved.exists.contains_key(&key) {
            continue;
        }
        let result = execute_query(database, transaction, query, params)?;
        match kind {
            "IN" => {
                if result.columns.len() != 1 {
                    return Err(unsupported(
                        "subquery",
                        "IN (SELECT ...) must select exactly one column",
                    ));
                }
                resolved.in_lists.insert(
                    key,
                    result
                        .rows
                        .into_iter()
                        .map(|mut row| row.remove(0))
                        .collect(),
                );
            }
            _ => {
                resolved.exists.insert(key, !result.rows.is_empty());
            }
        }
    }
    Ok(resolved)
}

fn collect_subqueries<'a>(expression: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expression {
        Expr::Nested(inner) => collect_subqueries(inner, out),
        Expr::UnaryOp { expr: inner, .. } => collect_subqueries(inner, out),
        Expr::BinaryOp { left, right, .. } => {
            collect_subqueries(left, out);
            collect_subqueries(right, out);
        }
        Expr::InSubquery { .. } | Expr::Exists { .. } => out.push(expression),
        _ => {}
    }
}

/// UNION / INTERSECT / EXCEPT over two fully-executed sub-selects.
/// Column names come from the left side; counts must match. The ALL
/// quantifier keeps duplicates; every other form applies set semantics
/// (dedup, membership filter). Each side executes as its own query with
/// the outer statement's ORDER BY/LIMIT stripped - they apply to the
/// combined result only.
fn execute_set_operation(
    query: &Query,
    body: &sqlparser::ast::SetExpr,
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    params: &[Value],
) -> Result<SqlResult> {
    let SetExpr::SetOperation {
        op,
        set_quantifier: quantifier,
        left,
        right,
    } = body
    else {
        unreachable!("dispatch checked");
    };
    use std::collections::HashSet;

    let strip_side_clauses = |side: &sqlparser::ast::SetExpr| -> Query {
        let mut side_query = query.clone();
        side_query.body = Box::new(side.clone());
        side_query.order_by = None;
        side_query.limit_clause = Some(sqlparser::ast::LimitClause::LimitOffset {
            limit: None,
            offset: None,
            limit_by: Vec::new(),
        });
        side_query.fetch = None;
        side_query
    };

    let keep_duplicates = matches!(quantifier, sqlparser::ast::SetQuantifier::All);

    let left_result = execute_query(database, transaction, &strip_side_clauses(left), params)?;
    let right_result = execute_query(database, transaction, &strip_side_clauses(right), params)?;

    if left_result.columns.len() != right_result.columns.len() {
        return Err(DbError::InvalidState(format!(
            "set operation sides have different column counts: {} vs {}",
            left_result.columns.len(),
            right_result.columns.len()
        )));
    }

    let rows = match op {
        sqlparser::ast::SetOperator::Union => {
            let mut rows = left_result.rows;
            rows.extend(right_result.rows);
            if !keep_duplicates {
                apply_distinct(rows)
            } else {
                rows
            }
        }
        sqlparser::ast::SetOperator::Intersect => {
            let right_set: HashSet<Vec<Value>> = right_result.rows.into_iter().collect();
            let mut seen = HashSet::new();
            let mut rows = Vec::new();
            for row in left_result.rows {
                if right_set.contains(&row) && seen.insert(row.clone()) {
                    rows.push(row);
                }
            }
            rows
        }
        sqlparser::ast::SetOperator::Except => {
            let right_set: HashSet<Vec<Value>> = right_result.rows.into_iter().collect();
            let mut seen = HashSet::new();
            let mut rows = Vec::new();
            for row in left_result.rows {
                if !right_set.contains(&row) && seen.insert(row.clone()) {
                    rows.push(row);
                }
            }
            rows
        }
        other => {
            return Err(unsupported(
                "SELECT",
                &format!("set operator {other} is not supported"),
            ));
        }
    };

    // Whole-statement ORDER BY resolves against the output columns.
    let mut rows = rows;
    if let Some(order_by) = &query.order_by {
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Err(unsupported(
                "SELECT",
                "unsupported ORDER BY shape over set operations",
            ));
        };
        let mut terms = Vec::new();
        for kind in expressions {
            let Expr::Identifier(identifier) = &kind.expr else {
                return Err(unsupported(
                    "SELECT",
                    "ORDER BY over set operations must reference a column name",
                ));
            };
            let name = &identifier.value;
            let position = left_result
                .columns
                .iter()
                .position(|column| &column.name == name)
                .ok_or_else(|| {
                    DbError::InvalidState(format!("unknown column {name} in ORDER BY"))
                })?;
            terms.push((position, kind.options.asc.unwrap_or(true)));
        }
        let mut sorted = rows;
        sorted.sort_by(|left, right| {
            for (position, asc) in &terms {
                let ordering = joined_value_ordering(&left[*position], &right[*position]);
                let ordering = if *asc { ordering } else { ordering.reverse() };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });
        rows = sorted;
    }

    let (offset, limit) = query_window(query, params)?;
    let rows = rows.into_iter().skip(offset).take(limit).collect();
    Ok(SqlResult::rows(left_result.columns, rows))
}

type ScalarSubqueries = std::collections::HashMap<String, Value>;

fn collect_scalar_subqueries<'a>(expression: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expression {
        Expr::Nested(inner) => collect_scalar_subqueries(inner, out),
        Expr::Subquery(_) => out.push(expression),
        _ => {}
    }
}

/// Scalar subqueries in projections resolve once per statement to a
/// single value (exactly one row, first column).
fn resolve_scalar_subqueries(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    projection: &[SelectItem],
    params: &[Value],
) -> Result<ScalarSubqueries> {
    let mut resolved = ScalarSubqueries::default();
    let mut found = Vec::new();
    for item in projection {
        if let Ok((expr, _)) = projection_expression(item) {
            collect_scalar_subqueries(expr, &mut found);
        }
    }
    for expression in found {
        let key = expression.to_string();
        if resolved.contains_key(&key) {
            continue;
        }
        let Expr::Subquery(query) = expression else {
            unreachable!("collector only pushes Subquery");
        };
        let result = execute_query(database, transaction, query, params)?;
        if result.rows.len() != 1 || result.columns.is_empty() {
            return Err(unsupported(
                "subquery",
                "scalar subqueries must return exactly one row",
            ));
        }
        resolved.insert(key, result.rows[0][0].clone());
    }
    Ok(resolved)
}

/// DISTINCT deduplicates projected rows preserving first-seen order
/// (PostgreSQL applies DISTINCT after projection, before LIMIT).
fn apply_distinct(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    let mut seen = std::collections::HashSet::new();
    let mut unique = Vec::with_capacity(rows.len());
    for row in rows {
        if seen.insert(row.clone()) {
            unique.push(row);
        }
    }
    unique
}

fn validate_query_shape(query: &Query) -> Result<()> {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
    {
        return Err(unsupported(
            "SELECT",
            "only a single SELECT without locking or other clauses is supported",
        ));
    }
    Ok(())
}

fn query_window(query: &Query, params: &[Value]) -> Result<(usize, usize)> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok((0, usize::MAX));
    };
    let (limit, offset) = match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } if limit_by.is_empty() => (limit.as_ref(), offset.as_ref()),
        LimitClause::OffsetCommaLimit { offset, limit } => {
            let offset = non_negative_integer(offset, params, "OFFSET")?;
            let limit = non_negative_integer(limit, params, "LIMIT")?;
            return Ok((offset, limit));
        }
        _ => {
            return Err(unsupported(
                "SELECT",
                "only literal LIMIT/OFFSET is supported",
            ));
        }
    };
    let limit = limit
        .map(|expression| non_negative_integer(expression, params, "LIMIT"))
        .transpose()?
        .unwrap_or(usize::MAX);
    let offset = offset
        .map(|offset| non_negative_integer(&offset.value, params, "OFFSET"))
        .transpose()?
        .unwrap_or(0);
    Ok((offset, limit))
}

fn non_negative_integer(expression: &Expr, params: &[Value], clause: &str) -> Result<usize> {
    match literal_value(expression, params)? {
        Value::I64(value) if value >= 0 => usize::try_from(value)
            .map_err(|_| DbError::InvalidState(format!("SQL {clause} is too large"))),
        Value::U64(value) => usize::try_from(value)
            .map_err(|_| DbError::InvalidState(format!("SQL {clause} is too large"))),
        _ => Err(DbError::InvalidState(format!(
            "SQL {clause} must be a non-negative integer"
        ))),
    }
}

/// A projection term that evaluates per row instead of reading a fixed
/// column or constant: binary arithmetic over nested operands.
enum ComputedTerm {
    Column(usize),
    Literal(Value),
    Binary {
        op: BinaryOperator,
        left: Box<ComputedTerm>,
        right: Box<ComputedTerm>,
    },
}

impl ComputedTerm {
    fn evaluate(&self, values: &[Value]) -> Result<Value> {
        match self {
            Self::Column(position) => values.get(*position).cloned().ok_or_else(|| {
                DbError::InvalidState("row is missing a projected value".to_owned())
            }),
            Self::Literal(value) => Ok(value.clone()),
            Self::Binary { op, left, right } => {
                let lhs = left.evaluate(values)?;
                let rhs = right.evaluate(values)?;
                arithmetic(op, &lhs, &rhs)
            }
        }
    }
}

fn arithmetic(op: &BinaryOperator, lhs: &Value, rhs: &Value) -> Result<Value> {
    use BinaryOperator::*;
    let (l, r) = match (lhs, rhs) {
        (Value::U64(l), Value::U64(r)) => (*l as i128, *r as i128),
        (Value::I64(l), Value::I64(r)) => (*l as i128, *r as i128),
        (Value::U64(l), Value::I64(r)) => (*l as i128, *r as i128),
        (Value::I64(l), Value::U64(r)) => (*l as i128, *r as i128),
        _ => {
            return Err(DbError::InvalidState(format!(
                "arithmetic requires numeric operands, got {lhs:?} {op} {rhs:?}"
            )));
        }
    };
    let value = match op {
        Plus => l.checked_add(r),
        Minus => l.checked_sub(r),
        Multiply => l.checked_mul(r),
        Divide => {
            if r == 0 {
                return Err(DbError::InvalidState("division by zero".to_owned()));
            }
            Some(l.div_euclid(r))
        }
        Modulo => {
            if r == 0 {
                return Err(DbError::InvalidState("modulo by zero".to_owned()));
            }
            Some(l.rem_euclid(r))
        }
        _ => {
            return Err(unsupported(
                "projection",
                "only arithmetic operators are supported here",
            ));
        }
    }
    .ok_or_else(|| DbError::InvalidState("arithmetic overflow".to_owned()))?;
    if value < 0 || value > u64::MAX as i128 {
        return Err(DbError::InvalidState(format!(
            "arithmetic result {value} does not fit the engine's integer types"
        )));
    }
    Ok(Value::U64(value as u64))
}

fn plan_computed(
    expression: &Expr,
    table: &TableDefinition,
    params: &[Value],
) -> Option<ComputedTerm> {
    match expression {
        Expr::Identifier(identifier) => table
            .columns
            .iter()
            .position(|column| column.name == identifier.value)
            .map(ComputedTerm::Column),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => table
            .columns
            .iter()
            .position(|column| column.name == parts[parts.len() - 1].value)
            .map(ComputedTerm::Column),
        Expr::Nested(inner) => plan_computed(inner, table, params),
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ) =>
        {
            let left = plan_computed(left, table, params)?;
            let right = plan_computed(right, table, params)?;
            Some(ComputedTerm::Binary {
                op: op.clone(),
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        Expr::Value(_) | Expr::UnaryOp { .. } => literal_value(expression, params)
            .ok()
            .map(ComputedTerm::Literal),
        _ => None,
    }
}

#[derive(Clone, Copy)]
struct OrderTerm {
    position: usize,
    ascending: bool,
    nulls_first: bool,
}

fn order_plan(order_by: Option<&OrderBy>, table: &TableDefinition) -> Result<Vec<OrderTerm>> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    if order_by.interpolate.is_some() {
        return Err(unsupported("ORDER BY", "INTERPOLATE is not supported"));
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(unsupported("ORDER BY", "ORDER BY ALL is not supported"));
    };
    if expressions.is_empty() {
        return Err(DbError::InvalidState(
            "ORDER BY must contain at least one expression".to_owned(),
        ));
    }
    expressions
        .iter()
        .map(|expression| {
            if expression.with_fill.is_some() {
                return Err(unsupported("ORDER BY", "WITH FILL is not supported"));
            }
            let position = column_position(table, &expression.expr)?.ok_or_else(|| {
                unsupported(
                    "ORDER BY",
                    "only plain table columns can be used for ordering",
                )
            })?;
            let ascending = expression.options.asc.unwrap_or(true);
            Ok(OrderTerm {
                position,
                ascending,
                // Match the conventional SQL default while keeping explicit
                // NULLS FIRST/LAST available for callers that need it.
                nulls_first: expression.options.nulls_first.unwrap_or(!ascending),
            })
        })
        .collect()
}

fn validate_order_rows(rows: &[Row], table: &TableDefinition, terms: &[OrderTerm]) -> Result<()> {
    for row in rows {
        for term in terms {
            let value = row.values.get(term.position).ok_or_else(|| {
                DbError::InvalidState("row is missing an ORDER BY column".to_owned())
            })?;
            let column = table.columns.get(term.position).ok_or_else(|| {
                DbError::InvalidState("ORDER BY column position is invalid".to_owned())
            })?;
            if !value.matches(column.data_type) {
                return Err(DbError::InvalidState(format!(
                    "row value does not match ORDER BY column {}",
                    column.name
                )));
            }
        }
    }
    Ok(())
}

fn compare_rows(left: &Row, right: &Row, terms: &[OrderTerm]) -> Ordering {
    for term in terms {
        let left_value = &left.values[term.position];
        let right_value = &right.values[term.position];
        match (left_value, right_value) {
            (Value::Null, Value::Null) => continue,
            (Value::Null, _) => {
                return if term.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (_, Value::Null) => {
                return if term.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            _ => {
                let ordering = value_cmp(left_value, right_value).unwrap_or(Ordering::Equal);
                if ordering != Ordering::Equal {
                    return if term.ascending {
                        ordering
                    } else {
                        ordering.reverse()
                    };
                }
            }
        }
    }
    // Make pagination deterministic when the requested ordering is not
    // unique. The scan already uses canonical primary-key order, but keeping
    // the tie-breaker explicit makes that contract independent of the store.
    left.primary.cmp(&right.primary)
}

struct ProjectionPlan {
    columns: Vec<SqlColumn>,
    positions: Vec<Option<usize>>,
    literals: Vec<Option<Value>>,
    /// Per-row arithmetic expressions, checked before positions.
    computed: Vec<Option<ComputedTerm>>,
}

fn projection_plan_for_select(
    select: &Select,
    table: &TableDefinition,
    params: &[Value],
    scalars: &ScalarSubqueries,
) -> Result<ProjectionPlan> {
    // Plain DISTINCT is applied post-projection by the caller.
    projection_plan(select, table, params, scalars)
}

fn projection_plan(
    select: &Select,
    table: &TableDefinition,
    params: &[Value],
    scalars: &ScalarSubqueries,
) -> Result<ProjectionPlan> {
    if select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !matches!(&select.group_by, GroupByExpr::Expressions(expressions, modifiers) if expressions.is_empty() && modifiers.is_empty())
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(unsupported(
            "SELECT",
            "only a plain projection is supported",
        ));
    }
    let mut columns = Vec::new();
    let mut positions = Vec::new();
    let mut literals = Vec::new();
    let mut computed: Vec<Option<ComputedTerm>> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(options) if options.to_string().is_empty() => {
                for (position, column) in table.columns.iter().enumerate() {
                    columns.push(SqlColumn {
                        name: column.name.clone(),
                    });
                    positions.push(Some(position));
                    literals.push(None);
                    computed.push(None);
                }
            }
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => {
                let alias = match item {
                    SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                    _ => None,
                };
                if let Some(position) = column_position(table, expression)? {
                    columns.push(SqlColumn {
                        name: alias.unwrap_or_else(|| table.columns[position].name.clone()),
                    });
                    positions.push(Some(position));
                    literals.push(None);
                    computed.push(None);
                } else if let Expr::Subquery(_) = expression {
                    let key = expression.to_string();
                    let value = scalars.get(&key).ok_or_else(|| {
                        DbError::InvalidState(
                            "scalar subquery was not resolved before projection".to_owned(),
                        )
                    })?;
                    columns.push(SqlColumn {
                        name: alias.unwrap_or_else(|| expression.to_string()),
                    });
                    positions.push(None);
                    literals.push(Some(value.clone()));
                    computed.push(None);
                } else if let Some(term) = plan_computed(expression, table, params) {
                    columns.push(SqlColumn {
                        name: alias.unwrap_or_else(|| expression.to_string()),
                    });
                    positions.push(None);
                    literals.push(None);
                    computed.push(Some(term));
                } else {
                    columns.push(SqlColumn {
                        name: alias.unwrap_or_else(|| expression.to_string()),
                    });
                    positions.push(None);
                    literals.push(Some(literal_value(expression, params)?));
                    computed.push(None);
                }
            }
            _ => return Err(unsupported("SELECT", "this projection is not supported")),
        }
    }
    if columns.is_empty() {
        return Err(DbError::InvalidState(
            "SELECT must project at least one column".to_owned(),
        ));
    }
    while computed.len() < columns.len() {
        computed.push(None);
    }
    Ok(ProjectionPlan {
        columns,
        positions,
        literals,
        computed,
    })
}

fn project_row(projection: &ProjectionPlan, row: &Row) -> Result<Vec<Value>> {
    project_values(projection, &row.values)
}

fn project_values(projection: &ProjectionPlan, values: &[Value]) -> Result<Vec<Value>> {
    let mut output = Vec::with_capacity(projection.columns.len());
    for ((position, literal), computed) in projection
        .positions
        .iter()
        .zip(&projection.literals)
        .zip(&projection.computed)
    {
        if let Some(term) = computed {
            output.push(term.evaluate(values)?);
        } else {
            output.push(match (position, literal) {
                (Some(position), None) => values.get(*position).cloned().ok_or_else(|| {
                    DbError::InvalidState("row is missing a projected value".to_owned())
                })?,
                (None, Some(value)) => value.clone(),
                _ => {
                    return Err(DbError::InvalidState(
                        "invalid SQL projection plan".to_owned(),
                    ));
                }
            });
        }
    }
    Ok(output)
}

fn projection_expression(item: &SelectItem) -> Result<(&Expr, Option<String>)> {
    match item {
        SelectItem::UnnamedExpr(expression) => Ok((expression, None)),
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias.value.clone()))),
        _ => Err(unsupported(
            "SELECT",
            "literal SELECT accepts expressions only",
        )),
    }
}

fn plain_table<'a>(
    database: &'a RelationalDatabase,
    factor: &sqlparser::ast::TableFactor,
) -> Result<&'a TableDefinition> {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            find_table(database.catalog(), simple_object_name(name, "table")?)
        }
        _ => Err(unsupported("JOIN", "only plain tables are supported")),
    }
}

/// Inner equi-join over snapshots: nested loop with the ON equality
/// resolved to column positions at planning time. v1 shape per
/// `design/OLTP_COMPETITIVE_GAPS.md` slice 3: wildcard projection,
/// optional conjunctive WHERE over the combined schema, no aliases,
/// no ORDER BY.
#[allow(clippy::too_many_arguments)]
fn execute_join_query(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    query: &Query,
    select: &sqlparser::ast::Select,
    params: &[Value],
) -> Result<SqlResult> {
    // Fold joins left-to-right: the accumulated combined rows join each
    // next relation as a pairwise nested loop, so ON operands must land
    // one side in the accumulated scope and one in the new table.
    let mut scope: Vec<(String, &TableDefinition)> = vec![(
        relation_name(&select.from[0].relation)?,
        plain_table(database, &select.from[0].relation)?,
    )];
    let mut combined_rows: Vec<Vec<Value>> = transaction
        .scan(database, scope[0].1.id, usize::MAX)?
        .into_iter()
        .map(|row| row.values)
        .collect();
    let mut combined_columns: Vec<SqlColumn> = scope[0]
        .1
        .columns
        .iter()
        .map(|column| SqlColumn {
            name: format!("{}.{}", scope[0].0, column.name),
        })
        .collect();

    for join in &select.from[0].joins {
        let right_name = relation_name(&join.relation)?;
        let right = plain_table(database, &join.relation)?;
        let left_outer = matches!(
            join.join_operator,
            sqlparser::ast::JoinOperator::Left(_) | sqlparser::ast::JoinOperator::LeftOuter(_)
        );
        let right_outer = matches!(
            join.join_operator,
            sqlparser::ast::JoinOperator::Right(_) | sqlparser::ast::JoinOperator::RightOuter(_)
        );
        let full_outer = matches!(
            join.join_operator,
            sqlparser::ast::JoinOperator::FullOuter(_)
        );
        let none_constraint = sqlparser::ast::JoinConstraint::None;
        let constraint = match &join.join_operator {
            sqlparser::ast::JoinOperator::CrossJoin(sqlparser::ast::JoinConstraint::None) => {
                &none_constraint
            }
            sqlparser::ast::JoinOperator::Join(constraint)
            | sqlparser::ast::JoinOperator::Inner(constraint)
            | sqlparser::ast::JoinOperator::Left(constraint)
            | sqlparser::ast::JoinOperator::LeftOuter(constraint)
            | sqlparser::ast::JoinOperator::Right(constraint)
            | sqlparser::ast::JoinOperator::RightOuter(constraint)
            | sqlparser::ast::JoinOperator::FullOuter(constraint) => constraint,
            _ => {
                return Err(unsupported(
                    "JOIN",
                    "only INNER, LEFT/RIGHT/FULL OUTER JOIN are supported",
                ));
            }
        };
        // USING/NATURAL are equi-join sugar: terms pair each shared
        // column, and the incoming duplicates are dropped from the
        // emitted schema so the using columns appear once.
        let mut merged_incoming: Vec<usize> = Vec::new();
        let on_terms: Vec<(usize, usize, BinaryOperator)> = match constraint {
            sqlparser::ast::JoinConstraint::On(expression) => {
                on_positions_in_scope(expression, &scope, right)?
            }
            sqlparser::ast::JoinConstraint::Using(names) => {
                let mut terms = Vec::new();
                for name in names {
                    let column_name = name.to_string();
                    let scope_position = unique_bare_position(&combined_columns, &column_name)?;
                    let Some(right_position) =
                        right.columns.iter().position(|c| c.name == column_name)
                    else {
                        return Err(DbError::InvalidState(format!(
                            "USING column {column_name} missing from {right_name}"
                        )));
                    };
                    merged_incoming.push(right_position);
                    terms.push((scope_position, right_position, BinaryOperator::Eq));
                }
                if names.is_empty() {
                    return Err(unsupported("JOIN", "USING requires at least one column"));
                }
                terms
            }
            sqlparser::ast::JoinConstraint::Natural => {
                let mut terms = Vec::new();
                for (index, column) in right.columns.iter().enumerate() {
                    let matches = combined_columns
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| c.name.ends_with(&format!(".{}", column.name)))
                        .map(|(p, _)| p)
                        .collect::<Vec<_>>();
                    if matches.len() == 1 {
                        merged_incoming.push(index);
                        terms.push((matches[0], index, BinaryOperator::Eq));
                    }
                }
                if terms.is_empty() {
                    return Err(unsupported("JOIN", "NATURAL JOIN found no shared columns"));
                }
                terms
            }
            // CROSS JOIN: no ON terms, so every pair matches.
            sqlparser::ast::JoinConstraint::None => Vec::new(),
        };

        let incoming_offset = combined_columns.len();
        for column in &right.columns {
            combined_columns.push(SqlColumn {
                name: format!("{right_name}.{}", column.name),
            });
        }
        scope.push((right_name, right));

        let right_rows = transaction.scan(database, right.id, usize::MAX)?;
        let null_tail = vec![Value::Null; right.columns.len()];
        let scope_width = combined_columns.len() - right.columns.len();
        let null_head = vec![Value::Null; scope_width];
        let width = combined_columns.len();
        let mut next_rows = Vec::new();
        let mut right_matched = vec![false; right_rows.len()];
        for left_row in &combined_rows {
            let mut matched = false;
            for (right_index, right_row) in right_rows.iter().enumerate() {
                if !on_terms
                    .iter()
                    .all(|(scope_position, right_position, on_op)| {
                        join_pair_matches(
                            &left_row[*scope_position],
                            &right_row.values[*right_position],
                            on_op,
                        )
                    })
                {
                    continue;
                }
                matched = true;
                right_matched[right_index] = true;
                let mut combined = Vec::with_capacity(width);
                combined.extend_from_slice(left_row);
                combined.extend_from_slice(&right_row.values);
                if let Some(selection) = &select.selection
                    && !join_predicate(selection, &combined, &combined_columns, params)?
                {
                    continue;
                }
                next_rows.push(combined);
            }
            // LEFT OUTER: an accumulated row with no ON match survives
            // once with NULLs for the incoming columns. WHERE still
            // applies - a null-extended row failing the predicate drops.
            if (left_outer || full_outer) && !matched {
                let mut combined = Vec::with_capacity(width);
                combined.extend_from_slice(left_row);
                combined.extend_from_slice(&null_tail);
                if let Some(selection) = &select.selection
                    && !join_predicate(selection, &combined, &combined_columns, params)?
                {
                    continue;
                }
                next_rows.push(combined);
            }
        }
        // RIGHT/FULL OUTER: incoming rows no accumulated row matched
        // emit once with NULLs for the accumulated columns.
        if right_outer || full_outer {
            for (right_index, right_row) in right_rows.iter().enumerate() {
                if right_matched[right_index] {
                    continue;
                }
                let mut combined = Vec::with_capacity(width);
                combined.extend_from_slice(&null_head);
                combined.extend_from_slice(&right_row.values);
                if let Some(selection) = &select.selection
                    && !join_predicate(selection, &combined, &combined_columns, params)?
                {
                    continue;
                }
                next_rows.push(combined);
            }
        }
        if !merged_incoming.is_empty() {
            // Drop the incoming duplicates (descending so positions stay
            // valid) from every emitted row and from the output schema.
            let mut dropped = merged_incoming.clone();
            dropped.sort_unstable();
            for row in &mut next_rows {
                for position in dropped.iter().rev() {
                    row.remove(*position + incoming_offset);
                }
            }
            for position in dropped.iter().rev() {
                combined_columns.remove(incoming_offset + position);
            }
        }
        combined_rows = next_rows;
    }

    let aggregate_items: Vec<&SelectItem> = select
        .projection
        .iter()
        .filter(|item| matches!(projection_expression(item), Ok((Expr::Function(_), _))))
        .collect();
    let grouped = matches!(
        &select.group_by,
        GroupByExpr::Expressions(expressions, _) if !expressions.is_empty()
    ) || (!aggregate_items.is_empty()
        && aggregate_items.len() == select.projection.len());
    if grouped {
        return joined_grouped_aggregates(
            select,
            &aggregate_items,
            combined_rows,
            &combined_columns,
            query.order_by.as_ref(),
            params,
        );
    }
    if let Some(order_by) = &query.order_by {
        sort_joined_rows(&mut combined_rows, order_by, &combined_columns, params)?;
    }
    if !aggregate_items.is_empty() {
        return Err(unsupported(
            "JOIN",
            "mixing aggregates and plain columns requires GROUP BY",
        ));
    }
    let projection = join_projection_plan(select, &combined_columns, params)?;
    let projected = combined_rows
        .into_iter()
        .map(|row| project_values(&projection, &row))
        .collect::<Result<Vec<_>>>()?;
    let projected = if select.distinct.is_some() {
        apply_distinct(projected)
    } else {
        projected
    };
    let (offset, limit) = query_window(query, params)?;
    let projected = projected
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    Ok(SqlResult::rows(projection.columns, projected))
}

/// GROUP BY over joins: hash-group combined rows by the group-by column
/// positions, then emit each group's columns plus its aggregates in
/// first-seen order. Group-by items must be plain columns; aggregates
/// reuse the no-GROUP-BY plan machinery per group.
fn joined_grouped_aggregates(
    select: &Select,
    aggregate_items: &[&SelectItem],
    rows: Vec<Vec<Value>>,
    combined_columns: &[SqlColumn],
    order_by: Option<&OrderBy>,
    params: &[Value],
) -> Result<SqlResult> {
    let GroupByExpr::Expressions(expressions, _) = &select.group_by else {
        unreachable!("caller checked");
    };
    let mut group_positions = Vec::new();
    for expr in expressions {
        let (table_hint, name) = match expr {
            Expr::Identifier(identifier) => (None, identifier.value.clone()),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
                Some(parts[parts.len() - 2].value.clone()),
                parts[parts.len() - 1].value.clone(),
            ),
            _ => {
                return Err(unsupported(
                    "JOIN",
                    "GROUP BY items must be columns over joins",
                ));
            }
        };
        let position = match table_hint {
            Some(hint) => combined_columns
                .iter()
                .position(|column| column.name == format!("{hint}.{name}"))
                .ok_or_else(|| {
                    DbError::InvalidState(format!("unknown column {hint}.{name} in join GROUP BY"))
                })?,
            None => {
                let matches: Vec<usize> = combined_columns
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| {
                        column.name == name || column.name.ends_with(&format!(".{name}"))
                    })
                    .map(|(position, _)| position)
                    .collect();
                match matches.len() {
                    1 => matches[0],
                    0 => {
                        return Err(DbError::InvalidState(format!(
                            "unknown column {name} in join GROUP BY"
                        )));
                    }
                    _ => {
                        return Err(unsupported(
                            "JOIN",
                            "ambiguous unqualified GROUP BY column; qualify it",
                        ));
                    }
                }
            }
        };
        group_positions.push(position);
    }

    let mut groups: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();
    for row in rows {
        let key: Vec<Value> = group_positions.iter().map(|p| row[*p].clone()).collect();
        match groups.iter_mut().find(|(existing, _)| *existing == key) {
            Some((_, members)) => members.push(row),
            None => groups.push((key, vec![row])),
        }
    }

    let mut columns = Vec::new();
    let mut output_rows = Vec::new();
    for item in select.projection.iter() {
        if matches!(projection_expression(item), Ok((Expr::Function(_), _))) {
            continue;
        }
        let (expr, alias) = projection_expression(item)?;
        let position = match expr {
            Expr::Identifier(identifier) => combined_columns
                .iter()
                .position(|column| {
                    column.name == identifier.value
                        || column.name.ends_with(&format!(".{}", identifier.value))
                })
                .ok_or_else(|| {
                    DbError::InvalidState(format!(
                        "unknown column {} must appear in GROUP BY or an aggregate",
                        identifier.value
                    ))
                })?,
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => combined_columns
                .iter()
                .position(|column| {
                    column.name
                        == format!(
                            "{}.{}",
                            parts[parts.len() - 2].value,
                            parts[parts.len() - 1].value
                        )
                })
                .ok_or_else(|| DbError::InvalidState("unknown qualified column".to_owned()))?,
            _ => {
                return Err(unsupported(
                    "JOIN",
                    "grouped projections must be columns or aggregates",
                ));
            }
        };
        if !group_positions.contains(&position) {
            return Err(DbError::InvalidState(
                "column must appear in the GROUP BY clause or be used in an aggregate function"
                    .to_owned(),
            ));
        }
        columns.push(SqlColumn {
            name: alias
                .unwrap_or_else(|| {
                    combined_columns[position]
                        .name
                        .split('.')
                        .next_back()
                        .unwrap_or("")
                        .to_owned()
                })
                .replace('.', ""),
        });
    }

    // Column list: group-by columns first (in projection order is not
    // guaranteed; we use group_positions order), then aggregates. Built
    // before the group loop so HAVING can evaluate against it.
    let mut final_columns: Vec<SqlColumn> = group_positions
        .iter()
        .map(|p| SqlColumn {
            name: combined_columns[*p]
                .name
                .split('.')
                .next_back()
                .unwrap_or_default()
                .to_owned(),
        })
        .collect();
    for item in aggregate_items {
        let (_, alias) = projection_expression(item)?;
        let name = match alias {
            Some(alias) => alias,
            None => {
                let Ok((Expr::Function(func), _)) = projection_expression(item) else {
                    unreachable!()
                };
                func.name.to_string().to_lowercase()
            }
        };
        final_columns.push(SqlColumn { name });
    }
    let _ = columns;

    for (key, members) in &groups {
        let mut output = key.clone();
        let aggregate_refs: Vec<&SelectItem> = aggregate_items.to_vec();
        let aggregated = joined_aggregates_into(&aggregate_refs, members, combined_columns)?;
        output.extend(aggregated);
        // HAVING evaluates against this group's output row: group-key
        // columns by name, aggregates matched to their projected output
        // position (so HAVING may reference aggregates not selected
        // verbatim only when they appear in the projection).
        if let Some(having) = &select.having
            && !having_predicate(
                having,
                &output,
                &final_columns,
                group_positions.len(),
                aggregate_items,
                params,
            )?
        {
            continue;
        }
        output_rows.push(output);
    }

    if let Some(order_by) = order_by {
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Err(unsupported("JOIN", "unsupported ORDER BY shape over joins"));
        };
        let mut terms = Vec::new();
        for kind in expressions {
            let Expr::Identifier(identifier) = &kind.expr else {
                return Err(unsupported(
                    "JOIN",
                    "ORDER BY must reference an output column name",
                ));
            };
            let name = &identifier.value;
            let position = final_columns
                .iter()
                .position(|column| &column.name == name)
                .ok_or_else(|| {
                    DbError::InvalidState(format!("unknown column {name} in join ORDER BY"))
                })?;
            terms.push((position, kind.options.asc.unwrap_or(true)));
        }
        output_rows.sort_by(|left, right| {
            for (position, asc) in &terms {
                let ordering = joined_value_ordering(&left[*position], &right[*position]);
                let ordering = if *asc { ordering } else { ordering.reverse() };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });
    }
    Ok(SqlResult::rows(final_columns, output_rows))
}

/// HAVING over a grouped join: identifiers resolve against the output
/// schema, and function operands must match a projected aggregate (by
/// canonical SQL text) whose computed value sits at
/// group_columns + its index among aggregate items.
fn having_predicate(
    expression: &Expr,
    output: &[Value],
    final_columns: &[SqlColumn],
    group_column_count: usize,
    aggregate_items: &[&SelectItem],
    params: &[Value],
) -> Result<bool> {
    let resolve_operand = |expression: &Expr| -> Result<Value> {
        match expression {
            Expr::Function(_) => {
                let text = expression.to_string();
                let normalized = text.replace(' ', "").to_ascii_lowercase();
                for (index, item) in aggregate_items.iter().enumerate() {
                    let (item_expr, _) = projection_expression(item)?;
                    let item_text = item_expr.to_string().replace(' ', "").to_ascii_lowercase();
                    if item_text == normalized {
                        return Ok(output[group_column_count + index].clone());
                    }
                }
                Err(unsupported(
                    "JOIN",
                    "HAVING aggregates must appear in the SELECT projection",
                ))
            }
            _ => combined_value(expression, output, final_columns),
        }
    };
    match expression {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(having_predicate(
                left,
                output,
                final_columns,
                group_column_count,
                aggregate_items,
                params,
            )? && having_predicate(
                right,
                output,
                final_columns,
                group_column_count,
                aggregate_items,
                params,
            )?),
            BinaryOperator::Or => Ok(having_predicate(
                left,
                output,
                final_columns,
                group_column_count,
                aggregate_items,
                params,
            )? || having_predicate(
                right,
                output,
                final_columns,
                group_column_count,
                aggregate_items,
                params,
            )?),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let lhs = resolve_operand(left)?;
                let rhs = literal_value(right, params)?;
                Ok(compare_joined(lhs, rhs, op))
            }
            _ => Err(unsupported(
                "JOIN",
                "unsupported HAVING operator over joins",
            )),
        },
        _ => Err(unsupported("JOIN", "unsupported HAVING shape over joins")),
    }
}

/// Aggregate values for one group, in `items` order.
fn joined_aggregates_into(
    items: &[&SelectItem],
    rows: &[Vec<Value>],
    combined_columns: &[SqlColumn],
) -> Result<Vec<Value>> {
    let result = joined_aggregates(items, rows, combined_columns)?;
    Ok(result.rows.into_iter().next().unwrap_or_default())
}

#[derive(Clone, Copy)]
enum JoinedAggregate {
    CountStar,
    CountColumn,
    Sum,
    Min,
    Max,
}

/// No-GROUP-BY aggregates over the full joined row set. The morsel
/// analytical engine is single-table; this covers the common
/// `SELECT count(*) FROM a JOIN b ...` shape directly.
fn joined_aggregates(
    items: &[&SelectItem],
    rows: &[Vec<Value>],
    combined_columns: &[SqlColumn],
) -> Result<SqlResult> {
    fn resolve_operand(expr: &Expr, combined_columns: &[SqlColumn]) -> Result<Option<usize>> {
        match expr {
            Expr::Identifier(identifier) => combined_columns
                .iter()
                .position(|column| {
                    column.name == identifier.value
                        || column.name.ends_with(&format!(".{}", identifier.value))
                })
                .map(Some)
                .ok_or_else(|| {
                    DbError::InvalidState(format!(
                        "unknown column {} in join aggregate",
                        identifier.value
                    ))
                }),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let qualified = format!(
                    "{}.{}",
                    parts[parts.len() - 2].value,
                    parts[parts.len() - 1].value
                );
                combined_columns
                    .iter()
                    .position(|column| column.name == qualified)
                    .map(Some)
                    .ok_or_else(|| {
                        DbError::InvalidState(format!(
                            "unknown column {qualified} in join aggregate"
                        ))
                    })
            }
            _ => Err(unsupported(
                "JOIN",
                "aggregate arguments must be columns or *",
            )),
        }
    }

    let mut columns = Vec::new();
    let mut plans = Vec::new();
    for item in items {
        let (expr, alias) = projection_expression(item)?;
        let Expr::Function(func) = expr else {
            unreachable!("filtered to functions");
        };
        let func_name = func.name.to_string().to_uppercase();
        let sqlparser::ast::FunctionArguments::List(argument_list) = &func.args else {
            return Err(unsupported("JOIN", "unsupported aggregate argument shape"));
        };
        if argument_list.args.len() != 1 {
            return Err(unsupported("JOIN", "aggregates take exactly one argument"));
        }
        let arg = match &argument_list.args[0] {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => None,
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => {
                Some(resolve_operand(expr, combined_columns)?)
            }
            _ => {
                return Err(unsupported(
                    "JOIN",
                    "aggregate arguments must be columns or *",
                ));
            }
        };
        let kind = match (func_name.as_str(), arg.is_none()) {
            ("COUNT", true) => JoinedAggregate::CountStar,
            ("COUNT", false) => JoinedAggregate::CountColumn,
            ("SUM", false) => JoinedAggregate::Sum,
            ("MIN", false) => JoinedAggregate::Min,
            ("MAX", false) => JoinedAggregate::Max,
            _ => {
                return Err(unsupported(
                    "JOIN",
                    "only COUNT/SUM/MIN/MAX are supported over joins",
                ));
            }
        };
        columns.push(SqlColumn {
            name: alias.unwrap_or_else(|| func_name.to_lowercase()),
        });
        plans.push((kind, arg.flatten()));
    }

    let count_star = plans
        .iter()
        .any(|(kind, _)| matches!(kind, JoinedAggregate::CountStar));
    let mut result = vec![Value::U64(rows.len() as u64); plans.len()];
    for (index, (kind, position)) in plans.iter().enumerate() {
        result[index] = match kind {
            JoinedAggregate::CountStar => Value::U64(rows.len() as u64),
            JoinedAggregate::CountColumn => Value::U64(
                rows.iter()
                    .filter(|row| !matches!(&row[position.expect("resolved")], Value::Null))
                    .count() as u64,
            ),
            JoinedAggregate::Sum | JoinedAggregate::Min | JoinedAggregate::Max => {
                let mut accumulator: Option<Value> = None;
                for row in rows {
                    let value = &row[position.expect("resolved")];
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    accumulator = Some(match (kind, accumulator.take()) {
                        (JoinedAggregate::Min, Some(current)) => {
                            if joined_value_ordering(value, &current) == Ordering::Less {
                                value.clone()
                            } else {
                                current
                            }
                        }
                        (JoinedAggregate::Max, Some(current)) => {
                            if joined_value_ordering(value, &current) == Ordering::Greater {
                                value.clone()
                            } else {
                                current
                            }
                        }
                        (JoinedAggregate::Sum, Some(Value::U64(current))) => match value {
                            Value::U64(addend) => Value::U64(current + addend),
                            Value::I64(addend) => Value::I64(current as i64 + addend),
                            other => {
                                return Err(DbError::InvalidState(format!("cannot sum {other:?}")));
                            }
                        },
                        (JoinedAggregate::Sum, Some(Value::I64(current))) => match value {
                            Value::I64(addend) => Value::I64(current + addend),
                            Value::U64(addend) => Value::I64(current + *addend as i64),
                            other => {
                                return Err(DbError::InvalidState(format!("cannot sum {other:?}")));
                            }
                        },
                        (_, None) => value.clone(),
                        _ => unreachable!("min/max handled above"),
                    });
                }
                accumulator.unwrap_or(Value::Null)
            }
        };
    }
    let _ = count_star;
    Ok(SqlResult::rows(columns, vec![result]))
}

/// Projection over the combined join schema: wildcard spans both tables
/// (qualified names), identifiers resolve qualified or by unique column
/// name, literals and bind parameters project directly. Aggregates and
/// function calls stay refused - the aggregate executor is single-table.
fn join_projection_plan(
    select: &Select,
    combined_columns: &[SqlColumn],
    params: &[Value],
) -> Result<ProjectionPlan> {
    if select.distinct.is_some()
        && !matches!(
            select.distinct.as_ref(),
            Some(sqlparser::ast::Distinct::Distinct)
        )
    {
        return Err(unsupported("SELECT", "only plain DISTINCT is supported"));
    }
    if select.select_modifiers.is_some()
        || select.group_by != GroupByExpr::Expressions(Vec::new(), Vec::new())
        || select.having.is_some()
    {
        return Err(unsupported(
            "JOIN",
            "only a plain projection is supported over joins",
        ));
    }
    let resolve = |expression: &Expr| -> Result<Option<usize>> {
        let (table_hint, name) = match expression {
            Expr::Identifier(identifier) => (None, identifier.value.clone()),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
                Some(parts[parts.len() - 2].value.clone()),
                parts[parts.len() - 1].value.clone(),
            ),
            _ => {
                return Err(unsupported(
                    "JOIN",
                    "projections must be columns, literals, or parameters",
                ));
            }
        };
        match table_hint {
            Some(hint) => combined_columns
                .iter()
                .position(|column| column.name == format!("{hint}.{name}"))
                .map(Some)
                .ok_or_else(|| {
                    DbError::InvalidState(format!(
                        "unknown column {hint}.{name} in join projection"
                    ))
                }),
            None => {
                let matches: Vec<usize> = combined_columns
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| {
                        column.name == name || column.name.ends_with(&format!(".{name}"))
                    })
                    .map(|(position, _)| position)
                    .collect();
                match matches.len() {
                    1 => Ok(Some(matches[0])),
                    0 => Err(DbError::InvalidState(format!(
                        "unknown column {name} in join projection"
                    ))),
                    _ => Err(unsupported(
                        "JOIN",
                        "ambiguous unqualified column; qualify it with the table name",
                    )),
                }
            }
        }
    };
    let mut columns = Vec::new();
    let mut positions = Vec::new();
    let mut computed: Vec<Option<ComputedTerm>> = Vec::new();
    let mut literals = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(options) if options.to_string().is_empty() => {
                for (position, column) in combined_columns.iter().enumerate() {
                    columns.push(SqlColumn {
                        name: column.name.clone(),
                    });
                    positions.push(Some(position));
                    literals.push(None);
                }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                let position = resolve(&Expr::Identifier(identifier.clone()))?;
                columns.push(SqlColumn {
                    name: identifier.value.clone(),
                });
                positions.push(position);
                literals.push(None);
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() >= 2 => {
                let position = resolve(&Expr::CompoundIdentifier(parts.clone()))?;
                columns.push(SqlColumn {
                    name: parts[parts.len() - 1].value.clone(),
                });
                positions.push(position);
                literals.push(None);
            }
            SelectItem::ExprWithAlias { expr, alias, .. }
                if matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) =>
            {
                let position = resolve(expr)?;
                columns.push(SqlColumn {
                    name: alias.value.clone(),
                });
                positions.push(position);
                literals.push(None);
            }
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => {
                let name = match item {
                    SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                    _ => "literal".to_owned(),
                };
                columns.push(SqlColumn { name });
                positions.push(None);
                literals.push(Some(literal_value(expression, params)?));
                computed.push(None);
            }
            other => {
                return Err(unsupported(
                    "JOIN",
                    &format!("unsupported projection item {other}"),
                ));
            }
        }
    }
    while computed.len() < columns.len() {
        computed.push(None);
    }
    Ok(ProjectionPlan {
        columns,
        positions,
        literals,
        computed,
    })
}

fn sort_joined_rows(
    rows: &mut [Vec<Value>],
    order_by: &OrderBy,
    combined_columns: &[SqlColumn],
    _params: &[Value],
) -> Result<()> {
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(unsupported("JOIN", "unsupported ORDER BY shape over joins"));
    };
    let mut terms = Vec::new();
    for kind in expressions {
        // Bare names match uniquely across the combined schema; qualified
        // names (users.email) must name an existing relation prefix.
        let (table_hint, name) = match &kind.expr {
            Expr::Identifier(identifier) => (None, identifier.value.clone()),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
                Some(parts[parts.len() - 2].value.clone()),
                parts[parts.len() - 1].value.clone(),
            ),
            _ => {
                return Err(unsupported(
                    "JOIN",
                    "ORDER BY must reference a column by name",
                ));
            }
        };
        let position = match table_hint {
            Some(hint) => combined_columns
                .iter()
                .position(|column| column.name == format!("{hint}.{name}"))
                .ok_or_else(|| {
                    DbError::InvalidState(format!("unknown column {hint}.{name} in join ORDER BY"))
                })?,
            None => {
                let matches: Vec<usize> = combined_columns
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| {
                        column.name == name || column.name.ends_with(&format!(".{name}"))
                    })
                    .map(|(position, _)| position)
                    .collect();
                match matches.len() {
                    1 => matches[0],
                    0 => {
                        return Err(DbError::InvalidState(format!(
                            "unknown column {name} in join ORDER BY"
                        )));
                    }
                    _ => {
                        return Err(unsupported(
                            "JOIN",
                            "ambiguous unqualified ORDER BY column; qualify it with the table name",
                        ));
                    }
                }
            }
        };
        terms.push((position, kind.options.asc.unwrap_or(true)));
    }
    rows.sort_by(|left, right| {
        for (position, asc) in &terms {
            let ordering = joined_value_ordering(&left[*position], &right[*position]);
            let ordering = if *asc { ordering } else { ordering.reverse() };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
    Ok(())
}

fn joined_value_ordering(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::U64(l), Value::U64(r)) => l.cmp(r),
        (Value::I64(l), Value::I64(r)) => l.cmp(r),
        (Value::Text(l), Value::Text(r)) => l.cmp(r),
        (Value::U64(l), Value::I64(r)) if *r >= 0 => l.cmp(&(*r as u64)),
        (Value::I64(l), Value::U64(r)) if *l >= 0 => (*l as u64).cmp(r),
        _ => Ordering::Equal,
    }
}

fn relation_name(relation: &sqlparser::ast::TableFactor) -> Result<String> {
    match relation {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => Ok(match alias {
            Some(alias) => alias.name.value.clone(),
            None => simple_object_name(name, "table")?.to_owned(),
        }),
        _ => Err(unsupported("JOIN", "only plain tables are supported")),
    }
}

#[derive(Clone, Copy)]
enum JoinSide {
    Accumulated,
    Incoming,
}

/// Resolve one ON operand to a global combined-column position. The
/// operand must land either in the accumulated scope (qualified by any
/// earlier relation's name, or uniquely by bare name) or in the incoming
/// table.
fn resolve_on_operand(
    expression: &Expr,
    scope: &[(String, &TableDefinition)],
    incoming: &TableDefinition,
) -> Result<(JoinSide, usize)> {
    let (table_hint, column_name) = match expression {
        Expr::Identifier(identifier) => (None, identifier.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
            Some(parts[parts.len() - 2].value.clone()),
            parts[parts.len() - 1].value.clone(),
        ),
        _ => return Err(unsupported("JOIN", "ON must compare two columns")),
    };
    let locate = |table: &TableDefinition| {
        table
            .columns
            .iter()
            .position(|column| column.name == column_name)
    };
    match table_hint.as_deref() {
        Some(hint) => {
            if let Some((_, table)) = scope.iter().find(|(name, _)| name == hint) {
                locate(table)
                    .ok_or_else(|| unsupported("JOIN", "ON column not found in referenced table"))
                    .map(|position| (JoinSide::Accumulated, position))
            } else {
                locate(incoming)
                    .map(|position| (JoinSide::Incoming, position))
                    .ok_or_else(|| unsupported("JOIN", "ON references a table outside the join"))
            }
        }
        None => {
            if let Some(position) = locate(incoming) {
                return Ok((JoinSide::Incoming, position));
            }
            let matches: Vec<usize> = scope
                .iter()
                .filter_map(|(_, table)| locate(table))
                .collect();
            match matches.len() {
                1 => Ok((JoinSide::Accumulated, matches[0])),
                0 => Err(unsupported(
                    "JOIN",
                    "ON column does not resolve to any joined table",
                )),
                _ => Err(unsupported(
                    "JOIN",
                    "ambiguous unqualified ON column; qualify it with the table name",
                )),
            }
        }
    }
}

/// Resolve `a.x = b.y` (either orientation) to positions: one operand in
/// the accumulated scope, one in the incoming table. `offset` is the
/// incoming table's first column index in the final combined schema.
type OnTerms = Vec<(usize, usize, BinaryOperator)>;

fn on_positions_in_scope(
    expression: &Expr,
    scope: &[(String, &TableDefinition)],
    incoming: &TableDefinition,
) -> Result<OnTerms> {
    let mut terms = Vec::new();
    collect_on_comparisons(expression, scope, incoming, &mut terms)?;
    Ok(terms)
}

/// Flatten an AND-tree of column comparisons into per-pair terms.
fn collect_on_comparisons(
    expression: &Expr,
    scope: &[(String, &TableDefinition)],
    incoming: &TableDefinition,
    terms: &mut Vec<(usize, usize, BinaryOperator)>,
) -> Result<()> {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expression
    {
        collect_on_comparisons(left, scope, incoming, terms)?;
        collect_on_comparisons(right, scope, incoming, terms)?;
        return Ok(());
    }
    let Expr::BinaryOp {
        left: lhs,
        op,
        right: rhs,
    } = expression
    else {
        return Err(unsupported("JOIN", "ON must be column comparisons"));
    };
    if !matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    ) {
        return Err(unsupported("JOIN", "unsupported ON operator"));
    }
    let (first_side, first_position) = resolve_on_operand(lhs, scope, incoming)?;
    let (second_side, second_position) = resolve_on_operand(rhs, scope, incoming)?;
    match (first_side, second_side) {
        (JoinSide::Accumulated, JoinSide::Incoming) => {
            terms.push((first_position, second_position, op.clone()));
        }
        (JoinSide::Incoming, JoinSide::Accumulated) => {
            terms.push((second_position, first_position, op.clone()));
        }
        _ => {
            return Err(unsupported(
                "JOIN",
                "ON must compare one column from the joined tables so far and one from the new table",
            ));
        }
    }
    Ok(())
}

/// Unique unqualified column lookup over the combined join schema.
fn unique_bare_position(combined_columns: &[SqlColumn], name: &str) -> Result<usize> {
    let matches: Vec<usize> = combined_columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name == name || column.name.ends_with(&format!(".{name}")))
        .map(|(position, _)| position)
        .collect();
    match matches.len() {
        1 => Ok(matches[0]),
        0 => Err(DbError::InvalidState(format!(
            "unknown column {name} in join USING"
        ))),
        _ => Err(unsupported(
            "JOIN",
            "ambiguous USING column; qualify it or rename one side",
        )),
    }
}

fn ordering_matches(ordering: Ordering, op: &BinaryOperator) -> bool {
    match op {
        BinaryOperator::Eq => ordering == Ordering::Equal,
        BinaryOperator::NotEq => ordering != Ordering::Equal,
        BinaryOperator::Lt => ordering == Ordering::Less,
        BinaryOperator::LtEq => ordering != Ordering::Greater,
        BinaryOperator::Gt => ordering == Ordering::Greater,
        BinaryOperator::GtEq => ordering != Ordering::Less,
        _ => false,
    }
}

fn column_reference_name(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Identifier(identifier) => Some(identifier.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.clone()),
        _ => None,
    }
}

/// Cross-type equivalence for join keys, following coerce_value's
/// non-negative integer rule.
/// Per-pair ON evaluation with the non-negative integer coercion rule.
/// Incomparable types (e.g. Text vs U64) match only for Eq/NotEq via
/// direct equality, mirroring compare_joined's fallback.
fn join_pair_matches(left: &Value, right: &Value, op: &BinaryOperator) -> bool {
    match (left, right) {
        (Value::U64(_), Value::U64(_))
        | (Value::I64(_), Value::I64(_))
        | (Value::Text(_), Value::Text(_)) => {
            ordering_matches(joined_value_ordering(left, right), op)
        }
        (Value::U64(a), Value::I64(b)) | (Value::I64(b), Value::U64(a)) if *b >= 0 => {
            ordering_matches((*a as i64).cmp(b), op)
        }
        (l, r) => {
            let equal = l == r;
            match op {
                BinaryOperator::Eq => equal,
                BinaryOperator::NotEq => !equal,
                _ => false,
            }
        }
    }
}

/// Conjunctive WHERE over the combined joined schema: comparisons between
/// a combined column and a literal/param, AND/OR composition.
pub(super) fn join_predicate(
    expression: &Expr,
    combined: &[Value],
    columns: &[SqlColumn],
    params: &[Value],
) -> Result<bool> {
    match expression {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(join_predicate(left, combined, columns, params)?
                && join_predicate(right, combined, columns, params)?),
            BinaryOperator::Or => Ok(join_predicate(left, combined, columns, params)?
                || join_predicate(right, combined, columns, params)?),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                // Either or both sides may be columns (cross-table
                // predicates like users.id = tiers.user_id).
                let resolve = |expression: &Expr| -> Result<Value> {
                    if matches!(
                        expression,
                        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                    ) {
                        combined_value(expression, combined, columns)
                    } else {
                        literal_value(expression, params)
                    }
                };
                Ok(compare_joined(resolve(left)?, resolve(right)?, op))
            }
            _ => Err(unsupported("JOIN", "unsupported WHERE operator over joins")),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let candidate = combined_value(expr, combined, columns)?;
            let values = list
                .iter()
                .map(|item| literal_value(item, params))
                .collect::<Result<Vec<_>>>()?;
            let hit = membership_truth(&candidate, values);
            Ok(if *negated {
                hit.not() == Truth::True
            } else {
                hit == Truth::True
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let candidate = combined_value(expr, combined, columns)?;
            let low_value = literal_value(low, params)?;
            let high_value = literal_value(high, params)?;
            let mut both = compare_joined(candidate.clone(), low_value, &BinaryOperator::GtEq)
                && compare_joined(candidate, high_value, &BinaryOperator::LtEq);
            if *negated {
                both = !both;
            }
            Ok(both)
        }
        _ => Err(unsupported("JOIN", "unsupported WHERE shape over joins")),
    }
}

fn combined_value(expression: &Expr, combined: &[Value], columns: &[SqlColumn]) -> Result<Value> {
    let name = column_reference_name(expression)
        .ok_or_else(|| unsupported("JOIN", "WHERE operands must be columns or literals"))?;
    let position = columns
        .iter()
        .position(|column| column.name == name || column.name.ends_with(&format!(".{name}")))
        .ok_or_else(|| DbError::InvalidState(format!("unknown column {name} in join WHERE")))?;
    Ok(combined[position].clone())
}

fn compare_joined(left: Value, right: Value, op: &BinaryOperator) -> bool {
    compare_values(&left, &right, op) == Truth::True
}

fn membership_truth<I>(candidate: &Value, values: I) -> Truth
where
    I: IntoIterator<Item = Value>,
{
    let mut unknown = false;
    for value in values {
        match compare_values(candidate, &value, &BinaryOperator::Eq) {
            Truth::True => return Truth::True,
            Truth::Unknown => unknown = true,
            Truth::False => {}
        }
    }
    if unknown {
        Truth::Unknown
    } else {
        Truth::False
    }
}

pub(super) fn row_matches(
    selection: Option<&Expr>,
    row: &Row,
    table: &TableDefinition,
    params: &[Value],
    subqueries: &ResolvedSubqueries,
) -> Result<bool> {
    selection
        .map(|expression| {
            predicate(expression, row, table, params, subqueries).map(|truth| truth == Truth::True)
        })
        .unwrap_or(Ok(true))
}

fn predicate(
    expression: &Expr,
    row: &Row,
    table: &TableDefinition,
    params: &[Value],
    subqueries: &ResolvedSubqueries,
) -> Result<Truth> {
    match expression {
        Expr::Nested(expression) => predicate(expression, row, table, params, subqueries),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(predicate(expr, row, table, params, subqueries)?.not()),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(predicate(left, row, table, params, subqueries)?
                .and(predicate(right, row, table, params, subqueries)?)),
            BinaryOperator::Or => Ok(predicate(left, row, table, params, subqueries)?
                .or(predicate(right, row, table, params, subqueries)?)),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => compare_predicate(op, left, right, row, table, params),
            _ => Err(unsupported(
                "WHERE",
                "this predicate operator is not supported",
            )),
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let key = subquery.to_string();
            let candidates = subqueries.in_lists.get(&key).ok_or_else(|| {
                DbError::InvalidState("subquery was not resolved before evaluation".to_owned())
            })?;
            let candidate = eval_expression(expr, row, table, params)?;
            let hit = membership_truth(&candidate, candidates.iter().cloned());
            Ok(if *negated { hit.not() } else { hit })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let candidate = eval_expression(expr, row, table, params)?;
            let values = list
                .iter()
                .map(|item| literal_value(item, params))
                .collect::<Result<Vec<_>>>()?;
            let hit = membership_truth(&candidate, values);
            Ok(if *negated { hit.not() } else { hit })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let at_least_low =
                compare_predicate(&BinaryOperator::GtEq, expr, low, row, table, params)?;
            let at_most_high =
                compare_predicate(&BinaryOperator::LtEq, expr, high, row, table, params)?;
            let both = at_least_low.and(at_most_high);
            Ok(if *negated { both.not() } else { both })
        }
        Expr::IsNull(expression) => Ok(
            if matches!(
                eval_expression(expression, row, table, params)?,
                Value::Null
            ) {
                Truth::True
            } else {
                Truth::False
            },
        ),
        Expr::Exists { subquery, negated } => {
            let key = subquery.to_string();
            let present = subqueries.exists.get(&key).ok_or_else(|| {
                DbError::InvalidState("subquery was not resolved before evaluation".to_owned())
            })?;
            Ok(if *negated != *present {
                Truth::True
            } else {
                Truth::False
            })
        }
        Expr::IsNotNull(expression) => Ok(
            if matches!(
                eval_expression(expression, row, table, params)?,
                Value::Null
            ) {
                Truth::False
            } else {
                Truth::True
            },
        ),
        _ => match eval_expression(expression, row, table, params)? {
            Value::Bool(true) => Ok(Truth::True),
            Value::Bool(false) => Ok(Truth::False),
            Value::Null => Ok(Truth::Unknown),
            _ => Err(DbError::InvalidState(
                "SQL WHERE requires a boolean expression".to_owned(),
            )),
        },
    }
}

fn compare_predicate(
    operator: &BinaryOperator,
    left: &Expr,
    right: &Expr,
    row: &Row,
    table: &TableDefinition,
    params: &[Value],
) -> Result<Truth> {
    let left = eval_expression(left, row, table, params)?;
    let right = eval_expression(right, row, table, params)?;
    Ok(compare_values(&left, &right, operator))
}

fn compare_values(left: &Value, right: &Value, operator: &BinaryOperator) -> Truth {
    let Some(ordering) = value_cmp(left, right) else {
        return Truth::Unknown;
    };
    let matched = match operator {
        BinaryOperator::Eq => ordering.is_eq(),
        BinaryOperator::NotEq => !ordering.is_eq(),
        BinaryOperator::Gt => ordering.is_gt(),
        BinaryOperator::Lt => ordering.is_lt(),
        BinaryOperator::GtEq => !ordering.is_lt(),
        BinaryOperator::LtEq => !ordering.is_gt(),
        _ => return Truth::Unknown,
    };
    if matched { Truth::True } else { Truth::False }
}

fn eval_expression(
    expression: &Expr,
    row: &Row,
    table: &TableDefinition,
    params: &[Value],
) -> Result<Value> {
    match expression {
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ) =>
        {
            let lhs = eval_expression(left, row, table, params)?;
            let rhs = eval_expression(right, row, table, params)?;
            arithmetic(op, &lhs, &rhs)
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let position = column_position(table, expression)?
                .ok_or_else(|| DbError::InvalidState(format!("unknown SQL column {expression}")))?;
            row.values
                .get(position)
                .cloned()
                .ok_or_else(|| DbError::InvalidState("row is missing a SQL column".to_owned()))
        }
        _ => literal_value(expression, params),
    }
}

fn value_cmp(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::I64(left), Value::I64(right)) => Some(left.cmp(right)),
        (Value::U64(left), Value::U64(right)) => Some(left.cmp(right)),
        (Value::I64(left), Value::U64(right)) => {
            if *left < 0 {
                Some(Ordering::Less)
            } else {
                Some((*left as u64).cmp(right))
            }
        }
        (Value::U64(left), Value::I64(right)) => {
            if *right < 0 {
                Some(Ordering::Greater)
            } else {
                Some(left.cmp(&(*right as u64)))
            }
        }
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        (Value::Bytes(left), Value::Bytes(right)) => Some(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Truth {
    True,
    False,
    Unknown,
}

impl Truth {
    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            _ => Self::True,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            _ => Self::False,
        }
    }
}

fn is_aggregate_query(select: &Select) -> bool {
    match &select.group_by {
        GroupByExpr::Expressions(expressions, modifiers)
            if !expressions.is_empty() || !modifiers.is_empty() =>
        {
            return true;
        }
        GroupByExpr::All(_) => return true,
        _ => {}
    }
    select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(Expr::Function(_))
                | SelectItem::ExprWithAlias {
                    expr: Expr::Function(_),
                    ..
                }
        )
    })
}

fn execute_aggregate_query(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    query: &Query,
    select: &Select,
    table: &TableDefinition,
    params: &[Value],
) -> Result<SqlResult> {
    let mut group_by_col_ids = Vec::new();
    if let GroupByExpr::Expressions(expressions, _) = &select.group_by {
        for expr in expressions {
            let pos = column_position(table, expr)?.ok_or_else(|| {
                unsupported("GROUP BY", "only table columns are supported in GROUP BY")
            })?;
            group_by_col_ids.push(table.columns[pos].id);
        }
    }

    let mut result_columns = Vec::new();
    let mut analytical_query = crate::morsel::AnalyticalQuery::new(table.id);
    for col_id in &group_by_col_ids {
        analytical_query = analytical_query.with_group_by(*col_id);
    }

    // Constants are group-invariant and project alongside aggregates
    // without needing GROUP BY.
    let mut constant_values: Vec<Option<Value>> = vec![None; select.projection.len()];
    for (item_index, item) in select.projection.iter().enumerate() {
        let (expr, alias) = projection_expression(item)?;
        if let Some(pos) = column_position(table, expr)? {
            let col = &table.columns[pos];
            if !group_by_col_ids.contains(&col.id) {
                return Err(DbError::InvalidState(format!(
                    "column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
                    col.name
                )));
            }
            result_columns.push(SqlColumn {
                name: alias.unwrap_or_else(|| col.name.clone()),
            });
        } else if let Expr::Function(func) = expr {
            let func_name = func.name.to_string().to_uppercase();
            let kind = match func_name.as_str() {
                "COUNT" => crate::morsel::AggregateKind::Count,
                "SUM" => crate::morsel::AggregateKind::Sum,
                "AVG" => crate::morsel::AggregateKind::Avg,
                "MIN" => crate::morsel::AggregateKind::Min,
                "MAX" => crate::morsel::AggregateKind::Max,
                _ => return Err(unsupported("SELECT", "unsupported aggregate function")),
            };
            let func_args = match &func.args {
                sqlparser::ast::FunctionArguments::List(list) => list.args.as_slice(),
                sqlparser::ast::FunctionArguments::None => &[],
                _ => return Err(unsupported("SELECT", "unsupported aggregate argument type")),
            };
            let col_id = match func_args {
                [
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard),
                ] => {
                    if kind != crate::morsel::AggregateKind::Count {
                        return Err(unsupported(
                            "SELECT",
                            "wildcard argument is only valid for COUNT(*)",
                        ));
                    }
                    None
                }
                [
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                        inner,
                    )),
                ] => {
                    let pos = column_position(table, inner)?.ok_or_else(|| {
                        unsupported("SELECT", "aggregate argument must be a table column")
                    })?;
                    Some(table.columns[pos].id)
                }
                _ => {
                    return Err(unsupported(
                        "SELECT",
                        "aggregate function requires exactly one column or wildcard",
                    ));
                }
            };
            // PostgreSQL names an unaliased aggregate output column after
            // the function alone ("count"), not the call text.
            let default_name = func_name.to_lowercase();
            let final_alias = alias.unwrap_or(default_name);
            result_columns.push(SqlColumn {
                name: final_alias.clone(),
            });
            analytical_query = analytical_query.with_aggregate(kind, col_id, final_alias);
        } else {
            let value = literal_value(expr, params)?;
            constant_values[item_index] = Some(value);
            result_columns.push(SqlColumn {
                name: alias.unwrap_or_else(|| expr.to_string()),
            });
        }
    }

    let (offset, limit) = query_window(query, params)?;
    if limit == 0 {
        return Ok(SqlResult::rows(result_columns, Vec::new()));
    }

    let subqueries = resolve_subqueries(database, transaction, select.selection.as_ref(), params)?;
    let rows = transaction.scan(database, table.id, usize::MAX)?;
    let filtered_rows: Vec<Row> = if let Some(selection) = &select.selection {
        let mut matching = Vec::new();
        for row in rows {
            if predicate(selection, &row, table, params, &subqueries)? == Truth::True {
                matching.push(row);
            }
        }
        matching
    } else {
        rows
    };

    let mut groups: std::collections::BTreeMap<
        Vec<Value>,
        Vec<crate::morsel::AggregateAccumulator>,
    > = std::collections::BTreeMap::new();
    let mut global_accs: Vec<crate::morsel::AggregateAccumulator> = analytical_query
        .aggregates
        .iter()
        .map(|spec| crate::morsel::AggregateAccumulator::new(spec.kind))
        .collect();

    for row in &filtered_rows {
        if group_by_col_ids.is_empty() {
            for (acc, spec) in global_accs.iter_mut().zip(&analytical_query.aggregates) {
                let val = if let Some(col_id) = spec.column {
                    Some(row.value(table, col_id)?)
                } else {
                    None
                };
                acc.update(val)?;
            }
        } else {
            let mut group_key = Vec::with_capacity(group_by_col_ids.len());
            for col_id in &group_by_col_ids {
                group_key.push(row.value(table, *col_id)?.clone());
            }
            let accs = groups.entry(group_key).or_insert_with(|| {
                analytical_query
                    .aggregates
                    .iter()
                    .map(|spec| crate::morsel::AggregateAccumulator::new(spec.kind))
                    .collect()
            });
            for (acc, spec) in accs.iter_mut().zip(&analytical_query.aggregates) {
                let val = if let Some(col_id) = spec.column {
                    Some(row.value(table, col_id)?)
                } else {
                    None
                };
                acc.update(val)?;
            }
        }
    }

    let mut result_rows = Vec::new();
    if group_by_col_ids.is_empty() {
        let finalized: Vec<Value> = global_accs
            .into_iter()
            .map(crate::morsel::AggregateAccumulator::finalize)
            .collect();
        let mut acc_iter = finalized.into_iter();
        let mut row_values = Vec::with_capacity(select.projection.len());
        for (item_index, item) in select.projection.iter().enumerate() {
            if constant_values[item_index].is_some() {
                row_values.push(constant_values[item_index].clone().expect("checked"));
            } else if !matches!(projection_expression(item)?.0, Expr::Function(_))
                && column_position(table, projection_expression(item)?.0)?.is_none()
            {
                unreachable!("guard loop rejected this shape");
            } else {
                row_values.push(acc_iter.next().expect("accumulator per aggregate item"));
            }
        }
        result_rows.push(row_values);
    } else {
        for (group_key, accs) in groups {
            let mut row_values = Vec::new();
            let mut group_iter = group_key.into_iter();
            let mut acc_iter = accs.into_iter();
            for (item_index, item) in select.projection.iter().enumerate() {
                if constant_values[item_index].is_some() {
                    row_values.push(constant_values[item_index].clone().expect("checked"));
                    continue;
                }
                let (expr, _) = projection_expression(item)?;
                if column_position(table, expr)?.is_some() {
                    if let Some(val) = group_iter.next() {
                        row_values.push(val);
                    }
                } else if let Some(acc) = acc_iter.next() {
                    row_values.push(acc.finalize());
                }
            }
            result_rows.push(row_values);
        }
    }

    // ORDER BY over grouped output resolves against the output column
    // names (aliases included), mirroring the join grouped path.
    if let Some(order_by) = &query.order_by {
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Err(unsupported(
                "GROUP BY",
                "unsupported ORDER BY shape over aggregates",
            ));
        };
        let mut terms = Vec::new();
        for kind in expressions {
            let Expr::Identifier(identifier) = &kind.expr else {
                return Err(unsupported(
                    "GROUP BY",
                    "ORDER BY must reference an output column name",
                ));
            };
            let name = &identifier.value;
            let position = result_columns
                .iter()
                .position(|column| &column.name == name)
                .ok_or_else(|| {
                    DbError::InvalidState(format!("unknown column {name} in aggregate ORDER BY"))
                })?;
            terms.push((position, kind.options.asc.unwrap_or(true)));
        }
        result_rows.sort_by(|left, right| {
            for (position, asc) in &terms {
                let ordering = joined_value_ordering(&left[*position], &right[*position]);
                let ordering = if *asc { ordering } else { ordering.reverse() };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });
    }

    let final_rows = result_rows.into_iter().skip(offset).take(limit).collect();
    Ok(SqlResult::rows(result_columns, final_rows))
}
