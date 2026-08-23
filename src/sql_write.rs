use sqlparser::ast::{
    AssignmentTarget, Delete, Expr, FromTable, Insert, ObjectName, SelectItem, SetExpr,
    TableObject, Update,
};

use crate::{
    DbError, RelationalDatabase, RelationalDatabaseTransaction, Result, Row, SqlColumn, SqlResult,
    TableDefinition, Value,
};

use super::query::{ResolvedSubqueries, join_predicate, row_matches};
use super::{
    column_position_by_name, find_table, simple_object_name, unsupported,
    values::{coerce_value, literal_value, sql_primary_key},
};

pub(super) fn execute_insert(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    insert: &Insert,
    params: &[Value],
) -> Result<SqlResult> {
    if !insert.optimizer_hints.is_empty()
        || insert.or.is_some()
        || insert.ignore
        || insert.table_alias.is_some()
        || insert.overwrite
        || !insert.assignments.is_empty()
        || insert.partitioned.is_some()
        || !insert.after_columns.is_empty()
        || insert.has_table_keyword
        || insert.on.is_some()
        || insert.output.is_some()
        || insert.replace_into
        || insert.priority.is_some()
        || insert.insert_alias.is_some()
        || insert.settings.is_some()
        || insert.format_clause.is_some()
        || insert.multi_table_insert_type.is_some()
    {
        return Err(unsupported(
            "INSERT",
            "only INSERT INTO table VALUES (...) is supported",
        ));
    }
    let table_name = match &insert.table {
        TableObject::TableName(name) => simple_object_name(name, "table")?,
        _ => return Err(unsupported("INSERT", "the target must be a plain table")),
    };
    let table = find_table(database.catalog(), table_name)?;
    let source = insert
        .source
        .as_deref()
        .ok_or_else(|| unsupported("INSERT", "VALUES is required"))?;
    if source.with.is_some() || source.fetch.is_some() || !source.locks.is_empty() {
        return Err(unsupported("INSERT", "the source must be plain"));
    }
    let target_positions = insert_positions(table, &insert.columns)?;
    let returning_plan = insert
        .returning
        .as_ref()
        .map(|items| returning_plan(table, items))
        .transpose()?;

    // Two source shapes: literal VALUES rows and a full SELECT query.
    // SELECT sources run through the normal query executor, so they get
    // its whole surface (joins, aggregates, ORDER BY/LIMIT) for free.
    let mut inserted_rows: Vec<Vec<Value>> = Vec::new();
    let mut returned_rows: Vec<Vec<Value>> = Vec::new();
    match source.body.as_ref() {
        SetExpr::Values(values) => {
            if source.order_by.is_some()
                || source.limit_clause.is_some()
                || source.for_clause.is_some()
            {
                return Err(unsupported("INSERT", "the VALUES source must be plain"));
            }
            for values in &values.rows {
                if values.len() != target_positions.len() {
                    return Err(DbError::InvalidState(format!(
                        "INSERT row has {} values but {} columns were targeted",
                        values.len(),
                        target_positions.len()
                    )));
                }
                let mut row_values = vec![Value::Null; table.columns.len()];
                for (expression, position) in values.iter().zip(&target_positions) {
                    let value = literal_value(expression, params)?;
                    row_values[*position] = coerce_value(value, &table.columns[*position])?;
                }
                inserted_rows.push(row_values);
            }
        }
        SetExpr::Select(_) => {
            let result = super::query::execute_query(database, transaction, source, params)?;
            if result.columns.len() != target_positions.len() {
                return Err(DbError::InvalidState(format!(
                    "INSERT SELECT yields {} columns but {} were targeted",
                    result.columns.len(),
                    target_positions.len()
                )));
            }
            for row in result.rows {
                let mut row_values = vec![Value::Null; table.columns.len()];
                for (value, position) in row.into_iter().zip(&target_positions) {
                    row_values[*position] = coerce_value(value, &table.columns[*position])?;
                }
                inserted_rows.push(row_values);
            }
        }
        _ => {
            return Err(unsupported(
                "INSERT",
                "the source must be VALUES rows or a SELECT",
            ));
        }
    }

    let mut affected = 0;
    for row_values in inserted_rows {
        let primary = sql_primary_key(table, &row_values)?;
        if let Some((_, positions)) = &returning_plan {
            returned_rows.push(
                positions
                    .iter()
                    .map(|position| row_values[*position].clone())
                    .collect(),
            );
        }
        transaction.insert(
            database,
            table.id,
            Row {
                primary,
                values: row_values,
            },
        )?;
        affected += 1;
    }
    if let Some((columns, _)) = returning_plan {
        return Ok(SqlResult::rows(columns, returned_rows));
    }
    Ok(SqlResult::command(affected))
}

/// RETURNING column list over the inserted table: wildcard spans every
/// column; bare names resolve directly. Expressions stay refused.
fn returning_plan(
    table: &TableDefinition,
    items: &[SelectItem],
) -> Result<(Vec<SqlColumn>, Vec<usize>)> {
    let mut columns = Vec::new();
    let mut positions = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(options) if options.to_string().is_empty() => {
                for (position, column) in table.columns.iter().enumerate() {
                    columns.push(SqlColumn {
                        name: column.name.clone(),
                    });
                    positions.push(position);
                }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                let position = table
                    .columns
                    .iter()
                    .position(|column| column.name == identifier.value)
                    .ok_or_else(|| {
                        DbError::InvalidState(format!("column {} does not exist", identifier.value))
                    })?;
                columns.push(SqlColumn {
                    name: identifier.value.clone(),
                });
                positions.push(position);
            }
            _ => {
                return Err(unsupported(
                    "RETURNING",
                    "only column names or * are supported",
                ));
            }
        }
    }
    Ok((columns, positions))
}

pub(super) fn execute_update(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    update: &Update,
    params: &[Value],
) -> Result<SqlResult> {
    if !update.table.joins.is_empty()
        || update.assignments.is_empty()
        || update.output.is_some()
        || update.or.is_some()
        || !update.order_by.is_empty()
        || update.limit.is_some()
    {
        return Err(unsupported(
            "UPDATE",
            "only single-table literal assignments are supported",
        ));
    }
    let table = super::table_from_join(database.catalog(), &update.table)?;
    let assignments = update_assignments(table, &update.assignments)?;
    let returning_plan = update
        .returning
        .as_ref()
        .map(|items| returning_plan(table, items))
        .transpose()?;

    // UPDATE ... FROM: the WHERE may reference one source table; each
    // target row updates at most once (first matching source row wins,
    // matching PostgreSQL's non-determinism contract).
    let from_scope = match &update.from {
        Some(kind) => {
            let tables = match kind {
                sqlparser::ast::UpdateTableFromKind::AfterSet(tables)
                | sqlparser::ast::UpdateTableFromKind::BeforeSet(tables) => tables,
            };
            Some(cross_table_scope(database.catalog(), tables, table)?)
        }
        None => None,
    };
    if from_scope.is_some() && update.selection.is_none() {
        return Err(unsupported(
            "UPDATE",
            "UPDATE FROM requires a WHERE clause referencing the source table",
        ));
    }

    let rows = if from_scope.is_none() {
        match super::query::primary_key_rows(
            database,
            transaction,
            table,
            update.selection.as_ref(),
            params,
        )? {
            Some(rows) => rows,
            None => transaction.scan(database, table.id, usize::MAX)?,
        }
    } else {
        transaction.scan(database, table.id, usize::MAX)?
    };
    let mut affected = 0;
    let mut returned_rows: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        if let Some((from_table, combined_columns)) = &from_scope {
            // FROM-mode rows never fall through to the single-table
            // walker: an unmatched target row is simply not updated.
            let from_rows = transaction.scan(database, from_table.id, usize::MAX)?;
            for source_row in &from_rows {
                let mut combined = Vec::with_capacity(row.values.len() + source_row.values.len());
                combined.extend_from_slice(&row.values);
                combined.extend_from_slice(&source_row.values);
                if join_predicate(
                    update.selection.as_ref().expect("checked above"),
                    &combined,
                    combined_columns,
                    params,
                )? {
                    let updated = apply_assignments(
                        database,
                        transaction,
                        table,
                        &assignments,
                        &row,
                        params,
                    )?;
                    if let Some((_, positions)) = &returning_plan {
                        returned_rows.push(
                            positions
                                .iter()
                                .map(|p| updated.values[*p].clone())
                                .collect(),
                        );
                    }
                    affected += 1;
                    break;
                }
            }
            continue;
        }
        if !row_matches(
            update.selection.as_ref(),
            &row,
            table,
            params,
            &ResolvedSubqueries::new(),
        )? {
            continue;
        }
        let mut updated = row.clone();
        for (position, expression) in &assignments {
            let value = literal_value(expression, params)?;
            updated.values[*position] = coerce_value(value, &table.columns[*position])?;
        }
        let primary_changed = if let Some(columns) = database.catalog().primary_key(table.id) {
            columns.iter().try_fold(false, |changed, column| {
                let position = table
                    .columns
                    .iter()
                    .position(|candidate| candidate.id == *column)
                    .ok_or_else(|| {
                        DbError::InvalidState("primary-key column is missing".to_owned())
                    })?;
                Ok::<_, DbError>(changed || updated.values[position] != row.values[position])
            })?
        } else {
            updated.values[0] != row.values[0]
        };
        if primary_changed {
            return Err(DbError::InvalidState(
                "updating the SQL primary key is not supported; delete and insert the row instead"
                    .to_owned(),
            ));
        }
        if let Some((_, positions)) = &returning_plan {
            returned_rows.push(
                positions
                    .iter()
                    .map(|p| updated.values[*p].clone())
                    .collect(),
            );
        }
        transaction.update(database, table.id, updated)?;
        affected += 1;
    }
    match returning_plan {
        Some((columns, _)) => Ok(SqlResult::rows(columns, returned_rows)),
        None => Ok(SqlResult::command(affected)),
    }
}

/// Apply assignments to one target row in place (shared by the plain and
/// FROM-join update paths), enforcing primary-key invariance.
fn apply_assignments(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    table: &TableDefinition,
    assignments: &[(usize, &sqlparser::ast::Expr)],
    row: &Row,
    params: &[Value],
) -> Result<Row> {
    let mut updated = row.clone();
    for (position, expression) in assignments {
        let value = literal_value(expression, params)?;
        updated.values[*position] = coerce_value(value, &table.columns[*position])?;
    }
    let primary_changed = if let Some(columns) = database.catalog().primary_key(table.id) {
        columns.iter().try_fold(false, |changed, column| {
            let position = table
                .columns
                .iter()
                .position(|candidate| candidate.id == *column)
                .ok_or_else(|| DbError::InvalidState("primary-key column is missing".to_owned()))?;
            Ok::<_, DbError>(changed || updated.values[position] != row.values[position])
        })?
    } else {
        updated.values[0] != row.values[0]
    };
    if primary_changed {
        return Err(DbError::InvalidState(
            "updating the SQL primary key is not supported; delete and insert the row instead"
                .to_owned(),
        ));
    }
    transaction.update(database, table.id, updated.clone())?;
    Ok(updated)
}

/// Resolve a single-table FROM/USING clause into (table, combined schema)
/// where the target's columns come first (unqualified) and the source
/// table's follow, qualified by its name.
fn cross_table_scope<'a>(
    catalog: &'a crate::Catalog,
    tables: &'a [sqlparser::ast::TableWithJoins],
    target: &TableDefinition,
) -> Result<(&'a TableDefinition, Vec<SqlColumn>)> {
    if tables.len() != 1 {
        return Err(unsupported(
            "UPDATE",
            "only one source table is supported in FROM",
        ));
    }
    let from_table = super::table_from_join(catalog, &tables[0])?;
    let mut combined_columns: Vec<SqlColumn> = target
        .columns
        .iter()
        .map(|column| SqlColumn {
            name: column.name.clone(),
        })
        .collect();
    for column in &from_table.columns {
        combined_columns.push(SqlColumn {
            name: format!("{}.{}", from_table.name, column.name),
        });
    }
    Ok((from_table, combined_columns))
}

pub(super) fn execute_delete(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    delete: &Delete,
    params: &[Value],
) -> Result<SqlResult> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1
        || !delete.tables.is_empty()
        || delete.output.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return Err(unsupported("DELETE", "only one plain table is supported"));
    }
    let table = super::table_from_join(database.catalog(), &tables[0])?;
    let returning_plan = delete
        .returning
        .as_ref()
        .map(|items| returning_plan(table, items))
        .transpose()?;
    // DELETE ... USING source: the WHERE may reference one source table;
    // a target row deletes if any combined predicate match exists.
    let using_scope = match &delete.using {
        Some(tables) => Some(cross_table_scope(database.catalog(), tables, table)?),
        None => None,
    };
    if using_scope.is_some() && delete.selection.is_none() {
        return Err(unsupported(
            "DELETE",
            "DELETE USING requires a WHERE clause referencing the source table",
        ));
    }
    let rows = if using_scope.is_none() {
        match super::query::primary_key_rows(
            database,
            transaction,
            table,
            delete.selection.as_ref(),
            params,
        )? {
            Some(rows) => rows,
            None => transaction.scan(database, table.id, usize::MAX)?,
        }
    } else {
        transaction.scan(database, table.id, usize::MAX)?
    };
    let mut affected = 0;
    let mut returned_rows: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        if let Some((using_table, combined_columns)) = &using_scope {
            let using_rows = transaction.scan(database, using_table.id, usize::MAX)?;
            let mut matched = false;
            for source_row in &using_rows {
                let mut combined = Vec::with_capacity(row.values.len() + source_row.values.len());
                combined.extend_from_slice(&row.values);
                combined.extend_from_slice(&source_row.values);
                if join_predicate(
                    delete.selection.as_ref().expect("checked above"),
                    &combined,
                    combined_columns,
                    params,
                )? {
                    matched = true;
                    break;
                }
            }
            if !matched {
                continue;
            }
            if let Some((_, positions)) = &returning_plan {
                returned_rows.push(positions.iter().map(|p| row.values[*p].clone()).collect());
            }
            transaction.delete_row(database, table.id, row.clone())?;
            affected += 1;
            continue;
        }
        if !row_matches(
            delete.selection.as_ref(),
            &row,
            table,
            params,
            &ResolvedSubqueries::new(),
        )? {
            continue;
        }
        if let Some((_, positions)) = &returning_plan {
            returned_rows.push(positions.iter().map(|p| row.values[*p].clone()).collect());
        }
        transaction.delete_row(database, table.id, row.clone())?;
        affected += 1;
    }
    match returning_plan {
        Some((columns, _)) => Ok(SqlResult::rows(columns, returned_rows)),
        None => Ok(SqlResult::command(affected)),
    }
}

fn update_assignments<'a>(
    table: &TableDefinition,
    assignments: &'a [sqlparser::ast::Assignment],
) -> Result<Vec<(usize, &'a sqlparser::ast::Expr)>> {
    assignments
        .iter()
        .map(|assignment| {
            let name = match &assignment.target {
                AssignmentTarget::ColumnName(name) => simple_object_name(name, "column")?,
                AssignmentTarget::Tuple(_) => {
                    return Err(unsupported("UPDATE", "tuple assignments are not supported"));
                }
            };
            let position = column_position_by_name(table, name)?;
            Ok((position, &assignment.value))
        })
        .collect()
}

pub(super) fn insert_positions(
    table: &TableDefinition,
    columns: &[ObjectName],
) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Ok((0..table.columns.len()).collect());
    }
    let mut positions = Vec::with_capacity(columns.len());
    for column in columns {
        let name = simple_object_name(column, "column")?;
        let position = column_position_by_name(table, name)?;
        if positions.contains(&position) {
            return Err(DbError::InvalidState(format!(
                "INSERT repeats column {name}"
            )));
        }
        positions.push(position);
    }
    Ok(positions)
}
