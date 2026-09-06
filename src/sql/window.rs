//! Window function evaluation for the embedded SQL tier.
//!
//! Window functions are a projection over the full filtered row set: each
//! output value depends on a partition of rows (PARTITION BY), ordered
//! within that partition (ORDER BY), walked once per function. The phase
//! runs between row matching and row projection in the plain-SELECT path
//! and appends one column per window call; the projection plan resolves
//! those columns positionally. Named windows and explicit frames are
//! refused honestly — the default frame is the whole partition for
//! ranking/value functions and the running-prefix (RANGE UNBOUNDED
//! PRECEDING..CURRENT ROW) for aggregates, which is what the alpha
//! surface needs.

use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, SelectItem, WindowType};

use crate::morsel::{AggregateAccumulator, AggregateKind};
use crate::{Result, Row, TableDefinition, Value};

use super::query::{OrderTerm, compare_rows};
use super::{column_position, unsupported};

/// One window function call resolved from the projection.
#[derive(Clone)]
pub(crate) struct WindowCall {
    /// Function name, lowercased.
    name: String,
    /// The argument column position. `None` only for `count(*)`-style
    /// calls; every other supported function takes exactly one column.
    argument: Option<usize>,
    /// PARTITION BY column positions.
    partition_by: Vec<usize>,
    /// ORDER BY terms within each partition (position, direction,
    /// null placement), the same shape statement ORDER BY uses.
    order_by: Vec<OrderTerm>,
}

impl WindowCall {
    /// The static result type the projection reports for this call.
    pub(crate) fn static_type(&self, table: &TableDefinition) -> crate::ColumnType {
        match self.name.as_str() {
            "row_number" | "rank" | "dense_rank" => crate::ColumnType::I64,
            "count" => crate::ColumnType::U64,
            "avg" => crate::ColumnType::Float64,
            "sum" | "min" | "max" | "lag" | "lead" | "first_value" | "last_value" => self
                .argument
                .and_then(|position| table.columns.get(position))
                .map(|column| column.data_type)
                .unwrap_or(crate::ColumnType::I64),
            _ => crate::ColumnType::I64,
        }
    }
}

/// Extract every window function call from the projection. Returns the
/// calls in projection order; `None` when the projection has none.
pub(crate) fn window_calls(
    select: &sqlparser::ast::Select,
    table: &TableDefinition,
) -> Result<Option<Vec<WindowCall>>> {
    let mut calls = Vec::new();
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expression) => expression,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        let Expr::Function(function) = expr else {
            continue;
        };
        let Some(over) = &function.over else {
            continue;
        };
        calls.push(window_call(function, over, table)?);
    }
    Ok(if calls.is_empty() { None } else { Some(calls) })
}

fn window_call(
    function: &Function,
    over: &WindowType,
    table: &TableDefinition,
) -> Result<WindowCall> {
    let (partition_by, order_by) = match over {
        WindowType::WindowSpec(spec) => {
            if spec.window_name.is_some() {
                return Err(unsupported(
                    "OVER",
                    "window names inside OVER are not supported",
                ));
            }
            if spec.window_frame.is_some() {
                return Err(unsupported(
                    "OVER",
                    "window frames are not supported; the default frame (whole partition for ranking and value functions, running prefix for aggregates) applies",
                ));
            }
            let mut partition = Vec::new();
            for expression in &spec.partition_by {
                let Some(position) = column_position(table, expression)? else {
                    return Err(unsupported(
                        "PARTITION BY",
                        "only table columns are supported",
                    ));
                };
                partition.push(position);
            }
            let mut order = Vec::new();
            for term in &spec.order_by {
                if term.with_fill.is_some() {
                    return Err(unsupported("OVER ORDER BY", "WITH FILL is not supported"));
                }
                let Some(position) = column_position(table, &term.expr)? else {
                    return Err(unsupported(
                        "OVER ORDER BY",
                        "only table columns are supported",
                    ));
                };
                let ascending = term.options.asc.unwrap_or(true);
                order.push(OrderTerm {
                    position,
                    ascending,
                    nulls_first: term.options.nulls_first.unwrap_or(!ascending),
                });
            }
            (partition, order)
        }
        WindowType::NamedWindow(name) => {
            return Err(unsupported(
                "OVER",
                &format!("named window {name} is not supported; inline the window specification"),
            ));
        }
    };

    let name = function.name.to_string().to_ascii_lowercase();
    let argument = match name.as_str() {
        "row_number" | "rank" | "dense_rank" => {
            if !function_argument_list(function)?.is_empty() {
                return Err(unsupported("OVER", &format!("{name} takes no arguments")));
            }
            None
        }
        "lag" | "lead" | "first_value" | "last_value" | "sum" | "avg" | "min" | "max" => {
            let arguments = function_argument_list(function)?;
            match arguments.as_slice() {
                [single] => Some(argument_column(single, table)?),
                _ => {
                    return Err(unsupported(
                        "OVER",
                        &format!("{name} requires exactly one argument"),
                    ));
                }
            }
        }
        "count" => match function_argument_list(function)?.as_slice() {
            [] => None,
            [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => None,
            [single] => Some(argument_column(single, table)?),
            _ => {
                return Err(unsupported(
                    "OVER",
                    "count requires exactly one argument (or none for count)",
                ));
            }
        },
        "nth_value" => {
            return Err(unsupported(
                "OVER",
                "nth_value is not supported; first_value and last_value are",
            ));
        }
        other => {
            return Err(unsupported(
                "OVER",
                &format!("window function {other} is not supported"),
            ));
        }
    };
    Ok(WindowCall {
        name,
        argument,
        partition_by,
        order_by,
    })
}

fn function_argument_list(function: &Function) -> Result<Vec<&FunctionArg>> {
    match &function.args {
        sqlparser::ast::FunctionArguments::List(list) => Ok(list.args.iter().collect()),
        sqlparser::ast::FunctionArguments::None => Ok(Vec::new()),
        sqlparser::ast::FunctionArguments::Subquery(_) => Err(unsupported(
            "OVER",
            "subquery window function arguments are not supported",
        )),
    }
}

fn argument_column(argument: &FunctionArg, table: &TableDefinition) -> Result<usize> {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = argument else {
        return Err(unsupported(
            "OVER",
            "window function argument must be a plain column expression",
        ));
    };
    let Some(position) = column_position(table, expr)? else {
        return Err(unsupported(
            "OVER",
            "window function argument must be a table column",
        ));
    };
    Ok(position)
}

/// Evaluate the window calls over the matched rows and produce, per row,
/// the appended window values in call order. Rows keep their incoming
/// order; window evaluation sorts a partition index copy internally.
pub(crate) fn evaluate_window_columns(
    rows: &[Row],
    calls: &[WindowCall],
) -> Result<Vec<Vec<Value>>> {
    // Column j of call j: one value per row.
    let mut columns: Vec<Vec<Value>> = Vec::with_capacity(calls.len());
    for call in calls {
        columns.push(evaluate_call(rows, call)?);
    }
    // Transpose: row i gets calls[j] value for each j.
    let mut per_row = vec![Vec::with_capacity(calls.len()); rows.len()];
    for (row_index, row_output) in per_row.iter_mut().enumerate() {
        for column in &columns {
            row_output.push(column[row_index].clone());
        }
    }
    Ok(per_row)
}

fn evaluate_call(rows: &[Row], call: &WindowCall) -> Result<Vec<Value>> {
    // Group row indices by partition key. BTreeMap keeps a deterministic
    // partition walk order (first-seen key order in the value order).
    let mut partitions: std::collections::BTreeMap<Vec<Value>, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        let key = call
            .partition_by
            .iter()
            .map(|&position| row.values[position].clone())
            .collect();
        partitions.entry(key).or_default().push(index);
    }

    let mut output: Vec<Value> = vec![Value::Null; rows.len()];
    for (_, indices) in partitions {
        let mut ordered = indices;
        if !call.order_by.is_empty() {
            ordered
                .sort_by(|&left, &right| compare_rows(&rows[left], &rows[right], &call.order_by));
        }
        let partition_values = partition_values(rows, &ordered, call)?;
        for (position_in_partition, row_index) in ordered.iter().enumerate() {
            output[*row_index] = partition_values[position_in_partition].clone();
        }
    }
    Ok(output)
}

fn partition_values(rows: &[Row], ordered: &[usize], call: &WindowCall) -> Result<Vec<Value>> {
    match call.name.as_str() {
        "row_number" => Ok(row_numbers(ordered.len())),
        "rank" | "dense_rank" => Ok(ranks(rows, ordered, call)),
        "lag" | "lead" => Ok(offsets(rows, ordered, call)),
        "first_value" | "last_value" => Ok(frame_values(rows, ordered, call)),
        "count" | "sum" | "avg" | "min" | "max" => running_aggregates(rows, ordered, call),
        other => Err(unsupported(
            "OVER",
            &format!("window function {other} is not supported"),
        )),
    }
}

fn row_numbers(count: usize) -> Vec<Value> {
    (1..=count as i64).map(Value::I64).collect()
}

fn ranks(rows: &[Row], ordered: &[usize], call: &WindowCall) -> Vec<Value> {
    let dense = call.name == "dense_rank";
    let mut output = Vec::with_capacity(ordered.len());
    let mut rank = 1i64;
    let mut dense_rank = 1i64;
    for (position, &row_index) in ordered.iter().enumerate() {
        if position > 0 {
            let previous = ordered[position - 1];
            let tied = call.order_by.is_empty()
                || call.order_by.iter().all(|term| {
                    rows[previous].values[term.position] == rows[row_index].values[term.position]
                });
            if !tied {
                rank = position as i64 + 1;
                dense_rank += 1;
            }
        }
        output.push(Value::I64(if dense { dense_rank } else { rank }));
    }
    output
}

fn offsets(rows: &[Row], ordered: &[usize], call: &WindowCall) -> Vec<Value> {
    let argument = call
        .argument
        .expect("lag and lead validate an argument at plan time");
    let mut output = Vec::with_capacity(ordered.len());
    for (position, _) in ordered.iter().enumerate() {
        // lag looks one row back; lead one row forward. Both offset by
        // one (the default) — explicit offsets are refused at plan time.
        let source = if call.name == "lag" {
            position.checked_sub(1)
        } else {
            Some(position + 1).filter(|&next| next < ordered.len())
        };
        output.push(match source {
            Some(source) => rows[ordered[source]].values[argument].clone(),
            None => Value::Null,
        });
    }
    output
}

fn frame_values(rows: &[Row], ordered: &[usize], call: &WindowCall) -> Vec<Value> {
    let argument = call
        .argument
        .expect("value functions validate an argument at plan time");
    let source = if call.name == "first_value" {
        ordered.first().copied()
    } else {
        ordered.last().copied()
    };
    let value = source
        .map(|source| rows[source].values[argument].clone())
        .unwrap_or(Value::Null);
    vec![value; ordered.len()]
}

fn aggregate_kind(name: &str) -> Result<AggregateKind> {
    match name {
        "count" => Ok(AggregateKind::Count),
        "sum" => Ok(AggregateKind::Sum),
        "avg" => Ok(AggregateKind::Avg),
        "min" => Ok(AggregateKind::Min),
        "max" => Ok(AggregateKind::Max),
        other => Err(unsupported(
            "OVER",
            &format!("window function {other} is not supported"),
        )),
    }
}

fn running_aggregates(rows: &[Row], ordered: &[usize], call: &WindowCall) -> Result<Vec<Value>> {
    // Without ORDER BY the default frame is the whole partition: every
    // row sees the full aggregate (PostgreSQL semantics). With ORDER BY
    // the default frame is the running prefix through the current row.
    if call.order_by.is_empty() {
        let mut whole = AggregateAccumulator::new(aggregate_kind(&call.name)?);
        for &row_index in ordered {
            let value = call
                .argument
                .map(|position| &rows[row_index].values[position]);
            whole.update(value)?;
        }
        let total = whole.finalize();
        return Ok(vec![total; ordered.len()]);
    }
    let mut accumulator = AggregateAccumulator::new(aggregate_kind(&call.name)?);
    let mut output = Vec::with_capacity(ordered.len());
    for &row_index in ordered {
        let value = call
            .argument
            .map(|position| &rows[row_index].values[position]);
        // The accumulator skips NULLs for every aggregate except count(*),
        // which is exactly the SQL window default.
        accumulator.update(value)?;
        output.push(accumulator.clone().finalize());
    }
    Ok(output)
}
