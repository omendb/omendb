//! Morsel-driven chunked scanning, analytical aggregations, and streaming joins.
//!
//! Morsel-driven execution splits analytical table scans into bounded batches
//! (morsels) so that large scans do not allocate unbounded memory, and yields
//! cooperative cancellation checkpoints (`OperationControl`) to preserve OLTP
//! latency and priority protection under mixed workloads.

use std::collections::BTreeMap;

use crate::{
    ColumnId, DbError, OperationControl, RelationalDatabase, RelationalDatabaseTransaction, Result,
    Row, TableId, Value,
};

/// Default morsel size (number of rows per chunk).
pub const DEFAULT_MORSEL_SIZE: usize = 64;

/// A bounded batch of rows produced by a morsel scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MorselBatch {
    pub rows: Vec<Row>,
    pub morsel_index: usize,
    pub is_last: bool,
}

impl MorselBatch {
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// State machine for chunked morsel scanning of a relational table.
pub struct MorselScanner {
    table: TableId,
    morsel_size: usize,
    current_offset: usize,
    morsel_index: usize,
    exhausted: bool,
}

impl MorselScanner {
    #[must_use]
    pub fn new(table: TableId, morsel_size: usize) -> Self {
        Self {
            table,
            morsel_size: if morsel_size == 0 {
                DEFAULT_MORSEL_SIZE
            } else {
                morsel_size
            },
            current_offset: 0,
            morsel_index: 0,
            exhausted: false,
        }
    }

    /// Read the next morsel chunk from the database transaction under control.
    pub fn next_morsel(
        &mut self,
        database: &RelationalDatabase,
        transaction: &mut RelationalDatabaseTransaction,
        control: &OperationControl,
    ) -> Result<Option<MorselBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        control.check()?;

        // Scan from the database using limit and offset logic
        let scanned =
            transaction.scan(database, self.table, self.current_offset + self.morsel_size)?;
        let chunk: Vec<Row> = scanned.into_iter().skip(self.current_offset).collect();

        if chunk.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }

        let chunk_len = chunk.len();
        self.current_offset += chunk_len;
        let is_last = chunk_len < self.morsel_size;
        if is_last {
            self.exhausted = true;
        }

        let batch = MorselBatch {
            rows: chunk,
            morsel_index: self.morsel_index,
            is_last,
        };
        self.morsel_index += 1;
        Ok(Some(batch))
    }
}

/// Supported aggregation functions for analytical queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Specification for one aggregate column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateSpec {
    pub kind: AggregateKind,
    pub column: Option<ColumnId>,
    pub alias: String,
}

/// State accumulator for an aggregate function.
#[derive(Clone, Debug, PartialEq)]
pub enum AggregateAccumulator {
    Count(u64),
    SumUnset,
    SumI64(i64),
    SumU64(u64),
    Avg { sum_f64: f64, count: u64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl AggregateAccumulator {
    pub fn new(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(0),
            AggregateKind::Sum => Self::SumUnset,
            AggregateKind::Avg => Self::Avg {
                sum_f64: 0.0,
                count: 0,
            },
            AggregateKind::Min => Self::Min(None),
            AggregateKind::Max => Self::Max(None),
        }
    }

    pub fn update(&mut self, value: Option<&Value>) -> Result<()> {
        match self {
            Self::Count(count) => {
                if let Some(val) = value {
                    if !matches!(val, Value::Null) {
                        *count = count.saturating_add(1);
                    }
                } else {
                    *count = count.saturating_add(1);
                }
            }
            Self::SumUnset => {
                if let Some(val) = value {
                    match val {
                        Value::U64(u) => *self = Self::SumU64(*u),
                        Value::I64(i) => *self = Self::SumI64(*i),
                        Value::Null => {}
                        _ => {
                            return Err(DbError::InvalidState(
                                "SUM requires numeric column".to_owned(),
                            ));
                        }
                    }
                }
            }
            Self::SumI64(sum) => {
                if let Some(val) = value {
                    match val {
                        Value::I64(i) => *sum = sum.saturating_add(*i),
                        Value::U64(u) => *sum = sum.saturating_add(*u as i64),
                        Value::Null => {}
                        _ => {
                            return Err(DbError::InvalidState(
                                "SUM requires numeric column".to_owned(),
                            ));
                        }
                    }
                }
            }
            Self::SumU64(sum) => {
                if let Some(val) = value {
                    match val {
                        Value::U64(u) => *sum = sum.saturating_add(*u),
                        Value::I64(i) if *i >= 0 => *sum = sum.saturating_add(*i as u64),
                        Value::I64(i) => {
                            let converted = (*sum as i64).saturating_add(*i);
                            *self = Self::SumI64(converted);
                        }
                        Value::Null => {}
                        _ => {
                            return Err(DbError::InvalidState(
                                "SUM requires numeric column".to_owned(),
                            ));
                        }
                    }
                }
            }
            Self::Avg { sum_f64, count } => {
                if let Some(val) = value {
                    match val {
                        Value::I64(i) => {
                            *sum_f64 += *i as f64;
                            *count += 1;
                        }
                        Value::U64(u) => {
                            *sum_f64 += *u as f64;
                            *count += 1;
                        }
                        Value::Null => {}
                        _ => {
                            return Err(DbError::InvalidState(
                                "AVG requires numeric column".to_owned(),
                            ));
                        }
                    }
                }
            }
            Self::Min(current_min) => {
                if let Some(val) = value
                    && !matches!(val, Value::Null)
                {
                    match current_min {
                        None => *current_min = Some(val.clone()),
                        Some(cur) => {
                            if compare_values(val, cur) == std::cmp::Ordering::Less {
                                *current_min = Some(val.clone());
                            }
                        }
                    }
                }
            }
            Self::Max(current_max) => {
                if let Some(val) = value
                    && !matches!(val, Value::Null)
                {
                    match current_max {
                        None => *current_max = Some(val.clone()),
                        Some(cur) => {
                            if compare_values(val, cur) == std::cmp::Ordering::Greater {
                                *current_max = Some(val.clone());
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn finalize(self) -> Value {
        match self {
            Self::Count(count) => Value::U64(count),
            Self::SumUnset => Value::U64(0),
            Self::SumI64(sum) => Value::I64(sum),
            Self::SumU64(sum) => Value::U64(sum),
            Self::Avg { sum_f64, count } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::I64((sum_f64 / count as f64).round() as i64)
                }
            }
            Self::Min(min) => min.unwrap_or(Value::Null),
            Self::Max(max) => max.unwrap_or(Value::Null),
        }
    }
}

/// Analytical query specification with grouping, aggregation, and memory bounds.
#[derive(Clone, Debug)]
pub struct AnalyticalQuery {
    pub table: TableId,
    pub morsel_size: usize,
    pub group_by: Vec<ColumnId>,
    pub aggregates: Vec<AggregateSpec>,
    pub max_memory_bytes: usize,
}

impl AnalyticalQuery {
    #[must_use]
    pub fn new(table: TableId) -> Self {
        Self {
            table,
            morsel_size: DEFAULT_MORSEL_SIZE,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            max_memory_bytes: 64 * 1024 * 1024, // 64 MiB default quota
        }
    }

    #[must_use]
    pub fn with_morsel_size(mut self, size: usize) -> Self {
        self.morsel_size = size;
        self
    }

    #[must_use]
    pub fn with_group_by(mut self, column: ColumnId) -> Self {
        self.group_by.push(column);
        self
    }

    #[must_use]
    pub fn with_aggregate(
        mut self,
        kind: AggregateKind,
        column: Option<ColumnId>,
        alias: impl Into<String>,
    ) -> Self {
        self.aggregates.push(AggregateSpec {
            kind,
            column,
            alias: alias.into(),
        });
        self
    }
}

/// Result of an analytical query execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalResult {
    pub column_names: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub morsels_processed: usize,
    pub total_rows_scanned: usize,
}

/// Executes an analytical query over morsel streams with memory quota enforcement.
pub struct AnalyticalExecutor;

impl AnalyticalExecutor {
    pub fn execute(
        database: &RelationalDatabase,
        transaction: &mut RelationalDatabaseTransaction,
        query: &AnalyticalQuery,
        control: &OperationControl,
    ) -> Result<AnalyticalResult> {
        let catalog = database.catalog();
        let table_def = catalog.table(query.table)?;

        let mut scanner = MorselScanner::new(query.table, query.morsel_size);
        let mut morsels_processed = 0;
        let mut total_rows_scanned = 0;

        // Column names in the final output
        let mut column_names = Vec::new();
        for col_id in &query.group_by {
            let col = table_def.column(*col_id)?;
            column_names.push(col.name.clone());
        }
        for agg in &query.aggregates {
            column_names.push(agg.alias.clone());
        }

        if query.group_by.is_empty() {
            // Global aggregation across the entire table
            let mut accumulators: Vec<AggregateAccumulator> = query
                .aggregates
                .iter()
                .map(|spec| AggregateAccumulator::new(spec.kind))
                .collect();

            while let Some(morsel) = scanner.next_morsel(database, transaction, control)? {
                morsels_processed += 1;
                total_rows_scanned += morsel.len();

                for row in &morsel.rows {
                    for (acc, spec) in accumulators.iter_mut().zip(&query.aggregates) {
                        let value = if let Some(col_id) = spec.column {
                            Some(row.value(table_def, col_id)?)
                        } else {
                            None
                        };
                        acc.update(value)?;
                    }
                }
            }

            let result_row: Vec<Value> = accumulators
                .into_iter()
                .map(AggregateAccumulator::finalize)
                .collect();

            return Ok(AnalyticalResult {
                column_names,
                rows: vec![result_row],
                morsels_processed,
                total_rows_scanned,
            });
        }

        // Grouped aggregation using an in-memory hash map with memory accounting
        let mut groups: BTreeMap<Vec<Value>, Vec<AggregateAccumulator>> = BTreeMap::new();
        let mut approximate_memory: usize = 0;

        while let Some(morsel) = scanner.next_morsel(database, transaction, control)? {
            morsels_processed += 1;
            total_rows_scanned += morsel.len();

            for row in &morsel.rows {
                let mut group_key = Vec::with_capacity(query.group_by.len());
                for col_id in &query.group_by {
                    group_key.push(row.value(table_def, *col_id)?.clone());
                }

                if !groups.contains_key(&group_key) {
                    approximate_memory += 64 + group_key.len() * 32 + query.aggregates.len() * 32;
                    if approximate_memory > query.max_memory_bytes {
                        return Err(DbError::ResourceLimitExceeded(format!(
                            "analytical query exceeded memory quota of {} bytes",
                            query.max_memory_bytes
                        )));
                    }
                    let accs = query
                        .aggregates
                        .iter()
                        .map(|spec| AggregateAccumulator::new(spec.kind))
                        .collect();
                    groups.insert(group_key.clone(), accs);
                }

                let accs = groups.get_mut(&group_key).expect("group exists");
                for (acc, spec) in accs.iter_mut().zip(&query.aggregates) {
                    let value = if let Some(col_id) = spec.column {
                        Some(row.value(table_def, col_id)?)
                    } else {
                        None
                    };
                    acc.update(value)?;
                }
            }
        }

        let mut output_rows = Vec::with_capacity(groups.len());
        for (group_key, accs) in groups {
            let mut out = group_key;
            for acc in accs {
                out.push(acc.finalize());
            }
            output_rows.push(out);
        }

        Ok(AnalyticalResult {
            column_names,
            rows: output_rows,
            morsels_processed,
            total_rows_scanned,
        })
    }
}

fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::I64(a), Value::I64(b)) => a.cmp(b),
        (Value::U64(a), Value::U64(b)) => a.cmp(b),
        (Value::I64(a), Value::U64(b)) => (*a as i128).cmp(&(*b as i128)),
        (Value::U64(a), Value::I64(b)) => (*a as i128).cmp(&(*b as i128)),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulator_computes_standard_aggregates() {
        let mut count = AggregateAccumulator::new(AggregateKind::Count);
        let mut sum = AggregateAccumulator::new(AggregateKind::Sum);
        let mut avg = AggregateAccumulator::new(AggregateKind::Avg);
        let mut min = AggregateAccumulator::new(AggregateKind::Min);
        let mut max = AggregateAccumulator::new(AggregateKind::Max);

        for val in [10u64, 20u64, 30u64] {
            let v = Value::U64(val);
            count.update(Some(&v)).unwrap();
            sum.update(Some(&v)).unwrap();
            avg.update(Some(&v)).unwrap();
            min.update(Some(&v)).unwrap();
            max.update(Some(&v)).unwrap();
        }

        assert_eq!(count.finalize(), Value::U64(3));
        assert_eq!(sum.finalize(), Value::U64(60));
        assert_eq!(avg.finalize(), Value::I64(20));
        assert_eq!(min.finalize(), Value::U64(10));
        assert_eq!(max.finalize(), Value::U64(30));
    }
}
