//! Record solved parameter domains in the retained AST without evaluating
//! values. Metadata names are captured before this rewrite.
use std::collections::BTreeMap;
use std::ops::ControlFlow;

use sqlparser::ast::{
    CastKind, DataType, ExactNumberInfo, Expr, Statement, TimezoneInfo, Value,
    visit_expressions_mut,
};

use crate::{ColumnType, Result};

pub(super) fn coercions(
    statement: &mut Statement,
    types: &[Option<ColumnType>],
    literals: &BTreeMap<*const Expr, ColumnType>,
) -> Result<()> {
    // sqlparser's expression closure is post-order: the newly inserted Cast
    // and its placeholder child are not visited again during this walk.
    if let ControlFlow::Break(error) = visit_expressions_mut(statement, |expr| {
        let node = expr as *const Expr;
        let Expr::Value(value) = expr else {
            return ControlFlow::Continue(());
        };
        let ty = if let Value::Placeholder(name) = &value.value {
            let index = match crate::sql::values::parameter_index(name) {
                Ok(index) => index,
                Err(error) => return ControlFlow::Break(error),
            };
            types.get(index).copied().flatten()
        } else {
            literals.get(&node).copied()
        };
        if let Some(ty) = ty {
            *expr = Expr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(Expr::Value(value.clone())),
                data_type: data_type(ty),
                array: false,
                format: None,
            };
        }
        ControlFlow::Continue(())
    }) {
        return Err(error);
    }
    Ok(())
}

fn data_type(ty: ColumnType) -> DataType {
    match ty {
        ColumnType::Bytes => DataType::Bytea,
        ColumnType::Bool => DataType::Boolean,
        ColumnType::I64 => DataType::BigInt(None),
        ColumnType::U64 => DataType::UInt64,
        ColumnType::Text => DataType::Text,
        ColumnType::Float64 => DataType::DoublePrecision,
        ColumnType::Date => DataType::Date,
        ColumnType::Timestamp => DataType::Timestamp(None, TimezoneInfo::None),
        ColumnType::Decimal => DataType::Decimal(ExactNumberInfo::None),
        ColumnType::Uuid => DataType::Uuid,
    }
}
