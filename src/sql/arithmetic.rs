//! Scalar SQL arithmetic shared by projections, predicates and UPDATE.
//!
//! Promotion follows binding::common_type: float > decimal > integer, with
//! mixed I64/U64 bounded by I64. Intermediate width never widens a SQL result.

use sqlparser::ast::BinaryOperator;

use crate::sql_types::{DecimalValue, F64};
use crate::{DbError, Result, Value};

use super::unsupported;

pub(super) fn evaluate(op: &BinaryOperator, lhs: &Value, rhs: &Value) -> Result<Value> {
    use BinaryOperator::*;

    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return Ok(Value::Null);
    }
    let float_pair = matches!(
        (lhs, rhs),
        (Value::Float64(_), _) | (_, Value::Float64(_))
            if matches!(rhs, Value::I64(_) | Value::U64(_) | Value::Float64(_) | Value::Decimal(_))
                && matches!(lhs, Value::I64(_) | Value::U64(_) | Value::Float64(_) | Value::Decimal(_))
    );
    if float_pair {
        let l = sql_float(lhs);
        let r = sql_float(rhs);
        let value = match op {
            Plus => l + r,
            Minus => l - r,
            Multiply => l * r,
            Divide if r == 0.0 && !l.is_nan() => return Err(DbError::SqlDivisionByZero),
            Divide => l / r,
            _ => {
                return Err(unsupported(
                    "arithmetic",
                    "modulo is not defined for float operands",
                ));
            }
        };
        return Ok(Value::Float64(F64::new(value)));
    }

    let decimal_pair = matches!(
        (lhs, rhs),
        (Value::Decimal(_) | Value::I64(_) | Value::U64(_), Value::Decimal(_) | Value::I64(_) | Value::U64(_))
            if matches!(lhs, Value::Decimal(_)) || matches!(rhs, Value::Decimal(_))
    );
    if decimal_pair {
        let l = sql_decimal(lhs)?;
        let r = sql_decimal(rhs)?;
        let (mantissa, scale) = match op {
            Plus | Minus => {
                let scale = l.scale.max(r.scale);
                let l = l.rescale(scale)?;
                let r = r.rescale(scale)?;
                let mantissa = if *op == Plus {
                    l.mantissa.checked_add(r.mantissa)
                } else {
                    l.mantissa.checked_sub(r.mantissa)
                };
                (mantissa, scale)
            }
            Multiply => (l.mantissa.checked_mul(r.mantissa), l.scale + r.scale),
            Divide => return decimal_divide(&l, &r),
            _ => {
                return Err(unsupported(
                    "arithmetic",
                    "modulo is not defined for numeric operands",
                ));
            }
        };
        return Ok(Value::Decimal(DecimalValue::new(
            mantissa.ok_or_else(|| out_of_range("arithmetic"))?,
            scale,
        )?));
    }

    let (l, r) = match (lhs, rhs) {
        (Value::U64(l), Value::U64(r)) => (i128::from(*l), i128::from(*r)),
        (Value::I64(l), Value::I64(r)) => (i128::from(*l), i128::from(*r)),
        (Value::U64(l), Value::I64(r)) => (i128::from(*l), i128::from(*r)),
        (Value::I64(l), Value::U64(r)) => (i128::from(*l), i128::from(*r)),
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
        Divide | Modulo if r == 0 => return Err(DbError::SqlDivisionByZero),
        // SQL integer division truncates toward zero; remainder follows the
        // dividend's sign. Euclidean division is not the SQL contract.
        Divide => l.checked_div(r),
        Modulo => l.checked_rem(r),
        _ => {
            return Err(unsupported(
                "arithmetic",
                "only arithmetic operators are supported here",
            ));
        }
    }
    .ok_or_else(|| out_of_range("arithmetic"))?;
    if matches!((lhs, rhs), (Value::U64(_), Value::U64(_))) {
        u64::try_from(value)
            .map(Value::U64)
            .map_err(|_| out_of_range("unsigned arithmetic"))
    } else {
        i64::try_from(value)
            .map(Value::I64)
            .map_err(|_| out_of_range("signed arithmetic"))
    }
}

fn out_of_range(what: &str) -> DbError {
    DbError::SqlNumericValueOutOfRange(format!("{what} overflow"))
}

fn sql_float(value: &Value) -> f64 {
    match value {
        Value::Float64(inner) => inner.0,
        Value::I64(inner) => *inner as f64,
        Value::U64(inner) => *inner as f64,
        Value::Decimal(inner) => inner.to_f64(),
        other => unreachable!("float promotion requires numeric operand, got {other:?}"),
    }
}

fn sql_decimal(value: &Value) -> Result<DecimalValue> {
    match value {
        Value::Decimal(inner) => Ok(*inner),
        Value::I64(inner) => DecimalValue::new(i128::from(*inner), 0),
        Value::U64(inner) => DecimalValue::new(i128::from(*inner), 0),
        other => Err(DbError::InvalidState(format!(
            "numeric arithmetic requires numeric operands, got {other:?}"
        ))),
    }
}

/// Bounded numeric division, rounded half away from zero. The retained scale
/// policy is dividend scale + divisor mantissa digits + 4, not PostgreSQL's
/// arbitrary-precision numeric division scale. Intermediates must fit i128
/// and the result must satisfy DecimalValue's existing precision bound.
fn decimal_divide(l: &DecimalValue, r: &DecimalValue) -> Result<Value> {
    if r.mantissa == 0 {
        return Err(DbError::SqlDivisionByZero);
    }
    let target_scale = i64::from(l.scale) + i64::from(DecimalValue::digit_count(r.mantissa)) + 4;
    let shift = target_scale + i64::from(r.scale) - i64::from(l.scale);
    let factor = 10_i128
        .checked_pow(u32::try_from(shift).map_err(|_| out_of_range("numeric division"))?)
        .ok_or_else(|| out_of_range("numeric division"))?;
    let numerator = l
        .mantissa
        .checked_mul(factor)
        .ok_or_else(|| out_of_range("numeric division"))?;
    let quotient = numerator / r.mantissa;
    let remainder = numerator % r.mantissa;
    // Comparing twice the remainder handles odd divisors and exact results.
    // Valid decimal mantissas are below 10^38, so doubling fits u128.
    let rounded = if remainder.unsigned_abs() * 2 >= r.mantissa.unsigned_abs() {
        quotient + numerator.signum() * r.mantissa.signum()
    } else {
        quotient
    };
    let scale = u32::try_from(target_scale).map_err(|_| out_of_range("numeric division"))?;
    Ok(Value::Decimal(DecimalValue::new(rounded, scale)?))
}
