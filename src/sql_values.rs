use sqlparser::ast::{Expr, UnaryOperator, Value as AstValue};

use crate::{ColumnDefinition, ColumnType, DbError, Key, Result, TableDefinition, Value};

use super::unsupported;

pub(super) fn literal_value(expression: &Expr, params: &[Value]) -> Result<Value> {
    match expression {
        Expr::Nested(expression) => literal_value(expression, params),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => literal_value(expr, params),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match literal_value(expr, params)? {
            Value::I64(value) => value
                .checked_neg()
                .map(Value::I64)
                .ok_or_else(|| DbError::InvalidState("integer literal underflow".to_owned())),
            _ => Err(DbError::InvalidState(
                "unary minus requires an integer literal".to_owned(),
            )),
        },
        Expr::Value(value) => match &value.value {
            AstValue::Number(number, _) => number
                .to_string()
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| DbError::InvalidState("SQL numeric literals must be i64".to_owned())),
            AstValue::Boolean(value) => Ok(Value::Bool(*value)),
            AstValue::Null => Ok(Value::Null),
            AstValue::SingleQuotedByteStringLiteral(value)
            | AstValue::DoubleQuotedByteStringLiteral(value) => {
                Ok(Value::Bytes(value.as_bytes().to_vec()))
            }
            AstValue::HexStringLiteral(value) => decode_hex(value),
            AstValue::Placeholder(name) => parameter_value(name, params),
            value => value
                .clone()
                .into_string()
                .map(Value::Text)
                .ok_or_else(|| unsupported("literal", "this literal type is not supported")),
        },
        _ => Err(unsupported(
            "expression",
            "only literals and simple column references are supported",
        )),
    }
}

pub(super) fn parameter_index(name: &str) -> Result<usize> {
    let digits = name
        .strip_prefix('$')
        .or_else(|| name.strip_prefix('?'))
        .ok_or_else(|| {
            DbError::SqlParameter(format!(
                "placeholder {name} is not positional; use $1 or ?1"
            ))
        })?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DbError::SqlParameter(format!(
            "placeholder {name} is not positional; use $1 or ?1"
        )));
    }
    let one_based = digits.parse::<usize>().map_err(|_| {
        DbError::SqlParameter(format!("placeholder {name} has an invalid position"))
    })?;
    one_based
        .checked_sub(1)
        .ok_or_else(|| DbError::SqlParameter(format!("placeholder {name} must start at 1")))
}

fn parameter_value(name: &str, params: &[Value]) -> Result<Value> {
    let index = parameter_index(name)?;
    params.get(index).cloned().ok_or_else(|| {
        DbError::SqlParameter(format!(
            "placeholder {name} requires parameter {}, but only {} were supplied",
            index + 1,
            params.len()
        ))
    })
}

fn decode_hex(value: &str) -> Result<Value> {
    if !value.len().is_multiple_of(2) {
        return Err(DbError::InvalidState(
            "hex byte literals must contain pairs of digits".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().as_chunks::<2>().0 {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(Value::Bytes(bytes))
}

fn hex_digit(digit: u8) -> Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(DbError::InvalidState(
            "hex byte literals contain a non-hex digit".to_owned(),
        )),
    }
}

pub(super) fn coerce_value(value: Value, column: &ColumnDefinition) -> Result<Value> {
    // Integer literals always parse as I64; coerce into the column's integer
    // type when the value fits, mirroring sql_primary_key's rule.
    let value = match (&value, column.data_type) {
        (Value::I64(inner), ColumnType::U64) if *inner >= 0 => {
            Value::U64(u64::try_from(*inner).map_err(|_| {
                DbError::InvalidState(format!("value does not satisfy SQL column {}", column.name))
            })?)
        }
        (Value::U64(inner), ColumnType::I64) => {
            Value::I64(i64::try_from(*inner).map_err(|_| {
                DbError::InvalidState(format!("value does not satisfy SQL column {}", column.name))
            })?)
        }
        _ => value,
    };
    let valid = matches!(
        (&value, column.data_type),
        (Value::Null, _)
            | (Value::Bytes(_), ColumnType::Bytes)
            | (Value::Bool(_), ColumnType::Bool)
            | (Value::I64(_), ColumnType::I64)
            | (Value::U64(_), ColumnType::U64)
            | (Value::Text(_), ColumnType::Text)
    );
    if matches!(value, Value::Null) && !column.nullable {
        return Err(DbError::SqlNotNullViolation {
            column: column.name.clone(),
        });
    }
    if !valid {
        return Err(DbError::InvalidState(format!(
            "value does not satisfy SQL column {}",
            column.name
        )));
    }
    Ok(value)
}

pub(super) fn sql_primary_key(table: &TableDefinition, values: &[Value]) -> Result<Key> {
    let value = values
        .first()
        .ok_or_else(|| DbError::InvalidState("SQL row has no primary key".to_owned()))?;
    let record = match value {
        Value::I64(value) if *value >= 0 => u64::try_from(*value).map_err(|_| {
            DbError::InvalidState("SQL id must be a non-negative integer".to_owned())
        })?,
        Value::U64(value) => *value,
        // Text- and bytes-keyed tables (credentials, grants) need stable
        // per-value records or every row collides on one key. FNV-1a 64
        // is dependency-free and deterministic across processes and
        // restarts, which the durable key requires.
        Value::Text(text) => fnv1a_64(text.as_bytes()),
        Value::Bytes(bytes) => fnv1a_64(bytes),
        _ => return Ok(Key::new(table.id.0, 0)),
    };
    Ok(Key::new(table.id.0, record))
}

/// FNV-1a 64: stable across builds and processes, unlike std's SipHash
/// whose keys are randomized per process.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
