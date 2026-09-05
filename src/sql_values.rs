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
            // Negative decimal and float literals apply the unary minus
            // to the parsed value, matching PostgreSQL input syntax.
            Value::Decimal(value) => Ok(Value::Decimal(
                crate::sql_types::DecimalValue::new(-value.mantissa, value.scale)
                    .map_err(|_| DbError::InvalidState("decimal literal underflow".to_owned()))?,
            )),
            Value::Float64(value) => Ok(Value::Float64(crate::sql_types::F64::new(-value.0))),
            _ => Err(DbError::InvalidState(
                "unary minus requires a numeric literal".to_owned(),
            )),
        },
        Expr::Value(value) => match &value.value {
            // Numeric literals: integers stay i64; fractional or
            // exponent-form numbers parse as decimal values (PostgreSQL
            // treats untyped 10.5 as numeric).
            AstValue::Number(number, _) => {
                let text = number.to_string();
                match text.parse::<i64>() {
                    Ok(value) => Ok(Value::I64(value)),
                    Err(_) => parse_decimal_text(&text).map_err(|_| {
                        DbError::SqlNumericValueOutOfRange(format!(
                            "SQL numeric literal {text} is out of range"
                        ))
                    }),
                }
            }
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
        Expr::Function(func) if func.name.to_string().eq_ignore_ascii_case("coalesce") => {
            let sqlparser::ast::FunctionArguments::List(list) = &func.args else {
                return Err(unsupported("COALESCE", "at least one argument is required"));
            };
            if list.args.is_empty() {
                return Err(unsupported("COALESCE", "at least one argument is required"));
            }
            for argument in &list.args {
                let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    expression,
                )) = argument
                else {
                    return Err(unsupported("COALESCE", "arguments must be expressions"));
                };
                let value = literal_value(expression, params)?;
                if !matches!(value, Value::Null) {
                    return Ok(value);
                }
            }
            Ok(Value::Null)
        }
        Expr::Function(func) if function_has_no_arguments(func) => {
            session_function_value(&func.name.to_string().to_lowercase())
        }
        _ => Err(unsupported(
            "expression",
            "only literals and simple column references are supported",
        )),
    }
}

fn function_has_no_arguments(func: &sqlparser::ast::Function) -> bool {
    match &func.args {
        sqlparser::ast::FunctionArguments::None => true,
        sqlparser::ast::FunctionArguments::List(list) => {
            list.args.is_empty() && list.clauses.is_empty()
        }
        sqlparser::ast::FunctionArguments::Subquery(_) => false,
    }
}

/// Zero-argument session functions that clients call for capability
/// detection and connection startup. They are properties of the server,
/// not the statement, so they evaluate in any expression position that
/// accepts a literal.
pub(super) fn session_function_value(name: &str) -> Result<Value> {
    match name {
        "version" => Ok(Value::Text(format!(
            "OmenDB {} (on PostgreSQL wire protocol)",
            env!("CARGO_PKG_VERSION")
        ))),
        "current_user" | "user" => Ok(Value::Text("omendb".to_owned())),
        "current_schema" => Ok(Value::Text("public".to_owned())),
        "current_database" => Ok(Value::Text("omendb".to_owned())),
        _ => Err(unsupported(
            "function",
            "this function is not supported; supported zero-argument functions are version(), current_user(), current_schema(), current_database()",
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

/// Coerce one comparison operand toward the opposite side's column type.
/// Only literal-shaped values convert (string into date/numeric/uuid,
/// integer into decimal/float); a value already of the target type and
/// unconvertible shapes pass through untouched so the comparison
/// evaluates Unknown instead of erroring.
pub(crate) fn coerce_for_comparison(value: Value, column: &ColumnDefinition) -> Result<Value> {
    match (&value, column.data_type) {
        (Value::Text(_), crate::ColumnType::Date)
        | (Value::Text(_), crate::ColumnType::Timestamp)
        | (Value::Text(_), crate::ColumnType::Decimal)
        | (Value::Text(_), crate::ColumnType::Float64)
        | (Value::Text(_), crate::ColumnType::Uuid)
        | (Value::I64(_), crate::ColumnType::Decimal)
        | (Value::I64(_), crate::ColumnType::Float64)
        | (Value::U64(_), crate::ColumnType::Decimal)
        | (Value::U64(_), crate::ColumnType::Float64) => coerce_value(value, column),
        _ => Ok(value),
    }
}

pub(crate) fn coerce_value(value: Value, column: &ColumnDefinition) -> Result<Value> {
    // Integer literals always parse as I64; coerce into the column's integer
    // type when the value fits, mirroring sql_primary_key's rule. String
    // literals convert into the column's type like PostgreSQL input parsing.
    let value = match (&value, column.data_type) {
        (Value::I64(inner), ColumnType::U64) if *inner >= 0 => {
            Value::U64(u64::try_from(*inner).map_err(|_| {
                DbError::SqlNumericValueOutOfRange(format!(
                    "value does not fit SQL column {}",
                    column.name
                ))
            })?)
        }
        (Value::U64(inner), ColumnType::I64) => {
            Value::I64(i64::try_from(*inner).map_err(|_| {
                DbError::SqlNumericValueOutOfRange(format!(
                    "value does not fit SQL column {}",
                    column.name
                ))
            })?)
        }
        // Typed columns accept integer and string literals through the
        // same input grammar as explicit casts: '2026-08-31', 42, 'NaN'.
        (Value::I64(inner), ColumnType::Float64) => {
            Value::Float64(crate::sql_types::F64::new(*inner as f64))
        }
        (Value::Decimal(inner), ColumnType::Float64) => {
            Value::Float64(crate::sql_types::F64::new(inner.to_f64()))
        }
        (Value::U64(inner), ColumnType::Float64) => {
            Value::Float64(crate::sql_types::F64::new(*inner as f64))
        }
        (Value::Text(inner), ColumnType::Float64) => {
            parse_float_text(inner).map_err(|_| DbError::SqlDatatypeMismatch {
                column: column.name.clone(),
            })?
        }
        (Value::Text(inner), ColumnType::Date) => {
            parse_date_text(inner).map_err(|_| DbError::SqlDatatypeMismatch {
                column: column.name.clone(),
            })?
        }
        (Value::Text(inner), ColumnType::Timestamp) => {
            parse_timestamp_text(inner).map_err(|_| DbError::SqlDatatypeMismatch {
                column: column.name.clone(),
            })?
        }
        (Value::I64(inner), ColumnType::Decimal) => {
            crate::sql_types::DecimalValue::new(*inner as i128, 0)
                .map(Value::Decimal)
                .map_err(|_| {
                    DbError::SqlNumericValueOutOfRange(format!(
                        "value does not fit SQL column {}",
                        column.name
                    ))
                })?
        }
        (Value::U64(inner), ColumnType::Decimal) => {
            crate::sql_types::DecimalValue::new(*inner as i128, 0)
                .map(Value::Decimal)
                .map_err(|_| {
                    DbError::SqlNumericValueOutOfRange(format!(
                        "value does not fit SQL column {}",
                        column.name
                    ))
                })?
        }
        (Value::Text(inner), ColumnType::Decimal) => {
            parse_decimal_text(inner).map_err(|_| DbError::SqlDatatypeMismatch {
                column: column.name.clone(),
            })?
        }
        (Value::Text(inner), ColumnType::Uuid) => crate::sql_types::UuidValue::parse(inner)
            .map(Value::Uuid)
            .map_err(|_| DbError::SqlDatatypeMismatch {
                column: column.name.clone(),
            })?,
        // PostgreSQL bytea hex input: \x followed by hex digit pairs.
        (Value::Text(inner), ColumnType::Bytes) => {
            let text = inner
                .strip_prefix("\\x")
                .ok_or_else(|| DbError::SqlDatatypeMismatch {
                    column: column.name.clone(),
                })?;
            decode_hex(text)?
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
            | (Value::Float64(_), ColumnType::Float64)
            | (Value::Date(_), ColumnType::Date)
            | (Value::Timestamp(_), ColumnType::Timestamp)
            | (Value::Decimal(_), ColumnType::Decimal)
            | (Value::Uuid(_), ColumnType::Uuid)
    );
    if matches!(value, Value::Null) && !column.nullable {
        return Err(DbError::SqlNotNullViolation {
            column: column.name.clone(),
        });
    }
    if !valid {
        return Err(DbError::SqlDatatypeMismatch {
            column: column.name.clone(),
        });
    }
    Ok(value)
}

/// Parse a float literal: decimal or scientific notation, plus infinity
/// and NaN spellings (PostgreSQL input syntax).
#[cfg(feature = "pgwire")]
pub(crate) fn parse_float_parameter(text: &str) -> Result<f64> {
    match parse_float_text(text)? {
        Value::Float64(inner) => Ok(inner.0),
        _ => unreachable!("float parser returns floats"),
    }
}

/// Parse a date literal into its Value form (shared by SQL coercion and
/// wire text parameters).
#[cfg(feature = "pgwire")]
pub(crate) fn parse_date_parameter(text: &str) -> Result<Value> {
    parse_date_text(text)
}

/// Parse a timestamp literal into its Value form.
#[cfg(feature = "pgwire")]
pub(crate) fn parse_timestamp_parameter(text: &str) -> Result<Value> {
    parse_timestamp_text(text)
}

/// Parse a decimal literal into its Value form.
#[cfg(feature = "pgwire")]
pub(crate) fn parse_decimal_parameter(text: &str) -> Result<Value> {
    parse_decimal_text(text)
}

pub(super) fn parse_float_text(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    let lower = trimmed.to_ascii_lowercase();
    let float = match lower.as_str() {
        "infinity" | "inf" => f64::INFINITY,
        "-infinity" | "-inf" => f64::NEG_INFINITY,
        "nan" => f64::NAN,
        _ => trimmed
            .parse::<f64>()
            .map_err(|_| DbError::InvalidState(format!("invalid float8 input: {trimmed}")))?,
    };
    Ok(Value::Float64(crate::sql_types::F64::new(float)))
}

/// Parse a date literal: strict `YYYY-MM-DD` in the proleptic Gregorian
/// calendar (PostgreSQL's ISO format without timezone conversion).
pub(super) fn parse_date_text(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    let bytes = trimmed.as_bytes();
    let malformed = || DbError::InvalidState(format!("invalid date input: {trimmed}"));
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(malformed());
    }
    let year: i32 = digits(&trimmed[..4])
        .ok_or_else(malformed)?
        .parse()
        .map_err(|_| malformed())?;
    let month: u32 = digits(&trimmed[5..7])
        .ok_or_else(malformed)?
        .parse()
        .map_err(|_| malformed())?;
    let day: u32 = digits(&trimmed[8..])
        .ok_or_else(malformed)?
        .parse()
        .map_err(|_| malformed())?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !is_gregorian_valid(year, month, day)
    {
        return Err(malformed());
    }
    let days = days_from_civil(year, month, day);
    Ok(Value::Date(crate::sql_types::DateValue::from_days(days)?))
}

/// Parse a timestamp literal: `YYYY-MM-DD[ T]HH:MM[:SS[.ffffff]]`, no
/// timezone. Fractional digits beyond microseconds truncate (PostgreSQL
/// rounds; OmenDB documents truncation for the first cut).
pub(super) fn parse_timestamp_text(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    let malformed = || DbError::InvalidState(format!("invalid timestamp input: {trimmed}"));
    let (date_part, time_part) = match trimmed.find(['T', ' ']) {
        Some(position) => (&trimmed[..position], &trimmed[position + 1..]),
        None => (trimmed, "00:00:00"),
    };
    if time_part.is_empty() {
        return Err(malformed());
    }
    let Value::Date(date) = parse_date_text(date_part)? else {
        return Err(malformed());
    };
    let mut micros = i64::from(date.0) * 86_400_000_000;

    let (clock, fraction) = match time_part.find('.') {
        Some(position) => (&time_part[..position], &time_part[position + 1..]),
        None => (time_part, ""),
    };
    let fields: Vec<&str> = clock.split(':').collect();
    if fields.len() < 2 || fields.len() > 3 {
        return Err(malformed());
    }
    let hour: i64 = fields[0].parse().map_err(|_| malformed())?;
    let minute: i64 = fields[1].parse().map_err(|_| malformed())?;
    let second: i64 = fields
        .get(2)
        .map_or(Ok(0), |s| s.parse::<i64>())
        .map_err(|_| malformed())?;
    if !(0..=24).contains(&hour) || !(0..=60).contains(&minute) || !(0..=60).contains(&second) {
        return Err(malformed());
    }
    micros += hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000;

    if !fraction.is_empty() {
        if fraction.len() > 6 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
            return Err(malformed());
        }
        let padded = format!("{fraction}000000");
        let scaled: i64 = padded[..6].parse().map_err(|_| malformed())?;
        micros += scaled;
    }
    Ok(Value::Timestamp(
        crate::sql_types::TimestampValue::from_micros(micros)?,
    ))
}

/// Parse a decimal literal with optional sign, integer part, fraction,
/// and optional exponent (PostgreSQL numeric input grammar).
pub(super) fn parse_decimal_text(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    let malformed = || DbError::InvalidState(format!("invalid numeric input: {trimmed}"));
    let (negative, unsigned) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let unsigned = unsigned.trim_start_matches('+');
    let (mantissa_digits, exponent) = match unsigned.find(['e', 'E']) {
        Some(position) => {
            let exponent: i32 = unsigned[position + 1..].parse().map_err(|_| malformed())?;
            (&unsigned[..position], exponent)
        }
        None => (unsigned, 0),
    };
    let (integer_part, fraction_part) = match mantissa_digits.find('.') {
        Some(position) => (
            &mantissa_digits[..position],
            &mantissa_digits[position + 1..],
        ),
        None => (mantissa_digits, ""),
    };
    if integer_part.is_empty() && fraction_part.is_empty() {
        return Err(malformed());
    }
    if !integer_part.bytes().all(|b| b.is_ascii_digit())
        || !fraction_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(malformed());
    }
    let digits = format!("{integer_part}{fraction_part}");
    let mantissa: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().map_err(|_| {
            DbError::SqlNumericValueOutOfRange(format!(
                "numeric input exceeds {0} digits",
                crate::sql_types::DECIMAL_MAX_PRECISION
            ))
        })?
    };
    // scale = fraction digits - positive exponent (digits the value keeps
    // after the decimal point once the exponent shifts the fraction).
    let fraction_len = i32::try_from(fraction_part.len()).map_err(|_| malformed())?;
    let scale = fraction_len.checked_sub(exponent).ok_or_else(malformed)?;
    let value = if scale < 0 {
        // Positive exponents beyond the fraction grow the integer part.
        let zeros = u32::try_from(-scale).map_err(|_| malformed())?;
        let grown = mantissa
            .checked_mul(10_i128.checked_pow(zeros).ok_or_else(malformed)?)
            .ok_or_else(|| {
                DbError::SqlNumericValueOutOfRange("numeric input out of range".to_owned())
            })?;
        crate::sql_types::DecimalValue::new(if negative { -grown } else { grown }, 0)?
    } else {
        crate::sql_types::DecimalValue::new(
            if negative { -mantissa } else { mantissa },
            u32::try_from(scale).map_err(|_| malformed())?,
        )?
    };
    Ok(Value::Decimal(value))
}

fn digits(text: &str) -> Option<&str> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        None
    } else {
        Some(text)
    }
}

/// Gregorian leap-year validity.
fn is_gregorian_valid(year: i32, month: u32, day: u32) -> bool {
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    day <= days_in_month
}

/// Days from 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's
/// civil-from-days inverse).
fn days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let year_of_era = year - era * 400;
    let month_shift = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * (month_shift as i32) + 2) / 5 + day as i32 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
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
        // Typed scalar keys hash their canonical bytes through the same
        // stable FNV-1a: UUID raw bits, date/timestamp LE integers, float
        // LE bits (NaN payload normalized), decimal normalized digits so
        // 1.50 and 1.5 collide as one primary key, as they are.
        Value::Uuid(uuid) => fnv1a_64(&uuid.0),
        Value::Date(crate::sql_types::DateValue(days)) => fnv1a_64(&days.to_le_bytes()),
        Value::Timestamp(crate::sql_types::TimestampValue(micros)) => {
            fnv1a_64(&micros.to_le_bytes())
        }
        Value::Float64(inner) => {
            let bits = if inner.0 == 0.0 {
                0.0f64.to_bits()
            } else {
                inner.0.to_bits()
            };
            fnv1a_64(&bits.to_le_bytes())
        }
        Value::Decimal(value) => {
            let normalized = value.normalized();
            let mut bytes = Vec::with_capacity(20);
            bytes.extend_from_slice(&normalized.mantissa.to_le_bytes());
            bytes.extend_from_slice(&normalized.scale.to_le_bytes());
            fnv1a_64(&bytes)
        }
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

#[cfg(test)]
mod typed_literal_tests {
    use super::*;
    use crate::relational::{ColumnDefinition, ColumnId};

    fn column(data_type: ColumnType) -> ColumnDefinition {
        ColumnDefinition {
            id: ColumnId(1),
            name: "c".to_owned(),
            data_type,
            nullable: true,
        }
    }

    #[test]
    fn parses_date_iso() {
        let v = coerce_value(Value::Text("2026-08-31".into()), &column(ColumnType::Date)).unwrap();
        let Value::Date(d) = v else { panic!() };
        // 2026-08-31 -> days since epoch; verify against Python: (date(2026,8,31)-date(1970,1,1)).days
        assert_eq!(d.0, 20696);
    }

    #[test]
    fn rejects_bad_dates() {
        for bad in [
            "2026-02-30",
            "2026-13-01",
            "2026-00-10",
            "not a date",
            "2026-1-1",
        ] {
            assert!(
                coerce_value(Value::Text(bad.into()), &column(ColumnType::Date)).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn parses_timestamp_with_time() {
        let v = coerce_value(
            Value::Text("2026-08-31 13:45:21.123456".into()),
            &column(ColumnType::Timestamp),
        )
        .unwrap();
        let Value::Timestamp(t) = v else { panic!() };
        assert_eq!(
            t.0,
            20696 * 86_400_000_000
                + 13 * 3_600_000_000
                + 45 * 60_000_000
                + 21 * 1_000_000
                + 123_456
        );
        // midnight form
        let v2 = coerce_value(
            Value::Text("2026-08-31".into()),
            &column(ColumnType::Timestamp),
        )
        .unwrap();
        let Value::Timestamp(t2) = v2 else { panic!() };
        assert_eq!(t2.0, 20696 * 86_400_000_000);
    }

    #[test]
    fn parses_decimal_variants() {
        // Exponent forms normalize like PostgreSQL: trailing fractional
        // zeros introduced by a positive exponent are dropped from dscale
        // (`1.5e2` -> `150`, scale 0, not `150.0`).
        let cases = [
            ("1.50", 150, 2),
            ("1.5", 15, 1),
            ("-2.75", -275, 2),
            ("42", 42, 0),
            ("0.001", 1, 3),
            ("1e3", 1000, 0),
            ("1.5e2", 150, 0),
            ("-1.5e-2", -15, 3),
        ];
        for (text, mantissa, scale) in cases {
            let v = coerce_value(Value::Text(text.into()), &column(ColumnType::Decimal)).unwrap();
            let Value::Decimal(d) = v else {
                panic!("{text}")
            };
            assert_eq!((d.mantissa, d.scale), (mantissa, scale), "{text}");
        }
        for bad in ["1.2.3", "abc", "", "1e", ". 5"] {
            assert!(
                coerce_value(Value::Text(bad.into()), &column(ColumnType::Decimal)).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn parses_float_and_uuid() {
        let v = coerce_value(Value::Text("2.5".into()), &column(ColumnType::Float64)).unwrap();
        let Value::Float64(f) = v else { panic!() };
        assert_eq!(f.0, 2.5);
        let nan = coerce_value(Value::Text("NaN".into()), &column(ColumnType::Float64)).unwrap();
        let Value::Float64(f2) = nan else { panic!() };
        assert!(f2.0.is_nan());
        let u = coerce_value(
            Value::Text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into()),
            &column(ColumnType::Uuid),
        )
        .unwrap();
        let Value::Uuid(_) = u else { panic!() };
    }

    #[test]
    fn integer_literal_into_decimal() {
        let v = coerce_value(Value::I64(42), &column(ColumnType::Decimal)).unwrap();
        let Value::Decimal(d) = v else { panic!() };
        assert_eq!((d.mantissa, d.scale), (42, 0));
    }
}
