//! Wire value encoding and parameter decoding: typed scalars with
//! PostgreSQL-faithful text and binary formats.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use postgres_types::IsNull;

use super::{map_db_error, pg_error};
use crate::Value;

/// Encode one logical value. Native types let pgwire's `ToSqlText` honor the
/// negotiated column format (text or binary) instead of forcing text.
/// Typed scalars use wire-faithful custom `ToSql` types so binary-format
/// clients (tokio-postgres, rust-postgres) decode exactly.
fn encode_value(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&Option::<String>::None),
        Value::Bool(inner) => encoder.encode_field(inner),
        Value::U64(inner) => encoder.encode_field(&(*inner as i64)),
        Value::I64(inner) => encoder.encode_field(inner),
        Value::Text(inner) => encoder.encode_field(inner),
        Value::Bytes(inner) => encoder.encode_field(inner),
        Value::Float64(inner) => encoder.encode_field(&PgFloat8(inner.0)),
        Value::Date(inner) => encoder.encode_field(&PgDate(inner.0)),
        Value::Timestamp(inner) => encoder.encode_field(&PgTimestamp(inner.0)),
        Value::Decimal(inner) => encoder.encode_field(&PgNumeric(*inner)),
        Value::Uuid(inner) => encoder.encode_field(&PgUuid(inner.0)),
    }
}

/// float8 with PostgreSQL text rendering (NaN, Infinity) and binary
/// passthrough.
#[derive(Clone, Copy, Debug)]
struct PgFloat8(f64);

impl pgwire::types::ToSqlText for PgFloat8 {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let text = if self.0.is_nan() {
            "NaN".to_owned()
        } else if self.0 == f64::INFINITY {
            "Infinity".to_owned()
        } else if self.0 == f64::NEG_INFINITY {
            "-Infinity".to_owned()
        } else {
            format!("{}", self.0)
        };
        out.extend_from_slice(text.as_bytes());
        Ok(IsNull::No)
    }
}

impl postgres_types::ToSql for PgFloat8 {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(&self.0.to_be_bytes());
        Ok(IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::FLOAT8 | Type::FLOAT4)
    }
    postgres_types::to_sql_checked!();
}

/// date: days since 2000-01-01, i32 big-endian.
#[derive(Clone, Copy, Debug)]
struct PgDate(i32);

impl pgwire::types::ToSqlText for PgDate {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(crate::sql_types::date_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl postgres_types::ToSql for PgDate {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // The wire carries days since 2000-01-01; the value stores days
        // since the Unix epoch.
        let wire = self.0 - crate::sql_types::POSTGRES_EPOCH_OFFSET_DAYS;
        out.extend_from_slice(&wire.to_be_bytes());
        Ok(IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::DATE)
    }
    postgres_types::to_sql_checked!();
}

/// timestamp without timezone: microseconds since 2000-01-01, i64
/// big-endian; text renders ISO `YYYY-MM-DD HH:MM:SS[.ffffff]`.
#[derive(Clone, Copy, Debug)]
struct PgTimestamp(i64);

impl pgwire::types::ToSqlText for PgTimestamp {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(crate::sql_types::timestamp_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl postgres_types::ToSql for PgTimestamp {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // The wire carries microseconds since 2000-01-01; the value
        // stores microseconds since the Unix epoch.
        let wire = self.0 - crate::sql_types::POSTGRES_EPOCH_OFFSET_MICROS;
        out.extend_from_slice(&wire.to_be_bytes());
        Ok(IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TIMESTAMP)
    }
    postgres_types::to_sql_checked!();
}

/// numeric: PostgreSQL's base-10000 group format
/// (`ndigits | weight | sign | dscale | groups`), all big-endian i16.
#[derive(Clone, Copy, Debug)]
struct PgNumeric(crate::sql_types::DecimalValue);

impl pgwire::types::ToSqlText for PgNumeric {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(crate::sql_types::decimal_text(&self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl postgres_types::ToSql for PgNumeric {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // PostgreSQL numeric: value = sum(groups[i] * 10^(4*(weight - i))).
        // The weight places the first group; groups list only nonzero
        // span (leading/trailing zero groups trimmed; dscale still
        // carries the stored scale).
        let (weight, groups) = numeric_groups_weighted(&self.0)?;
        let ndigits = u16::try_from(groups.len()).map_err(|_| "too many digit groups")?;
        out.extend_from_slice(&ndigits.to_be_bytes());
        out.extend_from_slice(&weight.to_be_bytes());
        out.extend_from_slice(
            &u16::from(self.0.mantissa < 0)
                .checked_mul(0x4000)
                .expect("sign")
                .to_be_bytes(),
        );
        // dscale is an i16 field; the precision bound keeps it small.
        let dscale = u16::try_from(self.0.scale).map_err(|_| "numeric scale overflow")?;
        out.extend_from_slice(&dscale.to_be_bytes());
        for group in groups {
            out.extend_from_slice(&group.to_be_bytes());
        }
        Ok(IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
    postgres_types::to_sql_checked!();
}

/// uuid: 16 raw bytes; text renders canonical hyphenated hex.
#[derive(Clone, Copy, Debug)]
struct PgUuid([u8; 16]);

impl pgwire::types::ToSqlText for PgUuid {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(crate::sql_types::UuidValue(self.0).format().as_bytes());
        Ok(IsNull::No)
    }
}

impl postgres_types::ToSql for PgUuid {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::UUID)
    }
    postgres_types::to_sql_checked!();
}

// ---------------------------------------------------------------------------
// Wire-format helpers for the typed scalars. Formats were pinned against a
// live PostgreSQL 18 via COPY BINARY probes (see docs/pgwire-compatibility).
// ---------------------------------------------------------------------------

/// Base-10000 groups with their PostgreSQL weight: value =
/// sum(groups[i] * 10^(4*(weight - i))). Leading zero groups above the
/// first nonzero group and trailing zero groups below the last nonzero
/// group are trimmed (PG emits no padding groups; dscale carries the
/// stored scale separately).
fn numeric_groups_weighted(
    value: &crate::sql_types::DecimalValue,
) -> std::result::Result<(i16, Vec<u16>), String> {
    // PostgreSQL numeric: value = sum(groups[i] * 10^(4*(weight - i))).
    // Groups align to the decimal point (base-10000 grid), not to the
    // raw mantissa: 19.99 -> [19, 9900] at weight 0, while the mantissa
    // 1999 alone would group as [1999]. Trailing all-zero groups are
    // trimmed and the weight compensates; the first group always holds
    // the most significant nonzero digit.
    let mantissa = value.mantissa.unsigned_abs();
    if mantissa == 0 {
        // PostgreSQL emits zero as ndigits 0, weight 0.
        return Ok((0, Vec::new()));
    }
    let scale = value.scale;
    // Pad the mantissa digits so the last group boundary lands at or
    // below the least significant digit: structural zeros fill the
    // positions under 10^-scale down to the group base.
    let pad = (4 - scale % 4) % 4;
    let mut full = mantissa.to_string();
    for _ in 0..pad {
        full.push('0');
    }
    // Chunk from the right in 4s: the rightmost chunk boundary is the
    // bottom group base; the leftmost chunk may be short (unpadded top
    // group, matching PostgreSQL's no-leading-zero-groups rule).
    let mut groups: Vec<u16> = full
        .as_bytes()
        .rchunks(4)
        .map(|chunk| {
            std::str::from_utf8(chunk)
                .expect("digits")
                .parse::<u16>()
                .expect("group < 10000")
        })
        .collect();
    groups.reverse();
    let untrimmed = groups.len();
    while groups.last().is_some_and(|group| *group == 0) {
        groups.pop();
    }
    // Bottom group base = 4*floor(-scale/4); each trimmed group shifts
    // the first group's base up by one slot.
    let bottom_slots = -(scale.div_ceil(4) as i32);
    let weight = bottom_slots + (groups.len() as i32 - 1) + (untrimmed - groups.len()) as i32;
    let weight = i16::try_from(weight).map_err(|_| "numeric weight overflow".to_owned())?;
    Ok((weight, groups))
}

pub(super) fn value_type(value: Option<&Value>) -> Type {
    match value {
        Some(Value::Bool(_)) => Type::BOOL,
        Some(Value::U64(_) | Value::I64(_)) => Type::INT8,
        Some(Value::Text(_)) => Type::TEXT,
        Some(Value::Bytes(_)) => Type::BYTEA,
        Some(Value::Float64(_)) => Type::FLOAT8,
        Some(Value::Date(_)) => Type::DATE,
        Some(Value::Timestamp(_)) => Type::TIMESTAMP,
        Some(Value::Decimal(_)) => Type::NUMERIC,
        Some(Value::Uuid(_)) => Type::UUID,
        _ => Type::TEXT,
    }
}

pub(super) fn fields_from_result(
    result: &crate::SqlResult,
    format: &pgwire::api::portal::Format,
) -> Arc<Vec<FieldInfo>> {
    let sample = result.rows.first();
    Arc::new(
        result
            .columns
            .iter()
            .enumerate()
            .map(|(position, column)| {
                // Static projection types come first: they hold even when
                // no row matches (a parameterized describe probe runs with
                // benign values). Sample inference is the fallback for
                // genuinely dynamic shapes (set-union arms, subqueries).
                let ty = column
                    .column_type
                    .map(column_type_to_pg)
                    .unwrap_or_else(|| value_type(sample.and_then(|row| row.get(position))));
                let field_format = format.format_for(position);
                FieldInfo::new(column.name.clone(), None, None, ty, field_format)
            })
            .collect::<Vec<_>>(),
    )
}

fn estimated_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 5,
        Value::U64(_) | Value::I64(_) => 20,
        Value::Text(value) => value.len(),
        Value::Bytes(value) => value.len(),
        Value::Float64(_) => 24,
        Value::Date(_) => 13,
        Value::Timestamp(_) => 30,
        Value::Decimal(_) => 40,
        Value::Uuid(_) => 40,
    }
}

fn estimated_result_payload_bytes(result: &crate::SqlResult) -> usize {
    result.rows.iter().fold(0usize, |total, row| {
        row.iter().fold(total, |total, value| {
            total.saturating_add(estimated_value_bytes(value))
        })
    })
}

pub(super) fn check_result_payload(
    result: &crate::SqlResult,
    max_result_bytes: Option<usize>,
) -> crate::Result<()> {
    let estimated_bytes = estimated_result_payload_bytes(result);
    if let Some(limit) = max_result_bytes
        && estimated_bytes > limit
    {
        return Err(crate::DbError::ResourceLimitExceeded(format!(
            "result payload is estimated at {estimated_bytes} bytes, exceeding the configured limit of {limit}"
        )));
    }
    Ok(())
}

pub(super) fn encode_response_with_format(
    result: crate::SqlResult,
    format: &pgwire::api::portal::Format,
    max_result_bytes: Option<usize>,
) -> PgWireResult<Response> {
    check_result_payload(&result, max_result_bytes).map_err(map_db_error)?;
    if result.rows.is_empty() && result.columns.is_empty() {
        return Ok(Response::Execution(
            Tag::new("OK").with_rows(result.affected_rows),
        ));
    }
    let schema = fields_from_result(&result, format);
    let rows = result.rows.clone();
    let data_row_stream = stream::iter(rows).then(move |row| {
        let schema = schema.clone();
        async move {
            let mut encoder = DataRowEncoder::new(schema);
            for value in &row {
                encode_value(&mut encoder, value)?;
            }
            Ok(encoder.take_row())
        }
    });
    Ok(Response::Query(QueryResponse::new(
        fields_from_result(&result, format),
        data_row_stream,
    )))
}

pub(super) fn column_type_to_pg(column_type: crate::ColumnType) -> Type {
    match column_type {
        crate::ColumnType::Bool => Type::BOOL,
        crate::ColumnType::U64 | crate::ColumnType::I64 => Type::INT8,
        crate::ColumnType::Text => Type::TEXT,
        crate::ColumnType::Bytes => Type::BYTEA,
        crate::ColumnType::Float64 => Type::FLOAT8,
        crate::ColumnType::Date => Type::DATE,
        crate::ColumnType::Timestamp => Type::TIMESTAMP,
        crate::ColumnType::Decimal => Type::NUMERIC,
        crate::ColumnType::Uuid => Type::UUID,
    }
}

fn decode_parameter(raw: Option<&[u8]>, binary: bool, declared: &Type) -> PgWireResult<Value> {
    let declared = declared.clone();
    let Some(raw) = raw else {
        return Ok(Value::Null);
    };
    if binary {
        return Ok(match declared {
            Type::BOOL => Value::Bool(!raw.is_empty() && raw[0] != 0),
            Type::INT2 => match <[u8; 2]>::try_from(raw) {
                Ok(inner) => Value::I64(i64::from(i16::from_be_bytes(inner))),
                Err(_) => return Err(pg_error("22P02", "malformed int2 parameter".to_owned())),
            },
            Type::INT4 => match <[u8; 4]>::try_from(raw) {
                Ok(inner) => Value::I64(i64::from(i32::from_be_bytes(inner))),
                Err(_) => return Err(pg_error("22P02", "malformed int4 parameter".to_owned())),
            },
            Type::INT8 => match <[u8; 8]>::try_from(raw) {
                Ok(inner) => Value::I64(i64::from_be_bytes(inner)),
                Err(_) => return Err(pg_error("22P02", "malformed int8 parameter".to_owned())),
            },
            Type::BYTEA => Value::Bytes(raw.to_vec()),
            Type::FLOAT8 => match <[u8; 8]>::try_from(raw) {
                Ok(inner) => Value::Float64(crate::sql_types::F64::new(f64::from_be_bytes(inner))),
                Err(_) => return Err(pg_error("22P02", "malformed float8 parameter".to_owned())),
            },
            Type::FLOAT4 => match <[u8; 4]>::try_from(raw) {
                Ok(inner) => {
                    Value::Float64(crate::sql_types::F64::new(f32::from_be_bytes(inner) as f64))
                }
                Err(_) => return Err(pg_error("22P02", "malformed float4 parameter".to_owned())),
            },
            Type::DATE => match <[u8; 4]>::try_from(raw) {
                Ok(inner) => Value::Date(crate::sql_types::DateValue(
                    i32::from_be_bytes(inner) + crate::sql_types::POSTGRES_EPOCH_OFFSET_DAYS,
                )),
                Err(_) => return Err(pg_error("22P02", "malformed date parameter".to_owned())),
            },
            Type::TIMESTAMP => match <[u8; 8]>::try_from(raw) {
                Ok(inner) => Value::Timestamp(crate::sql_types::TimestampValue(
                    i64::from_be_bytes(inner) + crate::sql_types::POSTGRES_EPOCH_OFFSET_MICROS,
                )),
                Err(_) => {
                    return Err(pg_error(
                        "22P02",
                        "malformed timestamp parameter".to_owned(),
                    ));
                }
            },
            Type::NUMERIC => match decode_numeric_parameter(raw) {
                Ok(value) => Value::Decimal(value),
                Err(reason) => return Err(pg_error("22P02", reason)),
            },
            Type::UUID => match <[u8; 16]>::try_from(raw) {
                Ok(inner) => Value::Uuid(crate::sql_types::UuidValue(inner)),
                Err(_) => return Err(pg_error("22P02", "malformed uuid parameter".to_owned())),
            },
            _ => Value::Text(String::from_utf8_lossy(raw).into_owned()),
        });
    }
    let text = String::from_utf8_lossy(raw).into_owned();
    Ok(match declared {
        Type::BOOL => Value::Bool(matches!(text.as_str(), "t" | "true" | "1")),
        Type::INT2 | Type::INT4 | Type::INT8 => match text.parse::<i64>() {
            Ok(inner) => Value::I64(inner),
            Err(_) => {
                return Err(pg_error(
                    "22P02",
                    format!("invalid integer parameter: {text}"),
                ));
            }
        },
        Type::BYTEA => Value::Bytes(text.into_bytes()),
        // Text-format typed parameters run the same input grammar as SQL
        // literals, so 'NaN', 'Infinity', ISO dates, and exponent-form
        // numerics all parse.
        Type::FLOAT8 | Type::FLOAT4 => match crate::sql::typed_input::float(&text) {
            Ok(value) => Value::Float64(crate::sql_types::F64::new(value)),
            Err(_) => {
                return Err(pg_error(
                    "22P02",
                    format!("invalid float parameter: {text}"),
                ));
            }
        },
        Type::DATE => match crate::sql::typed_input::date(&text) {
            Ok(value) => value,
            Err(_) => return Err(pg_error("22P02", format!("invalid date parameter: {text}"))),
        },
        Type::TIMESTAMP => match crate::sql::typed_input::timestamp(&text) {
            Ok(value) => value,
            Err(_) => {
                return Err(pg_error(
                    "22P02",
                    format!("invalid timestamp parameter: {text}"),
                ));
            }
        },
        Type::NUMERIC => match crate::sql::typed_input::decimal(&text) {
            Ok(value) => value,
            Err(_) => {
                return Err(pg_error(
                    "22P02",
                    format!("invalid numeric parameter: {text}"),
                ));
            }
        },
        Type::UUID => match crate::sql_types::UuidValue::parse(&text) {
            Ok(value) => Value::Uuid(value),
            Err(_) => return Err(pg_error("22P02", format!("invalid uuid parameter: {text}"))),
        },
        _ => Value::Text(text),
    })
}

/// Decode a binary-format PostgreSQL numeric parameter (base-10000
/// groups) into a decimal value.
fn decode_numeric_parameter(
    raw: &[u8],
) -> std::result::Result<crate::sql_types::DecimalValue, String> {
    if raw.len() < 8 || !raw.len().is_multiple_of(2) {
        return Err("malformed numeric parameter width".to_owned());
    }
    let be16 = |offset: usize| u16::from_be_bytes([raw[offset], raw[offset + 1]]);
    let ndigits = be16(0) as usize;
    let weight = i16::from_be_bytes([raw[2], raw[3]]) as i64;
    let sign = be16(4);
    let dscale = be16(6) as u32;
    if raw.len() != 8 + ndigits * 2 {
        return Err("numeric parameter group count mismatch".to_owned());
    }
    if dscale > crate::sql_types::DECIMAL_MAX_PRECISION {
        return Err(format!(
            "numeric parameter scale {dscale} exceeds the maximum"
        ));
    }
    let mut groups = Vec::with_capacity(ndigits);
    for index in 0..ndigits {
        groups.push(u64::from(be16(8 + index * 2)));
    }
    // value = sum(groups[i] * 10000^(weight - i)); mantissa = that
    // scaled to 10^dscale.
    let mut mantissa: i128 = 0;
    for (index, group) in groups.iter().enumerate() {
        let exponent = weight - index as i64; // groups at 10^(4*exponent)
        if exponent >= 0 {
            let factor = 10_i128
                .checked_pow(
                    u32::try_from(4 * exponent).map_err(|_| "numeric too large".to_owned())?,
                )
                .ok_or_else(|| "numeric too large".to_owned())?;
            mantissa = mantissa
                .checked_add(i128::from(*group) * factor)
                .ok_or_else(|| "numeric too large".to_owned())?;
        } else {
            let negative_exponent = -4 * exponent;
            if negative_exponent <= dscale as i64 {
                // Keep digits within the dscale window.
                let shift = dscale as i64 - negative_exponent;
                let factor = 10_i128
                    .checked_pow(u32::try_from(shift).map_err(|_| "numeric too large".to_owned())?)
                    .ok_or_else(|| "numeric too large".to_owned())?;
                mantissa = mantissa
                    .checked_add(i128::from(*group) * factor)
                    .ok_or_else(|| "numeric too large".to_owned())?;
            }
            // Groups below the dscale window are dropped (client-side
            // dscale promises those digits are zero in practice).
        }
    }
    if sign == 0x4000 {
        mantissa = -mantissa;
    }
    crate::sql_types::DecimalValue::new(mantissa, dscale).map_err(|error| error.to_string())
}

pub(super) fn decode_parameters(
    parameters: &[Option<Vec<u8>>],
    parameter_format: &pgwire::api::portal::Format,
    resolved: &[Type],
) -> PgWireResult<Vec<Value>> {
    (0..parameters.len())
        .map(|index| {
            decode_parameter(
                parameters.get(index).and_then(|raw| raw.as_deref()),
                parameter_format.is_binary(index),
                resolved.get(index).unwrap_or(&Type::TEXT),
            )
        })
        .collect()
}
