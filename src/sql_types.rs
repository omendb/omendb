//! Scalar value types for the relational layer.
//!
//! Storage keeps typed values opaque to SeerDB; this module defines the
//! SQL-facing scalar types beyond the legacy integer/text/bytes set:
//! Float64, Date, Timestamp (no timezone), Decimal (fixed-point), and
//! UUID. Each type is a distinct column type with a total order,
//! canonical encoding, and a wire representation matching PostgreSQL.

use crate::{DbError, Result};

/// Microseconds between 1970-01-01 and 2000-01-01 (PostgreSQL's epoch).
pub const POSTGRES_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Days between 1970-01-01 and 2000-01-01 (PostgreSQL's date epoch).
pub const POSTGRES_EPOCH_OFFSET_DAYS: i32 = 10_957;

/// Representable date range, days from 1970-01-01 (4714 BC .. 5874897 AD).
pub const MIN_DATE_DAYS: i32 = -2_441_474;
pub const MAX_DATE_DAYS: i32 = 2_145_148_992;

/// Representable timestamp range, microseconds from 1970-01-01
/// (0001-01-01 00:00:00 .. 9999-12-31 23:59:59.999999).
pub const MIN_TIMESTAMP_MICROS: i64 = -62_135_596_800_000_000;
pub const MAX_TIMESTAMP_MICROS: i64 = 253_402_300_799_999_999;

/// Maximum total significant digits in a decimal value.
pub const DECIMAL_MAX_PRECISION: u32 = 38;

/// An IEEE-754 binary64 value with the SQL (PostgreSQL) total order:
/// NaN is a real value, greatest of all and equal to itself, and
/// negative zero equals positive zero.
#[derive(Clone, Copy, Debug, Default)]
pub struct F64(pub f64);

impl F64 {
    /// Wraps a float. NaN is accepted as a value with the greatest order
    /// (PostgreSQL semantics for `'NaN'::float8`).
    #[must_use]
    pub fn new(value: f64) -> Self {
        Self(value)
    }

    /// Canonical hash input: zero signs and NaN payloads normalized so
    /// equal values hash equal.
    fn hash_bits(&self) -> u64 {
        if self.0 == 0.0 {
            0.0f64.to_bits()
        } else if self.0.is_nan() {
            0x7ff8_0000_0000_0000
        } else {
            self.0.to_bits()
        }
    }
}

impl PartialEq for F64 {
    fn eq(&self, other: &Self) -> bool {
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => true,
            (true, false) | (false, true) => false,
            (false, false) => self.0 == other.0,
        }
    }
}

impl Eq for F64 {}

impl PartialOrd for F64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let (left, right) = (self.0, other.0);
        match (left.is_nan(), right.is_nan()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            (false, false) => {
                if left == 0.0 && right == 0.0 {
                    std::cmp::Ordering::Equal
                } else {
                    left.total_cmp(&right)
                }
            }
        }
    }
}

impl std::hash::Hash for F64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash_bits().hash(state);
    }
}

/// A date in the proleptic Gregorian calendar, days from 1970-01-01.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DateValue(pub i32);

impl DateValue {
    /// Construct from days since the Unix epoch, validating the range.
    pub fn from_days(days: i32) -> Result<Self> {
        if !(MIN_DATE_DAYS..=MAX_DATE_DAYS).contains(&days) {
            return Err(DbError::SqlNumericValueOutOfRange(format!(
                "date value {days} days is outside the representable range"
            )));
        }
        Ok(Self(days))
    }
}

/// A timestamp without timezone, microseconds from 1970-01-01 00:00:00.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimestampValue(pub i64);

impl TimestampValue {
    /// Construct from microseconds since the Unix epoch, validating the
    /// representable range.
    pub fn from_micros(micros: i64) -> Result<Self> {
        if !(MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS).contains(&micros) {
            return Err(DbError::SqlNumericValueOutOfRange(format!(
                "timestamp value {micros} is outside the representable range"
            )));
        }
        Ok(Self(micros))
    }
}

/// A fixed-point decimal: value = mantissa / 10^scale, with at most
/// [`DECIMAL_MAX_PRECISION`] total significant digits (an i128-backed
/// bound; PostgreSQL's numeric is wider, and this is OmenDB's documented
/// limit). The scale is carried per value so mixed-scale arithmetic can
/// rescale, and canonical use normalizes trailing zeros so 1.50 == 1.5.
#[derive(Clone, Copy, Debug)]
pub struct DecimalValue {
    pub mantissa: i128,
    pub scale: u32,
}

impl DecimalValue {
    /// Construct with checked digit width: significant digits plus scale
    /// must fit the precision bound.
    pub fn new(mantissa: i128, scale: u32) -> Result<Self> {
        if scale > DECIMAL_MAX_PRECISION {
            return Err(DbError::SqlNumericValueOutOfRange(format!(
                "decimal scale {scale} exceeds the maximum {DECIMAL_MAX_PRECISION}"
            )));
        }
        if Self::digit_count(mantissa).saturating_add(scale) > DECIMAL_MAX_PRECISION {
            return Err(DbError::SqlNumericValueOutOfRange(format!(
                "decimal value needs more than {DECIMAL_MAX_PRECISION} digits"
            )));
        }
        Ok(Self { mantissa, scale })
    }

    /// Number of decimal digits in a mantissa's absolute value (zero
    /// has zero digits).
    #[must_use]
    pub fn digit_count(mantissa: i128) -> u32 {
        let mut digits = 0_u32;
        let mut value = mantissa.unsigned_abs();
        while value > 0 {
            value /= 10;
            digits += 1;
        }
        digits
    }

    /// Remove trailing zero digits from the mantissa, reducing the
    /// scale, so equal values share one canonical form.
    #[must_use]
    pub fn normalized(&self) -> Self {
        let mut mantissa = self.mantissa;
        let mut scale = self.scale;
        while scale > 0 && mantissa % 10 == 0 {
            mantissa /= 10;
            scale -= 1;
        }
        Self { mantissa, scale }
    }

    /// Rescale to a target scale, rounding half-away-from-zero when the
    /// target is coarser. Errors when the result would exceed the
    /// precision bound.
    pub fn rescale(&self, target: u32) -> Result<Self> {
        if target >= self.scale {
            let factor = 10_i128
                .checked_pow(target - self.scale)
                .ok_or_else(|| decimal_overflow("rescale"))?;
            let mantissa = self
                .mantissa
                .checked_mul(factor)
                .ok_or_else(|| decimal_overflow("rescale"))?;
            return Self::new(mantissa, target);
        }
        let divisor = 10_i128
            .checked_pow(self.scale - target)
            .ok_or_else(|| decimal_overflow("rescale"))?;
        let quotient = self.mantissa / divisor;
        let remainder = self.mantissa % divisor;
        let half = divisor / 2;
        let rounded = if remainder.abs() >= half {
            quotient + self.mantissa.signum()
        } else {
            quotient
        };
        Self::new(rounded, target)
    }
}

fn decimal_overflow(what: &str) -> DbError {
    DbError::SqlNumericValueOutOfRange(format!("decimal {what} overflow"))
}

impl PartialEq for DecimalValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for DecimalValue {}

impl PartialOrd for DecimalValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DecimalValue {
    /// Compare by aligning the two scales. All valid mantissas are below
    /// 10^38 < i128::MAX, so an overflowing alignment product implies the
    /// smaller-scale side exceeds the other side in magnitude, which
    /// resolves the order by sign without losing totality.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.scale == other.scale {
            return self.mantissa.cmp(&other.mantissa);
        }
        let self_is_smaller_scale = self.scale < other.scale;
        let (smaller, larger) = if self_is_smaller_scale {
            (self, other)
        } else {
            (other, self)
        };
        // Scale differences are bounded by DECIMAL_MAX_PRECISION, and
        // 10^38 fits i128, so the pow itself cannot overflow.
        let factor = 10_i128
            .checked_pow(larger.scale - smaller.scale)
            .expect("scale difference bounded by precision");
        match smaller.mantissa.checked_mul(factor) {
            Some(aligned) => {
                let (left, right) = if self_is_smaller_scale {
                    (aligned, larger.mantissa)
                } else {
                    (larger.mantissa, aligned)
                };
                left.cmp(&right)
            }
            // |smaller.mantissa * factor| > i128::MAX > 10^38 > any
            // valid |larger.mantissa|: the smaller-scale value wins by
            // its sign.
            None => {
                if smaller.mantissa >= 0 {
                    if self_is_smaller_scale {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                } else if self_is_smaller_scale {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
        }
    }
}

impl std::hash::Hash for DecimalValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let normalized = self.normalized();
        normalized.mantissa.hash(state);
        normalized.scale.hash(state);
    }
}

/// A UUID (RFC 4122, arbitrary version): 128 stored bits.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UuidValue(pub [u8; 16]);

impl UuidValue {
    /// Parse the canonical 8-4-4-4-12 hex text form (case-insensitive;
    /// hyphens optional positionally only in the standard placement).
    pub fn parse(text: &str) -> Result<Self> {
        let hex: Vec<char> = text.chars().filter(|c| *c != '-').collect();
        if hex.len() != 32 {
            return Err(DbError::InvalidState(format!(
                "invalid uuid text representation: {text}"
            )));
        }
        let mut bytes = [0u8; 16];
        for (index, pair) in hex.chunks(2).enumerate() {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    /// Render the canonical 8-4-4-4-12 lowercase hex form.
    #[must_use]
    pub fn format(&self) -> String {
        let mut text = String::with_capacity(36);
        for (index, byte) in self.0.iter().enumerate() {
            if matches!(index, 4 | 6 | 8 | 10) {
                text.push('-');
            }
            text.push_str(&format!("{byte:02x}"));
        }
        text
    }
}

fn hex_digit(digit: char) -> Result<u8> {
    match digit {
        '0'..='9' => Ok(digit as u8 - b'0'),
        'a'..='f' => Ok(digit as u8 - b'a' + 10),
        'A'..='F' => Ok(digit as u8 - b'A' + 10),
        _ => Err(DbError::InvalidState(
            "invalid uuid text representation".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f64_order_matches_sql_semantics() {
        use std::cmp::Ordering::*;
        assert_eq!(F64(1.5).cmp(&F64(2.5)), Less);
        assert_eq!(F64(-0.0), F64(0.0));
        assert_eq!(F64(f64::INFINITY).cmp(&F64(f64::MAX)), Greater);
        assert_eq!(F64(f64::NEG_INFINITY).cmp(&F64(f64::MIN)), Less);
        // NaN is greatest and equal to itself.
        assert_eq!(F64(f64::NAN).cmp(&F64(f64::INFINITY)), Greater);
        assert_eq!(F64(f64::NAN).cmp(&F64(f64::NAN)), Equal);
        assert_eq!(F64(f64::NAN), F64(f64::NAN));
    }

    #[test]
    fn f64_hash_treats_equal_values_equally() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let hash = |value: &F64| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&F64(-0.0)), hash(&F64(0.0)));
        assert_eq!(hash(&F64(f64::NAN)), hash(&F64(f64::NAN)));
        assert_ne!(hash(&F64(1.0)), hash(&F64(2.0)));
    }

    #[test]
    fn decimal_comparison_and_normalization() {
        let a = DecimalValue::new(150, 2).unwrap().normalized();
        let b = DecimalValue::new(15, 1).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), std::cmp::Ordering::Equal);

        assert!(DecimalValue::new(123, 2).unwrap() < DecimalValue::new(124, 2).unwrap());
        assert!(DecimalValue::new(-5, 0).unwrap() < DecimalValue::new(0, 0).unwrap());

        // Mixed-scale comparison where alignment does not overflow.
        assert!(DecimalValue::new(15, 1).unwrap() > DecimalValue::new(149, 2).unwrap());
        // Alignment overflow: a 38-digit mantissa at scale 0 against a
        // scale-37 value; the alignment product overflows i128 and the
        // overflow path must order by sign.
        let huge = DecimalValue::new(9_999_999_999_999_999_999_999_999_999_999_999_999, 0).unwrap();
        let tiny = DecimalValue::new(5, 37).unwrap();
        assert!(huge > tiny);
        let tiny_negative = DecimalValue::new(-5, 37).unwrap();
        assert!(tiny_negative < huge);
    }

    #[test]
    fn decimal_hash_normalizes_trailing_zeros() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let hash = |value: &DecimalValue| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(
            hash(&DecimalValue::new(150, 2).unwrap()),
            hash(&DecimalValue::new(15, 1).unwrap())
        );
    }

    #[test]
    fn decimal_rescale_rounds_half_away_from_zero() {
        let value = DecimalValue::new(125, 2).unwrap();
        assert_eq!(value.rescale(1).unwrap().mantissa, 13); // 1.25 -> 1.3
        let down = DecimalValue::new(124, 2).unwrap();
        assert_eq!(down.rescale(1).unwrap().mantissa, 12); // 1.24 -> 1.2
        let negative = DecimalValue::new(-125, 2).unwrap();
        assert_eq!(negative.rescale(1).unwrap().mantissa, -13); // -1.25 -> -1.3
        // Upscaling is exact.
        let up = DecimalValue::new(12, 1).unwrap();
        let rescaled = up.rescale(3).unwrap();
        assert_eq!((rescaled.mantissa, rescaled.scale), (1_200, 3));
        // Precision bound rejects 39-digit values.
        assert!(DecimalValue::new(10_i128.pow(38), 0).is_err());
    }

    #[test]
    fn uuid_roundtrips_canonical_text() {
        let text = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        let parsed = UuidValue::parse(text).unwrap();
        assert_eq!(parsed.format(), text);
        let uppercase = "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11";
        assert_eq!(
            UuidValue::parse(uppercase).unwrap(),
            parsed,
            "uppercase hex parses to the same value"
        );
        assert!(UuidValue::parse("not-a-uuid").is_err());
        assert!(UuidValue::parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1").is_err());
    }

    #[test]
    fn date_and_timestamp_bounds_are_enforced() {
        assert!(TimestampValue::from_micros(0).is_ok());
        assert!(TimestampValue::from_micros(MIN_TIMESTAMP_MICROS).is_ok());
        assert!(TimestampValue::from_micros(MAX_TIMESTAMP_MICROS).is_ok());
        assert!(TimestampValue::from_micros(MIN_TIMESTAMP_MICROS - 1).is_err());
        assert!(TimestampValue::from_micros(MAX_TIMESTAMP_MICROS + 1).is_err());

        assert!(DateValue::from_days(0).is_ok());
        assert!(DateValue::from_days(MAX_DATE_DAYS).is_ok());
        assert!(DateValue::from_days(MIN_DATE_DAYS).is_ok());
        assert!(DateValue::from_days(MIN_DATE_DAYS - 1).is_err());
        assert!(DateValue::from_days(MAX_DATE_DAYS + 1).is_err());
    }
}
