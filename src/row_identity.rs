use std::collections::BTreeSet;

use crate::model::Key;
use crate::relational::{ColumnId, TableId, Value};
use crate::{DbError, Result};

const MAGIC: [u8; 4] = *b"DBRI";
const VERSION: u8 = 1;
const MAX_COMPONENTS: usize = 256;

/// Canonical logical identity for one relational row.
///
/// This primitive is intentionally separate from the legacy fixed-width
/// [`crate::Key`]. Both selected relational backends use it at their byte-KV
/// boundary for catalog-owned composite primary keys and for their legacy
/// single-key envelope. Callers must validate values against the table
/// catalog before creating an identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowIdentity {
    table: TableId,
    columns: Vec<ColumnId>,
    values: Vec<Value>,
}

impl RowIdentity {
    /// Creates an identity from the table and its ordered primary-key components.
    ///
    /// Column order is part of the identity. `NULL` is rejected because a
    /// primary-key component must identify one row rather than an unknown
    /// value.
    pub fn new(table: TableId, columns: Vec<ColumnId>, values: Vec<Value>) -> Result<Self> {
        if columns.is_empty() {
            return Err(DbError::InvalidState(
                "row identity requires at least one primary-key column".to_owned(),
            ));
        }
        if columns.len() > MAX_COMPONENTS {
            return Err(DbError::InvalidState(format!(
                "row identity has too many primary-key columns: {}",
                columns.len()
            )));
        }
        if columns.len() != values.len() {
            return Err(DbError::InvalidState(format!(
                "row identity has {} columns but {} values",
                columns.len(),
                values.len()
            )));
        }
        let mut distinct = BTreeSet::new();
        if columns.iter().any(|column| !distinct.insert(*column)) {
            return Err(DbError::InvalidState(
                "row identity repeats a primary-key column".to_owned(),
            ));
        }
        if values.iter().any(Value::is_null) {
            return Err(DbError::InvalidState(
                "row identity cannot contain NULL".to_owned(),
            ));
        }
        Ok(Self {
            table,
            columns,
            values,
        })
    }

    /// Returns the table identified by this key.
    #[must_use]
    pub fn table(&self) -> TableId {
        self.table
    }

    /// Returns primary-key columns in their canonical catalog order.
    #[must_use]
    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
    }

    /// Returns primary-key values in the same order as [`Self::columns`].
    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Encodes the identity with explicit type tags and length-delimited values.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(&self.table.0.to_le_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.columns.len())
                .map_err(|_| DbError::InvalidState("too many identity columns".to_owned()))?
                .to_le_bytes(),
        );
        for (column, value) in self.columns.iter().zip(&self.values) {
            bytes.extend_from_slice(&column.0.to_le_bytes());
            value.encode(&mut bytes)?;
        }
        Ok(bytes)
    }

    /// Decodes and validates one canonical identity, rejecting unknown or
    /// malformed versions as durable corruption.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor { bytes, offset: 0 };
        if cursor.take(4)? != MAGIC {
            return Err(cursor.corrupt("unknown row-identity format"));
        }
        if cursor.byte()? != VERSION {
            return Err(cursor.corrupt("unsupported row-identity version"));
        }
        let table = TableId(cursor.u64()?);
        let count = cursor.u32()? as usize;
        if count == 0 || count > MAX_COMPONENTS {
            return Err(cursor.corrupt("invalid row-identity component count"));
        }
        let mut columns = Vec::with_capacity(count);
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            columns.push(ColumnId(cursor.u16()?));
            values.push(cursor.value()?);
        }
        cursor.finish()?;
        Self::new(table, columns, values).map_err(|error| match error {
            DbError::InvalidState(reason) => DbError::Corruption {
                artifact: "row identity",
                reason,
            },
            other => other,
        })
    }
}

pub(crate) fn encode_legacy_key(table: TableId, key: Key) -> Result<Vec<u8>> {
    RowIdentity::new(table, vec![ColumnId(0)], vec![Value::Bytes(key.0.to_vec())])?.encode()
}

pub(crate) fn decode_legacy_key(table: TableId, bytes: &[u8]) -> Result<Key> {
    let identity = RowIdentity::decode(bytes)?;
    if identity.table() != table || identity.columns() != [ColumnId(0)] {
        return Err(DbError::Corruption {
            artifact: "row identity",
            reason: "legacy primary identity does not match its table or encoding".to_owned(),
        });
    }
    let [Value::Bytes(bytes)] = identity.values() else {
        return Err(DbError::Corruption {
            artifact: "row identity",
            reason: "legacy primary identity has the wrong value type".to_owned(),
        });
    };
    let value: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| DbError::Corruption {
            artifact: "row identity",
            reason: "legacy primary identity has the wrong width".to_owned(),
        })?;
    Ok(Key(value))
}

impl Value {
    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Cursor<'_> {
    fn take(&mut self, length: usize) -> Result<&[u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| self.corrupt("row-identity length overflows"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| self.corrupt("truncated row identity"))?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("u16 width"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("u64 width"),
        ))
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.u32()? as usize;
        Ok(self.take(length)?.to_vec())
    }

    fn value(&mut self) -> Result<Value> {
        match self.byte()? {
            0 => Err(self.corrupt("NULL is not valid in a row identity")),
            1 => Ok(Value::Bytes(self.bytes()?)),
            2 => match self.byte()? {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(self.corrupt("invalid boolean in row identity")),
            },
            3 => Ok(Value::I64(i64::from_le_bytes(
                self.take(8)?.try_into().expect("i64 width"),
            ))),
            4 => Ok(Value::U64(self.u64()?)),
            5 => String::from_utf8(self.bytes()?)
                .map(Value::Text)
                .map_err(|_| self.corrupt("text is not UTF-8 in row identity")),
            _ => Err(self.corrupt("unknown value tag in row identity")),
        }
    }

    fn finish(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(self.corrupt("trailing row-identity bytes"))
        }
    }

    fn corrupt(&self, reason: &str) -> DbError {
        DbError::Corruption {
            artifact: "row identity",
            reason: reason.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAGIC, RowIdentity, VERSION};
    use crate::DbError;
    use crate::relational::{ColumnId, TableId, Value};

    fn identity(columns: &[u16], values: Vec<Value>) -> RowIdentity {
        RowIdentity::new(
            TableId(7),
            columns.iter().copied().map(ColumnId).collect(),
            values,
        )
        .expect("identity")
    }

    #[test]
    fn round_trip_preserves_table_columns_and_values() {
        let original = identity(
            &[5, 4, 3, 2, 1],
            vec![
                Value::Bytes(vec![0, 1, 255]),
                Value::Bool(true),
                Value::I64(i64::MIN),
                Value::U64(u64::MAX),
                Value::Text("tenant".to_owned()),
            ],
        );
        let encoded = original.encode().expect("encode");
        let decoded = RowIdentity::decode(&encoded).expect("decode");
        assert_eq!(decoded, original);
        assert_eq!(decoded.table(), TableId(7));
        assert_eq!(
            decoded.columns(),
            &[
                ColumnId(5),
                ColumnId(4),
                ColumnId(3),
                ColumnId(2),
                ColumnId(1)
            ]
        );
        assert_eq!(
            decoded.values(),
            &[
                Value::Bytes(vec![0, 1, 255]),
                Value::Bool(true),
                Value::I64(i64::MIN),
                Value::U64(u64::MAX),
                Value::Text("tenant".to_owned()),
            ]
        );
    }

    #[test]
    fn encoding_is_unambiguous_across_types_boundaries_and_column_order() {
        let text_pair = identity(
            &[1, 2],
            vec![Value::Text("a".to_owned()), Value::Text("bc".to_owned())],
        )
        .encode()
        .expect("encode text pair");
        let different_boundaries = identity(
            &[1, 2],
            vec![Value::Text("ab".to_owned()), Value::Text("c".to_owned())],
        )
        .encode()
        .expect("encode different boundaries");
        let signed = identity(&[1], vec![Value::I64(1)])
            .encode()
            .expect("encode signed");
        let unsigned = identity(&[1], vec![Value::U64(1)])
            .encode()
            .expect("encode unsigned");
        let reversed = identity(&[2, 1], vec![Value::U64(42), Value::U64(7)])
            .encode()
            .expect("encode reversed");
        let ordered = identity(&[1, 2], vec![Value::U64(7), Value::U64(42)])
            .encode()
            .expect("encode ordered");

        assert_ne!(text_pair, different_boundaries);
        assert_ne!(signed, unsigned);
        assert_ne!(reversed, ordered);
    }

    #[test]
    fn constructor_rejects_empty_mismatched_duplicate_and_null_components() {
        assert!(matches!(
            RowIdentity::new(TableId(1), Vec::new(), Vec::new()),
            Err(DbError::InvalidState(reason)) if reason.contains("at least one")
        ));
        assert!(matches!(
            RowIdentity::new(TableId(1), vec![ColumnId(1)], Vec::new()),
            Err(DbError::InvalidState(reason)) if reason.contains("values")
        ));
        assert!(matches!(
            RowIdentity::new(
                TableId(1),
                vec![ColumnId(1), ColumnId(1)],
                vec![Value::U64(1), Value::U64(2)]
            ),
            Err(DbError::InvalidState(reason)) if reason.contains("repeats")
        ));
        assert!(matches!(
            RowIdentity::new(TableId(1), vec![ColumnId(1)], vec![Value::Null]),
            Err(DbError::InvalidState(reason)) if reason.contains("NULL")
        ));
    }

    #[test]
    fn decoder_rejects_unknown_versions_malformed_values_and_trailing_bytes() {
        let encoded = identity(&[1], vec![Value::U64(9)])
            .encode()
            .expect("encode");
        let mut wrong_magic = encoded.clone();
        wrong_magic[..4].copy_from_slice(b"NOPE");
        assert!(matches!(
            RowIdentity::decode(&wrong_magic),
            Err(DbError::Corruption { artifact: "row identity", reason })
                if reason == "unknown row-identity format"
        ));

        let mut wrong_version = encoded.clone();
        wrong_version[4] = VERSION + 1;
        assert!(matches!(
            RowIdentity::decode(&wrong_version),
            Err(DbError::Corruption { artifact: "row identity", reason })
                if reason == "unsupported row-identity version"
        ));

        let mut invalid_tag = encoded.clone();
        invalid_tag[19] = 99;
        assert!(matches!(
            RowIdentity::decode(&invalid_tag),
            Err(DbError::Corruption { artifact: "row identity", reason })
                if reason.contains("value tag")
        ));

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            RowIdentity::decode(&trailing),
            Err(DbError::Corruption { artifact: "row identity", reason })
                if reason == "trailing row-identity bytes"
        ));

        assert!(matches!(
            RowIdentity::decode(&encoded[..encoded.len() - 1]),
            Err(DbError::Corruption { artifact: "row identity", reason })
                if reason == "truncated row identity"
        ));

        let mut null_value = encoded;
        null_value[19] = 0;
        assert!(matches!(
            RowIdentity::decode(&null_value),
            Err(DbError::Corruption { artifact: "row identity", reason })
                if reason.contains("NULL")
        ));
        assert_eq!(MAGIC, *b"DBRI");
    }
}
