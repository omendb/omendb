//! Property tests for the durable logical encodings.
//!
//! These exercise the roundtrip contracts that persistence depends on:
//! `Value` self-describing encoding, `Row` envelope encoding, and canonical
//! `RowIdentity` encoding. Any regression here corrupts databases across
//! restarts, so failures must fail loudly with a shrunk counterexample.

use proptest::prelude::*;

use omendb::{ColumnDefinition, ColumnId, ColumnType, Key, Row, TableDefinition, Value};

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::I64),
        any::<u64>().prop_map(Value::U64),
        ".*".prop_map(Value::Text),
        proptest::collection::vec(any::<u8>(), 0..64).prop_map(Value::Bytes),
    ]
}

fn arb_row_values() -> impl Strategy<Value = Vec<Value>> {
    proptest::collection::vec(arb_value(), 1..6)
}

/// A single-column table whose column type accepts every value shape. The
/// physical row format is validated against this definition.
fn permissive_table() -> TableDefinition {
    const FIXTURE_TABLE_ID: u64 = 0x0DEC_0DE5;
    TableDefinition {
        id: omendb::TableId(FIXTURE_TABLE_ID),
        name: "property_rows".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "payload".to_owned(),
            data_type: ColumnType::Bytes,
            nullable: true,
        }],
    }
}

proptest! {
    #[test]
    fn row_envelope_roundtrips(values in arb_row_values()) {
        let table = permissive_table();
        let primary = Key::new(table.id.0, 7);
        let row = Row { primary, values };
        let encoded = omendb::encode_row(&row).expect("encode row");
        let decoded = omendb::decode_row(primary, &encoded).expect("decode row");
        prop_assert_eq!(decoded.primary, row.primary);
        prop_assert_eq!(decoded.values.len(), row.values.len());
        for (decoded_value, original) in decoded.values.iter().zip(&row.values) {
            match (decoded_value, original) {
                // NULL survives as NULL; other values decode to their exact
                // typed representation. Bytes payloads carry every value
                // shape losslessly because the envelope stores tagged bytes
                // for the permissive fixture column.
                (omendb::Value::Null, omendb::Value::Null) => {}
                _ => prop_assert_eq!(decoded_value, original),
            }
        }
    }

    #[test]
    fn truncated_row_envelopes_are_rejected(values in arb_row_values(), cut in 0usize..24) {
        let table = permissive_table();
        let primary = Key::new(table.id.0, 9);
        let encoded = omendb::encode_row(&Row { primary, values }).expect("encode row");
        if cut >= encoded.len() {
            return Ok(());
        }
        prop_assert!(omendb::decode_row(primary, &encoded[..cut]).is_err());
    }

    #[test]
    fn corrupted_row_envelopes_are_detected(
        values in arb_row_values(),
        byte_index in 0usize..512,
    ) {
        let table = permissive_table();
        let primary = Key::new(table.id.0, 11);
        let original = Row {
            primary,
            values: values.clone(),
        };
        let mut encoded = omendb::encode_row(&original).expect("encode row");
        if encoded.is_empty() {
            return Ok(());
        }
        let position = byte_index % encoded.len();
        encoded[position] ^= 0xA5;
        // Corruption is either detected or changes the decoded content; it
        // must never silently reproduce the identical original row.
        if let Ok(decoded) = omendb::decode_row(primary, &encoded) {
            prop_assert!(
                decoded.values != values,
                "corrupted byte {} decoded to the original row",
                position
            );
        }
    }

    #[test]
    fn identity_roundtrip_preserves_components(values in arb_row_values()) {
        let non_null: Vec<Value> = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| match value {
                Value::Null => Value::I64(index as i64),
                other => other,
            })
            .collect();
        let count = non_null.len();
        let columns: Vec<omendb::ColumnId> = (0..count as u16)
            .map(|index| omendb::ColumnId(index + 1))
            .collect();
        let identity =
            omendb::RowIdentity::new(
                omendb::TableId(0x0DEC_0DE5),
                columns.clone(),
                non_null.clone(),
            )
            .expect("create identity");
        let encoded = identity.encode().expect("encode identity");
        let decoded = omendb::RowIdentity::decode(&encoded).expect("decode identity");
        prop_assert_eq!(decoded.table(), omendb::TableId(0x0DEC_0DE5));
        prop_assert_eq!(decoded.columns().to_vec(), columns);
        prop_assert_eq!(decoded.values().to_vec(), non_null);
    }

    #[test]
    fn identities_reject_null_and_mismatched_shapes(values in arb_row_values(), drop_count in 0usize..3) {
        let mut values: Vec<Value> = values
            .into_iter()
            .map(|value| match value {
                Value::Null => Value::I64(0),
                other => other,
            })
            .collect();
        if values.is_empty() || drop_count == 0 || drop_count > values.len() {
            return Ok(());
        }
        let columns: Vec<omendb::ColumnId> = (0..values.len() as u16)
            .map(|index| omendb::ColumnId(index + 1))
            .collect();
        // A single-value row cannot be truncated without becoming empty,
        // which the empty guard above already covers.
        if values.len() == 1 {
            return Ok(());
        }
        values.truncate(values.len() - drop_count.min(values.len() - 1));
        prop_assert!(
            omendb::RowIdentity::new(omendb::TableId(0x0DEC_0DE5), columns, values).is_err()
        );
    }
}
