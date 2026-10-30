use super::*;
use crate::{
    ArithmeticOperator, ColumnDefinition, ColumnType, F64, IndexExpression, IndexKeyPart,
    IndexPredicate,
};
use tempfile::tempdir;

fn table() -> TableDefinition {
    TableDefinition {
        id: TableId(1),
        name: "values".to_owned(),
        columns: [
            ColumnType::U64,
            ColumnType::I64,
            ColumnType::Float64,
            ColumnType::U64,
            ColumnType::I64,
            ColumnType::Float64,
        ]
        .into_iter()
        .enumerate()
        .map(|(position, data_type)| ColumnDefinition {
            id: ColumnId(position as u16 + 1),
            name: format!("c{position}"),
            data_type,
            nullable: false,
        })
        .collect(),
    }
}

fn add(left: IndexExpression, right: IndexExpression) -> IndexExpression {
    IndexExpression::Arithmetic {
        left: Box::new(left),
        op: ArithmeticOperator::Add,
        right: Box::new(right),
    }
}

fn column(id: u16) -> IndexExpression {
    IndexExpression::Column(ColumnId(id))
}

#[test]
fn legacy_catalog_acceptance_depends_on_each_key_parts_possible_types() {
    use IndexKeyPart::{Column, Expression};
    let cases = [
        ("unindexed floats", None, vec![], false, false),
        (
            "integer primary key",
            Some(vec![ColumnId(1), ColumnId(2)]),
            vec![],
            false,
            false,
        ),
        (
            "float primary key",
            Some(vec![ColumnId(1), ColumnId(3)]),
            vec![],
            true,
            false,
        ),
        (
            "integer index",
            None,
            vec![Column(ColumnId(1))],
            false,
            false,
        ),
        ("float index", None, vec![Column(ColumnId(3))], true, false),
        (
            "separate signed parts",
            None,
            vec![Column(ColumnId(1)), Column(ColumnId(2))],
            false,
            false,
        ),
        (
            "unsigned arithmetic",
            None,
            vec![Expression(add(column(1), column(4)))],
            false,
            false,
        ),
        (
            "signed arithmetic",
            None,
            vec![Expression(add(column(2), column(5)))],
            false,
            false,
        ),
        (
            "separate arithmetic parts",
            None,
            vec![
                Expression(add(column(1), column(4))),
                Expression(add(column(2), column(5))),
            ],
            false,
            false,
        ),
        (
            "mixed arithmetic",
            None,
            vec![Expression(add(column(1), column(2)))],
            true,
            true,
        ),
        (
            "nested mixed arithmetic",
            None,
            vec![Expression(add(add(column(1), column(4)), column(2)))],
            true,
            true,
        ),
        (
            "float expression leaf",
            None,
            vec![Expression(column(3))],
            true,
            false,
        ),
        (
            "homogeneous float arithmetic",
            None,
            vec![Expression(add(column(3), column(6)))],
            true,
            false,
        ),
        (
            "nested float expression",
            None,
            vec![Expression(add(column(1), add(column(2), column(3))))],
            true,
            true,
        ),
    ];
    for (name, primary, parts, float_keys, mixed_arithmetic) in cases {
        for unique in [false, true] {
            let mut catalog = Catalog::default();
            catalog
                .create_table_with_primary_key(table(), primary.clone())
                .unwrap();
            if !parts.is_empty() {
                catalog
                    .create_index(IndexDefinition {
                        id: IndexId(1),
                        table: TableId(1),
                        parts: parts.clone(),
                        // Float predicates do not emit float key bytes.
                        predicate: Some(IndexPredicate {
                            terms: vec![(ColumnId(6), Value::Float64(F64(-0.0)))],
                        }),
                        unique,
                    })
                    .unwrap();
            }
            let current =
                encode_catalog_state(&catalog, &BTreeMap::new(), &BTreeMap::new()).unwrap();
            assert_eq!(decode_catalog_state(&current).unwrap().0, catalog, "{name}");
            for (magic, affected) in [
                (LEGACY_CATALOG_MAGIC, float_keys || mixed_arithmetic),
                (LEGACY_ARITHMETIC_CATALOG_MAGIC, mixed_arithmetic),
            ] {
                let mut legacy = current.clone();
                legacy[..4].copy_from_slice(magic);
                let decoded = decode_catalog_state(&legacy);
                if affected {
                    assert!(
                        matches!(decoded, Err(DbError::InvalidState(_))),
                        "{name}: {decoded:?}"
                    );
                    // Invalid bytes still fail strict decoding before the
                    // schema-dependent conversion check.
                    legacy.push(0);
                    assert!(matches!(
                        decode_catalog_state(&legacy),
                        Err(DbError::Corruption { .. })
                    ));
                } else {
                    assert_eq!(decoded.unwrap().0, catalog, "{name}");
                }
            }
        }
    }
}

#[test]
fn odc2_open_refuses_mixed_expression_keys_even_when_empty_without_rewriting() {
    for populated in [false, true] {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let mut store = DirectSeerStore::create(&path, Options::for_test()).unwrap();
        store.create_table(table(), None).unwrap();
        store
            .create_index(IndexDefinition {
                id: IndexId(1),
                table: TableId(1),
                parts: vec![IndexKeyPart::Expression(add(column(1), column(2)))],
                predicate: None,
                unique: true,
            })
            .unwrap();
        if populated {
            store
                .insert(
                    TableId(1),
                    Row {
                        primary: Key::new(1, 1),
                        values: vec![
                            Value::U64(1),
                            Value::I64(-1),
                            Value::Float64(F64(2.0)),
                            Value::U64(2),
                            Value::I64(-2),
                            Value::Float64(F64(3.0)),
                        ],
                    },
                )
                .unwrap();
        }
        let catalog_tree = store.catalog_tree;
        store.close().unwrap();
        drop(store);
        let database = TransactionDatabase::open(&path, Options::for_test()).unwrap();
        let mut tx = database.begin().unwrap();
        let mut legacy = tx
            .get(catalog_tree, DIRECT_CATALOG_MARKER)
            .unwrap()
            .unwrap();
        legacy[..4].copy_from_slice(LEGACY_ARITHMETIC_CATALOG_MAGIC);
        tx.put(catalog_tree, DIRECT_CATALOG_MARKER, &legacy)
            .unwrap();
        tx.commit().unwrap();
        drop(tx);
        let head = database.commit_sequence().unwrap();
        database.close().unwrap();
        drop(database);

        assert!(matches!(DirectSeerStore::open(&path, Options::for_test()),
            Err(DbError::InvalidState(reason)) if reason.contains("mixed-type expression-index arithmetic")));
        let database = TransactionDatabase::open(&path, Options::for_test()).unwrap();
        assert_eq!(database.commit_sequence().unwrap(), head);
        let mut tx = database.begin().unwrap();
        assert_eq!(
            tx.get(catalog_tree, DIRECT_CATALOG_MARKER)
                .unwrap()
                .unwrap(),
            legacy
        );
        tx.abort().unwrap();
        drop(tx);
        database.close().unwrap();
    }
}

#[test]
fn legacy_open_refuses_float_keys_without_rewriting_and_upgrades_safe_schema_atomically() {
    for affected in [false, true] {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let mut store = DirectSeerStore::create(&path, Options::for_test()).unwrap();
        store.create_table(table(), None).unwrap();
        if affected {
            store
                .create_index(IndexDefinition {
                    id: IndexId(1),
                    table: TableId(1),
                    parts: vec![IndexKeyPart::Column(ColumnId(3))],
                    predicate: None,
                    unique: false,
                })
                .unwrap();
        }
        let original = Row {
            primary: Key::new(1, 1),
            values: vec![
                Value::U64(1),
                Value::I64(-1),
                Value::Float64(F64(-0.0)),
                Value::U64(2),
                Value::I64(-2),
                Value::Float64(F64(f64::from_bits(0xfff0_0000_0000_0001))),
            ],
        };
        store.insert(TableId(1), original.clone()).unwrap();
        let catalog_tree = store.catalog_tree;
        store.close().unwrap();
        drop(store);

        let database = TransactionDatabase::open(&path, Options::for_test()).unwrap();
        let mut tx = database.begin().unwrap();
        let mut legacy = tx
            .get(catalog_tree, DIRECT_CATALOG_MARKER)
            .unwrap()
            .unwrap();
        assert_eq!(&legacy[..4], DIRECT_CATALOG_MAGIC);
        legacy[..4].copy_from_slice(LEGACY_CATALOG_MAGIC);
        tx.put(catalog_tree, DIRECT_CATALOG_MARKER, &legacy)
            .unwrap();
        tx.commit().unwrap();
        drop(tx);
        database.close().unwrap();
        drop(database);

        match DirectSeerStore::open(&path, Options::for_test()) {
            Err(DbError::InvalidState(reason)) if affected => {
                assert!(reason.contains("previous binary"));
            }
            Ok(mut store) if !affected => {
                let rows = store.scan(TableId(1)).unwrap();
                assert_eq!(rows, vec![original]);
                for (position, bits) in [(2, (-0.0_f64).to_bits()), (5, 0xfff0_0000_0000_0001)] {
                    let Value::Float64(value) = rows[0].values[position] else {
                        panic!("float")
                    };
                    assert_eq!(value.0.to_bits(), bits);
                }
                // Ordinary row writes preserve the safe legacy envelope.
                store.update(TableId(1), rows[0].clone()).unwrap();
                let mut tx = store.database.begin().unwrap();
                assert_eq!(
                    tx.get(catalog_tree, DIRECT_CATALOG_MARKER)
                        .unwrap()
                        .unwrap(),
                    legacy
                );
                tx.abort().unwrap();
                drop(tx);
                // The first float key and new envelope publish in the same decision.
                store
                    .create_index(IndexDefinition {
                        id: IndexId(1),
                        table: TableId(1),
                        parts: vec![IndexKeyPart::Column(ColumnId(3))],
                        predicate: None,
                        unique: true,
                    })
                    .unwrap();
                assert_eq!(
                    store
                        .index_get(TableId(1), IndexId(1), &[Value::Float64(F64(0.0))])
                        .unwrap()
                        .len(),
                    1
                );
                store.close().unwrap();
                drop(store);
                let reopened = DirectSeerStore::open(&path, Options::for_test()).unwrap();
                assert_eq!(
                    reopened
                        .index_get(TableId(1), IndexId(1), &[Value::Float64(F64(0.0))])
                        .unwrap()
                        .len(),
                    1
                );
                reopened.close().unwrap();
            }
            other => panic!("unexpected legacy open result: {:?}", other.err()),
        }

        let database = TransactionDatabase::open(&path, Options::for_test()).unwrap();
        let mut tx = database.begin().unwrap();
        let after = tx
            .get(catalog_tree, DIRECT_CATALOG_MARKER)
            .unwrap()
            .unwrap();
        if affected {
            assert_eq!(after, legacy, "refusal must not rewrite persisted state");
        } else {
            assert_eq!(&after[..4], DIRECT_CATALOG_MAGIC);
        }
        tx.abort().unwrap();
        drop(tx);
        database.close().unwrap();
    }
}
