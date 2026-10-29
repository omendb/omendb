//! Equality keys must agree with scalar equality without changing row payloads.
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DbError, F64, IndexDefinition, IndexId, IndexKeyPart,
    Key, RelationalBackendConfig, RelationalDatabase, Row, RowIdentity, TableDefinition, TableId,
    Value,
};

const TABLE: TableId = TableId(7);
const INDEX: IndexId = IndexId(7);

fn equal_pairs() -> [(f64, f64); 2] {
    [
        (-0.0, 0.0),
        (
            f64::from_bits(0x7ff8_0000_0000_0001),
            f64::from_bits(0xfff0_0000_0000_0001),
        ),
    ]
}

fn row(id: u64, value: f64) -> Row {
    Row {
        primary: Key::new(TABLE.0, id),
        values: vec![Value::U64(id), Value::Float64(F64(value))],
    }
}

fn index(unique: bool) -> IndexDefinition {
    IndexDefinition {
        id: INDEX,
        table: TABLE,
        parts: vec![IndexKeyPart::Column(ColumnId(2))],
        predicate: None,
        unique,
    }
}

fn create(path: &std::path::Path, primary: bool) -> RelationalDatabase {
    let mut db = RelationalDatabase::create(RelationalBackendConfig::new(path)).unwrap();
    db.create_table_with_schema_and_primary_key(
        TableDefinition {
            id: TABLE,
            name: "items".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "tenant".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "value".to_owned(),
                    data_type: ColumnType::Float64,
                    nullable: false,
                },
            ],
        },
        primary.then(|| vec![ColumnId(1), ColumnId(2)]),
        Default::default(),
    )
    .unwrap();
    db
}

#[test]
fn equal_float_identities_encode_identically_but_rows_preserve_bits() {
    for (left, right) in equal_pairs() {
        assert_eq!(F64(left), F64(right));
        let identity = |value| {
            RowIdentity::new(TABLE, vec![ColumnId(2)], vec![Value::Float64(F64(value))]).unwrap()
        };
        assert_eq!(
            identity(left).encode().unwrap(),
            identity(right).encode().unwrap()
        );
        for value in [left, right] {
            let original = row(1, value);
            let decoded =
                omendb::decode_row(original.primary, &omendb::encode_row(&original).unwrap())
                    .unwrap();
            let Value::Float64(decoded) = decoded.values[1] else {
                panic!("float payload")
            };
            assert_eq!(decoded.0.to_bits(), value.to_bits());
        }
    }
}

#[test]
fn indexed_float_equality_survives_reopen_update_delete_and_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    let mut db = create(&path, false);
    db.create_index(index(false)).unwrap();
    for (offset, (stored, lookup)) in equal_pairs().into_iter().enumerate() {
        let id = offset as u64 + 1;
        db.insert(TABLE, row(id, stored)).unwrap();
        assert_eq!(
            db.index_get(TABLE, INDEX, &[Value::Float64(F64(lookup))])
                .unwrap(),
            vec![row(id, stored)]
        );
    }
    db.close().unwrap();
    let mut db = RelationalDatabase::open(RelationalBackendConfig::new(&path)).unwrap();
    for (offset, (stored, lookup)) in equal_pairs().into_iter().enumerate() {
        assert_eq!(
            db.index_get(TABLE, INDEX, &[Value::Float64(F64(lookup))])
                .unwrap(),
            vec![row(offset as u64 + 1, stored)]
        );
    }
    db.update(TABLE, row(1, 2.0)).unwrap();
    db.delete(TABLE, Key::new(TABLE.0, 2)).unwrap();
    for (_, lookup) in equal_pairs() {
        assert!(
            db.index_get(TABLE, INDEX, &[Value::Float64(F64(lookup))])
                .unwrap()
                .is_empty()
        );
    }
    let mut tx = db.begin().unwrap();
    tx.insert(&db, TABLE, row(3, -0.0)).unwrap();
    assert_eq!(
        tx.index_get(&db, TABLE, INDEX, &[Value::Float64(F64(0.0))])
            .unwrap(),
        vec![row(3, -0.0)]
    );
    drop(tx);
    assert!(
        db.index_get(TABLE, INDEX, &[Value::Float64(F64(0.0))])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.index_get(TABLE, INDEX, &[Value::Float64(F64(2.0))])
            .unwrap(),
        vec![row(1, 2.0)]
    );
}

#[test]
fn unique_float_inserts_and_index_builds_reject_equal_values() {
    for (left, right) in equal_pairs() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = create(&directory.path().join("insert"), false);
        db.create_index(index(true)).unwrap();
        db.insert(TABLE, row(1, left)).unwrap();
        assert!(
            matches!(
                db.insert(TABLE, row(2, right)),
                Err(DbError::UniqueViolation { .. })
            ),
            "equal value bypassed unique index"
        );
        assert_eq!(db.scan(TABLE, 10).unwrap().len(), 1);
        assert_eq!(
            db.index_get(TABLE, INDEX, &[Value::Float64(F64(right))])
                .unwrap(),
            vec![row(1, left)]
        );

        let mut build = create(&directory.path().join("build"), false);
        build.insert(TABLE, row(1, left)).unwrap();
        build.insert(TABLE, row(2, right)).unwrap();
        let before = build.catalog().clone();
        let rows_before = build.scan(TABLE, 10).unwrap();
        assert!(
            matches!(
                build.create_index(index(true)),
                Err(DbError::UniqueViolation { .. })
            ),
            "build admitted equal values"
        );
        // Tree-ID reservations may advance the storage frontier even when
        // publication fails; the relational catalog and data must not change.
        assert_eq!(build.catalog(), &before);
        assert_eq!(build.scan(TABLE, 10).unwrap(), rows_before);
        assert!(!build.catalog().indexes().any(|index| index.id == INDEX));
        build.create_index(index(false)).unwrap();
        assert_eq!(
            build
                .index_get(TABLE, INDEX, &[Value::Float64(F64(right))])
                .unwrap()
                .len(),
            2
        );
    }
}

#[test]
fn composite_primary_keys_use_float_equality_for_lookup_and_uniqueness() {
    for (left, right) in equal_pairs() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db");
        let db = create(&path, true);
        db.insert(TABLE, row(1, left)).unwrap();
        let identity = RowIdentity::new(
            TABLE,
            vec![ColumnId(1), ColumnId(2)],
            vec![Value::U64(1), Value::Float64(F64(right))],
        )
        .unwrap();
        assert!(db.get_by_identity(TABLE, &identity).unwrap().is_some());
        assert!(
            db.insert(TABLE, row(1, right)).is_err(),
            "equal primary key inserted twice"
        );
        assert_eq!(db.scan(TABLE, 10).unwrap().len(), 1);
        db.close().unwrap();
        let db = RelationalDatabase::open(RelationalBackendConfig::new(path)).unwrap();
        assert!(db.get_by_identity(TABLE, &identity).unwrap().is_some());
    }
}

#[test]
fn float_foreign_keys_use_the_same_equality_as_the_referenced_unique_index() {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    db.execute_sql("CREATE TABLE parents (id BIGINT PRIMARY KEY, value DOUBLE PRECISION)")
        .unwrap();
    db.execute_sql("CREATE UNIQUE INDEX parent_value ON parents(value)")
        .unwrap();
    db.execute_sql("CREATE TABLE children (id BIGINT PRIMARY KEY, value DOUBLE PRECISION, FOREIGN KEY (value) REFERENCES parents(value))").unwrap();
    for (offset, (parent, child)) in equal_pairs().into_iter().enumerate() {
        let id = Value::I64(offset as i64 + 1);
        db.execute_sql_with_params(
            "INSERT INTO parents VALUES ($1, $2)",
            &[id.clone(), Value::Float64(F64(parent))],
        )
        .unwrap();
        db.execute_sql_with_params(
            "INSERT INTO children VALUES ($1, $2)",
            &[id.clone(), Value::Float64(F64(child))],
        )
        .unwrap();
        assert!(matches!(
            db.execute_sql_with_params("DELETE FROM parents WHERE id = $1", &[id]),
            Err(DbError::ForeignKeyViolation { .. })
        ));
    }
    assert_eq!(
        db.execute_sql("SELECT COUNT(*) FROM parents").unwrap().rows,
        vec![vec![Value::U64(2)]]
    );
}
