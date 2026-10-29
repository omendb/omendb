use omendb::{DbError, RelationalBackendConfig, RelationalDatabase, SchemaMutation, TableId};

fn items(database: &mut RelationalDatabase) -> TableId {
    database
        .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .unwrap();
    database
        .catalog()
        .tables()
        .find(|table| table.name == "items")
        .unwrap()
        .id
}

#[test]
fn grants_follow_incarnations_across_rename_recreation_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let config = RelationalBackendConfig::new(directory.path().join("db"));
    let mut database = RelationalDatabase::create(config.clone()).unwrap();
    let table = items(&mut database);
    let definition = database.catalog().table(table).unwrap().clone();
    let (principal, _) = database
        .transaction(|db, tx| {
            let id = tx.create_principal(db, "alice")?;
            tx.set_table_grant(db, id, table, true, false)?;
            Ok(id)
        })
        .unwrap();
    let view = database.authorization(principal).unwrap();
    view.require_read(&database, table).unwrap();
    assert!(matches!(
        view.require_write(&database, table),
        Err(DbError::PermissionDenied(_))
    ));
    assert!(matches!(
        view.require_administration(&database),
        Err(DbError::PermissionDenied(_))
    ));
    database
        .execute_sql("ALTER TABLE items RENAME TO renamed")
        .unwrap();
    database
        .transaction(|db, tx| tx.rename_principal(db, principal, "renamed_alice"))
        .unwrap();
    assert!(database.principal("alice").unwrap().is_none());
    assert_eq!(
        database.principal("renamed_alice").unwrap().unwrap().id,
        principal
    );
    database.close().unwrap();
    let mut database = RelationalDatabase::open(config.clone()).unwrap();
    database
        .authorization(principal)
        .unwrap()
        .require_read(&database, table)
        .unwrap();
    database
        .apply_schema_mutations(&[SchemaMutation::DropTable { table }])
        .unwrap();
    database.create_table(definition).unwrap(); // Deliberately reuse the TableId and original name.
    assert!(matches!(
        database
            .authorization(principal)
            .unwrap()
            .require_read(&database, table),
        Err(DbError::PermissionDenied(_))
    ));
    database
        .transaction(|db, tx| {
            tx.set_table_grant(db, principal, table, false, true)?;
            tx.set_database_administrator(db, principal, true)
        })
        .unwrap();
    database
        .authorization(principal)
        .unwrap()
        .require_read(&database, table)
        .unwrap();
    database
        .transaction(|db, tx| tx.drop_principal(db, principal))
        .unwrap();
    assert!(!database.authorization_bootstrap_enabled().unwrap());
    let (replacement, _) = database
        .transaction(|db, tx| tx.create_principal(db, "renamed_alice"))
        .unwrap();
    assert_ne!(replacement, principal);
    assert!(matches!(
        database.authorization(principal),
        Err(DbError::PermissionDenied(_))
    ));
    assert!(matches!(
        database
            .authorization(replacement)
            .unwrap()
            .require_read(&database, table),
        Err(DbError::PermissionDenied(_))
    ));
    assert!(matches!(
        database
            .authorization(replacement)
            .unwrap()
            .require_administration(&database),
        Err(DbError::PermissionDenied(_))
    ));
    database.close().unwrap();
    let database = RelationalDatabase::open(config).unwrap();
    assert!(!database.authorization_bootstrap_enabled().unwrap());
    assert!(matches!(
        database
            .authorization(replacement)
            .unwrap()
            .require_read(&database, table),
        Err(DbError::PermissionDenied(_))
    ));
}

#[test]
fn failed_authority_operation_aborts_row_and_principal_changes_together() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    let table = items(&mut database);
    let mut transaction = database.begin().unwrap();
    transaction
        .execute_sql(&database, "INSERT INTO items VALUES (1)")
        .unwrap();
    let id = transaction.create_principal(&database, "alice").unwrap();
    assert!(database.principal("alice").unwrap().is_none());
    assert!(
        transaction
            .set_table_grant(&database, id, TableId(987), true, false)
            .is_err()
    );
    assert!(matches!(
        transaction.set_table_grant(&database, id, table, true, false),
        Err(DbError::TransactionAborted)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(DbError::TransactionAborted)
    ));
    assert!(database.principal("alice").unwrap().is_none());
    assert!(database.authorization_bootstrap_enabled().unwrap());
    assert!(
        database
            .execute_sql("SELECT id FROM items")
            .unwrap()
            .rows
            .is_empty()
    );

    // The successful counterpart publishes data, identity, credentials and
    // privileges in one decision, rather than provisioning separate SQL rows.
    let (id, _) = database
        .transaction(|db, tx| {
            let id = tx.create_principal(db, "alice")?;
            tx.set_principal_credential(db, id, Some(vec![7; 64]))?;
            tx.set_table_grant(db, id, table, false, true)?;
            tx.execute_sql(db, "INSERT INTO items VALUES (1)")?;
            Ok(id)
        })
        .unwrap();
    assert!(!database.authorization_bootstrap_enabled().unwrap());
    database
        .authorization(id)
        .unwrap()
        .require_write(&database, table)
        .unwrap();
    assert_eq!(
        database
            .execute_sql("SELECT id FROM items")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn revocation_is_current_but_does_not_fence_authorized_data_commits() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    let table = items(&mut database);
    database
        .execute_sql(
            "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES items(id))",
        )
        .unwrap();
    database
        .execute_sql("INSERT INTO items VALUES (1)")
        .unwrap();
    database
        .execute_sql("INSERT INTO children VALUES (1, 1)")
        .unwrap();
    let (id, _) = database
        .transaction(|db, tx| {
            let id = tx.create_principal(db, "writer")?;
            tx.set_table_grant(db, id, table, false, true)?;
            Ok(id)
        })
        .unwrap();
    let mut revoke = database.begin().unwrap();
    revoke
        .set_table_grant(&database, id, table, false, false)
        .unwrap();
    let admitted = database.authorization(id).unwrap();
    admitted.require_write(&database, table).unwrap();
    database
        .execute_sql("INSERT INTO items VALUES (2)")
        .unwrap();
    let mut data = database.begin().unwrap();
    data.execute_sql(&database, "INSERT INTO items VALUES (3)")
        .unwrap();
    // This authority-only writer predates the publication of item 2. It must
    // not acquire FK row dependencies and fail because those rows changed.
    revoke.commit().unwrap();
    assert!(matches!(
        database
            .authorization(id)
            .unwrap()
            .require_write(&database, table),
        Err(DbError::PermissionDenied(_))
    ));
    // An admitted view is retained for its operation; commit is not a new
    // execution boundary and must not be retroactively reauthorized.
    admitted.require_write(&database, table).unwrap();
    data.commit().unwrap();
    assert_eq!(
        database
            .execute_sql("SELECT id FROM items")
            .unwrap()
            .rows
            .len(),
        3
    );
}

#[test]
fn administration_and_views_are_bound_to_one_database_handle() {
    let directory = tempfile::tempdir().unwrap();
    let mut first =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("first")))
            .unwrap();
    let mut second = RelationalDatabase::create(RelationalBackendConfig::new(
        directory.path().join("second"),
    ))
    .unwrap();
    let table = items(&mut first);
    assert_eq!(items(&mut second), table);
    let first_view = first.bootstrap_authorization().unwrap();
    first_view.require_administration(&first).unwrap();
    assert!(matches!(
        first_view.require_read(&second, table),
        Err(DbError::PermissionDenied(_))
    ));
    assert!(matches!(
        first_view.require_administration(&second),
        Err(DbError::PermissionDenied(_))
    ));
}
