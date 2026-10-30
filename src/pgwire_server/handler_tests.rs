use super::*;
use pgwire::api::DefaultClient;

#[test]
fn missing_or_poisoned_identity_never_confers_bootstrap_authority() {
    let directory = tempfile::tempdir().unwrap();
    let database = Arc::new(std::sync::RwLock::new(
        crate::RelationalDatabase::create(crate::RelationalBackendConfig::new(
            directory.path().join("db"),
        ))
        .unwrap(),
    ));
    let address = "127.0.0.1:6543".parse().unwrap();
    let factory = super::super::auth::build_factory(
        &database,
        address,
        Arc::new(QueryWorkers::new()),
        None,
        None,
        None,
    )
    .unwrap();
    let handler = &factory.handler;
    let control = OperationControl::default();
    let run = |sql| handler.run_simple_statement(address, sql, &control, false);
    run("BEGIN").unwrap();
    let PgWireError::UserError(error) = run("SELECT 1").unwrap_err() else {
        panic!("expected authorization error");
    };
    assert_eq!(error.code, "42501");
    assert!(
        handler.lock_transactions().unwrap()[&address]
            .transaction
            .is_none()
    );
    run("ROLLBACK").unwrap();
    handler
        .identities
        .lock()
        .unwrap()
        .insert(address, SessionAuthority::Bootstrap);
    run("SELECT 1").unwrap();
    let identities = handler.identities.clone();
    let _ = std::panic::catch_unwind(move || {
        let _guard = identities.lock().unwrap();
        panic!("poison registry");
    });
    assert!(run("SELECT 1").is_err());
    run("ROLLBACK").unwrap();
}

#[test]
fn idle_expiry_releases_transactions_without_restoring_autocommit() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = crate::RelationalDatabase::create(crate::RelationalBackendConfig::new(
        directory.path().join("db"),
    ))
    .unwrap();
    database
        .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .unwrap();
    let database = Arc::new(std::sync::RwLock::new(database));
    let address = "127.0.0.1:6543".parse().unwrap();
    let factory = super::super::auth::build_factory(
        &database,
        address,
        Arc::new(QueryWorkers::new()),
        None,
        None,
        None,
    )
    .unwrap();
    let handler = &factory.handler;
    handler
        .identities
        .lock()
        .unwrap()
        .insert(address, SessionAuthority::Bootstrap);
    let control = OperationControl::with_cancellation(CancellationToken::new());
    let run = |sql| handler.run_simple_statement(address, sql, &control, false);
    let mut client = DefaultClient::<ParsedStatement>::new(address, false);
    for failed in [false, true] {
        run("BEGIN").unwrap();
        run("INSERT INTO items VALUES (1)").unwrap();
        if failed {
            assert!(run("SAVEPOINT missing").is_err());
        }
        handler
            .lock_transactions()
            .unwrap()
            .get_mut(&address)
            .unwrap()
            .last_used = Instant::now() - TX_IDLE_TIMEOUT - Duration::from_secs(1);
        let Err(error) = run("INSERT INTO items VALUES (2)") else {
            panic!("expired block stays failed");
        };
        let PgWireError::UserError(error) = error else {
            panic!("SQL error")
        };
        assert_eq!(error.code, "25P02");
        handler.sync_transaction_status(&mut client).unwrap();
        assert_eq!(client.transaction_status(), TransactionStatus::Error);
        let response = run("COMMIT").unwrap();
        assert!(matches!(response, Response::TransactionEnd(_)));
        handler.sync_transaction_status(&mut client).unwrap();
        assert_eq!(client.transaction_status(), TransactionStatus::Idle);
        assert!(
            database
                .write()
                .unwrap()
                .execute_sql("SELECT id FROM items")
                .unwrap()
                .rows
                .is_empty()
        );
    }
}
