use super::*;
use pgwire::api::DefaultClient;

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
    let control = OperationControl::with_cancellation(CancellationToken::new());
    let run = |sql| {
        handler.run_statement(
            address,
            sql,
            &[],
            &pgwire::api::portal::Format::UnifiedText,
            &control,
        )
    };
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
