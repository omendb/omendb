use std::collections::BTreeMap;
use std::path::Path;

use omendb::{
    AggregateKind, AnalyticalQuery, CancellationToken, ColumnDefinition, ColumnId, ColumnType,
    DatabaseConfig, DbError, Key, OperationControl, RelationalBackendConfig, RelationalBackendKind,
    RelationalDatabase, Row, TableDefinition, TableId, Value,
};
use serde::Deserialize;
use tempfile::tempdir;

const ACCOUNTS_TABLE: TableId = TableId(301);
const LEDGER_TABLE: TableId = TableId(302);

const R4_TRACE: &str = include_str!("fixtures/r4-analytical-oltp-trace.jsonl");

fn backend_config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(omendb::SeerKernelConfig::new(directory.to_owned()))
        }
    }
}

fn accounts_schema() -> TableDefinition {
    TableDefinition {
        id: ACCOUNTS_TABLE,
        name: "accounts".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "category".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "balance".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(5),
                name: "status".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn ledger_schema() -> TableDefinition {
    TableDefinition {
        id: LEDGER_TABLE,
        name: "ledger_entries".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "account_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "amount".to_owned(),
                data_type: ColumnType::I64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "tag".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn account_row(id: u64, tenant_id: u64, category: &str, balance: u64, status: &str) -> Row {
    Row {
        primary: Key::new(ACCOUNTS_TABLE.0, id),
        values: vec![
            Value::U64(id),
            Value::U64(tenant_id),
            Value::Text(category.to_owned()),
            Value::U64(balance),
            Value::Text(status.to_owned()),
        ],
    }
}

fn ledger_row(id: u64, account_id: u64, amount: i64, tag: &str) -> Row {
    Row {
        primary: Key::new(LEDGER_TABLE.0, id),
        values: vec![
            Value::U64(id),
            Value::U64(account_id),
            Value::I64(amount),
            Value::Text(tag.to_owned()),
        ],
    }
}

#[derive(Clone, Debug, Default)]
struct AccountModel {
    tenant_id: u64,
    category: String,
    balance: u64,
    status: String,
}

#[derive(Clone, Debug, Default)]
struct OracleModel {
    accounts: BTreeMap<u64, AccountModel>,
    ledger_entries: BTreeMap<u64, (u64, i64, String)>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct TraceEvent {
    seq: u64,
    kind: String,
    accounts_count: Option<usize>,
    entries_count: Option<usize>,
    name: Option<String>,
    query: Option<String>,
    operations: Option<Vec<Operation>>,
    expect_accounts: Option<usize>,
    expect_entries: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct Operation {
    op: String,
    from: Option<u64>,
    to: Option<u64>,
    account: Option<u64>,
    amount: Option<i64>,
    status: Option<String>,
}

fn install_r4_schema(database: &mut RelationalDatabase) {
    database
        .create_table(accounts_schema())
        .expect("create accounts");
    database
        .create_table(ledger_schema())
        .expect("create ledger");
}

#[test]
fn public_facade_replays_r4_analytical_workload_across_backends() {
    for backend_kind in [
        RelationalBackendKind::Temporary,
        RelationalBackendKind::Seer,
    ] {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("db");
        let config = backend_config(backend_kind, &db_path);

        let mut database = RelationalDatabase::open(config.clone()).expect("open db");
        install_r4_schema(&mut database);

        let mut oracle = OracleModel::default();

        for line in R4_TRACE.lines().filter(|l| !l.trim().is_empty()) {
            let event: TraceEvent = serde_json::from_str(line).expect("parse event");
            match event.kind.as_str() {
                "seed_population" => {
                    let count = event.accounts_count.unwrap_or(200);
                    for id in 1..=count as u64 {
                        let tenant_id = (id % 5) + 1;
                        let category = match id % 3 {
                            0 => "premium",
                            1 => "standard",
                            _ => "enterprise",
                        };
                        let balance = id * 100;
                        let status = if id % 10 == 0 { "inactive" } else { "active" };

                        database
                            .insert(
                                ACCOUNTS_TABLE,
                                account_row(id, tenant_id, category, balance, status),
                            )
                            .expect("insert account");

                        oracle.accounts.insert(
                            id,
                            AccountModel {
                                tenant_id,
                                category: category.to_owned(),
                                balance,
                                status: status.to_owned(),
                            },
                        );
                    }

                    let entries_count = event.entries_count.unwrap_or(600);
                    for id in 1..=entries_count as u64 {
                        let account_id = (id % count as u64) + 1;
                        let amount = (id as i64) * 10;
                        let tag = "initial_deposit";

                        database
                            .insert(LEDGER_TABLE, ledger_row(id, account_id, amount, tag))
                            .expect("insert ledger");

                        oracle
                            .ledger_entries
                            .insert(id, (account_id, amount, tag.to_owned()));
                    }
                }
                "oltp_batch" => {
                    for op in event.operations.unwrap_or_default() {
                        match op.op.as_str() {
                            "transfer" => {
                                let from_id = op.from.expect("from id");
                                let to_id = op.to.expect("to id");
                                let amount = op.amount.expect("amount") as u64;

                                let from = oracle.accounts.get_mut(&from_id).expect("from account");
                                from.balance = from.balance.saturating_sub(amount);
                                let from_row = account_row(
                                    from_id,
                                    from.tenant_id,
                                    &from.category,
                                    from.balance,
                                    &from.status,
                                );

                                let to = oracle.accounts.get_mut(&to_id).expect("to account");
                                to.balance = to.balance.saturating_add(amount);
                                let to_row = account_row(
                                    to_id,
                                    to.tenant_id,
                                    &to.category,
                                    to.balance,
                                    &to.status,
                                );

                                database
                                    .update(ACCOUNTS_TABLE, from_row)
                                    .expect("update from");
                                database.update(ACCOUNTS_TABLE, to_row).expect("update to");
                            }
                            "credit" => {
                                let acc_id = op.account.expect("acc id");
                                let amount = op.amount.expect("amount") as u64;
                                let acc = oracle.accounts.get_mut(&acc_id).expect("account");
                                acc.balance = acc.balance.saturating_add(amount);
                                let row = account_row(
                                    acc_id,
                                    acc.tenant_id,
                                    &acc.category,
                                    acc.balance,
                                    &acc.status,
                                );
                                database.update(ACCOUNTS_TABLE, row).expect("credit");
                            }
                            "update_status" => {
                                let acc_id = op.account.expect("acc id");
                                let status = op.status.expect("status");
                                let acc = oracle.accounts.get_mut(&acc_id).expect("account");
                                acc.status = status.clone();
                                let row = account_row(
                                    acc_id,
                                    acc.tenant_id,
                                    &acc.category,
                                    acc.balance,
                                    &status,
                                );
                                database.update(ACCOUNTS_TABLE, row).expect("update status");
                            }
                            other => panic!("unrecognized op: {other}"),
                        }
                    }
                }
                "analytical_query" => {
                    let query_sql = event.query.as_deref().expect("query SQL");
                    let sql_res = database.execute_sql(query_sql).expect("execute sql");

                    match event.name.as_deref().unwrap_or_default() {
                        "global_totals" => {
                            let expected_count = oracle.accounts.len() as u64;
                            let expected_sum: u64 =
                                oracle.accounts.values().map(|a| a.balance).sum();
                            let expected_min =
                                oracle.accounts.values().map(|a| a.balance).min().unwrap();
                            let expected_max =
                                oracle.accounts.values().map(|a| a.balance).max().unwrap();
                            let expected_avg =
                                (expected_sum as f64 / expected_count as f64).round() as i64;

                            assert_eq!(sql_res.rows.len(), 1);
                            assert_eq!(sql_res.rows[0][0], Value::U64(expected_count));
                            assert_eq!(sql_res.rows[0][1], Value::U64(expected_sum));
                            assert_eq!(sql_res.rows[0][2], Value::I64(expected_avg));
                            assert_eq!(sql_res.rows[0][3], Value::U64(expected_min));
                            assert_eq!(sql_res.rows[0][4], Value::U64(expected_max));
                        }
                        "category_breakdown" => {
                            let mut oracle_cats: BTreeMap<String, (u64, u64)> = BTreeMap::new();
                            for acc in oracle.accounts.values() {
                                let entry =
                                    oracle_cats.entry(acc.category.clone()).or_insert((0, 0));
                                entry.0 += 1;
                                entry.1 += acc.balance;
                            }

                            assert_eq!(sql_res.rows.len(), oracle_cats.len());
                            for row in &sql_res.rows {
                                if let Value::Text(cat) = &row[0] {
                                    let (exp_count, exp_sum) =
                                        oracle_cats.get(cat).expect("known category");
                                    assert_eq!(row[1], Value::U64(*exp_count));
                                    assert_eq!(row[2], Value::U64(*exp_sum));
                                } else {
                                    panic!("expected category string");
                                }
                            }
                        }
                        "status_counts" => {
                            let mut oracle_statuses: BTreeMap<String, u64> = BTreeMap::new();
                            for acc in oracle.accounts.values() {
                                *oracle_statuses.entry(acc.status.clone()).or_insert(0) += 1;
                            }

                            assert_eq!(sql_res.rows.len(), oracle_statuses.len());
                            for row in &sql_res.rows {
                                if let Value::Text(st) = &row[0] {
                                    let exp_count = oracle_statuses.get(st).expect("known status");
                                    assert_eq!(row[1], Value::U64(*exp_count));
                                } else {
                                    panic!("expected status string");
                                }
                            }
                        }
                        "active_premium_totals" => {
                            let (exp_count, exp_sum) = oracle
                                .accounts
                                .values()
                                .filter(|a| a.category == "premium" && a.status == "active")
                                .fold((0u64, 0u64), |(c, s), a| (c + 1, s + a.balance));

                            assert_eq!(sql_res.rows.len(), 1);
                            assert_eq!(sql_res.rows[0][0], Value::U64(exp_count));
                            assert_eq!(sql_res.rows[0][1], Value::U64(exp_sum));
                        }
                        _ => {}
                    }
                }
                "checkpoint_and_verify" => {
                    database.checkpoint().expect("checkpoint");
                    let verify = database.verify().expect("verify");
                    assert_eq!(verify.verified_tables, 2);
                    assert!(verify.verified_rows > 0);
                }
                _ => {}
            }
        }

        // Reopen database and verify persisted state
        database.close().expect("close db");
        let mut reopened = RelationalDatabase::open(config).expect("reopen");
        let reopened_verify = reopened.verify().expect("verify reopened");
        assert_eq!(reopened_verify.verified_tables, 2);
        assert!(reopened_verify.verified_rows > 0);

        let final_query = "SELECT COUNT(*), SUM(balance) FROM accounts";
        let final_res = reopened.execute_sql(final_query).expect("final query");
        let exp_count = oracle.accounts.len() as u64;
        let exp_sum: u64 = oracle.accounts.values().map(|a| a.balance).sum();
        assert_eq!(final_res.rows[0][0], Value::U64(exp_count));
        assert_eq!(final_res.rows[0][1], Value::U64(exp_sum));
    }
}

#[test]
fn morsel_scanner_enforces_memory_quota_and_cancellation() {
    let dir = tempdir().expect("tempdir");
    let mut database = RelationalDatabase::open(backend_config(
        RelationalBackendKind::Temporary,
        &dir.path().join("db"),
    ))
    .expect("open db");

    install_r4_schema(&mut database);

    for id in 1..=500 {
        let cat = format!("cat_{}", id % 100);
        database
            .insert(ACCOUNTS_TABLE, account_row(id, 1, &cat, id * 10, "active"))
            .expect("insert");
    }

    // Test memory budget limit in analytical executor
    let query_low_memory = AnalyticalQuery::new(ACCOUNTS_TABLE)
        .with_morsel_size(16)
        .with_group_by(ColumnId(3)) // Group by category (100 distinct groups)
        .with_aggregate(AggregateKind::Count, None, "count");

    // Setting an artificially tiny memory quota (e.g. 500 bytes) triggers ResourceLimitExceeded
    let mut constrained_query = query_low_memory.clone();
    constrained_query.max_memory_bytes = 500;

    let res = database.query_analytical(&constrained_query);
    assert!(
        matches!(res, Err(DbError::ResourceLimitExceeded(_))),
        "Expected ResourceLimitExceeded under tight memory quota, got {res:?}"
    );

    // Test cooperative cancellation during morsel scan
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let control = OperationControl::with_cancellation(cancellation);

    let cancel_res = database.query_analytical_with_control(&query_low_memory, &control);
    assert!(
        matches!(cancel_res, Err(DbError::Cancelled)),
        "Expected DbError::Cancelled under cancelled control, got {cancel_res:?}"
    );
}
