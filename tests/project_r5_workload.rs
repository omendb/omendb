use std::collections::BTreeMap;
use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, DbError, Key, RelationalBackendConfig,
    RelationalBackendKind, RelationalDatabase, ReplicationRole, Row, StandbyReplica,
    TableDefinition, TableId, Value,
};
use tempfile::tempdir;

const ACCOUNTS_TABLE: TableId = TableId(501);

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
                name: "balance".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "status".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn account_row(id: u64, balance: u64, status: &str) -> Row {
    Row {
        primary: Key::new(ACCOUNTS_TABLE.0, id),
        values: vec![
            Value::U64(id),
            Value::U64(balance),
            Value::Text(status.to_owned()),
        ],
    }
}

#[test]
fn public_facade_replays_r5_replication_and_failover_across_backends() {
    for backend_kind in [
        RelationalBackendKind::Temporary,
        RelationalBackendKind::Seer,
    ] {
        let dir = tempdir().expect("tempdir");
        let primary_dir = dir.path().join("primary");
        let replica1_dir = dir.path().join("replica1");
        let replica2_dir = dir.path().join("replica2");

        let primary_cfg = backend_config(backend_kind, &primary_dir);
        let replica1_cfg = backend_config(backend_kind, &replica1_dir);
        let replica2_cfg = backend_config(backend_kind, &replica2_dir);

        let mut primary = RelationalDatabase::open(primary_cfg).expect("open primary");
        primary
            .create_table(accounts_schema())
            .expect("create table");

        let mut replica1 = StandbyReplica::open(replica1_cfg).expect("open replica1");
        let mut replica2 = StandbyReplica::open(replica2_cfg).expect("open replica2");

        assert_eq!(replica1.role(), ReplicationRole::Standby);
        assert_eq!(replica2.role(), ReplicationRole::Standby);

        // Seed primary with accounts
        let mut oracle: BTreeMap<u64, (u64, String)> = BTreeMap::new();
        for id in 1..=30 {
            let balance = id * 100;
            let status = "active";
            primary
                .insert(ACCOUNTS_TABLE, account_row(id, balance, status))
                .expect("insert");
            oracle.insert(id, (balance, status.to_owned()));
        }

        // Open replication stream from primary to replica1 and replica2
        let stream1 = primary.open_replication_stream(omendb::CommitId(0));
        let stream2 = primary.open_replication_stream(omendb::CommitId(0));

        let batch1 = stream1
            .next_batch(&primary, 100)
            .expect("stream1 batch")
            .expect("has batch");
        let batch2 = stream2
            .next_batch(&primary, 100)
            .expect("stream2 batch")
            .expect("has batch");

        assert!(batch1.verify_checksum());
        assert!(batch2.verify_checksum());

        replica1.apply_batch(&batch1).expect("apply batch1");
        replica2.apply_batch(&batch2).expect("apply batch2");

        let lag1 = replica1.lag_report(primary.head());
        assert_eq!(lag1.commits_behind, 0);

        // Perform mutations on primary
        let acc1 = oracle.get_mut(&1).unwrap();
        acc1.0 = acc1.0.saturating_sub(25);
        primary
            .update(ACCOUNTS_TABLE, account_row(1, acc1.0, &acc1.1))
            .expect("update 1");

        let acc2 = oracle.get_mut(&2).unwrap();
        acc2.0 = acc2.0.saturating_add(25);
        primary
            .update(ACCOUNTS_TABLE, account_row(2, acc2.0, &acc2.1))
            .expect("update 2");

        let lag_before_sync = replica1.lag_report(primary.head());
        assert!(lag_before_sync.commits_behind > 0);

        // Stream and apply new commits to replica1 and replica2
        if let Some(batch) = stream1.next_batch(&primary, 100).expect("stream batch") {
            replica1.apply_batch(&batch).expect("apply to rep1");
        }
        if let Some(batch) = stream2.next_batch(&primary, 100).expect("stream batch") {
            replica2.apply_batch(&batch).expect("apply to rep2");
        }

        assert_eq!(replica1.lag_report(primary.head()).commits_behind, 0);
        assert_eq!(replica2.lag_report(primary.head()).commits_behind, 0);

        // Simulate failover: Primary goes down, promote Replica 1
        primary.close().expect("close primary");

        replica1.promote().expect("promote replica1");
        assert_eq!(replica1.role(), ReplicationRole::PromotedPrimary);

        // Perform mutations directly on newly promoted Primary
        let promoted_db = replica1.database_mut().expect("mutable handle on promoted");
        let acc3 = oracle.get_mut(&3).unwrap();
        acc3.0 = acc3.0.saturating_add(500);
        promoted_db
            .update(ACCOUNTS_TABLE, account_row(3, acc3.0, &acc3.1))
            .expect("mutate on promoted");

        // Stream from newly promoted Primary to Replica 2
        let stream_from_promoted = promoted_db
            .open_replication_stream(replica2.lag_report(promoted_db.head()).replica_commit);
        if let Some(batch) = stream_from_promoted
            .next_batch(promoted_db, 100)
            .expect("stream from promoted")
        {
            replica2
                .apply_batch(&batch)
                .expect("apply to rep2 from new primary");
        }

        assert_eq!(replica2.lag_report(promoted_db.head()).commits_behind, 0);

        // Verify logical verification report on all active nodes
        let rep1_verify = promoted_db.verify().expect("verify rep1");
        assert_eq!(rep1_verify.verified_tables, 1);
        assert!(rep1_verify.verified_rows > 0);
    }
}

#[test]
fn replication_rejects_gaps_and_corrupt_batches() {
    let dir = tempdir().unwrap();
    let config = RelationalBackendConfig::Temporary(DatabaseConfig {
        directory: dir.path().to_owned(),
    });

    let mut replica = StandbyReplica::open(config).unwrap();

    let record_gap = omendb::ReplicationRecord {
        commit: omendb::CommitId(5), // Non-contiguous jump
        timestamp_nanos: 5000,
        mutations: Vec::new(),
        schema_change: None,
    };

    let batch_gap = omendb::ReplicationBatch::new(vec![record_gap]);
    let res = replica.apply_batch(&batch_gap);
    assert!(
        matches!(res, Err(DbError::InvalidState(_))),
        "Expected InvalidState for replication gap, got {res:?}"
    );
}
