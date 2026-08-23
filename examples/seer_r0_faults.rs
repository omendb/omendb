//! Run the typed SeerDB R0 publication fault matrix.
//!
//! Every case commits one row and its unique secondary-index entry in one
//! SeerDB batch, injects one storage publication fault, drops the fenced
//! writer, and reopens the directory. Recovery may expose either the prior
//! generation or the complete new generation, but never a partial row/index
//! state.

#[cfg(not(feature = "seerdb-fault-injection"))]
fn main() {
    eprintln!("rerun with --features seerdb-fault-injection");
}

#[cfg(feature = "seerdb-fault-injection")]
mod enabled {
    use anyhow::{Context, Result, bail};
    use omendb::{
        ColumnDefinition, ColumnId, ColumnType, FaultPoint, IndexDefinition, IndexId, Key,
        RelationalMutation, Row, SeerKernelConfig, SeerRelationalStore, TableDefinition, TableId,
        Value,
    };
    use serde_json::json;

    const TABLE: TableId = TableId(7);
    const INDEX: IndexId = IndexId(9);

    const MATRIX: [FaultPoint; 12] = [
        FaultPoint::BeforeWalAppend,
        FaultPoint::AfterWalAppend,
        FaultPoint::WalSync,
        FaultPoint::AfterWalSync,
        FaultPoint::DataSync,
        FaultPoint::PackedPageSync,
        FaultPoint::ManifestMirrorSync,
        FaultPoint::ManifestSync,
        FaultPoint::AfterManifestPublish,
        FaultPoint::WalTruncate,
        FaultPoint::ShortWrite,
        FaultPoint::TornWrite,
    ];

    pub fn run() -> Result<()> {
        let mut cases = Vec::with_capacity(MATRIX.len());
        for point in MATRIX {
            cases.push(run_case(point)?);
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({"cases": cases}))?
        );
        Ok(())
    }

    fn run_case(point: FaultPoint) -> Result<serde_json::Value> {
        let directory = tempfile::tempdir().context("create temporary SeerDB directory")?;
        let path = directory.path().join("seerdb");
        let key = Key::new(7, 1);
        let config = SeerKernelConfig::new(path.clone());
        let mut store = SeerRelationalStore::create(config.clone()).context("create store")?;
        store.create_table(table())?;
        store.create_index(index())?;
        let baseline = store.commit_id();

        store.inject_fault(point)?;
        let result = store.commit_batch([RelationalMutation::Insert {
            table: TABLE,
            row: row(),
        }]);
        if result.is_ok() {
            bail!("fault {point:?} did not fail the typed commit");
        }
        drop(store);

        let reopened =
            SeerRelationalStore::open(config).with_context(|| format!("reopen after {point:?}"))?;
        let recovered = reopened.commit_id();
        let new_generation = recovered.0 == baseline.0 + 1;
        let old_generation = recovered == baseline;
        if !old_generation && !new_generation {
            bail!(
                "fault {point:?} recovered unexpected commit {} (baseline {})",
                recovered.0,
                baseline.0
            );
        }
        let row_found = reopened.get(TABLE, recovered, key)?.is_some();
        let index_rows =
            reopened.index_get(TABLE, recovered, INDEX, &[Value::Text("one".into())])?;
        let index_found = !index_rows.is_empty();
        if row_found != new_generation || index_found != new_generation {
            bail!(
                "fault {point:?} exposed partial state: commit {}, row {}, index {}",
                recovered.0,
                row_found,
                index_found
            );
        }

        Ok(json!({
            "fault": format!("{point:?}"),
            "commit_error": true,
            "baseline_commit": baseline.0,
            "recovered_commit": recovered.0,
            "complete_new_generation": new_generation,
            "row_visible": row_found,
            "index_visible": index_found,
        }))
    }

    fn table() -> TableDefinition {
        TableDefinition {
            id: TABLE,
            name: "users".into(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "email".into(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "age".into(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        }
    }

    fn index() -> IndexDefinition {
        IndexDefinition {
            id: INDEX,
            table: TABLE,
            columns: vec![ColumnId(1)],
            unique: true,
        }
    }

    fn row() -> Row {
        Row {
            primary: Key::new(7, 1),
            values: vec![Value::Text("one".into()), Value::U64(1)],
        }
    }
}

#[cfg(feature = "seerdb-fault-injection")]
fn main() -> anyhow::Result<()> {
    enabled::run()
}
