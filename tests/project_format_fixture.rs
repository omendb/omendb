//! On-disk format fixtures: prove reopen across commits and toolchains.
//!
//! [`FORMAT_FIXTURES`] names one minimal database per backend, generated at
//! the current on-disk format by `generate_current_format_fixtures`
//! (`cargo test --test project_format_fixture -- --ignored --nocapture`) and
//! committed under `tests/fixtures/format-current/`. The consumer test opens
//! each committed fixture and asserts its logical content plus integrity.
//!
//! When an intentional format change lands, regenerate the fixture in the
//! same PR and move any compatibility expectations here explicitly. A
//! failing consumer test without regeneration means an unintentional,
//! silently breaking format change.

use std::path::{Path, PathBuf};

use omendb::{
    DatabaseConfig, RelationalBackendConfig, RelationalBackendKind, RelationalDatabase,
    SeerKernelConfig,
};
use tempfile::tempdir;

const FIXTURE_ROOT: &str = "tests/fixtures/format-current";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT)
}

fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    }
}

/// The exact logical state every fixture must carry. Keep this small and
/// stable: it is the contract future formats are checked against.
const SEED_STATEMENT: &str = "INSERT INTO inventory VALUES (1, 'widget', 10), (2, 'gadget', 0)";
const EXPECTED_ROWS: &[(i64, &str, i64)] = &[(1, "widget", 10), (2, "gadget", 0)];

fn create_database(kind: RelationalBackendKind, directory: &Path) -> RelationalDatabase {
    let mut database = RelationalDatabase::create(config(kind, directory))
        .unwrap_or_else(|error| panic!("{kind:?}: create fixture database: {error:?}"));
    database
        .execute_sql("CREATE TABLE inventory (id BIGINT PRIMARY KEY, label TEXT NOT NULL, count BIGINT NOT NULL)")
        .expect("create fixture table");
    database.execute_sql(SEED_STATEMENT).expect("seed fixture");
    database
        .execute_sql("CREATE INDEX inventory_label_idx ON inventory (label)")
        .expect("create fixture index");
    database
}

fn assert_fixture_content(database: &mut RelationalDatabase, kind: RelationalBackendKind) {
    let rows = database
        .execute_sql("SELECT id, label, count FROM inventory ORDER BY id")
        .expect("read fixture rows")
        .rows;
    let expected: Vec<Vec<omendb::Value>> = EXPECTED_ROWS
        .iter()
        .map(|(id, label, count)| {
            vec![
                omendb::Value::I64(*id),
                omendb::Value::Text((*label).to_owned()),
                omendb::Value::I64(*count),
            ]
        })
        .collect();
    assert_eq!(rows, expected, "{kind:?} fixture content drifted");

    let indexed = database
        .execute_sql("SELECT id FROM inventory WHERE label = 'gadget'")
        .expect("fixture index lookup")
        .rows;
    assert_eq!(indexed, vec![vec![omendb::Value::I64(2)]]);

    let report = database.verify().expect("verify fixture");
    assert_eq!(report.verified_rows, EXPECTED_ROWS.len() as u64);
    assert_eq!(
        report.verified_indexes, 2,
        "fixture must include the primary key and the secondary index"
    );
}

#[test]
fn current_format_fixtures_reopen_with_expected_content() {
    for (kind, name) in [
        (RelationalBackendKind::Temporary, "temporary"),
        (RelationalBackendKind::Seer, "seer"),
    ] {
        let path = fixture_root().join(name);
        assert!(
            path.exists(),
            "missing {kind:?} format fixture at {}; regenerate with `cargo test --test project_format_fixture -- --ignored --nocapture` and commit it",
            path.display()
        );
        // Open through a copy so concurrent test runs never mutate the
        // committed artifact.
        let workspace = tempdir().expect("fixture workspace");
        let working = workspace.path().join(name);
        copy_tree(&path, &working);
        let mut database = RelationalDatabase::open(config(kind, &working))
            .unwrap_or_else(|error| panic!("{kind:?}: open committed fixture: {error:?}"));
        assert_fixture_content(&mut database, kind);
        database.close().expect("close fixture");
    }
}

#[ignore = "generator: writes committed fixtures; run explicitly"]
#[test]
fn generate_current_format_fixtures() {
    for (kind, name) in [
        (RelationalBackendKind::Temporary, "temporary"),
        (RelationalBackendKind::Seer, "seer"),
    ] {
        let root = tempdir().expect("generator root");
        let source = root.path().join(name);
        let mut database = create_database(kind, &source);
        database.verify().expect("verify generated fixture");
        database.close().expect("close generated fixture");

        let target = fixture_root().join(name);
        if target.exists() {
            std::fs::remove_dir_all(&target).expect("remove stale fixture");
        }
        std::fs::create_dir_all(&target).expect("create fixture directory");
        copy_tree(&source, &target);
        println!("wrote {:?} fixture to {}", kind, target.display());
    }
}

fn copy_tree(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).expect("create destination directory");
    for entry in std::fs::read_dir(source).expect("read source tree") {
        let entry = entry.expect("source entry");
        let destination = target.join(entry.file_name());
        if entry.file_type().expect("entry type").is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            std::fs::copy(entry.path(), &destination).expect("copy file");
        }
    }
}
