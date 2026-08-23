use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, DatabaseConfig, ForeignKeyDefinition,
    IndexDefinition, IndexId, Key, NoFaults, RelationalMutation, RelationalStore, Row,
    TableDefinition, TableId, Value,
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

mod support;

const USERS_TABLE: TableId = TableId(1);
const PROJECTS_TABLE: TableId = TableId(2);
const MEMBERSHIPS_TABLE: TableId = TableId(3);
const USER_PRIMARY_INDEX: IndexId = IndexId(2);
const PROJECT_PRIMARY_INDEX: IndexId = IndexId(3);
const PROJECT_SLUG_INDEX: IndexId = IndexId(1);
const PROJECT_OWNER_FK: ConstraintId = ConstraintId(1);
const MEMBERSHIP_PROJECT_FK: ConstraintId = ConstraintId(2);
const MEMBERSHIP_USER_FK: ConstraintId = ConstraintId(3);

#[derive(Debug, Default, Deserialize)]
struct TraceEvent {
    seq: u64,
    kind: String,
    tenant_id: Option<u64>,
    query: Option<String>,
    slug: Option<String>,
    expect: Option<String>,
    expect_project_id: Option<u64>,
    operations: Option<Vec<Operation>>,
}

#[derive(Debug, Deserialize)]
struct Operation {
    op: String,
    tenant_id: Option<u64>,
    user_id: Option<u64>,
    project_id: Option<u64>,
    slug: Option<String>,
    owner_id: Option<u64>,
}

#[derive(Debug, Default)]
struct Keys {
    users: BTreeSet<Key>,
    projects: BTreeSet<Key>,
    memberships: BTreeSet<Key>,
}

fn users_table() -> TableDefinition {
    TableDefinition {
        id: USERS_TABLE,
        name: "r1_users".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "user_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "email".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn projects_table() -> TableDefinition {
    TableDefinition {
        id: PROJECTS_TABLE,
        name: "r1_projects".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "project_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "slug".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "owner_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn memberships_table() -> TableDefinition {
    TableDefinition {
        id: MEMBERSHIPS_TABLE,
        name: "r1_memberships".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "project_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "user_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "role".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn main() -> Result<()> {
    let mut arguments = env::args().skip(1);
    let trace_path = arguments
        .next()
        .map(PathBuf::from)
        .context("usage: r1_replay <trace.jsonl> [--expected-digest <sha256>]")?;
    let mut expected_digest = None;
    while let Some(argument) = arguments.next() {
        if argument == "--expected-digest" {
            expected_digest = Some(
                arguments
                    .next()
                    .context("--expected-digest requires a SHA-256 value")?,
            );
        } else {
            bail!("unknown argument {argument}");
        }
    }

    let directory = tempfile::tempdir().context("create temporary OmenDB directory")?;
    let mut store = RelationalStore::create(DatabaseConfig {
        directory: directory.path().to_path_buf(),
    })
    .context("create OmenDB relational store")?;
    let checkpoint_each_commit = support::checkpoint_each_commit();
    store
        .create_table(users_table())
        .context("create users table")?;
    store
        .create_table(projects_table())
        .context("create projects table")?;
    store
        .create_table(memberships_table())
        .context("create memberships table")?;
    let mut faults = NoFaults;
    let setup_started = Instant::now();
    store
        .create_index(
            IndexDefinition {
                id: USER_PRIMARY_INDEX,
                table: USERS_TABLE,
                columns: vec![ColumnId(1), ColumnId(2)],
                unique: true,
            },
            &mut faults,
        )
        .context("create user primary index")?;
    store
        .create_index(
            IndexDefinition {
                id: PROJECT_PRIMARY_INDEX,
                table: PROJECTS_TABLE,
                columns: vec![ColumnId(1), ColumnId(2)],
                unique: true,
            },
            &mut faults,
        )
        .context("create project primary index")?;
    let setup_commit = store
        .create_index(
            IndexDefinition {
                id: PROJECT_SLUG_INDEX,
                table: PROJECTS_TABLE,
                columns: vec![ColumnId(1), ColumnId(3)],
                unique: true,
            },
            &mut faults,
        )
        .context("create project tenant/slug index")?;
    let mut metrics = support::ReplayMetrics::default();
    for foreign_key in r1_foreign_keys() {
        store
            .create_foreign_key(foreign_key)
            .context("create R1 foreign key")?;
    }
    metrics.record("setup_index", setup_started);
    if checkpoint_each_commit {
        let started = Instant::now();
        store
            .checkpoint(&mut faults)
            .context("checkpoint R1 setup")?;
        metrics.record("setup_checkpoint", started);
    }
    let replay_started = Instant::now();
    let mut keys = Keys::default();
    let mut expected_seq = 1_u64;
    let mut trace_commits = 0_u64;
    let mut results = Vec::new();

    for (line_number, line) in fs::read_to_string(&trace_path)
        .with_context(|| format!("read {}", trace_path.display()))?
        .lines()
        .enumerate()
    {
        if line.trim().is_empty() {
            continue;
        }
        let event: TraceEvent = serde_json::from_str(line)
            .with_context(|| format!("parse {} line {}", trace_path.display(), line_number + 1))?;
        if event.seq != expected_seq {
            bail!("trace sequence {} != {}", event.seq, expected_seq);
        }
        expected_seq += 1;
        match event.kind.as_str() {
            "commit" => {
                apply_commit(
                    &mut store,
                    &mut faults,
                    &mut keys,
                    &event,
                    &mut metrics,
                    checkpoint_each_commit,
                )?;
                trace_commits += 1;
            }
            "read" => {
                let started = Instant::now();
                results.push(read_project(&store, &event)?);
                metrics.record("read", started);
            }
            "backup_verify" => {
                if event.expect.as_deref() != Some("same-state-digest") {
                    bail!("unsupported backup expectation");
                }
                let started = Instant::now();
                let backup_digest = state_digest(&store, &keys)?;
                metrics.record("backup_verify", started);
                results.push(json!({
                    "seq": event.seq,
                    "backup_digest": backup_digest,
                }));
            }
            kind => bail!("unsupported R1 event {kind}"),
        }
    }

    metrics.add_storage(store.metrics());
    let started = Instant::now();
    let digest = state_digest(&store, &keys)?;
    metrics.record("state_digest", started);
    metrics.record("replay_total", replay_started);
    if let Some(expected) = expected_digest
        && expected != digest
    {
        bail!("state digest {digest} != expected {expected}");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "trace": trace_path.file_name().and_then(|name| name.to_str()),
            "events": expected_seq - 1,
            "trace_commits": trace_commits,
            "setup_commit_id": setup_commit.0,
            "storage_commit_id": store.commit_id().0,
            "state_digest": digest,
            "commit_durability": if checkpoint_each_commit {
                "wal_sync_plus_checkpoint_each_commit"
            } else {
                "wal_sync_until_explicit_checkpoint"
            },
            "results": results,
            "metrics": metrics.json(),
        }))?
    );
    Ok(())
}

fn r1_foreign_keys() -> [ForeignKeyDefinition; 3] {
    [
        ForeignKeyDefinition {
            id: PROJECT_OWNER_FK,
            table: PROJECTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(4)],
            referenced_table: USERS_TABLE,
            referenced_columns: vec![ColumnId(1), ColumnId(2)],
            on_delete: omendb::ReferentialAction::default(),
            timing: omendb::ConstraintTiming::default(),
        },
        ForeignKeyDefinition {
            id: MEMBERSHIP_PROJECT_FK,
            table: MEMBERSHIPS_TABLE,
            columns: vec![ColumnId(1), ColumnId(2)],
            referenced_table: PROJECTS_TABLE,
            referenced_columns: vec![ColumnId(1), ColumnId(2)],
            on_delete: omendb::ReferentialAction::default(),
            timing: omendb::ConstraintTiming::default(),
        },
        ForeignKeyDefinition {
            id: MEMBERSHIP_USER_FK,
            table: MEMBERSHIPS_TABLE,
            columns: vec![ColumnId(1), ColumnId(3)],
            referenced_table: USERS_TABLE,
            referenced_columns: vec![ColumnId(1), ColumnId(2)],
            on_delete: omendb::ReferentialAction::default(),
            timing: omendb::ConstraintTiming::default(),
        },
    ]
}

fn apply_commit(
    store: &mut RelationalStore,
    faults: &mut NoFaults,
    keys: &mut Keys,
    event: &TraceEvent,
    metrics: &mut support::ReplayMetrics,
    checkpoint_each_commit: bool,
) -> Result<()> {
    let tenant = event.tenant_id.context("R1 commit has no tenant_id")?;
    let operations = event
        .operations
        .as_ref()
        .context("R1 commit has no operations")?;
    let mut mutations = Vec::new();
    let mut added = Keys::default();
    for operation in operations {
        let operation_tenant = operation.tenant_id.unwrap_or(tenant);
        match operation.op.as_str() {
            "create_user" => {
                let user_id = operation.user_id.context("create_user has no user_id")?;
                let key = Key::new(USERS_TABLE.0, packed_pair(operation_tenant, user_id)?);
                mutations.push(RelationalMutation::Insert {
                    table: USERS_TABLE,
                    row: Row {
                        primary: key,
                        values: vec![
                            Value::U64(operation_tenant),
                            Value::U64(user_id),
                            Value::Text(format!("user-{operation_tenant}-{user_id}@example.test")),
                        ],
                    },
                });
                added.users.insert(key);
            }
            "create_project" => {
                let project_id = operation
                    .project_id
                    .context("create_project has no project_id")?;
                let slug = operation
                    .slug
                    .as_deref()
                    .context("create_project has no slug")?;
                let owner_id = operation
                    .owner_id
                    .context("create_project has no owner_id")?;
                let key = Key::new(PROJECTS_TABLE.0, packed_pair(operation_tenant, project_id)?);
                mutations.push(RelationalMutation::Insert {
                    table: PROJECTS_TABLE,
                    row: project_row(key, operation_tenant, project_id, slug, owner_id),
                });
                added.projects.insert(key);
            }
            "add_membership" => {
                let project_id = operation
                    .project_id
                    .context("add_membership has no project_id")?;
                let user_id = operation.user_id.context("add_membership has no user_id")?;
                let key = Key::new(
                    MEMBERSHIPS_TABLE.0,
                    packed_membership(operation_tenant, project_id, user_id)?,
                );
                mutations.push(RelationalMutation::Insert {
                    table: MEMBERSHIPS_TABLE,
                    row: Row {
                        primary: key,
                        values: vec![
                            Value::U64(operation_tenant),
                            Value::U64(project_id),
                            Value::U64(user_id),
                            Value::Text("member".to_owned()),
                        ],
                    },
                });
                added.memberships.insert(key);
            }
            "remove_membership" => {
                let project_id = operation
                    .project_id
                    .context("remove_membership has no project_id")?;
                let user_id = operation
                    .user_id
                    .context("remove_membership has no user_id")?;
                let key = Key::new(
                    MEMBERSHIPS_TABLE.0,
                    packed_membership(operation_tenant, project_id, user_id)?,
                );
                mutations.push(RelationalMutation::Delete {
                    table: MEMBERSHIPS_TABLE,
                    primary: key,
                });
            }
            "rename_project" => {
                let project_id = operation
                    .project_id
                    .context("rename_project has no project_id")?;
                let slug = operation
                    .slug
                    .as_deref()
                    .context("rename_project has no slug")?;
                let key = Key::new(PROJECTS_TABLE.0, packed_pair(operation_tenant, project_id)?);
                let previous = store
                    .get(PROJECTS_TABLE, store.commit_id(), key)?
                    .context("rename_project targets an absent project")?;
                let owner_id = value_u64(&previous, 3)?;
                mutations.push(RelationalMutation::Update {
                    table: PROJECTS_TABLE,
                    row: project_row(key, operation_tenant, project_id, slug, owner_id),
                });
            }
            operation => bail!("unsupported R1 operation {operation}"),
        }
    }
    let started = Instant::now();
    store
        .commit_batch(mutations, faults)
        .context("commit R1 event")?;
    metrics.record("commit", started);
    if checkpoint_each_commit {
        let started = Instant::now();
        store.checkpoint(faults).context("checkpoint R1 event")?;
        metrics.record("commit_checkpoint", started);
    }
    keys.users.extend(added.users);
    keys.projects.extend(added.projects);
    keys.memberships.extend(added.memberships);
    Ok(())
}

fn read_project(store: &RelationalStore, event: &TraceEvent) -> Result<serde_json::Value> {
    if event.query.as_deref() != Some("project_by_slug") {
        bail!("unsupported R1 read query");
    }
    let tenant = event.tenant_id.context("R1 read has no tenant_id")?;
    let slug = event.slug.as_deref().context("R1 read has no slug")?;
    let matches = store.index_get(
        PROJECTS_TABLE,
        store.commit_id(),
        PROJECT_SLUG_INDEX,
        &[Value::U64(tenant), Value::Text(slug.to_owned())],
    )?;
    if let Some(expected) = event.expect_project_id {
        if matches.len() != 1 {
            bail!(
                "R1 read expected project {expected}, got {} matches",
                matches.len()
            );
        }
        if value_u64(&matches[0], 1)? != expected {
            bail!("R1 read expected project {expected}");
        }
    } else if event.expect.as_deref() == Some("not_found") && !matches.is_empty() {
        bail!("R1 read expected no rows");
    }
    Ok(json!({
        "seq": event.seq,
        "project_ids": matches.iter().map(|row| value_u64(row, 1)).collect::<Result<Vec<_>>>()?,
    }))
}

fn state_digest(store: &RelationalStore, keys: &Keys) -> Result<String> {
    let snapshot = store.commit_id();
    let mut canonical = String::new();
    for key in &keys.users {
        if let Some(row) = store.get(USERS_TABLE, snapshot, *key)? {
            canonical.push_str(&format!(
                "user|{}|{}\n",
                value_u64(&row, 0)?,
                value_u64(&row, 1)?
            ));
        }
    }
    for key in &keys.projects {
        if let Some(row) = store.get(PROJECTS_TABLE, snapshot, *key)? {
            canonical.push_str(&format!(
                "project|{}|{}|{}|{}\n",
                value_u64(&row, 0)?,
                value_u64(&row, 1)?,
                value_text(&row, 2)?,
                value_u64(&row, 3)?,
            ));
        }
    }
    for key in &keys.memberships {
        if let Some(row) = store.get(MEMBERSHIPS_TABLE, snapshot, *key)? {
            canonical.push_str(&format!(
                "membership|{}|{}|{}\n",
                value_u64(&row, 0)?,
                value_u64(&row, 1)?,
                value_u64(&row, 2)?,
            ));
        }
    }
    let mut digest = Sha256::new();
    digest.update(canonical.as_bytes());
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn project_row(key: Key, tenant: u64, project: u64, slug: &str, owner: u64) -> Row {
    Row {
        primary: key,
        values: vec![
            Value::U64(tenant),
            Value::U64(project),
            Value::Text(slug.to_owned()),
            Value::U64(owner),
        ],
    }
}

fn value_u64(row: &Row, position: usize) -> Result<u64> {
    match row.values.get(position) {
        Some(Value::U64(value)) => Ok(*value),
        other => bail!("expected U64 at column {position}, got {other:?}"),
    }
}

fn value_text(row: &Row, position: usize) -> Result<&str> {
    match row.values.get(position) {
        Some(Value::Text(value)) => Ok(value),
        other => bail!("expected Text at column {position}, got {other:?}"),
    }
}

fn packed_pair(tenant: u64, id: u64) -> Result<u64> {
    if tenant > u64::from(u32::MAX) || id > u64::from(u32::MAX) {
        bail!("R1 composite ID exceeds the smoke adapter bounds");
    }
    Ok((tenant << 32) | id)
}

fn packed_membership(tenant: u64, project: u64, user: u64) -> Result<u64> {
    if tenant > 0xffff || project > 0xffffff || user > 0xffffff {
        bail!("R1 membership ID exceeds the smoke adapter bounds");
    }
    Ok((tenant << 48) | (project << 24) | user)
}
