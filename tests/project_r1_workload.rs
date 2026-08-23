use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use omendb::{
    CommitId, RelationalBackendKind, RelationalDatabase, RelationalDatabaseTransaction,
    RelationalSnapshotLease, Row, Value,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

mod support;

use support::{
    MEMBERSHIPS_TABLE, PROJECT_SLUG_INDEX, PROJECTS_TABLE, USERS_TABLE, config, install_schema,
    membership_key, membership_row, pair_key, project_row, user_row,
};

const EXPECTED_R1_DIGEST: &str = "d8b2f8f31a865c4f2af7214dd6b21ebae5023de495338e5f4e810e76e96c027d";
const R1_TRACE: &str =
    include_str!("fixtures/r1-ordinary-oltp-trace.jsonl");

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Model {
    users: BTreeSet<(u64, u64)>,
    projects: BTreeMap<(u64, u64), Project>,
    memberships: BTreeSet<(u64, u64, u64)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Project {
    slug: String,
    owner_id: u64,
}

#[derive(Debug, Deserialize)]
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

fn stage_operation(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    tenant: u64,
    operation: &Operation,
) {
    let tenant = operation.tenant_id.unwrap_or(tenant);
    match operation.op.as_str() {
        "create_user" => transaction
            .insert(
                database,
                USERS_TABLE,
                user_row(tenant, operation.user_id.expect("user ID")),
            )
            .expect("create user"),
        "create_project" => transaction
            .insert(
                database,
                PROJECTS_TABLE,
                project_row(
                    tenant,
                    operation.project_id.expect("project ID"),
                    operation.slug.as_deref().expect("project slug"),
                    operation.owner_id.expect("project owner"),
                ),
            )
            .expect("create project"),
        "add_membership" => transaction
            .insert(
                database,
                MEMBERSHIPS_TABLE,
                membership_row(
                    tenant,
                    operation.project_id.expect("project ID"),
                    operation.user_id.expect("user ID"),
                ),
            )
            .expect("add membership"),
        "remove_membership" => transaction
            .delete(
                database,
                MEMBERSHIPS_TABLE,
                membership_key(
                    tenant,
                    operation.project_id.expect("project ID"),
                    operation.user_id.expect("user ID"),
                ),
            )
            .expect("remove membership"),
        "rename_project" => {
            let project_id = operation.project_id.expect("project ID");
            let key = pair_key(PROJECTS_TABLE, tenant, project_id);
            let previous = transaction
                .get(database, PROJECTS_TABLE, key)
                .expect("read project for rename")
                .expect("rename target");
            let owner_id = match previous.values.get(3) {
                Some(Value::U64(value)) => *value,
                other => panic!("project owner is not U64: {other:?}"),
            };
            transaction
                .update(
                    database,
                    PROJECTS_TABLE,
                    project_row(
                        tenant,
                        project_id,
                        operation.slug.as_deref().expect("new project slug"),
                        owner_id,
                    ),
                )
                .expect("rename project");
        }
        operation => panic!("unsupported R1 operation: {operation}"),
    }
}

fn apply_model(model: &mut Model, tenant: u64, operation: &Operation) {
    let tenant = operation.tenant_id.unwrap_or(tenant);
    match operation.op.as_str() {
        "create_user" => assert!(
            model
                .users
                .insert((tenant, operation.user_id.expect("user ID")))
        ),
        "create_project" => {
            let project = operation.project_id.expect("project ID");
            let slug = operation.slug.as_deref().expect("project slug");
            let owner_id = operation.owner_id.expect("project owner");
            assert!(model.users.contains(&(tenant, owner_id)));
            assert!(model.projects.iter().all(|((candidate_tenant, _), value)| {
                *candidate_tenant != tenant || value.slug != slug
            }));
            assert!(
                model
                    .projects
                    .insert(
                        (tenant, project),
                        Project {
                            slug: slug.to_owned(),
                            owner_id,
                        },
                    )
                    .is_none()
            );
        }
        "add_membership" => {
            let project = operation.project_id.expect("project ID");
            let user = operation.user_id.expect("user ID");
            assert!(model.projects.contains_key(&(tenant, project)));
            assert!(model.users.contains(&(tenant, user)));
            assert!(model.memberships.insert((tenant, project, user)));
        }
        "remove_membership" => assert!(model.memberships.remove(&(
            tenant,
            operation.project_id.expect("project ID"),
            operation.user_id.expect("user ID"),
        ))),
        "rename_project" => {
            let project = operation.project_id.expect("project ID");
            let slug = operation.slug.as_deref().expect("new project slug");
            assert!(
                model
                    .projects
                    .iter()
                    .all(|(key, value)| { *key == (tenant, project) || value.slug != slug })
            );
            model
                .projects
                .get_mut(&(tenant, project))
                .expect("rename target")
                .slug = slug.to_owned();
        }
        operation => panic!("unsupported R1 operation: {operation}"),
    }
}

fn model_rows(model: &Model, table: omendb::TableId) -> Vec<Row> {
    match table {
        USERS_TABLE => model
            .users
            .iter()
            .map(|(tenant, user)| user_row(*tenant, *user))
            .collect(),
        PROJECTS_TABLE => model
            .projects
            .iter()
            .map(|((tenant, project), value)| {
                project_row(*tenant, *project, &value.slug, value.owner_id)
            })
            .collect(),
        MEMBERSHIPS_TABLE => model
            .memberships
            .iter()
            .map(|(tenant, project, user)| membership_row(*tenant, *project, *user))
            .collect(),
        table => panic!("unsupported model table {table:?}"),
    }
}

fn model_digest(model: &Model) -> String {
    let mut canonical = String::new();
    for (tenant, user) in &model.users {
        canonical.push_str(&format!("user|{tenant}|{user}\n"));
    }
    for ((tenant, project), value) in &model.projects {
        canonical.push_str(&format!(
            "project|{tenant}|{project}|{}|{}\n",
            value.slug, value.owner_id
        ));
    }
    for (tenant, project, user) in &model.memberships {
        canonical.push_str(&format!("membership|{tenant}|{project}|{user}\n"));
    }
    let digest = Sha256::digest(canonical.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn assert_database_matches(database: &RelationalDatabase, model: &Model, snapshot: CommitId) {
    for table in [USERS_TABLE, PROJECTS_TABLE, MEMBERSHIPS_TABLE] {
        assert_eq!(
            database.scan(table, snapshot, usize::MAX).expect("scan"),
            model_rows(model, table),
            "table {table:?} diverged at snapshot {snapshot:?}"
        );
    }
}

fn exercise_public_r1(kind: RelationalBackendKind, directory: &Path) {
    let database_config = config(kind, directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    install_schema(&mut database);

    let mut model = Model::default();
    let mut retained_model = None;
    let mut retained_lease: Option<RelationalSnapshotLease> = None;
    let mut retained_commit = None;
    let mut expected_seq = 1;

    for line in R1_TRACE.lines().filter(|line| !line.trim().is_empty()) {
        let event: TraceEvent = serde_json::from_str(line).expect("trace event");
        assert_eq!(event.seq, expected_seq);
        expected_seq += 1;
        match event.kind.as_str() {
            "commit" => {
                let tenant = event.tenant_id.expect("commit tenant");
                let operations = event.operations.as_ref().expect("commit operations");
                let mut candidate = model.clone();
                for operation in operations {
                    apply_model(&mut candidate, tenant, operation);
                }
                let (_, commit) = database
                    .transaction(|database, transaction| {
                        for operation in operations {
                            stage_operation(database, transaction, tenant, operation);
                        }
                        Ok(())
                    })
                    .expect("commit R1 event");
                model = candidate;
                assert_database_matches(&database, &model, commit);
                if retained_lease.is_none() {
                    retained_model = Some(model.clone());
                    retained_commit = Some(commit);
                    retained_lease = Some(database.retain(commit).expect("retain seed"));
                }
            }
            "read" => {
                assert_eq!(event.query.as_deref(), Some("project_by_slug"));
                let tenant = event.tenant_id.expect("read tenant");
                let slug = event.slug.as_deref().expect("read slug");
                let rows = database
                    .index_get(
                        PROJECTS_TABLE,
                        database.commit_id(),
                        PROJECT_SLUG_INDEX,
                        &[Value::U64(tenant), Value::Text(slug.to_owned())],
                    )
                    .expect("project slug lookup");
                let project_ids = rows
                    .iter()
                    .map(|row| match row.values.get(1) {
                        Some(Value::U64(value)) => *value,
                        other => panic!("project ID is not U64: {other:?}"),
                    })
                    .collect::<Vec<_>>();
                if let Some(expected) = event.expect_project_id {
                    assert_eq!(project_ids, vec![expected]);
                } else {
                    assert_eq!(event.expect.as_deref(), Some("not_found"));
                    assert!(project_ids.is_empty());
                }
            }
            "backup_verify" => {
                assert_eq!(event.expect.as_deref(), Some("same-state-digest"));
                assert_eq!(model_digest(&model), EXPECTED_R1_DIGEST);
            }
            kind => panic!("unsupported R1 event {kind}"),
        }
    }

    assert_eq!(expected_seq - 1, 6);
    assert_eq!(model_digest(&model), EXPECTED_R1_DIGEST);
    let current = database.commit_id();
    assert_database_matches(&database, &model, current);
    let retained_model = retained_model.expect("retained model");
    let retained_commit = retained_commit.expect("retained commit");
    assert_database_matches(&database, &retained_model, retained_commit);

    database.verify().expect("logical verification");
    database.checkpoint().expect("checkpoint");
    database.compact().expect("compact");
    assert_database_matches(&database, &model, current);
    assert_database_matches(&database, &retained_model, retained_commit);
    database
        .release(retained_lease.take().expect("release lease"))
        .expect("release retained snapshot");
    assert!(
        database
            .scan(PROJECTS_TABLE, retained_commit, usize::MAX)
            .is_err()
    );
    database.close().expect("close");

    let mut reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_eq!(reopened.commit_id(), current);
    assert_database_matches(&reopened, &model, current);
    assert_eq!(model_digest(&model), EXPECTED_R1_DIGEST);
    assert_eq!(
        reopened
            .index_get(
                PROJECTS_TABLE,
                current,
                PROJECT_SLUG_INDEX,
                &[Value::U64(1), Value::Text("beta".to_owned())],
            )
            .expect("reopened project lookup")
            .len(),
        1
    );
    reopened.verify().expect("reopened verification");
    reopened.close().expect("reopened close");
}

#[test]
fn public_facade_replays_canonical_r1_across_selected_backends() {
    let temporary = tempdir().expect("temporary directory");
    exercise_public_r1(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise_public_r1(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
