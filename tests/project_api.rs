use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, ForeignKeyDefinition, IndexDefinition,
    IndexId, Key, RelationalBackendConfig, RelationalDatabase, Row, TableDefinition, TableId,
    Value,
};
use tempfile::tempdir;

const USERS: TableId = TableId(1);
const PROJECTS: TableId = TableId(2);
const USER_ID_INDEX: IndexId = IndexId(1);
const USER_EMAIL_INDEX: IndexId = IndexId(2);

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn users_table() -> TableDefinition {
    TableDefinition {
        id: USERS,
        name: "users".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "email".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn projects_table() -> TableDefinition {
    TableDefinition {
        id: PROJECTS,
        name: "projects".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "owner_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "name".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn user(id: u64, email: &str) -> Row {
    Row {
        primary: Key::new(USERS.0, id),
        values: vec![Value::U64(id), Value::Text(email.to_owned())],
    }
}

fn project(id: u64, owner_id: u64, name: &str) -> Row {
    Row {
        primary: Key::new(PROJECTS.0, id),
        values: vec![
            Value::U64(id),
            Value::U64(owner_id),
            Value::Text(name.to_owned()),
        ],
    }
}

fn exercise_project_api(directory: &Path) {
    let database_config = config(directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    database.create_table(users_table()).expect("users table");
    database
        .create_table(projects_table())
        .expect("projects table");
    database
        .create_index(IndexDefinition {
            id: USER_ID_INDEX,
            table: USERS,
            columns: vec![ColumnId(1)],
            unique: true,
        })
        .expect("user ID index");
    database
        .create_index(IndexDefinition {
            id: USER_EMAIL_INDEX,
            table: USERS,
            columns: vec![ColumnId(2)],
            unique: true,
        })
        .expect("user email index");
    database
        .create_foreign_key(ForeignKeyDefinition {
            id: ConstraintId(1),
            table: PROJECTS,
            columns: vec![ColumnId(2)],
            referenced_table: USERS,
            referenced_columns: vec![ColumnId(1)],
            on_delete: omendb::ReferentialAction::default(),
            timing: omendb::ConstraintTiming::default(),
        })
        .expect("project owner foreign key");

    let (indexed_users, _seed_commit) = database
        .transaction(|database, transaction| {
            transaction.insert(database, USERS, user(1, "alice@example.test"))?;
            transaction.insert(database, PROJECTS, project(7, 1, "alpha"))?;
            transaction.index_get(
                database,
                USERS,
                USER_EMAIL_INDEX,
                &[Value::Text("alice@example.test".to_owned())],
            )
        })
        .expect("seed transaction");
    assert_eq!(indexed_users, vec![user(1, "alice@example.test")]);

    let update_commit = database
        .update(PROJECTS, project(7, 1, "renamed"))
        .expect("update project");

    database.close().expect("close");

    let reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_eq!(reopened.commit_id(), update_commit);
    assert_eq!(
        reopened
            .index_get(
                USERS,
                USER_EMAIL_INDEX,
                &[Value::Text("alice@example.test".to_owned())],
            )
            .expect("reopened indexed user"),
        vec![user(1, "alice@example.test")]
    );
    assert_eq!(
        reopened.scan(PROJECTS, 10).expect("reopened project scan"),
        vec![project(7, 1, "renamed")]
    );
    reopened.close().expect("reopened close");
}

#[test]
fn project_facing_api_supports_an_ordinary_transactional_lifecycle() {
    let temporary = tempdir().expect("temporary directory");
    exercise_project_api(&temporary.path().join("temporary"));
}
