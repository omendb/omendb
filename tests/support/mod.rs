use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, DatabaseConfig, ForeignKeyDefinition,
    IndexDefinition, IndexId, Key, RelationalBackendConfig, RelationalBackendKind,
    RelationalDatabase, Row, SeerKernelConfig, TableDefinition, TableId, Value,
};

pub const USERS_TABLE: TableId = TableId(1);
pub const PROJECTS_TABLE: TableId = TableId(2);
pub const MEMBERSHIPS_TABLE: TableId = TableId(3);
pub const PROJECT_SLUG_INDEX: IndexId = IndexId(1);
pub const USER_PRIMARY_INDEX: IndexId = IndexId(2);
pub const PROJECT_PRIMARY_INDEX: IndexId = IndexId(3);
pub const MEMBERSHIP_USER_INDEX: IndexId = IndexId(4);
pub const PROJECT_OWNER_FK: ConstraintId = ConstraintId(1);
pub const MEMBERSHIP_PROJECT_FK: ConstraintId = ConstraintId(2);
pub const MEMBERSHIP_USER_FK: ConstraintId = ConstraintId(3);

pub fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    }
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

pub fn install_schema(database: &mut RelationalDatabase) {
    database.create_table(users_table()).expect("users table");
    database
        .create_table(projects_table())
        .expect("projects table");
    database
        .create_table(memberships_table())
        .expect("memberships table");
    for index in [
        IndexDefinition {
            id: USER_PRIMARY_INDEX,
            table: USERS_TABLE,
            columns: vec![ColumnId(1), ColumnId(2)],
            unique: true,
        },
        IndexDefinition {
            id: PROJECT_PRIMARY_INDEX,
            table: PROJECTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(2)],
            unique: true,
        },
        IndexDefinition {
            id: PROJECT_SLUG_INDEX,
            table: PROJECTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(3)],
            unique: true,
        },
        IndexDefinition {
            id: MEMBERSHIP_USER_INDEX,
            table: MEMBERSHIPS_TABLE,
            columns: vec![ColumnId(1), ColumnId(3)],
            unique: false,
        },
    ] {
        database.create_index(index).expect("index");
    }
    for foreign_key in [
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
    ] {
        database
            .create_foreign_key(foreign_key)
            .expect("foreign key");
    }
}

pub fn pair_key(table: TableId, tenant: u64, id: u64) -> Key {
    assert!(tenant <= u64::from(u32::MAX));
    assert!(id <= u64::from(u32::MAX));
    Key::new(table.0, (tenant << 32) | id)
}

pub fn membership_key(tenant: u64, project: u64, user: u64) -> Key {
    assert!(tenant <= 0xffff);
    assert!(project <= 0xffffff);
    assert!(user <= 0xffffff);
    Key::new(MEMBERSHIPS_TABLE.0, (tenant << 48) | (project << 24) | user)
}

pub fn user_row(tenant: u64, user: u64) -> Row {
    Row {
        primary: pair_key(USERS_TABLE, tenant, user),
        values: vec![
            Value::U64(tenant),
            Value::U64(user),
            Value::Text(format!("user-{tenant}-{user}@example.test")),
        ],
    }
}

pub fn project_row(tenant: u64, project: u64, slug: &str, owner_id: u64) -> Row {
    Row {
        primary: pair_key(PROJECTS_TABLE, tenant, project),
        values: vec![
            Value::U64(tenant),
            Value::U64(project),
            Value::Text(slug.to_owned()),
            Value::U64(owner_id),
        ],
    }
}

pub fn membership_row(tenant: u64, project: u64, user: u64) -> Row {
    Row {
        primary: membership_key(tenant, project, user),
        values: vec![
            Value::U64(tenant),
            Value::U64(project),
            Value::U64(user),
            Value::Text("member".to_owned()),
        ],
    }
}
