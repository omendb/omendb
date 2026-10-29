//! Native principal identities and authorization policy. Credentials are opaque
//! to this layer; protocol adapters own their authentication algorithms.

use std::collections::BTreeMap;

use crate::{DbError, Result, TableId};

mod codec;
pub(crate) use codec::{decode, encode};

/// A principal incarnation, independent of its reusable login name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrincipalId(pub(crate) [u8; 16]);

/// Public principal metadata. Authentication material is deliberately excluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub id: PrincipalId,
    pub name: String,
}

/// The current storage adapter's canonical logical tree is allocated once per
/// table incarnation, survives rename, and is never reused after table drop.
/// This is not a physical page or extent address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TableIncarnation {
    pub(crate) table: TableId,
    pub(crate) tree: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TableGrant {
    pub(crate) read: bool,
    pub(crate) write: bool,
}

/// An owned authorization view admitted from the current committed catalog.
/// Retain it for one operation; acquire a fresh view for the next boundary.
/// Committing already-authorized work does not acquire another view.
#[derive(Debug)]
pub struct AuthorizationView {
    pub(crate) owner_id: u64,
    pub(crate) administrator: bool,
    pub(crate) grants: BTreeMap<TableIncarnation, TableGrant>,
}

impl AuthorizationView {
    pub(crate) fn require_table(
        &self,
        owner_id: u64,
        table: TableIncarnation,
        write: bool,
    ) -> Result<()> {
        if self.owner_id != owner_id {
            return Err(DbError::PermissionDenied(
                "authorization view belongs to another database handle".to_owned(),
            ));
        }
        if self.administrator
            || self
                .grants
                .get(&table)
                .is_some_and(|grant| grant.write || (!write && grant.read))
        {
            Ok(())
        } else {
            Err(DbError::PermissionDenied(format!(
                "{} access to table {} required",
                if write { "write" } else { "read" },
                table.table.0
            )))
        }
    }
}

// No Debug implementation: an opaque credential must not appear in diagnostics.
pub(crate) struct PrincipalRecord {
    pub(crate) principal: Principal,
    pub(crate) administrator: bool,
    pub(crate) credential: Option<Vec<u8>>,
    pub(crate) grants: BTreeMap<TableIncarnation, TableGrant>,
}

pub(crate) struct AuthorizationCatalog {
    pub(crate) bootstrap: bool,
    pub(crate) principals: BTreeMap<PrincipalId, PrincipalRecord>,
}

impl AuthorizationCatalog {
    pub(crate) fn bootstrap() -> Self {
        Self {
            bootstrap: true,
            principals: BTreeMap::new(),
        }
    }

    pub(crate) fn find(&self, name: &str) -> Option<&PrincipalRecord> {
        self.principals
            .values()
            .find(|record| record.principal.name == name)
    }

    pub(crate) fn record_mut(&mut self, id: PrincipalId) -> Result<&mut PrincipalRecord> {
        self.principals
            .get_mut(&id)
            .ok_or_else(|| DbError::InvalidState("principal does not exist".to_owned()))
    }

    pub(crate) fn create_principal(&mut self, name: &str) -> Result<PrincipalId> {
        validate_name(name)?;
        if self.find(name).is_some() {
            return Err(DbError::InvalidState(format!(
                "principal {name} already exists"
            )));
        }
        // Random incarnations also prevent identity transfer across databases,
        // aborted creations, and explicit reinitialization of legacy histories.
        for _ in 0..8 {
            let mut bytes = [0; 16];
            getrandom::fill(&mut bytes).map_err(|error| DbError::Io {
                operation: "generate principal identity",
                source: std::io::Error::other(error),
            })?;
            let id = PrincipalId(bytes);
            if bytes == [0; 16] || self.principals.contains_key(&id) {
                continue;
            }
            self.principals.insert(
                id,
                PrincipalRecord {
                    principal: Principal {
                        id,
                        name: name.to_owned(),
                    },
                    administrator: false,
                    credential: None,
                    grants: BTreeMap::new(),
                },
            );
            // Removing the last principal must never re-enable bootstrap.
            self.bootstrap = false;
            return Ok(id);
        }
        Err(DbError::InvalidState(
            "could not allocate a distinct principal identity".to_owned(),
        ))
    }

    pub(crate) fn rename_principal(&mut self, id: PrincipalId, name: &str) -> Result<()> {
        validate_name(name)?;
        if self
            .find(name)
            .is_some_and(|record| record.principal.id != id)
        {
            return Err(DbError::InvalidState(format!(
                "principal {name} already exists"
            )));
        }
        self.record_mut(id)?.principal.name = name.to_owned();
        Ok(())
    }

    pub(crate) fn validate_tables(&self, tables: &BTreeMap<TableId, seerdb::TreeId>) -> Result<()> {
        for record in self.principals.values() {
            for object in record.grants.keys() {
                if tables.get(&object.table).map(|tree| tree.get()) != Some(object.tree) {
                    return Err(corrupt("grant references an absent table incarnation"));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn prune_tables(&mut self, tables: &BTreeMap<TableId, seerdb::TreeId>) -> bool {
        let mut changed = false;
        for record in self.principals.values_mut() {
            record.grants.retain(|object, _| {
                let keep = tables.get(&object.table).map(|tree| tree.get()) == Some(object.tree);
                changed |= !keep;
                keep
            });
        }
        changed
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 1024 || name.contains('\0') {
        return Err(DbError::InvalidState(
            "principal name must contain 1..=1024 bytes and no NUL".to_owned(),
        ));
    }
    Ok(())
}

fn corrupt(reason: &str) -> DbError {
    DbError::Corruption {
        artifact: "native authorization catalog",
        reason: reason.to_owned(),
    }
}
