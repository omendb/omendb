use super::*;
use crate::authorization::{
    AuthorizationView, Principal, PrincipalId, TableGrant, TableIncarnation,
};

impl RelationalDatabase {
    /// Initialize a pre-authorization history through a trusted native caller.
    /// Existing or malformed authority is never overwritten. Legacy wire tables
    /// must be removed and their credentials/grants explicitly reprovisioned.
    pub fn initialize_authorization(&mut self) -> Result<CommitId> {
        self.store
            .initialize_authorization()
            .map(|commit| CommitId(commit.get()))
    }

    /// Return explicit bootstrap eligibility, refusing uninitialized or legacy
    /// authority. This is not inferred from principal or credential counts.
    pub fn authorization_bootstrap_enabled(&self) -> Result<bool> {
        self.store.ensure_no_legacy_authorization()?;
        Ok(self.store.authorization_catalog()?.bootstrap)
    }

    /// Find current committed principal metadata by its exact login name.
    pub fn principal(&self, name: &str) -> Result<Option<Principal>> {
        Ok(self
            .store
            .authorization_catalog()?
            .find(name)
            .map(|record| record.principal.clone()))
    }

    #[cfg(feature = "pgwire")]
    pub(crate) fn authentication_principal(
        &self,
        name: &str,
    ) -> Result<Option<(Principal, Option<Vec<u8>>)>> {
        Ok(self
            .store
            .authorization_catalog()?
            .find(name)
            .map(|record| (record.principal.clone(), record.credential.clone())))
    }

    /// Admit one operation using the current committed principal incarnation,
    /// independently of any older data transaction's snapshot.
    pub fn authorization(&self, principal: PrincipalId) -> Result<AuthorizationView> {
        let mut catalog = self.store.authorization_catalog()?;
        let record = catalog
            .principals
            .remove(&principal)
            .ok_or_else(|| DbError::PermissionDenied("principal no longer exists".to_owned()))?;
        Ok(AuthorizationView {
            owner_id: self.handle_id,
            administrator: record.administrator,
            grants: record.grants,
        })
    }

    /// Admit a trusted bootstrap operation only while its explicit native
    /// capability remains enabled. Creating the first principal revokes it.
    pub fn bootstrap_authorization(&self) -> Result<AuthorizationView> {
        if !self.authorization_bootstrap_enabled()? {
            return Err(DbError::PermissionDenied(
                "bootstrap authority is disabled".to_owned(),
            ));
        }
        Ok(AuthorizationView {
            owner_id: self.handle_id,
            administrator: true,
            grants: Default::default(),
        })
    }
}

impl AuthorizationView {
    /// Require read access to the currently resolved table incarnation.
    pub fn require_read(&self, database: &RelationalDatabase, table: TableId) -> Result<()> {
        self.require_table(
            database.handle_id,
            TableIncarnation {
                table,
                tree: database.store.table_tree(table)?.get(),
            },
            false,
        )
    }

    /// Require write access (which also permits reads) to this incarnation.
    pub fn require_write(&self, database: &RelationalDatabase, table: TableId) -> Result<()> {
        self.require_table(
            database.handle_id,
            TableIncarnation {
                table,
                tree: database.store.table_tree(table)?.get(),
            },
            true,
        )
    }

    /// Require explicit database administration for this database handle.
    pub fn require_administration(&self, database: &RelationalDatabase) -> Result<()> {
        if self.owner_id != database.handle_id || !self.administrator {
            return Err(DbError::PermissionDenied(
                "database administration required for this database handle".to_owned(),
            ));
        }
        Ok(())
    }
}

impl RelationalDatabaseTransaction {
    // All authority edits share the irreversible operation guard and the data
    // transaction's native decision. They do not publish a SQL schema version.
    fn mutate_authorization<T>(
        &mut self,
        database: &RelationalDatabase,
        operation: impl FnOnce(
            &mut crate::authorization::AuthorizationCatalog,
            &std::collections::BTreeMap<TableId, seerdb::TreeId>,
        ) -> Result<T>,
    ) -> Result<T> {
        self.operation(database, |transaction| {
            transaction.ensure_sql_catalog(database)?;
            let result = transaction.backend_mut().mutate_authorization(operation)?;
            transaction.wrote = true;
            Ok(result)
        })
    }

    /// Find principal metadata in this transaction's snapshot and staged edits.
    pub fn principal(
        &mut self,
        database: &RelationalDatabase,
        name: &str,
    ) -> Result<Option<Principal>> {
        self.operation(database, |transaction| {
            Ok(transaction
                .backend_mut()
                .authorization_catalog()?
                .find(name)
                .map(|record| record.principal.clone()))
        })
    }

    /// Create a fresh principal incarnation without any privileges or credential.
    /// Creating the first principal revokes the explicit bootstrap capability;
    /// dropping principals cannot restore it.
    pub fn create_principal(
        &mut self,
        database: &RelationalDatabase,
        name: &str,
    ) -> Result<PrincipalId> {
        self.mutate_authorization(database, |catalog, _| catalog.create_principal(name))
    }

    /// Rename a principal while preserving its identity and privileges.
    pub fn rename_principal(
        &mut self,
        database: &RelationalDatabase,
        id: PrincipalId,
        name: &str,
    ) -> Result<()> {
        self.mutate_authorization(database, |catalog, _| catalog.rename_principal(id, name))
    }

    /// Drop the principal and all of its credentials and grants atomically.
    pub fn drop_principal(&mut self, database: &RelationalDatabase, id: PrincipalId) -> Result<()> {
        self.mutate_authorization(database, |catalog, _| {
            catalog
                .principals
                .remove(&id)
                .ok_or_else(|| DbError::InvalidState("principal does not exist".to_owned()))?;
            Ok(())
        })
    }

    /// Replace opaque authentication material. Only trusted native callers and
    /// their protocol adapters should manage credentials; values are not logged.
    pub fn set_principal_credential(
        &mut self,
        database: &RelationalDatabase,
        id: PrincipalId,
        credential: Option<Vec<u8>>,
    ) -> Result<()> {
        self.mutate_authorization(database, |catalog, _| {
            catalog.record_mut(id)?.credential = credential;
            Ok(())
        })
    }

    /// Grant or revoke coarse table privileges on the resolved incarnation.
    /// Write access implies read access; neither grants administration.
    pub fn set_table_grant(
        &mut self,
        database: &RelationalDatabase,
        id: PrincipalId,
        table: TableId,
        read: bool,
        write: bool,
    ) -> Result<()> {
        self.mutate_authorization(database, |catalog, tables| {
            let tree = tables.get(&table).ok_or_else(|| {
                DbError::InvalidState(format!("table {} does not exist", table.0))
            })?;
            let object = TableIncarnation {
                table,
                tree: tree.get(),
            };
            let record = catalog.record_mut(id)?;
            if read || write {
                record.grants.insert(object, TableGrant { read, write });
            } else {
                record.grants.remove(&object);
            }
            Ok(())
        })
    }

    /// Set the explicit database-administration capability, not a table-name
    /// wildcard. This capability includes all table reads and writes.
    pub fn set_database_administrator(
        &mut self,
        database: &RelationalDatabase,
        id: PrincipalId,
        enabled: bool,
    ) -> Result<()> {
        self.mutate_authorization(database, |catalog, _| {
            catalog.record_mut(id)?.administrator = enabled;
            Ok(())
        })
    }
}
