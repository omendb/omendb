//! Authorization is a separate catalog record, not a schema publication.
//! Data transactions never read this key for admission: a fresh committed view
//! authorizes each operation without retroactively fencing its eventual commit.

use super::*;
use crate::authorization::{self, AuthorizationCatalog};

const MARKER: &[u8] = b"\0omendb/authorization/v1";

pub(super) fn initialize_new(transaction: &mut Transaction, tree: TreeId) -> Result<()> {
    transaction
        .put(
            tree,
            MARKER,
            &authorization::encode(&AuthorizationCatalog::bootstrap())?,
        )
        .map_err(map_seer_error)
}

pub(super) fn validate_existing(
    transaction: &mut Transaction,
    tree: TreeId,
    tables: &BTreeMap<TableId, TreeId>,
) -> Result<()> {
    // Histories predating native authorization remain usable by trusted native
    // callers. Serving requires explicit initialization; missing is not trust.
    if let Some(bytes) = transaction.get(tree, MARKER).map_err(map_seer_error)? {
        authorization::decode(&bytes)?.validate_tables(tables)?;
    }
    Ok(())
}

fn read_catalog(
    transaction: &mut Transaction,
    tree: TreeId,
    tables: &BTreeMap<TableId, TreeId>,
) -> Result<AuthorizationCatalog> {
    let bytes = transaction.get(tree, MARKER).map_err(map_seer_error)?.ok_or_else(|| DbError::InvalidState(
        "native authorization is uninitialized; use the trusted native initialize_authorization API before serving".to_owned()
    ))?;
    let catalog = authorization::decode(&bytes)?;
    catalog.validate_tables(tables)?;
    Ok(catalog)
}

impl DirectSeerStore {
    pub(crate) fn authorization_catalog(&self) -> Result<AuthorizationCatalog> {
        let mut transaction = self.database.begin().map_err(map_seer_error)?;
        read_catalog(&mut transaction, self.catalog_tree, &self.table_trees)
    }

    pub(crate) fn ensure_no_legacy_authorization(&self) -> Result<()> {
        if self.catalog.tables().any(|table| {
            (table.id.0 == u64::MAX - 1 && table.name == "pgwire_auth")
                || (table.id.0 == u64::MAX - 2 && table.name == "pgwire_grants")
        }) {
            return Err(DbError::InvalidState(
                "legacy pgwire authorization tables require trusted removal and explicit native principal/grant reprovisioning; name-based grants cannot be imported safely".to_owned()
            ));
        }
        Ok(())
    }

    pub(crate) fn initialize_authorization(&self) -> Result<CommitSeq> {
        self.ensure_no_legacy_authorization()?;
        let mut transaction = self.database.begin().map_err(map_seer_error)?;
        if transaction
            .get(self.catalog_tree, MARKER)
            .map_err(map_seer_error)?
            .is_some()
        {
            return Err(DbError::InvalidState(
                "native authorization is already initialized".to_owned(),
            ));
        }
        initialize_new(&mut transaction, self.catalog_tree)?;
        Ok(transaction.commit().map_err(map_seer_error)?.csn)
    }

    pub(super) fn prune_authorization(
        &self,
        transaction: &mut Transaction,
        tables: &BTreeMap<TableId, TreeId>,
    ) -> Result<()> {
        if let Some(bytes) = transaction
            .get(self.catalog_tree, MARKER)
            .map_err(map_seer_error)?
        {
            let mut catalog = authorization::decode(&bytes)?;
            // Corrupt metadata is not silently repaired by dropping its target.
            catalog.validate_tables(&self.table_trees)?;
            if catalog.prune_tables(tables) {
                transaction
                    .put(self.catalog_tree, MARKER, &authorization::encode(&catalog)?)
                    .map_err(map_seer_error)?;
            }
        }
        Ok(())
    }
}

impl DirectTransaction {
    pub(crate) fn authorization_catalog(&mut self) -> Result<AuthorizationCatalog> {
        read_catalog(&mut self.transaction, self.catalog_tree, &self.table_trees)
    }

    pub(crate) fn mutate_authorization<T>(
        &mut self,
        operation: impl FnOnce(&mut AuthorizationCatalog, &BTreeMap<TableId, TreeId>) -> Result<T>,
    ) -> Result<T> {
        // get observes both the fixed snapshot and this transaction's own
        // staged catalog writes; its dependency serializes concurrent editors.
        let mut catalog = self.authorization_catalog()?;
        let value = operation(&mut catalog, &self.table_trees)?;
        let encoded = authorization::encode(&catalog)?;
        self.transaction
            .put(self.catalog_tree, MARKER, &encoded)
            .map_err(map_seer_error)?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_history_preserves_data_but_requires_explicit_reprovisioning() {
        let directory = tempfile::tempdir().unwrap();
        let config = crate::RelationalBackendConfig::new(directory.path().join("db"));
        let mut database = crate::RelationalDatabase::create(config.clone()).unwrap();
        database
            .execute_sql("CREATE TABLE data (id BIGINT PRIMARY KEY)")
            .unwrap();
        database
            .execute_sql("INSERT INTO data VALUES (42)")
            .unwrap();
        database.close().unwrap();
        let mut store = DirectSeerStore::open(&config.path, Options::default()).unwrap();
        let legacy_id = TableId(u64::MAX - 1);
        store
            .create_table(
                TableDefinition {
                    id: legacy_id,
                    name: "pgwire_auth".to_owned(),
                    columns: vec![crate::ColumnDefinition {
                        id: ColumnId(1),
                        name: "username".to_owned(),
                        data_type: crate::ColumnType::Text,
                        nullable: false,
                    }],
                },
                None,
            )
            .unwrap();
        let mut transaction = store.database.begin().unwrap();
        transaction.delete(store.catalog_tree, MARKER).unwrap();
        transaction.commit().unwrap();
        store.close().unwrap();
        drop(store);

        let mut database = crate::RelationalDatabase::open(config.clone()).unwrap();
        assert_eq!(
            database
                .execute_sql("SELECT id FROM data")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(database.authorization_bootstrap_enabled().is_err());
        assert!(database.initialize_authorization().is_err());
        assert!(database.catalog().table(legacy_id).is_ok());
        database
            .apply_schema_mutations(&[SchemaMutation::DropTable { table: legacy_id }])
            .unwrap();
        database.initialize_authorization().unwrap();
        assert!(database.authorization_bootstrap_enabled().unwrap());
        database.close().unwrap();
        let mut database = crate::RelationalDatabase::open(config).unwrap();
        assert!(database.authorization_bootstrap_enabled().unwrap());
        assert_eq!(
            database
                .execute_sql("SELECT id FROM data")
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn missing_and_malformed_authorization_never_mean_bootstrap() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            DirectSeerStore::create(directory.path().join("db"), Options::default()).unwrap();
        let mut transaction = store.database.begin().unwrap();
        transaction.delete(store.catalog_tree, MARKER).unwrap();
        transaction.commit().unwrap();
        assert!(store.authorization_catalog().is_err());
        store.initialize_authorization().unwrap();
        assert!(store.authorization_catalog().unwrap().bootstrap);
        let mut transaction = store.database.begin().unwrap();
        transaction
            .put(store.catalog_tree, MARKER, b"broken")
            .unwrap();
        transaction.commit().unwrap();
        assert!(matches!(
            store.authorization_catalog(),
            Err(DbError::Corruption { .. })
        ));
        assert!(store.initialize_authorization().is_err());
        store.close().unwrap();
        assert!(matches!(
            DirectSeerStore::open(directory.path().join("db"), Options::default()),
            Err(DbError::Corruption { .. })
        ));
    }
}
