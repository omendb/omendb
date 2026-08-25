//! Reference model for the replacement transactional SeerDB seam.
//!
//! This module is test-only by design. It provides a small deterministic model
//! for snapshot isolation, tree lifecycle, and atomic multi-tree mutation
//! before those semantics are moved into the durable engine. It must not be
//! mistaken for a production backend or a second source of storage truth.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TreeId(u64);

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct CommitSeq(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxnState {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Eq, PartialEq)]
enum ModelError {
    Inactive,
    WriteConflict { tree: TreeId, key: Vec<u8> },
    TreeConflict(TreeId),
    TreeNotVisible(TreeId),
}

#[derive(Clone, Debug)]
struct Version {
    commit: CommitSeq,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
struct TreeHistory {
    lifecycle: Vec<(CommitSeq, bool)>,
    keys: BTreeMap<Vec<u8>, Vec<Version>>,
}

#[derive(Debug, Default)]
struct ModelDb {
    commit: CommitSeq,
    next_tree_id: u64,
    trees: BTreeMap<TreeId, TreeHistory>,
}

#[derive(Debug)]
struct ModelTransaction {
    snapshot: CommitSeq,
    writes: BTreeMap<(TreeId, Vec<u8>), Option<Vec<u8>>>,
    created: BTreeSet<TreeId>,
    dropped: BTreeSet<TreeId>,
    state: TxnState,
}

impl ModelDb {
    fn begin(&self) -> ModelTransaction {
        ModelTransaction {
            snapshot: self.commit,
            writes: BTreeMap::new(),
            created: BTreeSet::new(),
            dropped: BTreeSet::new(),
            state: TxnState::Active,
        }
    }

    fn create_tree(&mut self) -> TreeId {
        self.next_tree_id += 1;
        TreeId(self.next_tree_id)
    }

    fn tree_value(&self, tree: TreeId, key: &[u8], snapshot: CommitSeq) -> Option<Vec<u8>> {
        let history = self.trees.get(&tree)?;
        if !visible_at(&history.lifecycle, snapshot) {
            return None;
        }
        history
            .keys
            .get(key)
            .and_then(|versions| visible_version(versions, snapshot))
            .and_then(|version| version.value.clone())
    }

    fn latest_key_commit(&self, tree: TreeId, key: &[u8]) -> Option<CommitSeq> {
        self.trees
            .get(&tree)
            .and_then(|history| history.keys.get(key))
            .and_then(|versions| versions.last())
            .map(|version| version.commit)
    }

    fn tree_exists_at(&self, tree: TreeId, snapshot: CommitSeq) -> bool {
        self.trees
            .get(&tree)
            .is_some_and(|history| visible_at(&history.lifecycle, snapshot))
    }
}

impl ModelTransaction {
    fn create_tree(&mut self, db: &mut ModelDb) -> Result<TreeId, ModelError> {
        self.check_active()?;
        let tree = db.create_tree();
        self.created.insert(tree);
        Ok(tree)
    }

    fn drop_tree(&mut self, tree: TreeId) -> Result<(), ModelError> {
        self.check_active()?;
        if !self.created.contains(&tree) {
            self.dropped.insert(tree);
        }
        Ok(())
    }

    fn put(&mut self, tree: TreeId, key: &[u8], value: &[u8]) -> Result<(), ModelError> {
        self.check_active()?;
        self.check_tree_for_write(tree)?;
        self.writes
            .insert((tree, key.to_vec()), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&mut self, tree: TreeId, key: &[u8]) -> Result<(), ModelError> {
        self.check_active()?;
        self.check_tree_for_write(tree)?;
        self.writes.insert((tree, key.to_vec()), None);
        Ok(())
    }

    fn get(&self, db: &ModelDb, tree: TreeId, key: &[u8]) -> Result<Option<Vec<u8>>, ModelError> {
        self.check_active()?;
        if let Some(value) = self.writes.get(&(tree, key.to_vec())) {
            return Ok(value.clone());
        }
        if self.dropped.contains(&tree) && !self.created.contains(&tree) {
            return Err(ModelError::TreeNotVisible(tree));
        }
        if self.created.contains(&tree) {
            return Ok(None);
        }
        if !db.tree_exists_at(tree, self.snapshot) {
            return Err(ModelError::TreeNotVisible(tree));
        }
        Ok(db.tree_value(tree, key, self.snapshot))
    }

    fn commit(mut self, db: &mut ModelDb) -> Result<CommitSeq, ModelError> {
        self.check_active()?;
        self.validate(db)?;
        let commit = CommitSeq(db.commit.0 + 1);

        for tree in &self.created {
            let history = db.trees.entry(*tree).or_default();
            history.lifecycle.push((commit, true));
        }
        for ((tree, key), value) in self.writes {
            let history = db.trees.entry(tree).or_default();
            history
                .keys
                .entry(key)
                .or_default()
                .push(Version { commit, value });
        }
        for tree in &self.dropped {
            let history = db.trees.entry(*tree).or_default();
            history.lifecycle.push((commit, false));
        }

        db.commit = commit;
        self.state = TxnState::Committed;
        Ok(commit)
    }

    fn abort(mut self) -> Result<(), ModelError> {
        self.check_active()?;
        self.state = TxnState::Aborted;
        Ok(())
    }

    fn validate(&self, db: &ModelDb) -> Result<(), ModelError> {
        for tree in &self.created {
            if db.trees.contains_key(tree) {
                return Err(ModelError::TreeConflict(*tree));
            }
        }
        for tree in &self.dropped {
            if !db.tree_exists_at(*tree, self.snapshot)
                || db
                    .trees
                    .get(tree)
                    .and_then(|history| history.lifecycle.last())
                    .is_some_and(|(commit, _)| *commit > self.snapshot)
            {
                return Err(ModelError::TreeConflict(*tree));
            }
        }
        for (tree, key) in self.writes.keys() {
            if self.created.contains(tree) {
                continue;
            }
            if !db.tree_exists_at(*tree, self.snapshot) {
                return Err(ModelError::TreeNotVisible(*tree));
            }
            if db
                .latest_key_commit(*tree, key)
                .is_some_and(|commit| commit > self.snapshot)
            {
                return Err(ModelError::WriteConflict {
                    tree: *tree,
                    key: key.clone(),
                });
            }
        }
        Ok(())
    }

    fn check_tree_for_write(&self, tree: TreeId) -> Result<(), ModelError> {
        if self.dropped.contains(&tree) {
            return Err(ModelError::TreeNotVisible(tree));
        }
        Ok(())
    }

    fn check_active(&self) -> Result<(), ModelError> {
        if self.state == TxnState::Active {
            Ok(())
        } else {
            Err(ModelError::Inactive)
        }
    }
}

fn visible_at(lifecycle: &[(CommitSeq, bool)], snapshot: CommitSeq) -> bool {
    lifecycle
        .iter()
        .rev()
        .find(|(commit, _)| *commit <= snapshot)
        .is_some_and(|(_, exists)| *exists)
}

fn visible_version(versions: &[Version], snapshot: CommitSeq) -> Option<&Version> {
    versions
        .iter()
        .rev()
        .find(|version| version.commit <= snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_with_tree() -> (ModelDb, TreeId) {
        let mut db = ModelDb::default();
        let mut tx = db.begin();
        let tree = tx.create_tree(&mut db).expect("tree id");
        tx.commit(&mut db).expect("tree commit");
        (db, tree)
    }

    #[test]
    fn disjoint_writers_commit_from_one_snapshot() {
        let (mut db, tree) = database_with_tree();
        let mut first = db.begin();
        let mut second = db.begin();
        first.put(tree, b"a", b"one").expect("first write");
        second.put(tree, b"b", b"two").expect("second write");

        first.commit(&mut db).expect("first commit");
        second.commit(&mut db).expect("disjoint commit");

        let reader = db.begin();
        assert_eq!(
            reader.get(&db, tree, b"a").expect("a"),
            Some(b"one".to_vec())
        );
        assert_eq!(
            reader.get(&db, tree, b"b").expect("b"),
            Some(b"two".to_vec())
        );
    }

    #[test]
    fn same_key_writers_conflict_without_partial_mutation() {
        let (mut db, tree) = database_with_tree();
        let mut first = db.begin();
        let mut second = db.begin();
        first.put(tree, b"key", b"first").expect("first write");
        second.put(tree, b"key", b"second").expect("second write");

        first.commit(&mut db).expect("first commit");
        assert_eq!(
            second.commit(&mut db),
            Err(ModelError::WriteConflict {
                tree,
                key: b"key".to_vec()
            })
        );

        let reader = db.begin();
        assert_eq!(
            reader.get(&db, tree, b"key").expect("read"),
            Some(b"first".to_vec())
        );
    }

    #[test]
    fn snapshots_keep_old_values_after_new_commit() {
        let (mut db, tree) = database_with_tree();
        let mut seed = db.begin();
        seed.put(tree, b"key", b"old").expect("seed write");
        seed.commit(&mut db).expect("seed commit");

        let old = db.begin();
        let mut update = db.begin();
        update.put(tree, b"key", b"new").expect("update write");
        update.commit(&mut db).expect("update commit");

        assert_eq!(
            old.get(&db, tree, b"key").expect("old read"),
            Some(b"old".to_vec())
        );
        let current = db.begin();
        assert_eq!(
            current.get(&db, tree, b"key").expect("current read"),
            Some(b"new".to_vec())
        );
    }

    #[test]
    fn deletes_and_tree_drops_preserve_old_snapshot_visibility() {
        let (mut db, tree) = database_with_tree();
        let mut seed = db.begin();
        seed.put(tree, b"key", b"value").expect("seed write");
        seed.commit(&mut db).expect("seed commit");

        let before_delete = db.begin();
        let mut delete = db.begin();
        delete.delete(tree, b"key").expect("delete");
        delete.commit(&mut db).expect("delete commit");
        let current = db.begin();
        assert_eq!(current.get(&db, tree, b"key").expect("current read"), None);
        assert_eq!(
            before_delete
                .get(&db, tree, b"key")
                .expect("historical read"),
            Some(b"value".to_vec())
        );

        let before_drop = db.begin();
        let mut drop = db.begin();
        drop.drop_tree(tree).expect("drop tree");
        drop.commit(&mut db).expect("drop commit");
        let current = db.begin();
        assert_eq!(
            current.get(&db, tree, b"key"),
            Err(ModelError::TreeNotVisible(tree))
        );
        assert_eq!(
            before_drop.get(&db, tree, b"key").expect("old tree read"),
            None
        );
    }

    #[test]
    fn multi_tree_commit_is_atomic_and_abort_burns_tree_id() {
        let (mut db, first_tree) = database_with_tree();
        let mut create = db.begin();
        let second_tree = create.create_tree(&mut db).expect("second tree");
        create.commit(&mut db).expect("second tree commit");

        let mut writer = db.begin();
        writer
            .put(first_tree, b"one", b"1")
            .expect("first mutation");
        writer
            .put(second_tree, b"two", b"2")
            .expect("second mutation");
        writer.commit(&mut db).expect("atomic commit");

        let mut aborted = db.begin();
        let burned = aborted.create_tree(&mut db).expect("burned tree");
        aborted.abort().expect("abort");
        let mut next = db.begin();
        let replacement = next.create_tree(&mut db).expect("replacement tree");
        assert!(replacement > burned);
        next.abort().expect("abort replacement");

        let reader = db.begin();
        assert_eq!(
            reader.get(&db, first_tree, b"one").expect("first read"),
            Some(b"1".to_vec())
        );
        assert_eq!(
            reader.get(&db, second_tree, b"two").expect("second read"),
            Some(b"2".to_vec())
        );
        assert_eq!(
            reader.get(&db, burned, b"x"),
            Err(ModelError::TreeNotVisible(burned))
        );
    }

    #[test]
    fn failed_multi_tree_commit_does_not_publish_any_tree_write() {
        let (mut db, first_tree) = database_with_tree();
        let mut create = db.begin();
        let second_tree = create.create_tree(&mut db).expect("second tree");
        create.commit(&mut db).expect("tree commit");

        let mut stale = db.begin();
        let mut winner = db.begin();
        stale
            .put(first_tree, b"conflict", b"stale")
            .expect("stale first");
        stale
            .put(second_tree, b"other", b"stale")
            .expect("stale second");
        winner
            .put(first_tree, b"conflict", b"winner")
            .expect("winner write");
        winner.commit(&mut db).expect("winner commit");

        assert!(matches!(
            stale.commit(&mut db),
            Err(ModelError::WriteConflict { tree, .. }) if tree == first_tree
        ));
        let reader = db.begin();
        assert_eq!(
            reader.get(&db, second_tree, b"other").expect("other read"),
            None
        );
    }
}
