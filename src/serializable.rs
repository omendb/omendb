use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;

use crate::model::{CommitId, Key};

/// Algorithm used by the certifier to validate serializability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CertifierAlgorithm {
    /// Precise read-set and write-set anti-dependency validation.
    PreciseValidation,
    /// SSI dependency graph certification with cycle detection.
    SsiLike,
}

/// A declared transaction specification for multi-writer concurrency certification.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TransactionDependencySpec {
    pub id: u64,
    pub snapshot: CommitId,
    pub point_reads: BTreeSet<Key>,
    pub range_reads: Vec<RangeInclusive<Key>>,
    pub unique_checks: BTreeSet<Key>,
    pub foreign_key_checks: BTreeSet<Key>,
    pub catalog_reads: BTreeSet<Key>,
    pub point_writes: BTreeMap<Key, Vec<u8>>,
    pub point_deletes: BTreeSet<Key>,
    pub catalog_writes: BTreeMap<Key, Vec<u8>>,
    pub catalog_deletes: BTreeSet<Key>,
}

impl TransactionDependencySpec {
    #[must_use]
    pub fn new(id: u64, snapshot: CommitId) -> Self {
        Self {
            id,
            snapshot,
            ..Self::default()
        }
    }

    pub fn read(&mut self, key: Key) {
        self.point_reads.insert(key);
    }

    pub fn read_range(&mut self, start: Key, end: Key) {
        self.range_reads.push(start..=end);
    }

    pub fn check_unique(&mut self, key: Key) {
        self.unique_checks.insert(key);
    }

    pub fn check_foreign_key(&mut self, key: Key) {
        self.foreign_key_checks.insert(key);
    }

    pub fn read_catalog(&mut self, key: Key) {
        self.catalog_reads.insert(key);
    }

    pub fn write(&mut self, key: Key, value: Vec<u8>) {
        self.point_writes.insert(key, value);
    }

    pub fn delete(&mut self, key: Key) {
        self.point_deletes.insert(key);
    }

    pub fn write_catalog(&mut self, key: Key, value: Vec<u8>) {
        self.catalog_writes.insert(key, value);
    }

    pub fn delete_catalog(&mut self, key: Key) {
        self.catalog_deletes.insert(key);
    }

    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.point_writes.is_empty()
            && self.point_deletes.is_empty()
            && self.catalog_writes.is_empty()
            && self.catalog_deletes.is_empty()
    }

    #[must_use]
    pub fn write_count(&self) -> usize {
        self.point_writes.len()
            + self.point_deletes.len()
            + self.catalog_writes.len()
            + self.catalog_deletes.len()
    }

    #[must_use]
    pub fn read_count(&self) -> usize {
        self.point_reads.len()
            + self.range_reads.len()
            + self.unique_checks.len()
            + self.foreign_key_checks.len()
            + self.catalog_reads.len()
    }
}

/// Conflict classification reported by the certifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CertificationConflict {
    WriteWrite { key: Key },
    ReadWriteAntiDependency { key: Key },
    RangeAntiDependency { start: Key, end: Key },
    UniqueConstraintViolation { key: Key },
    ForeignKeyViolation { key: Key },
    CatalogConflict { key: Key },
    DependencyCycle,
}

#[derive(Clone, Debug)]
struct CommittedEntry {
    spec: TransactionDependencySpec,
    commit: CommitId,
}

/// Metrics gathered during multi-writer transaction certification.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CertifierMetrics {
    pub attempts: u64,
    pub committed: u64,
    pub conflicts: u64,
    pub write_write_conflicts: u64,
    pub read_write_conflicts: u64,
    pub range_conflicts: u64,
    pub constraint_conflicts: u64,
    pub cycle_conflicts: u64,
    pub max_dependency_edges: u64,
}

/// In-memory dependency certifier for fine-grained multi-writer transaction isolation.
#[derive(Clone, Debug)]
pub struct SerializableCertifier {
    algorithm: CertifierAlgorithm,
    committed: Vec<CommittedEntry>,
    current_commit: CommitId,
    metrics: CertifierMetrics,
}

impl SerializableCertifier {
    #[must_use]
    pub fn new(algorithm: CertifierAlgorithm) -> Self {
        Self {
            algorithm,
            committed: Vec::new(),
            current_commit: CommitId(0),
            metrics: CertifierMetrics::default(),
        }
    }

    #[must_use]
    pub fn current_commit(&self) -> CommitId {
        self.current_commit
    }

    #[must_use]
    pub fn metrics(&self) -> &CertifierMetrics {
        &self.metrics
    }

    /// Validate a candidate transaction against all transactions committed since its snapshot.
    pub fn validate(
        &mut self,
        candidate: &TransactionDependencySpec,
    ) -> Result<(), CertificationConflict> {
        self.metrics.attempts += 1;

        if candidate.snapshot >= self.current_commit && candidate.is_read_only() {
            return Ok(());
        }

        match self.algorithm {
            CertifierAlgorithm::PreciseValidation => self.validate_precise(candidate),
            CertifierAlgorithm::SsiLike => self.validate_ssi(candidate),
        }
    }

    fn validate_precise(
        &mut self,
        candidate: &TransactionDependencySpec,
    ) -> Result<(), CertificationConflict> {
        for entry in &self.committed {
            if entry.commit <= candidate.snapshot {
                continue;
            }

            // 1. Write-Write conflicts: did both transactions write or delete the same key?
            for key in candidate.point_writes.keys() {
                if entry.spec.point_writes.contains_key(key)
                    || entry.spec.point_deletes.contains(key)
                {
                    self.metrics.conflicts += 1;
                    self.metrics.write_write_conflicts += 1;
                    return Err(CertificationConflict::WriteWrite { key: *key });
                }
            }
            for key in &candidate.point_deletes {
                if entry.spec.point_writes.contains_key(key)
                    || entry.spec.point_deletes.contains(key)
                {
                    self.metrics.conflicts += 1;
                    self.metrics.write_write_conflicts += 1;
                    return Err(CertificationConflict::WriteWrite { key: *key });
                }
            }

            // 2. Read-Write anti-dependencies: candidate read a point key that earlier committed tx modified
            for key in &candidate.point_reads {
                if entry.spec.point_writes.contains_key(key)
                    || entry.spec.point_deletes.contains(key)
                {
                    self.metrics.conflicts += 1;
                    self.metrics.read_write_conflicts += 1;
                    return Err(CertificationConflict::ReadWriteAntiDependency { key: *key });
                }
            }

            // 3. Unique / Foreign-key constraint checks
            for key in &candidate.unique_checks {
                if entry.spec.point_writes.contains_key(key) {
                    self.metrics.conflicts += 1;
                    self.metrics.constraint_conflicts += 1;
                    return Err(CertificationConflict::UniqueConstraintViolation { key: *key });
                }
            }
            for key in &candidate.foreign_key_checks {
                if entry.spec.point_deletes.contains(key) {
                    self.metrics.conflicts += 1;
                    self.metrics.constraint_conflicts += 1;
                    return Err(CertificationConflict::ForeignKeyViolation { key: *key });
                }
            }

            // 4. Catalog conflict
            for key in candidate
                .catalog_writes
                .keys()
                .chain(&candidate.catalog_deletes)
            {
                if entry.spec.catalog_writes.contains_key(key)
                    || entry.spec.catalog_deletes.contains(key)
                {
                    self.metrics.conflicts += 1;
                    return Err(CertificationConflict::CatalogConflict { key: *key });
                }
            }
            for key in &candidate.catalog_reads {
                if entry.spec.catalog_writes.contains_key(key)
                    || entry.spec.catalog_deletes.contains(key)
                {
                    self.metrics.conflicts += 1;
                    return Err(CertificationConflict::CatalogConflict { key: *key });
                }
            }

            // 5. Range read phantom checks
            for range in &candidate.range_reads {
                for key in entry
                    .spec
                    .point_writes
                    .keys()
                    .chain(&entry.spec.point_deletes)
                {
                    if range.contains(key) {
                        self.metrics.conflicts += 1;
                        self.metrics.range_conflicts += 1;
                        return Err(CertificationConflict::RangeAntiDependency {
                            start: *range.start(),
                            end: *range.end(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_ssi(
        &mut self,
        candidate: &TransactionDependencySpec,
    ) -> Result<(), CertificationConflict> {
        // Check write-write overlap first
        for entry in &self.committed {
            if entry.commit > candidate.snapshot {
                for key in candidate
                    .point_writes
                    .keys()
                    .chain(&candidate.point_deletes)
                {
                    if entry.spec.point_writes.contains_key(key)
                        || entry.spec.point_deletes.contains(key)
                    {
                        self.metrics.conflicts += 1;
                        self.metrics.write_write_conflicts += 1;
                        return Err(CertificationConflict::WriteWrite { key: *key });
                    }
                }
            }
        }

        // Build serialization dependency graph and detect cycles
        let mut nodes = self.committed.iter().map(|e| e.spec.id).collect::<Vec<_>>();
        nodes.push(candidate.id);

        let mut edges: BTreeSet<(u64, u64)> = BTreeSet::new();

        for (i, left) in self.committed.iter().enumerate() {
            for right in self.committed.iter().skip(i + 1) {
                // Dependency: left read a key that right modified later (left -> right)
                if has_rw_antidependency(&left.spec, &right.spec, right.commit > left.spec.snapshot)
                {
                    edges.insert((left.spec.id, right.spec.id));
                }
                if has_rw_antidependency(&right.spec, &left.spec, left.commit > right.spec.snapshot)
                {
                    edges.insert((right.spec.id, left.spec.id));
                }
            }

            // Candidate edges
            if has_rw_antidependency(&left.spec, candidate, true) {
                edges.insert((left.spec.id, candidate.id));
            }
            if has_rw_antidependency(candidate, &left.spec, left.commit > candidate.snapshot) {
                edges.insert((candidate.id, left.spec.id));
            }
        }

        self.metrics.max_dependency_edges =
            self.metrics.max_dependency_edges.max(edges.len() as u64);

        if has_dependency_cycle(&nodes, &edges) {
            self.metrics.conflicts += 1;
            self.metrics.cycle_conflicts += 1;
            return Err(CertificationConflict::DependencyCycle);
        }

        Ok(())
    }

    /// Commit a certified transaction and advance the commit frontier.
    pub fn commit(&mut self, spec: TransactionDependencySpec) -> CommitId {
        let commit = if !spec.is_read_only() {
            self.current_commit = CommitId(self.current_commit.0 + 1);
            self.current_commit
        } else {
            self.current_commit
        };

        self.committed.push(CommittedEntry { spec, commit });
        self.metrics.committed += 1;
        commit
    }

    /// Prune committed history older than the oldest active snapshot lease.
    pub fn prune_retained(&mut self, oldest_active_snapshot: CommitId) {
        self.committed
            .retain(|entry| entry.commit >= oldest_active_snapshot);
    }
}

fn has_rw_antidependency(
    reader: &TransactionDependencySpec,
    writer: &TransactionDependencySpec,
    writer_after_reader_snapshot: bool,
) -> bool {
    if !writer_after_reader_snapshot {
        return false;
    }

    // Point read overlap with write
    for key in &reader.point_reads {
        if writer.point_writes.contains_key(key) || writer.point_deletes.contains(key) {
            return true;
        }
    }

    // Range read overlap with write
    for range in &reader.range_reads {
        for key in writer.point_writes.keys().chain(&writer.point_deletes) {
            if range.contains(key) {
                return true;
            }
        }
    }

    // Catalog read overlap with catalog write
    for key in &reader.catalog_reads {
        if writer.catalog_writes.contains_key(key) || writer.catalog_deletes.contains(key) {
            return true;
        }
    }

    false
}

fn has_dependency_cycle(nodes: &[u64], edges: &BTreeSet<(u64, u64)>) -> bool {
    let mut in_degrees: BTreeMap<u64, usize> = BTreeMap::new();
    let mut adjacency: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for node in nodes {
        in_degrees.insert(*node, 0);
        adjacency.insert(*node, Vec::new());
    }

    for (from, to) in edges {
        if let Some(deg) = in_degrees.get_mut(to) {
            *deg += 1;
        }
        if let Some(neighbors) = adjacency.get_mut(from) {
            neighbors.push(*to);
        }
    }

    let mut queue = in_degrees
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(node, _)| *node)
        .collect::<std::collections::VecDeque<_>>();

    let mut visited_count = 0;
    while let Some(node) = queue.pop_front() {
        visited_count += 1;
        if let Some(neighbors) = adjacency.get(&node) {
            for neighbor in neighbors {
                if let Some(deg) = in_degrees.get_mut(neighbor) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(*neighbor);
                    }
                }
            }
        }
    }

    visited_count < nodes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disjoint_writers_pass_precise_validation() {
        let mut certifier = SerializableCertifier::new(CertifierAlgorithm::PreciseValidation);

        let mut tx1 = TransactionDependencySpec::new(1, CommitId(0));
        tx1.read(Key::new(1, 10));
        tx1.write(Key::new(1, 10), b"v10".to_vec());
        assert!(certifier.validate(&tx1).is_ok());
        let c1 = certifier.commit(tx1);
        assert_eq!(c1, CommitId(1));

        // Tx2 started at snapshot 0, but operates on disjoint key 20
        let mut tx2 = TransactionDependencySpec::new(2, CommitId(0));
        tx2.read(Key::new(1, 20));
        tx2.write(Key::new(1, 20), b"v20".to_vec());
        assert!(certifier.validate(&tx2).is_ok());
        let c2 = certifier.commit(tx2);
        assert_eq!(c2, CommitId(2));

        assert_eq!(certifier.metrics().committed, 2);
        assert_eq!(certifier.metrics().conflicts, 0);
    }

    #[test]
    fn write_write_conflict_is_detected() {
        let mut certifier = SerializableCertifier::new(CertifierAlgorithm::PreciseValidation);

        let mut tx1 = TransactionDependencySpec::new(1, CommitId(0));
        tx1.write(Key::new(1, 10), b"v1".to_vec());
        certifier.commit(tx1);

        let mut tx2 = TransactionDependencySpec::new(2, CommitId(0));
        tx2.write(Key::new(1, 10), b"v2".to_vec());
        let result = certifier.validate(&tx2);
        assert_eq!(
            result,
            Err(CertificationConflict::WriteWrite {
                key: Key::new(1, 10)
            })
        );
        assert_eq!(certifier.metrics().write_write_conflicts, 1);
    }

    #[test]
    fn read_write_antidependency_is_detected() {
        let mut certifier = SerializableCertifier::new(CertifierAlgorithm::PreciseValidation);

        let mut tx1 = TransactionDependencySpec::new(1, CommitId(0));
        tx1.write(Key::new(1, 10), b"v1".to_vec());
        certifier.commit(tx1);

        let mut tx2 = TransactionDependencySpec::new(2, CommitId(0));
        tx2.read(Key::new(1, 10));
        tx2.write(Key::new(1, 20), b"v2".to_vec());
        let result = certifier.validate(&tx2);
        assert_eq!(
            result,
            Err(CertificationConflict::ReadWriteAntiDependency {
                key: Key::new(1, 10)
            })
        );
        assert_eq!(certifier.metrics().read_write_conflicts, 1);
    }

    #[test]
    fn range_anti_dependency_is_detected() {
        let mut certifier = SerializableCertifier::new(CertifierAlgorithm::PreciseValidation);

        // Tx1 inserts row 15
        let mut tx1 = TransactionDependencySpec::new(1, CommitId(0));
        tx1.write(Key::new(1, 15), b"v15".to_vec());
        certifier.commit(tx1);

        // Tx2 scanned range 10..=20 from snapshot 0
        let mut tx2 = TransactionDependencySpec::new(2, CommitId(0));
        tx2.read_range(Key::new(1, 10), Key::new(1, 20));
        tx2.write(Key::new(1, 99), b"v99".to_vec());
        let result = certifier.validate(&tx2);
        assert_eq!(
            result,
            Err(CertificationConflict::RangeAntiDependency {
                start: Key::new(1, 10),
                end: Key::new(1, 20),
            })
        );
        assert_eq!(certifier.metrics().range_conflicts, 1);
    }

    #[test]
    fn ssi_cycle_detection_aborts_write_skew() {
        let mut certifier = SerializableCertifier::new(CertifierAlgorithm::SsiLike);

        // Tx1: reads key 1, writes key 2
        let mut tx1 = TransactionDependencySpec::new(1, CommitId(0));
        tx1.read(Key::new(1, 1));
        tx1.write(Key::new(1, 2), b"tx1".to_vec());
        assert!(certifier.validate(&tx1).is_ok());
        certifier.commit(tx1);

        // Tx2: reads key 2, writes key 1 (classic write-skew cycle)
        let mut tx2 = TransactionDependencySpec::new(2, CommitId(0));
        tx2.read(Key::new(1, 2));
        tx2.write(Key::new(1, 1), b"tx2".to_vec());
        let result = certifier.validate(&tx2);
        assert_eq!(result, Err(CertificationConflict::DependencyCycle));
        assert_eq!(certifier.metrics().cycle_conflicts, 1);
    }
}
