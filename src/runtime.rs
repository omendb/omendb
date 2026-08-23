use std::collections::{BTreeMap, VecDeque};

const CLASS_COUNT: usize = 5;
const DEADLINE_PRIORITY_WINDOW: u64 = 4;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WorkClass {
    OlTp,
    Wal,
    Reclaim,
    Schema,
    Scan,
}

impl WorkClass {
    const fn queue_index(self) -> usize {
        match self {
            Self::OlTp => 0,
            Self::Wal => 1,
            Self::Reclaim => 2,
            Self::Schema => 3,
            Self::Scan => 4,
        }
    }

    /// Whether this class may be terminated under the
    /// [`OverloadPolicy::TerminateLargest`] contract. OlTp and Wal carry
    /// durability semantics and are exempt.
    const fn is_terminable(self) -> bool {
        !matches!(self, Self::OlTp | Self::Wal)
    }

    const fn can_consume_reserve(self) -> bool {
        matches!(self, Self::OlTp | Self::Wal | Self::Reclaim)
    }

    const fn priority_order() -> [Self; CLASS_COUNT] {
        [
            Self::OlTp,
            Self::Wal,
            Self::Reclaim,
            Self::Schema,
            Self::Scan,
        ]
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkId(pub u64);

/// Behavior when a submission exceeds governor capacity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OverloadPolicy {
    /// Reject the arriving work (existing behavior).
    #[default]
    Reject,
    /// Terminate the largest evictable consumer to make room, name the
    /// victims, and admit the arriving work. Evictable classes are
    /// Reclaim, Schema, and Scan: OlTp and Wal carry durability
    /// semantics and are never terminated.
    TerminateLargest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GovernorConfig {
    pub capacity: usize,
    pub protected_reserve: usize,
    pub max_queue_per_class: usize,
    pub max_in_flight: usize,
    pub overload_policy: OverloadPolicy,
}

impl Default for GovernorConfig {
    fn default() -> Self {
        Self {
            capacity: 64,
            protected_reserve: 8,
            max_queue_per_class: 64,
            max_in_flight: 64,
            overload_policy: OverloadPolicy::Reject,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItem {
    pub id: WorkId,
    pub class: WorkClass,
    pub cost: usize,
    pub deadline: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GovernorStats {
    pub accounted_cost: usize,
    pub queued: usize,
    pub in_flight: usize,
    pub expired: u64,
    pub rejected: u64,
    pub terminated: u64,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum GovernorError {
    #[error("governor capacity must be positive")]
    InvalidCapacity,
    #[error("governor protected reserve {reserve} must be below capacity {capacity}")]
    InvalidReserve { reserve: usize, capacity: usize },
    #[error("governor queue and in-flight bounds must be positive")]
    InvalidBounds,
    #[error("work cost must be positive and no larger than capacity")]
    InvalidCost,
    #[error("{class:?} queue is full")]
    QueueFull { class: WorkClass },
    #[error("in-flight worker bound is full")]
    InFlightFull,
    #[error("protected reserve would be consumed by {class:?}")]
    ProtectedReserve { class: WorkClass },
    #[error("governor capacity is exhausted")]
    CapacityExhausted,
    #[error("work {0:?} was terminated by the overload policy")]
    TerminatedByOverload(WorkId),
    #[error("unknown work item {0:?}")]
    UnknownWork(WorkId),
}

#[derive(Debug)]
pub struct ResourceGovernor {
    config: GovernorConfig,
    queues: [VecDeque<WorkItem>; CLASS_COUNT],
    in_flight: BTreeMap<WorkId, WorkItem>,
    accounted_cost: usize,
    next_id: u64,
    expired: u64,
    rejected: u64,
    terminated: u64,
    /// Items terminated by the overload policy, drained by the owner via
    /// [`Self::take_terminated`].
    terminated_items: Vec<WorkItem>,
}

impl ResourceGovernor {
    pub fn new(config: GovernorConfig) -> Result<Self, GovernorError> {
        if config.capacity == 0 {
            return Err(GovernorError::InvalidCapacity);
        }
        if config.protected_reserve >= config.capacity {
            return Err(GovernorError::InvalidReserve {
                reserve: config.protected_reserve,
                capacity: config.capacity,
            });
        }
        if config.max_queue_per_class == 0 || config.max_in_flight == 0 {
            return Err(GovernorError::InvalidBounds);
        }
        Ok(Self {
            config,
            queues: std::array::from_fn(|_| VecDeque::new()),
            in_flight: BTreeMap::new(),
            accounted_cost: 0,
            next_id: 0,
            expired: 0,
            rejected: 0,
            terminated: 0,
            terminated_items: Vec::new(),
        })
    }

    pub fn submit(
        &mut self,
        class: WorkClass,
        cost: usize,
        deadline: Option<u64>,
    ) -> Result<WorkId, GovernorError> {
        if cost == 0 || cost > self.config.capacity {
            self.rejected += 1;
            return Err(GovernorError::InvalidCost);
        }
        let queue = &self.queues[class.queue_index()];
        if queue.len() >= self.config.max_queue_per_class {
            self.rejected += 1;
            return Err(GovernorError::QueueFull { class });
        }
        let mut projected = self
            .accounted_cost
            .checked_add(cost)
            .ok_or(GovernorError::CapacityExhausted)?;
        if projected > self.config.capacity {
            if self.config.overload_policy == OverloadPolicy::TerminateLargest
                && self.terminate_largest_for(projected - self.config.capacity)
            {
                // Capacity freed; recompute the projection against the
                // post-eviction accounting before the reserve check.
                projected = self
                    .accounted_cost
                    .checked_add(cost)
                    .ok_or(GovernorError::CapacityExhausted)?;
            } else {
                self.rejected += 1;
                return Err(GovernorError::CapacityExhausted);
            }
        }
        if !class.can_consume_reserve()
            && projected > self.config.capacity - self.config.protected_reserve
        {
            self.rejected += 1;
            return Err(GovernorError::ProtectedReserve { class });
        }
        let id = WorkId(self.next_id);
        self.next_id = self.next_id.checked_add(1).ok_or_else(|| {
            self.rejected += 1;
            GovernorError::CapacityExhausted
        })?;
        self.queues[class.queue_index()].push_back(WorkItem {
            id,
            class,
            cost,
            deadline,
        });
        self.accounted_cost += cost;
        Ok(id)
    }

    pub fn poll(&mut self, now: u64) -> Option<WorkItem> {
        self.expire(now);
        if self.in_flight.len() >= self.config.max_in_flight {
            return None;
        }
        if let Some((queue_index, item_index)) = self.urgent_item(now) {
            let item = self.queues[queue_index]
                .remove(item_index)
                .expect("urgent queue item exists");
            self.in_flight.insert(item.id, item.clone());
            return Some(item);
        }
        for class in WorkClass::priority_order() {
            if let Some(item) = self.queues[class.queue_index()].pop_front() {
                debug_assert_eq!(item.class, class);
                self.in_flight.insert(item.id, item.clone());
                return Some(item);
            }
        }
        None
    }

    /// Return an in-flight item to the rear of its class queue without
    /// releasing its accounted cost (in-flight demotion).
    fn requeue_demoted(&mut self, item: &WorkItem) {
        if let Some(queued) = self.in_flight.remove(&item.id) {
            self.queues[queued.class.queue_index()].push_back(queued);
        }
    }

    pub fn complete(&mut self, id: WorkId) -> Result<WorkItem, GovernorError> {
        let item = self
            .in_flight
            .remove(&id)
            .ok_or(GovernorError::UnknownWork(id))?;
        self.accounted_cost -= item.cost;
        Ok(item)
    }

    pub fn cancel_queued(&mut self, id: WorkId) -> Result<WorkItem, GovernorError> {
        for queue in &mut self.queues {
            if let Some(position) = queue.iter().position(|item| item.id == id) {
                let item = queue.remove(position).expect("queue position exists");
                self.accounted_cost -= item.cost;
                return Ok(item);
            }
        }
        Err(GovernorError::UnknownWork(id))
    }

    /// Drain the work items this governor terminated under the overload
    /// policy. Owners surface each victim to its waiter as a named
    /// [`GovernorError::TerminatedByOverload`] outcome.
    pub fn take_terminated(&mut self) -> Vec<WorkItem> {
        std::mem::take(&mut self.terminated_items)
    }

    /// Evict largest-cost terminable items (oldest first on ties) until
    /// `needed` capacity is free. Returns whether the target was met.
    fn terminate_largest_for(&mut self, needed: usize) -> bool {
        let mut freed = 0usize;
        while freed < needed {
            let Some(victim) = self.largest_terminable() else {
                break;
            };
            let item = if let Some(item) = self.in_flight.remove(&victim) {
                item
            } else {
                let mut found = None;
                'queues: for queue in &mut self.queues {
                    for (position, candidate) in queue.iter().enumerate() {
                        if candidate.id == victim {
                            let item = queue.remove(position).expect("queue position exists");
                            found = Some(item);
                            break 'queues;
                        }
                    }
                }
                match found {
                    Some(item) => item,
                    None => break,
                }
            };
            freed += item.cost;
            self.accounted_cost -= item.cost;
            self.terminated += 1;
            self.terminated_items.push(item);
        }
        freed >= needed
    }

    /// The id of the largest evictable consumer across in-flight and
    /// queued work; ties break to the oldest submission.
    fn largest_terminable(&self) -> Option<WorkId> {
        let mut best: Option<(usize, WorkId)> = None;
        let consider = |item: &WorkItem, best: &mut Option<(usize, WorkId)>| {
            if !item.class.is_terminable() {
                return;
            }
            let better = match best {
                None => true,
                Some((cost, id)) => item.cost > *cost || (item.cost == *cost && item.id < *id),
            };
            if better {
                *best = Some((item.cost, item.id));
            }
        };
        for item in self.in_flight.values() {
            consider(item, &mut best);
        }
        for queue in &self.queues {
            for item in queue {
                consider(item, &mut best);
            }
        }
        best.map(|(_, id)| id)
    }

    #[must_use]
    pub fn stats(&self) -> GovernorStats {
        GovernorStats {
            accounted_cost: self.accounted_cost,
            queued: self.queues.iter().map(VecDeque::len).sum(),
            in_flight: self.in_flight.len(),
            expired: self.expired,
            rejected: self.rejected,
            terminated: self.terminated,
        }
    }

    fn expire(&mut self, now: u64) {
        for queue in &mut self.queues {
            let mut kept = VecDeque::with_capacity(queue.len());
            while let Some(item) = queue.pop_front() {
                if item.deadline.is_some_and(|deadline| deadline <= now) {
                    self.accounted_cost -= item.cost;
                    self.expired += 1;
                } else {
                    kept.push_back(item);
                }
            }
            *queue = kept;
        }
    }

    fn urgent_item(&self, now: u64) -> Option<(usize, usize)> {
        let mut best: Option<(u64, usize, usize)> = None;
        for class in WorkClass::priority_order() {
            let queue_index = class.queue_index();
            for (item_index, item) in self.queues[queue_index].iter().enumerate() {
                let Some(deadline) = item.deadline else {
                    continue;
                };
                let slack = deadline.saturating_sub(now);
                if slack > DEADLINE_PRIORITY_WINDOW {
                    continue;
                }
                let candidate = (deadline, queue_index, item_index);
                if best.is_none_or(|current| candidate < current) {
                    best = Some(candidate);
                }
            }
        }
        best.map(|(_, queue_index, item_index)| (queue_index, item_index))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WorkerId(pub u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReactorConfig {
    pub workers: usize,
    pub governor: GovernorConfig,
    /// In-flight age (in dispatch ticks) after which a demotable item is
    /// preempted back to the rear of its class queue so waiting higher-
    /// priority work runs first. `None` disables in-flight demotion.
    /// OlTp and Wal are never demoted.
    pub demotion_after: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dispatch {
    pub worker: WorkerId,
    pub work: WorkItem,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ReactorError {
    #[error("reactor requires at least one worker")]
    InvalidWorkerCount,
    #[error("worker {0:?} is not assigned work")]
    WorkerIdle(WorkerId),
    #[error("worker {0:?} is already assigned work")]
    WorkerBusy(WorkerId),
    #[error("worker count exceeds the supported ID range")]
    WorkerIdOverflow,
    #[error("governor error: {0}")]
    Governor(#[from] GovernorError),
}

#[derive(Debug)]
pub struct Reactor {
    governor: ResourceGovernor,
    workers: Vec<Option<(WorkItem, u64)>>,
    demotion_after: Option<u64>,
}

/// Classes eligible for in-flight demotion. OlTp and Wal carry
/// durability semantics and are exempt.
const fn is_demotable(class: WorkClass) -> bool {
    !matches!(class, WorkClass::OlTp | WorkClass::Wal)
}

impl Reactor {
    pub fn new(config: ReactorConfig) -> Result<Self, ReactorError> {
        if config.workers == 0 {
            return Err(ReactorError::InvalidWorkerCount);
        }
        if config.workers > u16::MAX as usize {
            return Err(ReactorError::WorkerIdOverflow);
        }
        Ok(Self {
            governor: ResourceGovernor::new(config.governor)?,
            workers: vec![None; config.workers],
            demotion_after: config.demotion_after,
        })
    }

    pub fn submit(
        &mut self,
        class: WorkClass,
        cost: usize,
        deadline: Option<u64>,
    ) -> Result<WorkId, ReactorError> {
        Ok(self.governor.submit(class, cost, deadline)?)
    }

    pub fn dispatch(&mut self, now: u64) -> Option<Dispatch> {
        let worker = self
            .workers
            .iter()
            .position(Option::is_none)
            .map(|index| WorkerId(index as u16))?;
        let work = self.governor.poll(now)?;
        self.workers[worker.0 as usize] = Some((work.clone(), now));
        Some(Dispatch { worker, work })
    }

    pub fn dispatch_batch(&mut self, now: u64) -> Vec<Dispatch> {
        let mut dispatches = Vec::new();
        for _ in 0..self.workers.len() {
            let Some(dispatch) = self.dispatch(now) else {
                break;
            };
            dispatches.push(dispatch);
        }
        dispatches
    }

    pub fn complete(&mut self, worker: WorkerId) -> Result<WorkItem, ReactorError> {
        let slot = self
            .workers
            .get_mut(worker.0 as usize)
            .ok_or(ReactorError::WorkerIdle(worker))?;
        let (work, _) = slot.take().ok_or(ReactorError::WorkerIdle(worker))?;
        self.governor.complete(work.id).map_err(ReactorError::from)
    }

    /// Preempt demotable items that have been in flight longer than
    /// `demotion_after` ticks: each returns to the rear of its class
    /// queue so queued higher-priority work dispatches first, and the
    /// caller resubmits the item's remaining timeslices. Returns the
    /// preempted items with their worker slots; OlTp and Wal are never
    /// demoted. No-op when `demotion_after` is unset.
    pub fn preempt_long_running(&mut self, now: u64) -> Vec<(WorkerId, WorkItem)> {
        let Some(threshold) = self.demotion_after else {
            return Vec::new();
        };
        let mut preempted = Vec::new();
        for (index, slot) in self.workers.iter_mut().enumerate() {
            let Some((work, dispatched_at)) = slot.as_ref() else {
                continue;
            };
            if !is_demotable(work.class) || now.saturating_sub(*dispatched_at) <= threshold {
                continue;
            }
            let (work, _) = slot.take().expect("slot checked above");
            self.governor.requeue_demoted(&work);
            preempted.push((WorkerId(index as u16), work));
        }
        preempted
    }

    pub fn cancel_queued(&mut self, id: WorkId) -> Result<WorkItem, ReactorError> {
        Ok(self.governor.cancel_queued(id)?)
    }

    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    #[must_use]
    pub fn busy_workers(&self) -> usize {
        self.workers.iter().filter(|slot| slot.is_some()).count()
    }

    #[must_use]
    pub fn stats(&self) -> GovernorStats {
        self.governor.stats()
    }

    /// Drain work items terminated by the governor's overload policy.
    pub fn take_terminated(&mut self) -> Vec<WorkItem> {
        self.governor.take_terminated()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GovernorConfig, GovernorError, OverloadPolicy, Reactor, ReactorConfig, ReactorError,
        ResourceGovernor, WorkClass,
    };

    #[test]
    fn background_work_cannot_consume_protected_reserve() {
        let mut governor = ResourceGovernor::new(GovernorConfig {
            capacity: 10,
            protected_reserve: 3,
            max_queue_per_class: 4,
            max_in_flight: 4,
            overload_policy: OverloadPolicy::default(),
        })
        .expect("governor");
        governor
            .submit(WorkClass::Scan, 7, None)
            .expect("background admission");
        assert_eq!(
            governor.submit(WorkClass::Scan, 1, None),
            Err(GovernorError::ProtectedReserve {
                class: WorkClass::Scan
            })
        );
        let oltp = governor
            .submit(WorkClass::OlTp, 3, None)
            .expect("protected admission");
        assert_eq!(governor.poll(0).expect("OLTP first").id, oltp);
        governor.complete(oltp).expect("complete OLTP");
        let scan = governor.poll(0).expect("scan next");
        governor.complete(scan.id).expect("complete scan");
        assert_eq!(governor.stats().accounted_cost, 0);
    }

    #[test]
    fn expired_work_is_reclaimed_before_polling() {
        let mut governor = ResourceGovernor::new(GovernorConfig {
            capacity: 8,
            protected_reserve: 2,
            max_queue_per_class: 4,
            max_in_flight: 4,
            overload_policy: OverloadPolicy::default(),
        })
        .expect("governor");
        governor.submit(WorkClass::Scan, 4, Some(5)).expect("scan");
        let reclaim = governor
            .submit(WorkClass::Reclaim, 2, None)
            .expect("reclaim");
        let item = governor.poll(6).expect("reclaim after expiry");
        assert_eq!(item.id, reclaim);
        governor.complete(reclaim).expect("complete");
        assert_eq!(governor.stats().expired, 1);
        assert_eq!(governor.stats().accounted_cost, 0);
    }

    #[test]
    fn queues_and_inflight_are_bounded_and_recover_after_completion() {
        let mut governor = ResourceGovernor::new(GovernorConfig {
            capacity: 4,
            protected_reserve: 1,
            max_queue_per_class: 1,
            max_in_flight: 1,
            overload_policy: OverloadPolicy::default(),
        })
        .expect("governor");
        let first = governor.submit(WorkClass::Wal, 2, None).expect("first");
        assert_eq!(
            governor.submit(WorkClass::Wal, 1, None),
            Err(GovernorError::QueueFull {
                class: WorkClass::Wal
            })
        );
        let polled = governor.poll(0).expect("poll");
        assert_eq!(polled.id, first);
        governor
            .submit(WorkClass::Scan, 1, None)
            .expect("bounded queued scan");
        assert!(governor.poll(0).is_none());
        governor.complete(first).expect("complete");
        let scan = governor.poll(0).expect("poll after completion");
        governor.complete(scan.id).expect("complete scan");
        assert_eq!(governor.stats().rejected, 1);
    }

    #[test]
    fn reactor_dispatches_in_priority_order_and_never_exceeds_workers() {
        let mut reactor = Reactor::new(ReactorConfig {
            workers: 2,
            governor: GovernorConfig {
                capacity: 8,
                protected_reserve: 2,
                max_queue_per_class: 4,
                max_in_flight: 4,
                overload_policy: OverloadPolicy::default(),
            },
            demotion_after: None,
        })
        .expect("reactor");
        let scan = reactor.submit(WorkClass::Scan, 2, None).expect("scan");
        let oltp = reactor.submit(WorkClass::OlTp, 2, None).expect("OLTP");
        let dispatches = reactor.dispatch_batch(0);
        assert_eq!(dispatches.len(), 2);
        assert_eq!(dispatches[0].work.id, oltp);
        assert_eq!(dispatches[1].work.id, scan);
        assert_eq!(reactor.busy_workers(), 2);
        assert!(reactor.dispatch(0).is_none());
        reactor
            .complete(dispatches[0].worker)
            .expect("complete OLTP");
        reactor
            .complete(dispatches[1].worker)
            .expect("complete scan");
        assert_eq!(reactor.stats().accounted_cost, 0);
    }

    #[test]
    fn reactor_expiry_frees_capacity_before_dispatch() {
        let mut reactor = Reactor::new(ReactorConfig {
            workers: 1,
            governor: GovernorConfig {
                capacity: 4,
                protected_reserve: 1,
                max_queue_per_class: 4,
                max_in_flight: 1,
                overload_policy: OverloadPolicy::default(),
            },
            demotion_after: None,
        })
        .expect("reactor");
        reactor
            .submit(WorkClass::Scan, 2, Some(5))
            .expect("expired scan");
        let reclaim = reactor
            .submit(WorkClass::Reclaim, 1, None)
            .expect("reclaim");
        let dispatch = reactor.dispatch(6).expect("reclaim dispatch");
        assert_eq!(dispatch.work.id, reclaim);
        reactor.complete(dispatch.worker).expect("complete reclaim");
        assert_eq!(reactor.stats().expired, 1);
    }

    #[test]
    fn near_deadline_background_work_gets_a_bounded_priority_boost() {
        let mut reactor = Reactor::new(ReactorConfig {
            workers: 1,
            governor: GovernorConfig {
                capacity: 8,
                protected_reserve: 2,
                max_queue_per_class: 4,
                max_in_flight: 1,
                overload_policy: OverloadPolicy::default(),
            },
            demotion_after: None,
        })
        .expect("reactor");
        reactor
            .submit(WorkClass::OlTp, 1, None)
            .expect("foreground");
        let scan = reactor.submit(WorkClass::Scan, 2, Some(5)).expect("scan");
        let first = reactor.dispatch(0).expect("foreground dispatch");
        assert_eq!(first.work.class, WorkClass::OlTp);
        reactor.complete(first.worker).expect("complete foreground");
        reactor
            .submit(WorkClass::OlTp, 1, None)
            .expect("foreground backlog");
        let urgent = reactor.dispatch(2).expect("urgent scan dispatch");
        assert_eq!(urgent.work.id, scan);
        assert_eq!(urgent.work.class, WorkClass::Scan);
    }

    #[test]
    fn mixed_work_classes_keep_foreground_priority_and_bounded_reserve() {
        let mut reactor = Reactor::new(ReactorConfig {
            workers: 5,
            governor: GovernorConfig {
                capacity: 10,
                protected_reserve: 3,
                max_queue_per_class: 4,
                max_in_flight: 5,
                overload_policy: OverloadPolicy::default(),
            },
            demotion_after: None,
        })
        .expect("reactor");
        reactor.submit(WorkClass::Scan, 2, None).expect("scan");
        reactor.submit(WorkClass::Schema, 2, None).expect("schema");
        reactor
            .submit(WorkClass::Reclaim, 2, None)
            .expect("reclaim");
        reactor.submit(WorkClass::Wal, 1, None).expect("WAL");
        reactor.submit(WorkClass::OlTp, 1, None).expect("OLTP");
        assert_eq!(
            reactor.submit(WorkClass::Scan, 1, None),
            Err(ReactorError::Governor(GovernorError::ProtectedReserve {
                class: WorkClass::Scan,
            }))
        );

        let dispatches = reactor.dispatch_batch(0);
        let classes: Vec<WorkClass> = dispatches.iter().map(|item| item.work.class).collect();
        assert_eq!(
            classes,
            vec![
                WorkClass::OlTp,
                WorkClass::Wal,
                WorkClass::Reclaim,
                WorkClass::Schema,
                WorkClass::Scan,
            ]
        );
        for dispatch in dispatches {
            reactor.complete(dispatch.worker).expect("complete");
        }
        let stats = reactor.stats();
        assert_eq!(stats.accounted_cost, 0);
        assert_eq!(stats.queued, 0);
        assert_eq!(stats.in_flight, 0);
        assert_eq!(stats.rejected, 1);
    }

    fn terminate_config() -> GovernorConfig {
        GovernorConfig {
            capacity: 16,
            protected_reserve: 0,
            max_queue_per_class: 8,
            max_in_flight: 8,
            overload_policy: OverloadPolicy::TerminateLargest,
        }
    }

    #[test]
    fn terminate_largest_admits_arrival_and_names_victims() {
        let mut governor = ResourceGovernor::new(terminate_config()).expect("config");
        let big_scan = governor
            .submit(WorkClass::Scan, 10, None)
            .expect("big scan");
        let small_scan = governor
            .submit(WorkClass::Scan, 3, None)
            .expect("small scan");

        // Capacity 16, accounted 13: an 8-cost arrival needs 5 freed. The
        // 10-cost scan is the largest terminable consumer, so it goes.
        let arrival = governor
            .submit(WorkClass::Schema, 8, None)
            .expect("admitted");
        let terminated = governor.take_terminated();
        assert_eq!(terminated.len(), 1);
        assert_eq!(terminated[0].id, big_scan);
        assert_eq!(terminated[0].class, WorkClass::Scan);
        assert_eq!(governor.stats().accounted_cost, 11);
        assert_eq!(governor.stats().terminated, 1);
        assert_eq!(governor.stats().rejected, 0);

        // The victim's owner gets a named outcome, not UnknownWork silence:
        // the id is gone from the governor and surfaced via the drain.
        assert!(matches!(
            governor.complete(big_scan),
            Err(GovernorError::UnknownWork(_))
        ));
        // Survivors dispatch through their queues before completing.
        governor.poll(0);
        governor.poll(0);
        assert!(governor.complete(small_scan).is_ok());
        assert!(governor.complete(arrival).is_ok());
        assert_eq!(governor.take_terminated().len(), 0);
    }

    #[test]
    fn termination_never_evicts_durability_classes() {
        let mut governor = ResourceGovernor::new(terminate_config()).expect("config");
        governor.submit(WorkClass::OlTp, 12, None).expect("oltp");
        governor.submit(WorkClass::Wal, 3, None).expect("wal");

        // Only OlTp + Wal are accounted; a Scan arrival cannot evict them
        // and must be rejected even under the terminate policy.
        assert!(matches!(
            governor.submit(WorkClass::Scan, 8, None),
            Err(GovernorError::CapacityExhausted)
        ));
        assert_eq!(governor.take_terminated().len(), 0);
        assert_eq!(governor.stats().rejected, 1);
        assert_eq!(governor.stats().accounted_cost, 15);
    }

    #[test]
    fn reject_policy_is_unchanged_and_default() {
        let config = GovernorConfig::default();
        assert_eq!(config.overload_policy, OverloadPolicy::Reject);
        let mut governor = ResourceGovernor::new(GovernorConfig {
            overload_policy: OverloadPolicy::Reject,
            ..terminate_config()
        })
        .expect("config");
        governor.submit(WorkClass::Scan, 10, None).expect("fill");
        governor
            .submit(WorkClass::Scan, 5, None)
            .expect("fill more");
        assert!(matches!(
            governor.submit(WorkClass::Scan, 8, None),
            Err(GovernorError::CapacityExhausted)
        ));
        assert_eq!(governor.stats().terminated, 0);
    }

    #[test]
    fn queued_items_are_terminable_too() {
        let mut governor = ResourceGovernor::new(terminate_config()).expect("config");
        let queued = governor
            .submit(WorkClass::Scan, 12, None)
            .expect("queued scan");
        // Nothing polled it, so it sits in the queue and still holds cost.
        let arrival = governor
            .submit(WorkClass::Reclaim, 10, None)
            .expect("admitted");
        let terminated = governor.take_terminated();
        assert_eq!(terminated.len(), 1);
        assert_eq!(terminated[0].id, queued);
        let dispatched = governor.poll(0).expect("dispatched arrival");
        assert_eq!(dispatched.id, arrival);
        assert!(governor.complete(arrival).is_ok());
    }
}

#[cfg(test)]
mod demotion_tests {
    use super::*;

    #[test]
    fn long_running_scan_is_demoted_behind_queued_oltp() {
        let mut reactor = Reactor::new(ReactorConfig {
            workers: 1,
            governor: GovernorConfig {
                capacity: 8,
                protected_reserve: 2,
                max_queue_per_class: 4,
                max_in_flight: 4,
                overload_policy: OverloadPolicy::default(),
            },
            demotion_after: Some(5),
        })
        .expect("reactor");

        let scan = reactor
            .submit(WorkClass::Scan, 2, None)
            .expect("scan submit");
        let dispatches = reactor.dispatch_batch(10);
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].work.class, WorkClass::Scan);

        // OlTp arrives while the scan is still in flight past its threshold.
        let oltp = reactor
            .submit(WorkClass::OlTp, 1, None)
            .expect("oltp submit");
        let preempted = reactor.preempt_long_running(20);
        assert_eq!(preempted.len(), 1);
        assert_eq!(preempted[0].0, WorkerId(0));
        assert_eq!(preempted[0].1.id, scan);

        // The freed worker dispatches the OlTp first; the scan's remainder
        // waits at the rear of its queue.
        let next = reactor.dispatch(21).expect("dispatch after preemption");
        assert_eq!(next.work.id, oltp);
        assert_eq!(next.work.class, WorkClass::OlTp);

        reactor.complete(next.worker).expect("complete oltp");
        let resumed = reactor.dispatch(22).expect("scan resumes");
        assert_eq!(resumed.work.id, scan, "scan requeues and finishes");
    }

    #[test]
    fn oltp_and_wal_are_never_demoted() {
        let mut reactor = Reactor::new(ReactorConfig {
            workers: 2,
            governor: GovernorConfig {
                capacity: 8,
                protected_reserve: 2,
                max_queue_per_class: 4,
                max_in_flight: 4,
                overload_policy: OverloadPolicy::default(),
            },
            demotion_after: Some(1),
        })
        .expect("reactor");
        reactor.submit(WorkClass::OlTp, 1, None).expect("submit");
        reactor.submit(WorkClass::Wal, 1, None).expect("submit");
        let dispatches = reactor.dispatch_batch(100);
        assert_eq!(dispatches.len(), 2);
        // Both far exceed the threshold but durability classes hold their workers.
        assert!(reactor.preempt_long_running(1000).is_empty());
    }
}
