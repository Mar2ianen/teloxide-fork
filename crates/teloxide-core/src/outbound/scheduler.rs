//! Deterministic outbound scheduling model (Commit 1).
//!
//! Pure state machine: time is passed as a parameter, no Tokio, no actor.
//! The actor, completion-aware permits and the public API are added in
//! Commit 2 on top of this model.
//!
//! Invariants:
//!
//! - one enqueue yields at most one permit;
//! - FIFO within the same effective priority; ordering lanes are strictly FIFO
//!   in enqueue order regardless of priority or `not_before`;
//! - at most one in-flight request per ordering lane, and only the lane head
//!   can be granted;
//! - cancelling a waiting job removes it without a phantom lock;
//! - rolling-window budget is debited at grant time and never refunded;
//! - `RetryAfter` penalties carry an explicit scope and are never derived from
//!   the request scope;
//! - the scheduler never retries requests on its own;
//! - pending jobs of the same latest-wins slot are replaced (the replacement
//!   inherits the queue position and the scheduling age of the superseded job),
//!   and incompatible metadata is rejected instead of silently creating a
//!   second pending job;
//! - a job whose weight fits no applicable window is rejected at enqueue;
//! - a top-aged candidate blocked by window capacity reserves the window: later
//!   jobs consuming the same window are held back until the blocked candidate
//!   can be granted, so a heavy job cannot starve behind a stream of lighter
//!   ones;
//! - arbitration is event-driven: a persistent candidate heap is popped
//!   incrementally, failed candidates sleep in a blocked deadline heap, and a
//!   full collect+sort pass never happens per tick.

use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeSet, BinaryHeap, HashMap, VecDeque},
    num::NonZeroU32,
    time::{Duration, Instant},
};

use super::types::{
    AgingPolicy, EnqueueError, EnqueueOutcome, Grant, JobId, OutboundChatKey, OutboundClass,
    OutboundCompletion, OutboundEnqueueMode, OutboundLaneKey, OutboundLimits, OutboundMeta,
    OutboundPriority, OutboundScope, SchedulerConfigError, SchedulerWakeup, WindowLimit,
};

/// A granted job whose permit is still in flight. The lane is released on
/// completion; the penalty scope is carried by the completion itself, so it
/// is not stored here.
struct InFlight {
    lane: Option<OutboundLaneKey>,
}

/// A live job that has not been granted yet.
struct Job {
    meta: OutboundMeta,
    sequence: u64,
    /// The moment the job became ready; aging is measured from this instant.
    ready_at: Instant,
    /// The delay deadline, if the job is still waiting for time. Cleared when
    /// the job becomes ready.
    not_before: Option<Instant>,
    /// The latest-wins slot this job belongs to, if any. Used to clean up
    /// the coalesce map when the job is granted or cancelled.
    coalesce_key: Option<InternalCoalesceKey>,
    /// The job's position inside its ordering lane, assigned from the
    /// lane's own counter at enqueue (and inherited by a replacement).
    /// The lane counter is independent of the global sequence, so lane
    /// FIFO survives the global sequence wraparound. `None` for unlaned
    /// jobs.
    lane_order: Option<u64>,
    /// Whether the job's freshest candidate entry is still in the
    /// candidate heap (cleared when it pops, granted, parked or blocked).
    in_candidate_heap: bool,
    /// The effective priority of the freshest candidate entry for this
    /// job; entries with a lower effective are stale and skipped on pop.
    candidate_effective: u8,
}

/// An ordering lane: strictly FIFO in sequence order. The lane head is the
/// only job that can be granted; the head can still be a delayed job, which
/// blocks the whole lane until it becomes ready.
///
/// Pending jobs are keyed by `(lane order, job id)` for O(log N) insertion
/// and head removal. The order comes from the lane's own counter, so lane
/// FIFO is immune to the global sequence wraparound. Cancelled or
/// superseded entries are dropped lazily when they reach the head and
/// counted in `stale` for periodic compaction.
struct LaneState {
    pending: BTreeSet<(u64, JobId)>,
    in_flight: Option<JobId>,
    /// Order counter of the next enqueued job; wraps after 2^64 enqueues
    /// to this lane (unobservable in practice).
    next_order: u64,
    /// Lane-pending entries whose job is gone; compacted when they
    /// dominate the pending set.
    stale: usize,
    /// Whether the current head is already represented in the candidate
    /// heap; prevents duplicate lane entries.
    in_candidate_heap: bool,
    /// The effective priority of the freshest candidate entry for this
    /// lane; entries with a lower effective are stale and skipped on pop.
    candidate_effective: u8,
}

struct DelayedJob {
    not_before: Instant,
    sequence: u64,
    job: JobId,
}

impl Ord for DelayedJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.not_before
            .cmp(&other.not_before)
            .then_with(|| self.sequence.cmp(&other.sequence))
            .then_with(|| self.job.cmp(&other.job))
    }
}

impl PartialOrd for DelayedJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DelayedJob {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for DelayedJob {}

/// Identity of a latest-wins slot, derived by the scheduler from the request
/// metadata. The caller only supplies the `user_key`; scope, lane and class
/// always come from the `OutboundMeta`, so a replacement can never change
/// them. The accounting weight is deliberately not part of the key: a weight
/// change on an existing slot is rejected with an explicit error.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct InternalCoalesceKey {
    scope: OutboundScope,
    lane: Option<OutboundLaneKey>,
    class: OutboundClass,
    user_key: u64,
}

/// Sliding window with weighted entries. The budget is debited at grant time
/// and never refunded.
struct RollingWindow {
    capacity: u32,
    window: Duration,
    history: VecDeque<(Instant, u32)>,
    used: u64,
}

impl RollingWindow {
    fn new(limit: WindowLimit) -> Self {
        Self { capacity: limit.capacity, window: limit.window, history: VecDeque::new(), used: 0 }
    }

    fn prune(&mut self, now: Instant) {
        let Some(cutoff) = now.checked_sub(self.window) else { return };
        while let Some(&(at, weight)) = self.history.front() {
            if at <= cutoff {
                self.used -= u64::from(weight);
                self.history.pop_front();
            } else {
                break;
            }
        }
    }

    fn is_idle(&mut self, now: Instant) -> bool {
        self.prune(now);
        self.history.is_empty()
    }

    fn can_consume(&mut self, now: Instant, weight: u32) -> bool {
        self.prune(now);
        self.used + u64::from(weight) <= u64::from(self.capacity)
    }

    fn consume(&mut self, now: Instant, weight: u32) {
        debug_assert!(
            self.can_consume(now, weight),
            "window budget must be admitted before consumption"
        );
        self.history.push_back((now, weight));
        self.used += u64::from(weight);
    }

    /// The earliest moment at which `weight` would fit again.
    ///
    /// The history is not pruned here: an already expired entry yields an
    /// instant in the past, which the caller caps with `now`.
    fn earliest_for(&self, now: Instant, weight: u32) -> Option<Instant> {
        if self.used + u64::from(weight) <= u64::from(self.capacity) {
            return Some(now);
        }
        let mut remaining = self.used;
        for &(at, w) in &self.history {
            remaining -= u64::from(w);
            if remaining + u64::from(weight) <= u64::from(self.capacity) {
                return Some(at + self.window);
            }
        }
        None
    }
}

/// A set of windows a request must pass all at once.
struct WindowSet {
    windows: Vec<RollingWindow>,
}

impl WindowSet {
    fn new(limits: &[WindowLimit]) -> Self {
        Self { windows: limits.iter().copied().map(RollingWindow::new).collect() }
    }

    fn can_consume(&mut self, now: Instant, weight: u32) -> bool {
        self.windows.iter_mut().all(|window| window.can_consume(now, weight))
    }

    fn consume(&mut self, now: Instant, weight: u32) {
        for window in &mut self.windows {
            window.consume(now, weight);
        }
    }

    /// The earliest moment the whole set admits `weight`: every window must
    /// allow it, so the set is ready when the last blocking window is.
    fn earliest_for(&self, now: Instant, weight: u32) -> Option<Instant> {
        self.windows
            .iter()
            .map(|window| window.earliest_for(now, weight))
            .try_fold(now, |acc, t| Some(acc.max(t?)))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum PenaltyKey {
    Global,
    Chat(OutboundChatKey),
}

/// A job that is ready to be granted: either an unlaned job or the head of
/// a free ordering lane.
#[derive(Clone, Copy)]
struct Candidate {
    job: JobId,
    sequence: u64,
    effective: u8,
    weight: NonZeroU32,
    scope: OutboundScope,
}

/// What a candidate-heap entry refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum CandidateRef {
    Job(JobId),
    Lane(OutboundLaneKey),
}

/// Persistent candidate key: highest effective priority first, FIFO by
/// sequence within one level. The key is validated lazily on pop (aging may
/// have raised the effective priority, the lane head may have changed).
#[derive(Clone, Copy, PartialEq, Eq)]
struct CandidateKey {
    effective: u8,
    sequence: u64,
    reference: CandidateRef,
}

impl Ord for CandidateKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.effective
            .cmp(&other.effective)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| self.reference.cmp(&other.reference))
    }
}

impl PartialOrd for CandidateKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A candidate that failed admission: it sleeps until `until` and is then
/// re-inserted into the candidate heap. The reference is re-validated on
/// promotion, so a lane entry whose head changed wakes the current head.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BlockedJob {
    until: Instant,
    reference: CandidateRef,
}

/// The moment a candidate's effective priority rises by one level, so that
/// the candidate heap can be re-keyed by event instead of by a full
/// recomputation per tick.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct AgingEvent {
    at: Instant,
    reference: CandidateRef,
}

/// A window that a top-aged candidate could not fit: it is held back until
/// `until`, and the candidates that consume it are parked in `queue` so
/// that lighter traffic cannot starve the blocked candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum WindowRef {
    Global,
    Chat(OutboundChatKey),
}

/// A window held back for a blocked top-aged candidate. `until` is the
/// moment the hold ends (re-armed when the head is granted or a candidate
/// is re-blocked); while the hold is active every consumer of the window is
/// parked in `queue`. On expiry exactly the head is promoted per tick, so a
/// rate-limited drain stays linear instead of re-pushing the whole queue.
struct Reservation {
    until: Instant,
    queue: VecDeque<CandidateRef>,
}

/// The verdict of the admission check for one candidate.
#[derive(Clone, Copy)]
enum Admission {
    /// The candidate can be granted right now.
    Pass,
    /// The candidate cannot be granted before `until`; `reserve` is the
    /// window (if any) that must be held back until `until` so that the
    /// candidate is not starved by lighter traffic.
    Blocked { until: Instant, reserve: Option<WindowRef> },
    /// The candidate consumes a window that is already reserved for an
    /// older blocked candidate; it is parked without further processing.
    Reserved,
}

/// Effective priority: base priority raised by aging, capped at the highest
/// level. Aging is measured from the moment the job became ready.
fn effective_priority(job: &Job, aging: &AgingPolicy, now: Instant) -> u8 {
    let base = u64::from(job.meta.priority.get());
    let waited = now.saturating_duration_since(job.ready_at);
    let quantum = aging.quantum.as_nanos().max(1);
    let boost = (waited.as_nanos() / quantum).min(u64::from(aging.max_boost).into());
    (base + boost as u64).min(u64::from(OutboundPriority::HIGHEST.get())) as u8
}

/// Deterministic outbound scheduling state machine.
pub(crate) struct SchedulerState {
    jobs: HashMap<JobId, Job>,
    /// Persistent candidate heap: unlaned ready jobs and free lane heads,
    /// ordered by (effective priority desc, sequence asc). Entries are
    /// validated lazily on pop; dead entries are counted and compacted.
    candidates: BinaryHeap<CandidateKey>,
    /// Candidates that failed admission, ordered by their earliest
    /// eligibility.
    blocked: BinaryHeap<Reverse<BlockedJob>>,
    /// Aging deadlines: when a candidate's effective priority rises, its
    /// heap entry is re-keyed by event.
    aging_events: BinaryHeap<Reverse<AgingEvent>>,
    /// Windows held back for a top-aged candidate that could not fit; the
    /// consumers of a reserved window sleep in its queue.
    reservations: HashMap<WindowRef, Reservation>,
    delayed: BinaryHeap<Reverse<DelayedJob>>,
    lanes: HashMap<OutboundLaneKey, LaneState>,
    coalesce: HashMap<InternalCoalesceKey, JobId>,
    global_windows: WindowSet,
    chat_window_sets: HashMap<OutboundChatKey, WindowSet>,
    penalties: HashMap<PenaltyKey, Instant>,
    in_flight: HashMap<JobId, InFlight>,
    /// Candidate-heap entries whose job/lane is gone; compacted when they
    /// dominate the heap.
    stale_candidates: usize,
    /// Blocked-heap entries whose job/lane is gone; dropped lazily on pop.
    stale_blocked: usize,
    /// Number of delayed-heap entries whose job is gone; compacted
    /// opportunistically so that a hot latest-wins key cannot grow the heap.
    stale_delayed: usize,
    next_sequence: u64,
    next_job_id: u64,
    aging: AgingPolicy,
    chat_limits: Vec<WindowLimit>,
    global_limits: Vec<WindowLimit>,
}

impl SchedulerState {
    pub(crate) fn new(
        limits: OutboundLimits,
        aging: AgingPolicy,
    ) -> Result<Self, SchedulerConfigError> {
        for window in limits.global.iter().chain(limits.chat.iter()) {
            if window.capacity == 0 {
                return Err(SchedulerConfigError::ZeroWindowCapacity);
            }
            if window.window.is_zero() {
                return Err(SchedulerConfigError::ZeroWindowDuration);
            }
        }
        if aging.quantum.is_zero() {
            return Err(SchedulerConfigError::ZeroAgingQuantum);
        }
        // Anti-starvation is a guarantee only if aging can lift a LOWEST
        // job all the way to HIGHEST; otherwise an endless stream of
        // higher-priority jobs could starve it forever.
        if u16::from(aging.max_boost)
            < u16::from(OutboundPriority::HIGHEST.get() - OutboundPriority::LOWEST.get())
        {
            return Err(SchedulerConfigError::AgingCannotReachHighest {
                max_boost: aging.max_boost,
            });
        }
        let global_limits = limits.global;
        let chat_limits = limits.chat;
        let global_windows = WindowSet::new(&global_limits);
        Ok(Self {
            jobs: HashMap::new(),
            candidates: BinaryHeap::new(),
            blocked: BinaryHeap::new(),
            aging_events: BinaryHeap::new(),
            reservations: HashMap::new(),
            delayed: BinaryHeap::new(),
            lanes: HashMap::new(),
            coalesce: HashMap::new(),
            global_windows,
            chat_window_sets: HashMap::new(),
            penalties: HashMap::new(),
            in_flight: HashMap::new(),
            stale_candidates: 0,
            stale_blocked: 0,
            stale_delayed: 0,
            next_sequence: 0,
            next_job_id: 0,
            aging,
            chat_limits,
            global_limits,
        })
    }

    /// Enqueues a job. With [`OutboundEnqueueMode::ReplacePending`] a still
    /// pending job of the same latest-wins slot is superseded: the new job
    /// inherits its queue position and scheduling age, the old job id is
    /// invalidated, and the superseded id is reported in the outcome. A
    /// weight change on an existing slot is rejected, as is a weight that
    /// never fits an applicable window.
    pub(crate) fn enqueue(
        &mut self,
        meta: OutboundMeta,
        mode: OutboundEnqueueMode,
        not_before: Option<Instant>,
        now: Instant,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        // A job that never fits an applicable window could never be
        // granted; reject it at enqueue time instead of parking it forever.
        let weight = meta.weight.get();
        if let Some(window) = self.global_limits.iter().find(|w| weight > w.capacity) {
            return Err(EnqueueError::WeightExceedsWindow {
                scope: meta.scope,
                weight: meta.weight,
                capacity: window.capacity,
            });
        }
        if let OutboundScope::Chat(_) = meta.scope {
            if let Some(window) = self.chat_limits.iter().find(|w| weight > w.capacity) {
                return Err(EnqueueError::WeightExceedsWindow {
                    scope: meta.scope,
                    weight: meta.weight,
                    capacity: window.capacity,
                });
            }
        }

        let coalesce_key = match mode {
            OutboundEnqueueMode::Fifo => None,
            OutboundEnqueueMode::ReplacePending { user_key } => Some(InternalCoalesceKey {
                scope: meta.scope,
                lane: meta.lane,
                class: meta.class,
                user_key,
            }),
        };

        let mut superseded = None;
        let mut inherited_ready_at = None;
        let mut inherited_lane_order = None;
        let sequence = match coalesce_key {
            Some(key) => match self.coalesce.get(&key).copied() {
                Some(old) if self.jobs.contains_key(&old) && !self.in_flight.contains_key(&old) => {
                    if self.jobs[&old].meta.weight != meta.weight {
                        return Err(EnqueueError::IncompatibleCoalesceMetadata);
                    }
                    let old_job = &self.jobs[&old];
                    let sequence = old_job.sequence;
                    // Age is inherited only for ready -> ready replacements:
                    // a delayed job's age has not started yet, so the
                    // replacement starts aging when it actually becomes
                    // ready.
                    inherited_ready_at = old_job.not_before.is_none().then_some(old_job.ready_at);
                    inherited_lane_order = old_job.lane_order;
                    self.remove_waiting(old);
                    superseded = Some(old);
                    sequence
                }
                _ => self.take_sequence(),
            },
            None => self.take_sequence(),
        };

        let job = JobId(self.next_job_id);
        self.next_job_id += 1;
        // A replacement of a ready job keeps its scheduling age, so a hot
        // latest-wins slot cannot starve by resetting its own aging. A
        // delayed job restarts aging when it becomes ready (at promotion).
        let ready_at = match (not_before, inherited_ready_at) {
            (None, Some(ready_at)) => ready_at,
            _ => now,
        };
        // A replacement of a laned job inherits the lane order, keeping
        // its position in the lane FIFO.
        let lane_order = match (meta.lane, inherited_lane_order) {
            (Some(_), Some(order)) => Some(order),
            _ => None,
        };
        self.jobs.insert(
            job,
            Job {
                meta,
                sequence,
                ready_at,
                not_before,
                coalesce_key,
                lane_order,
                in_candidate_heap: false,
                candidate_effective: 0,
            },
        );
        if let Some(until) = not_before {
            self.delayed.push(Reverse(DelayedJob { not_before: until, sequence, job }));
        }
        match meta.lane {
            Some(lane) => {
                let order = self.insert_lane_pending(lane, job, lane_order);
                self.jobs.get_mut(&job).expect("job exists").lane_order = Some(order);
                // A replacement of the queued head may change its priority:
                // drop the lane's candidate state so that the fresh key
                // reflects the new head (the old heap entry is skipped as
                // stale by its effective mismatch).
                if superseded.is_some_and(|old| {
                    self.lanes
                        .get(&lane)
                        .is_some_and(|l| l.pending.first().is_some_and(|&(_, id)| id == old))
                }) {
                    if let Some(lane_state) = self.lanes.get_mut(&lane) {
                        lane_state.in_candidate_heap = false;
                        lane_state.candidate_effective = 0;
                    }
                }
                self.push_lane_head_candidate(lane, now);
            }
            None if not_before.is_none() => self.push_job_candidate(job, now),
            None => {} // unlaned delayed: only the heap
        }
        if let Some(key) = coalesce_key {
            self.coalesce.insert(key, job);
        }
        self.compact_delayed_if_needed();
        self.compact_candidate_heap_if_needed();
        Ok(EnqueueOutcome { job, superseded })
    }

    /// Issues the next global sequence number. Before the counter wraps,
    /// all live sequences are rebased so that no data structure holds
    /// values from both sides of the wrap.
    fn take_sequence(&mut self) -> u64 {
        if self.next_sequence == u64::MAX {
            self.rebase_sequences();
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        sequence
    }

    fn rebase_sequences(&mut self) {
        let Some(min) = self.jobs.values().map(|job| job.sequence).min() else {
            self.next_sequence = 0; // no live jobs: the counter restarts
            return;
        };
        if min == 0 {
            // A live job holds sequence 0 while the counter is about to
            // wrap: renumber all live sequences densely in their current
            // (FIFO) order, so that newly issued sequences continue after
            // them instead of colliding with sequence 0.
            let mut ordered: Vec<u64> = self.jobs.values().map(|job| job.sequence).collect();
            ordered.sort_unstable();
            let mut renumbered: HashMap<u64, u64> = HashMap::with_capacity(ordered.len());
            for (new, &old) in ordered.iter().enumerate() {
                renumbered.insert(old, new as u64);
            }
            for job in self.jobs.values_mut() {
                job.sequence = renumbered[&job.sequence];
            }
            // Stale heap entries (cancelled jobs) have sequences that are
            // no longer in the map: drop them, re-key the live ones from
            // their current job (lane entries from their current head).
            // Keep only live delayed jobs (a stale entry can share its
            // sequence with a latest-wins replacement, so filtering by
            // sequence alone would keep dead entries and break the stale
            // counter); re-key from the live job's renumbered sequence.
            self.delayed = self
                .delayed
                .drain()
                .filter_map(|Reverse(mut delayed)| {
                    let job = self.jobs.get(&delayed.job)?;
                    delayed.sequence = job.sequence;
                    Some(Reverse(delayed))
                })
                .collect();
            self.stale_delayed = 0; // the rebuild dropped the stale entries
            let mut rebuilt = BinaryHeap::new();
            for mut key in self.candidates.drain() {
                match key.reference {
                    CandidateRef::Job(job_id) => {
                        if let Some(job) = self.jobs.get(&job_id) {
                            key.sequence = job.sequence;
                            rebuilt.push(key);
                        }
                    }
                    CandidateRef::Lane(lane) => {
                        let live_head_sequence = {
                            let Some(lane_state) = self.lanes.get_mut(&lane) else {
                                continue;
                            };
                            // drop stale heads first, exactly like
                            // push_lane_head_candidate does, so that a
                            // cancelled head cannot hide the live one
                            while let Some(&(_, job_id)) = lane_state.pending.first() {
                                if self.jobs.contains_key(&job_id) {
                                    break;
                                }
                                lane_state.pending.pop_first();
                                lane_state.stale -= 1;
                            }
                            lane_state
                                .pending
                                .first()
                                .and_then(|&(_, job_id)| self.jobs.get(&job_id))
                                .map(|head| head.sequence)
                        };
                        match live_head_sequence {
                            Some(sequence) => {
                                key.sequence = sequence;
                                rebuilt.push(key);
                            }
                            None => {
                                // no live head: the lane entry is gone;
                                // release the candidate state so that a
                                // future push can represent the lane again
                                if let Some(lane_state) = self.lanes.get_mut(&lane) {
                                    lane_state.in_candidate_heap = false;
                                    lane_state.candidate_effective = 0;
                                }
                                if self
                                    .lanes
                                    .get(&lane)
                                    .is_some_and(|l| l.pending.is_empty() && l.in_flight.is_none())
                                {
                                    self.lanes.remove(&lane);
                                }
                            }
                        }
                    }
                }
            }
            self.candidates = rebuilt;
            self.next_sequence = ordered.len() as u64;
            return;
        }
        for job in self.jobs.values_mut() {
            job.sequence -= min;
        }
        self.delayed = self
            .delayed
            .drain()
            .map(|Reverse(mut d)| {
                d.sequence -= min;
                Reverse(d)
            })
            .collect();
        self.candidates = self
            .candidates
            .drain()
            .map(|mut key| {
                key.sequence -= min;
                key
            })
            .collect();
        let max = self.jobs.values().map(|job| job.sequence).max().unwrap();
        self.next_sequence = max + 1;
    }

    /// Removes a job that has not been granted yet. All removal is lazy:
    /// the delayed-heap, blocked-heap and candidate-heap entries are
    /// skipped when they surface, the lane-pending entry is dropped when it
    /// reaches the head, and the coalesce entry is removed eagerly because
    /// a stale slot must never be reused.
    fn remove_waiting(&mut self, job: JobId) {
        let Some(job_struct) = self.jobs.get(&job) else { return };
        let coalesce_key = job_struct.coalesce_key;
        let was_delayed = job_struct.not_before.is_some();
        let lane = job_struct.meta.lane;
        if was_delayed {
            self.stale_delayed += 1;
        }
        // Count the entry that stays behind in the candidate heap or lane
        // queue; it is dropped when it surfaces.
        match lane {
            Some(lane) => {
                if let Some(lane_state) = self.lanes.get_mut(&lane) {
                    lane_state.stale += 1;
                }
            }
            None if !was_delayed => self.stale_candidates += 1,
            None => {}
        }
        self.remove_coalesce_entry(job, coalesce_key);
        self.jobs.remove(&job);
    }

    /// Cancels a job that has not been granted yet. Granted jobs are
    /// cancelled through their permit completion instead.
    pub(crate) fn cancel(&mut self, job: JobId) {
        self.remove_waiting(job);
        // A cancel-only churn must not leave a bloated delayed heap behind.
        self.compact_delayed_if_needed();
    }

    fn remove_coalesce_entry(&mut self, job: JobId, key: Option<InternalCoalesceKey>) {
        if let Some(key) = key {
            if self.coalesce.get(&key) == Some(&job) {
                self.coalesce.remove(&key);
            }
        }
    }

    /// Pushes an unlaned job into the candidate heap and schedules its
    /// next aging deadline.
    fn push_job_candidate(&mut self, job_id: JobId, now: Instant) {
        let Some(job) = self.jobs.get(&job_id) else { return };
        let effective = effective_priority(job, &self.aging, now);
        let sequence = job.sequence;
        let base = job.meta.priority.get();
        let ready_at = job.ready_at;
        let job_struct = self.jobs.get_mut(&job_id).expect("job exists");
        job_struct.in_candidate_heap = true;
        job_struct.candidate_effective = effective;
        self.candidates.push(CandidateKey {
            effective,
            sequence,
            reference: CandidateRef::Job(job_id),
        });
        self.schedule_aging(CandidateRef::Job(job_id), effective, base, ready_at);
    }

    /// Schedules the moment the candidate's effective priority rises above
    /// `keyed_effective` (one aging quantum later), unless it is already at
    /// the highest level.
    fn schedule_aging(
        &mut self,
        reference: CandidateRef,
        keyed_effective: u8,
        base: u8,
        ready_at: Instant,
    ) {
        if keyed_effective >= OutboundPriority::HIGHEST.get() {
            return;
        }
        let target = u64::from(keyed_effective) + 1;
        let base = u64::from(base);
        if target <= base {
            return; // defensive: effective can never drop below base
        }
        let quantum = self.aging.quantum.as_nanos().max(1) as u64;
        let at = ready_at + Duration::from_nanos((target - base) * quantum);
        self.aging_events.push(Reverse(AgingEvent { at, reference }));
    }

    /// Re-keys candidates whose aging deadline has passed, so that the
    /// candidate heap stays ordered without recomputing every job on every
    /// tick.
    fn promote_aging(&mut self, now: Instant) {
        while let Some(top) = self.aging_events.peek() {
            if top.0.at > now {
                break;
            }
            let AgingEvent { reference, .. } = self.aging_events.pop().unwrap().0;
            match reference {
                CandidateRef::Job(job_id) => {
                    let Some(job) = self.jobs.get(&job_id) else { continue };
                    if !job.in_candidate_heap {
                        continue; // granted, blocked or parked; its wake-up
                                  // is managed by its current structure
                    }
                    let effective = effective_priority(job, &self.aging, now);
                    if effective <= job.candidate_effective {
                        continue; // the event fired early (timing slop)
                    }
                    let sequence = job.sequence;
                    let base = job.meta.priority.get();
                    let ready_at = job.ready_at;
                    let job_struct = self.jobs.get_mut(&job_id).expect("job exists");
                    job_struct.candidate_effective = effective;
                    self.candidates.push(CandidateKey {
                        effective,
                        sequence,
                        reference: CandidateRef::Job(job_id),
                    });
                    self.schedule_aging(reference, effective, base, ready_at);
                }
                CandidateRef::Lane(lane) => {
                    let Some(lane_state) = self.lanes.get_mut(&lane) else { continue };
                    if !lane_state.in_candidate_heap {
                        continue;
                    }
                    let Some(&(_, job_id)) = lane_state.pending.first() else { continue };
                    let Some(job) = self.jobs.get(&job_id) else { continue };
                    if job.not_before.is_some_and(|not_before| not_before > now) {
                        continue;
                    }
                    let effective = effective_priority(job, &self.aging, now);
                    if effective <= lane_state.candidate_effective {
                        // The head changed or the event fired early: re-arm
                        // the event for the current head's maturity so that
                        // its aging is never lost.
                        self.schedule_aging(
                            reference,
                            effective,
                            job.meta.priority.get(),
                            job.ready_at,
                        );
                        continue;
                    }
                    lane_state.candidate_effective = effective;
                    self.candidates.push(CandidateKey {
                        effective,
                        sequence: job.sequence,
                        reference: CandidateRef::Lane(lane),
                    });
                    self.schedule_aging(
                        reference,
                        effective,
                        job.meta.priority.get(),
                        job.ready_at,
                    );
                }
            }
        }
    }

    /// Pushes the head of a free ordering lane into the candidate heap,
    /// unless it is already represented, delayed, or the lane is in flight.
    /// Stale heads are drained first; a lane left without pending jobs is
    /// removed.
    fn push_lane_head_candidate(&mut self, lane: OutboundLaneKey, now: Instant) {
        let (head_id, delayed, already_queued) = {
            let Some(lane_state) = self.lanes.get_mut(&lane) else { return };
            if lane_state.in_flight.is_some() {
                return;
            }
            while let Some(&(_, job_id)) = lane_state.pending.first() {
                if self.jobs.contains_key(&job_id) {
                    break;
                }
                lane_state.pending.pop_first();
                lane_state.stale -= 1;
            }
            match lane_state.pending.first() {
                Some(&(_, job_id)) => {
                    let delayed = self
                        .jobs
                        .get(&job_id)
                        .is_some_and(|job| job.not_before.is_some_and(|nb| nb > now));
                    (Some(job_id), delayed, lane_state.in_candidate_heap)
                }
                None => (None, false, lane_state.in_candidate_heap),
            }
        };
        let Some(job_id) = head_id else {
            self.lanes.remove(&lane);
            return;
        };
        if delayed || already_queued {
            return; // delayed head (promotion will push) or already queued
        }
        let Some(job) = self.jobs.get(&job_id) else {
            self.lanes.remove(&lane);
            return;
        };
        let effective = effective_priority(job, &self.aging, now);
        let sequence = job.sequence;
        let base = job.meta.priority.get();
        let ready_at = job.ready_at;
        let lane_state = self.lanes.get_mut(&lane).expect("lane exists");
        lane_state.in_candidate_heap = true;
        lane_state.candidate_effective = effective;
        self.candidates.push(CandidateKey {
            effective,
            sequence,
            reference: CandidateRef::Lane(lane),
        });
        self.schedule_aging(CandidateRef::Lane(lane), effective, base, ready_at);
    }

    fn insert_lane_pending(
        &mut self,
        lane: OutboundLaneKey,
        job: JobId,
        inherited_order: Option<u64>,
    ) -> u64 {
        let lane_state = self.lanes.entry(lane).or_insert_with(|| LaneState {
            pending: BTreeSet::new(),
            in_flight: None,
            next_order: 0,
            stale: 0,
            in_candidate_heap: false,
            candidate_effective: 0,
        });
        let order = match inherited_order {
            Some(order) => order,
            None => {
                // The per-lane counter wraps after 2^64 enqueues; rebase
                // the pending window densely to 0..len so that numeric key
                // order stays equal to FIFO order across the wrap. The
                // dense form cannot overflow (unlike a window shift by the
                // head order, which wraps when the head order is 0).
                if lane_state.next_order == 0 && !lane_state.pending.is_empty() {
                    let mut rebuilt = BTreeSet::new();
                    for (new_order, &(_, job_id)) in lane_state.pending.iter().enumerate() {
                        let new_order = new_order as u64;
                        rebuilt.insert((new_order, job_id));
                        if let Some(job_struct) = self.jobs.get_mut(&job_id) {
                            job_struct.lane_order = Some(new_order);
                        }
                    }
                    lane_state.pending = rebuilt;
                    lane_state.next_order = lane_state.pending.len() as u64;
                }
                let order = lane_state.next_order;
                lane_state.next_order = lane_state.next_order.wrapping_add(1);
                order
            }
        };
        lane_state.pending.insert((order, job));
        order
    }

    fn promote_delayed(&mut self, now: Instant) {
        while let Some(top) = self.delayed.peek() {
            if top.0.not_before > now {
                break;
            }
            let DelayedJob { job, .. } = self.delayed.pop().unwrap().0;
            let Some(meta) = self.jobs.get(&job).map(|job| job.meta) else {
                self.stale_delayed -= 1; // cancelled or superseded while delayed
                continue;
            };
            let job_struct = self.jobs.get_mut(&job).expect("job exists");
            job_struct.ready_at = now;
            job_struct.not_before = None;
            match meta.lane {
                Some(lane) => self.push_lane_head_candidate(lane, now),
                None => self.push_job_candidate(job, now),
            }
        }
    }

    /// Wakes candidates whose blocked deadline has passed and re-inserts
    /// them (re-validating the reference: a lane entry wakes its current
    /// head, a dead job is skipped).
    fn promote_blocked(&mut self, now: Instant) {
        while let Some(top) = self.blocked.peek() {
            if top.0.until > now {
                break;
            }
            let BlockedJob { reference, .. } = self.blocked.pop().unwrap().0;
            match reference {
                CandidateRef::Job(job_id) => {
                    let Some(meta) = self.jobs.get(&job_id).map(|job| job.meta) else {
                        self.stale_blocked += 1;
                        continue;
                    };
                    let Some(job) = self.jobs.get(&job_id) else { continue };
                    if job.not_before.is_some() {
                        continue; // re-delayed somehow; the heap will wake it
                    }
                    self.push_job_candidate(job_id, now);
                    let _ = meta;
                }
                CandidateRef::Lane(lane) => self.push_lane_head_candidate(lane, now),
            }
        }
    }

    /// Releases expired window reservations: exactly the head of each
    /// parked queue is re-inserted per tick (the hold is re-armed when it
    /// is granted or blocked again), so a rate-limited drain stays linear
    /// instead of re-pushing the whole queue on every tick.
    fn promote_reservations(&mut self, now: Instant) {
        let expired: Vec<WindowRef> = self
            .reservations
            .iter()
            .filter(|(_, reservation)| reservation.until <= now)
            .map(|(window, _)| *window)
            .collect();
        for window in expired {
            let mut remove = false;
            let head = {
                let Some(reservation) = self.reservations.get_mut(&window) else { continue };
                // drop parked references that no longer point at a live
                // job; a lane reference stays valid as long as the lane
                // has a live head (the wake-up below uses the current one)
                while let Some(&reference) = reservation.queue.front() {
                    if reference_alive(reference, &self.jobs, &self.lanes) {
                        break;
                    }
                    reservation.queue.pop_front();
                }
                match reservation.queue.pop_front() {
                    Some(reference) => Some(reference),
                    None => {
                        remove = true;
                        None
                    }
                }
            };
            if remove {
                self.reservations.remove(&window);
            } else if let Some(reference) = head {
                self.push_reference(reference, now);
            }
        }
    }

    /// Re-inserts a reference into the candidate heap: a lane reference
    /// wakes its current head (a cancelled head falls through to the next
    /// pending job of the lane).
    fn push_reference(&mut self, reference: CandidateRef, now: Instant) {
        match reference {
            CandidateRef::Job(job_id) => self.push_job_candidate(job_id, now),
            CandidateRef::Lane(lane) => self.push_lane_head_candidate(lane, now),
        }
    }

    /// After a grant consumed a window, extends the window's hold (if any)
    /// to the moment the next parked candidate fits, so the parked queue
    /// paces itself through the window instead of being re-pushed.
    fn rearm_reservations(&mut self, candidate: Candidate, now: Instant) {
        let windows = match candidate.scope {
            OutboundScope::Global => vec![WindowRef::Global],
            OutboundScope::Chat(chat) => vec![WindowRef::Global, WindowRef::Chat(chat)],
        };
        for window in windows {
            let until = {
                let Some(reservation) = self.reservations.get(&window) else { continue };
                let Some(&head_ref) = reservation.queue.front() else { continue };
                let Some(weight) = reference_weight(head_ref, &self.jobs, &self.lanes) else {
                    continue; // dead parked candidate; dropped on promotion
                };
                match window {
                    WindowRef::Global => self.global_windows.earliest_for(now, weight),
                    WindowRef::Chat(chat) => self
                        .chat_window_sets
                        .get(&chat)
                        .and_then(|set| set.earliest_for(now, weight)),
                }
            };
            let Some(until) = until else { continue };
            if let Some(reservation) = self.reservations.get_mut(&window) {
                reservation.until = until.max(now);
            }
        }
    }

    /// Rebuilds the delayed heap when stale entries dominate, so that a hot
    /// latest-wins key cannot grow the heap without bound.
    fn compact_delayed_if_needed(&mut self) {
        if self.stale_delayed == 0 || self.stale_delayed * 2 <= self.delayed.len() {
            return;
        }
        self.delayed =
            self.delayed.drain().filter(|node| self.jobs.contains_key(&node.0.job)).collect();
        self.stale_delayed = 0;
    }

    /// Rebuilds the candidate heap when stale entries dominate.
    fn compact_candidate_heap_if_needed(&mut self) {
        if self.stale_candidates == 0 || self.stale_candidates * 2 <= self.candidates.len() {
            return;
        }
        let mut live = BinaryHeap::new();
        for key in self.candidates.drain() {
            let alive = match key.reference {
                CandidateRef::Job(job_id) => self.jobs.contains_key(&job_id),
                CandidateRef::Lane(lane) => self.lanes.get(&lane).is_some_and(|state| {
                    state.pending.iter().any(|&(_, id)| self.jobs.contains_key(&id))
                }),
            };
            if alive {
                live.push(key);
            }
        }
        self.candidates = live;
        self.stale_candidates = 0;
    }

    /// Rebuilds the lane pending sets when stale entries dominate, so that
    /// churn behind a live lane head cannot grow them without bound.
    fn compact_lanes_if_needed(&mut self) {
        for lane_state in self.lanes.values_mut() {
            if lane_state.stale * 2 > lane_state.pending.len() {
                lane_state.pending.retain(|&(_, id)| self.jobs.contains_key(&id));
                lane_state.stale = 0;
            }
        }
    }

    /// Grants every job that can be admitted at `now`.
    ///
    /// Arbitration is event-driven: candidates are popped from the
    /// persistent heap one at a time and either granted, parked in the
    /// blocked deadline heap (failed admission), or parked in a reserved
    /// window's queue (the window is held back for an older candidate).
    /// No full collect+sort pass happens per tick, so a rate-limited
    /// gradual drain stays near O(N log N) overall.
    pub(crate) fn grant_ready(&mut self, now: Instant) -> Vec<Grant> {
        self.promote_aging(now);
        self.promote_delayed(now);
        self.promote_blocked(now);
        self.promote_reservations(now);
        self.compact_lanes_if_needed();

        let mut grants = Vec::new();
        loop {
            let Some(candidate) = self.pop_candidate(now) else { break };
            match self.admission(candidate, now) {
                Admission::Pass => {
                    self.grant(candidate, now);
                    grants.push(Grant { job: candidate.job });
                    self.rearm_reservations(candidate, now);
                }
                Admission::Blocked { until, reserve } => {
                    if let Some(window) = reserve {
                        let reservation = self
                            .reservations
                            .entry(window)
                            .or_insert_with(|| Reservation { until, queue: VecDeque::new() });
                        if until > reservation.until {
                            reservation.until = until;
                        }
                    }
                    self.blocked.push(Reverse(BlockedJob {
                        until,
                        reference: self.reference_of(candidate),
                    }));
                }
                Admission::Reserved => {
                    let window = self.reservation_window(candidate, now);
                    let reference = self.reference_of(candidate);
                    if let Some(reservation) = self.reservations.get_mut(&window) {
                        reservation.queue.push_back(reference);
                    } else {
                        // Defensive: no reservation found, keep the
                        // candidate alive by re-inserting it.
                        self.push_candidate(candidate, now);
                    }
                }
            }
        }
        self.prune_expired_penalties(now);
        self.prune_idle_chat_windows(now);
        grants
    }

    /// Pops the best live candidate from the persistent heap, re-keying
    /// entries whose effective priority grew or whose lane head changed.
    fn pop_candidate(&mut self, now: Instant) -> Option<Candidate> {
        loop {
            let key = self.candidates.pop()?;
            match key.reference {
                CandidateRef::Job(job_id) => {
                    let (candidate_effective, sequence, weight, scope) = {
                        let Some(job) = self.jobs.get(&job_id) else {
                            self.stale_candidates += 1;
                            continue;
                        };
                        if key.effective < job.candidate_effective {
                            // a fresher entry (re-keyed by an aging event)
                            // exists; this one is stale
                            self.stale_candidates += 1;
                            continue;
                        }
                        (job.candidate_effective, job.sequence, job.meta.weight, job.meta.scope)
                    };
                    self.jobs.get_mut(&job_id).expect("job exists").in_candidate_heap = false;
                    return Some(Candidate {
                        job: job_id,
                        sequence,
                        effective: candidate_effective,
                        weight,
                        scope,
                    });
                }
                CandidateRef::Lane(lane) => {
                    let outcome = {
                        let Some(lane_state) = self.lanes.get_mut(&lane) else {
                            self.stale_candidates += 1;
                            continue;
                        };
                        if key.effective < lane_state.candidate_effective {
                            // a fresher lane entry exists
                            self.stale_candidates += 1;
                            continue;
                        }
                        lane_state.in_candidate_heap = false;
                        if lane_state.in_flight.is_some() {
                            continue;
                        }
                        while let Some(&(_, job_id)) = lane_state.pending.first() {
                            if self.jobs.contains_key(&job_id) {
                                break;
                            }
                            lane_state.pending.pop_first();
                            lane_state.stale -= 1;
                        }
                        match lane_state.pending.first() {
                            Some(&(_, job_id)) => match self.jobs.get(&job_id) {
                                Some(job) if job.not_before.is_none_or(|nb| nb <= now) => {
                                    let effective = effective_priority(job, &self.aging, now);
                                    // Re-key when the key does not match the
                                    // current head: the head may have been
                                    // replaced with a different priority
                                    // (effective) or a different sequence.
                                    let stale_key =
                                        effective != key.effective || job.sequence != key.sequence;
                                    (
                                        Some((
                                            job_id,
                                            job.sequence,
                                            effective,
                                            job.meta.weight,
                                            job.meta.scope,
                                        )),
                                        stale_key,
                                    )
                                }
                                _ => (None, false), // dead or delayed head
                            },
                            None => (None, false), // empty lane: removed below
                        }
                    };
                    let Some((job_id, sequence, effective, weight, scope)) = outcome.0 else {
                        // no grantable head: drop the lane if it is empty
                        // and free
                        if self
                            .lanes
                            .get(&lane)
                            .is_some_and(|l| l.pending.is_empty() && l.in_flight.is_none())
                        {
                            self.lanes.remove(&lane);
                        }
                        continue;
                    };
                    if outcome.1 {
                        // the head changed (replacement or cancellation):
                        // re-key with the fresh effective and sequence
                        self.push_lane_head_candidate(lane, now);
                        continue;
                    }
                    return Some(Candidate { job: job_id, sequence, effective, weight, scope });
                }
            }
        }
    }

    /// Re-inserts a candidate into the persistent heap (used by the
    /// defensive reservation path).
    fn push_candidate(&mut self, candidate: Candidate, now: Instant) {
        let Some(job) = self.jobs.get(&candidate.job) else { return };
        match job.meta.lane {
            Some(lane) => self.push_lane_head_candidate(lane, now),
            None => self.push_job_candidate(candidate.job, now),
        }
    }

    /// The heap reference that represents this candidate.
    fn reference_of(&self, candidate: Candidate) -> CandidateRef {
        self.jobs
            .get(&candidate.job)
            .and_then(|job| job.meta.lane)
            .map(CandidateRef::Lane)
            .unwrap_or(CandidateRef::Job(candidate.job))
    }

    /// The window whose *active* reservation parks this candidate. An
    /// expired-but-retained reservation (its queue is being drained) must
    /// not capture candidates: the `Reserved` verdict was produced by an
    /// active hold, and the candidate joins that hold.
    fn reservation_window(&self, candidate: Candidate, now: Instant) -> WindowRef {
        if self.reservation_active(WindowRef::Global, now) {
            return WindowRef::Global;
        }
        match candidate.scope {
            OutboundScope::Chat(chat) => WindowRef::Chat(chat),
            OutboundScope::Global => WindowRef::Global, // unreachable: see admission
        }
    }

    /// Admission check: penalties, window capacity and reservations. A
    /// candidate that does not fit a window reserves that window so that
    /// lighter traffic cannot starve it; candidates of a reserved window
    /// are parked.
    fn reservation_active(&self, window: WindowRef, now: Instant) -> bool {
        self.reservations.get(&window).is_some_and(|reservation| reservation.until > now)
    }

    fn admission(&mut self, candidate: Candidate, now: Instant) -> Admission {
        let weight = candidate.weight.get();
        if self.reservation_active(WindowRef::Global, now) {
            return Admission::Reserved;
        }
        let mut until = now;
        let mut reserve = None;

        if self.penalty_active(PenaltyKey::Global, now) {
            until = until.max(self.penalties[&PenaltyKey::Global]);
        }
        if !self.global_windows.can_consume(now, weight) {
            until = until.max(self.global_windows.earliest_for(now, weight).unwrap_or(now));
            reserve = Some(WindowRef::Global);
        }
        if let OutboundScope::Chat(chat) = candidate.scope {
            if self.reservation_active(WindowRef::Chat(chat), now) {
                return Admission::Reserved;
            }
            if self.penalty_active(PenaltyKey::Chat(chat), now) {
                until = until.max(self.penalties[&PenaltyKey::Chat(chat)]);
            }
            let windows = self
                .chat_window_sets
                .entry(chat)
                .or_insert_with(|| WindowSet::new(&self.chat_limits));
            if !windows.can_consume(now, weight) {
                until = until.max(windows.earliest_for(now, weight).unwrap_or(now));
                if reserve.is_none() {
                    reserve = Some(WindowRef::Chat(chat));
                }
            }
        }
        if until > now {
            Admission::Blocked { until, reserve }
        } else {
            Admission::Pass
        }
    }

    fn penalty_active(&self, key: PenaltyKey, now: Instant) -> bool {
        self.penalties.get(&key).is_some_and(|&until| now < until)
    }

    fn grant(&mut self, candidate: Candidate, now: Instant) {
        let job = self.jobs.remove(&candidate.job).expect("candidate job exists");
        self.remove_coalesce_entry(candidate.job, job.coalesce_key);
        if let Some(lane) = job.meta.lane {
            let lane_state = self.lanes.get_mut(&lane).expect("lane exists");
            let head =
                lane_state.pending.pop_first().expect("the lane head is queued when granted");
            debug_assert_eq!(head.1, candidate.job, "only the lane head is granted");
            debug_assert!(lane_state.in_flight.is_none(), "a lane is admitted only when free");
            lane_state.in_flight = Some(candidate.job);
        }
        self.global_windows.consume(now, job.meta.weight.get());
        if let OutboundScope::Chat(chat) = job.meta.scope {
            self.chat_window_sets
                .entry(chat)
                .or_insert_with(|| WindowSet::new(&self.chat_limits))
                .consume(now, job.meta.weight.get());
        }
        self.in_flight.insert(candidate.job, InFlight { lane: job.meta.lane });
    }

    /// Finishes a granted job and releases its lane exactly once (a repeated
    /// completion is a no-op). A `RetryAfter` completion penalizes the
    /// reported scope until the reported instant.
    pub(crate) fn complete(&mut self, job: JobId, completion: OutboundCompletion, now: Instant) {
        let Some(in_flight) = self.in_flight.remove(&job) else { return };
        if let Some(lane) = in_flight.lane {
            let has_pending = match self.lanes.get_mut(&lane) {
                Some(state) => {
                    debug_assert_eq!(state.in_flight, Some(job), "the lane belongs to the job");
                    state.in_flight = None;
                    !state.pending.is_empty()
                }
                None => false,
            };
            if has_pending {
                self.push_lane_head_candidate(lane, now);
            } else {
                self.lanes.remove(&lane);
            }
        }
        if let OutboundCompletion::RetryAfter { scope, until } = completion {
            if until > now {
                self.penalize(scope, until);
            }
        }
    }

    /// Penalizes a scope until `until`, extending an existing penalty only
    /// when the new deadline is later (max(old, new)).
    pub(crate) fn penalize(&mut self, scope: OutboundScope, until: Instant) {
        let key = match scope {
            OutboundScope::Global => PenaltyKey::Global,
            OutboundScope::Chat(chat) => PenaltyKey::Chat(chat),
        };
        let entry = self.penalties.entry(key).or_insert(until);
        if until > *entry {
            *entry = until;
        }
    }

    fn prune_expired_penalties(&mut self, now: Instant) {
        self.penalties.retain(|_, until| *until > now);
    }

    fn prune_idle_chat_windows(&mut self, now: Instant) {
        self.chat_window_sets
            .retain(|_, set| set.windows.iter_mut().any(|window| !window.is_idle(now)));
    }

    /// The earliest moment at which something may become grantable: the
    /// next delayed promotion, the next blocked candidate wake-up, or the
    /// next reservation release. Stale heap heads are dropped before the
    /// deadlines are read.
    pub(crate) fn next_deadline(&mut self, now: Instant) -> SchedulerWakeup {
        while let Some(top) = self.delayed.peek() {
            if self.jobs.contains_key(&top.0.job) {
                break;
            }
            self.delayed.pop();
            self.stale_delayed -= 1;
        }
        while let Some(top) = self.blocked.peek() {
            if reference_alive(top.0.reference, &self.jobs, &self.lanes) {
                break;
            }
            self.blocked.pop();
            self.stale_blocked -= 1;
        }
        // drop aging events whose reference is no longer a candidate (it
        // was granted, blocked or parked; its wake-up is managed elsewhere)
        while let Some(top) = self.aging_events.peek() {
            let active = match top.0.reference {
                CandidateRef::Job(job_id) => {
                    self.jobs.get(&job_id).is_some_and(|job| job.in_candidate_heap)
                }
                CandidateRef::Lane(lane) => {
                    self.lanes.get(&lane).is_some_and(|lane| lane.in_candidate_heap)
                }
            };
            if active {
                break;
            }
            self.aging_events.pop();
        }

        let mut earliest: Option<Instant> = None;
        let mut consider = |candidate: Instant| {
            earliest = Some(match earliest {
                Some(current) => current.min(candidate),
                None => candidate,
            });
        };
        if let Some(top) = self.delayed.peek() {
            consider(top.0.not_before);
        }
        if let Some(top) = self.blocked.peek() {
            consider(top.0.until);
        }
        if let Some(top) = self.aging_events.peek() {
            consider(top.0.at);
        }
        for reservation in self.reservations.values() {
            if reservation.until > now {
                consider(reservation.until);
            }
        }
        match earliest {
            Some(at) if at <= now => SchedulerWakeup::Immediate,
            Some(at) => SchedulerWakeup::At(at),
            None => SchedulerWakeup::ExternalEvent,
        }
    }
}

fn reference_alive(
    reference: CandidateRef,
    jobs: &HashMap<JobId, Job>,
    lanes: &HashMap<OutboundLaneKey, LaneState>,
) -> bool {
    match reference {
        CandidateRef::Job(job_id) => jobs.contains_key(&job_id),
        CandidateRef::Lane(lane) => lanes
            .get(&lane)
            .is_some_and(|state| state.pending.iter().any(|&(_, id)| jobs.contains_key(&id))),
    }
}

/// The accounting weight of the job a reference points at: for a lane, the
/// current head.
fn reference_weight(
    reference: CandidateRef,
    jobs: &HashMap<JobId, Job>,
    lanes: &HashMap<OutboundLaneKey, LaneState>,
) -> Option<u32> {
    match reference {
        CandidateRef::Job(job_id) => jobs.get(&job_id).map(|job| job.meta.weight.get()),
        CandidateRef::Lane(lane) => lanes
            .get(&lane)
            .and_then(|state| state.pending.first())
            .and_then(|&(_, job_id)| jobs.get(&job_id))
            .map(|job| job.meta.weight.get()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use super::super::types::OutboundClass;

    fn base() -> Instant {
        Instant::now()
    }

    fn meta(
        scope: OutboundScope,
        lane: Option<OutboundLaneKey>,
        priority: OutboundPriority,
    ) -> OutboundMeta {
        OutboundMeta {
            scope,
            lane,
            class: OutboundClass(0),
            priority,
            weight: NonZeroU32::new(1).unwrap(),
        }
    }

    fn meta_with_class(
        scope: OutboundScope,
        lane: Option<OutboundLaneKey>,
        priority: OutboundPriority,
        class: u64,
    ) -> OutboundMeta {
        OutboundMeta {
            scope,
            lane,
            class: OutboundClass(class),
            priority,
            weight: NonZeroU32::new(1).unwrap(),
        }
    }

    fn global(priority: OutboundPriority) -> OutboundMeta {
        meta(OutboundScope::Global, None, priority)
    }

    fn limits(capacity: u32) -> OutboundLimits {
        OutboundLimits {
            global: vec![WindowLimit { capacity, window: Duration::from_secs(1) }],
            chat: vec![WindowLimit { capacity, window: Duration::from_secs(1) }],
        }
    }

    fn aging() -> AgingPolicy {
        AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX }
    }

    fn scheduler(limits: OutboundLimits, aging: AgingPolicy) -> SchedulerState {
        SchedulerState::new(limits, aging).unwrap()
    }

    fn jobs(grants: &[Grant]) -> Vec<JobId> {
        grants.iter().map(|grant| grant.job).collect()
    }

    fn fifo(s: &mut SchedulerState, meta: OutboundMeta, now: Instant) -> JobId {
        s.enqueue(meta, OutboundEnqueueMode::Fifo, None, now).unwrap().job
    }

    fn replace(
        s: &mut SchedulerState,
        meta: OutboundMeta,
        user_key: u64,
        now: Instant,
    ) -> EnqueueOutcome {
        s.enqueue(meta, OutboundEnqueueMode::ReplacePending { user_key }, None, now).unwrap()
    }

    #[test]
    fn fifo_within_priority_and_lane() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        let c = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a, b, c]);
    }

    #[test]
    fn fifo_survives_sequence_wraparound() {
        let mut s = scheduler(limits(100), aging());
        s.next_sequence = u64::MAX - 1;
        let t0 = base();
        let a = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        let c = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a, b, c]);
    }

    #[test]
    fn different_chats_do_not_block_each_other() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = fifo(
            &mut s,
            meta(
                OutboundScope::Chat(OutboundChatKey(1)),
                Some(OutboundLaneKey(1)),
                OutboundPriority::NORMAL,
            ),
            t0,
        );
        let b = fifo(
            &mut s,
            meta(
                OutboundScope::Chat(OutboundChatKey(2)),
                Some(OutboundLaneKey(2)),
                OutboundPriority::NORMAL,
            ),
            t0,
        );
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a, b]);
    }

    #[test]
    fn one_lane_gets_no_second_permit_until_completion() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        let a = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        assert!(s.grant_ready(t0).is_empty());
        s.complete(a, OutboundCompletion::Success, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]);
    }

    #[test]
    fn global_and_chat_windows_apply_together() {
        let mut s = scheduler(
            OutboundLimits {
                global: vec![WindowLimit { capacity: 1, window: Duration::from_millis(500) }],
                chat: vec![WindowLimit { capacity: 1, window: Duration::from_secs(10) }],
            },
            aging(),
        );
        let t0 = base();
        let a = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);

        // chat 2's window is free, but the global window is exhausted
        let b = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(2)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert!(s.grant_ready(t0).is_empty());

        // the global window refilled, but chat 1's window is still exhausted
        let t1 = t0 + Duration::from_millis(501);
        let c = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t1,
        );
        assert_eq!(jobs(&s.grant_ready(t1)), vec![b]);
        assert!(s.grant_ready(t1).is_empty());

        // chat 1's window refills as well
        let t2 = t0 + Duration::from_secs(10) + Duration::from_millis(1);
        assert_eq!(jobs(&s.grant_ready(t2)), vec![c]);
    }

    #[test]
    fn delayed_job_becomes_ready_exactly_at_its_deadline() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let not_before = t0 + Duration::from_secs(5);
        let a = s
            .enqueue(
                global(OutboundPriority::NORMAL),
                OutboundEnqueueMode::Fifo,
                Some(not_before),
                t0,
            )
            .unwrap()
            .job;
        assert!(s.grant_ready(t0).is_empty());
        assert_eq!(s.next_deadline(t0), SchedulerWakeup::At(not_before));
        assert!(s.grant_ready(t0 + Duration::from_millis(4999)).is_empty());
        assert_eq!(jobs(&s.grant_ready(not_before)), vec![a]);
    }

    #[test]
    fn cancelling_the_head_job_unblocks_the_next_one() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        let a = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        s.cancel(a);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]);
    }

    #[test]
    fn cancelling_a_waiting_job_does_not_consume_budget() {
        let mut s = scheduler(limits(2), aging());
        let t0 = base();
        let a = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        s.cancel(b);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        // the cancelled job never consumed window budget
        let c = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![c]);
    }

    #[test]
    fn critical_bypasses_background() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let bg = fifo(&mut s, global(OutboundPriority::BACKGROUND), t0);
        let critical = fifo(&mut s, global(OutboundPriority::CRITICAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![critical, bg]);
    }

    #[test]
    fn priorities_are_strictly_ordered() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let background = fifo(&mut s, global(OutboundPriority::BACKGROUND), t0);
        let normal = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        let interactive = fifo(&mut s, global(OutboundPriority::INTERACTIVE), t0);
        let critical = fifo(&mut s, global(OutboundPriority::CRITICAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![critical, interactive, normal, background]);
    }

    #[test]
    fn aging_prevents_background_starvation() {
        let mut s = scheduler(
            limits(100),
            AgingPolicy { quantum: Duration::from_millis(1), max_boost: u8::MAX },
        );
        let t0 = base();
        let bg = fifo(&mut s, global(OutboundPriority::LOWEST), t0);
        let mut criticals = Vec::new();
        for i in 1..=4u64 {
            criticals.push(fifo(
                &mut s,
                global(OutboundPriority::HIGHEST),
                t0 + Duration::from_micros(i * 100),
            ));
        }
        // after max_boost quanta the background job ties at the highest
        // level and outranks everything that arrived later (FIFO by age)
        let t1 = t0 + Duration::from_millis(255) + Duration::from_micros(1);
        let mut granted = jobs(&s.grant_ready(t1));
        assert_eq!(granted.first(), Some(&bg));
        granted.remove(0);
        assert_eq!(granted, criticals);
    }

    #[test]
    fn penalty_extension_keeps_the_later_deadline() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let scope = OutboundScope::Chat(OutboundChatKey(1));
        s.penalize(scope, t0 + Duration::from_secs(5));
        s.penalize(scope, t0 + Duration::from_secs(3)); // shorter: ignored
        let a = fifo(&mut s, meta(scope, None, OutboundPriority::CRITICAL), t0);
        assert!(s.grant_ready(t0 + Duration::from_secs(4)).is_empty());
        s.penalize(scope, t0 + Duration::from_secs(7)); // longer: replaces
        assert!(s.grant_ready(t0 + Duration::from_secs(6)).is_empty());
        assert_eq!(jobs(&s.grant_ready(t0 + Duration::from_secs(7))), vec![a]);
    }

    #[test]
    fn same_priority_scoped_penalty_does_not_block_other_scopes() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let _a = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t0,
        );
        let b = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(2)), None, OutboundPriority::NORMAL),
            t0,
        );
        s.penalize(OutboundScope::Chat(OutboundChatKey(1)), t0 + Duration::from_secs(60));
        // the penalized chat 1 job must not block the same-priority chat 2 job
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]);
    }

    #[test]
    fn same_priority_chat_window_block_does_not_block_other_scopes() {
        let mut s = scheduler(
            OutboundLimits {
                global: vec![WindowLimit { capacity: 100, window: Duration::from_secs(1) }],
                chat: vec![WindowLimit { capacity: 1, window: Duration::from_secs(60) }],
            },
            aging(),
        );
        let t0 = base();
        let a = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]); // chat 1 window exhausted
        let b = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(2)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]); // chat 2 unaffected
    }

    #[test]
    fn retry_after_blocks_only_the_scoped_chat() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let _a = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::CRITICAL),
            t0,
        );
        let b = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(2)), None, OutboundPriority::BACKGROUND),
            t0,
        );
        s.penalize(OutboundScope::Chat(OutboundChatKey(1)), t0 + Duration::from_secs(60));
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]);
        assert!(s.grant_ready(t0).is_empty());
    }

    #[test]
    fn lane_order_is_strict_across_priorities() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        let a = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, meta(chat, lane, OutboundPriority::CRITICAL), t0);
        // the lane is strictly FIFO: the critical job cannot overtake
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        s.complete(a, OutboundCompletion::Success, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]);
    }

    #[test]
    fn delayed_lane_head_blocks_later_lane_jobs() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        let not_before = t0 + Duration::from_secs(5);
        let a = s
            .enqueue(
                meta(chat, lane, OutboundPriority::NORMAL),
                OutboundEnqueueMode::Fifo,
                Some(not_before),
                t0,
            )
            .unwrap()
            .job;
        let b = fifo(&mut s, meta(chat, lane, OutboundPriority::CRITICAL), t0);
        // the delayed head blocks the lane regardless of the later priority
        assert!(s.grant_ready(t0).is_empty());
        assert_eq!(jobs(&s.grant_ready(not_before)), vec![a]);
        s.complete(a, OutboundCompletion::Success, not_before);
        assert_eq!(jobs(&s.grant_ready(not_before)), vec![b]);
    }

    #[test]
    fn completion_releases_the_lane_exactly_once() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        let a = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        s.complete(a, OutboundCompletion::Success, t0);
        s.complete(a, OutboundCompletion::Success, t0); // no-op
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]);
    }

    #[test]
    fn failed_completion_does_not_refund_the_window_budget() {
        let mut s = scheduler(limits(1), aging());
        let t0 = base();
        let a = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        s.complete(a, OutboundCompletion::Failed, t0);
        let _b = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert!(s.grant_ready(t0).is_empty());
    }

    #[test]
    fn cancelled_after_grant_releases_the_lane_without_refunding() {
        let mut s = scheduler(limits(2), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let scope = OutboundScope::Chat(OutboundChatKey(1));
        let a = fifo(&mut s, meta(scope, lane, OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);

        // the permit was dropped without an explicit completion
        s.complete(a, OutboundCompletion::CancelledAfterGrant, t0);

        // the lane is released, but the window budget is not refunded
        let b = fifo(&mut s, meta(scope, lane, OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]);
        let _c = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert!(s.grant_ready(t0).is_empty());
    }

    #[test]
    fn retry_after_completion_penalizes_the_reported_scope() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let chat = OutboundChatKey(1);
        let a = fifo(&mut s, meta(OutboundScope::Chat(chat), None, OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        s.complete(
            a,
            OutboundCompletion::RetryAfter {
                scope: OutboundScope::Chat(chat),
                until: t0 + Duration::from_secs(5),
            },
            t0,
        );
        let b = fifo(&mut s, meta(OutboundScope::Chat(chat), None, OutboundPriority::CRITICAL), t0);
        assert!(s.grant_ready(t0 + Duration::from_secs(4)).is_empty());
        assert_eq!(jobs(&s.grant_ready(t0 + Duration::from_secs(5))), vec![b]);
    }

    #[test]
    fn global_retry_after_from_a_chat_request() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        // a chat request can report a *global* flood penalty
        s.complete(
            a,
            OutboundCompletion::RetryAfter {
                scope: OutboundScope::Global,
                until: t0 + Duration::from_secs(5),
            },
            t0,
        );
        // the global penalty blocks every scope
        let b = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(2)), None, OutboundPriority::CRITICAL),
            t0,
        );
        assert!(s.grant_ready(t0 + Duration::from_secs(4)).is_empty());
        assert_eq!(jobs(&s.grant_ready(t0 + Duration::from_secs(5))), vec![b]);
    }

    #[test]
    fn pending_job_is_replaced_and_the_old_one_is_superseded() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        let outcome = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        assert_eq!(outcome.superseded, Some(a.job));
        assert_eq!(jobs(&s.grant_ready(t0)), vec![outcome.job]);
    }

    #[test]
    fn replacement_inherits_the_queue_position() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        let b = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        let c = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        // the replacement keeps b's position instead of joining the tail
        let outcome = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        assert_eq!(outcome.superseded, Some(b.job));
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a, outcome.job, c]);
    }

    #[test]
    fn cancelling_the_superseded_job_id_does_not_remove_the_replacement() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        let outcome = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        s.cancel(a.job); // the old id is invalidated: must not remove the replacement
        assert_eq!(jobs(&s.grant_ready(t0)), vec![outcome.job]);
    }

    #[test]
    fn in_flight_jobs_are_not_replaced() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let scope = OutboundScope::Chat(OutboundChatKey(1));
        let old = replace(&mut s, meta(scope, lane, OutboundPriority::NORMAL), 1, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![old.job]);

        // the in-flight job is never replaced: the new job becomes pending
        let fresh = replace(&mut s, meta(scope, lane, OutboundPriority::NORMAL), 1, t0);
        assert_eq!(fresh.superseded, None);
        assert!(s.grant_ready(t0).is_empty()); // lane is locked

        // after the old completes, the latest pending runs
        s.complete(old.job, OutboundCompletion::Success, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![fresh.job]);
    }

    #[test]
    fn exactly_one_latest_pending_remains_behind_in_flight() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let scope = OutboundScope::Chat(OutboundChatKey(1));
        let old = replace(&mut s, meta(scope, lane, OutboundPriority::NORMAL), 1, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![old.job]);

        let a = replace(&mut s, meta(scope, lane, OutboundPriority::NORMAL), 1, t0);
        let b = replace(&mut s, meta(scope, lane, OutboundPriority::NORMAL), 1, t0);
        let c = replace(&mut s, meta(scope, lane, OutboundPriority::NORMAL), 1, t0);
        assert_eq!(a.superseded, None);
        assert_eq!(b.superseded, Some(a.job));
        assert_eq!(c.superseded, Some(b.job));

        s.complete(old.job, OutboundCompletion::Success, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![c.job]);
    }

    #[test]
    fn replacement_does_not_debit_rate_budget() {
        let mut s = scheduler(limits(1), aging());
        let t0 = base();
        let a = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        let outcome = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        assert_eq!(outcome.superseded, Some(a.job));
        // the superseded job was never granted, so the window holds one unit
        assert_eq!(jobs(&s.grant_ready(t0)), vec![outcome.job]);
        let _x = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert!(s.grant_ready(t0).is_empty());
    }

    #[test]
    fn replacement_keeps_the_penalty_scope() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let scope = OutboundScope::Chat(OutboundChatKey(1));
        let a = replace(&mut s, meta(scope, None, OutboundPriority::NORMAL), 1, t0);
        s.penalize(scope, t0 + Duration::from_secs(60));
        let outcome = replace(&mut s, meta(scope, None, OutboundPriority::NORMAL), 1, t0);
        assert_eq!(outcome.superseded, Some(a.job));
        assert!(s.grant_ready(t0).is_empty());
    }

    #[test]
    fn hot_latest_wins_slot_keeps_its_scheduling_age() {
        let mut s = scheduler(
            limits(100),
            AgingPolicy { quantum: Duration::from_millis(1), max_boost: u8::MAX },
        );
        let t0 = base();
        let _first = replace(&mut s, global(OutboundPriority::LOWEST), 7, t0);
        // the lowest-priority slot is replaced every half quantum while
        // highest-priority jobs keep arriving; the inherited age must keep
        // growing through the replacements
        let mut last = None;
        for i in 1..=10u64 {
            let t = t0 + Duration::from_micros(i * 500);
            last = Some(replace(&mut s, global(OutboundPriority::LOWEST), 7, t).job);
            let _critical = fifo(&mut s, global(OutboundPriority::HIGHEST), t);
        }
        // after max_boost quanta the slot reaches the highest level and,
        // being the oldest at that level, is granted before the flood
        let t1 = t0 + Duration::from_millis(255) + Duration::from_micros(1);
        let granted = jobs(&s.grant_ready(t1));
        assert_eq!(granted.first(), Some(&last.unwrap()));
    }

    #[test]
    fn hot_latest_wins_key_does_not_bloat_the_queue() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let mut superseded = 0;
        let mut last = None;
        for _ in 0..10 {
            let outcome = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
            superseded += usize::from(outcome.superseded.is_some());
            last = Some(outcome.job);
        }
        assert_eq!(superseded, 9);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![last.unwrap()]);
    }

    #[test]
    fn replacement_does_not_break_fifo_of_other_lanes() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let scope_a = OutboundScope::Chat(OutboundChatKey(1));
        let scope_b = OutboundScope::Chat(OutboundChatKey(2));
        let a1 = replace(
            &mut s,
            meta(scope_a, Some(OutboundLaneKey(1)), OutboundPriority::NORMAL),
            1,
            t0,
        );
        let b1 = replace(
            &mut s,
            meta(scope_b, Some(OutboundLaneKey(2)), OutboundPriority::NORMAL),
            1,
            t0,
        );
        // replacing the head of lane A must not push it behind lane B
        let a2 = replace(
            &mut s,
            meta(scope_a, Some(OutboundLaneKey(1)), OutboundPriority::NORMAL),
            1,
            t0,
        );
        let b2 = replace(
            &mut s,
            meta(scope_b, Some(OutboundLaneKey(2)), OutboundPriority::NORMAL),
            1,
            t0,
        );
        assert_eq!(a2.superseded, Some(a1.job));
        assert_eq!(b2.superseded, Some(b1.job));
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a2.job, b2.job]);
    }

    #[test]
    fn coalesce_slot_is_bound_to_the_request_metadata() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = replace(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            7,
            t0,
        );
        // the same user key with a different scope is a different slot
        let b = replace(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(2)), None, OutboundPriority::NORMAL),
            7,
            t0,
        );
        assert_eq!(b.superseded, None);

        // a different class is a different slot as well
        let c = replace(
            &mut s,
            meta_with_class(
                OutboundScope::Chat(OutboundChatKey(1)),
                None,
                OutboundPriority::NORMAL,
                9,
            ),
            7,
            t0,
        );
        assert_eq!(c.superseded, None);

        // none of the replacements collided: all three jobs are still pending
        // and are granted in FIFO order
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a.job, b.job, c.job]);
    }

    #[test]
    fn incompatible_weight_is_an_explicit_error() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        let heavy = OutboundMeta {
            weight: NonZeroU32::new(10).unwrap(),
            ..global(OutboundPriority::NORMAL)
        };
        assert_eq!(
            s.enqueue(heavy, OutboundEnqueueMode::ReplacePending { user_key: 7 }, None, t0),
            Err(EnqueueError::IncompatibleCoalesceMetadata)
        );
        // the old job is untouched and still grants
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a.job]);
    }

    #[test]
    fn alternating_weights_do_not_bloat_the_queue() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let mut last = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0).job;
        for i in 0..10u32 {
            let weight = if i % 2 == 0 { 10 } else { 1 };
            let meta = OutboundMeta {
                weight: NonZeroU32::new(weight).unwrap(),
                ..global(OutboundPriority::NORMAL)
            };
            match s.enqueue(meta, OutboundEnqueueMode::ReplacePending { user_key: 7 }, None, t0) {
                Ok(outcome) => {
                    assert_eq!(outcome.superseded, Some(last));
                    last = outcome.job;
                }
                Err(EnqueueError::IncompatibleCoalesceMetadata)
                | Err(EnqueueError::WeightExceedsWindow { .. }) => {}
            }
        }
        // exactly one pending job remains despite the weight alternation
        assert_eq!(jobs(&s.grant_ready(t0)), vec![last]);
    }

    #[test]
    fn stale_coalesce_entries_are_removed_on_grant_and_cancel() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let meta = global(OutboundPriority::NORMAL);
        let key = InternalCoalesceKey {
            scope: meta.scope,
            lane: meta.lane,
            class: meta.class,
            user_key: 7,
        };
        let a = replace(&mut s, meta, 7, t0);
        assert_eq!(s.coalesce.get(&key), Some(&a.job));
        // after the grant the slot is cleaned up
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a.job]);
        assert!(s.coalesce.is_empty());
        // and after a cancel too
        let b = replace(&mut s, global(OutboundPriority::NORMAL), 7, t0);
        s.cancel(b.job);
        assert!(s.coalesce.is_empty());
    }

    #[test]
    fn delayed_latest_wins_does_not_bloat_the_delayed_heap() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let not_before = t0 + Duration::from_secs(60);
        let mut first = true;
        for _ in 0..100 {
            let outcome = s
                .enqueue(
                    global(OutboundPriority::NORMAL),
                    OutboundEnqueueMode::ReplacePending { user_key: 7 },
                    Some(not_before),
                    t0,
                )
                .unwrap();
            if first {
                assert_eq!(outcome.superseded, None);
                first = false;
            } else {
                assert!(outcome.superseded.is_some());
            }
        }
        // stale heap entries are compacted: only one live delayed job remains
        assert!(s.delayed.len() <= 8, "heap grew to {} entries", s.delayed.len());
    }

    #[test]
    fn expired_penalties_are_pruned() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        s.penalize(OutboundScope::Global, t0 + Duration::from_secs(5));
        assert!(s.penalties.contains_key(&PenaltyKey::Global));
        s.grant_ready(t0 + Duration::from_secs(6)); // triggers the prune
        assert!(s.penalties.is_empty());
    }

    #[test]
    fn idle_chat_windows_are_pruned() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        assert!(s.chat_window_sets.contains_key(&OutboundChatKey(1)));
        // after the window expires, the idle set is pruned
        s.grant_ready(t0 + Duration::from_secs(2));
        assert!(s.chat_window_sets.is_empty());
    }

    #[test]
    fn next_deadline_does_not_return_now_for_a_blocked_candidate() {
        let mut s = scheduler(
            OutboundLimits {
                global: vec![WindowLimit { capacity: 100, window: Duration::from_secs(1) }],
                chat: vec![WindowLimit { capacity: 1, window: Duration::from_secs(10) }],
            },
            aging(),
        );
        let t0 = base();
        let a = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]); // chat 1 window exhausted
        let _b = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert!(s.grant_ready(t0).is_empty());
        // the global window is free, but the chat window is not: the
        // candidate's constraints are AND-ed, so the wake-up is the max
        match s.next_deadline(t0) {
            SchedulerWakeup::At(at) => assert_eq!(at, t0 + Duration::from_secs(10)),
            other => panic!("expected a future wake-up, got {other:?}"),
        }
    }

    #[test]
    fn oversized_weight_is_rejected_at_enqueue() {
        let mut s = scheduler(limits(1), aging());
        let t0 = base();
        let heavy = OutboundMeta {
            weight: NonZeroU32::new(2).unwrap(),
            ..global(OutboundPriority::NORMAL)
        };
        // the weight exceeds the window capacity: the job could never be
        // granted, so it is rejected instead of waiting forever
        assert_eq!(
            s.enqueue(heavy, OutboundEnqueueMode::Fifo, None, t0),
            Err(EnqueueError::WeightExceedsWindow {
                scope: OutboundScope::Global,
                weight: NonZeroU32::new(2).unwrap(),
                capacity: 1,
            })
        );
        // the scheduler state is unchanged and nothing schedules a wake-up
        assert_eq!(s.grant_ready(t0), vec![]);
        assert_eq!(s.next_deadline(t0), SchedulerWakeup::ExternalEvent);
    }

    #[test]
    fn aging_policy_must_span_the_full_priority_range() {
        assert!(matches!(
            SchedulerState::new(
                limits(100),
                AgingPolicy { quantum: Duration::from_secs(1), max_boost: 3 },
            ),
            Err(SchedulerConfigError::AgingCannotReachHighest { max_boost: 3 })
        ));
        // a spanning policy is accepted
        let _ = SchedulerState::new(limits(100), aging()).unwrap();
    }

    #[test]
    fn invalid_window_configs_are_rejected() {
        assert!(matches!(
            SchedulerState::new(
                OutboundLimits {
                    global: vec![WindowLimit { capacity: 0, window: Duration::from_secs(1) }],
                    chat: vec![],
                },
                aging(),
            ),
            Err(SchedulerConfigError::ZeroWindowCapacity)
        ));
        assert!(matches!(
            SchedulerState::new(
                OutboundLimits {
                    global: vec![WindowLimit { capacity: 1, window: Duration::ZERO }],
                    chat: vec![],
                },
                aging(),
            ),
            Err(SchedulerConfigError::ZeroWindowDuration)
        ));
        assert!(matches!(
            SchedulerState::new(
                limits(100),
                AgingPolicy { quantum: Duration::ZERO, max_boost: u8::MAX },
            ),
            Err(SchedulerConfigError::ZeroAgingQuantum)
        ));
    }

    #[test]
    fn empty_chat_window_set_admits_everything() {
        let mut s = scheduler(
            OutboundLimits {
                global: vec![WindowLimit { capacity: 1, window: Duration::from_secs(1) }],
                chat: vec![],
            },
            aging(),
        );
        let t0 = base();
        let a = fifo(
            &mut s,
            meta(OutboundScope::Chat(OutboundChatKey(1)), None, OutboundPriority::NORMAL),
            t0,
        );
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        // the empty set admits everything and is pruned right away
        assert!(s.chat_window_sets.is_empty());
    }

    #[test]
    fn multiple_windows_in_one_set_all_must_pass() {
        let mut s = scheduler(
            OutboundLimits {
                global: vec![
                    WindowLimit { capacity: 1, window: Duration::from_secs(1) },
                    WindowLimit { capacity: 1, window: Duration::from_secs(5) },
                ],
                chat: vec![],
            },
            aging(),
        );
        let t0 = base();
        let a = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]); // both windows consumed
        let b = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert!(s.grant_ready(t0).is_empty());
        // the candidate is ready when the *last* blocking window frees it
        match s.next_deadline(t0) {
            SchedulerWakeup::At(at) => assert_eq!(at, t0 + Duration::from_secs(5)),
            other => panic!("expected a future wake-up, got {other:?}"),
        }
        assert_eq!(jobs(&s.grant_ready(t0 + Duration::from_secs(5))), vec![b]);
    }

    #[test]
    fn delayed_replacement_waits_for_its_own_not_before() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let a_outcome = s
            .enqueue(
                global(OutboundPriority::NORMAL),
                OutboundEnqueueMode::ReplacePending { user_key: 7 },
                Some(t0 + Duration::from_secs(10)),
                t0,
            )
            .unwrap();
        assert_eq!(a_outcome.superseded, None);
        let a = a_outcome.job;
        let b_outcome = s
            .enqueue(
                global(OutboundPriority::NORMAL),
                OutboundEnqueueMode::ReplacePending { user_key: 7 },
                Some(t0 + Duration::from_secs(20)),
                t0,
            )
            .unwrap();
        assert_eq!(b_outcome.superseded, Some(a));
        let b = b_outcome.job;
        // the old deadline does not leak into the replacement
        assert!(s.grant_ready(t0 + Duration::from_secs(10)).is_empty());
        assert_eq!(jobs(&s.grant_ready(t0 + Duration::from_secs(20))), vec![b]);
    }

    #[test]
    fn delayed_replacement_keeps_blocking_its_lane() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let scope = OutboundScope::Chat(OutboundChatKey(1));
        let old_outcome = s
            .enqueue(
                meta(scope, lane, OutboundPriority::NORMAL),
                OutboundEnqueueMode::ReplacePending { user_key: 1 },
                Some(t0 + Duration::from_secs(10)),
                t0,
            )
            .unwrap();
        assert_eq!(old_outcome.superseded, None);
        let old = old_outcome.job;
        let fresh_outcome = s
            .enqueue(
                meta(scope, lane, OutboundPriority::NORMAL),
                OutboundEnqueueMode::ReplacePending { user_key: 1 },
                Some(t0 + Duration::from_secs(20)),
                t0,
            )
            .unwrap();
        assert_eq!(fresh_outcome.superseded, Some(old));
        let fresh = fresh_outcome.job;
        let later = fifo(&mut s, meta(scope, lane, OutboundPriority::CRITICAL), t0);
        // the delayed lane head blocks the whole lane regardless of priority
        assert!(s.grant_ready(t0 + Duration::from_secs(10)).is_empty());
        assert_eq!(jobs(&s.grant_ready(t0 + Duration::from_secs(20))), vec![fresh]);
        s.complete(fresh, OutboundCompletion::Success, t0 + Duration::from_secs(20));
        assert_eq!(jobs(&s.grant_ready(t0 + Duration::from_secs(20))), vec![later]);
    }

    #[test]
    fn stale_delayed_counter_stays_consistent() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let not_before = t0 + Duration::from_secs(60);
        let mut ids = Vec::new();
        for i in 0..20u64 {
            ids.push(
                s.enqueue(
                    global(OutboundPriority::NORMAL),
                    OutboundEnqueueMode::ReplacePending { user_key: i },
                    Some(not_before),
                    t0,
                )
                .unwrap()
                .job,
            );
        }
        // a delayed job cancelled after the last compaction leaves a stale
        // heap entry that must be dropped exactly once at promotion time
        s.cancel(ids[19]);
        assert_eq!(jobs(&s.grant_ready(not_before)), ids[..19]);
    }

    #[test]
    fn lane_fifo_survives_order_wraparound() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = OutboundLaneKey(1);
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        // make the lane's own order counter wrap around
        s.lanes.insert(
            lane,
            LaneState {
                pending: BTreeSet::new(),
                in_flight: None,
                next_order: u64::MAX - 1,
                stale: 0,
                in_candidate_heap: false,
                candidate_effective: 0,
            },
        );
        let a = fifo(&mut s, meta(chat, Some(lane), OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, meta(chat, Some(lane), OutboundPriority::NORMAL), t0);
        let c = fifo(&mut s, meta(chat, Some(lane), OutboundPriority::NORMAL), t0);
        // the lane is strictly FIFO across the counter wraparound
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        s.complete(a, OutboundCompletion::Success, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![b]);
        s.complete(b, OutboundCompletion::Success, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![c]);
    }

    #[test]
    fn lane_order_rebase_at_base_zero_keeps_fifo() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = OutboundLaneKey(1);
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        let head = fifo(&mut s, meta(chat, Some(lane), OutboundPriority::NORMAL), t0);
        // churn behind the head: superseded records stay as stale entries
        let mut live = None;
        for _ in 0..5u64 {
            let outcome = s
                .enqueue(
                    meta(chat, Some(lane), OutboundPriority::NORMAL),
                    OutboundEnqueueMode::ReplacePending { user_key: 1 },
                    None,
                    t0,
                )
                .unwrap();
            live = Some(outcome.job);
        }
        // simulate the wrap: the counter is 0 while a live order-0 head is
        // still pending (a window shift by the head order would overflow)
        s.lanes.get_mut(&lane).unwrap().next_order = 0;
        let fresh = fifo(&mut s, meta(chat, Some(lane), OutboundPriority::NORMAL), t0);
        // the dense rebase keeps the FIFO order across the wrap
        assert_eq!(jobs(&s.grant_ready(t0)), vec![head]);
        s.complete(head, OutboundCompletion::Success, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![live.unwrap()]);
        s.complete(live.unwrap(), OutboundCompletion::Success, t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![fresh]);
    }

    #[test]
    fn cancelling_a_lanes_only_job_removes_the_lane() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = OutboundLaneKey(1);
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        let a = fifo(&mut s, meta(chat, Some(lane), OutboundPriority::NORMAL), t0);
        s.cancel(a);
        assert!(s.lanes.contains_key(&lane));
        s.grant_ready(t0);
        assert!(!s.lanes.contains_key(&lane), "a lane with no jobs must be removed");
    }

    #[test]
    fn churn_behind_a_blocked_top_candidate_is_parked_and_bounded() {
        let mut s = scheduler(limits(10), aging());
        let t0 = base();
        // fill the global window so that a weight-10 job cannot fit now
        let lights: Vec<_> =
            (0..10).map(|_| fifo(&mut s, global(OutboundPriority::NORMAL), t0)).collect();
        assert_eq!(jobs(&s.grant_ready(t0)), lights);
        // a heavy job that cannot fit now
        let heavy = OutboundMeta {
            weight: NonZeroU32::new(10).unwrap(),
            ..global(OutboundPriority::NORMAL)
        };
        let heavy_job = fifo(&mut s, heavy, t0);
        // latest-wins churn behind it: every replacement supersedes the
        // previous pending job
        let mut live = None;
        for _ in 0..100u64 {
            let outcome = s
                .enqueue(
                    global(OutboundPriority::NORMAL),
                    OutboundEnqueueMode::ReplacePending { user_key: 7 },
                    None,
                    t0,
                )
                .unwrap();
            live = Some(outcome.job);
        }
        s.grant_ready(t0);
        // the heavy candidate is blocked (reserving the window) and the
        // churn tail is parked in the reservation queue; no candidates
        // remain in the heap
        assert_eq!(s.blocked.len(), 1, "the heavy job must be the blocked head");
        let parked = s
            .reservations
            .get(&WindowRef::Global)
            .map(|reservation| reservation.queue.len())
            .unwrap_or(0);
        assert_eq!(parked, 1, "the churn tail must be parked, got {parked}");
        assert!(s.candidates.is_empty(), "no candidates may remain after grant_ready");
        // once the window drains, the heavy job is granted first; the tail
        // waits for the window to free again
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(jobs(&s.grant_ready(t1)), vec![heavy_job]);
        assert_eq!(jobs(&s.grant_ready(t1 + Duration::from_secs(1))), vec![live.unwrap()]);
    }

    #[test]
    fn churn_behind_a_live_lane_head_stays_bounded() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        // a live delayed lane head blocks the lane and the compaction
        let _head = s
            .enqueue(
                meta(chat, lane, OutboundPriority::NORMAL),
                OutboundEnqueueMode::Fifo,
                Some(t0 + Duration::from_secs(60)),
                t0,
            )
            .unwrap()
            .job;
        // latest-wins churn behind the head
        for _ in 0..100u64 {
            let _ = s
                .enqueue(
                    meta(chat, lane, OutboundPriority::NORMAL),
                    OutboundEnqueueMode::ReplacePending { user_key: 1 },
                    None,
                    t0,
                )
                .unwrap();
        }
        s.grant_ready(t0); // triggers the lane compaction
        let lane_state = s.lanes.get(&OutboundLaneKey(1)).unwrap();
        assert!(
            lane_state.pending.len() <= 3,
            "lane pending grew to {} entries",
            lane_state.pending.len()
        );
    }

    /// Stress test for the mass-grant path: two delayed groups whose
    /// sequence order diverges from their aging order, so every grant used
    /// to remove a job from the middle of the sequence-ordered queue. A
    /// quadratic removal loop takes minutes at this size; the lazy
    /// ready/lane structures keep it linear. Run manually.
    #[test]
    #[ignore = "stress: run manually to verify the grant path has no quadratic removal"]
    fn mass_grant_of_promoted_delayed_jobs_stays_linear() {
        let mut s = scheduler(limits(100_000), aging());
        let t0 = base();
        let job_count = 50_000u64;
        // group 1: early sequences, promoted only at t0 + 100s
        for _ in 0..job_count {
            let _ = s
                .enqueue(
                    global(OutboundPriority::NORMAL),
                    OutboundEnqueueMode::Fifo,
                    Some(t0 + Duration::from_secs(100)),
                    t0,
                )
                .unwrap();
        }
        // group 2: later sequences, promoted at t0 + 1s and aged longer
        for _ in 0..job_count {
            let _ = s
                .enqueue(
                    global(OutboundPriority::NORMAL),
                    OutboundEnqueueMode::Fifo,
                    Some(t0 + Duration::from_secs(1)),
                    t0,
                )
                .unwrap();
        }
        // the global penalty keeps the already-ready group 2 waiting
        s.penalize(OutboundScope::Global, t0 + Duration::from_secs(100));
        assert!(s.grant_ready(t0 + Duration::from_secs(1)).is_empty());

        let started = Instant::now();
        let granted = s.grant_ready(t0 + Duration::from_secs(100));
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_secs(10), "grant_ready took {elapsed:?}");
        assert_eq!(granted.len(), job_count as usize * 2);
    }

    #[test]
    fn delayed_to_ready_replacement_restarts_aging() {
        let mut s = scheduler(
            limits(100),
            AgingPolicy { quantum: Duration::from_millis(1), max_boost: u8::MAX },
        );
        let t0 = base();
        let _old = s
            .enqueue(
                global(OutboundPriority::LOWEST),
                OutboundEnqueueMode::ReplacePending { user_key: 7 },
                Some(t0 + Duration::from_millis(100)),
                t0,
            )
            .unwrap()
            .job;
        // the delayed job is replaced by an immediate one at t0 + 90
        let fresh = s
            .enqueue(
                global(OutboundPriority::LOWEST),
                OutboundEnqueueMode::ReplacePending { user_key: 7 },
                None,
                t0 + Duration::from_millis(90),
            )
            .unwrap()
            .job;
        // a mid-priority job enqueued at the same moment
        let mid = s
            .enqueue(
                global(OutboundPriority::new(50)),
                OutboundEnqueueMode::Fifo,
                None,
                t0 + Duration::from_millis(90),
            )
            .unwrap()
            .job;
        // at t0 + 91 the fresh job has waited 1ms (boost 1) while the mid
        // job sits at 50: with the old bug the fresh job inherited the
        // delayed job's ready_at (boost 91) and would overtake the mid job
        let granted = jobs(&s.grant_ready(t0 + Duration::from_millis(91)));
        assert_eq!(granted, vec![mid, fresh]);
    }

    #[test]
    fn cancelling_many_delayed_jobs_compacts_the_heap() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let not_before = t0 + Duration::from_secs(60);
        let mut ids = Vec::new();
        for _ in 0..10_000u64 {
            ids.push(
                s.enqueue(
                    global(OutboundPriority::NORMAL),
                    OutboundEnqueueMode::Fifo,
                    Some(not_before),
                    t0,
                )
                .unwrap()
                .job,
            );
        }
        // a cancel-only churn must not leave a bloated heap behind
        for id in &ids {
            s.cancel(*id);
        }
        assert_eq!(jobs(&s.grant_ready(not_before + Duration::from_secs(1))), vec![]);
        assert!(s.delayed.is_empty(), "the delayed heap must be compacted");
    }

    #[test]
    fn heavy_aged_job_is_not_starved_by_light_traffic() {
        let mut s = scheduler(
            OutboundLimits {
                global: vec![WindowLimit { capacity: 10, window: Duration::from_secs(1) }],
                chat: vec![],
            },
            AgingPolicy { quantum: Duration::from_millis(1), max_boost: u8::MAX },
        );
        let t0 = base();
        // five light jobs occupy half of the window
        let lights: Vec<_> =
            (0..5).map(|_| fifo(&mut s, global(OutboundPriority::NORMAL), t0)).collect();
        assert_eq!(jobs(&s.grant_ready(t0)), lights); // window 5/10
                                                      // a heavy job has been waiting long enough to age to the top
        let heavy = OutboundMeta {
            weight: NonZeroU32::new(10).unwrap(),
            ..global(OutboundPriority::LOWEST)
        };
        let heavy_job = s
            .enqueue(heavy, OutboundEnqueueMode::Fifo, None, t0 - Duration::from_millis(256))
            .unwrap()
            .job;
        // an endless stream of light jobs keeps arriving, one per tick
        let mut granted_heavy = false;
        for i in 1..=1200u64 {
            let t = t0 + Duration::from_millis(i);
            let _ = fifo(&mut s, global(OutboundPriority::NORMAL), t);
            if jobs(&s.grant_ready(t)).contains(&heavy_job) {
                granted_heavy = true;
                break;
            }
        }
        assert!(granted_heavy, "the heavy aged job must be granted within finite time");
    }

    /// Stress test for the rate-limited gradual drain: with capacity 1 and
    /// one grant per tick, a full collect+sort per call would be
    /// quadratic. Run manually.
    #[test]
    #[ignore = "stress: run manually to verify the gradual drain has no quadratic arbitration"]
    fn gradual_drain_of_many_jobs_stays_linear() {
        let mut s = scheduler(
            OutboundLimits {
                global: vec![WindowLimit { capacity: 1, window: Duration::from_millis(1) }],
                chat: vec![],
            },
            aging(),
        );
        let t0 = base();
        let job_count = 50_000u64;
        for _ in 0..job_count {
            let _ = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        }
        let started = Instant::now();
        let mut granted = 0usize;
        for i in 1..=job_count {
            granted += jobs(&s.grant_ready(t0 + Duration::from_millis(i))).len();
        }
        let elapsed = started.elapsed();
        assert_eq!(granted, job_count as usize, "every job must be granted");
        assert!(elapsed < Duration::from_secs(10), "gradual drain took {elapsed:?}");
    }

    #[test]
    fn cancelling_a_parked_lane_head_wakes_the_next_lane_job() {
        let mut s = scheduler(limits(1), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let chat1 = OutboundScope::Chat(OutboundChatKey(1));
        let chat2 = OutboundScope::Chat(OutboundChatKey(2));
        // a chat-1 job fills both windows
        let a = fifo(&mut s, meta(chat1, None, OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        // a second chat-1 job is blocked and reserves the global window
        let b = fifo(&mut s, meta(chat1, None, OutboundPriority::NORMAL), t0);
        assert!(s.grant_ready(t0).is_empty());
        // a lane head is parked by the reservation and then cancelled
        let head = fifo(&mut s, meta(chat2, lane, OutboundPriority::NORMAL), t0);
        let next = fifo(&mut s, meta(chat2, lane, OutboundPriority::NORMAL), t0);
        assert!(s.grant_ready(t0).is_empty());
        s.cancel(head);
        // when the reservation expires the woken jobs drain in FIFO order:
        // b (older), then the lane's next head (the parked one was dead)
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(jobs(&s.grant_ready(t1)), vec![b]);
        assert_eq!(jobs(&s.grant_ready(t1 + Duration::from_secs(1))), vec![next]);
    }

    #[test]
    fn replacing_a_queued_lane_head_rekeys_the_candidate() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        // a critical lane head is queued, then replaced by a low-priority
        // job while it is already in the candidate heap
        let old = s
            .enqueue(
                meta(chat, Some(OutboundLaneKey(1)), OutboundPriority::CRITICAL),
                OutboundEnqueueMode::ReplacePending { user_key: 1 },
                None,
                t0,
            )
            .unwrap()
            .job;
        let fresh_outcome = s
            .enqueue(
                meta(chat, Some(OutboundLaneKey(1)), OutboundPriority::LOWEST),
                OutboundEnqueueMode::ReplacePending { user_key: 1 },
                None,
                t0,
            )
            .unwrap();
        assert_eq!(fresh_outcome.superseded, Some(old));
        let fresh = fresh_outcome.job;
        // a normal job in another lane
        let other =
            fifo(&mut s, meta(chat, Some(OutboundLaneKey(2)), OutboundPriority::NORMAL), t0);
        // the replacement must not overtake the other lane's normal job
        assert_eq!(jobs(&s.grant_ready(t0)), vec![other, fresh]);
    }

    #[test]
    fn repeatedly_replacing_a_queued_lane_head_rekeys_the_candidate() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        // CRITICAL -> LOWEST -> CRITICAL replacements of the queued head
        let a = s
            .enqueue(
                meta(chat, Some(OutboundLaneKey(1)), OutboundPriority::CRITICAL),
                OutboundEnqueueMode::ReplacePending { user_key: 1 },
                None,
                t0,
            )
            .unwrap()
            .job;
        let b_outcome = s
            .enqueue(
                meta(chat, Some(OutboundLaneKey(1)), OutboundPriority::LOWEST),
                OutboundEnqueueMode::ReplacePending { user_key: 1 },
                None,
                t0,
            )
            .unwrap();
        assert_eq!(b_outcome.superseded, Some(a));
        let c_outcome = s
            .enqueue(
                meta(chat, Some(OutboundLaneKey(1)), OutboundPriority::CRITICAL),
                OutboundEnqueueMode::ReplacePending { user_key: 1 },
                None,
                t0,
            )
            .unwrap();
        assert_eq!(c_outcome.superseded, Some(b_outcome.job));
        // a normal job in another lane
        let other =
            fifo(&mut s, meta(chat, Some(OutboundLaneKey(2)), OutboundPriority::NORMAL), t0);
        // the final replacement is CRITICAL: it must beat the other lane
        assert_eq!(jobs(&s.grant_ready(t0)), vec![c_outcome.job, other]);
    }

    #[test]
    fn sequence_rebase_at_zero_with_stale_candidates_does_not_panic() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        // a live job holds sequence 0
        let a = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert_eq!(a, JobId(0));
        // a cancelled job leaves a stale candidate entry behind
        let b = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        s.cancel(b);
        // the counter is about to wrap with a live sequence 0: the next
        // enqueue triggers the dense rebase, which must not panic on the
        // stale candidate entry
        s.next_sequence = u64::MAX;
        let c = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a, c]);
    }

    #[test]
    fn sequence_rebase_at_zero_keeps_a_lane_head_after_cancel() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        let lane = Some(OutboundLaneKey(1));
        let chat = OutboundScope::Chat(OutboundChatKey(1));
        // a live unlaned job holds sequence 0
        let live = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        // two lane jobs; the head is cancelled and stays as a stale entry
        let a = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        let b = fifo(&mut s, meta(chat, lane, OutboundPriority::NORMAL), t0);
        s.cancel(a);
        // force the dense rebase while the stale head is still in pending
        s.next_sequence = u64::MAX;
        let c = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        // the lane's next head must still be granted, in FIFO order
        assert_eq!(jobs(&s.grant_ready(t0)), vec![live, b, c]);
    }

    #[test]
    fn sequence_rebase_at_zero_keeps_a_delayed_replacement_live() {
        let mut s = scheduler(limits(100), aging());
        let t0 = base();
        // a live unlaned job holds sequence 0
        let live = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        // a delayed latest-wins slot: the superseded job leaves a stale
        // heap entry that shares its sequence with the replacement
        let not_before = t0 + Duration::from_secs(60);
        let a = s
            .enqueue(
                global(OutboundPriority::CRITICAL),
                OutboundEnqueueMode::ReplacePending { user_key: 7 },
                Some(not_before),
                t0,
            )
            .unwrap()
            .job;
        let b_outcome = s
            .enqueue(
                global(OutboundPriority::CRITICAL),
                OutboundEnqueueMode::ReplacePending { user_key: 7 },
                Some(not_before),
                t0,
            )
            .unwrap();
        assert_eq!(b_outcome.superseded, Some(a));
        // force the dense rebase
        s.next_sequence = u64::MAX;
        let c = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        // the delayed replacement must still be promoted at its deadline
        // (outranking the aged NORMAL jobs), and the stale entry must not
        // underflow the stale counter
        assert_eq!(
            jobs(&s.grant_ready(not_before + Duration::from_secs(1))),
            vec![b_outcome.job, live, c]
        );
    }

    #[test]
    fn parked_candidates_join_an_active_reservation_not_an_expired_one() {
        let mut s = scheduler(
            OutboundLimits {
                global: vec![WindowLimit { capacity: 1, window: Duration::from_secs(1) }],
                chat: vec![WindowLimit { capacity: 1, window: Duration::from_secs(60) }],
            },
            aging(),
        );
        let t0 = base();
        let chat1 = OutboundScope::Chat(OutboundChatKey(1));
        let a = fifo(&mut s, meta(chat1, None, OutboundPriority::NORMAL), t0);
        assert_eq!(jobs(&s.grant_ready(t0)), vec![a]);
        // a global job is blocked by the global window: the global hold
        // expires at t0 + 1s
        let g = fifo(&mut s, global(OutboundPriority::NORMAL), t0);
        assert!(s.grant_ready(t0).is_empty());
        // two chat-1 jobs park in the (active) global hold
        let c1 = fifo(&mut s, meta(chat1, None, OutboundPriority::CRITICAL), t0);
        let c2 = fifo(&mut s, meta(chat1, None, OutboundPriority::CRITICAL), t0);
        assert!(s.grant_ready(t0).is_empty());
        assert_eq!(s.reservations.get(&WindowRef::Global).map(|r| r.queue.len()), Some(2));
        // a third chat-1 job arrives at the expiry tick
        let c3 = fifo(
            &mut s,
            meta(chat1, None, OutboundPriority::CRITICAL),
            t0 + Duration::from_secs(1),
        );
        // during the pass the global hold is expired but retained (its head
        // is promoted): the promoted candidate is re-blocked by the chat-1
        // window, and the next chat-1 candidate must join the fresh active
        // chat hold, not the expired global one
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(jobs(&s.grant_ready(t1)), vec![g]);
        assert_eq!(
            s.reservations.get(&WindowRef::Chat(OutboundChatKey(1))).map(|r| r.queue.len()),
            Some(1),
            "the candidate must join the active chat hold"
        );
        assert_eq!(
            s.reservations.get(&WindowRef::Global).map(|r| r.queue.len()),
            Some(1),
            "the expired global hold must keep only its own queue"
        );
        let _ = (c1, c2, c3);
    }
}
