# Outbound scheduler — design note (Commit 1)

Deterministic outbound scheduling model in `crates/teloxide-core/src/outbound/`.
Internal draft: no Tokio, no actor, no public API yet. Time is passed as a
parameter (`now: Instant`); everything is a pure state machine.

## Scope

The scheduler owns only the shared admission/rate/order layer. It never
executes requests, never type-erases request futures and never retries them.
The `Drafter` actor remains its own lifecycle state machine; it becomes a
consumer of the scheduler, not its replacement.

## State

```text
jobs: HashMap<JobId, Job>
      Job { meta, sequence, ready_at, not_before, coalesce_key, lane_order,
            in_candidate_heap, candidate_effective }
candidates: BinaryHeap<CandidateKey>     persistent candidate heap
      CandidateKey { effective, sequence, Job(JobId) | Lane(OutboundLaneKey) }
      (effective desc, sequence asc; entries validated lazily on pop)
blocked: BinaryHeap<Reverse<BlockedJob>> failed candidates, by earliest
      eligibility; BlockedJob { until, reference }
aging_events: BinaryHeap<Reverse<AgingEvent>>
      the moment a candidate's effective priority rises one level; the
      candidate heap is re-keyed by event, never by a per-tick full scan
delayed: BinaryHeap<Reverse<DelayedJob>>   (not_before, sequence, job)
reservations: HashMap<WindowRef, Reservation>
      a window held back for a blocked top-aged candidate:
      Reservation { until, queue: VecDeque<CandidateRef> } (parked
      consumers; a lane reference wakes the lane's current head)
lanes: HashMap<OutboundLaneKey, LaneState>
      LaneState { pending: BTreeSet<(u64 order, JobId)>, in_flight,
                  next_order, stale, in_candidate_heap, candidate_effective }
      (order is the lane's own counter — lane FIFO is immune to the global
       sequence wraparound; the window is rebased densely on counter wrap)
coalesce: HashMap<InternalCoalesceKey, JobId>
global_windows: WindowSet                  Vec<RollingWindow>, all must admit
chat_window_sets: HashMap<OutboundChatKey, WindowSet>
penalties: HashMap<PenaltyKey, Instant>    extended via max(old, new)
in_flight: HashMap<JobId, InFlight>        granted jobs (lane only)
next_sequence / next_job_id                u64 counters (wraparound-safe)
```

`OutboundLimits { global: Vec<WindowLimit>, chat: Vec<WindowLimit> }` — each
entry of a vector is one window of a window set; a request must pass every
window of the set at once (`WindowSet::earliest_for` is the `max` of the
per-window release moments).

## Configuration validation

`SchedulerState::new` returns `Result` and rejects: zero window capacity,
zero window duration, zero aging quantum, and an aging policy whose
`max_boost` cannot lift `LOWEST` to `HIGHEST` (the anti-starvation
guarantee). `enqueue` rejects a weight that never fits an applicable window
(`EnqueueError::WeightExceedsWindow`) — such a job could never be granted.

## Selection algorithm (event-driven)

1. `promote_aging`: re-key candidates whose aging deadline passed (their
   effective priority rose); stale events for granted/blocked/parked jobs
   are skipped.
2. `promote_delayed`: jobs whose `not_before` passed become ready and are
   pushed into the candidate heap (lane heads via the lane, others
   directly).
3. `promote_blocked`: candidates whose blocked deadline passed are
   re-inserted (a lane reference wakes its current head; dead jobs are
   skipped).
4. `promote_reservations`: for each expired window hold, exactly the head
   of the parked queue is re-inserted (the hold is re-armed when it is
   granted or blocked again) — a rate-limited drain stays linear instead of
   re-pushing the whole queue per tick. A lane reference wakes the lane's
   current head, so cancelling a parked lane head falls through to the next
   pending job of the lane.
5. Loop: pop the best candidate from the persistent heap (stale entries
   are skipped, a lane entry re-keys when its head changed) and either:
   - grant it (window budgets are debited, the lane locks, the window hold
     is re-armed for the next parked candidate), or
   - block it (failed admission: penalties or full windows; a window that
     does not fit the candidate is held back, see below), or
   - park it (its window is already held for an older candidate).

Candidates are all unlaned ready jobs plus the head of every free ordering
lane. A scoped `RetryAfter` penalty or an exhausted per-chat window never
stalls other scopes of the same priority (HEOL). A lane entry whose head
was replaced (or cancelled) is re-keyed on pop with the current head's
effective priority and sequence; aging events are re-armed from the current
head's maturity, so a priority change of a queued lane head never leaves a
stale key in the heap.

## Ordering lanes

Strict FIFO in enqueue order, **independent of priority and `not_before`**:
the lane head is the only grantable job, so a later Critical job can never
overtake an earlier Normal head, and a delayed head blocks the whole lane.
Priority only chooses between the heads of different lanes (and between
unlaned jobs).

## Fairness

Numeric `u8` priority (higher = more urgent) with named constants
(`LOWEST = 0`, `BACKGROUND = 32`, `NORMAL = 128`, `INTERACTIVE = 192`,
`CRITICAL = 224`, `HIGHEST = 255`); callers may use any value. Aging:

```text
effective = min(base + min(waited / quantum, max_boost), HIGHEST)
```

`waited` is measured from `ready_at` (enqueue time, or promotion time for a
delayed job). The anti-starvation guarantee is unconditional: `max_boost`
must span the whole range, enforced at construction. Aging raises the
effective priority by one level per quantum; each rise is scheduled as an
aging event, so the candidate heap is re-keyed by deadline, not recomputed
per tick. Bound: once a job's boost has matured it competes at the highest
level, where FIFO by sequence puts it ahead of everything that arrived
later.

## Weighted anti-starvation (window reservation)

A top-aged candidate that does not fit a window reserves that window until
the moment the window admits its weight (`earliest_for`). While the hold is
active, every consumer of the window is parked in the reservation queue, so
a stream of lighter jobs cannot keep the window full forever and starve the
blocked candidate. A held global window stops the whole downstream drain
(conservative); a held chat window stops only that chat. When the hold
expires, the blocked candidate is granted first and the hold is re-armed
for the next parked candidate.

## Windows

Budget is debited at grant time (not at enqueue) and never refunded, even on
`Failed` or `CancelledAfterGrant`. Cancelling a waiting job consumes nothing.

## Cancellation and permit lifecycle

- `cancel(job)`: only waiting jobs. All removal is lazy: the delayed,
  blocked, candidate and lane-pending entries are skipped when they
  surface; the coalesce entry is removed eagerly. The delayed heap is
  compacted after every cancel so that a cancel-only churn cannot bloat it.
- `complete(job, completion, now)`: removes the in-flight record, releases
  the lane exactly once (a repeated completion is a no-op) and re-inserts
  the lane head. A `RetryAfter` completion carries an **explicit penalty
  scope** (a chat-scoped request can report a global flood penalty); the
  scheduler penalizes the reported scope until `until`.

## Wake-up model

`next_deadline(now) -> SchedulerWakeup` reads the tops of the delayed,
blocked and aging-event heaps plus the active window holds (stale heads are
dropped first). `At(Instant)` is the earliest moment at which something may
become grantable; `ExternalEvent` means nothing time-based will change.
After a `grant_ready` pass `Immediate` never occurs.

## Latest-wins (KeepLatest)

`OutboundEnqueueMode::ReplacePending { user_key }`:

- Only a not-yet-granted job is replaced; in-flight requests are never
  cancelled by the scheduler.
- The slot key is derived **by the scheduler** from the new request's
  metadata (`scope`, `lane`, `class`) plus the caller's `user_key`; the
  caller cannot spoof a slot with different semantics. A weight change on
  an existing slot is an explicit `EnqueueError::IncompatibleCoalesceMetadata`
  and never falls back to a silent second pending job.
- The replacement inherits the superseded job's sequence (queue position)
  **and its scheduling age** (`ready_at`) for ready -> ready replacements
  only; any transition out of delayed starts aging at the moment the job
  actually becomes ready.
- The old `JobId` is invalidated: a late `cancel(old_id)` is a no-op.
- `EnqueueOutcome { job, superseded }` reports the superseded id; its waiter
  must be completed with `Superseded` (Commit 2) — never silently dropped,
  never given a fake permit.
- Replacing a pending job does not debit rate budget; the penalty scope is
  preserved (scope is part of the key).
- Stale state is bounded: coalesce entries are removed on grant/cancel,
  expired penalties and idle chat windows are pruned during `grant_ready`,
  and the delayed/candidate heaps and the lane pending sets are compacted
  when stale entries dominate.

The scheduler implements only KeepLatest. Debounce and value coalescing stay
in the calling layers (e.g. `Drafter`).

## Complexity

Targets (ТЗ §29) for the model: enqueue O(1)/O(log N), cancel O(1) lazy,
delayed insertion O(log N), `next_deadline` O(1) (heap tops; stale heads
drained lazily), grant amortized O(log N) (one heap pop, lane heads via
`BTreeSet`), completion O(log N) for lanes. Arbitration is event-driven: no
full collect+sort pass happens per tick, and a rate-limited gradual drain
is linear in the number of ticks. The
`gradual_drain_of_many_jobs_stays_linear` and
`mass_grant_of_promoted_delayed_jobs_stays_linear` ignored stress tests
(50k jobs at capacity 1 over 50k ticks; 100k promoted jobs) guard against
quadratic regressions.

## Out of scope for Commit 1

Actor + `OutboundPermit` (with `Drop` -> `CancelledAfterGrant`), `QueueFull`,
`Closed` and `Superseded` errors, shutdown with an explicit error, bounded
backlog, settings updates, `Requester` adapter, payload classification,
`OrderedStart` lanes (Commit 1 is serial-only), `Throttle` and `Drafter`
migration — all later commits.
