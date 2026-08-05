# Outbound scheduler — design note (Commits 1–3)

Deterministic outbound scheduling model in `crates/teloxide-core/src/outbound/`.

- **Commit 1**: the pure state machine (`SchedulerState`) — no Tokio, no
  actor. Time is passed as a parameter (`now: Instant`).
- **Commit 2**: the Tokio actor (`OutboundActor`), the clone-friendly
  `OutboundQueue`/`OutboundQueueHandle`, `OutboundAcquire` (a future with
  cancellation) and the completion-aware `OutboundPermit`.
- **Commit 3**: the `Outbound<R>` requester adaptor and `ScheduledRequest<R>`
  — the vertical slice that wires real typed requests through the queue
  without any automatic retry (see "Requester adaptor (Commit 3)" below).
  Public API: `OutboundMetadata`, `OutboundPriority`, `OutboundScope`,
  `OutboundCompletion`, `OutboundLimits`, `OutboundSettings` (with
  `OutboundSettings::default()`), `AgingPolicy`, `OutboundQueueError`,
  `OutboundAcquireError` (now `Display` + `std::error::Error`),
  `OutboundSnapshot`, `SchedulerConfigError`, `Outbound`, `ScheduledRequest`,
  `OutboundRequestError<E>`, `outbound::class`. Draft quality: naming and
  shape are expected to be refined during the architectural review of each
  commit. `OutboundScope`/`OutboundChatKey`/`OutboundMetadata` are
  intentionally not `Copy` (the chat key stores the username as text).

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

## Actor (Commit 2)

`OutboundQueue::new(settings)` returns the queue plus the actor future;
`new_spawn` spawns it on the current runtime. The handle speaks command
RPC: `Enqueue` goes through a **bounded** channel of capacity
`OutboundSettings::queue_capacity` (fail-fast `QueueFull`), while lifecycle
commands (`Cancel`, `Complete`, `Penalize`, `GetLimits`, `SetLimits`,
`GetSnapshot`, `Shutdown`) ride a separate **unbounded** channel so that
`Drop`-based completions can never await a bounded send and a saturated
ingress cannot delay them. The actor is the sole owner of the scheduler
state. The actor clock derives `now` from the Tokio clock, so
`#[tokio::test(start_paused = true)]` drives the scheduler deterministically.

### Acquire lifecycle

- `acquire(metadata)` enqueues a FIFO request; `acquire_latest_wins(metadata,
  user_key)` uses a latest-wins slot. `serial_lane()` allocates a strict FIFO
  ordering lane (`OutboundLane`); at most one lane request is in flight and
  the lane is served in enqueue order.
- `OutboundAcquire` resolves with `OutboundPermit` or an error. The grant
  receiver is owned by the future from the moment of creation (it is not
  nested inside the enqueue reply), and the **permit is minted by the actor
  at grant time and travels through the waiter channel**. The permit's own
  `Drop` is therefore the universal safety net: dropping the acquire future
  in any state — while the enqueue command is still in flight (the failed
  enqueue reply cancels the pending job), while waiting for a grant, or
  after the grant was already buffered in its channel — drops a live permit,
  which reports `CancelledAfterGrant`. A permit can never be lost: it either
  reaches the caller or completes the job itself.
- The actor mints permits with its own **lifecycle** sender (never an
  enqueue sender), so dropping the last external handle and any
  outstanding acquire futures closes the enqueue ingress and the actor
  ends by itself (pending waiters resolve with `OutboundAcquireError::Closed`;
  permits already granted stay valid until dropped). `shutdown()` resolves
  the waiters the same way and remains available for explicit
  termination.
- `OutboundPermit::complete(outcome)` reports the terminal outcome;
  `RetryAfter` carries an **explicit scope** and a duration (the actor
  converts it to an absolute penalty deadline). Dropping the permit without
  completion reports `CancelledAfterGrant` (best effort).

### Backpressure and shutdown

- The enqueue ingress is a **bounded channel** of capacity
  `OutboundSettings::queue_capacity`: an acquire that cannot be admitted
  fails fast with `OutboundQueueError::QueueFull` (`try_send`, the actor
  never sees the command). Inside the scheduler the backlog is bounded the
  same way — the check lives in `SchedulerState::enqueue`, so a latest-wins
  replacement (which removes the superseded job and does not grow the
  backlog) is admitted even at capacity, while a brand-new job at capacity
  fails fast.
- Lifecycle commands (cancel, complete, penalize, limits, snapshot,
  shutdown) flow through a separate **unbounded** channel: `Drop`-based
  completions are synchronous and can never await a bounded send, and a
  saturated ingress must not delay them. The actor `select!`s both channels
  fairly (tokio select semantics; no strict priority between them).
- The actor mints permits with its own lifecycle sender, never an enqueue
  sender: dropping the last external handle **and any outstanding acquire
  futures** (each acquire holds a handle clone, i.e. an enqueue sender)
  closes the ingress and the actor ends by itself (pending waiters resolve
  with `OutboundAcquireError::Closed`; permits already granted stay valid
  until dropped, their completions are best-effort). `shutdown()` resolves
  the waiters the same way and is still available for explicit
  termination. Panics inside the actor loop are contained: the task ends,
  waiters resolve with `Closed`.

### Wake-up

After every command batch and every timer tick the actor runs one admission
pass and resets its timer to `next_deadline`. `Immediate` (an invariant
breach: `grant_ready` left an admissible candidate behind) maps to an
immediate wake-up — a visible busy loop, not a silent year-long deadlock;
`ExternalEvent` parks the timer until a command arrives.

### `set_limits`

Replaces the windows while **keeping the already debited grant history**: a
grant is never refunded, so a same-value update must not enable a fresh
burst — `set_limits` changes the limits, it does not reset the rate budget.
Every pending job must still fit the new windows (otherwise the update is
rejected as a whole with
`SchedulerConfigError::PendingWeightExceedsWindow` and the previous limits
stay in effect). Blocked and parked candidates are **re-armed**: their
deadlines were derived from the old windows and must not delay candidates
under the new limits. The actor is woken by the command, so newly admissible
candidates are granted without waiting for a stale deadline. Invalid limits
are reported as `OutboundSetLimitsError::Invalid(SchedulerConfigError)`.

History carry-over follows an explicitly **prospective policy**: the old
windows are pruned at the update instant (deterministically, independent of
whether an admission happened to prune earlier), then the ledger is taken
from the longest old window, which retains every event any not-longer new
window could still cover. Lengthening a window beyond the old maximum does
not retroactively constrain grants that already expired under the old
windows; shrinking a window may temporarily hold more history than its
capacity (the debit is never refunded) until it expires.

## Requester adaptor (Commit 3)

`Outbound<R>` wraps any `Requester` and returns `ScheduledRequest<R::Method>`
values from a small representative method set (`get_me`, `send_message`,
`send_rich_message`, `edit_message_text`, `send_chat_action`). A
`ScheduledRequest<Req>` is itself a `Request`: it holds the inner request,
the queue and the `OutboundMetadata` policy, and its `Send` future runs
the vertical slice:

```text
recompute the scope from the final payload (adaptor requests)
  ->  acquire permit (lane or queue handle)
  ->  ONLY NOW create and poll the inner request (send())
  ->  classify the outcome
        Ok(_)                   -> Success
        Err(RetryAfter(secs))   -> RetryAfter { explicit scope, duration }
        Err(_)                  -> Failed
  ->  complete the permit with a completion barrier
  ->  return the original outcome unchanged
```

Key restrictions of the slice:

- **No automatic retry**: the queue records the penalty; retry policy stays
  in the calling layer.
- **The inner send future is created after the grant**: the request object
  itself is built by the adaptor call (`inner.send_message(...)`), but its
  `send()` future is created and polled only once the permit is held —
  nothing about it (e.g. a timeout captured at construction) starts
  before that. Dropping the scheduled request before the grant cancels
  the pending job; dropping it while the inner send future runs releases
  the permit as `CancelledAfterGrant` (the lane is freed, the next lane
  job proceeds).
- **The scope follows the payload actually sent**: the payload is publicly
  mutable until `send` (teloxide's `send_ref` flow changes `chat_id` before
  sending), so adaptor-created requests recompute the scope from the final
  payload at send time via a per-payload scope function. Class, priority
  and weight are policy fixed at construction; admission, rate windows and
  `RetryAfter` penalties always follow the chat that actually receives
  the request. Ordering lanes are an explicitly assigned policy
  (`ScheduledRequest::on_lane`) and do not move with the payload.
  Channel usernames are not collapsed into the global scope: `@name` is
  stored **as text** in its own `OutboundChatKey::Username` (canonical
  lower-case form without the leading `@`), so a `RetryAfter` from one
  channel never blocks unrelated channels or numeric chats, and two
  usernames can never collide into one scheduling identity. A username
  and the numeric id of the same chat are intentionally separate
  identities (spec §11.2).
- **Completion barrier**: the adaptor completes the permit with
  `OutboundPermit::complete_and_await`, which resolves only after the actor
  applied the outcome. A `RetryAfter` penalty is therefore registered
  before the scheduled future returns, and a subsequent acquire from the
  same caller cannot overtake it (see the actor note below).
- **`OutboundRequestError<E>`** is the generic error of a scheduled
  request: `Acquire(OutboundAcquireError)` reports queue-level failures
  (closed, full, superseded) where the inner request never ran, and
  `Inner(E)` returns the original inner error untouched. The adaptor is
  generic over `E: AsResponseParameters`, so custom requesters with their
  own error types (not only `RequestError`) compose with the queue.
- **`retry_after_scope`** is the single classifier that maps a
  `RetryAfter` outcome to a penalty scope, receiving the request context
  (metadata + error) instead of copying `metadata.scope` at the call site.
  The current policy is explicit and temporary: the penalty follows the
  request's own scope. Classification reads the `RetryAfter` duration
  through `AsResponseParameters::retry_after` (the same trait the base
  `Request` already requires from `Err`), so it works for any inner error
  type. A future global-flood flag or a chat-to-global promotion policy
  plugs in here without touching the execution path.
- The `class` module holds draft request classes (`READ`, `MESSAGE_SEND`,
  `MESSAGE_MUTATION`, `CHAT_ACTION`) that payload classification will refine
  in a later commit.

Actor note (Commit 2 refinement): causal ordering of completions is
guaranteed by two mechanisms instead of a channel bias:

1. **Bounded lifecycle drain**: before the fair `select!`, the actor drains
   up to `LIFECYCLE_DRAIN_BATCH` (64) already-arrived lifecycle commands.
   Both sends are synchronous, so a completion sent before a subsequent
   acquire from the same caller is in its channel first and is applied by
   the drain before the acquire is even considered.
2. **Per-request completion barrier**: the adaptor uses
   `OutboundPermit::complete_and_await`, which waits until the actor
   applied the outcome. The scheduled future does not resolve before the
   penalty is registered, so a caller that awaits the request result and
   then acquires again is ordered deterministically.

A strict `biased` select was rejected: the public handle exposes
`penalize`/`snapshot`/`limits` with no admission, so an absolute lifecycle
priority would let a busy lifecycle producer starve the enqueue ingress.
The bounded drain keeps the causal ordering for same-caller sequences
while the fair `select!` guarantees enqueue progress under any lifecycle
load.

## Out of scope (later commits)

Payload classification for the full method set, `OrderedStart` lanes
(Commit 2 is serial-only), `Bot::outbound`-style extension sugar,
`Throttle` and `Drafter` migration, durable outbox, observability hooks.
