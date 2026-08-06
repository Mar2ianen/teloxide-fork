# Outbound scheduler — design note (Commits 1–5)

Deterministic outbound scheduling model in `crates/teloxide-core/src/outbound/`.

- **Commit 1**: the pure state machine (`SchedulerState`) — no Tokio, no
  actor. Time is passed as a parameter (`now: Instant`).
- **Commit 2**: the Tokio actor (`OutboundActor`), the clone-friendly
  `OutboundQueue`/`OutboundQueueHandle`, `OutboundAcquire` (a future with
  cancellation) and the completion-aware `OutboundPermit`.
- **Commit 3**: the `Outbound<R>` requester adaptor and `ScheduledRequest<R>`
  — the vertical slice that wires real typed requests through the queue
  without any automatic retry (see "Requester adaptor (Commit 3)" below).
- **Commit 4**: full payload classification. Every Bot API payload
  implements `OutboundPayload` (generated from the schema, strictly
  classified), and every `Requester` method of `Outbound<R>` returns a
  `ScheduledRequest` (see "Payload classification (Commit 4)" below).
  Public API: `OutboundMetadata`, `OutboundPriority`, `OutboundScope`,
  `OutboundCompletion`, `OutboundLimits`, `OutboundSettings` (with
  `OutboundSettings::default()`), `AgingPolicy`, `OutboundQueueError`,
  `OutboundAcquireError` (now `Display` + `std::error::Error`),
  `OutboundSnapshot`, `SchedulerConfigError`, `Outbound`, `ScheduledRequest`,
  `OutboundRequestError<E>`, `outbound::class`. Draft quality: naming and
  shape are expected to be refined during the architectural review of each
  commit. `OutboundScope`/`OutboundChatKey`/`OutboundMetadata` are
  intentionally not `Copy` (the chat key stores the username as text).
- **Commit 5**: the `Throttle` compatibility layer (`ThrottleCompat`)
  over the scheduler with parity tests against the legacy worker, plus
  the chat-kind window limits extension (`WindowLimit::kind`,
  `WindowChatKind`). The legacy `Throttle` worker is kept for the
  head-to-head comparison (see "Throttle compatibility layer (Commit 5)"
  below).

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
values for the full method set (Commit 4 generates the `Requester` impl).
A
`ScheduledRequest<Req>` is itself a `Request`: it holds the inner request,
the queue, the lane and the request-level `OutboundOverrides`
(priority/weight/class), and its `Send` future runs the vertical slice:

```text
compute the effective hint from the final payload + overrides (adaptor
requests: scope/class/priority/weight, batch weights from the current
batch length)
  ->  acquire permit (lane or queue handle)
  ->  ONLY NOW create and poll the inner request (send()/send_ref())
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
- **The classification follows the payload actually sent**: the payload
  is publicly mutable until `send` (teloxide's `send_ref` flow changes
  `chat_id` before sending), so the effective hint (scope, class,
  priority, weight) is recomputed from the FINAL payload at send time;
  batch weights depend on the current batch length. Request-level
  overrides (`ScheduledRequest::priority/weight/class`) are fixed on the
  request and applied on top of the hint at send time; the scope is not
  overridable. Admission, rate windows and `RetryAfter` penalties always
  follow the chat that actually receives the request. Ordering lanes are
  an explicitly assigned policy (`ScheduledRequest::on_lane`) and do not
  move with the payload. Channel usernames are not collapsed into the
  global scope: `OutboundChatKey` has a closed representation (public
  constructors `id`/`username` only, the username constructor
  canonicalizes), so a `RetryAfter` from one channel never blocks
  unrelated channels or numeric chats, and two usernames can never
  collide into one scheduling identity. A username and the numeric id of
  the same chat are intentionally separate identities (spec §11.2).
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
  `MESSAGE_MUTATION`, `CHAT_ACTION`, `OTHER`); the taxonomy will be
  refined during the `Throttle` migration.

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

## Payload classification (Commit 4)

Every Bot API payload now implements the `OutboundPayload` trait (in
`crates/teloxide-core/src/payloads/*.rs`, generated by `codegen_payloads`):

```rust
pub trait OutboundPayload {
    fn outbound_hint(&self) -> OutboundHint; // scope, class, priority, weight
}
```

- The hint is computed from the **final payload at send time**: the payload
  is publicly mutable until `send`, so admission, lanes and `RetryAfter`
  penalties always follow what is actually sent. The manual per-payload
  `scope_fn` machinery of Commit 3 is gone entirely — there is exactly one
  classification path.
- `ScheduledRequest::metadata()` reclassifies the current payload on
  demand, so it never goes stale after setter calls or mutations.
- **The classification table is strict.** `method_policy` in
  `payloads/codegen.rs` lists every method explicitly (scope policy,
  class, priority). A method missing from the table fails code
  generation with a loud panic — there is no silent
  `Global + Normal + weight 1` fallback, because an unclassified `send*`
  that bypasses per-chat windows is worse than a broken build.
- Scope policies: `RecipientField("chat_id")` (numeric id or canonical
  textual username), `ChatIdField("chat_id")` (numeric chats),
  `OptionalChatIdField` (menu buttons: `None` falls back to global),
  `UserIdField("user_id")` (per-user identity), `Custom(...)` for the
  hand-written classifiers taking `&field`
  (`draft_chat_id_scope` for draft payloads, `target_message_scope` for
  `get_game_high_scores`: `Common` targets address the chat, `Inline`
  falls back to global) and `CustomValue(...)` for `Copy` fields taken by
  value (`game_chat_id_scope` for `set_game_score.chat_id: u32`).
  `set_game_score_inline` and `repost_story` are global: the former has
  no chat identity, the latter acts on behalf of a business account and
  must not penalize the story's source chat. Usernames stay textual and
  canonicalized (lower-case, single optional `@`); `OutboundChatKey` has
  a closed representation, so the canonical form cannot be bypassed.
- **Full `Requester` coverage**: `impl Requester for Outbound<B>` (via
  `requester_forward!`) returns `ScheduledRequest<B::$T>` for all 195
  methods; the `where` block requires each `B::$T: Clone + Send + 'static`
  with `Payload: Payload<Output: Send> + OutboundPayload`. `Clone` is
  needed because `send_ref` defers the inner `Request::send_ref` CALL
  (not just the polling) until after the grant: `Request::send_ref` is
  only *recommended* to be lazy, so any side effect it has (opening a
  resource, capturing a deadline, ...) must happen after admission. The
  inner error only needs `Error + Send + AsResponseParameters` (no
  `'static` on the error wrapper), though the request types themselves
  are `'static`. The generated impl doubles as a compile-time
  completeness test for the whole trait.
- **Per-request overrides**: `ScheduledRequest::priority/weight/class`
  (or `with_outbound_overrides`) apply on top of the payload
  classification; scope is deliberately not overridable. This is what
  gives the Drafter migration its preview-vs-final priorities on the
  same payload type. Overridden weight is validated against the windows
  like any other weight (`WeightExceedsWindow` on acquire).
- **Weights**: `method_policy` rows carry a `WeightPolicy` — `One` for
  single requests, `Len("field")` for batch methods that send N messages
  per call (`send_media_group.media`, `forward_messages.message_ids`,
  `copy_messages.message_ids`), so per-window budgets measure message
  traffic, not call count.
- The `class` taxonomy is still draft (`READ`, `MESSAGE_SEND`,
  `MESSAGE_MUTATION`, `CHAT_ACTION`, `OTHER`); it will be refined when
  `Throttle` migrates. `send_chat_action` is the only `BACKGROUND`
  priority today.
- **Class filtering is solved at the COMPATIBILITY layer, not in the
  scheduler**: the `ThrottleCompat` allowlist (see below) routes exactly
  the legacy throttled methods through the queue; everything else
  (reads, admin calls, `send_chat_action`, ...) calls the inner bot
  directly and never touches the windows. The scheduler itself still has
  no class-aware windows (a raw `Outbound` adaptor accounts every
  chat-scoped request against the chat windows); that stays a separate
  decision for the `Outbound` users.
- **Batch weights**: the scheduler keeps the generated len-based weights
  (raw `Outbound` semantics); the compatibility layer forces weight 1 on
  every throttled request, preserving the legacy "one API call = one
  message" accounting.
- **Not** in `OutboundPayload` by design: `ReplacePending`/`user_key`
  (latest-wins slots are chosen by the calling layer), serial lanes
  (`on_lane` stays an explicit choice), retry policy and correlation ids.
  The payload classifies only what is actually being sent.

## Throttle compatibility layer (Commit 5)

`ThrottleCompat<B>` (`crates/teloxide-core/src/adaptors/throttle_compat/`)
reimplements the legacy `Throttle` contract on top of the outbound
scheduler, keeping the legacy worker untouched for head-to-head parity
testing. The public `Throttle` is NOT switched yet (that happens in a
later commit, together with the legacy worker removal).

Reproduced legacy semantics:

- **Allowlist**: the throttled method list matches the legacy
  `requester_impl` exactly (25 message-send methods). It is an explicit
  compatibility predicate, NOT derived from the `class` taxonomy —
  `copy_message(s)`/`forward_message(s)` are `OTHER`, so a
  `queue only MESSAGE_SEND` predicate would have silently let them
  bypass the old limits. Everything else passes through directly.
- **Limits mapping**: `messages_per_sec_overall` and
  `messages_per_sec_chat` become 1-second windows; the per-minute limit
  is split by chat kind (`WindowChatKind`) into
  `messages_per_min_chat` (users/groups) and
  `messages_per_min_channel_or_supergroup` (channels, `-100…` ids and
  usernames), reproducing the legacy distinction exactly. This required
  the small scheduler extension of Commit 5: `WindowLimit` gains a
  `kind` field and per-chat window sets are filtered by the chat kind at
  creation; global windows must be `Any` (validated).
- **Weight 1**: every throttled request overrides the payload weight to
  1 (`ScheduledRequest::weight`), so a media group of ten items costs
  one unit, like the legacy worker. Without this, the generated batch
  weights would make batch requests permanently inadmissible against
  `messages_per_sec_chat = 1`.
- **Per-chat FIFO**: all throttled requests share one priority, so the
  scheduler's (priority, sequence) arbitration plus the shared per-chat
  windows reproduce the legacy "request order in chats is not changed";
  cross-chat interleaving also matches (a blocked chat head is skipped,
  a free chat proceeds).
- **Global `RetryAfter` freeze**: a `RetryAfter` outcome completes the
  permit with `OutboundCompletion::RetryAfter { scope: Global, .. }`
  (the penalty scope is written directly by the compatibility layer —
  the adaptor's own `ScheduledRequest` keeps the default policy, which
  follows the request scope), because the legacy worker freezes the
  whole bot. With `Settings::retry` the request SLEEPS until the
  penalty expires (outside the queue — it holds no pending slot) and
  only THEN re-queues, exactly like the legacy re-send-after-freeze:
  requests that arrived during the freeze keep their place ahead of the
  retry. The outcome is classified EXACTLY ONCE
  (`AsResponseParameters::retry_after` is not required to be pure — a
  stateful error must not be re-examined by the completion or the
  retry decision). The penalty deadline is anchored at the moment the
  error was OBSERVED — ONE shared timestamp drives the scheduler
  penalty (`OutboundPermit::complete_observed_at`), the local retry
  sleep and the compat-side freeze deadline, so a completion processed
  late by the actor cannot extend a freeze whose deadline already
  passed — the legacy worker receives the absolute `until` computed at
  the error site. Without retries the error is returned.
- **`on_queue_full` + FIFO admission**: the backlog is bounded by
  `messages_per_sec_overall` (the legacy channel capacity) through a
  capacity semaphore (`tokio::sync::Semaphore`, one permit per slot). A
  request acquires its slot through a SINGLE `acquire_owned()` future —
  registering in the semaphore's FIFO waitlist is atomic, so a permit
  released between a failed `try_acquire` and the waitlist registration
  can never be taken by a newer request. The callback fires at most
  once per 4 seconds (passing the bound), both when the LAST slot is
  taken (the legacy worker fires when its queue REACHES the capacity,
  on the N-th request) and when a request has to wait. The moment
  matters: the legacy worker checks `queue.len() == capacity()` BEFORE
  applying the rate limits and granting anything, so the compatibility
  layer fires the callback on the ENQUEUE ACCEPTANCE of the last slot —
  the instant the actor put the job into the scheduler backlog — and
  not on its grant. The acquire is therefore TWO-PHASE for the compat
  path: `OutboundQueueHandle::enqueue` resolves when the actor accepted
  the job, and the returned `OutboundGrant` awaits the actual permit.
  A last pending request cancelled before its grant cannot erase the
  full-backlog event that already happened, and a request granted at
  t=0 reports the backlog of t=0. A slot freed by a grant, a
  cancellation or the actor's death automatically wakes the next
  waiter. The permit is held while the job is pending and released on
  grant — exactly like the legacy worker popping a request from its
  channel before sending it. The semaphore removes the entire class of
  hand-rolled gate races (lost wakeups, dead waiters blocking the
  queue, waiters not woken by the actor's death): admission turns are
  handed out by tokio's waitlist, cancellation-safe and in order. The
  notification never fires for a request going straight into a direct
  send (the actor died before the acceptance): the queue no longer
  exists. While a global `RetryAfter` freeze is active the callback is
  silent as well — exactly like the legacy worker, which does not run
  its queue checks while frozen; `CompatState` tracks the freeze
  deadline (`observed_at + duration`, max semantics) and the saturation
  monitor sleeps past it before re-checking. A full-backlog event that
  happened during the freeze is DEFERRED and emitted exactly once at
  the thaw boundary: the legacy worker still reads the messages out of
  its bounded channel after the thaw and reports `queue.len() ==
  capacity()` even if every pending request was cancelled before it.
  The deferred event is cleared when the actor dies (a dead legacy
  worker would never run the callback again); the monitor probes the
  actor's liveness before emitting. The monitor keeps the
  notifications going while the backlog stays full: the legacy worker
  re-checks `queue.len() == capacity()` on every iteration and re-fires
  once the 4-second rate limit expired, so a backlog that stays full
  for a long time produces several notifications even without new
  requests — the monitor re-fires at the same boundary and exits when a
  slot frees up, resetting its `monitor_active` flag BEFORE releasing
  the permit it observed, so a waiter that immediately re-takes the
  last slot spawns a fresh monitor instead of losing the notifications.
  `QueueFull` from the scheduler is an invariant breach for the
  compatibility layer (the semaphore permits equal the queue's own
  backlog bound); it can only be observed in a burst whose cancels the
  actor has not processed yet. The rejected request KEEPS its slot and
  retries after a yield until the actor drains the cancel — dropping
  the slot would hand it to the next waiter and invert the FIFO order.
  The direct-send fallback on the actor's death releases the slot
  BEFORE the direct request runs, so a slow or hanging direct send
  cannot hold the parked waiters. The cancellation identity is a CLIENT
  TOKEN minted before the enqueue is sent: dropping the enqueue/grant
  future sends `Cancel { token }`, and the actor applies it whether the
  enqueue was already processed (the token is mapped to the job) or
  still in flight (the cancel is remembered and applied on acceptance)
  — a future dropped between the actor's acceptance reply and the
  caller's observation of it can never leave a ghost job pending in the
  scheduler.
  The completion is NON-BLOCKING (`OutboundPermit::complete`, not the
  adaptor's per-request `complete_and_await` barrier): the legacy
  request loop returns its result right after the inner request
  finished (the worker is only told about a `RetryAfter` freeze, and
  even that without waiting for it to be applied), so a granted
  request must not stall on an actor ack. Ordering is preserved
  anyway: the completion lands synchronously in the actor's lifecycle
  channel, and the actor drains lifecycle commands (applying the
  penalty) before it considers any later enqueue from the same
  caller.
- **Inner execution path + shared/owned semantics**: the legacy worker
  picks the inner request path by an exact table — `retry = true`
  (default) always uses inner `send_ref()`, an outer `send_ref()` uses
  inner `send_ref()`, and only an owned `send()` with retries disabled
  uses inner `send()`. The compatibility layer reproduces it on the
  legacy storage model: `CompatRequest` holds `Arc<R>`, so cloning the
  WRAPPER makes an owned `send()` shared — `send()` performs
  `Arc::try_unwrap` and falls back to inner `send_ref()` whenever
  another wrapper clone exists, even with `retry = false`. `send_ref()`
  clones only the `Arc`, never `R` (a custom `R::clone` with side
  effects is not invoked), and `payload_mut()` goes through
  `Arc::make_mut`. A TRULY owned execution runs the inner request
  through `IntoFuture::into_future` (`owned.take().unwrap().await`),
  NOT through `Request::send` — in the regular `retry = false` path and
  in the direct-send fallback alike, because the legacy worker only
  ever calls `.await` on the taken request and a custom requester may
  distinguish the two.
- **`limits()`/`set_limits()`**: the legacy async API is preserved; the
  scheduler's `set_limits` carries the debited history over, so changing
  limits never resets the rate budget. The queue actor is the single
  source of truth: `limits()` reads the scheduler through the handle and
  maps the windows back to the legacy `Limits`, and `set_limits` only
  forwards the update. There is deliberately no client-side mirror, so
  concurrent or cancelled updates cannot desync the two views. Like the
  legacy worker, `limits()` PANICS when the actor is gone (a silent
  default could hand the caller a completely wrong state). Invalid new
  limits (zero capacity) are rejected and logged; the previous limits
  stay in effect.

- **`Debug`**: `ThrottleCompat<B>` keeps the legacy `Debug` contract
  (the legacy type derives it); the callback closure is not printable
  and is skipped by the manual impl.

Documented temporary incompatibilities:

- `Settings::check_slow_mode` is accepted but ignored (the legacy worker
  asks `get_chat` whether slow mode explains a freeze);
- channel usernames are canonicalized (the legacy hashed the raw
  spelling, so `@Foo` and `foo` were different identities);
- zero-capacity limits: the legacy worker accepts `messages_per_min_chat
  = 0` (requests to such chats wait forever) and `set_limits` with
  zeroes pauses the traffic; the scheduler rejects zero-capacity windows
  at construction and in `set_limits`, so the compatibility layer
  rejects and logs such updates while the previous limits stay in
  effect. This must be resolved before the public `Throttle` switches to
  this layer.

The legacy worker was switched from `std::time::Instant` to
`tokio::time::Instant` (production-identical: tokio's Instant is the
same clock unless the runtime is paused) so that parity tests can drive
both engines on the same paused clock.

Parity tests (`throttle_compat/tests.rs`) run identical scenarios
through the legacy worker and `ThrottleCompat` on paused time and
compare the grant order: per-chat second limits, interleaved chats,
global second limits, the full-backlog drain, the RetryAfter freeze
(B1 granted before the freeze, A2 after it, the retried A1 last), a
request arriving DURING a freeze preceding the retried request (the
retry sleeps outside the queue), a saturated backlog with a REVERSED
poll order (the FIFO admission must hold regardless of the ingress
tie-break), the inner `send()`/`send_ref()` path table, `on_queue_full`
being SILENT during a freeze on both engines (0 callbacks before the
thaw, >= 1 after it) and REPEATING at >= 4s intervals while the backlog
stays full on both engines. The compat-only tests additionally pin
exact timings, the weight-1 batch accounting, the passthrough bypass,
`set_limits` (actor round-trip, zero-capacity rejection), the per-kind
minute windows and the capacity-semaphore lifecycle regressions: FIFO
among released waiters, a cancelled waiter not blocking the queue, a
cancelled pending request waking a parked waiter, a slot freed before
registration not being lost, the actor's death waking every waiter into
a direct send with the slot released BEFORE the direct request runs
(verified with hanging direct sends), a `QueueFull` behind an
unprocessed cancel preserving the waiter FIFO (the race is a coin flip
inside the actor, so the scenario is replayed), `on_queue_full` firing
when the backlog REACHES the capacity without an overflow request, the
actor's death NOT firing `on_queue_full` (the notification is deferred
until a real acquire succeeds), `on_queue_full` firing at the ENQUEUE
ACCEPTANCE of the last slot — before its grant, and surviving the
cancellation of that last pending request, a full backlog reached
during a freeze being reported exactly once after the thaw even when
every pending request was cancelled before it (parity with the legacy
worker reading the messages out of its bounded channel), dropping the
enqueue future after the actor's acceptance but before observing the
reply leaving NO ghost job in the scheduler (the token cancel), the
saturation monitor
respawning after the backlog re-fills (a monitor that observed a freed
slot must not leave the next saturation wave silent), a late-processed
completion not extending the freeze (parity with the legacy's absolute
`until`), a stateful `retry_after()` being classified exactly once, the
shared/owned `Arc` semantics (a cloned wrapper with `retry = false`
still sends through inner `send_ref()`, an outer `send_ref()` never
clones the inner request, and a truly owned execution resolves through
`IntoFuture` instead of `Request::send` — in the regular path and in
the direct-send fallback), a granted request completing without an
additional actor poll (the completion is fire-and-forget), the `Debug`
contract, a cancelled `set_limits` future not staling `limits()`, and
`limits()` panicking when the actor is dead.

## Out of scope (later commits)

`OrderedStart` lanes (Commit 2 is serial-only), `Bot::outbound`-style
extension sugar, class-aware window sets for the raw `Outbound` adaptor,
the public `Throttle` switch-over and legacy worker removal, `Drafter`
migration, durable outbox, observability hooks.
