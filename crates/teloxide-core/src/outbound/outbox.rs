//! Durable, application-defined outbox runtime for outbound work.
//!
//! The outbox stores opaque application payloads and frozen scheduler metadata;
//! it deliberately does not attempt to serialize arbitrary
//! [`crate::outbound::Outbound`] requests or multipart bodies. An application
//! supplies an [`OutboxExecutor`] that knows how to decode the payload and
//! perform the side effect.
//!
//! Delivery is **at least once**. A process crash after the remote API accepts
//! a request but before [`OutboxStore::complete`] commits can cause a replay.
//! The idempotency key is exposed to the executor so applications can make the
//! remote operation idempotent where the target API supports it.

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
};

use super::{
    OutboundAcquireError, OutboundCompletion, OutboundCorrelationId, OutboundLane,
    OutboundMetadata, OutboundQueue, OutboundScope,
};

/// Stable identity of an outbox record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxId(u64);

impl OutboxId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Identity of a worker lease owner.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxWorkerId(u64);

impl OutboxWorkerId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Fencing token for a claimed record.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OutboxLease {
    pub worker: OutboxWorkerId,
    pub token: u64,
    pub until: SystemTime,
}

/// Safe, bounded failure categories persisted by an outbox store.
///
/// The runtime intentionally persists categories rather than arbitrary
/// provider error text, which may contain response bodies or other sensitive
/// data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxFailureKind {
    RetryAfter,
    Transient,
    Permanent,
    SchedulerUnavailable,
    Exhausted,
}

/// Terminal or retryable failure information safe to persist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboxFailure {
    pub kind: OutboxFailureKind,
}

impl OutboxFailure {
    pub const fn new(kind: OutboxFailureKind) -> Self {
        Self { kind }
    }
}

/// Current durable state of an outbox record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxStatus {
    Pending,
    Claimed,
    Completed,
    Failed(OutboxFailureKind),
}

/// Application payload and immutable scheduling metadata submitted to a
/// durable store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewOutboxRequest {
    pub idempotency_key: String,
    pub payload: Vec<u8>,
    pub payload_version: u32,
    pub metadata: OutboundMetadata,
    pub available_at: SystemTime,
    pub max_attempts: NonZeroU32,
}

impl NewOutboxRequest {
    /// Creates a request available immediately with payload version `1` and
    /// five attempts. Applications should use a stable, domain-specific key.
    pub fn new(
        idempotency_key: impl Into<String>,
        payload: Vec<u8>,
        metadata: OutboundMetadata,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            payload,
            payload_version: 1,
            metadata,
            available_at: SystemTime::now(),
            max_attempts: NonZeroU32::new(5).expect("five is non-zero"),
        }
    }

    pub fn with_payload_version(mut self, payload_version: u32) -> Self {
        self.payload_version = payload_version;
        self
    }

    pub fn with_available_at(mut self, available_at: SystemTime) -> Self {
        self.available_at = available_at;
        self
    }

    pub fn with_max_attempts(mut self, max_attempts: NonZeroU32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

/// Result of durable enqueue and idempotency deduplication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxEnqueueResult {
    Inserted(OutboxId),
    Existing(OutboxId),
}

/// A record claimed by a worker and fenced by [`ClaimedOutboxRequest::lease`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedOutboxRequest {
    pub id: OutboxId,
    pub idempotency_key: String,
    pub payload: Vec<u8>,
    pub payload_version: u32,
    pub metadata: OutboundMetadata,
    pub available_at: SystemTime,
    /// Number of delivery attempts that have actually started.
    pub attempt: u32,
    pub claim_count: u32,
    pub max_attempts: NonZeroU32,
    pub lease: OutboxLease,
}

/// A safe point-in-time view useful for metrics, diagnostics and tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxSnapshot {
    pub id: OutboxId,
    pub idempotency_key: String,
    pub status: OutboxStatus,
    /// Number of delivery attempts that have actually started.
    pub attempt: u32,
    pub claim_count: u32,
    pub max_attempts: NonZeroU32,
    pub available_at: SystemTime,
    pub payload_version: u32,
    pub metadata: OutboundMetadata,
    pub lease: Option<OutboxLease>,
    pub last_failure: Option<OutboxFailureKind>,
}

/// A store mutation used after a failed execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboxRetry {
    pub available_at: SystemTime,
    pub failure: OutboxFailure,
}

/// Result of atomically starting a delivery attempt under a valid lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxAttemptStart {
    Started { attempt: u32 },
    Exhausted,
}

/// Store abstraction for durable outbox records.
///
/// Implementations should make lease-token checks atomic with their status
/// mutation. A PostgreSQL implementation can map `claim_due` to
/// `FOR UPDATE SKIP LOCKED` and the lease-aware mutations to a token/owner
/// predicate.
pub trait OutboxStore: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    fn enqueue(
        &self,
        request: NewOutboxRequest,
    ) -> BoxFuture<'_, Result<OutboxEnqueueResult, Self::Error>>;

    fn claim_due(
        &self,
        worker: OutboxWorkerId,
        now: SystemTime,
        lease_for: Duration,
        limit: NonZeroUsize,
    ) -> BoxFuture<'_, Result<Vec<ClaimedOutboxRequest>, Self::Error>>;

    /// Extends a still-valid lease. The owner and fencing token stay stable;
    /// the returned lease carries the new expiry.
    fn renew_lease(
        &self,
        id: OutboxId,
        lease: OutboxLease,
        until: SystemTime,
    ) -> BoxFuture<'_, Result<OutboxLease, Self::Error>>;

    /// Increments the delivery-attempt counter immediately before the
    /// executor is called. Claim retries never increment this counter.
    fn begin_attempt(
        &self,
        id: OutboxId,
        lease: OutboxLease,
    ) -> BoxFuture<'_, Result<OutboxAttemptStart, Self::Error>>;

    fn complete(
        &self,
        id: OutboxId,
        lease: OutboxLease,
        completed_at: SystemTime,
    ) -> BoxFuture<'_, Result<(), Self::Error>>;

    fn reschedule(
        &self,
        id: OutboxId,
        lease: OutboxLease,
        retry: OutboxRetry,
    ) -> BoxFuture<'_, Result<(), Self::Error>>;

    fn fail(
        &self,
        id: OutboxId,
        lease: OutboxLease,
        failure: OutboxFailure,
    ) -> BoxFuture<'_, Result<(), Self::Error>>;
}

/// Error returned by an application-defined executor.
///
/// The executor converts provider-specific errors to these safe categories;
/// raw provider errors are not persisted by the runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxExecutionError {
    RetryAfter { scope: OutboundScope, duration: Duration },
    Transient,
    Permanent,
}

/// Performs the application-specific side effect for one durable record.
pub trait OutboxExecutor: Send + Sync + 'static {
    fn execute(
        &self,
        request: ClaimedOutboxRequest,
    ) -> BoxFuture<'_, Result<(), OutboxExecutionError>>;
}

/// Runtime policy for a bounded outbox worker.
#[derive(Clone, Debug)]
pub struct OutboxWorkerSettings {
    pub worker: OutboxWorkerId,
    pub max_in_flight: NonZeroUsize,
    pub lease_for: Duration,
    pub poll_interval: Duration,
    pub scheduler_retry_delay: Duration,
    pub transient_backoff_base: Duration,
    pub transient_backoff_max: Duration,
    /// Optional runtime ordering lane. The lane is not persisted; it is a
    /// worker-local execution policy layered over durable metadata.
    pub lane: Option<OutboundLane>,
}

impl Default for OutboxWorkerSettings {
    fn default() -> Self {
        Self {
            worker: OutboxWorkerId::new(1),
            max_in_flight: NonZeroUsize::new(8).expect("eight is non-zero"),
            lease_for: Duration::from_secs(60),
            poll_interval: Duration::from_secs(1),
            scheduler_retry_delay: Duration::from_secs(1),
            transient_backoff_base: Duration::from_secs(1),
            transient_backoff_max: Duration::from_secs(60),
            lane: None,
        }
    }
}

/// Error returned by the worker. Store errors are passed through unchanged;
/// queue admission failures are rescheduled and therefore do not make the
/// worker lose the durable record.
#[derive(Debug)]
pub enum OutboxWorkerError<E> {
    Store(E),
    InvalidLeaseDuration,
}

impl<E: fmt::Display> fmt::Display for OutboxWorkerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(f, "outbox store operation failed: {error}"),
            Self::InvalidLeaseDuration => f.write_str("outbox lease_for must be positive"),
        }
    }
}

impl<E: Error + 'static> Error for OutboxWorkerError<E> {}

/// Drives one operation while renewing its lease at a bounded interval.
struct LeaseHeartbeat<S: OutboxStore> {
    store: Arc<S>,
    id: OutboxId,
    lease: OutboxLease,
    lease_for: Duration,
    renew_period: Duration,
    timer: Pin<Box<tokio::time::Sleep>>,
}

impl<S: OutboxStore> LeaseHeartbeat<S> {
    fn new(store: Arc<S>, id: OutboxId, lease: OutboxLease, lease_for: Duration) -> Self {
        let renew_period = (lease_for / 3).max(Duration::from_millis(1));
        Self {
            store,
            id,
            lease,
            lease_for,
            renew_period,
            timer: Box::pin(tokio::time::sleep(renew_period)),
        }
    }

    fn lease(&self) -> OutboxLease {
        self.lease
    }

    async fn drive<F, T>(&mut self, future: F) -> Result<T, OutboxWorkerError<S::Error>>
    where
        F: Future<Output = T>,
    {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => return Ok(result),
                _ = &mut self.timer => {
                    let until = SystemTime::now()
                        .checked_add(self.lease_for)
                        .unwrap_or_else(SystemTime::now);
                    self.lease = self
                        .store
                        .renew_lease(self.id, self.lease, until)
                        .await
                        .map_err(OutboxWorkerError::Store)?;
                    self.timer.as_mut().reset(tokio::time::Instant::now() + self.renew_period);
                }
            }
        }
    }
}

/// Bounded durable outbox runtime.
///
/// `run_once` claims at most `max_in_flight` records and drives their futures
/// without spawning one task per record. `run_until` repeatedly polls the
/// store and can be stopped by any caller-owned future.
pub struct OutboundOutbox<S, E> {
    store: Arc<S>,
    executor: Arc<E>,
    queue: OutboundQueue,
    settings: OutboxWorkerSettings,
}

impl<S, E> OutboundOutbox<S, E>
where
    S: OutboxStore,
    E: OutboxExecutor,
{
    pub fn new(
        store: S,
        executor: E,
        queue: OutboundQueue,
        settings: OutboxWorkerSettings,
    ) -> Self {
        Self { store: Arc::new(store), executor: Arc::new(executor), queue, settings }
    }

    pub fn store(&self) -> &S {
        self.store.as_ref()
    }

    pub fn settings(&self) -> &OutboxWorkerSettings {
        &self.settings
    }

    pub async fn enqueue(
        &self,
        request: NewOutboxRequest,
    ) -> Result<OutboxEnqueueResult, OutboxWorkerError<S::Error>> {
        self.store.enqueue(request).await.map_err(OutboxWorkerError::Store)
    }

    /// Claims and processes one bounded batch. The returned count includes
    /// records whose execution was rescheduled or terminally failed.
    pub async fn run_once(&self) -> Result<usize, OutboxWorkerError<S::Error>> {
        if self.settings.lease_for.is_zero() {
            return Err(OutboxWorkerError::InvalidLeaseDuration);
        }
        let claimed = self
            .store
            .claim_due(
                self.settings.worker,
                SystemTime::now(),
                self.settings.lease_for,
                self.settings.max_in_flight,
            )
            .await
            .map_err(OutboxWorkerError::Store)?;
        if claimed.is_empty() {
            return Ok(0);
        }

        let mut jobs = FuturesUnordered::new();
        for request in claimed {
            jobs.push(self.execute_claimed(request));
        }
        let mut processed = 0;
        let mut first_error = None;
        while let Some(result) = jobs.next().await {
            processed += 1;
            if let Err(error) = result {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(processed)
        }
    }

    /// Runs until `shutdown` resolves. Shutdown stops new claims but lets
    /// the currently running batch finish, including its lease-protected
    /// executor and durable terminal transitions.
    pub async fn run_until<F>(self, shutdown: F) -> Result<(), OutboxWorkerError<S::Error>>
    where
        F: Future<Output = ()> + Send,
    {
        let mut shutdown = Box::pin(shutdown);
        let mut first_poll = true;
        loop {
            if first_poll {
                first_poll = false;
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok(()),
                    _ = tokio::task::yield_now() => {}
                }
            } else {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok(()),
                    _ = tokio::time::sleep(self.settings.poll_interval) => {}
                }
            }
            self.run_once().await?;
        }
    }

    async fn execute_claimed(
        &self,
        mut request: ClaimedOutboxRequest,
    ) -> Result<(), OutboxWorkerError<S::Error>> {
        let id = request.id;
        let attempt_before_start = request.attempt;
        let max_attempts = request.max_attempts.get();
        let store = self.store.clone();
        let executor = self.executor.clone();
        let mut heartbeat =
            LeaseHeartbeat::new(store.clone(), id, request.lease, self.settings.lease_for);
        let correlation_id = OutboundCorrelationId::new(id.get());

        let acquire = match &self.settings.lane {
            Some(lane) => lane.acquire_with_correlation(request.metadata.clone(), correlation_id),
            None => self
                .queue
                .handle()
                .acquire_with_correlation(request.metadata.clone(), correlation_id),
        };
        let acquire = heartbeat.drive(acquire).await?;
        let mut permit = match acquire {
            Ok(permit) => permit,
            Err(OutboundAcquireError::QueueFull | OutboundAcquireError::Closed) => {
                return self
                    .retry_or_fail(
                        &mut heartbeat,
                        id,
                        attempt_before_start,
                        max_attempts,
                        OutboxFailure::new(OutboxFailureKind::SchedulerUnavailable),
                        self.settings.scheduler_retry_delay,
                    )
                    .await;
            }
            Err(
                OutboundAcquireError::WeightExceedsWindow { .. }
                | OutboundAcquireError::IncompatibleCoalesceMetadata,
            ) => {
                let lease = heartbeat.lease();
                heartbeat
                    .drive(store.fail(id, lease, OutboxFailure::new(OutboxFailureKind::Permanent)))
                    .await?
                    .map_err(OutboxWorkerError::Store)?;
                return Ok(());
            }
            Err(OutboundAcquireError::Superseded) => {
                return self
                    .retry_or_fail(
                        &mut heartbeat,
                        id,
                        attempt_before_start,
                        max_attempts,
                        OutboxFailure::new(OutboxFailureKind::SchedulerUnavailable),
                        self.settings.scheduler_retry_delay,
                    )
                    .await;
            }
        };

        let begin = heartbeat
            .drive(store.begin_attempt(id, heartbeat.lease()))
            .await?
            .map_err(OutboxWorkerError::Store)?;
        let attempt = match begin {
            OutboxAttemptStart::Started { attempt } => attempt,
            OutboxAttemptStart::Exhausted => {
                heartbeat.drive(permit.complete_and_await(OutboundCompletion::NoRequest)).await?;
                let lease = heartbeat.lease();
                heartbeat
                    .drive(store.fail(id, lease, OutboxFailure::new(OutboxFailureKind::Exhausted)))
                    .await?
                    .map_err(OutboxWorkerError::Store)?;
                return Ok(());
            }
        };

        request.attempt = attempt;
        request.lease = heartbeat.lease();
        // This is intentionally immediately adjacent to executor invocation:
        // the scheduler's OrderedStart lane is released only at this boundary.
        permit.start();
        let execution = heartbeat.drive(executor.execute(request)).await?;
        match execution {
            Ok(()) => {
                heartbeat.drive(permit.complete_and_await(OutboundCompletion::Success)).await?;
                let lease = heartbeat.lease();
                heartbeat
                    .drive(store.complete(id, lease, SystemTime::now()))
                    .await?
                    .map_err(OutboxWorkerError::Store)?;
                Ok(())
            }
            Err(OutboxExecutionError::RetryAfter { scope, duration }) => {
                heartbeat
                    .drive(
                        permit
                            .complete_and_await(OutboundCompletion::RetryAfter { scope, duration }),
                    )
                    .await?;
                self.retry_or_fail(
                    &mut heartbeat,
                    id,
                    attempt,
                    max_attempts,
                    OutboxFailure::new(OutboxFailureKind::RetryAfter),
                    duration,
                )
                .await
            }
            Err(OutboxExecutionError::Transient) => {
                heartbeat.drive(permit.complete_and_await(OutboundCompletion::Failed)).await?;
                self.retry_or_fail(
                    &mut heartbeat,
                    id,
                    attempt,
                    max_attempts,
                    OutboxFailure::new(OutboxFailureKind::Transient),
                    self.transient_backoff(attempt),
                )
                .await
            }
            Err(OutboxExecutionError::Permanent) => {
                heartbeat.drive(permit.complete_and_await(OutboundCompletion::Failed)).await?;
                let lease = heartbeat.lease();
                heartbeat
                    .drive(store.fail(id, lease, OutboxFailure::new(OutboxFailureKind::Permanent)))
                    .await?
                    .map_err(OutboxWorkerError::Store)?;
                Ok(())
            }
        }
    }

    async fn retry_or_fail(
        &self,
        heartbeat: &mut LeaseHeartbeat<S>,
        id: OutboxId,
        attempt: u32,
        max_attempts: u32,
        failure: OutboxFailure,
        delay: Duration,
    ) -> Result<(), OutboxWorkerError<S::Error>> {
        let lease = heartbeat.lease();
        if attempt >= max_attempts {
            heartbeat
                .drive(self.store.fail(id, lease, OutboxFailure::new(OutboxFailureKind::Exhausted)))
                .await?
                .map_err(OutboxWorkerError::Store)?;
            return Ok(());
        }
        let available_at = SystemTime::now().checked_add(delay).unwrap_or_else(SystemTime::now);
        heartbeat
            .drive(self.store.reschedule(id, lease, OutboxRetry { available_at, failure }))
            .await?
            .map_err(OutboxWorkerError::Store)?;
        Ok(())
    }

    fn transient_backoff(&self, attempt: u32) -> Duration {
        let mut delay = self.settings.transient_backoff_base;
        for _ in 1..attempt {
            delay = delay.checked_mul(2).unwrap_or(self.settings.transient_backoff_max);
            if delay >= self.settings.transient_backoff_max {
                return self.settings.transient_backoff_max;
            }
        }
        delay.min(self.settings.transient_backoff_max)
    }
}

/// In-memory store for deterministic tests, examples and small applications.
/// Production applications should implement [`OutboxStore`] against their
/// durable database.
#[derive(Clone, Default)]
pub struct InMemoryOutboxStore {
    inner: Arc<Mutex<InMemoryState>>,
}

#[derive(Default)]
struct InMemoryState {
    next_id: u64,
    next_lease_token: u64,
    records: HashMap<OutboxId, InMemoryRecord>,
    by_key: HashMap<String, OutboxId>,
    fail_next_complete: bool,
}

struct InMemoryRecord {
    request: NewOutboxRequest,
    attempt: u32,
    claim_count: u32,
    status: InMemoryStatus,
    last_failure: Option<OutboxFailureKind>,
}

enum InMemoryStatus {
    Pending,
    Claimed(OutboxLease),
    Completed,
    Failed(OutboxFailureKind),
}

#[derive(Debug)]
pub enum InMemoryOutboxError {
    Conflict { id: OutboxId },
    NotFound { id: OutboxId },
    LeaseMismatch { id: OutboxId },
    Poisoned,
    InjectedFailure,
}

impl fmt::Display for InMemoryOutboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { id } => {
                write!(f, "idempotency key conflicts with outbox record {}", id.get())
            }
            Self::NotFound { id } => write!(f, "outbox record {} was not found", id.get()),
            Self::LeaseMismatch { id } => {
                write!(f, "outbox lease mismatch for record {}", id.get())
            }
            Self::Poisoned => f.write_str("in-memory outbox mutex is poisoned"),
            Self::InjectedFailure => f.write_str("injected in-memory outbox failure"),
        }
    }
}

impl Error for InMemoryOutboxError {}

impl InMemoryOutboxStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes the next `complete` fail, leaving the record claimed until its
    /// lease expires. This models a crash or database outage after the remote
    /// side effect succeeded and is useful for at-least-once tests.
    pub fn fail_next_complete(&self) -> Result<(), InMemoryOutboxError> {
        let mut state = self.inner.lock().map_err(|_| InMemoryOutboxError::Poisoned)?;
        state.fail_next_complete = true;
        Ok(())
    }

    pub fn get(&self, id: OutboxId) -> Result<Option<OutboxSnapshot>, InMemoryOutboxError> {
        let state = self.inner.lock().map_err(|_| InMemoryOutboxError::Poisoned)?;
        Ok(state.records.get(&id).map(|record| snapshot(id, record)))
    }

    pub fn pending_count(&self) -> Result<usize, InMemoryOutboxError> {
        let state = self.inner.lock().map_err(|_| InMemoryOutboxError::Poisoned)?;
        Ok(state
            .records
            .values()
            .filter(|record| matches!(record.status, InMemoryStatus::Pending))
            .count())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, InMemoryState>, InMemoryOutboxError> {
        self.inner.lock().map_err(|_| InMemoryOutboxError::Poisoned)
    }
}

fn snapshot(id: OutboxId, record: &InMemoryRecord) -> OutboxSnapshot {
    let status = match &record.status {
        InMemoryStatus::Pending => OutboxStatus::Pending,
        InMemoryStatus::Claimed(_) => OutboxStatus::Claimed,
        InMemoryStatus::Completed => OutboxStatus::Completed,
        InMemoryStatus::Failed(kind) => OutboxStatus::Failed(*kind),
    };
    OutboxSnapshot {
        id,
        idempotency_key: record.request.idempotency_key.clone(),
        status,
        attempt: record.attempt,
        claim_count: record.claim_count,
        max_attempts: record.request.max_attempts,
        available_at: record.request.available_at,
        payload_version: record.request.payload_version,
        metadata: record.request.metadata.clone(),
        lease: match &record.status {
            InMemoryStatus::Claimed(lease) => Some(*lease),
            _ => None,
        },
        last_failure: record.last_failure,
    }
}

fn same_request(left: &NewOutboxRequest, right: &NewOutboxRequest) -> bool {
    left.payload == right.payload
        && left.payload_version == right.payload_version
        && left.metadata == right.metadata
        && left.max_attempts == right.max_attempts
}

fn lease_is_current(status: &InMemoryStatus, lease: OutboxLease) -> bool {
    matches!(status, InMemoryStatus::Claimed(current) if *current == lease && current.until > SystemTime::now())
}

impl OutboxStore for InMemoryOutboxStore {
    type Error = InMemoryOutboxError;

    fn enqueue(
        &self,
        request: NewOutboxRequest,
    ) -> BoxFuture<'_, Result<OutboxEnqueueResult, Self::Error>> {
        Box::pin(async move {
            let mut state = self.lock()?;
            if let Some(&id) = state.by_key.get(&request.idempotency_key) {
                let record = state.records.get(&id).expect("idempotency index is consistent");
                if same_request(&record.request, &request) {
                    return Ok(OutboxEnqueueResult::Existing(id));
                }
                return Err(InMemoryOutboxError::Conflict { id });
            }
            let id = OutboxId(state.next_id);
            state.next_id += 1;
            state.by_key.insert(request.idempotency_key.clone(), id);
            state.records.insert(
                id,
                InMemoryRecord {
                    request,
                    attempt: 0,
                    claim_count: 0,
                    status: InMemoryStatus::Pending,
                    last_failure: None,
                },
            );
            Ok(OutboxEnqueueResult::Inserted(id))
        })
    }

    fn claim_due(
        &self,
        worker: OutboxWorkerId,
        now: SystemTime,
        lease_for: Duration,
        limit: NonZeroUsize,
    ) -> BoxFuture<'_, Result<Vec<ClaimedOutboxRequest>, Self::Error>> {
        Box::pin(async move {
            let mut state = self.lock()?;
            for record in state.records.values_mut() {
                if matches!(&record.status, InMemoryStatus::Claimed(lease) if lease.until <= now) {
                    record.status = InMemoryStatus::Pending;
                }
            }
            let mut ids: Vec<_> = state
                .records
                .iter()
                .filter_map(|(&id, record)| {
                    (matches!(&record.status, InMemoryStatus::Pending)
                        && record.request.available_at <= now)
                        .then_some(id)
                })
                .collect();
            ids.sort_by_key(|id| {
                let record = state.records.get(id).expect("record id exists");
                (record.request.available_at, *id)
            });

            let mut claimed = Vec::new();
            for id in ids {
                if claimed.len() == limit.get() {
                    break;
                }
                state.next_lease_token += 1;
                let lease_token = state.next_lease_token;
                let record = state.records.get_mut(&id).expect("record id exists");
                if record.attempt >= record.request.max_attempts.get() {
                    record.status = InMemoryStatus::Failed(OutboxFailureKind::Exhausted);
                    continue;
                }
                record.claim_count += 1;
                let lease = OutboxLease {
                    worker,
                    token: lease_token,
                    until: now.checked_add(lease_for).unwrap_or(now),
                };
                record.status = InMemoryStatus::Claimed(lease);
                claimed.push(ClaimedOutboxRequest {
                    id,
                    idempotency_key: record.request.idempotency_key.clone(),
                    payload: record.request.payload.clone(),
                    payload_version: record.request.payload_version,
                    metadata: record.request.metadata.clone(),
                    available_at: record.request.available_at,
                    attempt: record.attempt,
                    claim_count: record.claim_count,
                    max_attempts: record.request.max_attempts,
                    lease,
                });
            }
            Ok(claimed)
        })
    }

    fn renew_lease(
        &self,
        id: OutboxId,
        lease: OutboxLease,
        until: SystemTime,
    ) -> BoxFuture<'_, Result<OutboxLease, Self::Error>> {
        Box::pin(async move {
            let mut state = self.lock()?;
            let record = state.records.get_mut(&id).ok_or(InMemoryOutboxError::NotFound { id })?;
            if !lease_is_current(&record.status, lease) {
                return Err(InMemoryOutboxError::LeaseMismatch { id });
            }
            let renewed = OutboxLease { until, ..lease };
            record.status = InMemoryStatus::Claimed(renewed);
            Ok(renewed)
        })
    }

    fn begin_attempt(
        &self,
        id: OutboxId,
        lease: OutboxLease,
    ) -> BoxFuture<'_, Result<OutboxAttemptStart, Self::Error>> {
        Box::pin(async move {
            let mut state = self.lock()?;
            let record = state.records.get_mut(&id).ok_or(InMemoryOutboxError::NotFound { id })?;
            if !lease_is_current(&record.status, lease) {
                return Err(InMemoryOutboxError::LeaseMismatch { id });
            }
            if record.attempt >= record.request.max_attempts.get() {
                return Ok(OutboxAttemptStart::Exhausted);
            }
            record.attempt += 1;
            Ok(OutboxAttemptStart::Started { attempt: record.attempt })
        })
    }

    fn complete(
        &self,
        id: OutboxId,
        lease: OutboxLease,
        _completed_at: SystemTime,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut state = self.lock()?;
            if state.fail_next_complete {
                state.fail_next_complete = false;
                return Err(InMemoryOutboxError::InjectedFailure);
            }
            let record = state.records.get_mut(&id).ok_or(InMemoryOutboxError::NotFound { id })?;
            if !lease_is_current(&record.status, lease) {
                return Err(InMemoryOutboxError::LeaseMismatch { id });
            }
            record.status = InMemoryStatus::Completed;
            Ok(())
        })
    }

    fn reschedule(
        &self,
        id: OutboxId,
        lease: OutboxLease,
        retry: OutboxRetry,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut state = self.lock()?;
            let record = state.records.get_mut(&id).ok_or(InMemoryOutboxError::NotFound { id })?;
            if !lease_is_current(&record.status, lease) {
                return Err(InMemoryOutboxError::LeaseMismatch { id });
            }
            record.request.available_at = retry.available_at;
            record.last_failure = Some(retry.failure.kind);
            record.status = InMemoryStatus::Pending;
            Ok(())
        })
    }

    fn fail(
        &self,
        id: OutboxId,
        lease: OutboxLease,
        failure: OutboxFailure,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut state = self.lock()?;
            let record = state.records.get_mut(&id).ok_or(InMemoryOutboxError::NotFound { id })?;
            if !lease_is_current(&record.status, lease) {
                return Err(InMemoryOutboxError::LeaseMismatch { id });
            }
            record.last_failure = Some(failure.kind);
            record.status = InMemoryStatus::Failed(failure.kind);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbound::{
        AgingPolicy, OutboundClass, OutboundPriority, OutboundSettings, WindowLimit,
    };
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    fn metadata() -> OutboundMetadata {
        OutboundMetadata {
            scope: OutboundScope::Global,
            class: OutboundClass::new(1),
            priority: OutboundPriority::NORMAL,
            weight: NonZeroU32::new(1).unwrap(),
        }
    }

    fn request(key: &str) -> NewOutboxRequest {
        NewOutboxRequest::new(key, vec![1, 2, 3], metadata())
            .with_available_at(SystemTime::UNIX_EPOCH)
            .with_max_attempts(NonZeroU32::new(3).unwrap())
    }

    fn queue() -> OutboundQueue {
        OutboundQueue::new_spawn(OutboundSettings {
            queue_capacity: 32,
            limits: crate::outbound::OutboundLimits {
                global: vec![WindowLimit::new(100, Duration::from_secs(60))],
                chat: Vec::new(),
            },
            aging: AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX },
        })
        .unwrap()
    }

    #[tokio::test]
    async fn dedup_same_key_and_reject_conflict() {
        let store = InMemoryOutboxStore::new();
        let first = store.enqueue(request("same")).await.unwrap();
        assert!(matches!(first, OutboxEnqueueResult::Inserted(_)));
        let second = store.enqueue(request("same")).await.unwrap();
        assert_eq!(
            second,
            OutboxEnqueueResult::Existing(match first {
                OutboxEnqueueResult::Inserted(id) => id,
                OutboxEnqueueResult::Existing(id) => id,
            })
        );
        let conflict = store.enqueue(NewOutboxRequest::new("same", vec![9], metadata())).await;
        assert!(matches!(conflict, Err(InMemoryOutboxError::Conflict { .. })));
    }

    #[tokio::test]
    async fn lease_fencing_and_expired_lease_reclaim() {
        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("lease")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let now = SystemTime::now();
        let first = store
            .claim_due(
                OutboxWorkerId::new(1),
                now,
                Duration::from_secs(1),
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        let stale = store
            .complete(
                id,
                OutboxLease { until: SystemTime::UNIX_EPOCH, ..first.lease },
                SystemTime::UNIX_EPOCH,
            )
            .await;
        assert!(matches!(stale, Err(InMemoryOutboxError::LeaseMismatch { .. })));
        let second = store
            .claim_due(
                OutboxWorkerId::new(2),
                now.checked_add(Duration::from_secs(2)).unwrap(),
                Duration::from_secs(30),
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_ne!(first.lease.token, second.lease.token);
        store.complete(id, second.lease, SystemTime::now()).await.unwrap();
    }

    #[tokio::test]
    async fn expired_lease_before_begin_attempt_does_not_consume_delivery_attempt() {
        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("expired-before-attempt")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let claimed = store
            .claim_due(
                OutboxWorkerId::new(1),
                SystemTime::now(),
                Duration::from_millis(1),
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let result = store.begin_attempt(id, claimed.lease).await;
        assert!(matches!(result, Err(InMemoryOutboxError::LeaseMismatch { .. })));
        let snapshot = store.get(id).unwrap().unwrap();
        assert_eq!(snapshot.attempt, 0);
        assert_eq!(snapshot.claim_count, 1);
    }

    struct RecordingExecutor {
        started: Arc<Mutex<Vec<OutboxId>>>,
        result: Mutex<VecDeque<Result<(), OutboxExecutionError>>>,
    }

    impl OutboxExecutor for RecordingExecutor {
        fn execute(
            &self,
            request: ClaimedOutboxRequest,
        ) -> BoxFuture<'_, Result<(), OutboxExecutionError>> {
            let id = request.id;
            let started = self.started.clone();
            let result = self.result.lock().unwrap().pop_front().unwrap_or(Ok(()));
            Box::pin(async move {
                started.lock().unwrap().push(id);
                result
            })
        }
    }

    struct DelayedExecutor {
        delay: Duration,
    }

    impl OutboxExecutor for DelayedExecutor {
        fn execute(
            &self,
            _request: ClaimedOutboxRequest,
        ) -> BoxFuture<'_, Result<(), OutboxExecutionError>> {
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(())
            })
        }
    }

    struct BlockingExecutor {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl OutboxExecutor for BlockingExecutor {
        fn execute(
            &self,
            _request: ClaimedOutboxRequest,
        ) -> BoxFuture<'_, Result<(), OutboxExecutionError>> {
            let started = self.started.clone();
            let release = self.release.clone();
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn scheduler_permit_is_acquired_before_executor_and_success_completes() {
        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("success")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let started = Arc::new(Mutex::new(Vec::new()));
        let executor =
            RecordingExecutor { started: started.clone(), result: Mutex::new(VecDeque::new()) };
        let worker =
            OutboundOutbox::new(store.clone(), executor, queue(), OutboxWorkerSettings::default());
        assert_eq!(worker.run_once().await.unwrap(), 1);
        assert_eq!(*started.lock().unwrap(), vec![id]);
        assert_eq!(store.get(id).unwrap().unwrap().status, OutboxStatus::Completed);
    }

    #[tokio::test]
    async fn lease_heartbeat_renews_during_slow_executor() {
        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("slow-executor")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let settings = OutboxWorkerSettings {
            lease_for: Duration::from_millis(30),
            ..OutboxWorkerSettings::default()
        };
        let executor = DelayedExecutor { delay: Duration::from_millis(120) };
        let worker = OutboundOutbox::new(store.clone(), executor, queue(), settings);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), worker.run_once()).await.unwrap().unwrap(),
            1
        );
        let snapshot = store.get(id).unwrap().unwrap();
        assert_eq!(snapshot.status, OutboxStatus::Completed);
        assert_eq!(snapshot.attempt, 1);
    }

    #[tokio::test]
    async fn lease_heartbeat_renews_during_scheduler_wait() {
        let queue = OutboundQueue::new_spawn(OutboundSettings {
            queue_capacity: 8,
            limits: crate::outbound::OutboundLimits {
                global: vec![WindowLimit::new(1, Duration::from_millis(120))],
                chat: Vec::new(),
            },
            aging: AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX },
        })
        .unwrap();
        let holder = queue.handle().acquire(metadata()).await.unwrap();
        holder.complete(OutboundCompletion::RetryAfter {
            scope: OutboundScope::Global,
            duration: Duration::from_millis(120),
        });
        assert!(queue.handle().limits().await.is_some());

        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("slow-scheduler")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let settings = OutboxWorkerSettings {
            lease_for: Duration::from_millis(30),
            ..OutboxWorkerSettings::default()
        };
        let executor = RecordingExecutor {
            started: Arc::new(Mutex::new(Vec::new())),
            result: Mutex::new(VecDeque::new()),
        };
        let worker = OutboundOutbox::new(store.clone(), executor, queue, settings);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), worker.run_once()).await.unwrap().unwrap(),
            1
        );
        assert_eq!(store.get(id).unwrap().unwrap().status, OutboxStatus::Completed);
    }

    struct OrderedExecutor {
        started: Arc<Mutex<Vec<OutboxId>>>,
        first_started: Arc<tokio::sync::Notify>,
        release_first: Arc<tokio::sync::Notify>,
    }

    impl OutboxExecutor for OrderedExecutor {
        fn execute(
            &self,
            request: ClaimedOutboxRequest,
        ) -> BoxFuture<'_, Result<(), OutboxExecutionError>> {
            let id = request.id;
            let started = self.started.clone();
            let first_started = self.first_started.clone();
            let release_first = self.release_first.clone();
            Box::pin(async move {
                started.lock().unwrap().push(id);
                if id == OutboxId::new(0) {
                    first_started.notify_one();
                    release_first.notified().await;
                }
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn ordered_start_lane_reaches_the_next_executor_before_first_finishes() {
        let store = InMemoryOutboxStore::new();
        store.enqueue(request("ordered-first")).await.unwrap();
        store.enqueue(request("ordered-second")).await.unwrap();
        let started = Arc::new(Mutex::new(Vec::new()));
        let first_started = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let executor = OrderedExecutor {
            started: started.clone(),
            first_started: first_started.clone(),
            release_first: release_first.clone(),
        };
        let queue = queue();
        let lane = queue.handle().ordered_start_lane();
        let settings = OutboxWorkerSettings {
            max_in_flight: NonZeroUsize::new(2).unwrap(),
            lane: Some(lane),
            ..OutboxWorkerSettings::default()
        };
        let worker = OutboundOutbox::new(store, executor, queue, settings);
        let task = tokio::spawn(async move { worker.run_once().await });
        first_started.notified().await;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if started.lock().unwrap().len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(*started.lock().unwrap(), vec![OutboxId::new(0), OutboxId::new(1)]);
        release_first.notify_one();
        assert_eq!(task.await.unwrap().unwrap(), 2);
    }

    #[tokio::test]
    async fn retry_after_penalizes_scheduler_and_reschedules() {
        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("retry-after")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let executor = RecordingExecutor {
            started: Arc::new(Mutex::new(Vec::new())),
            result: Mutex::new(VecDeque::from([Err(OutboxExecutionError::RetryAfter {
                scope: OutboundScope::Global,
                duration: Duration::from_secs(30),
            })])),
        };
        let worker =
            OutboundOutbox::new(store.clone(), executor, queue(), OutboxWorkerSettings::default());
        worker.run_once().await.unwrap();
        let snapshot = store.get(id).unwrap().unwrap();
        assert_eq!(snapshot.status, OutboxStatus::Pending);
        assert_eq!(snapshot.last_failure, Some(OutboxFailureKind::RetryAfter));
    }

    #[tokio::test]
    async fn transient_retry_is_capped_and_exhaustion_is_terminal() {
        let store = InMemoryOutboxStore::new();
        let id = match store
            .enqueue(request("transient").with_max_attempts(NonZeroU32::new(2).unwrap()))
            .await
            .unwrap()
        {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let executor = RecordingExecutor {
            started: Arc::new(Mutex::new(Vec::new())),
            result: Mutex::new(VecDeque::from([
                Err(OutboxExecutionError::Transient),
                Err(OutboxExecutionError::Transient),
            ])),
        };
        let settings = OutboxWorkerSettings {
            transient_backoff_base: Duration::ZERO,
            transient_backoff_max: Duration::from_secs(1),
            ..OutboxWorkerSettings::default()
        };
        let worker = OutboundOutbox::new(store.clone(), executor, queue(), settings);
        worker.run_once().await.unwrap();
        worker.run_once().await.unwrap();
        assert_eq!(
            store.get(id).unwrap().unwrap().status,
            OutboxStatus::Failed(OutboxFailureKind::Exhausted)
        );
    }

    #[tokio::test]
    async fn queue_closed_preserves_durable_record() {
        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("closed")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let queue = queue();
        queue.handle().shutdown();
        let executor = RecordingExecutor {
            started: Arc::new(Mutex::new(Vec::new())),
            result: Mutex::new(VecDeque::new()),
        };
        let settings = OutboxWorkerSettings {
            scheduler_retry_delay: Duration::ZERO,
            ..OutboxWorkerSettings::default()
        };
        let worker = OutboundOutbox::new(store.clone(), executor, queue, settings);
        worker.run_once().await.unwrap();
        let snapshot = store.get(id).unwrap().unwrap();
        assert_eq!(snapshot.status, OutboxStatus::Pending);
        assert_eq!(snapshot.attempt, 0);
        assert_eq!(snapshot.claim_count, 1);
    }

    #[tokio::test]
    async fn scheduler_queue_full_does_not_consume_delivery_attempt() {
        let queue = OutboundQueue::new_spawn(OutboundSettings {
            queue_capacity: 1,
            limits: crate::outbound::OutboundLimits {
                global: vec![WindowLimit::new(1, Duration::from_secs(1))],
                chat: Vec::new(),
            },
            aging: AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX },
        })
        .unwrap();
        let handle = queue.handle().clone();
        let holder = handle.acquire(metadata()).await.unwrap();
        let blocked = handle.acquire(metadata());
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        let store = InMemoryOutboxStore::new();
        let id = match store
            .enqueue(request("queue-full").with_max_attempts(NonZeroU32::new(1).unwrap()))
            .await
            .unwrap()
        {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let settings = OutboxWorkerSettings {
            scheduler_retry_delay: Duration::ZERO,
            ..OutboxWorkerSettings::default()
        };
        let executor = RecordingExecutor {
            started: Arc::new(Mutex::new(Vec::new())),
            result: Mutex::new(VecDeque::new()),
        };
        let worker = OutboundOutbox::new(store.clone(), executor, queue, settings);
        worker.run_once().await.unwrap();

        let snapshot = store.get(id).unwrap().unwrap();
        assert_eq!(snapshot.status, OutboxStatus::Pending);
        assert_eq!(snapshot.attempt, 0);
        assert_eq!(snapshot.claim_count, 1);
        holder.complete(OutboundCompletion::Success);
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_current_batch_without_a_second_claim() {
        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("graceful-shutdown")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let executor = BlockingExecutor { started: started.clone(), release: release.clone() };
        let worker =
            OutboundOutbox::new(store.clone(), executor, queue(), OutboxWorkerSettings::default());
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown_signal = shutdown.clone();
        let mut task =
            tokio::spawn(async move { worker.run_until(shutdown_signal.notified()).await });

        started.notified().await;
        shutdown.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut task).await.is_err(),
            "shutdown must not cancel the active batch"
        );
        release.notify_one();
        let result = task.await.unwrap();
        assert!(result.is_ok());
        let snapshot = store.get(id).unwrap().unwrap();
        assert_eq!(snapshot.status, OutboxStatus::Completed);
        assert_eq!(snapshot.claim_count, 1);
    }

    #[tokio::test]
    async fn failed_store_completion_leaves_claim_for_replay_after_lease() {
        let store = InMemoryOutboxStore::new();
        let id = match store.enqueue(request("at-least-once")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        store.fail_next_complete().unwrap();
        let executor = RecordingExecutor {
            started: Arc::new(Mutex::new(Vec::new())),
            result: Mutex::new(VecDeque::new()),
        };
        let settings = OutboxWorkerSettings {
            lease_for: Duration::from_secs(1),
            ..OutboxWorkerSettings::default()
        };
        let worker = OutboundOutbox::new(store.clone(), executor, queue(), settings);
        assert!(matches!(
            worker.run_once().await,
            Err(OutboxWorkerError::Store(InMemoryOutboxError::InjectedFailure))
        ));
        assert_eq!(store.get(id).unwrap().unwrap().status, OutboxStatus::Claimed);
    }

    #[tokio::test]
    async fn batch_drains_siblings_after_store_error() {
        let store = InMemoryOutboxStore::new();
        let first_id = match store.enqueue(request("batch-first")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        let second_id = match store.enqueue(request("batch-second")).await.unwrap() {
            OutboxEnqueueResult::Inserted(id) => id,
            OutboxEnqueueResult::Existing(_) => unreachable!(),
        };
        store.fail_next_complete().unwrap();
        let started = Arc::new(Mutex::new(Vec::new()));
        let executor =
            RecordingExecutor { started: started.clone(), result: Mutex::new(VecDeque::new()) };
        let settings = OutboxWorkerSettings {
            max_in_flight: NonZeroUsize::new(2).unwrap(),
            ..OutboxWorkerSettings::default()
        };
        let worker = OutboundOutbox::new(store.clone(), executor, queue(), settings);

        assert!(matches!(
            worker.run_once().await,
            Err(OutboxWorkerError::Store(InMemoryOutboxError::InjectedFailure))
        ));
        assert_eq!(started.lock().unwrap().len(), 2);
        let statuses = [
            store.get(first_id).unwrap().unwrap().status,
            store.get(second_id).unwrap().unwrap().status,
        ];
        assert_eq!(statuses.iter().filter(|status| **status == OutboxStatus::Completed).count(), 1);
        assert_eq!(statuses.iter().filter(|status| **status == OutboxStatus::Claimed).count(), 1);
    }

    struct CountingExecutor {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    impl OutboxExecutor for CountingExecutor {
        fn execute(
            &self,
            _request: ClaimedOutboxRequest,
        ) -> BoxFuture<'_, Result<(), OutboxExecutionError>> {
            let active = self.active.clone();
            let peak = self.peak.clone();
            Box::pin(async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn worker_concurrency_is_bounded() {
        let store = InMemoryOutboxStore::new();
        for index in 0..8 {
            store.enqueue(request(&format!("bounded-{index}"))).await.unwrap();
        }
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let executor = CountingExecutor { active: active.clone(), peak: peak.clone() };
        let settings = OutboxWorkerSettings {
            max_in_flight: NonZeroUsize::new(2).unwrap(),
            ..OutboxWorkerSettings::default()
        };
        let worker = OutboundOutbox::new(store, executor, queue(), settings);
        assert_eq!(worker.run_once().await.unwrap(), 2);
        assert!(peak.load(Ordering::SeqCst) <= 2);
    }
}
