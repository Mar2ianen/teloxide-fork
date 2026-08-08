use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use teloxide_core::types::ChatId;
use tokio::time::Instant;

/// Stable chat identity used by a shared limiter and outbound queue.
///
/// Create one queue-backed limiter adapter per Drafter and share the
/// underlying outbound queue across drafters that use the same bot token.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DrafterRateLimitKey {
    pub chat_id: ChatId,
}

impl From<ChatId> for DrafterRateLimitKey {
    fn from(chat_id: ChatId) -> Self {
        Self { chat_id }
    }
}

/// The scope affected by a Telegram flood-control response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrafterRateLimitScope {
    Chat(ChatId),
    Global,
}

/// Scheduler priority. Final delivery must not be starved by refresh traffic.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DrafterPriority {
    RefreshPreview,
    ChangedPreview,
    SegmentCommit,
    Final,
}

/// Broad outbound method class selected from the backend's current state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrafterRequestClass {
    Send,
    Mutation,
}

/// Failure to acquire a shared Drafter scheduling slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DrafterAcquireError {
    Closed,
    QueueFull,
    Superseded,
    InvalidConfiguration,
}

impl fmt::Display for DrafterAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("drafter rate limiter is closed"),
            Self::QueueFull => f.write_str("drafter rate limiter queue is full"),
            Self::Superseded => f.write_str("drafter rate-limit request was superseded"),
            Self::InvalidConfiguration => {
                f.write_str("drafter rate limiter configuration is invalid")
            }
        }
    }
}

impl std::error::Error for DrafterAcquireError {}

/// The result reported by the operation guarded by a DrafterPermit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DrafterPermitCompletion {
    Success,
    Failed,
    RetryAfter { scope: DrafterRateLimitScope, duration: Duration },
    CancelledAfterGrant,
}

pub(crate) trait DrafterPermitLease: Send {
    fn complete(
        self: Box<Self>,
        completion: DrafterPermitCompletion,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

struct NoopPermitLease;

impl DrafterPermitLease for NoopPermitLease {
    fn complete(
        self: Box<Self>,
        _completion: DrafterPermitCompletion,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }
}

/// A successful reservation from a DrafterRateLimiter.
///
/// The permit owns the scheduler reservation until the guarded backend
/// operation reports an outcome. Dropping it without completion delegates to
/// the underlying limiter cancellation behavior.
pub struct DrafterPermit {
    lease: Option<Box<dyn DrafterPermitLease>>,
}

impl fmt::Debug for DrafterPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DrafterPermit").field("active", &self.lease.is_some()).finish()
    }
}

impl DrafterPermit {
    pub fn new() -> Self {
        Self { lease: Some(Box::new(NoopPermitLease)) }
    }

    pub(crate) fn from_lease(lease: impl DrafterPermitLease + 'static) -> Self {
        Self { lease: Some(Box::new(lease)) }
    }

    pub async fn complete(mut self, completion: DrafterPermitCompletion) {
        if let Some(lease) = self.lease.take() {
            lease.complete(completion).await;
        }
    }
}

impl Default for DrafterPermit {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared rate-limiter contract used by all scheduler workers.
pub trait DrafterRateLimiter: Send + Sync + 'static {
    fn acquire(
        &self,
        key: DrafterRateLimitKey,
        priority: DrafterPriority,
        request_class: DrafterRequestClass,
    ) -> impl Future<Output = Result<DrafterPermit, DrafterAcquireError>> + Send;

    fn penalize(&self, scope: DrafterRateLimitScope, retry_after: Duration);

    fn completion_handles_retry_after(&self) -> bool {
        false
    }

    /// Transfers a granted operation permit to a per-request scheduler when
    /// the backend can account each underlying Bot API request separately.
    /// Legacy limiters keep ownership of the permit in the Drafter worker.
    fn request_context(
        &self,
        permit: DrafterPermit,
        _key: DrafterRateLimitKey,
        _priority: DrafterPriority,
    ) -> Result<super::DrafterRequestContext, DrafterPermit> {
        Err(permit)
    }

    /// Whether the limiter supports per-request scheduling, including
    /// separate permits for backend cleanup requests.
    fn uses_request_scheduler(&self) -> bool {
        false
    }
}

#[derive(Default)]
struct LimiterState {
    global_next: Option<Instant>,
    global_penalty_until: Option<Instant>,
    chat_next: HashMap<ChatId, Instant>,
    chat_penalty_until: HashMap<ChatId, Instant>,
    next_waiter_id: u64,
    operation_count: u64,
    waiters: Vec<LimiterWaiter>,
}

#[derive(Clone, Copy)]
struct LimiterWaiter {
    id: u64,
    key: DrafterRateLimitKey,
    priority: DrafterPriority,
}

struct WaiterGuard {
    state: Arc<Mutex<LimiterState>>,
    notify: Arc<tokio::sync::Notify>,
    id: u64,
    active: bool,
}

enum AcquireWait {
    Until(Instant),
    YieldToPriority,
    Acquired,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            state.waiters.retain(|waiter| waiter.id != self.id);
        }
        self.notify.notify_waiters();
    }
}

/// A small in-process limiter with independent chat and bot-token budgets.
///
/// It deliberately owns reservations rather than sleeping in the drafter
/// worker. This lets several drafters share one bot-level budget while each
/// worker continues to keep only its latest preview state.
#[derive(Clone)]
pub struct InProcessRateLimiter {
    state: Arc<Mutex<LimiterState>>,
    notify: Arc<tokio::sync::Notify>,
    per_chat_interval: Duration,
    global_interval: Duration,
}

impl InProcessRateLimiter {
    #[must_use]
    pub fn new(per_chat_interval: Duration, global_interval: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(LimiterState::default())),
            notify: Arc::new(tokio::sync::Notify::new()),
            per_chat_interval,
            global_interval,
        }
    }

    #[must_use]
    pub fn with_defaults() -> Self {
        Self::default()
    }
}

impl Default for InProcessRateLimiter {
    fn default() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_millis(50))
    }
}

impl DrafterRateLimiter for InProcessRateLimiter {
    async fn acquire(
        &self,
        key: DrafterRateLimitKey,
        priority: DrafterPriority,
        _request_class: DrafterRequestClass,
    ) -> Result<DrafterPermit, DrafterAcquireError> {
        let (id, mut waiter) = {
            let mut state = self.state.lock().expect("drafter limiter mutex poisoned");
            state.record_operation(Instant::now());
            let id = state.next_waiter_id;
            state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
            state.waiters.push(LimiterWaiter { id, key, priority });
            (
                id,
                WaiterGuard {
                    state: Arc::clone(&self.state),
                    notify: Arc::clone(&self.notify),
                    id,
                    active: true,
                },
            )
        };

        loop {
            let now = Instant::now();
            let wait = {
                let mut state = self.state.lock().expect("drafter limiter mutex poisoned");
                let Some(current) = state.waiters.iter().find(|waiter| waiter.id == id).copied()
                else {
                    return Ok(DrafterPermit::new());
                };
                let own_deadline = state.availability_deadline(current.key, now);
                let has_priority_waiter = state.waiters.iter().any(|other| {
                    other.id != current.id
                        && (other.priority > current.priority
                            || (other.priority == current.priority && other.id < current.id))
                        && state.availability_deadline(other.key, now) <= now
                });
                if !has_priority_waiter && own_deadline <= now {
                    state.waiters.retain(|waiter| waiter.id != id);
                    state.chat_next.insert(key.chat_id, now + self.per_chat_interval);
                    state.global_next = Some(now + self.global_interval);
                    waiter.active = false;
                    self.notify.notify_waiters();
                    AcquireWait::Acquired
                } else if has_priority_waiter && own_deadline <= now {
                    AcquireWait::YieldToPriority
                } else {
                    AcquireWait::Until(own_deadline)
                }
            };

            match wait {
                AcquireWait::Until(deadline) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {},
                        _ = self.notify.notified() => {},
                    }
                }
                AcquireWait::YieldToPriority => {
                    tokio::task::yield_now().await;
                }
                AcquireWait::Acquired => return Ok(DrafterPermit::new()),
            }
        }
    }

    fn penalize(&self, scope: DrafterRateLimitScope, retry_after: Duration) {
        let deadline = Instant::now() + retry_after;
        let mut state = self.state.lock().expect("drafter limiter mutex poisoned");
        state.record_operation(deadline - retry_after);
        match scope {
            DrafterRateLimitScope::Global => {
                state.global_penalty_until =
                    Some(state.global_penalty_until.unwrap_or(deadline).max(deadline));
            }
            DrafterRateLimitScope::Chat(chat_id) => {
                let current = state.chat_penalty_until.entry(chat_id).or_insert(deadline);
                *current = (*current).max(deadline);
            }
        }
        self.notify.notify_waiters();
    }
}

impl LimiterState {
    fn record_operation(&mut self, now: Instant) {
        self.operation_count = self.operation_count.wrapping_add(1);
        if self.operation_count % 512 == 0
            || self.chat_next.len() > 4096
            || self.chat_penalty_until.len() > 4096
        {
            self.chat_next.retain(|_, deadline| *deadline > now);
            self.chat_penalty_until.retain(|_, deadline| *deadline > now);
        }
    }

    fn availability_deadline(&self, key: DrafterRateLimitKey, now: Instant) -> Instant {
        let chat_deadline = self
            .chat_next
            .get(&key.chat_id)
            .copied()
            .unwrap_or(now)
            .max(self.chat_penalty_until.get(&key.chat_id).copied().unwrap_or(now));
        let global_deadline =
            self.global_next.unwrap_or(now).max(self.global_penalty_until.unwrap_or(now));
        chat_deadline.max(global_deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn final_waiter_precedes_refresh_waiter() {
        let limiter = InProcessRateLimiter::new(Duration::from_secs(1), Duration::from_secs(1));
        let key = DrafterRateLimitKey { chat_id: ChatId(1) };
        let _ =
            limiter.acquire(key, DrafterPriority::ChangedPreview, DrafterRequestClass::Send).await;

        let refresh = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                let _ = limiter
                    .acquire(key, DrafterPriority::RefreshPreview, DrafterRequestClass::Send)
                    .await;
            })
        };
        tokio::task::yield_now().await;
        let final_delivery = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                let _ =
                    limiter.acquire(key, DrafterPriority::Final, DrafterRequestClass::Send).await;
            })
        };
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(final_delivery.is_finished());
        assert!(!refresh.is_finished());

        final_delivery.await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(refresh.is_finished());
        refresh.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn chat_penalty_does_not_block_another_chat() {
        let limiter = InProcessRateLimiter::new(Duration::ZERO, Duration::ZERO);
        let penalized = DrafterRateLimitKey { chat_id: ChatId(1) };
        let other = DrafterRateLimitKey { chat_id: ChatId(2) };
        limiter.penalize(DrafterRateLimitScope::Chat(penalized.chat_id), Duration::from_secs(5));

        let _ = limiter
            .acquire(other, DrafterPriority::ChangedPreview, DrafterRequestClass::Send)
            .await;
        let waiter = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                let _ = limiter
                    .acquire(penalized, DrafterPriority::ChangedPreview, DrafterRequestClass::Send)
                    .await;
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        waiter.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn penalized_final_does_not_block_other_chat() {
        let limiter = InProcessRateLimiter::new(Duration::ZERO, Duration::ZERO);
        let chat_a = DrafterRateLimitKey { chat_id: ChatId(1) };
        let chat_b = DrafterRateLimitKey { chat_id: ChatId(2) };
        limiter.penalize(DrafterRateLimitScope::Chat(chat_a.chat_id), Duration::from_secs(60));

        let final_waiter = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                let _ = limiter
                    .acquire(chat_a, DrafterPriority::Final, DrafterRequestClass::Send)
                    .await;
            })
        };
        tokio::task::yield_now().await;

        let other_chat = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                let _ = limiter
                    .acquire(chat_b, DrafterPriority::ChangedPreview, DrafterRequestClass::Send)
                    .await;
            })
        };
        tokio::task::yield_now().await;
        assert!(other_chat.is_finished());
        other_chat.await.unwrap();
        assert!(!final_waiter.is_finished());
        final_waiter.abort();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn low_priority_waiter_yields_to_registered_high_priority_waiter() {
        let limiter = InProcessRateLimiter::new(Duration::ZERO, Duration::ZERO);
        let low_key = DrafterRateLimitKey { chat_id: ChatId(1) };
        let high_key = DrafterRateLimitKey { chat_id: ChatId(2) };
        {
            let mut state = limiter.state.lock().unwrap();
            state.next_waiter_id = 1;
            state.waiters.push(LimiterWaiter {
                id: 0,
                key: high_key,
                priority: DrafterPriority::Final,
            });
        }

        let low_waiter = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                let _ = limiter
                    .acquire(low_key, DrafterPriority::ChangedPreview, DrafterRequestClass::Send)
                    .await;
            })
        };
        tokio::task::yield_now().await;
        assert!(limiter
            .state
            .lock()
            .unwrap()
            .waiters
            .iter()
            .any(|waiter| waiter.priority == DrafterPriority::ChangedPreview));

        let release_high = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                let mut state = limiter.state.lock().unwrap();
                state.waiters.retain(|waiter| waiter.id != 0);
                limiter.notify.notify_waiters();
            })
        };
        low_waiter.await.unwrap();
        release_high.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn expired_chat_deadlines_are_pruned_opportunistically() {
        let limiter = InProcessRateLimiter::new(Duration::from_secs(1), Duration::ZERO);
        let active = ChatId(10_000);
        limiter.penalize(DrafterRateLimitScope::Chat(active), Duration::from_secs(60));
        for index in 0..600 {
            let key = DrafterRateLimitKey { chat_id: ChatId(index) };
            let _ = limiter
                .acquire(key, DrafterPriority::ChangedPreview, DrafterRequestClass::Send)
                .await;
            limiter.penalize(DrafterRateLimitScope::Chat(key.chat_id), Duration::from_secs(1));
        }
        {
            let state = limiter.state.lock().unwrap();
            assert!(state.chat_next.len() >= 600);
            assert!(state.chat_penalty_until.len() >= 601);
        }

        tokio::time::advance(Duration::from_secs(2)).await;
        for index in 600..1_024 {
            let _ = limiter
                .acquire(
                    DrafterRateLimitKey { chat_id: ChatId(index) },
                    DrafterPriority::ChangedPreview,
                    DrafterRequestClass::Send,
                )
                .await;
        }
        let state = limiter.state.lock().unwrap();
        assert!(state.chat_next.values().all(|deadline| *deadline > Instant::now()));
        assert!(state.chat_penalty_until.contains_key(&active));
        assert!(state.chat_penalty_until.len() < 10);
    }
}
