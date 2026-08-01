use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use teloxide_core::types::ChatId;
use tokio::time::Instant;

/// A key used by the shared limiter. The same limiter instance should be
/// passed to every drafter belonging to one bot token.
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

/// A successful reservation from a [`DrafterRateLimiter`].
#[derive(Debug)]
pub struct DrafterPermit {
    _private: (),
}

impl DrafterPermit {
    /// Creates a permit for a limiter implementation.
    pub const fn new() -> Self {
        Self { _private: () }
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
    ) -> impl Future<Output = DrafterPermit> + Send;

    fn penalize(&self, scope: DrafterRateLimitScope, retry_after: Duration);
}

#[derive(Default)]
struct LimiterState {
    global_next: Option<Instant>,
    global_penalty_until: Option<Instant>,
    chat_next: HashMap<ChatId, Instant>,
    chat_penalty_until: HashMap<ChatId, Instant>,
    next_waiter_id: u64,
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
    async fn acquire(&self, key: DrafterRateLimitKey, priority: DrafterPriority) -> DrafterPermit {
        let (id, mut waiter) = {
            let mut state = self.state.lock().expect("drafter limiter mutex poisoned");
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
            let deadline = {
                let mut state = self.state.lock().expect("drafter limiter mutex poisoned");
                let Some(current) = state.waiters.iter().find(|waiter| waiter.id == id).copied()
                else {
                    return DrafterPermit::new();
                };
                let own_deadline = state.availability_deadline(current.key, now);
                let has_priority_waiter = state.waiters.iter().any(|other| {
                    other.id != current.id
                        && (other.priority > current.priority
                            || (other.priority == current.priority && other.id < current.id))
                });
                if !has_priority_waiter && own_deadline <= now {
                    state.waiters.retain(|waiter| waiter.id != id);
                    state.chat_next.insert(key.chat_id, now + self.per_chat_interval);
                    state.global_next = Some(now + self.global_interval);
                    waiter.active = false;
                    self.notify.notify_waiters();
                    None
                } else {
                    let blocker_deadline = state
                        .waiters
                        .iter()
                        .filter(|other| {
                            other.id != current.id
                                && (other.priority > current.priority
                                    || (other.priority == current.priority
                                        && other.id < current.id))
                        })
                        .map(|other| state.availability_deadline(other.key, now))
                        .min();
                    Some(own_deadline.max(blocker_deadline.unwrap_or(own_deadline)))
                }
            };

            if let Some(deadline) = deadline {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => {},
                    _ = self.notify.notified() => {},
                }
                continue;
            }
            return DrafterPermit::new();
        }
    }

    fn penalize(&self, scope: DrafterRateLimitScope, retry_after: Duration) {
        let deadline = Instant::now() + retry_after;
        let mut state = self.state.lock().expect("drafter limiter mutex poisoned");
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
        let _ = limiter.acquire(key, DrafterPriority::ChangedPreview).await;

        let refresh = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                limiter.acquire(key, DrafterPriority::RefreshPreview).await;
            })
        };
        tokio::task::yield_now().await;
        let final_delivery = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                limiter.acquire(key, DrafterPriority::Final).await;
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

        let _ = limiter.acquire(other, DrafterPriority::ChangedPreview).await;
        let waiter = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                limiter.acquire(penalized, DrafterPriority::ChangedPreview).await;
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        waiter.await.unwrap();
    }
}
