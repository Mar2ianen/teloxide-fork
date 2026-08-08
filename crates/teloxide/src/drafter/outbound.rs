//! Outbound-queue backed Drafter rate limiting.

use std::{error::Error, fmt, future::Future, num::NonZeroU32, pin::Pin, time::Duration};

use teloxide_core::{
    errors::RequestError,
    outbound::{
        class, OutboundAcquireError, OutboundClass, OutboundCompletion, OutboundLane,
        OutboundMetadata, OutboundPermit, OutboundPriority, OutboundQueue, OutboundScope,
    },
};

use super::{
    DrafterAcquireError, DrafterPermit, DrafterPermitCompletion, DrafterPriority,
    DrafterRateLimitKey, DrafterRateLimitScope, DrafterRateLimiter, DrafterRequestClass,
};

use super::limiter::DrafterPermitLease;

/// A Drafter limiter backed by a shared outbound queue.
///
/// Each adapter owns one serial lane, while all adapters created from cloned
/// queues share the queue's global and per-chat admission state. Create one
/// adapter per Drafter instance; share the underlying OutboundQueue for one
/// bot token.
#[derive(Clone)]
pub struct DrafterOutboundLimiter {
    queue: OutboundQueue,
    lane: OutboundLane,
}

impl DrafterOutboundLimiter {
    #[must_use]
    pub fn new(queue: OutboundQueue) -> Self {
        let lane = queue.handle().serial_lane();
        Self { queue, lane }
    }

    async fn acquire_outbound(
        &self,
        key: DrafterRateLimitKey,
        priority: DrafterPriority,
        request_class: DrafterRequestClass,
    ) -> Result<DrafterPermit, OutboundAcquireError> {
        let metadata = OutboundMetadata {
            scope: OutboundScope::Chat(teloxide_core::outbound::OutboundChatKey::id(key.chat_id.0)),
            class: OutboundClass::new(class_for(request_class)),
            priority: map_priority(priority),
            weight: NonZeroU32::new(1).expect("one is non-zero"),
        };
        self.lane
            .acquire(metadata)
            .await
            .map(|permit| DrafterPermit::from_lease(OutboundPermitLease { permit }))
    }

    #[must_use]
    pub fn queue(&self) -> &OutboundQueue {
        &self.queue
    }

    /// Creates a fresh adapter with a new serial lane over the same shared
    /// queue. Use this when constructing another Drafter instance; cloning an
    /// adapter intentionally keeps its lane and is only suitable for aliases
    /// of one Drafter.
    #[must_use]
    pub fn for_drafter(&self) -> Self {
        Self::new(self.queue.clone())
    }
}

/// Error returned by a standard Telegram backend when either Telegram or the
/// per-request outbound scheduler rejects a typed request.
#[derive(Clone, Debug)]
pub enum DrafterRequestError {
    /// The Telegram request failed after its permit was granted.
    Inner(RequestError),
    /// The request could not be admitted and was never executed.
    Acquire(OutboundAcquireError),
    /// An admitted Telegram request exceeded the configured request timeout.
    Timeout,
}

impl fmt::Display for DrafterRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner(error) => write!(f, "the scheduled request failed: {error}"),
            Self::Acquire(error) => write!(f, "the outbound queue rejected the request: {error}"),
            Self::Timeout => f.write_str("the scheduled request timed out"),
        }
    }
}

impl Error for DrafterRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Inner(error) => Some(error),
            Self::Acquire(error) => Some(error),
            Self::Timeout => None,
        }
    }
}

/// Per-operation context that transfers the already granted first permit to
/// the first real Bot API request and acquires additional permits for every
/// subsequent request in the same backend operation.
pub struct DrafterRequestContext {
    limiter: DrafterOutboundLimiter,
    initial_permit: Option<DrafterPermit>,
    key: DrafterRateLimitKey,
    priority: DrafterPriority,
    request_timeout: Option<Duration>,
}

impl DrafterRequestContext {
    fn new(
        limiter: DrafterOutboundLimiter,
        initial_permit: DrafterPermit,
        key: DrafterRateLimitKey,
        priority: DrafterPriority,
    ) -> Self {
        Self { limiter, initial_permit: Some(initial_permit), key, priority, request_timeout: None }
    }

    /// Configures the timeout applied only after a request receives a permit.
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Cancels a transferred permit when the backend operation is known not to
    /// issue any request. This is the explicit completion path for local
    /// no-op operations; dropping the context remains only a fallback.
    pub async fn cancel_unused(mut self) {
        if let Some(permit) = self.initial_permit.take() {
            permit.complete(DrafterPermitCompletion::CancelledAfterGrant).await;
        }
    }

    /// Executes exactly one typed request under one completion-aware permit.
    /// The first call consumes the permit transferred by the Drafter machine;
    /// later calls acquire their own permit from the shared queue.
    pub async fn execute<T, F, Fut>(
        &mut self,
        request_class: DrafterRequestClass,
        request: F,
    ) -> Result<T, DrafterRequestError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, RequestError>>,
    {
        let permit = match self.initial_permit.take() {
            Some(permit) => permit,
            None => self
                .limiter
                .acquire_outbound(self.key, self.priority, request_class)
                .await
                .map_err(DrafterRequestError::Acquire)?,
        };
        let result = if let Some(timeout) = self.request_timeout {
            match tokio::time::timeout(timeout, request()).await {
                Ok(result) => result,
                Err(_) => {
                    permit.complete(DrafterPermitCompletion::Failed).await;
                    return Err(DrafterRequestError::Timeout);
                }
            }
        } else {
            request().await
        };
        let completion = match &result {
            Ok(_) => DrafterPermitCompletion::Success,
            Err(RequestError::RetryAfter(seconds)) => DrafterPermitCompletion::RetryAfter {
                scope: DrafterRateLimitScope::Global,
                duration: Duration::from_secs(seconds.seconds() as u64),
            },
            Err(_) => DrafterPermitCompletion::Failed,
        };
        permit.complete(completion).await;
        result.map_err(DrafterRequestError::Inner)
    }
}

struct OutboundPermitLease {
    permit: OutboundPermit,
}

impl DrafterPermitLease for OutboundPermitLease {
    fn complete(
        self: Box<Self>,
        completion: DrafterPermitCompletion,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let outcome = match completion {
            DrafterPermitCompletion::Success => OutboundCompletion::Success,
            DrafterPermitCompletion::Failed => OutboundCompletion::Failed,
            DrafterPermitCompletion::RetryAfter { scope, duration } => {
                OutboundCompletion::RetryAfter { scope: map_scope(scope), duration }
            }
            DrafterPermitCompletion::CancelledAfterGrant => OutboundCompletion::CancelledAfterGrant,
        };
        Box::pin(async move {
            self.permit.complete_and_await(outcome).await;
        })
    }
}

impl DrafterRateLimiter for DrafterOutboundLimiter {
    async fn acquire(
        &self,
        key: DrafterRateLimitKey,
        priority: DrafterPriority,
        request_class: DrafterRequestClass,
    ) -> Result<DrafterPermit, DrafterAcquireError> {
        self.acquire_outbound(key, priority, request_class).await.map_err(map_acquire_error)
    }

    fn penalize(&self, scope: DrafterRateLimitScope, retry_after: std::time::Duration) {
        self.queue.handle().penalize(map_scope(scope), retry_after);
    }

    fn completion_handles_retry_after(&self) -> bool {
        true
    }

    fn request_context(
        &self,
        permit: DrafterPermit,
        key: DrafterRateLimitKey,
        priority: DrafterPriority,
    ) -> Result<super::DrafterRequestContext, DrafterPermit> {
        Ok(DrafterRequestContext::new(self.clone(), permit, key, priority))
    }

    fn uses_request_scheduler(&self) -> bool {
        true
    }
}

fn map_scope(scope: DrafterRateLimitScope) -> OutboundScope {
    match scope {
        DrafterRateLimitScope::Global => OutboundScope::Global,
        DrafterRateLimitScope::Chat(chat_id) => {
            OutboundScope::Chat(teloxide_core::outbound::OutboundChatKey::id(chat_id.0))
        }
    }
}

fn map_priority(priority: DrafterPriority) -> OutboundPriority {
    match priority {
        DrafterPriority::RefreshPreview => OutboundPriority::BACKGROUND,
        DrafterPriority::ChangedPreview => OutboundPriority::NORMAL,
        DrafterPriority::SegmentCommit => OutboundPriority::INTERACTIVE,
        DrafterPriority::Final => OutboundPriority::CRITICAL,
    }
}

fn class_for(request_class: DrafterRequestClass) -> u64 {
    match request_class {
        DrafterRequestClass::Send => class::MESSAGE_SEND,
        DrafterRequestClass::Mutation => class::MESSAGE_MUTATION,
    }
}

fn map_acquire_error(error: OutboundAcquireError) -> DrafterAcquireError {
    match error {
        OutboundAcquireError::Closed => DrafterAcquireError::Closed,
        OutboundAcquireError::QueueFull => DrafterAcquireError::QueueFull,
        OutboundAcquireError::Superseded => DrafterAcquireError::Superseded,
        _ => DrafterAcquireError::InvalidConfiguration,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use teloxide_core::{
        outbound::{AgingPolicy, OutboundLimits, OutboundSettings, WindowLimit},
        types::ChatId,
    };

    fn queue() -> OutboundQueue {
        OutboundQueue::new_spawn(OutboundSettings {
            limits: OutboundLimits { global: Vec::new(), chat: Vec::new() },
            queue_capacity: 16,
            aging: AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX },
        })
        .unwrap()
    }

    #[test]
    fn operation_class_mapping_distinguishes_send_and_edit() {
        assert_eq!(class_for(DrafterRequestClass::Send), class::MESSAGE_SEND);
        assert_eq!(class_for(DrafterRequestClass::Mutation), class::MESSAGE_MUTATION);
    }

    #[tokio::test(start_paused = true)]
    async fn queue_backed_permit_completes_successfully() {
        let limiter = DrafterOutboundLimiter::new(queue());
        let permit = limiter
            .acquire(
                DrafterRateLimitKey { chat_id: ChatId(1) },
                DrafterPriority::Final,
                DrafterRequestClass::Send,
            )
            .await
            .unwrap();

        permit.complete(DrafterPermitCompletion::Success).await;
    }

    #[tokio::test(start_paused = true)]
    async fn request_context_accounts_each_real_request() {
        let queue = OutboundQueue::new_spawn(OutboundSettings {
            limits: OutboundLimits {
                global: vec![WindowLimit::new(2, Duration::from_secs(60))],
                chat: Vec::new(),
            },
            queue_capacity: 16,
            aging: AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX },
        })
        .unwrap();
        let limiter = DrafterOutboundLimiter::new(queue.clone());
        let initial = limiter
            .acquire(
                DrafterRateLimitKey { chat_id: ChatId(1) },
                DrafterPriority::Final,
                DrafterRequestClass::Send,
            )
            .await
            .unwrap();
        let mut context = limiter
            .request_context(
                initial,
                DrafterRateLimitKey { chat_id: ChatId(1) },
                DrafterPriority::Final,
            )
            .unwrap();

        context.execute(DrafterRequestClass::Send, || async { Ok(()) }).await.unwrap();
        context.execute(DrafterRequestClass::Mutation, || async { Ok(()) }).await.unwrap();

        let next = limiter.acquire(
            DrafterRateLimitKey { chat_id: ChatId(1) },
            DrafterPriority::Final,
            DrafterRequestClass::Send,
        );
        tokio::pin!(next);
        assert!(futures::poll!(next.as_mut()).is_pending());
        tokio::task::yield_now().await;
        assert_eq!(queue.handle().snapshot().await.unwrap().pending, 1);

        tokio::time::advance(Duration::from_secs(60)).await;
        next.await.unwrap().complete(DrafterPermitCompletion::Success).await;
    }

    #[tokio::test(start_paused = true)]
    async fn request_admission_is_not_counted_against_request_timeout() {
        let queue = OutboundQueue::new_spawn(OutboundSettings {
            limits: OutboundLimits {
                global: vec![WindowLimit::new(1, Duration::from_secs(60))],
                chat: Vec::new(),
            },
            queue_capacity: 16,
            aging: AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX },
        })
        .unwrap();
        let limiter = DrafterOutboundLimiter::new(queue);
        let initial = limiter
            .acquire(
                DrafterRateLimitKey { chat_id: ChatId(1) },
                DrafterPriority::Final,
                DrafterRequestClass::Send,
            )
            .await
            .unwrap();
        let mut context = limiter
            .request_context(
                initial,
                DrafterRateLimitKey { chat_id: ChatId(1) },
                DrafterPriority::Final,
            )
            .unwrap()
            .with_request_timeout(Duration::from_secs(5));

        context.execute(DrafterRequestClass::Send, || async { Ok(()) }).await.unwrap();
        let request_started = Arc::new(AtomicBool::new(false));
        let request_started_ref = Arc::clone(&request_started);
        let second = context.execute(DrafterRequestClass::Mutation, move || async move {
            request_started_ref.store(true, Ordering::Relaxed);
            Ok(())
        });
        tokio::pin!(second);

        assert!(futures::poll!(second.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(!request_started.load(Ordering::Relaxed));

        tokio::time::advance(Duration::from_secs(55)).await;
        second.await.unwrap();
        assert!(request_started.load(Ordering::Relaxed));
    }

    #[tokio::test(start_paused = true)]
    async fn queue_backed_retry_after_blocks_next_acquire_until_penalty_expires() {
        let limiter = DrafterOutboundLimiter::new(queue());
        let permit = limiter
            .acquire(
                DrafterRateLimitKey { chat_id: ChatId(1) },
                DrafterPriority::Final,
                DrafterRequestClass::Send,
            )
            .await
            .unwrap();
        permit
            .complete(DrafterPermitCompletion::RetryAfter {
                scope: DrafterRateLimitScope::Global,
                duration: Duration::from_secs(5),
            })
            .await;

        let next = limiter.acquire(
            DrafterRateLimitKey { chat_id: ChatId(1) },
            DrafterPriority::Final,
            DrafterRequestClass::Send,
        );
        tokio::pin!(next);
        assert!(futures::poll!(next.as_mut()).is_pending());

        tokio::time::advance(Duration::from_secs(5)).await;
        let permit = next.await.unwrap();
        permit.complete(DrafterPermitCompletion::Success).await;
    }
}
