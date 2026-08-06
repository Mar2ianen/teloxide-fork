//! The outbound request adaptor.
//!
//! [`ScheduledRequest`] runs any [`Request`] through an [`OutboundQueue`]:
//! the metadata is computed once at construction, a permit is acquired, the
//! inner request is executed **only after the grant** (nothing about the
//! inner request — e.g. a timeout captured at construction — can start
//! before the permit is held), the outcome is classified (success /
//! explicit-scope `RetryAfter` / failure) and the permit is completed
//! exactly once on every path. There is no automatic retry: the queue
//! records the penalty, retry stays the policy of the calling layer.
//!
//! [`Outbound`] wraps any [`Requester`] so that every method returns a
//! [`ScheduledRequest`]; [`ScheduledRequest::on_lane`] attaches a serial
//! ordering lane. The classification (`OutboundPayload`) is generated from
//! the Bot API schema for every payload, so the hint always reflects the
//! payload that is actually sent.

use std::{error::Error, fmt, future::IntoFuture, num::NonZeroU32};

use url::Url;

use crate::{
    errors::AsResponseParameters,
    outbound::{
        OutboundAcquireError, OutboundClass, OutboundCompletion, OutboundHint, OutboundLane,
        OutboundMetadata, OutboundOverrides, OutboundPayload, OutboundPriority, OutboundQueue,
        OutboundScope,
    },
    requests::{HasPayload, Output, Payload, Request, Requester},
    types::*,
};

/// Draft request classes used by the generated [`OutboundPayload`] impls
/// (spec §11.1). The taxonomy will be refined when `Throttle` migrates
/// onto the outbound queue; the scope classification is the strict part.
pub mod class {
    /// Read-only queries (`get_me`, ...).
    pub const READ: u64 = 1;
    /// Ordinary message sends.
    pub const MESSAGE_SEND: u64 = 2;
    /// Edits of existing messages.
    pub const MESSAGE_MUTATION: u64 = 3;
    /// Ephemeral typing indicators.
    pub const CHAT_ACTION: u64 = 4;
    /// Everything else (admin, stickers, payments, business accounts, ...).
    pub const OTHER: u64 = 5;
}

/// The error of a [`ScheduledRequest`]: either the inner request failed
/// after the grant, or the request could not be admitted to the queue and
/// never ran.
///
/// The adaptor is generic over the inner error: any `Requester` with its
/// own error type (not only [`crate::RequestError`]) can be scheduled, and
/// `RetryAfter` classification works through
/// [`AsResponseParameters::retry_after`], so error-rewriting adaptors
/// compose with the queue.
#[derive(Clone, Debug)]
pub enum OutboundRequestError<E> {
    /// The inner request failed after the grant. The original error is
    /// returned untouched.
    Inner(E),
    /// The request could not be admitted to the outbound queue: the queue
    /// was full, shut down, or the acquire was superseded before the
    /// grant. The inner request was never executed.
    Acquire(OutboundAcquireError),
}

impl<E: fmt::Display> fmt::Display for OutboundRequestError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner(error) => write!(f, "the scheduled request failed: {error}"),
            Self::Acquire(error) => write!(f, "the outbound queue rejected the request: {error}"),
        }
    }
}

impl<E: Error> Error for OutboundRequestError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            // `E` is intentionally not required to be `'static` (a
            // `Requester` error only needs `Error + Send +
            // AsResponseParameters`), so the inner source chain is not
            // exposed; the `Display` impl carries the inner message.
            Self::Inner(_) => None,
            Self::Acquire(error) => Some(error),
        }
    }
}

impl<E: AsResponseParameters> AsResponseParameters for OutboundRequestError<E> {
    fn response_parameters(&self) -> Option<ResponseParameters> {
        match self {
            Self::Inner(error) => error.response_parameters(),
            // A queue-level rejection is local; there is nothing Telegram
            // could act on.
            Self::Acquire(_) => None,
        }
    }
}

/// A request executed through an [`OutboundQueue`].
///
/// The payload object is created when the `Requester` method is called
/// (or passed to [`ScheduledRequest::new`]); the effective classification
/// is NOT computed then. At send time (`send`/`send_ref`) the effective
/// hint is computed from the FINAL payload — the payload is publicly
/// mutable until `send` (teloxide's `send_ref` flow changes `chat_id`
/// before sending), and batch weights depend on the current batch length —
/// and the request-level overrides are applied on top. The inner
/// `send`/`send_ref` future is created only after the permit is held.
/// Dropping the future before the grant cancels the pending job; dropping
/// it while the inner send future runs releases the permit as
/// `CancelledAfterGrant` (the lane, if any, is freed, the rate budget
/// stays consumed).
///
/// Per-request scheduling overrides ([`priority`](Self::priority),
/// [`weight`](Self::weight), [`class`](Self::class)) are fixed on the
/// `ScheduledRequest` and applied on top of the payload classification at
/// send time; the scope always follows the payload.
///
/// The type requires `Req: HasPayload`; the [`Request`] implementation
/// requires `Req: Clone + Send + 'static` and `Req::Payload::Output: Send`
/// (the completion barrier keeps the outcome across an `await`, so the
/// request future must be sendable). `send_ref` classifies the payload
/// at call time, clones the request into the plan and invokes the inner
/// `request.send_ref()` only AFTER the grant — so any side effect of
/// `Request::send_ref` (deadline capture, resource opening, ...) is
/// deferred past admission.
#[must_use = "Scheduled requests are lazy and do nothing unless sent or awaited"]
pub struct ScheduledRequest<Req: HasPayload> {
    request: Req,
    queue: OutboundQueue,
    lane: Option<OutboundLane>,
    overrides: OutboundOverrides,
}

impl<Req: HasPayload> Clone for ScheduledRequest<Req>
where
    Req: Clone,
{
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            queue: self.queue.clone(),
            lane: self.lane.clone(),
            overrides: self.overrides,
        }
    }
}

impl<Req: HasPayload> ScheduledRequest<Req> {
    pub fn new(request: Req, queue: OutboundQueue) -> Self {
        Self { request, queue, lane: None, overrides: OutboundOverrides::default() }
    }

    /// Attaches a serial ordering lane: at most one request of the lane is
    /// in flight and the lane is served strictly in enqueue order.
    pub fn on_lane(mut self, lane: &OutboundLane) -> Self {
        self.lane = Some(lane.clone());
        self
    }

    /// Overrides the payload's base priority (see [`OutboundOverrides`]).
    pub fn priority(mut self, priority: OutboundPriority) -> Self {
        self.overrides.priority = Some(priority);
        self
    }

    /// Overrides the payload's accounting weight (see
    /// [`OutboundOverrides`]). The weight must fit every window that
    /// applies to the scope, otherwise the acquire fails with
    /// `WeightExceedsWindow`.
    pub fn weight(mut self, weight: NonZeroU32) -> Self {
        self.overrides.weight = Some(weight);
        self
    }

    /// Overrides the payload's request class (see
    /// [`OutboundOverrides`]).
    pub fn class(mut self, class: OutboundClass) -> Self {
        self.overrides.class = Some(class);
        self
    }

    /// Replaces the whole override set (see [`OutboundOverrides`]).
    pub fn with_outbound_overrides(mut self, overrides: OutboundOverrides) -> Self {
        self.overrides = overrides;
        self
    }

    /// The EFFECTIVE classification of the CURRENT payload: the base
    /// payload hint (computed on demand, so it always reflects setter
    /// calls and payload mutations) with the request-level overrides
    /// applied. The metadata actually used for admission is computed the
    /// same way at send time from the final payload.
    pub fn metadata(&self) -> OutboundHint
    where
        Req::Payload: OutboundPayload,
    {
        let hint = self.request.payload_ref().outbound_hint();
        OutboundHint {
            scope: hint.scope,
            class: self.overrides.class.unwrap_or(hint.class),
            priority: self.overrides.priority.unwrap_or(hint.priority),
            weight: self.overrides.weight.unwrap_or(hint.weight),
        }
    }

    /// Unwraps the inner request.
    pub fn into_inner(self) -> Req {
        self.request
    }
}

impl<Req: HasPayload + Request> HasPayload for ScheduledRequest<Req> {
    type Payload = Req::Payload;

    fn payload_mut(&mut self) -> &mut Self::Payload {
        self.request.payload_mut()
    }

    fn payload_ref(&self) -> &Self::Payload {
        self.request.payload_ref()
    }
}

impl<Req: HasPayload + Request> std::ops::Deref for ScheduledRequest<Req> {
    type Target = Req::Payload;

    fn deref(&self) -> &Self::Target {
        self.payload_ref()
    }
}

impl<Req: HasPayload + Request> std::ops::DerefMut for ScheduledRequest<Req> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.payload_mut()
    }
}

impl<Req> Request for ScheduledRequest<Req>
where
    Req: HasPayload + 'static + std::marker::Send + Request + Clone,
    Req::Err: AsResponseParameters,
    // The request future keeps the outcome across the completion-barrier
    // await; every real payload output is `Send`, so this is a formality.
    Req::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
{
    type Err = OutboundRequestError<Req::Err>;
    type Send = Send<Req>;
    type SendRef = SendRef<Req>;

    fn send(self) -> Self::Send {
        Send::new(self)
    }

    fn send_ref(&self) -> Self::SendRef {
        // The payload is classified NOW (the caller may mutate it until
        // this point) and a clone of the request is handed to the plan.
        // The inner `request.send_ref()` is called only AFTER the grant
        // (inside the future): `Request::send_ref` is only *recommended*
        // to be lazy, so any side effect it has (opening a resource,
        // capturing a deadline, ...) must happen after admission.
        let hint = self.request.payload_ref().outbound_hint();
        let metadata = effective_metadata(hint, self.overrides);
        SendRef::new(SendRefPlan {
            request: self.request.clone(),
            queue: self.queue.clone(),
            lane: self.lane.clone(),
            metadata,
        })
    }
}

impl<Req> IntoFuture for ScheduledRequest<Req>
where
    Req: HasPayload + 'static + std::marker::Send + Request + Clone,
    Req::Err: AsResponseParameters,
    Req::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
{
    type Output = Result<Output<Req>, OutboundRequestError<Req::Err>>;
    type IntoFuture = <Self as Request>::Send;

    fn into_future(self) -> Self::IntoFuture {
        self.send()
    }
}

req_future! {
    def: |it: ScheduledRequest<U>| {
        async move {
            // The hint is computed from the FINAL payload: the payload is
            // publicly mutable until send (teloxide's `send_ref` flow
            // changes `chat_id` before sending), so admission, lanes and
            // `RetryAfter` penalties always follow what is actually sent.
            let ScheduledRequest { request, queue, lane, overrides } = it;
            let hint = request.payload_ref().outbound_hint();
            let metadata = effective_metadata(hint, overrides);
            // The classifier below runs after the acquire consumed the
            // metadata, so the policy copy is kept separately.
            let policy_metadata = metadata.clone();
            let acquire = match &lane {
                Some(lane) => lane.acquire(metadata),
                None => queue.handle().acquire(metadata),
            };
            let permit = match acquire.await {
                Ok(permit) => permit,
                Err(error) => return Err(OutboundRequestError::Acquire(error)),
            };
            // The inner send future is created and polled only now, after
            // the grant: nothing about it (e.g. a timeout captured at
            // construction) can start before the permit is held.
            let outcome = request.send().await;
            match &outcome {
                Ok(_) => {
                    // The completion barrier: the future resolves only
                    // after the actor applied the outcome, so a subsequent
                    // acquire from the same caller sees the penalty.
                    permit.complete_and_await(OutboundCompletion::Success).await
                }
                Err(error) => match error.retry_after() {
                    Some(seconds) => {
                        permit.complete_and_await(OutboundCompletion::RetryAfter {
                            scope: retry_after_scope(&policy_metadata, error),
                            duration: seconds.duration(),
                        }).await
                    }
                    None => permit.complete_and_await(OutboundCompletion::Failed).await,
                },
            }
            outcome.map_err(OutboundRequestError::Inner)
        }
    }
    pub Send<U> (inner0) -> Result<Output<U>, OutboundRequestError<<U as Request>::Err>>
    where
        U: 'static,
        U: std::marker::Send + Request,
        U::Err: AsResponseParameters,
        // The future keeps the outcome across the completion barrier
        // await, so the payload output must itself be sendable.
        U::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
}

/// Everything a `send_ref` execution needs, owned upfront: a clone of the
/// request, the queue, the lane and the metadata classified at call time.
/// The inner `request.send_ref()` is invoked inside the future, after the
/// permit is held.
struct SendRefPlan<U: Request> {
    request: U,
    queue: OutboundQueue,
    lane: Option<OutboundLane>,
    metadata: OutboundMetadata,
}

req_future! {
    def: |it: SendRefPlan<U>| {
        async move {
            let policy_metadata = it.metadata.clone();
            let acquire = match &it.lane {
                Some(lane) => lane.acquire(it.metadata),
                None => it.queue.handle().acquire(it.metadata),
            };
            let permit = match acquire.await {
                Ok(permit) => permit,
                Err(error) => return Err(OutboundRequestError::Acquire(error)),
            };
            // The inner `send_ref` is called only now, after the grant:
            // any side effect of the call (deadline capture, resource
            // opening, ...) is deferred past admission.
            let outcome = it.request.send_ref().await;
            match &outcome {
                Ok(_) => {
                    permit.complete_and_await(OutboundCompletion::Success).await
                }
                Err(error) => match error.retry_after() {
                    Some(seconds) => {
                        permit.complete_and_await(OutboundCompletion::RetryAfter {
                            scope: retry_after_scope(&policy_metadata, error),
                            duration: seconds.duration(),
                        }).await
                    }
                    None => permit.complete_and_await(OutboundCompletion::Failed).await,
                },
            }
            outcome.map_err(OutboundRequestError::Inner)
        }
    }
    pub SendRef<U> (inner2) -> Result<Output<U>, OutboundRequestError<<U as Request>::Err>>
    where
        U: 'static,
        U: std::marker::Send + Request + Clone,
        U::Err: AsResponseParameters,
        U::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
}

/// Builds the scheduler metadata from the effective classification: the
/// payload hint with request-level overrides applied. Scope always comes
/// from the payload — it is tied to what is actually sent and is
/// deliberately not overridable.
fn effective_metadata(hint: OutboundHint, overrides: OutboundOverrides) -> OutboundMetadata {
    OutboundMetadata {
        scope: hint.scope,
        class: overrides.class.unwrap_or(hint.class),
        priority: overrides.priority.unwrap_or(hint.priority),
        weight: overrides.weight.unwrap_or(hint.weight),
    }
}

/// Classifies the penalty scope of a `RetryAfter` outcome.
///
/// This is the single place where the scope decision lives: it receives the
/// request context (metadata plus the actual error) instead of copying
/// `metadata.scope` at the call site.
///
/// The current policy is explicit and temporary: the penalty follows the
/// request's own scope (`metadata.scope`). A future policy that promotes
/// some classes to the global scope (a chat-scoped request hitting a global
/// flood limit) plugs in here without touching the execution path; the
/// `_error` argument exists for exactly that classifier.
fn retry_after_scope<E>(metadata: &OutboundMetadata, _error: &E) -> OutboundScope {
    metadata.scope.clone()
}

/// An outbound-scheduling adaptor over any [`Requester`].
///
/// Every method builds the request through the inner requester and returns
/// a [`ScheduledRequest`] classified by the payload. This is the primary
/// entry point:
///
/// The `Requester` impl requires every inner request type to be
/// `Clone + Send + 'static` (and its payload to be `Send`-output and
/// classifiable): `send_ref` clones the request at call time into an
/// owned plan and invokes the inner `send_ref` only after the grant —
/// the same clone contract `Throttle` uses. The `'static` restriction
/// matches `JsonRequest` (see its `FIXME` — teloxide has no
/// non-`'static` payloads), and it is the request TYPES that carry it: a
/// requester whose request types borrow data cannot be wrapped, even
/// though `OutboundRequestError<E>` itself supports non-`'static` inner
/// errors.
///
/// ```no_run
/// use teloxide_core::{
///     outbound::{Outbound, OutboundQueue, OutboundSettings},
///     requests::Requester,
///     Bot,
/// };
/// # async fn f() {
/// let queue = OutboundQueue::new_spawn(OutboundSettings::default()).unwrap();
/// let bot = Outbound::new(Bot::from_env(), queue);
/// let _ = bot.get_me().await;
/// # }
/// ```
#[derive(Clone)]
pub struct Outbound<R> {
    inner: R,
    queue: OutboundQueue,
}

impl<R> Outbound<R> {
    pub fn new(inner: R, queue: OutboundQueue) -> Self {
        Self { inner, queue }
    }

    /// The wrapped requester.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// Unwraps the inner requester.
    pub fn into_inner(self) -> R {
        self.inner
    }

    /// The queue requests are scheduled through.
    pub fn queue(&self) -> &OutboundQueue {
        &self.queue
    }
}

/// Builds a [`ScheduledRequest`] from the inner requester's method call.
macro_rules! f {
    ($m:ident $this:ident ($($arg:ident : $T:ty),*)) => {
        ScheduledRequest::new($this.inner().$m($($arg),*), $this.queue.clone())
    };
}

macro_rules! fty {
    ($T:ident) => {
        ScheduledRequest<B::$T>
    };
}

impl<B: Requester> Requester for Outbound<B>
where
    B::AddStickerToSet: Clone + std::marker::Send + 'static,
    <B::AddStickerToSet as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::AnswerCallbackQuery: Clone + std::marker::Send + 'static,
    <B::AnswerCallbackQuery as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::AnswerChatJoinRequestQuery: Clone + std::marker::Send + 'static,
    <B::AnswerChatJoinRequestQuery as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::AnswerGuestQuery: Clone + std::marker::Send + 'static,
    <B::AnswerGuestQuery as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::AnswerInlineQuery: Clone + std::marker::Send + 'static,
    <B::AnswerInlineQuery as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::AnswerPreCheckoutQuery: Clone + std::marker::Send + 'static,
    <B::AnswerPreCheckoutQuery as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::AnswerShippingQuery: Clone + std::marker::Send + 'static,
    <B::AnswerShippingQuery as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::AnswerWebAppQuery: Clone + std::marker::Send + 'static,
    <B::AnswerWebAppQuery as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ApproveChatJoinRequest: Clone + std::marker::Send + 'static,
    <B::ApproveChatJoinRequest as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ApproveSuggestedPost: Clone + std::marker::Send + 'static,
    <B::ApproveSuggestedPost as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::BanChatMember: Clone + std::marker::Send + 'static,
    <B::BanChatMember as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::BanChatSenderChat: Clone + std::marker::Send + 'static,
    <B::BanChatSenderChat as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::Close: Clone + std::marker::Send + 'static,
    <B::Close as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::CloseForumTopic: Clone + std::marker::Send + 'static,
    <B::CloseForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::CloseGeneralForumTopic: Clone + std::marker::Send + 'static,
    <B::CloseGeneralForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ConvertGiftToStars: Clone + std::marker::Send + 'static,
    <B::ConvertGiftToStars as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::CopyMessage: Clone + std::marker::Send + 'static,
    <B::CopyMessage as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::CopyMessages: Clone + std::marker::Send + 'static,
    <B::CopyMessages as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::CreateChatInviteLink: Clone + std::marker::Send + 'static,
    <B::CreateChatInviteLink as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::CreateChatSubscriptionInviteLink: Clone + std::marker::Send + 'static,
    <B::CreateChatSubscriptionInviteLink as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::CreateForumTopic: Clone + std::marker::Send + 'static,
    <B::CreateForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::CreateInvoiceLink: Clone + std::marker::Send + 'static,
    <B::CreateInvoiceLink as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::CreateNewStickerSet: Clone + std::marker::Send + 'static,
    <B::CreateNewStickerSet as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeclineChatJoinRequest: Clone + std::marker::Send + 'static,
    <B::DeclineChatJoinRequest as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeclineSuggestedPost: Clone + std::marker::Send + 'static,
    <B::DeclineSuggestedPost as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteAllMessageReactions: Clone + std::marker::Send + 'static,
    <B::DeleteAllMessageReactions as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteBusinessMessages: Clone + std::marker::Send + 'static,
    <B::DeleteBusinessMessages as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteChatPhoto: Clone + std::marker::Send + 'static,
    <B::DeleteChatPhoto as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteChatStickerSet: Clone + std::marker::Send + 'static,
    <B::DeleteChatStickerSet as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteEphemeralMessage: Clone + std::marker::Send + 'static,
    <B::DeleteEphemeralMessage as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteForumTopic: Clone + std::marker::Send + 'static,
    <B::DeleteForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteMessage: Clone + std::marker::Send + 'static,
    <B::DeleteMessage as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteMessageReaction: Clone + std::marker::Send + 'static,
    <B::DeleteMessageReaction as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteMessages: Clone + std::marker::Send + 'static,
    <B::DeleteMessages as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteMyCommands: Clone + std::marker::Send + 'static,
    <B::DeleteMyCommands as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteStickerFromSet: Clone + std::marker::Send + 'static,
    <B::DeleteStickerFromSet as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteStickerSet: Clone + std::marker::Send + 'static,
    <B::DeleteStickerSet as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteStory: Clone + std::marker::Send + 'static,
    <B::DeleteStory as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::DeleteWebhook: Clone + std::marker::Send + 'static,
    <B::DeleteWebhook as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditChatInviteLink: Clone + std::marker::Send + 'static,
    <B::EditChatInviteLink as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditChatSubscriptionInviteLink: Clone + std::marker::Send + 'static,
    <B::EditChatSubscriptionInviteLink as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditEphemeralMessageCaption: Clone + std::marker::Send + 'static,
    <B::EditEphemeralMessageCaption as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditEphemeralMessageMedia: Clone + std::marker::Send + 'static,
    <B::EditEphemeralMessageMedia as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditEphemeralMessageReplyMarkup: Clone + std::marker::Send + 'static,
    <B::EditEphemeralMessageReplyMarkup as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditEphemeralMessageText: Clone + std::marker::Send + 'static,
    <B::EditEphemeralMessageText as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditForumTopic: Clone + std::marker::Send + 'static,
    <B::EditForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditGeneralForumTopic: Clone + std::marker::Send + 'static,
    <B::EditGeneralForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageCaption: Clone + std::marker::Send + 'static,
    <B::EditMessageCaption as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageCaptionInline: Clone + std::marker::Send + 'static,
    <B::EditMessageCaptionInline as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageChecklist: Clone + std::marker::Send + 'static,
    <B::EditMessageChecklist as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageLiveLocation: Clone + std::marker::Send + 'static,
    <B::EditMessageLiveLocation as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageLiveLocationInline: Clone + std::marker::Send + 'static,
    <B::EditMessageLiveLocationInline as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageMedia: Clone + std::marker::Send + 'static,
    <B::EditMessageMedia as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageMediaInline: Clone + std::marker::Send + 'static,
    <B::EditMessageMediaInline as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageReplyMarkup: Clone + std::marker::Send + 'static,
    <B::EditMessageReplyMarkup as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageReplyMarkupInline: Clone + std::marker::Send + 'static,
    <B::EditMessageReplyMarkupInline as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageText: Clone + std::marker::Send + 'static,
    <B::EditMessageText as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditMessageTextInline: Clone + std::marker::Send + 'static,
    <B::EditMessageTextInline as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditStory: Clone + std::marker::Send + 'static,
    <B::EditStory as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::EditUserStarSubscription: Clone + std::marker::Send + 'static,
    <B::EditUserStarSubscription as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ExportChatInviteLink: Clone + std::marker::Send + 'static,
    <B::ExportChatInviteLink as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ForwardMessage: Clone + std::marker::Send + 'static,
    <B::ForwardMessage as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ForwardMessages: Clone + std::marker::Send + 'static,
    <B::ForwardMessages as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetAvailableGifts: Clone + std::marker::Send + 'static,
    <B::GetAvailableGifts as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetBusinessAccountGifts: Clone + std::marker::Send + 'static,
    <B::GetBusinessAccountGifts as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetBusinessAccountStarBalance: Clone + std::marker::Send + 'static,
    <B::GetBusinessAccountStarBalance as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetBusinessConnection: Clone + std::marker::Send + 'static,
    <B::GetBusinessConnection as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetChat: Clone + std::marker::Send + 'static,
    <B::GetChat as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetChatAdministrators: Clone + std::marker::Send + 'static,
    <B::GetChatAdministrators as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetChatGifts: Clone + std::marker::Send + 'static,
    <B::GetChatGifts as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetChatMember: Clone + std::marker::Send + 'static,
    <B::GetChatMember as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetChatMemberCount: Clone + std::marker::Send + 'static,
    <B::GetChatMemberCount as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetChatMembersCount: Clone + std::marker::Send + 'static,
    <B::GetChatMembersCount as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetChatMenuButton: Clone + std::marker::Send + 'static,
    <B::GetChatMenuButton as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetCustomEmojiStickers: Clone + std::marker::Send + 'static,
    <B::GetCustomEmojiStickers as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetFile: Clone + std::marker::Send + 'static,
    <B::GetFile as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetForumTopicIconStickers: Clone + std::marker::Send + 'static,
    <B::GetForumTopicIconStickers as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetGameHighScores: Clone + std::marker::Send + 'static,
    <B::GetGameHighScores as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetManagedBotAccessSettings: Clone + std::marker::Send + 'static,
    <B::GetManagedBotAccessSettings as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetManagedBotToken: Clone + std::marker::Send + 'static,
    <B::GetManagedBotToken as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetMe: Clone + std::marker::Send + 'static,
    <B::GetMe as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetMyCommands: Clone + std::marker::Send + 'static,
    <B::GetMyCommands as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetMyDefaultAdministratorRights: Clone + std::marker::Send + 'static,
    <B::GetMyDefaultAdministratorRights as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetMyDescription: Clone + std::marker::Send + 'static,
    <B::GetMyDescription as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetMyName: Clone + std::marker::Send + 'static,
    <B::GetMyName as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetMyShortDescription: Clone + std::marker::Send + 'static,
    <B::GetMyShortDescription as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetMyStarBalance: Clone + std::marker::Send + 'static,
    <B::GetMyStarBalance as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetStarTransactions: Clone + std::marker::Send + 'static,
    <B::GetStarTransactions as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetStickerSet: Clone + std::marker::Send + 'static,
    <B::GetStickerSet as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetUpdates: Clone + std::marker::Send + 'static,
    <B::GetUpdates as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetUserChatBoosts: Clone + std::marker::Send + 'static,
    <B::GetUserChatBoosts as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetUserGifts: Clone + std::marker::Send + 'static,
    <B::GetUserGifts as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetUserPersonalChatMessages: Clone + std::marker::Send + 'static,
    <B::GetUserPersonalChatMessages as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetUserProfileAudios: Clone + std::marker::Send + 'static,
    <B::GetUserProfileAudios as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetUserProfilePhotos: Clone + std::marker::Send + 'static,
    <B::GetUserProfilePhotos as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GetWebhookInfo: Clone + std::marker::Send + 'static,
    <B::GetWebhookInfo as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::GiftPremiumSubscription: Clone + std::marker::Send + 'static,
    <B::GiftPremiumSubscription as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::HideGeneralForumTopic: Clone + std::marker::Send + 'static,
    <B::HideGeneralForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::KickChatMember: Clone + std::marker::Send + 'static,
    <B::KickChatMember as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::LeaveChat: Clone + std::marker::Send + 'static,
    <B::LeaveChat as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::LogOut: Clone + std::marker::Send + 'static,
    <B::LogOut as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::PinChatMessage: Clone + std::marker::Send + 'static,
    <B::PinChatMessage as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::PostStory: Clone + std::marker::Send + 'static,
    <B::PostStory as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::PromoteChatMember: Clone + std::marker::Send + 'static,
    <B::PromoteChatMember as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ReadBusinessMessage: Clone + std::marker::Send + 'static,
    <B::ReadBusinessMessage as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::RefundStarPayment: Clone + std::marker::Send + 'static,
    <B::RefundStarPayment as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::RemoveBusinessAccountProfilePhoto: Clone + std::marker::Send + 'static,
    <B::RemoveBusinessAccountProfilePhoto as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::RemoveChatVerification: Clone + std::marker::Send + 'static,
    <B::RemoveChatVerification as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::RemoveMyProfilePhoto: Clone + std::marker::Send + 'static,
    <B::RemoveMyProfilePhoto as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::RemoveUserVerification: Clone + std::marker::Send + 'static,
    <B::RemoveUserVerification as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ReopenForumTopic: Clone + std::marker::Send + 'static,
    <B::ReopenForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ReopenGeneralForumTopic: Clone + std::marker::Send + 'static,
    <B::ReopenGeneralForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ReplaceManagedBotToken: Clone + std::marker::Send + 'static,
    <B::ReplaceManagedBotToken as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::ReplaceStickerInSet: Clone + std::marker::Send + 'static,
    <B::ReplaceStickerInSet as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::RepostStory: Clone + std::marker::Send + 'static,
    <B::RepostStory as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::RestrictChatMember: Clone + std::marker::Send + 'static,
    <B::RestrictChatMember as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::RevokeChatInviteLink: Clone + std::marker::Send + 'static,
    <B::RevokeChatInviteLink as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SavePreparedInlineMessage: Clone + std::marker::Send + 'static,
    <B::SavePreparedInlineMessage as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SavePreparedKeyboardButton: Clone + std::marker::Send + 'static,
    <B::SavePreparedKeyboardButton as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendAnimation: Clone + std::marker::Send + 'static,
    <B::SendAnimation as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendAudio: Clone + std::marker::Send + 'static,
    <B::SendAudio as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendChatAction: Clone + std::marker::Send + 'static,
    <B::SendChatAction as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendChatJoinRequestWebApp: Clone + std::marker::Send + 'static,
    <B::SendChatJoinRequestWebApp as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendChecklist: Clone + std::marker::Send + 'static,
    <B::SendChecklist as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendContact: Clone + std::marker::Send + 'static,
    <B::SendContact as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendDice: Clone + std::marker::Send + 'static,
    <B::SendDice as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendDocument: Clone + std::marker::Send + 'static,
    <B::SendDocument as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendGame: Clone + std::marker::Send + 'static,
    <B::SendGame as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendGift: Clone + std::marker::Send + 'static,
    <B::SendGift as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendGiftChat: Clone + std::marker::Send + 'static,
    <B::SendGiftChat as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendInvoice: Clone + std::marker::Send + 'static,
    <B::SendInvoice as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendLivePhoto: Clone + std::marker::Send + 'static,
    <B::SendLivePhoto as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendLocation: Clone + std::marker::Send + 'static,
    <B::SendLocation as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendMediaGroup: Clone + std::marker::Send + 'static,
    <B::SendMediaGroup as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendMessage: Clone + std::marker::Send + 'static,
    <B::SendMessage as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendMessageDraft: Clone + std::marker::Send + 'static,
    <B::SendMessageDraft as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendPaidMedia: Clone + std::marker::Send + 'static,
    <B::SendPaidMedia as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendPhoto: Clone + std::marker::Send + 'static,
    <B::SendPhoto as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendPoll: Clone + std::marker::Send + 'static,
    <B::SendPoll as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendRichMessage: Clone + std::marker::Send + 'static,
    <B::SendRichMessage as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendRichMessageDraft: Clone + std::marker::Send + 'static,
    <B::SendRichMessageDraft as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendSticker: Clone + std::marker::Send + 'static,
    <B::SendSticker as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendVenue: Clone + std::marker::Send + 'static,
    <B::SendVenue as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendVideo: Clone + std::marker::Send + 'static,
    <B::SendVideo as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendVideoNote: Clone + std::marker::Send + 'static,
    <B::SendVideoNote as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SendVoice: Clone + std::marker::Send + 'static,
    <B::SendVoice as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetBusinessAccountBio: Clone + std::marker::Send + 'static,
    <B::SetBusinessAccountBio as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetBusinessAccountGiftSettings: Clone + std::marker::Send + 'static,
    <B::SetBusinessAccountGiftSettings as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetBusinessAccountName: Clone + std::marker::Send + 'static,
    <B::SetBusinessAccountName as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetBusinessAccountProfilePhoto: Clone + std::marker::Send + 'static,
    <B::SetBusinessAccountProfilePhoto as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetBusinessAccountUsername: Clone + std::marker::Send + 'static,
    <B::SetBusinessAccountUsername as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetChatAdministratorCustomTitle: Clone + std::marker::Send + 'static,
    <B::SetChatAdministratorCustomTitle as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetChatDescription: Clone + std::marker::Send + 'static,
    <B::SetChatDescription as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetChatMemberTag: Clone + std::marker::Send + 'static,
    <B::SetChatMemberTag as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetChatMenuButton: Clone + std::marker::Send + 'static,
    <B::SetChatMenuButton as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetChatPermissions: Clone + std::marker::Send + 'static,
    <B::SetChatPermissions as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetChatPhoto: Clone + std::marker::Send + 'static,
    <B::SetChatPhoto as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetChatStickerSet: Clone + std::marker::Send + 'static,
    <B::SetChatStickerSet as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetChatTitle: Clone + std::marker::Send + 'static,
    <B::SetChatTitle as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetCustomEmojiStickerSetThumbnail: Clone + std::marker::Send + 'static,
    <B::SetCustomEmojiStickerSetThumbnail as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetGameScore: Clone + std::marker::Send + 'static,
    <B::SetGameScore as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetGameScoreInline: Clone + std::marker::Send + 'static,
    <B::SetGameScoreInline as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetManagedBotAccessSettings: Clone + std::marker::Send + 'static,
    <B::SetManagedBotAccessSettings as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetMessageReaction: Clone + std::marker::Send + 'static,
    <B::SetMessageReaction as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetMyCommands: Clone + std::marker::Send + 'static,
    <B::SetMyCommands as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetMyDefaultAdministratorRights: Clone + std::marker::Send + 'static,
    <B::SetMyDefaultAdministratorRights as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetMyDescription: Clone + std::marker::Send + 'static,
    <B::SetMyDescription as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetMyName: Clone + std::marker::Send + 'static,
    <B::SetMyName as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetMyProfilePhoto: Clone + std::marker::Send + 'static,
    <B::SetMyProfilePhoto as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetMyShortDescription: Clone + std::marker::Send + 'static,
    <B::SetMyShortDescription as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetPassportDataErrors: Clone + std::marker::Send + 'static,
    <B::SetPassportDataErrors as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetStickerEmojiList: Clone + std::marker::Send + 'static,
    <B::SetStickerEmojiList as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetStickerKeywords: Clone + std::marker::Send + 'static,
    <B::SetStickerKeywords as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetStickerMaskPosition: Clone + std::marker::Send + 'static,
    <B::SetStickerMaskPosition as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetStickerPositionInSet: Clone + std::marker::Send + 'static,
    <B::SetStickerPositionInSet as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetStickerSetThumbnail: Clone + std::marker::Send + 'static,
    <B::SetStickerSetThumbnail as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetStickerSetTitle: Clone + std::marker::Send + 'static,
    <B::SetStickerSetTitle as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetUserEmojiStatus: Clone + std::marker::Send + 'static,
    <B::SetUserEmojiStatus as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::SetWebhook: Clone + std::marker::Send + 'static,
    <B::SetWebhook as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::StopMessageLiveLocation: Clone + std::marker::Send + 'static,
    <B::StopMessageLiveLocation as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::StopMessageLiveLocationInline: Clone + std::marker::Send + 'static,
    <B::StopMessageLiveLocationInline as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::StopPoll: Clone + std::marker::Send + 'static,
    <B::StopPoll as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::TransferBusinessAccountStars: Clone + std::marker::Send + 'static,
    <B::TransferBusinessAccountStars as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::TransferGift: Clone + std::marker::Send + 'static,
    <B::TransferGift as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::UnbanChatMember: Clone + std::marker::Send + 'static,
    <B::UnbanChatMember as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::UnbanChatSenderChat: Clone + std::marker::Send + 'static,
    <B::UnbanChatSenderChat as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::UnhideGeneralForumTopic: Clone + std::marker::Send + 'static,
    <B::UnhideGeneralForumTopic as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::UnpinAllChatMessages: Clone + std::marker::Send + 'static,
    <B::UnpinAllChatMessages as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::UnpinAllForumTopicMessages: Clone + std::marker::Send + 'static,
    <B::UnpinAllForumTopicMessages as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::UnpinAllGeneralForumTopicMessages: Clone + std::marker::Send + 'static,
    <B::UnpinAllGeneralForumTopicMessages as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::UnpinChatMessage: Clone + std::marker::Send + 'static,
    <B::UnpinChatMessage as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::UpgradeGift: Clone + std::marker::Send + 'static,
    <B::UpgradeGift as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::UploadStickerFile: Clone + std::marker::Send + 'static,
    <B::UploadStickerFile as HasPayload>::Payload:
        Payload<Output: std::marker::Send> + OutboundPayload,
    B::VerifyChat: Clone + std::marker::Send + 'static,
    <B::VerifyChat as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
    B::VerifyUser: Clone + std::marker::Send + 'static,
    <B::VerifyUser as HasPayload>::Payload: Payload<Output: std::marker::Send> + OutboundPayload,
{
    type Err = OutboundRequestError<B::Err>;

    requester_forward! {
    add_sticker_to_set,
                answer_callback_query,
                answer_chat_join_request_query,
                answer_guest_query,
                answer_inline_query,
                answer_pre_checkout_query,
                answer_shipping_query,
                answer_web_app_query,
                approve_chat_join_request,
                approve_suggested_post,
                ban_chat_member,
                ban_chat_sender_chat,
                close,
                close_forum_topic,
                close_general_forum_topic,
                convert_gift_to_stars,
                copy_message,
                copy_messages,
                create_chat_invite_link,
                create_chat_subscription_invite_link,
                create_forum_topic,
                create_invoice_link,
                create_new_sticker_set,
                decline_chat_join_request,
                decline_suggested_post,
                delete_all_message_reactions,
                delete_business_messages,
                delete_chat_photo,
                delete_chat_sticker_set,
                delete_ephemeral_message,
                delete_forum_topic,
                delete_message,
                delete_message_reaction,
                delete_messages,
                delete_my_commands,
                delete_sticker_from_set,
                delete_sticker_set,
                delete_story,
                delete_webhook,
                edit_chat_invite_link,
                edit_chat_subscription_invite_link,
                edit_ephemeral_message_caption,
                edit_ephemeral_message_media,
                edit_ephemeral_message_reply_markup,
                edit_ephemeral_message_text,
                edit_forum_topic,
                edit_general_forum_topic,
                edit_message_caption,
                edit_message_caption_inline,
                edit_message_checklist,
                edit_message_live_location,
                edit_message_live_location_inline,
                edit_message_media,
                edit_message_media_inline,
                edit_message_reply_markup,
                edit_message_reply_markup_inline,
                edit_message_text,
                edit_message_text_inline,
                edit_story,
                edit_user_star_subscription,
                export_chat_invite_link,
                forward_message,
                forward_messages,
                get_available_gifts,
                get_business_account_gifts,
                get_business_account_star_balance,
                get_business_connection,
                get_chat,
                get_chat_administrators,
                get_chat_gifts,
                get_chat_member,
                get_chat_member_count,
                get_chat_members_count,
                get_chat_menu_button,
                get_custom_emoji_stickers,
                get_file,
                get_forum_topic_icon_stickers,
                get_game_high_scores,
                get_managed_bot_access_settings,
                get_managed_bot_token,
                get_me,
                get_my_commands,
                get_my_default_administrator_rights,
                get_my_description,
                get_my_name,
                get_my_short_description,
                get_my_star_balance,
                get_star_transactions,
                get_sticker_set,
                get_updates,
                get_user_chat_boosts,
                get_user_gifts,
                get_user_personal_chat_messages,
                get_user_profile_audios,
                get_user_profile_photos,
                get_webhook_info,
                gift_premium_subscription,
                hide_general_forum_topic,
                kick_chat_member,
                leave_chat,
                log_out,
                pin_chat_message,
                post_story,
                promote_chat_member,
                read_business_message,
                refund_star_payment,
                remove_business_account_profile_photo,
                remove_chat_verification,
                remove_my_profile_photo,
                remove_user_verification,
                reopen_forum_topic,
                reopen_general_forum_topic,
                replace_managed_bot_token,
                replace_sticker_in_set,
                repost_story,
                restrict_chat_member,
                revoke_chat_invite_link,
                save_prepared_inline_message,
                save_prepared_keyboard_button,
                send_animation,
                send_audio,
                send_chat_action,
                send_chat_join_request_web_app,
                send_checklist,
                send_contact,
                send_dice,
                send_document,
                send_game,
                send_gift,
                send_gift_chat,
                send_invoice,
                send_live_photo,
                send_location,
                send_media_group,
                send_message,
                send_message_draft,
                send_paid_media,
                send_photo,
                send_poll,
                send_rich_message,
                send_rich_message_draft,
                send_sticker,
                send_venue,
                send_video,
                send_video_note,
                send_voice,
                set_business_account_bio,
                set_business_account_gift_settings,
                set_business_account_name,
                set_business_account_profile_photo,
                set_business_account_username,
                set_chat_administrator_custom_title,
                set_chat_description,
                set_chat_member_tag,
                set_chat_menu_button,
                set_chat_permissions,
                set_chat_photo,
                set_chat_sticker_set,
                set_chat_title,
                set_custom_emoji_sticker_set_thumbnail,
                set_game_score,
                set_game_score_inline,
                set_managed_bot_access_settings,
                set_message_reaction,
                set_my_commands,
                set_my_default_administrator_rights,
                set_my_description,
                set_my_name,
                set_my_profile_photo,
                set_my_short_description,
                set_passport_data_errors,
                set_sticker_emoji_list,
                set_sticker_keywords,
                set_sticker_mask_position,
                set_sticker_position_in_set,
                set_sticker_set_thumbnail,
                set_sticker_set_title,
                set_user_emoji_status,
                set_webhook,
                stop_message_live_location,
                stop_message_live_location_inline,
                stop_poll,
                transfer_business_account_stars,
                transfer_gift,
                unban_chat_member,
                unban_chat_sender_chat,
                unhide_general_forum_topic,
                unpin_all_chat_messages,
                unpin_all_forum_topic_messages,
                unpin_all_general_forum_topic_messages,
                unpin_chat_message,
                upgrade_gift,
                upload_sticker_file,
                verify_chat,
                verify_user,
                => f, fty
        }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        marker::PhantomData,
        num::NonZeroU32,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::sync::Notify;
    use url::Url;

    use super::*;
    use crate::{
        errors::RequestError,
        outbound::{
            classify::scope_of_recipient, AgingPolicy, OutboundChatKey, OutboundClass,
            OutboundLimits, OutboundOverrides, OutboundPriority, OutboundSettings, WindowLimit,
        },
        payloads::*,
        requests::Payload,
    };

    // ---------- fake requester / fake request ----------

    /// A fake request whose `Send` future counts polls and either returns
    /// the programmed result immediately or waits for a release signal.
    /// The output type is the real `P::Output` of the payload, so the fake
    /// is only usable with payloads whose output is cheap to construct
    /// (`SendChatAction` -> `True` in these tests).
    #[derive(Clone)]
    struct FakeRequest<P: Payload, FakeErr = RequestError> {
        _payload: P,
        result: Result<P::Output, FakeErr>,
        polls: Arc<AtomicUsize>,
        release: Option<Arc<Notify>>,
        /// Signalled on the first poll of the inner `Send` future, proving
        /// that the request actually entered the inner execution.
        entered: Option<Arc<Notify>>,
        /// Incremented on EVERY `Request::send_ref` CALL (not poll): pins
        /// the deferred-construction contract of the scheduled adaptor.
        send_ref_calls: Arc<AtomicUsize>,
    }

    impl<P: Payload, FakeErr> HasPayload for FakeRequest<P, FakeErr> {
        type Payload = P;

        fn payload_mut(&mut self) -> &mut Self::Payload {
            &mut self._payload
        }

        fn payload_ref(&self) -> &Self::Payload {
            &self._payload
        }
    }

    impl<P, FakeErr> Request for FakeRequest<P, FakeErr>
    where
        P: Payload<Output: Clone + std::marker::Send> + std::marker::Send + 'static,
        FakeErr: Error + std::marker::Send + AsResponseParameters + Clone + 'static,
    {
        type Err = FakeErr;
        type Send = FakeSend<P, FakeErr>;
        type SendRef = FakeSend<P, FakeErr>;

        fn send(self) -> Self::Send {
            FakeSend {
                result: self.result,
                polls: self.polls,
                release: self.release,
                entered: self.entered,
                _payload: PhantomData,
            }
        }

        fn send_ref(&self) -> Self::SendRef {
            self.send_ref_calls.fetch_add(1, Ordering::SeqCst);
            FakeSend {
                result: self.result.clone(),
                polls: self.polls.clone(),
                release: None,
                entered: None,
                _payload: PhantomData,
            }
        }
    }

    impl<P, FakeErr> IntoFuture for FakeRequest<P, FakeErr>
    where
        P: Payload<Output: Clone + std::marker::Send> + std::marker::Send + 'static,
        FakeErr: Error + std::marker::Send + AsResponseParameters + Clone + 'static,
    {
        type Output = Result<P::Output, FakeErr>;
        type IntoFuture = FakeSend<P, FakeErr>;

        fn into_future(self) -> Self::IntoFuture {
            self.send()
        }
    }

    struct FakeSend<P: Payload, FakeErr = RequestError> {
        result: Result<P::Output, FakeErr>,
        polls: Arc<AtomicUsize>,
        release: Option<Arc<Notify>>,
        entered: Option<Arc<Notify>>,
        _payload: PhantomData<fn() -> P>,
    }

    impl<P: Payload, FakeErr> Unpin for FakeSend<P, FakeErr> {}

    impl<P, FakeErr> Future for FakeSend<P, FakeErr>
    where
        P: Payload<Output: Clone + std::marker::Send>,
        FakeErr: Clone,
    {
        type Output = Result<P::Output, FakeErr>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            let this = self.get_mut();
            if let Some(entered) = &this.entered {
                entered.notify_one();
            }
            if let Some(notify) = &this.release {
                let notified = notify.notified();
                futures::pin_mut!(notified);
                if notified.poll(cx).is_pending() {
                    return Poll::Pending;
                }
            }
            Poll::Ready(this.result.clone())
        }
    }

    /// Peeks a pinned future once with a noop waker. Used to observe the
    /// state of scheduled requests without running them to completion.
    fn poll_once<F: Future>(fut: Pin<&mut F>) -> Poll<F::Output> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        fut.poll(&mut cx)
    }

    /// A requester whose programmed result is produced by
    /// `send_chat_action` (output `True`); every other method is
    /// unimplemented and never called by these tests.
    struct FakeRequester<FakeErr = RequestError> {
        result: Result<True, FakeErr>,
        polls: Arc<AtomicUsize>,
        release: Option<Arc<Notify>>,
        entered: Option<Arc<Notify>>,
        send_ref_calls: Arc<AtomicUsize>,
    }

    impl<FakeErr> FakeRequester<FakeErr> {
        fn new(result: Result<True, FakeErr>) -> Self {
            Self {
                result,
                polls: Arc::new(AtomicUsize::new(0)),
                release: None,
                entered: None,
                send_ref_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    macro_rules! f_unused {
        ($m:ident $this:ident ($($arg:ident : $T:ty),*)) => {{
            let _ = $this;
            $( let _ = $arg; )*
            unimplemented!("fake requester method {} is not used in these tests", stringify!($m))
        }};
    }

    macro_rules! fty {
        ($T:ident) => {
            FakeRequest<$T, FakeErr>
        };
    }

    impl<FakeErr> Requester for FakeRequester<FakeErr>
    where
        FakeErr: Error + std::marker::Send + AsResponseParameters + Clone + 'static,
    {
        type Err = FakeErr;
        type SendChatAction = FakeRequest<SendChatAction, FakeErr>;

        fn send_chat_action<C>(&self, chat_id: C, action: ChatAction) -> Self::SendChatAction
        where
            C: Into<Recipient>,
        {
            FakeRequest::<SendChatAction, FakeErr> {
                _payload: SendChatAction::new(chat_id, action),
                result: self.result.clone(),
                polls: self.polls.clone(),
                release: self.release.clone(),
                entered: self.entered.clone(),
                send_ref_calls: self.send_ref_calls.clone(),
            }
        }

        requester_forward! {
            add_sticker_to_set,
            answer_callback_query,
            answer_chat_join_request_query,
            answer_guest_query,
            answer_inline_query,
            answer_pre_checkout_query,
            answer_shipping_query,
            answer_web_app_query,
            approve_chat_join_request,
            approve_suggested_post,
            ban_chat_member,
            ban_chat_sender_chat,
            close,
            close_forum_topic,
            close_general_forum_topic,
            convert_gift_to_stars,
            copy_message,
            copy_messages,
            create_chat_invite_link,
            create_chat_subscription_invite_link,
            create_forum_topic,
            create_invoice_link,
            create_new_sticker_set,
            decline_chat_join_request,
            decline_suggested_post,
            delete_all_message_reactions,
            delete_business_messages,
            delete_chat_photo,
            delete_chat_sticker_set,
            delete_ephemeral_message,
            delete_forum_topic,
            delete_message,
            delete_message_reaction,
            delete_messages,
            delete_my_commands,
            delete_sticker_from_set,
            delete_sticker_set,
            delete_story,
            delete_webhook,
            edit_chat_invite_link,
            edit_chat_subscription_invite_link,
            edit_ephemeral_message_caption,
            edit_ephemeral_message_media,
            edit_ephemeral_message_reply_markup,
            edit_ephemeral_message_text,
            edit_forum_topic,
            edit_general_forum_topic,
            edit_message_caption,
            edit_message_caption_inline,
            edit_message_checklist,
            edit_message_live_location,
            edit_message_live_location_inline,
            edit_message_media,
            edit_message_media_inline,
            edit_message_reply_markup,
            edit_message_reply_markup_inline,
            edit_message_text,
            edit_message_text_inline,
            edit_story,
            edit_user_star_subscription,
            export_chat_invite_link,
            forward_message,
            forward_messages,
            get_available_gifts,
            get_business_account_gifts,
            get_business_account_star_balance,
            get_business_connection,
            get_chat,
            get_chat_administrators,
            get_chat_gifts,
            get_chat_member,
            get_chat_member_count,
            get_chat_members_count,
            get_chat_menu_button,
            get_custom_emoji_stickers,
            get_file,
            get_forum_topic_icon_stickers,
            get_game_high_scores,
            get_managed_bot_access_settings,
            get_managed_bot_token,
            get_me,
            get_my_commands,
            get_my_default_administrator_rights,
            get_my_description,
            get_my_name,
            get_my_short_description,
            get_my_star_balance,
            get_star_transactions,
            get_sticker_set,
            get_updates,
            get_user_chat_boosts,
            get_user_gifts,
            get_user_personal_chat_messages,
            get_user_profile_audios,
            get_user_profile_photos,
            get_webhook_info,
            gift_premium_subscription,
            hide_general_forum_topic,
            kick_chat_member,
            leave_chat,
            log_out,
            pin_chat_message,
            post_story,
            promote_chat_member,
            read_business_message,
            refund_star_payment,
            remove_business_account_profile_photo,
            remove_chat_verification,
            remove_my_profile_photo,
            remove_user_verification,
            reopen_forum_topic,
            reopen_general_forum_topic,
            replace_managed_bot_token,
            replace_sticker_in_set,
            repost_story,
            restrict_chat_member,
            revoke_chat_invite_link,
            save_prepared_inline_message,
            save_prepared_keyboard_button,
            send_animation,
            send_audio,
            send_chat_join_request_web_app,
            send_checklist,
            send_contact,
            send_dice,
            send_document,
            send_game,
            send_gift,
            send_gift_chat,
            send_invoice,
            send_live_photo,
            send_location,
            send_media_group,
            send_message,
            send_message_draft,
            send_paid_media,
            send_photo,
            send_poll,
            send_rich_message,
            send_rich_message_draft,
            send_sticker,
            send_venue,
            send_video,
            send_video_note,
            send_voice,
            set_business_account_bio,
            set_business_account_gift_settings,
            set_business_account_name,
            set_business_account_profile_photo,
            set_business_account_username,
            set_chat_administrator_custom_title,
            set_chat_description,
            set_chat_member_tag,
            set_chat_menu_button,
            set_chat_permissions,
            set_chat_photo,
            set_chat_sticker_set,
            set_chat_title,
            set_custom_emoji_sticker_set_thumbnail,
            set_game_score,
            set_game_score_inline,
            set_managed_bot_access_settings,
            set_message_reaction,
            set_my_commands,
            set_my_default_administrator_rights,
            set_my_description,
            set_my_name,
            set_my_profile_photo,
            set_my_short_description,
            set_passport_data_errors,
            set_sticker_emoji_list,
            set_sticker_keywords,
            set_sticker_mask_position,
            set_sticker_position_in_set,
            set_sticker_set_thumbnail,
            set_sticker_set_title,
            set_user_emoji_status,
            set_webhook,
            stop_message_live_location,
            stop_message_live_location_inline,
            stop_poll,
            transfer_business_account_stars,
            transfer_gift,
            unban_chat_member,
            unban_chat_sender_chat,
            unhide_general_forum_topic,
            unpin_all_chat_messages,
            unpin_all_forum_topic_messages,
            unpin_all_general_forum_topic_messages,
            unpin_chat_message,
            upgrade_gift,
            upload_sticker_file,
            verify_chat,
            verify_user,
            => f_unused, fty
        }
    }

    // ---------- helpers ----------

    fn settings() -> OutboundSettings {
        OutboundSettings {
            limits: OutboundLimits { global: Vec::new(), chat: Vec::new() },
            queue_capacity: 1024,
            aging: AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX },
        }
    }

    fn window(capacity: u32, window: Duration) -> WindowLimit {
        WindowLimit { capacity, window }
    }

    fn chat_metadata(chat: i64) -> OutboundMetadata {
        OutboundMetadata {
            scope: OutboundScope::Chat(OutboundChatKey::id(chat)),
            class: OutboundClass::new(class::MESSAGE_SEND),
            priority: OutboundPriority::NORMAL,
            weight: NonZeroU32::new(1).unwrap(),
        }
    }

    fn fake_chat_action(result: Result<True, RequestError>) -> FakeRequest<SendChatAction> {
        FakeRequest::<SendChatAction> {
            _payload: SendChatAction::new(ChatId(1), ChatAction::Typing),
            result,
            polls: Arc::new(AtomicUsize::new(0)),
            release: None,
            entered: None,
            send_ref_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    // ---------- scheduled request tests ----------

    #[tokio::test(start_paused = true)]
    async fn success_completes_the_permit_and_returns_the_output() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let scheduled = ScheduledRequest::new(fake_chat_action(Ok(True)), queue.clone());
        let output =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap();
        assert_eq!(output, True);
        let snapshot = queue.handle().snapshot().await.unwrap();
        assert_eq!(snapshot.in_flight, 0);
        assert_eq!(snapshot.pending, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn plain_error_is_returned_and_the_permit_is_completed_as_failed() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let scheduled = ScheduledRequest::new(
            fake_chat_action(Err(RequestError::MigrateToChatId(ChatId(1)))),
            queue.clone(),
        );
        let error =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap_err();
        assert!(matches!(error, OutboundRequestError::Inner(RequestError::MigrateToChatId(_))));
        let snapshot = queue.handle().snapshot().await.unwrap();
        assert_eq!(snapshot.in_flight, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_is_classified_with_an_explicit_scope_and_penalizes_the_chat() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let scheduled = ScheduledRequest::new(
            fake_chat_action(Err(RequestError::RetryAfter(Seconds::from_seconds(5)))),
            queue.clone(),
        );
        let error =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap_err();
        assert!(matches!(error, OutboundRequestError::Inner(RequestError::RetryAfter(_))));

        // The classifier reported the chat scope of the payload (ChatId(1)):
        // a new acquire for chat 1 is blocked by the penalty, chat 2 is not.
        let blocked = queue.handle().acquire(chat_metadata(1));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        let other = queue.handle().acquire(chat_metadata(2));
        let permit = tokio::time::timeout(Duration::from_secs(1), other).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);

        tokio::time::advance(Duration::from_secs(6)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn the_inner_request_is_not_polled_before_the_permit() {
        let mut settings = settings();
        settings.limits =
            OutboundLimits { global: vec![window(1, Duration::from_secs(60))], chat: Vec::new() };
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();
        // Занять окно, чтобы запрос не мог получить permit.
        let holder = handle.acquire(chat_metadata(1)).await.unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let request = FakeRequest::<SendChatAction> {
            _payload: SendChatAction::new(ChatId(1), ChatAction::Typing),
            result: Ok(True),
            polls: polls.clone(),
            release: None,
            entered: None,
            send_ref_calls: Arc::new(AtomicUsize::new(0)),
        };
        let scheduled = ScheduledRequest::new(request, queue.clone());
        let mut scheduled = Box::pin(scheduled.into_future());
        assert!(poll_once(scheduled.as_mut()).is_pending());

        // Дождаться, пока actor обработал enqueue и поставил job в pending
        // за занятым окном: enqueue и snapshot идут разными каналами,
        // поэтому крутим до тех пор, пока pending не станет виден.
        for _ in 0..100 {
            tokio::task::yield_now().await;
            let snapshot = handle.snapshot().await.unwrap();
            if snapshot.pending == 1 {
                break;
            }
        }
        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 1, "job должен ждать за занятым окном");
        assert_eq!(polls.load(Ordering::SeqCst), 0, "inner request polled before the permit");

        // Окно освобождается только временем: grant, затем inner выполняется.
        holder.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let output =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap();
        assert_eq!(output, True);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_before_the_grant_cancels_the_pending_job() {
        let mut settings = settings();
        settings.limits =
            OutboundLimits { global: vec![window(1, Duration::from_secs(60))], chat: Vec::new() };
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();
        let holder = handle.acquire(chat_metadata(1)).await.unwrap();
        let scheduled = ScheduledRequest::new(fake_chat_action(Ok(True)), queue.clone());
        let mut scheduled = Box::pin(scheduled.into_future());
        tokio::task::yield_now().await;
        assert!(poll_once(scheduled.as_mut()).is_pending());
        drop(scheduled);
        tokio::task::yield_now().await;
        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.in_flight, 1); // только holder
        holder.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_while_the_inner_request_runs_releases_the_lane() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();
        let lane = handle.serial_lane();

        // Первый запрос lane запускается в отдельной task: он входит в
        // «HTTP-запрос» (inner future poll-ится, permit удерживается,
        // lane занята) и зависает до release.
        let release = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let task = tokio::spawn({
            let queue = queue.clone();
            let lane = lane.clone();
            let release = release.clone();
            let entered = entered.clone();
            async move {
                let first = ScheduledRequest::new(
                    FakeRequest::<SendChatAction> {
                        _payload: SendChatAction::new(ChatId(1), ChatAction::Typing),
                        result: Ok(True),
                        polls: Arc::new(AtomicUsize::new(0)),
                        release: Some(release),
                        entered: Some(entered),
                        send_ref_calls: Arc::new(AtomicUsize::new(0)),
                    },
                    queue,
                )
                .on_lane(&lane);
                let _ = tokio::time::timeout(Duration::from_secs(30), first).await;
            }
        });

        // Ждём, пока первый реально ВОШЁЛ в inner request: grant получен,
        // permit живёт, serial lane удерживается.
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("первый запрос не вошёл в inner request");

        // Второй запрос той же lane ждёт завершения первого.
        let second =
            ScheduledRequest::new(fake_chat_action(Ok(True)), queue.clone()).on_lane(&lane);
        let mut second = Box::pin(second.into_future());
        assert!(poll_once(second.as_mut()).is_pending());

        // Abort первого во время выполнения: permit дропается как
        // CancelledAfterGrant, lane освобождается, второй выполняется.
        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;
        let output = tokio::time::timeout(Duration::from_secs(1), second).await.unwrap().unwrap();
        assert_eq!(output, True);
        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.in_flight, 0, "abort не должен оставлять in-flight слот");
    }

    // ---------- adaptor tests ----------

    #[test]
    fn retry_after_scope_policy_follows_the_request_scope() {
        let chat = OutboundMetadata {
            scope: OutboundScope::Chat(OutboundChatKey::id(9)),
            class: OutboundClass::new(class::MESSAGE_SEND),
            priority: OutboundPriority::NORMAL,
            weight: NonZeroU32::new(1).unwrap(),
        };
        let error = RequestError::RetryAfter(Seconds::from_seconds(3));
        assert_eq!(retry_after_scope(&chat, &error), OutboundScope::Chat(OutboundChatKey::id(9)));

        let global = OutboundMetadata { scope: OutboundScope::Global, ..chat };
        assert_eq!(retry_after_scope(&global, &error), OutboundScope::Global);
    }

    #[test]
    fn channel_usernames_get_distinct_chat_scopes() {
        let foo = scope_of_recipient(&Recipient::ChannelUsername("foo".into()));
        let bar = scope_of_recipient(&Recipient::ChannelUsername("bar".into()));
        let foo_again = scope_of_recipient(&Recipient::ChannelUsername("foo".into()));
        let numeric = scope_of_recipient(&Recipient::Id(ChatId(7)));

        assert_eq!(foo, foo_again, "тот же username — та же identity");
        assert_ne!(foo, bar, "разные username не должны сливаться в один scope");
        assert_ne!(foo, numeric, "username и числовой chat id — разные identity");
        assert_eq!(foo, OutboundScope::Chat(OutboundChatKey::username("foo")));

        // Канонизация: регистр и ведущий `@` не создают новую identity.
        let upper = scope_of_recipient(&Recipient::ChannelUsername("@FOO".into()));
        assert_eq!(upper, foo, "username case-insensitive (Telegram семантика)");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_for_a_channel_username_blocks_only_that_username() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();
        let outbound = Outbound::new(
            FakeRequester::new(Err(RequestError::RetryAfter(Seconds::from_seconds(3)))),
            queue.clone(),
        );

        let scheduled =
            outbound.send_chat_action(Recipient::ChannelUsername("foo".into()), ChatAction::Typing);
        let error =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap_err();
        assert!(matches!(error, OutboundRequestError::Inner(RequestError::RetryAfter(_))));

        // Penalty легла на @foo; @bar и числовой chat не заблокированы.
        let foo_meta = OutboundMetadata {
            scope: scope_of_recipient(&Recipient::ChannelUsername("foo".into())),
            class: OutboundClass::new(class::CHAT_ACTION),
            priority: OutboundPriority::NORMAL,
            weight: NonZeroU32::new(1).unwrap(),
        };
        let bar_meta = OutboundMetadata {
            scope: scope_of_recipient(&Recipient::ChannelUsername("bar".into())),
            ..foo_meta
        };
        let blocked = handle.acquire(foo_meta);
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        let bar = handle.acquire(bar_meta);
        let permit = tokio::time::timeout(Duration::from_secs(1), bar).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
        let numeric = handle.acquire(chat_metadata(7));
        let permit = tokio::time::timeout(Duration::from_secs(1), numeric).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);

        tokio::time::advance(Duration::from_secs(4)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn canonical_usernames_share_one_per_chat_window() {
        let mut settings = settings();
        settings.limits =
            OutboundLimits { global: Vec::new(), chat: vec![window(1, Duration::from_secs(60))] };
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();

        let foo = OutboundMetadata {
            scope: scope_of_recipient(&Recipient::ChannelUsername("@Foo".into())),
            class: OutboundClass::new(class::CHAT_ACTION),
            priority: OutboundPriority::NORMAL,
            weight: NonZeroU32::new(1).unwrap(),
        };
        let foo_lower = OutboundMetadata {
            scope: scope_of_recipient(&Recipient::ChannelUsername("foo".into())),
            ..foo.clone()
        };

        // @Foo и foo — одна canonical identity: одно per-chat окно на обоих.
        let first = handle.acquire(foo).await.unwrap();
        let second = handle.acquire(foo_lower);
        tokio::pin!(second);
        tokio::task::yield_now().await;
        assert!(futures::poll!(second.as_mut()).is_pending(), "та же identity должна делить окно");

        first.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), second).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn mutating_the_payload_chat_id_updates_the_scheduled_scope() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();
        let outbound = Outbound::new(
            FakeRequester::new(Err(RequestError::RetryAfter(Seconds::from_seconds(3)))),
            queue.clone(),
        );

        // Запрос построен для Chat(1), но payload мутирован до send —
        // штатный teloxide `send_ref` flow меняет `chat_id` перед
        // отправкой. Scope должен следовать за payload, который реально
        // уходит на wire.
        let mut scheduled = outbound.send_chat_action(ChatId(1), ChatAction::Typing);
        scheduled.chat_id = ChatId(2).into();
        let error =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap_err();
        assert!(matches!(error, OutboundRequestError::Inner(RequestError::RetryAfter(_))));

        // Penalty лёг на Chat(2) — чат, куда ушёл запрос; Chat(1) свободен.
        let chat1 = handle.acquire(chat_metadata(1));
        let permit = tokio::time::timeout(Duration::from_secs(1), chat1).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);

        let blocked = handle.acquire(chat_metadata(2));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(4)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn send_ref_recomputes_the_scope_from_the_mutated_payload() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();
        let outbound = Outbound::new(
            FakeRequester::new(Err(RequestError::RetryAfter(Seconds::from_seconds(3)))),
            queue.clone(),
        );

        // Публичный `Request::send_ref` flow: payload мутируется до
        // отправки по ссылке; scope обязан следовать за Chat(2).
        let mut scheduled = outbound.send_chat_action(ChatId(1), ChatAction::Typing);
        scheduled.chat_id = ChatId(2).into();
        let future = scheduled.send_ref();
        let error =
            tokio::time::timeout(Duration::from_secs(1), future).await.unwrap().unwrap_err();
        assert!(matches!(error, OutboundRequestError::Inner(RequestError::RetryAfter(_))));

        let chat1 = handle.acquire(chat_metadata(1));
        let permit = tokio::time::timeout(Duration::from_secs(1), chat1).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);

        let blocked = handle.acquire(chat_metadata(2));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(4)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    /// Пины weight-семантику batch payload-ов: вес должен расти с длиной
    /// batch-а (окна меряют реальный message traffic, а не число вызовов),
    /// а пустой batch не обнуляет вес.
    #[test]
    fn batch_payload_weights_scale_with_the_batch_length() {
        // send_media_group: одна единица на каждый media.
        let media = InputMedia::Photo(InputMediaPhoto::new(InputFile::file_id("f1".into())));
        let payload = SendMediaGroup::new(ChatId(1), std::iter::repeat_n(media, 3));
        assert_eq!(payload.outbound_hint().weight, NonZeroU32::new(3).unwrap());

        // forward_messages: одна единица на каждое сообщение.
        let payload = ForwardMessages::new(
            ChatId(1),
            ChatId(2),
            [MessageId(1), MessageId(2), MessageId(3), MessageId(4)],
        );
        assert_eq!(payload.outbound_hint().weight, NonZeroU32::new(4).unwrap());

        // copy_messages: то же самое.
        let payload = CopyMessages::new(
            ChatId(1),
            ChatId(2),
            [MessageId(1), MessageId(2), MessageId(3), MessageId(4), MessageId(5)],
        );
        assert_eq!(payload.outbound_hint().weight, NonZeroU32::new(5).unwrap());

        // Пустой batch не даёт weight 0: minimum одна единица.
        let payload = ForwardMessages::new(ChatId(1), ChatId(2), []);
        assert_eq!(payload.outbound_hint().weight, NonZeroU32::new(1).unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn weight_override_reaches_the_scheduler() {
        // Окно capacity=1: вес 5 не помещается ни в одно применимое окно.
        let mut settings = settings();
        settings.limits =
            OutboundLimits { global: vec![window(1, Duration::from_secs(60))], chat: Vec::new() };
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let outbound = Outbound::new(FakeRequester::<RequestError>::new(Ok(True)), queue.clone());
        let error = outbound
            .send_chat_action(ChatId(42), ChatAction::Typing)
            .weight(NonZeroU32::new(5).unwrap())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                OutboundRequestError::Acquire(OutboundAcquireError::WeightExceedsWindow {
                    scope: OutboundScope::Chat(_),
                    weight: _,
                    capacity: 1
                })
            ),
            "weight override должен дойти до scheduler-а: {error:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn per_request_overrides_beat_the_payload_classification() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let outbound = Outbound::new(FakeRequester::<RequestError>::new(Ok(True)), queue.clone());

        // Без overrides metadata() = классификация payload.
        let base = outbound.send_chat_action(ChatId(42), ChatAction::Typing);
        assert_eq!(base.metadata().priority, OutboundPriority::BACKGROUND);

        // Overrides применяются поверх payload-классификации; scope
        // остаётся payload-овым.
        let scheduled = base
            .clone()
            .priority(OutboundPriority::CRITICAL)
            .weight(NonZeroU32::new(3).unwrap())
            .class(OutboundClass::new(class::MESSAGE_SEND));
        let effective = scheduled.metadata();
        assert_eq!(effective.scope, OutboundScope::Chat(OutboundChatKey::id(42)));
        assert_eq!(effective.priority, OutboundPriority::CRITICAL);
        assert_eq!(effective.weight, NonZeroU32::new(3).unwrap());
        assert_eq!(effective.class, OutboundClass::new(class::MESSAGE_SEND));

        // Сброс: with_outbound_overrides(default) возвращает payload-базу.
        let reset = base
            .clone()
            .priority(OutboundPriority::CRITICAL)
            .with_outbound_overrides(OutboundOverrides::default());
        assert_eq!(reset.metadata().priority, OutboundPriority::BACKGROUND);

        let output =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap();
        assert_eq!(output, True);
    }

    #[tokio::test(start_paused = true)]
    async fn critical_override_overtakes_background_requests_in_the_same_scope() {
        let mut settings = settings();
        settings.limits =
            OutboundLimits { global: Vec::new(), chat: vec![window(1, Duration::from_secs(60))] };
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let outbound = Outbound::new(FakeRequester::<RequestError>::new(Ok(True)), queue.clone());

        // Первый запрос занимает per-chat окно.
        let first = outbound.send_chat_action(ChatId(1), ChatAction::Typing);
        let output = tokio::time::timeout(Duration::from_secs(1), first).await.unwrap().unwrap();
        assert_eq!(output, True);

        // Два ожидающих в одном scope: обычный chat action (BACKGROUND)
        // и critical-оверрайд. FIFO по priority одинаков — при равных
        // приоритетах первым пошёл бы normal, поэтому grant critical-а
        // доказывает, что override дошёл до арбитража.
        let normal = outbound.send_chat_action(ChatId(1), ChatAction::Typing);
        let critical = outbound
            .send_chat_action(ChatId(1), ChatAction::Typing)
            .priority(OutboundPriority::CRITICAL);
        let mut normal = Box::pin(normal.into_future());
        let mut critical = Box::pin(critical.into_future());
        tokio::task::yield_now().await;
        // NORMAL poll-ится и enqueue-ится ПЕРВЫМ: при сломанном override
        // (оба BACKGROUND) он имел бы более раннюю sequence и получил бы
        // grant первым — тест обязан это отловить.
        assert!(poll_once(normal.as_mut()).is_pending(), "normal ждёт окно");
        assert!(poll_once(critical.as_mut()).is_pending(), "critical ждёт окно");
        // Дай actor-у обработать оба enqueue при t=0: admission взведёт
        // таймер на момент освобождения окна, и advance(61) разбудит его.
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(61)).await;
        let output =
            tokio::time::timeout(Duration::from_secs(1), critical.as_mut()).await.unwrap().unwrap();
        assert_eq!(output, True);
        assert!(
            futures::poll!(normal.as_mut()).is_pending(),
            "normal не может получить permit раньше critical-оверрайда"
        );

        tokio::time::advance(Duration::from_secs(61)).await;
        let output =
            tokio::time::timeout(Duration::from_secs(1), normal.as_mut()).await.unwrap().unwrap();
        assert_eq!(output, True);
    }

    /// Пинает отложенную конструкцию inner `send_ref`: базовый
    /// `Request::send_ref` лишь *рекомендует* лень, но контракт этого не
    /// гарантирует — кастомный requester может синхронно открыть ресурс
    /// или зафиксировать deadline прямо при вызове. Scheduled-адаптор
    /// обязан вызывать inner `send_ref()` только после grant.
    #[tokio::test(start_paused = true)]
    async fn send_ref_construction_side_effects_are_deferred_until_the_grant() {
        // Окно занято: запрос гарантированно ждёт permit.
        let mut settings = settings();
        settings.limits =
            OutboundLimits { global: Vec::new(), chat: vec![window(1, Duration::from_secs(60))] };
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();
        let blocker = handle.acquire(chat_metadata(1)).await.unwrap();

        let send_ref_calls = Arc::new(AtomicUsize::new(0));
        let request = FakeRequest::<SendChatAction> {
            _payload: SendChatAction::new(ChatId(1), ChatAction::Typing),
            result: Ok(True),
            polls: Arc::new(AtomicUsize::new(0)),
            release: None,
            entered: None,
            send_ref_calls: send_ref_calls.clone(),
        };
        let plan = ScheduledRequest::new(request, queue.clone()).send_ref();

        // Ни сам вызов `send_ref()`, ни ПЕРВЫЙ poll будущего не должны
        // трогать inner `Request::send_ref` (side effect отложен): poll
        // до grant ставит job в очередь и должен вернуть Pending.
        assert_eq!(send_ref_calls.load(Ordering::SeqCst), 0);
        tokio::pin!(plan);
        assert!(poll_once(plan.as_mut()).is_pending(), "до grant future не может резолвиться");
        tokio::task::yield_now().await;
        assert_eq!(send_ref_calls.load(Ordering::SeqCst), 0);

        // После grant inner `send_ref` вызывается ровно один раз.
        blocker.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let output = tokio::time::timeout(Duration::from_secs(1), plan).await.unwrap().unwrap();
        assert_eq!(output, True);
        assert_eq!(send_ref_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn adaptor_schedules_a_chat_action_with_chat_scope() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let outbound = Outbound::new(FakeRequester::<RequestError>::new(Ok(True)), queue.clone());
        let scheduled = outbound.send_chat_action(ChatId(42), ChatAction::Typing);
        assert_eq!(scheduled.metadata().scope, OutboundScope::Chat(OutboundChatKey::id(42)));
        assert_eq!(scheduled.metadata().class, OutboundClass::new(class::CHAT_ACTION));
        assert_eq!(
            scheduled.metadata().priority,
            OutboundPriority::BACKGROUND,
            "chat actions are background traffic"
        );
        let output =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap();
        assert_eq!(output, True);
    }

    /// Ошибка, заимствующая данные: deliberately NOT `'static`. This pins
    /// the error-wrapper contract: `OutboundRequestError<E>: Error`
    /// requires only `E: Error`, so no `'static` leaks into the inner
    /// error bound. (Wrapping a full requester with borrowed errors is a
    /// separate question: its request types would also carry the
    /// lifetime and the `Requester` impl requires `'static` request
    /// types.)
    #[derive(Debug)]
    struct BorrowedError<'a>(&'a str);

    impl fmt::Display for BorrowedError<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "borrowed error: {}", self.0)
        }
    }

    impl Error for BorrowedError<'_> {}

    #[test]
    fn outbound_error_accepts_non_static_inner_errors() {
        fn assert_error<E: Error>() {}
        assert_error::<OutboundRequestError<BorrowedError<'_>>>();
    }

    /// Кастомная ошибка requester-а: `RetryAfter` классифицируется через
    /// `AsResponseParameters`, а не через match по `RequestError`.
    #[derive(Debug, Clone, PartialEq)]
    struct CustomError;

    impl fmt::Display for CustomError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "custom error")
        }
    }

    impl Error for CustomError {}

    impl AsResponseParameters for CustomError {
        fn response_parameters(&self) -> Option<ResponseParameters> {
            Some(ResponseParameters::RetryAfter(Seconds::from_seconds(2)))
        }
    }

    #[test]
    fn global_read_payload_is_classified_global() {
        let hint = GetMe::default().outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Global);
        assert_eq!(hint.class, OutboundClass::new(class::READ));
        assert_eq!(hint.priority, OutboundPriority::NORMAL);
        assert_eq!(hint.weight, NonZeroU32::new(1).unwrap());
    }

    #[test]
    fn message_mutation_payload_is_classified_chat_scoped() {
        let hint = EditMessageText::new(ChatId(9), MessageId(1), "new text").outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Chat(OutboundChatKey::id(9)));
        assert_eq!(hint.class, OutboundClass::new(class::MESSAGE_MUTATION));
        assert_eq!(hint.priority, OutboundPriority::NORMAL);
    }

    #[test]
    fn optional_chat_id_falls_back_to_global() {
        let mut payload = GetChatMenuButton::new();
        assert_eq!(payload.outbound_hint().scope, OutboundScope::Global);
        payload.chat_id = Some(ChatId(5));
        assert_eq!(payload.outbound_hint().scope, OutboundScope::Chat(OutboundChatKey::id(5)));
    }

    #[test]
    fn custom_scope_rules_classify_special_payloads() {
        // Draft payloads адресуются владельцу draft-а (user id).
        let hint = SendMessageDraft::new(UserId(11), 1).outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Chat(OutboundChatKey::id(11)));

        // transfer_gift — по new_owner_chat_id.
        let hint = TransferGift::new(
            BusinessConnectionId("bc".into()),
            OwnedGiftId("gift".into()),
            ChatId(22),
        )
        .outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Chat(OutboundChatKey::id(22)));

        // repost_story — действие от имени business account; source chat
        // не должен штрафоваться, поэтому scope глобальный.
        let hint = RepostStory::new(
            BusinessConnectionId("bc".into()),
            ChatId(33),
            StoryId(1),
            Seconds::from_seconds(5),
        )
        .outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Global);
    }

    #[test]
    fn inline_mutations_are_global() {
        let hint = EditMessageTextInline::new(String::from("inline_id"), String::from("text"))
            .outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Global);
        let hint = EditMessageCaptionInline::new(String::from("inline_id")).outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Global);
    }

    #[test]
    fn game_score_scopes_follow_the_target_message() {
        // Inline game score не имеет chat identity.
        let hint =
            SetGameScoreInline::new(UserId(7), 10, String::from("inline_id")).outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Global);

        // Обычный set_game_score адресует чат, в котором лежит сообщение.
        let hint = SetGameScore::new(UserId(7), 10, 42, MessageId(1)).outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Chat(OutboundChatKey::id(42)));

        // get_game_high_scores: Common target → чат, Inline → global.
        let hint = GetGameHighScores::new(
            UserId(7),
            TargetMessage::Common { chat_id: ChatId(43).into(), message_id: MessageId(2) },
        )
        .outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Chat(OutboundChatKey::id(43)));
        let hint = GetGameHighScores::new(
            UserId(7),
            TargetMessage::Inline { inline_message_id: String::from("im") },
        )
        .outbound_hint();
        assert_eq!(hint.scope, OutboundScope::Global);
    }

    #[tokio::test(start_paused = true)]
    async fn custom_requester_errors_classify_retry_after() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();
        let outbound =
            Outbound::new(FakeRequester::<CustomError>::new(Err(CustomError)), queue.clone());

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            outbound.send_chat_action(ChatId(3), ChatAction::Typing),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(error, OutboundRequestError::Inner(CustomError)));

        // Penalty из кастомной ошибки (RetryAfter 2s) легла на Chat(3).
        let blocked = handle.acquire(chat_metadata(3));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(3)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn integration_adaptor_queue_and_retry_after() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();

        // Полный путь: адаптор -> ScheduledRequest -> очередь -> результат.
        let outbound = Outbound::new(FakeRequester::<RequestError>::new(Ok(True)), queue.clone());
        let output = tokio::time::timeout(
            Duration::from_secs(1),
            outbound.send_chat_action(ChatId(5), ChatAction::Typing),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(output, True);

        // RetryAfter через адаптор: классификатор сообщает Chat(5),
        // очередь применяет penalty к этому чату.
        let outbound = Outbound::new(
            FakeRequester::new(Err(RequestError::RetryAfter(Seconds::from_seconds(4)))),
            queue.clone(),
        );
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            outbound.send_chat_action(ChatId(5), ChatAction::Typing),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(error, OutboundRequestError::Inner(RequestError::RetryAfter(_))));

        let blocked = handle.acquire(chat_metadata(5));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(5)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }
}
