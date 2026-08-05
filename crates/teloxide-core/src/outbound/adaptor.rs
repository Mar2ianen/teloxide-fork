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
//! [`Outbound`] wraps any [`Requester`] so that its methods return
//! [`ScheduledRequest`]s; [`ScheduledRequest::on_lane`] attaches a serial
//! ordering lane. This is a vertical slice: only a few representative
//! methods are wired; the full payload classification lands with the
//! codegen commit.

use std::{error::Error, fmt, future::IntoFuture, num::NonZeroU32, sync::Arc};

use crate::{
    errors::AsResponseParameters,
    outbound::{
        OutboundAcquireError, OutboundChatKey, OutboundClass, OutboundCompletion, OutboundLane,
        OutboundMetadata, OutboundPriority, OutboundQueue, OutboundScope,
    },
    payloads::{EditMessageText, SendChatAction, SendMessage, SendRichMessage},
    requests::{HasPayload, Output, Payload, Request, Requester},
    types::{InputRichMessage, MessageId, Recipient, ResponseParameters},
};

/// Draft request classes of the built-in adaptor methods (spec §11.1).
pub mod class {
    /// Read-only queries (`get_me`, ...).
    pub const READ: u64 = 1;
    /// Ordinary message sends.
    pub const MESSAGE_SEND: u64 = 2;
    /// Edits of existing messages.
    pub const MESSAGE_MUTATION: u64 = 3;
    /// Ephemeral typing indicators.
    pub const CHAT_ACTION: u64 = 4;
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

impl<E: Error + 'static> Error for OutboundRequestError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Inner(error) => Some(error),
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
/// The policy metadata (class, priority, weight) is fixed at construction;
/// for adaptor-created requests the scope is recomputed from the final
/// payload at send time (the payload is publicly mutable until `send`, see
/// [`Outbound`]). The permit is acquired when the future is polled.
/// Dropping the future before the grant cancels the pending job; dropping
/// it while the inner send future runs releases the permit as
/// `CancelledAfterGrant` (the lane, if any, is freed, the rate budget
/// stays consumed).
///
/// [`Request`] is implemented only for `Req: Clone` because `send_ref` is
/// built as `self.clone().send()`: custom request types must be cloneable
/// to be scheduled. This is a deliberate trade-off of the vertical slice
/// (the base trait prefers `send_ref` over a full clone); the codegen
/// commit may revisit it with a borrow-aware `SendRef`.
///
/// The type requires `Req: HasPayload`; the [`Request`] implementation
/// additionally requires `Req::Payload::Output: Send`, because the
/// completion barrier keeps the outcome across an `await` and the request
/// future must therefore be sendable.
#[must_use = "Scheduled requests are lazy and do nothing unless sent or awaited"]
pub struct ScheduledRequest<Req: HasPayload> {
    request: Req,
    queue: OutboundQueue,
    lane: Option<OutboundLane>,
    metadata: OutboundMetadata,
    /// Recomputed the scope from the final payload at send time. Adaptor
    /// methods install a per-payload function here, because the payload is
    /// publicly mutable between construction and `send` (teloxide's
    /// `send_ref` flow changes `chat_id` before sending): admission, lanes
    /// and `RetryAfter` penalties must follow the payload that is actually
    /// sent. `None` keeps the construction-time scope.
    scope_fn: Option<fn(&Req::Payload) -> OutboundScope>,
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
            metadata: self.metadata.clone(),
            scope_fn: self.scope_fn,
        }
    }
}

impl<Req: HasPayload> ScheduledRequest<Req> {
    pub fn new(request: Req, queue: OutboundQueue, metadata: OutboundMetadata) -> Self {
        Self { request, queue, lane: None, metadata, scope_fn: None }
    }

    fn with_scope_fn(
        request: Req,
        queue: OutboundQueue,
        metadata: OutboundMetadata,
        scope_fn: fn(&Req::Payload) -> OutboundScope,
    ) -> Self {
        Self { request, queue, lane: None, metadata, scope_fn: Some(scope_fn) }
    }

    /// Attaches a serial ordering lane: at most one request of the lane is
    /// in flight and the lane is served strictly in enqueue order.
    pub fn on_lane(mut self, lane: &OutboundLane) -> Self {
        self.lane = Some(lane.clone());
        self
    }

    /// The policy metadata this request was constructed with.
    ///
    /// For adaptor-created requests the `scope` is recomputed from the
    /// payload at send time, so `metadata().scope` may differ from the
    /// scope actually used for admission and `RetryAfter` penalties after
    /// a payload mutation.
    pub fn metadata(&self) -> &OutboundMetadata {
        &self.metadata
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
    Req::Payload: Payload<Output: std::marker::Send>,
{
    type Err = OutboundRequestError<Req::Err>;
    type Send = Send<Req>;
    type SendRef = Send<Req>;

    fn send(self) -> Self::Send {
        Send::new(self)
    }

    fn send_ref(&self) -> Self::SendRef {
        self.clone().send()
    }
}

impl<Req> IntoFuture for ScheduledRequest<Req>
where
    Req: HasPayload + 'static + std::marker::Send + Request + Clone,
    Req::Err: AsResponseParameters,
    Req::Payload: Payload<Output: std::marker::Send>,
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
            // The payload is publicly mutable until send (teloxide's
            // `send_ref` flow changes `chat_id` before sending), so the
            // scope is recomputed from the payload actually sent. The
            // rest of the metadata (class, priority, weight) is policy
            // fixed at construction.
            let ScheduledRequest { request, queue, lane, metadata, scope_fn } = it;
            let metadata = match scope_fn {
                Some(scope_fn) => {
                    OutboundMetadata { scope: scope_fn(request.payload_ref()), ..metadata }
                }
                None => metadata,
            };
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
        U::Payload: Payload<Output: std::marker::Send>,
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
/// a [`ScheduledRequest`] scheduled with an explicit class and scope. This
/// is the primary entry point:
///
/// ```no_run
/// use teloxide_core::{
///     outbound::{Outbound, OutboundQueue, OutboundSettings},
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

impl<R: Requester> Outbound<R> {
    /// [`Requester::get_me`] scheduled as a global-scope read.
    pub fn get_me(&self) -> ScheduledRequest<R::GetMe> {
        let request = self.inner.get_me();
        ScheduledRequest::new(
            request,
            self.queue.clone(),
            metadata(OutboundScope::Global, class::READ, OutboundPriority::NORMAL),
        )
    }

    /// [`Requester::send_message`] scheduled as a chat-scoped message send.
    pub fn send_message<C, T>(&self, chat_id: C, text: T) -> ScheduledRequest<R::SendMessage>
    where
        C: Into<Recipient>,
        T: Into<String>,
    {
        let request = self.inner.send_message(chat_id, text);
        let metadata = metadata(
            scope_of_recipient(&request.payload_ref().chat_id),
            class::MESSAGE_SEND,
            OutboundPriority::NORMAL,
        );
        ScheduledRequest::with_scope_fn(request, self.queue.clone(), metadata, send_message_scope)
    }

    /// [`Requester::send_rich_message`] scheduled as a chat-scoped message
    /// send.
    pub fn send_rich_message<C>(
        &self,
        chat_id: C,
        rich_message: InputRichMessage,
    ) -> ScheduledRequest<R::SendRichMessage>
    where
        C: Into<Recipient>,
    {
        let request = self.inner.send_rich_message(chat_id, rich_message);
        let metadata = metadata(
            scope_of_recipient(&request.payload_ref().chat_id),
            class::MESSAGE_SEND,
            OutboundPriority::NORMAL,
        );
        ScheduledRequest::with_scope_fn(
            request,
            self.queue.clone(),
            metadata,
            send_rich_message_scope,
        )
    }

    /// [`Requester::edit_message_text`] scheduled as a chat-scoped message
    /// mutation.
    pub fn edit_message_text<C, T>(
        &self,
        chat_id: C,
        message_id: MessageId,
        text: T,
    ) -> ScheduledRequest<R::EditMessageText>
    where
        C: Into<Recipient>,
        T: Into<String>,
    {
        let request = self.inner.edit_message_text(chat_id, message_id, text);
        let metadata = metadata(
            scope_of_recipient(&request.payload_ref().chat_id),
            class::MESSAGE_MUTATION,
            OutboundPriority::NORMAL,
        );
        ScheduledRequest::with_scope_fn(
            request,
            self.queue.clone(),
            metadata,
            edit_message_text_scope,
        )
    }

    /// [`Requester::send_chat_action`] scheduled as a chat-scoped,
    /// background chat action.
    pub fn send_chat_action<C>(
        &self,
        chat_id: C,
        action: crate::types::ChatAction,
    ) -> ScheduledRequest<R::SendChatAction>
    where
        C: Into<Recipient>,
    {
        let request = self.inner.send_chat_action(chat_id, action);
        let metadata = metadata(
            scope_of_recipient(&request.payload_ref().chat_id),
            class::CHAT_ACTION,
            OutboundPriority::BACKGROUND,
        );
        ScheduledRequest::with_scope_fn(
            request,
            self.queue.clone(),
            metadata,
            send_chat_action_scope,
        )
    }
}

fn metadata(scope: OutboundScope, class: u64, priority: OutboundPriority) -> OutboundMetadata {
    OutboundMetadata {
        scope,
        class: OutboundClass::new(class),
        priority,
        weight: NonZeroU32::new(1).unwrap(),
    }
}

fn send_message_scope(payload: &SendMessage) -> OutboundScope {
    scope_of_recipient(&payload.chat_id)
}

fn send_rich_message_scope(payload: &SendRichMessage) -> OutboundScope {
    scope_of_recipient(&payload.chat_id)
}

fn edit_message_text_scope(payload: &EditMessageText) -> OutboundScope {
    scope_of_recipient(&payload.chat_id)
}

fn send_chat_action_scope(payload: &SendChatAction) -> OutboundScope {
    scope_of_recipient(&payload.chat_id)
}

/// Canonicalizes a channel username into the identity form: strips the
/// single optional leading `@` and lower-cases (Telegram usernames are
/// case-insensitive ASCII), so `@Foo`, `foo` and `@FOO` are one chat
/// identity.
fn canonical_username(username: &str) -> String {
    username.strip_prefix('@').unwrap_or(username).to_ascii_lowercase()
}

fn scope_of_recipient(recipient: &Recipient) -> OutboundScope {
    match recipient {
        Recipient::Id(chat_id) => OutboundScope::Chat(OutboundChatKey::new(chat_id.0)),
        // Username addressing has no numeric chat id, but the username is
        // still a per-chat identity: store it as text (canonical lower-case
        // form) so that per-chat windows and penalties apply to exactly
        // one channel and can never collide.
        Recipient::ChannelUsername(username) => {
            OutboundScope::Chat(OutboundChatKey::Username(Arc::from(canonical_username(username))))
        }
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
        outbound::{AgingPolicy, OutboundLimits, OutboundSettings, WindowLimit},
        payloads::*,
        requests::Payload,
        types::*,
    };

    // ---------- fake requester / fake request ----------

    /// A fake request whose `Send` future counts polls and either returns
    /// the programmed result immediately or waits for a release signal.
    /// The output type is the real `P::Output` of the payload, so the fake
    /// is only usable with payloads whose output is cheap to construct
    /// (`SendChatAction` -> `True` in these tests).
    #[derive(Clone)]
    struct FakeRequest<P: Payload> {
        _payload: P,
        result: Result<P::Output, RequestError>,
        polls: Arc<AtomicUsize>,
        release: Option<Arc<Notify>>,
        /// Signalled on the first poll of the inner `Send` future, proving
        /// that the request actually entered the inner execution.
        entered: Option<Arc<Notify>>,
    }

    impl<P: Payload> HasPayload for FakeRequest<P> {
        type Payload = P;

        fn payload_mut(&mut self) -> &mut Self::Payload {
            &mut self._payload
        }

        fn payload_ref(&self) -> &Self::Payload {
            &self._payload
        }
    }

    impl<P> Request for FakeRequest<P>
    where
        P: Payload<Output: Clone + std::marker::Send> + std::marker::Send + 'static,
    {
        type Err = RequestError;
        type Send = FakeSend<P>;
        type SendRef = FakeSend<P>;

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
            FakeSend {
                result: self.result.clone(),
                polls: self.polls.clone(),
                release: None,
                entered: None,
                _payload: PhantomData,
            }
        }
    }

    impl<P> IntoFuture for FakeRequest<P>
    where
        P: Payload<Output: Clone + std::marker::Send> + std::marker::Send + 'static,
    {
        type Output = Result<P::Output, RequestError>;
        type IntoFuture = FakeSend<P>;

        fn into_future(self) -> Self::IntoFuture {
            self.send()
        }
    }

    struct FakeSend<P: Payload> {
        result: Result<P::Output, RequestError>,
        polls: Arc<AtomicUsize>,
        release: Option<Arc<Notify>>,
        entered: Option<Arc<Notify>>,
        _payload: PhantomData<fn() -> P>,
    }

    impl<P: Payload> Unpin for FakeSend<P> {}

    impl<P> Future for FakeSend<P>
    where
        P: Payload<Output: Clone + std::marker::Send>,
    {
        type Output = Result<P::Output, RequestError>;

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
    struct FakeRequester {
        result: Result<True, RequestError>,
        polls: Arc<AtomicUsize>,
        release: Option<Arc<Notify>>,
        entered: Option<Arc<Notify>>,
    }

    impl FakeRequester {
        fn new(result: Result<True, RequestError>) -> Self {
            Self { result, polls: Arc::new(AtomicUsize::new(0)), release: None, entered: None }
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
            FakeRequest<$T>
        };
    }

    impl Requester for FakeRequester {
        type Err = RequestError;
        type SendChatAction = FakeRequest<SendChatAction>;

        fn send_chat_action<C>(&self, chat_id: C, action: ChatAction) -> Self::SendChatAction
        where
            C: Into<Recipient>,
        {
            FakeRequest {
                _payload: SendChatAction::new(chat_id, action),
                result: self.result.clone(),
                polls: self.polls.clone(),
                release: self.release.clone(),
                entered: self.entered.clone(),
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
            scope: OutboundScope::Chat(OutboundChatKey::new(chat)),
            class: OutboundClass::new(class::MESSAGE_SEND),
            priority: OutboundPriority::NORMAL,
            weight: NonZeroU32::new(1).unwrap(),
        }
    }

    fn fake_chat_action(result: Result<True, RequestError>) -> FakeRequest<SendChatAction> {
        FakeRequest {
            _payload: SendChatAction::new(ChatId(1), ChatAction::Typing),
            result,
            polls: Arc::new(AtomicUsize::new(0)),
            release: None,
            entered: None,
        }
    }

    // ---------- scheduled request tests ----------

    #[tokio::test(start_paused = true)]
    async fn success_completes_the_permit_and_returns_the_output() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let scheduled =
            ScheduledRequest::new(fake_chat_action(Ok(True)), queue.clone(), chat_metadata(1));
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
            chat_metadata(1),
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
            chat_metadata(7),
        );
        let error =
            tokio::time::timeout(Duration::from_secs(1), scheduled).await.unwrap().unwrap_err();
        assert!(matches!(error, OutboundRequestError::Inner(RequestError::RetryAfter(_))));

        // The classifier reported the chat scope: a new acquire for chat 7
        // is blocked by the penalty, chat 8 is not.
        let blocked = queue.handle().acquire(chat_metadata(7));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        let other = queue.handle().acquire(chat_metadata(8));
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
        let request = FakeRequest {
            _payload: SendChatAction::new(ChatId(1), ChatAction::Typing),
            result: Ok(True),
            polls: polls.clone(),
            release: None,
            entered: None,
        };
        let scheduled = ScheduledRequest::new(request, queue.clone(), chat_metadata(1));
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
        let scheduled =
            ScheduledRequest::new(fake_chat_action(Ok(True)), queue.clone(), chat_metadata(1));
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
                    FakeRequest {
                        _payload: SendChatAction::new(ChatId(1), ChatAction::Typing),
                        result: Ok(True),
                        polls: Arc::new(AtomicUsize::new(0)),
                        release: Some(release),
                        entered: Some(entered),
                    },
                    queue,
                    chat_metadata(1),
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
            ScheduledRequest::new(fake_chat_action(Ok(True)), queue.clone(), chat_metadata(1))
                .on_lane(&lane);
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
            scope: OutboundScope::Chat(OutboundChatKey::new(9)),
            class: OutboundClass::new(class::MESSAGE_SEND),
            priority: OutboundPriority::NORMAL,
            weight: NonZeroU32::new(1).unwrap(),
        };
        let error = RequestError::RetryAfter(Seconds::from_seconds(3));
        assert_eq!(retry_after_scope(&chat, &error), OutboundScope::Chat(OutboundChatKey::new(9)));

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
        assert_eq!(foo, OutboundScope::Chat(OutboundChatKey::Username(Arc::from("foo"))));

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

    #[tokio::test(start_paused = true)]
    async fn adaptor_schedules_a_chat_action_with_chat_scope() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let outbound = Outbound::new(FakeRequester::new(Ok(True)), queue.clone());
        let scheduled = outbound.send_chat_action(ChatId(42), ChatAction::Typing);
        assert_eq!(scheduled.metadata().scope, OutboundScope::Chat(OutboundChatKey::new(42)));
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

    #[tokio::test(start_paused = true)]
    async fn integration_adaptor_queue_and_retry_after() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();

        // Полный путь: адаптор -> ScheduledRequest -> очередь -> результат.
        let outbound = Outbound::new(FakeRequester::new(Ok(True)), queue.clone());
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
