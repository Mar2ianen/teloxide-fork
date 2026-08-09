//! Unit and functional tests of the scheduler-backed compatibility layer.

use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use url::Url;

use crate::{
    adaptors::{
        throttle::{Limits, Settings},
        throttle_compat::ThrottleCompat,
    },
    errors::{AsResponseParameters, RequestError},
    outbound::WindowChatKind,
    payloads::*,
    requests::{HasPayload, Payload, Request, Requester},
    types::*,
};

/// Virtual-time tick used by the scenario drivers.
const TICK: Duration = Duration::from_millis(250);

/// A fake request with a programmed result: the first `fail_count`
/// executions fail with `RetryAfter(fail_after)`, the rest succeed. The
/// execution counter is shared through the clone, so a retried request
/// (cloned by `CompatRequest` or shared via `Arc` by the compatibility
/// request wrapper) observes the same program.
struct FakeRequest<P: Payload> {
    payload: P,
    /// Result of the inner `send()`.
    send_result: Result<P::Output, RequestError>,
    /// Result of the inner `send_ref()`.
    send_ref_result: Result<P::Output, RequestError>,
    /// Result of the inner `IntoFuture::into_future()`. Distinct from
    /// `send_result`: the previous worker contract executes a truly owned
    /// request through `owned.take().unwrap().await` (IntoFuture), so a
    /// custom requester whose `into_future` differs from `send` must
    /// observe the into_future result.
    into_future_result: Result<P::Output, RequestError>,
    fail_count: usize,
    fail_after: Duration,
    executions: Arc<AtomicUsize>,
    /// When non-zero, the send futures record their entry in `entered`
    /// (as this unique bit) and hang forever instead of resolving — used
    /// by the direct-send fallback test.
    hang_bit: usize,
    entered: Arc<AtomicUsize>,
    /// Shared clone counter: every `R::clone()` is observable, so the
    /// tests can assert that the compatibility layer never clones the
    /// inner request itself (it shares it through an `Arc`, like the
    /// previous throttle request wrapper).
    clones: Arc<AtomicUsize>,
}

impl<P> Clone for FakeRequest<P>
where
    P: Payload + Clone,
    P::Output: Clone,
{
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            payload: self.payload.clone(),
            send_result: self.send_result.clone(),
            send_ref_result: self.send_ref_result.clone(),
            into_future_result: self.into_future_result.clone(),
            fail_count: self.fail_count,
            fail_after: self.fail_after,
            executions: self.executions.clone(),
            hang_bit: self.hang_bit,
            entered: self.entered.clone(),
            clones: self.clones.clone(),
        }
    }
}

impl<P: Payload> HasPayload for FakeRequest<P> {
    type Payload = P;

    fn payload_mut(&mut self) -> &mut Self::Payload {
        &mut self.payload
    }

    fn payload_ref(&self) -> &Self::Payload {
        &self.payload
    }
}

impl<P> Request for FakeRequest<P>
where
    P: Payload<Output: Clone + Send> + Send + 'static,
{
    type Err = RequestError;
    type Send = FakeSend<P>;
    type SendRef = FakeSend<P>;

    fn send(self) -> Self::Send {
        FakeSend {
            result: self.send_result,
            fail_count: self.fail_count,
            fail_after: self.fail_after,
            executions: self.executions,
            hang_bit: self.hang_bit,
            entered: self.entered,
            _payload: PhantomData,
        }
    }

    fn send_ref(&self) -> Self::SendRef {
        FakeSend {
            result: self.send_ref_result.clone(),
            fail_count: self.fail_count,
            fail_after: self.fail_after,
            executions: self.executions.clone(),
            hang_bit: self.hang_bit,
            entered: self.entered.clone(),
            _payload: PhantomData,
        }
    }
}

impl<P> IntoFuture for FakeRequest<P>
where
    P: Payload<Output: Clone + Send> + Send + 'static,
{
    type Output = Result<P::Output, RequestError>;
    type IntoFuture = FakeSend<P>;

    fn into_future(self) -> Self::IntoFuture {
        FakeSend {
            result: self.into_future_result,
            fail_count: self.fail_count,
            fail_after: self.fail_after,
            executions: self.executions,
            hang_bit: self.hang_bit,
            entered: self.entered,
            _payload: PhantomData,
        }
    }
}

struct FakeSend<P: Payload> {
    result: Result<P::Output, RequestError>,
    fail_count: usize,
    fail_after: Duration,
    executions: Arc<AtomicUsize>,
    hang_bit: usize,
    entered: Arc<AtomicUsize>,
    _payload: PhantomData<fn() -> P>,
}

impl<P: Payload> Unpin for FakeSend<P> {}

impl<P> Future for FakeSend<P>
where
    P: Payload<Output: Clone>,
{
    type Output = Result<P::Output, RequestError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.hang_bit != 0 {
            this.entered.fetch_or(this.hang_bit, Ordering::SeqCst);
            return Poll::Pending;
        }
        if this.executions.fetch_add(1, Ordering::SeqCst) < this.fail_count {
            Poll::Ready(Err(RequestError::RetryAfter(Seconds::from_seconds(
                this.fail_after.as_secs() as u32,
            ))))
        } else {
            Poll::Ready(this.result.clone())
        }
    }
}

/// A minimal `Message` value (the output of the send methods); the
/// `id` distinguishes the `send()` and `send_ref()` results.
fn fake_message(id: u32) -> Message {
    serde_json::from_str(&format!(
        r#"{{"message_id":{id},"date":0,"chat":{{"id":1,"type":"private"}}}}"#
    ))
    .unwrap()
}

/// A bot whose `send_message`/`forward_messages`/`send_chat_action` return
/// programmed fake requests; every other method is unimplemented and never
/// called by these tests (the workers do not touch the bot unless
/// `check_slow_mode` is on).
#[derive(Clone, Debug)]
struct FakeBot {
    /// Requests yet to fail: the FIRST `fail_budget` requests created by
    /// this bot fail their first execution with `RetryAfter(fail_after)`.
    fail_budget: Arc<AtomicUsize>,
    fail_after: Duration,
    /// When set, the inner send futures record their entry in `entered`
    /// (as a unique bit per request) and hang forever — used to verify
    /// that the direct-send fallback releases the capacity slot BEFORE
    /// running the direct request.
    hang: bool,
    /// Bit allocation for hanging requests (the next request gets
    /// `1 << n`).
    next_hang_bit: Arc<AtomicUsize>,
    /// Bitmap of the hanging requests that entered their inner send.
    entered: Arc<AtomicUsize>,
    /// Shared counter of `FakeRequest::clone` calls (see
    /// [`FakeRequest::clones`]).
    clones: Arc<AtomicUsize>,
}

impl FakeBot {
    fn ok() -> Self {
        Self {
            fail_budget: Arc::new(AtomicUsize::new(0)),
            fail_after: Duration::from_secs(3),
            hang: false,
            next_hang_bit: Arc::new(AtomicUsize::new(0)),
            entered: Arc::new(AtomicUsize::new(0)),
            clones: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn hanging() -> Self {
        Self { hang: true, ..Self::ok() }
    }

    fn failing_first(mut self, count: usize, after: Duration) -> Self {
        self.fail_budget = Arc::new(AtomicUsize::new(count));
        self.fail_after = after;
        self
    }

    /// The fail count for the next created request (consumes the budget).
    fn next_fail_count(&self) -> usize {
        let remaining = self.fail_budget.load(Ordering::SeqCst);
        self.fail_budget.store(remaining.saturating_sub(1), Ordering::SeqCst);
        usize::from(remaining > 0)
    }

    /// The hang bit for the next created request, or 0 when the hang
    /// mode is off.
    fn next_hang_bit(&self) -> usize {
        if !self.hang {
            return 0;
        }
        1 << self.next_hang_bit.fetch_add(1, Ordering::SeqCst)
    }
}

macro_rules! f_unused {
    ($m:ident $this:ident ($($arg:ident : $T:ty),*)) => {{
        let _ = $this;
        $( let _ = $arg; )*
        unimplemented!("fake bot method {} is not used in these tests", stringify!($m))
    }};
}

macro_rules! fty {
    ($T:ident) => {
        FakeRequest<$T>
    };
}

/// The request type of every unused [`StatefulBot`] method.
macro_rules! fty_stateful {
    ($T:ident) => {
        StatefulRequest<$T>
    };
}

impl Requester for FakeBot {
    type Err = RequestError;
    type SendMessage = FakeRequest<SendMessage>;
    type ForwardMessages = FakeRequest<ForwardMessages>;
    type SendChatAction = FakeRequest<SendChatAction>;

    fn send_message<C, T>(&self, chat_id: C, text: T) -> Self::SendMessage
    where
        C: Into<Recipient>,
        T: Into<String>,
    {
        FakeRequest {
            payload: SendMessage::new(chat_id, text),
            send_result: Ok(fake_message(1)),
            send_ref_result: Ok(fake_message(2)),
            into_future_result: Ok(fake_message(3)),
            fail_count: self.next_fail_count(),
            fail_after: self.fail_after,
            executions: Arc::new(AtomicUsize::new(0)),
            hang_bit: self.next_hang_bit(),
            entered: self.entered.clone(),
            clones: self.clones.clone(),
        }
    }

    fn forward_messages<C, F, M>(
        &self,
        chat_id: C,
        from_chat_id: F,
        message_ids: M,
    ) -> Self::ForwardMessages
    where
        C: Into<Recipient>,
        F: Into<Recipient>,
        M: IntoIterator<Item = MessageId>,
    {
        FakeRequest {
            payload: ForwardMessages::new(chat_id, from_chat_id, message_ids),
            send_result: Ok(vec![MessageId(1)]),
            send_ref_result: Ok(vec![MessageId(2)]),
            into_future_result: Ok(vec![MessageId(3)]),
            fail_count: self.next_fail_count(),
            fail_after: self.fail_after,
            executions: Arc::new(AtomicUsize::new(0)),
            hang_bit: self.next_hang_bit(),
            entered: self.entered.clone(),
            clones: self.clones.clone(),
        }
    }

    fn send_chat_action<C>(&self, chat_id: C, action: ChatAction) -> Self::SendChatAction
    where
        C: Into<Recipient>,
    {
        FakeRequest {
            payload: SendChatAction::new(chat_id, action),
            send_result: Ok(True),
            send_ref_result: Ok(True),
            into_future_result: Ok(True),
            fail_count: self.next_fail_count(),
            fail_after: self.fail_after,
            executions: Arc::new(AtomicUsize::new(0)),
            hang_bit: self.next_hang_bit(),
            entered: self.entered.clone(),
            clones: self.clones.clone(),
        }
    }

    requester_forward! {
        get_me,
        get_updates,
        send_photo,
        send_live_photo,
        send_audio,
        send_document,
        send_video,
        send_animation,
        send_voice,
        send_video_note,
        send_paid_media,
        send_media_group,
        send_location,
        send_venue,
        send_contact,
        send_poll,
        send_checklist,
        send_dice,
        send_sticker,
        send_invoice,
        send_game,
        send_rich_message,
        forward_message,
        copy_message,
        copy_messages
        => f_unused, fty
    }

    requester_forward! {
        set_webhook,
        delete_webhook,
        get_webhook_info,
        log_out,
        close,
        edit_message_live_location,
        edit_message_live_location_inline,
        stop_message_live_location,
        stop_message_live_location_inline,
        edit_message_checklist,
        set_message_reaction,
        get_user_profile_photos,
        set_user_emoji_status,
        get_file,
        kick_chat_member,
        ban_chat_member,
        unban_chat_member,
        restrict_chat_member,
        promote_chat_member,
        set_chat_administrator_custom_title,
        ban_chat_sender_chat,
        unban_chat_sender_chat,
        set_chat_permissions,
        export_chat_invite_link,
        create_chat_invite_link,
        edit_chat_invite_link,
        create_chat_subscription_invite_link,
        edit_chat_subscription_invite_link,
        revoke_chat_invite_link,
        set_chat_photo,
        delete_chat_photo,
        set_chat_title,
        set_chat_description,
        pin_chat_message,
        unpin_chat_message,
        unpin_all_chat_messages,
        leave_chat,
        get_chat,
        get_chat_administrators,
        get_chat_members_count,
        get_chat_member_count,
        get_chat_member,
        set_chat_sticker_set,
        delete_chat_sticker_set,
        get_forum_topic_icon_stickers,
        create_forum_topic,
        edit_forum_topic,
        close_forum_topic,
        reopen_forum_topic,
        delete_forum_topic,
        unpin_all_forum_topic_messages,
        edit_general_forum_topic,
        close_general_forum_topic,
        reopen_general_forum_topic,
        hide_general_forum_topic,
        unhide_general_forum_topic,
        unpin_all_general_forum_topic_messages,
        answer_callback_query,
        get_user_chat_boosts,
        set_my_commands,
        get_business_connection,
        get_my_commands,
        set_my_name,
        get_my_name,
        set_my_description,
        get_my_description,
        set_my_short_description,
        get_my_short_description,
        set_chat_menu_button,
        get_chat_menu_button,
        set_my_default_administrator_rights,
        get_my_default_administrator_rights,
        delete_my_commands,
        answer_inline_query,
        answer_web_app_query,
        save_prepared_inline_message,
        edit_message_text,
        edit_message_text_inline,
        edit_message_caption,
        edit_message_caption_inline,
        edit_message_media,
        edit_message_media_inline,
        edit_message_reply_markup,
        edit_message_reply_markup_inline,
        stop_poll,
        approve_suggested_post,
        decline_suggested_post,
        delete_message,
        delete_messages,
        get_sticker_set,
        get_custom_emoji_stickers,
        upload_sticker_file,
        create_new_sticker_set,
        add_sticker_to_set,
        set_sticker_position_in_set,
        delete_sticker_from_set,
        replace_sticker_in_set,
        set_sticker_set_thumbnail,
        set_custom_emoji_sticker_set_thumbnail,
        set_sticker_set_title,
        delete_sticker_set,
        set_sticker_emoji_list,
        set_sticker_keywords,
        set_sticker_mask_position,
        get_available_gifts,
        send_gift,
        send_gift_chat,
        gift_premium_subscription,
        verify_user,
        verify_chat,
        remove_user_verification,
        remove_chat_verification,
        read_business_message,
        delete_business_messages,
        set_business_account_name,
        set_business_account_username,
        set_business_account_bio,
        set_business_account_profile_photo,
        remove_business_account_profile_photo,
        set_business_account_gift_settings,
        get_business_account_star_balance,
        transfer_business_account_stars,
        get_business_account_gifts,
        convert_gift_to_stars,
        upgrade_gift,
        transfer_gift,
        post_story,
        edit_story,
        delete_story,
        send_message_draft,
        get_user_profile_audios,
        set_chat_member_tag,
        get_user_personal_chat_messages,
        answer_guest_query,
        get_managed_bot_token,
        replace_managed_bot_token,
        get_managed_bot_access_settings,
        set_managed_bot_access_settings,
        set_my_profile_photo,
        remove_my_profile_photo,
        get_user_gifts,
        get_chat_gifts,
        repost_story,
        save_prepared_keyboard_button,
        delete_message_reaction,
        delete_all_message_reactions,
        answer_shipping_query,
        create_invoice_link,
        answer_pre_checkout_query,
        get_my_star_balance,
        get_star_transactions,
        refund_star_payment,
        edit_user_star_subscription,
        set_passport_data_errors,
        set_game_score,
        set_game_score_inline,
        answer_chat_join_request_query,
        send_chat_join_request_web_app,
        edit_ephemeral_message_text,
        edit_ephemeral_message_media,
        edit_ephemeral_message_caption,
        edit_ephemeral_message_reply_markup,
        delete_ephemeral_message,
        send_rich_message_draft,
        approve_chat_join_request,
        decline_chat_join_request,
        get_game_high_scores
        => f_unused, fty
    }
}

/// A `StatefulError` reports `RetryAfter(10)` on the FIRST
/// `retry_after()` call and nothing afterwards. The compatibility layer
/// must classify an outcome exactly once — a second call would turn the
/// outcome into a plain failure (no freeze, no retry), while the legacy
/// worker classifies once and keeps the value.
#[derive(Clone, Debug)]
struct StatefulError {
    retry_calls: Arc<AtomicUsize>,
}

impl std::fmt::Display for StatefulError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stateful test error")
    }
}

impl std::error::Error for StatefulError {}

impl AsResponseParameters for StatefulError {
    fn retry_after(&self) -> Option<Seconds> {
        if self.retry_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Some(Seconds::from_seconds(10))
        } else {
            None
        }
    }

    fn response_parameters(&self) -> Option<ResponseParameters> {
        self.retry_after().map(ResponseParameters::RetryAfter)
    }
}

/// A request whose first execution fails with a [`StatefulError`] (per
/// request) and whose later executions succeed.
#[derive(Clone)]
struct StatefulRequest<P: Payload> {
    payload: P,
    /// The successful result of the inner send.
    send_ref_result: Result<P::Output, StatefulError>,
    executions: Arc<AtomicUsize>,
    retry_calls: Arc<AtomicUsize>,
}

impl<P: Payload> HasPayload for StatefulRequest<P> {
    type Payload = P;

    fn payload_mut(&mut self) -> &mut Self::Payload {
        &mut self.payload
    }

    fn payload_ref(&self) -> &Self::Payload {
        &self.payload
    }
}

impl<P> Request for StatefulRequest<P>
where
    P: Payload<Output: Clone + Send> + Send + 'static,
{
    type Err = StatefulError;
    type Send = StatefulSend<P>;
    type SendRef = StatefulSend<P>;

    fn send(self) -> Self::Send {
        StatefulSend {
            executions: self.executions,
            retry_calls: self.retry_calls,
            result: self.send_ref_result,
            _payload: PhantomData,
        }
    }

    fn send_ref(&self) -> Self::SendRef {
        StatefulSend {
            executions: self.executions.clone(),
            retry_calls: self.retry_calls.clone(),
            result: self.send_ref_result.clone(),
            _payload: PhantomData,
        }
    }
}

impl<P> IntoFuture for StatefulRequest<P>
where
    P: Payload<Output: Clone + Send> + Send + 'static,
{
    type Output = Result<P::Output, StatefulError>;
    type IntoFuture = StatefulSend<P>;

    fn into_future(self) -> Self::IntoFuture {
        self.send()
    }
}

struct StatefulSend<P: Payload> {
    executions: Arc<AtomicUsize>,
    retry_calls: Arc<AtomicUsize>,
    result: Result<P::Output, StatefulError>,
    _payload: PhantomData<fn() -> P>,
}

impl<P: Payload> Unpin for StatefulSend<P> {}

impl<P> Future for StatefulSend<P>
where
    P: Payload<Output: Clone>,
{
    type Output = Result<P::Output, StatefulError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.executions.fetch_add(1, Ordering::SeqCst) == 0 {
            Poll::Ready(Err(StatefulError { retry_calls: this.retry_calls.clone() }))
        } else {
            Poll::Ready(this.result.clone())
        }
    }
}

/// A bot whose requests are [`StatefulRequest`]s. The unused methods are
/// never called and reuse [`FakeRequest`] types for the
/// `ThrottleCompat` where-clauses.
#[derive(Clone)]
struct StatefulBot;

impl Requester for StatefulBot {
    type Err = StatefulError;
    type SendMessage = StatefulRequest<SendMessage>;

    fn send_message<C, T>(&self, chat_id: C, text: T) -> Self::SendMessage
    where
        C: Into<Recipient>,
        T: Into<String>,
    {
        StatefulRequest {
            payload: SendMessage::new(chat_id, text),
            send_ref_result: Ok(fake_message(2)),
            executions: Arc::new(AtomicUsize::new(0)),
            retry_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    requester_forward! {
        send_rich_message,
        forward_message,
        forward_messages,
        copy_message,
        copy_messages,
        send_photo,
        send_live_photo,
        send_audio,
        send_document,
        send_video,
        send_animation,
        send_voice,
        send_video_note,
        send_paid_media,
        send_media_group,
        send_location,
        send_venue,
        send_contact,
        send_poll,
        send_checklist,
        send_dice,
        send_sticker,
        send_invoice,
        send_game,
        send_chat_action
        => f_unused, fty_stateful
    }

    requester_forward! {
        get_me,
        get_updates,
        set_webhook,
        delete_webhook,
        get_webhook_info,
        log_out,
        close,
        edit_message_live_location,
        edit_message_live_location_inline,
        stop_message_live_location,
        stop_message_live_location_inline,
        edit_message_checklist,
        set_message_reaction,
        get_user_profile_photos,
        set_user_emoji_status,
        get_file,
        kick_chat_member,
        ban_chat_member,
        unban_chat_member,
        restrict_chat_member,
        promote_chat_member,
        set_chat_administrator_custom_title,
        ban_chat_sender_chat,
        unban_chat_sender_chat,
        set_chat_permissions,
        export_chat_invite_link,
        create_chat_invite_link,
        edit_chat_invite_link,
        create_chat_subscription_invite_link,
        edit_chat_subscription_invite_link,
        revoke_chat_invite_link,
        set_chat_photo,
        delete_chat_photo,
        set_chat_title,
        set_chat_description,
        pin_chat_message,
        unpin_chat_message,
        unpin_all_chat_messages,
        leave_chat,
        get_chat,
        get_chat_administrators,
        get_chat_members_count,
        get_chat_member_count,
        get_chat_member,
        set_chat_sticker_set,
        delete_chat_sticker_set,
        get_forum_topic_icon_stickers,
        create_forum_topic,
        edit_forum_topic,
        close_forum_topic,
        reopen_forum_topic,
        delete_forum_topic,
        unpin_all_forum_topic_messages,
        edit_general_forum_topic,
        close_general_forum_topic,
        reopen_general_forum_topic,
        hide_general_forum_topic,
        unhide_general_forum_topic,
        unpin_all_general_forum_topic_messages,
        answer_callback_query,
        get_user_chat_boosts,
        set_my_commands,
        get_business_connection,
        get_my_commands,
        set_my_name,
        get_my_name,
        set_my_description,
        get_my_description,
        set_my_short_description,
        get_my_short_description,
        set_chat_menu_button,
        get_chat_menu_button,
        set_my_default_administrator_rights,
        get_my_default_administrator_rights,
        delete_my_commands,
        answer_inline_query,
        answer_web_app_query,
        save_prepared_inline_message,
        edit_message_text,
        edit_message_text_inline,
        edit_message_caption,
        edit_message_caption_inline,
        edit_message_media,
        edit_message_media_inline,
        edit_message_reply_markup,
        edit_message_reply_markup_inline,
        stop_poll,
        approve_suggested_post,
        decline_suggested_post,
        delete_message,
        delete_messages,
        get_sticker_set,
        get_custom_emoji_stickers,
        upload_sticker_file,
        create_new_sticker_set,
        add_sticker_to_set,
        set_sticker_position_in_set,
        delete_sticker_from_set,
        replace_sticker_in_set,
        set_sticker_set_thumbnail,
        set_custom_emoji_sticker_set_thumbnail,
        set_sticker_set_title,
        delete_sticker_set,
        set_sticker_emoji_list,
        set_sticker_keywords,
        set_sticker_mask_position,
        get_available_gifts,
        send_gift,
        send_gift_chat,
        gift_premium_subscription,
        verify_user,
        verify_chat,
        remove_user_verification,
        remove_chat_verification,
        read_business_message,
        delete_business_messages,
        set_business_account_name,
        set_business_account_username,
        set_business_account_bio,
        set_business_account_profile_photo,
        remove_business_account_profile_photo,
        set_business_account_gift_settings,
        get_business_account_star_balance,
        transfer_business_account_stars,
        get_business_account_gifts,
        convert_gift_to_stars,
        upgrade_gift,
        transfer_gift,
        post_story,
        edit_story,
        delete_story,
        send_message_draft,
        get_user_profile_audios,
        set_chat_member_tag,
        get_user_personal_chat_messages,
        answer_guest_query,
        get_managed_bot_token,
        replace_managed_bot_token,
        get_managed_bot_access_settings,
        set_managed_bot_access_settings,
        set_my_profile_photo,
        remove_my_profile_photo,
        get_user_gifts,
        get_chat_gifts,
        repost_story,
        save_prepared_keyboard_button,
        delete_message_reaction,
        delete_all_message_reactions,
        answer_shipping_query,
        create_invoice_link,
        answer_pre_checkout_query,
        get_my_star_balance,
        get_star_transactions,
        refund_star_payment,
        edit_user_star_subscription,
        set_passport_data_errors,
        set_game_score,
        set_game_score_inline,
        answer_chat_join_request_query,
        send_chat_join_request_web_app,
        edit_ephemeral_message_text,
        edit_ephemeral_message_media,
        edit_ephemeral_message_caption,
        edit_ephemeral_message_reply_markup,
        delete_ephemeral_message,
        send_rich_message_draft,
        approve_chat_join_request,
        decline_chat_join_request,
        get_game_high_scores
        => f_unused, fty_stateful
    }
}

/// Drives the runtime and polls every unresolved future once per round,
/// for a bounded number of rounds. Used where the completion barrier
/// round-trips span more rounds than `drain_rounds`' two-quiet-round
/// heuristic allows (e.g. the first round after a time advance is quiet
/// while the actor processes the fired timer).
async fn drive_rounds(
    futs: &mut [Pin<Box<dyn Future<Output = ()>>>],
    resolved: &mut [bool],
    rounds: usize,
) {
    for _ in 0..rounds {
        tokio::task::yield_now().await;
        for (i, fut) in futs.iter_mut().enumerate() {
            if !resolved[i] && poll_once(fut.as_mut()).is_ready() {
                resolved[i] = true;
            }
        }
    }
}

/// Peeks a pinned future once with a noop waker.
fn poll_once<F: Future + ?Sized>(fut: Pin<&mut F>) -> Poll<F::Output> {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    fut.poll(&mut cx)
}

/// Polls all unresolved futures in rounds until two consecutive rounds
/// resolve nothing.
///
/// A round is: yield to the actor, then poll every future once. A future
/// advancing through the completion barrier returns `Pending` mid-round
/// and resolves only on the NEXT round (the actor must process the
/// completion first), so a single quiet round cannot be trusted: the
/// drain ends only after two quiet rounds in a row.
async fn drain_rounds(
    futs: &mut [Pin<Box<dyn Future<Output = ()>>>],
    resolved: &mut [bool],
    reverse: bool,
) {
    let indices: Vec<usize> =
        if reverse { (0..futs.len()).rev().collect() } else { (0..futs.len()).collect() };
    let mut quiet_rounds = 0;
    loop {
        tokio::task::yield_now().await;
        let mut progress = false;
        for i in &indices {
            if !resolved[*i] && poll_once(futs[*i].as_mut()).is_ready() {
                resolved[*i] = true;
                progress = true;
            }
        }
        if progress {
            quiet_rounds = 0;
        } else {
            quiet_rounds += 1;
            if quiet_rounds >= 2 {
                break;
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn public_throttle_alias_uses_outbound_scheduler() {
    let (throttle, actor) = crate::adaptors::Throttle::new(FakeBot::ok(), default_limits());
    let actor_task = tokio::spawn(actor);
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        throttle.send_message(ChatId(1), "public alias"),
    )
    .await
    .unwrap();
    assert!(result.is_ok());
    actor_task.abort();
}

/// Drives one engine: submits `sends` (chat, text) at t=0, ticks the
/// paused clock, and records the completion order with timestamps
/// (relative to the virtual t=0).
async fn drive_completions<R: Requester>(
    bot: R,
    sends: &[(i64, &str)],
    reverse: bool,
) -> Vec<(usize, Duration)>
where
    R::SendMessage: 'static,
{
    // The paused clock is frozen until the first `advance`, so capturing
    // the start before the first polls gives the true t=0.
    let start = tokio::time::Instant::now();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    for (i, &(chat, text)) in sends.iter().enumerate() {
        let order = Arc::clone(&order);
        let request = bot.send_message(ChatId(chat), text);
        futures.push(Box::pin(async move {
            let _ = request.await;
            order.lock().unwrap().push((i, tokio::time::Instant::now() - start));
        }));
    }
    let mut resolved = vec![false; futures.len()];
    // First poll: enqueue everything at t=0 in the SAME order as the
    // drain rounds (the poll order is the enqueue order, so the reverse
    // variant exercises a reversed ingress tie-break), then drain what
    // the actor grants immediately.
    let indices: Vec<usize> =
        if reverse { (0..futures.len()).rev().collect() } else { (0..futures.len()).collect() };
    for i in indices {
        let _ = poll_once(futures[i].as_mut());
    }
    drain_rounds(&mut futures, &mut resolved, reverse).await;

    let mut ticks = 0;
    while resolved.iter().any(|r| !r) && ticks < 400 {
        tokio::time::advance(TICK).await;
        drain_rounds(&mut futures, &mut resolved, reverse).await;
        ticks += 1;
    }
    assert!(resolved.iter().all(|r| *r), "scenario did not finish within the virtual budget");
    let order = order.lock().unwrap().clone();
    order
}

/// Common per-second limits used by most scenarios.
fn default_limits() -> Limits {
    Limits {
        messages_per_sec_chat: 1,
        messages_per_min_chat: 20,
        messages_per_min_channel_or_supergroup: 10,
        messages_per_sec_overall: 30,
    }
}

// ---------- unit: limits mapping ----------

#[test]
fn limits_map_to_outbound_windows() {
    use crate::outbound::{OutboundLimits, WindowLimit};
    let mapped = super::to_outbound_limits(default_limits());
    assert_eq!(
        mapped,
        OutboundLimits {
            global: vec![WindowLimit::new(30, Duration::from_secs(1))],
            chat: vec![
                WindowLimit::new(1, Duration::from_secs(1)),
                WindowLimit::for_chat_kind(20, Duration::from_secs(60), WindowChatKind::NonChannel,),
                WindowLimit::for_chat_kind(
                    10,
                    Duration::from_secs(60),
                    WindowChatKind::ChannelOrSupergroup,
                ),
            ],
        }
    );
}

// ---------- compat functional tests ----------

#[tokio::test(start_paused = true)]
async fn compat_zero_global_rate_limit_pauses_until_reconfigured() {
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 0;
    let (compat, actor) = ThrottleCompat::with_settings(
        FakeBot::ok(),
        Settings { limits, retry: true, check_slow_mode: false, ..Settings::default() },
    );
    let actor_task = tokio::spawn(actor);

    let request = compat.send_message(ChatId(1), "paused").send();
    tokio::pin!(request);
    tokio::task::yield_now().await;
    assert!(futures::poll!(request.as_mut()).is_pending());

    let mut enabled = limits;
    enabled.messages_per_sec_overall = 1;
    compat.set_limits(enabled).await;

    let result = tokio::time::timeout(Duration::from_secs(1), request).await.unwrap();
    assert!(result.is_ok(), "request remained paused after global limit reconfiguration");
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_enforces_the_per_chat_second_limit_in_fifo_order() {
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let actor_task = tokio::spawn(actor);

    let sends = [(1, "a"), (1, "b"), (1, "c")];
    let order = drive_completions(compat, &sends, false).await;

    assert_eq!(
        order,
        vec![(0, Duration::ZERO), (1, Duration::from_secs(1)), (2, Duration::from_secs(2))]
    );
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_applies_the_global_limit_across_chats() {
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 2;
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), limits);
    let actor_task = tokio::spawn(actor);

    let sends = [(1, "a"), (2, "b"), (1, "c"), (2, "d")];
    let order = drive_completions(compat, &sends, false).await;

    assert_eq!(
        order,
        vec![
            (0, Duration::ZERO),
            (1, Duration::ZERO),
            (2, Duration::from_secs(1)),
            (3, Duration::from_secs(1)),
        ]
    );
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_counts_a_batch_method_as_one_message() {
    // A batch of 4 forwarded messages must cost ONE unit (the legacy
    // worker counts API calls): with `messages_per_sec_chat = 1` the
    // forward is admitted at t=0 and the following send at t=1. Without
    // the weight-1 override the forward would carry weight 4 and be
    // rejected outright (`WeightExceedsWindow`).
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    let forward = compat.forward_messages(
        ChatId(1),
        ChatId(2),
        [MessageId(1), MessageId(2), MessageId(3), MessageId(4)],
    );
    let order0 = Arc::clone(&order);
    futs.push(Box::pin(async move {
        let _ = forward.await;
        order0.lock().unwrap().push((0, tokio::time::Instant::now()));
    }));
    let send = compat.send_message(ChatId(1), "after");
    let order1 = Arc::clone(&order);
    futs.push(Box::pin(async move {
        let _ = send.await;
        order1.lock().unwrap().push((1, tokio::time::Instant::now()));
    }));
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    let mut resolved = [false; 2];
    drain_rounds(&mut futs, &mut resolved, false).await;
    let mut ticks = 0;
    while resolved.iter().any(|r| !r) && ticks < 20 {
        tokio::time::advance(TICK).await;
        drain_rounds(&mut futs, &mut resolved, false).await;
        ticks += 1;
    }
    assert!(resolved.iter().all(|r| *r));
    let order = order.lock().unwrap().clone();
    assert_eq!(order[0].0, 0, "forward первым");
    assert_eq!(order[1].0, 1, "send вторым");
    assert_eq!(order[1].1 - order[0].1, Duration::from_secs(1), "forward стоит ровно 1 единицу");
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_retry_after_penalizes_globally_and_retries() {
    let bot = FakeBot::ok().failing_first(1, Duration::from_secs(3));
    let (compat, actor) = ThrottleCompat::new(bot, default_limits());
    let actor_task = tokio::spawn(actor);

    // A1 fails with RetryAfter(3s); B1 (another chat) is queued behind it.
    // A global penalty freezes all matching requests until it expires, so
    // B1 must NOT be granted at t=1 (when A1's per-chat 1s window frees)
    // but only at t=3 together with the retried A1.
    let sends = [(1, "a1"), (2, "b1"), (1, "a2")];
    let order = drive_completions(compat, &sends, false).await;

    // B1 is queued before the penalty and its chat is free, so
    // freeze and its chat is free), the freeze then blocks A2 until t=3,
    // and the retried A1 re-queues behind A2 (chat 1/s window).
    assert_eq!(
        order,
        vec![(1, Duration::ZERO), (2, Duration::from_secs(3)), (0, Duration::from_secs(4)),]
    );
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_on_queue_full_notifies_and_sends_everything() {
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 2; // backlog bound = 2
    let full: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let settings = Settings {
        limits,
        on_queue_full: {
            let full = Arc::clone(&full);
            Box::new(move |pending| {
                full.lock().unwrap().push(pending);
                Box::pin(async move {})
            })
        },
        retry: true,
        check_slow_mode: false,
    };
    let compat = ThrottleCompat::spawn_with_settings(FakeBot::ok(), settings);

    // Burst 1 at t=0: 5 sends with a backlog bound of 2. The first
    // overflow must fire IMMEDIATELY with the exact bound (2), the
    // retries during the next second are rate-limited away.
    let sends = [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")];
    let order = drive_completions(compat.clone(), &sends, false).await;
    let order: Vec<usize> = order.into_iter().map(|(i, _)| i).collect();
    assert_eq!(order, vec![0, 1, 2, 3, 4], "все запросы проходят в порядке подачи");

    // Burst 2 at t=5 (past the 4-second rate limit): the overflow fires
    // again with the same bound.
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    let sends2 = [(6, "f"), (7, "g"), (8, "h"), (9, "i"), (10, "j")];
    let _ = drive_completions(compat, &sends2, false).await;

    let full = full.lock().unwrap().clone();
    assert_eq!(full, vec![2, 2], "ровно два overflow-а, каждый с точным размером backlog-а");
}

#[tokio::test(start_paused = true)]
async fn compat_passthrough_methods_bypass_the_queue() {
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    let action = compat.send_chat_action(ChatId(1), ChatAction::Typing);
    let order0 = Arc::clone(&order);
    futs.push(Box::pin(async move {
        let _ = action.await;
        order0.lock().unwrap().push(0);
    }));
    let send1 = compat.send_message(ChatId(1), "one");
    let order1 = Arc::clone(&order);
    futs.push(Box::pin(async move {
        let _ = send1.await;
        order1.lock().unwrap().push(1);
    }));
    let send2 = compat.send_message(ChatId(1), "two");
    let order2 = Arc::clone(&order);
    futs.push(Box::pin(async move {
        let _ = send2.await;
        order2.lock().unwrap().push(2);
    }));
    let mut resolved = [false; 3];
    for (i, fut) in futs.iter_mut().enumerate() {
        // The chat action bypasses the queue: it can complete on the
        // FIRST poll, so the resolved bit must be set before the drain.
        if poll_once(fut.as_mut()).is_ready() {
            resolved[i] = true;
        }
    }
    drain_rounds(&mut futs, &mut resolved, false).await;
    let mut ticks = 0;
    while resolved.iter().any(|r| !r) && ticks < 20 {
        tokio::time::advance(TICK).await;
        drain_rounds(&mut futs, &mut resolved, false).await;
        ticks += 1;
    }
    assert!(resolved.iter().all(|r| *r));
    // chat action (not throttled) resolves immediately; the two sends obey
    // the 1/s chat limit.
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_set_limits_changes_the_windows() {
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let actor_task = tokio::spawn(actor);

    let first = compat.send_message(ChatId(1), "first");
    let _ = tokio::time::timeout(Duration::from_secs(1), first).await.unwrap().unwrap();

    // Raise the per-chat second limit: two more sends must both complete
    // at the next second boundary.
    let mut new = default_limits();
    new.messages_per_sec_chat = 10;
    compat.set_limits(new).await;
    assert_eq!(compat.limits().await, new, "limits() следует за scheduler-ом");

    // A zero chat-minute limit pauses matching requests until a later
    // settings update raises it again.
    let mut zero = default_limits();
    zero.messages_per_min_chat = 0;
    compat.set_limits(zero).await;
    assert_eq!(compat.limits().await, zero, "нулевой лимит должен примениться");

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    for i in 0..2 {
        let order = Arc::clone(&order);
        let send = compat.send_message(ChatId(1), "more");
        futs.push(Box::pin(async move {
            let _ = send.await;
            order.lock().unwrap().push(i);
        }));
    }
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    let mut resolved = [false; 2];
    drain_rounds(&mut futs, &mut resolved, false).await;
    for _ in 0..5 {
        tokio::time::advance(TICK).await;
        drain_rounds(&mut futs, &mut resolved, false).await;
    }
    assert!(
        resolved.iter().all(|resolved| !resolved),
        "нулевой лимит должен поставить запросы на паузу"
    );

    compat.set_limits(new).await;
    let mut ticks = 0;
    while resolved.iter().any(|r| !r) && ticks < 20 {
        tokio::time::advance(TICK).await;
        drain_rounds(&mut futs, &mut resolved, false).await;
        ticks += 1;
    }
    assert!(resolved.iter().all(|r| *r), "обе отправки прошли после set_limits");
    assert_eq!(order.lock().unwrap().len(), 2);
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_distinguishes_channel_and_regular_minute_limits() {
    // Regular chats get `messages_per_min_chat` (20), channels and
    // supergroups get `messages_per_min_channel_or_supergroup` (10).
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30; // isolate the minute windows
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), limits);
    let actor_task = tokio::spawn(actor);

    // 11 sends to a regular chat: all fit the 20/min window.
    let mut all_futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let regular = Arc::new(Mutex::new(0usize));
    for _ in 0..11 {
        let regular = Arc::clone(&regular);
        let send = compat.send_message(ChatId(123), "r");
        all_futs.push(Box::pin(async move {
            let _ = send.await;
            *regular.lock().unwrap() += 1;
        }));
    }
    // 11 sends to a channel: only 10 fit the 10/min window; the 11th
    // waits until the first entry expires.
    let channel_done = Arc::new(Mutex::new(0usize));
    for _ in 0..11 {
        let channel_done = Arc::clone(&channel_done);
        let send = compat.send_message(ChatId(-1001234567890), "c");
        all_futs.push(Box::pin(async move {
            let _ = send.await;
            *channel_done.lock().unwrap() += 1;
        }));
    }
    let mut resolved = vec![false; all_futs.len()];
    for fut in all_futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    drain_rounds(&mut all_futs, &mut resolved, false).await;

    // t=1s: the 1s windows are irrelevant (capacity 30), so the regular
    // chat has all 11 granted; the channel only 10.
    tokio::time::advance(Duration::from_secs(1)).await;
    drain_rounds(&mut all_futs, &mut resolved, false).await;
    assert_eq!(*regular.lock().unwrap(), 11, "regular chat: 20/min window");
    assert_eq!(*channel_done.lock().unwrap(), 10, "channel: 10/min window");

    // The 11th channel send completes when the first entry expires.
    tokio::time::advance(Duration::from_secs(60)).await;
    drain_rounds(&mut all_futs, &mut resolved, false).await;
    assert_eq!(*channel_done.lock().unwrap(), 11);
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_inner_execution_path_honors_settings() {
    // Default settings (`retry = true`) execute the inner `send_ref()` even
    // for an owned request. The fake bot returns message_id 2 from
    // `send_ref()` and 1 from `send()`.
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let actor_task = tokio::spawn(actor);
    let output = tokio::time::timeout(Duration::from_secs(1), compat.send_message(ChatId(1), "x"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.id.0, 2, "retry=true -> inner send_ref");
    actor_task.abort();

    // `retry = false` + owned `send()` still uses the request's
    // `IntoFuture` implementation, not `Request::send`.
    let settings = Settings { limits: default_limits(), ..<_>::default() }.no_retry();
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let actor_task = tokio::spawn(actor);
    let output = tokio::time::timeout(Duration::from_secs(1), compat.send_message(ChatId(1), "x"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.id.0, 3, "owned + retry=false -> IntoFuture");
    actor_task.abort();

    // `retry = false` + outer `send_ref()` uses the inner `send_ref()`.
    let settings = Settings { limits: default_limits(), ..<_>::default() }.no_retry();
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let actor_task = tokio::spawn(actor);
    let output = tokio::time::timeout(
        Duration::from_secs(1),
        compat.send_message(ChatId(1), "x").send_ref(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(output.id.0, 2, "outer send_ref -> inner send_ref");
    actor_task.abort();
}

// ---------- scheduler ordering regressions ----------

// ---------- capacity semaphore: lifecycle regressions ----------
//
// The admission gate is a `tokio::sync::Semaphore` (one permit per backlog
// slot, capacity `messages_per_sec_overall`). These tests pin the
// lifecycle properties the hand-rolled oneshot gate of the previous
// revision did NOT have: lost wakeups, dead waiters blocking the queue,
// waiters not woken by the actor's death and a FIFO order that only held
// under a specific poll sequence.

#[tokio::test(start_paused = true)]
async fn capacity_waiters_preserve_fifo_order() {
    // Backlog bound 2, all per-second limits raised: the ONLY ordering
    // constraint is the capacity gate. R2/R3 park on the semaphore when
    // R0/R1 fill the backlog; both are woken by the two completions and
    // must be admitted in submission order.
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 2;
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), limits);
    let actor_task = tokio::spawn(actor);

    let sends = [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")];
    let order = drive_completions(compat, &sends, false).await;
    let order: Vec<usize> = order.into_iter().map(|(i, _)| i).collect();
    assert_eq!(order, vec![0, 1, 2, 3, 4], "waiters сохраняют FIFO порядок подачи");
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn cancelled_capacity_waiter_does_not_block_the_next() {
    // R0/R1 fill the backlog; R2 (head waiter) and R3 park on the
    // semaphore. R2 is cancelled WHILE PARKED, before any slot frees: the
    // slot released by R0's completion must wake R3 instead of being
    // lost behind the dead waiter.
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 2;
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), limits);
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    for i in 0..4 {
        let order = Arc::clone(&order);
        let send = compat.send_message(ChatId(1), "m");
        futs.push(Box::pin(async move {
            let _ = send.await;
            order.lock().unwrap().push(i);
        }));
    }
    let mut resolved = [false; 4];
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    // R2 is parked as the head waiter; cancel it before any slot frees
    // (the replacement future never resolves, so R2 stays "cancelled").
    futs[2] = Box::pin(std::future::pending::<()>());
    drain_rounds(&mut futs, &mut resolved, false).await;
    // R0/R1 complete at t=0 (global 2/s window); R3 takes R0's freed slot
    // and waits for the window at t=1.
    assert_eq!(resolved, [true, true, false, false]);
    tokio::time::advance(Duration::from_secs(1)).await;
    drain_rounds(&mut futs, &mut resolved, false).await;
    assert_eq!(
        resolved,
        [true, true, false, true],
        "отмена head waiter-а не должна блокировать следующего"
    );
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 3]);
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_pending_request_wakes_a_capacity_waiter() {
    // R0 primes a GLOBAL freeze (it fails with RetryAfter(10) and sleeps
    // OUTSIDE the queue, like the previous worker contract); R1/R2 submitted during
    // the freeze stay PENDING and fill the two backlog slots, so R3
    // parks on the semaphore. Cancelling R1 releases its slot AND
    // cancels its job, and the freed slot must reach R3 — the hand-rolled
    // gate lost exactly this wake-up, because `signal()` ran only after
    // a successful grant.
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 2; // backlog bound = 2
    let bot = FakeBot::ok().failing_first(1, Duration::from_secs(10));
    let (compat, actor) = ThrottleCompat::new(bot, limits);
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    {
        let order = Arc::clone(&order);
        let send = compat.send_message(ChatId(1), "freeze");
        futs.push(Box::pin(async move {
            let _ = send.await;
            order.lock().unwrap().push(0);
        }));
    }
    let mut resolved = [false; 4];
    let _ = poll_once(futs[0].as_mut());
    // Drive until R0 failed and the global penalty is registered (R0 is
    // now sleeping until t=10, holding no slot and no job).
    drain_rounds(&mut futs, &mut resolved, false).await;

    for i in 1..4 {
        let order = Arc::clone(&order);
        let send = compat.send_message(ChatId(i as i64 + 1), "m");
        futs.push(Box::pin(async move {
            let _ = send.await;
            order.lock().unwrap().push(i);
        }));
    }
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    // R1/R2 are pending behind the freeze (backlog full), R3 is parked.
    drain_rounds(&mut futs, &mut resolved, false).await;
    assert_eq!(resolved, [false, false, false, false], "freeze активен, все ждут");
    // Cancel R1 while its job is pending: the slot frees and R3 wakes
    // (the replacement future never resolves, so R1 stays "cancelled").
    futs[1] = Box::pin(std::future::pending::<()>());
    drain_rounds(&mut futs, &mut resolved, false).await;
    // The freeze expires at t=10: R2 (enqueued first) and R3 (admitted
    // on R1's freed slot) are granted; the retried R0 completes at t=11.
    // The clock is advanced IN STEPS so the actor runs an admission pass
    // at each deadline (a single long advance would coalesce both timer
    // firings into one pass at the final time).
    tokio::time::advance(Duration::from_secs(10)).await;
    drain_rounds(&mut futs, &mut resolved, false).await;
    assert_eq!(resolved, [false, false, true, true], "R2/R3 granted на t=10");
    tokio::time::advance(Duration::from_secs(1)).await;
    drain_rounds(&mut futs, &mut resolved, false).await;
    assert_eq!(resolved, [true, false, true, true], "отмена pending job-а будит capacity waiter");
    assert_eq!(*order.lock().unwrap(), vec![2, 3, 0]);
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn slot_freed_before_waiter_registration_is_not_lost() {
    // The gate of the previous revision lost this wake-up: a slot freed
    // while the waiting queue was empty was gone forever. With the
    // semaphore the freed permit IS the state, so a request that
    // registers afterwards finds it. R0/R1 fill the backlog, R2 is NOT
    // polled yet; R0 completes and frees a slot; R2's first poll must
    // then be admitted instead of parking forever.
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 2;
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), limits);
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    for i in 0..3 {
        let order = Arc::clone(&order);
        let send = compat.send_message(ChatId(i as i64 + 1), "m");
        futs.push(Box::pin(async move {
            let _ = send.await;
            order.lock().unwrap().push(i);
        }));
    }
    let mut resolved = [false; 3];
    // Only the first two requests are polled: they fill the backlog.
    let _ = poll_once(futs[0].as_mut());
    let _ = poll_once(futs[1].as_mut());
    // R0/R1 are granted at t=0 and free their slots; R2 registers only
    // inside this drain and must be admitted immediately.
    drain_rounds(&mut futs, &mut resolved, false).await;
    assert_eq!(resolved, [true, true, false], "R2 admitted, ждёт только global window");
    tokio::time::advance(Duration::from_secs(1)).await;
    drain_rounds(&mut futs, &mut resolved, false).await;
    assert!(resolved.iter().all(|r| *r), "slot, освобождённый до регистрации, не потерян");
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn actor_death_wakes_all_capacity_waiters_and_direct_sends() {
    // R0/R1 hold the two backlog slots; R2/R3 park on the semaphore.
    // Killing the actor resolves the pending acquires with `Closed`, the
    // released slots cascade through the waiters, and every request
    // degrades to a direct send after the actor drops its queue.
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    for i in 0..4 {
        let results = Arc::clone(&results);
        let send = compat.send_message(ChatId(1), "m");
        futs.push(Box::pin(async move {
            let output = send.await.unwrap();
            results.lock().unwrap().push((i, output.id.0));
        }));
    }
    let mut resolved = [false; 4];
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    actor_task.abort();
    drain_rounds(&mut futs, &mut resolved, false).await;
    assert!(
        resolved.iter().all(|r| *r),
        "все запросы завершаются direct send после смерти actor-а"
    );
    let mut results = results.lock().unwrap().clone();
    results.sort();
    // The requests were never cloned, so the direct-send fallback uses
    // the owned execution contract: Owned -> `IntoFuture` (id 3), not
    // `Request::send` (id 1).
    assert_eq!(
        results,
        vec![(0, 3), (1, 3), (2, 3), (3, 3)],
        "все direct-send через IntoFuture (Owned), сообщение id=3"
    );
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn cancelled_set_limits_cannot_stale_limits() {
    // The client future is cancelled AFTER the actor committed the update
    // but BEFORE it read the response: `limits()` must still report the
    // new limits, because there is no client-side mirror to desync (the
    // actor is the single source of truth).
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let mut actor = Box::pin(actor);

    let mut new = default_limits();
    new.messages_per_sec_chat = 7;
    let mut set_fut = Box::pin(compat.set_limits(new));
    // The first poll sends the command; the future then waits for the
    // actor's response.
    assert!(poll_once(set_fut.as_mut()).is_pending());
    // One actor poll applies the update and resolves the response.
    assert!(poll_once(actor.as_mut()).is_pending());
    // Cancel the caller side before it reads the response.
    drop(set_fut);

    // limits() reads the actor, so the committed value is observed.
    let mut limits_fut = Box::pin(compat.limits());
    assert!(poll_once(limits_fut.as_mut()).is_pending());
    assert!(poll_once(actor.as_mut()).is_pending());
    match poll_once(limits_fut.as_mut()) {
        std::task::Poll::Ready(limits) => assert_eq!(limits, new),
        std::task::Poll::Pending => panic!("limits() не ответил после обработки actor-ом"),
    }
}

// ---------- retry timing, direct-send fallback and QueueFull FIFO ----------

/// Two-phase scenario driver: `first` is submitted at t=0 and fails once
/// with `RetryAfter(fail_after)`, `second` is submitted at t=1 during the
/// freeze, and both must complete in queue order after the freeze.
async fn drive_compat_freeze_then_request<R: Requester>(
    bot: R,
    first: (i64, &str),
    second: (i64, &str),
    fail_after: Duration,
) -> Vec<usize>
where
    R::SendMessage: 'static,
{
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    {
        let order = Arc::clone(&order);
        let request = bot.send_message(ChatId(first.0), first.1);
        futs.push(Box::pin(async move {
            let _ = request.await;
            order.lock().unwrap().push(0);
        }));
    }
    let mut resolved = vec![false; 1];
    let _ = poll_once(futs[0].as_mut());
    drain_rounds(&mut futs, &mut resolved, false).await;

    tokio::time::advance(Duration::from_secs(1)).await;
    let order_for_request = Arc::clone(&order);
    let request = bot.send_message(ChatId(second.0), second.1);
    futs.push(Box::pin(async move {
        let _ = request.await;
        order_for_request.lock().unwrap().push(1);
    }));
    let mut resolved = vec![false; 2];
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    drain_rounds(&mut futs, &mut resolved, false).await;

    tokio::time::advance(fail_after).await;
    drain_rounds(&mut futs, &mut resolved, false).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    drain_rounds(&mut futs, &mut resolved, false).await;
    assert!(resolved.iter().all(|resolved| *resolved), "оба запроса завершились после freeze-а");
    let order = order.lock().unwrap().clone();
    order
}

#[tokio::test(start_paused = true)]
async fn compat_request_during_freeze_precedes_the_retried_request() {
    // A request arriving during a RetryAfter freeze is queued ahead of the
    // sleeping retry; the retry is re-enqueued only after the freeze ends.
    let bot = FakeBot::ok().failing_first(1, Duration::from_secs(10));
    let (compat, actor) = ThrottleCompat::new(bot, default_limits());
    let actor_task = tokio::spawn(actor);
    let order =
        drive_compat_freeze_then_request(compat, (1, "a"), (2, "b"), Duration::from_secs(10)).await;
    actor_task.abort();
    assert_eq!(order, vec![1, 0], "запрос во время freeze проходит раньше retry");
}

#[tokio::test(start_paused = true)]
async fn actor_death_releases_slots_before_direct_sends_run() {
    // The direct-send fallback must release the capacity slot BEFORE
    // running the direct request: with a backlog bound of 2 and four
    // requests, all four must enter their (hanging) direct send after
    // the actor dies — holding the slot through the direct send would
    // leave the parked waiters blocked behind the two hanging sends.
    let mut limits = default_limits();
    limits.messages_per_sec_overall = 2; // backlog bound = 2
    let bot = FakeBot::hanging();
    let entered = bot.entered.clone();
    let (compat, actor) = ThrottleCompat::new(bot, limits);
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    for _ in 0..4 {
        let send = compat.send_message(ChatId(1), "m");
        futs.push(Box::pin(async move {
            let _ = send.await;
        }));
    }
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    actor_task.abort();
    // Drive the Closed cascade: enough rounds for every waiter to wake,
    // acquire and enter its direct send.
    for _ in 0..8 {
        tokio::task::yield_now().await;
        for fut in futs.iter_mut() {
            let _ = poll_once(fut.as_mut());
        }
    }
    assert_eq!(
        entered.load(Ordering::SeqCst),
        0b1111,
        "все четыре waiter-а входят в direct send после смерти actor-а"
    );
}

#[tokio::test(start_paused = true)]
async fn queue_full_after_cancel_lag_preserves_waiter_fifo() {
    // The scheduler can answer `QueueFull` to the first woken waiter
    // when a cancelled job has not been drained yet (the cancel and the
    // waiter's enqueue race inside the actor's `select!`). The waiter
    // must KEEP its slot in that case: dropping it would hand the slot
    // to the next waiter and invert the FIFO order. The race is a coin
    // flip inside the actor, so the scenario is replayed: with the bug
    // about one third of the runs would complete the tail waiter first
    // (the select picks the enqueue over the cancel roughly 1/3 of the
    // time), so 64 replays leave a failure probability of ~2^-38.
    for _ in 0..64 {
        let mut limits = default_limits();
        limits.messages_per_sec_chat = 30;
        limits.messages_per_sec_overall = 2; // backlog bound = 2
        let bot = FakeBot::ok().failing_first(1, Duration::from_secs(10));
        let (compat, actor) = ThrottleCompat::new(bot, limits);
        let actor_task = tokio::spawn(actor);

        // Phase 1: R0 fails once (RetryAfter(10)), freezing the queue.
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
        {
            let order = Arc::clone(&order);
            let send = compat.send_message(ChatId(1), "freeze");
            futs.push(Box::pin(async move {
                let _ = send.await;
                order.lock().unwrap().push(0);
            }));
        }
        let mut resolved = [false; 5];
        let _ = poll_once(futs[0].as_mut());
        drain_rounds(&mut futs, &mut resolved, false).await;

        // Phase 2: during the freeze, R1/R2 stay pending (backlog full);
        // R3/R4 park on the semaphore.
        for i in 1..5 {
            let order = Arc::clone(&order);
            let send = compat.send_message(ChatId(i as i64 + 1), "m");
            futs.push(Box::pin(async move {
                let _ = send.await;
                order.lock().unwrap().push(i);
            }));
        }
        for fut in futs.iter_mut() {
            let _ = poll_once(fut.as_mut());
        }
        drain_rounds(&mut futs, &mut resolved, false).await;

        // Cancel R1 (pending): the slot frees and R3 (head waiter) is
        // admitted. Poll R3 BEFORE the actor runs: its enqueue and R1's
        // cancel are now both in the actor's channels, so the scheduler
        // may answer QueueFull to R3 (the cancel-lag race).
        futs[1] = Box::pin(std::future::pending::<()>());
        let _ = poll_once(futs[3].as_mut());
        tokio::task::yield_now().await;

        // Drain the rest, including the freeze.
        drain_rounds(&mut futs, &mut resolved, false).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        drain_rounds(&mut futs, &mut resolved, false).await;
        tokio::time::advance(Duration::from_secs(2)).await;
        drain_rounds(&mut futs, &mut resolved, false).await;

        let order = order.lock().unwrap().clone();
        let head = order.iter().position(|&i| i == 3).expect("B завершился");
        let tail = order.iter().position(|&i| i == 4).expect("C завершился");
        assert!(head < tail, "waiter FIFO нарушен после QueueFull: {order:?}");
        actor_task.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn compat_on_queue_full_fires_when_the_backlog_reaches_capacity() {
    // The compatibility callback fires when the backlog REACHES the capacity (the
    // N-th pending request), not when the N+1-th is rejected. Two
    // requests to a 1/s chat fill the backlog exactly (the chat limit
    // keeps the second one pending); no overflow request is submitted.
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 1;
    limits.messages_per_min_chat = 100;
    limits.messages_per_sec_overall = 2; // backlog bound = 2
    let full: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let settings = Settings {
        limits,
        on_queue_full: {
            let full = Arc::clone(&full);
            Box::new(move |pending| {
                full.lock().unwrap().push(pending);
                Box::pin(async move {})
            })
        },
        retry: true,
        check_slow_mode: false,
    };
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    for _ in 0..2 {
        let send = compat.send_message(ChatId(1), "m");
        futs.push(Box::pin(async move {
            let _ = send.await;
        }));
    }
    let mut resolved = [false; 2];
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    drive_rounds(&mut futs, &mut resolved, 4).await;
    // Both grants (t=0 and t=1) and the notification happened.
    tokio::time::advance(Duration::from_secs(2)).await;
    drive_rounds(&mut futs, &mut resolved, 4).await;
    assert!(resolved.iter().all(|r| *r), "оба запроса завершились");
    let full = full.lock().unwrap().clone();
    assert_eq!(full, vec![2], "callback срабатывает при заполнении backlog-а (N-й запрос)");
    actor_task.abort();
}

#[tokio::test]
#[should_panic(expected = "worker died before last `Throttle` instance")]
async fn limits_panics_when_the_scheduler_is_dead() {
    // The compatibility layer must not silently hand out defaults when
    // its scheduler actor has died.
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let actor_task = tokio::spawn(actor);
    actor_task.abort();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let _ = compat.limits().await;
}

// ---------- penalty anchor, single classification, saturation monitor
// ----------

#[tokio::test(start_paused = true)]
async fn compat_late_processed_completion_does_not_extend_the_freeze() {
    // A RetryAfter completion carries the observation timestamp. Processing
    // it five seconds later must not extend a three-second freeze from the
    // actor's processing time.
    let fail_after = Duration::from_secs(3);
    let start = tokio::time::Instant::now();
    let bot = FakeBot::ok().failing_first(1, fail_after);
    let (compat, actor) = ThrottleCompat::new(bot, default_limits());
    let mut actor = Box::pin(actor);
    let done = Arc::new(Mutex::new(None));
    let request = compat.send_message(ChatId(1), "a");
    let done2 = Arc::clone(&done);
    let mut future = Box::pin(async move {
        let _ = request.await;
        *done2.lock().unwrap() = Some(tokio::time::Instant::now() - start);
    });

    let _ = poll_once(future.as_mut());
    let _ = poll_once(actor.as_mut());
    let _ = poll_once(future.as_mut());
    tokio::time::advance(Duration::from_secs(5)).await;
    for _ in 0..16 {
        if done.lock().unwrap().is_some() {
            break;
        }
        let _ = poll_once(future.as_mut());
        let _ = poll_once(actor.as_mut());
        tokio::task::yield_now().await;
    }

    let done = done.lock().unwrap().unwrap();
    assert!(done <= Duration::from_secs(5) + Duration::from_millis(1), "freeze продлён: {done:?}");
}

#[tokio::test(start_paused = true)]
async fn stateful_retry_after_is_classified_once() {
    // The first `retry_after()` call of a StatefulError reports
    // RetryAfter(10), later calls report nothing. The compat must
    // classify the outcome EXACTLY ONCE: a second classification would
    // register `Failed` instead of the freeze and skip the retry. The
    // retried request must succeed AND the global freeze must delay a
    // later request until t=10.
    let (compat, actor) = ThrottleCompat::new(StatefulBot, default_limits());
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    for i in 0..2 {
        let order = Arc::clone(&order);
        let send = compat.send_message(ChatId(i as i64 + 1), "m");
        futs.push(Box::pin(async move {
            let result = send.await;
            order.lock().unwrap().push((i, result.map(|m| m.id.0)));
        }));
    }
    let mut resolved = [false; 2];
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    drain_rounds(&mut futs, &mut resolved, false).await;
    // R0 failed (stateful) and is retrying after the freeze; R1 is
    // blocked by the global penalty until t=10.
    assert_eq!(resolved, [false, false]);
    tokio::time::advance(Duration::from_secs(10)).await;
    drive_rounds(&mut futs, &mut resolved, 8).await;
    assert!(resolved.iter().all(|r| *r), "оба запроса завершились");
    let order = order.lock().unwrap().clone();
    let order: Vec<(usize, i32)> = order
        .into_iter()
        .map(|(i, result)| (i, result.expect("retry должен завершиться успехом")))
        .collect();
    assert_eq!(order, vec![(0, 2), (1, 2)], "retry сработал, ошибка классифицирована один раз");
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_queue_full_is_silent_during_a_freeze() {
    // A full-backlog notification is deferred while a global RetryAfter
    // freeze is active and emitted only after the thaw.
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 2;
    let start = tokio::time::Instant::now();
    let fires: Arc<Mutex<Vec<(Duration, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let settings = Settings {
        limits,
        on_queue_full: {
            let fires = Arc::clone(&fires);
            Box::new(move |pending| {
                fires.lock().unwrap().push((tokio::time::Instant::now() - start, pending));
                Box::pin(async move {})
            })
        },
        retry: true,
        check_slow_mode: false,
    };
    let (compat, actor) = ThrottleCompat::with_settings(
        FakeBot::ok().failing_first(1, Duration::from_secs(15)),
        settings,
    );
    let actor_task = tokio::spawn(actor);
    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let freezer = compat.send_message(ChatId(1), "freeze");
    futs.push(Box::pin(async move {
        let _ = freezer.await;
    }));
    let mut resolved = vec![false; 3];
    let _ = poll_once(futs[0].as_mut());
    drain_rounds(&mut futs, &mut resolved, false).await;
    for i in 1..3 {
        let request = compat.send_message(ChatId(i as i64 + 1), "m");
        futs.push(Box::pin(async move {
            let _ = request.await;
        }));
    }
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    drain_rounds(&mut futs, &mut resolved, false).await;

    tokio::time::advance(Duration::from_secs(14)).await;
    drive_rounds(&mut futs, &mut resolved, 4).await;
    assert!(fires.lock().unwrap().is_empty(), "callback во время freeze не вызывается");

    tokio::time::advance(Duration::from_secs(2)).await;
    drive_rounds(&mut futs, &mut resolved, 4).await;
    let fires = fires.lock().unwrap().clone();
    assert!(!fires.is_empty(), "callback после thaw не вызван");
    assert_eq!(fires[0].1, 2, "pending = capacity: {fires:?}");
    assert!(fires[0].0 >= Duration::from_secs(15), "callback до thaw: {fires:?}");
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_queue_full_repeats_while_the_backlog_stays_full() {
    // The saturation monitor re-fires while the backlog remains full. Chat
    // limit 1/s paces grants; fourteen requests keep capacity two occupied
    // long enough to observe several notifications.
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 1;
    limits.messages_per_min_chat = 100;
    limits.messages_per_sec_overall = 2;
    let sends: Vec<(i64, &str)> = (0..14).map(|_| (1, "m")).collect();
    let start = tokio::time::Instant::now();
    let fires: Arc<Mutex<Vec<(Duration, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let settings = Settings {
        limits,
        on_queue_full: {
            let fires = Arc::clone(&fires);
            Box::new(move |pending| {
                fires.lock().unwrap().push((tokio::time::Instant::now() - start, pending));
                Box::pin(async move {})
            })
        },
        retry: true,
        check_slow_mode: false,
    };
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let actor_task = tokio::spawn(actor);
    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    for &(chat, text) in &sends {
        let request = compat.send_message(ChatId(chat), text);
        futs.push(Box::pin(async move {
            let _ = request.await;
        }));
    }
    let mut resolved = vec![false; futs.len()];
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    drain_rounds(&mut futs, &mut resolved, false).await;
    let mut ticks = 0;
    while resolved.iter().any(|resolved| !*resolved) && ticks < 200 {
        tokio::time::advance(TICK).await;
        drain_rounds(&mut futs, &mut resolved, false).await;
        ticks += 1;
    }
    assert!(resolved.iter().all(|resolved| *resolved), "все запросы завершились");
    actor_task.abort();

    let fires = fires.lock().unwrap().clone();
    assert!(fires.len() >= 3, "callback повторяется: {fires:?}");
    assert!(fires.iter().all(|(_, pending)| *pending == 2), "pending = capacity: {fires:?}");
    for pair in fires.windows(2) {
        assert!(
            pair[1].0 - pair[0].0 >= Duration::from_secs(4),
            "интервалы между callback-ами >= 4s: {fires:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn compat_implements_debug() {
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let debug = format!("{compat:?}");
    std::mem::drop(actor);
    assert!(debug.contains("ThrottleCompat"), "{debug}");
}

// ---------- shared/owned semantics, actor liveness, monitor respawn ----------

#[tokio::test(start_paused = true)]
async fn compat_cloned_no_retry_owned_send_uses_inner_send_ref() {
    // Cloning the wrapper shares an owned send, so retry=false still uses
    // the inner send_ref() path rather than cloning or consuming the request.
    let settings = Settings { limits: default_limits(), ..<_>::default() }.no_retry();
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let actor_task = tokio::spawn(actor);
    let request = compat.send_message(ChatId(1), "x");
    let clone = request.clone();
    let output =
        tokio::time::timeout(Duration::from_secs(1), request.send()).await.unwrap().unwrap();
    drop(clone);
    assert_eq!(output.id.0, 2, "cloned + owned + retry=false -> inner send_ref");
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn compat_outer_send_ref_does_not_clone_the_inner_request() {
    // send_ref() clones only the Arc wrapper; a side-effecting inner
    // Request::clone() must not be invoked.
    let bot = FakeBot::ok();
    let clones = bot.clones.clone();
    let settings = Settings { limits: default_limits(), ..<_>::default() }.no_retry();
    let (compat, actor) = ThrottleCompat::with_settings(bot, settings);
    let actor_task = tokio::spawn(actor);
    let before = clones.load(Ordering::SeqCst);
    let request = compat.send_message(ChatId(1), "x");
    let _ =
        tokio::time::timeout(Duration::from_secs(1), request.send_ref()).await.unwrap().unwrap();
    assert_eq!(clones.load(Ordering::SeqCst), before, "send_ref не клонирует inner request");
    actor_task.abort();
}

#[tokio::test(start_paused = true)]
async fn actor_death_does_not_fire_on_queue_full() {
    // When the actor is dead there is no backlog to fill: every request
    // goes straight to the direct send, so `on_queue_full` must not
    // fire (the legacy callback lives inside the worker, which is
    // already gone).
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 30;
    limits.messages_per_sec_overall = 2;
    let full: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let settings = Settings {
        limits,
        on_queue_full: {
            let full = Arc::clone(&full);
            Box::new(move |pending| {
                full.lock().unwrap().push(pending);
                Box::pin(async move {})
            })
        },
        retry: true,
        check_slow_mode: false,
    };
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    for _ in 0..2 {
        let send = compat.send_message(ChatId(1), "m");
        futs.push(Box::pin(async move {
            let _ = send.await;
        }));
    }
    let mut resolved = [false; 2];
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    actor_task.abort();
    drive_rounds(&mut futs, &mut resolved, 8).await;
    assert!(resolved.iter().all(|r| *r), "все запросы ушли в direct send");
    assert!(full.lock().unwrap().is_empty(), "on_queue_full не вызывается при мёртвом actor");
}

#[tokio::test(start_paused = true)]
async fn saturation_monitor_respawns_after_re_saturation() {
    // After the monitor notices a free slot and exits, a NEW saturation
    // episode must spawn a fresh monitor: repeated callbacks must not
    // disappear (the monitor-active flag is cleared before the permit is
    // released, so a waiter that takes the freed last slot sees `false`
    // and restarts the monitor).
    let mut limits = default_limits();
    limits.messages_per_sec_chat = 1;
    limits.messages_per_min_chat = 100;
    limits.messages_per_sec_overall = 2;
    let full: Arc<Mutex<Vec<(Duration, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let start = tokio::time::Instant::now();
    let settings = Settings {
        limits,
        on_queue_full: {
            let full = Arc::clone(&full);
            Box::new(move |pending| {
                full.lock().unwrap().push((tokio::time::Instant::now() - start, pending));
                Box::pin(async move {})
            })
        },
        retry: true,
        check_slow_mode: false,
    };
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let actor_task = tokio::spawn(actor);

    let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let mut resolved = Vec::new();
    for _ in 0..2 {
        let send = compat.send_message(ChatId(1), "m");
        futs.push(Box::pin(async move {
            let _ = send.await;
        }));
        resolved.push(false);
    }
    for fut in futs.iter_mut() {
        let _ = poll_once(fut.as_mut());
    }
    drive_rounds(&mut futs, &mut resolved, 4).await;
    // The backlog drains (1/s chat), the monitor exits at ~t=4.
    let mut ticks = 0;
    while ticks < 24 {
        tokio::time::advance(TICK).await;
        drive_rounds(&mut futs, &mut resolved, 2).await;
        ticks += 1;
    }
    assert!(resolved.iter().all(|r| *r), "первая волна завершилась");
    let after_first_wave = full.lock().unwrap().len();
    assert!(after_first_wave >= 1, "первая волна: callback был");

    // Second wave: re-saturate the backlog — the callback must fire
    // again (the monitor respawned).
    let second_wave_start = futs.len();
    for _ in 0..2 {
        let send = compat.send_message(ChatId(1), "m");
        futs.push(Box::pin(async move {
            let _ = send.await;
        }));
        resolved.push(false);
    }
    for fut in futs.iter_mut().skip(second_wave_start) {
        let _ = poll_once(fut.as_mut());
    }
    let mut ticks = 0;
    while ticks < 24 {
        tokio::time::advance(TICK).await;
        drive_rounds(&mut futs, &mut resolved, 2).await;
        ticks += 1;
    }
    assert!(resolved.iter().all(|r| *r), "вторая волна завершилась");
    let full = full.lock().unwrap().clone();
    assert!(
        full.len() > after_first_wave,
        "повторное заполнение снова вызывает callback: {full:?}"
    );
    actor_task.abort();
}

// ---------- Commit 5 revision: admission-phase on_queue_full, IntoFuture,
// non-blocking completion ----------

#[tokio::test(start_paused = true)]
async fn on_queue_full_fires_before_the_last_pending_job_is_granted() {
    // The previous worker contract fires `on_queue_full` when its queue REACHES the
    // capacity — BEFORE the rate limits are applied and BEFORE any grant.
    // The compat layer must fire it on the ENQUEUE ACCEPTANCE of the last
    // slot: a last pending request that is cancelled before its grant must
    // not erase the full-backlog event that already happened.
    let mut limits = default_limits();
    limits.messages_per_sec_overall = 2; // capacity = 2
    let fires: Arc<Mutex<Vec<(Duration, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let start = tokio::time::Instant::now();
    let settings = Settings {
        limits,
        on_queue_full: {
            let fires = Arc::clone(&fires);
            Box::new(move |pending| {
                fires.lock().unwrap().push((tokio::time::Instant::now() - start, pending));
                Box::pin(async move {})
            })
        },
        retry: true,
        check_slow_mode: false,
    };
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let mut actor = Box::pin(actor);

    let send_a = compat.send_message(ChatId(1), "a");
    let send_b = compat.send_message(ChatId(1), "b");
    let mut fut_a = Box::pin(async move {
        let _ = send_a.await;
    });
    let mut fut_b = Box::pin(async move {
        let _ = send_b.await;
    });

    // Both requests take their slots and enqueue BEFORE the actor grants
    // anything — the callback happens at acceptance, before the first unlock. B
    // takes the LAST slot.
    let _ = poll_once(fut_a.as_mut());
    let _ = poll_once(fut_b.as_mut());
    // The actor accepts both jobs: A is granted at t=0 (chat limit 1/s),
    // B's grant waits for t=1.
    let _ = poll_once(actor.as_mut());
    let _ = poll_once(fut_a.as_mut());
    // B's ACCEPTANCE already fired the callback — before B's grant. In
    // the previous design (firing after the grant) it would not fire at
    // all: B is cancelled below before t=1.
    let _ = poll_once(fut_b.as_mut());
    // The callback runs in its own spawned task: let it execute.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let fires_before_cancel = fires.lock().unwrap().clone();
    assert!(
        !fires_before_cancel.is_empty(),
        "callback должен сработать на acceptance последнего слота, до его grant"
    );
    assert_eq!(fires_before_cancel[0].1, 2, "pending = capacity в момент заполнения");
    assert!(
        fires_before_cancel[0].0 < Duration::from_secs(1),
        "callback в t=0, а не в момент grant последнего запроса: {:?}",
        fires_before_cancel[0].0
    );

    // Cancel B before its grant: the full-backlog event already happened.
    // (A resolved back at t=0; polling it again would panic.)
    drop(fut_b);
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    let fires_after = fires.lock().unwrap().clone();
    assert_eq!(
        fires_after.len(),
        fires_before_cancel.len(),
        "отмена последнего pending request не отменяет уже произошедший full-event: \
         {fires_after:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn compat_owned_direct_fallback_uses_into_future_not_send() {
    // Direct-send fallback executes a truly owned request through
    // IntoFuture in both fallback phases.
    let settings = Settings { limits: default_limits(), ..<_>::default() }.no_retry();
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    std::mem::drop(actor);
    let output = tokio::time::timeout(Duration::from_secs(1), compat.send_message(ChatId(1), "x"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.id.0, 3, "direct fallback (enqueue phase): owned -> IntoFuture");
    std::mem::drop(compat);

    // The actor accepts R1 but dies before its grant. Its grant is blocked
    // by the per-chat window, so this exercises the grant-phase fallback.
    let settings = Settings { limits: default_limits(), ..<_>::default() }.no_retry();
    let (compat, actor) = ThrottleCompat::with_settings(FakeBot::ok(), settings);
    let mut actor = Box::pin(actor);
    let send0 = compat.send_message(ChatId(1), "r0");
    let mut fut0 = Box::pin(async move {
        let _ = send0.await;
    });
    let _ = poll_once(fut0.as_mut());
    let _ = poll_once(actor.as_mut());
    let _ = poll_once(fut0.as_mut());

    let send = compat.send_message(ChatId(1), "x");
    let mut fut = Box::pin(async move { send.await.unwrap() });
    let _ = poll_once(fut.as_mut());
    let _ = poll_once(actor.as_mut());
    let snapshot_fut = compat.queue.handle().snapshot();
    tokio::pin!(snapshot_fut);
    let _ = poll_once(snapshot_fut.as_mut());
    let mut snapshot = None;
    for _ in 0..8 {
        let _ = poll_once(actor.as_mut());
        if let Poll::Ready(Some(value)) = poll_once(snapshot_fut.as_mut()) {
            snapshot = Some(value);
            break;
        }
    }
    let snapshot = snapshot.expect("snapshot должен ответить");
    assert_eq!(snapshot.pending, 1, "R1 accepted and waits for grant: {snapshot:?}");
    assert_eq!(snapshot.in_flight, 0, "R0 already completed: {snapshot:?}");
    std::mem::drop(actor);

    let mut output = None;
    for _ in 0..16 {
        if let Poll::Ready(out) = poll_once(fut.as_mut()) {
            output = Some(out);
            break;
        }
        tokio::task::yield_now().await;
    }
    let output = output.expect("grant-phase fallback должен завершиться");
    assert_eq!(output.id.0, 3, "direct fallback (grant phase): owned -> IntoFuture");
}

#[tokio::test(start_paused = true)]
async fn granted_request_completes_without_an_additional_actor_poll() {
    // A granted request returns its result right after the inner request
    // finishes; RetryAfter notification is asynchronous and must not add
    // another actor round-trip to ordinary completion.
    // The compat layer must NOT stall a granted request on an actor ack
    // round-trip: success and plain failure resolve without polling the
    // actor after the grant.

    // Success.
    let (compat, actor) = ThrottleCompat::new(FakeBot::ok(), default_limits());
    let mut actor = Box::pin(actor);
    let send = compat.send_message(ChatId(1), "m");
    let mut fut = Box::pin(async move { send.await });
    let _ = poll_once(fut.as_mut()); // semaphore slot + enqueue phase
    let _ = poll_once(actor.as_mut()); // acceptance + grant
    let mut outcome = None;
    for _ in 0..16 {
        if let Poll::Ready(out) = poll_once(fut.as_mut()) {
            outcome = Some(out);
            break;
        }
        tokio::task::yield_now().await;
    }
    let output =
        outcome.expect("success должен завершиться без дополнительного poll actor-а").unwrap();
    assert_eq!(output.id.0, 2, "retry=true -> inner send_ref");
    std::mem::drop(actor);

    // Non-retried failure: the result resolves the same way (the freeze
    // deadline is recorded locally, the completion is fire-and-forget).
    let settings = Settings { limits: default_limits(), ..<_>::default() }.no_retry();
    let (compat, actor) = ThrottleCompat::with_settings(
        FakeBot::ok().failing_first(1, Duration::from_secs(3)),
        settings,
    );
    let mut actor = Box::pin(actor);
    let send = compat.send_message(ChatId(1), "m");
    let mut fut = Box::pin(async move { send.await });
    let _ = poll_once(fut.as_mut());
    let _ = poll_once(actor.as_mut());
    let mut outcome = None;
    for _ in 0..16 {
        if let Poll::Ready(out) = poll_once(fut.as_mut()) {
            outcome = Some(out);
            break;
        }
        tokio::task::yield_now().await;
    }
    let error =
        outcome.expect("failure должен завершиться без дополнительного poll actor-а").unwrap_err();
    assert!(matches!(error, RequestError::RetryAfter(_)), "{error:?}");
    std::mem::drop(actor);
}

#[tokio::test(start_paused = true)]
async fn compat_full_backlog_cancelled_during_freeze_still_reports_after_thaw() {
    // A RetryAfter freeze starts; two requests fill the backlog during the
    // freeze and are cancelled before thaw. The deferred full-backlog event
    // still fires at the thaw boundary because acceptance already happened.
    let freeze = Duration::from_secs(15);
    let mut limits = default_limits();
    limits.messages_per_sec_overall = 2;
    let start = tokio::time::Instant::now();
    let fires: Arc<Mutex<Vec<(Duration, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let settings = Settings {
        limits,
        on_queue_full: {
            let fires = Arc::clone(&fires);
            Box::new(move |pending| {
                fires.lock().unwrap().push((tokio::time::Instant::now() - start, pending));
                Box::pin(async move {})
            })
        },
        retry: true,
        check_slow_mode: false,
    };
    let (compat, actor) =
        ThrottleCompat::with_settings(FakeBot::ok().failing_first(1, freeze), settings);
    let mut actor = Box::pin(actor);

    let send_a = compat.send_message(ChatId(1), "a");
    let mut fut_a = Box::pin(async move {
        let _ = send_a.await;
    });
    let _ = poll_once(fut_a.as_mut());
    let _ = poll_once(actor.as_mut());
    let _ = poll_once(fut_a.as_mut());

    let send_b = compat.send_message(ChatId(1), "b");
    let send_c = compat.send_message(ChatId(1), "c");
    let mut fut_b = Box::pin(async move {
        let _ = send_b.await;
    });
    let mut fut_c = Box::pin(async move {
        let _ = send_c.await;
    });
    let _ = poll_once(fut_b.as_mut());
    let _ = poll_once(fut_c.as_mut());
    let _ = poll_once(actor.as_mut());
    let _ = poll_once(fut_b.as_mut());
    let _ = poll_once(fut_c.as_mut());
    drop(fut_b);
    drop(fut_c);

    tokio::time::advance(Duration::from_secs(14)).await;
    let _ = poll_once(actor.as_mut());
    assert!(fires.lock().unwrap().is_empty(), "callback во время freeze не вызывается");

    tokio::time::advance(Duration::from_secs(2)).await;
    let mut a_done = false;
    for _ in 0..128 {
        let _ = poll_once(actor.as_mut());
        if !a_done {
            a_done = poll_once(fut_a.as_mut()).is_ready();
        }
        tokio::task::yield_now().await;
        if !fires.lock().unwrap().is_empty() && a_done {
            break;
        }
    }
    let fires = fires.lock().unwrap().clone();
    assert!(!fires.is_empty(), "callback после thaw не вызван");
    assert_eq!(fires[0].1, 2, "pending = capacity: {fires:?}");
    assert!(fires[0].0 >= freeze, "callback только после thaw: {fires:?}");
}
