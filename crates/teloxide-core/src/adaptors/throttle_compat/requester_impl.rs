use url::Url;

use std::sync::Arc;

use crate::{
    adaptors::throttle_compat::{CompatRequest, ThrottleCompat},
    errors::AsResponseParameters,
    requests::{HasPayload, Payload, Requester},
    types::*,
};

macro_rules! f {
    ($m:ident $this:ident ($($arg:ident : $T:ty),*)) => {
        CompatRequest {
            request: Arc::new($this.bot.$m($($arg),*)),
            queue: $this.queue.clone(),
            state: $this.state.clone(),
        }
    };
}

macro_rules! fty {
    ($T:ident) => {
        CompatRequest<B::$T>
    };
}

macro_rules! fid {
    ($m:ident $this:ident ($($arg:ident : $T:ty),*)) => {
        $this.bot.$m($($arg),*)
    };
}

macro_rules! ftyid {
    ($T:ident) => {
        B::$T
    };
}

impl<B: Requester> Requester for ThrottleCompat<B>
where
    B::Err: AsResponseParameters,

    B::SendMessage: Clone + Send + Sync + 'static,
    <B::SendMessage as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendRichMessage: Clone + Send + Sync + 'static,
    <B::SendRichMessage as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::ForwardMessage: Clone + Send + Sync + 'static,
    <B::ForwardMessage as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::ForwardMessages: Clone + Send + Sync + 'static,
    <B::ForwardMessages as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::CopyMessage: Clone + Send + Sync + 'static,
    <B::CopyMessage as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::CopyMessages: Clone + Send + Sync + 'static,
    <B::CopyMessages as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendPhoto: Clone + Send + Sync + 'static,
    <B::SendPhoto as HasPayload>::Payload: Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendLivePhoto: Clone + Send + Sync + 'static,
    <B::SendLivePhoto as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendAudio: Clone + Send + Sync + 'static,
    <B::SendAudio as HasPayload>::Payload: Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendDocument: Clone + Send + Sync + 'static,
    <B::SendDocument as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendVideo: Clone + Send + Sync + 'static,
    <B::SendVideo as HasPayload>::Payload: Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendAnimation: Clone + Send + Sync + 'static,
    <B::SendAnimation as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendVoice: Clone + Send + Sync + 'static,
    <B::SendVoice as HasPayload>::Payload: Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendVideoNote: Clone + Send + Sync + 'static,
    <B::SendVideoNote as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendPaidMedia: Clone + Send + Sync + 'static,
    <B::SendPaidMedia as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendMediaGroup: Clone + Send + Sync + 'static,
    <B::SendMediaGroup as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendLocation: Clone + Send + Sync + 'static,
    <B::SendLocation as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendVenue: Clone + Send + Sync + 'static,
    <B::SendVenue as HasPayload>::Payload: Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendContact: Clone + Send + Sync + 'static,
    <B::SendContact as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendPoll: Clone + Send + Sync + 'static,
    <B::SendPoll as HasPayload>::Payload: Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendChecklist: Clone + Send + Sync + 'static,
    <B::SendChecklist as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendDice: Clone + Send + Sync + 'static,
    <B::SendDice as HasPayload>::Payload: Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendSticker: Clone + Send + Sync + 'static,
    <B::SendSticker as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendInvoice: Clone + Send + Sync + 'static,
    <B::SendInvoice as HasPayload>::Payload:
        Payload<Output: Send> + crate::outbound::OutboundPayload,
    B::SendGame: Clone + Send + Sync + 'static,
    <B::SendGame as HasPayload>::Payload: Payload<Output: Send> + crate::outbound::OutboundPayload,
{
    type Err = B::Err;

    requester_forward! {
        send_message,
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
        send_game
        => f, fty
    }

    requester_forward! {
        get_me,
        log_out,
        close,
        get_updates,
        set_webhook,
        delete_webhook,
        get_webhook_info,
        edit_message_live_location,
        edit_message_live_location_inline,
        stop_message_live_location,
        stop_message_live_location_inline,
        edit_message_checklist,
        send_chat_action,
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
        => fid, ftyid
    }
}

download_forward! {
    B
    ThrottleCompat<B>
    { this => this.bot }
}
