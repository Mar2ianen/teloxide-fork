// waffle: efficiency is not important here, and I don't want to rewrite this
#![allow(clippy::format_collect)]

use std::{borrow::Borrow, collections::HashSet, ops::Deref};

use itertools::Itertools;

use crate::codegen::{
    add_preamble,
    convert::{convert_for, Convert},
    ensure_files_contents, project_root, reformat,
    schema::{self, Doc, Method, Param, Type},
    to_uppercase,
};

#[test]
fn codegen_payloads() {
    let base_path = project_root().join("src/payloads/");
    let schema = schema::get();

    let mut files = Vec::new();

    for method in schema.methods {
        let file_name = format!("{}.rs", method.names.2);
        let path = base_path.join(&*file_name);

        let uses = uses(&method);

        let method_doc = render_doc(&method.doc, method.sibling.as_deref());
        let eq_hash_derive = if eq_hash_suitable(&method) { " Eq, Hash," } else { "" };
        let default_derive = if default_needed(&method) { " Default," } else { "" };

        let return_ty = method.return_ty.to_string();

        let required = params(method.params.iter().filter(|p| !matches!(&p.ty, Type::Option(_))));
        let required = match &*required {
            "" => "".to_owned(),
            _ => format!("        required {{\n{required}\n        }}"),
        };

        let optional = params(method.params.iter().filter_map(|p| match &p.ty {
            Type::Option(inner) => Some(Param {
                name: p.name.clone(),
                ty: inner.deref().clone(),
                descr: p.descr.clone(),
            }),
            _ => None,
        }));
        let optional = match &*optional {
            "" => "".to_owned(),
            _ if required.is_empty() => format!("        optional {{\n{optional}\n        }}"),
            _ => format!("\n        optional {{\n{optional}\n        }}"),
        };

        let multipart = multipart_input_file_fields(&method)
            .map(|field| format!("    @[multipart = {}]\n", field.join(", ")))
            .unwrap_or_default();

        let validation = method
            .validation
            .as_deref()
            .map(|function| format!("    @[validate = {function}]\n"))
            .unwrap_or_default();

        let derive = if !multipart.is_empty() || !partial_eq_suitable(&method) {
            "#[derive(Debug, Clone, Serialize)]".to_owned()
        } else {
            format!("#[derive(Debug, PartialEq,{eq_hash_derive}{default_derive} Clone, Serialize)]")
        };

        let timeout_secs = match &*method.names.2 {
            "get_updates" => "    @[timeout_secs = timeout]\n",
            _ => "",
        };

        let contents = format!(
            "\
{uses}

impl_payload! {{
{multipart}{validation}{timeout_secs}{method_doc}
    {derive}
    pub {Method} ({Method}Setters) => {return_ty} {{
{required}{optional}
    }}
}}
",
            Method = method.names.1,
        );

        let contents = match method.names.2.as_str() {
            "edit_message_text" | "edit_message_text_inline" => {
                let contents = contents.replace(
                    "            pub text: String [into],",
                    "            #[serde(skip_serializing_if = \"String::is_empty\")]\n            pub text: String [into],",
                );
                let constructor = if method.names.2 == "edit_message_text" {
                    r#"
impl EditMessageText {
    /// Creates a rich-only edit request.
    pub fn rich(
        chat_id: impl Into<Recipient>,
        message_id: MessageId,
        rich_message: InputRichMessage,
    ) -> Self {
        let mut payload = Self::new(chat_id, message_id, String::new());
        payload.rich_message = Some(rich_message);
        payload
    }
}
"#
                } else {
                    r#"
impl EditMessageTextInline {
    /// Creates a rich-only edit request.
    pub fn rich(
        inline_message_id: impl Into<String>,
        rich_message: InputRichMessage,
    ) -> Self {
        let mut payload = Self::new(inline_message_id, String::new());
        payload.rich_message = Some(rich_message);
        payload
    }
}
"#
                };
                format!("{contents}{constructor}")
            }
            _ => contents,
        };

        let policy = method_policy(&method.names.2);
        let outbound_impl = format!(
            "\n\nimpl crate::outbound::OutboundPayload for {Method} {{\n    fn \
             outbound_hint(&self) -> crate::outbound::OutboundHint {{\n        \
             crate::outbound::OutboundHint {{\n            scope: {scope},\n            class: \
             crate::outbound::OutboundClass::new(crate::outbound::class::{class}),\n            \
             priority: crate::outbound::OutboundPriority::{priority},\n            weight: \
             {weight},\n        }}\n    }}\n}}\n",
            Method = method.names.1,
            scope = policy.scope_expr(),
            class = policy.class,
            priority = policy.priority,
            weight = policy.weight_expr(),
        );
        let contents = format!("{contents}{outbound_impl}");

        files.push((path, reformat(add_preamble("codegen_payloads", contents))));
    }

    ensure_files_contents(files.iter().map(|(p, c)| (&**p, &**c)))
}

/// Strict outbound classification table for every Bot API method.
///
/// A method missing from this table fails code generation with a loud
/// error instead of silently falling back to a global/default policy: an
/// unclassified `send*` that bypasses per-chat windows is worse than a
/// broken build.
fn method_policy(name: &str) -> MethodPolicy {
    match name {
        "answer_callback_query" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "answer_chat_join_request_query" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "answer_guest_query" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "answer_inline_query" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "answer_pre_checkout_query" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "answer_shipping_query" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "answer_web_app_query" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "close" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "convert_gift_to_stars" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "create_invoice_link" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_business_messages" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_my_commands" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_sticker_from_set" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_sticker_set" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_story" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_webhook" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_caption_inline" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_live_location_inline" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_media_inline" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_reply_markup_inline" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_text_inline" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_story" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_available_gifts" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_business_account_gifts" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_business_account_star_balance" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_business_connection" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_custom_emoji_stickers" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_file" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_forum_topic_icon_stickers" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_me" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_my_commands" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_my_default_administrator_rights" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_my_description" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_my_name" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_my_short_description" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_my_star_balance" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_star_transactions" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_sticker_set" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_updates" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_webhook_info" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "log_out" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "post_story" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "remove_business_account_profile_photo" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "remove_my_profile_photo" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_chat_join_request_web_app" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_business_account_bio" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_business_account_gift_settings" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_business_account_name" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_business_account_profile_photo" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_business_account_username" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_custom_emoji_sticker_set_thumbnail" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_my_commands" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_my_default_administrator_rights" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_my_description" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_my_name" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_my_profile_photo" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_my_short_description" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_sticker_emoji_list" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_sticker_keywords" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_sticker_mask_position" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_sticker_position_in_set" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_sticker_set_title" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_webhook" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "stop_message_live_location_inline" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "transfer_business_account_stars" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "upgrade_gift" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "approve_chat_join_request" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "ban_chat_member" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "ban_chat_sender_chat" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "close_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "close_general_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "copy_message" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "copy_messages" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::Len("message_ids"),
        },
        "create_chat_invite_link" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "create_chat_subscription_invite_link" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "create_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "decline_chat_join_request" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_all_message_reactions" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_chat_photo" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_chat_sticker_set" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_ephemeral_message" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_message" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_message_reaction" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "delete_messages" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_chat_invite_link" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_chat_subscription_invite_link" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_ephemeral_message_caption" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_ephemeral_message_media" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_ephemeral_message_reply_markup" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_ephemeral_message_text" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_general_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_caption" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_live_location" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_media" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_reply_markup" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_text" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "export_chat_invite_link" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "forward_message" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "forward_messages" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::Len("message_ids"),
        },
        "get_chat" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_chat_administrators" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_chat_gifts" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_chat_member" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_chat_member_count" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_chat_members_count" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_user_chat_boosts" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "hide_general_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "kick_chat_member" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "leave_chat" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "pin_chat_message" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "promote_chat_member" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "remove_chat_verification" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "reopen_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "reopen_general_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "restrict_chat_member" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "revoke_chat_invite_link" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_animation" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_audio" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_chat_action" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "CHAT_ACTION",
            priority: "BACKGROUND",
            weight: WeightPolicy::One,
        },
        "send_contact" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_dice" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_document" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_gift_chat" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_invoice" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_live_photo" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_location" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_media_group" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::Len("media"),
        },
        "send_message" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_paid_media" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_photo" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_poll" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_rich_message" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_sticker" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_venue" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_video" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_video_note" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_voice" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_chat_administrator_custom_title" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_chat_description" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_chat_member_tag" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_chat_permissions" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_chat_photo" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_chat_sticker_set" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_chat_title" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_message_reaction" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "stop_message_live_location" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "stop_poll" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "unban_chat_member" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "unban_chat_sender_chat" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "unhide_general_forum_topic" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "unpin_all_chat_messages" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "unpin_all_forum_topic_messages" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "unpin_all_general_forum_topic_messages" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "unpin_chat_message" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "verify_chat" => MethodPolicy {
            scope: ScopePolicy::RecipientField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "approve_suggested_post" => MethodPolicy {
            scope: ScopePolicy::ChatIdField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "decline_suggested_post" => MethodPolicy {
            scope: ScopePolicy::ChatIdField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_message_checklist" => MethodPolicy {
            scope: ScopePolicy::ChatIdField("chat_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "read_business_message" => MethodPolicy {
            scope: ScopePolicy::ChatIdField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_checklist" => MethodPolicy {
            scope: ScopePolicy::ChatIdField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_game" => MethodPolicy {
            scope: ScopePolicy::ChatIdField("chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_chat_menu_button" => MethodPolicy {
            scope: ScopePolicy::OptionalChatIdField("chat_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_chat_menu_button" => MethodPolicy {
            scope: ScopePolicy::OptionalChatIdField("chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "add_sticker_to_set" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "create_new_sticker_set" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "edit_user_star_subscription" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "MESSAGE_MUTATION",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_game_high_scores" => MethodPolicy {
            scope: ScopePolicy::Custom("target_message_scope", "target"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_managed_bot_access_settings" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_managed_bot_token" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_user_gifts" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_user_personal_chat_messages" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_user_profile_audios" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "get_user_profile_photos" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "READ",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "gift_premium_subscription" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "refund_star_payment" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "remove_user_verification" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "replace_managed_bot_token" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "replace_sticker_in_set" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "save_prepared_inline_message" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "save_prepared_keyboard_button" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_gift" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_game_score" => MethodPolicy {
            scope: ScopePolicy::CustomValue("game_chat_id_scope", "chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_game_score_inline" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_managed_bot_access_settings" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_passport_data_errors" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_sticker_set_thumbnail" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "set_user_emoji_status" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "upload_sticker_file" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "verify_user" => MethodPolicy {
            scope: ScopePolicy::UserIdField("user_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_message_draft" => MethodPolicy {
            scope: ScopePolicy::Custom("draft_chat_id_scope", "chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "send_rich_message_draft" => MethodPolicy {
            scope: ScopePolicy::Custom("draft_chat_id_scope", "chat_id"),
            class: "MESSAGE_SEND",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "transfer_gift" => MethodPolicy {
            scope: ScopePolicy::Custom("new_owner_chat_id_scope", "new_owner_chat_id"),
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        "repost_story" => MethodPolicy {
            scope: ScopePolicy::Global,
            class: "OTHER",
            priority: "NORMAL",
            weight: WeightPolicy::One,
        },
        unknown => panic!(
            "no outbound classification for method `{unknown}`: add it to `method_policy` in \
             payloads/codegen.rs (scope policy, class, priority, weight)"
        ),
    }
}

/// How the scope of a method is derived from its payload.
enum ScopePolicy {
    /// The request applies to no chat; global windows only.
    Global,
    /// A `Recipient` field (numeric id or channel username).
    RecipientField(&'static str),
    /// A numeric `ChatId` field.
    ChatIdField(&'static str),
    /// An optional numeric `ChatId` field: `None` falls back to global.
    OptionalChatIdField(&'static str),
    /// A numeric `UserId` field.
    UserIdField(&'static str),
    /// A hand-written classifier (function name, field name).
    Custom(&'static str, &'static str),
    /// A hand-written classifier for a `Copy` field, passed BY VALUE
    /// (function name, field name).
    CustomValue(&'static str, &'static str),
}

/// Weight of one request in window capacity units.
///
/// `One` is the correct semantic default for non-batch methods; batch
/// methods that send/forward/copy N messages in one call account for N
/// units so that per-window budgets measure actual message traffic, not
/// call count.
#[derive(Clone, Copy)]
enum WeightPolicy {
    /// A single unit regardless of the payload contents.
    One,
    /// The weight is the length of the named `Vec` field (clamped to at
    /// least 1, so an empty batch still consumes one unit).
    Len(&'static str),
}

/// Classification of one method for the generated `OutboundPayload` impl.
struct MethodPolicy {
    scope: ScopePolicy,
    class: &'static str,
    priority: &'static str,
    weight: WeightPolicy,
}

impl MethodPolicy {
    /// The scope expression, in terms of `self`.
    fn scope_expr(&self) -> String {
        match self.scope {
            ScopePolicy::Global => "crate::outbound::OutboundScope::Global".to_owned(),
            ScopePolicy::RecipientField(field) => {
                format!("crate::outbound::classify::scope_of_recipient(&self.{field})")
            }
            ScopePolicy::ChatIdField(field) => {
                format!("crate::outbound::classify::chat_id_scope(self.{field})")
            }
            ScopePolicy::OptionalChatIdField(field) => format!(
                "match self.{field} {{ Some(chat_id) => \
                 crate::outbound::classify::chat_id_scope(chat_id), None => \
                 crate::outbound::OutboundScope::Global }}"
            ),
            ScopePolicy::UserIdField(field) => {
                format!("crate::outbound::classify::user_id_scope(self.{field})")
            }
            ScopePolicy::Custom(function, field) => {
                format!("crate::outbound::classify::{function}(&self.{field})")
            }
            ScopePolicy::CustomValue(function, field) => {
                format!("crate::outbound::classify::{function}(self.{field})")
            }
        }
    }

    /// The weight expression, in terms of `self`.
    fn weight_expr(&self) -> String {
        match self.weight {
            WeightPolicy::One => "std::num::NonZeroU32::new(1).unwrap()".to_owned(),
            WeightPolicy::Len(field) => format!(
                "std::num::NonZeroU32::new(self.{field}.len() as u32) \
                 .unwrap_or(std::num::NonZeroU32::MIN)"
            ),
        }
    }
}

fn uses(method: &Method) -> String {
    enum Use {
        Prelude,
        Crate(String),
        External(String),
    }

    fn ty_use(ty: &Type) -> Use {
        match ty {
            Type::True => Use::Crate(String::from("use crate::types::True;")),
            Type::u8
            | Type::u16
            | Type::u32
            | Type::i32
            | Type::u64
            | Type::i64
            | Type::f64
            | Type::bool
            | Type::String => Use::Prelude,
            Type::Option(inner) | Type::ArrayOf(inner) => ty_use(inner),
            Type::RawTy(raw) => Use::Crate(["use crate::types::", raw, ";"].concat()),
            Type::Url => Use::External(String::from("use url::Url;")),
            Type::DateTime => Use::External(String::from("use chrono::{DateTime, Utc};")),
        }
    }

    let mut crate_uses = HashSet::new();
    let mut external_uses = HashSet::new();

    external_uses.insert(String::from("use serde::Serialize;"));

    core::iter::once(&method.return_ty)
        .chain(method.params.iter().map(|p| &p.ty))
        .map(ty_use)
        .for_each(|u| match u {
            Use::Prelude => {}
            Use::Crate(u) => {
                crate_uses.insert(u);
            }
            Use::External(u) => {
                external_uses.insert(u);
            }
        });

    let external_uses = external_uses.into_iter().join("\n");

    if crate_uses.is_empty() {
        external_uses
    } else {
        let crate_uses = crate_uses.into_iter().join("");

        format!("{external_uses}\n\n{crate_uses}",)
    }
}

fn render_doc(doc: &Doc, sibling: Option<&str>) -> String {
    let links = match &doc.md_links {
        links if links.is_empty() => String::new(),
        links => {
            let l: String =
                links.iter().map(|(name, link)| format!("\n    /// [{name}]: {link}")).collect();

            format!("\n    ///{l}")
        }
    };

    let sibling_note = sibling
        .map(|s| {
            format!(
                "\n    /// \n    /// See also: [`{s}`](crate::payloads::{s})",
                s = to_uppercase(s)
            )
        })
        .unwrap_or_default();

    ["    /// ", &doc.md.replace('\n', "\n    /// "), &sibling_note, &links].concat()
}

fn partial_eq_suitable(method: &Method) -> bool {
    fn ty_partial_eq_suitable(ty: &Type) -> bool {
        match ty {
            Type::Option(inner) | Type::ArrayOf(inner) => ty_partial_eq_suitable(inner),
            Type::RawTy(raw) => !matches!(
                raw.as_str(),
                "InputSticker"
                    | "InputProfilePhoto"
                    | "InputStoryContent"
                    | "InputMedia"
                    | "InputPaidMedia"
                    | "InputPollMedia"
                    | "InputPollOption"
                    | "InputPollOptionMedia"
                    | "InputRichMessage"
            ),
            _ => true,
        }
    }

    method.params.iter().all(|param| ty_partial_eq_suitable(&param.ty))
}

fn eq_hash_suitable(method: &Method) -> bool {
    fn ty_eq_hash_suitable(ty: &Type) -> bool {
        match ty {
            Type::f64 => false,
            Type::Option(inner) | Type::ArrayOf(inner) => ty_eq_hash_suitable(&*inner),

            Type::True
            | Type::u8
            | Type::u16
            | Type::u32
            | Type::i32
            | Type::u64
            | Type::i64
            | Type::bool
            | Type::String => true,

            Type::Url | Type::DateTime => true,

            Type::RawTy(raw) => {
                raw != "InputSticker" && raw != "MaskPosition" && raw != "InlineQueryResult"
            }
        }
    }

    method.params.iter().all(|p| ty_eq_hash_suitable(&p.ty))
}

fn default_needed(method: &Method) -> bool {
    method.params.iter().all(|p| matches!(p.ty, Type::Option(_)))
}

fn params(params: impl Iterator<Item = impl Borrow<Param>>) -> String {
    params
        .map(|param| {
            let param = param.borrow();
            let doc = render_doc(&param.descr, None).replace('\n', "\n        ");
            let field = &param.name;
            let ty = &param.ty;
            let flatten = match ty {
                Type::RawTy(s) if s == "MessageId" && field == "reply_to_message_id" => {
                    "\n            #[serde(serialize_with = \
                     \"crate::types::serialize_reply_to_message_id\")]"
                }
                Type::RawTy(s)
                    if s == "MessageId" || s == "TargetMessage" || s == "StickerType" =>
                {
                    "\n            #[serde(flatten)]"
                }
                Type::ArrayOf(b) if **b == Type::RawTy("MessageId".to_string()) => {
                    "\n            #[serde(with = \"crate::types::vec_msg_id_as_vec_int\")]"
                }
                _ => "",
            };
            let with = match ty {
                Type::DateTime => {
                    "\n            #[serde(with = \
                     \"crate::types::serde_opt_date_from_unix_timestamp\")]"
                }
                _ => "",
            };
            let rename = match field.strip_suffix('_') {
                Some(field) => format!("\n            #[serde(rename = \"{field}\")]"),
                None => "".to_owned(),
            };
            let convert = match convert_for(ty) {
                Convert::Id(_) => "",
                Convert::Into(_) => " [into]",
                Convert::Collect(_) => " [collect]",
            };
            format!("        {doc}{flatten}{with}{rename}\n            pub {field}: {ty}{convert},")
        })
        .join("\n")
}

fn multipart_input_file_fields(m: &Method) -> Option<Vec<&str>> {
    let mut fields: Vec<_> =
        m.params.iter().filter(|&p| ty_is_multiparty(&p.ty)).map(|p| &*p.name).collect();

    fields.extend(m.multipart.iter().map(String::as_str));

    if fields.is_empty() {
        None
    } else {
        Some(fields)
    }
}

fn ty_is_multiparty(ty: &Type) -> bool {
    matches!(ty, Type::RawTy(x) if x == "InputFile" || x == "InputSticker" || x == "InputProfilePhoto")
        || matches!(ty, Type::Option(inner) if ty_is_multiparty(inner))
}
