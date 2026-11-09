# Bot API 10.4 audit — November 9, 2026

This audit covers the **10.3 → 10.4 delta** in the official Telegram Bot API.
It does not re-certify the entire inherited 10.3 implementation. Feature work
is based on `next` and targets `next`; package versions and release tags remain
unchanged until a separate versioned release promotion.

## Reproducible sources

- [`api.html.gz`](api.html.gz): exact official [Bot API](https://core.telegram.org/bots/api) HTML retrieved on November 9, 2026. Its SHA-256 is in [`source.json`](source.json).
- [`changelog.txt`](changelog.txt): the November 9 section of that page. The separate [changelog page](https://core.telegram.org/bots/api-changelog#november-9-2026) still ended at 10.3 when retrieved; the API page already contained the 10.4 changelog.
- [`surface.json`](surface.json): readable tables for the new types/methods and changed verification/ephemeral parameters.
- [`widgets.html.gz`](widgets.html.gz): the official [widget guide](https://core.telegram.org/bots/widgets), with provenance in [`widgets-source.json`](widgets-source.json).

Run `python3 docs/bot-api-10.4/audit.py` from any directory. It reads the compressed
official HTML independently, verifies its hash, and compares names, wire types,
requiredness and unions with `custom_v2.json`. The existing Rust schema tests
then compare that mirror with `schema.ron` and the public Rust types. CI runs
the external audit in the formatting job.

## Coverage checklist

| Official change | Implementation and verification |
|---|---|
| Widget media | `Widget`, `MediaWidget`, message getters/captions/entities, `ExternalReplyInfoKind::Widget`; serde tests |
| Sending widgets | Generated `send_widget`, `send_shared_widget`, all official parameters; HTTP capture tests |
| Query updates | `WidgetQuery`, `SharedWidgetQuery`, three nested `WidgetQueryType` variants and wallet details; normal/inline contexts, malformed update preservation, sender/chat/user visitors |
| Update subscriptions | `AllowedUpdate`, dispatcher filters and handler interests; works with long polling and both webhook listeners through the common `Update` model |
| Answers and edits | Generated answer, normal/inline edit siblings and shared edit; `True` returns, exact endpoint and empty-string versus omitted-data tests |
| Inline content | Both widget input-content types, builders and `InputMessageContent` variants; serde and default parse-mode traversal, including media-result overrides |
| Wallets | Wallet card, NFT, attributes, positioned stickers, transfers and chat field; balances, amounts and NFT indices remain decimal strings |
| Area positions | Canonical `AreaPosition`; `StoryAreaPosition` remains a compatibility alias |
| Ephemeral replies | Optional receiver and chat on all 14 send methods; explicit callback-only constructor; JSON and actual uploaded-file multipart tests |
| Outbound routing | Optional target handling; callback-only replies use global scope even if a now-ignored chat was supplied. New sends, edits and reads retain their scheduler classes; widget answers use interactive priority. Resource-scoped edit penalties, explicit shared bindings, concurrent isolation and deadline-cancellation tests |
| Rich quotes | `TextQuote::rich_texts`, `ReplyParameters::quote_rich_texts`; preserve truthful `Eq`/`Hash` through the rich-text tree |
| Share themes | Emoji/custom-emoji variants, unique-gift variant, backdrops and getter; chat field and serde/schema tests |
| Gifts | Generated `get_unique_gift` and `delete_business_account_gift`; typed results and HTTP tests |
| TON rich text/entities | Typed `RichTextTonAddress`, recursive rich validation; `ton_address` and `bank_card_number` entity variants |
| Verification formatting | Both new parameters on both methods; default parse-mode and explicit override tests |
| Waived paid messages | Empty service type, message kind/getter/filter |
| Semantic changes | Widget copy prohibition, send-other-messages permission wording, Premium secondary username suffixes, private bot topic links documented |
| Adaptors | CacheMe, Trace, DefaultParseMode, ErasedRequester, Outbound, ThrottleCompat, and reference/Box/Arc/Rc forwarding |
| Other call sites | Examples, sugar helpers, drafter backends, docs and tests migrated to optional chat setters |

No new widget method contains `InputFile`; HTML is sent as a UTF-8 string in
JSON. Inline-answer methods retain their existing multipart transport, and
widget contents contribute no file parts. Existing ephemeral-capable media
sends retain multipart transport and attachment traversal.

## Server/client semantics

Widgets currently work in production only in private chats, with protected
content. Group, forwarding and inline scenarios require Telegram's test server.
Widget messages cannot be copied. HTML is immutable after sending; edit methods
change state, while normal caption methods change captions. Send plain HTML:
the server checks the 32768-byte raw / 4096-byte zlib-level-9 limits and packs it.
The library preserves HTML and data without sanitizing or reinterpreting them.

Queries carry 1–64 bytes, answers 0–1024 bytes. Answer within 15 seconds; each user
can send at most 5 queries per 5 seconds across widgets. This incoming limit is
not a global outbound-answer limit. Answer data is private to the querying
widget and does not update stored state. Shared public-data changes are limited
to once per second; `cache_time` is 5–3600 seconds, initially 30. Successful shared-state edits must be paced by the application across all
producers; a serial lane alone does not enforce one-second spacing.

`OutboundQueue` isolates edit `RetryAfter` penalties by widget resource.
`edit_shared_widget_data` uses its exact, case-sensitive `widget_id`. The two
message-addressed edit methods cannot infer that ID from their wire parameters:
use `.shared_widget(widget_id)` when editing shared public data. This scheduling
annotation joins their penalty scope to the shared endpoint without changing
the HTTP body. Keep the binding consistent with the final message target.
Without it, edits use their chat/message or inline-message address, isolating
unrelated traffic but providing no cross-message shared-widget coordination.
Multiple bot instances share penalties when using the same queue and ID;
separate queues/processes require an application-level coordinator. Resource
requests still consume configured global/class windows and obey actual global
penalties. The queue does not retry a failed edit automatically.

The runnable example captures a 14-second local answer budget at handler entry
and uses `timeout_at` around the entire scheduled answer, including queue wait
and HTTP. Telegram delivery latency has already spent part of the server
deadline, so applications should reduce the budget further when appropriate.
Dropping an answer while it waits cancels admission and prevents a late send;
cancellation after HTTP dispatch cannot retract a request already transmitted.

Wallet query types describe data received from Telegram. Signature/domain checks
(`bot<bot_id>`) and transaction confirmation belong to application logic; a
transaction notification is not proof of finality. TON decimal values are not
converted to machine-sized integers.

For ephemeral callback replies, omit `receiver_user_id`; `chat_id` is ignored,
and `answer_callback_query` is still required. Channel direct-message chats do
not support these replies. Missing chat targets and ephemeral objects with
neither receiver nor callback are rejected by `Payload::validate()` before JSON/multipart preparation or HTTP
dispatch. When a receiver is specified, supply the target chat;
when both receiver and callback are specified, the receiver must be the user who
pressed the button. Those identity/membership facts remain server-validated.

A private bot topic can be linked as `https://t.me/<bot_username>/<message_thread_id>`;
only the user whose private chat contains that topic can open it.

## Widget sandbox check

The example is [`widgets.rs`](../../crates/teloxide/examples/widgets.rs) and its
[`status.html`](../../crates/teloxide/examples/widgets/status.html). It handles
query updates with the real Bot API and detects clients lacking `request()`.

The optional HTML smoke check uses
[TelegramMessenger/widget-sandbox v0.1.1](https://github.com/TelegramMessenger/widget-sandbox/tree/v0.1.1).
From an installed checkout of that tag, run:

```sh
pnpm exec tsx /absolute/path/to/teloxide/docs/bot-api-10.4/check-widget-sandbox.mts
```

This executes the example in QuickJS in light and dark schemes, checks validated
render operations, `ready`, the explicit request-upgrade path and HTML compression
limits. SDK v1 in this tag predates `TelegramWidget.request`; it cannot certify
the current native query/wallet bridge. Those Bot API wire shapes are tested in
Rust. No live Telegram request, wallet signing or transfer is performed by these
checks.
