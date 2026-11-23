# Bot API 10.5 audit — November 22, 2026

This audit covers the **10.4 → 10.5 delta** in Telegram's official Bot API.
It checks the API model and method changes against a frozen official HTML
snapshot; it does not certify live Telegram behavior or re-audit inherited API
coverage.

## Reproducible sources

- [`api.html.gz`](api.html.gz) is the official Bot API page retrieved on
  November 23, 2026. Its SHA-256 is recorded in [`source.json`](source.json).
- [`changelog.txt`](changelog.txt) records the 10.5 changelog summary; the
  [official changelog](https://core.telegram.org/bots/api-changelog#november-22-2026)
  is the source.
- Run `python3 docs/bot-api-10.5/audit.py` from the repository root. The script
  verifies the frozen source hash and compares changed/new object fields,
  requiredness, wire types, `setMySettings` parameters, and `getMe`'s return type
  with `custom_v2.json`. The Rust schema/codegen checks validate generated
  payload and requester surfaces separately.

## Coverage checklist

| Official change | Implementation |
|---|---|
| Bot settings | Generated `set_my_settings` payload and `Requester` method with all 14 optional parameters; callback query mode is a typed enum. |
| Request adaptors | Forwarded through `Bot`, cache, parse-mode, trace, erased, throttle-compat and outbound requesters; global scheduler classification. |
| Bot identity | `get_me` returns `BotFullInfo`, including active usernames, inline placeholder/location and topic-sidebar state. `Me` remains a compatibility alias. |
| User model | Moved bot-configuration fields from `User` to `BotFullInfo`, matching the 10.5 split. |
| Invited users | Added optional `WriteAccessAllowed::inviter` for owner invitations. |
| Expandable quotes | Replaced quote `text` with rich `blocks` in incoming and input quote models; nested input blocks use existing recursive request validation. |

Package versions and release tags remain unchanged on the development branch;
10.5 work targets `next` for review.
