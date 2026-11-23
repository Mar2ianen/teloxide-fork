<div align="center">
  <img src="../../media/teloxide-core-logo.svg" width="250"/>

  <h1>teloxide-core</h1>
  <a href="https://github.com/teloxide/teloxide-core/actions">
    <img src="https://github.com/teloxide/teloxide-core/workflows/Continuous%20integration/badge.svg">
  </a>
  <a href="https://docs.rs/teloxide_core/">
    <img src="https://docs.rs/teloxide-core/badge.svg">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg">
  </a>
  <a href="https://core.telegram.org/bots/api">
    <img src="https://img.shields.io/badge/API%20coverage-Bot%20API%2010.5%20core-yellowgreen.svg">
  </a>
  <a href="https://crates.io/crates/teloxide_core">
    <img src="https://img.shields.io/crates/v/teloxide_core.svg">
  </a>
  <a href="https://t.me/teloxide">
    <img src="https://img.shields.io/badge/official%20chat-t.me%2Fteloxide-blueviolet">
  </a>

  The core part of [`teloxide`] providing tools for making requests to the [Telegram Bot API] with ease. This library is fully asynchronous and built using [`tokio`].
</div>

```toml
teloxide-core = "0.17.0-rc.1"
```
_Compiler support: requires rustc 1.85+_.

The 10.5 delta adds bot settings and the full bot-info response, write-access
events for owner-invited users and arbitrary blocks in expandable quotations.
See the [official-source audit](../../docs/bot-api-10.5/README.md) for the
coverage checklist. The preceding 10.4 audit is available at
[`docs/bot-api-10.4`](../../docs/bot-api-10.4/README.md).

The current development line exposes Telegram Bot API 10.5, including rich-message
buttons and document blocks, expandable quotations, compact tables, ephemeral
message parameters and editing methods, native draft Stop controls, disabled
buttons, force-reply flags, stopped-generation updates and new service
messages. See the repository's
[`MIGRATION_GUIDE.md`](../../MIGRATION_GUIDE.md) for upgrade notes.

[`teloxide`]: https://docs.rs/teloxide
[Telegram Bot API]: https://core.telegram.org/bots/api
[`tokio`]: https://tokio.rs
