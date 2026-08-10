//! Public throttle adaptor backed by the outbound scheduler.
//!
//! The old worker/request-lock implementation was removed after the
//! scheduler-backed compatibility layer completed its parity period.

/// `Settings` and `Limits` structures.
mod settings;
pub use settings::{Limits, Settings};

/// Automatic request limits respecting mechanism.
///
/// Telegram has strict [limits], which, if exceeded will sooner or later cause
/// `RequestError::RetryAfter(_)` errors. This wrapper schedules throttled
/// requests so they can be sent without exceeding the configured windows.
///
/// It is recommended to use this wrapper before other wrappers (i.e.:
/// `SomeWrapper<Throttle<Bot>>`) because inner wrappers may otherwise cause
/// `Throttle` to miscalculate limits usage.
///
/// [limits]: https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
///
/// ## Examples
///
/// ```no_run (throttle fails to spawn task without tokio runtime)
/// use teloxide_core::{adaptors::throttle::Limits, requests::RequesterExt, Bot};
///
/// let bot = Bot::new("TOKEN")
///     .throttle(Limits::default());
///
/// /* send many requests here */
/// ```
///
/// ## Note about send-by-@channelusername
///
/// Telegram has limits on sending messages to the same chat. The scheduler
/// canonicalizes channel usernames, but `ChatId::ChannelUsername` remains
/// less precise than a numeric chat identifier; numeric IDs are preferred.
pub type Throttle<B> = crate::adaptors::throttle_compat::ThrottleCompat<B>;
