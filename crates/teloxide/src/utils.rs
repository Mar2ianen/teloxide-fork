//! Some useful utilities.

pub mod command;
pub mod html;
pub mod markdown;
pub mod render;
#[cfg(feature = "rich-text")]
pub mod rich_text;
pub(crate) mod shutdown_token;
#[cfg(any(feature = "time-rendering", feature = "rich-text"))]
pub mod time;

pub use teloxide_core::net::client_from_env;
