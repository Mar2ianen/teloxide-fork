//! Some useful utilities.

pub mod command;
pub mod html;
pub mod markdown;
pub mod render;
pub(crate) mod shutdown_token;
#[cfg(feature = "time-rendering")]
pub mod time;

pub use teloxide_core::net::client_from_env;
