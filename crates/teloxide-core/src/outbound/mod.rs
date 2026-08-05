//! Deterministic outbound scheduling model (internal draft, Commit 1).
//!
//! A pure state machine with explicit time: no Tokio, no actor, no public
//! API. The actor, completion-aware permits and the `Requester` adapter are
//! added in later commits; this module is not exported publicly yet.
//!
//! The scheduler owns only the shared admission/rate/order layer: priorities
//! with aging, per-chat ordering lanes, rolling windows, `RetryAfter`
//! penalties and latest-wins replacement of pending jobs. It never executes
//! requests itself and never retries them (retry remains the policy of the
//! calling layer).

mod scheduler;
mod types;
