use std::time::Duration;

use super::{DraftConfigError, DrafterCapabilities};

/// Timing and retry settings for a [`Drafter`](super::Drafter) lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DraftConfig {
    /// Time to wait after the first dirty update before producing a preview.
    pub coalesce_window: Duration,
    /// Minimum time between preview attempts by one drafter.
    pub min_update_interval: Duration,
    /// Time between native-draft refreshes.
    pub refresh_interval: Duration,
    /// Maximum duration of one preview request.
    pub request_timeout: Duration,
    /// Initial delay used for retryable preview failures.
    pub retry_initial: Duration,
    /// Maximum delay used for retryable preview failures.
    pub retry_max: Duration,
    /// Maximum number of additional terminal attempts after the first one.
    pub terminal_retry_budget: u32,
    /// Overall deadline for one segment-commit or final-delivery operation.
    pub terminal_timeout: Duration,
    /// Number of consecutive preview failures after which previews are
    /// disabled.
    pub max_consecutive_preview_failures: Option<u32>,
}

impl Default for DraftConfig {
    fn default() -> Self {
        Self {
            coalesce_window: Duration::from_millis(75),
            min_update_interval: Duration::from_secs(1),
            refresh_interval: Duration::from_secs(15),
            request_timeout: Duration::from_secs(8),
            retry_initial: Duration::from_secs(1),
            retry_max: Duration::from_secs(15),
            terminal_retry_budget: 3,
            terminal_timeout: Duration::from_secs(30),
            max_consecutive_preview_failures: Some(5),
        }
    }
}

impl DraftConfig {
    pub(crate) fn validate(
        &self,
        capabilities: DrafterCapabilities,
    ) -> Result<(), DraftConfigError> {
        if self.coalesce_window.is_zero() {
            return Err(DraftConfigError::ZeroDuration("coalesce_window"));
        }
        if self.min_update_interval.is_zero() {
            return Err(DraftConfigError::ZeroDuration("min_update_interval"));
        }
        if self.request_timeout.is_zero() {
            return Err(DraftConfigError::ZeroDuration("request_timeout"));
        }
        if self.retry_initial.is_zero() {
            return Err(DraftConfigError::ZeroDuration("retry_initial"));
        }
        if self.terminal_timeout.is_zero() {
            return Err(DraftConfigError::ZeroDuration("terminal_timeout"));
        }
        if self.retry_initial > self.retry_max {
            return Err(DraftConfigError::RetryRange);
        }
        if capabilities.expires_without_refresh && self.request_timeout >= self.refresh_interval {
            return Err(DraftConfigError::RequestTimeoutNotBelowRefresh);
        }
        if capabilities.expires_without_refresh && self.refresh_interval >= Duration::from_secs(30)
        {
            return Err(DraftConfigError::RefreshIntervalTooLong);
        }
        Ok(())
    }
}

/// A compact schedule view useful when configuring a drafter from an
/// application-specific configuration object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DraftSchedule {
    pub coalesce_window: Duration,
    pub min_update_interval: Duration,
    pub refresh_interval: Duration,
}

impl From<&DraftConfig> for DraftSchedule {
    fn from(config: &DraftConfig) -> Self {
        Self {
            coalesce_window: config.coalesce_window,
            min_update_interval: config.min_update_interval,
            refresh_interval: config.refresh_interval,
        }
    }
}
