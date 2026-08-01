use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::Instant;

#[cfg(feature = "tracing")]
use tracing::Instrument;

use super::observer::next_instance_id;
use super::{
    AccumulatorSource, CleanupFailure, DraftAbortError, DraftCommitError, DraftConfig,
    DraftFinishError, DraftFlushError, DraftPushError, DraftRevision, DraftStartError,
    DrafterBackend, DrafterCapabilities, DrafterErrorClass, DrafterEvent, DrafterEventKind,
    DrafterObserver, DrafterOperation, DrafterPermit, DrafterPriority, DrafterRateLimiter,
    PreviewAck, PreviewSource, ReplacePreview,
};

/// A cloneable synchronous producer handle.
pub struct DraftSink<U> {
    apply: Arc<dyn Fn(U) -> Result<DraftRevision, DraftPushError> + Send + Sync>,
}

impl<U> Clone for DraftSink<U> {
    fn clone(&self) -> Self {
        Self { apply: Arc::clone(&self.apply) }
    }
}

impl<U> DraftSink<U> {
    pub(crate) fn new(
        apply: impl Fn(U) -> Result<DraftRevision, DraftPushError> + Send + Sync + 'static,
    ) -> Self {
        Self { apply: Arc::new(apply) }
    }

    /// Replace the latest preview snapshot.
    pub fn update(&self, preview: U) -> Result<DraftRevision, DraftPushError> {
        (self.apply)(preview)
    }

    /// Push one semantic update into an accumulator source.
    pub fn push(&self, update: U) -> Result<DraftRevision, DraftPushError> {
        (self.apply)(update)
    }
}

enum Command<B: DrafterBackend> {
    Flush {
        target: DraftRevision,
        reply: oneshot::Sender<Result<DraftRevision, DraftFlushError<B::Error>>>,
    },
    Commit {
        final_payload: B::Final,
        reply: oneshot::Sender<Result<B::SegmentOutput, DraftCommitError<B::Error>>>,
    },
    Finish {
        final_payload: B::Final,
        reply: oneshot::Sender<Result<B::Output, DraftFinishError<B::Error>>>,
    },
    Abort {
        reply: oneshot::Sender<Result<(), DraftAbortError<B::Error>>>,
    },
}

struct FlushWaiter<E> {
    target: DraftRevision,
    reply: oneshot::Sender<Result<DraftRevision, DraftFlushError<E>>>,
}

enum PreviewRunResult<B: DrafterBackend> {
    Idle,
    Continue,
    Command(Option<Command<B>>),
}

struct Worker<S, B, L>
where
    S: PreviewSource,
    B: DrafterBackend<Preview = S::Preview>,
    L: DrafterRateLimiter,
{
    source: Arc<S>,
    notify: Arc<Notify>,
    command_rx: mpsc::Receiver<Command<B>>,
    backend: Option<B>,
    limiter: L,
    config: DraftConfig,
    capabilities: DrafterCapabilities,
    last_delivered: DraftRevision,
    last_attempt: Option<Instant>,
    next_watchdog: Option<Instant>,
    retry_not_before: Option<Instant>,
    retry_delay: Duration,
    consecutive_preview_failures: u32,
    preview_disabled: bool,
    flush_waiters: Vec<FlushWaiter<B::Error>>,
    observer: Arc<dyn DrafterObserver>,
    rate_limit_key: super::DrafterRateLimitKey,
    instance_id: u64,
    segment: u64,
    segment_counter: Arc<AtomicU64>,
}

fn emit_event(observer: &Arc<dyn DrafterObserver>, event: DrafterEvent) {
    observer.record(event);
}

impl<S, B, L> Worker<S, B, L>
where
    S: PreviewSource,
    B: DrafterBackend<Preview = S::Preview>,
    L: DrafterRateLimiter,
{
    async fn run(mut self) {
        loop {
            match self.run_due_preview().await {
                PreviewRunResult::Continue => continue,
                PreviewRunResult::Command(Some(command)) => {
                    if !self.handle_command(command).await {
                        self.record(DrafterEventKind::WorkerStop, None, None);
                        return;
                    }
                    continue;
                }
                PreviewRunResult::Command(None) => {
                    self.source.close();
                    self.record(DrafterEventKind::WorkerStop, None, None);
                    return;
                }
                PreviewRunResult::Idle => {}
            }

            let deadline = self.next_deadline();
            tokio::select! {
                biased;
                command = self.command_rx.recv() => match command {
                    Some(command) => {
                        if !self.handle_command(command).await {
                            self.record(DrafterEventKind::WorkerStop, None, None);
                            return;
                        }
                    }
                    None => {
                        self.source.close();
                        self.record(DrafterEventKind::WorkerStop, None, None);
                        return;
                    }
                },
                _ = self.notify.notified() => {},
                _ = async {
                    if let Some(deadline) = deadline {
                        tokio::time::sleep_until(deadline).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {},
            }
        }
    }

    fn record(
        &self,
        kind: DrafterEventKind,
        revision: Option<DraftRevision>,
        operation: Option<DrafterOperation>,
    ) {
        let preview_message_id = self.backend.as_ref().and_then(DrafterBackend::preview_message_id);
        self.record_with_preview_message_id(kind, revision, operation, preview_message_id);
    }

    fn record_with_preview_message_id(
        &self,
        kind: DrafterEventKind,
        revision: Option<DraftRevision>,
        operation: Option<DrafterOperation>,
        preview_message_id: Option<teloxide_core::types::MessageId>,
    ) {
        emit_event(
            &self.observer,
            DrafterEvent {
                instance_id: self.instance_id,
                kind,
                mode: self.capabilities.mode,
                chat_id: self.rate_limit_key.chat_id,
                segment: self.segment,
                revision,
                from_revision: None,
                to_revision: None,
                operation,
                draft_id: self.backend.as_ref().and_then(DrafterBackend::draft_id),
                preview_message_id,
            },
        );
    }

    fn record_range(
        &self,
        kind: DrafterEventKind,
        revision: Option<DraftRevision>,
        from_revision: DraftRevision,
        to_revision: DraftRevision,
        operation: Option<DrafterOperation>,
    ) {
        emit_event(
            &self.observer,
            DrafterEvent {
                instance_id: self.instance_id,
                kind,
                mode: self.capabilities.mode,
                chat_id: self.rate_limit_key.chat_id,
                segment: self.segment,
                revision,
                from_revision: Some(from_revision),
                to_revision: Some(to_revision),
                operation,
                draft_id: self.backend.as_ref().and_then(DrafterBackend::draft_id),
                preview_message_id: self
                    .backend
                    .as_ref()
                    .and_then(DrafterBackend::preview_message_id),
            },
        );
    }

    fn next_deadline(&self) -> Option<Instant> {
        if self.preview_disabled {
            return None;
        }

        let current_revision = self.source.current_revision();
        if current_revision > self.last_delivered {
            let flush_requested =
                self.flush_waiters.iter().any(|waiter| waiter.target > self.last_delivered);
            let mut deadline = if flush_requested {
                Instant::now()
            } else {
                self.source.dirty_since().unwrap_or_else(Instant::now) + self.config.coalesce_window
            };
            if let Some(last_attempt) = self.last_attempt {
                deadline = deadline.max(last_attempt + self.config.min_update_interval);
            }
            if let Some(retry_not_before) = self.retry_not_before {
                deadline = deadline.max(retry_not_before);
            }
            return Some(deadline);
        }

        self.next_watchdog.map(|watchdog| {
            let mut deadline = watchdog;
            if let Some(last_attempt) = self.last_attempt {
                deadline = deadline.max(last_attempt + self.config.min_update_interval);
            }
            if let Some(retry_not_before) = self.retry_not_before {
                deadline = deadline.max(retry_not_before);
            }
            deadline
        })
    }

    async fn run_due_preview(&mut self) -> PreviewRunResult<B> {
        if !self.source.is_running() {
            return PreviewRunResult::Idle;
        }

        if self.preview_disabled {
            self.complete_flushes();
            return PreviewRunResult::Idle;
        }

        let now = Instant::now();
        let current_revision = self.source.current_revision();
        let changed = current_revision > self.last_delivered;
        let flush_requested =
            self.flush_waiters.iter().any(|waiter| waiter.target > self.last_delivered);
        let changed_due = changed
            && (flush_requested
                || self
                    .source
                    .dirty_since()
                    .is_some_and(|since| now >= since + self.config.coalesce_window))
            && self
                .last_attempt
                .is_none_or(|last_attempt| now >= last_attempt + self.config.min_update_interval)
            && self.retry_not_before.is_none_or(|deadline| now >= deadline);
        let refresh_due = !changed
            && self.capabilities.expires_without_refresh
            && self.next_watchdog.is_some_and(|deadline| now >= deadline)
            && self
                .last_attempt
                .is_none_or(|last_attempt| now >= last_attempt + self.config.min_update_interval)
            && self.retry_not_before.is_none_or(|deadline| now >= deadline);
        if !changed_due && !refresh_due {
            self.complete_flushes();
            return PreviewRunResult::Idle;
        }

        let reason =
            if changed_due { DrafterOperation::Preview } else { DrafterOperation::Refresh };
        let priority = if changed_due {
            DrafterPriority::ChangedPreview
        } else {
            DrafterPriority::RefreshPreview
        };
        if refresh_due {
            self.record(DrafterEventKind::Refresh, Some(current_revision), Some(reason));
        }

        let Some(backend) = self.backend.as_ref() else {
            return PreviewRunResult::Idle;
        };
        let key = backend.rate_limit_key();
        let _permit: DrafterPermit = tokio::select! {
            permit = self.limiter.acquire(key, priority) => permit,
            command = self.command_rx.recv() => {
                return PreviewRunResult::Command(command);
            }
        };

        if !self.source.is_running() {
            return PreviewRunResult::Continue;
        }

        // The state is intentionally read only now. Updates that arrived while
        // waiting for the shared limiter therefore replace the stale payload.
        let Some(snapshot) = self.source.snapshot() else {
            self.last_delivered = current_revision;
            self.source.mark_delivered(current_revision);
            self.next_watchdog = None;
            self.complete_flushes();
            return PreviewRunResult::Continue;
        };
        self.last_attempt = Some(Instant::now());
        self.retry_not_before = None;
        let operation = if refresh_due { DrafterOperation::Refresh } else { reason };
        self.record(DrafterEventKind::PreviewStart, Some(snapshot.revision), Some(operation));
        let result = {
            let backend = self.backend.as_mut().expect("backend exists before preview");
            tokio::time::timeout(self.config.request_timeout, backend.update(snapshot.preview))
                .await
        };

        match result {
            Ok(Ok(PreviewAck)) => {
                let skipped_from = DraftRevision(self.last_delivered.get().saturating_add(1));
                let skipped_to = DraftRevision(snapshot.revision.get().saturating_sub(1));
                self.last_delivered = self.last_delivered.max(snapshot.revision);
                self.source.mark_delivered(snapshot.revision);
                self.retry_delay = self.config.retry_initial;
                self.consecutive_preview_failures = 0;
                self.retry_not_before = None;
                self.next_watchdog = self
                    .capabilities
                    .expires_without_refresh
                    .then_some(Instant::now() + self.config.refresh_interval);
                self.record(
                    DrafterEventKind::PreviewSuccess,
                    Some(snapshot.revision),
                    Some(operation),
                );
                if skipped_from <= skipped_to {
                    self.record_range(
                        DrafterEventKind::Coalesced,
                        Some(snapshot.revision),
                        skipped_from,
                        skipped_to,
                        Some(operation),
                    );
                }
                self.complete_flushes();
            }
            Ok(Err(error)) => {
                let class = self
                    .backend
                    .as_ref()
                    .expect("backend exists after preview")
                    .classify_error(operation, &error);
                self.record(
                    DrafterEventKind::PreviewError,
                    Some(snapshot.revision),
                    Some(operation),
                );
                self.handle_preview_error(class, operation);
            }
            Err(_) => {
                let retry_safe = !matches!(
                    self.capabilities.mode,
                    super::DrafterMode::EditInPlace | super::DrafterMode::StatusEditThenSendFinal
                ) || self.last_delivered > DraftRevision::default();
                self.record(
                    DrafterEventKind::PreviewTimeout,
                    Some(snapshot.revision),
                    Some(operation),
                );
                self.handle_preview_error(DrafterErrorClass::Transient { retry_safe }, operation);
            }
        }
        PreviewRunResult::Continue
    }

    fn handle_preview_error(&mut self, class: DrafterErrorClass, operation: DrafterOperation) {
        self.consecutive_preview_failures = self.consecutive_preview_failures.saturating_add(1);
        match class {
            DrafterErrorClass::RetryAfter { delay, scope } => {
                self.record(DrafterEventKind::RetryAfter, None, Some(operation));
                self.limiter.penalize(scope, delay);
                self.retry_not_before = Some(Instant::now() + delay);
            }
            DrafterErrorClass::Transient { retry_safe: true }
                if self
                    .config
                    .max_consecutive_preview_failures
                    .is_none_or(|max| self.consecutive_preview_failures < max) =>
            {
                self.record(DrafterEventKind::TransientRetry, None, Some(operation));
                self.retry_not_before = Some(Instant::now() + self.retry_delay);
                self.retry_delay = (self.retry_delay * 2).min(self.config.retry_max);
            }
            DrafterErrorClass::Transient { retry_safe: false }
            | DrafterErrorClass::Permanent
            | DrafterErrorClass::Ambiguous
            | DrafterErrorClass::Transient { retry_safe: true } => {
                self.preview_disabled = true;
                self.next_watchdog = None;
                self.complete_flushes();
            }
        }
    }

    fn complete_flushes(&mut self) {
        let last_delivered = self.last_delivered;
        let preview_disabled = self.preview_disabled;
        let observer = Arc::clone(&self.observer);
        let mode = self.capabilities.mode;
        let chat_id = self.rate_limit_key.chat_id;
        let segment = self.segment;
        let mut remaining = Vec::with_capacity(self.flush_waiters.len());
        for waiter in self.flush_waiters.drain(..) {
            if waiter.target <= last_delivered {
                let _ = waiter.reply.send(Ok(last_delivered));
                emit_event(
                    &observer,
                    DrafterEvent {
                        instance_id: self.instance_id,
                        kind: DrafterEventKind::FlushComplete,
                        mode,
                        chat_id,
                        segment,
                        revision: Some(last_delivered),
                        from_revision: None,
                        to_revision: None,
                        operation: None,
                        draft_id: self.backend.as_ref().and_then(DrafterBackend::draft_id),
                        preview_message_id: self
                            .backend
                            .as_ref()
                            .and_then(DrafterBackend::preview_message_id),
                    },
                );
            } else if preview_disabled {
                let _ = waiter.reply.send(Err(DraftFlushError::PreviewDisabled));
            } else {
                remaining.push(waiter);
            }
        }
        self.flush_waiters = remaining;
    }

    async fn handle_command(&mut self, command: Command<B>) -> bool {
        match command {
            Command::Flush { target, reply } => {
                self.record(DrafterEventKind::FlushStart, Some(target), None);
                if target <= self.last_delivered {
                    let _ = reply.send(Ok(self.last_delivered));
                    self.record(DrafterEventKind::FlushComplete, Some(self.last_delivered), None);
                } else if self.preview_disabled {
                    let _ = reply.send(Err(DraftFlushError::PreviewDisabled));
                } else {
                    self.flush_waiters.push(FlushWaiter { target, reply });
                }
                true
            }
            Command::Commit { final_payload, reply } => {
                self.source.begin_transition();
                self.flush_waiters.drain(..).for_each(|waiter| {
                    let _ = waiter.reply.send(Err(DraftFlushError::WorkerStopped));
                });
                let Some(backend) = self.backend.as_mut() else {
                    let _ = reply.send(Err(DraftCommitError::WorkerStopped));
                    return false;
                };
                let key = backend.rate_limit_key();
                let observer = Arc::clone(&self.observer);
                let mode = self.capabilities.mode;
                let chat_id = self.rate_limit_key.chat_id;
                let segment = self.segment;
                let result = loop {
                    let _permit = self.limiter.acquire(key, DrafterPriority::SegmentCommit).await;
                    match backend.commit_segment(&final_payload).await {
                        Ok(output) => break Ok(output),
                        Err(error) => {
                            let class =
                                backend.classify_error(DrafterOperation::SegmentCommit, &error);
                            if let DrafterErrorClass::RetryAfter { delay, scope } = class {
                                emit_event(
                                    &observer,
                                    DrafterEvent {
                                        instance_id: self.instance_id,
                                        kind: DrafterEventKind::RetryAfter,
                                        mode,
                                        chat_id,
                                        segment,
                                        revision: None,
                                        from_revision: None,
                                        to_revision: None,
                                        operation: Some(DrafterOperation::SegmentCommit),
                                        draft_id: backend.draft_id(),
                                        preview_message_id: backend.preview_message_id(),
                                    },
                                );
                                self.limiter.penalize(scope, delay);
                                continue;
                            }
                            break Err(error);
                        }
                    }
                };
                let cleanup_failure = backend.take_cleanup_failure();
                match result {
                    Ok(output) => {
                        if let Some(failure) = cleanup_failure {
                            self.observe_cleanup_failure(failure);
                        }
                        self.record(
                            DrafterEventKind::SegmentCommit,
                            None,
                            Some(DrafterOperation::SegmentCommit),
                        );
                        self.reset_segment_state();
                        self.segment = self.segment.saturating_add(1);
                        self.segment_counter.store(self.segment, Ordering::Release);
                        self.record(DrafterEventKind::SegmentRotate, None, None);
                        self.source.reopen_segment();
                        let _ = reply.send(Ok(output));
                        true
                    }
                    Err(error) => {
                        self.record(
                            DrafterEventKind::SegmentCommitError,
                            None,
                            Some(DrafterOperation::SegmentCommit),
                        );
                        self.source.close();
                        let _ = reply.send(Err(DraftCommitError::Backend(error)));
                        false
                    }
                }
            }
            Command::Finish { final_payload, reply } => {
                self.record(DrafterEventKind::FinishStart, None, Some(DrafterOperation::Final));
                self.source.close();
                self.flush_waiters.drain(..).for_each(|waiter| {
                    let _ = waiter.reply.send(Err(DraftFlushError::WorkerStopped));
                });
                let Some(backend) = self.backend.as_mut() else {
                    let _ = reply.send(Err(DraftFinishError::WorkerStopped));
                    return false;
                };
                let key = backend.rate_limit_key();
                let observer = Arc::clone(&self.observer);
                let mode = self.capabilities.mode;
                let chat_id = self.rate_limit_key.chat_id;
                let segment = self.segment;
                let result = loop {
                    let _permit = self.limiter.acquire(key, DrafterPriority::Final).await;
                    match backend.finish(&final_payload).await {
                        Ok(output) => break Ok(output),
                        Err(error) => {
                            let class = backend.classify_error(DrafterOperation::Final, &error);
                            if let DrafterErrorClass::RetryAfter { delay, scope } = class {
                                emit_event(
                                    &observer,
                                    DrafterEvent {
                                        instance_id: self.instance_id,
                                        kind: DrafterEventKind::RetryAfter,
                                        mode,
                                        chat_id,
                                        segment,
                                        revision: None,
                                        from_revision: None,
                                        to_revision: None,
                                        operation: Some(DrafterOperation::Final),
                                        draft_id: backend.draft_id(),
                                        preview_message_id: backend.preview_message_id(),
                                    },
                                );
                                self.limiter.penalize(scope, delay);
                                continue;
                            }
                            break Err(error);
                        }
                    }
                };
                let cleanup_failure = backend.take_cleanup_failure();
                if let Some(failure) = cleanup_failure {
                    self.observe_cleanup_failure(failure);
                }
                if result.is_ok() {
                    self.record(
                        DrafterEventKind::FinalSuccess,
                        None,
                        Some(DrafterOperation::Final),
                    );
                } else {
                    self.record(DrafterEventKind::FinalError, None, Some(DrafterOperation::Final));
                }
                self.backend.take();
                let _ = reply.send(result.map_err(DraftFinishError::Backend));
                false
            }
            Command::Abort { reply } => {
                self.record(DrafterEventKind::Abort, None, Some(DrafterOperation::Cleanup));
                self.source.close();
                self.flush_waiters.drain(..).for_each(|waiter| {
                    let _ = waiter.reply.send(Err(DraftFlushError::WorkerStopped));
                });
                let Some(backend) = self.backend.as_mut() else {
                    let _ = reply.send(Err(DraftAbortError::WorkerStopped));
                    return false;
                };
                let (result, error_class) = {
                    let result = backend.abort().await;
                    let error_class = result
                        .as_ref()
                        .err()
                        .map(|error| backend.classify_error(DrafterOperation::Cleanup, error));
                    (result, error_class)
                };
                if let Some(DrafterErrorClass::RetryAfter { delay, scope }) = error_class {
                    self.record(
                        DrafterEventKind::RetryAfter,
                        None,
                        Some(DrafterOperation::Cleanup),
                    );
                    self.limiter.penalize(scope, delay);
                }
                if result.is_err() {
                    self.record(
                        DrafterEventKind::CleanupError,
                        None,
                        Some(DrafterOperation::Cleanup),
                    );
                    self.record(
                        DrafterEventKind::AbortError,
                        None,
                        Some(DrafterOperation::Cleanup),
                    );
                }
                self.backend.take();
                let _ = reply.send(result.map_err(DraftAbortError::Backend));
                false
            }
        }
    }

    fn reset_segment_state(&mut self) {
        self.last_delivered = DraftRevision::default();
        self.last_attempt = None;
        self.next_watchdog = None;
        self.retry_not_before = None;
        self.retry_delay = self.config.retry_initial;
        self.consecutive_preview_failures = 0;
        self.preview_disabled = false;
    }

    fn observe_cleanup_failure(&mut self, failure: CleanupFailure<B::Error>) {
        let class = self
            .backend
            .as_ref()
            .expect("backend exists while observing cleanup")
            .classify_error(DrafterOperation::Cleanup, &failure.error);
        if let DrafterErrorClass::RetryAfter { delay, scope } = class {
            self.record_with_preview_message_id(
                DrafterEventKind::RetryAfter,
                None,
                Some(DrafterOperation::Cleanup),
                Some(failure.message_id),
            );
            self.limiter.penalize(scope, delay);
        }
        self.record_with_preview_message_id(
            DrafterEventKind::CleanupError,
            None,
            Some(DrafterOperation::Cleanup),
            Some(failure.message_id),
        );
    }
}

/// Owns a drafter worker and the transition to its permanent result.
pub struct Drafter<S, B, L>
where
    S: PreviewSource,
    B: DrafterBackend<Preview = S::Preview>,
    L: DrafterRateLimiter,
{
    source: Arc<S>,
    commands: mpsc::Sender<Command<B>>,
    worker: Option<tokio::task::JoinHandle<()>>,
    _limiter: PhantomData<L>,
}

impl<S, B, L> Drafter<S, B, L>
where
    S: PreviewSource,
    B: DrafterBackend<Preview = S::Preview>,
    L: DrafterRateLimiter,
{
    fn spawn(
        source: S,
        backend: B,
        limiter: L,
        config: DraftConfig,
        observer: Arc<dyn DrafterObserver>,
    ) -> Result<(Self, DraftSink<S::Update>), DraftStartError> {
        let capabilities = backend.capabilities();
        config.validate(capabilities).map_err(DraftStartError::InvalidConfig)?;
        let rate_limit_key = backend.rate_limit_key();
        let instance_id = next_instance_id();
        let segment_counter = Arc::new(AtomicU64::new(0));
        observer.record(DrafterEvent {
            instance_id,
            kind: DrafterEventKind::Spawn,
            mode: capabilities.mode,
            chat_id: rate_limit_key.chat_id,
            segment: 0,
            revision: None,
            from_revision: None,
            to_revision: None,
            operation: None,
            draft_id: backend.draft_id(),
            preview_message_id: backend.preview_message_id(),
        });
        let source = Arc::new(source);
        let notify = Arc::new(Notify::new());
        let (commands, command_rx) = mpsc::channel(16);
        let worker = Worker {
            source: Arc::clone(&source),
            notify: Arc::clone(&notify),
            command_rx,
            backend: Some(backend),
            limiter,
            config: config.clone(),
            capabilities,
            last_delivered: DraftRevision::default(),
            last_attempt: None,
            next_watchdog: None,
            retry_not_before: None,
            retry_delay: config.retry_initial,
            consecutive_preview_failures: 0,
            preview_disabled: false,
            flush_waiters: Vec::new(),
            observer,
            rate_limit_key,
            instance_id,
            segment: 0,
            segment_counter: Arc::clone(&segment_counter),
        };
        let sink_observer = Arc::clone(&worker.observer);
        let sink_segment_counter = Arc::clone(&segment_counter);
        #[cfg(feature = "tracing")]
        let worker_span = tracing::info_span!(
            "teloxide.drafter",
            instance_id,
            mode = ?capabilities.mode,
            chat_id = rate_limit_key.chat_id.0,
        );
        #[cfg(feature = "tracing")]
        let worker = tokio::spawn(worker.run().instrument(worker_span));
        #[cfg(not(feature = "tracing"))]
        let worker = tokio::spawn(worker.run());
        let sink_source = Arc::clone(&source);
        let sink_notify = Arc::clone(&notify);
        let sink_mode = capabilities.mode;
        let sink_chat_id = rate_limit_key.chat_id;
        let sink = DraftSink::new(move |update| {
            let revision = sink_source.apply(update)?;
            emit_event(
                &sink_observer,
                DrafterEvent {
                    instance_id,
                    kind: DrafterEventKind::Update,
                    mode: sink_mode,
                    chat_id: sink_chat_id,
                    segment: sink_segment_counter.load(Ordering::Acquire),
                    revision: Some(revision),
                    from_revision: None,
                    to_revision: None,
                    operation: None,
                    draft_id: None,
                    preview_message_id: None,
                },
            );
            sink_notify.notify_one();
            Ok(revision)
        });
        Ok((Self { source, commands, worker: Some(worker), _limiter: PhantomData }, sink))
    }

    /// Waits until a revision existing at call time has been delivered.
    pub async fn flush(&self) -> Result<DraftRevision, DraftFlushError<B::Error>> {
        let target = self.source.current_revision();
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Command::Flush { target, reply })
            .await
            .map_err(|_| DraftFlushError::WorkerStopped)?;
        receiver.await.map_err(|_| DraftFlushError::WorkerStopped)?
    }

    /// Commits the current segment and reopens the producer for a new one.
    pub async fn commit_segment(
        &mut self,
        final_payload: B::Final,
    ) -> Result<B::SegmentOutput, DraftCommitError<B::Error>> {
        self.source.begin_transition();
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Command::Commit { final_payload, reply })
            .await
            .map_err(|_| DraftCommitError::WorkerStopped)?;
        receiver.await.map_err(|_| DraftCommitError::WorkerStopped)?
    }

    /// Stops previews and sends the permanent final payload.
    pub async fn finish(
        mut self,
        final_payload: B::Final,
    ) -> Result<B::Output, DraftFinishError<B::Error>> {
        self.source.close();
        let (reply, receiver) = oneshot::channel();
        if self.commands.send(Command::Finish { final_payload, reply }).await.is_err() {
            return Err(DraftFinishError::WorkerStopped);
        }
        let result = receiver.await.map_err(|_| DraftFinishError::WorkerStopped)?;
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
        result
    }

    /// Stops previews and invokes backend abort cleanup.
    pub async fn abort(mut self) -> Result<(), DraftAbortError<B::Error>> {
        self.source.close();
        let (reply, receiver) = oneshot::channel();
        if self.commands.send(Command::Abort { reply }).await.is_err() {
            return Err(DraftAbortError::WorkerStopped);
        }
        let result = receiver.await.map_err(|_| DraftAbortError::WorkerStopped)?;
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
        result
    }
}

impl<S, B, L> Drop for Drafter<S, B, L>
where
    S: PreviewSource,
    B: DrafterBackend<Preview = S::Preview>,
    L: DrafterRateLimiter,
{
    fn drop(&mut self) {
        self.source.close();
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

impl<P, B, L> Drafter<ReplacePreview<P>, B, L>
where
    P: Clone + Send + 'static,
    B: DrafterBackend<Preview = P>,
    L: DrafterRateLimiter,
{
    /// Creates a drafter whose updates replace the latest preview snapshot.
    pub fn snapshots(
        backend: B,
        limiter: L,
        config: DraftConfig,
    ) -> Result<(Self, DraftSink<P>), DraftStartError> {
        Self::snapshots_with_observer(backend, limiter, config, super::observer::default_observer())
    }

    /// Creates a snapshot drafter with a lifecycle observer.
    pub fn snapshots_with_observer(
        backend: B,
        limiter: L,
        config: DraftConfig,
        observer: Arc<dyn DrafterObserver>,
    ) -> Result<(Self, DraftSink<P>), DraftStartError> {
        Self::spawn(ReplacePreview::new(), backend, limiter, config, observer)
    }
}

impl<A, B, L> Drafter<AccumulatorSource<A>, B, L>
where
    A: super::DraftAccumulator,
    B: DrafterBackend<Preview = A::Preview>,
    L: DrafterRateLimiter,
{
    /// Creates a drafter backed by a semantic accumulator.
    pub fn accumulating(
        accumulator: A,
        backend: B,
        limiter: L,
        config: DraftConfig,
    ) -> Result<(Self, DraftSink<A::Update>), DraftStartError> {
        Self::accumulating_with_observer(
            accumulator,
            backend,
            limiter,
            config,
            super::observer::default_observer(),
        )
    }

    /// Creates an accumulator drafter with a lifecycle observer.
    pub fn accumulating_with_observer(
        accumulator: A,
        backend: B,
        limiter: L,
        config: DraftConfig,
        observer: Arc<dyn DrafterObserver>,
    ) -> Result<(Self, DraftSink<A::Update>), DraftStartError> {
        Self::spawn(AccumulatorSource::new(accumulator), backend, limiter, config, observer)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        fmt,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::drafter::{
        DraftAccumulator, DrafterMode, DrafterRateLimitKey, DrafterRateLimitScope,
        InProcessRateLimiter,
    };

    #[derive(Clone, Default)]
    struct NoopLimiter;

    impl DrafterRateLimiter for NoopLimiter {
        async fn acquire(
            &self,
            _key: DrafterRateLimitKey,
            _priority: DrafterPriority,
        ) -> DrafterPermit {
            DrafterPermit::new()
        }

        fn penalize(&self, _scope: DrafterRateLimitScope, _retry_after: Duration) {}
    }

    #[derive(Clone, Default)]
    struct RecordingLimiter {
        penalties: Arc<Mutex<Vec<(DrafterRateLimitScope, Duration)>>>,
    }

    impl DrafterRateLimiter for RecordingLimiter {
        async fn acquire(
            &self,
            _key: DrafterRateLimitKey,
            _priority: DrafterPriority,
        ) -> DrafterPermit {
            DrafterPermit::new()
        }

        fn penalize(&self, scope: DrafterRateLimitScope, retry_after: Duration) {
            self.penalties.lock().unwrap().push((scope, retry_after));
        }
    }

    #[derive(Clone, Default)]
    struct RecordingObserver {
        events: Arc<Mutex<Vec<DrafterEvent>>>,
    }

    impl DrafterObserver for RecordingObserver {
        fn record(&self, event: DrafterEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    struct FakeBackend {
        previews: Arc<Mutex<Vec<String>>>,
        expires_without_refresh: bool,
    }

    impl Default for FakeBackend {
        fn default() -> Self {
            Self { previews: Arc::new(Mutex::new(Vec::new())), expires_without_refresh: false }
        }
    }

    impl DrafterBackend for FakeBackend {
        type Preview = String;
        type Final = String;
        type SegmentOutput = String;
        type Output = String;
        type Error = Infallible;

        fn capabilities(&self) -> DrafterCapabilities {
            DrafterCapabilities {
                mode: DrafterMode::EditInPlace,
                expires_without_refresh: self.expires_without_refresh,
                supports_draft_thinking: false,
                supports_rich_preview: false,
            }
        }

        async fn update(&mut self, preview: String) -> Result<PreviewAck, Self::Error> {
            self.previews.lock().unwrap().push(preview);
            Ok(PreviewAck)
        }

        async fn commit_segment(&mut self, final_payload: &String) -> Result<String, Self::Error> {
            Ok(final_payload.clone())
        }

        async fn finish(&mut self, final_payload: &String) -> Result<String, Self::Error> {
            Ok(final_payload.clone())
        }

        async fn abort(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum TestError {
        RetryAfter,
        Transient,
        Ambiguous,
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{self:?}")
        }
    }

    impl std::error::Error for TestError {}

    struct ClassifiedBackend {
        previews: Arc<Mutex<Vec<String>>>,
        update_calls: usize,
        fail_update_at: Option<usize>,
        final_attempts: Arc<Mutex<usize>>,
        retry_final_once: bool,
        ambiguous_final: bool,
        commit_attempts: Arc<Mutex<usize>>,
        retry_commit_once: bool,
        cleanup_retry_once: bool,
        abort_cleanup_retry_once: bool,
        preview_message_id: Option<teloxide_core::types::MessageId>,
        expires_without_refresh: bool,
    }

    impl DrafterBackend for ClassifiedBackend {
        type Preview = String;
        type Final = String;
        type SegmentOutput = String;
        type Output = String;
        type Error = TestError;

        fn capabilities(&self) -> DrafterCapabilities {
            DrafterCapabilities {
                mode: DrafterMode::NativeDraft,
                expires_without_refresh: self.expires_without_refresh,
                supports_draft_thinking: false,
                supports_rich_preview: false,
            }
        }

        async fn update(&mut self, preview: String) -> Result<PreviewAck, Self::Error> {
            self.update_calls += 1;
            self.previews.lock().unwrap().push(preview);
            if self.fail_update_at == Some(self.update_calls) {
                Err(TestError::Transient)
            } else {
                Ok(PreviewAck)
            }
        }

        async fn commit_segment(&mut self, final_payload: &String) -> Result<String, Self::Error> {
            *self.commit_attempts.lock().unwrap() += 1;
            if self.retry_commit_once {
                self.retry_commit_once = false;
                Err(TestError::RetryAfter)
            } else {
                Ok(final_payload.clone())
            }
        }

        async fn finish(&mut self, final_payload: &String) -> Result<String, Self::Error> {
            *self.final_attempts.lock().unwrap() += 1;
            if self.ambiguous_final {
                Err(TestError::Ambiguous)
            } else if self.retry_final_once {
                self.retry_final_once = false;
                Err(TestError::RetryAfter)
            } else {
                Ok(final_payload.clone())
            }
        }

        async fn abort(&mut self) -> Result<(), Self::Error> {
            if self.abort_cleanup_retry_once {
                self.abort_cleanup_retry_once = false;
                Err(TestError::RetryAfter)
            } else {
                Ok(())
            }
        }

        fn preview_message_id(&self) -> Option<teloxide_core::types::MessageId> {
            self.preview_message_id
        }

        fn take_cleanup_failure(&mut self) -> Option<CleanupFailure<Self::Error>> {
            if self.cleanup_retry_once {
                self.cleanup_retry_once = false;
                let message_id = self.preview_message_id.take().expect("cleanup preview id");
                Some(CleanupFailure { message_id, error: TestError::RetryAfter })
            } else {
                None
            }
        }

        fn classify_error(
            &self,
            _operation: DrafterOperation,
            error: &Self::Error,
        ) -> DrafterErrorClass {
            match error {
                TestError::RetryAfter => DrafterErrorClass::RetryAfter {
                    delay: Duration::from_millis(20),
                    scope: DrafterRateLimitScope::Global,
                },
                TestError::Transient => DrafterErrorClass::Transient { retry_safe: true },
                TestError::Ambiguous => DrafterErrorClass::Ambiguous,
            }
        }
    }

    #[derive(Clone)]
    struct BlockingPreviewLimiter {
        preview_started: Arc<tokio::sync::Notify>,
    }

    impl DrafterRateLimiter for BlockingPreviewLimiter {
        async fn acquire(
            &self,
            _key: DrafterRateLimitKey,
            priority: DrafterPriority,
        ) -> DrafterPermit {
            if matches!(priority, DrafterPriority::RefreshPreview | DrafterPriority::ChangedPreview)
            {
                self.preview_started.notify_one();
                std::future::pending::<()>().await;
            }
            DrafterPermit::new()
        }

        fn penalize(&self, _scope: DrafterRateLimitScope, _retry_after: Duration) {}
    }

    #[derive(Clone)]
    struct PermitGateLimiter {
        permit_started: Arc<tokio::sync::Notify>,
        release_permit: Arc<tokio::sync::Notify>,
    }

    impl DrafterRateLimiter for PermitGateLimiter {
        async fn acquire(
            &self,
            _key: DrafterRateLimitKey,
            priority: DrafterPriority,
        ) -> DrafterPermit {
            if matches!(priority, DrafterPriority::RefreshPreview | DrafterPriority::ChangedPreview)
            {
                self.permit_started.notify_one();
                self.release_permit.notified().await;
            }
            DrafterPermit::new()
        }

        fn penalize(&self, _scope: DrafterRateLimitScope, _retry_after: Duration) {}
    }

    #[tokio::test(start_paused = true)]
    async fn latest_snapshot_is_sent_once_after_coalesce_window() {
        let previews = Arc::new(Mutex::new(Vec::new()));
        let backend = FakeBackend { previews: Arc::clone(&previews), ..FakeBackend::default() };
        let (drafter, sink) = Drafter::snapshots(
            backend,
            NoopLimiter,
            DraftConfig { min_update_interval: Duration::from_millis(1), ..DraftConfig::default() },
        )
        .unwrap();
        for value in 0..100 {
            sink.update(value.to_string()).unwrap();
        }
        tokio::time::advance(Duration::from_millis(100)).await;
        drafter.flush().await.unwrap();
        assert_eq!(&*previews.lock().unwrap(), &["99"]);
        let _ = drafter.abort().await;
    }

    #[tokio::test(start_paused = true)]
    async fn sink_rejects_updates_after_finish_starts() {
        let (drafter, sink) = Drafter::snapshots(
            FakeBackend::default(),
            NoopLimiter,
            DraftConfig { min_update_interval: Duration::from_millis(1), ..DraftConfig::default() },
        )
        .unwrap();
        let finish = tokio::spawn(async move { drafter.finish("done".to_owned()).await });
        tokio::task::yield_now().await;
        assert!(matches!(sink.update("late".to_owned()), Err(DraftPushError::Closed)));
        finish.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn finish_preempts_preview_waiting_for_limiter() {
        let previews = Arc::new(Mutex::new(Vec::new()));
        let preview_started = Arc::new(tokio::sync::Notify::new());
        let backend = FakeBackend { previews: Arc::clone(&previews), ..FakeBackend::default() };
        let limiter = BlockingPreviewLimiter { preview_started: Arc::clone(&preview_started) };
        let (drafter, sink) = Drafter::snapshots(
            backend,
            limiter,
            DraftConfig {
                coalesce_window: Duration::from_millis(1),
                min_update_interval: Duration::from_millis(1),
                ..DraftConfig::default()
            },
        )
        .unwrap();

        sink.update("stale".to_owned()).unwrap();
        tokio::time::advance(Duration::from_millis(2)).await;
        preview_started.notified().await;
        let result = drafter.finish("final".to_owned()).await.unwrap();

        assert_eq!(result, "final");
        assert!(previews.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn closed_source_is_rechecked_after_permit() {
        let previews = Arc::new(Mutex::new(Vec::new()));
        let permit_started = Arc::new(tokio::sync::Notify::new());
        let release_permit = Arc::new(tokio::sync::Notify::new());
        let backend = FakeBackend { previews: Arc::clone(&previews), ..FakeBackend::default() };
        let limiter = PermitGateLimiter {
            permit_started: Arc::clone(&permit_started),
            release_permit: Arc::clone(&release_permit),
        };
        let (drafter, sink) = Drafter::snapshots(
            backend,
            limiter,
            DraftConfig {
                coalesce_window: Duration::from_millis(1),
                min_update_interval: Duration::from_millis(1),
                ..DraftConfig::default()
            },
        )
        .unwrap();

        sink.update("stale".to_owned()).unwrap();
        tokio::time::advance(Duration::from_millis(2)).await;
        permit_started.notified().await;
        drafter.source.close();
        release_permit.notify_one();

        let result = drafter.finish("final".to_owned()).await.unwrap();
        assert_eq!(result, "final");
        assert!(previews.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_is_built_after_permit_and_uses_latest_revision() {
        let previews = Arc::new(Mutex::new(Vec::new()));
        let permit_started = Arc::new(tokio::sync::Notify::new());
        let release_permit = Arc::new(tokio::sync::Notify::new());
        let backend = FakeBackend { previews: Arc::clone(&previews), ..FakeBackend::default() };
        let limiter = PermitGateLimiter {
            permit_started: Arc::clone(&permit_started),
            release_permit: Arc::clone(&release_permit),
        };
        let (drafter, sink) = Drafter::snapshots(
            backend,
            limiter,
            DraftConfig {
                coalesce_window: Duration::from_millis(1),
                min_update_interval: Duration::from_millis(1),
                ..DraftConfig::default()
            },
        )
        .unwrap();

        sink.update("old".to_owned()).unwrap();
        tokio::time::advance(Duration::from_millis(2)).await;
        permit_started.notified().await;
        sink.update("latest".to_owned()).unwrap();
        release_permit.notify_one();
        drafter.flush().await.unwrap();

        assert_eq!(&*previews.lock().unwrap(), &["latest"]);
        let _ = drafter.abort().await;
    }

    struct TextAccumulator(String);

    impl DraftAccumulator for TextAccumulator {
        type Update = String;
        type Preview = String;

        fn apply(&mut self, update: Self::Update) {
            self.0.push_str(&update);
        }

        fn snapshot(&self) -> Option<Self::Preview> {
            (!self.0.is_empty()).then(|| self.0.clone())
        }

        fn reset_segment(&mut self) {
            self.0.clear();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn accumulator_builds_preview_only_when_worker_reads_it() {
        let previews = Arc::new(Mutex::new(Vec::new()));
        let backend = FakeBackend { previews: Arc::clone(&previews), ..FakeBackend::default() };
        let (drafter, sink) = Drafter::accumulating(
            TextAccumulator(String::new()),
            backend,
            NoopLimiter,
            DraftConfig {
                coalesce_window: Duration::from_millis(10),
                min_update_interval: Duration::from_millis(1),
                ..DraftConfig::default()
            },
        )
        .unwrap();
        sink.push("a".to_owned()).unwrap();
        sink.push("b".to_owned()).unwrap();
        tokio::time::advance(Duration::from_millis(11)).await;
        drafter.flush().await.unwrap();
        assert_eq!(&*previews.lock().unwrap(), &["ab"]);
        let _ = drafter.abort().await;
    }

    #[tokio::test(start_paused = true)]
    async fn accumulator_resets_after_segment_commit() {
        let previews = Arc::new(Mutex::new(Vec::new()));
        let backend = FakeBackend { previews: Arc::clone(&previews), ..FakeBackend::default() };
        let (mut drafter, sink) = Drafter::accumulating(
            TextAccumulator(String::new()),
            backend,
            NoopLimiter,
            DraftConfig {
                coalesce_window: Duration::from_millis(1),
                min_update_interval: Duration::from_millis(1),
                ..DraftConfig::default()
            },
        )
        .unwrap();

        sink.push("abc".to_owned()).unwrap();
        drafter.flush().await.unwrap();
        drafter.commit_segment("abc".to_owned()).await.unwrap();
        sink.push("d".to_owned()).unwrap();
        drafter.flush().await.unwrap();

        assert_eq!(&*previews.lock().unwrap(), &["abc", "d"]);
        let _ = drafter.abort().await;
    }

    #[tokio::test(start_paused = true)]
    async fn expiring_backend_is_refreshed_without_new_updates() {
        let previews = Arc::new(Mutex::new(Vec::new()));
        let backend =
            FakeBackend { previews: Arc::clone(&previews), expires_without_refresh: true };
        let (drafter, sink) = Drafter::snapshots(
            backend,
            NoopLimiter,
            DraftConfig {
                coalesce_window: Duration::from_millis(1),
                min_update_interval: Duration::from_millis(1),
                refresh_interval: Duration::from_millis(20),
                request_timeout: Duration::from_millis(5),
                ..DraftConfig::default()
            },
        )
        .unwrap();
        sink.update("state".to_owned()).unwrap();
        drafter.flush().await.unwrap();
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(25)).await;
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(previews.lock().unwrap().len(), 2);
        let _ = drafter.abort().await;
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_respects_retry_backoff() {
        let previews = Arc::new(Mutex::new(Vec::new()));
        let backend = ClassifiedBackend {
            previews: Arc::clone(&previews),
            update_calls: 0,
            fail_update_at: Some(2),
            final_attempts: Arc::new(Mutex::new(0)),
            retry_final_once: false,
            ambiguous_final: false,
            commit_attempts: Arc::new(Mutex::new(0)),
            retry_commit_once: false,
            cleanup_retry_once: false,
            abort_cleanup_retry_once: false,
            preview_message_id: None,
            expires_without_refresh: true,
        };
        let limiter = InProcessRateLimiter::new(Duration::ZERO, Duration::ZERO);
        let (drafter, sink) = Drafter::snapshots(
            backend,
            limiter,
            DraftConfig {
                coalesce_window: Duration::from_millis(1),
                min_update_interval: Duration::from_millis(1),
                refresh_interval: Duration::from_millis(10),
                request_timeout: Duration::from_millis(5),
                retry_initial: Duration::from_millis(20),
                retry_max: Duration::from_millis(40),
                ..DraftConfig::default()
            },
        )
        .unwrap();

        sink.update("state".to_owned()).unwrap();
        drafter.flush().await.unwrap();
        tokio::time::advance(Duration::from_millis(11)).await;
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(previews.lock().unwrap().len(), 2);

        tokio::time::advance(Duration::from_millis(19)).await;
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(previews.lock().unwrap().len(), 2);

        tokio::time::advance(Duration::from_millis(1)).await;
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(previews.lock().unwrap().len(), 3);
        let _ = drafter.abort().await;
    }

    #[tokio::test(start_paused = true)]
    async fn final_retries_explicit_retry_after() {
        let final_attempts = Arc::new(Mutex::new(0));
        let backend = ClassifiedBackend {
            previews: Arc::new(Mutex::new(Vec::new())),
            update_calls: 0,
            fail_update_at: None,
            final_attempts: Arc::clone(&final_attempts),
            retry_final_once: true,
            ambiguous_final: false,
            commit_attempts: Arc::new(Mutex::new(0)),
            retry_commit_once: false,
            cleanup_retry_once: false,
            abort_cleanup_retry_once: false,
            preview_message_id: None,
            expires_without_refresh: false,
        };
        let limiter = InProcessRateLimiter::new(Duration::ZERO, Duration::ZERO);
        let (drafter, _sink) =
            Drafter::snapshots(backend, limiter, DraftConfig::default()).unwrap();
        let finish = tokio::spawn(async move { drafter.finish("final".to_owned()).await });

        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(*final_attempts.lock().unwrap(), 1);
        tokio::time::advance(Duration::from_millis(20)).await;
        let result = finish.await.unwrap().unwrap();

        assert_eq!(result, "final");
        assert_eq!(*final_attempts.lock().unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_retry_after_penalizes_shared_limiter() {
        let penalties = Arc::new(Mutex::new(Vec::new()));
        let limiter = RecordingLimiter { penalties: Arc::clone(&penalties) };
        let backend = ClassifiedBackend {
            previews: Arc::new(Mutex::new(Vec::new())),
            update_calls: 0,
            fail_update_at: None,
            final_attempts: Arc::new(Mutex::new(0)),
            retry_final_once: false,
            ambiguous_final: false,
            commit_attempts: Arc::new(Mutex::new(0)),
            retry_commit_once: false,
            cleanup_retry_once: true,
            abort_cleanup_retry_once: false,
            preview_message_id: Some(teloxide_core::types::MessageId(77)),
            expires_without_refresh: false,
        };
        let observer = RecordingObserver::default();
        let observer_ref = Arc::new(observer.clone());
        let (drafter, _sink) = Drafter::snapshots_with_observer(
            backend,
            limiter,
            DraftConfig::default(),
            Arc::clone(&observer_ref) as Arc<dyn DrafterObserver>,
        )
        .unwrap();

        drafter.finish("final".to_owned()).await.unwrap();

        assert_eq!(
            penalties.lock().unwrap().as_slice(),
            &[(DrafterRateLimitScope::Global, Duration::from_millis(20))]
        );
        let events = observer.events.lock().unwrap();
        let cleanup_event = events
            .iter()
            .find(|event| event.kind == DrafterEventKind::CleanupError)
            .expect("cleanup event");
        assert_eq!(cleanup_event.preview_message_id, Some(teloxide_core::types::MessageId(77)));
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_failure_detaches_old_preview_before_next_segment() {
        let observer = RecordingObserver::default();
        let observer_ref = Arc::new(observer.clone());
        let backend = ClassifiedBackend {
            previews: Arc::new(Mutex::new(Vec::new())),
            update_calls: 0,
            fail_update_at: None,
            final_attempts: Arc::new(Mutex::new(0)),
            retry_final_once: false,
            ambiguous_final: false,
            commit_attempts: Arc::new(Mutex::new(0)),
            retry_commit_once: false,
            cleanup_retry_once: true,
            abort_cleanup_retry_once: false,
            preview_message_id: Some(teloxide_core::types::MessageId(77)),
            expires_without_refresh: false,
        };
        let (mut drafter, sink) = Drafter::snapshots_with_observer(
            backend,
            NoopLimiter,
            DraftConfig::default(),
            Arc::clone(&observer_ref) as Arc<dyn DrafterObserver>,
        )
        .unwrap();

        drafter.commit_segment("segment".to_owned()).await.unwrap();
        sink.update("next segment".to_owned()).unwrap();
        drafter.flush().await.unwrap();

        let events = observer.events.lock().unwrap();
        let cleanup_event = events
            .iter()
            .find(|event| event.kind == DrafterEventKind::CleanupError)
            .expect("cleanup event");
        assert_eq!(cleanup_event.preview_message_id, Some(teloxide_core::types::MessageId(77)));
        let preview_start = events
            .iter()
            .rev()
            .find(|event| event.kind == DrafterEventKind::PreviewStart)
            .expect("next segment preview");
        assert_eq!(preview_start.preview_message_id, None);
    }

    #[tokio::test(start_paused = true)]
    async fn abort_cleanup_retry_after_penalizes_shared_limiter() {
        let penalties = Arc::new(Mutex::new(Vec::new()));
        let limiter = RecordingLimiter { penalties: Arc::clone(&penalties) };
        let backend = ClassifiedBackend {
            previews: Arc::new(Mutex::new(Vec::new())),
            update_calls: 0,
            fail_update_at: None,
            final_attempts: Arc::new(Mutex::new(0)),
            retry_final_once: false,
            ambiguous_final: false,
            commit_attempts: Arc::new(Mutex::new(0)),
            retry_commit_once: false,
            cleanup_retry_once: false,
            abort_cleanup_retry_once: true,
            preview_message_id: Some(teloxide_core::types::MessageId(88)),
            expires_without_refresh: false,
        };
        let (drafter, _sink) =
            Drafter::snapshots(backend, limiter, DraftConfig::default()).unwrap();

        assert!(matches!(
            drafter.abort().await,
            Err(DraftAbortError::Backend(TestError::RetryAfter))
        ));
        assert_eq!(
            penalties.lock().unwrap().as_slice(),
            &[(DrafterRateLimitScope::Global, Duration::from_millis(20))]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn segment_commit_retries_explicit_retry_after() {
        let commit_attempts = Arc::new(Mutex::new(0));
        let backend = ClassifiedBackend {
            previews: Arc::new(Mutex::new(Vec::new())),
            update_calls: 0,
            fail_update_at: None,
            final_attempts: Arc::new(Mutex::new(0)),
            retry_final_once: false,
            ambiguous_final: false,
            commit_attempts: Arc::clone(&commit_attempts),
            retry_commit_once: true,
            cleanup_retry_once: false,
            abort_cleanup_retry_once: false,
            preview_message_id: None,
            expires_without_refresh: false,
        };
        let limiter = InProcessRateLimiter::new(Duration::ZERO, Duration::ZERO);
        let (mut drafter, _sink) =
            Drafter::snapshots(backend, limiter, DraftConfig::default()).unwrap();
        let commit =
            tokio::spawn(async move { drafter.commit_segment("segment".to_owned()).await });

        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(*commit_attempts.lock().unwrap(), 1);
        tokio::time::advance(Duration::from_millis(20)).await;
        assert_eq!(commit.await.unwrap().unwrap(), "segment");
        assert_eq!(*commit_attempts.lock().unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_final_is_not_retried() {
        let final_attempts = Arc::new(Mutex::new(0));
        let backend = ClassifiedBackend {
            previews: Arc::new(Mutex::new(Vec::new())),
            update_calls: 0,
            fail_update_at: None,
            final_attempts: Arc::clone(&final_attempts),
            retry_final_once: false,
            ambiguous_final: true,
            commit_attempts: Arc::new(Mutex::new(0)),
            retry_commit_once: false,
            cleanup_retry_once: false,
            abort_cleanup_retry_once: false,
            preview_message_id: None,
            expires_without_refresh: false,
        };
        let (drafter, _sink) =
            Drafter::snapshots(backend, NoopLimiter, DraftConfig::default()).unwrap();
        let result = drafter.finish("final".to_owned()).await;

        assert!(matches!(result, Err(DraftFinishError::Backend(TestError::Ambiguous))));
        assert_eq!(*final_attempts.lock().unwrap(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn metrics_observer_records_scheduler_lifecycle() {
        let collector = Arc::new(super::super::DrafterMetricsCollector::default());
        let (drafter, sink) = Drafter::snapshots_with_observer(
            FakeBackend::default(),
            NoopLimiter,
            DraftConfig {
                coalesce_window: Duration::from_millis(1),
                min_update_interval: Duration::from_millis(1),
                ..DraftConfig::default()
            },
            Arc::clone(&collector) as Arc<dyn DrafterObserver>,
        )
        .unwrap();

        for index in 0..100 {
            sink.update(index.to_string()).unwrap();
        }
        tokio::time::advance(Duration::from_millis(2)).await;
        drafter.flush().await.unwrap();
        let _ = drafter.finish("final".to_owned()).await;

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.received_updates, 100);
        assert_eq!(snapshot.sent_previews, 1);
        assert_eq!(snapshot.coalesced_updates, 99);
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_events_have_unique_instance_ids() {
        let observer = RecordingObserver::default();
        let observer_ref = Arc::new(observer.clone());
        let (drafter_a, _sink_a) = Drafter::snapshots_with_observer(
            FakeBackend::default(),
            NoopLimiter,
            DraftConfig::default(),
            Arc::clone(&observer_ref) as Arc<dyn DrafterObserver>,
        )
        .unwrap();
        let (drafter_b, _sink_b) = Drafter::snapshots_with_observer(
            FakeBackend::default(),
            NoopLimiter,
            DraftConfig::default(),
            observer_ref,
        )
        .unwrap();

        let spawn_ids: Vec<_> = {
            let events = observer.events.lock().unwrap();
            events
                .iter()
                .filter(|event| event.kind == DrafterEventKind::Spawn)
                .map(|event| event.instance_id)
                .collect()
        };
        assert_eq!(spawn_ids.len(), 2);
        assert_ne!(spawn_ids[0], spawn_ids[1]);
        drafter_a.finish("a".to_owned()).await.unwrap();
        drafter_b.finish("b".to_owned()).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn update_after_segment_commit_has_new_segment_id() {
        let observer = RecordingObserver::default();
        let observer_ref = Arc::new(observer.clone());
        let (mut drafter, sink) = Drafter::snapshots_with_observer(
            FakeBackend::default(),
            NoopLimiter,
            DraftConfig::default(),
            observer_ref,
        )
        .unwrap();

        drafter.commit_segment("segment".to_owned()).await.unwrap();
        sink.update("new segment".to_owned()).unwrap();

        let update_segment = {
            let events = observer.events.lock().unwrap();
            events
                .iter()
                .rev()
                .find(|event| event.kind == DrafterEventKind::Update)
                .expect("update event")
                .segment
        };
        assert_eq!(update_segment, 1);
        drafter.finish("final".to_owned()).await.unwrap();
    }
}
