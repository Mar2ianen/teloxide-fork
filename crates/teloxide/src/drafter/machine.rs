use std::{marker::PhantomData, sync::Arc, time::Duration};

use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::Instant;

#[cfg(feature = "tracing")]
use tracing::Instrument;

use super::{
    AccumulatorSource, DraftAbortError, DraftCommitError, DraftConfig, DraftFinishError,
    DraftFlushError, DraftPushError, DraftRevision, DraftStartError, DrafterBackend,
    DrafterCapabilities, DrafterErrorClass, DrafterOperation, DrafterPermit, DrafterPriority,
    DrafterRateLimiter, PreviewAck, PreviewSource, ReplacePreview,
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
}

impl<S, B, L> Worker<S, B, L>
where
    S: PreviewSource,
    B: DrafterBackend<Preview = S::Preview>,
    L: DrafterRateLimiter,
{
    async fn run(mut self) {
        loop {
            if self.run_due_preview().await {
                continue;
            }

            let deadline = self.next_deadline();
            tokio::select! {
                biased;
                command = self.command_rx.recv() => match command {
                    Some(command) => {
                        if !self.handle_command(command).await {
                            return;
                        }
                    }
                    None => {
                        self.source.close();
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

        self.next_watchdog
    }

    async fn run_due_preview(&mut self) -> bool {
        if !self.source.is_running() {
            return false;
        }

        if self.preview_disabled {
            self.complete_flushes();
            return false;
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
            && self.next_watchdog.is_some_and(|deadline| now >= deadline);
        if !changed_due && !refresh_due {
            self.complete_flushes();
            return false;
        }

        let reason =
            if changed_due { DrafterOperation::Preview } else { DrafterOperation::Refresh };
        let priority = if changed_due {
            DrafterPriority::ChangedPreview
        } else {
            DrafterPriority::RefreshPreview
        };

        let Some(backend) = self.backend.as_ref() else {
            return false;
        };
        let key = backend.rate_limit_key();
        let _permit: DrafterPermit = self.limiter.acquire(key, priority).await;

        // The state is intentionally read only now. Updates that arrived while
        // waiting for the shared limiter therefore replace the stale payload.
        let Some(snapshot) = self.source.snapshot() else {
            self.last_delivered = current_revision;
            self.source.mark_delivered(current_revision);
            self.next_watchdog = None;
            self.complete_flushes();
            return true;
        };
        self.last_attempt = Some(Instant::now());
        self.retry_not_before = None;
        let operation = if refresh_due { DrafterOperation::Refresh } else { reason };
        let result = {
            let backend = self.backend.as_mut().expect("backend exists before preview");
            tokio::time::timeout(self.config.request_timeout, backend.update(snapshot.preview))
                .await
        };

        match result {
            Ok(Ok(PreviewAck)) => {
                self.last_delivered = self.last_delivered.max(snapshot.revision);
                self.source.mark_delivered(snapshot.revision);
                self.retry_delay = self.config.retry_initial;
                self.consecutive_preview_failures = 0;
                self.retry_not_before = None;
                self.next_watchdog = self
                    .capabilities
                    .expires_without_refresh
                    .then_some(Instant::now() + self.config.refresh_interval);
                self.complete_flushes();
            }
            Ok(Err(error)) => {
                let class = self
                    .backend
                    .as_ref()
                    .expect("backend exists after preview")
                    .classify_error(operation, &error);
                self.handle_preview_error(class);
            }
            Err(_) => {
                let retry_safe = !matches!(
                    self.capabilities.mode,
                    super::DrafterMode::EditInPlace | super::DrafterMode::StatusEditThenSendFinal
                ) || self.last_delivered > DraftRevision::default();
                self.handle_preview_error(DrafterErrorClass::Transient { retry_safe });
            }
        }
        true
    }

    fn handle_preview_error(&mut self, class: DrafterErrorClass) {
        self.consecutive_preview_failures = self.consecutive_preview_failures.saturating_add(1);
        match class {
            DrafterErrorClass::RetryAfter { delay, scope } => {
                self.limiter.penalize(scope, delay);
                self.retry_not_before = Some(Instant::now() + delay);
            }
            DrafterErrorClass::Transient { retry_safe: true }
                if self
                    .config
                    .max_consecutive_preview_failures
                    .is_none_or(|max| self.consecutive_preview_failures < max) =>
            {
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
        let mut remaining = Vec::with_capacity(self.flush_waiters.len());
        for waiter in self.flush_waiters.drain(..) {
            if waiter.target <= last_delivered {
                let _ = waiter.reply.send(Ok(last_delivered));
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
                if target <= self.last_delivered {
                    let _ = reply.send(Ok(self.last_delivered));
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
                let _permit = self.limiter.acquire(key, DrafterPriority::SegmentCommit).await;
                let result = backend.commit_segment(final_payload).await;
                match result {
                    Ok(output) => {
                        self.source.reopen_segment();
                        self.reset_segment_state();
                        let _ = reply.send(Ok(output));
                        true
                    }
                    Err(error) => {
                        self.source.close();
                        let _ = reply.send(Err(DraftCommitError::Backend(error)));
                        false
                    }
                }
            }
            Command::Finish { final_payload, reply } => {
                self.source.close();
                self.flush_waiters.drain(..).for_each(|waiter| {
                    let _ = waiter.reply.send(Err(DraftFlushError::WorkerStopped));
                });
                let Some(backend) = self.backend.take() else {
                    let _ = reply.send(Err(DraftFinishError::WorkerStopped));
                    return false;
                };
                let key = backend.rate_limit_key();
                let _permit = self.limiter.acquire(key, DrafterPriority::Final).await;
                let result = backend.finish(final_payload).await;
                let _ = reply.send(result.map_err(DraftFinishError::Backend));
                false
            }
            Command::Abort { reply } => {
                self.source.close();
                self.flush_waiters.drain(..).for_each(|waiter| {
                    let _ = waiter.reply.send(Err(DraftFlushError::WorkerStopped));
                });
                let Some(backend) = self.backend.take() else {
                    let _ = reply.send(Err(DraftAbortError::WorkerStopped));
                    return false;
                };
                let result = backend.abort().await;
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
    ) -> Result<(Self, DraftSink<S::Update>), DraftStartError> {
        let capabilities = backend.capabilities();
        config.validate(capabilities).map_err(DraftStartError::InvalidConfig)?;
        let source = Arc::new(source);
        let notify = Arc::new(Notify::new());
        let (commands, command_rx) = mpsc::channel(16);
        #[cfg(feature = "tracing")]
        let rate_limit_key = backend.rate_limit_key();
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
        };
        #[cfg(feature = "tracing")]
        let worker_span = tracing::info_span!(
            "teloxide.drafter",
            mode = ?capabilities.mode,
            chat_id = rate_limit_key.chat_id.0,
        );
        #[cfg(feature = "tracing")]
        let worker = tokio::spawn(worker.run().instrument(worker_span));
        #[cfg(not(feature = "tracing"))]
        let worker = tokio::spawn(worker.run());
        let sink_source = Arc::clone(&source);
        let sink_notify = Arc::clone(&notify);
        let sink = DraftSink::new(move |update| {
            let revision = sink_source.apply(update)?;
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
        Self::spawn(ReplacePreview::new(), backend, limiter, config)
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
        Self::spawn(AccumulatorSource::new(accumulator), backend, limiter, config)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::drafter::{
        DraftAccumulator, DrafterMode, DrafterRateLimitKey, DrafterRateLimitScope,
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

        async fn commit_segment(&mut self, final_payload: String) -> Result<String, Self::Error> {
            Ok(final_payload)
        }

        async fn finish(self, final_payload: String) -> Result<String, Self::Error> {
            Ok(final_payload)
        }

        async fn abort(self) -> Result<(), Self::Error> {
            Ok(())
        }
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
        sink.update("a".to_owned()).unwrap();
        sink.update("b".to_owned()).unwrap();
        tokio::time::advance(Duration::from_millis(100)).await;
        drafter.flush().await.unwrap();
        assert_eq!(&*previews.lock().unwrap(), &["b"]);
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
}
