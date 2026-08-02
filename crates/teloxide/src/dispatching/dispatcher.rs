use crate::{
    dispatching::{
        distribution::default_distribution_function, DefaultKey, DpHandlerDescription,
        ShutdownToken,
    },
    error_handlers::{ErrorHandler, LoggingErrorHandler},
    requests::{Request, Requester},
    stop::StopToken,
    types::{Update, UpdateKind},
    update_listeners::{self, UpdateListener},
};

use dptree::di::DependencyMap;
use either::Either;
use futures::{
    future::{self, BoxFuture},
    stream::FuturesUnordered,
    FutureExt as _, StreamExt as _,
};
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;

use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    ops::{ControlFlow, Deref},
    pin::pin,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
};

/// The builder for [`Dispatcher`].
///
/// See also: ["Dispatching or
/// REPLs?"](../dispatching/index.html#dispatching-or-repls)
pub struct DispatcherBuilder<R, Err, Key> {
    bot: R,
    dependencies: DependencyMap,
    handler: Arc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Arc<dyn ErrorHandler<Err> + Send + Sync>,
    worker_error_handler: Arc<dyn ErrorHandler<WorkerDispatchError> + Send + Sync>,
    ctrlc_handler: bool,
    distribution_f: fn(&Update) -> Option<Key>,
    worker_queue_size: usize,
}

impl<R, Err, Key> DispatcherBuilder<R, Err, Key>
where
    R: Clone + Requester + Clone + Send + Sync + 'static,
    Err: Debug + Send + Sync + 'static,
{
    /// Specifies a handler that will be called for an unhandled update.
    ///
    /// By default, it is a mere [`log::warn`].
    #[must_use]
    pub fn default_handler<H, Fut>(self, handler: H) -> Self
    where
        H: Fn(Arc<Update>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler = Arc::new(handler);

        Self {
            default_handler: Arc::new(move |upd| {
                let handler = Arc::clone(&handler);
                Box::pin(handler(upd))
            }),
            ..self
        }
    }

    /// Specifies a handler that will be called on a handler error.
    ///
    /// By default, it is [`LoggingErrorHandler`].
    #[must_use]
    pub fn error_handler(self, handler: Arc<dyn ErrorHandler<Err> + Send + Sync>) -> Self {
        Self { error_handler: handler, ..self }
    }

    /// Specifies a handler that will be called when the dispatcher cannot
    /// deliver an update to a worker.
    ///
    /// This happens when a worker task dies, the dispatcher spawns a fresh
    /// worker and retries the dispatch once, but the replacement worker also
    /// stops before accepting the update. The update is dropped after the
    /// handler is called.
    ///
    /// By default, it is [`LoggingErrorHandler`].
    #[must_use]
    pub fn worker_error_handler(
        self,
        handler: Arc<dyn ErrorHandler<WorkerDispatchError> + Send + Sync>,
    ) -> Self {
        Self { worker_error_handler: handler, ..self }
    }

    /// Specifies dependencies that can be used inside of handlers.
    ///
    /// By default, there is no dependencies.
    #[must_use]
    pub fn dependencies(self, dependencies: DependencyMap) -> Self {
        Self { dependencies, ..self }
    }

    /// Enables the `^C` handler that [`shutdown`]s dispatching.
    ///
    /// [`shutdown`]: ShutdownToken::shutdown
    #[cfg(feature = "ctrlc_handler")]
    #[must_use]
    pub fn enable_ctrlc_handler(self) -> Self {
        Self { ctrlc_handler: true, ..self }
    }

    /// Specifies size of the queue for workers.
    ///
    /// By default it's 64.
    #[must_use]
    pub fn worker_queue_size(self, size: usize) -> Self {
        Self { worker_queue_size: size, ..self }
    }

    /// Specifies the stack size available to the dispatcher.
    ///
    /// By default, it's 8 * 1024 * 1024 bytes (8 MiB).
    #[must_use]
    #[deprecated(since = "0.15.0", note = "This method is a no-op; you can just remove it.")]
    pub fn stack_size(self, _size: usize) -> Self {
        self
    }

    /// Specifies the distribution function that decides how updates are grouped
    /// before execution.
    ///
    /// ## Update grouping
    ///
    /// When [`Dispatcher`] receives updates, it runs dispatching tree
    /// (handlers) concurrently. This means that multiple updates can be
    /// processed at the same time.
    ///
    /// However, this is not always convenient. For example, if you have global
    /// state, then you may want to process some updates sequentially, to
    /// prevent state inconsistencies.
    ///
    /// This is why `teloxide` allows grouping updates. Updates for which the
    /// distribution function `f` returns the same "distribution key" `K` will
    /// be run in sequence (while still being processed concurrently with the
    /// updates with different distribution keys).
    ///
    /// Updates for which `f` returns `None` will always be processed in
    /// parallel.
    ///
    /// ## Default distribution function
    ///
    /// By default the distribution function is equivalent to `|upd|
    /// upd.chat().map(|chat| chat.id)`, so updates from the same chat will be
    /// processed sequentially.
    ///
    /// This pair nicely with dialogue system, which has state attached to
    /// chats.
    ///
    /// ## Examples
    ///
    /// Grouping updates by user who caused this update to happen:
    ///
    /// ```
    /// use teloxide::{dispatching::Dispatcher, dptree, Bot};
    ///
    /// let bot = Bot::new("TOKEN");
    /// let handler = dptree::entry() /* ... */;
    /// let dp = Dispatcher::builder(bot, handler)
    ///     .distribution_function(|upd| upd.from().map(|user| user.id))
    ///     .build();
    /// # let _: Dispatcher<_, (), _> = dp;
    /// ```
    ///
    /// Not grouping updates at all, always processing updates concurrently:
    ///
    /// ```
    /// use teloxide::{dispatching::Dispatcher, dptree, Bot};
    ///
    /// let bot = Bot::new("TOKEN");
    /// let handler = dptree::entry() /* ... */;
    /// let dp = Dispatcher::builder(bot, handler).distribution_function(|_| None::<()>).build();
    /// # let _: Dispatcher<_, (), _> = dp;
    /// ```
    #[must_use]
    pub fn distribution_function<K>(
        self,
        f: fn(&Update) -> Option<K>,
    ) -> DispatcherBuilder<R, Err, K>
    where
        K: Hash + Eq,
    {
        let Self {
            bot,
            dependencies,
            handler,
            default_handler,
            error_handler,
            worker_error_handler,
            ctrlc_handler,
            distribution_f: _,
            worker_queue_size,
        } = self;

        DispatcherBuilder {
            bot,
            dependencies,
            handler,
            default_handler,
            error_handler,
            worker_error_handler,
            ctrlc_handler,
            distribution_f: f,
            worker_queue_size,
        }
    }

    /// Constructs [`Dispatcher`].
    ///
    /// ## Panics
    /// This function will panic at run-time if [`dptree`] fails to type-check
    /// the provided handler. An appropriate error message will be emitted.
    #[must_use]
    pub fn build(self) -> Dispatcher<R, Err, Key> {
        let Self {
            bot,
            dependencies,
            handler,
            default_handler,
            error_handler,
            worker_error_handler,
            distribution_f,
            worker_queue_size,
            ctrlc_handler,
        } = self;

        dptree::type_check(
            handler.sig(),
            &dependencies,
            &[
                dptree::Type::of::<R>(),
                dptree::Type::of::<teloxide_core::types::Update>(),
                dptree::Type::of::<teloxide_core::types::Me>(),
            ],
        );

        // If the `ctrlc_handler` feature is not enabled, don't emit a warning.
        let _ = ctrlc_handler;

        let dp = Dispatcher {
            bot,
            dependencies,
            handler,
            default_handler,
            error_handler,
            worker_error_handler,
            state: ShutdownToken::new(),
            distribution_f,
            worker_queue_size,
            workers: HashMap::new(),
            default_worker: None,
            current_number_of_active_workers: Default::default(),
            max_number_of_active_workers: Default::default(),
        };

        #[cfg(feature = "ctrlc_handler")]
        {
            if ctrlc_handler {
                let mut dp = dp;
                dp.setup_ctrlc_handler_inner();
                return dp;
            }
        }

        dp
    }
}

/// The base for update dispatching.
///
/// ## Update grouping
///
/// `Dispatcher` generally processes updates concurrently. However, by default,
/// updates from the same chat are processed sequentially. Learn more about
/// [update grouping].
///
/// See also: ["Dispatching or
/// REPLs?"](../dispatching/index.html#dispatching-or-repls)
///
/// [update grouping]: DispatcherBuilder#update-grouping
pub struct Dispatcher<R, Err, Key> {
    bot: R,
    dependencies: DependencyMap,

    handler: Arc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,

    distribution_f: fn(&Update) -> Option<Key>,
    worker_queue_size: usize,
    current_number_of_active_workers: Arc<AtomicU32>,
    max_number_of_active_workers: Arc<AtomicU32>,
    // Tokio TX channel parts associated with chat IDs that consume updates sequentially.
    workers: HashMap<Key, Worker>,
    // The default TX part that consume updates concurrently.
    default_worker: Option<Worker>,

    error_handler: Arc<dyn ErrorHandler<Err> + Send + Sync>,
    worker_error_handler: Arc<dyn ErrorHandler<WorkerDispatchError> + Send + Sync>,

    state: ShutdownToken,
}

struct Worker {
    tx: tokio::sync::mpsc::Sender<Update>,
    handle: tokio::task::JoinHandle<()>,
    is_waiting: Arc<AtomicBool>,
}

// TODO: it is allowed to return message as response on telegram request in
// webhooks, so we can allow this too. See more there: https://core.telegram.org/bots/api#making-requests-when-getting-updates

/// A handler that processes updates from Telegram.
pub type UpdateHandler<Err> = dptree::Handler<'static, Result<(), Err>, DpHandlerDescription>;

type DefaultHandler = Arc<dyn Fn(Arc<Update>) -> BoxFuture<'static, ()> + Send + Sync>;

impl<R, Err> Dispatcher<R, Err, DefaultKey>
where
    R: Requester + Clone + Send + Sync + 'static,
    Err: Send + Sync + 'static,
{
    /// Constructs a new [`DispatcherBuilder`] with `bot` and `handler`.
    #[must_use]
    pub fn builder(bot: R, handler: UpdateHandler<Err>) -> DispatcherBuilder<R, Err, DefaultKey>
    where
        Err: Debug,
    {
        const DEFAULT_WORKER_QUEUE_SIZE: usize = 64;

        DispatcherBuilder {
            bot,
            dependencies: DependencyMap::new(),
            handler: Arc::new(handler),
            default_handler: Arc::new(|upd| {
                log::warn!("Unhandled update: {upd:?}");
                Box::pin(async {})
            }),
            error_handler: LoggingErrorHandler::new(),
            worker_error_handler: LoggingErrorHandler::new(),
            ctrlc_handler: false,
            worker_queue_size: DEFAULT_WORKER_QUEUE_SIZE,
            distribution_f: default_distribution_function,
        }
    }
}

impl<R, Err, Key> Dispatcher<R, Err, Key>
where
    R: Requester + Clone + Send + Sync + 'static,
    Err: Send + Sync + 'static,
    Key: Hash + Eq + Clone + Send,
{
    /// Starts your bot with the default parameters.
    ///
    /// The default parameters are a long polling update listener and log all
    /// errors produced by this listener.
    ///
    /// Each time a handler is invoked, [`Dispatcher`] adds the following
    /// dependencies (in addition to those passed to
    /// [`DispatcherBuilder::dependencies`]):
    ///
    ///  - Your bot passed to [`Dispatcher::builder`];
    ///  - An update from Telegram;
    ///  - [`crate::types::Me`] (can be used in [`HandlerExt::filter_command`]).
    ///
    /// [`HandlerExt::filter_command`]: crate::dispatching::HandlerExt::filter_command
    pub async fn dispatch(&mut self)
    where
        R: Requester + Clone,
        <R as Requester>::GetUpdates: Send,
    {
        let listener = update_listeners::polling_default(self.bot.clone()).await;
        let error_handler =
            LoggingErrorHandler::with_custom_text("An error from the update listener");

        self.dispatch_with_listener(listener, error_handler).await;
    }

    /// Starts your bot with custom `update_listener` and
    /// `update_listener_error_handler`.
    ///
    /// This method adds the same dependencies as [`Dispatcher::dispatch`].
    pub async fn dispatch_with_listener<'a, UListener, Eh>(
        &'a mut self,
        update_listener: UListener,
        update_listener_error_handler: Arc<Eh>,
    ) where
        UListener: UpdateListener + Send + 'a,
        Eh: ErrorHandler<UListener::Err> + Send + Sync + 'a,
        UListener::Err: Debug,
    {
        self.try_dispatch_with_listener(update_listener, update_listener_error_handler)
            .await
            .expect("Couldn't prepare dispatching context")
    }

    /// Same as `dispatch_with_listener` but returns a `Err(_)` instead of
    /// panicking when the initial telegram api call (`get_me`) fails.
    ///
    /// Starts your bot with custom `update_listener` and
    /// `update_listener_error_handler`.
    ///
    /// This method adds the same dependencies as [`Dispatcher::dispatch`].
    pub async fn try_dispatch_with_listener<'a, UListener, Eh>(
        &'a mut self,
        mut update_listener: UListener,
        update_listener_error_handler: Arc<Eh>,
    ) -> Result<(), R::Err>
    where
        UListener: UpdateListener + Send + 'a,
        Eh: ErrorHandler<UListener::Err> + Send + Sync + 'a,
        UListener::Err: Debug,
    {
        // FIXME: there should be a way to check if dependency is already inserted
        let me = self.bot.get_me().send().await?;
        self.dependencies.insert(me);
        self.dependencies.insert(self.bot.clone());

        let description = self.handler.description();
        let allowed_updates = description.allowed_updates();
        log::debug!("hinting allowed updates: {allowed_updates:?}");
        update_listener.hint_allowed_updates(&mut allowed_updates.into_iter());

        let stop_token = Some(update_listener.stop_token());
        self.start_listening(update_listener, update_listener_error_handler, stop_token).await;

        Ok(())
    }

    async fn start_listening<'a, UListener, Eh>(
        &'a mut self,
        mut update_listener: UListener,
        update_listener_error_handler: Arc<Eh>,
        mut stop_token: Option<StopToken>,
    ) where
        UListener: UpdateListener + 'a,
        Eh: ErrorHandler<UListener::Err> + 'a,
        UListener::Err: Debug,
    {
        self.state.start_dispatching();

        let stream = update_listener.as_stream();
        tokio::pin!(stream);

        loop {
            self.remove_inactive_workers_if_needed().await;

            let res = future::select(stream.next(), pin!(self.state.wait_for_changes()))
                .map(either)
                .await
                .map_either(|l| l.0, |r| r.0);

            match res {
                Either::Left(upd) => match upd {
                    Some(upd) => self.process_update(upd, &update_listener_error_handler).await,
                    None => break,
                },
                Either::Right(()) => {
                    if self.state.is_shutting_down() {
                        if let Some(token) = stop_token.take() {
                            log::debug!("Start shutting down dispatching...");
                            token.stop();
                        }
                    }
                }
            }
        }

        self.workers
            .drain()
            .map(|(_chat_id, worker)| worker.handle)
            .chain(self.default_worker.take().map(|worker| worker.handle))
            .collect::<FuturesUnordered<_>>()
            .for_each(|res| async {
                res.expect("Failed to wait for a worker.");
            })
            .await;

        self.state.done();
    }

    async fn process_update<LErr, LErrHandler>(
        &mut self,
        update: Result<Update, LErr>,
        err_handler: &Arc<LErrHandler>,
    ) where
        LErrHandler: ErrorHandler<LErr>,
    {
        let upd = match update {
            Ok(upd) => upd,
            Err(err) => {
                err_handler.clone().handle_error(err).await;
                return;
            }
        };

        if let UpdateKind::Error(err) = upd.kind {
            log::error!(
                "Cannot parse an update.\nError: {err:?}\n\
                            This is a bug in teloxide-core, please open an issue here: \
                            https://github.com/teloxide/teloxide/issues.",
            );
            return;
        }

        let key = (self.distribution_f)(&upd);

        match self.try_dispatch_update(key.clone(), upd).await {
            Ok(()) => {}
            Err((update, termination)) => {
                // The worker task died; retry the dispatch with a fresh worker
                // once before reporting the failure.
                log::warn!(
                    "A worker task died while dispatching an update ({termination}); retrying the \
                     dispatch with a fresh worker once"
                );

                if let Err((update, termination)) = self.try_dispatch_update(key, update).await {
                    // The replacement worker also stopped before accepting the
                    // update. Report the undeliverable update through the
                    // dispatcher error policy instead of panicking.
                    self.worker_error_handler
                        .clone()
                        .handle_error(WorkerDispatchError { update, termination })
                        .await;
                }
            }
        }
    }

    /// Sends an update to the worker responsible for `key`, spawning the worker
    /// on demand.
    ///
    /// If the worker task died (its channel is closed), the dead worker is
    /// removed from the dispatcher and the update is returned back along with
    /// the reason the task stopped, so that the caller can spawn a fresh
    /// worker and retry the dispatch.
    async fn try_dispatch_update(
        &mut self,
        key: Option<Key>,
        update: Update,
    ) -> Result<(), (Update, WorkerTermination)> {
        let send_result = {
            let worker = match key {
                Some(ref key) => {
                    if !self.workers.contains_key(key) {
                        self.workers.insert(key.clone(), self.new_worker());
                    }
                    self.workers.get_mut(key).expect("worker was inserted just above")
                }
                None => {
                    if self.default_worker.is_none() {
                        self.default_worker = Some(self.new_default_worker());
                    }
                    self.default_worker.as_mut().expect("worker was inserted just above")
                }
            };

            worker.tx.send(update).await
        };

        match send_result {
            Ok(()) => Ok(()),
            Err(SendError(update)) => {
                // The channel is closed, which means the worker task has
                // terminated. Remove it from the dispatcher and learn why it
                // stopped so that the caller can spawn a replacement.
                let dead_worker = match key {
                    Some(key) => self.workers.remove(&key),
                    None => self.default_worker.take(),
                };

                let termination = match dead_worker {
                    Some(worker) => await_worker_termination(worker.handle).await,
                    None => {
                        log::error!("The worker task is missing while its channel is closed");
                        WorkerTermination::Finished
                    }
                };

                Err((update, termination))
            }
        }
    }

    fn new_worker(&self) -> Worker {
        spawn_worker(
            self.dependencies.clone(),
            Arc::clone(&self.handler),
            Arc::clone(&self.default_handler),
            Arc::clone(&self.error_handler),
            Arc::clone(&self.current_number_of_active_workers),
            Arc::clone(&self.max_number_of_active_workers),
            self.worker_queue_size,
        )
    }

    fn new_default_worker(&self) -> Worker {
        spawn_default_worker(
            self.dependencies.clone(),
            Arc::clone(&self.handler),
            Arc::clone(&self.default_handler),
            Arc::clone(&self.error_handler),
            self.worker_queue_size,
        )
    }

    async fn remove_inactive_workers_if_needed(&mut self) {
        let workers = self.workers.len();
        let max = self.max_number_of_active_workers.load(Ordering::Relaxed) as usize;

        if workers <= max {
            return;
        }

        self.remove_inactive_workers().await;
    }

    #[inline(never)] // Cold function.
    async fn remove_inactive_workers(&mut self) {
        let handles = self
            .workers
            .iter()
            .filter(|(_, worker)| {
                worker.tx.capacity() == self.worker_queue_size
                    && worker.is_waiting.load(Ordering::Relaxed)
            })
            .map(|(k, _)| k)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|key| {
                let Worker { tx, handle, .. } = self.workers.remove(&key).unwrap();

                // Close channel, worker should stop almost immediately
                // (it's been supposedly waiting on the channel)
                drop(tx);

                handle
            });

        for handle in handles {
            // We must wait for worker to stop anyway, even though it should stop
            // immediately. This helps in case if we've checked that the worker
            // is waiting in between it received the update and set the flag.
            let _ = handle.await;
        }
    }

    /// Returns a shutdown token, which can later be used to
    /// [`ShutdownToken::shutdown`].
    pub fn shutdown_token(&self) -> ShutdownToken {
        self.state.clone()
    }
}

impl<R, Err, Key> Dispatcher<R, Err, Key> {
    #[cfg(feature = "ctrlc_handler")]
    fn setup_ctrlc_handler_inner(&mut self) {
        let token = self.state.clone();
        tokio::spawn(async move {
            loop {
                tokio::signal::ctrl_c().await.expect("Failed to listen for ^C");

                match token.shutdown() {
                    Ok(f) => {
                        log::info!("^C received, trying to shutdown the dispatcher...");
                        f.await;
                        log::info!("dispatcher is shutdown...");
                    }
                    Err(_) => {
                        log::info!("^C received, the dispatcher isn't running, ignoring the signal")
                    }
                }
            }
        });
    }
}

fn spawn_worker<Err>(
    deps: DependencyMap,
    handler: Arc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Arc<dyn ErrorHandler<Err> + Send + Sync>,
    current_number_of_active_workers: Arc<AtomicU32>,
    max_number_of_active_workers: Arc<AtomicU32>,
    queue_size: usize,
) -> Worker
where
    Err: Send + Sync + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel(queue_size);
    let is_waiting = Arc::new(AtomicBool::new(true));
    let is_waiting_local = Arc::clone(&is_waiting);

    let deps = Arc::new(deps);

    let handle = tokio::spawn(async move {
        while let Some(update) = rx.recv().await {
            is_waiting_local.store(false, Ordering::Relaxed);
            {
                let current = current_number_of_active_workers.fetch_add(1, Ordering::Relaxed) + 1;
                max_number_of_active_workers.fetch_max(current, Ordering::Relaxed);
            }

            let deps = Arc::clone(&deps);
            let handler = Arc::clone(&handler);
            let default_handler = Arc::clone(&default_handler);
            let error_handler = Arc::clone(&error_handler);

            handle_update(update, deps, handler, default_handler, error_handler).await;

            current_number_of_active_workers.fetch_sub(1, Ordering::Relaxed);
            is_waiting_local.store(true, Ordering::Relaxed);
        }
    });

    Worker { tx, handle, is_waiting }
}

fn spawn_default_worker<Err>(
    deps: DependencyMap,
    handler: Arc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Arc<dyn ErrorHandler<Err> + Send + Sync>,
    queue_size: usize,
) -> Worker
where
    Err: Send + Sync + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(queue_size);

    let deps = Arc::new(deps);

    let handle = tokio::spawn(ReceiverStream::new(rx).for_each_concurrent(None, move |update| {
        let deps = Arc::clone(&deps);
        let handler = Arc::clone(&handler);
        let default_handler = Arc::clone(&default_handler);
        let error_handler = Arc::clone(&error_handler);

        handle_update(update, deps, handler, default_handler, error_handler)
    }));

    Worker { tx, handle, is_waiting: Arc::new(AtomicBool::new(true)) }
}

async fn handle_update<Err>(
    update: Update,
    deps: Arc<DependencyMap>,
    handler: Arc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Arc<dyn ErrorHandler<Err> + Send + Sync>,
) where
    Err: Send + Sync + 'static,
{
    let mut deps = deps.deref().clone();
    deps.insert(update);

    match handler.dispatch(deps).await {
        ControlFlow::Break(Ok(())) => {}
        ControlFlow::Break(Err(err)) => error_handler.clone().handle_error(err).await,
        ControlFlow::Continue(deps) => {
            let update = deps.get();
            (default_handler)(update).await;
        }
    }
}

/// The reason a dispatcher worker task stopped.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum WorkerTermination {
    /// The worker task panicked while processing an update.
    Panicked { message: Option<String> },
    /// The worker task was aborted, for example because the Tokio runtime was
    /// shut down.
    Cancelled,
    /// The worker task finished without an error.
    Finished,
}

impl std::fmt::Display for WorkerTermination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Panicked { message: Some(message) } => write!(f, "panicked: {message}"),
            Self::Panicked { message: None } => f.write_str("panicked"),
            Self::Cancelled => f.write_str("was cancelled"),
            Self::Finished => f.write_str("finished without an error"),
        }
    }
}

/// An error reported by the dispatcher when an update cannot be delivered to
/// a worker: the original worker task died, a fresh worker was spawned, but
/// it also stopped before accepting the update.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerDispatchError {
    /// The update that could not be delivered and was dropped.
    pub update: Update,
    /// The reason the worker task stopped.
    pub termination: WorkerTermination,
}

/// Awaits a terminated worker task and returns the reason it stopped.
async fn await_worker_termination(handle: tokio::task::JoinHandle<()>) -> WorkerTermination {
    match handle.await {
        Ok(()) => WorkerTermination::Finished,
        Err(err) if err.is_panic() => {
            let payload = err.into_panic();
            let message = payload
                .downcast_ref::<&'static str>()
                .map(|message| (*message).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned());
            WorkerTermination::Panicked { message }
        }
        Err(_) => WorkerTermination::Cancelled,
    }
}

fn either<L, R>(x: future::Either<L, R>) -> Either<L, R> {
    match x {
        future::Either::Left(l) => Either::Left(l),
        future::Either::Right(r) => Either::Right(r),
    }
}
#[cfg(test)]
mod tests {
    use std::{convert::Infallible, sync::Mutex};

    use teloxide_core::Bot;

    use crate::types::UpdateId;

    use super::*;

    fn update(id: u32) -> Update {
        serde_json::from_value(serde_json::json!({
            "update_id": id,
            "message": {
                "message_id": id,
                "date": 0,
                "chat": { "id": 100, "type": "private", "first_name": "Chat" },
                "from": { "id": 200, "is_bot": false, "first_name": "User" },
                "text": "hello"
            }
        }))
        .expect("the fixture update must deserialize")
    }

    /// A handler that records every incoming update and then panics, which
    /// kills the worker task that processed the update.
    fn panicking_handler(seen_updates: Arc<Mutex<Vec<UpdateId>>>) -> UpdateHandler<Infallible> {
        dptree::entry().endpoint(dptree::di::Asyncify({
            let seen_updates = Arc::clone(&seen_updates);
            move |update: Update| -> Result<(), Infallible> {
                seen_updates.lock().expect("test mutex is not poisoned").push(update.id);
                panic!("test: worker task panic");
            }
        }))
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        for _ in 0..1000 {
            if condition() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the condition was not satisfied in time");
    }

    #[tokio::test]
    async fn test_tokio_spawn() {
        tokio::spawn(async {
            // Just check that this code compiles.
            if false {
                Dispatcher::<_, Infallible, _>::builder(Bot::new(""), dptree::entry())
                    .build()
                    .dispatch()
                    .await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn worker_panic_is_contained_worker_is_respawned_and_dispatch_is_retried() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));
        let policy_errors = Arc::new(Mutex::new(Vec::new()));

        let policy: Arc<dyn ErrorHandler<WorkerDispatchError> + Send + Sync> = {
            let policy_errors = Arc::clone(&policy_errors);
            Arc::new(move |error: WorkerDispatchError| {
                let policy_errors = Arc::clone(&policy_errors);
                async move { policy_errors.lock().expect("test mutex is not poisoned").push(error) }
            })
        };

        let mut dispatcher =
            Dispatcher::builder(Bot::new("test"), panicking_handler(Arc::clone(&seen_updates)))
                .worker_error_handler(policy)
                .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        let update_1 = update(1);
        let key = default_distribution_function(&update_1).expect("the fixture has a chat");
        dispatcher
            .process_update::<Infallible, _>(Ok(update_1.clone()), &listener_error_handler)
            .await;

        // The update was accepted and the worker task died while processing it.
        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 1).await;
        wait_until(|| {
            dispatcher
                .workers
                .get(&key)
                .expect("the dead worker is still in the map")
                .handle
                .is_finished()
        })
        .await;
        assert!(policy_errors.lock().expect("test mutex is not poisoned").is_empty());

        let dead_worker_id =
            dispatcher.workers.get(&key).expect("the dead worker is still in the map").handle.id();

        // The next update hits the closed channel: the dispatcher must respawn
        // the worker and retry the dispatch once instead of panicking.
        dispatcher.process_update::<Infallible, _>(Ok(update(2)), &listener_error_handler).await;

        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 2).await;
        assert!(policy_errors.lock().expect("test mutex is not poisoned").is_empty());

        let respawned_worker_id =
            dispatcher.workers.get(&key).expect("the respawned worker exists").handle.id();
        assert_ne!(dead_worker_id, respawned_worker_id);
    }

    #[tokio::test]
    async fn worker_termination_extracts_panic_message() {
        let handle = tokio::spawn(async { panic!("worker task panicked") });
        let termination = await_worker_termination(handle).await;
        assert_eq!(
            termination,
            WorkerTermination::Panicked { message: Some("worker task panicked".to_owned()) }
        );
    }

    #[tokio::test]
    async fn worker_termination_reports_cancellation() {
        let handle = tokio::spawn(async { std::future::pending::<()>().await });
        handle.abort();
        let termination = await_worker_termination(handle).await;
        assert!(matches!(termination, WorkerTermination::Cancelled));
    }

    #[tokio::test]
    async fn worker_termination_reports_clean_finish() {
        let handle = tokio::spawn(async {});
        let termination = await_worker_termination(handle).await;
        assert!(matches!(termination, WorkerTermination::Finished));
    }
}
