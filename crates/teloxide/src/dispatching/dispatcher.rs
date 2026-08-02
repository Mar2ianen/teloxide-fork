use crate::{
    dispatching::{
        distribution::default_distribution_function, DefaultKey, DpHandlerDescription,
        ShutdownToken,
    },
    error_handlers::{ErrorHandler, LoggingErrorHandler},
    requests::{Request, Requester},
    stop::StopToken,
    types::{Update, UpdateId, UpdateKind},
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
    any::Any,
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    ops::{ControlFlow, Deref},
    panic::AssertUnwindSafe,
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
    worker_error_handler: Arc<dyn ErrorHandler<WorkerError> + Send + Sync>,
    ctrlc_handler: bool,
    distribution_f: fn(&Update) -> Option<Key>,
    worker_queue_size: usize,
}

impl<R, Err, Key> DispatcherBuilder<R, Err, Key>
where
    R: Clone + Requester + Send + Sync + 'static,
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

    /// Specifies a handler that will be called when the dispatcher detects an
    /// abnormal event in its worker infrastructure.
    ///
    /// The following events are reported: a handler panic contained inside a
    /// worker, an abnormal termination of a worker task, and an update that
    /// could not be delivered to any worker. Abnormal worker terminations
    /// observed during shutdown are only logged.
    ///
    /// The handler itself is invoked from a panic-safe boundary: a panic
    /// inside it is contained and logged instead of killing a worker or the
    /// dispatcher.
    ///
    /// By default, it is [`LoggingErrorHandler`].
    #[must_use]
    pub fn worker_error_handler(
        self,
        handler: Arc<dyn ErrorHandler<WorkerError> + Send + Sync>,
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
            #[cfg(test)]
            worker_factory: None,
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
    worker_error_handler: Arc<dyn ErrorHandler<WorkerError> + Send + Sync>,
    #[cfg(test)]
    worker_factory: Option<fn(WorkerDeps<Err>) -> Worker>,

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

        self.await_workers_shutdown().await;

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
            Err((update, first_termination)) => {
                // The worker task died; the retry below spawns a fresh worker.
                match self.try_dispatch_update(key, update).await {
                    Ok(()) => {
                        // The fresh worker accepted the update; report the
                        // death of the original worker through the dispatcher
                        // error policy.
                        report_worker_error(
                            &self.worker_error_handler,
                            WorkerError::WorkerTerminated { termination: first_termination },
                        )
                        .await;
                    }
                    Err((update, retry_termination)) => {
                        // The replacement worker also stopped before accepting
                        // the update. Report the undeliverable update through
                        // the dispatcher error policy instead of panicking.
                        report_worker_error(
                            &self.worker_error_handler,
                            WorkerError::UpdateUndeliverable {
                                update: Box::new(update),
                                first_termination,
                                retry_termination,
                            },
                        )
                        .await;
                    }
                }
            }
        }
    }

    /// Sends an update to the worker responsible for `key`, spawning the worker
    /// on demand.
    ///
    /// If the worker task died (its channel is closed), the update is returned
    /// back along with the reason the task stopped, so that the caller can
    /// spawn a fresh worker and retry the dispatch.
    async fn try_dispatch_update(
        &mut self,
        key: Option<Key>,
        update: Update,
    ) -> Result<(), (Update, WorkerTermination)> {
        let worker = match key {
            Some(ref key) => {
                if !self.workers.contains_key(key) {
                    self.workers.insert(key.clone(), self.new_worker());
                }
                self.workers.remove(key).expect("worker was inserted just above")
            }
            None => {
                if self.default_worker.is_none() {
                    self.default_worker = Some(self.new_default_worker());
                }
                self.default_worker.take().expect("worker was inserted just above")
            }
        };

        match worker.tx.send(update).await {
            Ok(()) => {
                // Put the worker back so that updates for the same key keep
                // their order.
                match key {
                    Some(key) => {
                        self.workers.insert(key, worker);
                    }
                    None => {
                        self.default_worker = Some(worker);
                    }
                }
                Ok(())
            }
            Err(SendError(update)) => {
                // The channel is closed, which means the worker task has
                // terminated. Learn why it stopped so that the caller can
                // spawn a fresh worker and retry the dispatch.
                let termination = await_worker_termination(worker.handle).await;
                Err((update, termination))
            }
        }
    }

    fn new_worker(&self) -> Worker {
        #[cfg(test)]
        if let Some(factory) = self.worker_factory {
            return factory(self.worker_deps());
        }

        spawn_worker(
            self.worker_deps(),
            Arc::clone(&self.current_number_of_active_workers),
            Arc::clone(&self.max_number_of_active_workers),
            self.worker_queue_size,
        )
    }

    fn new_default_worker(&self) -> Worker {
        #[cfg(test)]
        if let Some(factory) = self.worker_factory {
            return factory(self.worker_deps());
        }

        spawn_default_worker(self.worker_deps(), self.worker_queue_size)
    }

    fn worker_deps(&self) -> WorkerDeps<Err> {
        WorkerDeps {
            dependencies: Arc::new(self.dependencies.clone()),
            handler: Arc::clone(&self.handler),
            default_handler: Arc::clone(&self.default_handler),
            error_handler: Arc::clone(&self.error_handler),
            worker_error_handler: Arc::clone(&self.worker_error_handler),
        }
    }

    /// Overrides worker spawning in tests so that deterministic failures can
    /// be injected. Not present in production builds.
    #[cfg(test)]
    fn set_worker_factory(&mut self, factory: fn(WorkerDeps<Err>) -> Worker) {
        self.worker_factory = Some(factory);
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

    /// Awaits the termination of all workers, logging abnormal terminations
    /// instead of panicking.
    async fn await_workers_shutdown(&mut self) {
        let handles = self
            .workers
            .drain()
            .map(|(_chat_id, worker)| worker.handle)
            .chain(self.default_worker.take().map(|worker| worker.handle))
            .collect::<FuturesUnordered<_>>();

        handles
            .for_each(|result| async {
                match worker_termination_from_result(result) {
                    WorkerTermination::Finished => {}
                    termination => {
                        log::error!(
                            "A worker task ended abnormally during shutdown: {termination}"
                        );
                    }
                }
            })
            .await;
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

/// The handler-related dependencies shared by dispatcher workers.
struct WorkerDeps<Err> {
    dependencies: Arc<DependencyMap>,
    handler: Arc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Arc<dyn ErrorHandler<Err> + Send + Sync>,
    worker_error_handler: Arc<dyn ErrorHandler<WorkerError> + Send + Sync>,
}

fn spawn_worker<Err>(
    deps: WorkerDeps<Err>,
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
            let _active_worker = ActiveWorkerGuard::new(
                &current_number_of_active_workers,
                &max_number_of_active_workers,
            );

            let deps = Arc::clone(&deps);
            let handler = Arc::clone(&deps.handler);
            let default_handler = Arc::clone(&deps.default_handler);
            let error_handler = Arc::clone(&deps.error_handler);
            let worker_error_handler = Arc::clone(&deps.worker_error_handler);

            // Catch panics per update so that a panicking handler cannot
            // terminate the worker task together with the queued updates.
            handle_update_catching_panics(
                update,
                Arc::clone(&deps.dependencies),
                handler,
                default_handler,
                error_handler,
                worker_error_handler,
            )
            .await;

            is_waiting_local.store(true, Ordering::Relaxed);
        }
    });

    Worker { tx, handle, is_waiting }
}

fn spawn_default_worker<Err>(deps: WorkerDeps<Err>, queue_size: usize) -> Worker
where
    Err: Send + Sync + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(queue_size);

    let deps = Arc::new(deps);

    let handle = tokio::spawn(ReceiverStream::new(rx).for_each_concurrent(None, move |update| {
        let deps = Arc::clone(&deps);
        let handler = Arc::clone(&deps.handler);
        let default_handler = Arc::clone(&deps.default_handler);
        let error_handler = Arc::clone(&deps.error_handler);
        let worker_error_handler = Arc::clone(&deps.worker_error_handler);

        async move {
            // Catch panics per update so that a panicking handler cannot
            // cancel the sibling updates processed concurrently by this
            // worker.
            handle_update_catching_panics(
                update,
                Arc::clone(&deps.dependencies),
                handler,
                default_handler,
                error_handler,
                worker_error_handler,
            )
            .await;
        }
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

/// Runs [`handle_update`], containing any handler panic inside the worker
/// task and reporting it through the dispatcher error policy.
async fn handle_update_catching_panics<Err>(
    update: Update,
    deps: Arc<DependencyMap>,
    handler: Arc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Arc<dyn ErrorHandler<Err> + Send + Sync>,
    worker_error_handler: Arc<dyn ErrorHandler<WorkerError> + Send + Sync>,
) where
    Err: Send + Sync + 'static,
{
    let update_id = update.id;

    let result =
        AssertUnwindSafe(handle_update(update, deps, handler, default_handler, error_handler))
            .catch_unwind()
            .await;

    if let Err(payload) = result {
        let message = panic_message(&*payload);
        report_worker_error(
            &worker_error_handler,
            WorkerError::HandlerPanicked { update_id, message },
        )
        .await;
    }
}

/// Reports an abnormal worker event through the dispatcher error policy,
/// containing any panic raised by the policy itself so that it cannot kill a
/// worker or the dispatcher.
async fn report_worker_error(
    worker_error_handler: &Arc<dyn ErrorHandler<WorkerError> + Send + Sync>,
    error: WorkerError,
) {
    let error_message = error.to_string();
    let handler = Arc::clone(worker_error_handler);

    let result =
        AssertUnwindSafe(async move { handler.handle_error(error).await }).catch_unwind().await;

    if let Err(payload) = result {
        let message = panic_message(&*payload);
        log::error!(
            "The dispatcher worker error handler panicked while handling \"{error_message}\": {}",
            message.as_deref().unwrap_or("unknown panic payload")
        );
    }
}

/// An RAII guard tracking the number of worker tasks currently processing an
/// update. The counter is decremented on drop, including during unwinding.
struct ActiveWorkerGuard {
    current: Arc<AtomicU32>,
}

impl ActiveWorkerGuard {
    fn new(current: &Arc<AtomicU32>, max: &Arc<AtomicU32>) -> Self {
        let count = current.fetch_add(1, Ordering::Relaxed) + 1;
        max.fetch_max(count, Ordering::Relaxed);
        Self { current: Arc::clone(current) }
    }
}

impl Drop for ActiveWorkerGuard {
    fn drop(&mut self) {
        self.current.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Extracts the message of a panic payload.
fn panic_message(payload: &(dyn Any + Send)) -> Option<String> {
    payload
        .downcast_ref::<&'static str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
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

/// An abnormal event detected by the dispatcher around its worker
/// infrastructure.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkerError {
    /// A handler panic was contained inside a worker; the worker keeps
    /// processing the queued updates.
    HandlerPanicked {
        /// The update whose handler panicked.
        update_id: UpdateId,
        /// The panic message, if it could be extracted from the payload.
        message: Option<String>,
    },
    /// A worker task terminated abnormally; the dispatcher spawned a fresh
    /// worker to keep dispatching.
    WorkerTerminated {
        /// The reason the worker task stopped.
        termination: WorkerTermination,
    },
    /// An update could not be delivered to any worker: the original worker
    /// task died and the freshly spawned replacement also stopped before
    /// accepting the update.
    UpdateUndeliverable {
        /// The update that could not be delivered and was dropped.
        update: Box<Update>,
        /// The reason the original worker task stopped.
        first_termination: WorkerTermination,
        /// The reason the replacement worker task stopped.
        retry_termination: WorkerTermination,
    },
}

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HandlerPanicked { update_id, message } => write!(
                f,
                "a handler panicked while processing update {update_id:?}: {}",
                message.as_deref().unwrap_or("unknown panic payload")
            ),
            Self::WorkerTerminated { termination } => {
                write!(f, "a worker task terminated abnormally: {termination}")
            }
            Self::UpdateUndeliverable { update, first_termination, retry_termination } => write!(
                f,
                "cannot deliver update {update_id:?}: the worker task {first_termination} and the \
                 replacement {retry_termination}",
                update_id = update.id,
            ),
        }
    }
}

impl std::error::Error for WorkerError {}

/// Awaits a terminated worker task and returns the reason it stopped.
async fn await_worker_termination(handle: tokio::task::JoinHandle<()>) -> WorkerTermination {
    worker_termination_from_result(handle.await)
}

/// Classifies the outcome of an awaited worker task.
fn worker_termination_from_result(result: Result<(), tokio::task::JoinError>) -> WorkerTermination {
    match result {
        Ok(()) => WorkerTermination::Finished,
        Err(err) if err.is_panic() => {
            WorkerTermination::Panicked { message: panic_message(&*err.into_panic()) }
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
    use tokio::sync::Notify;

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

    /// A handler that records every incoming update and then panics.
    fn panicking_handler(seen_updates: Arc<Mutex<Vec<UpdateId>>>) -> UpdateHandler<Infallible> {
        dptree::entry().endpoint(dptree::di::Asyncify({
            let seen_updates = Arc::clone(&seen_updates);
            move |update: Update| -> Result<(), Infallible> {
                seen_updates.lock().expect("test mutex is not poisoned").push(update.id);
                panic!("test: worker task panic");
            }
        }))
    }

    /// A handler that records every incoming update and completes normally.
    fn recording_handler(seen_updates: Arc<Mutex<Vec<UpdateId>>>) -> UpdateHandler<Infallible> {
        dptree::entry().endpoint(dptree::di::Asyncify({
            let seen_updates = Arc::clone(&seen_updates);
            move |update: Update| -> Result<(), Infallible> {
                seen_updates.lock().expect("test mutex is not poisoned").push(update.id);
                Ok(())
            }
        }))
    }

    /// A handler whose first invocation waits on `release` (signalling
    /// `started` first) and then panics; the rest of the invocations record
    /// the update and complete normally.
    fn blocking_then_panicking_handler(
        seen_updates: Arc<Mutex<Vec<UpdateId>>>,
        started: Arc<AtomicBool>,
        release: Arc<Notify>,
    ) -> UpdateHandler<Infallible> {
        let first_call = Arc::new(AtomicBool::new(true));
        dptree::entry().endpoint(move |update: Update| {
            let first_call = Arc::clone(&first_call);
            let seen_updates = Arc::clone(&seen_updates);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                if first_call.swap(false, Ordering::Relaxed) {
                    started.store(true, Ordering::Relaxed);
                    release.notified().await;
                    seen_updates.lock().expect("test mutex is not poisoned").push(update.id);
                    panic!("test: worker task panic");
                }
                seen_updates.lock().expect("test mutex is not poisoned").push(update.id);
                Ok(())
            }
        })
    }

    /// A handler that blocks update 1 on `release` (signalling `started`
    /// first), makes update 2 panic and completes update 3, exercising the
    /// concurrent processing of the default worker.
    fn concurrent_handler(
        seen_updates: Arc<Mutex<Vec<UpdateId>>>,
        started: Arc<AtomicBool>,
        release: Arc<Notify>,
    ) -> UpdateHandler<Infallible> {
        dptree::entry().endpoint(move |update: Update| {
            let seen_updates = Arc::clone(&seen_updates);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                match update.id.0 {
                    1 => {
                        started.store(true, Ordering::Relaxed);
                        release.notified().await;
                        seen_updates.lock().expect("test mutex is not poisoned").push(update.id);
                        Ok(())
                    }
                    2 => {
                        seen_updates.lock().expect("test mutex is not poisoned").push(update.id);
                        panic!("test: default worker sibling panic");
                    }
                    _ => {
                        seen_updates.lock().expect("test mutex is not poisoned").push(update.id);
                        Ok(())
                    }
                }
            }
        })
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

    /// A policy that records the reported errors.
    fn test_policy(
        policy_errors: &Arc<Mutex<Vec<WorkerError>>>,
    ) -> Arc<dyn ErrorHandler<WorkerError> + Send + Sync> {
        let policy_errors = Arc::clone(policy_errors);
        Arc::new(move |error: WorkerError| {
            let policy_errors = Arc::clone(&policy_errors);
            async move { policy_errors.lock().expect("test mutex is not poisoned").push(error) }
        })
    }

    /// A policy that panics on every reported error.
    fn panicking_policy() -> Arc<dyn ErrorHandler<WorkerError> + Send + Sync> {
        Arc::new(|_error: WorkerError| async move { panic!("test: worker error policy panic") })
    }

    /// A handler that signals `started` and then blocks on `release`.
    fn blocking_handler(
        started: Arc<AtomicBool>,
        release: Arc<Notify>,
    ) -> UpdateHandler<Infallible> {
        dptree::entry().endpoint(move |_update: Update| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                started.store(true, Ordering::Relaxed);
                release.notified().await;
                Ok(())
            }
        })
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
    async fn handler_panic_is_contained_and_worker_keeps_processing() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));
        let policy_errors = Arc::new(Mutex::new(Vec::new()));

        let mut dispatcher =
            Dispatcher::builder(Bot::new("test"), panicking_handler(Arc::clone(&seen_updates)))
                .worker_error_handler(test_policy(&policy_errors))
                .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        let update_1 = update(1);
        let key = default_distribution_function(&update_1).expect("the fixture has a chat");
        dispatcher.process_update::<Infallible, _>(Ok(update_1), &listener_error_handler).await;

        let worker_id = dispatcher.workers.get(&key).expect("worker exists").handle.id();

        for id in [2, 3] {
            dispatcher
                .process_update::<Infallible, _>(Ok(update(id)), &listener_error_handler)
                .await;
        }

        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 3).await;
        wait_until(|| policy_errors.lock().expect("test mutex is not poisoned").len() == 3).await;
        wait_until(|| dispatcher.current_number_of_active_workers.load(Ordering::Relaxed) == 0)
            .await;

        // The same worker processed all three updates: the panics were
        // contained instead of terminating the worker task.
        assert_eq!(dispatcher.workers.get(&key).expect("worker exists").handle.id(), worker_id);
        assert_eq!(
            *seen_updates.lock().expect("test mutex is not poisoned"),
            vec![UpdateId(1), UpdateId(2), UpdateId(3)]
        );
        // Every panic was reported through the dispatcher error policy.
        let policy_errors = policy_errors.lock().expect("test mutex is not poisoned");
        assert_eq!(policy_errors.len(), 3);
        assert!(policy_errors
            .iter()
            .all(|error| matches!(error, WorkerError::HandlerPanicked { .. })));
    }

    #[tokio::test]
    async fn dead_worker_is_respawned_and_dispatch_is_retried() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));
        let policy_errors = Arc::new(Mutex::new(Vec::new()));

        let mut dispatcher =
            Dispatcher::builder(Bot::new("test"), recording_handler(Arc::clone(&seen_updates)))
                .worker_error_handler(test_policy(&policy_errors))
                .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        let update_1 = update(1);
        let key = default_distribution_function(&update_1).expect("the fixture has a chat");
        dispatcher.process_update::<Infallible, _>(Ok(update_1), &listener_error_handler).await;

        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 1).await;
        let dead_worker_id = dispatcher.workers.get(&key).expect("worker exists").handle.id();

        // Kill the worker task; the channel becomes closed.
        dispatcher.workers.get(&key).expect("worker exists").handle.abort();
        wait_until(|| dispatcher.workers.get(&key).expect("worker exists").handle.is_finished())
            .await;

        // The next update hits the closed channel: the dispatcher respawns
        // the worker and retries the dispatch once.
        dispatcher.process_update::<Infallible, _>(Ok(update(2)), &listener_error_handler).await;

        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 2).await;

        assert_eq!(
            *seen_updates.lock().expect("test mutex is not poisoned"),
            vec![UpdateId(1), UpdateId(2)]
        );
        assert_ne!(
            dispatcher.workers.get(&key).expect("worker exists").handle.id(),
            dead_worker_id
        );
        assert_eq!(
            *policy_errors.lock().expect("test mutex is not poisoned"),
            vec![WorkerError::WorkerTerminated { termination: WorkerTermination::Cancelled }]
        );
    }

    #[tokio::test]
    async fn buffered_updates_survive_handler_panic() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));
        let policy_errors = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());

        let mut dispatcher = Dispatcher::builder(
            Bot::new("test"),
            blocking_then_panicking_handler(
                Arc::clone(&seen_updates),
                Arc::clone(&started),
                Arc::clone(&release),
            ),
        )
        .worker_error_handler(test_policy(&policy_errors))
        .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        dispatcher.process_update::<Infallible, _>(Ok(update(1)), &listener_error_handler).await;

        // Wait until the worker is blocked inside the handler of update 1.
        wait_until(|| started.load(Ordering::Relaxed)).await;

        // The worker is busy: updates 2 and 3 sit in the channel buffer.
        dispatcher.process_update::<Infallible, _>(Ok(update(2)), &listener_error_handler).await;
        dispatcher.process_update::<Infallible, _>(Ok(update(3)), &listener_error_handler).await;

        // Let the handler of update 1 panic: the buffered updates must not be
        // lost, and the processing order must be preserved.
        release.notify_one();
        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 3).await;

        assert_eq!(
            *seen_updates.lock().expect("test mutex is not poisoned"),
            vec![UpdateId(1), UpdateId(2), UpdateId(3)]
        );
        assert_eq!(policy_errors.lock().expect("test mutex is not poisoned").len(), 1);
    }

    #[tokio::test]
    async fn default_worker_panic_does_not_cancel_siblings() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));
        let policy_errors = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());

        let mut dispatcher = Dispatcher::builder(
            Bot::new("test"),
            concurrent_handler(
                Arc::clone(&seen_updates),
                Arc::clone(&started),
                Arc::clone(&release),
            ),
        )
        .distribution_function(|_| None::<()>)
        .worker_error_handler(test_policy(&policy_errors))
        .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        dispatcher.process_update::<Infallible, _>(Ok(update(1)), &listener_error_handler).await;
        wait_until(|| started.load(Ordering::Relaxed)).await;

        // Updates 2 and 3 are processed concurrently with the blocked update 1.
        dispatcher.process_update::<Infallible, _>(Ok(update(2)), &listener_error_handler).await;
        dispatcher.process_update::<Infallible, _>(Ok(update(3)), &listener_error_handler).await;
        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 2).await;

        release.notify_one();
        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 3).await;

        // Update 2 panicked, but update 3 completed: the panic did not cancel
        // the sibling updates.
        let mut seen = seen_updates.lock().expect("test mutex is not poisoned").clone();
        seen.sort();
        assert_eq!(seen, vec![UpdateId(1), UpdateId(2), UpdateId(3)]);
        assert_eq!(policy_errors.lock().expect("test mutex is not poisoned").len(), 1);
    }

    #[tokio::test]
    async fn shutdown_with_dead_worker_does_not_panic() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));
        let policy_errors = Arc::new(Mutex::new(Vec::new()));

        let mut dispatcher =
            Dispatcher::builder(Bot::new("test"), recording_handler(Arc::clone(&seen_updates)))
                .worker_error_handler(test_policy(&policy_errors))
                .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        let update_1 = update(1);
        let key = default_distribution_function(&update_1).expect("the fixture has a chat");
        dispatcher.process_update::<Infallible, _>(Ok(update_1), &listener_error_handler).await;

        dispatcher.workers.get(&key).expect("worker exists").handle.abort();
        wait_until(|| dispatcher.workers.get(&key).expect("worker exists").handle.is_finished())
            .await;

        // The dispatcher must not panic while waiting for a dead worker.
        dispatcher.await_workers_shutdown().await;

        assert!(policy_errors.lock().expect("test mutex is not poisoned").is_empty());
    }

    #[tokio::test]
    async fn active_worker_counter_is_balanced_when_worker_is_aborted() {
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());

        let mut dispatcher = Dispatcher::builder(
            Bot::new("test"),
            blocking_handler(Arc::clone(&started), Arc::clone(&release)),
        )
        .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        let update_1 = update(1);
        let key = default_distribution_function(&update_1).expect("the fixture has a chat");
        dispatcher.process_update::<Infallible, _>(Ok(update_1), &listener_error_handler).await;

        // The worker is blocked inside the handler with the guard live.
        wait_until(|| started.load(Ordering::Relaxed)).await;
        assert_eq!(dispatcher.current_number_of_active_workers.load(Ordering::Relaxed), 1);

        // Aborting the worker must drop the guard and balance the counter.
        dispatcher.workers.get(&key).expect("worker exists").handle.abort();
        wait_until(|| dispatcher.workers.get(&key).expect("worker exists").handle.is_finished())
            .await;
        wait_until(|| dispatcher.current_number_of_active_workers.load(Ordering::Relaxed) == 0)
            .await;
    }

    #[tokio::test]
    async fn panicking_worker_error_handler_does_not_kill_keyed_worker() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));

        let mut dispatcher =
            Dispatcher::builder(Bot::new("test"), panicking_handler(Arc::clone(&seen_updates)))
                .worker_error_handler(panicking_policy())
                .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        let update_1 = update(1);
        let key = default_distribution_function(&update_1).expect("the fixture has a chat");
        dispatcher.process_update::<Infallible, _>(Ok(update_1), &listener_error_handler).await;

        let worker_id = dispatcher.workers.get(&key).expect("worker exists").handle.id();

        dispatcher.process_update::<Infallible, _>(Ok(update(2)), &listener_error_handler).await;

        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 2).await;

        // The handler and the error policy panicked for both updates, but the
        // worker survived and kept processing.
        assert_eq!(
            *seen_updates.lock().expect("test mutex is not poisoned"),
            vec![UpdateId(1), UpdateId(2)]
        );
        assert_eq!(dispatcher.workers.get(&key).expect("worker exists").handle.id(), worker_id);
    }

    #[tokio::test]
    async fn panicking_worker_error_handler_does_not_cancel_default_worker_siblings() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());

        let mut dispatcher = Dispatcher::builder(
            Bot::new("test"),
            concurrent_handler(
                Arc::clone(&seen_updates),
                Arc::clone(&started),
                Arc::clone(&release),
            ),
        )
        .distribution_function(|_| None::<()>)
        .worker_error_handler(panicking_policy())
        .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        dispatcher.process_update::<Infallible, _>(Ok(update(1)), &listener_error_handler).await;
        wait_until(|| started.load(Ordering::Relaxed)).await;

        dispatcher.process_update::<Infallible, _>(Ok(update(2)), &listener_error_handler).await;
        dispatcher.process_update::<Infallible, _>(Ok(update(3)), &listener_error_handler).await;
        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 2).await;

        release.notify_one();
        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 3).await;

        // Update 2 panicked and the error policy panicked while reporting it;
        // update 3 still completed.
        let mut seen = seen_updates.lock().expect("test mutex is not poisoned").clone();
        seen.sort();
        assert_eq!(seen, vec![UpdateId(1), UpdateId(2), UpdateId(3)]);
    }

    #[tokio::test]
    async fn panicking_worker_error_handler_does_not_kill_the_dispatcher() {
        let seen_updates = Arc::new(Mutex::new(Vec::new()));

        let mut dispatcher =
            Dispatcher::builder(Bot::new("test"), recording_handler(Arc::clone(&seen_updates)))
                .worker_error_handler(panicking_policy())
                .build();

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        let update_1 = update(1);
        let key = default_distribution_function(&update_1).expect("the fixture has a chat");
        dispatcher.process_update::<Infallible, _>(Ok(update_1), &listener_error_handler).await;

        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 1).await;
        dispatcher.workers.get(&key).expect("worker exists").handle.abort();
        wait_until(|| dispatcher.workers.get(&key).expect("worker exists").handle.is_finished())
            .await;

        // The retry succeeds, then the error policy panics while reporting
        // the dead worker; the dispatcher must survive and finish the update.
        dispatcher.process_update::<Infallible, _>(Ok(update(2)), &listener_error_handler).await;

        wait_until(|| seen_updates.lock().expect("test mutex is not poisoned").len() == 2).await;
        assert_eq!(
            *seen_updates.lock().expect("test mutex is not poisoned"),
            vec![UpdateId(1), UpdateId(2)]
        );
    }

    /// A worker factory that produces workers with an already-closed channel,
    /// simulating a worker task that died right after being spawned.
    fn dead_worker_factory(_deps: WorkerDeps<Infallible>) -> Worker {
        let (tx, rx) = tokio::sync::mpsc::channel::<Update>(1);
        drop(rx);
        let handle = tokio::spawn(async {});
        Worker { tx, handle, is_waiting: Arc::new(AtomicBool::new(false)) }
    }

    #[tokio::test]
    async fn undeliverable_update_is_reported_to_the_error_policy() {
        let policy_errors = Arc::new(Mutex::new(Vec::new()));

        let mut dispatcher =
            Dispatcher::<_, Infallible, _>::builder(Bot::new("test"), dptree::entry())
                .worker_error_handler(test_policy(&policy_errors))
                .build();
        dispatcher.set_worker_factory(dead_worker_factory);

        let listener_error_handler = Arc::new(|error: Infallible| async move { match error {} });

        // Both the original and the replacement worker are dead on arrival, so
        // the update cannot be delivered and must be reported.
        dispatcher.process_update::<Infallible, _>(Ok(update(1)), &listener_error_handler).await;

        let policy_errors = policy_errors.lock().expect("test mutex is not poisoned");
        assert_eq!(policy_errors.len(), 1);
        match &policy_errors[0] {
            WorkerError::UpdateUndeliverable { update, first_termination, retry_termination } => {
                assert_eq!(update.id, UpdateId(1));
                assert_eq!(*first_termination, WorkerTermination::Finished);
                assert_eq!(*retry_termination, WorkerTermination::Finished);
            }
            error => panic!("expected UpdateUndeliverable, got {error:?}"),
        }
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
