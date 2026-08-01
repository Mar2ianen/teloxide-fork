use std::sync::Mutex;

use tokio::time::Instant;

use super::{DraftPushError, DraftRevision};

/// A state machine that turns producer updates into a fresh preview.
pub trait DraftAccumulator: Send + 'static {
    type Update: Send + 'static;
    type Preview: Send + 'static;

    fn apply(&mut self, update: Self::Update);
    fn snapshot(&self) -> Option<Self::Preview>;
}

/// An owned snapshot produced by a source after a permit was acquired.
#[derive(Debug)]
pub struct PreviewSnapshot<P> {
    pub revision: DraftRevision,
    pub preview: P,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceGate {
    Running,
    Transition,
    Closed,
}

/// Internal source contract shared by snapshot and accumulator modes.
pub trait PreviewSource: Send + Sync + 'static {
    type Update: Send + 'static;
    type Preview: Send + 'static;

    fn apply(&self, update: Self::Update) -> Result<DraftRevision, DraftPushError>;
    fn snapshot(&self) -> Option<PreviewSnapshot<Self::Preview>>;
    fn current_revision(&self) -> DraftRevision;
    fn dirty_since(&self) -> Option<Instant>;
    fn mark_delivered(&self, revision: DraftRevision);
    fn begin_transition(&self);
    fn reopen_segment(&self);
    fn close(&self);
    fn is_running(&self) -> bool;
}

struct SnapshotState<P> {
    gate: SourceGate,
    revision: DraftRevision,
    preview: Option<P>,
    dirty_since: Option<Instant>,
}

/// Replace-the-latest source used by [`DraftSink::update`](super::DraftSink::update).
pub struct ReplacePreview<P> {
    state: Mutex<SnapshotState<P>>,
}

impl<P> ReplacePreview<P> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SnapshotState {
                gate: SourceGate::Running,
                revision: DraftRevision::default(),
                preview: None,
                dirty_since: None,
            }),
        }
    }
}

impl<P> Default for ReplacePreview<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: Clone + Send + 'static> PreviewSource for ReplacePreview<P> {
    type Update = P;
    type Preview = P;

    fn apply(&self, update: Self::Update) -> Result<DraftRevision, DraftPushError> {
        let mut state = self.state.lock().expect("drafter source mutex poisoned");
        match state.gate {
            SourceGate::Running => {}
            SourceGate::Transition => return Err(DraftPushError::ClosedForTransition),
            SourceGate::Closed => return Err(DraftPushError::Closed),
        }
        state.revision.0 = state.revision.0.saturating_add(1);
        if state.dirty_since.is_none() {
            state.dirty_since = Some(Instant::now());
        }
        state.preview = Some(update);
        Ok(state.revision)
    }

    fn snapshot(&self) -> Option<PreviewSnapshot<Self::Preview>> {
        let state = self.state.lock().expect("drafter source mutex poisoned");
        Some(PreviewSnapshot { revision: state.revision, preview: state.preview.clone()? })
    }

    fn current_revision(&self) -> DraftRevision {
        self.state.lock().expect("drafter source mutex poisoned").revision
    }

    fn dirty_since(&self) -> Option<Instant> {
        self.state.lock().expect("drafter source mutex poisoned").dirty_since
    }

    fn mark_delivered(&self, revision: DraftRevision) {
        let mut state = self.state.lock().expect("drafter source mutex poisoned");
        if state.revision <= revision {
            state.dirty_since = None;
        }
    }

    fn begin_transition(&self) {
        let mut state = self.state.lock().expect("drafter source mutex poisoned");
        if state.gate == SourceGate::Running {
            state.gate = SourceGate::Transition;
        }
    }

    fn reopen_segment(&self) {
        let mut state = self.state.lock().expect("drafter source mutex poisoned");
        state.gate = SourceGate::Running;
        state.revision = DraftRevision::default();
        state.preview = None;
        state.dirty_since = None;
    }

    fn close(&self) {
        self.state.lock().expect("drafter source mutex poisoned").gate = SourceGate::Closed;
    }

    fn is_running(&self) -> bool {
        self.state.lock().expect("drafter source mutex poisoned").gate == SourceGate::Running
    }
}

struct AccumulatorState<A> {
    gate: SourceGate,
    revision: DraftRevision,
    accumulator: A,
    dirty_since: Option<Instant>,
}

/// Source backed by a user-owned semantic accumulator.
pub struct AccumulatorSource<A: DraftAccumulator> {
    state: Mutex<AccumulatorState<A>>,
}

impl<A: DraftAccumulator> AccumulatorSource<A> {
    #[must_use]
    pub fn new(accumulator: A) -> Self {
        Self {
            state: Mutex::new(AccumulatorState {
                gate: SourceGate::Running,
                revision: DraftRevision::default(),
                accumulator,
                dirty_since: None,
            }),
        }
    }
}

impl<A: DraftAccumulator> PreviewSource for AccumulatorSource<A> {
    type Update = A::Update;
    type Preview = A::Preview;

    fn apply(&self, update: Self::Update) -> Result<DraftRevision, DraftPushError> {
        let mut state = self.state.lock().expect("drafter accumulator mutex poisoned");
        match state.gate {
            SourceGate::Running => {}
            SourceGate::Transition => return Err(DraftPushError::ClosedForTransition),
            SourceGate::Closed => return Err(DraftPushError::Closed),
        }
        state.accumulator.apply(update);
        state.revision.0 = state.revision.0.saturating_add(1);
        if state.dirty_since.is_none() {
            state.dirty_since = Some(Instant::now());
        }
        Ok(state.revision)
    }

    fn snapshot(&self) -> Option<PreviewSnapshot<Self::Preview>> {
        let state = self.state.lock().expect("drafter accumulator mutex poisoned");
        Some(PreviewSnapshot { revision: state.revision, preview: state.accumulator.snapshot()? })
    }

    fn current_revision(&self) -> DraftRevision {
        self.state.lock().expect("drafter accumulator mutex poisoned").revision
    }

    fn dirty_since(&self) -> Option<Instant> {
        self.state.lock().expect("drafter accumulator mutex poisoned").dirty_since
    }

    fn mark_delivered(&self, revision: DraftRevision) {
        let mut state = self.state.lock().expect("drafter accumulator mutex poisoned");
        if state.revision <= revision {
            state.dirty_since = None;
        }
    }

    fn begin_transition(&self) {
        let mut state = self.state.lock().expect("drafter accumulator mutex poisoned");
        if state.gate == SourceGate::Running {
            state.gate = SourceGate::Transition;
        }
    }

    fn reopen_segment(&self) {
        let mut state = self.state.lock().expect("drafter accumulator mutex poisoned");
        state.gate = SourceGate::Running;
        state.revision = DraftRevision::default();
        state.dirty_since = None;
    }

    fn close(&self) {
        self.state.lock().expect("drafter accumulator mutex poisoned").gate = SourceGate::Closed;
    }

    fn is_running(&self) -> bool {
        self.state.lock().expect("drafter accumulator mutex poisoned").gate == SourceGate::Running
    }
}
