use std::{
    any::{type_name, Any},
    marker::PhantomData,
};

use serde::de::DeserializeOwned;

use crate::{job, subscriber::Subscriber};

pub struct Builder<S = ()> {
    inner: Subscriber,
    _state: PhantomData<S>,
}

impl Builder<()> {
    pub(super) fn new() -> Self {
        Self {
            inner: Subscriber::new(),
            _state: PhantomData,
        }
    }

    /// Returns a fully-configured and valid [`Subscriber`].
    pub fn build(self) -> Subscriber {
        self.inner
    }

    /// Registers the given state associated with its type `S` and runs the
    /// given builder closure which allows the registration of jobs with state
    /// of type `S`.
    ///
    /// # Panics
    ///
    /// Panics if already registered a state of type `S`.
    pub fn with_state<S, B>(mut self, state: &S, builder_fn: B) -> Builder<()>
    where
        S: Clone,
        S: Send + Sync + 'static,
        B: FnOnce(Builder<S>) -> Builder<S>,
    {
        let duplicate = self.inner.job_registry.register_state::<S>(state.clone());
        if duplicate {
            let ty = type_name::<S>();
            panic!("already registered state with type {ty:?}");
        }
        cast(builder_fn(cast(self)))
    }
}

impl<S> Builder<S> {
    /// Registers the given job of type `J`.
    ///
    /// # Panics
    ///
    /// - If already registered a job of name `J::NAME`.
    /// - If the provided configuration is invalid as per [`validate`].
    ///
    /// [`validate`]: crate::job::Config::validate
    pub fn register<J>(mut self) -> Builder<S>
    where
        J: job::Job<State = S>,
        J: DeserializeOwned,
        J: Send + Sync + 'static,
        J::State: Clone + Any,
        J::State: Sync + Send + 'static,
    {
        let name = J::NAME;

        let config = J::config();
        if let Err(msg) = config.validate() {
            println!("invalid configuration for job {name:?}: {msg}");
        }

        let duplicate = self.inner.job_registry.register::<J>();
        if duplicate {
            panic!("already registered job with name {name:?}");
        }
        self.inner.queue_names.insert(config.queue);

        self
    }
}

#[inline(always)]
fn cast<A, B>(this: Builder<A>) -> Builder<B> {
    Builder {
        inner: this.inner,
        _state: PhantomData,
    }
}
