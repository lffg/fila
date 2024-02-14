use std::{
    any::{type_name, Any},
    collections::HashSet,
    marker::PhantomData,
};

use serde::de::DeserializeOwned;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::{
    job,
    subscriber::{magic, Subscriber},
};

/// Support shenanigans for the builder pattern.
///
/// "bp" stands for "build phase".
mod bp {
    use std::marker::PhantomData;

    pub struct Jobs<S>(PhantomData<S>);
    pub struct HasJobs;
    pub struct HasPool;

    pub trait GeneralConfiguration {}
    impl GeneralConfiguration for Jobs<()> {}
    impl GeneralConfiguration for HasJobs {}
}

/// A [`Subscriber`] builder, which leverages the Rust's type system to ensure
/// a well-defined ordering of configurations and that all required parameters
/// are indeed provided.
///
/// To construct a builder, use the [`Subscriber::builder`] associated function.
///
/// ### Order of configuration
///
/// 1. Definition of jobs and their corresponding state values, through
///    [`register`] and [`with_state`], respectively.
/// 2. Provide general configurations and required values:
///    - [`with_cancellation_token`](Builder::with_cancellation_token)
///    - [`with_pool`](Builder::with_pool) (required)
/// 3. Build the actual [`Subscriber`], via [`build`](Builder::build).
///
/// [`register`]: Builder::register
/// [`with_state`]: Builder::with_state
pub struct Builder<P = bp::Jobs<()>> {
    job_registry: magic::JobRegistry,
    queue_names: HashSet<&'static str>,
    pool: Option<PgPool>,
    cancel_token: Option<CancellationToken>,
    _phase: PhantomData<P>,
}

impl Builder<bp::Jobs<()>> {
    pub(super) fn new() -> Self {
        Self {
            job_registry: magic::JobRegistry::new(),
            queue_names: HashSet::new(),
            pool: None,
            cancel_token: None,
            _phase: PhantomData,
        }
    }

    /// Registers the given state associated with its type `S` and runs the
    /// given builder closure which allows the registration of jobs with state
    /// of type `S`.
    ///
    /// # Panics
    ///
    /// Panics if already registered a state of type `S`.
    pub fn with_state<S, B>(mut self, state: &S, builder_fn: B) -> Builder<bp::Jobs<()>>
    where
        S: Clone,
        S: Send + Sync + 'static,
        B: FnOnce(Builder<bp::Jobs<S>>) -> Builder<bp::Jobs<S>>,
    {
        let duplicate = self.job_registry.register_state::<S>(state.clone());
        if duplicate {
            let ty = type_name::<S>();
            panic!("already registered state with type {ty:?}");
        }
        cast(builder_fn(cast(self)))
    }
}

impl<S> Builder<bp::Jobs<S>> {
    /// Registers the given job of type `J`.
    ///
    /// # Panics
    ///
    /// - If already registered a job of name `J::NAME`.
    /// - If the provided configuration is invalid as per [`validate`].
    ///
    /// [`validate`]: crate::job::Config::validate
    pub fn register<J>(mut self) -> Builder<bp::Jobs<S>>
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

        let duplicate = self.job_registry.register::<J>();
        if duplicate {
            panic!("already registered job with name {name:?}");
        }
        self.queue_names.insert(config.queue);

        self
    }
}

impl<P: bp::GeneralConfiguration> Builder<P> {
    /// Uses the provided [`CancellationToken`] which later may be used to
    /// perform graceful shutdown of the subscriber-related tasks.
    pub fn with_cancellation_token(
        mut self,
        cancellation_token: CancellationToken,
    ) -> Builder<bp::HasJobs> {
        self.cancel_token = Some(cancellation_token);
        cast(self)
    }

    /// Provides the database pool.
    pub fn with_pool(mut self, pool: PgPool) -> Builder<bp::HasPool> {
        self.pool = Some(pool);
        cast(self)
    }
}

impl Builder<bp::HasPool> {
    /// Returns a fully-configured and valid [`Subscriber`].
    pub fn build(self) -> Subscriber {
        // All unwraps are safe due to the way this builder is implemented.
        Subscriber {
            job_registry: self.job_registry,
            queue_names: self.queue_names,
            pool: self.pool.unwrap(),
            cancellation_token: self.cancel_token.unwrap(),
        }
    }
}

#[inline(always)]
fn cast<A, B>(this: Builder<A>) -> Builder<B> {
    Builder {
        job_registry: this.job_registry,
        queue_names: this.queue_names,
        pool: this.pool,
        cancel_token: this.cancel_token,
        _phase: PhantomData,
    }
}
