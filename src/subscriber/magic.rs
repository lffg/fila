use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
};

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::{
    error::{Result, ResultExt},
    job,
};

type JobMap = HashMap<job::Name, Box<dyn ErasedJob + Send + Sync + 'static>>;

type TypeMap = HashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>;

pub struct JobRegistry {
    job_map: JobMap,
    state_map: TypeMap,
}

impl JobRegistry {
    /// Constructs a new job registry.
    pub fn new() -> Self {
        let mut registry = Self {
            job_map: Default::default(),
            state_map: Default::default(),
        };
        // Add the "default state", the unit value.
        // Otherwise, will fail to dispatch jobs that don't have it defined.
        registry.register_state(());
        registry
    }

    /// Dispatches and executes a job given its name.
    ///
    /// The outer result represents a failure in the library side. The inner
    /// errors represents a failure during the job execution side.
    ///
    /// Panics if there isn't a job registered with the given name.
    pub async fn dispatch_and_exec(
        &self,
        ctx: ErasedJobContext<'_>,
    ) -> Result<Result<(), job::Error>> {
        let job_handle = &self.job_map[ctx.name];
        job_handle.exec(ctx, &self.state_map).await
    }

    /// Returns the configuration for the given job name.
    pub fn config_for(&self, name: job::BorrowedName<'_>) -> &job::Config {
        self.job_map[name].config()
    }

    /// Adds the given state value so that it may be used in posterior job
    /// executions.
    ///
    /// Returns `true` if this call has overridden a previously registered
    /// state of the same type.
    pub fn register_state<S>(&mut self, state: S) -> bool
    where
        S: Any + Clone,
        S: Send + Sync + 'static,
    {
        let ty = TypeId::of::<S>();
        self.state_map.insert(ty, Box::new(state)).is_some()
    }

    /// Erases the given [`Job`](job::Job) type and registers it into the
    /// dispatcher.
    ///
    /// Does *not* check the validity of the job's configuration.
    ///
    /// Returns `true` if this call has overridden a previously registered job
    /// of the same name.
    pub fn register<J>(&mut self) -> bool
    where
        J: job::Job + DeserializeOwned,
        J: Send + Sync + 'static,
        J::State: Clone + Any,
        J::State: Sync + Send + 'static,
    {
        let erased_job = Box::new(ClosureErasedJob {
            run: move |ctx: ErasedJobContext<'_>, state_map: &TypeMap| {
                let payload: J = serde_json::from_str(ctx.encoded_payload)
                    .with_ctx("failed to deserialize job during dispatch")?;

                // SAFETY: These unwraps are safe since the subscriber builder API
                // guarantees that a given `Job` with a `State` `T` can only be
                // registered if a value of type `T` has been previously provided
                // using the `with_state` method.
                let ty = TypeId::of::<J::State>();
                let state = TypeMap::get(state_map, &ty).unwrap();
                let state = state.downcast_ref::<J::State>().unwrap().clone();

                let ctx = job::Context {
                    state,
                    attempt: ctx.attempt,
                };

                Ok(async move { J::exec(payload, &ctx).await })
            },
            config: J::config(),
        });
        self.job_map.insert(J::NAME, erased_job).is_some()
    }
}

/// Context necessary to dispatch and execute a job.
///
/// Also includes all the information of [`job::Context`], except the state
/// itself, which is dynamically retrieved during job dispatch.
pub struct ErasedJobContext<'a> {
    pub name: job::BorrowedName<'a>,
    pub encoded_payload: &'a str,
    pub attempt: u16,
}

/// An object-safe job runner trait.
///
/// We wouldn't be able to simply use the [`job::Job`] trait, as it isn't
/// object-safe.
#[async_trait]
trait ErasedJob {
    /// Runs the job.
    async fn exec(
        &self,
        ctx: ErasedJobContext<'_>,
        state_map: &TypeMap,
    ) -> Result<Result<(), job::Error>>;

    fn config(&self) -> &job::Config;
}

struct ClosureErasedJob<R> {
    run: R,
    config: job::Config,
}

#[async_trait]
impl<R, Fut> ErasedJob for ClosureErasedJob<R>
where
    R: Send + Sync + 'static,
    R: Fn(ErasedJobContext<'_>, &TypeMap) -> Result<Fut>,
    Fut: Future<Output = Result<(), job::Error>>,
    Fut: Send + Sync + 'static,
{
    async fn exec(
        &self,
        ctx: ErasedJobContext<'_>,
        state_map: &TypeMap,
    ) -> Result<Result<(), job::Error>> {
        Ok((self.run)(ctx, state_map)?.await)
    }

    fn config(&self) -> &job::Config {
        &self.config
    }
}
