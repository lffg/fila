use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    future::Future,
    marker::PhantomData,
};

use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::job;

use super::Subscriber;

type JobMap = HashMap<job::Name, Box<dyn ErasedJob + Send + Sync + 'static>>;

type TypeMap = HashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>;

pub struct SubscriberBuilder<S = ()> {
    pub(super) queue_names: HashSet<job::QueueName>,
    pub(super) state_map: TypeMap,
    pub(super) jobs: JobMap,
    _state: PhantomData<S>,
}

impl SubscriberBuilder<()> {
    pub(super) fn new() -> Self {
        Self {
            queue_names: Default::default(),
            state_map: Default::default(),
            jobs: Default::default(),
            _state: PhantomData,
        }
    }

    pub fn build(self) -> Subscriber {
        Subscriber {
            cancel_token: CancellationToken::new(),
            inner: self,
        }
    }

    pub fn with_state<S, B>(mut self, state: &S, builder_fn: B) -> SubscriberBuilder<()>
    where
        S: Clone,
        S: Send + Sync + 'static,
        B: FnOnce(SubscriberBuilder<S>) -> SubscriberBuilder<S>,
    {
        let ty = TypeId::of::<S>();
        self.state_map.insert(ty, Box::new(state.clone()));
        cast(builder_fn(cast(self)))
    }
}

impl<S> SubscriberBuilder<S> {
    pub fn register<J>(mut self) -> SubscriberBuilder<S>
    where
        J: job::Job<State = S>,
        J: DeserializeOwned,
        J: Send + Sync + 'static,
        J::State: Clone + Any,
        J::State: Sync + Send + 'static,
    {
        if self.jobs.contains_key(J::NAME) {
            panic!("already registered job with name {:?}", J::NAME);
        }
        let ty = TypeId::of::<J::State>();

        let config = J::config();
        config.validate().unwrap();

        self.queue_names.insert(config.queue);
        self.jobs.insert(
            J::NAME,
            Box::new(JobData {
                run: move |payload: &str, ctx: ContextWithoutState, state_map: &TypeMap| {
                    // TODO: Get rid of these unwraps.
                    let payload: J = serde_json::from_str(payload).unwrap();
                    let state = TypeMap::get(state_map, &ty).unwrap();
                    let state: &J::State = state.downcast_ref().unwrap();
                    let ctx = ctx.into_context::<J::State>(J::config(), state.clone());
                    async move { J::exec(payload, &ctx).await }
                },
                config,
            }),
        );
        self
    }
}

#[async_trait::async_trait]
pub(crate) trait ErasedJob {
    async fn run(
        &self,
        enc_payload: &str,
        ctx: ContextWithoutState,
        state_map: &TypeMap,
    ) -> Result<(), job::Error>;

    fn config(&self) -> &job::Config;
}

struct JobData<R> {
    run: R,
    config: job::Config,
}

#[async_trait::async_trait]
impl<R, Fut> ErasedJob for JobData<R>
where
    R: Send + Sync + 'static,
    R: Fn(&str, ContextWithoutState, &TypeMap) -> Fut,
    Fut: Future<Output = Result<(), job::Error>>,
    Fut: Send + Sync + 'static,
{
    async fn run(
        &self,
        enc_payload: &str,
        ctx: ContextWithoutState,
        state_map: &TypeMap,
    ) -> Result<(), job::Error> {
        (self.run)(enc_payload, ctx, state_map).await
    }

    fn config(&self) -> &job::Config {
        &self.config
    }
}

pub(crate) struct ContextWithoutState {
    pub(crate) attempt: u16,
}

impl ContextWithoutState {
    fn into_context<S>(self, config: job::Config, state: S) -> job::Context<S> {
        job::Context {
            state,
            config,
            attempt: self.attempt,
        }
    }
}

#[inline(always)]
fn cast<A, B>(b: SubscriberBuilder<A>) -> SubscriberBuilder<B> {
    SubscriberBuilder {
        queue_names: b.queue_names,
        state_map: b.state_map,
        jobs: b.jobs,
        _state: PhantomData,
    }
}
