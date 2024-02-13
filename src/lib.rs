pub mod job;

pub mod publisher;
pub use publisher::send;

pub mod subscriber;

pub mod error;

pub(crate) const PG_TOPIC_NAME: &str = "fila_jobs_insert";
