mod cancelled_after_failures;
mod catch_panic;
mod graceful_shutdown;
mod successful;

static SCHEMA_QUERY: &str = include_str!("../../db/schema.sql");

#[ctor::ctor]
fn test_setup() {
    let no_capture = std::env::var("NOCAPTURE").ok();
    if let Some("1") = no_capture.as_deref() {
        tracing_subscriber::fmt::init();
    }
}
