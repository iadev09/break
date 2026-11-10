//! Optional control-protocol child; never runs as part of the client CLI.
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(unix)]
    return brk::fixture::serve(brk::fixture::native::start);
    #[cfg(not(unix))]
    Err("the fixture CPU contract requires Unix getrusage".into())
}
