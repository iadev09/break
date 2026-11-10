mod config;
mod fixture;
mod report;

use clap::Parser;

pub type Result<T> = brk::fixture::Result<T>;

pub fn run() -> Result<()> {
    if std::env::args_os().nth(1).as_deref()
        == Some(std::ffi::OsStr::new("--fixture"))
    {
        return brk::fixture::serve(brk::fixture::native::start);
    }
    // Cargo's test runner invokes a custom bench with no arguments (or --test).
    // Compile-smoke invocations must not start load. `cargo bench` adds --bench.
    if std::env::args_os().len() == 1
        || std::env::args_os().any(|arg| arg == "--test")
    {
        return Ok(());
    }
    // Cargo passes --bench to a custom bench harness.
    let args = config::Args::parse_from(
        std::env::args_os().filter(|arg| arg != "--bench"),
    );
    args.validate()?;
    report::run(&args)
}
