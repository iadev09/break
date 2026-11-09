mod config;
mod cpu;
mod fixture;
mod report;

use clap::Parser;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn run() -> Result<()> {
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
    if args.fixture {
        return fixture::serve(&args);
    }
    report::run(&args)
}
