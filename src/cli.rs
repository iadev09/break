//! Command-line adapter; the library itself installs no process signal handler.
use std::process::ExitCode;
use std::time::Duration;

use crate::{
    Acceptance, BreakReason, Broken, CancellationToken, Observer, Phase,
    PhaseStats, Plan, Stats, Target,
};
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "brk",
    version,
    color = clap::ColorChoice::Never,
    about = env!("CARGO_PKG_DESCRIPTION")
)]
struct Args {
    /// URL to test: http:// over TCP, https:// over TLS.
    url: String,
    /// Connect to this address instead of resolving the URL's host. The URL
    /// still supplies the Host header and the TLS server name.
    #[arg(long, value_name = "ADDRESS")]
    connect: Option<std::net::SocketAddr>,
    /// For https://, trust the authorities in this PEM file instead of the
    /// platform's.
    #[cfg(feature = "tls")]
    #[arg(long, value_name = "FILE")]
    cacert: Option<std::path::PathBuf>,
    /// Fixed number of connections.
    #[arg(long)]
    conn: usize,
    /// Initial concurrent requests per connection; defaults to pace.
    #[arg(long)]
    start: Option<usize>,
    /// Additional concurrent requests per connection in each phase.
    #[arg(long)]
    pace: usize,
    /// Time at each concurrency level, for example 10s.
    #[arg(long, value_parser = humantime::parse_duration)]
    phase: Duration,
    /// Request deadline, including setup for the first request on a connection.
    #[arg(long, value_parser = humantime::parse_duration)]
    timeout: Duration,
    /// Minimum successful 2xx percentage within the request deadline.
    #[arg(long, alias = "sla", default_value_t = 99.99)]
    accept: f64,
    /// Optional per-connection ceiling; reaching it does not find a breakpoint.
    #[arg(long = "max")]
    max_concurrency: Option<usize>,
    /// Additionally require this exact response body, without text decoding.
    #[arg(long)]
    expect_body: Option<String>,
}

pub fn main() -> ExitCode {
    let args = Args::parse();
    match execute(args) {
        Ok(done) => {
            print_boundary(&done.stats);
            if let Some(last) = &done.stats.last_accepted {
                println!(
                    "Done ({} concurrent requests): tested ceiling completed; breakpoint not found.",
                    last.phase.total_concurrency
                );
            }
            ExitCode::SUCCESS
        }
        Err(broken) => {
            print_boundary(&broken.stats);
            let label = if matches!(broken.reason, BreakReason::Cancelled) {
                "Cancelled"
            } else {
                "Broken"
            };
            if let Some(last) = &broken.stats.last_phase {
                eprintln!(
                    "{label} ({} concurrent requests): {}",
                    last.phase.total_concurrency, broken.reason
                );
            } else {
                eprintln!("{label}: {}", broken.reason);
            }
            if broken.connection_failed() {
                eprintln!(
                    "Connection setup failed; this does not establish a client or server capacity limit."
                );
            } else if matches!(broken.reason, BreakReason::Acceptance)
                && !broken.has_capacity_boundary()
            {
                eprintln!(
                    "Initial load rejected; no passing baseline was established."
                );
            } else if !broken.has_capacity_boundary() {
                eprintln!("No server capacity claim for this failure.");
            }
            if matches!(broken.reason, BreakReason::Cancelled) {
                ExitCode::from(130)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}

fn print_boundary(stats: &Stats) {
    if let Some(last) = &stats.last_accepted {
        println!(
            "Last accepted: {} requests/connection, {} total concurrent requests",
            last.phase.requests_per_connection, last.phase.total_concurrency
        );
    }
    if let Some(last) = &stats.last_phase {
        println!(
            "Last tested: phase {}, {} requests/connection, {} total concurrent requests",
            last.phase.index,
            last.phase.requests_per_connection,
            last.phase.total_concurrency
        );
    }
}

fn execute(args: Args) -> Result<crate::Done, Broken> {
    let setup_error = |reason| Broken { reason, stats: Box::default() };
    let mut target = Target::new(&args.url).map_err(setup_error)?;
    if let Some(address) = args.connect {
        target = target.connect_to(address);
    }
    #[cfg(feature = "tls")]
    if let Some(path) = &args.cacert {
        let pem = std::fs::read(path)
            .map_err(|error| setup_error(BreakReason::Io(error)))?;
        target = target.trust_pem(&pem).map_err(setup_error)?;
    }
    if let Some(body) = args.expect_body {
        target = target.expect_body(body.into_bytes());
    }
    let plan = Plan {
        conn: args.conn,
        start: args.start.unwrap_or(args.pace),
        pace: args.pace,
        phase_duration: args.phase,
        timeout: args.timeout,
        accept: args.accept,
        max_concurrency: args.max_concurrency,
    };
    let acceptance = plan.validate().map_err(setup_error)?;
    let stop = CancellationToken::new();
    let signal = stop.clone();
    ctrlc::set_handler(move || signal.cancel()).map_err(|_| {
        setup_error(BreakReason::Worker("could not install Ctrl-C handler"))
    })?;
    crate::run(&target, plan, &stop, &mut Printer(acceptance))
}

struct Printer(Acceptance);
impl Observer for Printer {
    fn phase_started(
        &mut self,
        phase: &Phase,
    ) {
        println!(
            "Phase {}: {} connections x {} requests = {} concurrent requests | window={}",
            phase.index,
            phase.connections,
            phase.requests_per_connection,
            phase.total_concurrency,
            humantime::format_duration(phase.duration)
        );
    }
    fn phase_finished(
        &mut self,
        stats: &PhaseStats,
    ) {
        let marker = if stats.cancelled {
            "Cancelled"
        } else if stats.accepted(self.0) {
            "Done"
        } else {
            "Broken"
        };
        if stats.accepted(self.0) {
            println!(
                "  {marker} ({} concurrent requests)",
                stats.phase.total_concurrency
            );
        } else {
            eprintln!(
                "  {marker} ({} concurrent requests)",
                stats.phase.total_concurrency
            );
        }
        println!(
            "  sent={} attempts={} successful={} errors={} successful-requests/s={:.2} elapsed={}",
            stats.counts.sent,
            stats.counts.attempted,
            stats.counts.successful,
            stats.counts.failed(),
            stats.requests_per_sec(),
            humantime::format_duration(stats.elapsed)
        );
        println!(
            "  target-reached-once={}/{} timeout={} transport={} abandoned={} connect={} connect-timeout={} status={} body={}",
            stats.connections_at_target,
            stats.phase.connections,
            stats.counts.timeout,
            stats.counts.transport,
            stats.counts.abandoned,
            stats.counts.connect,
            stats.counts.connect_timeout,
            stats.counts.status,
            stats.counts.body
        );
        println!(
            "  observed-outstanding-avg={:.2} target-coverage={:.2}% connection-time-at-target={:.2}% active={} drain={}",
            stats.load.mean_outstanding(),
            stats.load.target_fraction(stats.phase.total_concurrency) * 100.0,
            stats.load.connection_target_fraction(stats.phase.connections)
                * 100.0,
            humantime::format_duration(stats.load.active_elapsed),
            humantime::format_duration(stats.drain_elapsed())
        );
        match stats.latency.mean_millis() {
            Some(mean) => println!(
                "  request-avg-ms={mean:.3} completed-response-samples={} (TCP setup excluded)",
                stats.latency.responses
            ),
            None => println!(
                "  request-avg-ms=n/a completed-response-samples=0 (TCP setup excluded)"
            ),
        }
        println!(
            "  connect-attempts={} setup-seconds={:.6} write-blocked={} max-send-delay={} max-deadline-lateness={}",
            stats.load.connect_attempts,
            stats.load.connect_nanos as f64 / 1e9,
            stats.load.write_blocked,
            humantime::format_duration(stats.load.max_send_delay),
            humantime::format_duration(stats.load.max_deadline_lateness)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_numeric_concurrency_without_rate_units_or_warmup() {
        let base = [
            "brk",
            "http://example.test/plain",
            "--conn",
            "4",
            "--phase",
            "10s",
            "--pace",
            "100",
            "--timeout",
            "2s",
        ];
        let mut arguments = base.to_vec();
        arguments.extend(["--start", "500", "--sla", "99.99", "--max", "1000"]);
        let args = Args::try_parse_from(arguments).unwrap();
        assert_eq!(args.conn, 4);
        assert_eq!(args.start, Some(500));
        assert_eq!(args.pace, 100);
        assert_eq!(args.max_concurrency, Some(1_000));
        for unsupported in ["--warmup", "--tls", "--proto", "-t", "--max-rate"]
        {
            let mut arguments = base.to_vec();
            arguments.extend([unsupported, "1"]);
            assert!(Args::try_parse_from(arguments).is_err());
        }
        assert!(Args::try_parse_from(["brk", "http://example.test"]).is_err());
    }
}
