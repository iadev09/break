//! Command-line adapter; the library itself installs no process signal handler.
use std::ffi::OsString;
use std::process::ExitCode;
use std::time::Duration;

use crate::{
    Acceptance, BreakReason, Broken, CancellationToken, ConnectionPlan,
    Observer, Phase, PhaseStats, Plan, Stats, Target,
};
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "brk",
    version,
    color = clap::ColorChoice::Never,
    about = env!("CARGO_PKG_DESCRIPTION")
)]
struct Cli {
    #[command(subcommand)]
    test: Test,
}

/// Two separate tests. Each raises one thing and judges one contract.
#[derive(Subcommand)]
enum Test {
    /// Raise concurrent requests per connection over connections that stay
    /// open. This is the default test.
    #[command(alias = "req")]
    Requests(RequestArgs),
    /// Raise concurrent connections, each carrying a fixed number of
    /// requests before it is replaced.
    #[command(alias = "conn")]
    Connections(ConnectionArgs),
}

#[derive(Args)]
struct Common {
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
    /// For https://, accept any server certificate instead of verifying it:
    /// a self-signed or private host. The connection is still encrypted.
    #[cfg(feature = "tls")]
    #[arg(long, conflicts_with = "cacert")]
    insecure: bool,
    /// For https://, make every handshake a full one instead of resuming.
    #[cfg(feature = "tls")]
    #[arg(long)]
    no_resume: bool,
    /// For https://, offer only this TLS version: 1.2 or 1.3.
    #[cfg(feature = "tls")]
    #[arg(long, value_name = "VERSION")]
    tls_version: Option<String>,
    /// For https://, offer only this key exchange group, for example X25519
    /// or X25519MLKEM768.
    #[cfg(feature = "tls")]
    #[arg(long, value_name = "NAME")]
    tls_group: Option<String>,
    /// For https://, offer only this cipher suite, for example
    /// TLS13_AES_128_GCM_SHA256.
    #[cfg(feature = "tls")]
    #[arg(long, value_name = "NAME")]
    tls_cipher: Option<String>,
    /// Speak HTTP/2: only h2 is offered over https://, and http:// starts
    /// with prior knowledge. The request test's level is then concurrent
    /// streams per connection.
    #[cfg(feature = "http2")]
    #[arg(long)]
    http2: bool,
    /// Client worker threads. By default one for every CPU the process may
    /// run on; never more than there are connections.
    #[arg(long, value_name = "COUNT")]
    threads: Option<usize>,
    /// Time at each level, for example 10s.
    #[arg(long, value_parser = humantime::parse_duration)]
    phase: Duration,
    /// Request deadline. In the request test it includes setup for the first
    /// request on a connection; in the connection test a setup has this
    /// deadline of its own.
    #[arg(long, value_parser = humantime::parse_duration)]
    timeout: Duration,
    /// Minimum successful percentage within the deadline.
    #[arg(long, alias = "sla", default_value_t = 99.99)]
    accept: f64,
    /// Before the first phase, read the machine for one phase with no
    /// traffic: what everything else on it uses, and so what the test had.
    #[arg(long)]
    baseline: bool,
    /// Additionally require this exact response body, without text decoding.
    #[arg(long)]
    expect_body: Option<String>,
    /// POST this file's contents with every request (`-` reads standard
    /// input): read once at start, sent with its content-length.
    #[arg(long, value_name = "FILE")]
    body_file: Option<std::path::PathBuf>,
    /// Require each response body to be as long as the body sent: an echo.
    #[arg(long, requires = "body_file")]
    expect_echo: bool,
}

#[derive(Args)]
struct RequestArgs {
    #[command(flatten)]
    common: Common,
    /// Fixed number of connections.
    #[arg(long)]
    conn: usize,
    /// Initial concurrent requests per connection; defaults to pace.
    #[arg(long)]
    start: Option<usize>,
    /// Additional concurrent requests per connection in each phase.
    #[arg(long)]
    pace: usize,
    /// Optional per-connection ceiling; reaching it does not find a breakpoint.
    #[arg(long = "max")]
    max_concurrency: Option<usize>,
}

#[derive(Args)]
struct ConnectionArgs {
    #[command(flatten)]
    common: Common,
    /// Initial concurrent connections; defaults to pace.
    #[arg(long)]
    start: Option<usize>,
    /// Additional concurrent connections in each phase.
    #[arg(long)]
    pace: usize,
    /// Exact number of requests each connection carries, one at a time.
    #[arg(long)]
    requests: usize,
    /// Optional ceiling on connections; reaching it does not find a breakpoint.
    #[arg(long = "max")]
    max_owners: Option<usize>,
}

/// `brk <url> ...` is the request test: the default needs no name.
fn parse(
    arguments: impl IntoIterator<Item = OsString>
) -> Result<Cli, clap::Error> {
    let mut arguments: Vec<OsString> = arguments.into_iter().collect();
    let named = arguments.get(1).and_then(|first| first.to_str()).is_some_and(
        |first| {
            matches!(
                first,
                "requests"
                    | "req"
                    | "connections"
                    | "conn"
                    | "help"
                    | "-h"
                    | "--help"
                    | "-V"
                    | "--version"
            )
        },
    );
    if !named && arguments.len() > 1 {
        arguments.insert(1, "requests".into());
    }
    Cli::try_parse_from(arguments)
}

/// What a level counts in each test's output.
#[derive(Clone, Copy)]
enum Unit {
    Requests,
    Connections,
}

impl Unit {
    fn name(self) -> &'static str {
        match self {
            Self::Requests => "concurrent requests",
            Self::Connections => "concurrent connections",
        }
    }
}

pub fn main() -> ExitCode {
    let cli = parse(std::env::args_os()).unwrap_or_else(|error| error.exit());
    let unit = match &cli.test {
        Test::Requests(_) => Unit::Requests,
        Test::Connections(_) => Unit::Connections,
    };
    match execute(cli.test) {
        Ok(done) => {
            print_boundary(&done.stats, unit);
            if let Some(last) = &done.stats.last_accepted {
                println!(
                    "Done ({} {}): tested ceiling completed; breakpoint not found.",
                    last.phase.total_concurrency,
                    unit.name()
                );
            }
            ExitCode::SUCCESS
        }
        Err(broken) => {
            print_boundary(&broken.stats, unit);
            let label = if matches!(broken.reason, BreakReason::Cancelled) {
                "Cancelled"
            } else {
                "Broken"
            };
            if let Some(last) = &broken.stats.last_phase {
                eprintln!(
                    "{label} ({} {}): {}",
                    last.phase.total_concurrency,
                    unit.name(),
                    broken.reason
                );
            } else {
                eprintln!("{label}: {}", broken.reason);
            }
            if broken.connection_failed() {
                eprintln!(
                    "Connection setup failed; this does not establish a client or server capacity limit."
                );
            } else if matches!(
                broken.reason,
                BreakReason::Acceptance | BreakReason::Connections
            ) && !broken.has_capacity_boundary()
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

fn print_boundary(
    stats: &Stats,
    unit: Unit,
) {
    match unit {
        Unit::Requests => {
            if let Some(last) = &stats.last_accepted {
                println!(
                    "Last accepted: {} requests/connection, {} total concurrent requests",
                    last.phase.requests_per_connection,
                    last.phase.total_concurrency
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
        Unit::Connections => {
            if let Some(last) = &stats.last_accepted {
                println!(
                    "Last accepted: {} concurrent connections",
                    last.phase.connections
                );
            }
            if let Some(last) = &stats.last_phase {
                println!(
                    "Last tested: phase {}, {} concurrent connections",
                    last.phase.index, last.phase.connections
                );
            }
        }
    }
}

fn target(common: &Common) -> Result<Target, BreakReason> {
    let mut target = Target::new(&common.url)?;
    if let Some(address) = common.connect {
        target = target.connect_to(address);
    }
    #[cfg(feature = "tls")]
    {
        if let Some(path) = &common.cacert {
            target = target.trust_pem(&std::fs::read(path)?)?;
        }
        if common.insecure {
            target = target.without_verification();
        }
        if common.no_resume {
            target = target.without_resumption();
        }
        if let Some(version) = &common.tls_version {
            target = target.tls_version(version)?;
        }
        if let Some(group) = &common.tls_group {
            target = target.tls_group(group)?;
        }
        if let Some(cipher) = &common.tls_cipher {
            target = target.tls_cipher(cipher)?;
        }
    }
    #[cfg(feature = "http2")]
    if common.http2 {
        target = target.http2();
    }
    if let Some(path) = &common.body_file {
        let body = if path.as_os_str() == "-" {
            let mut body = Vec::new();
            std::io::Read::read_to_end(&mut std::io::stdin(), &mut body)?;
            body
        } else {
            std::fs::read(path)?
        };
        target = target.post_body(body);
    }
    if common.expect_echo {
        target = target.expect_echo();
    }
    if let Some(body) = &common.expect_body {
        target = target.expect_body(body.clone().into_bytes());
    }
    Ok(target)
}

/// The machine with no traffic for one phase's length, before the ramp.
fn baseline(phase: Duration) {
    let Some(before) = crate::MachineSample::read() else {
        println!("Baseline: no machine CPU reading on this platform");
        return;
    };
    std::thread::sleep(phase);
    match crate::MachineSample::read().and_then(|after| before.until(after)) {
        Some(machine) => {
            println!("Baseline: {} with no traffic", Machine(&machine))
        }
        None => println!("Baseline: the machine CPU counters gave no reading"),
    }
}

/// One machine window, as the phases print it.
struct Machine<'a>(&'a crate::MachineCpu);

impl std::fmt::Display for Machine<'_> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        let machine = self.0;
        write!(
            f,
            "machine busy {:.2} of {} CPUs ({:.1}%), unused {:.2}: user {:.1}% system {:.1}% softirq {:.1}% irq {:.1}% iowait {:.1}% steal {:.1}% over {:.1}s",
            machine.busy_cpus,
            machine.cpus,
            machine.busy_percent,
            f64::from(machine.cpus) - machine.busy_cpus,
            machine.user_percent,
            machine.system_percent,
            machine.softirq_percent,
            machine.irq_percent,
            machine.iowait_percent,
            machine.steal_percent,
            machine.seconds
        )
    }
}

fn execute(test: Test) -> Result<crate::Done, Broken> {
    let setup_error = |reason| Broken { reason, stats: Box::default() };
    let stop = CancellationToken::new();
    let signal = stop.clone();
    let interrupt = || {
        ctrlc::set_handler(move || signal.cancel()).map_err(|_| {
            setup_error(BreakReason::Worker("could not install Ctrl-C handler"))
        })
    };
    match test {
        Test::Requests(args) => {
            let target = target(&args.common).map_err(setup_error)?;
            let plan = Plan {
                conn: args.conn,
                start: args.start.unwrap_or(args.pace),
                pace: args.pace,
                phase_duration: args.common.phase,
                timeout: args.common.timeout,
                accept: args.common.accept,
                max_concurrency: args.max_concurrency,
                threads: args.common.threads,
            };
            let acceptance = plan.validate().map_err(setup_error)?;
            if args.common.baseline {
                baseline(args.common.phase);
            }
            interrupt()?;
            crate::run(
                &target,
                plan,
                &stop,
                &mut Printer { acceptance, each: None },
            )
        }
        Test::Connections(args) => {
            let target = target(&args.common).map_err(setup_error)?;
            let plan = ConnectionPlan {
                start: args.start.unwrap_or(args.pace),
                pace: args.pace,
                requests: args.requests,
                phase_duration: args.common.phase,
                timeout: args.common.timeout,
                accept: args.common.accept,
                max_owners: args.max_owners,
                threads: args.common.threads,
            };
            let acceptance = plan.validate().map_err(setup_error)?;
            if args.common.baseline {
                baseline(args.common.phase);
            }
            interrupt()?;
            crate::run_connections(
                &target,
                plan,
                &stop,
                &mut Printer { acceptance, each: Some(plan.requests) },
            )
        }
    }
}

struct Printer {
    acceptance: Acceptance,
    /// Requests each connection carries, in the connection test.
    each: Option<usize>,
}

impl Printer {
    fn unit(&self) -> Unit {
        match self.each {
            None => Unit::Requests,
            Some(_) => Unit::Connections,
        }
    }
}

impl Observer for Printer {
    fn workers_started(
        &mut self,
        cpu_workers: usize,
    ) {
        println!("Client: {cpu_workers} worker threads");
    }

    fn phase_started(
        &mut self,
        phase: &Phase,
    ) {
        match self.each {
            None => println!(
                "Phase {}: {} connections x {} requests = {} concurrent requests | window={}",
                phase.index,
                phase.connections,
                phase.requests_per_connection,
                phase.total_concurrency,
                humantime::format_duration(phase.duration)
            ),
            Some(each) => println!(
                "Phase {}: {} concurrent connections, {} requests each | window={}",
                phase.index,
                phase.connections,
                each,
                humantime::format_duration(phase.duration)
            ),
        }
    }

    fn phase_finished(
        &mut self,
        stats: &PhaseStats,
    ) {
        let accepted = stats.accepted(self.acceptance);
        let marker = if stats.cancelled {
            "Cancelled"
        } else if accepted {
            "Done"
        } else {
            "Broken"
        };
        let unit = self.unit().name();
        if accepted {
            println!("  {marker} ({} {unit})", stats.phase.total_concurrency);
        } else {
            eprintln!("  {marker} ({} {unit})", stats.phase.total_concurrency);
        }
        match &stats.machine {
            Some(machine) => println!("  loaded: {}", Machine(machine)),
            None => println!(
                "  loaded: no machine reading (not fully loaded, or no reading on this platform)"
            ),
        }
        if let Some(tls) = &stats.tls {
            println!(
                "  tls: version={} cipher={} group={}",
                tls.version,
                tls.cipher,
                tls.group.unwrap_or("none")
            );
        }
        if let Some(setups) = &stats.setups {
            let setup_mean = setups
                .mean_millis()
                .map_or_else(|| "n/a".to_owned(), |mean| format!("{mean:.3}"));
            println!(
                "  connections: attempts={} established={} failed={} timeout={} resumed={} established/s={:.2} setup-avg-ms={setup_mean} setup-max={}",
                setups.attempted,
                setups.established,
                setups.failed,
                setups.timeout,
                setups.resumed,
                stats.connections_per_sec(),
                humantime::format_duration(setups.longest)
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
#[path = "../tests/cli.rs"]
mod tests;
