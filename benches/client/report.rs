use std::io::{self, Write};
use std::time::{Duration, Instant};

use brk::{CancellationToken, Observer, Phase, PhaseStats, Plan, Target};

use super::Result;
use super::config::{Args, Protocol};
use super::fixture;
use brk::fixture::{Cpu, Process};

#[derive(Default)]
struct Window {
    client: Option<Cpu>,
    server: Option<Cpu>,
    elapsed: Duration,
}

struct Measure<'a> {
    fixture: Option<&'a mut Process>,
    stop: CancellationToken,
    client_start: Option<Cpu>,
    server_start: Option<Cpu>,
    start: Option<Instant>,
    window: Window,
    error: Option<String>,
}

impl Measure<'_> {
    fn begin(&mut self) -> Result<()> {
        // Responder IPC is outside the client's sampled CPU window.
        self.server_start = self
            .fixture
            .as_mut()
            .map(|fixture| fixture.sample())
            .transpose()?;
        self.client_start = Some(Cpu::read()?);
        self.start = Some(Instant::now());
        Ok(())
    }

    fn end(&mut self) -> Result<()> {
        let ended = Instant::now();
        self.window.client = Some(Cpu::read()?.since(
            self.client_start.ok_or("no initial client CPU reading")?,
        )?);
        self.window.elapsed = ended.duration_since(
            self.start.ok_or("no initial measurement timestamp")?,
        );
        self.window.server = match (&mut self.fixture, self.server_start) {
            (Some(fixture), Some(start)) => {
                Some(fixture.sample()?.since(start)?)
            }
            _ => None,
        };
        Ok(())
    }
}

impl Observer for Measure<'_> {
    fn phase_started(
        &mut self,
        _: &Phase,
    ) {
        if let Err(error) = self.begin() {
            self.error = Some(error.to_string());
            self.stop.cancel();
        }
    }

    fn phase_finished(
        &mut self,
        _: &PhaseStats,
    ) {
        if self.error.is_none()
            && let Err(error) = self.end()
        {
            self.error = Some(error.to_string());
            self.stop.cancel();
        }
    }
}

struct Case {
    protocol: Protocol,
    connections: usize,
    depth: usize,
    threads: usize,
    repeat: usize,
}

struct Row {
    stats: PhaseStats,
    window: Window,
    threads: usize,
    accepted: bool,
    reason: String,
    responder: String,
}

fn measure(
    args: &Args,
    case: &Case,
) -> Result<Row> {
    let mut fixture = args
        .url
        .is_none()
        .then(|| fixture::start(args, case.protocol))
        .transpose()?;
    let url = args
        .url
        .clone()
        .unwrap_or_else(|| fixture.as_ref().unwrap().ready().url());
    let mut target = Target::new(&url)?.expect_body("ok");
    if case.protocol != Protocol::H1 {
        target = target.http2();
    }
    if let Some(trust) = fixture
        .as_ref()
        .and_then(|fixture| fixture.ready().trust_pem.as_deref())
    {
        target = target.trust_pem(trust.as_bytes())?;
    }
    let responder = fixture
        .as_ref()
        .map(|fixture| fixture.ready().implementation.clone())
        .unwrap_or_else(|| "external".to_owned());
    let stop = CancellationToken::new();
    let mut observer = Measure {
        fixture: fixture.as_mut(),
        stop: stop.clone(),
        client_start: None,
        server_start: None,
        start: None,
        window: Window::default(),
        error: None,
    };
    let outcome = brk::run(
        &target,
        Plan {
            conn: case.connections,
            start: case.depth,
            pace: 1,
            phase_duration: args.phase,
            timeout: args.timeout,
            accept: args.accept,
            max_concurrency: Some(case.depth),
            threads: Some(case.threads),
        },
        &stop,
        &mut observer,
    );
    if let Some(error) = observer.error {
        return Err(error.into());
    }
    let window = observer.window;
    let (accepted, reason, stats) = match outcome {
        Ok(done) => (true, String::new(), done.stats),
        Err(broken) => (false, broken.reason.to_string(), *broken.stats),
    };
    let threads = stats.cpu_workers;
    let stats = stats.last_phase.ok_or_else(|| {
        format!(
            "{} failed before a measured phase: {reason}",
            case.protocol.name(),
        )
    })?;
    if window.client.is_none() {
        return Err("phase ended without a client CPU measurement".into());
    }
    if let Some(fixture) = &mut fixture {
        fixture.stop()?;
    }
    Ok(Row { stats, window, threads, accepted, reason, responder })
}

fn ratio(
    numerator: f64,
    denominator: f64,
) -> String {
    if denominator > 0.0 {
        format!("{:.6}", numerator / denominator)
    } else {
        String::new()
    }
}

fn write_row(
    output: &mut impl Write,
    args: &Args,
    case: &Case,
    row: &Row,
) -> io::Result<()> {
    let stats = &row.stats;
    let cpu = row.window.client.unwrap();
    let server = row.window.server;
    let total_ns = cpu.user_ns as f64 + cpu.system_ns as f64;
    let server_s = server.map(|cpu| cpu.seconds());
    let average = stats.load.mean_outstanding();
    let coverage = stats.load.target_fraction(stats.phase.total_concurrency);
    let fields = [
        case.repeat.to_string(),
        case.protocol.name().to_owned(),
        case.connections.to_string(),
        case.depth.to_string(),
        stats.phase.total_concurrency.to_string(),
        case.threads.to_string(),
        row.threads.to_string(),
        args.server_threads.map(|n| n.to_string()).unwrap_or_default(),
        args.phase.as_secs_f64().to_string(),
        args.timeout.as_secs_f64().to_string(),
        stats.elapsed.as_secs_f64().to_string(),
        row.window.elapsed.as_secs_f64().to_string(),
        stats.counts.attempted.to_string(),
        stats.counts.successful.to_string(),
        stats.counts.failed().to_string(),
        stats.counts.timeout.to_string(),
        stats.counts.transport.to_string(),
        stats.counts.abandoned.to_string(),
        stats.counts.status.to_string(),
        stats.counts.body.to_string(),
        stats.counts.connect.to_string(),
        stats.counts.connect_timeout.to_string(),
        format!("{:.3}", stats.requests_per_sec()),
        format!("{average:.3}"),
        format!("{coverage:.6}"),
        format!("{:.9}", cpu.user_ns as f64 / 1e9),
        format!("{:.9}", cpu.system_ns as f64 / 1e9),
        ratio(total_ns / 1e3, stats.counts.attempted as f64),
        ratio(total_ns / 1e3, stats.counts.successful as f64),
        ratio(cpu.seconds(), row.window.elapsed.as_secs_f64()),
        ratio(
            cpu.seconds(),
            row.window.elapsed.as_secs_f64() * row.threads as f64,
        ),
        server
            .map(|cpu| format!("{:.9}", cpu.user_ns as f64 / 1e9))
            .unwrap_or_default(),
        server
            .map(|cpu| format!("{:.9}", cpu.system_ns as f64 / 1e9))
            .unwrap_or_default(),
        server_s
            .map(|s| ratio(s, row.window.elapsed.as_secs_f64()))
            .unwrap_or_default(),
        server_s
            .map(|s| {
                ratio(
                    s,
                    row.window.elapsed.as_secs_f64()
                        * args.server_threads.unwrap() as f64,
                )
            })
            .unwrap_or_default(),
        stats.load.max_deadline_lateness.as_secs_f64().to_string(),
        row.accepted.to_string(),
        row.responder.clone(),
        (case.protocol == Protocol::H2Tls).to_string(),
    ];
    let escaped = fields.map(|field| {
        if field.contains([',', '"', '\n', '\r']) {
            format!("\"{}\"", field.replace('"', "\"\""))
        } else {
            field
        }
    });
    writeln!(output, "{}", escaped.join(","))
}

pub fn run(args: &Args) -> Result<()> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    writeln!(
        output,
        "# break={},os={},arch={},available_parallelism={}",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism()?.get()
    )?;
    writeln!(
        output,
        "# source={},source_dirty={},accept={},body_bytes=2,responder={}",
        std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout)
                .trim()
                .to_owned())
            .unwrap_or_else(|| "unknown".to_owned()),
        std::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .ok()
            .map(|output| !output.stdout.is_empty())
            .unwrap_or(true),
        args.accept,
        if args.url.is_some() {
            "external"
        } else if args.fixture_bin.is_some() {
            "custom_child"
        } else {
            "native_hyper_child"
        }
    )?;
    writeln!(
        output,
        "repeat,protocol,connections,depth,target,requested_threads,actual_threads,server_threads,phase_s,timeout_s,elapsed_s,cpu_window_s,attempts,successful,errors,timeouts,transport_errors,abandoned,status_errors,body_errors,connect_errors,connect_timeouts,successful_rps,mean_outstanding,target_coverage,client_user_s,client_system_s,client_cpu_us_per_attempt,client_cpu_us_per_success,client_cpu_cores,client_thread_utilization,server_user_s,server_system_s,server_cpu_cores,server_thread_utilization,max_deadline_lateness_s,accepted,responder,tls"
    )?;
    output.flush()?;
    let mut peaks: Vec<(Protocol, f64, usize, usize, usize)> = Vec::new();
    for repeat in 1..=args.repeat {
        let mut protocols = args.protocols.clone();
        if repeat % 2 == 0 {
            protocols.reverse();
        }
        for &connections in &args.connections {
            for &threads in &args.threads {
                for &depth in &args.depths {
                    for &protocol in &protocols {
                        let case = Case {
                            protocol,
                            connections,
                            threads,
                            depth,
                            repeat,
                        };
                        eprintln!(
                            "{}/{}, {}: {connections} connections, depth {depth}, {threads} client threads",
                            repeat,
                            args.repeat,
                            protocol.name()
                        );
                        let row = measure(args, &case)?;
                        write_row(&mut output, args, &case, &row)?;
                        output.flush()?;
                        if !row.accepted {
                            eprintln!("  rejected: {}", row.reason);
                        } else {
                            let rate = row.stats.requests_per_sec();
                            if let Some(peak) =
                                peaks.iter_mut().find(|peak| peak.0 == protocol)
                            {
                                if rate > peak.1 {
                                    *peak = (
                                        protocol,
                                        rate,
                                        connections,
                                        depth,
                                        row.threads,
                                    );
                                }
                            } else {
                                peaks.push((
                                    protocol,
                                    rate,
                                    connections,
                                    depth,
                                    row.threads,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    for (protocol, rate, connections, depth, threads) in peaks {
        eprintln!(
            "{} best accepted observed rate: {rate:.0} req/s; connections={connections} depth={depth} threads={threads}",
            protocol.name()
        );
    }
    eprintln!(
        "Observed throughput is not an isolated client capacity limit. Check coverage, errors, client CPU and responder headroom; repeat with more responder capacity or an external server."
    );
    Ok(())
}
