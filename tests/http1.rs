//! Real ephemeral TCP fixtures exercise pipelining, phase boundaries and drain.
//! No deployment configuration, production service or sustained benchmark is used.
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use brk::{
    BreakReason, CancellationToken, Observer, Phase, PhaseStats, Plan, Target,
};

#[derive(Clone, Copy)]
enum Mode {
    Fixed,
    Chunked,
    Truncated,
    CloseDelimited,
    Ambiguous,
}

#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    wake: Condvar,
}
impl Gate {
    fn wait(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.wake.wait(open).unwrap();
        }
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

struct Fixture {
    address: SocketAddr,
    stopping: Arc<AtomicBool>,
    status: Arc<AtomicU16>,
    requests: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    gate: Option<Arc<Gate>>,
    arrivals: mpsc::Receiver<()>,
    thread: Option<JoinHandle<Vec<JoinHandle<()>>>>,
}

impl Fixture {
    fn new(
        mode: Mode,
        held: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stopping = Arc::new(AtomicBool::new(false));
        let status = Arc::new(AtomicU16::new(200));
        let requests = Arc::new(AtomicUsize::new(0));
        let connections = Arc::new(AtomicUsize::new(0));
        let gate = held.then(|| Arc::new(Gate::default()));
        let (arriving, arrivals) = mpsc::channel();
        let mut fixture = Self {
            address,
            stopping,
            status,
            requests,
            connections,
            gate,
            arrivals,
            thread: None,
        };
        let stop = fixture.stopping.clone();
        let status = fixture.status.clone();
        let requests = fixture.requests.clone();
        let connections = fixture.connections.clone();
        let gate = fixture.gate.clone();
        fixture.thread = Some(thread::spawn(move || {
            let mut handlers = Vec::new();
            for accepted in listener.incoming() {
                let stream = accepted.unwrap();
                if stop.load(Ordering::Acquire) {
                    break;
                }
                connections.fetch_add(1, Ordering::Relaxed);
                let status = status.clone();
                let requests = requests.clone();
                let gate = gate.clone();
                let arriving = held.then(|| arriving.clone());
                handlers.push(thread::spawn(move || {
                    handle(stream, mode, status, requests, gate, arriving)
                }));
            }
            handlers
        }));
        fixture
    }

    fn target(&self) -> Target {
        Target::new(&format!("http://{}/plain", self.address))
            .unwrap()
            .expect_body("ok")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(gate) = &self.gate {
            gate.release();
        }
        self.stopping.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address); // Wake the accept owner, without polling.
        if let Some(thread) = self.thread.take() {
            for handler in thread.join().unwrap() {
                handler.join().unwrap();
            }
        }
    }
}

fn handle(
    mut stream: TcpStream,
    mode: Mode,
    status: Arc<AtomicU16>,
    requests: Arc<AtomicUsize>,
    gate: Option<Arc<Gate>>,
    arrivals: Option<mpsc::Sender<()>>,
) {
    let mut writer = stream.try_clone().unwrap();
    let (reply, replies) = mpsc::channel();
    let output = thread::spawn(move || {
        for status in replies {
            if let Some(gate) = &gate {
                gate.wait();
            }
            let response = match mode {
                Mode::Fixed => format!(
                    "HTTP/1.1 {status} fixture\r\nContent-Length: 2\r\n\r\nok"
                ),
                Mode::Chunked => format!(
                    "HTTP/1.1 {status} fixture\r\nTransfer-Encoding: chunked\r\n\r\n1\r\no\r\n1\r\nk\r\n0\r\nX-Test: yes\r\n\r\n"
                ),
                Mode::Truncated => format!(
                    "HTTP/1.1 {status} fixture\r\nContent-Length: 2\r\n\r\no"
                ),
                Mode::CloseDelimited => format!(
                    "HTTP/1.1 {status} fixture\r\nConnection: close\r\n\r\nok"
                ),
                Mode::Ambiguous => format!(
                    "HTTP/1.1 {status} fixture\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
                ),
            };
            if writer.write_all(response.as_bytes()).is_err() {
                break;
            }
            if matches!(mode, Mode::CloseDelimited | Mode::Ambiguous) {
                let _ = writer.shutdown(Shutdown::Both);
                break;
            }
        }
    });
    let mut buffered = Vec::new();
    loop {
        let mut bytes = [0; 4096];
        let count = match stream.read(&mut bytes) {
            Ok(0) | Err(_) => break,
            Ok(count) => count,
        };
        buffered.extend_from_slice(&bytes[..count]);
        while let Some(end) =
            buffered.windows(4).position(|bytes| bytes == b"\r\n\r\n")
        {
            buffered.drain(..end + 4);
            requests.fetch_add(1, Ordering::Relaxed);
            if let Some(arrivals) = &arrivals {
                let _ = arrivals.send(());
            }
            if reply.send(status.load(Ordering::Acquire)).is_err() {
                break;
            }
        }
    }
    drop(reply);
    output.join().unwrap();
}

fn plan() -> Plan {
    Plan {
        conn: 2,
        start: 2,
        pace: 3,
        phase_duration: Duration::from_millis(100),
        timeout: Duration::from_secs(2),
        accept: 99.99,
        max_concurrency: Some(2),
    }
}

struct Capture<'a> {
    fixture: &'a Fixture,
    events: Vec<(bool, usize)>,
    phases: Vec<PhaseStats>,
    reject_after_first: bool,
}
impl Observer for Capture<'_> {
    fn phase_started(
        &mut self,
        phase: &Phase,
    ) {
        self.events.push((true, phase.index));
        if phase.index == 0 {
            assert_eq!(self.fixture.requests.load(Ordering::Acquire), 0);
        }
    }
    fn phase_finished(
        &mut self,
        stats: &PhaseStats,
    ) {
        self.events.push((false, stats.phase.index));
        self.phases.push(*stats);
        assert_eq!(
            self.fixture.requests.load(Ordering::Acquire) as u64,
            self.phases.iter().map(|phase| phase.counts.attempted).sum::<u64>()
        );
        if self.reject_after_first {
            self.fixture.status.store(503, Ordering::Release);
        }
    }
}

#[test]
fn start_and_pace_are_per_connection_and_callbacks_surround_drained_phases() {
    let fixture = Fixture::new(Mode::Fixed, false);
    let mut observer = Capture {
        fixture: &fixture,
        events: Vec::new(),
        phases: Vec::new(),
        reject_after_first: true,
    };
    let broken = brk::run(
        &fixture.target(),
        Plan { max_concurrency: None, ..plan() },
        &CancellationToken::new(),
        &mut observer,
    )
    .unwrap_err();
    assert!(matches!(broken.reason, BreakReason::Acceptance));
    assert_eq!(observer.events, [(true, 0), (false, 0), (true, 1), (false, 1)]);
    assert_eq!(observer.phases[0].phase.requests_per_connection, 2);
    assert_eq!(observer.phases[0].phase.total_concurrency, 4);
    assert_eq!(observer.phases[1].phase.requests_per_connection, 5);
    assert_eq!(observer.phases[1].phase.total_concurrency, 10);
    assert!(observer.phases[0].counts.successful > 0);
    assert_eq!(observer.phases[1].counts.successful, 0);
    assert_eq!(
        observer.phases[1].counts.status,
        observer.phases[1].counts.attempted
    );
    assert_eq!(fixture.connections.load(Ordering::Acquire), 2);
    assert_eq!(broken.stats.last_accepted.unwrap().phase.index, 0);
    assert!(broken.has_capacity_boundary());
}

#[test]
fn real_pipelined_requests_reach_the_limit_without_waiting_for_a_response() {
    for depth in [4, 256] {
        let fixture = Fixture::new(Mode::Fixed, true);
        let plan = Plan {
            conn: 3,
            start: depth,
            max_concurrency: Some(depth),
            ..plan()
        };
        let expected = plan.conn * depth;
        let target = fixture.target();
        let stop = CancellationToken::new();
        let cancelling = stop.clone();
        let client =
            thread::spawn(move || brk::run(&target, plan, &stop, &mut ()));
        let all_pending = (0..expected)
            .all(|_| fixture.arrivals.recv_timeout(plan.timeout).is_ok());
        cancelling.cancel();
        fixture.gate.as_ref().unwrap().release();
        let broken = client.join().unwrap().unwrap_err();
        assert!(
            all_pending,
            "every pipeline must arrive before any response is released"
        );
        assert!(matches!(broken.reason, BreakReason::Cancelled));
        let phase = broken.stats.last_phase.unwrap();
        assert_eq!(phase.counts.sent, expected as u64);
        assert_eq!(phase.counts.successful, expected as u64);
        assert_eq!(phase.connections_at_target, plan.conn);
        assert!(phase.load.mean_outstanding() > 0.0);
        assert_eq!(fixture.connections.load(Ordering::Acquire), plan.conn);
    }
}

#[test]
fn successful_2xx_and_complete_bodies_produce_done_with_stats() {
    let fixture = Fixture::new(Mode::Chunked, false);
    fixture.status.store(201, Ordering::Release);
    let done =
        brk::run(&fixture.target(), plan(), &CancellationToken::new(), &mut ())
            .unwrap();
    assert_eq!(done.stats.phases, 1);
    assert!(done.stats.total.successful > 4);
    assert_eq!(done.stats.total.failed(), 0);
    assert_eq!(fixture.connections.load(Ordering::Acquire), 2);
}

#[test]
fn close_delimited_body_finishes_at_eof_and_replaces_the_connection() {
    let fixture = Fixture::new(Mode::CloseDelimited, false);
    let plan = Plan { conn: 1, start: 1, max_concurrency: Some(1), ..plan() };
    let done =
        brk::run(&fixture.target(), plan, &CancellationToken::new(), &mut ())
            .unwrap();
    assert!(done.stats.total.successful > 1);
    assert_eq!(done.stats.total.failed(), 0);
}

#[test]
fn incomplete_body_times_out_in_its_original_phase_before_end_callback() {
    let fixture = Fixture::new(Mode::Truncated, false);
    let plan = Plan {
        conn: 1,
        start: 1,
        phase_duration: Duration::from_millis(50),
        timeout: Duration::from_millis(200),
        max_concurrency: Some(1),
        ..plan()
    };
    let mut observer = Capture {
        fixture: &fixture,
        events: Vec::new(),
        phases: Vec::new(),
        reject_after_first: false,
    };
    let broken = brk::run(
        &fixture.target(),
        plan,
        &CancellationToken::new(),
        &mut observer,
    )
    .unwrap_err();
    assert!(matches!(broken.reason, BreakReason::Acceptance));
    let failed = broken.stats.last_phase.unwrap();
    assert_eq!(failed.counts.attempted, 1);
    assert_eq!(failed.counts.timeout, 1);
    assert!(failed.elapsed >= plan.timeout);
    assert_eq!(observer.events, [(true, 0), (false, 0)]);
}

#[test]
fn wrong_body_is_a_failed_measured_attempt() {
    let fixture = Fixture::new(Mode::Fixed, false);
    let broken = brk::run(
        &fixture.target().expect_body("different"),
        plan(),
        &CancellationToken::new(),
        &mut (),
    )
    .unwrap_err();
    assert!(matches!(broken.reason, BreakReason::Acceptance));
    assert_eq!(broken.stats.total.successful, 0);
    assert_eq!(broken.stats.total.body, broken.stats.total.attempted);
}

#[test]
fn initial_connection_failure_returns_broken_with_paired_callbacks() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let target = Target::new(&format!("http://{address}/plain")).unwrap();
    #[derive(Default)]
    struct Events(Vec<bool>);
    impl Observer for Events {
        fn phase_started(
            &mut self,
            _: &Phase,
        ) {
            self.0.push(true);
        }
        fn phase_finished(
            &mut self,
            _: &PhaseStats,
        ) {
            self.0.push(false);
        }
    }
    let mut events = Events::default();
    let broken = brk::run(
        &target,
        Plan {
            conn: 1,
            start: 10_000,
            max_concurrency: Some(10_000),
            ..plan()
        },
        &CancellationToken::new(),
        &mut events,
    )
    .unwrap_err();
    assert!(matches!(broken.reason, BreakReason::Io(_)));
    assert_eq!(events.0, [true, false]);
    assert!(broken.stats.last_accepted.is_none());
    assert_eq!(broken.stats.total.sent, 0);
    assert_eq!(broken.stats.total.connect, 1);
    assert!(broken.connection_failed());
    assert!(!broken.has_capacity_boundary());
}

#[test]
fn ambiguous_http_framing_does_not_become_a_success() {
    let fixture = Fixture::new(Mode::Ambiguous, false);
    let broken =
        brk::run(&fixture.target(), plan(), &CancellationToken::new(), &mut ())
            .unwrap_err();
    assert!(matches!(broken.reason, BreakReason::Protocol(_)));
    assert_eq!(broken.stats.total.successful, 0);
}

#[test]
fn increasing_phases_retain_connections_and_stop_at_the_declared_ceiling() {
    let fixture = Fixture::new(Mode::Fixed, false);
    let plan = Plan { start: 3, pace: 2, max_concurrency: Some(7), ..plan() };
    let mut observer = Capture {
        fixture: &fixture,
        events: Vec::new(),
        phases: Vec::new(),
        reject_after_first: false,
    };
    let done = brk::run(
        &fixture.target(),
        plan,
        &CancellationToken::new(),
        &mut observer as &mut dyn Observer,
    )
    .unwrap();
    assert_eq!(
        observer
            .phases
            .iter()
            .map(|phase| phase.phase.requests_per_connection)
            .collect::<Vec<_>>(),
        [3, 5, 7]
    );
    assert_eq!(done.stats.last_accepted.unwrap().phase.total_concurrency, 14);
    assert_eq!(fixture.connections.load(Ordering::Acquire), 2);
    assert_eq!(done.stats.total.failed(), 0);
}

#[test]
fn a_ceiling_between_pace_levels_is_measured_instead_of_silently_skipped() {
    let fixture = Fixture::new(Mode::Fixed, false);
    let plan = Plan { start: 3, pace: 2, max_concurrency: Some(6), ..plan() };
    let mut observer = Capture {
        fixture: &fixture,
        events: Vec::new(),
        phases: Vec::new(),
        reject_after_first: false,
    };
    let done = brk::run(
        &fixture.target(),
        plan,
        &CancellationToken::new(),
        &mut observer,
    )
    .unwrap();
    assert_eq!(
        observer
            .phases
            .iter()
            .map(|phase| phase.phase.requests_per_connection)
            .collect::<Vec<_>>(),
        [3, 5, 6]
    );
    assert_eq!(done.stats.last_accepted.unwrap().phase.total_concurrency, 12);
    for stats in observer.phases {
        assert_eq!(stats.load.active_elapsed, plan.phase_duration);
        assert!(stats.load.mean_outstanding() > 0.0);
        assert!(
            stats.load.target_fraction(stats.phase.total_concurrency) <= 1.0
        );
        assert!(stats.load.connection_target_fraction(plan.conn) <= 1.0);
        assert!(stats.load.latest_first_target.is_some());
        assert_eq!(stats.counts.attempted, stats.counts.successful);
    }
}

#[test]
fn an_initial_sla_rejection_does_not_establish_a_capacity_boundary() {
    let fixture = Fixture::new(Mode::Fixed, false);
    fixture.status.store(503, Ordering::Release);
    let broken =
        brk::run(&fixture.target(), plan(), &CancellationToken::new(), &mut ())
            .unwrap_err();
    assert!(matches!(broken.reason, BreakReason::Acceptance));
    assert!(!broken.has_capacity_boundary());
    assert!(!broken.connection_failed());
}

#[test]
fn impossible_request_slot_allocation_is_broken_instead_of_panicking() {
    let fixture = Fixture::new(Mode::Fixed, false);
    let plan = Plan {
        conn: 1,
        start: usize::MAX,
        max_concurrency: Some(usize::MAX),
        ..plan()
    };
    let broken =
        brk::run(&fixture.target(), plan, &CancellationToken::new(), &mut ())
            .unwrap_err();
    assert!(matches!(broken.reason, BreakReason::Resources(_)));
    assert_eq!(broken.stats.total.sent, 0);
    assert!(broken.stats.last_accepted.is_none());
}

#[test]
fn callback_unwinding_releases_gated_workers_without_sending_load() {
    struct Panic;
    impl Observer for Panic {
        fn phase_started(
            &mut self,
            _: &Phase,
        ) {
            panic!("fixture observer failure");
        }
    }
    let fixture = Fixture::new(Mode::Fixed, false);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        brk::run(
            &fixture.target(),
            plan(),
            &CancellationToken::new(),
            &mut Panic,
        )
    }));
    assert!(result.is_err());
    assert_eq!(fixture.requests.load(Ordering::Acquire), 0);
}

#[cfg(feature = "cli")]
fn cli(fixture: &Fixture) -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_brk"));
    command.arg(format!("http://{}/plain", fixture.address)).args([
        "--conn",
        "1",
        "--start",
        "1",
        "--pace",
        "1",
        "--phase",
        "100ms",
        "--timeout",
        "2s",
        "--max",
        "1",
        "--expect-body",
        "ok",
    ]);
    command
}

#[cfg(feature = "cli")]
#[test]
fn cli_reports_exit_codes_and_plain_status_output() {
    let fixture = Fixture::new(Mode::Fixed, false);
    let done = cli(&fixture).env("NO_COLOR", "1").output().unwrap();
    assert_eq!(done.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&done.stdout)
            .contains("Done (1 concurrent requests)")
    );
    assert!(!done.stdout.contains(&0x1b));

    fixture.status.store(503, Ordering::Release);
    let broken = cli(&fixture).env("NO_COLOR", "1").output().unwrap();
    assert_eq!(broken.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&broken.stderr)
            .contains("Broken (1 concurrent requests)")
    );
    assert!(!broken.stderr.contains(&0x1b));

    let forced = cli(&fixture)
        .env_remove("NO_COLOR")
        .env("CLICOLOR_FORCE", "1")
        .output()
        .unwrap();
    assert_eq!(forced.status.code(), Some(1));
    assert!(!forced.stderr.contains(&0x1b));

    let invalid = std::process::Command::new(env!("CARGO_BIN_EXE_brk"))
        .arg("--unknown-option")
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    assert!(!invalid.stderr.contains(&0x1b));
}

#[cfg(all(feature = "cli", unix))]
#[test]
fn cli_ctrl_c_drains_owned_requests_and_exits_130() {
    use std::process::Stdio;
    let fixture = Fixture::new(Mode::Fixed, true);
    let child = cli(&fixture)
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let arrived = fixture.arrivals.recv_timeout(plan().timeout).is_ok();
    let pid = child.id().try_into().unwrap();
    // Signal only the child created by this test, after its handler is installed.
    let signalled = arrived && unsafe { libc::kill(pid, libc::SIGINT) == 0 };
    let result = child.wait_with_output().unwrap();
    fixture.gate.as_ref().unwrap().release();
    assert!(signalled);
    assert_eq!(result.status.code(), Some(130));
    assert!(String::from_utf8_lossy(&result.stderr).contains("Cancelled"));
}
