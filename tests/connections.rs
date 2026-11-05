//! The connection test against a real listener: each connection carries its
//! declared requests, asks to close on the last and is replaced, and a setup
//! that fails is this test's outcome rather than a request failure.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use brk::{BreakReason, CancellationToken, ConnectionPlan, Target};

#[derive(Default)]
struct Seen {
    connections: AtomicUsize,
    requests: AtomicUsize,
    /// Requests that asked the server to close the connection.
    closing: AtomicUsize,
    /// Connections that carried anything after their closing request.
    overrun: AtomicUsize,
}

struct Fixture {
    address: SocketAddr,
    seen: Arc<Seen>,
    stopping: Arc<AtomicBool>,
    thread: Option<JoinHandle<Vec<JoinHandle<()>>>>,
}

impl Fixture {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut fixture = Self {
            address: listener.local_addr().unwrap(),
            seen: Arc::default(),
            stopping: Arc::default(),
            thread: None,
        };
        let seen = fixture.seen.clone();
        let stopping = fixture.stopping.clone();
        fixture.thread = Some(thread::spawn(move || {
            let mut handlers = Vec::new();
            for accepted in listener.incoming() {
                let stream = accepted.unwrap();
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                seen.connections.fetch_add(1, Ordering::Relaxed);
                let seen = seen.clone();
                handlers.push(thread::spawn(move || serve(stream, &seen)));
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
        self.stopping.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address); // Wake the accept owner, without polling.
        if let Some(thread) = self.thread.take() {
            for handler in thread.join().unwrap() {
                handler.join().unwrap();
            }
        }
    }
}

/// Answer each request; close after the one that asks for it.
fn serve(
    mut stream: TcpStream,
    seen: &Seen,
) {
    let mut buffered = Vec::new();
    let mut closed = false;
    loop {
        let mut bytes = [0; 4096];
        let count = match stream.read(&mut bytes) {
            Ok(0) | Err(_) => break,
            Ok(count) => count,
        };
        if closed {
            seen.overrun.fetch_add(1, Ordering::Relaxed);
            break;
        }
        buffered.extend_from_slice(&bytes[..count]);
        while let Some(end) =
            buffered.windows(4).position(|bytes| bytes == b"\r\n\r\n")
        {
            let head = buffered.drain(..end + 4).collect::<Vec<_>>();
            seen.requests.fetch_add(1, Ordering::Relaxed);
            let closing =
                head.windows(17).any(|bytes| bytes == b"Connection: close");
            let reply: &[u8] = if closing {
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok"
            } else {
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
            };
            if stream.write_all(reply).is_err() {
                return;
            }
            if closing {
                seen.closing.fetch_add(1, Ordering::Relaxed);
                closed = true;
                let _ = stream.shutdown(std::net::Shutdown::Write);
            }
        }
    }
}

fn plan() -> ConnectionPlan {
    ConnectionPlan {
        start: 2,
        pace: 2,
        requests: 3,
        phase_duration: Duration::from_millis(100),
        timeout: Duration::from_secs(2),
        accept: 99.99,
        max_owners: Some(4),
        threads: None,
    }
}

#[test]
fn each_connection_carries_its_requests_then_asks_to_close_and_is_replaced() {
    let fixture = Fixture::new();
    let done = brk::run_connections(
        &fixture.target(),
        plan(),
        &CancellationToken::new(),
        &mut (),
    )
    .unwrap();
    assert_eq!(done.stats.phases, 2);
    let last = done.stats.last_accepted.unwrap();
    assert_eq!(last.phase.connections, 4);
    assert_eq!(last.phase.requests_per_connection, 1);
    let setups = last.setups.unwrap();
    assert!(setups.established > 4, "owners replace their connections");
    assert_eq!(setups.established, setups.attempted);
    assert_eq!(done.stats.total.failed(), 0);

    let seen = &fixture.seen;
    let connections = seen.connections.load(Ordering::Acquire);
    let requests = seen.requests.load(Ordering::Acquire);
    let closing = seen.closing.load(Ordering::Acquire);
    assert_eq!(requests as u64, done.stats.total.attempted);
    // A connection that was closed carried exactly three requests; only the
    // connections a phase end interrupted carried fewer.
    assert!(closing > 0);
    assert!(requests >= 3 * closing);
    assert!(requests < 3 * connections + 1);
    assert!(connections - closing <= 2 + 4, "{connections} {closing}");
    assert_eq!(seen.overrun.load(Ordering::Acquire), 0);
}

#[test]
fn a_refused_setup_breaks_the_connection_test_and_counts_no_request() {
    // A bound address that nothing accepts on any more.
    let address =
        TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let target = Target::new(&format!("http://{address}/plain")).unwrap();
    let broken = brk::run_connections(
        &target,
        plan(),
        &CancellationToken::new(),
        &mut (),
    )
    .unwrap_err();
    assert!(matches!(broken.reason, BreakReason::Connections));
    let phase = broken.stats.last_phase.unwrap();
    let setups = phase.setups.unwrap();
    assert!(setups.attempted > 0);
    assert_eq!(setups.established, 0);
    assert_eq!(setups.failed, setups.attempted);
    assert_eq!(phase.counts.attempted, 0);
    assert!(!broken.connection_failed());
    assert!(!broken.has_capacity_boundary());
}
