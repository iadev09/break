//! A real TLS listener exercises the handshake, pipelining inside records and
//! the choice of trusted authority. No deployment certificate is used: each
//! fixture makes a throwaway one.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use brk::{CancellationToken, ConnectionPlan, Plan, Target};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

const NAME: &str = "example.test";

struct Fixture {
    address: SocketAddr,
    /// The certificate the listener presents, which is also its own authority.
    authority: String,
    stopping: Arc<AtomicBool>,
    requests: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    thread: Option<JoinHandle<Vec<JoinHandle<()>>>>,
}

fn certificate() -> rcgen::CertifiedKey<rcgen::KeyPair> {
    rcgen::generate_simple_self_signed(vec![NAME.to_owned()]).unwrap()
}

impl Fixture {
    fn new() -> Self {
        let identity = certificate();
        let config = Arc::new(
            rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![identity.cert.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                    identity.signing_key.serialize_der(),
                )),
            )
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut fixture = Self {
            address: listener.local_addr().unwrap(),
            authority: identity.cert.pem(),
            stopping: Arc::default(),
            requests: Arc::default(),
            connections: Arc::default(),
            thread: None,
        };
        let stopping = fixture.stopping.clone();
        let requests = fixture.requests.clone();
        let connections = fixture.connections.clone();
        fixture.thread = Some(thread::spawn(move || {
            let mut handlers = Vec::new();
            for accepted in listener.incoming() {
                let stream = accepted.unwrap();
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                connections.fetch_add(1, Ordering::Relaxed);
                let config = config.clone();
                let requests = requests.clone();
                handlers.push(thread::spawn(move || {
                    serve(stream, config, requests)
                }));
            }
            handlers
        }));
        fixture
    }

    fn target(&self) -> Target {
        Target::new(&format!("https://{NAME}/plain"))
            .unwrap()
            .connect_to(self.address)
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

/// Answer every complete request head on one connection, in order.
fn serve(
    stream: TcpStream,
    config: Arc<rustls::ServerConfig>,
    requests: Arc<AtomicUsize>,
) {
    let session = rustls::ServerConnection::new(config).unwrap();
    let mut stream = rustls::StreamOwned::new(session, stream);
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
            let head = buffered.drain(..end + 4).collect::<Vec<_>>();
            assert!(
                head.windows(NAME.len() + 6)
                    .any(|bytes| bytes == format!("Host: {NAME}").as_bytes()),
                "the request names the URL's host, not the connected address"
            );
            requests.fetch_add(1, Ordering::Relaxed);
            if stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .is_err()
            {
                return;
            }
        }
    }
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

#[test]
fn tls_carries_the_same_pipelined_requests_to_the_named_host() {
    // The deep pipeline answers with more decrypted bytes than one read takes.
    for depth in [2, 1024] {
        let fixture = Fixture::new();
        let target =
            fixture.target().trust_pem(fixture.authority.as_bytes()).unwrap();
        let plan =
            Plan { start: depth, max_concurrency: Some(depth), ..plan() };
        let done = brk::run(&target, plan, &CancellationToken::new(), &mut ())
            .unwrap();
        assert!(done.stats.total.successful as usize > 2 * depth, "{depth}");
        assert_eq!(done.stats.total.failed(), 0, "{depth}");
        assert_eq!(
            fixture.requests.load(Ordering::Acquire) as u64,
            done.stats.total.attempted,
            "{depth}"
        );
        assert_eq!(fixture.connections.load(Ordering::Acquire), 2, "{depth}");
    }
}

#[test]
fn an_untrusted_authority_fails_setup_instead_of_counting_as_load() {
    let fixture = Fixture::new();
    let other = certificate().cert.pem();
    let target = fixture.target().trust_pem(other.as_bytes()).unwrap();
    let broken = brk::run(&target, plan(), &CancellationToken::new(), &mut ())
        .unwrap_err();
    assert!(broken.connection_failed());
    assert_eq!(broken.stats.total.successful, 0);
    assert_eq!(fixture.requests.load(Ordering::Acquire), 0);
}

#[test]
fn a_returning_owner_resumes_its_own_session_unless_told_not_to() {
    for resume in [true, false] {
        let fixture = Fixture::new();
        let target =
            fixture.target().trust_pem(fixture.authority.as_bytes()).unwrap();
        let target = if resume { target } else { target.without_resumption() };
        let plan = ConnectionPlan {
            start: 2,
            pace: 2,
            requests: 1,
            phase_duration: Duration::from_millis(100),
            timeout: Duration::from_secs(2),
            accept: 99.99,
            max_owners: Some(2),
        };
        let done = brk::run_connections(
            &target,
            plan,
            &CancellationToken::new(),
            &mut (),
        )
        .unwrap();
        let setups = done.stats.last_accepted.unwrap().setups.unwrap();
        assert!(setups.established > 2, "{resume}");
        assert_eq!(done.stats.total.failed(), 0, "{resume}");
        if resume {
            // Each of the two owners has nothing to resume the first time.
            assert!(setups.resumed > 0);
            assert!(setups.resumed <= setups.established - 2);
        } else {
            assert_eq!(setups.resumed, 0);
        }
    }
}
