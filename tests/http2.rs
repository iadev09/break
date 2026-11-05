//! Both tests over HTTP/2 against a real server, in cleartext with prior
//! knowledge and over TLS with `h2` negotiated.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use brk::{BreakReason, CancellationToken, ConnectionPlan, Plan, Target};
use bytes::Bytes;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

const NAME: &str = "example.test";

struct Fixture {
    address: SocketAddr,
    /// The TLS listener's certificate, which is its own authority.
    authority: Option<String>,
    connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    stop: CancellationToken,
    thread: Option<JoinHandle<()>>,
}

/// Answer every stream of one connection with `ok`.
async fn serve<I: AsyncRead + AsyncWrite + Unpin>(
    io: I,
    limit: Option<u32>,
    requests: Arc<AtomicUsize>,
) {
    let mut builder = h2::server::Builder::new();
    if let Some(limit) = limit {
        builder.max_concurrent_streams(limit);
    }
    let Ok(mut connection) = builder.handshake::<_, Bytes>(io).await else {
        return;
    };
    while let Some(Ok((_request, mut respond))) = connection.accept().await {
        requests.fetch_add(1, Ordering::Relaxed);
        let response = http::Response::builder().status(200).body(()).unwrap();
        if let Ok(mut send) = respond.send_response(response, false) {
            let _ = send.send_data(Bytes::from_static(b"ok"), true);
        }
    }
}

impl Fixture {
    fn new(
        secure: bool,
        limit: Option<u32>,
    ) -> Self {
        let acceptor = secure.then(|| {
            let identity =
                rcgen::generate_simple_self_signed(vec![NAME.to_owned()])
                    .unwrap();
            let mut config = rustls::ServerConfig::builder_with_provider(
                Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
            )
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![identity.cert.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                    identity.signing_key.serialize_der(),
                )),
            )
            .unwrap();
            config.alpn_protocols = vec![b"h2".to_vec()];
            (
                tokio_rustls::TlsAcceptor::from(Arc::new(config)),
                identity.cert.pem(),
            )
        });
        let authority = acceptor.as_ref().map(|(_, pem)| pem.clone());
        let acceptor = acceptor.map(|(acceptor, _)| acceptor);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut fixture = Self {
            address: listener.local_addr().unwrap(),
            authority,
            connections: Arc::default(),
            requests: Arc::default(),
            stop: CancellationToken::new(),
            thread: None,
        };
        let connections = fixture.connections.clone();
        let requests = fixture.requests.clone();
        let stop = fixture.stop.clone();
        fixture.thread = Some(thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = TcpListener::from_std(listener).unwrap();
                    loop {
                        let stream = tokio::select! {
                            () = stop.cancelled() => break,
                            accepted = listener.accept() => accepted.unwrap().0,
                        };
                        connections.fetch_add(1, Ordering::Relaxed);
                        let requests = requests.clone();
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor {
                                None => serve(stream, limit, requests).await,
                                Some(acceptor) => {
                                    if let Ok(stream) =
                                        acceptor.accept(stream).await
                                    {
                                        serve(stream, limit, requests).await;
                                    }
                                }
                            }
                        });
                    }
                });
        }));
        fixture
    }

    fn target(&self) -> Target {
        let target = match &self.authority {
            None => Target::new(&format!("http://{NAME}/plain")).unwrap(),
            Some(authority) => Target::new(&format!("https://{NAME}/plain"))
                .unwrap()
                .trust_pem(authority.as_bytes())
                .unwrap(),
        };
        target.connect_to(self.address).http2().expect_body("ok")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.cancel();
        self.thread.take().unwrap().join().unwrap();
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
        max_concurrency: Some(8),
    }
}

#[test]
fn the_request_test_raises_concurrent_streams_over_cleartext_and_tls() {
    for secure in [false, true] {
        let fixture = Fixture::new(secure, None);
        let done = brk::run(
            &fixture.target(),
            plan(),
            &CancellationToken::new(),
            &mut (),
        )
        .unwrap();
        assert_eq!(done.stats.phases, 3, "{secure}");
        let last = done.stats.last_accepted.unwrap();
        assert_eq!(last.phase.requests_per_connection, 8, "{secure}");
        assert_eq!(last.connections_at_target, 2, "{secure}");
        assert!(done.stats.total.successful > 16, "{secure}");
        assert_eq!(done.stats.total.failed(), 0, "{secure}");
        assert_eq!(
            fixture.requests.load(Ordering::Acquire) as u64,
            done.stats.total.attempted,
            "{secure}"
        );
        // Streams multiply on the two connections; no connection is added.
        assert_eq!(fixture.connections.load(Ordering::Acquire), 2, "{secure}");
        assert_eq!(last.tls.is_some(), secure);
    }
}

#[test]
fn a_peer_stream_limit_below_the_level_leaves_the_phase_unfulfilled() {
    let fixture = Fixture::new(false, Some(2));
    let plan = Plan { start: 4, max_concurrency: Some(4), ..plan() };
    let broken =
        brk::run(&fixture.target(), plan, &CancellationToken::new(), &mut ())
            .unwrap_err();
    let phase = broken.stats.last_phase.unwrap();
    assert!(
        matches!(broken.reason, BreakReason::Unfulfilled),
        "{:?} {:?} at_target={}",
        broken.reason,
        phase.counts,
        phase.connections_at_target
    );
    assert_eq!(phase.connections_at_target, 0);
    assert!(phase.counts.successful > 0);
    assert_eq!(phase.counts.failed(), 0);
    assert!(!broken.has_capacity_boundary());
}

#[test]
fn the_connection_test_replaces_connections_after_their_streams() {
    for secure in [false, true] {
        let fixture = Fixture::new(secure, None);
        let plan = ConnectionPlan {
            start: 2,
            pace: 2,
            requests: 3,
            phase_duration: Duration::from_millis(100),
            timeout: Duration::from_secs(2),
            accept: 99.99,
            max_owners: Some(2),
        };
        let done = brk::run_connections(
            &fixture.target(),
            plan,
            &CancellationToken::new(),
            &mut (),
        )
        .unwrap();
        let setups = done.stats.last_accepted.unwrap().setups.unwrap();
        assert!(setups.established > 2, "{secure}");
        assert_eq!(setups.established, setups.attempted, "{secure}");
        assert_eq!(done.stats.total.failed(), 0, "{secure}");
        let requests = fixture.requests.load(Ordering::Acquire) as u64;
        assert_eq!(requests, done.stats.total.attempted, "{secure}");
        // Each replaced connection carried exactly three streams; only the
        // two a phase end interrupted may have carried fewer, down to none.
        assert!(requests <= 3 * setups.established, "{secure}");
        assert!(requests + 6 >= 3 * setups.established, "{secure}");
    }
}
