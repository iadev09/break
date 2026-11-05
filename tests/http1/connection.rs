use super::*;

#[test]
fn timeout_retirement_separates_expired_attempts_from_unexpired_followers() {
    let start = Instant::now();
    let mut meter = Meter::new(start, Duration::from_secs(10), 3);
    let mut counts = Counts::default();
    retire_attempts(
        [1, 5, 6].into_iter().map(|seconds| Attempt {
            deadline: start + Duration::from_secs(seconds),
            began: start,
        }),
        &mut counts,
        Outcome::Abandoned,
        &mut meter,
        start + Duration::from_secs(2),
    );
    assert_eq!(counts.attempted, 3);
    assert_eq!(counts.timeout, 1);
    assert_eq!(counts.abandoned, 2);
    assert_eq!(counts.transport, 0);
}

#[test]
fn a_transport_incident_does_not_become_one_independent_fault_per_pending_request()
 {
    let start = Instant::now();
    let mut meter = Meter::new(start, Duration::from_secs(10), 3);
    let mut counts = Counts::default();
    retire_attempts(
        [5, 6, 7].into_iter().map(|seconds| Attempt {
            deadline: start + Duration::from_secs(seconds),
            began: start,
        }),
        &mut counts,
        Outcome::Transport,
        &mut meter,
        start + Duration::from_secs(2),
    );
    assert_eq!(counts.attempted, 3);
    assert_eq!(counts.transport, 1);
    assert_eq!(counts.abandoned, 2);
    assert_eq!(counts.timeout, 0);
}

#[test]
fn request_latency_excludes_setup_and_preserves_the_original_deadline() {
    let setup_began = Instant::now();
    let request_began = setup_began + Duration::from_millis(50);
    let attempt = Attempt {
        deadline: setup_began + Duration::from_millis(100),
        began: request_began,
    };
    let mut latency = LatencyStats::default();
    attempt
        .record_response(setup_began + Duration::from_millis(70), &mut latency);
    assert_eq!(latency.mean_millis(), Some(20.0));
    assert_eq!(attempt.deadline, setup_began + Duration::from_millis(100));
}

#[test]
fn late_complete_responses_have_a_latency_sample_but_retired_attempts_do_not() {
    let start = Instant::now();
    let attempt =
        Attempt { deadline: start + Duration::from_millis(10), began: start };
    let mut latency = LatencyStats::default();
    let completed = start + Duration::from_millis(15);
    attempt.record_response(completed, &mut latency);
    let mut counts = Counts::default();
    let mut meter = Meter::new(start, Duration::from_secs(1), 1);
    retire_attempts(
        std::iter::once(attempt),
        &mut counts,
        Outcome::Transport,
        &mut meter,
        completed,
    );
    assert_eq!(counts.timeout, 1);
    assert_eq!(latency.responses, 1);
    assert_eq!(latency.mean_millis(), Some(15.0));
}

#[test]
fn a_reply_waiting_in_the_socket_past_its_deadline_is_read_not_abandoned() {
    use std::io::Write as _;
    use std::net::{TcpListener, TcpStream as Blocking};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let socket = Blocking::connect(address).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    peer.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok").unwrap();
    // The reply is in the socket before the connection is turned.
    socket.peek(&mut [0]).unwrap();
    socket.set_nonblocking(true).unwrap();

    let client = crate::Target::new(&format!("http://{address}/plain"))
        .unwrap()
        .resolve()
        .unwrap()
        .client();
    let now = Instant::now();
    // The phase is over and the request's deadline has passed.
    let window = Window {
        per_connection: 1,
        start: now - Duration::from_secs(10),
        duration: Duration::from_secs(1),
        timeout: Duration::from_millis(100),
        lifetime: None,
    };
    let mut connection = Conn::new(
        Stream::Plain(TcpStream::from_std(socket)),
        &client,
        window,
        now,
    )
    .unwrap();
    connection.reserve(window).unwrap();
    connection.pending.push_back(Attempt {
        deadline: now - Duration::from_secs(1),
        began: now - Duration::from_secs(2),
    });
    let mut timers = VecDeque::new();
    let mut turn = Turn {
        client: &client,
        window,
        halted: false,
        timers: &mut timers,
        owner: 0,
    };
    let mut flags = Flags { readable: true, writable: true, ended: false };
    let mut summary = Summary::default();
    let mut meter = Meter::new(window.start, window.duration, 1);

    let step =
        connection.exchange(&mut turn, &mut flags, &mut summary, &mut meter);

    // A late reply: counted as one, timed, and the connection is kept.
    assert!(matches!(step, Step::End(End::Retain)));
    assert_eq!(summary.counts.timeout, 1);
    assert_eq!(summary.counts.abandoned, 0);
    assert!(summary.latency.mean_millis().is_some());
}
