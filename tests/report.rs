use super::*;
#[test]
fn perfect_status_counts_do_not_pass_when_concurrency_was_not_established() {
    let mut measured = PhaseStats {
        phase: Phase {
            index: 0,
            connections: 4,
            requests_per_connection: 100,
            total_concurrency: 400,
            duration: Duration::from_secs(1),
        },
        counts: Counts {
            attempted: 10_000,
            successful: 10_000,
            ..Counts::default()
        },
        elapsed: Duration::from_secs(1),
        connections_at_target: 3,
        load: LoadStats::default(),
        latency: LatencyStats::default(),
        setups: None,
        tls: None,
        completed: true,
        cancelled: false,
        machine: None,
    };
    let accept = "99.99".parse().unwrap();
    assert!(!measured.accepted(accept));
    measured.connections_at_target = 4;
    assert!(measured.accepted(accept));
    measured.counts.successful = 9_998;
    assert!(!measured.accepted(accept));
}
