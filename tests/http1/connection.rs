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
