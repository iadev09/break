use super::*;

#[test]
fn no_completed_response_has_no_mean() {
    assert_eq!(LatencyStats::default().mean_millis(), None);
}

#[test]
fn aggregation_weights_responses_instead_of_averaging_owner_means() {
    let mut first = LatencyStats::default();
    first.record(Duration::from_millis(1));
    first.record(Duration::from_millis(3));
    let mut second = LatencyStats::default();
    second.record(Duration::from_millis(8));
    first.add(second);
    assert_eq!(first.responses, 3);
    assert_eq!(first.total_nanos, 12_000_000);
    assert_eq!(first.mean_millis(), Some(4.0));
}
