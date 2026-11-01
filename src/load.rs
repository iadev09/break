use std::time::{Duration, Instant};

/// Client-observed load during admission. Drain is excluded from the integrals.
#[derive(Clone, Copy, Debug, Default)]
pub struct LoadStats {
    /// Time through the last connection owner's admission stop.
    pub active_elapsed: Duration,
    /// Integral of fully written requests awaiting a complete response.
    pub outstanding_request_nanos: u128,
    /// Sum of connection time spent at its requested pipeline depth.
    pub connection_nanos_at_target: u128,
    /// Latest first attainment across owners; not simultaneous global attainment.
    pub latest_first_target: Option<Duration>,
    /// Sum of TCP setup time, overlapping across owners.
    pub connect_nanos: u128,
    pub connect_attempts: u64,
    pub write_blocked: u64,
    /// Longest interval from admitting a request to fully writing it.
    pub max_send_delay: Duration,
    /// Longest delay in observing an expired request deadline.
    pub max_deadline_lateness: Duration,
}

impl LoadStats {
    pub fn mean_outstanding(&self) -> f64 {
        ratio(self.outstanding_request_nanos, self.active_elapsed.as_nanos())
    }

    /// Mean outstanding divided by the declared target, not a pass threshold.
    pub fn target_fraction(
        &self,
        target: usize,
    ) -> f64 {
        if target == 0 { 0.0 } else { self.mean_outstanding() / target as f64 }
    }

    /// Fraction of connection time at full depth; not a global simultaneity claim.
    pub fn connection_target_fraction(
        &self,
        connections: usize,
    ) -> f64 {
        if connections == 0 {
            0.0
        } else {
            ratio(
                self.connection_nanos_at_target,
                self.active_elapsed.as_nanos(),
            ) / connections as f64
        }
    }

    pub(crate) fn add(
        &mut self,
        other: Self,
    ) {
        self.active_elapsed = self.active_elapsed.max(other.active_elapsed);
        self.outstanding_request_nanos += other.outstanding_request_nanos;
        self.connection_nanos_at_target += other.connection_nanos_at_target;
        self.latest_first_target =
            self.latest_first_target.max(other.latest_first_target);
        self.connect_nanos += other.connect_nanos;
        self.connect_attempts += other.connect_attempts;
        self.write_blocked += other.write_blocked;
        self.max_send_delay = self.max_send_delay.max(other.max_send_delay);
        self.max_deadline_lateness =
            self.max_deadline_lateness.max(other.max_deadline_lateness);
    }
}

fn ratio(
    numerator: u128,
    denominator: u128,
) -> f64 {
    if denominator == 0 { 0.0 } else { numerator as f64 / denominator as f64 }
}

pub(crate) struct Meter {
    start: Instant,
    end: Instant,
    last: Instant,
    closed: Option<Instant>,
    depth: usize,
    target: usize,
    pub stats: LoadStats,
}

impl Meter {
    pub fn new(
        start: Instant,
        duration: Duration,
        target: usize,
    ) -> Self {
        Self {
            start,
            end: start + duration,
            last: start,
            closed: None,
            depth: 0,
            target,
            stats: LoadStats::default(),
        }
    }

    pub fn change(
        &mut self,
        depth: usize,
        now: Instant,
    ) {
        let until = now.min(self.closed.unwrap_or(self.end)).max(self.last);
        let elapsed = until.duration_since(self.last).as_nanos();
        self.stats.outstanding_request_nanos += elapsed * self.depth as u128;
        if self.depth == self.target {
            self.stats.connection_nanos_at_target += elapsed;
        }
        self.last = until;
        self.depth = depth;
        if depth == self.target && now < self.end && self.closed.is_none() {
            self.stats
                .latest_first_target
                .get_or_insert(now.duration_since(self.start));
        }
    }

    pub fn close_admission(
        &mut self,
        now: Instant,
    ) {
        if self.closed.is_none() {
            self.change(self.depth, now);
            let end = now.min(self.end);
            self.closed = Some(end);
            self.stats.active_elapsed = end.duration_since(self.start);
        }
    }

    pub fn expired(
        &mut self,
        deadline: Instant,
        now: Instant,
    ) {
        self.stats.max_deadline_lateness = self
            .stats
            .max_deadline_lateness
            .max(now.saturating_duration_since(deadline));
    }

    pub fn finish(
        mut self,
        now: Instant,
    ) -> LoadStats {
        self.close_admission(now);
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_attainment_does_not_claim_sustained_load_and_drain_adds_no_area() {
        let start = Instant::now();
        let mut meter = Meter::new(start, Duration::from_secs(10), 4);
        meter.change(4, start + Duration::from_secs(2));
        meter.change(1, start + Duration::from_secs(3));
        meter.change(0, start + Duration::from_secs(12));
        let stats = meter.finish(start + Duration::from_secs(12));
        assert_eq!(stats.active_elapsed, Duration::from_secs(10));
        assert_eq!(stats.latest_first_target, Some(Duration::from_secs(2)));
        assert_eq!(stats.outstanding_request_nanos, 11_000_000_000);
        assert_eq!(stats.connection_nanos_at_target, 1_000_000_000);
        assert_eq!(stats.mean_outstanding(), 1.1);
        assert_eq!(stats.target_fraction(4), 0.275);
    }

    #[test]
    fn cancelling_admission_freezes_area_while_pending_work_drains() {
        let start = Instant::now();
        let mut meter = Meter::new(start, Duration::from_secs(10), 4);
        meter.change(4, start + Duration::from_secs(1));
        meter.close_admission(start + Duration::from_secs(3));
        meter.change(0, start + Duration::from_secs(8));
        let stats = meter.finish(start + Duration::from_secs(8));
        assert_eq!(stats.active_elapsed, Duration::from_secs(3));
        assert_eq!(stats.outstanding_request_nanos, 8_000_000_000);
    }

    #[test]
    fn owners_reaching_depth_at_different_times_do_not_claim_full_global_load()
    {
        let start = Instant::now();
        let end = start + Duration::from_secs(10);
        let mut first = Meter::new(start, Duration::from_secs(10), 2);
        first.change(2, start);
        first.change(0, start + Duration::from_secs(5));
        let mut second = Meter::new(start, Duration::from_secs(10), 2);
        second.change(2, start + Duration::from_secs(5));
        let mut stats = first.finish(end);
        stats.add(second.finish(end));
        assert_eq!(stats.mean_outstanding(), 2.0);
        assert_eq!(stats.target_fraction(4), 0.5);
        assert_eq!(stats.connection_target_fraction(2), 0.5);
        assert_eq!(stats.latest_first_target, Some(Duration::from_secs(5)));
    }
}
