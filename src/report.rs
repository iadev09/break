use std::time::Duration;

use crate::{Acceptance, LoadStats};

/// Completed logical attempts, including failures, belonging to one phase.
#[derive(Clone, Copy, Debug, Default)]
pub struct Counts {
    pub sent: u64,
    pub attempted: u64,
    pub successful: u64,
    pub timeout: u64,
    pub transport: u64,
    pub status: u64,
    pub body: u64,
    /// Pending attempts abandoned with a retired connection before their deadline.
    pub abandoned: u64,
    /// Failed TCP setup attempts, separate from local resource failures.
    pub connect: u64,
    pub connect_timeout: u64,
}

impl Counts {
    pub fn failed(self) -> u64 {
        self.attempted - self.successful
    }

    pub(crate) fn add(
        &mut self,
        other: Self,
    ) {
        self.sent += other.sent;
        self.attempted += other.attempted;
        self.successful += other.successful;
        self.timeout += other.timeout;
        self.transport += other.transport;
        self.status += other.status;
        self.body += other.body;
        self.abandoned += other.abandoned;
        self.connect += other.connect;
        self.connect_timeout += other.connect_timeout;
    }
}

/// Phase zero starts at start; each later phase adds pace per connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Phase {
    pub index: usize,
    pub connections: usize,
    pub requests_per_connection: usize,
    pub total_concurrency: usize,
    pub duration: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct PhaseStats {
    pub phase: Phase,
    pub counts: Counts,
    /// Includes drain of all attempts admitted before the phase ended.
    pub elapsed: Duration,
    /// Owners that reached the requested depth at least once during admission.
    /// This does not assert simultaneous global or sustained attainment.
    pub connections_at_target: usize,
    pub load: LoadStats,
    /// False when execution was interrupted by a setup/execution/protocol failure.
    pub completed: bool,
    pub cancelled: bool,
}

impl PhaseStats {
    /// Time after admission stopped, including response drain and result collection.
    pub fn drain_elapsed(&self) -> Duration {
        self.elapsed.saturating_sub(self.load.active_elapsed)
    }

    pub fn accepted(
        &self,
        acceptance: Acceptance,
    ) -> bool {
        self.completed
            && !self.cancelled
            && self.connections_at_target == self.phase.connections
            && acceptance.accepts(self.counts.successful, self.counts.attempted)
    }

    pub fn requests_per_sec(&self) -> f64 {
        self.counts.successful as f64 / self.elapsed.as_secs_f64()
    }
}

/// Bounded history. Acceptance uses fresh phase counts, never cumulative totals.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub cpu_workers: usize,
    pub phases: usize,
    pub total: Counts,
    pub last_accepted: Option<PhaseStats>,
    pub last_phase: Option<PhaseStats>,
}

#[derive(Debug)]
pub struct Done {
    pub stats: Stats,
}

#[derive(Debug, thiserror::Error)]
pub enum BreakReason {
    #[error("invalid plan: {0}")]
    Invalid(&'static str),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("client resources: {0}")]
    Resources(&'static str),
    #[error("HTTP protocol: {0}")]
    Protocol(&'static str),
    #[error("client worker: {0}")]
    Worker(&'static str),
    #[error("the declared concurrency was not established")]
    Unfulfilled,
    #[error("the phase exceeded the HTTP acceptance budget")]
    Acceptance,
    #[error("concurrency representation exhausted")]
    Overflow,
    #[error("cancelled")]
    Cancelled,
}

#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct Broken {
    pub reason: BreakReason,
    pub stats: Box<Stats>,
}

impl Broken {
    /// An observed rejection following a passing level, not a confidence interval.
    pub fn has_capacity_boundary(&self) -> bool {
        matches!(self.reason, BreakReason::Acceptance)
            && self.stats.last_accepted.is_some()
    }

    pub fn connection_failed(&self) -> bool {
        self.stats.last_phase.is_some_and(|phase| {
            phase.counts.connect > 0 || phase.counts.connect_timeout > 0
        })
    }
}

/// Runs on the caller at phase boundaries, never on the request path.
pub trait Observer {
    /// Workers are ready but no request from this phase has been sent.
    fn phase_started(
        &mut self,
        _phase: &Phase,
    ) {
    }

    /// Every admitted attempt has completed, failed or reached its timeout.
    fn phase_finished(
        &mut self,
        _stats: &PhaseStats,
    ) {
    }
}

impl Observer for () {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn perfect_status_counts_do_not_pass_when_concurrency_was_not_established()
    {
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
            completed: true,
            cancelled: false,
        };
        let accept = "99.99".parse().unwrap();
        assert!(!measured.accepted(accept));
        measured.connections_at_target = 4;
        assert!(measured.accepted(accept));
        measured.counts.successful = 9_998;
        assert!(!measured.accepted(accept));
    }
}
