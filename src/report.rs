use std::time::Duration;

use crate::{Acceptance, LatencyStats, LoadStats};

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

/// Connection setups of one phase in the connection test, counted apart from
/// requests. A setup is the connect call through the end of the handshake.
#[derive(Clone, Copy, Debug, Default)]
pub struct Setups {
    pub attempted: u64,
    pub established: u64,
    /// Refused, reset or otherwise failed by the peer or the network.
    pub failed: u64,
    /// Not established within the declared deadline.
    pub timeout: u64,
    /// Established connections whose TLS handshake resumed an earlier session.
    pub resumed: u64,
    /// Sum of the setup time of established connections.
    pub established_nanos: u128,
    /// Longest setup of an established connection.
    pub longest: Duration,
}

impl Setups {
    pub fn mean_millis(&self) -> Option<f64> {
        (self.established != 0).then(|| {
            self.established_nanos as f64 / self.established as f64 / 1e6
        })
    }

    pub(crate) fn add(
        &mut self,
        other: Self,
    ) {
        self.attempted += other.attempted;
        self.established += other.established;
        self.failed += other.failed;
        self.timeout += other.timeout;
        self.resumed += other.resumed;
        self.established_nanos += other.established_nanos;
        self.longest = self.longest.max(other.longest);
    }
}

/// Phase zero starts at start; each later phase adds pace. The request test
/// raises requests per connection; the connection test raises connections.
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
    pub latency: LatencyStats,
    /// The connection test's own outcomes; `None` in the request test, where
    /// connections are opened once and a failed setup is not a measurement.
    pub setups: Option<Setups>,
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
            && self.connections_accepted(acceptance)
            && acceptance.accepts(self.counts.successful, self.counts.attempted)
    }

    /// The request test needs every connection at its depth; the connection
    /// test needs its setups within the acceptance budget.
    pub fn connections_accepted(
        &self,
        acceptance: Acceptance,
    ) -> bool {
        match self.setups {
            None => self.connections_at_target == self.phase.connections,
            Some(setups) => {
                acceptance.accepts(setups.established, setups.attempted)
            }
        }
    }

    pub fn connections_per_sec(&self) -> f64 {
        self.setups.map_or(0.0, |setups| {
            setups.established as f64 / self.elapsed.as_secs_f64()
        })
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
    #[error("the phase exceeded the connection acceptance budget")]
    Connections,
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
        matches!(
            self.reason,
            BreakReason::Acceptance | BreakReason::Connections
        ) && self.stats.last_accepted.is_some()
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
#[path = "../tests/report.rs"]
mod tests;
