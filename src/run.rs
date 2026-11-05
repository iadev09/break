use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::worker::{Churn, Command, Workers, receive};
use crate::{
    Acceptance, BreakReason, Broken, CancellationToken, Counts, Done,
    LatencyStats, LoadStats, Observer, Phase, PhaseStats, Setups, Stats,
    Target,
};

#[derive(Clone, Copy, Debug)]
pub struct Plan {
    pub conn: usize,
    pub start: usize,
    pub pace: usize,
    pub phase_duration: Duration,
    pub timeout: Duration,
    pub accept: f64,
    /// Optional per-connection ceiling; it does not assert a server breakpoint.
    pub max_concurrency: Option<usize>,
}

impl Plan {
    pub fn validate(&self) -> Result<Acceptance, BreakReason> {
        if self.conn == 0 || self.start == 0 || self.pace == 0 {
            return Err(BreakReason::Invalid(
                "conn, start and pace must be greater than zero",
            ));
        }
        self.conn.checked_mul(self.start).ok_or(BreakReason::Overflow)?;
        self.ramp().validate(self.accept)
    }

    fn ramp(&self) -> Ramp {
        Ramp {
            raises: Raises::Depth { connections: self.conn },
            start: self.start,
            pace: self.pace,
            max: self.max_concurrency,
            phase_duration: self.phase_duration,
            timeout: self.timeout,
        }
    }
}

/// The connection test. Each owner establishes a connection, sends exactly
/// `requests` requests one at a time, lets the connection end and establishes
/// the next. Phases raise the number of owners; setups are judged on their own.
#[derive(Clone, Copy, Debug)]
pub struct ConnectionPlan {
    /// Connection owners in phase zero.
    pub start: usize,
    /// Additional owners in each later phase.
    pub pace: usize,
    /// Exact number of requests each connection carries.
    pub requests: usize,
    pub phase_duration: Duration,
    /// Deadline of a setup, and separately of each request.
    pub timeout: Duration,
    pub accept: f64,
    /// Optional ceiling on owners; it does not assert a server breakpoint.
    pub max_owners: Option<usize>,
}

impl ConnectionPlan {
    pub fn validate(&self) -> Result<Acceptance, BreakReason> {
        if self.start == 0 || self.pace == 0 || self.requests == 0 {
            return Err(BreakReason::Invalid(
                "start, pace and requests must be greater than zero",
            ));
        }
        self.ramp().validate(self.accept)
    }

    fn ramp(&self) -> Ramp {
        Ramp {
            raises: Raises::Owners { requests: self.requests },
            start: self.start,
            pace: self.pace,
            max: self.max_owners,
            phase_duration: self.phase_duration,
            timeout: self.timeout,
        }
    }
}

/// What each phase raises, and what stays fixed beside it.
#[derive(Clone, Copy)]
enum Raises {
    /// Requests per connection, over connections that stay open.
    Depth { connections: usize },
    /// Connection owners, each connection carrying a fixed number of requests.
    Owners { requests: usize },
}

#[derive(Clone, Copy)]
struct Ramp {
    raises: Raises,
    start: usize,
    pace: usize,
    max: Option<usize>,
    phase_duration: Duration,
    timeout: Duration,
}

impl Ramp {
    fn validate(
        &self,
        accept: f64,
    ) -> Result<Acceptance, BreakReason> {
        if self.phase_duration.is_zero() || self.timeout.is_zero() {
            return Err(BreakReason::Invalid(
                "phase and timeout must be greater than zero",
            ));
        }
        if self.max.is_some_and(|max| max < self.start) {
            return Err(BreakReason::Invalid(
                "max must admit the initial start level",
            ));
        }
        Instant::now()
            .checked_add(self.phase_duration)
            .and_then(|end| end.checked_add(self.timeout))
            .ok_or(BreakReason::Invalid(
                "time budgets exceed the clock range",
            ))?;
        Acceptance::try_from(accept).map_err(BreakReason::Invalid)
    }
}

/// Synchronous owner of CPU workers. Phase callbacks run on the caller, with
/// workers gated before start and fully drained before finish. No process-global
/// signal handler is installed. Caller cancellation never detaches owned work.
pub fn run(
    target: &Target,
    plan: Plan,
    cancellation: &CancellationToken,
    observer: &mut (impl Observer + ?Sized),
) -> Result<Done, Broken> {
    finish(plan.validate(), plan.ramp(), target, cancellation, observer)
}

/// The connection test, with the same ownership and callbacks as [`run`].
/// A phase is accepted when both its setups and its requests are.
pub fn run_connections(
    target: &Target,
    plan: ConnectionPlan,
    cancellation: &CancellationToken,
    observer: &mut (impl Observer + ?Sized),
) -> Result<Done, Broken> {
    finish(plan.validate(), plan.ramp(), target, cancellation, observer)
}

fn finish(
    acceptance: Result<Acceptance, BreakReason>,
    ramp: Ramp,
    target: &Target,
    cancellation: &CancellationToken,
    observer: &mut (impl Observer + ?Sized),
) -> Result<Done, Broken> {
    let mut stats = Stats::default();
    let result = acceptance.and_then(|acceptance| {
        execute(target, ramp, acceptance, cancellation, observer, &mut stats)
    });
    match result {
        Ok(()) => Ok(Done { stats }),
        Err(reason) => Err(Broken { reason, stats: Box::new(stats) }),
    }
}

fn execute(
    target: &Target,
    ramp: Ramp,
    acceptance: Acceptance,
    cancellation: &CancellationToken,
    observer: &mut (impl Observer + ?Sized),
    stats: &mut Stats,
) -> Result<(), BreakReason> {
    if cancellation.is_cancelled() {
        return Err(BreakReason::Cancelled);
    }
    let prepared = target.resolve()?;
    let stop = cancellation.child_token();
    // Kept connections belong to the request test. The connection test's
    // owners are bounded only by its ceiling, when one is declared.
    let (owners, kept) = match ramp.raises {
        Raises::Depth { connections } => (connections, connections),
        Raises::Owners { .. } => (ramp.max.unwrap_or(usize::MAX), 0),
    };
    let workers = Workers::start(&prepared, owners, kept, stop.clone())?;
    stats.cpu_workers = workers.len();
    let result = phases(&workers, ramp, acceptance, &stop, observer, stats);
    let finished = workers.finish();
    result.and(finished)
}

fn phases(
    workers: &Workers,
    plan: Ramp,
    acceptance: Acceptance,
    stop: &CancellationToken,
    observer: &mut (impl Observer + ?Sized),
    stats: &mut Stats,
) -> Result<(), BreakReason> {
    let mut index = 0_usize;
    let mut level = plan.start;
    loop {
        if stop.is_cancelled() {
            return Err(BreakReason::Cancelled);
        }
        let (connections, depth, lifetime) = match plan.raises {
            Raises::Depth { connections } => (connections, level, None),
            // One request at a time: pipeline depth is the request test's.
            Raises::Owners { requests } => (level, 1, Some(requests)),
        };
        let phase = Phase {
            index,
            connections,
            requests_per_connection: depth,
            total_concurrency: connections
                .checked_mul(depth)
                .ok_or(BreakReason::Overflow)?,
            duration: plan.phase_duration,
        };
        // Bound the event-driven load integral before starting any phase traffic.
        plan.phase_duration
            .checked_add(plan.timeout)
            .ok_or(BreakReason::Overflow)?
            .as_nanos()
            .checked_mul(phase.total_concurrency as u128)
            .ok_or(BreakReason::Overflow)?;
        let (start, waiting) = watch::channel(None);
        let phase_stop = CancellationToken::new();
        let mut readiness = Vec::new();
        let mut replies = Vec::new();
        readiness
            .try_reserve(workers.len())
            .map_err(|_| BreakReason::Resources("phase readiness handles"))?;
        replies
            .try_reserve(workers.len())
            .map_err(|_| BreakReason::Resources("phase result handles"))?;
        for worker in 0..workers.len() {
            let (ready, ready_wait) = mpsc::channel();
            let (reply, reply_wait) = mpsc::channel();
            workers.send(
                worker,
                Command {
                    phase,
                    churn: lifetime.map(|requests| Churn {
                        owners: connections / workers.len()
                            + usize::from(worker < connections % workers.len()),
                        requests,
                    }),
                    timeout: plan.timeout,
                    start: waiting.clone(),
                    phase_stop: phase_stop.clone(),
                    ready,
                    reply,
                },
            )?;
            readiness.push(ready_wait);
            replies.push(reply_wait);
        }
        for ready in readiness {
            receive(ready)?;
        }
        observer.phase_started(&phase);
        let began = Instant::now();
        start.send_replace(Some(began));
        let mut counts = Counts::default();
        let mut at_target = 0;
        let mut failure = None;
        let mut load = LoadStats::default();
        let mut latency = LatencyStats::default();
        let mut setups = Setups::default();
        let mut tls = None;
        for reply in replies {
            match receive(reply) {
                Ok(summary) => {
                    setups.add(summary.setups);
                    tls = tls.or(summary.tls);
                    counts.add(summary.counts);
                    at_target += summary.at_target;
                    load.add(summary.load);
                    latency.add(summary.latency);
                    if let Some(reason) = summary.failure {
                        failure.get_or_insert(reason);
                    }
                }
                Err(reason) => {
                    phase_stop.cancel();
                    failure.get_or_insert(reason);
                }
            }
        }
        let measured = PhaseStats {
            phase,
            counts,
            elapsed: began.elapsed(),
            connections_at_target: at_target,
            load,
            latency,
            setups: lifetime.map(|_| setups),
            tls,
            completed: failure.is_none() && !stop.is_cancelled(),
            cancelled: stop.is_cancelled(),
        };
        stats.phases += 1;
        stats.total.add(counts);
        stats.last_phase = Some(measured);
        observer.phase_finished(&measured);
        if let Some(reason) = failure {
            return Err(reason);
        }
        if measured.cancelled {
            return Err(BreakReason::Cancelled);
        }
        if !measured.connections_accepted(acceptance) {
            return Err(match plan.raises {
                Raises::Depth { .. } => BreakReason::Unfulfilled,
                Raises::Owners { .. } => BreakReason::Connections,
            });
        }
        if !measured.accepted(acceptance) {
            return Err(BreakReason::Acceptance);
        }
        stats.last_accepted = Some(measured);
        if stop.is_cancelled() {
            return Err(BreakReason::Cancelled);
        }
        if plan.max == Some(level) {
            return Ok(());
        }
        let next = level.checked_add(plan.pace).ok_or(BreakReason::Overflow)?;
        level = plan.max.map_or(next, |max| next.min(max));
        index = index.checked_add(1).ok_or(BreakReason::Overflow)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> Plan {
        Plan {
            conn: 4,
            start: 10,
            pace: 5,
            phase_duration: Duration::from_secs(10),
            timeout: Duration::from_secs(2),
            accept: 99.99,
            max_concurrency: None,
        }
    }

    #[test]
    fn rejects_invalid_inputs_before_workers_or_traffic() {
        let plan = plan();
        assert!(plan.validate().is_ok());
        for invalid in [
            Plan { conn: 0, ..plan },
            Plan { start: 0, ..plan },
            Plan { pace: 0, ..plan },
            Plan { phase_duration: Duration::ZERO, ..plan },
            Plan { timeout: Duration::ZERO, ..plan },
            Plan { max_concurrency: Some(9), ..plan },
            Plan { accept: f64::NAN, ..plan },
            Plan { conn: usize::MAX, ..plan },
        ] {
            assert!(invalid.validate().is_err());
        }
    }
}
