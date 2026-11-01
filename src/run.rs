use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::worker::{Command, Workers, receive};
use crate::{
    Acceptance, BreakReason, Broken, CancellationToken, Counts, Done, Observer,
    Phase, PhaseStats, Stats, Target,
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
        if self.phase_duration.is_zero() || self.timeout.is_zero() {
            return Err(BreakReason::Invalid(
                "phase and timeout must be greater than zero",
            ));
        }
        if self.max_concurrency.is_some_and(|max| max < self.start) {
            return Err(BreakReason::Invalid(
                "max must admit the initial start level",
            ));
        }
        self.conn.checked_mul(self.start).ok_or(BreakReason::Overflow)?;
        Instant::now()
            .checked_add(self.phase_duration)
            .and_then(|end| end.checked_add(self.timeout))
            .ok_or(BreakReason::Invalid(
                "time budgets exceed the clock range",
            ))?;
        Acceptance::try_from(self.accept).map_err(BreakReason::Invalid)
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
    let mut stats = Stats::default();
    let result = execute(target, plan, cancellation, observer, &mut stats);
    match result {
        Ok(()) => Ok(Done { stats }),
        Err(reason) => Err(Broken { reason, stats: Box::new(stats) }),
    }
}

fn execute(
    target: &Target,
    plan: Plan,
    cancellation: &CancellationToken,
    observer: &mut (impl Observer + ?Sized),
    stats: &mut Stats,
) -> Result<(), BreakReason> {
    let acceptance = plan.validate()?;
    if cancellation.is_cancelled() {
        return Err(BreakReason::Cancelled);
    }
    let prepared = target.resolve()?;
    let stop = cancellation.child_token();
    let workers = Workers::start(&prepared, plan.conn, stop.clone())?;
    stats.cpu_workers = workers.len();
    let result = phases(&workers, plan, acceptance, &stop, observer, stats);
    let finished = workers.finish();
    result.and(finished)
}

fn phases(
    workers: &Workers,
    plan: Plan,
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
        let phase = Phase {
            index,
            connections: plan.conn,
            requests_per_connection: level,
            total_concurrency: plan
                .conn
                .checked_mul(level)
                .ok_or(BreakReason::Overflow)?,
            duration: plan.phase_duration,
        };
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
        for reply in replies {
            match receive(reply) {
                Ok(summary) => {
                    counts.add(summary.counts);
                    at_target += summary.at_target;
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
        if at_target != plan.conn {
            return Err(BreakReason::Unfulfilled);
        }
        if !measured.accepted(acceptance) {
            return Err(BreakReason::Acceptance);
        }
        stats.last_accepted = Some(measured);
        if stop.is_cancelled() {
            return Err(BreakReason::Cancelled);
        }
        if plan.max_concurrency == Some(level) {
            return Ok(());
        }
        let next = level.checked_add(plan.pace).ok_or(BreakReason::Overflow)?;
        if plan.max_concurrency.is_some_and(|max| next > max) {
            return Ok(());
        }
        level = next;
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
