use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use tokio::io::Interest;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use super::decode::Decoder;
use super::transport::Transport;
use super::{Client, Outcome, is_local_resource};
use crate::load::Meter;
use crate::{BreakReason, Counts, LatencyStats, LoadStats};

#[derive(Clone, Copy)]
pub(crate) struct Window {
    pub per_connection: usize,
    pub start: Instant,
    pub duration: Duration,
    pub timeout: Duration,
}

#[derive(Default)]
pub(crate) struct Summary {
    pub counts: Counts,
    pub at_target: bool,
    pub failure: Option<BreakReason>,
    pub load: LoadStats,
    pub latency: LatencyStats,
}

#[derive(Clone, Copy)]
struct Writing {
    offset: usize,
    began: Instant,
}

#[derive(Clone, Copy)]
struct Attempt {
    deadline: Instant,
    began: Instant,
}

impl Attempt {
    fn record_response(
        self,
        now: Instant,
        latency: &mut LatencyStats,
    ) {
        latency.record(now.duration_since(self.began));
    }
}

pub(crate) struct Connection {
    stream: Transport,
    decoder: Decoder,
    pending: VecDeque<Attempt>,
    buffer: Vec<u8>,
    writing: Option<Writing>,
    first_deadline: Option<Instant>,
}

enum End {
    Retain,
    Reconnect,
    Failed(BreakReason),
}

impl Connection {
    async fn connect(
        client: &Client,
        timeout: Duration,
        stop: &CancellationToken,
    ) -> Result<Self, BreakReason> {
        let deadline = Instant::now() + timeout;
        let stream = tokio::select! {
            _ = stop.cancelled() => return Err(BreakReason::Cancelled),
            result = tokio::time::timeout_at(deadline.into(), async {
                let stream =
                    TcpStream::connect(client.addresses.as_ref()).await?;
                stream.set_nodelay(true)?;
                // An `https://` target is connected once its handshake is done.
                Transport::open(stream, client).await
            }) => {
                result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection deadline exceeded"))??
            }
        };
        let mut buffer = Vec::new();
        // This is an I/O chunk, not a response-size or request-concurrency limit.
        buffer
            .try_reserve(16 * 1024)
            .map_err(|_| BreakReason::Resources("socket read buffer"))?;
        buffer.resize(16 * 1024, 0);
        Ok(Self {
            stream,
            decoder: Decoder::new(client.status, client.body.clone()),
            pending: VecDeque::new(),
            buffer,
            writing: None,
            first_deadline: Some(deadline),
        })
    }

    fn abandon(
        &mut self,
        counts: &mut Counts,
        cause: Outcome,
        meter: &mut Meter,
    ) {
        let now = Instant::now();
        retire_attempts(self.pending.drain(..), counts, cause, meter, now);
        if let Some(writing) = self.writing {
            meter.stats.max_send_delay = meter
                .stats
                .max_send_delay
                .max(now.duration_since(writing.began));
        }
        self.writing = None;
        meter.change(0, now);
    }

    fn receive(
        &mut self,
        size: usize,
        counts: &mut Counts,
        latency: &mut LatencyStats,
        meter: &mut Meter,
    ) -> Result<bool, BreakReason> {
        let mut used = 0;
        while used < size {
            if self.pending.is_empty() {
                return Err(BreakReason::Protocol(
                    "unsolicited response bytes",
                ));
            }
            let (read, reply) = self.decoder.feed(&self.buffer[used..size])?;
            used += read;
            if let Some(reply) = reply {
                let attempt = self.pending.pop_front().unwrap();
                let deadline = attempt.deadline;
                let now = Instant::now();
                attempt.record_response(now, latency);
                meter.change(
                    self.pending
                        .len()
                        .saturating_sub(usize::from(self.writing.is_some())),
                    now,
                );
                if now >= deadline {
                    meter.expired(deadline, now);
                    Outcome::Timeout
                } else {
                    reply.outcome
                }
                .record(counts);
                if reply.close {
                    return Ok(true);
                }
            } else if read == 0 {
                return Err(BreakReason::Protocol(
                    "response decoder made no progress",
                ));
            }
        }
        Ok(false)
    }

    async fn exchange(
        &mut self,
        client: &Client,
        window: Window,
        stop: &CancellationToken,
        phase_stop: &CancellationToken,
        summary: &mut Summary,
        meter: &mut Meter,
    ) -> End {
        if self.pending.try_reserve(window.per_connection).is_err() {
            return End::Failed(BreakReason::Resources(
                "request deadline slots",
            ));
        }
        let end = window.start + window.duration;
        loop {
            tokio::task::consume_budget().await;
            let now = Instant::now();
            let admitting =
                now < end && !stop.is_cancelled() && !phase_stop.is_cancelled();
            if !admitting {
                meter.close_admission(now);
            }
            if self
                .pending
                .front()
                .is_some_and(|attempt| attempt.deadline <= now)
            {
                self.abandon(&mut summary.counts, Outcome::Abandoned, meter);
                return End::Reconnect;
            }
            if !admitting && self.pending.is_empty() {
                return End::Retain;
            }
            let can_write = self.writing.is_some()
                || (admitting && self.pending.len() < window.per_connection);
            let interest = if can_write {
                Interest::READABLE | Interest::WRITABLE
            } else {
                Interest::READABLE
            };
            let deadline = self.pending.front().map_or(end, |attempt| {
                let deadline = attempt.deadline;
                if admitting { deadline.min(end) } else { deadline }
            });
            let ready = tokio::select! {
                _ = stop.cancelled(), if admitting => continue,
                _ = phase_stop.cancelled(), if admitting => continue,
                _ = tokio::time::sleep_until(deadline.into()) => continue,
                result = self.stream.ready(interest) => match result {
                    Ok(ready) => ready,
                    Err(error) => { self.abandon(&mut summary.counts, io_outcome(&error), meter); return End::Failed(error.into()); }
                },
            };
            if ready.is_readable() {
                match self.stream.try_read(&mut self.buffer) {
                    Ok(0) => {
                        let mut complete = false;
                        if let Some(reply) = self.decoder.eof()
                            && let Some(attempt) = self.pending.pop_front()
                        {
                            complete = true;
                            let deadline = attempt.deadline;
                            let now = Instant::now();
                            attempt.record_response(now, &mut summary.latency);
                            meter.change(
                                self.pending.len().saturating_sub(usize::from(
                                    self.writing.is_some(),
                                )),
                                now,
                            );
                            if now >= deadline {
                                meter.expired(deadline, now);
                                Outcome::Timeout
                            } else {
                                reply.outcome
                            }
                            .record(&mut summary.counts);
                        }
                        self.abandon(
                            &mut summary.counts,
                            if complete {
                                Outcome::Abandoned
                            } else {
                                Outcome::Transport
                            },
                            meter,
                        );
                        return End::Reconnect;
                    }
                    Ok(size) => {
                        match self.receive(
                            size,
                            &mut summary.counts,
                            &mut summary.latency,
                            meter,
                        ) {
                            Ok(true) => {
                                self.abandon(
                                    &mut summary.counts,
                                    Outcome::Abandoned,
                                    meter,
                                );
                                return End::Reconnect;
                            }
                            Ok(false) => {}
                            Err(error) => {
                                self.abandon(
                                    &mut summary.counts,
                                    Outcome::Abandoned,
                                    meter,
                                );
                                return End::Failed(error);
                            }
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    }
                    Err(error) => {
                        self.abandon(
                            &mut summary.counts,
                            io_outcome(&error),
                            meter,
                        );
                        return if is_local_resource(&error) {
                            End::Failed(error.into())
                        } else {
                            End::Reconnect
                        };
                    }
                }
            }
            if ready.is_writable() && can_write {
                loop {
                    // Use Tokio's own cooperative quantum, not an invented batch limit.
                    // Returning to the outer loop services reads before another write burst.
                    if !tokio::task::coop::has_budget_remaining() {
                        break;
                    }
                    tokio::task::consume_budget().await;
                    let now = Instant::now();
                    if self
                        .pending
                        .front()
                        .is_some_and(|attempt| attempt.deadline <= now)
                    {
                        break;
                    }
                    if self.writing.is_none() {
                        if now >= end
                            || stop.is_cancelled()
                            || phase_stop.is_cancelled()
                            || self.pending.len() >= window.per_connection
                        {
                            break;
                        }
                        let deadline = self
                            .first_deadline
                            .take()
                            .unwrap_or(now + window.timeout);
                        self.pending
                            .push_back(Attempt { deadline, began: now });
                        self.writing = Some(Writing { offset: 0, began: now });
                    }
                    let writing = self.writing.unwrap();
                    match self
                        .stream
                        .try_write(&client.request[writing.offset..])
                    {
                        Ok(0) => {
                            self.abandon(
                                &mut summary.counts,
                                Outcome::Transport,
                                meter,
                            );
                            return End::Reconnect;
                        }
                        Ok(written) => {
                            let next = writing.offset + written;
                            if next == client.request.len() {
                                self.writing = None;
                                summary.counts.sent += 1;
                                let now = Instant::now();
                                meter.stats.max_send_delay = meter
                                    .stats
                                    .max_send_delay
                                    .max(now.duration_since(writing.began));
                                meter.change(self.pending.len(), now);
                            } else {
                                self.writing =
                                    Some(Writing { offset: next, ..writing });
                            }
                        }
                        Err(error)
                            if error.kind() == io::ErrorKind::WouldBlock =>
                        {
                            meter.stats.write_blocked += 1;
                            break;
                        }
                        Err(error) => {
                            self.abandon(
                                &mut summary.counts,
                                io_outcome(&error),
                                meter,
                            );
                            return if is_local_resource(&error) {
                                End::Failed(error.into())
                            } else {
                                End::Reconnect
                            };
                        }
                    }
                }
            }
        }
    }
}

fn io_outcome(error: &io::Error) -> Outcome {
    if is_local_resource(error) {
        Outcome::Abandoned
    } else {
        Outcome::Transport
    }
}

fn retire_attempts(
    attempts: impl Iterator<Item = Attempt>,
    counts: &mut Counts,
    cause: Outcome,
    meter: &mut Meter,
    now: Instant,
) {
    let mut direct = Some(cause);
    for attempt in attempts {
        let deadline = attempt.deadline;
        let outcome = if deadline <= now {
            meter.expired(deadline, now);
            Outcome::Timeout
        } else {
            direct.take().unwrap_or(Outcome::Abandoned)
        };
        outcome.record(counts);
    }
}

/// One persistent owner per declared connection, not one task per request.
pub(crate) async fn run_slot(
    client: &Client,
    mut connection: Option<Connection>,
    window: Window,
    stop: &CancellationToken,
    phase_stop: &CancellationToken,
) -> (Option<Connection>, Summary) {
    let mut summary = Summary::default();
    let mut meter =
        Meter::new(window.start, window.duration, window.per_connection);
    loop {
        if connection.is_none() {
            if Instant::now() >= window.start + window.duration
                || stop.is_cancelled()
                || phase_stop.is_cancelled()
            {
                break;
            }
            let began = Instant::now();
            let connected =
                Connection::connect(client, window.timeout, stop).await;
            meter.stats.connect_attempts += 1;
            meter.stats.connect_nanos += began.elapsed().as_nanos();
            match connected {
                Ok(opened) => connection = Some(opened),
                Err(error) => {
                    if let BreakReason::Io(error) = &error
                        && !is_local_resource(error)
                    {
                        if error.kind() == io::ErrorKind::TimedOut {
                            Outcome::ConnectTimeout
                        } else {
                            Outcome::Connect
                        }
                        .record(&mut summary.counts);
                    }
                    phase_stop.cancel();
                    summary.failure = Some(error);
                    break;
                }
            }
        }
        match connection
            .as_mut()
            .unwrap()
            .exchange(
                client,
                window,
                stop,
                phase_stop,
                &mut summary,
                &mut meter,
            )
            .await
        {
            End::Retain => break,
            End::Reconnect => connection = None,
            End::Failed(error) => {
                phase_stop.cancel();
                summary.failure = Some(error);
                connection = None;
                break;
            }
        }
    }
    summary.load = meter.finish(Instant::now());
    summary.at_target = summary.load.latest_first_target.is_some();
    (connection, summary)
}

#[cfg(test)]
#[path = "../../tests/http1/connection.rs"]
mod tests;
