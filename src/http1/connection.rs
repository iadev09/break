use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use tokio::io::Interest;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use super::decode::Decoder;
use super::{Client, Outcome, is_local_resource};
use crate::{BreakReason, Counts};

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
}

pub(crate) struct Connection {
    stream: TcpStream,
    decoder: Decoder,
    pending: VecDeque<Instant>,
    buffer: Vec<u8>,
    writing: Option<usize>,
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
            result = tokio::time::timeout_at(deadline.into(), TcpStream::connect(client.addresses.as_ref())) => {
                result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection deadline exceeded"))??
            }
        };
        stream.set_nodelay(true)?;
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
    ) {
        let now = Instant::now();
        for deadline in self.pending.drain(..) {
            if deadline <= now { Outcome::Timeout } else { Outcome::Transport }
                .record(counts);
        }
        self.writing = None;
    }

    fn receive(
        &mut self,
        size: usize,
        counts: &mut Counts,
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
                let deadline = self.pending.pop_front().unwrap();
                if Instant::now() >= deadline {
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
            if self.pending.front().is_some_and(|deadline| *deadline <= now) {
                self.abandon(&mut summary.counts);
                return End::Reconnect;
            }
            let admitting =
                now < end && !stop.is_cancelled() && !phase_stop.is_cancelled();
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
            let deadline =
                self.pending.front().copied().map_or(end, |deadline| {
                    if admitting { deadline.min(end) } else { deadline }
                });
            let ready = tokio::select! {
                _ = stop.cancelled(), if admitting => continue,
                _ = phase_stop.cancelled(), if admitting => continue,
                _ = tokio::time::sleep_until(deadline.into()) => continue,
                result = self.stream.ready(interest) => match result {
                    Ok(ready) => ready,
                    Err(error) => { self.abandon(&mut summary.counts); return End::Failed(error.into()); }
                },
            };
            if ready.is_writable() && can_write {
                loop {
                    if self.writing.is_none() {
                        if Instant::now() >= end
                            || stop.is_cancelled()
                            || phase_stop.is_cancelled()
                            || self.pending.len() >= window.per_connection
                        {
                            break;
                        }
                        let deadline = self
                            .first_deadline
                            .take()
                            .unwrap_or_else(|| Instant::now() + window.timeout);
                        self.pending.push_back(deadline);
                        self.writing = Some(0);
                    }
                    let offset = self.writing.unwrap();
                    match self.stream.try_write(&client.request[offset..]) {
                        Ok(0) => {
                            self.abandon(&mut summary.counts);
                            return End::Reconnect;
                        }
                        Ok(written) => {
                            let next = offset + written;
                            if next == client.request.len() {
                                self.writing = None;
                                summary.counts.sent += 1;
                                summary.at_target |=
                                    self.pending.len() == window.per_connection;
                            } else {
                                self.writing = Some(next);
                            }
                        }
                        Err(error)
                            if error.kind() == io::ErrorKind::WouldBlock =>
                        {
                            break;
                        }
                        Err(error) => {
                            self.abandon(&mut summary.counts);
                            return if is_local_resource(&error) {
                                End::Failed(error.into())
                            } else {
                                End::Reconnect
                            };
                        }
                    }
                }
            }
            if ready.is_readable() {
                match self.stream.try_read(&mut self.buffer) {
                    Ok(0) => {
                        if let Some(reply) = self.decoder.eof()
                            && let Some(deadline) = self.pending.pop_front()
                        {
                            if Instant::now() >= deadline {
                                Outcome::Timeout
                            } else {
                                reply.outcome
                            }
                            .record(&mut summary.counts);
                        }
                        self.abandon(&mut summary.counts);
                        return End::Reconnect;
                    }
                    Ok(size) => match self.receive(size, &mut summary.counts) {
                        Ok(true) => {
                            self.abandon(&mut summary.counts);
                            return End::Reconnect;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            self.abandon(&mut summary.counts);
                            return End::Failed(error);
                        }
                    },
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    }
                    Err(error) => {
                        self.abandon(&mut summary.counts);
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

/// One persistent owner per declared connection, not one task per request.
pub(crate) async fn run_slot(
    client: &Client,
    mut connection: Option<Connection>,
    window: Window,
    stop: &CancellationToken,
    phase_stop: &CancellationToken,
) -> (Option<Connection>, Summary) {
    let mut summary = Summary::default();
    loop {
        if connection.is_none() {
            if Instant::now() >= window.start + window.duration
                || stop.is_cancelled()
                || phase_stop.is_cancelled()
            {
                break;
            }
            match Connection::connect(client, window.timeout, stop).await {
                Ok(opened) => connection = Some(opened),
                Err(error) => {
                    phase_stop.cancel();
                    summary.failure = Some(error);
                    break;
                }
            }
        }
        match connection
            .as_mut()
            .unwrap()
            .exchange(client, window, stop, phase_stop, &mut summary)
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
    (connection, summary)
}
