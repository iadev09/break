use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

use mio::net::TcpStream;
#[cfg(feature = "http2")]
use tokio_util::sync::CancellationToken;

use super::decode::Decoder;
#[cfg(feature = "tls")]
use super::transport::secure::Secure;
#[cfg(feature = "http2")]
use super::transport::{Sessions, Transport, own_sessions};
use super::{Client, Outcome, is_local_resource};
use crate::load::Meter;
use crate::{BreakReason, Counts, LatencyStats, LoadStats, Setups};

#[derive(Clone, Copy)]
pub(crate) struct Window {
    pub per_connection: usize,
    pub start: Instant,
    pub duration: Duration,
    pub timeout: Duration,
    /// The connection test: each connection carries exactly this many
    /// requests and is then replaced. `None` keeps connections open.
    pub lifetime: Option<usize>,
}

#[derive(Default)]
pub(crate) struct Summary {
    pub counts: Counts,
    pub at_target: bool,
    pub failure: Option<BreakReason>,
    pub load: LoadStats,
    pub latency: LatencyStats,
    pub setups: Setups,
    pub tls: Option<crate::Negotiated>,
}

/// Requests admitted together and not yet all handed to the socket. They
/// are written together: one write carries as many as the socket takes.
#[derive(Clone, Copy)]
struct Writing {
    /// Bytes of the burst the socket has taken.
    offset: usize,
    /// Requests in the burst, and the length of each.
    count: usize,
    each: usize,
    began: Instant,
    /// The connection's last request, which asks the peer to close.
    closing: bool,
}

impl Writing {
    /// Requests of the burst that are not completely sent.
    fn unsent(&self) -> usize {
        self.count - self.offset / self.each
    }
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

/// The socket of one HTTP/1.1 connection, as the engine reads and writes it.
pub(super) enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<Secure<TcpStream>>),
}

impl Stream {
    /// One read. The second value is whether more may be waiting: a plain
    /// socket that did not fill the buffer has been emptied, and its next
    /// bytes announce themselves with a new readiness event. Under TLS the
    /// records behind the decrypted bytes are not visible, so only a read
    /// that finds nothing settles it.
    fn read(
        &mut self,
        buffer: &mut [u8],
    ) -> io::Result<(usize, bool)> {
        match self {
            Self::Plain(stream) => {
                let size = stream.read(buffer)?;
                Ok((size, size == buffer.len()))
            }
            #[cfg(feature = "tls")]
            Self::Tls(secure) => Ok((secure.try_read(buffer)?, true)),
        }
    }

    fn write(
        &mut self,
        bytes: &[u8],
    ) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(bytes),
            #[cfg(feature = "tls")]
            Self::Tls(secure) => secure.try_write(bytes),
        }
    }

    pub(super) fn socket(&mut self) -> &mut TcpStream {
        match self {
            Self::Plain(stream) => stream,
            #[cfg(feature = "tls")]
            Self::Tls(secure) => secure.socket(),
        }
    }

    pub(super) fn negotiated(&self) -> Option<crate::Negotiated> {
        match self {
            Self::Plain(_) => None,
            #[cfg(feature = "tls")]
            Self::Tls(secure) => secure.negotiated(),
        }
    }

    pub(super) fn resumed(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            #[cfg(feature = "tls")]
            Self::Tls(secure) => secure.resumed(),
        }
    }
}

/// What an owner knows of its socket between readiness events. An event
/// sets a side; a read that finds nothing or a write that finds no room
/// clears it. Nothing is asked of the kernel in between.
#[derive(Clone, Copy, Default)]
pub(super) struct Flags {
    pub readable: bool,
    pub writable: bool,
    /// The peer ended its side, or the socket failed. No further event will
    /// announce it, so a read that emptied the socket is not the last one.
    pub ended: bool,
}

/// What one turn of a connection works with.
pub(super) struct Turn<'a> {
    pub client: &'a Client,
    pub window: Window,
    /// The run or the phase was stopped: nothing new is admitted.
    pub halted: bool,
    /// Deadlines of this thread in the order they were set. Every request has
    /// the same budget, so the order is also the order they fall due.
    pub timers: &'a mut VecDeque<(Instant, usize)>,
    /// The owner the deadlines belong to.
    pub owner: usize,
}

pub(super) enum Step {
    /// Nothing more until the socket or a deadline says so.
    Wait,
    /// The socket may hold more; the other owners go first.
    Again,
    End(End),
}

/// One HTTP/1.1 connection: a state the engine turns, not a task.
pub(crate) struct Conn {
    stream: Stream,
    decoder: Decoder,
    pending: VecDeque<Attempt>,
    buffer: Vec<u8>,
    writing: Option<Writing>,
    first_deadline: Option<Instant>,
    /// Requests this connection may still admit, in the connection test.
    remaining: Option<usize>,
    /// The peer answered the closing request and announced its close.
    pub(super) peer_closing: bool,
}

impl Conn {
    /// `deadline` is the setup's own. Where setup is not measured apart, the
    /// first request answers within it.
    pub(super) fn new(
        stream: Stream,
        client: &Client,
        window: Window,
        deadline: Instant,
    ) -> Result<Self, BreakReason> {
        let mut buffer = Vec::new();
        // This is an I/O chunk, not a response-size or request-concurrency limit.
        buffer
            .try_reserve(super::CHUNK)
            .map_err(|_| BreakReason::Resources("socket read buffer"))?;
        buffer.resize(super::CHUNK, 0);
        Ok(Self {
            stream,
            decoder: Decoder::new(client.status, client.body.clone()),
            pending: VecDeque::new(),
            buffer,
            writing: None,
            // Where setup is measured on its own, the first request's budget
            // starts when it is written, like every other request's.
            first_deadline: window.lifetime.is_none().then_some(deadline),
            remaining: window.lifetime,
            peer_closing: false,
        })
    }

    pub(super) fn stream(&self) -> &Stream {
        &self.stream
    }

    /// Admitted requests the socket has not completely taken.
    fn unsent(&self) -> usize {
        self.writing.map_or(0, |writing| writing.unsent())
    }

    pub(super) fn socket(&mut self) -> &mut TcpStream {
        self.stream.socket()
    }

    /// Room for a phase's requests, before any of them is admitted.
    pub(super) fn reserve(
        &mut self,
        window: Window,
    ) -> Result<(), BreakReason> {
        self.pending
            .try_reserve(window.per_connection)
            .map_err(|_| BreakReason::Resources("request deadline slots"))
    }

    /// The deadline this connection is waiting on, if it has one.
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.pending.front().map(|attempt| attempt.deadline)
    }

    /// Whether the peer has ended the connection it said it would close.
    /// Reading to the end leaves the closing wait state with the peer.
    pub(super) fn peer_closed(
        &mut self,
        flags: &mut Flags,
    ) -> bool {
        while flags.readable {
            match self.stream.read(&mut self.buffer) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    flags.readable = false;
                }
                _ => return true,
            }
        }
        false
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
        now: Instant,
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
                attempt.record_response(now, latency);
                meter.change(
                    self.pending.len().saturating_sub(self.unsent()),
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

    /// Everything this connection can do now: what was due, one read, and
    /// the requests its free slots admit. It never waits; it says what it
    /// needs next.
    pub(super) fn exchange(
        &mut self,
        turn: &mut Turn<'_>,
        flags: &mut Flags,
        summary: &mut Summary,
        meter: &mut Meter,
    ) -> Step {
        let window = turn.window;
        let end = window.start + window.duration;
        let mut worked = false;
        loop {
            // One reading of the clock serves everything this pass does.
            let now = Instant::now();
            let admitting = now < end && !turn.halted;
            if !admitting {
                meter.close_admission(now);
            }
            // What has arrived is read before a deadline is judged. A reply
            // that waited in the socket past its deadline is a late reply and
            // is counted as one; it is not a connection to give up.
            if worked || !flags.readable {
                if self
                    .pending
                    .front()
                    .is_some_and(|attempt| attempt.deadline <= now)
                {
                    self.abandon(
                        &mut summary.counts,
                        Outcome::Abandoned,
                        meter,
                    );
                    return Step::End(End::Reconnect);
                }
                if self.remaining == Some(0)
                    && self.pending.is_empty()
                    && self.writing.is_none()
                {
                    // Every request of this connection is answered.
                    return Step::End(End::Reconnect);
                }
                if !admitting && self.pending.is_empty() {
                    return Step::End(End::Retain);
                }
                if worked {
                    return if flags.readable {
                        Step::Again
                    } else {
                        Step::Wait
                    };
                }
            }
            worked = true;
            if flags.readable {
                match self.stream.read(&mut self.buffer) {
                    Ok((0, _)) => {
                        let mut complete = false;
                        if let Some(reply) = self.decoder.eof()
                            && let Some(attempt) = self.pending.pop_front()
                        {
                            complete = true;
                            let deadline = attempt.deadline;
                            attempt.record_response(now, &mut summary.latency);
                            meter.change(
                                self.pending
                                    .len()
                                    .saturating_sub(self.unsent()),
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
                        return Step::End(End::Reconnect);
                    }
                    Ok((size, more)) => {
                        flags.readable = more || flags.ended;
                        match self.receive(
                            size,
                            now,
                            &mut summary.counts,
                            &mut summary.latency,
                            meter,
                        ) {
                            Ok(true) => {
                                self.peer_closing = true;
                                self.abandon(
                                    &mut summary.counts,
                                    Outcome::Abandoned,
                                    meter,
                                );
                                return Step::End(End::Reconnect);
                            }
                            Ok(false) => {}
                            Err(error) => {
                                self.abandon(
                                    &mut summary.counts,
                                    Outcome::Abandoned,
                                    meter,
                                );
                                return Step::End(End::Failed(error));
                            }
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        flags.readable = false;
                    }
                    Err(error) => {
                        self.abandon(
                            &mut summary.counts,
                            io_outcome(&error),
                            meter,
                        );
                        return Step::End(if is_local_resource(&error) {
                            End::Failed(error.into())
                        } else {
                            End::Reconnect
                        });
                    }
                }
            }
            // A response just read may have freed a slot. The next request is
            // written in this same turn. A burst ends with the free slots or
            // with a full socket; reads are served again before the next one.
            let mut wake = Some(now);
            while flags.writable {
                // The first write of a burst is at the turn's own time; later
                // ones read the clock, so a long burst does not date its
                // requests early.
                let now = wake.take().unwrap_or_else(Instant::now);
                if self
                    .pending
                    .front()
                    .is_some_and(|attempt| attempt.deadline <= now)
                {
                    break;
                }
                if self.writing.is_none() {
                    if now >= end
                        || turn.halted
                        || self.pending.len() >= window.per_connection
                        || self.remaining == Some(0)
                    {
                        break;
                    }
                    // Every free slot is admitted now, with this instant and
                    // its deadline, whether or not the socket takes all of
                    // them at once. The connection test admits one at a time.
                    let closing = self.remaining == Some(1);
                    let count = match &mut self.remaining {
                        Some(remaining) => {
                            *remaining -= 1;
                            1
                        }
                        None => (window.per_connection - self.pending.len())
                            .min(turn.client.burst()),
                    };
                    let each = if closing {
                        turn.client.closing.len()
                    } else {
                        turn.client.request.len()
                    };
                    let mut timed = false;
                    for _ in 0..count {
                        let deadline = match self.first_deadline.take() {
                            Some(deadline) => deadline,
                            None => {
                                timed = true;
                                now + window.timeout
                            }
                        };
                        self.pending
                            .push_back(Attempt { deadline, began: now });
                    }
                    if timed {
                        // One entry wakes the owner for all of them.
                        turn.timers
                            .push_back((now + window.timeout, turn.owner));
                    }
                    self.writing = Some(Writing {
                        offset: 0,
                        count,
                        each,
                        began: now,
                        closing,
                    });
                }
                let writing = self.writing.unwrap();
                let bytes = if writing.closing {
                    &turn.client.closing[..]
                } else {
                    &turn.client.requests[..writing.count * writing.each]
                };
                match self.stream.write(&bytes[writing.offset..]) {
                    Ok(0) => {
                        self.abandon(
                            &mut summary.counts,
                            Outcome::Transport,
                            meter,
                        );
                        return Step::End(End::Reconnect);
                    }
                    Ok(written) => {
                        let sent = Writing {
                            offset: writing.offset + written,
                            ..writing
                        };
                        let completed = writing.unsent() - sent.unsent();
                        self.writing =
                            (sent.offset < bytes.len()).then_some(sent);
                        if completed > 0 {
                            summary.counts.sent += completed as u64;
                            meter.stats.max_send_delay = meter
                                .stats
                                .max_send_delay
                                .max(now.duration_since(writing.began));
                            meter.change(
                                self.pending.len() - sent.unsent(),
                                now,
                            );
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        meter.stats.write_blocked += 1;
                        flags.writable = false;
                    }
                    Err(error) => {
                        self.abandon(
                            &mut summary.counts,
                            io_outcome(&error),
                            meter,
                        );
                        return Step::End(if is_local_resource(&error) {
                            End::Failed(error.into())
                        } else {
                            End::Reconnect
                        });
                    }
                }
            }
        }
    }
}

pub(crate) enum End {
    Retain,
    Reconnect,
    Failed(BreakReason),
}

/// One connection of a protocol that is driven through polls, as its owner
/// drives it. HTTP/1.1 has an engine of its own; this is HTTP/2's.
/// The owner's loop, its setup accounting and its phase rules are the same
/// for every protocol; only what happens on the connection differs.
#[cfg(feature = "http2")]
pub(crate) trait Carrier: Sized {
    async fn connect(
        client: &Client,
        window: Window,
        stop: &CancellationToken,
        sessions: Option<&Sessions>,
    ) -> Result<Self, BreakReason>;

    async fn exchange(
        &mut self,
        client: &Client,
        window: Window,
        stop: &CancellationToken,
        phase_stop: &CancellationToken,
        summary: &mut Summary,
        meter: &mut Meter,
    ) -> End;

    fn negotiated(&self) -> Option<crate::Negotiated>;

    fn resumed(&self) -> bool;

    /// Let a connection that reached the end of its declared lifetime end.
    async fn retire(
        self,
        window: Window,
    );
}

/// Establish the transport of one connection within the setup deadline: the
/// TCP stream and, for an `https://` target, its TLS handshake.
#[cfg(feature = "http2")]
pub(crate) async fn open(
    client: &Client,
    window: Window,
    stop: &CancellationToken,
    sessions: Option<&Sessions>,
) -> Result<(Transport, Instant), BreakReason> {
    let deadline = Instant::now() + window.timeout;
    let stream = tokio::select! {
        _ = stop.cancelled() => return Err(BreakReason::Cancelled),
        result = tokio::time::timeout_at(deadline.into(), async {
            let stream =
                tokio::net::TcpStream::connect(client.addresses.as_ref())
                    .await?;
            stream.set_nodelay(true)?;
            // An `https://` target is connected once its handshake is done.
            Transport::open(stream, client, sessions).await
        }) => {
            result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection deadline exceeded"))??
        }
    };
    Ok((stream, deadline))
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
#[cfg(feature = "http2")]
pub(crate) async fn run_owner<C: Carrier>(
    client: &Client,
    mut connection: Option<C>,
    window: Window,
    stop: &CancellationToken,
    phase_stop: &CancellationToken,
) -> (Option<C>, Summary) {
    let mut summary = Summary::default();
    let mut meter =
        Meter::new(window.start, window.duration, window.per_connection);
    // In the connection test an owner is one visitor: it remembers its own
    // TLS sessions and nobody else's.
    let churn = window.lifetime.is_some();
    let sessions = if churn { own_sessions(client) } else { None };
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
                C::connect(client, window, stop, sessions.as_ref()).await;
            let setup = began.elapsed();
            meter.stats.connect_attempts += 1;
            meter.stats.connect_nanos += setup.as_nanos();
            match connected {
                Ok(opened) => {
                    if summary.tls.is_none() {
                        summary.tls = opened.negotiated();
                    }
                    if churn {
                        let setups = &mut summary.setups;
                        setups.attempted += 1;
                        setups.established += 1;
                        setups.resumed += u64::from(opened.resumed());
                        setups.established_nanos += setup.as_nanos();
                        setups.longest = setups.longest.max(setup);
                    }
                    connection = Some(opened);
                }
                Err(error) => {
                    if let BreakReason::Io(error) = &error
                        && !is_local_resource(error)
                    {
                        if churn {
                            // Here a failed setup is the measured outcome,
                            // and the owner tries again like any visitor.
                            summary.setups.attempted += 1;
                            if error.kind() == io::ErrorKind::TimedOut {
                                summary.setups.timeout += 1;
                            } else {
                                summary.setups.failed += 1;
                            }
                            continue;
                        }
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
            End::Reconnect => {
                if let Some(ended) = connection.take()
                    && churn
                {
                    ended.retire(window).await;
                }
            }
            End::Failed(error) => {
                phase_stop.cancel();
                summary.failure = Some(error);
                connection = None;
                break;
            }
        }
    }
    if let Some(connection) = &connection
        && summary.tls.is_none()
    {
        summary.tls = connection.negotiated();
    }
    summary.load = meter.finish(Instant::now());
    summary.at_target = summary.load.latest_first_target.is_some();
    (connection, summary)
}

#[cfg(test)]
#[path = "../../tests/http1/connection.rs"]
mod tests;
