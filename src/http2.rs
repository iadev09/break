//! HTTP/2 protocol state on the same Mio worker loop as HTTP/1.1.
use std::collections::VecDeque;
use std::io;
use std::time::Instant;

use bytes::Bytes;
use fnv::FnvHashMap;
use h2::client::sans_io::{Event, Session};
use http::Request;
use mio::net::TcpStream;

use crate::BreakReason;
use crate::http1::connection::{
    End, Flags, Step, Stream, Summary, Turn, Window,
};
use crate::http1::engine::Protocol;
use crate::http1::{Client, Outcome, is_local_resource};
use crate::load::Meter;

#[derive(Clone)]
pub(crate) struct Head {
    pub(crate) uri: http::Uri,
    pub(crate) send: Option<Bytes>,
}

#[derive(Clone, Copy)]
struct Queued {
    began: Instant,
    deadline: Instant,
}

struct Attempt {
    id: h2::StreamId,
    began: Instant,
    deadline: Instant,
    status: bool,
    matches: bool,
    offset: usize,
}

pub(crate) struct Connection {
    stream: Stream,
    session: Session,
    pending: FnvHashMap<u32, Attempt>,
    deadlines: VecDeque<(Instant, u32)>,
    /// Intent only: no stream ID or protocol request exists before publication.
    /// A fair worker turn can collect more replies before filling the next batch.
    queued: VecDeque<Queued>,
    buffer: Vec<u8>,
    first_deadline: Option<Instant>,
    remaining: Option<usize>,
    closed: bool,
    retiring: Option<Instant>,
}

fn io_error(error: h2::Error) -> io::Error {
    if error.is_io() {
        error.into_io().expect("I/O source")
    } else {
        io::Error::other(error)
    }
}

impl Connection {
    /// Advance frames and write only while the socket is known writable.
    fn flush(
        &mut self,
        flags: &mut Flags,
        now: Instant,
    ) -> io::Result<bool> {
        loop {
            self.closed |= self.session.drive(now).map_err(io_error)?;
            let output = self.session.output();
            if output.is_empty() || !flags.writable {
                return Ok(self.closed);
            }
            match self.stream.write(output) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => self.session.advance_output(count),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    flags.writable = false;
                    return Ok(self.closed);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn read(
        &mut self,
        flags: &mut Flags,
    ) -> io::Result<usize> {
        if !flags.readable {
            return Ok(0);
        }
        match self.stream.read(&mut self.buffer) {
            Ok((0, _)) => {
                flags.readable = false;
                self.session.end_input();
                Ok(0)
            }
            Ok((count, more)) => {
                // Transport readiness does not say whether the protocol's
                // response batch is complete. Plain short reads drain it;
                // TLS may still hold decrypted bytes and must be probed again.
                flags.readable = more || flags.ended;
                self.session.input().extend_from_slice(&self.buffer[..count]);
                Ok(count)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                flags.readable = false;
                Ok(0)
            }
            Err(error) => Err(error),
        }
    }

    fn trim_deadlines(&mut self) {
        while self
            .deadlines
            .front()
            .is_some_and(|(_, id)| !self.pending.contains_key(id))
        {
            self.deadlines.pop_front();
        }
    }

    fn received(
        &mut self,
        client: &Client,
        now: Instant,
        summary: &mut Summary,
        meter: &mut Meter,
    ) -> io::Result<()> {
        let mut event = self.session.next_event();
        while let Some(next) = event {
            match next {
                Event::Informational { .. } => (),
                Event::Headers { stream, response } => {
                    if let Some(attempt) =
                        self.pending.get_mut(&stream.as_u32())
                    {
                        attempt.status = response.status().is_success()
                            && client.status.is_none_or(|expected| {
                                expected == response.status()
                            });
                    }
                }
                Event::Data { stream, bytes } => {
                    if let Some(attempt) =
                        self.pending.get_mut(&stream.as_u32())
                    {
                        if let Some(expected) = &client.body {
                            attempt.matches &= expected.get(
                                attempt.offset..attempt.offset + bytes.len(),
                            ) == Some(bytes.as_ref());
                        }
                        attempt.offset += bytes.len();
                    }
                    // The bytes have been validated. Return their credit and
                    // advance the same receive cursor in one store operation.
                    event = self
                        .session
                        .release_capacity_and_next_event(stream, bytes.len())
                        .map_err(io_error)?;
                    continue;
                }
                Event::End { stream, .. } => {
                    if let Some(attempt) = self.pending.remove(&stream.as_u32())
                    {
                        summary
                            .latency
                            .record(now.duration_since(attempt.began));
                        let outcome = if now >= attempt.deadline {
                            meter.expired(attempt.deadline, now);
                            Outcome::Timeout
                        } else if !attempt.status {
                            Outcome::Status
                        } else if !attempt.matches
                            || client.body.as_ref().is_some_and(|expected| {
                                expected.len() != attempt.offset
                            })
                        {
                            Outcome::Body
                        } else {
                            Outcome::Accepted
                        };
                        outcome.record(&mut summary.counts);
                        meter.change(self.pending.len(), now);
                    }
                }
                Event::Failed { stream, .. } => {
                    if let Some(attempt) = self.pending.remove(&stream.as_u32())
                    {
                        let outcome = if now >= attempt.deadline {
                            meter.expired(attempt.deadline, now);
                            Outcome::Timeout
                        } else {
                            Outcome::Transport
                        };
                        outcome.record(&mut summary.counts);
                        meter.change(self.pending.len(), now);
                    }
                }
            }
            event = self.session.next_event();
        }
        self.trim_deadlines();
        Ok(())
    }

    /// Drop expired intent before allocating an HTTP/2 stream. Queue waiting
    /// spends the original deadline; it does not start a new request budget.
    fn expire_queued(
        &mut self,
        now: Instant,
        summary: &mut Summary,
        meter: &mut Meter,
    ) {
        while self.queued.front().is_some_and(|queued| queued.deadline <= now) {
            let queued = self.queued.pop_front().expect("queued intent");
            meter.expired(queued.deadline, now);
            Outcome::Timeout.record(&mut summary.counts);
        }
    }

    fn publish_queued(
        &mut self,
        client: &Client,
        summary: &mut Summary,
        meter: &mut Meter,
    ) -> io::Result<()> {
        let head = client.http2.as_ref().expect("H2 request head");
        while !self.queued.is_empty() && self.session.can_open() {
            let now = Instant::now();
            self.expire_queued(now, summary, meter);
            let Some(queued) = self.queued.front().copied() else {
                break;
            };
            let request = match &head.send {
                None => Request::get(head.uri.clone()),
                Some(body) => Request::post(head.uri.clone())
                    .header(
                        http::header::CONTENT_TYPE,
                        "application/octet-stream",
                    )
                    .header(http::header::CONTENT_LENGTH, body.len()),
            }
            .body(())
            .expect("validated URI");
            let Some(id) = self
                .session
                .send_request(request, head.send.is_none())
                .map_err(io_error)?
            else {
                break;
            };
            self.queued.pop_front();
            self.pending.insert(
                id.as_u32(),
                Attempt {
                    id,
                    began: queued.began,
                    deadline: queued.deadline,
                    status: false,
                    matches: true,
                    offset: 0,
                },
            );
            self.deadlines.push_back((queued.deadline, id.as_u32()));
            summary.counts.sent += 1;
            meter.change(self.pending.len(), now);
            if let Some(body) = &head.send {
                self.session
                    .send_data(id, body.clone(), true)
                    .map_err(io_error)?;
            }
        }
        Ok(())
    }

    fn lost(
        &mut self,
        error: Option<io::Error>,
        now: Instant,
        summary: &mut Summary,
        meter: &mut Meter,
    ) -> Step {
        let mut first = true;
        for (_, attempt) in self.pending.drain() {
            let outcome = if now >= attempt.deadline {
                meter.expired(attempt.deadline, now);
                Outcome::Timeout
            } else if first {
                first = false;
                Outcome::Transport
            } else {
                Outcome::Abandoned
            };
            outcome.record(&mut summary.counts);
        }
        for queued in self.queued.drain(..) {
            let outcome = if now >= queued.deadline {
                meter.expired(queued.deadline, now);
                Outcome::Timeout
            } else if first {
                first = false;
                Outcome::Transport
            } else {
                Outcome::Abandoned
            };
            outcome.record(&mut summary.counts);
        }
        self.deadlines.clear();
        meter.change(0, now);
        Step::End(match error {
            Some(error) if is_local_resource(&error) => {
                End::Failed(error.into())
            }
            _ => End::Reconnect,
        })
    }
}

impl Protocol for Connection {
    const HANDSHAKE: bool = true;

    fn new(
        stream: Stream,
        _: &Client,
        window: Window,
        deadline: Instant,
    ) -> Result<Self, BreakReason> {
        let mut builder = h2::client::Builder::new();
        builder.initial_max_send_streams(0);
        let session = Session::new(builder, Instant::now());
        let mut buffer = Vec::new();
        buffer
            .try_reserve(crate::http1::CHUNK)
            .map_err(|_| BreakReason::Resources("H2 input buffer"))?;
        buffer.resize(crate::http1::CHUNK, 0);
        Ok(Self {
            stream,
            session,
            pending: FnvHashMap::default(),
            deadlines: VecDeque::new(),
            queued: VecDeque::new(),
            buffer,
            first_deadline: Some(deadline),
            remaining: window.lifetime,
            closed: false,
            retiring: None,
        })
    }
    fn stream(&self) -> &Stream {
        &self.stream
    }
    fn socket(&mut self) -> &mut TcpStream {
        self.stream.socket()
    }
    fn reserve(
        &mut self,
        window: Window,
    ) -> Result<(), BreakReason> {
        self.pending
            .try_reserve(window.per_connection)
            .map_err(|_| BreakReason::Resources("H2 request state"))?;
        self.deadlines
            .try_reserve(window.per_connection)
            .map_err(|_| BreakReason::Resources("H2 deadline slots"))?;
        self.queued
            .try_reserve(window.per_connection)
            .map_err(|_| BreakReason::Resources("H2 queued requests"))
    }
    fn deadline(&self) -> Option<Instant> {
        self.retiring.or_else(|| {
            self.deadlines
                .front()
                .map(|&(deadline, _)| deadline)
                .into_iter()
                .chain(self.queued.front().map(|queued| queued.deadline))
                .min()
        })
    }
    fn peer_closing(&self) -> bool {
        false
    }
    fn peer_closed(
        &mut self,
        _: &mut Flags,
    ) -> bool {
        true
    }

    fn handshake(
        &mut self,
        flags: &mut Flags,
    ) -> io::Result<bool> {
        loop {
            let now = Instant::now();
            self.read(flags)?;
            if self.flush(flags, now)? {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if self.session.is_ready() {
                return Ok(true);
            }
            if !flags.readable {
                return Ok(false);
            }
        }
    }

    fn exchange(
        &mut self,
        turn: &mut Turn<'_>,
        flags: &mut Flags,
        summary: &mut Summary,
        meter: &mut Meter,
    ) -> Step {
        let now = Instant::now();
        let window = turn.window;
        let admitting = now < window.start + window.duration && !turn.halted;
        if !admitting {
            meter.close_admission(now);
        }
        if let Some(deadline) = self.retiring {
            if now >= deadline {
                return Step::End(End::Reconnect);
            }
            let result = self.flush(flags, now).and_then(|closed| {
                if closed {
                    self.stream.finish(&mut flags.writable)
                } else {
                    Ok(false)
                }
            });
            return match result {
                Ok(false) => Step::Wait,
                _ => Step::End(End::Reconnect),
            };
        }
        // Staging is one local queue turn, not a timer or a request delay.
        // Its requests still spend the original end-to-end deadline budget.
        let emit = !self.queued.is_empty();
        self.expire_queued(now, summary, meter);
        // Buffered protocol work is driven even when the socket is not ready.
        // Socket reads remain readiness-gated and bounded by a byte budget.
        let mut read = 0;
        loop {
            match self.read(flags) {
                Ok(count) => read += count,
                Err(error) => {
                    return self.lost(Some(error), now, summary, meter);
                }
            }
            match self.session.drive(Instant::now()) {
                Ok(closed) => self.closed |= closed,
                Err(error) => {
                    return self.lost(
                        Some(io_error(error)),
                        now,
                        summary,
                        meter,
                    );
                }
            }
            if let Err(error) =
                self.received(turn.client, Instant::now(), summary, meter)
            {
                return self.lost(Some(error), now, summary, meter);
            }
            if self.closed {
                return self.lost(None, now, summary, meter);
            }
            if !flags.readable || read >= crate::http1::CHUNK {
                break;
            }
        }
        let now = Instant::now();
        let admitting = now < window.start + window.duration && !turn.halted;
        if !admitting {
            meter.close_admission(now);
        }
        while let Some(&(deadline, id)) = self.deadlines.front() {
            if deadline > now {
                break;
            }
            self.deadlines.pop_front();
            if let Some(attempt) = self.pending.remove(&id) {
                self.session.reset(attempt.id, h2::Reason::CANCEL);
                meter.expired(deadline, now);
                Outcome::Timeout.record(&mut summary.counts);
                meter.change(self.pending.len(), now);
            }
        }
        self.trim_deadlines();
        self.expire_queued(now, summary, meter);
        if self.pending.is_empty() && self.queued.is_empty() {
            if self.remaining == Some(0) {
                self.session.close(h2::Reason::NO_ERROR);
                let deadline = now + window.timeout;
                self.retiring = Some(deadline);
                turn.timers.push_back((deadline, turn.owner));
                return Step::Again;
            }
            if !admitting {
                return Step::End(End::Retain);
            }
        }
        // Intent is bounded by both the declared concurrency and the current
        // peer window. A lower peer limit remains Unfulfilled, not a queue timeout.
        let available =
            self.session.available_streams().saturating_sub(self.queued.len());
        let room = window
            .per_connection
            .saturating_sub(self.pending.len() + self.queued.len());
        for _ in 0..room.min(available) {
            let now = Instant::now();
            if !admitting
                || now >= window.start + window.duration
                || self.remaining == Some(0)
            {
                break;
            }
            let first = self
                .first_deadline
                .take()
                .filter(|_| window.lifetime.is_none());
            let deadline = first.unwrap_or(now + window.timeout);
            self.queued.push_back(Queued { began: now, deadline });
            if first.is_none() {
                turn.timers.push_back((deadline, turn.owner));
            }
            if let Some(remaining) = self.remaining.as_mut() {
                *remaining -= 1;
            }
        }
        if !emit
            && !self.queued.is_empty()
            && self.queued.len() < window.per_connection
            && flags.writable
        {
            // A local work turn, not a timer: service other ready connections
            // before constructing and writing a partial request batch. A full
            // window cannot grow on another turn, so publish it immediately.
            return Step::Again;
        }
        if let Err(error) = self.publish_queued(turn.client, summary, meter) {
            return self.lost(Some(error), Instant::now(), summary, meter);
        }
        let before_flush = self.pending.len();
        let writable = flags.writable;
        if let Err(error) = self.flush(flags, Instant::now()) {
            return self.lost(Some(error), Instant::now(), summary, meter);
        }
        // Flushing can unblock protocol input already buffered in the session.
        // Drain its events now; no new socket edge is required for that work.
        if let Err(error) =
            self.received(turn.client, Instant::now(), summary, meter)
        {
            return self.lost(Some(error), Instant::now(), summary, meter);
        }
        if self.closed {
            return self.lost(None, Instant::now(), summary, meter);
        }
        if writable && !flags.writable {
            meter.stats.write_blocked += 1;
        }
        if flags.readable || self.pending.len() < before_flush {
            Step::Again
        } else {
            Step::Wait
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;

    #[test]
    fn a_short_plain_read_drains_readiness_without_an_empty_socket_probe() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut writer =
            std::net::TcpStream::connect(listener.local_addr().unwrap())
                .unwrap();
        let (reader, _) = listener.accept().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut reader = TcpStream::from_std(reader);
        let mut poll = mio::Poll::new().unwrap();
        poll.registry()
            .register(&mut reader, mio::Token(0), mio::Interest::READABLE)
            .unwrap();
        let client = crate::Target::new("http://example.test/")
            .unwrap()
            .connect_to(listener.local_addr().unwrap())
            .resolve()
            .unwrap()
            .client();
        let now = Instant::now();
        let window = Window {
            per_connection: 8,
            start: now,
            duration: Duration::from_secs(10),
            timeout: Duration::from_millis(100),
            lifetime: None,
        };
        let mut connection = Connection::new(
            Stream::Plain(reader),
            &client,
            window,
            now + window.timeout,
        )
        .unwrap();
        writer.write_all(b"abc").unwrap();
        let mut events = mio::Events::with_capacity(1);
        poll.poll(&mut events, None).unwrap();
        let mut flags = Flags { readable: true, ..Flags::default() };
        assert_eq!(connection.read(&mut flags).unwrap(), 3);
        assert!(
            !flags.readable,
            "a short plain read drains transport readiness"
        );
        assert_eq!(connection.read(&mut flags).unwrap(), 0);
        assert_eq!(
            connection.session.input().len(),
            3,
            "protocol input remains available without another socket read"
        );
    }
    #[test]
    fn queued_expiry_publishes_only_live_intent_with_its_original_deadline() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let _peer =
            std::net::TcpStream::connect(listener.local_addr().unwrap())
                .unwrap();
        let (socket, _) = listener.accept().unwrap();
        socket.set_nonblocking(true).unwrap();
        let client = crate::Target::new("http://example.test/")
            .unwrap()
            .connect_to(listener.local_addr().unwrap())
            .http2()
            .resolve()
            .unwrap()
            .client();
        let began = Instant::now();
        let window = Window {
            per_connection: 2,
            start: began,
            duration: Duration::from_secs(10),
            timeout: Duration::from_millis(100),
            lifetime: None,
        };
        let mut connection = Connection::new(
            Stream::Plain(TcpStream::from_std(socket)),
            &client,
            window,
            began + window.timeout,
        )
        .unwrap();
        connection.session.input().extend_from_slice(&[
            0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 0,
        ]);
        let queued = Instant::now();
        connection.session.drive(queued).unwrap();
        let expired = queued + window.timeout;
        let retained = expired + window.timeout;
        connection
            .queued
            .push_back(Queued { began: queued, deadline: expired });
        connection
            .queued
            .push_back(Queued { began: queued, deadline: retained });
        let mut summary = Summary::default();
        let mut meter =
            Meter::new(began, window.duration, window.per_connection, None);
        connection.expire_queued(expired, &mut summary, &mut meter);
        assert_eq!(summary.counts.timeout, 1);
        assert_eq!(summary.counts.sent, 0);
        assert_eq!(meter.stats.outstanding_request_nanos, 0);
        assert!(
            meter.stats.latest_first_target.is_none(),
            "queued intent cannot claim protocol concurrency"
        );
        assert_eq!(connection.queued.len(), 1);
        assert_eq!(connection.queued[0].deadline, retained);
        connection.expire_queued(expired, &mut summary, &mut meter);
        assert_eq!(
            summary.counts.timeout, 1,
            "stale timer entries cannot retire twice"
        );
        connection.publish_queued(&client, &mut summary, &mut meter).unwrap();
        assert_eq!(summary.counts.sent, 1);
        assert!(connection.queued.is_empty());
        assert_eq!(connection.pending[&1].deadline, retained);
        let mut wire = Vec::new();
        loop {
            connection.session.drive(expired).unwrap();
            let len = connection.session.output().len();
            if len == 0 {
                break;
            }
            wire.extend_from_slice(connection.session.output());
            connection.session.advance_output(len);
        }
        let mut bytes = &wire[b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".len()..];
        let mut sent = Vec::new();
        while !bytes.is_empty() {
            assert!(bytes.len() >= 9);
            let len = (usize::from(bytes[0]) << 16)
                | (usize::from(bytes[1]) << 8)
                | usize::from(bytes[2]);
            let id =
                u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]])
                    & 0x7fff_ffff;
            if bytes[3] == 1 {
                sent.push(id);
            }
            bytes = &bytes[9 + len..];
        }
        assert_eq!(
            sent,
            vec![1],
            "expired intent must not allocate or publish a stream"
        );
    }

    #[test]
    fn a_full_window_publishes_now_but_a_partial_refill_keeps_its_staging_turn()
    {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let _peer =
            std::net::TcpStream::connect(listener.local_addr().unwrap())
                .unwrap();
        let (socket, _) = listener.accept().unwrap();
        socket.set_nonblocking(true).unwrap();
        let client = crate::Target::new("http://example.test/")
            .unwrap()
            .connect_to(listener.local_addr().unwrap())
            .http2()
            .resolve()
            .unwrap()
            .client();
        let began = Instant::now();
        let window = Window {
            per_connection: 8,
            start: began,
            duration: Duration::from_secs(10),
            timeout: Duration::from_millis(100),
            lifetime: None,
        };
        let first_deadline = began + window.timeout;
        let mut connection = Connection::new(
            Stream::Plain(TcpStream::from_std(socket)),
            &client,
            window,
            first_deadline,
        )
        .unwrap();
        // Empty peer SETTINGS followed by its acknowledgement of ours.
        connection.session.input().extend_from_slice(&[
            0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 0,
        ]);
        let mut timers = VecDeque::new();
        let mut turn = Turn {
            client: &client,
            window,
            halted: false,
            timers: &mut timers,
            owner: 0,
        };
        let mut flags = Flags { writable: true, ..Flags::default() };
        let mut summary = Summary::default();
        let mut meter =
            Meter::new(began, window.duration, window.per_connection, None);
        assert!(matches!(
            connection.exchange(
                &mut turn,
                &mut flags,
                &mut summary,
                &mut meter
            ),
            Step::Wait
        ));
        assert_eq!(summary.counts.sent, 8);
        assert_eq!(connection.pending.len(), 8);
        assert!(connection.queued.is_empty());
        assert_eq!(connection.pending[&1].deadline, first_deadline);

        // Stream 1 answers with :status 200 and END_STREAM. Seven others stay open.
        connection
            .session
            .input()
            .extend_from_slice(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x88]);
        assert!(matches!(
            connection.exchange(
                &mut turn,
                &mut flags,
                &mut summary,
                &mut meter
            ),
            Step::Again
        ));
        assert_eq!(summary.counts.successful, 1);
        assert_eq!(summary.counts.sent, 8);
        assert_eq!(connection.pending.len(), 7);
        assert_eq!(connection.queued.len(), 1);
        let queued = connection.queued[0];
        assert_eq!(queued.deadline, queued.began + window.timeout);
        assert!(matches!(
            connection.exchange(
                &mut turn,
                &mut flags,
                &mut summary,
                &mut meter
            ),
            Step::Wait
        ));
        assert_eq!(summary.counts.sent, 9);
        assert_eq!(connection.pending.len(), 8);
        assert!(connection.queued.is_empty());
        assert_eq!(connection.pending[&17].deadline, queued.deadline);
        assert_eq!(connection.pending[&17].began, queued.began);
        assert_eq!(summary.counts.failed(), 0);
    }
}
