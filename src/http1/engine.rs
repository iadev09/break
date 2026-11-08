//! The HTTP/1.1 engine: one readiness loop on each worker thread, with every
//! connection a plain state in it.
//!
//! Nothing exists per request or per connection but that state: no task, no
//! waker, no timer and nothing shared between threads. The loop asks the
//! kernel which sockets changed, remembers it, and turns each owner that has
//! work in the order the work arrived. Deadlines wait in one queue per
//! thread; every request has the same budget, so the order they were set in
//! is the order they fall due.
//!
//! The coordinator still speaks to a worker through its channel, the phase's
//! start signal and the two cancellation tokens. Those are waited on with a
//! waker that interrupts the readiness loop, so a worker has one place where
//! it sleeps.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll as Polled, Wake, Waker};
use std::time::{Duration, Instant};

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use tokio::sync::mpsc::UnboundedReceiver;

use super::connection::{
    Conn, End, Flags, Step, Stream, Summary, Turn, Window,
};
#[cfg(feature = "tls")]
use super::transport::secure::Secure;
use super::transport::{Sessions, own_sessions};
use super::{Client, Outcome, is_local_resource};
use crate::load::Meter;
use crate::worker::Command;
use crate::{BreakReason, CancellationToken};

/// The token of the waker; owners use their index.
const SIGNAL: Token = Token(usize::MAX);

/// Interrupts the readiness loop when the coordinator has something to say.
struct Signal(mio::Waker);

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // A waker that cannot be written to leaves nothing to report it with;
        // the run's own deadlines still end every wait.
        let _ = self.0.wake();
    }
}

/// A connection being established: TCP first, then TLS for `https://`.
struct Setup {
    began: Instant,
    deadline: Instant,
    /// Which of the target's addresses is being tried.
    address: usize,
    stage: Stage,
}

enum Stage {
    Connecting(TcpStream),
    #[cfg(feature = "tls")]
    Securing(Box<Secure<TcpStream>>),
}

#[derive(Default)]
enum State {
    /// No connection: the next turn establishes one, or ends the phase.
    #[default]
    Idle,
    Setup(Setup),
    Open(Conn),
    /// The peer said it closes; the owner reads until it has.
    Retiring(Conn, Instant),
}

/// One declared connection of a phase: it holds a connection, replaces it
/// when it ends, and accounts for both.
#[derive(Default)]
struct Owner {
    state: State,
    flags: Flags,
    /// The socket is registered for write readiness. It is only while the
    /// owner believes it full or not yet connected: a socket with room
    /// would otherwise report every change of that room.
    armed: bool,
    /// Waiting in the run queue.
    queued: bool,
    /// Finished this phase.
    done: bool,
    summary: Summary,
    meter: Option<Meter>,
    /// In the connection test an owner is one visitor: it remembers its own
    /// TLS sessions and nobody else's.
    sessions: Option<Sessions>,
}

enum Turned {
    Wait,
    Again,
    Done,
}

/// What a setup step came to.
enum Opened {
    Pending,
    Ready(Stream),
    Failed(BreakReason),
}

struct Phase<'a> {
    client: &'a Client,
    window: Window,
    loaded: &'a std::sync::Arc<crate::load::Loaded>,
    churn: bool,
    /// The run was cancelled.
    cancelled: bool,
    /// The run or the phase was stopped.
    halted: bool,
    /// An owner failed in a way that ends the phase for everyone.
    failed: bool,
}

impl Owner {
    fn begin(
        &mut self,
        phase: &Phase<'_>,
    ) -> Result<(), BreakReason> {
        let window = phase.window;
        self.done = false;
        self.queued = false;
        self.summary = Summary::default();
        self.meter = Some(Meter::new(
            window.start,
            window.duration,
            window.per_connection,
            Some(phase.loaded.clone()),
        ));
        self.sessions =
            if phase.churn { own_sessions(phase.client) } else { None };
        if let State::Open(connection) = &mut self.state {
            connection.reserve(window)?;
        }
        Ok(())
    }

    /// Whether a deadline this owner waits on has passed.
    fn due(
        &self,
        now: Instant,
    ) -> bool {
        match &self.state {
            State::Idle => false,
            State::Setup(setup) => setup.deadline <= now,
            State::Open(connection) => {
                connection.deadline().is_some_and(|deadline| deadline <= now)
            }
            State::Retiring(_, deadline) => *deadline <= now,
        }
    }

    fn finish(
        &mut self,
        failure: Option<BreakReason>,
    ) -> Turned {
        if let State::Open(connection) = &self.state
            && self.summary.tls.is_none()
        {
            self.summary.tls = connection.stream().negotiated();
        }
        if failure.is_some() {
            self.summary.failure = failure;
        }
        if let Some(meter) = self.meter.take() {
            self.summary.load = meter.finish(Instant::now());
        }
        self.summary.at_target =
            self.summary.load.latest_first_target.is_some();
        self.done = true;
        Turned::Done
    }

    /// Start the TCP connection to one of the target's addresses.
    fn connect(
        &mut self,
        index: usize,
        address: usize,
        began: Instant,
        deadline: Instant,
        phase: &Phase<'_>,
        registry: &mio::Registry,
    ) -> io::Result<()> {
        let mut stream = TcpStream::connect(phase.client.addresses[address])?;
        registry.register(
            &mut stream,
            Token(index),
            Interest::READABLE | Interest::WRITABLE,
        )?;
        self.flags = Flags::default();
        self.armed = true;
        self.state = State::Setup(Setup {
            began,
            deadline,
            address,
            stage: Stage::Connecting(stream),
        });
        Ok(())
    }

    /// Advance a setup as far as the socket allows.
    fn open(
        &mut self,
        phase: &Phase<'_>,
    ) -> Opened {
        let State::Setup(Setup { began, deadline, address, stage }) =
            std::mem::take(&mut self.state)
        else {
            return Opened::Pending;
        };
        if phase.cancelled {
            return Opened::Failed(BreakReason::Cancelled);
        }
        // The socket is asked before the deadline is judged: a setup that
        // completes on this turn has completed.
        match self.advance(stage, phase) {
            Ok(Ok(stream)) => Opened::Ready(stream),
            Ok(Err(_)) if deadline <= Instant::now() => Opened::Failed(
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "connection deadline exceeded",
                )
                .into(),
            ),
            Ok(Err(stage)) => {
                self.state =
                    State::Setup(Setup { began, deadline, address, stage });
                Opened::Pending
            }
            Err(error) => Opened::Failed(error.into()),
        }
    }

    /// The stream once it is established, or the stage it still waits in.
    fn advance(
        &mut self,
        stage: Stage,
        phase: &Phase<'_>,
    ) -> io::Result<Result<Stream, Stage>> {
        #[cfg(feature = "tls")]
        let stream = match stage {
            Stage::Connecting(stream) => stream,
            Stage::Securing(secure) => return self.secure(secure),
        };
        #[cfg(not(feature = "tls"))]
        let Stage::Connecting(stream) = stage;
        if !self.flags.writable {
            return Ok(Err(Stage::Connecting(stream)));
        }
        // A socket that became writable has connected or failed.
        if let Some(error) = stream.take_error()? {
            return Err(error);
        }
        match stream.peer_addr() {
            Ok(_) => {}
            Err(error) if in_progress(&error) => {
                self.flags.writable = false;
                return Ok(Err(Stage::Connecting(stream)));
            }
            Err(error) => return Err(error),
        }
        stream.set_nodelay(true)?;
        #[cfg(feature = "tls")]
        if let Some(tls) = self.sessions.as_ref().or(phase.client.tls.as_ref())
        {
            let secure = Box::new(Secure::start(stream, tls)?);
            return self.secure(secure);
        }
        #[cfg(not(feature = "tls"))]
        let _ = phase;
        Ok(Ok(Stream::Plain(stream)))
    }

    /// Advance the TLS handshake as far as the socket allows.
    #[cfg(feature = "tls")]
    fn secure(
        &mut self,
        mut secure: Box<Secure<TcpStream>>,
    ) -> io::Result<Result<Stream, Stage>> {
        Ok(
            if secure.handshake_step(
                &mut self.flags.readable,
                &mut self.flags.writable,
            )? {
                Ok(Stream::Tls(secure))
            } else {
                Err(Stage::Securing(secure))
            },
        )
    }

    fn turn(
        &mut self,
        index: usize,
        phase: &mut Phase<'_>,
        timers: &mut VecDeque<(Instant, usize)>,
        registry: &mio::Registry,
    ) -> Turned {
        let turned = self.advance_state(index, phase, timers, registry);
        // Ask for write readiness exactly while the socket is believed full.
        let wanted = !self.flags.writable;
        if wanted != self.armed
            && !self.done
            && let Some(socket) = self.socket()
        {
            let interest = if wanted {
                Interest::READABLE | Interest::WRITABLE
            } else {
                Interest::READABLE
            };
            if let Err(error) =
                registry.reregister(socket, Token(index), interest)
            {
                self.state = State::Idle;
                phase.failed = true;
                return self.finish(Some(error.into()));
            }
            self.armed = wanted;
        }
        turned
    }

    /// The socket of whatever connection this owner holds.
    fn socket(&mut self) -> Option<&mut TcpStream> {
        match &mut self.state {
            State::Idle => None,
            State::Setup(setup) => Some(match &mut setup.stage {
                Stage::Connecting(stream) => stream,
                #[cfg(feature = "tls")]
                Stage::Securing(secure) => secure.socket(),
            }),
            State::Open(connection) | State::Retiring(connection, _) => {
                Some(connection.socket())
            }
        }
    }

    fn advance_state(
        &mut self,
        index: usize,
        phase: &mut Phase<'_>,
        timers: &mut VecDeque<(Instant, usize)>,
        registry: &mio::Registry,
    ) -> Turned {
        let window = phase.window;
        loop {
            match &mut self.state {
                State::Idle => {
                    let now = Instant::now();
                    if now >= window.start + window.duration || phase.halted {
                        return self.finish(None);
                    }
                    let deadline = now + window.timeout;
                    timers.push_back((deadline, index));
                    if let Err(error) =
                        self.connect(index, 0, now, deadline, phase, registry)
                    {
                        match self.failed(error.into(), now, phase) {
                            Some(turned) => return turned,
                            None => return Turned::Again,
                        }
                    }
                    return Turned::Wait;
                }
                State::Setup(setup) => {
                    let (began, deadline, address) =
                        (setup.began, setup.deadline, setup.address);
                    match self.open(phase) {
                        Opened::Pending => return Turned::Wait,
                        Opened::Ready(stream) => {
                            let elapsed = began.elapsed();
                            let meter = self.meter.as_mut().unwrap();
                            meter.stats.connect_attempts += 1;
                            meter.stats.connect_nanos += elapsed.as_nanos();
                            if self.summary.tls.is_none() {
                                self.summary.tls = stream.negotiated();
                            }
                            if phase.churn {
                                let setups = &mut self.summary.setups;
                                setups.attempted += 1;
                                setups.established += 1;
                                setups.resumed += u64::from(stream.resumed());
                                setups.established_nanos += elapsed.as_nanos();
                                setups.longest = setups.longest.max(elapsed);
                            }
                            let opened = Conn::new(
                                stream,
                                phase.client,
                                window,
                                deadline,
                            )
                            .and_then(|mut connection| {
                                connection.reserve(window)?;
                                Ok(connection)
                            });
                            match opened {
                                Ok(connection) => {
                                    self.state = State::Open(connection);
                                }
                                Err(error) => {
                                    phase.failed = true;
                                    return self.finish(Some(error));
                                }
                            }
                        }
                        Opened::Failed(error) => {
                            // Another address of the target may still answer
                            // within the same deadline.
                            let next = address + 1;
                            if matches!(&error, BreakReason::Io(error) if error.kind() != io::ErrorKind::TimedOut)
                                && next < phase.client.addresses.len()
                                && self
                                    .connect(
                                        index, next, began, deadline, phase,
                                        registry,
                                    )
                                    .is_ok()
                            {
                                return Turned::Wait;
                            }
                            self.state = State::Idle;
                            match self.failed(error, began, phase) {
                                Some(turned) => return turned,
                                None => return Turned::Again,
                            }
                        }
                    }
                }
                State::Open(connection) => {
                    let mut turn = Turn {
                        client: phase.client,
                        window,
                        halted: phase.halted,
                        timers,
                        owner: index,
                    };
                    match connection.exchange(
                        &mut turn,
                        &mut self.flags,
                        &mut self.summary,
                        self.meter.as_mut().unwrap(),
                    ) {
                        Step::Wait => return Turned::Wait,
                        Step::Again => return Turned::Again,
                        Step::End(End::Retain) => return self.finish(None),
                        Step::End(End::Reconnect) => {
                            let State::Open(ended) =
                                std::mem::take(&mut self.state)
                            else {
                                unreachable!("the connection was matched above")
                            };
                            if phase.churn && ended.peer_closing {
                                let deadline = Instant::now() + window.timeout;
                                timers.push_back((deadline, index));
                                self.state = State::Retiring(ended, deadline);
                            }
                        }
                        Step::End(End::Failed(error)) => {
                            self.state = State::Idle;
                            phase.failed = true;
                            return self.finish(Some(error));
                        }
                    }
                }
                State::Retiring(connection, deadline) => {
                    if *deadline > Instant::now()
                        && !connection.peer_closed(&mut self.flags)
                    {
                        return Turned::Wait;
                    }
                    self.state = State::Idle;
                }
            }
        }
    }

    /// Account for a setup that did not establish a connection. `None` when
    /// the owner tries again, as a visitor does in the connection test.
    fn failed(
        &mut self,
        error: BreakReason,
        began: Instant,
        phase: &mut Phase<'_>,
    ) -> Option<Turned> {
        let elapsed = began.elapsed();
        let meter = self.meter.as_mut().unwrap();
        meter.stats.connect_attempts += 1;
        meter.stats.connect_nanos += elapsed.as_nanos();
        if let BreakReason::Io(error) = &error
            && !is_local_resource(error)
        {
            let timed_out = error.kind() == io::ErrorKind::TimedOut;
            if phase.churn {
                // Here a failed setup is the measured outcome.
                self.summary.setups.attempted += 1;
                if timed_out {
                    self.summary.setups.timeout += 1;
                } else {
                    self.summary.setups.failed += 1;
                }
                return None;
            }
            if timed_out { Outcome::ConnectTimeout } else { Outcome::Connect }
                .record(&mut self.summary.counts);
        }
        phase.failed = true;
        Some(self.finish(Some(error)))
    }
}

/// Whether a connection attempt is still under way, as the socket reports it.
fn in_progress(error: &io::Error) -> bool {
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::EINPROGRESS) {
        return true;
    }
    error.kind() == io::ErrorKind::NotConnected
}

struct Engine {
    poll: Poll,
    events: Events,
    signal: Arc<Signal>,
    owners: Vec<Owner>,
    /// Owners with work, in the order it arrived.
    queue: VecDeque<usize>,
    timers: VecDeque<(Instant, usize)>,
}

impl Engine {
    fn new(kept: usize) -> io::Result<Self> {
        let poll = Poll::new()?;
        let signal =
            Arc::new(Signal(mio::Waker::new(poll.registry(), SIGNAL)?));
        let mut owners = Vec::new();
        owners.try_reserve(kept).map_err(|_| io::ErrorKind::OutOfMemory)?;
        owners.resize_with(kept, Owner::default);
        Ok(Self {
            poll,
            // The kernel hands over at most this many events at once; the
            // rest wait for the next call. It bounds no connection count.
            events: Events::with_capacity(1024),
            signal,
            owners,
            queue: VecDeque::new(),
            timers: VecDeque::new(),
        })
    }

    fn enqueue(
        owners: &mut [Owner],
        queue: &mut VecDeque<usize>,
        index: usize,
    ) {
        if let Some(owner) = owners.get_mut(index)
            && !owner.done
            && !owner.queued
        {
            owner.queued = true;
            queue.push_back(index);
        }
    }

    /// Sleep until a socket changes, the coordinator signals or `timeout`
    /// passes, and remember what the sockets said. `true` when the
    /// coordinator signalled.
    fn park(
        &mut self,
        timeout: Option<Duration>,
    ) -> bool {
        loop {
            match self.poll.poll(&mut self.events, timeout) {
                Ok(()) => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                // The readiness loop itself failed. The run's deadlines turn
                // this into failed requests, which is what it is.
                Err(_) => return true,
            }
        }
        let mut signalled = false;
        for event in &self.events {
            let Token(index) = event.token();
            if event.token() == SIGNAL {
                signalled = true;
                continue;
            }
            let Some(owner) = self.owners.get_mut(index) else {
                continue;
            };
            // An error or a closed half is found by the read or write that
            // meets it, so both sides are woken for it.
            let broken = event.is_error();
            owner.flags.ended |= event.is_read_closed() || broken;
            owner.flags.readable |=
                event.is_readable() || event.is_read_closed() || broken;
            owner.flags.writable |=
                event.is_writable() || event.is_write_closed() || broken;
            Self::enqueue(&mut self.owners, &mut self.queue, index);
        }
        signalled
    }

    /// Drive one of the coordinator's signals to its end, sleeping on the
    /// readiness loop in between.
    fn wait<F: Future>(
        &mut self,
        future: F,
    ) -> F::Output {
        let waker = Waker::from(self.signal.clone());
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let Polled::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            self.park(None);
        }
    }

    fn phase(
        &mut self,
        client: &Client,
        command: &Command,
        start: Instant,
        stop: &CancellationToken,
    ) -> crate::worker::Summary {
        let window = Window {
            start,
            duration: command.phase.duration,
            timeout: command.timeout,
            per_connection: command.phase.requests_per_connection,
            lifetime: command.churn.map(|churn| churn.requests),
        };
        let end = window.start + window.duration;
        let mut phase = Phase {
            client,
            window,
            loaded: &command.loaded,
            churn: command.churn.is_some(),
            cancelled: stop.is_cancelled(),
            halted: stop.is_cancelled() || command.phase_stop.is_cancelled(),
            failed: false,
        };
        let mut total = crate::worker::Summary::default();
        // The request test resumes each retained connection; the connection
        // test keeps none and starts its owners without one.
        if let Some(churn) = command.churn {
            self.owners.clear();
            if self.owners.try_reserve(churn.owners).is_err() {
                command.phase_stop.cancel();
                total.failure =
                    Some(BreakReason::Resources("connection owners"));
                return total;
            }
            self.owners.resize_with(churn.owners, Owner::default);
        }
        self.queue.clear();
        self.timers.clear();
        for index in 0..self.owners.len() {
            if let Err(error) = self.owners[index].begin(&phase) {
                phase.failed = true;
                self.owners[index].finish(Some(error));
            }
            Self::enqueue(&mut self.owners, &mut self.queue, index);
        }
        let mut live = self.owners.iter().filter(|owner| !owner.done).count();
        // The two tokens are watched only so that their cancellation
        // interrupts the readiness loop. What they say is read once per
        // interruption, never per request.
        let waker = Waker::from(self.signal.clone());
        let mut context = Context::from_waker(&waker);
        let mut stopped = std::pin::pin!(stop.cancelled());
        let mut phase_stopped = std::pin::pin!(command.phase_stop.cancelled());
        let mut watching = stopped.as_mut().poll(&mut context).is_pending()
            && phase_stopped.as_mut().poll(&mut context).is_pending();
        let mut announced = phase.halted;
        let mut closed = false;
        while live > 0 {
            if phase.failed && !command.phase_stop.is_cancelled() {
                command.phase_stop.cancel();
            }
            if (phase.halted || phase.failed) && !announced {
                // Every owner learns that nothing new is admitted.
                announced = true;
                phase.halted = true;
                for index in 0..self.owners.len() {
                    Self::enqueue(&mut self.owners, &mut self.queue, index);
                }
            }
            // One pass over the work that had arrived. Work an owner leaves
            // for itself goes behind the others, and the sockets are asked
            // again before it is taken up.
            for _ in 0..self.queue.len() {
                let Some(index) = self.queue.pop_front() else {
                    break;
                };
                let owner = &mut self.owners[index];
                owner.queued = false;
                if owner.done {
                    continue;
                }
                match owner.turn(
                    index,
                    &mut phase,
                    &mut self.timers,
                    self.poll.registry(),
                ) {
                    Turned::Wait => {}
                    Turned::Again => {
                        Self::enqueue(&mut self.owners, &mut self.queue, index);
                    }
                    Turned::Done => live -= 1,
                }
            }
            if live == 0 {
                break;
            }
            if phase.failed && !announced {
                continue;
            }
            let now = Instant::now();
            if !closed && now >= end {
                // The phase's window ended: owners close their admission.
                closed = true;
                for index in 0..self.owners.len() {
                    Self::enqueue(&mut self.owners, &mut self.queue, index);
                }
            }
            while let Some(&(deadline, index)) = self.timers.front() {
                if deadline > now {
                    break;
                }
                self.timers.pop_front();
                if self.owners.get(index).is_some_and(|owner| owner.due(now)) {
                    Self::enqueue(&mut self.owners, &mut self.queue, index);
                }
            }
            let timeout = if self.queue.is_empty() {
                let next = self.timers.front().map(|&(deadline, _)| deadline);
                let next = match (next, closed) {
                    (Some(deadline), false) => Some(deadline.min(end)),
                    (None, false) => Some(end),
                    (next, true) => next,
                };
                next.map(|at| at.saturating_duration_since(now))
            } else {
                Some(Duration::ZERO)
            };
            if self.park(timeout) && watching {
                phase.cancelled = stop.is_cancelled();
                phase.halted |=
                    phase.cancelled || command.phase_stop.is_cancelled();
                watching = !phase.halted;
            }
        }
        for owner in &mut self.owners {
            let summary = std::mem::take(&mut owner.summary);
            total.setups.add(summary.setups);
            total.tls = total.tls.or(summary.tls);
            total.counts.add(summary.counts);
            total.at_target += usize::from(summary.at_target);
            total.load.add(summary.load);
            total.latency.add(summary.latency);
            if let Some(failure) = summary.failure {
                total.failure.get_or_insert(failure);
            }
            if phase.churn {
                owner.state = State::Idle;
            }
        }
        total
    }
}

/// A worker thread's whole life: wait for a phase, run it, report it.
pub(crate) fn serve(
    client: Client,
    kept: usize,
    mut commands: UnboundedReceiver<Command>,
    stop: CancellationToken,
    ready: std::sync::mpsc::Sender<io::Result<()>>,
) {
    let mut engine = match Engine::new(kept) {
        Ok(engine) => engine,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    let _ = ready.send(Ok(()));
    loop {
        let command = engine.wait(async {
            tokio::select! {
                biased;
                () = stop.cancelled() => None,
                command = commands.recv() => command,
            }
        });
        let Some(mut command) = command else {
            break;
        };
        let _ = command.ready.send(());
        let start = engine.wait(async {
            tokio::select! {
                biased;
                () = stop.cancelled() => None,
                changed = command.start.wait_for(|start| start.is_some()) => {
                    changed.ok().and_then(|value| *value)
                }
            }
        });
        let summary = match start {
            Some(start) => engine.phase(&client, &command, start, &stop),
            None => crate::worker::Summary::default(),
        };
        // A worker with no connection this phase has nothing to end.
        if summary.counts.attempted > 0 || summary.failure.is_some() {
            command.loaded.finished();
        }
        let _ = command.reply.send(summary);
    }
}
