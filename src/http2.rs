//! HTTP/2 on the same owner loop. The level is concurrent streams on a
//! connection, where HTTP/1.1 pipelines requests.

use std::future::poll_fn;
use std::io;
use std::time::Instant;

use bytes::Bytes;
use futures_util::{StreamExt, stream::FuturesUnordered};
use http::{Request, StatusCode};
use tokio_util::sync::CancellationToken;

use crate::http1::{
    Carrier, Client, End, Outcome, Sessions, Summary, Transport, Window,
    is_local_resource, open,
};
use crate::load::Meter;
use crate::{BreakReason, Negotiated};

/// What every request of the target carries.
#[derive(Clone)]
pub(crate) struct Head {
    pub(crate) uri: http::Uri,
    /// Sent with every request, which is then a POST: one shared buffer,
    /// sent as slices of itself.
    pub(crate) send: Option<Bytes>,
}

/// Send a request body as the peer's flow control allows, in slices of the
/// one shared buffer.
async fn send_body(
    mut stream: h2::SendStream<Bytes>,
    mut rest: Bytes,
) -> Result<(), h2::Error> {
    if rest.is_empty() {
        return stream.send_data(rest, true);
    }
    while !rest.is_empty() {
        stream.reserve_capacity(rest.len());
        let capacity = match poll_fn(|cx| stream.poll_capacity(cx)).await {
            Some(capacity) => capacity?,
            None => return Err(h2::Error::from(h2::Reason::CANCEL)),
        };
        if capacity == 0 {
            continue;
        }
        let chunk = rest.split_to(capacity.min(rest.len()));
        stream.send_data(chunk, rest.is_empty())?;
    }
    Ok(())
}

pub(crate) struct Connection {
    send: h2::client::SendRequest<Bytes>,
    /// The connection's own work: frames move only while this is polled.
    driver: h2::client::Connection<Transport, Bytes>,
    negotiated: Option<Negotiated>,
    resumed: bool,
    /// Streams this connection may still open, in the connection test.
    remaining: Option<usize>,
}

/// One answered, failed or expired stream.
struct Finished {
    outcome: Outcome,
    began: Instant,
    deadline: Instant,
    /// A whole response was read, so its time is a latency sample.
    complete: bool,
}

fn io_error(error: h2::Error) -> io::Error {
    if error.is_io() {
        error.into_io().expect("an I/O error carries its source")
    } else {
        io::Error::other(error)
    }
}

/// Read one response to its end and judge it, within the stream's deadline.
/// A stream that misses the deadline is dropped, which resets it; the
/// connection and its other streams stay.
async fn stream(
    response: h2::client::ResponseFuture,
    sending: Option<(h2::SendStream<Bytes>, Bytes)>,
    status: Option<StatusCode>,
    body: Option<Bytes>,
    began: Instant,
    deadline: Instant,
) -> Finished {
    // A body goes out while the response comes in: an echo can answer
    // before the request has ended.
    let send = async move {
        match sending {
            Some((stream, body)) => send_body(stream, body).await,
            None => Ok(()),
        }
    };
    let receive = async {
        let response = response.await?;
        let received = response.status();
        let status_ok = received.is_success()
            && status.is_none_or(|expected| expected == received);
        let mut payload = response.into_body();
        let mut offset = 0;
        let mut matches = true;
        while let Some(chunk) = payload.data().await {
            let chunk = chunk?;
            if !chunk.is_empty() {
                payload.flow_control().release_capacity(chunk.len())?;
            }
            if let Some(expected) = &body {
                matches &= expected.get(offset..offset + chunk.len())
                    == Some(chunk.as_ref());
            }
            offset += chunk.len();
        }
        let body_ok = matches
            && body.as_ref().is_none_or(|expected| expected.len() == offset);
        Ok::<_, h2::Error>(if !status_ok {
            Outcome::Status
        } else if !body_ok {
            Outcome::Body
        } else {
            Outcome::Accepted
        })
    };
    let answered = tokio::time::timeout_at(deadline.into(), async {
        let (sent, received) = tokio::join!(send, receive);
        sent?;
        received
    })
    .await;
    let (outcome, complete) = match answered {
        Ok(Ok(outcome)) => (outcome, true),
        Ok(Err(_)) => (Outcome::Transport, false),
        Err(_) => (Outcome::Timeout, false),
    };
    Finished { outcome, began, deadline, complete }
}

impl Carrier for Connection {
    async fn connect(
        client: &Client,
        window: Window,
        stop: &CancellationToken,
        sessions: Option<&Sessions>,
    ) -> Result<Self, BreakReason> {
        let (transport, deadline) =
            open(client, window, stop, sessions).await?;
        let negotiated = transport.negotiated();
        let resumed = transport.resumed();
        // The connection is established once the peer's settings are in:
        // no stream is opened on an assumed limit. The preface and that wait
        // belong to the same setup deadline.
        let established = async {
            let (mut send, mut driver) = h2::client::Builder::new()
                .initial_max_send_streams(0)
                .handshake::<_, Bytes>(transport)
                .await?;
            tokio::select! {
                biased;
                ready = poll_fn(|cx| send.poll_ready(cx)) => ready?,
                closed = &mut driver => {
                    closed?;
                    return Err(h2::Error::from(h2::Reason::CONNECT_ERROR));
                }
            }
            Ok((send, driver))
        };
        let (send, driver) = tokio::select! {
            _ = stop.cancelled() => return Err(BreakReason::Cancelled),
            result = tokio::time::timeout_at(deadline.into(), established) => {
                result
                    .map_err(|_| io::Error::new(
                        io::ErrorKind::TimedOut,
                        "connection deadline exceeded",
                    ))?
                    .map_err(io_error)?
            }
        };
        Ok(Self {
            send,
            driver,
            negotiated,
            resumed,
            remaining: window.lifetime,
        })
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
        let Self { send, driver, remaining, .. } = self;
        let head = client
            .http2
            .as_ref()
            .expect("an HTTP/2 owner has its request head");
        let end = window.start + window.duration;
        let phase_end = tokio::time::sleep_until(end.into());
        let stopped = stop.cancelled();
        let phase_stopped = phase_stop.cancelled();
        tokio::pin!(phase_end, stopped, phase_stopped);
        let mut streams = FuturesUnordered::new();
        loop {
            let now = Instant::now();
            let admitting =
                now < end && !stop.is_cancelled() && !phase_stop.is_cancelled();
            if !admitting {
                meter.close_admission(now);
            }
            if *remaining == Some(0) && streams.is_empty() {
                // Every stream of this connection is answered.
                return End::Reconnect;
            }
            if !admitting && streams.is_empty() {
                return End::Retain;
            }
            let opens = admitting
                && streams.len() < window.per_connection
                && *remaining != Some(0);
            tokio::select! {
                biased;
                finished = streams.next(), if !streams.is_empty() => {
                    let finished: Finished =
                        finished.expect("a stream was outstanding");
                    let now = Instant::now();
                    meter.change(streams.len(), now);
                    if matches!(finished.outcome, Outcome::Timeout) {
                        meter.expired(finished.deadline, now);
                    }
                    if finished.complete {
                        summary
                            .latency
                            .record(now.duration_since(finished.began));
                    }
                    finished.outcome.record(&mut summary.counts);
                }
                // The peer's stream limit is honoured here: a level above it
                // is never reached, and the phase reports that.
                ready = poll_fn(|cx| send.poll_ready(cx)), if opens => {
                    let now = Instant::now();
                    let opened = ready.and_then(|()| {
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
                        .expect("a validated target URI");
                        send.send_request(request, head.send.is_none())
                    });
                    match opened {
                        Ok((response, request_body)) => {
                            if let Some(remaining) = remaining.as_mut() {
                                *remaining -= 1;
                            }
                            summary.counts.sent += 1;
                            streams.push(stream(
                                response,
                                head.send
                                    .clone()
                                    .map(|body| (request_body, body)),
                                client.status,
                                client.body.clone(),
                                now,
                                now + window.timeout,
                            ));
                            meter.change(streams.len(), now);
                        }
                        Err(error) => {
                            return lost(
                                Some(error),
                                streams.len(),
                                summary,
                                meter,
                            );
                        }
                    }
                }
                closed = &mut *driver => {
                    return lost(closed.err(), streams.len(), summary, meter);
                }
                () = &mut phase_end, if admitting => {}
                () = &mut stopped, if admitting => {}
                () = &mut phase_stopped, if admitting => {}
            }
        }
    }

    fn negotiated(&self) -> Option<Negotiated> {
        self.negotiated
    }

    fn resumed(&self) -> bool {
        self.resumed
    }

    /// With no stream left and its handle gone, the connection says goodbye
    /// and closes; that is given the declared deadline and no longer.
    async fn retire(
        self,
        window: Window,
    ) {
        let Self { send, driver, .. } = self;
        drop(send);
        let _ = tokio::time::timeout(window.timeout, driver).await;
    }
}

/// The connection ended under its outstanding streams. One incident is one
/// transport failure; the streams that went down with it are abandoned.
fn lost(
    error: Option<h2::Error>,
    outstanding: usize,
    summary: &mut Summary,
    meter: &mut Meter,
) -> End {
    for index in 0..outstanding {
        if index == 0 { Outcome::Transport } else { Outcome::Abandoned }
            .record(&mut summary.counts);
    }
    meter.change(0, Instant::now());
    match error.map(io_error) {
        Some(error) if is_local_resource(&error) => End::Failed(error.into()),
        _ => End::Reconnect,
    }
}
