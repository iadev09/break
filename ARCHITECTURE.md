# break

## Public API

- `src/lib.rs`: root exports; no process-global signal handler.
- `src/run.rs`: `Plan`, validation and `run -> Result<Done, Broken>`.
  Phase `n` targets `conn × (start + n × pace)`, starting at phase zero.
- `src/report.rs`: `Phase`, `Counts`, `PhaseStats`, bounded `Stats`, results,
  `BreakReason`, and the `Observer` phase-boundary contract.
- `src/acceptance.rs`: exact success-ratio comparisons, converted once from the
  plan's decimal percentage. Counts from prior phases cannot mask a rejection.
- `src/load.rs`: event-driven outstanding-request integrals, connection time at
  target, setup/send/deadline diagnostics. Owners aggregate once per phase without
  shared counters, sampling tasks or polling. First attainment is not a claim of
  sustained or simultaneous global load.
- `src/latency.rs`: phase-local completed-response timing sums and sample counts.
  Admission timestamps are stored beside FIFO deadlines after TCP setup; existing
  response observation timestamps are reused. Complete replies, including drain
  and late/invalid replies, contribute; incomplete/retired attempts do not. Worker
  aggregation combines sums/counts without shared request counters. Mean request
  latency excludes setup even though the first request's SLA deadline includes it.

## Execution

- `src/worker.rs`: available-CPU current-thread Tokio executors, capped by the
  fixed connection count. Each owns its connections and phase-local counters.
  Channels carry phase commands/results, not individual requests.
- `src/http1.rs`: target validation, cold address resolution and reusable GET
  bytes. Transport-specific preparation remains behind the root API.
- `src/http1/connection.rs`: actual HTTP/1 pipelining, partial writes, readiness,
  FIFO request deadlines and connection replacement. One persistent future per
  connection; no per-request task or shared request counter. A response releases
  its slot only after complete-body validation. Socket-read chunks are not payload
  or concurrency limits. Declared phase/deadline timers do not pace requests.
  A worker's connections are futures of one task, polled in the order they are
  woken. That task runs outside Tokio's cooperative budget: the budget is per
  task, and shared by many connections it starved the ones not answered first.
  Fully sent requests
  enter the load integral; complete responses or retirement leave it. Expired
  attempts, direct transport failures and unexpired abandoned followers remain
  distinct failed outcomes. TCP setup failures are counted separately.
- `src/http1/transport.rs`: what carries a connection's bytes. A TCP stream, or
  the same stream under TLS for an `https://` target (`tls` feature). Both expose
  socket readiness and non-blocking reads and writes, so the connection loop does
  not know which it drives. The TLS session is driven directly on that readiness:
  records already owed are written before new request bytes are accepted, and
  decrypted bytes still held by the session are reported readable without
  waiting for the socket. The handshake completes inside connection setup.
  The target may declare the version, key exchange group and cipher suite it
  offers; what a connection negotiated is reported with each phase.
- `src/http2.rs`: the same owner loop over HTTP/2 (`http2` feature). One
  future per connection polls the connection's driver, opens streams while the
  level and the peer's limit allow, and collects each stream's outcome. A
  stream has its own deadline; missing it drops and resets that stream alone.
  Setup ends when the peer's settings have arrived. The framing drives the
  transport through polls, which the TCP stream and the TLS session answer.
- `src/http1/decode.rs`: incremental response framing with httparse. Handles
  interim responses, Content-Length, chunked/trailers, close-delimited bodies and
  coalesced/fragmented replies. Payload bytes are checked in place. Ambiguous or
  unsupported framing fails explicitly; header-field storage grows as needed.

## Two tests, one phase loop

`run` is the request test and `run_connections` the connection test. Both
enter the same phase loop with a ramp that names what is raised: requests per
connection over connections that stay open, or connection owners whose
connections each carry a fixed number of requests. Nothing else differs in how
phases start, drain and report.

In the connection test a connection has a lifetime: it admits its declared
requests one at a time, sends the last with `Connection: close`, and once that
is answered waits for the peer to close before its owner establishes the next.
Setups are counted in `Setups`, apart from `Counts`: a setup that fails is
that test's outcome and the owner tries again, where in the request test it
ends the phase as a setup error. A setup has the declared timeout as its own
deadline, and the first request's budget starts when it is written. Workers
keep no connection across phases; each phase tells a worker how many owners it
runs. Over TLS every owner has its own session memory for the phase, so a
resumed handshake means the server remembered that visitor.

A phase of the connection test is accepted when its setups meet the acceptance
budget and its requests do; `BreakReason::Connections` and
`BreakReason::Acceptance` say which did not. The request test's `Unfulfilled`
check does not apply: there is no pipeline depth to establish.

## Phase ownership

Workers acknowledge readiness, then wait. The caller invokes `phase_started`,
records a common monotonic start and releases them. At phase end, admission stops;
all admitted attempts retain their deadlines until completion/failure. Only after
all workers report does the caller invoke `phase_finished`. Live connections are
retained for the next phase; its counters are fresh.

Both callbacks bracket setup/traffic/drain, excluding DNS and worker startup.
CPU/RSS collection is downstream work and never part of the request loop.
A client that cannot write the requested pipeline depth on every connection is
`Broken::Unfulfilled`, not a capacity success. Invalid input, I/O, allocation,
protocol errors and cancellation remain distinct from HTTP acceptance failure.
Timing out an ordered H1 response retires that connection; later pending attempts
are counted as failed rather than reassigned to another response or phase.

The ceiling is optional and measured per connection. `Done` establishes no
untested breakpoint. Result history retains the last accepted and last tested
phase rather than growing with every phase. Dropping the worker owner cancels
admission and joins its threads, including during callback unwinding.

## Command

- `src/cli.rs`: arguments, Ctrl-C ownership and phase/result output.
- `src/bin/brk.rs`: CLI entry point.

Transport: HTTP/1.1 GET over TCP or, selected by an `https://` URL, over TLS. No
protocol selector, warmup or scenario loader is advertised. Concurrency is outstanding client work, not server-handler
parallelism. Protocol-specific scheduling does not change the phase contract.

## Tests

- `tests/latency.rs`: empty sample sets and response-weighted aggregation.
- `tests/http1/connection.rs`: request timing independent of setup/deadline,
  complete late responses and direct errors versus cascaded abandoned attempts.
- `tests/report.rs`: acceptance guards when connection targets are unfulfilled.

- `src/acceptance.rs`: exact percentage boundaries.
- `src/run.rs`: plan validation before traffic.
- `src/load.rs`: observed load versus first attainment, separate owners reaching
  their targets at different times, and drain/cancellation boundaries.
- `src/http1.rs`: URL and client-resource classification.
- `src/http1/decode.rs`: arbitrary fragmentation, interim/chunked/pipelined
  responses, invalid framing and dynamically sized header fields.
- `tests/tls.rs`: a real TLS listener: pipelining inside records at shallow and
  deep depth, the URL's host on the wire with `connect_to`, an untrusted
  authority failing setup instead of counting as load, an owner resuming its
  own session unless resumption is declined, and a declared offer being what
  is negotiated and reported.
- `tests/connections.rs`: the connection test against a real listener: each
  connection carries its declared requests, asks to close on the last and is
  replaced; a refused setup breaks the connection contract and counts no
  request.
- `tests/http2.rs`: both tests over HTTP/2 against a real server, in
  cleartext and over TLS: streams multiplying on fixed connections, a peer's
  stream limit leaving a phase unfulfilled, and connections replaced after
  their streams.
- `tests/cli.rs`: the two tests' names, short forms and separate options.
- `src/cli.rs`: numeric load controls and unsupported-option rejection.
- `tests/http1.rs`: real ephemeral TCP fixtures; fixed connections and growing
  pipelines, callback ordering, per-phase counters, body/deadline failures,
  resource/setup errors, cancellation and drain. No deployment/server framework.
