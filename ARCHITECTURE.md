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

- `src/worker.rs`: one thread for every CPU the process may run on, or the
  declared number, capped by the fixed connection count. Each owns its
  connections and phase-local counters. Channels carry phase commands/results,
  not individual requests. An HTTP/1.1 worker runs the engine below; an HTTP/2
  worker runs a current-thread Tokio executor, because that framing is driven
  through polls. That executor's one task runs outside Tokio's cooperative
  budget: the budget is per task, and shared by a thread's connections it
  starved the ones not answered first.
- `src/http1.rs`: target validation, cold address resolution and reusable GET
  or fixed-body POST bytes. Echo validation shares the sent buffer.
  Transport-specific preparation remains behind the root API.
- `src/http1/engine.rs`: the HTTP/1.1 readiness loop of a worker thread. It
  asks the kernel which sockets changed, remembers each socket's readable and
  writable side until a read finds nothing or a write finds no room, and turns
  the owners that have work in the order the work arrived. There is no task,
  waker or timer per connection or request, and nothing shared between threads
  on the request path. Deadlines wait in one queue per thread; every request
  has the same budget, so the order they were set in is the order they fall
  due. The coordinator's channel, start signal and cancellation tokens are
  waited on with a waker that interrupts the loop, and what the tokens say is
  read once per interruption. An owner establishes its connection (TCP, then
  TLS), drives it, replaces it when it ends and accounts for each.
- `src/http1/connection.rs`: actual HTTP/1 pipelining, partial writes, FIFO
  request deadlines and what ends a connection, as a state the engine turns.
  One turn does what was due, one read and the requests its free slots admit,
  then says whether it waits or has more. The free slots are admitted at one
  instant and leave in one write, as far as the socket takes them; a request
  counts as sent, and enters the load integral, when its last byte has gone. No per-request task or shared request
  counter. A response releases its slot only after complete-body validation.
  Socket-read chunks are not payload or concurrency limits. Declared
  phase/deadline timers do not pace requests. Fully sent requests
  enter the load integral; complete responses or retirement leave it. Expired
  attempts, direct transport failures and unexpired abandoned followers remain
  distinct failed outcomes. TCP setup failures are counted separately. The
  owner loop for protocols driven through polls lives here too.
- `src/http1/transport.rs`: what carries a connection's bytes. A TCP stream, or
  the same stream under TLS for an `https://` target (`tls` feature). Both expose
  non-blocking reads and writes, so the connection loop does not know which it
  drives. The TLS session is one type over the engine's sockets and Tokio's,
  and is driven directly on their readiness:
  records already owed are written before new request bytes are accepted, and
  decrypted bytes still held by the session are reported readable without
  waiting for the socket. The handshake completes inside connection setup.
  The target may declare the version, key exchange group and cipher suite it
  offers; what a connection negotiated is reported with each phase.
- `src/http2.rs`: the same owner loop over HTTP/2 (`http2` feature). One
  future per connection polls the connection's driver, opens streams while the
  level and the peer's limit allow, and collects each stream's outcome. A
  stream has its own deadline; missing it drops and resets that stream alone.
  Setup ends when the peer's settings have arrived. Each POST stream queues
  one shared body buffer; h2 frames it under the peer's flow-control windows.
  There is no separate body-capacity waiter competing with request readiness
  for a pending stream's send-side waker. The framing drives the transport
  through polls, which the TCP stream and the TLS session answer.
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

Start/finish callbacks bracket setup/traffic/drain, excluding DNS and worker
startup. The loaded/end-of-admission callbacks delimit a narrower machine CPU
window. `src/cpu.rs` reads Linux kernel counters at those boundaries;
`PhaseStats::machine` carries the machine-wide delta. Process CPU/RSS collection
remains downstream work, outside the request loop.
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

Requests are GET or fixed-body POST, over TCP or TLS. HTTP/1.1 is the default;
`--http2` selects HTTP/2 with cleartext prior knowledge or TLS ALPN. Certificate
verification is enabled unless explicitly disabled with `--insecure`. The CLI
can measure a no-traffic machine CPU baseline for the declared phase duration.
There is no warmup or scenario loader. Concurrency is outstanding client work,
not server-handler parallelism. Protocol-specific scheduling does not change
the phase contract.

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
- `tests/cli.rs`: the two tests' names, short forms, separate options, POST/echo
  arguments and incompatible TLS verification options.
- `src/cli.rs`: numeric load controls and unsupported-option rejection.
- `tests/http1.rs`: real ephemeral TCP fixtures; fixed connections and growing
  pipelines, callback ordering, per-phase counters, body/deadline failures,
  resource/setup errors, cancellation and drain. Binary POST echoes must match
  bytes, not only length; HTTP/2 tests cover bodies larger than a flow window,
  over cleartext and TLS. TLS tests also cover explicit verification opt-out.
  No deployment/server framework.
