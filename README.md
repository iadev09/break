<h1>
  <img src="assets/wordmark.svg" width="188" height="96" alt="brk">
  <img src="assets/logo.svg" width="96" height="96" alt="break">
</h1>

**Finds where contracts are broken**<br>
<sub><em>Assuming your SLA breaks before your box does.</em></sub>

`break` is an HTTP benchmark that raises concurrent load until observed failures
exceed your SLA budget. You define the successful-response ratio and timeout;
`brk` drives the requested load and evaluates the actual HTTP outcomes in each
phase, in the environment where you run it.

A successful request is a complete 2xx response within budget. HTTP errors,
transport failures and timeouts count against that phase. The result shows the
last load that met your SLA and the first that did not. If the requested load
cannot be established, it returns an explicit error instead of claiming a server
limit. Request rate and resource measurements describe the run; request outcomes
decide whether it passes.

Package: `break`; command and Rust library: `brk`.

```sh
cargo install break
```

```sh
brk http://127.0.0.1:8080/api/json \
  --conn 64 --start 2 --pace 2 \
  --phase 10s --timeout 16ms --accept 99.99
```

Example output:

```text
Done (128 concurrent requests)
Done (256 concurrent requests)
Broken (384 concurrent requests)
```

Exit codes: `0` Done, `1` Broken, `2` argument parsing errors, `130` Ctrl-C.

- `--conn`: fixed connection-owner count; closed connections are replaced.
- `--start`: initial concurrent requests per connection; defaults to `pace`.
- `--pace`: additional concurrent requests per connection in each phase.
- `--phase`: time at a load level. There is no delay between requests.
- `--timeout`: request budget through the complete response, including TCP setup
  for the first request on a new connection. Addresses are resolved before load.
- `--accept`, alias `--sla`: minimum successful 2xx percentage, default 99.99.
- `--max`: optional per-connection ceiling. Without it there is no fixed final
  phase. Reaching it reports tested load, not a server breakpoint.
- `--expect-body`: additionally require an exact response body.
- `--threads`: client worker threads. Optional, but advised. Not part of the
  load; see [What `--threads` is](#what---threads-is).
- `--connect`: connect to this address instead of resolving the URL's host. The
  URL still supplies the `Host` header and, for `https://`, the server name.
- `--cacert`: for `https://`, trust the authorities in this PEM file instead of
  the platform's.

Phase zero targets `conn × start` requests. Phase `n` targets
`conn × (start + n × pace)`. A completed request frees one slot and immediately
allows its replacement. Requests/s is measured, not configured. Counters reset
per phase; pending requests retain their original phase and deadline during drain.
There is no warmup or scenario-file loader.

### What `--threads` is

The load is what you declare with `--conn`, `--start` and `--pace`: how many
requests, or connections, are outstanding at once. That is what the server is
asked to carry and what the SLA is judged on.

`--threads` is not part of it. The same connections carry the same concurrency
whether two threads drive them or forty. It is not a criterion of the
measurement and not something to tune for a better number: it only bounds how
much of the machine the client may use while it drives the declared load. It
exists so that a run is possible at all where client and server share a
machine.

It is optional, and declaring it is advised. The default depends on the
machine the client happens to run on, so a run that leaves it out is not the
same run elsewhere; a declared count is a stated condition that another run
can repeat.

- **Without it** there is one thread for every CPU the process may run on
  (following CPU affinity and cgroup quota where the platform has them), never
  more than there are connections. That suits a client with a machine of its
  own.
- **On the server's own machine** that default puts a full set of client
  threads beside the server's. The machine is then crowded, the client's own
  deadlines are noticed late, and the box gives way before the SLA does.
  Declare fewer.
- **Too few** and the client becomes the side that gives way: it cannot keep
  the declared concurrency outstanding.

The output says which of these a run was. `target-coverage` near 100% means the
client kept the declared load outstanding. `max-deadline-lateness` small beside
`--timeout` means the client observed its deadlines on time. While both hold,
whether a level is accepted does not depend on the thread count, which is the
sign that the count was a condition of the run and not a part of the
measurement. Report it that way, beside the machine, never as a load parameter.
The rate at that level can still be the client's; see
[Known limits](#known-limits).

A thread count reserves no CPU and pins nothing. On a shared machine the
boundary found belongs to client and server together: use it to compare
servers under the same conditions, not as one server's capacity. The run prints
how many worker threads it started.

The adapter implements HTTP/1.1 GET with actual pipelining on each connection.
Requests admitted together are written together: one write carries as many as
the socket takes, and each keeps its own admission time and deadline.
It reads ordered responses while filling free request slots, reuses connections
across phases, and validates complete bodies without text decoding.
Content-Length, chunked and close-delimited bodies are supported. Unsupported or
ambiguous framing is an explicit error. Pipelined outstanding requests are a
client-side measure, not a claim that all server handlers execute simultaneously.

The URL's scheme selects the transport: `http://` is TCP, `https://` is TLS on the
same connection model, with the same pipelining and the same outcomes. There is
no protocol-selection flag. The handshake belongs to connection setup: it counts
against the first request's deadline, and a certificate that does not verify is a
setup failure, never a measured request. Verification is always on. A connection
that ends without a TLS close is a transport failure, not a complete body. The
`tls` feature is enabled by default; a build without it accepts `http://` only.

```sh
brk https://example.test/plain --connect 127.0.0.1:8443 --cacert authority.pem \
  --conn 64 --start 2 --pace 2 --phase 10s --timeout 16ms
```

## Two tests

`brk` has two tests. Each raises one thing and judges one contract, and they
are run separately.

| Test | Stays fixed | Raised each phase | Decides a phase |
| --- | --- | --- | --- |
| `brk requests` (`req`, the default) | connections, kept open | concurrent requests per connection | request outcomes |
| `brk connections` (`conn`) | requests per connection | concurrent connections | connection outcomes, then request outcomes |

Everything above describes the request test: connections are opened once, and
a setup that fails there is a setup error that claims nothing about capacity.

In the connection test, establishing a connection is the measured work:

```sh
brk conn https://example.test/plain --start 64 --pace 64 --requests 1 \
  --phase 10s --timeout 100ms
```

- `--start`, `--pace`, `--max`: concurrent connections, not requests.
- `--requests`: the exact number of requests each connection carries, written
  one at a time. Pipeline depth is the request test's subject.
- `--no-resume`: for `https://`, make every handshake a full one.

Each connection is established, carries its requests and ends; its owner then
establishes the next one, with no delay. A setup is the connect call through
the end of the TLS handshake and has the declared `--timeout` as its own
deadline; each request's deadline starts when it is written. Setups are
counted apart from requests: attempted, established, failed, timed out. A phase
is accepted when the established share meets `--accept` and the requests do
too, and a break names which of the two it was.

The client asks to close with `Connection: close` on a connection's last
request and waits for the server to close. Ending the connection is then the
server's work, as it is with real clients that are told to go away, and the
closing wait state does not pile up on the client's local ports. Running out
of local ports or descriptors is a client failure and claims nothing about the
server.

What a handshake costs the server follows from what is negotiated, so that
is part of the declared load. Each phase prints the version, cipher suite and
key exchange group one of its connections settled on. Without options the TLS
library's default offer is used. `--tls-version` (`1.2` or `1.3`),
`--tls-group` (for example `X25519`, `secp256r1` or the post-quantum hybrid
`X25519MLKEM768`) and `--tls-cipher` (for example `TLS13_AES_128_GCM_SHA256`)
each offer only the named choice, in both tests. A combination the server does
not accept is a setup failure; one that cannot be offered at all is rejected
before any traffic.

Over TLS each connection owner is one visitor with its own session memory.
Sessions are resumed by default, as returning clients do; each phase reports
how many established connections resumed. A server whose session cache does
not serve a returning visitor shows up as full handshakes.

## HTTP/2

`--http2` runs either test over HTTP/2. Over `https://` only `h2` is offered
during the handshake; over `http://` the connection starts with prior
knowledge. A server that does not speak it fails setup.

In the request test the level is then concurrent streams on a connection,
where HTTP/1.1 pipelines requests: the connections stay fixed and each phase
opens more streams on them. A connection is established once the peer's
settings have arrived, within the setup deadline, so no stream is opened on an
assumed limit. A peer whose stream limit is below the level leaves the phase
unfulfilled, which is its policy and not a capacity break. A stream that
misses its deadline is reset and counted as a timeout; the connection and its
other streams stay, where an HTTP/1.1 timeout retires the whole connection.

In the connection test each connection carries its declared number of
streams, one at a time, and then says goodbye and closes.

`Done` means the declared ceiling was completed. `Broken` retains the reason,
last tested phase, last accepted phase and counters. Acceptance failure is distinct
from client I/O/resource failure, invalid protocol data or an unfulfilled target.
Those client/protocol failures do not establish server capacity.

## Load diagnostics

Each phase reports its target and observed mean outstanding requests. The mean is
the time integral of fully written requests awaiting complete responses, divided
by the active phase time. Setup gaps and connection replacement remain visible;
drain is excluded from this integral. Successful requests/s still includes drain.

`target-reached-once` records first attainment per connection. It does not prove
simultaneous global attainment or sustained load. `target-coverage` is the observed
mean divided by the target. `connection-time-at-target` is the fraction of
connection time at full pipeline depth. These are observations, not additional
acceptance thresholds. No undisclosed load tolerance is applied.

Setup attempts/time, blocked writes, maximum send delay and deadline-observation
lateness expose generator pressure. They do not independently identify server
time, network time or client scheduling delay. A worker thread turns its
connections in the order their work arrives; a write burst ends with the free
request slots or with a full socket, and reads are served before the next.
HTTP/1.1 runs on a readiness loop of its own with no task, waker or timer per
connection; HTTP/2 framing runs on Tokio.

`timeout`, `transport`, `abandoned`, `connect`, `connect-timeout`, `status` and
`body` are separate failed-attempt categories. Retiring an ordered connection
counts unexpired followers as `abandoned`; they still fail acceptance. One socket
incident is not reported as an independent transport fault for every follower.
Failed TCP setup counts a logical first-request attempt even though no HTTP
request was written. Local resource failures remain execution failures.

An initial rejection establishes no passing baseline. A rejection after a passing
phase brackets this run's observed boundary, at the declared pace resolution.
The runner does not repeat, refine or claim a confidence interval. An explicit
`--max` is tested even when it lies between normal pace levels.

## Known limits

- **HTTP/2 costs the client several times what HTTP/1.1 does.** Its framing
  still runs on Tokio with a timer for every stream, where HTTP/1.1 has a
  readiness loop of its own. On one 40-CPU Linux machine, over TLS, a request
  cost the client about 16 µs of CPU at 16 streams a connection, against
  three to four over HTTP/1.1. Give an HTTP/2 run more client threads, and
  read its rate with the next point in mind. A cheaper HTTP/2 path is
  intended and is not the priority.
- **The rate can be the client's while every diagnostic is clean.** A client
  whose threads are full still keeps the declared requests outstanding and
  still reads their responses within the deadline, only later. Coverage stays
  near 100%, lateness stays small and the phase is accepted. The verdict on
  that level stands; requests/s then describes the client, not the server.
  `brk` does not report its own CPU use: measure the process, or read it at
  the phase boundaries through the library, and compare it with the thread
  count.
- **Over TLS, HTTP/1.1 makes one read that finds nothing for each response.**
  That is client cost only and changes no outcome.
- **The request is a fixed GET.** No request body, no headers of your own, no
  scenario file, no warmup.
- **It has been run on Linux and macOS.** Nothing else has been tried.

## Library

`brk` has no scripting language and is not going to get one. Where another
tool takes a script, `brk` is a library: a bench that needs more than the
command line is a small Rust program of your own, your own `brk`.

That program decides everything around the load:

- which targets and plans run, and in what order;
- what is read at each phase boundary: the server's CPU and memory, a metrics
  endpoint, anything the machine can tell you;
- what is written, in which format, and what the process exits with;
- where it runs: on its own, or inside a test suite.

It decides nothing on the request path, and that is the point. A callback per
request would be the client's own cost and would change what is measured.
Yours run between phases, on the calling thread, and the compiler checks them
before a single request is sent. The request itself is a fixed GET with an
optional exact status and body.

```rust,no_run
use std::time::Duration;
use brk::{Broken, CancellationToken, Observer, PhaseStats, Plan, Target};

/// One line a phase: what a script would print, typed.
struct Lines(&'static str);

impl Observer for Lines {
    fn phase_finished(&mut self, stats: &PhaseStats) {
        println!(
            "{},{},{:.0},{}",
            self.0,
            stats.phase.total_concurrency,
            stats.requests_per_sec(),
            stats.counts.failed()
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plan = Plan {
        conn: 256, start: 1, pace: 1,
        phase_duration: Duration::from_secs(30),
        timeout: Duration::from_millis(100),
        accept: 99.99, max_concurrency: Some(16), threads: Some(4),
    };
    // The same plan against two builds of a server, one after the other.
    for (name, url) in [
        ("before", "http://127.0.0.1:8080/plain"),
        ("after", "http://127.0.0.1:8081/plain"),
    ] {
        let target = Target::new(url)?.expect_body("Hello, World!");
        let stop = CancellationToken::new();
        match brk::run(&target, plan, &stop, &mut Lines(name)) {
            Ok(done) => println!("{name}: ceiling reached, {} phases", done.stats.phases),
            Err(Broken { reason, stats }) => {
                let held = stats.last_accepted.map(|phase| phase.phase.total_concurrency);
                println!("{name}: {reason}; last accepted {held:?}");
            }
        }
    }
    Ok(())
}
```

`run(&target, plan, &cancellation, &mut observer)` returns `Result<Done, Broken>`;
`run_connections` does the same for the connection test.
Each owns its CPU workers and joins them before returning. Disable the CLI with
`default-features = false` when embedding the library.

```rust,no_run
use std::time::Duration;
use brk::{CancellationToken, Observer, Phase, PhaseStats, Plan, Target};

struct Measurements;
impl Observer for Measurements {
    fn phase_started(&mut self, phase: &Phase) {
        // Read the chosen server or generator counters here.
        println!("starting phase {}", phase.index);
    }
    fn phase_finished(&mut self, stats: &PhaseStats) {
        // Read the same counters again and associate their delta with this phase.
        println!("phase {}: {} completed attempts", stats.phase.index, stats.counts.attempted);
    }
}

let target = Target::new("http://127.0.0.1:8080/plain")?;
let plan = Plan {
    conn: 64, start: 2, pace: 2,
    phase_duration: Duration::from_secs(10),
    timeout: Duration::from_millis(16),
    accept: 99.99, max_concurrency: Some(8), threads: None,
};
let result = brk::run(&target, plan, &CancellationToken::new(), &mut Measurements);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`workers_started` runs once, when the worker threads are ready, with their
number. `phase_started` runs after worker readiness and before phase traffic, including
connection setup. `phase_finished` runs after every admitted attempt has finished
or timed out, including rejected and cancelled phases. DNS and CPU-worker startup
are outside these callbacks. The callbacks run synchronously on the caller, never
per request. Downstream can read CPU/RSS counters at these boundaries; generator
CPU and server CPU are separate measurements. Both result variants retain stats.

See [ARCHITECTURE.md](ARCHITECTURE.md) for module boundaries.

### Mean request latency

Every phase exposes `PhaseStats::latency`: the completed-response sample count,
exact sum in nanoseconds, and `mean_millis()` (`None` without a sample). The CLI
prints `request-avg-ms` and the sample count for every phase.

Timing starts when a request is admitted to its connection's write path, after
TCP setup, and ends after the complete HTTP response body is decoded and checked.
It includes write backpressure, queueing, network/response wait and client-side
validation, including drain. It is not server-handler CPU time. TCP setup remains
separate in `LoadStats`. The first request's existing deadline still includes
connection setup; adding request latency does not change acceptance or deadlines.

All fully decoded replies contribute, including late or status/body-failing
responses. Attempts without a complete response have no latency sample and are
never added as zero-duration replies; read the error counts alongside the mean.
Workers merge sums and counts, rather than averaging connection means. No
per-request task, shared counter, allocation or additional clock read is added.

Licensed under MIT OR Apache-2.0.
