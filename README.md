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
- `--connect`: connect to this address instead of resolving the URL's host. The
  URL still supplies the `Host` header and, for `https://`, the server name.
- `--cacert`: for `https://`, trust the authorities in this PEM file instead of
  the platform's.

Phase zero targets `conn × start` requests. Phase `n` targets
`conn × (start + n × pace)`. A completed request frees one slot and immediately
allows its replacement. Requests/s is measured, not configured. Counters reset
per phase; pending requests retain their original phase and deadline during drain.
There is no warmup, scenario-file loader or thread-count flag. CPU workers are
selected automatically from available parallelism and the connection count.

The adapter implements HTTP/1.1 GET with actual pipelining on each connection.
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

`Done` means the declared ceiling was completed. `Broken` retains the reason,
last tested phase, last accepted phase and counters. Acceptance failure is distinct
from client I/O/resource failure, invalid protocol data or an unfulfilled target.
Those client/protocol failures do not establish server capacity.

## Load diagnostics

These diagnostics are available from source; crates.io 0.1.0 does not include them.

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
time, network time or client scheduling delay. Write bursts share Tokio's
cooperative budget with reads and the other connection owners.

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

## Library

`run(&target, plan, &cancellation, &mut observer)` returns `Result<Done, Broken>`.
It owns its CPU workers and joins them before returning. Disable the CLI with
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
    accept: 99.99, max_concurrency: Some(8),
};
let result = brk::run(&target, plan, &CancellationToken::new(), &mut Measurements);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`phase_started` runs after worker readiness and before phase traffic, including
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
