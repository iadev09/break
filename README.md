# break

`brk` raises concurrent HTTP load until a phase breaks the declared success
ratio or the client cannot establish its target. Package: `break`; library and
command: `brk`.

```sh
brk http://127.0.0.1:8080/plain \
  --conn 4 --start 1 --pace 1 \
  --phase 10s --timeout 2s --accept 99.99
```

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

Phase zero targets `conn × start` requests. Phase `n` targets
`conn × (start + n × pace)`. A completed request frees one slot and immediately
allows its replacement. Requests/s is measured, not configured. Counters reset
per phase; pending requests retain their original phase and deadline during drain.
There is no warmup, scenario-file loader or thread-count flag. CPU workers are
selected automatically from available parallelism and the connection count.

The initial adapter implements plain HTTP/1.1 GET with actual pipelining on each
connection. It reads ordered responses while filling free request slots, reuses
connections across phases, and validates complete bodies without text decoding.
Content-Length, chunked and close-delimited bodies are supported. Unsupported or
ambiguous framing is an explicit error. There are no TLS or protocol-selection
flags. Pipelined outstanding requests are a client-side measure, not a claim that
all server handlers execute simultaneously.

`Done` means the declared ceiling was completed. `Broken` retains the reason,
last tested phase, last accepted phase and counters. Acceptance failure is distinct
from client I/O/resource failure, invalid protocol data or an unfulfilled target.
Those client/protocol failures do not establish server capacity.

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
    conn: 4, start: 1, pace: 1,
    phase_duration: Duration::from_secs(10),
    timeout: Duration::from_secs(2),
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

Licensed under MIT OR Apache-2.0.
