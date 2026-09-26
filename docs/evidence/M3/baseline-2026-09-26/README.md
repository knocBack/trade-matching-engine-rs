# M3 canonical 500 offered-command/s baseline — 2026-09-26

## Result

The three fresh release-profile loopback runs below demonstrate the provisional M3
gate: **500 offered `POST`/`DELETE` command attempts per second** with client-observed
HTTP-wall p90 below **1,000 ms**. This is a measured baseline only. It is not a
maximum-QPS result, a high-throughput claim, or evidence of overload behavior.

Each 120-second measurement offered, dispatched, and completed 60,000 commands. All
measurement responses were HTTP 200; there were no domain errors, 503s, timeouts,
connection errors, generator-capacity misses, or outstanding requests at the cut.

| Run | HTTP-wall p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | max (ms) | Queue high-water |
|---|---:|---:|---:|---:|---:|---:|
| `run-01` | 0.464167 | 0.688292 | 0.868833 | 4.194584 | 125.184916 | 60 |
| `run-02` | 0.456125 | 0.694291 | 0.887959 | 3.467875 | 101.325916 | 29 |
| `run-03` | 0.479292 | 0.531875 | 0.558333 | 0.678750 | 23.116375 | 23 |
| Combined measurement samples (180,000) | 0.473417 | 0.631458 | 0.757625 | 2.532750 | 125.184916 | 60 (maximum) |

The complete per-run outcome counts, per-endpoint/per-mix percentile tables, offered
response times, scheduling lag, warm-up results, server counters, environment, and
configuration are in each run's generated `summary.json`. `http_wall_ms.503` is null
because no 503 response occurred. Raw per-offer records are in `samples.csv.gz`.

## Protocol and fixture

- Server: `target/release/trade-http`, fresh process per run, loopback listener,
  release profile, source commit `a514a6b653f315a50850646a6b22cd4dae2fcc20`.
- Build identity: SHA-256
  `593700fcca2d5a7560c8e0d5ed800bba224ed4a24b5cf111fdb30b6d7e335dc0`
  for the release binary used for these runs.
- Generator: `docs/evidence/M3/benchmark.py`, version `m3-http-wall-v2`, Python
  3.14.2, HTTP/1.1 keep-alive pool of 128 clients, ten-second client timeout.
- Timing: 30-second 500 offered-command/s warm-up, immediately followed by a
  120-second 500 offered-command/s measurement scheduled at 2 ms intervals. The
  harness drains all dispatched work before final snapshots and server counters.
- Fixture: symbols `A,B,C,D`, round-robin by scheduled request number; 50% q1
  non-crossing limit-buy POSTs at tick 0, 25% q1 market-sell POSTs consuming
  preloaded tick-100 buys, and 25% DELETEs of distinct protected tick `-100` buys.
  Each symbol receives two preload pools of 5,157 orders (10% margin over warm-up
  plus measurement). Control `GET /book` snapshots are on fresh connections and are
  excluded from offered commands.
- Server configuration: queue capacity 256, in-flight limit 512, connection limit
  1,024, and body limit 16,384 bytes. Each process reported 75,000 timed commands
  admitted and engine-completed (15,000 warm-up plus 60,000 measurement), no
  admission/completion difference, and one graceful engine exit.

The generated summaries capture macOS 15.7.3 on an Apple M1 with eight physical and
logical CPUs and 16 GiB RAM. Affinity was not pinned; generator and server shared the
host with the Codex worker and shared no-mistakes daemon; power and thermal state were
not controlled. The harness's recorded Git status contains only this output directory,
which it created while running; the measured source commit above was otherwise clean.

## Reproduction

From this source commit, build once and invoke the checked-in harness once for each
output directory:

```sh
cargo build --release -p trade-http
python3 docs/evidence/M3/benchmark.py --server target/release/trade-http --output docs/evidence/M3/baseline-2026-09-26/run-01 --rate 500 --warmup 30 --duration 120 --clients 128 --timeout 10 --queue 256 --in-flight 512
python3 docs/evidence/M3/benchmark.py --server target/release/trade-http --output docs/evidence/M3/baseline-2026-09-26/run-02 --rate 500 --warmup 30 --duration 120 --clients 128 --timeout 10 --queue 256 --in-flight 512
python3 docs/evidence/M3/benchmark.py --server target/release/trade-http --output docs/evidence/M3/baseline-2026-09-26/run-03 --rate 500 --warmup 30 --duration 120 --clients 128 --timeout 10 --queue 256 --in-flight 512
```

## Artifact inventory and limits

Every `run-*` directory contains:

- `samples.csv.gz`: all 75,000 scheduled offers, including phase, scheduled,
  dispatch, and full-response completion timestamps; endpoint/category/status/code;
  bytes; and outcome.
- `preload.json.gz` and `final.json.gz`: four-symbol control snapshots before timing
  and after the drain.
- `server.stdout.txt` and `server.stderr.txt`: listening record and final server
  counters, plus captured standard error.
- `summary.json`: derived outcome, latency, reconciliation, configuration, build, and
  host records.

The three runs establish only the current 500-offered-command/s, one-second-p90
baseline gate. They do not replace the separate paused-engine saturation conformance
test or establish a stepped-overload result; no speculative maximum-QPS sweep was run.
