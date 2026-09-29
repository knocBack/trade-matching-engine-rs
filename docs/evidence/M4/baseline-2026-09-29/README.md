# M4 500 offered-command/s WebSocket evidence — 2026-09-29

## Result: carried HTTP threshold and healthy-reader baseline

This release-profile loopback run met the carried M3 HTTP target and all M4
healthy-reader checks. Its HTTP-wall p90 was **0.945500 ms**, below the one-second
ceiling at 500 offered order commands/s. This baseline does not establish a
maximum QPS; the bounded generator can establish only the offered-rate result.

| Measurement | M4 `run-01` | Result |
|---|---:|---:|---|
| 500 offered order commands/s, HTTP-wall p90 | 0.945500 ms | <= 1,000 ms; pass |
| Offered / dispatched / completed | 60,000 / 60,000 / 60,000 | reconciled |
| HTTP 200 / HTTP errors / overload | 60,000 / 0 / 0 | pass |
| Healthy WebSocket readers | 8 | pass |
| Reader continuity, resync close, measured disconnect | contiguous, 0, 0 | pass |

The carried HTTP target uses a release binary profile, loopback/co-located topology,
30-second warm-up, 120-second measurement, 500 offered commands/s, four-symbol deterministic
fixture, 128 HTTP/1.1 keep-alive clients, queue capacity 256, and in-flight limit
512. The M4 run additionally has two promptly-reading WebSocket subscribers per
symbol at the default 1,024-event ring capacity.

## Measured behavior

The HTTP command sequence was complete and error-free, while every healthy reader
received exactly its expected 28,124 live records continuously. M4 adds publication,
JSON encoding, socket-write, and reader scheduling work on the same unpinned
co-located host. This single run documents the agreed offered-rate result and does
not establish a capacity ceiling.

## WebSocket observations

The eight subscribers were `A,A,B,B,C,C,D,D`. Each received its initial snapshot at
sequence 15,471, then live sequences 15,472 through 43,595 without a gap, duplicate,
stream-ID change, symbol change, resync, or disconnect during warm-up or measurement.
Each reader received 22,500 measurement frames, an observed **187.5 frames/s**. The
server reported zero lag disconnects and zero lagged messages. The harness then made
eight deliberate normal client closes after collection; these are not measured-reader
disconnects.

## Minimum-capacity safety boundary

The focused command below passed against the actual loopback adapter. Its test starts
the server with the minimum valid ring capacity of 64 and verifies that a promptly
reading subscriber receives the ordinary trade, BBO, and L2 update sequence with
sequences 3, 4, and 5 and no `1013` / `resync_required` close. This is a safety
boundary, not the recommended production setting.

```sh
cargo test -p trade-http healthy_reader_receives_contiguous_three_record_match_at_minimum_ring_capacity -- --nocapture
```

Result: one matching library test passed; no test failed.

## Reproduction and artifact inventory

Build once from the recorded commit, then run the harness. It starts one fresh server
process and retains all raw evidence under the output directory.

```sh
cargo build --release -p trade-http
python3 docs/evidence/M4/benchmark.py \
  --server target/release/trade-http \
  --output docs/evidence/M4/baseline-2026-09-29/run-01 \
  --rate 500 --warmup 30 --duration 120 --clients 128 --timeout 10 \
  --queue 256 --in-flight 512
```

`run-01/summary.json` contains derived HTTP, reconciliation, environment, final
server-counter, frame-rate, sequence, close, resync, and disconnect fields.
`samples.csv.gz` retains all 75,000 scheduled HTTP attempts, including timing and
outcome. `subscriber-01-frames.jsonl.gz` through `subscriber-08-frames.jsonl.gz`
retain every decoded WebSocket application frame with its receive time. The preload
and final snapshots plus the complete server stdout/stderr are retained alongside
them. The retained run was captured by `../benchmark.py`, version `m4-http-ws-v1`.
The current harness is version `m4-http-ws-v2`; it validates every initial frame's
required metadata, non-boolean integer fields, and top-N projection against the
captured pre-load book. It validates every live frame's version, non-boolean integer
sequence, permitted type, selected symbol, shared stream ID, and continuity. Its gate
also reconciles each reader's final sequence with the server's final per-symbol
sequence and bounds frame-rate counting to the configured measurement window.
Generated summaries retain a normalized binary name and arguments, binary hash,
runtime details, and clean-or-dirty worktree state, without local paths or
user-specific status entries.

The environment was macOS 15.7.3 on an Apple M1 with eight logical CPUs and 16 GiB
RAM. Generator and server were loopback/co-located and not CPU-pinned; power and
thermal state were not controlled. The raw summary records the exact commit, release
binary SHA-256, Rust/Python versions, invocation, and working-tree state.
