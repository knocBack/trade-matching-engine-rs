# M4 500 offered-command/s WebSocket evidence — 2026-09-29

## Result: preserved non-qualifying comparison run

This release-profile loopback run met the absolute M3 HTTP threshold and all M4
healthy-reader checks, but it is **not a qualifying carried HTTP baseline**. Its
HTTP-wall p90 was **0.945500 ms**, below the one-second ceiling but **49.73%** above
the directly comparable M3 combined p90 of **0.631458 ms**. That exceeds the agreed
five-percent comparison allowance, so measurement stopped after this first fresh
process. No maximum-QPS claim, stepped sweep, or qualifying M4 HTTP baseline is
published.

| Measurement | M3 comparison | M4 `run-01` | Result |
|---|---:|---:|---|
| 500 offered order commands/s, HTTP-wall p90 | 0.631458 ms | 0.945500 ms | +49.73%; stop |
| Offered / dispatched / completed | 60,000 / 60,000 / 60,000 | 60,000 / 60,000 / 60,000 | reconciled |
| HTTP 200 / HTTP errors / overload | 60,000 / 0 / 0 | 60,000 / 0 / 0 | pass |
| Healthy WebSocket readers | not applicable | 8 | pass |
| Reader continuity, resync close, measured disconnect | not applicable | contiguous, 0, 0 | pass |

The comparison uses the M3 canonical combined measurement reported in
[`../../M3/baseline-2026-09-26/README.md`](../../M3/baseline-2026-09-26/README.md).
Both use the same release binary profile, loopback/co-located topology, 30-second
warm-up, 120-second measurement, 500 offered commands/s, four-symbol deterministic
fixture, 128 HTTP/1.1 keep-alive clients, queue capacity 256, and in-flight limit
512. The M4 run additionally has two promptly-reading WebSocket subscribers per
symbol at the default 1,024-event ring capacity. That added bounded egress work is
the material difference, not a capacity result.

## Concise RCA

The HTTP command sequence was complete and error-free, while every healthy reader
received exactly its expected 28,124 live records continuously. M4 therefore adds
real publication, JSON encoding, socket-write, and reader scheduling work on the
same unpinned co-located host. The measured p90 movement is correlated with that
added work, but this single stopped comparison cannot attribute it more finely or
establish a capacity ceiling. The appropriate next action is targeted profiling and
an approved follow-up comparison; it is not to relabel the 500 offered rate as a
maximum QPS or to waive the M3 comparison rule.

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
them. The harness is `../benchmark.py`, version `m4-http-ws-v1`.

The environment was macOS 15.7.3 on an Apple M1 with eight logical CPUs and 16 GiB
RAM. Generator and server were loopback/co-located and not CPU-pinned; power and
thermal state were not controlled. The raw summary records the exact commit, release
binary SHA-256, Rust/Python versions, invocation, and working-tree state.
