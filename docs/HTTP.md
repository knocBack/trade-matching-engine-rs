# M3: bounded in-process HTTP adapter

Run `cargo run --release -p trade-http -- --symbols A,B,C,D`.
The server prints its bound address at startup and JSON reconciliation counters on
exit. SIGINT or SIGTERM closes admission, drains all admitted FIFO work, flushes
connected responses and joins the engine. Restart starts empty. A disconnected
client cannot infer whether an admitted command completed; the adapter never retries.

The authoritative requirements and independent pre-implementation scenarios are
[requirements/M3.md](requirements/M3.md) and [scenarios/M3.md](scenarios/M3.md).
The schema references to an M2 amendment in the architecture are already fulfilled
by the stacked M2 branch. M3 exports its existing event JSON encoder, without
changing the M2 tape format or matching engine.

The M4 feed is [specified in requirements/M4.md](requirements/M4.md). It adds
`GET /market-data?symbol=...` as a one-symbol HTTP/1.1 WebSocket upgrade, while leaving
this M3 `GET /book` API unchanged. A successful upgrade requires and selects the exact
`trade-rs.market-data.v1` subprotocol; its first text frame is an authoritative initial
top-N L2 snapshot. See that contract for the frame schemas, reconnect-only recovery,
and terminal slow-consumer behaviour.

## Configuration and finite resources

| Flag | Default | Meaning |
|---|---|---|
| `--listen` | `127.0.0.1:8080` | HTTP/1.1 address; port 0 chooses a free port |
| `--symbols` | `BTC-USD` | Comma-separated, unique nonblank exact symbols |
| `--queue-capacity` | 256 | Pending commands and snapshot messages |
| `--in-flight` | 512 | Requests from before body collection through result serialization |
| `--body-limit` | 16384 | Bytes; positive, at most 16 KiB |
| `--connections` | 1024 | Live connection tasks, including slow readers and idle keep-alive |
| `--market-data-depth` | 20 | Maximum L2 levels per side; 1 through 100 |
| `--market-data-ring-capacity` | 1024 | Per-symbol power-of-two retained-event ring |
| `--market-data-connections` | 128 | WebSocket permits; no greater than `--connections` |
| `--websocket-write-timeout-ms` | 1000 | Per-send/flush deadline |
| `--websocket-close-timeout-ms` | 1000 | Best-effort close deadline |
| `--websocket-ping-interval-ms` | 15000 | WebSocket heartbeat interval |
| `--websocket-pong-timeout-ms` | 30000 | Peer liveness deadline, greater than ping interval |
| `--websocket-write-buffer` | 65536 | Hard maximum WebSocket write buffer in bytes |
| `--websocket-inbound-limit` | 1024 | Maximum reassembled inbound application message in bytes |

All limits are positive and fixed at startup. Connection admission also uses
try-acquire: excess TCP connections are closed before HTTP parsing. This is
separate from recognized request overload (503). Hyper's request-header buffer
is capped at 16 KiB, at most 64 headers, with a 30-second header-read timeout.
There is no processing timeout after engine admission. Partial pre-admission
bodies are bounded by the body cap and request/connection permits; drain cancels
their accumulation. Operational hard termination is not graceful shutdown.

The engine owns all books on one thread. A short admission mutex protects only
sender closure and counters; it contains no book and waits for no queue capacity.
Each typed request gets one `try_send`. Commands execute once, assign consecutive
indices starting at one, and return the existing M2 batch. Snapshots share the
queue and consume no command index. Serialization and socket writes happen
outside the engine owner. Connection tasks are reaped throughout service.

## Wire examples

```sh
curl -sS localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"type":"limit","symbol":"A","order_id":1,"owner_id":7,"side":"buy","price_ticks":100,"quantity":10}'
curl -sS 'localhost:8080/book?symbol=A'
curl -sS -X DELETE 'localhost:8080/orders/1?symbol=A'
```

POST accepts only the exact limit or market field set in the requirement.
Duplicate and unknown keys, exponent/fraction syntax, invalid UTF-8, out-of-range
integers, trailing JSON and blank symbols fail before insertion. No integer goes
through floating point. Zero quantity reaches M2. Symbols that contain nonblank
characters remain byte-for-byte unchanged, including surrounding whitespace.
JSON key order/whitespace may vary. Content type is case-insensitive
`application/json`, optionally with one `charset=utf-8` parameter (case-insensitive,
unquoted). Other parameters, duplicate content-type headers and missing content
type are 415. GET and DELETE accept no body. POST accepts no query; GET and DELETE
accept exactly one `symbol` query field, decoded with strict UTF-8 percent decoding
and form-style `+` as space. DELETE IDs consist of decimal digits, without a sign,
and may include leading zeros. Unknown query fields are malformed.

Every adapter response is application/json. Pre-engine failures contain only
`error.code`; domain rejections also contain the original single M2 rejection
event. Unknown symbol/not-found is 404, duplicate live ID 409, zero quantity 422.
Successful completion, including self-trade prevention-and-continue, is 200.
Queue or request-permit saturation is 503 `ingress_overloaded`; drain/closed
engine is 503 `service_unavailable`. Unexpected lost completion is 500
`completion_unavailable`, with uncertain command outcome and service shutdown.
Malformed HTTP framing is handled by Hyper before adapter routing; such protocol
errors are not domain events. Unknown route is 404; wrong method on known route
is 405, before request admission/body collection.

## Evidence and limitations

Counters count terminal engine batches (including typed rejection) as completed;
`domain_rejected` breaks out their reasons. Snapshot completions include failed
symbol lookups and separately count rejections. Request counters describe responses
constructed by the adapter, not proof the peer received the bytes. Failed oneshot
delivery and connection I/O errors have separate counters. Queue depth/high-water
sample actual channel occupancy; concurrent dequeue may make a transient peak
unobserved. At graceful drain the queue and admitted-minus-completed counts are zero.

The deterministic Python standard-library harness in
`evidence/M3/benchmark.py` uses release loopback HTTP/1.1 keep-alive and a bounded
connection pool with no pending-offer queue. It retains every offer, including
local generator-capacity misses, timeouts and connection errors. Canonical runs
use 30s warmup then 120s at 500 offered commands/s and 128 connections. Four
symbols rotate by offer index; the category changes every four offers, keeping
each symbol's mix balanced. Match/cancel pools use q1 at ticks 100/-100, new limits
q1 at tick 0. Pools have at least 10% warmup-plus-measurement margin. IDs are
symbol scoped: match 1..pool, cancels 1000000.., new limits 2000000+offer, market
3000000+offer. Owner 1 preloads, owner 3 submits. GETs are outside offered load.

```sh
cargo build --release -p trade-http
python3 docs/evidence/M3/benchmark.py --output /tmp/m3-run-1
# Repeat twice more into fresh directories (each invocation starts a fresh server).
python3 docs/evidence/M3/benchmark.py --rate 1000 --warmup 0 --duration 5 --output /tmp/m3-probe-1000
```

Raw CSV times are monotonic seconds relative to the first scheduled offer.
HTTP-wall is full response read minus dispatch; offered response time includes
scheduling lag. All attempted responses count in the provisional p90, with missing
and rejection fractions shown separately. The harness drains its dispatched tasks
before final snapshots and SIGTERM; final server counters include preload and
warmup, and the report subtracts preload explicitly. No debug per-request logging,
affinity, container, proxy, TLS, persistence or external service is involved.
