# Resource dossier

Gathered for: matching-engine architecture, high throughput, distributed-systems
constraints, and the specific blockers flagged during planning.

**Note on coverage:** the arXiv half of the intake pipeline was rate-limited from this
network (HTTP 429, confirmed independently of the tooling) at gather time. Academic
papers are therefore under-represented and worth a second pass later. The ABIDES
papers themselves are already in `reference/`.

---

## 1. The two things that decide this project

### Single-writer, not lock-free
- [Mechanical Sympathy - The Single Writer Principle](https://mechanical-sympathy.blogspot.com/2011/09/single-writer-principle.html) - Martin Thompson's original. Contention is avoided by not sharing writes, not by making shared writes clever.
- [The LMAX Architecture](https://martinfowler.com/articles/lmax.html) - Fowler on the business logic processor. The canonical description.
- [LMAX Disruptor user guide](https://lmax-exchange.github.io/disruptor/user-guide/index.html) and the [original paper](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) - ring buffer, sequence barriers, cache-line padding. Reported: 25M+ messages/sec, sub-50 ns latencies.
- [Designing a Matching Engine in Rust: Price-Time Priority Without GC Pauses](https://dev.to/ambrainorg/designing-a-matching-engine-in-rust-price-time-priority-without-gc-pausesr-2h56) - the closest thing to our exact design. Arena allocation, zero steady-state allocation on the hot path, single-writer book, preallocated event ring. Honest about Rust's costs.

**The load-bearing warning from that last one:** kernel scheduling, interrupt handling,
CPU pinning and power management move the latency tail more than matching code does -
and serialisation at the boundary is frequently the dominant cost. Do not micro-optimise
the match while the HTTP layer eats milliseconds.

### Determinism buys recovery
- [Matching engine architecture - recovery invariants and testing](https://www.ovasylenko.com/expertise/matching-engines) - journal-before-match, snapshots to bound replay, checksum gate before trading resumes.
- [intrepidkarthi/orderbook](https://github.com/intrepidkarthi/orderbook) - Go, but a working WAL + snapshot implementation with CI-gated crash recovery against a 2,000-command tape. Good model for our M9.
- [Designing a matching engine that keeps price-time priority](https://www.techinterview.org/post/3233477258/matching-engine-price-time-priority-system-design/)

---

## 2. Order book data structures

- [Kautenja - LOB notes](https://github.com/Kautenja/limit-order-book/blob/master/notes/lob.md) *(PRD reference)* - the classical design: tree of price levels, intrusive doubly linked list per level, hash map on order ID. O(1) cancel, O(1) execute, O(log M) add for a new level. Cited Nasdaq ITCH load: 100k-200k messages/sec at peak.
- [solarpx/limitbook](https://github.com/solarpx/limitbook) *(PRD reference)* - Rust. `BTreeMap<Tick, VecDeque<Order>>` + `HashMap<OrderId, (Side, Tick)>`. Published: ~204 ns resting limit order, ~290 ns crossing, ~31 ns cancel. Our ADR-0002 starting point.
- [amankrx/matching-engine-rs](https://github.com/amankrx/matching-engine-rs) - 11.3M messages/sec at 88 ns on ITCH data. Caveat: aggregate quantity per level only, so it is a book-*building* benchmark, not order-level matching. Useful as an upper bound on what the data structure can do.
- [joaquinbejar/OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) - the lock-free-shared-book counter-example. Atomics, per-level concurrency, rich order types. Reports 165k-188k ops/sec, dropping to 61k-70k with few price levels because of per-level contention. Self-declared not production ready. Read it to understand why ADR-0003 went the other way.
- [farrellh1/order-book-rs](https://github.com/farrellh1/order-book-rs) - directly contrasts `Arc<Mutex<OrderBook>>` against a bounded-channel single-writer design. Close to our M3 shape.
- [yashksaini-coder/matchbox](https://deepwiki.com/yashksaini-coder/matchbox) - single-writer extended across processes via Redis leader election. Its `crates/engine` is pure logic with zero I/O dependencies - exactly the separation that makes replay testing possible. Relevant much later, for HA.

---

## 3. The reference Go build (PRD's benchmark)

- [I had no idea what an order book was; now I've built one that handles 18k RPS](https://blog.saikat.in/i-had-no-idea-what-an-order-book-was-now-i-ve-built-one-that-handles-18k-rps) *(PRD reference)*

Worth reading for its bug list more than its architecture. The ones that apply to us:

| Their bug | Our defence |
|---|---|
| `float64` prices compared with `!=` | ADR-0004 - integer ticks from day one |
| Heap not stable → price-time priority silently broken at equal prices | M1 property test: FIFO within a price level |
| Unfilled market-order remainder pushed into the book at price 0.0, corrupting the heap invariant | M1 property test: the book never crosses; market orders never rest |
| Kafka defaulted to 1 partition - cost a third of throughput, invisible for a long time | ADR-0006 - no infrastructure without a measured reason |
| Offset committed before the DB write succeeded → silent trade loss | M6 - acknowledge only after the write lands |
| SIGTERM killed the engine with trades still buffered | M6 - ordered drain on shutdown |
| No book rebuild after restart despite pending orders in Postgres | M9 - journal and replay |

Their own conclusion about the next ceiling: the HTTP layer itself - JSON parsing,
allocation, per-request TCP overhead. That is the wall we should expect at M3, and it
argues for a binary protocol on persistent connections when we hit it.

---

## 4. Rust plumbing

### Channels and ring buffers
- [`disruptor` crate](https://crates.io/crates/disruptor) - LMAX pattern in Rust. SPSC/MPSC/MPMC, busy-spin wait strategies, batch publication, thread affinity. Its docs use an exchange as the motivating example. Reserve for when the channel appears in a profile.
- [`rtrb`](https://github.com/mgeier/rtrb) - wait-free SPSC, extracted from an unmerged crossbeam PR. Right for a single one-hop handoff.
- **Sizing note:** 20k orders/sec is one order every 50 µs. A `tokio::sync::mpsc` send costs tens of nanoseconds. The channel is three orders of magnitude from being our problem - start simple.

### WebSocket fanout (M4's real difficulty)
- [Axum WebSocket configuration](https://deepwiki.com/tokio-rs/axum/6.1-websocket-basics) - `write_buffer_size`, max write buffer, `tcp_nodelay`.
- [axum-socket-backpressure](https://crates.io/crates/axum-socket-backpressure) - makes a subtle and important point: `Sink::send` means "the sink accepted the frame" and `flush` means "internal buffers drained to the transport." Neither means the bytes left the kernel send buffer, nor that the peer is reading. A kernel-send-queue watchdog is a far better slow-consumer detector than any application heuristic.
- [Handling 5000 websocket connections with axum](https://github.com/tokio-rs/axum/discussions/3087) - a useful failure. They stored `SplitSink` in a global `DashMap`; writing to a sink held under a map guard serialises the fanout and lets one slow socket block every other. The anti-pattern to avoid: give each connection its own task and its own channel.

**The design consequence for market data:** `broadcast` *drops* for slow receivers
(`RecvError::Lagged`); bounded `mpsc` *blocks*. For market data, dropping is correct -
a stale tick has no value - but `Lagged` must be handled explicitly by resynchronising
the client with a fresh snapshot, never silently ignored. This is why M4 carries
sequence numbers: they turn a silent gap into a detectable one.

---

## 5. Deployment - colima and k3s (M7's known blockers)

- [Colima FAQ](https://colima.run/docs/faq/) · [configuration](https://colima.run/docs/configuration/)
- [Running a local Kubernetes cluster on Apple Silicon](https://cloud-chronicles.com/kubernetes-apple-silicon/)

Found before we hit them:

1. **Use `--vm-type=vz`.** Virtualization.framework rather than QEMU is the single largest performance lever on Apple Silicon. Build native `aarch64` images - an emulated x86_64 start was measured at ~240 s just to boot the VM.
2. **The VM has no host-reachable IP by default.** That is off by default because it needs root and slows startup. Without it, `LoadBalancer` and `NodePort` are not directly reachable from macOS - you are on port-forwarding. This directly shapes how M8 drives load from outside.
3. **Resource budget.** This machine has 8 cores and 16 GB. Colima defaults to 2 CPU / 2 GB, which is not enough. Whatever we give the VM is taken from the host - so an in-VM load generator competes with the engine for the very CPUs under test. This is the concrete reason M8 moves load generation off-box.
4. `--disable=traefik` unless we specifically want to benchmark that ingress path.
5. Disk grows but does not shrink.

---

## 6. ABIDES implementation reference (M8)

- [jpmorganchase/abides-jpmc-public](https://github.com/jpmorganchase/abides-jpmc-public) - the maintained fork. Split into abides-core, abides-markets, abides-gym.
- [abides-sim/abides](https://github.com/abides-sim/abides) - the original from the papers.
- [Architecture docs](https://deepwiki.com/jpmorganchase/abides-jpmc-public)

Their background configurations are a ready-made agent mix to copy:
- **RMSC03** - 1 exchange, 1 POV market maker, 100 value agents, 25 momentum, 5000 noise
- **RMSC04** - 1 exchange, 2 market makers, 102 value, 12 momentum, 1000 noise

Per ADR-0005 we take the agent taxonomy and the population ratios, and leave the
virtual-time kernel behind.

---

## 7. Open - worth a second pass

- arXiv on market microstructure and LOB modelling (blocked by rate limiting at gather time)
- Polymarket's CLOB API architecture (PRD reference; the Medium article returns 403 to automated fetches - needs a manual read, or their official docs)
- The PRD's six YouTube references (transcript extraction needs a `YOUTUBE_API_KEY`, not currently configured)
