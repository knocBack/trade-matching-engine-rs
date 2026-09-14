# Roadmap

Each milestone is a small step that leaves the system working and measurable.
No milestone depends on a leap of faith from the one before it.

The rule: **measure, then add, then re-measure.** We do not optimise anything we
have not first watched be slow, and we do not add infrastructure until the simple
version has shown us a concrete reason it is not enough.

---

## Phase 1 — The engine earns its numbers (single process, no cluster)

### M0 · Scaffold
Cargo workspace that builds green. CI on push: `fmt`, `clippy -D warnings`, `test`.
ADR habit established.
**Ships:** an empty repo that is already disciplined.

### M1 · The order book
A pure library crate. No I/O, no async, no network. Limit / market / cancel with
strict price-time priority. Property tests for the invariants that must never break:
the book never crosses, FIFO holds within a price level, quantity is conserved,
and a cancelled order never fills. Criterion benchmarks give us a ns/op number.
**Ships:** a correct book and its first honest latency figure.

### M2 · The engine loop
Command in, event out. One thread owns the book and is its only writer. Feed it a
recorded command tape, get a deterministic event tape back — byte-identical on
every replay. This property is what makes everything later (recovery, debugging,
A/B experiments) possible, so we buy it now while it is cheap.
**Ships:** a binary that replays a tape.

### M3 · Order ingestion
HTTP `POST /orders`, `DELETE /orders/:id`, `GET /book`. A bounded channel between
the web layer and the engine thread. Bounded on purpose: an unbounded queue hides
overload until it becomes an out-of-memory kill.
**Ships:** a server you can curl.

### M4 · Market data egress
WebSocket feed: trade ticks, BBO updates, L2 snapshots — each carrying a monotonic
sequence number so a consumer can detect it missed something and resynchronise.
Slow consumers get dropped, not buffered forever.
**Ships:** a live feed.

### M5 · The simulator and the proof
Synthetic agents in Rust — zero-intelligence, mean-reversion, momentum — modelled
on the ABIDES agent taxonomy. Four timestamps per order (generated, ingested,
matched, egressed) so we can say exactly which stage owns which microsecond.
Latency histograms, not averages.
**Ships:** the PRD's throughput and latency evidence, on one machine, with no
database and no cluster in the way. This is our clean ceiling.

### M6 · Persistence
Trades and order state drain asynchronously to a single PostgreSQL instance,
batched, on a bounded channel, never on the matching thread. Then we re-run M5
unchanged and prove the engine's latency did not move.
**Ships:** durable trades, and evidence the hot path stayed hot.

---

## Phase 2 — The system grows around the engine

### M7 · Containers, then one small cluster
Multi-stage Docker build. Then colima with k3s: an engine pod and a Postgres pod.
One replica each. No pooler, no load balancer, no service mesh — none of those have
earned their place yet.
**Ships:** `kubectl apply -f k8s/` brings the system up locally.

### M8 · External load
A Python load-driver in the ABIDES idiom, run from outside the cluster (another
machine, or a separate profile) so the load generator stops competing with the
engine for the same CPUs. Agents are just clients — they do not need to be clever.
**Ships:** load from off-box, and honest numbers because of it.

### M9 · Durability
Journal-before-match, periodic snapshots, replay-to-recover with a checksum gate.
The determinism from M2 is what makes this work.
**Ships:** the engine survives being killed.

---

## Phase 3 — Discuss before building

These are deliberately unplanned. Each needs its own conversation, and each should
be triggered by a measured problem rather than by ambition:

- Horizontal scale (sharding by symbol; the hard constraint is that one symbol's
  price-time priority cannot be split across processes)
- High availability and leader election
- Connection pooling — only once one Postgres connection is provably the limit
- Load balancing — only once one gateway is provably the limit
- The autonomous AI-ops layer described in the PRD
- Agent collusion and market-manipulation study (the PRD's one-year horizon)
