# Roadmap

Each milestone is a small step that leaves the system working and measurable.
No milestone depends on a leap of faith from the one before it.

The rule: **measure, then add, then re-measure.** We do not optimise anything we
have not first watched be slow, and we do not add infrastructure until the simple
version has shown us a concrete reason it is not enough.

---

## Phase 1 - The engine earns its numbers (single process, no cluster)

### M0 · Scaffold
Cargo workspace that builds green. Light CI on push: `fmt`, `clippy -D warnings`,
`test`. ADR habit established, and the delivery contract in `DELIVERY.md` in force
from the first PR onward - written requirements and scenarios before code, three
separate delivery roles per shipment, captain approves every merge.
**Ships:** an empty repo that is already disciplined.

### M1 · The order book
A pure library crate. No I/O, no async, no network. Limit / market / cancel with
strict price-time priority. Property tests for the invariants that must never break:
the book never crosses, FIFO holds within a price level, quantity is conserved,
and a cancelled order never fills. Every order carries an owner identity, and an owner's orders must never trade with
one another. When an incoming order encounters a resting order from the same owner,
the resting order is cancelled (emitted as `RestingMakerCancelled`) and matching
continues with the unchanged incoming remainder. Criterion benchmarks give us a ns/op
number.

**Multi-symbol from the start** (ADR-0007): a map of independent books, each one
self-contained. One symbol's book never reads another's. That isolation is what
makes per-symbol sharding a later configuration change rather than a rewrite.
**Ships:** a correct book and its first honest latency figure.

### M2 · The engine loop
Command in, event out. One thread owns every book and is their only writer. Feed it a
recorded command tape, get a deterministic event tape back - byte-identical on
every replay. This property is what makes everything later (recovery, debugging,
A/B experiments) possible, so we buy it now while it is cheap.
**Ships:** a binary that replays a tape.

### M3 · Order ingestion
HTTP `POST /orders`, `DELETE /orders/:id`, `GET /book`. A bounded channel between
the web layer and the engine thread. Bounded on purpose: an unbounded queue hides
overload until it becomes an out-of-memory kill.

M3 measures client-observed HTTP-wall latency and establishes the initial co-located
baseline in [BENCHMARK.md](BENCHMARK.md): 500 offered order-command HTTP requests per
second with p90 at or below one second. If the channel cannot accept a command,
ingress rejects it immediately without waiting; this plan does not choose a response
status. M3 replaces or revises the initial definition using observed HTTP-wall and
load-generator evidence.

**Ships:** a server you can curl, its first measured ingestion baseline, and explicit
overload evidence.

### M4 · Market data egress
WebSocket feed: trade ticks, BBO updates, L2 snapshots - each carrying a monotonic
sequence number so a consumer can detect it missed something and resynchronise.
Slow consumers get dropped, not buffered forever.

The pre-implementation v1 protocol, sequence, resynchronisation, bounded-buffer, and
shutdown contract is [requirements/M4.md](requirements/M4.md); its independently
authored acceptance scenarios are [scenarios/M4.md](scenarios/M4.md). One connection
covers one symbol; reconnect plus a fresh initial L2 snapshot is the recovery boundary.
**Ships:** a live feed.

### M5a · The simulator workload and timing instrumentation
Synthetic agents in Rust - zero-intelligence, mean-reversion, momentum - are modelled
on the ABIDES agent taxonomy. Each order records generated, ingested, matched, and
egressed timestamps so later evidence can attribute delay to a stage.
**Ships:** a controlled Rust client workload and timing instrumentation, without
claiming a final throughput result.

### M5b · The performance evidence
Run the M5a workload against the single-machine system with no database or cluster in
the path. Report latency histograms, not averages, with workload and measurement
evidence. This is the clean ceiling used to interpret later changes; it does not
replace M3's provisional HTTP baseline.
**Ships:** the PRD's throughput and latency evidence, with attribution good enough to
compare later work against it.

### M6 · Persistence
Trades and order state drain asynchronously to a single PostgreSQL instance,
batched, on a bounded channel, never on the matching thread. Then re-run the M5b
workload unchanged and show whether the engine's latency moved.
**Ships:** durable trades, and evidence the hot path stayed hot.

---

## Phase 2 - The system grows around the engine

### M7 · Containers, then one small cluster
Multi-stage Docker build. Then colima with k3s: an engine pod and a Postgres pod.
One replica each. No pooler, no load balancer, no service mesh - none of those have
earned their place yet.

M7 comes before M9 so the project first has a small, reproducible deployment and a
known place to observe its costs. M8 then establishes the external workload. Adding
journaling and recovery only after that makes M9's unchanged-workload comparison
attributable instead of mixing deployment and durability changes.
**Ships:** `kubectl apply -f k8s/` brings the system up locally.

### M8 · External load
A Python load-driver in the ABIDES idiom, run from outside the cluster (another
machine, or a separate profile) so the load generator stops competing with the
engine for the same CPUs. Agents are just clients - they do not need to be clever.
**Ships:** load from off-box, and honest numbers because of it.

### M9 · Durability
Journal-before-match, periodic snapshots, replay-to-recover with a checksum gate.
The determinism from M2 is what makes this work. After the recovery proof, re-run
M8's workload unchanged and compare it with the preceding M8 evidence; a new
workload would obscure the durability effect.
**Ships:** the engine survives being killed, plus comparable durability remeasurement.

---

## Phase 3 - Discuss before building

These are deliberately unplanned. Each needs its own conversation, and each should
be triggered by a measured problem rather than by ambition:

- Horizontal scale (sharding by symbol; the hard constraint is that one symbol's
  price-time priority cannot be split across processes)
- High availability and leader election
- Connection pooling - only once one Postgres connection is provably the limit
- Load balancing - only once one gateway is provably the limit
- A possible fleet-management evaluation of k0rdent or Project Sveltos - deferred;
  no design or selection is in scope
- The autonomous AI-ops layer described in the PRD
- Agent collusion and market-manipulation study (the PRD's one-year horizon)
