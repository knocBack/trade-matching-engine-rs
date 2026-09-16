# trade-rs

A deterministic, high-throughput matching engine and agent-based market simulator, written in Rust.

## What this is

An in-memory Central Limit Order Book (CLOB) with strict price-time priority, an order
ingestion API, a WebSocket market-data feed, and asynchronous persistence to PostgreSQL -
driven by a synthetic agent population modelled on the ABIDES simulation framework.

The longer-term PRD budget is sustained 15,000-20,000 orders/sec, sub-millisecond
median matching latency, p99 under 5 ms, and stable memory. It is not the first HTTP
benchmark gate: the deliberately modest, provisional M3 baseline is recorded in
[docs/BENCHMARK.md](docs/BENCHMARK.md).

Full requirements: [`docs/reference/PRD-final.pdf`](docs/reference/PRD-final.pdf).

## Status

M1 landed: a correct, multi-symbol order book with strict price-time priority and
self-trade prevention (same-owner cancellation). M2 adds an offline `trade-replay`
binary that deterministically replays a canonical command tape into an event tape.
M0 scaffold already landed (Rust workspace with light CI). See
[docs/ROADMAP.md](docs/ROADMAP.md) for the milestone sequence,
[docs/BENCHMARK.md](docs/BENCHMARK.md) for the provisional benchmark contract, and
[docs/adr/](docs/adr/) for the decision record.

## How this repo is built

Incrementally, and on purpose. Each milestone is a small, shippable step that leaves the
system working - not a leap. A single Postgres pod before a pooler; a single engine process
before a cluster; a measured bottleneck before an optimisation.

Every non-obvious choice is written down as an Architecture Decision Record in
[`docs/adr/`](docs/adr/): what the options were, what we picked, why, and what would make
us revisit it. Performance claims ship with before/after numbers.

## Layout

```
docs/
  ROADMAP.md        milestone sequence
  BENCHMARK.md      provisional benchmark contract
  DELIVERY.md       how work ships: roles, PR contract, merge authority
  PLAN-REVISION-REQUIREMENTS.md  written requirements for this documentation revision
  PLAN-REVISION-SCENARIOS.md     independently authored scenarios for this revision
  RESOURCES.md      gathered source material, with what each one settles
  adr/              architecture decision records
  reference/        PRD and the ABIDES papers
```

## How work ships

The three delivery roles remain implementer, scenario/test author, and reviewer.
Every shipment also starts from a written requirement: its author need only differ
from the scenario author, and a reviewer may author it. That is an independence
check, not a mandatory fourth role.

Every PR carries written requirements, scenarios, E2E and validation results, and a
reviewer verdict with two parts: requirement conformance and whether focused captain
attention is needed. The captain approves every merge.

The full contract: [`docs/DELIVERY.md`](docs/DELIVERY.md).
