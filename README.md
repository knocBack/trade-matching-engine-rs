# trade-rs

A deterministic, high-throughput matching engine and agent-based market simulator, written in Rust.

## What this is

An in-memory Central Limit Order Book (CLOB) with strict price-time priority, an order
ingestion API, a WebSocket market-data feed, and asynchronous persistence to PostgreSQL —
driven by a synthetic agent population modelled on the ABIDES simulation framework.

Targets: sustained 15,000–20,000 orders/sec, sub-millisecond median matching latency,
p99 under 5 ms, stable memory.

Full requirements: [`docs/reference/PRD-final.pdf`](docs/reference/PRD-final.pdf).

## Status

Planning. No code yet. See [`docs/ROADMAP.md`](docs/ROADMAP.md) for the milestone sequence
and [`docs/adr/`](docs/adr/) for the decision record.

## How this repo is built

Incrementally, and on purpose. Each milestone is a small, shippable step that leaves the
system working — not a leap. A single Postgres pod before a pooler; a single engine process
before a cluster; a measured bottleneck before an optimisation.

Every non-obvious choice is written down as an Architecture Decision Record in
[`docs/adr/`](docs/adr/): what the options were, what we picked, why, and what would make
us revisit it. Performance claims ship with before/after numbers.

## Layout

```
docs/
  ROADMAP.md        milestone sequence
  adr/              architecture decision records
  reference/        PRD and the ABIDES papers
learning/           persisted session learnings (see the learning dashboard)
```
