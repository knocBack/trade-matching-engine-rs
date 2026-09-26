# trade-rs

A deterministic, high-throughput matching engine and agent-based market simulator, written in Rust.

## What this is

An in-memory Central Limit Order Book (CLOB) with strict price-time priority and an
order-ingestion API, driven by a synthetic agent population modelled on the ABIDES
simulation framework. WebSocket market data and asynchronous PostgreSQL persistence are
planned later milestones, not current runtime surfaces.

The longer-term PRD budget is sustained 15,000-20,000 orders/sec, sub-millisecond
median matching latency, p99 under 5 ms, and stable memory. It is not the first HTTP
benchmark gate: the deliberately modest M3 baseline is recorded in
[docs/BENCHMARK.md](docs/BENCHMARK.md).

Full requirements: [`docs/reference/PRD-final.pdf`](docs/reference/PRD-final.pdf).

## Status

M0-M3 have landed: the workspace and light CI, the correct multi-symbol order book,
deterministic offline replay, and the bounded HTTP adapter. The requirements-only M4
WebSocket market-data contract is now defined for a separate implementation PR; no M4
runtime endpoint is claimed yet. See
[docs/ROADMAP.md](docs/ROADMAP.md) for the milestone sequence,
[docs/BENCHMARK.md](docs/BENCHMARK.md) for the current M3 benchmark baseline, and
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
  BENCHMARK.md      current M3 benchmark baseline
  DELIVERY.md       how work ships: roles, PR contract, merge authority
  PLAN-REVISION-REQUIREMENTS.md  written requirements for this documentation revision
  PLAN-REVISION-SCENARIOS.md     independently authored scenarios for this revision
  requirements/      milestone requirement contracts
  scenarios/         independently authored milestone scenarios
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

## HTTP adapter (M3)

`cargo run --release -p trade-http -- --symbols BTC-USD,ETH-USD` starts the
bounded HTTP/1.1 adapter over M2. See [HTTP.md](docs/HTTP.md) for endpoints,
finite limits, graceful drain and benchmark reproduction.

## Market-data contract (M4)

The normative pre-implementation WebSocket v1 contract is
[docs/requirements/M4.md](docs/requirements/M4.md); its independent acceptance inventory
is [docs/scenarios/M4.md](docs/scenarios/M4.md). Together they specify a one-symbol
feed, per-process stream identity, per-symbol contiguous sequences, initial L2 baseline,
reconnect-only resynchronisation, and bounded slow-consumer disconnection. Runtime
implementation is intentionally deferred to its own PR.
