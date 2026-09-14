# Architecture Decision Records

One file per decision that was not obvious. The point is not ceremony — it is that
in six months the reasoning is still here, including the options we rejected and
what would make us change our minds.

Format: `NNNN-short-title.md`, using [`TEMPLATE.md`](TEMPLATE.md).

Status is one of: `Proposed`, `Accepted`, `Superseded by NNNN`, `Revisited`.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-rust-over-go.md) | Rust over Go for the engine core | Accepted |
| [0002](0002-book-data-structure.md) | Order book data structures | Accepted |
| [0003](0003-concurrency-model.md) | Single-writer thread over lock-free shared book | Accepted |
| [0004](0004-fixed-point-prices.md) | Integer ticks, never floating point | Accepted |
| [0005](0005-abides-as-clients.md) | Simulator agents are clients, not a kernel | Accepted |
| [0006](0006-monolith-first.md) | One process before any cluster | Accepted |
