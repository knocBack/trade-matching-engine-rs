# 0007. Many symbols, one engine thread

**Status:** Accepted · **Date:** 2026-09-14 · **Milestone:** M1

## Context

A matching engine can hold one instrument or many. The choice looks like a scoping
question at M1 and turns out to be the scaling question at M7, because the only
clean way to scale a matching engine horizontally is to split it by symbol - one
symbol's price-time priority cannot be divided across processes without breaking it.

So whatever we choose now decides whether later sharding is a configuration change
or a rewrite.

## Options considered

### A. One symbol
The simplest possible engine. Cleanest benchmark numbers, because there is nothing
to attribute cost to except matching itself.

Cost: every later step that assumes many symbols - routing, per-symbol metrics,
sharding - arrives as retrofit. The `Symbol` parameter has to be threaded through
every signature that was written without it.

### B. Many symbols, one engine thread (chosen)
A map from symbol to an independent book, behind one writer thread. Realistic shape,
still fully deterministic, still no locks.

Cost: one symbol lookup on the hot path, and the benchmark has to isolate matching
cost from dispatch cost deliberately.

### C. Many symbols, one thread per symbol
The eventual sharded design, built now. Each symbol gets its own thread and its own
command queue.

Cost: real concurrency to reason about before we have any baseline to justify it.
Cross-symbol ordering becomes undefined - which is acceptable, but only once we have
decided it deliberately rather than inherited it. It also makes M2's replay property
substantially harder: a single deterministic tape across N independently scheduled
threads needs a merge discipline we have no reason to design yet.

## Decision

Option B. A map of independent books behind a single writer thread.

Each book is entirely self-contained: one symbol's book never reads another's state.
That isolation is the load-bearing property, not the threading.

## Why

Option C's threading is the part we can add later; Option C's *isolation* is the part
that is expensive to retrofit. B gives us the isolation now, at essentially no cost,
and leaves the threading decision for when a measurement asks for it.

Concretely: if M5b shows one thread saturating, turning B into C means giving each
book its own queue and thread. The books themselves do not change, because they were
never allowed to touch each other. If we had shipped A, the same move would mean
revisiting every signature in the crate.

This is the roadmap's rule applied to a design decision rather than an
infrastructure one - take the structure that keeps options open, take the
performance work only when measured.

## Consequences

- Book operations are `book(symbol).method(...)`, so the symbol lookup is on the hot
  path. It is a hash lookup against a small map; M1's benchmarks must report matching
  cost and dispatch cost separately so we know if that ever matters.
- M2's command tape carries a symbol per command. Replay stays byte-identical
  because there is still exactly one writer and one total order.
- Cross-symbol operations are forbidden by construction. If a future requirement
  needs one (a basket order, a cross-instrument risk check), it is a new ADR, not an
  incremental change.
- Per-symbol metrics come free, which M5b needs to show where load concentrates.

## What would make us revisit

- M5b shows the single engine thread saturated while cores sit idle. That is the
  signal to move to C, and the books are already shaped for it.
- The symbol lookup shows up as a measurable fraction of per-order cost in M1's
  benchmarks. That argues for interning symbols to a dense integer index and using a
  flat array instead of a map.
- A requirement appears that genuinely needs atomic action across two symbols.
