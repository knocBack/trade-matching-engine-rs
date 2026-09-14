# 0004. Integer ticks, never floating point

**Status:** Accepted · **Date:** 2026-09-14 · **Milestone:** M1

## Context

The Go reference implementation used `float64` for prices and quantities, and its
author flagged this as a known defect — noting that production engines use fixed-point
integer ticks instead.

## Options considered

### Option A — `f64`
Natural to write, matches how humans say prices. But `0.1 + 0.2 != 0.3` in binary
floating point. In a matching engine that means two orders that are "the same price"
may compare unequal, so they land in different price levels and never match. Worse,
summing fills accumulates error, so the books stop balancing by fractions of a cent.

### Option B — Integer ticks (`i64` in minimum price increments)
A price is a count of the smallest tradeable increment. A tick of 0.01 makes $100.25
simply `10025`. Comparison is exact, arithmetic is exact, and the values are cheap to
hash and to use as `BTreeMap` keys.

### Option C — A decimal crate (`rust_decimal`)
Exact decimal arithmetic with human-readable semantics. Correct, but larger than a
machine word and slower per operation, and we would be paying for generality that a
fixed tick size does not need.

## Decision

Option B. Prices are `i64` ticks; quantities are `u64` base units. Conversion to and
from human decimal strings happens **only** at the API boundary.

## Why

This is the cheapest correctness decision in the entire project, and it is
irreversible in practice — changing the price type later touches every struct, every
comparison, every test fixture, and every stored row. The reference implementation is
a live demonstration of the cost of getting it wrong.

Exactness is not a nicety here. Price-time priority is defined by exact equality at a
price level. A representation where equality is approximate makes the core rule
unimplementable.

## Consequences

**Easier:** exact comparison; hashable and orderable price keys; no rounding drift in
accumulated fills; direct mapping to a future array-indexed ladder (ADR-0002 Option C).

**Harder:** every boundary needs explicit parse and format. Tick size becomes an
instrument property that must be carried and validated.

**Given up:** nothing worth keeping.

## What would make us revisit

Instruments needing more than `i64` of tick range — not plausible here.
