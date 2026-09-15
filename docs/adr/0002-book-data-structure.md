# 0002. Order book data structures

**Status:** Accepted · **Date:** 2026-09-14 · **Milestone:** M1

## Context

A limit order book must answer, quickly and constantly: what is the best bid and
offer; add this order at this price; cancel order #12345. The traffic mix matters -
in real venues adds and cancels dominate as market makers adjust position, and
executions are a distant third. So cancel must be fast, not merely correct.

The reference notes (Kautenja) describe the classical shape. The Rust reference
(`solarpx/limitbook`) implements a simpler variant and publishes numbers: ~204 ns
for a resting limit order, ~31 ns for a cancel.

## Options considered

### Option A - Binary tree of price levels, each an intrusive doubly linked list
The textbook design. A balanced tree of price levels; at each level, a doubly linked
list of orders in arrival order; a hash map from order ID straight to the node.
Because the node carries its own list pointers and a back-pointer to its level,
cancel is O(1) - you land on the node and splice it out without scanning.
Add is O(log M) for a new price level, O(1) otherwise.

### Option B - `BTreeMap<Tick, VecDeque<Order>>` plus `HashMap<OrderId, (Side, Tick)>`
The pragmatic Rust version. `BTreeMap` keeps price levels ordered so the best price
is the first or last entry; `VecDeque` gives FIFO at each level; the hash map finds
an order's level in O(1). The flaw: cancelling means finding the order *inside* the
`VecDeque`, which is a scan of that level - O(k) in the level's depth.

### Option C - Flat array price ladder indexed by tick
Fastest possible level lookup: the price *is* the array index, so O(1) with perfect
cache locality. Requires a bounded, known price range and pre-allocating every tick,
most of which stay empty. This is what the lowest-latency venues do.

### Option D - Tombstoning
On cancellation, retain a marker and skip the tombstoned order later rather than
removing it immediately. This is intentionally generic: it is a cancellation
strategy, not a commitment to a particular queue or heap representation. It can make
the immediate cancellation path simpler, but retained tombstones require reliable
cleanup.

## Decision

Start with **Option B**, structured so that **Option A**'s O(1) cancel can be added
behind the same interface, and keep **Option C** as a documented experiment for
later. Reject **Option D** for the baseline.

## Why

Option B is the shortest path to a correct book, and correctness is M1's whole job.
Its weakness is bounded and known: cancel degrades with level depth. We will measure
that depth in M5b rather than guess at it.

Option A is strictly better on cancel but needs intrusive linked lists - which in
Rust means either arena indices or `unsafe`. That is a real design exercise and it
deserves its own milestone, with a benchmark proving it was needed.

Option C trades memory for latency in a way we cannot justify before we know our tick
range and depth profile.

Option D is not a baseline option because it cannot yet establish stable memory
behaviour or adequate cancellation cleanup. A future proposal would need to specify
and measure its cleanup discipline before it could be compared safely.

The order matters: **make it correct, measure it, then make it fast.** Doing the
arena rewrite first would mean optimising a number we have never seen.

## Consequences

**Easier:** safe Rust throughout M1; small, readable, easy to property-test.

**Harder:** a deep price level makes cancel visibly slower. We must watch for it.

**Given up:** best-in-class cancel latency, for now.

## What would make us revisit

M5b evidence showing cancel latency tracking level depth, or p99 pressure traced to
the cancel path. Revisit this ADR or create a new ADR with before/after numbers; this
is not a decision owned by ADR-0007.
