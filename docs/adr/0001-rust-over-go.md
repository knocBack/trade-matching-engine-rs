# 0001. Rust over Go for the engine core

**Status:** Accepted · **Date:** 2026-09-14 · **Milestone:** M0

## Context

The PRD allows either Go or Rust, and names the reference implementation: a Go
build that reached 18,210 req/s. Our targets are 15-20k RPS sustained, sub-millisecond
median matching latency, and **p99 under 5 ms**.

## Options considered

### Option A - Go
Fastest path. Goroutines and channels map cleanly onto a per-instrument engine loop,
and the reference build proves the target is reachable. The cost is the garbage
collector: Go's GC is very good, but it is a pause you did not schedule, and it lands
on the tail. The PRD explicitly calls out "catastrophic garbage collection pauses" as
a stability risk.

### Option B - Rust
No garbage collector, so no pause arrives behind your back. Ownership makes the
single-writer discipline a compile-time property rather than something code review
has to catch. The cost is real: slower early iteration while the borrow checker
teaches you, and a thinner ecosystem for exchange protocols.

## Decision

Rust.

## Why

The number that decides it is **p99 < 5 ms**, not the median. Median latency is easy
in both languages. A tail budget is a statement about the worst moments - and in Go,
the worst moments are partly scheduled by a runtime you do not control. In Rust,
memory is freed at points we choose.

The second reason is that the captain asked to learn the tradeoffs. Rust makes the
tradeoffs explicit: every allocation, every copy, every shared reference is visible
in the source. That is a feature for this project.

## Consequences

**Easier:** predictable tail latency; no GC tuning; the compiler enforces the
single-writer rule from ADR-0003.

**Harder:** slower first drafts; `unsafe` blocks (should we need them) demand the same
scrutiny as C; fewer off-the-shelf FIX/ITCH libraries.

**Given up:** the ability to copy the reference Go implementation directly. We use it
as a source of lessons - particularly its list of bugs - not as code.

## What would make us revisit

Nothing on the current targets. If the project pivoted to favour iteration speed over
tail latency, this would be worth reopening.
