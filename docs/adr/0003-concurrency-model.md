# 0003. Single-writer thread over a lock-free shared book

**Status:** Accepted · **Date:** 2026-09-14 · **Milestone:** M2

## Context

The PRD asks us to "avoid traditional mutex locking on the critical hot path" and to
evaluate "lock-free patterns or bounded message passing inspired by the LMAX
Disruptor architecture."

Those are two different things, and the difference matters.

## Options considered

### Option A - `Arc<Mutex<OrderBook>>`
Every request locks the book. Simple, obviously correct, and the thing the PRD tells
us not to do. Under load every order serialises on one lock, and the lock's wait time
lands squarely on the tail.

### Option B - Lock-free shared book (atomics, per-level concurrency)
Multiple threads mutate different price levels concurrently without blocking. This is
the literal reading of "lock-free." It has two problems. The first is that published
Rust implementations of this shape report ~165k ops/sec - impressive, but the design
contends per level and degrades when few levels are hot, which is exactly the shape
of real order flow. The second is worse: **it gives up deterministic ordering.** If two
threads race on a price level, execution order depends on the OS scheduler, so the
same input can produce two different sets of fills. For an exchange that must explain
a fill to a regulator, that is not a performance tradeoff - it is a correctness failure.

### Option C - Single writer, bounded channel in front
One thread owns the book outright and is its only writer. Everything else sends it
commands over a bounded queue. No lock is needed because nothing is shared. This is
the LMAX lesson and the actual meaning of the Disruptor's "single writer principle":
avoid contention not by making shared writes cheap, but by not sharing writes at all.

## Decision

Option C. One thread, one book, one writer, fed by a bounded channel.

## Why

The critical insight is that a lock and a lock-free structure are two answers to a
question we should refuse: *how do several threads safely write the same book?* They
should not. Matching a single symbol is inherently serial - price-time priority is a
total order - so parallelising it means fighting the problem's nature and paying in
both contention and determinism.

Determinism is the property everything downstream depends on. Replay-based crash
recovery (M9) needs it. Reproducible benchmarks need it. Debugging a bad fill by
replaying the tape needs it. A/B simulation experiments need it. We buy it in M2 while
it is free.

"No mutex on the hot path" is satisfied - there is no mutex at all, because there is
nothing to protect.

## Consequences

**Easier:** no data races by construction; byte-identical replay; the compiler enforces
it, since a non-`Send` book simply cannot be moved to another thread.

**Harder:** one symbol's throughput is capped by one core. That cap is inherent, not
incidental - scale comes from more symbols on more cores (ADR-0007's future), not from
more threads on one book.

At M3, if the bounded channel cannot accept a command, ingress rejects it immediately
without waiting. The response status remains an evidence-led M3 choice rather than a
decision in this ADR.

**Given up:** nothing real. The parallelism we did not take was not correct anyway.

## What would make us revisit

A single core provably failing to reach the later PRD budget for one symbol. The
bottleneck may be network and serialisation layers rather than matching; measurements
decide.

## Implementation note

Start with `tokio::sync::mpsc` (bounded). It is simple and its cost is nowhere near our
budget. If the channel ever shows up in a profile, the `disruptor` crate offers a true
ring buffer with busy-spin waiting and thread pinning - a later ADR with numbers, not a
day-one choice.
