# 0001. Rust over Go for the engine core

**Status:** Accepted · **Date:** 2026-09-14 · **Milestone:** M0

## Context

The PRD allows either Go or Rust and names a Go reference implementation that reached
18,210 requests per second. The later PRD budget is 15-20k sustained orders per
second, sub-millisecond median matching latency, and **p99 under 5 ms**. That
reference makes it likely that Go can meet the later budget; it is not rejected as
incapable.

## Options considered

### Option A - Go
Fastest path. Goroutines and channels map cleanly onto a per-instrument engine loop,
and the reference build makes the performance budget credible. Its garbage collector
can affect tail behaviour, so a Go implementation would need measured tail-latency
evidence and, where necessary, runtime tuning.

### Option B - Rust
No garbage collector, so ownership and lifetime are explicit in the program. The
ownership model makes the single-writer shape easier to express and inspect. The cost
is real: slower early iteration while the borrow checker teaches you, and a thinner
ecosystem for exchange protocols.

## Decision

Rust.

## Why

Rust is an ownership and learning choice. The captain wants the project to expose the
tradeoffs behind allocation, copying, sharing, and single-writer ownership rather
than hiding them behind a runtime. Rust gives that learning surface while keeping
memory lifetime explicit.

Avoiding garbage collection may make tail behaviour easier to reason about, but it
does not establish that Go would miss the PRD budget. Both language choices require
evidence. The initial HTTP baseline is deliberately provisional; later measurements,
not an unsupported language claim, settle performance.

## Consequences

**Easier:** predictable tail latency; no GC tuning; the compiler enforces the
single-writer rule from ADR-0003.

**Harder:** slower first drafts; `unsafe` blocks (should we need them) demand the same
scrutiny as C; fewer off-the-shelf FIX/ITCH libraries.

**Given up:** the ability to copy the reference Go implementation directly. We use it
as a source of lessons - particularly its list of bugs - not as code.

## What would make us revisit

If the learning priority changes, or measured delivery cost outweighs the ownership
benefit, reopen this decision with comparable evidence from both choices.
