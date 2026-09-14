# 0006. One process before any cluster

**Status:** Accepted · **Date:** 2026-09-14 · **Milestone:** M0–M7

## Context

The end goal is a distributed system on Kubernetes under colima, with a load balancer
and a database pooler. The captain's instruction is explicit: incremental steps, not
leaps; a simple Postgres pod is enough at the start; basic proof of concept first.

The Go reference is instructive here. It put Kafka, Zookeeper, Redis, and PostgreSQL
around the engine — four moving parts — and its single biggest performance win had
nothing to do with the matching code. Kafka defaulted to one partition, funnelling ten
instruments through one log. Fixing that one setting took throughput from 12,286 to
18,210 req/s. The engine was never the bottleneck; the infrastructure around it was,
and it took extensive debugging to see that.

## Options considered

### Option A — Build the target architecture now
Cluster, pooler, load balancer, message bus from the start. Every later milestone
lands in its final home. But we would be tuning a system whose baseline we never
measured, and every performance question would have five candidate causes.

### Option B — One process first, infrastructure only when measured
Phase 1 is a single binary and a single Postgres. Every added component must be
justified by a number from the previous milestone.

## Decision

Option B.

## Why

**We cannot attribute what we never isolated.** M5 produces our clean ceiling: the
engine's latency with nothing else in the path. Every component added afterwards is
measured against that line. When p99 moves, we know precisely what moved it, because
exactly one thing changed.

The reference project's Kafka-partition story is the argument in miniature. That bug
cost a third of its throughput and was invisible because it was one of many parts
added at once. With a measured baseline it would have been a single obvious step
change.

The second reason is that **components must earn their place.** A connection pooler
solves connection exhaustion — so first show one Postgres connection saturating. A load
balancer solves one gateway being the limit — so first show the gateway at its limit.
Adding either before the evidence means carrying complexity that solves nothing, while
obscuring what does.

This also serves the learning goal directly. "We added a pooler because connections
maxed out at N and here is the graph" teaches what a pooler is *for*. "We added a
pooler because production systems have one" teaches nothing.

## Consequences

**Easier:** unambiguous attribution of every latency change; a system that runs on a
laptop with one command; fast iteration.

**Harder:** some later refactoring when things do move into the cluster. Small,
because the boundaries (bounded channels, an event stream, an async persistence sink)
are the same boundaries a distributed version needs. Phase 1 is designed to make
Phase 2 a deployment change rather than a rewrite.

**Given up:** the satisfaction of an impressive early architecture diagram.

## What would make us revisit

A measurement showing single-process capacity genuinely exhausted. Note that our
20k RPS target is one order every 50 microseconds, against a match costing a few
hundred nanoseconds — so the limit will be I/O, serialisation, and the network, which
is exactly what Phase 2 addresses.
