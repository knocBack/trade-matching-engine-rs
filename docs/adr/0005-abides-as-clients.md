# 0005. Simulator agents are clients, not a simulation kernel

**Status:** Accepted · **Date:** 2026-09-14 · **Milestone:** M5 / M8

## Context

ABIDES, as the two papers describe it, is a Python discrete-event simulator built
around a kernel: a single-threaded event queue advancing global virtual time, with
pairwise network latency, a cubic jitter model, per-agent computation delay, and a
per-agent PRNG seeded from one global seed so that A/B experiments stay controlled.

The PRD asks for "thousands of dummy traders" bombarding the engine, and for latency
measured at generation, ingestion, matching, and egress.

There is a genuine mismatch. ABIDES's exchange agent lives *inside* its kernel in
simulated time. Our exchange is a real server in wall-clock time. You cannot have
both: virtual time means the simulator controls the clock, and our engine's whole
purpose is to be measured against a real one.

## Options considered

### Option A — Port the ABIDES kernel to Rust and run the engine inside it
Maximum fidelity to the papers: full determinism, controlled A/B, modelled network
latency. But the engine would then be measured in *simulated* time, which tells us
nothing about whether it does 20k RPS on real hardware. It answers a research question
we are not asking.

### Option B — Agents as ordinary network clients, in wall-clock time
Agents are load generators that happen to have trading strategies. They connect over
the real API and are timed by the real clock. We keep the ABIDES *agent taxonomy* —
zero-intelligence, mean-reversion, momentum — and its idea of a fundamental value the
agents form noisy beliefs about, but we drop the virtual-time kernel.

### Option C — Both, in sequence
Option B for performance work; optionally add a virtual-time harness much later if a
research question ever needs controlled A/B.

## Decision

Option B, per the captain's direction: the agents are clients, kept deliberately
simple. Rust agents in M5 for the clean in-process ceiling; a Python ABIDES-style
driver in M8, run off-box like a load-test tool.

## Why

The PRD's deliverable is *load-test evidence* — RPS, latency histograms, profiling
data. That is a wall-clock claim about a real server. A virtual-time kernel would
produce numbers that are precise and irrelevant.

Keeping agents on the outside also keeps a clean boundary: the engine never knows it
is being simulated, so the same agents can point at a local binary, a pod in the
cluster, or a remote host with no code change.

Running the Python driver from another machine in M8 matters more than it looks: a
load generator sharing the engine's CPUs steals exactly the resource under test and
quietly flatters — or distorts — the result.

## Consequences

**Easier:** one clock; agents work unchanged against local or clustered deployments;
Python stays out of the hot path entirely.

**Harder:** runs are not bit-reproducible, because real networks are not. We get
determinism from the *engine's* command-tape replay (ADR-0003) instead, which is
where it actually matters.

**Given up:** controlled A/B market experiments and co-location studies — the research
questions from the papers. Deliberate: those belong to the one-year horizon, not the
throughput deliverable.

## What would make us revisit

A research goal that needs controlled A/B — for example the PRD's market-manipulation
study, where you must run the same market twice with one agent changed.
