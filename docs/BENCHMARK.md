# Provisional benchmark contract

This is the initial M3 baseline, not a service-level promise or the later PRD
capacity budget.

## Initial co-located baseline

- **Offered order-command HTTP rate:** 500 requests per second. RPS and QPS are
  aliases here.
- **Latency:** client-observed HTTP-wall p90 at or below one second.

The rate is offered load, not an assertion that every offered request is accepted.
The definition deliberately does not mandate an order mix, warm-up duration,
transport protocol, binary-heap identity, or a zero-rejection policy.

## Exploratory probes

M3 may separately label 100K-500K offered requests per second as
**exploratory probes**. They are not current pass thresholds, acceptance criteria, or
capacity promises.

## Evidence replaces this definition

M3 must replace or revise this initial definition with observed client HTTP-wall and
load-generator evidence. Until then, this compact baseline is only the starting
comparison point. Overload handling is assessed separately: an overload rejection
must be immediate and non-waiting, without this document selecting an HTTP response
status.
