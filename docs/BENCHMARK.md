# M3 benchmark baseline

The qualifying three-run release-profile loopback evidence in
[`evidence/M3/baseline-2026-09-26`](evidence/M3/baseline-2026-09-26/README.md)
is the current M3 baseline. It is not a service-level promise or the later PRD
capacity budget.

## Current co-located baseline

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

## Canonical evidence

The canonical evidence records observed client HTTP-wall and load-generator results
for the current baseline. Overload handling is assessed separately: an overload
rejection must be immediate and non-waiting, without this document selecting an HTTP
response status.
