# M3 validation handoff

The immutable contracts are `docs/requirements/M3.md` (architecture author) and
`docs/scenarios/M3.md` (independent scenario author, written before production code).
The implementer has not authored executable HTTP tests. The pipeline must assign
executable scenario coverage to its independent test/fix agent and perform a fresh
independent review afterward, preserving the three roles in `docs/DELIVERY.md`.

Required coverage: H1–H9, P1–P7, D1–D4, S1–S4, Q1–Q10, M1–M8 and benchmark B1–B5.
The actual loopback HTTP adapter must be exercised, including a deterministically
paused owner BEFORE server startup for Q1/Q9/Q10. Existing private cfg(test)
EngineHooks allows initial pause and failure without exposing production controls.
Do not substitute connection failures for the attached-client 500 in Q9 or
weaken event assertions: settled self-trade cancellation preserves taker remainder.
Keep full status/bodies, snapshot/counter reconciliation, fixed property seeds,
and test output in the evidence. No sleeps as a substitute for observed admission.

Initial run 01M2P6SM2GM65DXBPC36A7G2MB failed in its Claude fix subprocess:
DeepSeek-V4-Pro-2 was unrecognized and the 30-minute deadline expired.
No fix commits survived; custody returned user_owned at a500fbe. Full original
review/fix output is retained in initial-review-log.toon.gz; its speculative
claims and unfinished test assertions are not conformance evidence.

The implementer rebased M3 onto main 88c4251 after M2 PR #4 merged, then fixed:
- sampled queue occupancy is sampled on dequeue too; no unsigned subtraction
  from a sample that may already exclude that dequeue;
- unsupported methods on recognized endpoints now increment 405 counters;
- timed client pool uses round-robin FIFO, and snapshots use fresh untimed
  connections, avoiding expired idle keep-alive connections.

The original trailing-JSON review claim was incorrect: serde_json::from_slice
rejects trailing non-whitespace. Preserve a real HTTP regression for that fact.

Recovery run `01M3B2PDXCQQXX74WD4MV84W7S` exercised the actual loopback server
with one connection slot held by an incomplete HTTP header for 32 seconds, then
verified that a follow-up `GET /book` acquired that released slot. The transcript
is retained by the gate as `m3-slow-header-timeout-http.txt`. Hyper 1.11.1's
timer-backed HTTP/1 builder has the same 30-second default, but the adapter now
sets `header_read_timeout(Duration::from_secs(30))` explicitly so this bounded
connection behavior does not depend on that dependency default.

Completion requires explicit independent requirement-conformance and focused
captain-attention verdicts, all three canonical release runs plus stepped probes,
an open unmerged PR targeting main, and an empty or explicitly routed captain-call
inventory. No changed product policy or extra transport is authorized.
