# Documentation-only plan-revision requirements

**Author:** Codex Sol

**Source:** captain-approved plan-revision handoff
**Scope:** this evidence governs the documentation-only revision; it does not design
implementation.

1. Change only Markdown planning documents. Do not change implementation, CI, Cargo,
   dependencies, generated files, resources, or reference PDFs. Preserve the
   incremental roadmap.
2. Retain the three delivery roles: implementer, scenario/test author, and reviewer.
   The written-requirement author must differ from the scenario author; the reviewer
   may author requirements. This is not a mandatory fourth role.
3. Require the reviewer verdict to state separately requirement conformance and
   whether focused captain attention is needed.
4. In M1, include owner identity and the invariant that an owner's orders never trade
   with one another. Do not prescribe a prevention action.
5. Define the provisional co-located M3 baseline as 500 offered order-command HTTP
   requests per second (RPS and QPS are aliases) and client-observed HTTP-wall p90 at
   or below one second.
6. Require M3 to replace or revise that definition with observed HTTP-wall and
   load-generator evidence. Label 100K-500K offered requests per second as
   exploratory probes only. Require immediate, non-waiting overload rejection without
   choosing 429 or 503; do not mandate a mix, warm-up, transport protocol,
   binary-heap identity, or zero-rejection policy.
7. Split M5 into named M5a and M5b with distinct incremental outcomes.
8. Justify Rust through ownership and learning while acknowledging Go likely meets the
   later PRD budget.
9. Explain why M7 precedes M9, and require M9 to remeasure an unchanged workload.
10. Correct stale ADR cross-references. In ADR-0002, add data-structure-neutral
    Option D tombstoning and reject it for baseline stable-memory and cancellation
    cleanup grounds.
11. Mention k0rdent or Project Sveltos only as deferred, non-designed possibilities.
12. Include this artifact, a separately authored Codex Terra scenario artifact, and
    actual documentation-validation commands and outcomes in the PR body.
