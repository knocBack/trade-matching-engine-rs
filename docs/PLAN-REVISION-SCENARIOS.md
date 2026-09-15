# Documentation validation scenarios

**Author:** Codex Terra

**Method:** authored from the written requirements only; no implementation was
inspected.

## Delivery contract

**Given** the existing three delivery roles — implementer, scenario/test author, and
reviewer — **when** the contract is revised, **then** those roles remain distinct.
The written-requirement author differs from the scenario author, may be the reviewer,
and is not made a fourth mandatory role.

**Then** a reviewer verdict has two separately visible statements: requirement
conformance and whether focused captain attention is needed.

## M1 identity and self-trade

**Given** an order has an owner identity, **when** it could match an order with that
same owner, **then** the documents state the no-self-trade invariant without
selecting a prevention action, rejection code, or execution strategy.

## M3 offered-load measurement and overload

**Given** a co-located client offers 500 order-command HTTP requests per second
(RPS/QPS are aliases), **when** client-observed HTTP-wall latency is measured,
**then** p90 is at or below one second.

**Given** M3 evidence is produced, **when** the benchmark definition is reviewed,
**then** the observed HTTP-wall and load-generator evidence replaces or revises the
initial definition.

**Given** 100K-500K offered requests per second is mentioned, **then** it is
labeled an exploratory M3 probe, not a pass threshold or promise. The documents force
no traffic mix, warm-up duration, transport protocol, binary-heap identity, or
zero-rejection policy.

**Given** the bounded ingress is full, **when** a command is refused, **then** the
documents require immediate, non-waiting rejection without selecting HTTP 429 or 503.

## Roadmap and decision records

**Given** M5 is described, **then** M5a and M5b have distinct named outcomes. Rust's
rationale makes its ownership and learning value explicit and recognises that Go
likely meets the later PRD budget.

**Given** M7, M8, and M9 are reviewed in sequence, **then** the rationale for M7
before M9 is explicit and M9 repeats the unchanged M8 workload.

**Given** ADR-0002 presents generic Option D tombstoning, **then** it is rejected for
the baseline because stable memory and cancellation cleanup are not established.
Cross-references are current, and k0rdent or Project Sveltos is deferred only, with
no design implied.

## Final documentation review

**Given** this PR is ready for review, **then** the evidence includes a docs-only
complete diff review, diff-whitespace check, relative Markdown-link validation, and
searches for stale references. The PR body records the exact commands and outcomes;
it does not claim executable tests ran.
