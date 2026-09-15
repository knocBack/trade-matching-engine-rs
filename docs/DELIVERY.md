# How work ships

This is the contract every milestone follows. It was set by the captain, not inferred.

## Merge authority

The captain approves every merge. No exceptions, and no standing autonomy.
A PR sits until the captain says land it.

## Who does what

Three distinct people per shipment. They are delivery roles, not three hats on one
head.

| Role | Does | Must not be |
|---|---|---|
| **Implementer** | Writes the production code | the scenario author or reviewer |
| **Scenario/test author** | Writes scenarios, then the E2E and validation tests | the implementer or reviewer |
| **Reviewer** | Reviews the code and reports the two-part verdict | the implementer or scenario author |

Every shipment also has a written requirement. Its author must differ from the
scenario author; the reviewer may author the requirement. The requirement author is
not a fourth mandatory delivery role or person. No other author separation is added
by this rule.

The reason is blind spots. An implementer tests what they thought about; a reviewer
who wrote the scenarios reviews against their own assumptions. Keeping the three
delivery roles separate is the cheapest way to find what nobody thought about.

## Tests are written before development

Test **scenarios** come first, as prose, before any production code exists.
Not test code - scenarios. What must be true, what must never happen, what the
system does at the edges.

The scenario document goes in the PR alongside the code. The scenario author writes
it from the written requirement without reading the implementation. If a scenario is
hard to write before the code exists, that is a signal the requirement is unclear -
resolve it then, not after.

## What a PR must contain

Every PR records:

1. **Written requirements** - including the requirement author and the independence
   check against the scenario author.
2. **Scenarios** - written before development by the scenario author.
3. **E2E results** - actual output, not a claim that tests pass.
4. **Validation results** - invariant and property checks for the milestone.
5. **The reviewer's verdict**, with both explicit parts:
   - **Requirement conformance:** conforms or does not conform to the written
     requirements, with reasoning.
   - **Focused captain attention:** no focused captain attention needed, or the
     exact decision, ambiguity, or risk that needs it.

For an explicitly documentation-only PR, E2E and executable validation are recorded
as not applicable with the reason, while the PR includes the actual documentation
validation commands and outcomes. The other evidence remains required.

A PR missing the applicable evidence is not ready, regardless of whether the code is
correct.

## CI

Keep it simple and light. GitHub Actions for what needs a hosted runner; a
self-hosted open-source runner for heavier work if and when the hosted minutes
become a real constraint.

CI runs `fmt`, `clippy -D warnings`, `test`, and the milestone's property tests.
Benchmarks run but do not gate, until we have a baseline worth regressing against.

The rule from the roadmap applies here too: do not add pipeline machinery until
the simple version has shown a concrete reason it is not enough.
