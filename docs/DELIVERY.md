# How work ships

This is the contract every milestone follows. It was set by the captain, not inferred.

## Merge authority

The captain approves every merge. No exceptions, and no standing autonomy.
A PR sits until the captain says land it.

## Who does what

Three distinct people per shipment. Not three hats on one head.

| Role | Does | Must not be |
|---|---|---|
| **Implementer** | Writes the production code | the reviewer or the test author |
| **Test author** | Writes test scenarios, then the E2E and validation tests | the implementer |
| **Reviewer** | Reviews the code and judges whether it needs captain attention | the implementer or the test author |

The reason is blind spots. An implementer tests what they thought about; a reviewer
who wrote the tests reviews against their own assumptions. Separating the three is
the cheapest way to find what nobody thought about.

## Tests are written before development

Test **scenarios** come first, as prose, before any production code exists.
Not test code - scenarios. What must be true, what must never happen, what the
system does at the edges.

The scenario document goes in the PR alongside the code. It is written by the test
author, from the requirement, without reading the implementation. If a scenario is
hard to write before the code exists, that is a signal the requirement is unclear -
resolve it then, not after.

## What a PR must contain

Every PR, no exceptions:

1. **The scenarios** - written before development started, by the test author.
2. **E2E test results** - actual output, not a claim that tests pass.
3. **Validation results** - the invariant and property checks for that milestone.
4. **The reviewer's verdict** - explicitly one of:
   - `no captain attention needed` - with the reasoning, or
   - `needs captain attention` - naming exactly what and why.

A PR missing any of these is not ready, regardless of whether the code is correct.

## CI

Keep it simple and light. GitHub Actions for what needs a hosted runner; a
self-hosted open-source runner for heavier work if and when the hosted minutes
become a real constraint.

CI runs `fmt`, `clippy -D warnings`, `test`, and the milestone's property tests.
Benchmarks run but do not gate, until we have a baseline worth regressing against.

The rule from the roadmap applies here too: do not add pipeline machinery until
the simple version has shown a concrete reason it is not enough.
