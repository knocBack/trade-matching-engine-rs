# M4 validation handoff

The executable M4 scenarios live in `crates/http/src/lib.rs`; the normative protocol
and independent acceptance inventory are `docs/requirements/M4.md` and
`docs/scenarios/M4.md`. The focused minimum-capacity healthy-reader regression is
`healthy_reader_receives_contiguous_three_record_match_at_minimum_ring_capacity`.
It was run for the evidence captured on 2026-09-29 and passed.

The HTTP-plus-WebSocket measurement harness is `benchmark.py`. Its first release
comparison run is retained in [`baseline-2026-09-29`](baseline-2026-09-29/README.md).
That run met the carried M3 target of 500 offered order commands/s with HTTP-wall p90
at or below one second, as well as the eight-reader continuity checks. It documents
that offered-rate baseline without claiming a maximum QPS.
