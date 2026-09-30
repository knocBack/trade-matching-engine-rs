# trade-rs — the whole project, in one page

## The system

```
                    ┌───────────────────────────────────────────────┐
                    │  crates/http        (tokio, async)            │
   POST /orders ───►│                                               │
   DELETE /orders/:id│  • bounded admission (try-acquire)           │
   GET  /book   ───►│  • every limit FIXED AT STARTUP               │
                    │  • assigns seq + timestamp  ◄── ONLY CLOCK    │
                    └───────────────┬───────────────────────────────┘
                                    │ bounded channel (cap 256)
                                    │ full ⇒ reject NOW, never block
                                    ▼
                    ┌───────────────────────────────────────────────┐
                    │  crates/orderbook   ONE THREAD, ONE WRITER    │
                    │                                               │
                    │  MultiSymbolBook { HashMap<Symbol, OrderBook> }│
                    │      └ OrderBook { bids, asks, live_orders }  │
                    │                                               │
                    │  execute(&mut self, Command) -> CommandOutcome│
                    │  PURE: no clock, no I/O, no threads, no async │
                    └───────────────┬───────────────────────────────┘
                                    │ Vec<Effect>
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
        ┌───────────────────────┐      ┌────────────────────────────┐
        │ WebSocket feed (M4)   │      │ crates/replay              │
        │ GET /market-data      │      │ command tape ─► event tape │
        │ • monotonic seq nums  │      │ byte-identical every run   │
        │ • per-symbol ring buf │      └────────────────────────────┘
        │ • SLOW CONSUMER       │
        │   DROPPED, not buffered│     ┌────────────────────────────┐
        │ • reconnect = fresh    │     │ Postgres (M6, NOT BUILT)   │
        │   L2 snapshot          │     │ batched, bounded channel,  │
        └───────────────────────┘      │ never on matching thread   │
                                       └────────────────────────────┘
```

## The one-sentence architecture
**A pure, single-threaded, deterministic core, wrapped in bounded async edges.**

Everything impure — clocks, sockets, disks — is in the outer ring. The middle is a
function. Every resource between them is finite and fixed at startup.

## Milestones — know this line exactly
| | What | Status |
|---|---|---|
| M0 | Cargo workspace, CI (`fmt`, `clippy -D warnings`, `test`), ADR + delivery discipline | **done** |
| M1 | Multi-symbol order book, price-time priority, self-trade prevention, property tests, criterion benches | **done** |
| M2 | Engine loop; command tape -> event tape, byte-identical replay | **done** |
| M3 | HTTP adapter; bounded channel; baseline 500 req/s offered, p90 <= 1s | **done** |
| M4 | WebSocket market data; seq numbers; slow consumers dropped | **done** |
| M5a | ABIDES-style Rust simulator agents + per-stage timestamps | not built |
| M5b | The real latency/throughput evidence (histograms, not averages) | not built |
| M6 | Postgres persistence, batched, off the hot path | not built |
| M7 | Docker multi-stage, k3s: engine pod + Postgres pod | not built |
| M8 | External load driver, off-box | not built |
| M9 | Journal-before-match, snapshots, replay-to-recover | not built |
