use criterion::{Criterion, black_box, criterion_group, criterion_main};
use trade_orderbook::{OrderId, OwnerId, PriceTicks, Quantity, Side, Symbol};
use trade_replay::{Engine, EngineCommand, decode_command_tape, replay};

fn command_tape() -> Vec<u8> {
    b"{\"format\":\"trade-rs-command-tape\",\"version\":1,\"symbols\":[\"XYZ\"]}\n{\"type\":\"limit\",\"symbol\":\"XYZ\",\"order_id\":1,\"owner_id\":2,\"side\":\"sell\",\"price_ticks\":100,\"quantity\":1}\n{\"type\":\"limit\",\"symbol\":\"XYZ\",\"order_id\":2,\"owner_id\":1,\"side\":\"buy\",\"price_ticks\":100,\"quantity\":1}\n".to_vec()
}

fn benches(c: &mut Criterion) {
    let tape = command_tape();
    c.bench_function("codec/decode_two_commands", |b| {
        b.iter(|| black_box(decode_command_tape(black_box(&tape)).unwrap()))
    });
    c.bench_function("replay/end_to_end_memory_two_commands", |b| {
        b.iter(|| black_box(replay(black_box(&tape)).unwrap()))
    });
    c.bench_function("engine/one_maker_fill", |b| {
        b.iter_batched(
            || {
                let mut engine = Engine::new(vec![Symbol::new("XYZ")]).unwrap();
                let _ = engine.handle(
                    1,
                    EngineCommand::Limit {
                        symbol: Symbol::new("XYZ"),
                        order_id: OrderId(1),
                        owner_id: OwnerId(2),
                        side: Side::Sell,
                        limit_price: PriceTicks(100),
                        quantity: Quantity(1),
                    },
                );
                engine
            },
            |mut engine| {
                black_box(engine.handle(
                    2,
                    EngineCommand::Limit {
                        symbol: Symbol::new("XYZ"),
                        order_id: OrderId(2),
                        owner_id: OwnerId(1),
                        side: Side::Buy,
                        limit_price: PriceTicks(100),
                        quantity: Quantity(1),
                    },
                ))
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(replay_benches, benches);
criterion_main!(replay_benches);
