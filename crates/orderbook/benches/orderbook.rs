use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use trade_orderbook::{
    Command, MultiSymbolBook, OrderBook, OrderId, OwnerId, PriceTicks, Quantity, Side, Symbol,
};

fn symbol(value: &str) -> Symbol {
    Symbol::new(value)
}

fn limit(symbol_name: &str, id: u64, owner: u64, side: Side, price: i64, quantity: u64) -> Command {
    Command::Limit {
        symbol: symbol(symbol_name),
        order_id: OrderId(id),
        owner_id: OwnerId(owner),
        side,
        limit_price: PriceTicks(price),
        quantity: Quantity(quantity),
    }
}

fn market(symbol_name: &str, id: u64, owner: u64, side: Side, quantity: u64) -> Command {
    Command::Market {
        symbol: symbol(symbol_name),
        order_id: OrderId(id),
        owner_id: OwnerId(owner),
        side,
        quantity: Quantity(quantity),
    }
}

fn direct_book_with_asks(levels: u64, depth: u64) -> OrderBook {
    let mut book = OrderBook::new(symbol("XYZ"));
    let mut id = 1;
    for level in 0..levels {
        for _ in 0..depth {
            book.execute(limit(
                "XYZ",
                id,
                id + 10_000,
                Side::Sell,
                100 + level as i64,
                1,
            ))
            .unwrap();
            id += 1;
        }
    }
    book
}

fn direct_book_with_bids(depth: u64) -> OrderBook {
    let mut book = OrderBook::new(symbol("XYZ"));
    for id in 1..=depth {
        book.execute(limit("XYZ", id, id + 10_000, Side::Buy, 99, 1))
            .unwrap();
    }
    book
}

fn routed_book(symbol_count: usize, with_maker: bool) -> MultiSymbolBook {
    let symbols: Vec<_> = (0..symbol_count)
        .map(|index| symbol(&format!("S{index:04}")))
        .collect();
    let mut book = MultiSymbolBook::new(symbols).unwrap();
    if with_maker {
        book.execute(limit("S0000", 1, 2, Side::Sell, 100, 1))
            .unwrap();
    }
    book
}

fn benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct/resting_limit");
    for (name, depth) in [("new_level", 0), ("existing_depth_64", 64)] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    if depth == 0 {
                        OrderBook::new(symbol("XYZ"))
                    } else {
                        direct_book_with_bids(depth)
                    }
                },
                |mut book| {
                    black_box(
                        book.execute(limit("XYZ", 90_000, 3, Side::Buy, 99, 1))
                            .unwrap(),
                    )
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();

    c.bench_function("direct/self_trade_prevention_continue", |b| {
        b.iter_batched(
            || {
                let mut book = OrderBook::new(symbol("XYZ"));
                book.execute(limit("XYZ", 1, 1, Side::Sell, 100, 7))
                    .unwrap();
                book.execute(limit("XYZ", 2, 2, Side::Sell, 101, 5))
                    .unwrap();
                (book, limit("XYZ", 90_000, 1, Side::Buy, 101, 5))
            },
            |(mut book, command)| black_box(book.execute(command).unwrap()),
            BatchSize::SmallInput,
        )
    });

    let mut group = c.benchmark_group("direct/limit_match");
    for (name, levels, quantity) in [
        ("full_one_maker", 1, 1),
        ("partial_maker", 1, 1),
        ("sweep_10_levels", 10, 10),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                move || {
                    let book =
                        direct_book_with_asks(levels, if name == "partial_maker" { 2 } else { 1 });
                    (book, limit("XYZ", 90_000, 3, Side::Buy, 200, quantity))
                },
                |(mut book, command)| black_box(book.execute(command).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();

    let mut group = c.benchmark_group("direct/market_match");
    for (name, levels, quantity) in [
        ("full_one_maker", 1, 1),
        ("empty_unfilled", 0, 1),
        ("sweep_10_levels", 10, 10),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                move || {
                    (
                        direct_book_with_asks(levels, 1),
                        market("XYZ", 90_000, 3, Side::Buy, quantity),
                    )
                },
                |(mut book, command)| black_box(book.execute(command).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();

    let mut group = c.benchmark_group("direct/cancel");
    for depth in [1_u64, 64, 1024] {
        let mut positions = vec![0_u64, depth / 2, depth - 1];
        positions.sort_unstable();
        positions.dedup();
        for position in positions {
            group.bench_with_input(
                BenchmarkId::new(format!("depth_{depth}"), position),
                &(depth, position),
                |b, &(depth, position)| {
                    b.iter_batched(
                        move || {
                            let book = direct_book_with_asks(1, depth);
                            (
                                book,
                                Command::Cancel {
                                    symbol: symbol("XYZ"),
                                    order_id: OrderId(position + 1),
                                },
                            )
                        },
                        |(mut book, command)| black_box(book.execute(command).unwrap()),
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }
    group.finish();

    let mut group = c.benchmark_group("router/dispatch");
    for count in [1_usize, 64, 1024] {
        group.bench_with_input(
            BenchmarkId::new("no_match_add", count),
            &count,
            |b, &count| {
                b.iter_batched(
                    move || {
                        (
                            routed_book(count, false),
                            limit("S0000", 90_000, 3, Side::Buy, 99, 1),
                        )
                    },
                    |(mut book, command)| black_box(book.execute(command).unwrap()),
                    BatchSize::SmallInput,
                )
            },
        );
        group.bench_with_input(
            BenchmarkId::new("one_maker_match", count),
            &count,
            |b, &count| {
                b.iter_batched(
                    move || {
                        (
                            routed_book(count, true),
                            limit("S0000", 90_000, 3, Side::Buy, 100, 1),
                        )
                    },
                    |(mut book, command)| black_box(book.execute(command).unwrap()),
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

criterion_group!(orderbook, benches);
criterion_main!(orderbook);
