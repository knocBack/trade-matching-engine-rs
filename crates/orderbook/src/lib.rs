//! A deterministic, synchronous, multi-symbol central limit order book.
//!
//! Prices are integer ticks and quantities are integer base units.  The crate has no
//! I/O, clocks, threads, or asynchronous runtime: command order is its only time.

use std::collections::{BTreeMap, HashMap, VecDeque};

/// An instrument in the fixed registry of a [`MultiSymbolBook`].
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Symbol(String);

impl Symbol {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Symbol {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// A symbol-scoped order identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct OrderId(pub u64);

/// The owner of an order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct OwnerId(pub u64);

/// An exact price measured in an instrument's minimum tick.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct PriceTicks(pub i64);

/// A base-unit quantity. Zero is representable so the command boundary can reject it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Quantity(pub u64);

/// The two order-book sides.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

/// A command accepted by a fixed-registry matching engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Limit {
        symbol: Symbol,
        order_id: OrderId,
        owner_id: OwnerId,
        side: Side,
        limit_price: PriceTicks,
        quantity: Quantity,
    },
    Market {
        symbol: Symbol,
        order_id: OrderId,
        owner_id: OwnerId,
        side: Side,
        quantity: Quantity,
    },
    Cancel {
        symbol: Symbol,
        order_id: OrderId,
    },
}

impl Command {
    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::Limit { symbol, .. }
            | Self::Market { symbol, .. }
            | Self::Cancel { symbol, .. } => symbol,
        }
    }

    #[must_use]
    pub fn order_id(&self) -> OrderId {
        match self {
            Self::Limit { order_id, .. }
            | Self::Market { order_id, .. }
            | Self::Cancel { order_id, .. } => *order_id,
        }
    }
}

/// Stable domain failures. Failed commands do not mutate any book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandError {
    UnknownSymbol,
    InvalidQuantity,
    DuplicateOrderId,
    OrderNotFound,
}

/// The terminal state of the submitted or cancelled order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Disposition {
    Filled,
    Resting { remaining: Quantity },
    Unfilled { remaining: Quantity },
    Cancelled { cancelled_quantity: Quantity },
}

/// One price-time-priority execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trade {
    pub symbol: Symbol,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub maker_owner_id: OwnerId,
    pub taker_owner_id: OwnerId,
    pub price: PriceTicks,
    pub quantity: Quantity,
}

/// An ordered state transition caused by an incoming command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    Fill(Trade),
    RestingMakerCancelled {
        symbol: Symbol,
        maker_order_id: OrderId,
        maker_owner_id: OwnerId,
        taker_order_id: OrderId,
        taker_owner_id: OwnerId,
        maker_side: Side,
        price: PriceTicks,
        cancelled_quantity: Quantity,
    },
}

/// Ordered effects plus the terminal result of one successful command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandOutcome {
    pub symbol: Symbol,
    pub order_id: OrderId,
    pub effects: Vec<Effect>,
    pub disposition: Disposition,
}

/// A detached resting order in a snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestingOrderSnapshot {
    pub order_id: OrderId,
    pub owner_id: OwnerId,
    pub side: Side,
    pub price: PriceTicks,
    pub remaining: Quantity,
}

/// One ordered price level in a snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceLevelSnapshot {
    pub price: PriceTicks,
    /// Oldest to newest.
    pub orders: Vec<RestingOrderSnapshot>,
}

/// A detached, ordered view of one book.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookSnapshot {
    pub symbol: Symbol,
    /// Highest price to lowest price.
    pub bids: Vec<PriceLevelSnapshot>,
    /// Lowest price to highest price.
    pub asks: Vec<PriceLevelSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataLevel {
    pub price: PriceTicks,
    pub quantity: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataSnapshot {
    pub symbol: Symbol,
    pub bids: Vec<MarketDataLevel>,
    pub asks: Vec<MarketDataLevel>,
}

#[derive(Clone, Debug)]
struct RestingOrder {
    order_id: OrderId,
    owner_id: OwnerId,
    quantity: Quantity,
}

#[derive(Clone, Copy, Debug)]
struct Locator {
    side: Side,
    price: PriceTicks,
}

#[derive(Clone, Debug, Default)]
struct PriceLevel {
    orders: VecDeque<RestingOrder>,
    quantity: u128,
}

type Levels = BTreeMap<PriceTicks, PriceLevel>;

/// A self-contained book for one symbol. It is also the direct matching benchmark
/// surface; [`MultiSymbolBook`] uses this exact implementation for dispatch.
#[derive(Clone, Debug)]
pub struct OrderBook {
    symbol: Symbol,
    bids: Levels,
    asks: Levels,
    live_orders: HashMap<OrderId, Locator>,
}

impl OrderBook {
    #[must_use]
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            live_orders: HashMap::new(),
        }
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Executes a command only if its symbol is this book's configured symbol.
    pub fn execute(&mut self, command: Command) -> Result<CommandOutcome, CommandError> {
        if command.symbol() != &self.symbol {
            return Err(CommandError::UnknownSymbol);
        }

        match command {
            Command::Limit {
                symbol,
                order_id,
                owner_id,
                side,
                limit_price,
                quantity,
            } => {
                self.validate_new(order_id, quantity)?;
                let (effects, remaining) =
                    self.match_incoming(order_id, owner_id, side, Some(limit_price), quantity);
                let disposition = if remaining.0 == 0 {
                    Disposition::Filled
                } else {
                    self.rest(order_id, owner_id, side, limit_price, remaining);
                    Disposition::Resting { remaining }
                };
                Ok(CommandOutcome {
                    symbol,
                    order_id,
                    effects,
                    disposition,
                })
            }
            Command::Market {
                symbol,
                order_id,
                owner_id,
                side,
                quantity,
            } => {
                self.validate_new(order_id, quantity)?;
                let (effects, remaining) =
                    self.match_incoming(order_id, owner_id, side, None, quantity);
                let disposition = if remaining.0 == 0 {
                    Disposition::Filled
                } else {
                    Disposition::Unfilled { remaining }
                };
                Ok(CommandOutcome {
                    symbol,
                    order_id,
                    effects,
                    disposition,
                })
            }
            Command::Cancel { symbol, order_id } => {
                let cancelled_quantity = self.cancel(order_id)?;
                Ok(CommandOutcome {
                    symbol,
                    order_id,
                    effects: Vec::new(),
                    disposition: Disposition::Cancelled { cancelled_quantity },
                })
            }
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> BookSnapshot {
        BookSnapshot {
            symbol: self.symbol.clone(),
            bids: Self::snapshot_levels(&self.bids, true),
            asks: Self::snapshot_levels(&self.asks, false),
        }
    }

    #[must_use]
    pub fn market_data_snapshot(&self, depth: usize) -> MarketDataSnapshot {
        MarketDataSnapshot {
            symbol: self.symbol.clone(),
            bids: Self::market_data_levels(&self.bids, true, depth),
            asks: Self::market_data_levels(&self.asks, false, depth),
        }
    }

    fn validate_new(&self, order_id: OrderId, quantity: Quantity) -> Result<(), CommandError> {
        if quantity.0 == 0 {
            return Err(CommandError::InvalidQuantity);
        }
        if self.live_orders.contains_key(&order_id) {
            return Err(CommandError::DuplicateOrderId);
        }
        Ok(())
    }

    fn match_incoming(
        &mut self,
        taker_order_id: OrderId,
        taker_owner_id: OwnerId,
        taker_side: Side,
        limit: Option<PriceTicks>,
        mut remaining: Quantity,
    ) -> (Vec<Effect>, Quantity) {
        let mut effects = Vec::new();
        while remaining.0 > 0 {
            let Some((price, candidate)) = self.next_candidate(taker_side, limit) else {
                break;
            };
            let maker = self.pop_best(taker_side.opposite(), price);
            debug_assert_eq!(maker.order_id, candidate.order_id);
            if maker.owner_id == taker_owner_id {
                effects.push(Effect::RestingMakerCancelled {
                    symbol: self.symbol.clone(),
                    maker_order_id: maker.order_id,
                    maker_owner_id: maker.owner_id,
                    taker_order_id,
                    taker_owner_id,
                    maker_side: taker_side.opposite(),
                    price,
                    cancelled_quantity: maker.quantity,
                });
                continue;
            }

            let executed = Quantity(remaining.0.min(maker.quantity.0));
            remaining.0 -= executed.0;
            let maker_remaining = Quantity(maker.quantity.0 - executed.0);
            effects.push(Effect::Fill(Trade {
                symbol: self.symbol.clone(),
                maker_order_id: maker.order_id,
                taker_order_id,
                maker_owner_id: maker.owner_id,
                taker_owner_id,
                price,
                quantity: executed,
            }));
            if maker_remaining.0 > 0 {
                self.restore_front(
                    taker_side.opposite(),
                    price,
                    RestingOrder {
                        quantity: maker_remaining,
                        ..maker
                    },
                );
            }
        }
        (effects, remaining)
    }

    fn next_candidate(
        &self,
        taker_side: Side,
        limit: Option<PriceTicks>,
    ) -> Option<(PriceTicks, RestingOrder)> {
        let (price, queue) = match taker_side {
            Side::Buy => self.asks.first_key_value()?,
            Side::Sell => self.bids.last_key_value()?,
        };
        if let Some(limit) = limit {
            let executable = match taker_side {
                Side::Buy => *price <= limit,
                Side::Sell => *price >= limit,
            };
            if !executable {
                return None;
            }
        }
        Some((*price, queue.orders.front()?.clone()))
    }

    fn pop_best(&mut self, side: Side, price: PriceTicks) -> RestingOrder {
        let levels = self.levels_mut(side);
        let level = levels.get_mut(&price).expect("candidate level exists");
        let maker = level.orders.pop_front().expect("candidate queue nonempty");
        level.quantity -= u128::from(maker.quantity.0);
        if levels
            .get(&price)
            .is_some_and(|level| level.orders.is_empty())
        {
            levels.remove(&price);
        }
        self.live_orders.remove(&maker.order_id);
        maker
    }

    fn restore_front(&mut self, side: Side, price: PriceTicks, order: RestingOrder) {
        let level = self.levels_mut(side).entry(price).or_default();
        level.orders.push_front(order.clone());
        level.quantity += u128::from(order.quantity.0);
        self.live_orders
            .insert(order.order_id, Locator { side, price });
    }

    fn rest(
        &mut self,
        order_id: OrderId,
        owner_id: OwnerId,
        side: Side,
        price: PriceTicks,
        quantity: Quantity,
    ) {
        let level = self.levels_mut(side).entry(price).or_default();
        level.orders.push_back(RestingOrder {
            order_id,
            owner_id,
            quantity,
        });
        level.quantity += u128::from(quantity.0);
        self.live_orders.insert(order_id, Locator { side, price });
    }

    fn cancel(&mut self, order_id: OrderId) -> Result<Quantity, CommandError> {
        let locator = self
            .live_orders
            .get(&order_id)
            .copied()
            .ok_or(CommandError::OrderNotFound)?;
        let levels = self.levels_mut(locator.side);
        let queue = levels
            .get_mut(&locator.price)
            .expect("locator level exists");
        let position = queue
            .orders
            .iter()
            .position(|order| order.order_id == order_id)
            .expect("locator order exists");
        let order = queue.orders.remove(position).expect("position exists");
        queue.quantity -= u128::from(order.quantity.0);
        if queue.orders.is_empty() {
            levels.remove(&locator.price);
        }
        self.live_orders.remove(&order_id);
        Ok(order.quantity)
    }

    fn levels_mut(&mut self, side: Side) -> &mut Levels {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn snapshot_levels(levels: &Levels, reverse: bool) -> Vec<PriceLevelSnapshot> {
        let entries: Vec<_> = if reverse {
            levels.iter().rev().collect()
        } else {
            levels.iter().collect()
        };
        entries
            .into_iter()
            .map(|(price, queue)| PriceLevelSnapshot {
                price: *price,
                orders: queue
                    .orders
                    .iter()
                    .map(|order| RestingOrderSnapshot {
                        order_id: order.order_id,
                        owner_id: order.owner_id,
                        side: if reverse { Side::Buy } else { Side::Sell },
                        price: *price,
                        remaining: order.quantity,
                    })
                    .collect(),
            })
            .collect()
    }

    fn market_data_levels(levels: &Levels, reverse: bool, depth: usize) -> Vec<MarketDataLevel> {
        if reverse {
            levels
                .iter()
                .rev()
                .take(depth)
                .map(|(price, level)| MarketDataLevel {
                    price: *price,
                    quantity: level.quantity,
                })
                .collect()
        } else {
            levels
                .iter()
                .take(depth)
                .map(|(price, level)| MarketDataLevel {
                    price: *price,
                    quantity: level.quantity,
                })
                .collect()
        }
    }
}

/// Routes commands to independent fixed-registry symbol books.
#[derive(Clone, Debug)]
pub struct MultiSymbolBook {
    books: HashMap<Symbol, OrderBook>,
}

impl MultiSymbolBook {
    /// Builds the fixed registry, rejecting duplicate symbols.
    pub fn new(symbols: impl IntoIterator<Item = Symbol>) -> Result<Self, DuplicateSymbolError> {
        let mut books = HashMap::new();
        for symbol in symbols {
            if books.contains_key(&symbol) {
                return Err(DuplicateSymbolError(symbol));
            }
            books.insert(symbol.clone(), OrderBook::new(symbol));
        }
        Ok(Self { books })
    }

    pub fn execute(&mut self, command: Command) -> Result<CommandOutcome, CommandError> {
        self.books
            .get_mut(command.symbol())
            .ok_or(CommandError::UnknownSymbol)?
            .execute(command)
    }

    pub fn snapshot(&self, symbol: &Symbol) -> Result<BookSnapshot, CommandError> {
        self.books
            .get(symbol)
            .map(OrderBook::snapshot)
            .ok_or(CommandError::UnknownSymbol)
    }

    pub fn market_data_snapshot(
        &self,
        symbol: &Symbol,
        depth: usize,
    ) -> Result<MarketDataSnapshot, CommandError> {
        self.books
            .get(symbol)
            .map(|book| book.market_data_snapshot(depth))
            .ok_or(CommandError::UnknownSymbol)
    }

    /// Returns sorted symbols so hash-map iteration is never observable.
    #[must_use]
    pub fn symbols(&self) -> Vec<Symbol> {
        let mut symbols: Vec<_> = self.books.keys().cloned().collect();
        symbols.sort();
        symbols
    }
}

/// Construction failure for a duplicate configured symbol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DuplicateSymbolError(pub Symbol);

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn sym(value: &str) -> Symbol {
        Symbol::from(value)
    }
    fn limit(symbol: &str, id: u64, owner: u64, side: Side, price: i64, quantity: u64) -> Command {
        Command::Limit {
            symbol: sym(symbol),
            order_id: OrderId(id),
            owner_id: OwnerId(owner),
            side,
            limit_price: PriceTicks(price),
            quantity: Quantity(quantity),
        }
    }
    fn market(symbol: &str, id: u64, owner: u64, side: Side, quantity: u64) -> Command {
        Command::Market {
            symbol: sym(symbol),
            order_id: OrderId(id),
            owner_id: OwnerId(owner),
            side,
            quantity: Quantity(quantity),
        }
    }

    #[test]
    fn strict_price_time_priority_and_maker_prices() {
        let mut book = MultiSymbolBook::new([sym("XYZ"), sym("ABC")]).unwrap();
        book.execute(limit("XYZ", 1, 10, Side::Sell, 101, 3))
            .unwrap();
        book.execute(limit("XYZ", 2, 11, Side::Sell, 101, 4))
            .unwrap();
        book.execute(limit("XYZ", 3, 12, Side::Sell, 102, 7))
            .unwrap();
        let outcome = book
            .execute(limit("XYZ", 4, 20, Side::Buy, 102, 10))
            .unwrap();
        assert_eq!(outcome.disposition, Disposition::Filled);
        assert_eq!(
            outcome.effects,
            vec![
                Effect::Fill(Trade {
                    symbol: sym("XYZ"),
                    maker_order_id: OrderId(1),
                    taker_order_id: OrderId(4),
                    maker_owner_id: OwnerId(10),
                    taker_owner_id: OwnerId(20),
                    price: PriceTicks(101),
                    quantity: Quantity(3)
                }),
                Effect::Fill(Trade {
                    symbol: sym("XYZ"),
                    maker_order_id: OrderId(2),
                    taker_order_id: OrderId(4),
                    maker_owner_id: OwnerId(11),
                    taker_owner_id: OwnerId(20),
                    price: PriceTicks(101),
                    quantity: Quantity(4)
                }),
                Effect::Fill(Trade {
                    symbol: sym("XYZ"),
                    maker_order_id: OrderId(3),
                    taker_order_id: OrderId(4),
                    maker_owner_id: OwnerId(12),
                    taker_owner_id: OwnerId(20),
                    price: PriceTicks(102),
                    quantity: Quantity(3)
                }),
            ]
        );
        assert_eq!(
            book.snapshot(&sym("XYZ")).unwrap().asks[0].orders[0].remaining,
            Quantity(4)
        );
        assert!(book.snapshot(&sym("ABC")).unwrap().asks.is_empty());
    }

    #[test]
    fn prevention_cancels_full_maker_and_continues_with_unchanged_taker() {
        let mut book = MultiSymbolBook::new([sym("XYZ")]).unwrap();
        book.execute(limit("XYZ", 1, 2, Side::Sell, 100, 2))
            .unwrap();
        book.execute(limit("XYZ", 2, 1, Side::Sell, 101, 7))
            .unwrap();
        book.execute(limit("XYZ", 3, 3, Side::Sell, 102, 5))
            .unwrap();
        let outcome = book.execute(limit("XYZ", 4, 1, Side::Buy, 102, 6)).unwrap();
        assert_eq!(outcome.disposition, Disposition::Filled);
        assert_eq!(
            outcome.effects,
            vec![
                Effect::Fill(Trade {
                    symbol: sym("XYZ"),
                    maker_order_id: OrderId(1),
                    taker_order_id: OrderId(4),
                    maker_owner_id: OwnerId(2),
                    taker_owner_id: OwnerId(1),
                    price: PriceTicks(100),
                    quantity: Quantity(2)
                }),
                Effect::RestingMakerCancelled {
                    symbol: sym("XYZ"),
                    maker_order_id: OrderId(2),
                    maker_owner_id: OwnerId(1),
                    taker_order_id: OrderId(4),
                    taker_owner_id: OwnerId(1),
                    maker_side: Side::Sell,
                    price: PriceTicks(101),
                    cancelled_quantity: Quantity(7)
                },
                Effect::Fill(Trade {
                    symbol: sym("XYZ"),
                    maker_order_id: OrderId(3),
                    taker_order_id: OrderId(4),
                    maker_owner_id: OwnerId(3),
                    taker_owner_id: OwnerId(1),
                    price: PriceTicks(102),
                    quantity: Quantity(4)
                }),
            ]
        );
        assert_eq!(
            book.snapshot(&sym("XYZ")).unwrap().asks[0].orders[0].remaining,
            Quantity(1)
        );
    }

    #[test]
    fn prevention_respects_stop_boundaries_and_market_is_unfilled() {
        let mut book = MultiSymbolBook::new([sym("XYZ")]).unwrap();
        book.execute(limit("XYZ", 1, 2, Side::Sell, 100, 5))
            .unwrap();
        book.execute(limit("XYZ", 2, 1, Side::Sell, 101, 9))
            .unwrap();
        let filled = book.execute(limit("XYZ", 3, 1, Side::Buy, 101, 5)).unwrap();
        assert_eq!(filled.effects.len(), 1);
        assert_eq!(
            book.snapshot(&sym("XYZ")).unwrap().asks[0].orders[0].order_id,
            OrderId(2)
        );
        let mut self_only = MultiSymbolBook::new([sym("XYZ")]).unwrap();
        self_only
            .execute(limit("XYZ", 1, 1, Side::Sell, 100, 2))
            .unwrap();
        self_only
            .execute(limit("XYZ", 2, 1, Side::Sell, 101, 3))
            .unwrap();
        let outcome = self_only
            .execute(market("XYZ", 3, 1, Side::Buy, 5))
            .unwrap();
        assert_eq!(
            outcome.disposition,
            Disposition::Unfilled {
                remaining: Quantity(5)
            }
        );
        assert_eq!(outcome.effects.len(), 2);
        assert!(self_only.snapshot(&sym("XYZ")).unwrap().asks.is_empty());
    }

    #[test]
    fn cancellation_is_final_and_validation_precedence_is_stable() {
        let mut book = MultiSymbolBook::new([sym("XYZ")]).unwrap();
        book.execute(limit("XYZ", 1, 1, Side::Sell, i64::MIN, 8))
            .unwrap();
        let before = book.snapshot(&sym("XYZ")).unwrap();
        assert_eq!(
            book.execute(limit("NOPE", 1, 1, Side::Buy, 0, 0)),
            Err(CommandError::UnknownSymbol)
        );
        assert_eq!(book.snapshot(&sym("XYZ")).unwrap(), before);
        assert_eq!(
            book.execute(limit("XYZ", 1, 1, Side::Buy, i64::MAX, 0)),
            Err(CommandError::InvalidQuantity)
        );
        let cancelled = book
            .execute(Command::Cancel {
                symbol: sym("XYZ"),
                order_id: OrderId(1),
            })
            .unwrap();
        assert_eq!(
            cancelled.disposition,
            Disposition::Cancelled {
                cancelled_quantity: Quantity(8)
            }
        );
        assert_eq!(
            book.execute(Command::Cancel {
                symbol: sym("XYZ"),
                order_id: OrderId(1)
            }),
            Err(CommandError::OrderNotFound)
        );
        assert!(book.snapshot(&sym("XYZ")).unwrap().asks.is_empty());
    }

    #[test]
    fn snapshots_are_detached_and_symbols_are_sorted() {
        let mut book = MultiSymbolBook::new([sym("Z"), sym("A")]).unwrap();
        book.execute(limit("A", 1, 1, Side::Buy, 0, 1)).unwrap();
        let mut snapshot = book.snapshot(&sym("A")).unwrap();
        snapshot.bids.clear();
        assert_eq!(book.snapshot(&sym("A")).unwrap().bids.len(), 1);
        assert_eq!(book.symbols(), vec![sym("A"), sym("Z")]);
        assert!(matches!(
            MultiSymbolBook::new([sym("A"), sym("A")]),
            Err(DuplicateSymbolError(value)) if value == sym("A")
        ));
    }

    #[test]
    fn market_data_snapshot_aggregates_only_requested_levels() {
        let mut book = MultiSymbolBook::new([sym("XYZ")]).unwrap();
        book.execute(limit("XYZ", 1, 1, Side::Buy, 100, 2)).unwrap();
        book.execute(limit("XYZ", 2, 2, Side::Buy, 100, 3)).unwrap();
        book.execute(limit("XYZ", 3, 3, Side::Buy, 99, 7)).unwrap();
        book.execute(limit("XYZ", 4, 4, Side::Sell, 101, 11))
            .unwrap();
        book.execute(limit("XYZ", 5, 5, Side::Sell, 102, 13))
            .unwrap();

        let snapshot = book.market_data_snapshot(&sym("XYZ"), 1).unwrap();
        assert_eq!(
            snapshot.bids,
            vec![MarketDataLevel {
                price: PriceTicks(100),
                quantity: 5,
            }]
        );
        assert_eq!(
            snapshot.asks,
            vec![MarketDataLevel {
                price: PriceTicks(101),
                quantity: 11,
            }]
        );
    }

    #[derive(Clone, Debug)]
    struct ModelOrder {
        id: OrderId,
        owner: OwnerId,
        side: Side,
        price: PriceTicks,
        quantity: Quantity,
        sequence: u64,
    }
    #[derive(Clone, Debug)]
    struct Model {
        symbols: Vec<Symbol>,
        orders: Vec<(Symbol, ModelOrder)>,
        next_sequence: u64,
    }
    impl Model {
        fn new(symbols: Vec<Symbol>) -> Self {
            Self {
                symbols,
                orders: Vec::new(),
                next_sequence: 0,
            }
        }
        fn execute(&mut self, command: Command) -> Result<CommandOutcome, CommandError> {
            let symbol = command.symbol().clone();
            if !self.symbols.contains(&symbol) {
                return Err(CommandError::UnknownSymbol);
            }
            match command {
                Command::Cancel { order_id, .. } => {
                    let index = self
                        .orders
                        .iter()
                        .position(|(s, o)| s == &symbol && o.id == order_id)
                        .ok_or(CommandError::OrderNotFound)?;
                    let (_, order) = self.orders.remove(index);
                    Ok(CommandOutcome {
                        symbol,
                        order_id,
                        effects: Vec::new(),
                        disposition: Disposition::Cancelled {
                            cancelled_quantity: order.quantity,
                        },
                    })
                }
                Command::Limit {
                    order_id,
                    owner_id,
                    side,
                    limit_price,
                    quantity,
                    ..
                } => self.execute_order(
                    symbol,
                    order_id,
                    owner_id,
                    side,
                    Some(limit_price),
                    quantity,
                ),
                Command::Market {
                    order_id,
                    owner_id,
                    side,
                    quantity,
                    ..
                } => self.execute_order(symbol, order_id, owner_id, side, None, quantity),
            }
        }
        fn execute_order(
            &mut self,
            symbol: Symbol,
            order_id: OrderId,
            owner_id: OwnerId,
            side: Side,
            limit: Option<PriceTicks>,
            quantity: Quantity,
        ) -> Result<CommandOutcome, CommandError> {
            if quantity.0 == 0 {
                return Err(CommandError::InvalidQuantity);
            }
            if self
                .orders
                .iter()
                .any(|(s, o)| s == &symbol && o.id == order_id)
            {
                return Err(CommandError::DuplicateOrderId);
            }
            let mut remaining = quantity;
            let mut effects = Vec::new();
            while remaining.0 > 0 {
                let candidate = self
                    .orders
                    .iter()
                    .enumerate()
                    .filter(|(_, (s, o))| {
                        s == &symbol
                            && o.side == side.opposite()
                            && limit.is_none_or(|p| match side {
                                Side::Buy => o.price <= p,
                                Side::Sell => o.price >= p,
                            })
                    })
                    .min_by_key(|(_, (_, o))| match side {
                        Side::Buy => (i128::from(o.price.0), o.sequence),
                        Side::Sell => (-i128::from(o.price.0), o.sequence),
                    })
                    .map(|(i, _)| i);
                let Some(index) = candidate else {
                    break;
                };
                let (_, maker) = self.orders[index].clone();
                if maker.owner == owner_id {
                    self.orders.remove(index);
                    effects.push(Effect::RestingMakerCancelled {
                        symbol: symbol.clone(),
                        maker_order_id: maker.id,
                        maker_owner_id: maker.owner,
                        taker_order_id: order_id,
                        taker_owner_id: owner_id,
                        maker_side: maker.side,
                        price: maker.price,
                        cancelled_quantity: maker.quantity,
                    });
                } else {
                    let executed = Quantity(remaining.0.min(maker.quantity.0));
                    remaining.0 -= executed.0;
                    self.orders[index].1.quantity.0 -= executed.0;
                    if self.orders[index].1.quantity.0 == 0 {
                        self.orders.remove(index);
                    }
                    effects.push(Effect::Fill(Trade {
                        symbol: symbol.clone(),
                        maker_order_id: maker.id,
                        taker_order_id: order_id,
                        maker_owner_id: maker.owner,
                        taker_owner_id: owner_id,
                        price: maker.price,
                        quantity: executed,
                    }));
                }
            }
            let disposition = if remaining.0 == 0 {
                Disposition::Filled
            } else if let Some(price) = limit {
                self.orders.push((
                    symbol.clone(),
                    ModelOrder {
                        id: order_id,
                        owner: owner_id,
                        side,
                        price,
                        quantity: remaining,
                        sequence: self.next_sequence,
                    },
                ));
                self.next_sequence += 1;
                Disposition::Resting { remaining }
            } else {
                Disposition::Unfilled { remaining }
            };
            Ok(CommandOutcome {
                symbol,
                order_id,
                effects,
                disposition,
            })
        }
        fn snapshot(&self, symbol: &Symbol) -> BookSnapshot {
            let levels = |side: Side, reverse: bool| {
                let mut entries: Vec<_> = self
                    .orders
                    .iter()
                    .filter(|(s, o)| s == symbol && o.side == side)
                    .map(|(_, o)| o.clone())
                    .collect();
                entries.sort_by_key(|o| {
                    (
                        if reverse {
                            -i128::from(o.price.0)
                        } else {
                            i128::from(o.price.0)
                        },
                        o.sequence,
                    )
                });
                let mut result: Vec<PriceLevelSnapshot> = Vec::new();
                for o in entries {
                    if result.last().is_none_or(|level| level.price != o.price) {
                        result.push(PriceLevelSnapshot {
                            price: o.price,
                            orders: Vec::new(),
                        });
                    }
                    result
                        .last_mut()
                        .unwrap()
                        .orders
                        .push(RestingOrderSnapshot {
                            order_id: o.id,
                            owner_id: o.owner,
                            side: o.side,
                            price: o.price,
                            remaining: o.quantity,
                        });
                }
                result
            };
            BookSnapshot {
                symbol: symbol.clone(),
                bids: levels(Side::Buy, true),
                asks: levels(Side::Sell, false),
            }
        }
    }

    fn command_strategy() -> impl Strategy<Value = Command> {
        (0u8..4, 0u64..16, 0u64..4, 0u8..2, -4i64..5, 0u64..7, 0u8..3).prop_map(
            |(s, id, owner, side, price, quantity, kind)| {
                let symbol = if s < 2 { sym("X") } else { sym("Y") };
                let side = if side == 0 { Side::Buy } else { Side::Sell };
                match kind {
                    0 => Command::Limit {
                        symbol,
                        order_id: OrderId(id),
                        owner_id: OwnerId(owner),
                        side,
                        limit_price: PriceTicks(price),
                        quantity: Quantity(quantity),
                    },
                    1 => Command::Market {
                        symbol,
                        order_id: OrderId(id),
                        owner_id: OwnerId(owner),
                        side,
                        quantity: Quantity(quantity),
                    },
                    _ => Command::Cancel {
                        symbol,
                        order_id: OrderId(id),
                    },
                }
            },
        )
    }

    proptest! {
        #[test]
        fn generated_commands_match_independent_model(commands in prop::collection::vec(command_strategy(), 1..80)) {
            let mut actual = MultiSymbolBook::new([sym("X"), sym("Y")]).unwrap();
            let mut model = Model::new(vec![sym("X"), sym("Y")]);
            for command in commands {
                prop_assert_eq!(actual.execute(command.clone()), model.execute(command));
                for symbol in [sym("X"), sym("Y")] { prop_assert_eq!(actual.snapshot(&symbol).unwrap(), model.snapshot(&symbol)); }
            }
        }
    }
}
