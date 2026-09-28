use std::{collections::BTreeMap, sync::Arc};

use serde_json::json;
use tokio::sync::broadcast;
use trade_orderbook::{MarketDataSnapshot, Symbol};
use trade_replay::EngineEvent;

/// Immutable engine-produced data. JSON encoding deliberately happens only in the
/// connection task, never on the book owner thread.
#[derive(Clone, Debug)]
pub(crate) enum Record {
    Trade {
        price_ticks: i64,
        quantity: u64,
    },
    Bbo {
        bid: Option<Level>,
        ask: Option<Level>,
    },
    L2 {
        projection: Projection,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct Event {
    pub(crate) sequence: u64,
    pub(crate) record: Record,
}

#[derive(Clone, Copy)]
pub(crate) enum RecordKind {
    Trade,
    Bbo,
    L2,
}

impl RecordKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Trade => "trade",
            Self::Bbo => "bbo",
            Self::L2 => "l2_snapshot",
        }
    }
}

impl Record {
    pub(crate) fn kind(&self) -> RecordKind {
        match self {
            Self::Trade { .. } => RecordKind::Trade,
            Self::Bbo { .. } => RecordKind::Bbo,
            Self::L2 { .. } => RecordKind::L2,
        }
    }
}

pub(crate) struct Publication {
    pub(crate) sequence: u64,
    pub(crate) kind: RecordKind,
    pub(crate) receivers: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Level {
    pub(crate) price_ticks: i64,
    pub(crate) quantity: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Projection {
    pub(crate) bids: Vec<Level>,
    pub(crate) asks: Vec<Level>,
}

pub(crate) struct Subscription {
    pub(crate) sequence: u64,
    pub(crate) projection: Projection,
    pub(crate) receiver: broadcast::Receiver<Arc<Event>>,
}

pub(crate) struct Feed {
    sequence: u64,
    projection: Projection,
    sender: broadcast::Sender<Arc<Event>>,
}

pub(crate) struct Feeds {
    by_symbol: BTreeMap<Symbol, Feed>,
}

impl Feeds {
    pub(crate) fn new(symbols: &[Symbol], capacity: usize) -> Self {
        let by_symbol = symbols
            .iter()
            .cloned()
            .map(|symbol| {
                let (sender, _) = broadcast::channel(capacity);
                (
                    symbol,
                    Feed {
                        sequence: 0,
                        projection: Projection {
                            bids: Vec::new(),
                            asks: Vec::new(),
                        },
                        sender,
                    },
                )
            })
            .collect();
        Self { by_symbol }
    }

    pub(crate) fn subscribe(&mut self, symbol: &Symbol) -> Option<Subscription> {
        let feed = self.by_symbol.get_mut(symbol)?;
        Some(Subscription {
            sequence: feed.sequence,
            projection: feed.projection.clone(),
            receiver: feed.sender.subscribe(),
        })
    }

    /// Publishes all records for a successful command before its HTTP completion can
    /// be delivered. There is exactly one non-waiting broadcast attempt per record.
    pub(crate) fn publish(
        &mut self,
        symbol: &Symbol,
        events: &[EngineEvent],
        snapshot: MarketDataSnapshot,
    ) -> Vec<Publication> {
        let feed = self
            .by_symbol
            .get_mut(symbol)
            .expect("configured symbol exists");
        let next_projection = projection(snapshot);
        let mut publications = Vec::new();
        for event in events {
            if let EngineEvent::TradeExecuted {
                price_ticks,
                quantity,
                ..
            } = event
            {
                publications.push(feed.send(Record::Trade {
                    price_ticks: price_ticks.0,
                    quantity: quantity.0,
                }));
            }
        }
        if bbo(&feed.projection) != bbo(&next_projection) {
            publications.push(feed.send(Record::Bbo {
                bid: next_projection.bids.first().cloned(),
                ask: next_projection.asks.first().cloned(),
            }));
        }
        if feed.projection != next_projection {
            publications.push(feed.send(Record::L2 {
                projection: next_projection.clone(),
            }));
        }
        feed.projection = next_projection;
        publications
    }
}

impl Feed {
    fn send(&mut self, record: Record) -> Publication {
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("market-data sequence exhausted");
        let kind = record.kind();
        let receivers = self.sender.send(Arc::new(Event {
            sequence: self.sequence,
            record,
        }));
        Publication {
            sequence: self.sequence,
            kind,
            receivers: receivers.unwrap_or(0),
        }
    }
}

fn bbo(projection: &Projection) -> (Option<Level>, Option<Level>) {
    (
        projection.bids.first().cloned(),
        projection.asks.first().cloned(),
    )
}

pub(crate) fn projection(snapshot: MarketDataSnapshot) -> Projection {
    let levels = |levels: Vec<trade_orderbook::MarketDataLevel>| {
        levels
            .into_iter()
            .map(|level| Level {
                price_ticks: level.price.0,
                quantity: level.quantity,
            })
            .collect()
    };
    Projection {
        bids: levels(snapshot.bids),
        asks: levels(snapshot.asks),
    }
}

fn level(level: &Level) -> serde_json::Value {
    json!({"price_ticks":level.price_ticks,"quantity":level.quantity.to_string()})
}

fn levels(levels: &[Level]) -> Vec<serde_json::Value> {
    levels.iter().map(level).collect()
}

pub(crate) fn initial_json(
    stream_id: &str,
    symbol: &Symbol,
    sequence: u64,
    depth: usize,
    projection: &Projection,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&json!({
        "version": 1,
        "type": "l2_snapshot",
        "reason": "initial",
        "stream_id": stream_id,
        "symbol": symbol.as_str(),
        "sequence": sequence,
        "depth": depth,
        "bids": levels(&projection.bids),
        "asks": levels(&projection.asks),
    }))
}

pub(crate) fn event_json(
    stream_id: &str,
    symbol: &Symbol,
    depth: usize,
    event: &Event,
) -> Result<String, serde_json::Error> {
    let value = match &event.record {
        Record::Trade {
            price_ticks,
            quantity,
        } => json!({
            "version": 1,
            "type": "trade",
            "stream_id": stream_id,
            "symbol": symbol.as_str(),
            "sequence": event.sequence,
            "price_ticks": price_ticks,
            "quantity": quantity,
        }),
        Record::Bbo { bid, ask } => json!({
            "version": 1,
            "type": "bbo",
            "stream_id": stream_id,
            "symbol": symbol.as_str(),
            "sequence": event.sequence,
            "bid": bid.as_ref().map(level),
            "ask": ask.as_ref().map(level),
        }),
        Record::L2 { projection } => json!({
            "version": 1,
            "type": "l2_snapshot",
            "reason": "update",
            "stream_id": stream_id,
            "symbol": symbol.as_str(),
            "sequence": event.sequence,
            "depth": depth,
            "bids": levels(&projection.bids),
            "asks": levels(&projection.asks),
        }),
    };
    serde_json::to_string(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lagged_receiver_is_terminally_detectable() {
        let symbol = Symbol::new("BTC-USD");
        let mut feeds = Feeds::new(std::slice::from_ref(&symbol), 64);
        let mut subscription = feeds.subscribe(&symbol).unwrap();
        for quantity in 1..=66 {
            feeds
                .by_symbol
                .get_mut(&symbol)
                .unwrap()
                .send(Record::Trade {
                    price_ticks: 100,
                    quantity,
                });
        }
        assert!(matches!(
            subscription.receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
        ));
    }
}
