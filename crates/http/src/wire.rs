//! Strict HTTP schema. Raw integer lexemes never pass through floating point.
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::{Value, json, value::RawValue};
use std::{collections::BTreeMap, fmt};
use trade_orderbook::{
    BookSnapshot, CommandError, OrderId, OwnerId, PriceTicks, Quantity, Side, Symbol,
};
use trade_replay::{EngineCommand, EngineEvent};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Error(pub u16, pub &'static str);
pub(crate) const MALFORMED: Error = Error(400, "malformed_request");
pub(crate) const UNAVAILABLE: Error = Error(503, "service_unavailable");
pub(crate) const OVERLOADED: Error = Error(503, "ingress_overloaded");
pub(crate) const MARKET_OVERLOADED: Error = Error(503, "market_data_overloaded");

struct Fields(BTreeMap<String, Box<RawValue>>);
impl<'de> Deserialize<'de> for Fields {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Object;
        impl<'de> Visitor<'de> for Object {
            type Value = Fields;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an object with unique fields")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Fields, M::Error> {
                let mut values = BTreeMap::new();
                while let Some((k, v)) = map.next_entry::<String, Box<RawValue>>()? {
                    if values.insert(k, v).is_some() {
                        return Err(de::Error::custom("duplicate field"));
                    }
                }
                Ok(Fields(values))
            }
        }
        d.deserialize_map(Object)
    }
}
impl Fields {
    fn take(&mut self, key: &str) -> Result<Box<RawValue>, Error> {
        self.0.remove(key).ok_or(MALFORMED)
    }
    fn string(&mut self, key: &str) -> Result<String, Error> {
        serde_json::from_str(self.take(key)?.get()).map_err(|_| MALFORMED)
    }
    fn unsigned(&mut self, key: &str) -> Result<u64, Error> {
        unsigned(self.take(key)?.get())
    }
    fn signed(&mut self, key: &str) -> Result<i64, Error> {
        let v = self.take(key)?;
        let digits = v.get().strip_prefix('-').unwrap_or(v.get());
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(MALFORMED);
        }
        v.get().parse().map_err(|_| MALFORMED)
    }
}
pub(crate) fn unsigned(v: &str) -> Result<u64, Error> {
    if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
        return Err(MALFORMED);
    }
    v.parse().map_err(|_| MALFORMED)
}
pub(crate) fn symbol(v: String) -> Result<Symbol, Error> {
    if v.trim().is_empty() {
        return Err(MALFORMED);
    }
    Ok(Symbol::new(v))
}
pub(crate) fn command(bytes: &[u8]) -> Result<EngineCommand, Error> {
    let mut f: Fields = serde_json::from_slice(bytes).map_err(|_| MALFORMED)?;
    let kind = f.string("type")?;
    let symbol = symbol(f.string("symbol")?)?;
    let order_id = OrderId(f.unsigned("order_id")?);
    let owner_id = OwnerId(f.unsigned("owner_id")?);
    let side = match f.string("side")?.as_str() {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        _ => return Err(MALFORMED),
    };
    let quantity = Quantity(f.unsigned("quantity")?);
    let cmd = match kind.as_str() {
        "limit" => EngineCommand::Limit {
            symbol,
            order_id,
            owner_id,
            side,
            quantity,
            limit_price: PriceTicks(f.signed("price_ticks")?),
        },
        "market" => EngineCommand::Market {
            symbol,
            order_id,
            owner_id,
            side,
            quantity,
        },
        _ => return Err(MALFORMED),
    };
    if !f.0.is_empty() {
        return Err(MALFORMED);
    }
    Ok(cmd)
}
fn unescape(value: &str) -> Result<String, Error> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut input = value.bytes();
    while let Some(b) = input.next() {
        bytes.push(match b {
            b'+' => b' ',
            b'%' => {
                let hi = input
                    .next()
                    .and_then(|v| (v as char).to_digit(16))
                    .ok_or(MALFORMED)?;
                let lo = input
                    .next()
                    .and_then(|v| (v as char).to_digit(16))
                    .ok_or(MALFORMED)?;
                (hi * 16 + lo) as u8
            }
            _ => b,
        });
    }
    String::from_utf8(bytes).map_err(|_| MALFORMED)
}
pub(crate) fn query(query: Option<&str>) -> Result<Symbol, Error> {
    let q = query.ok_or(MALFORMED)?;
    if q.contains('&') {
        return Err(MALFORMED);
    }
    let (key, value) = q.split_once('=').ok_or(MALFORMED)?;
    if unescape(key)? != "symbol" {
        return Err(MALFORMED);
    }
    symbol(unescape(value)?)
}
pub(crate) fn domain(reason: CommandError) -> Error {
    match reason {
        CommandError::UnknownSymbol => Error(404, "unknown_symbol"),
        CommandError::OrderNotFound => Error(404, "order_not_found"),
        CommandError::DuplicateOrderId => Error(409, "duplicate_order_id"),
        CommandError::InvalidQuantity => Error(422, "invalid_quantity"),
    }
}
pub(crate) fn events(events: &[EngineEvent]) -> (u16, String) {
    let rejection = events.last().and_then(|e| match e {
        EngineEvent::CommandRejected { reason, .. } => Some(domain(*reason)),
        _ => None,
    });
    let mut body = String::from("{\"events\":[");
    for (i, event) in events.iter().enumerate() {
        if i != 0 {
            body.push(',');
        }
        trade_replay::encode_event(&mut body, event);
    }
    body.push(']');
    if let Some(Error(_, code)) = rejection {
        body.push_str(&format!(",\"error\":{{\"code\":\"{code}\"}}"));
    }
    body.push('}');
    (rejection.map_or(200, |e| e.0), body)
}
pub(crate) fn snapshot(book: BookSnapshot) -> Value {
    let levels = |ls: Vec<trade_orderbook::PriceLevelSnapshot>| {
        ls.into_iter().map(|l| {
        json!({"price_ticks":l.price.0,"orders":l.orders.into_iter().map(|o| json!({"order_id":o.order_id.0,"owner_id":o.owner_id.0,"remaining":o.remaining.0})).collect::<Vec<_>>()})
    }).collect::<Vec<_>>()
    };
    json!({"symbol":book.symbol.as_str(),"bids":levels(book.bids),"asks":levels(book.asks)})
}
