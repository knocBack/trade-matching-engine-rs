//! Deterministic offline command-tape replay over the M1 matching library.
//!
//! The codec is intentionally small and strict: v1 accepts only its canonical JSON
//! Lines grammar, so a tape is both a readable artifact and a stable byte contract.

use std::{
    collections::HashSet,
    fmt,
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};
use trade_orderbook::{
    Command, CommandError, CommandOutcome, Disposition, Effect, MultiSymbolBook, OrderId, OwnerId,
    PriceTicks, Quantity, Side, Symbol,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineCommand {
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

impl EngineCommand {
    fn into_m1(self) -> Command {
        match self {
            Self::Limit {
                symbol,
                order_id,
                owner_id,
                side,
                limit_price,
                quantity,
            } => Command::Limit {
                symbol,
                order_id,
                owner_id,
                side,
                limit_price,
                quantity,
            },
            Self::Market {
                symbol,
                order_id,
                owner_id,
                side,
                quantity,
            } => Command::Market {
                symbol,
                order_id,
                owner_id,
                side,
                quantity,
            },
            Self::Cancel { symbol, order_id } => Command::Cancel { symbol, order_id },
        }
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineEvent {
    TradeExecuted {
        command_index: u64,
        symbol: Symbol,
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        maker_owner_id: OwnerId,
        taker_owner_id: OwnerId,
        price_ticks: PriceTicks,
        quantity: Quantity,
    },
    RestingOrderCancelled {
        command_index: u64,
        symbol: Symbol,
        maker_order_id: OrderId,
        maker_owner_id: OwnerId,
        taker_order_id: OrderId,
        taker_owner_id: OwnerId,
        maker_side: Side,
        price_ticks: PriceTicks,
        cancelled_quantity: Quantity,
    },
    CommandCompleted {
        command_index: u64,
        symbol: Symbol,
        order_id: OrderId,
        disposition: Disposition,
    },
    CommandRejected {
        command_index: u64,
        symbol: Symbol,
        order_id: OrderId,
        reason: CommandError,
    },
}

/// The only mutable owner of all M1 books during replay.
#[derive(Clone, Debug)]
pub struct Engine {
    books: MultiSymbolBook,
}

impl Engine {
    pub fn new(symbols: Vec<Symbol>) -> Result<Self, ReplayError> {
        MultiSymbolBook::new(symbols)
            .map(|books| Self { books })
            .map_err(|_| ReplayError::InvalidTape("invalid symbol registry".into()))
    }
    pub fn handle(&mut self, command_index: u64, command: EngineCommand) -> Vec<EngineEvent> {
        let symbol = command.symbol().clone();
        let order_id = command.order_id();
        match self.books.execute(command.into_m1()) {
            Ok(outcome) => map_outcome(command_index, outcome),
            Err(reason) => vec![EngineEvent::CommandRejected {
                command_index,
                symbol,
                order_id,
                reason,
            }],
        }
    }
    pub fn snapshot(&self, symbol: &Symbol) -> Result<trade_orderbook::BookSnapshot, CommandError> {
        self.books.snapshot(symbol)
    }
}

fn map_outcome(command_index: u64, outcome: CommandOutcome) -> Vec<EngineEvent> {
    let mut events = outcome
        .effects
        .into_iter()
        .map(|effect| match effect {
            Effect::Fill(trade) => EngineEvent::TradeExecuted {
                command_index,
                symbol: trade.symbol,
                maker_order_id: trade.maker_order_id,
                taker_order_id: trade.taker_order_id,
                maker_owner_id: trade.maker_owner_id,
                taker_owner_id: trade.taker_owner_id,
                price_ticks: trade.price,
                quantity: trade.quantity,
            },
            Effect::RestingMakerCancelled {
                symbol,
                maker_order_id,
                maker_owner_id,
                taker_order_id,
                taker_owner_id,
                maker_side,
                price,
                cancelled_quantity,
            } => EngineEvent::RestingOrderCancelled {
                command_index,
                symbol,
                maker_order_id,
                maker_owner_id,
                taker_order_id,
                taker_owner_id,
                maker_side,
                price_ticks: price,
                cancelled_quantity,
            },
        })
        .collect::<Vec<_>>();
    events.push(EngineEvent::CommandCompleted {
        command_index,
        symbol: outcome.symbol,
        order_id: outcome.order_id,
        disposition: outcome.disposition,
    });
    events
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandTape {
    pub symbols: Vec<Symbol>,
    pub commands: Vec<EngineCommand>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplayError {
    InvalidTape(String),
    Io(String),
}
impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTape(message) => write!(f, "invalid tape: {message}"),
            Self::Io(message) => write!(f, "I/O failure: {message}"),
        }
    }
}
impl std::error::Error for ReplayError {}
impl From<std::io::Error> for ReplayError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

pub fn replay(input: &[u8]) -> Result<Vec<u8>, ReplayError> {
    let tape = decode_command_tape(input)?;
    let mut engine = Engine::new(tape.symbols.clone())?;
    let mut events = Vec::new();
    for (offset, command) in tape.commands.into_iter().enumerate() {
        let index = u64::try_from(offset + 1)
            .map_err(|_| ReplayError::InvalidTape("too many commands".into()))?;
        events.extend(engine.handle(index, command));
    }
    Ok(encode_event_tape(&tape.symbols, &events).into_bytes())
}

/// Atomically replaces `output` only after the whole tape was decoded and replayed.
pub fn replay_file(input: &Path, output: &Path) -> Result<(), ReplayError> {
    if input == output {
        return Err(ReplayError::Io("input and output paths must differ".into()));
    }
    let bytes = fs::read(input)?;
    let result = replay(&bytes)?;
    let directory = output.parent().unwrap_or_else(|| Path::new("."));
    let name = output
        .file_name()
        .ok_or_else(|| ReplayError::Io("output must name a file".into()))?
        .to_string_lossy();
    let temporary_base = directory.join(format!(".{name}.trade-replay-{}.tmp", std::process::id()));
    let (temporary, mut file) = create_temp(&temporary_base)?;
    let write_result = file.write_all(&result).and_then(|()| file.sync_all());
    drop(file);
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    if let Err(error) = fs::rename(&temporary, output) {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    Ok(())
}

fn create_temp(path: &Path) -> Result<(std::path::PathBuf, File), ReplayError> {
    for suffix in 0..100_u32 {
        let candidate = if suffix == 0 {
            path.to_path_buf()
        } else {
            path.with_extension(format!("tmp.{suffix}"))
        };
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(ReplayError::Io("unable to create temporary output".into()))
}

pub fn decode_command_tape(input: &[u8]) -> Result<CommandTape, ReplayError> {
    if input.starts_with(&[0xef, 0xbb, 0xbf]) {
        return invalid("BOM is not permitted");
    }
    if input.is_empty() || !input.ends_with(b"\n") {
        return invalid("tape must end with LF");
    }
    if input.contains(&b'\r') {
        return invalid("CR is not permitted");
    }
    let source = std::str::from_utf8(input)
        .map_err(|_| ReplayError::InvalidTape("input is not UTF-8".into()))?;
    let mut records = source[..source.len() - 1].split('\n');
    let header = records
        .next()
        .ok_or_else(|| ReplayError::InvalidTape("missing header".into()))?;
    if header.is_empty() {
        return invalid("blank line");
    }
    let header = parse_canonical(header)?;
    let symbols = decode_header(header)?;
    let mut commands = Vec::new();
    for line in records {
        if line.is_empty() {
            return invalid("blank line");
        }
        commands.push(decode_command(parse_canonical(line)?)?);
    }
    Ok(CommandTape { symbols, commands })
}

fn invalid<T>(message: &str) -> Result<T, ReplayError> {
    Err(ReplayError::InvalidTape(message.into()))
}

fn decode_header(value: Json) -> Result<Vec<Symbol>, ReplayError> {
    let mut values = fields(value, &["format", "version", "symbols"])?.into_iter();
    if string(values.next().expect("validated field count"))? != "trade-rs-command-tape" {
        return invalid("unsupported command format");
    }
    if unsigned(values.next().expect("validated field count"))? != 1 {
        return invalid("unsupported command version");
    }
    let Json::Array(items) = values.next().expect("validated field count") else {
        return invalid("symbols must be an array");
    };
    if items.is_empty() {
        return invalid("symbol registry is empty");
    }
    let mut symbols = Vec::with_capacity(items.len());
    for item in items {
        let text = string(item)?;
        if text.is_empty() {
            return invalid("symbol is empty");
        }
        symbols.push(Symbol::new(text));
    }
    if !symbols.windows(2).all(|pair| pair[0] < pair[1]) {
        return invalid("symbols must be strictly sorted and unique");
    }
    Ok(symbols)
}

fn decode_command(value: Json) -> Result<EngineCommand, ReplayError> {
    let Json::Object(object) = &value else {
        return invalid("record must be an object");
    };
    let Some((key, Json::String(kind))) = object.first() else {
        return invalid("command type must be first");
    };
    if key != "type" {
        return invalid("command type must be first");
    }
    match kind.as_str() {
        "limit" => {
            let mut v = fields(
                value,
                &[
                    "type",
                    "symbol",
                    "order_id",
                    "owner_id",
                    "side",
                    "price_ticks",
                    "quantity",
                ],
            )?
            .into_iter();
            let _ = v.next();
            Ok(EngineCommand::Limit {
                symbol: Symbol::new(string(v.next().expect("validated field count"))?),
                order_id: OrderId(unsigned(v.next().expect("validated field count"))?),
                owner_id: OwnerId(unsigned(v.next().expect("validated field count"))?),
                side: side(v.next().expect("validated field count"))?,
                limit_price: PriceTicks(signed(v.next().expect("validated field count"))?),
                quantity: Quantity(unsigned(v.next().expect("validated field count"))?),
            })
        }
        "market" => {
            let mut v = fields(
                value,
                &["type", "symbol", "order_id", "owner_id", "side", "quantity"],
            )?
            .into_iter();
            let _ = v.next();
            Ok(EngineCommand::Market {
                symbol: Symbol::new(string(v.next().expect("validated field count"))?),
                order_id: OrderId(unsigned(v.next().expect("validated field count"))?),
                owner_id: OwnerId(unsigned(v.next().expect("validated field count"))?),
                side: side(v.next().expect("validated field count"))?,
                quantity: Quantity(unsigned(v.next().expect("validated field count"))?),
            })
        }
        "cancel" => {
            let mut v = fields(value, &["type", "symbol", "order_id"])?.into_iter();
            let _ = v.next();
            Ok(EngineCommand::Cancel {
                symbol: Symbol::new(string(v.next().expect("validated field count"))?),
                order_id: OrderId(unsigned(v.next().expect("validated field count"))?),
            })
        }
        _ => invalid("unknown command type"),
    }
}

fn fields(value: Json, names: &[&str]) -> Result<Vec<Json>, ReplayError> {
    let Json::Object(values) = value else {
        return invalid("record must be an object");
    };
    if values.len() != names.len()
        || values
            .iter()
            .zip(names)
            .any(|((key, _), expected)| key != expected)
    {
        return invalid("record fields or field order are invalid");
    }
    Ok(values.into_iter().map(|(_, value)| value).collect())
}
fn string(value: Json) -> Result<String, ReplayError> {
    if let Json::String(value) = value {
        Ok(value)
    } else {
        invalid("expected string")
    }
}
fn unsigned(value: Json) -> Result<u64, ReplayError> {
    let Json::Number(text) = value else {
        return invalid("expected unsigned integer");
    };
    let number = text
        .parse::<u64>()
        .map_err(|_| ReplayError::InvalidTape("unsigned integer out of range".into()))?;
    if text != number.to_string() {
        return invalid("noncanonical unsigned integer");
    }
    Ok(number)
}
fn signed(value: Json) -> Result<i64, ReplayError> {
    let Json::Number(text) = value else {
        return invalid("expected signed integer");
    };
    let number = text
        .parse::<i64>()
        .map_err(|_| ReplayError::InvalidTape("signed integer out of range".into()))?;
    if text != number.to_string() {
        return invalid("noncanonical signed integer");
    }
    Ok(number)
}
fn side(value: Json) -> Result<Side, ReplayError> {
    match string(value)?.as_str() {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => invalid("unknown side"),
    }
}

fn encode_event_tape(symbols: &[Symbol], events: &[EngineEvent]) -> String {
    let mut output = String::new();
    output.push_str(
        "{\"format\":\"trade-rs-event-tape\",\"version\":1,\"command_version\":1,\"symbols\":[",
    );
    for (index, symbol) in symbols.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        quote(&mut output, symbol.as_str());
    }
    output.push_str("]}\n");
    for event in events {
        encode_event(&mut output, event);
        output.push('\n');
    }
    output
}

fn encode_event(output: &mut String, event: &EngineEvent) {
    match event {
        EngineEvent::TradeExecuted {
            command_index,
            symbol,
            maker_order_id,
            taker_order_id,
            maker_owner_id,
            taker_owner_id,
            price_ticks,
            quantity,
        } => {
            output.push_str("{\"type\":\"trade_executed\",\"command_index\":");
            output.push_str(&command_index.to_string());
            output.push_str(",\"symbol\":");
            quote(output, symbol.as_str());
            output.push_str(",\"maker_order_id\":");
            output.push_str(&maker_order_id.0.to_string());
            output.push_str(",\"taker_order_id\":");
            output.push_str(&taker_order_id.0.to_string());
            output.push_str(",\"maker_owner_id\":");
            output.push_str(&maker_owner_id.0.to_string());
            output.push_str(",\"taker_owner_id\":");
            output.push_str(&taker_owner_id.0.to_string());
            output.push_str(",\"price_ticks\":");
            output.push_str(&price_ticks.0.to_string());
            output.push_str(",\"quantity\":");
            output.push_str(&quantity.0.to_string());
            output.push('}');
        }
        EngineEvent::RestingOrderCancelled {
            command_index,
            symbol,
            maker_order_id,
            maker_owner_id,
            taker_order_id,
            taker_owner_id,
            maker_side,
            price_ticks,
            cancelled_quantity,
        } => {
            output.push_str("{\"type\":\"resting_order_cancelled\",\"command_index\":");
            output.push_str(&command_index.to_string());
            output.push_str(",\"symbol\":");
            quote(output, symbol.as_str());
            output.push_str(",\"maker_order_id\":");
            output.push_str(&maker_order_id.0.to_string());
            output.push_str(",\"maker_owner_id\":");
            output.push_str(&maker_owner_id.0.to_string());
            output.push_str(",\"taker_order_id\":");
            output.push_str(&taker_order_id.0.to_string());
            output.push_str(",\"taker_owner_id\":");
            output.push_str(&taker_owner_id.0.to_string());
            output.push_str(",\"maker_side\":\"");
            output.push_str(side_name(*maker_side));
            output.push_str("\",\"price_ticks\":");
            output.push_str(&price_ticks.0.to_string());
            output.push_str(",\"cancelled_quantity\":");
            output.push_str(&cancelled_quantity.0.to_string());
            output.push_str(",\"reason\":\"self_trade_prevention\"}");
        }
        EngineEvent::CommandCompleted {
            command_index,
            symbol,
            order_id,
            disposition,
        } => {
            output.push_str("{\"type\":\"command_completed\",\"command_index\":");
            output.push_str(&command_index.to_string());
            output.push_str(",\"symbol\":");
            quote(output, symbol.as_str());
            output.push_str(",\"order_id\":");
            output.push_str(&order_id.0.to_string());
            output.push_str(",\"disposition\":");
            encode_disposition(output, *disposition);
            output.push('}');
        }
        EngineEvent::CommandRejected {
            command_index,
            symbol,
            order_id,
            reason,
        } => {
            output.push_str("{\"type\":\"command_rejected\",\"command_index\":");
            output.push_str(&command_index.to_string());
            output.push_str(",\"symbol\":");
            quote(output, symbol.as_str());
            output.push_str(",\"order_id\":");
            output.push_str(&order_id.0.to_string());
            output.push_str(",\"reason\":\"");
            output.push_str(reason_name(*reason));
            output.push_str("\"}");
        }
    }
}
fn encode_disposition(output: &mut String, disposition: Disposition) {
    match disposition {
        Disposition::Filled => output.push_str("{\"type\":\"filled\"}"),
        Disposition::Resting { remaining } => {
            output.push_str("{\"type\":\"resting\",\"remaining\":");
            output.push_str(&remaining.0.to_string());
            output.push('}');
        }
        Disposition::Unfilled { remaining } => {
            output.push_str("{\"type\":\"unfilled\",\"remaining\":");
            output.push_str(&remaining.0.to_string());
            output.push('}');
        }
        Disposition::Cancelled { cancelled_quantity } => {
            output.push_str("{\"type\":\"cancelled\",\"cancelled_quantity\":");
            output.push_str(&cancelled_quantity.0.to_string());
            output.push('}');
        }
    }
}
fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}
fn reason_name(reason: CommandError) -> &'static str {
    match reason {
        CommandError::UnknownSymbol => "unknown_symbol",
        CommandError::InvalidQuantity => "invalid_quantity",
        CommandError::DuplicateOrderId => "duplicate_order_id",
        CommandError::OrderNotFound => "order_not_found",
    }
}

fn quote(output: &mut String, text: &str) {
    output.push('"');
    for character in text.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\t' => output.push_str("\\t"),
            '\n' => output.push_str("\\n"),
            '\u{0c}' => output.push_str("\\f"),
            '\r' => output.push_str("\\r"),
            c if c <= '\u{1f}' => {
                output.push_str("\\u00");
                output.push_str(&format!("{:02x}", c as u32));
            }
            c => output.push(c),
        }
    }
    output.push('"');
}

#[derive(Clone, Debug)]
enum Json {
    String(String),
    Number(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
    Bool(bool),
    Null,
}

fn parse_canonical(line: &str) -> Result<Json, ReplayError> {
    let mut parser = Parser {
        input: line.as_bytes(),
        cursor: 0,
    };
    let value = parser.value()?;
    if parser.cursor != parser.input.len() {
        return invalid("trailing data");
    }
    let mut encoded = String::new();
    encode_json(&mut encoded, &value);
    if encoded != line {
        return invalid("record is not canonical JSON");
    }
    Ok(value)
}
fn encode_json(output: &mut String, value: &Json) {
    match value {
        Json::String(text) => quote(output, text),
        Json::Number(number) => output.push_str(number),
        Json::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Json::Null => output.push_str("null"),
        Json::Array(items) => {
            output.push('[');
            for (index, item) in items.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                encode_json(output, item);
            }
            output.push(']');
        }
        Json::Object(items) => {
            output.push('{');
            for (index, (key, value)) in items.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                quote(output, key);
                output.push(':');
                encode_json(output, value);
            }
            output.push('}');
        }
    }
}

struct Parser<'a> {
    input: &'a [u8],
    cursor: usize,
}
impl Parser<'_> {
    fn value(&mut self) -> Result<Json, ReplayError> {
        match self.peek() {
            Some(b'"') => self.string().map(Json::String),
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(b't') => {
                self.literal(b"true")?;
                Ok(Json::Bool(true))
            }
            Some(b'f') => {
                self.literal(b"false")?;
                Ok(Json::Bool(false))
            }
            Some(b'n') => {
                self.literal(b"null")?;
                Ok(Json::Null)
            }
            _ => invalid("invalid JSON value"),
        }
    }
    fn object(&mut self) -> Result<Json, ReplayError> {
        self.byte(b'{')?;
        let mut values = Vec::new();
        let mut seen = HashSet::new();
        if self.peek() == Some(b'}') {
            self.cursor += 1;
            return Ok(Json::Object(values));
        }
        loop {
            if self.peek() != Some(b'"') {
                return invalid("object key must be a string");
            }
            let key = self.string()?;
            if !seen.insert(key.clone()) {
                return invalid("duplicate object key");
            }
            self.byte(b':')?;
            let value = self.value()?;
            values.push((key, value));
            match self.next() {
                Some(b',') => (),
                Some(b'}') => break,
                _ => return invalid("invalid object separator"),
            }
        }
        Ok(Json::Object(values))
    }
    fn array(&mut self) -> Result<Json, ReplayError> {
        self.byte(b'[')?;
        let mut values = Vec::new();
        if self.peek() == Some(b']') {
            self.cursor += 1;
            return Ok(Json::Array(values));
        }
        loop {
            values.push(self.value()?);
            match self.next() {
                Some(b',') => (),
                Some(b']') => break,
                _ => return invalid("invalid array separator"),
            }
        }
        Ok(Json::Array(values))
    }
    fn number(&mut self) -> Result<Json, ReplayError> {
        let start = self.cursor;
        if self.peek() == Some(b'-') {
            self.cursor += 1;
        }
        match self.next() {
            Some(b'0') => (),
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.cursor += 1;
                }
            }
            _ => return invalid("invalid number"),
        }
        if self.peek() == Some(b'.') {
            self.cursor += 1;
            self.digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.cursor += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.cursor += 1;
            }
            self.digits()?;
        }
        Ok(Json::Number(
            std::str::from_utf8(&self.input[start..self.cursor])
                .expect("number is ASCII")
                .into(),
        ))
    }
    fn digits(&mut self) -> Result<(), ReplayError> {
        let start = self.cursor;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.cursor += 1;
        }
        if start == self.cursor {
            return invalid("invalid number");
        }
        Ok(())
    }
    fn string(&mut self) -> Result<String, ReplayError> {
        self.byte(b'"')?;
        let mut result = String::new();
        let start = self.cursor;
        loop {
            match self.next() {
                Some(b'"') => {
                    let end = self.cursor - 1;
                    result
                        .push_str(std::str::from_utf8(&self.input[start..end]).map_err(|_| {
                            ReplayError::InvalidTape("invalid UTF-8 string".into())
                        })?);
                    return Ok(result);
                }
                Some(b'\\') => {
                    let end = self.cursor - 1;
                    result
                        .push_str(std::str::from_utf8(&self.input[start..end]).map_err(|_| {
                            ReplayError::InvalidTape("invalid UTF-8 string".into())
                        })?);
                    let escaped = self.next().ok_or_else(|| {
                        ReplayError::InvalidTape("truncated string escape".into())
                    })?;
                    match escaped {
                        b'"' => result.push('"'),
                        b'\\' => result.push('\\'),
                        b'/' => result.push('/'),
                        b'b' => result.push('\u{08}'),
                        b'f' => result.push('\u{0c}'),
                        b'n' => result.push('\n'),
                        b'r' => result.push('\r'),
                        b't' => result.push('\t'),
                        b'u' => result.push(self.unicode_escape()?),
                        _ => return invalid("invalid string escape"),
                    }
                    let start_after_escape = self.cursor;
                    return self.string_tail(result, start_after_escape);
                }
                Some(byte) if byte < 0x20 => return invalid("control byte in string"),
                Some(_) => (),
                None => return invalid("unterminated string"),
            }
        }
    }
    fn string_tail(&mut self, mut result: String, mut start: usize) -> Result<String, ReplayError> {
        loop {
            match self.next() {
                Some(b'"') => {
                    let end = self.cursor - 1;
                    result
                        .push_str(std::str::from_utf8(&self.input[start..end]).map_err(|_| {
                            ReplayError::InvalidTape("invalid UTF-8 string".into())
                        })?);
                    return Ok(result);
                }
                Some(b'\\') => {
                    let end = self.cursor - 1;
                    result
                        .push_str(std::str::from_utf8(&self.input[start..end]).map_err(|_| {
                            ReplayError::InvalidTape("invalid UTF-8 string".into())
                        })?);
                    let escaped = self.next().ok_or_else(|| {
                        ReplayError::InvalidTape("truncated string escape".into())
                    })?;
                    match escaped {
                        b'"' => result.push('"'),
                        b'\\' => result.push('\\'),
                        b'/' => result.push('/'),
                        b'b' => result.push('\u{08}'),
                        b'f' => result.push('\u{0c}'),
                        b'n' => result.push('\n'),
                        b'r' => result.push('\r'),
                        b't' => result.push('\t'),
                        b'u' => result.push(self.unicode_escape()?),
                        _ => return invalid("invalid string escape"),
                    }
                    start = self.cursor;
                }
                Some(byte) if byte < 0x20 => return invalid("control byte in string"),
                Some(_) => (),
                None => return invalid("unterminated string"),
            }
        }
    }
    fn unicode_escape(&mut self) -> Result<char, ReplayError> {
        let unit = self.hex_unit()?;
        if (0xd800..=0xdbff).contains(&unit) {
            self.byte(b'\\')?;
            self.byte(b'u')?;
            let low = self.hex_unit()?;
            if !(0xdc00..=0xdfff).contains(&low) {
                return invalid("invalid unicode surrogate pair");
            }
            char::from_u32(0x10000 + (((unit - 0xd800) as u32) << 10) + (low - 0xdc00) as u32)
                .ok_or_else(|| ReplayError::InvalidTape("invalid unicode escape".into()))
        } else if (0xdc00..=0xdfff).contains(&unit) {
            invalid("unpaired unicode surrogate")
        } else {
            char::from_u32(unit as u32)
                .ok_or_else(|| ReplayError::InvalidTape("invalid unicode escape".into()))
        }
    }
    fn hex_unit(&mut self) -> Result<u16, ReplayError> {
        let mut value = 0_u16;
        for _ in 0..4 {
            let digit = match self.next() {
                Some(b'0'..=b'9') => self.input[self.cursor - 1] - b'0',
                Some(b'a'..=b'f') => self.input[self.cursor - 1] - b'a' + 10,
                Some(b'A'..=b'F') => self.input[self.cursor - 1] - b'A' + 10,
                _ => return invalid("invalid unicode escape"),
            };
            value = value * 16 + digit as u16;
        }
        Ok(value)
    }
    fn literal(&mut self, expected: &[u8]) -> Result<(), ReplayError> {
        if self.input.get(self.cursor..self.cursor + expected.len()) == Some(expected) {
            self.cursor += expected.len();
            Ok(())
        } else {
            invalid("invalid literal")
        }
    }
    fn byte(&mut self, expected: u8) -> Result<(), ReplayError> {
        if self.next() == Some(expected) {
            Ok(())
        } else {
            invalid("unexpected JSON byte")
        }
    }
    fn next(&mut self) -> Option<u8> {
        let value = self.peek()?;
        self.cursor += 1;
        Some(value)
    }
    fn peek(&self) -> Option<u8> {
        self.input.get(self.cursor).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    fn tape(records: &[&str]) -> Vec<u8> {
        format!("{}\n", records.join("\n")).into_bytes()
    }
    fn header(symbols: &str) -> String {
        format!("{{\"format\":\"trade-rs-command-tape\",\"version\":1,\"symbols\":{symbols}}}")
    }

    #[test]
    fn prevention_event_is_ordered_and_canonical() {
        let input = tape(&[
            &header("[\"XYZ\"]"),
            "{\"type\":\"limit\",\"symbol\":\"XYZ\",\"order_id\":1,\"owner_id\":2,\"side\":\"sell\",\"price_ticks\":100,\"quantity\":2}",
            "{\"type\":\"limit\",\"symbol\":\"XYZ\",\"order_id\":2,\"owner_id\":1,\"side\":\"sell\",\"price_ticks\":101,\"quantity\":7}",
            "{\"type\":\"limit\",\"symbol\":\"XYZ\",\"order_id\":3,\"owner_id\":3,\"side\":\"sell\",\"price_ticks\":102,\"quantity\":5}",
            "{\"type\":\"limit\",\"symbol\":\"XYZ\",\"order_id\":4,\"owner_id\":1,\"side\":\"buy\",\"price_ticks\":102,\"quantity\":6}",
        ]);
        let output = String::from_utf8(replay(&input).unwrap()).unwrap();
        assert!(output.contains("{\"type\":\"trade_executed\",\"command_index\":4,\"symbol\":\"XYZ\",\"maker_order_id\":1"));
        let cancellation = "{\"type\":\"resting_order_cancelled\",\"command_index\":4,\"symbol\":\"XYZ\",\"maker_order_id\":2,\"maker_owner_id\":1,\"taker_order_id\":4,\"taker_owner_id\":1,\"maker_side\":\"sell\",\"price_ticks\":101,\"cancelled_quantity\":7,\"reason\":\"self_trade_prevention\"}";
        assert!(output.contains(cancellation));
        assert!(
            output.find("maker_order_id\":1").unwrap()
                < output.find("resting_order_cancelled").unwrap()
        );
        assert!(output.ends_with('\n'));
    }
    #[test]
    fn domain_precedence_emits_one_rejection_and_continues() {
        let input = tape(&[
            &header("[\"XYZ\"]"),
            "{\"type\":\"limit\",\"symbol\":\"NOPE\",\"order_id\":1,\"owner_id\":1,\"side\":\"buy\",\"price_ticks\":0,\"quantity\":0}",
            "{\"type\":\"market\",\"symbol\":\"XYZ\",\"order_id\":2,\"owner_id\":1,\"side\":\"buy\",\"quantity\":0}",
            "{\"type\":\"limit\",\"symbol\":\"XYZ\",\"order_id\":3,\"owner_id\":1,\"side\":\"buy\",\"price_ticks\":0,\"quantity\":1}",
        ]);
        let output = String::from_utf8(replay(&input).unwrap()).unwrap();
        assert!(output.contains(
            "\"command_index\":1,\"symbol\":\"NOPE\",\"order_id\":1,\"reason\":\"unknown_symbol\""
        ));
        assert!(output.contains(
            "\"command_index\":2,\"symbol\":\"XYZ\",\"order_id\":2,\"reason\":\"invalid_quantity\""
        ));
        assert!(output.contains("\"command_index\":3,\"symbol\":\"XYZ\",\"order_id\":3,\"disposition\":{\"type\":\"resting\",\"remaining\":1}"));
    }
    #[test]
    fn strict_decoder_rejects_malformed_tapes() {
        for input in [
            b"\xef\xbb\xbf{}\n".as_slice(),
            b"{}\n".as_slice(),
            b"{\"format\": \"trade-rs-command-tape\",\"version\":1,\"symbols\":[\"XYZ\"]}\n"
                .as_slice(),
            b"{\"format\":\"trade-rs-command-tape\",\"version\":1,\"symbols\":[\"XYZ\"]}\r\n"
                .as_slice(),
            b"{\"format\":\"trade-rs-command-tape\",\"version\":1,\"symbols\":[\"XYZ\",\"XYZ\"]}\n"
                .as_slice(),
        ] {
            assert!(decode_command_tape(input).is_err());
        }
    }

    #[test]
    fn canonical_bytes_cover_integer_boundaries_and_string_escapes() {
        let symbol = "\\b\\t\\n\\f\\r\\\"\\\\é";
        let input = tape(&[
            &header(&format!("[\"{symbol}\"]")),
            &format!(
                "{{\"type\":\"limit\",\"symbol\":\"{symbol}\",\"order_id\":18446744073709551615,\"owner_id\":18446744073709551615,\"side\":\"buy\",\"price_ticks\":-9223372036854775808,\"quantity\":18446744073709551615}}"
            ),
            &format!(
                "{{\"type\":\"limit\",\"symbol\":\"{symbol}\",\"order_id\":0,\"owner_id\":0,\"side\":\"sell\",\"price_ticks\":-9223372036854775808,\"quantity\":1}}"
            ),
            &format!(
                "{{\"type\":\"limit\",\"symbol\":\"{symbol}\",\"order_id\":2,\"owner_id\":7,\"side\":\"sell\",\"price_ticks\":9223372036854775807,\"quantity\":18446744073709551615}}"
            ),
            &format!(
                "{{\"type\":\"limit\",\"symbol\":\"{symbol}\",\"order_id\":3,\"owner_id\":7,\"side\":\"buy\",\"price_ticks\":9223372036854775807,\"quantity\":1}}"
            ),
        ]);
        let output = String::from_utf8(replay(&input).unwrap()).unwrap();
        assert!(output.contains("\"symbols\":[\"\\b\\t\\n\\f\\r\\\"\\\\é\"]"));
        assert!(output.contains("\"order_id\":18446744073709551615"));
        assert!(output.contains("\"maker_owner_id\":18446744073709551615"));
        assert!(output.contains("\"price_ticks\":-9223372036854775808"));
        assert!(output.contains("\"price_ticks\":9223372036854775807"));
        assert!(output.contains("\"cancelled_quantity\":18446744073709551615"));
    }

    fn generated_command(
        kind: u8,
        symbol: &str,
        id: u64,
        owner: u64,
        buy: bool,
        price: i64,
        quantity: u64,
    ) -> EngineCommand {
        let symbol = Symbol::new(symbol);
        let side = if buy { Side::Buy } else { Side::Sell };
        match kind % 3 {
            0 => EngineCommand::Limit {
                symbol,
                order_id: OrderId(id),
                owner_id: OwnerId(owner),
                side,
                limit_price: PriceTicks(price),
                quantity: Quantity(quantity),
            },
            1 => EngineCommand::Market {
                symbol,
                order_id: OrderId(id),
                owner_id: OwnerId(owner),
                side,
                quantity: Quantity(quantity),
            },
            _ => EngineCommand::Cancel {
                symbol,
                order_id: OrderId(id),
            },
        }
    }

    proptest! {
        #[test]
        fn engine_parity_with_direct_m1_and_symbol_snapshots(
            commands in prop::collection::vec((0_u8..3, prop_oneof![Just("XYZ"), Just("ALT"), Just("NOPE")], 0_u64..8, 0_u64..4, any::<bool>(), -3_i64..4, 0_u64..5), 0..40)
        ) {
            let symbols = vec![Symbol::new("ALT"), Symbol::new("XYZ")];
            let mut engine = Engine::new(symbols.clone()).unwrap();
            let mut direct = MultiSymbolBook::new(symbols.clone()).unwrap();
            for (offset, (kind, name, id, owner, buy, price, quantity)) in commands.into_iter().enumerate() {
                let command = generated_command(kind, name, id, owner, buy, price, quantity);
                let expected_symbol = command.symbol().clone();
                let expected_id = command.order_id();
                let expected = match direct.execute(command.clone().into_m1()) {
                    Ok(outcome) => map_outcome((offset + 1) as u64, outcome),
                    Err(reason) => vec![EngineEvent::CommandRejected { command_index: (offset + 1) as u64, symbol: expected_symbol, order_id: expected_id, reason }],
                };
                prop_assert_eq!(engine.handle((offset + 1) as u64, command), expected);
                for symbol in &symbols { prop_assert_eq!(engine.snapshot(symbol), direct.snapshot(symbol)); }
            }
        }
    }
}
