//! Bounded HTTP/1.1 adapter. The engine lives only on its dedicated owner thread.
mod market;
mod wire;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    future::Future,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, interval, timeout},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message as WsMessage,
        protocol::{CloseFrame, Role, WebSocketConfig, frame::coding::CloseCode},
    },
};
use trade_orderbook::{BookSnapshot, CommandError, OrderId, Symbol};
use trade_replay::{Engine, EngineCommand, EngineEvent};
use uuid::Uuid;
use wire::{Error, MALFORMED, MARKET_OVERLOADED, OVERLOADED, UNAVAILABLE};

const MARKET_DATA_PROTOCOL: &str = "trade-rs.market-data.v1";
const MIN_MARKET_DATA_RING_CAPACITY: usize = 64;

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub symbols: Vec<Symbol>,
    pub queue_capacity: usize,
    pub in_flight: usize,
    pub body_limit: usize,
    pub connections: usize,
    pub market_data_depth: usize,
    pub market_data_ring_capacity: usize,
    pub market_data_connections: usize,
    pub websocket_write_timeout: Duration,
    pub websocket_close_timeout: Duration,
    pub websocket_ping_interval: Duration,
    pub websocket_pong_timeout: Duration,
    pub websocket_write_buffer: usize,
    pub websocket_inbound_limit: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8080".parse().unwrap(),
            symbols: vec![Symbol::new("BTC-USD")],
            queue_capacity: 256,
            in_flight: 512,
            body_limit: 16 * 1024,
            connections: 1024,
            market_data_depth: 20,
            market_data_ring_capacity: 1024,
            market_data_connections: 128,
            websocket_write_timeout: Duration::from_secs(1),
            websocket_close_timeout: Duration::from_secs(1),
            websocket_ping_interval: Duration::from_secs(15),
            websocket_pong_timeout: Duration::from_secs(30),
            websocket_write_buffer: 64 * 1024,
            websocket_inbound_limit: 1024,
        }
    }
}
impl Config {
    fn maximum_market_data_frame(&self) -> usize {
        let max_symbol = self
            .symbols
            .iter()
            .map(|symbol| symbol.as_str().len())
            .max()
            .unwrap_or(0);
        512_usize
            .saturating_add(max_symbol.saturating_mul(6))
            .saturating_add(self.market_data_depth.saturating_mul(2).saturating_mul(96))
    }

    fn validate(&self) -> io::Result<()> {
        if self.queue_capacity == 0
            || self.in_flight == 0
            || self.connections == 0
            || self.body_limit == 0
            || self.body_limit > 16 * 1024
            || !(1..=100).contains(&self.market_data_depth)
            || self.market_data_connections == 0
            || self.market_data_connections > self.connections
            || self.websocket_write_timeout.is_zero()
            || self.websocket_close_timeout.is_zero()
            || self.websocket_ping_interval.is_zero()
            || self.websocket_pong_timeout <= self.websocket_ping_interval
            || self.websocket_write_buffer < 2
            || self.websocket_inbound_limit == 0
            || [self.queue_capacity, self.in_flight, self.connections]
                .iter()
                .any(|&v| v > Semaphore::MAX_PERMITS)
            || self.symbols.is_empty()
            || self.symbols.iter().any(|s| s.as_str().trim().is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid finite HTTP or market-data limits",
            ));
        }
        if self.market_data_ring_capacity < MIN_MARKET_DATA_RING_CAPACITY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "market-data ring capacity must be at least 64",
            ));
        }
        if !self.market_data_ring_capacity.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "market-data ring capacity must be a power of two",
            ));
        }
        if self.maximum_market_data_frame() > self.websocket_write_buffer {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "market-data frame does not fit configured WebSocket write buffer",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketDataConfig {
    pub depth: usize,
    pub ring_capacity: usize,
    pub connection_limit: usize,
    pub websocket_write_timeout_ms: u64,
    pub websocket_close_timeout_ms: u64,
    pub websocket_ping_interval_ms: u64,
    pub websocket_pong_timeout_ms: u64,
    pub websocket_write_buffer: usize,
    pub websocket_inbound_limit: usize,
    pub maximum_frame_bytes: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketDataSymbolMetrics {
    pub subscribers_current: usize,
    pub subscribers_high_water: usize,
    pub sequence: u64,
    pub logical_events: BTreeMap<String, u64>,
    pub ring_sends_with_receivers: u64,
    pub events_without_receivers: u64,
    pub frames_written: BTreeMap<String, u64>,
    pub frame_bytes_written: BTreeMap<String, u64>,
}

fn market_data_symbol_metrics() -> MarketDataSymbolMetrics {
    let mut metrics = MarketDataSymbolMetrics::default();
    for kind in ["trade", "bbo", "l2_snapshot"] {
        metrics.logical_events.insert(kind.into(), 0);
        metrics.frames_written.insert(kind.into(), 0);
        metrics.frame_bytes_written.insert(kind.into(), 0);
    }
    metrics
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Metrics {
    pub requests: BTreeMap<String, u64>,
    pub commands_admitted: u64,
    /// Includes domain-rejected terminal batches.
    pub commands_completed: u64,
    pub domain_rejected: BTreeMap<String, u64>,
    pub overload: u64,
    pub queue_current: usize,
    pub queue_high_water: usize,
    pub snapshots_admitted: u64,
    pub snapshots_completed: u64,
    pub snapshots_rejected: u64,
    pub completion_delivery_failures: u64,
    pub connection_errors: u64,
    pub connections_rejected: u64,
    pub graceful_engine_exits: u64,
    pub abnormal_engine_exits: u64,
    pub market_data_connections_current: usize,
    pub market_data_connections_high_water: usize,
    pub market_data_connections_rejected: u64,
    pub market_data_barriers_admitted: u64,
    pub market_data_barriers_completed: u64,
    pub market_data_barriers_rejected: u64,
    pub market_data_lag_disconnects: u64,
    pub market_data_lagged_messages: u64,
    pub market_data_frames_written: u64,
    pub market_data_upgrade_outcomes: BTreeMap<String, u64>,
    pub market_data_symbols: BTreeMap<String, MarketDataSymbolMetrics>,
    pub market_data_disconnects: BTreeMap<String, u64>,
    pub market_data_forced_task_aborts: u64,
    pub market_data_graceful_publisher_exits: u64,
    pub market_data_abnormal_publisher_exits: u64,
    pub market_data_config: MarketDataConfig,
}
enum Work {
    Command(EngineCommand),
    Snapshot(Symbol),
    Subscribe(Symbol),
}
enum Completed {
    Events(Vec<EngineEvent>),
    Snapshot(Result<BookSnapshot, CommandError>),
    Subscription(Result<market::Subscription, CommandError>),
}
struct Message {
    work: Work,
    reply: oneshot::Sender<Completed>,
}
// This lock protects admission/close and counters only. It never contains a book,
// performs engine work, waits for capacity, or spans an await.
struct Admission {
    sender: Option<mpsc::Sender<Message>>,
    metrics: Metrics,
}
struct Shared {
    admission: Mutex<Admission>,
    stop: watch::Sender<bool>,
    permits: Arc<Semaphore>,
    market_permits: Arc<Semaphore>,
    body_limit: usize,
    stream_id: String,
    market_depth: usize,
    websocket: WebSocketLimits,
    websocket_tasks: Mutex<JoinSet<()>>,
    engine_exit: watch::Sender<EngineExit>,
}

#[derive(Clone, Copy)]
struct WebSocketLimits {
    write_timeout: Duration,
    close_timeout: Duration,
    ping_interval: Duration,
    pong_timeout: Duration,
    write_buffer: usize,
    inbound_limit: usize,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum EngineExit {
    Running,
    Graceful,
    Abnormal,
}
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
}
impl Handle {
    pub fn metrics(&self) -> Metrics {
        self.shared.admission.lock().unwrap().metrics.clone()
    }
    pub fn shutdown(&self) {
        self.shared.admission.lock().unwrap().sender.take();
        self.shared.stop.send_replace(true);
    }
    fn unavailable(&self) -> bool {
        self.shared.admission.lock().unwrap().sender.is_none()
    }
    fn admit(&self, work: Work) -> Result<oneshot::Receiver<Completed>, Error> {
        let (reply, receive) = oneshot::channel();
        let is_command = matches!(work, Work::Command(_));
        let is_subscription = matches!(work, Work::Subscribe(_));
        let mut state = self.shared.admission.lock().unwrap();
        let Some(sender) = &state.sender else {
            return Err(UNAVAILABLE);
        };
        match sender.try_send(Message { work, reply }) {
            Ok(()) => {
                let depth = sender.max_capacity() - sender.capacity();
                let m = &mut state.metrics;
                m.queue_current = depth;
                m.queue_high_water = m.queue_high_water.max(m.queue_current);
                if is_command {
                    m.commands_admitted += 1;
                } else if is_subscription {
                    m.market_data_barriers_admitted += 1;
                } else {
                    m.snapshots_admitted += 1;
                }
                Ok(receive)
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(OVERLOADED),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                state.sender.take();
                self.shared.stop.send_replace(true);
                Err(UNAVAILABLE)
            }
        }
    }
    fn record(&self, route: &str, status: u16, overload: bool) {
        let mut a = self.shared.admission.lock().unwrap();
        *a.metrics
            .requests
            .entry(format!("{route}:{status}"))
            .or_default() += 1;
        a.metrics.overload += u64::from(overload);
    }
    fn spawn_websocket<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.shared.websocket_tasks.lock().unwrap();
        while tasks.try_join_next().is_some() {}
        tasks.spawn(task);
    }
    fn market_connection_opened(&self, symbol: &Symbol) {
        let mut a = self.shared.admission.lock().unwrap();
        a.metrics.market_data_connections_current += 1;
        a.metrics.market_data_connections_high_water = a
            .metrics
            .market_data_connections_high_water
            .max(a.metrics.market_data_connections_current);
        let metrics = a
            .metrics
            .market_data_symbols
            .get_mut(symbol.as_str())
            .expect("configured symbol metrics exist");
        metrics.subscribers_current += 1;
        metrics.subscribers_high_water = metrics
            .subscribers_high_water
            .max(metrics.subscribers_current);
    }
    fn market_connection_closed(&self, symbol: &Symbol) {
        let mut a = self.shared.admission.lock().unwrap();
        a.metrics.market_data_connections_current =
            a.metrics.market_data_connections_current.saturating_sub(1);
        let metrics = a
            .metrics
            .market_data_symbols
            .get_mut(symbol.as_str())
            .expect("configured symbol metrics exist");
        metrics.subscribers_current = metrics.subscribers_current.saturating_sub(1);
    }
    fn market_rejected(&self) {
        self.shared
            .admission
            .lock()
            .unwrap()
            .metrics
            .market_data_connections_rejected += 1;
    }
    fn market_upgrade_outcome(&self, status: u16, code: Option<&str>) {
        let key = match code {
            Some(code) => format!("{status}:{code}"),
            None => status.to_string(),
        };
        *self
            .shared
            .admission
            .lock()
            .unwrap()
            .metrics
            .market_data_upgrade_outcomes
            .entry(key)
            .or_default() += 1;
    }
    fn market_publications(&self, symbol: &Symbol, publications: &[market::Publication]) {
        let mut a = self.shared.admission.lock().unwrap();
        let metrics = a
            .metrics
            .market_data_symbols
            .get_mut(symbol.as_str())
            .expect("configured symbol metrics exist");
        for publication in publications {
            metrics.sequence = publication.sequence;
            *metrics
                .logical_events
                .entry(publication.kind.as_str().into())
                .or_default() += 1;
            if publication.receivers == 0 {
                metrics.events_without_receivers += 1;
            } else {
                metrics.ring_sends_with_receivers += 1;
            }
        }
    }
    fn market_frame_written(&self, symbol: &Symbol, kind: market::RecordKind, bytes: usize) {
        let mut a = self.shared.admission.lock().unwrap();
        a.metrics.market_data_frames_written += 1;
        let metrics = a
            .metrics
            .market_data_symbols
            .get_mut(symbol.as_str())
            .expect("configured symbol metrics exist");
        *metrics
            .frames_written
            .entry(kind.as_str().into())
            .or_default() += 1;
        *metrics
            .frame_bytes_written
            .entry(kind.as_str().into())
            .or_default() += u64::try_from(bytes).unwrap_or(u64::MAX);
    }
    fn market_disconnected(&self, reason: &'static str) {
        *self
            .shared
            .admission
            .lock()
            .unwrap()
            .metrics
            .market_data_disconnects
            .entry(reason.into())
            .or_default() += 1;
    }
    fn market_forced_task_aborts(&self, count: usize) {
        self.shared
            .admission
            .lock()
            .unwrap()
            .metrics
            .market_data_forced_task_aborts += u64::try_from(count).unwrap_or(u64::MAX);
    }
}

struct MarketConnection {
    handle: Handle,
    symbol: Symbol,
}

impl Drop for MarketConnection {
    fn drop(&mut self) {
        self.handle.market_connection_closed(&self.symbol);
    }
}

pub struct Server {
    pub address: SocketAddr,
    pub handle: Handle,
    task: JoinHandle<io::Result<Metrics>>,
}
impl Server {
    pub async fn bind(config: Config) -> io::Result<Self> {
        Self::start(config, EngineHooks::default()).await
    }
    async fn start(config: Config, hooks: EngineHooks) -> io::Result<Self> {
        config.validate()?;
        let engine = Engine::new(config.symbols.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let listener = TcpListener::bind(config.listen).await?;
        let address = listener.local_addr()?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let (stop, _) = watch::channel(false);
        let (engine_exit, _) = watch::channel(EngineExit::Running);
        let metrics = Metrics {
            market_data_config: MarketDataConfig {
                depth: config.market_data_depth,
                ring_capacity: config.market_data_ring_capacity,
                connection_limit: config.market_data_connections,
                websocket_write_timeout_ms: duration_millis(config.websocket_write_timeout),
                websocket_close_timeout_ms: duration_millis(config.websocket_close_timeout),
                websocket_ping_interval_ms: duration_millis(config.websocket_ping_interval),
                websocket_pong_timeout_ms: duration_millis(config.websocket_pong_timeout),
                websocket_write_buffer: config.websocket_write_buffer,
                websocket_inbound_limit: config.websocket_inbound_limit,
                maximum_frame_bytes: config.maximum_market_data_frame(),
            },
            market_data_symbols: config
                .symbols
                .iter()
                .map(|symbol| (symbol.as_str().into(), market_data_symbol_metrics()))
                .collect(),
            ..Default::default()
        };
        let handle = Handle {
            shared: Arc::new(Shared {
                admission: Mutex::new(Admission {
                    sender: Some(sender),
                    metrics,
                }),
                stop,
                permits: Arc::new(Semaphore::new(config.in_flight)),
                market_permits: Arc::new(Semaphore::new(config.market_data_connections)),
                body_limit: config.body_limit,
                stream_id: Uuid::new_v4().to_string(),
                market_depth: config.market_data_depth,
                websocket: WebSocketLimits {
                    write_timeout: config.websocket_write_timeout,
                    close_timeout: config.websocket_close_timeout,
                    ping_interval: config.websocket_ping_interval,
                    pong_timeout: config.websocket_pong_timeout,
                    write_buffer: config.websocket_write_buffer,
                    inbound_limit: config.websocket_inbound_limit,
                },
                websocket_tasks: Mutex::new(JoinSet::new()),
                engine_exit,
            }),
        };
        let feeds = market::Feeds::new(&config.symbols, config.market_data_ring_capacity);
        let owner_handle = handle.clone();
        let owner = thread::Builder::new()
            .name("trade-engine".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    engine_loop(engine, receiver, &owner_handle, hooks, feeds)
                }));
                match result {
                    Ok(()) => {
                        owner_handle
                            .shared
                            .engine_exit
                            .send_replace(EngineExit::Graceful);
                    }
                    Err(_) => {
                        owner_handle
                            .shared
                            .admission
                            .lock()
                            .unwrap()
                            .metrics
                            .abnormal_engine_exits += 1;
                        owner_handle
                            .shared
                            .admission
                            .lock()
                            .unwrap()
                            .metrics
                            .market_data_abnormal_publisher_exits += 1;
                        owner_handle
                            .shared
                            .engine_exit
                            .send_replace(EngineExit::Abnormal);
                        owner_handle.shutdown();
                    }
                }
            })?;
        let serve_handle = handle.clone();
        let task =
            tokio::spawn(
                async move { serve(listener, serve_handle, owner, config.connections).await },
            );
        Ok(Self {
            address,
            handle,
            task,
        })
    }
    pub async fn wait(self) -> io::Result<Metrics> {
        self.task.await.map_err(io::Error::other)?
    }
    pub async fn shutdown(self) -> io::Result<Metrics> {
        self.handle.shutdown();
        self.wait().await
    }
}
// Test-only control pauses the actual owner before dequeue; no HTTP testing route
// or production pause/failure switch exists.
#[cfg(test)]
type EngineControl = Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>;
#[derive(Default)]
struct EngineHooks {
    #[cfg(test)]
    control: Option<EngineControl>,
}
impl EngineHooks {
    fn before_dequeue(&self) {
        #[cfg(test)]
        if let Some(control) = &self.control {
            let (lock, cond) = &**control;
            let mut state = lock.lock().unwrap();
            while state.0 {
                state = cond.wait(state).unwrap();
            }
            let fail = state.1;
            drop(state);
            assert!(!fail, "injected engine failure");
        }
    }
}
fn engine_loop(
    mut engine: Engine,
    mut receiver: mpsc::Receiver<Message>,
    handle: &Handle,
    hooks: EngineHooks,
    mut feeds: market::Feeds,
) {
    let mut index = 0u64;
    loop {
        hooks.before_dequeue();
        let Some(message) = receiver.blocking_recv() else {
            break;
        };
        handle
            .shared
            .admission
            .lock()
            .unwrap()
            .metrics
            .queue_current = receiver.len();
        let completed = match message.work {
            Work::Command(command) => {
                index = index.checked_add(1).expect("command index exhausted");
                let events = engine.handle(index, command);
                let rejected = matches!(events.last(), Some(EngineEvent::CommandRejected { .. }));
                if !rejected {
                    let symbol = events
                        .last()
                        .and_then(|event| match event {
                            EngineEvent::CommandCompleted { symbol, .. } => Some(symbol),
                            _ => None,
                        })
                        .expect("successful command completes");
                    let snapshot = engine
                        .market_data_snapshot(symbol, handle.shared.market_depth)
                        .expect("successful command symbol exists");
                    let publications = feeds.publish(symbol, &events, snapshot);
                    handle.market_publications(symbol, &publications);
                }
                let mut a = handle.shared.admission.lock().unwrap();
                a.metrics.commands_completed += 1;
                if let Some(EngineEvent::CommandRejected { reason, .. }) = events.last() {
                    *a.metrics
                        .domain_rejected
                        .entry(wire::domain(*reason).1.into())
                        .or_default() += 1;
                }
                Completed::Events(events)
            }
            Work::Snapshot(symbol) => {
                let result = engine.snapshot(&symbol);
                let mut a = handle.shared.admission.lock().unwrap();
                a.metrics.snapshots_completed += 1;
                a.metrics.snapshots_rejected += u64::from(result.is_err());
                Completed::Snapshot(result)
            }
            Work::Subscribe(symbol) => {
                let result = feeds.subscribe(&symbol).ok_or(CommandError::UnknownSymbol);
                let mut a = handle.shared.admission.lock().unwrap();
                a.metrics.market_data_barriers_completed += 1;
                a.metrics.market_data_barriers_rejected += u64::from(result.is_err());
                Completed::Subscription(result)
            }
        };
        if message.reply.send(completed).is_err() {
            handle
                .shared
                .admission
                .lock()
                .unwrap()
                .metrics
                .completion_delivery_failures += 1;
        }
    }
    handle
        .shared
        .admission
        .lock()
        .unwrap()
        .metrics
        .graceful_engine_exits += 1;
    handle
        .shared
        .admission
        .lock()
        .unwrap()
        .metrics
        .market_data_graceful_publisher_exits += 1;
}
async fn serve(
    listener: TcpListener,
    handle: Handle,
    owner: thread::JoinHandle<()>,
    max_connections: usize,
) -> io::Result<Metrics> {
    let connections = Arc::new(Semaphore::new(max_connections));
    let mut tasks = JoinSet::new();
    let mut stop = handle.shared.stop.subscribe();
    let mut failure = None;
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = match accepted { Ok(v) => v, Err(e) => { failure = Some(e); handle.shutdown(); break; } };
                let Ok(permit) = connections.clone().try_acquire_owned() else {
                    handle.shared.admission.lock().unwrap().metrics.connections_rejected += 1;
                    drop(stream);
                    continue;
                };
                let h = handle.clone();
                let mut stopping = h.shared.stop.subscribe();
                let connection_permit = Arc::new(permit);
                tasks.spawn(async move {
                    let service_handle = h.clone();
                    let mut builder = http1::Builder::new();
                    builder
                        .keep_alive(true)
                        .max_buf_size(16384)
                        .max_headers(64)
                        .timer(TokioTimer::new())
                        .header_read_timeout(Duration::from_secs(30));
                    let connection = builder
                        .serve_connection(TokioIo::new(stream), service_fn(move |request| request_handler(request, service_handle.clone(), connection_permit.clone())))
                        .with_upgrades();
                    tokio::pin!(connection);
                    let result = if *stopping.borrow() {
                        connection.as_mut().graceful_shutdown();
                        connection.await
                    } else {
                        tokio::select! {
                            result = &mut connection => result,
                            _ = stopping.changed() => {
                                connection.as_mut().graceful_shutdown();
                                connection.await
                            }
                        }
                    };
                    if result.is_err() { h.shared.admission.lock().unwrap().metrics.connection_errors += 1; }
                });
            }
        }
    }
    drop(listener);
    handle.shutdown();
    while tasks.join_next().await.is_some() {}
    tokio::task::spawn_blocking(move || owner.join())
        .await
        .map_err(io::Error::other)?
        .map_err(|_| io::Error::other("engine join failed"))?;
    let mut websocket_tasks = std::mem::replace(
        &mut *handle.shared.websocket_tasks.lock().unwrap(),
        JoinSet::new(),
    );
    if timeout(handle.shared.websocket.close_timeout, async {
        while websocket_tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        handle.market_forced_task_aborts(websocket_tasks.len());
        websocket_tasks.abort_all();
        while websocket_tasks.join_next().await.is_some() {}
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(handle.metrics())
}
#[derive(Clone, Copy)]
enum Route {
    Post,
    Delete,
    Book,
    MarketData,
}
impl Route {
    fn name(self) -> &'static str {
        match self {
            Self::Post => "POST /orders",
            Self::Delete => "DELETE /orders/:id",
            Self::Book => "GET /book",
            Self::MarketData => "GET /market-data",
        }
    }
}
fn route(request: &Request<Incoming>) -> Result<Route, Error> {
    let expected = match request.uri().path() {
        "/orders" => ("POST", Route::Post),
        "/book" => ("GET", Route::Book),
        "/market-data" => ("GET", Route::MarketData),
        p if p.starts_with("/orders/") && !p[8..].contains('/') => ("DELETE", Route::Delete),
        _ => return Err(Error(404, "route_not_found")),
    };
    if request.method().as_str() != expected.0 {
        return Err(Error(405, "method_not_allowed"));
    }
    Ok(expected.1)
}
async fn request_handler(
    mut request: Request<Incoming>,
    handle: Handle,
    connection_permit: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let route = match route(&request) {
        Ok(r) => r,
        Err(e) => {
            if e.0 == 405 {
                let endpoint = match request.uri().path() {
                    "/orders" => Route::Post,
                    "/book" => Route::Book,
                    "/market-data" => Route::MarketData,
                    _ => Route::Delete,
                };
                handle.record(endpoint.name(), e.0, false);
                if matches!(endpoint, Route::MarketData) {
                    handle.market_upgrade_outcome(e.0, Some(e.1));
                }
            }
            return Ok(error_response(e));
        }
    };
    if matches!(route, Route::MarketData) {
        let outcome = process_market_data(&request, &handle).await;
        return match outcome {
            Ok((symbol, subscription, permit, accept)) => {
                let upgraded = hyper::upgrade::on(&mut request);
                let task_handle = handle.clone();
                handle.market_connection_opened(&symbol);
                let connection = MarketConnection {
                    handle: task_handle.clone(),
                    symbol: symbol.clone(),
                };
                task_handle.clone().spawn_websocket(async move {
                    let _permit = permit;
                    let connection = connection;
                    market_data_socket(
                        upgraded,
                        task_handle.clone(),
                        symbol.clone(),
                        subscription,
                        connection_permit,
                    )
                    .await;
                    drop(connection);
                });
                handle.record(route.name(), 101, false);
                handle.market_upgrade_outcome(101, None);
                Ok(market_upgrade_response(&accept))
            }
            Err(error) => {
                let overload =
                    error.1 == "ingress_overloaded" || error.1 == "market_data_overloaded";
                handle.record(route.name(), error.0, overload);
                handle.market_upgrade_outcome(error.0, Some(error.1));
                Ok(error_response(error))
            }
        };
    }
    let outcome = process(request, route, &handle).await;
    let (status, body, overload) = match outcome {
        Ok((status, body)) => (status, body, false),
        Err(Error(status, code)) => (
            status,
            serde_json::json!({"error":{"code":code}}).to_string(),
            code == "ingress_overloaded",
        ),
    };
    handle.record(route.name(), status, overload);
    Ok(response(status, body))
}
async fn process(
    mut request: Request<Incoming>,
    route: Route,
    handle: &Handle,
) -> Result<(u16, String), Error> {
    if handle.unavailable() {
        return Err(UNAVAILABLE);
    }
    let _permit = handle
        .shared
        .permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| OVERLOADED)?;
    if matches!(route, Route::Post) {
        let mut types = request
            .headers()
            .get_all(hyper::header::CONTENT_TYPE)
            .iter();
        let content = types
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or(Error(415, "unsupported_media_type"))?;
        let parts: Vec<_> = content.split(';').map(str::trim).collect();
        if types.next().is_some()
            || !parts[0].eq_ignore_ascii_case("application/json")
            || parts.len() > 2
            || (parts.len() == 2 && !parts[1].eq_ignore_ascii_case("charset=utf-8"))
        {
            return Err(Error(415, "unsupported_media_type"));
        }
    }
    let mut body = Vec::new();
    let mut stopping = handle.shared.stop.subscribe();
    loop {
        if *stopping.borrow() {
            return Err(UNAVAILABLE);
        }
        let frame = tokio::select! {
            biased;
            _ = stopping.changed() => return Err(UNAVAILABLE),
            frame = request.body_mut().frame() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        let frame = frame.map_err(|_| MALFORMED)?;
        if let Ok(data) = frame.into_data() {
            if data.len() > handle.shared.body_limit.saturating_sub(body.len()) {
                return Err(Error(413, "body_too_large"));
            }
            body.extend_from_slice(&data);
        } else {
            return Err(MALFORMED);
        }
    }
    let work = match route {
        Route::Post => {
            if request.uri().query().is_some() {
                return Err(MALFORMED);
            }
            Work::Command(wire::command(&body)?)
        }
        Route::Delete => {
            if !body.is_empty() {
                return Err(MALFORMED);
            }
            Work::Command(EngineCommand::Cancel {
                symbol: wire::query(request.uri().query())?,
                order_id: OrderId(wire::unsigned(&request.uri().path()[8..])?),
            })
        }
        Route::Book => {
            if !body.is_empty() {
                return Err(MALFORMED);
            }
            Work::Snapshot(wire::query(request.uri().query())?)
        }
        Route::MarketData => unreachable!("market-data upgrades are handled before HTTP bodies"),
    };
    let receive = handle.admit(work)?;
    // After admission there is deliberately no timeout or shutdown cancellation.
    match receive.await {
        Ok(Completed::Events(events)) => Ok(wire::events(&events)),
        Ok(Completed::Snapshot(Ok(book))) => Ok((200, wire::snapshot(book).to_string())),
        Ok(Completed::Snapshot(Err(reason))) => Err(wire::domain(reason)),
        Ok(Completed::Subscription(_)) => unreachable!("HTTP work has HTTP completion"),
        Err(_) => {
            handle.shutdown();
            Err(Error(500, "completion_unavailable"))
        }
    }
}

fn market_upgrade_response(accept: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(101)
        .header(hyper::header::CONNECTION, "Upgrade")
        .header(hyper::header::UPGRADE, "websocket")
        .header("sec-websocket-accept", accept)
        .header("sec-websocket-protocol", MARKET_DATA_PROTOCOL)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

async fn process_market_data(
    request: &Request<Incoming>,
    handle: &Handle,
) -> Result<
    (
        Symbol,
        market::Subscription,
        tokio::sync::OwnedSemaphorePermit,
        String,
    ),
    Error,
> {
    if handle.unavailable() {
        return Err(UNAVAILABLE);
    }
    let symbol = wire::query(request.uri().query())?;
    let accept = websocket_accept(request)?;
    let permit = match handle.shared.market_permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            handle.market_rejected();
            return Err(MARKET_OVERLOADED);
        }
    };
    let receive = match handle.admit(Work::Subscribe(symbol.clone())) {
        Ok(receive) => receive,
        Err(error) => {
            if error.1 == "ingress_overloaded" {
                handle
                    .shared
                    .admission
                    .lock()
                    .unwrap()
                    .metrics
                    .market_data_barriers_rejected += 1;
            }
            return Err(error);
        }
    };
    match receive.await {
        Ok(Completed::Subscription(Ok(subscription))) => Ok((symbol, subscription, permit, accept)),
        Ok(Completed::Subscription(Err(reason))) => Err(wire::domain(reason)),
        Err(_) => Err(UNAVAILABLE),
        Ok(_) => unreachable!("subscription work has subscription completion"),
    }
}

fn websocket_accept(request: &Request<Incoming>) -> Result<String, Error> {
    if request.method() != hyper::Method::GET {
        return Err(Error(405, "method_not_allowed"));
    }
    if request
        .headers()
        .contains_key(hyper::header::TRANSFER_ENCODING)
        || request
            .headers()
            .get(hyper::header::CONTENT_LENGTH)
            .is_some_and(|value| value != "0")
    {
        return Err(MALFORMED);
    }
    let upgrade = header_tokens(request, "upgrade")?;
    let connection = header_tokens(request, "connection")?;
    if upgrade.is_empty() || connection.is_empty() {
        return Err(Error(426, "upgrade_required"));
    }
    if upgrade.len() != 1
        || !upgrade[0].eq_ignore_ascii_case("websocket")
        || !connection
            .iter()
            .any(|token| token.eq_ignore_ascii_case("upgrade"))
    {
        return Err(MALFORMED);
    }
    let single_header = |name| {
        let mut values = request.headers().get_all(name).iter();
        let value = values.next()?.to_str().ok()?;
        values.next().is_none().then_some(value)
    };
    let version = single_header("sec-websocket-version");
    let key = single_header("sec-websocket-key");
    let (Some("13"), Some(key)) = (version, key) else {
        return Err(MALFORMED);
    };
    if STANDARD
        .decode(key)
        .map_or(true, |decoded| decoded.len() != 16)
    {
        return Err(MALFORMED);
    }
    let decoded = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    let protocols = header_tokens(request, "sec-websocket-protocol")?;
    if protocols.len() != 1 || protocols[0] != MARKET_DATA_PROTOCOL {
        return Err(Error(400, "unsupported_subprotocol"));
    }
    Ok(decoded)
}

fn header_tokens<'a>(
    request: &'a Request<Incoming>,
    name: &'static str,
) -> Result<Vec<&'a str>, Error> {
    let mut tokens = Vec::new();
    for value in request.headers().get_all(name).iter() {
        for token in value.to_str().map_err(|_| MALFORMED)?.split(',') {
            let token = token.trim();
            if token.is_empty() || !token.bytes().all(is_http_token) {
                return Err(MALFORMED);
            }
            tokens.push(token);
        }
    }
    Ok(tokens)
}

fn is_http_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

type UpgradedSocket = WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>;

async fn market_data_socket(
    upgraded: hyper::upgrade::OnUpgrade,
    handle: Handle,
    symbol: Symbol,
    mut subscription: market::Subscription,
    _connection_permit: Arc<tokio::sync::OwnedSemaphorePermit>,
) {
    let Ok(upgraded) = upgraded.await else {
        return;
    };
    let limits = handle.shared.websocket;
    let config = WebSocketConfig {
        write_buffer_size: (limits.write_buffer / 2).min(8 * 1024),
        max_write_buffer_size: limits.write_buffer,
        max_message_size: Some(limits.inbound_limit),
        max_frame_size: Some(limits.inbound_limit),
        ..WebSocketConfig::default()
    };
    let mut socket =
        WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, Some(config)).await;
    let initial = match market::initial_json(
        &handle.shared.stream_id,
        &symbol,
        subscription.sequence,
        handle.shared.market_depth,
        &subscription.projection,
    ) {
        Ok(frame) => frame,
        Err(_) => {
            handle.market_disconnected("serialization");
            close_socket(
                &mut socket,
                CloseCode::Error,
                "serialization_failure",
                limits.close_timeout,
            )
            .await;
            return;
        }
    };
    let initial_bytes = initial.len();
    if let Err(failure) = send_text(&mut socket, initial, limits.write_timeout).await {
        handle.market_disconnected(failure.reason());
        return;
    }
    handle.market_frame_written(&symbol, market::RecordKind::L2, initial_bytes);
    let mut engine_exit = handle.shared.engine_exit.subscribe();
    let mut heartbeat = interval(limits.ping_interval);
    heartbeat.tick().await;
    let mut last_pong = Instant::now();
    loop {
        tokio::select! {
            received = subscription.receiver.recv() => match received {
                Ok(event) => {
                    let frame = match market::event_json(&handle.shared.stream_id, &symbol, handle.shared.market_depth, &event) {
                        Ok(frame) => frame,
                        Err(_) => {
                            handle.market_disconnected("serialization");
                            close_socket(&mut socket, CloseCode::Error, "serialization_failure", limits.close_timeout).await;
                            return;
                        }
                    };
                    let kind = event.record.kind();
                    let bytes = frame.len();
                    if let Err(failure) = send_text(&mut socket, frame, limits.write_timeout).await {
                        handle.market_disconnected(failure.reason());
                        return;
                    }
                    handle.market_frame_written(&symbol, kind, bytes);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    {
                        let mut metrics = handle.shared.admission.lock().unwrap();
                        metrics.metrics.market_data_lag_disconnects += 1;
                        metrics.metrics.market_data_lagged_messages += missed;
                    }
                    handle.market_disconnected("lag");
                    close_socket(&mut socket, CloseCode::Again, "resync_required", limits.close_timeout).await;
                    return;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    let (code, reason, metric_reason) = match engine_exit_reason(&mut engine_exit).await {
                        EngineExit::Abnormal => (CloseCode::Error, "internal_error", "abnormal_engine"),
                        EngineExit::Graceful | EngineExit::Running => {
                            (CloseCode::Away, "server_shutdown", "shutdown")
                        }
                    };
                    handle.market_disconnected(metric_reason);
                    close_socket(&mut socket, code, reason, limits.close_timeout).await;
                    return;
                }
            },
            inbound = socket.next() => match inbound {
                Some(Ok(WsMessage::Ping(payload))) => {
                    if let Err(failure) = send_message(&mut socket, WsMessage::Pong(payload), limits.write_timeout).await {
                        handle.market_disconnected(failure.reason());
                        return;
                    }
                }
                Some(Ok(WsMessage::Pong(_))) => last_pong = Instant::now(),
                Some(Ok(WsMessage::Close(_))) | None => {
                    handle.market_disconnected("peer_closed");
                    return;
                }
                Some(Err(_)) => {
                    handle.market_disconnected("peer_io");
                    return;
                }
                Some(Ok(WsMessage::Text(_))) | Some(Ok(WsMessage::Binary(_))) => {
                    handle.market_disconnected("protocol");
                    close_socket(&mut socket, CloseCode::Policy, "read_only", limits.close_timeout).await;
                    return;
                }
                Some(Ok(_)) => {
                    handle.market_disconnected("protocol");
                    return;
                }
            },
            _ = heartbeat.tick() => {
                if last_pong.elapsed() >= limits.pong_timeout {
                    handle.market_disconnected("heartbeat");
                    close_socket(&mut socket, CloseCode::Policy, "pong_timeout", limits.close_timeout).await;
                    return;
                }
                if let Err(failure) = send_message(&mut socket, WsMessage::Ping(Vec::new()), limits.write_timeout).await {
                    handle.market_disconnected(failure.reason());
                    return;
                }
            },
        }
    }
}

async fn engine_exit_reason(engine_exit: &mut watch::Receiver<EngineExit>) -> EngineExit {
    loop {
        let reason = *engine_exit.borrow_and_update();
        if reason != EngineExit::Running {
            return reason;
        }
        if engine_exit.changed().await.is_err() {
            return EngineExit::Abnormal;
        }
    }
}

enum WriteFailure {
    Timeout,
    BufferFull,
    PeerIo,
}

impl WriteFailure {
    fn reason(&self) -> &'static str {
        match self {
            Self::Timeout => "write_timeout",
            Self::BufferFull => "write_buffer_full",
            Self::PeerIo => "peer_io",
        }
    }
}

async fn send_text(
    socket: &mut UpgradedSocket,
    text: String,
    timeout_limit: Duration,
) -> Result<(), WriteFailure> {
    send_message(socket, WsMessage::Text(text), timeout_limit).await
}

async fn send_message(
    socket: &mut UpgradedSocket,
    message: WsMessage,
    timeout_limit: Duration,
) -> Result<(), WriteFailure> {
    match timeout(timeout_limit, socket.send(message)).await {
        Err(_) => Err(WriteFailure::Timeout),
        Ok(Ok(())) => Ok(()),
        Ok(Err(tokio_tungstenite::tungstenite::Error::WriteBufferFull(_))) => {
            Err(WriteFailure::BufferFull)
        }
        Ok(Err(_)) => Err(WriteFailure::PeerIo),
    }
}

async fn close_socket(
    socket: &mut UpgradedSocket,
    code: CloseCode,
    reason: &'static str,
    timeout_limit: Duration,
) {
    let _ = timeout(
        timeout_limit,
        socket.send(WsMessage::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}
fn response(status: u16, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}
fn error_response(error: Error) -> Response<Full<Bytes>> {
    let mut builder = Response::builder()
        .status(error.0)
        .header(hyper::header::CONTENT_TYPE, "application/json");
    if error.1 == "upgrade_required" {
        builder = builder.header(hyper::header::UPGRADE, "websocket");
    }
    builder
        .body(Full::new(Bytes::from(
            serde_json::json!({"error":{"code":error.1}}).to_string(),
        )))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    use hyper::{Method, StatusCode, client::conn::http1};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        time::{Duration, timeout},
    };
    use tokio_tungstenite::{
        MaybeTlsStream, connect_async,
        tungstenite::{Message as ClientMessage, client::IntoClientRequest},
    };

    const ORDER: &str = r#"{"type":"limit","symbol":"BTC-USD","order_id":%d,"owner_id":7,"side":"buy","price_ticks":100,"quantity":1}"#;

    fn paused_control() -> EngineControl {
        Arc::new((Mutex::new((true, false)), std::sync::Condvar::new()))
    }

    fn resume(control: &EngineControl) {
        let (lock, cond) = &**control;
        lock.lock().unwrap().0 = false;
        cond.notify_all();
    }

    fn pause(control: &EngineControl) {
        control.0.lock().unwrap().0 = true;
    }

    fn fail_next(control: &EngineControl) {
        control.0.lock().unwrap().1 = true;
    }

    async fn server(queue_capacity: usize) -> (Server, EngineControl) {
        let control = paused_control();
        let config = Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            symbols: vec![Symbol::new("BTC-USD")],
            queue_capacity,
            in_flight: 4,
            body_limit: 16 * 1024,
            connections: 8,
            market_data_connections: 8,
            ..Config::default()
        };
        let server = Server::start(
            config,
            EngineHooks {
                control: Some(control.clone()),
            },
        )
        .await
        .unwrap();
        (server, control)
    }

    async fn running_server(symbols: Vec<Symbol>, ring_capacity: usize) -> Server {
        Server::start(
            Config {
                listen: "127.0.0.1:0".parse().unwrap(),
                symbols,
                queue_capacity: 32,
                in_flight: 32,
                body_limit: 16 * 1024,
                connections: 16,
                market_data_connections: 8,
                market_data_ring_capacity: ring_capacity,
                ..Config::default()
            },
            EngineHooks::default(),
        )
        .await
        .unwrap()
    }

    type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

    async fn websocket(address: SocketAddr, symbol: &str) -> ClientSocket {
        let mut request = format!("ws://{address}/market-data?symbol={symbol}")
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            "sec-websocket-protocol",
            MARKET_DATA_PROTOCOL.parse().unwrap(),
        );
        let (socket, response) = connect_async(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            response.headers().get("sec-websocket-protocol").unwrap(),
            MARKET_DATA_PROTOCOL
        );
        socket
    }

    async fn next_json(socket: &mut ClientSocket) -> serde_json::Value {
        let frame = timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("market-data frame before timeout")
            .expect("open websocket")
            .expect("valid websocket frame");
        let ClientMessage::Text(text) = frame else {
            panic!("expected JSON text frame, got {frame:?}");
        };
        serde_json::from_str(&text).unwrap()
    }

    async fn request(
        address: SocketAddr,
        method: Method,
        path: &str,
        body: String,
    ) -> (StatusCode, serde_json::Value) {
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream)).await.unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap() });
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        drop(sender);
        connection.await.unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    async fn wait_for(handle: &Handle, predicate: impl Fn(&Metrics) -> bool) {
        timeout(Duration::from_secs(1), async {
            while !predicate(&handle.metrics()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("observed server state before timeout");
    }

    #[tokio::test]
    async fn queue_capacity_rejects_another_http_command_before_engine_execution() {
        let (server, control) = server(1).await;
        let first = tokio::spawn(request(
            server.address,
            Method::POST,
            "/orders",
            ORDER.replace("%d", "1"),
        ));
        wait_for(&server.handle, |metrics| metrics.commands_admitted == 1).await;

        let (status, body) = request(
            server.address,
            Method::POST,
            "/orders",
            ORDER.replace("%d", "2"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["code"], "ingress_overloaded");
        assert_eq!(server.handle.metrics().commands_admitted, 1);
        assert_eq!(server.handle.metrics().commands_completed, 0);

        resume(&control);
        assert_eq!(first.await.unwrap().0, StatusCode::OK);
        let metrics = server.shutdown().await.unwrap();
        assert_eq!(metrics.commands_admitted, 1);
        assert_eq!(metrics.commands_completed, 1);
    }

    #[tokio::test]
    async fn disconnected_client_after_admission_does_not_rollback_its_command() {
        let (server, control) = server(1).await;
        let mut stream = TcpStream::connect(server.address).await.unwrap();
        let body = ORDER.replace("%d", "51");
        let raw_request = format!(
            "POST /orders HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(raw_request.as_bytes()).await.unwrap();
        wait_for(&server.handle, |metrics| metrics.commands_admitted == 1).await;
        drop(stream);

        resume(&control);
        wait_for(&server.handle, |metrics| metrics.commands_completed == 1).await;
        let (status, book) = request(
            server.address,
            Method::GET,
            "/book?symbol=BTC-USD",
            String::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(book["bids"][0]["orders"][0]["order_id"], 51);
        let metrics = server.shutdown().await.unwrap();
        assert_eq!(metrics.commands_admitted, 1);
        assert_eq!(metrics.commands_completed, 1);
    }

    #[tokio::test]
    async fn shutdown_drains_an_admitted_command_before_engine_exit() {
        let (server, control) = server(1).await;
        let handle = server.handle.clone();
        let pending = tokio::spawn(request(
            server.address,
            Method::POST,
            "/orders",
            ORDER.replace("%d", "71"),
        ));
        wait_for(&handle, |metrics| metrics.commands_admitted == 1).await;

        let draining = tokio::spawn(server.shutdown());
        timeout(Duration::from_secs(1), async {
            while !handle.unavailable() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown closed admission before timeout");
        resume(&control);

        let (status, body) = pending.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["events"][0]["order_id"], 71);
        let metrics = draining.await.unwrap().unwrap();
        assert_eq!(metrics.commands_admitted, 1);
        assert_eq!(metrics.commands_completed, 1);
        assert_eq!(metrics.graceful_engine_exits, 1);
    }

    #[tokio::test]
    async fn shutdown_flushes_admitted_market_data_before_closing_sockets() {
        let (server, control) = server(4).await;
        resume(&control);
        let mut socket = websocket(server.address, "BTC-USD").await;
        assert_eq!(next_json(&mut socket).await["sequence"], 0);

        pause(&control);
        assert_eq!(
            request(
                server.address,
                Method::GET,
                "/book?symbol=BTC-USD",
                String::new(),
            )
            .await
            .0,
            StatusCode::OK
        );
        let handle = server.handle.clone();
        let pending = tokio::spawn(request(
            server.address,
            Method::POST,
            "/orders",
            ORDER.replace("%d", "72"),
        ));
        wait_for(&handle, |metrics| metrics.commands_admitted == 1).await;
        let draining = tokio::spawn(server.shutdown());
        resume(&control);

        assert_eq!(pending.await.unwrap().0, StatusCode::OK);
        assert_eq!(next_json(&mut socket).await["sequence"], 1);
        assert_eq!(next_json(&mut socket).await["sequence"], 2);
        assert_eq!(draining.await.unwrap().unwrap().commands_completed, 1);
    }

    #[tokio::test]
    async fn abnormal_engine_exit_closes_market_data_with_internal_error() {
        let (server, control) = server(4).await;
        resume(&control);
        let mut socket = websocket(server.address, "BTC-USD").await;
        assert_eq!(next_json(&mut socket).await["sequence"], 0);

        pause(&control);
        assert_eq!(
            request(
                server.address,
                Method::GET,
                "/book?symbol=BTC-USD",
                String::new(),
            )
            .await
            .0,
            StatusCode::OK
        );
        let pending = tokio::spawn(request(
            server.address,
            Method::POST,
            "/orders",
            ORDER.replace("%d", "73"),
        ));
        wait_for(&server.handle, |metrics| metrics.commands_admitted == 1).await;
        fail_next(&control);
        resume(&control);

        let close = timeout(Duration::from_secs(1), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            close,
            ClientMessage::Close(Some(frame)) if frame.code == CloseCode::Error
        ));
        assert_eq!(pending.await.unwrap().0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(server.wait().await.unwrap().abnormal_engine_exits, 1);
    }

    #[tokio::test]
    async fn healthy_reader_receives_contiguous_three_record_match_at_minimum_ring_capacity() {
        let server =
            running_server(vec![Symbol::new("BTC-USD")], MIN_MARKET_DATA_RING_CAPACITY).await;
        let mut socket = websocket(server.address, "BTC-USD").await;
        let initial = next_json(&mut socket).await;
        assert_eq!(initial["type"], "l2_snapshot");
        assert_eq!(initial["reason"], "initial");
        assert_eq!(initial["sequence"], 0);
        assert_eq!(initial["depth"], 20);
        assert_eq!(initial["bids"], serde_json::json!([]));
        assert_eq!(initial["asks"], serde_json::json!([]));

        let (status, _) = request(
            server.address,
            Method::POST,
            "/orders",
            ORDER.replace("%d", "1"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let bbo = next_json(&mut socket).await;
        let l2 = next_json(&mut socket).await;
        assert_eq!(bbo["type"], "bbo");
        assert_eq!(bbo["sequence"], 1);
        assert_eq!(l2["type"], "l2_snapshot");
        assert_eq!(l2["reason"], "update");
        assert_eq!(l2["sequence"], 2);

        let sell = r#"{"type":"limit","symbol":"BTC-USD","order_id":2,"owner_id":8,"side":"sell","price_ticks":100,"quantity":1}"#;
        assert_eq!(
            request(server.address, Method::POST, "/orders", sell.into())
                .await
                .0,
            StatusCode::OK
        );
        let trade = next_json(&mut socket).await;
        let bbo = next_json(&mut socket).await;
        let l2 = next_json(&mut socket).await;
        assert_eq!(trade["type"], "trade");
        assert_eq!(trade["sequence"], 3);
        assert_eq!(trade["price_ticks"], 100);
        assert_eq!(trade["quantity"], 1);
        assert_eq!(bbo["type"], "bbo");
        assert_eq!(bbo["sequence"], 4);
        assert_eq!(l2["type"], "l2_snapshot");
        assert_eq!(l2["sequence"], 5);
        assert_eq!(trade["stream_id"], initial["stream_id"]);
        assert_eq!(l2["symbol"], "BTC-USD");

        let metrics = server.shutdown().await.unwrap();
        assert_eq!(metrics.market_data_config.ring_capacity, 64);
        assert_eq!(metrics.market_data_lag_disconnects, 0);
        assert_eq!(metrics.market_data_disconnects.get("lag"), None);
    }

    #[tokio::test]
    async fn market_data_isolates_symbols_and_rejects_application_messages() {
        let server = running_server(
            vec![Symbol::new("BTC-USD"), Symbol::new("ETH-USD")],
            MIN_MARKET_DATA_RING_CAPACITY,
        )
        .await;
        let mut btc = websocket(server.address, "BTC-USD").await;
        let mut eth = websocket(server.address, "ETH-USD").await;
        assert_eq!(next_json(&mut btc).await["sequence"], 0);
        assert_eq!(next_json(&mut eth).await["sequence"], 0);

        assert_eq!(
            request(
                server.address,
                Method::POST,
                "/orders",
                ORDER.replace("%d", "1"),
            )
            .await
            .0,
            StatusCode::OK
        );
        assert_eq!(next_json(&mut btc).await["sequence"], 1);
        assert_eq!(next_json(&mut btc).await["sequence"], 2);
        assert!(
            timeout(Duration::from_millis(30), eth.next())
                .await
                .is_err()
        );

        eth.send(ClientMessage::Text("{}".into())).await.unwrap();
        let close = timeout(Duration::from_secs(1), eth.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(close, ClientMessage::Close(_)));
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn market_data_upgrade_failures_do_not_admit_a_subscription() {
        let server =
            running_server(vec![Symbol::new("BTC-USD")], MIN_MARKET_DATA_RING_CAPACITY).await;
        let (status, body) = request(
            server.address,
            Method::GET,
            "/market-data?symbol=BTC-USD",
            String::new(),
        )
        .await;
        assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
        assert_eq!(body["error"]["code"], "upgrade_required");

        let mut malformed = TcpStream::connect(server.address).await.unwrap();
        malformed
            .write_all(
                b"GET /market-data?symbol=BTC-USD HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade, @\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: trade-rs.market-data.v1\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = [0; 128];
        let size = timeout(Duration::from_secs(1), malformed.read(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(
            std::str::from_utf8(&response[..size])
                .unwrap()
                .starts_with("HTTP/1.1 400")
        );
        let metrics = server.handle.metrics();
        assert_eq!(metrics.market_data_barriers_admitted, 0);
        assert_eq!(metrics.market_data_connections_current, 0);
        assert_eq!(
            metrics.market_data_upgrade_outcomes["400:malformed_request"],
            1
        );
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn upgraded_socket_holds_the_total_connection_permit() {
        let server = Server::start(
            Config {
                listen: "127.0.0.1:0".parse().unwrap(),
                symbols: vec![Symbol::new("BTC-USD")],
                queue_capacity: 4,
                in_flight: 4,
                body_limit: 16 * 1024,
                connections: 1,
                market_data_connections: 1,
                ..Config::default()
            },
            EngineHooks::default(),
        )
        .await
        .unwrap();
        let _socket = websocket(server.address, "BTC-USD").await;
        let mut rejected = TcpStream::connect(server.address).await.unwrap();
        rejected
            .write_all(b"GET /book?symbol=BTC-USD HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut byte = [0];
        match timeout(Duration::from_secs(1), rejected.read(&mut byte)).await {
            Ok(Ok(0) | Err(_)) => {}
            result => panic!("expected rejected TCP connection, got {result:?}"),
        }
        wait_for(&server.handle, |metrics| metrics.connections_rejected == 1).await;
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn market_data_metrics_reconcile_publications_and_delivery() {
        let server =
            running_server(vec![Symbol::new("BTC-USD")], MIN_MARKET_DATA_RING_CAPACITY).await;
        let mut socket = websocket(server.address, "BTC-USD").await;
        assert_eq!(next_json(&mut socket).await["sequence"], 0);
        assert_eq!(
            request(
                server.address,
                Method::POST,
                "/orders",
                ORDER.replace("%d", "91"),
            )
            .await
            .0,
            StatusCode::OK
        );
        assert_eq!(next_json(&mut socket).await["sequence"], 1);
        assert_eq!(next_json(&mut socket).await["sequence"], 2);

        let metrics = server.shutdown().await.unwrap();
        let symbol = metrics.market_data_symbols.get("BTC-USD").unwrap();
        assert_eq!(metrics.market_data_upgrade_outcomes["101"], 1);
        assert_eq!(
            metrics.market_data_config.ring_capacity,
            MIN_MARKET_DATA_RING_CAPACITY
        );
        assert!(metrics.market_data_config.maximum_frame_bytes > 0);
        assert_eq!(symbol.subscribers_current, 0);
        assert_eq!(symbol.subscribers_high_water, 1);
        assert_eq!(symbol.sequence, 2);
        assert_eq!(symbol.logical_events["bbo"], 1);
        assert_eq!(symbol.logical_events["l2_snapshot"], 1);
        assert_eq!(symbol.ring_sends_with_receivers, 2);
        assert_eq!(symbol.events_without_receivers, 0);
        assert_eq!(symbol.frames_written["bbo"], 1);
        assert_eq!(symbol.frames_written["l2_snapshot"], 2);
        assert!(symbol.frame_bytes_written["l2_snapshot"] > 0);
        assert_eq!(metrics.market_data_disconnects["shutdown"], 1);
        assert_eq!(metrics.market_data_graceful_publisher_exits, 1);
    }

    #[tokio::test]
    async fn reconnect_uses_a_new_snapshot_baseline_without_replay() {
        let server =
            running_server(vec![Symbol::new("BTC-USD")], MIN_MARKET_DATA_RING_CAPACITY).await;
        let mut first = websocket(server.address, "BTC-USD").await;
        let initial = next_json(&mut first).await;
        assert_eq!(initial["sequence"], 0);
        assert_eq!(
            request(
                server.address,
                Method::POST,
                "/orders",
                ORDER.replace("%d", "1"),
            )
            .await
            .0,
            StatusCode::OK
        );
        assert_eq!(next_json(&mut first).await["sequence"], 1);
        assert_eq!(next_json(&mut first).await["sequence"], 2);
        drop(first);

        let sell = r#"{"type":"limit","symbol":"BTC-USD","order_id":2,"owner_id":8,"side":"sell","price_ticks":100,"quantity":1}"#;
        assert_eq!(
            request(server.address, Method::POST, "/orders", sell.into())
                .await
                .0,
            StatusCode::OK
        );
        let mut reconnected = websocket(server.address, "BTC-USD").await;
        let baseline = next_json(&mut reconnected).await;
        assert_eq!(baseline["reason"], "initial");
        assert_eq!(baseline["sequence"], 5);
        assert_eq!(baseline["bids"], serde_json::json!([]));
        assert_eq!(baseline["asks"], serde_json::json!([]));
        assert_eq!(baseline["stream_id"], initial["stream_id"]);

        assert_eq!(
            request(
                server.address,
                Method::POST,
                "/orders",
                ORDER.replace("%d", "3"),
            )
            .await
            .0,
            StatusCode::OK
        );
        assert_eq!(next_json(&mut reconnected).await["sequence"], 6);
        assert_eq!(next_json(&mut reconnected).await["sequence"], 7);
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn market_data_ring_capacity_rejects_below_minimum_and_non_power_of_two_values() {
        assert_eq!(Config::default().market_data_ring_capacity, 1024);

        for (capacity, expected) in [
            (32, "market-data ring capacity must be at least 64"),
            (96, "market-data ring capacity must be a power of two"),
        ] {
            let error = match Server::bind(Config {
                market_data_ring_capacity: capacity,
                ..Config::default()
            })
            .await
            {
                Ok(_) => panic!("ring capacity {capacity} unexpectedly started"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(error.to_string(), expected);
        }
    }
}
