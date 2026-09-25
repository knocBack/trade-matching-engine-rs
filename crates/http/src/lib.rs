//! Bounded HTTP/1.1 adapter. The engine lives only on its dedicated owner thread.
mod wire;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    convert::Infallible,
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
};
use trade_orderbook::{BookSnapshot, CommandError, OrderId, Symbol};
use trade_replay::{Engine, EngineCommand, EngineEvent};
use wire::{Error, MALFORMED, OVERLOADED, UNAVAILABLE};

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub symbols: Vec<Symbol>,
    pub queue_capacity: usize,
    pub in_flight: usize,
    pub body_limit: usize,
    pub connections: usize,
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
        }
    }
}
impl Config {
    fn validate(&self) -> io::Result<()> {
        if self.queue_capacity == 0
            || self.in_flight == 0
            || self.connections == 0
            || self.body_limit == 0
            || self.body_limit > 16 * 1024
            || [self.queue_capacity, self.in_flight, self.connections]
                .iter()
                .any(|&v| v > Semaphore::MAX_PERMITS)
            || self.symbols.is_empty()
            || self.symbols.iter().any(|s| s.as_str().trim().is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "positive finite limits and nonempty symbols required; body <= 16384",
            ));
        }
        Ok(())
    }
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
}
enum Work {
    Command(EngineCommand),
    Snapshot(Symbol),
}
enum Completed {
    Events(Vec<EngineEvent>),
    Snapshot(Result<BookSnapshot, CommandError>),
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
    body_limit: usize,
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
        let handle = Handle {
            shared: Arc::new(Shared {
                admission: Mutex::new(Admission {
                    sender: Some(sender),
                    metrics: Metrics::default(),
                }),
                stop,
                permits: Arc::new(Semaphore::new(config.in_flight)),
                body_limit: config.body_limit,
            }),
        };
        let owner_handle = handle.clone();
        let owner = thread::Builder::new()
            .name("trade-engine".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    engine_loop(engine, receiver, &owner_handle, hooks)
                }));
                if result.is_err() {
                    owner_handle
                        .shared
                        .admission
                        .lock()
                        .unwrap()
                        .metrics
                        .abnormal_engine_exits += 1;
                    owner_handle.shutdown();
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
                tasks.spawn(async move {
                    let _permit = permit;
                    let service_handle = h.clone();
                    let mut builder = http1::Builder::new();
                    builder
                        .keep_alive(true)
                        .max_buf_size(16384)
                        .max_headers(64)
                        .timer(TokioTimer::new())
                        .header_read_timeout(Duration::from_secs(30));
                    let connection = builder.serve_connection(TokioIo::new(stream), service_fn(move |request| request_handler(request, service_handle.clone())));
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
}
impl Route {
    fn name(self) -> &'static str {
        match self {
            Self::Post => "POST /orders",
            Self::Delete => "DELETE /orders/:id",
            Self::Book => "GET /book",
        }
    }
}
fn route(request: &Request<Incoming>) -> Result<Route, Error> {
    let expected = match request.uri().path() {
        "/orders" => ("POST", Route::Post),
        "/book" => ("GET", Route::Book),
        p if p.starts_with("/orders/") && !p[8..].contains('/') => ("DELETE", Route::Delete),
        _ => return Err(Error(404, "route_not_found")),
    };
    if request.method().as_str() != expected.0 {
        return Err(Error(405, "method_not_allowed"));
    }
    Ok(expected.1)
}
async fn request_handler(
    request: Request<Incoming>,
    handle: Handle,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let route = match route(&request) {
        Ok(r) => r,
        Err(e) => {
            if e.0 == 405 {
                let endpoint = match request.uri().path() {
                    "/orders" => Route::Post,
                    "/book" => Route::Book,
                    _ => Route::Delete,
                };
                handle.record(endpoint.name(), e.0, false);
            }
            return Ok(error_response(e));
        }
    };
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
    };
    let receive = handle.admit(work)?;
    // After admission there is deliberately no timeout or shutdown cancellation.
    match receive.await {
        Ok(Completed::Events(events)) => Ok(wire::events(&events)),
        Ok(Completed::Snapshot(Ok(book))) => Ok((200, wire::snapshot(book).to_string())),
        Ok(Completed::Snapshot(Err(reason))) => Err(wire::domain(reason)),
        Err(_) => {
            handle.shutdown();
            Err(Error(500, "completion_unavailable"))
        }
    }
}
fn response(status: u16, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}
fn error_response(error: Error) -> Response<Full<Bytes>> {
    response(
        error.0,
        serde_json::json!({"error":{"code":error.1}}).to_string(),
    )
}
