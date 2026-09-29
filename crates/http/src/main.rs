use std::io;
use tracing_subscriber::EnvFilter;
use trade_http::{Config, Server};
use trade_orderbook::Symbol;

#[tokio::main]
async fn main() -> io::Result<()> {
    init_logging();
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--help" {
            println!(
                "trade-http [--listen 127.0.0.1:8080] [--symbols BTC-USD,ETH-USD] [--queue-capacity 256] [--in-flight 512] [--body-limit 16384] [--connections 1024] [--market-data-depth 20] [--market-data-ring-capacity 1024] [--market-data-connections 128] [--websocket-write-timeout-ms 1000] [--websocket-close-timeout-ms 1000] [--websocket-ping-interval-ms 15000] [--websocket-pong-timeout-ms 30000] [--websocket-write-buffer 65536] [--websocket-inbound-limit 1024]\nMarket-data ring capacity must be a power of two and at least 64. SIGINT/SIGTERM drain admitted work. Startup and final JSON records are written to stdout; JSON logs are written to stderr and configured with RUST_LOG."
            );
            return Ok(());
        }
        let value = args
            .next()
            .ok_or_else(|| io::Error::other("missing argument value"))?;
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid argument: {flag}"),
            )
        };
        match flag.as_str() {
            "--listen" => config.listen = value.parse().map_err(|_| invalid())?,
            "--symbols" => config.symbols = value.split(',').map(Symbol::new).collect(),
            "--queue-capacity" => config.queue_capacity = value.parse().map_err(|_| invalid())?,
            "--in-flight" => config.in_flight = value.parse().map_err(|_| invalid())?,
            "--body-limit" => config.body_limit = value.parse().map_err(|_| invalid())?,
            "--connections" => config.connections = value.parse().map_err(|_| invalid())?,
            "--market-data-depth" => {
                config.market_data_depth = value.parse().map_err(|_| invalid())?
            }
            "--market-data-ring-capacity" => {
                config.market_data_ring_capacity = value.parse().map_err(|_| invalid())?
            }
            "--market-data-connections" => {
                config.market_data_connections = value.parse().map_err(|_| invalid())?
            }
            "--websocket-write-timeout-ms" => {
                config.websocket_write_timeout =
                    std::time::Duration::from_millis(value.parse().map_err(|_| invalid())?)
            }
            "--websocket-close-timeout-ms" => {
                config.websocket_close_timeout =
                    std::time::Duration::from_millis(value.parse().map_err(|_| invalid())?)
            }
            "--websocket-ping-interval-ms" => {
                config.websocket_ping_interval =
                    std::time::Duration::from_millis(value.parse().map_err(|_| invalid())?)
            }
            "--websocket-pong-timeout-ms" => {
                config.websocket_pong_timeout =
                    std::time::Duration::from_millis(value.parse().map_err(|_| invalid())?)
            }
            "--websocket-write-buffer" => {
                config.websocket_write_buffer = value.parse().map_err(|_| invalid())?
            }
            "--websocket-inbound-limit" => {
                config.websocket_inbound_limit = value.parse().map_err(|_| invalid())?
            }
            _ => return Err(invalid()),
        }
    }
    let server = match Server::bind(config).await {
        Ok(server) => server,
        Err(error) => {
            tracing::error!(error = %error, "server startup failed");
            return Err(error);
        }
    };
    println!(
        "{}",
        serde_json::json!({
            "listening":server.address.to_string(),
            "market_data_config":server.handle.metrics().market_data_config,
        })
    );
    let handle = server.handle.clone();
    let wait = server.wait();
    tokio::pin!(wait);
    let metrics = tokio::select! {
        result = &mut wait => result?,
        result = shutdown_signal() => {
            result?;
            tracing::info!("shutdown signal received");
            handle.shutdown();
            wait.await?
        },
    };
    println!("{}", serde_json::to_string(&metrics)?);
    if metrics.abnormal_engine_exits != 0 {
        tracing::error!(
            abnormal_engine_exits = metrics.abnormal_engine_exits,
            "server stopped after abnormal engine exit"
        );
        return Err(io::Error::other(
            "engine failed; admitted outcomes may be uncertain",
        ));
    }
    tracing::info!(
        requests = metrics.requests.values().sum::<u64>(),
        connection_errors = metrics.connection_errors,
        "server stopped"
    );
    Ok(())
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,trade_http=info"));
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .with_span_list(false)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()
        .ok();
}
async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = term.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
