use std::io;
use trade_http::{Config, Server};
use trade_orderbook::Symbol;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--help" {
            println!(
                "trade-http [--listen 127.0.0.1:8080] [--symbols BTC-USD,ETH-USD] [--queue-capacity 256] [--in-flight 512] [--body-limit 16384] [--connections 1024] [--market-data-depth 20] [--market-data-ring-capacity 1024] [--market-data-connections 128] [--websocket-write-timeout-ms 1000] [--websocket-close-timeout-ms 1000] [--websocket-ping-interval-ms 15000] [--websocket-pong-timeout-ms 30000] [--websocket-write-buffer 65536] [--websocket-inbound-limit 1024]\nMarket-data ring capacity must be a power of two and at least 64. SIGINT/SIGTERM drain admitted work. Final JSON counters are written to stdout."
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
    let server = Server::bind(config).await?;
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
        result = shutdown_signal() => { result?; handle.shutdown(); wait.await? },
    };
    println!("{}", serde_json::to_string(&metrics)?);
    if metrics.abnormal_engine_exits != 0 {
        return Err(io::Error::other(
            "engine failed; admitted outcomes may be uncertain",
        ));
    }
    Ok(())
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
