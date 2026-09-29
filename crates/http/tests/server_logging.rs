#![cfg(unix)]

use serde_json::Value;
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Output, Stdio},
    thread,
    time::Duration,
};

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn start_server(level: &str) -> (Child, TcpStream) {
    let port = unused_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_trade-http"))
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .env("RUST_LOG", level)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            return (child, stream);
        }
        thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    panic!("server did not listen on port {port}");
}

fn request(stream: &mut BufReader<TcpStream>) {
    stream
        .get_mut()
        .write_all(b"GET /book?symbol=BTC-USD HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    stream.get_mut().flush().unwrap();

    let mut status = String::new();
    stream.read_line(&mut status).unwrap();
    assert_eq!(status, "HTTP/1.1 200 OK\r\n");
    let mut content_length = None;
    loop {
        let mut header = String::new();
        stream.read_line(&mut header).unwrap();
        if header == "\r\n" {
            break;
        }
        if let Some(value) = header.strip_prefix("content-length: ") {
            content_length = Some(value.trim().parse::<usize>().unwrap());
        }
    }
    let mut body = vec![0; content_length.expect("response has content-length")];
    stream.read_exact(&mut body).unwrap();
    assert!(serde_json::from_slice::<Value>(&body).is_ok());
}

fn stop(child: Child) -> Output {
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    child.wait_with_output().unwrap()
}

fn logs(stderr: &[u8]) -> Vec<Value> {
    std::str::from_utf8(stderr)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn message(event: &Value) -> Option<&str> {
    event.pointer("/fields/message")?.as_str()
}

#[test]
fn info_logs_preserve_stdout_protocol_without_request_volume() {
    let (child, stream) = start_server("trade_http=info");
    // Reconnect to the server used by the process after it has started. The test's
    // request volume is deliberately larger than one so a per-request info event
    // would be observable in the captured stderr.
    let mut stream = BufReader::new(stream);
    for _ in 0..16 {
        request(&mut stream);
    }
    drop(stream);
    let output = stop(child);
    assert!(output.status.success());

    let stdout: Vec<Value> = std::str::from_utf8(&output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(stdout.len(), 2);
    assert!(stdout[0].get("listening").is_some());
    assert!(stdout[1].get("commands_completed").is_some());

    let events = logs(&output.stderr);
    assert!(
        events
            .iter()
            .any(|event| message(event) == Some("server listening"))
    );
    assert!(
        events
            .iter()
            .any(|event| message(event) == Some("server stopped"))
    );
    assert!(
        !events
            .iter()
            .any(|event| message(event) == Some("request completed"))
    );
}

#[test]
fn debug_enables_bounded_request_completion_details() {
    let (child, stream) = start_server("trade_http=debug");
    let mut stream = BufReader::new(stream);
    request(&mut stream);
    drop(stream);
    let output = stop(child);
    assert!(output.status.success());

    let event = logs(&output.stderr)
        .into_iter()
        .find(|event| message(event) == Some("request completed"))
        .expect("debug request completion event");
    assert_eq!(
        event.pointer("/fields/route").and_then(Value::as_str),
        Some("GET /book")
    );
    assert_eq!(
        event.pointer("/fields/status").and_then(Value::as_u64),
        Some(200)
    );
    assert_eq!(
        event.pointer("/fields/overload").and_then(Value::as_bool),
        Some(false)
    );
}

#[test]
fn trace_includes_websocket_disconnect_reason() {
    let (child, mut stream) = start_server("trade_http=trace");
    stream
        .write_all(
            b"GET /market-data?symbol=BTC-USD HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: trade-rs.market-data.v1\r\n\r\n",
        )
        .unwrap();
    let mut response = BufReader::new(stream);
    let mut status = String::new();
    response.read_line(&mut status).unwrap();
    assert_eq!(status, "HTTP/1.1 101 Switching Protocols\r\n");
    loop {
        let mut header = String::new();
        response.read_line(&mut header).unwrap();
        if header == "\r\n" {
            break;
        }
    }
    drop(response);
    thread::sleep(Duration::from_millis(100));

    let output = stop(child);
    assert!(output.status.success());
    let event = logs(&output.stderr)
        .into_iter()
        .find(|event| message(event) == Some("market-data client disconnected"))
        .expect("trace websocket disconnect event");
    assert_eq!(event.get("level").and_then(Value::as_str), Some("TRACE"));
    assert!(matches!(
        event.pointer("/fields/reason").and_then(Value::as_str),
        Some("peer_closed" | "peer_io")
    ));
}

#[test]
fn startup_failure_is_an_error_log() {
    let output = Command::new(env!("CARGO_BIN_EXE_trade-http"))
        .args(["--market-data-ring-capacity", "63"])
        .env("RUST_LOG", "trade_http=info")
        .output()
        .unwrap();
    assert!(!output.status.success());

    let event = std::str::from_utf8(&output.stderr)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|event| message(event) == Some("server startup failed"))
        .expect("startup error event");
    assert!(
        event
            .pointer("/fields/error")
            .and_then(Value::as_str)
            .is_some_and(|error| error.contains("ring capacity"))
    );
}
