#!/usr/bin/env python3
"""M4 loopback HTTP-plus-WebSocket baseline harness; Python standard library only.

It runs the M3 deterministic order fixture while two promptly-reading M4 clients
subscribe to each of four symbols.  The generator is deliberately bounded: an
offer with no free HTTP/1.1 connection is recorded as generator_capacity rather
than queued.  Every HTTP offer and every decoded WebSocket text frame is retained.
"""
import argparse
import asyncio
import base64
import csv
from collections import deque
import gzip
import hashlib
import json
import math
import os
import platform
import signal
import struct
import subprocess
import time
import uuid
from pathlib import Path

VERSION = "m4-http-ws-v2"
SYMBOLS = ["A", "B", "C", "D"]
PROTOCOL = "trade-rs.market-data.v1"
MARKET_DATA_DEPTH = 20


def command(symbol, order_id, kind="limit", quantity=1, price=0, owner=3):
    value = {"type": kind, "symbol": symbol, "order_id": order_id,
             "owner_id": owner, "side": "buy" if kind == "limit" else "sell",
             "quantity": quantity}
    if kind == "limit":
        value["price_ticks"] = price
    return value


def quantiles(values):
    values = sorted(values)
    if not values:
        return None
    return {**{f"p{p}": values[max(0, math.ceil(len(values) * p / 100) - 1)]
                for p in (50, 90, 95, 99)}, "max": values[-1]}


class HttpConnection:
    async def open(self, port):
        self.reader, self.writer = await asyncio.open_connection("127.0.0.1", port)
        self.port = port
        return self

    async def request(self, method, path, body=None):
        encoded = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
        request = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n"
                   f"Content-Type: application/json\r\nContent-Length: {len(encoded)}\r\n\r\n").encode()
        self.writer.write(request + encoded)
        await self.writer.drain()
        headers = await self.reader.readuntil(b"\r\n\r\n")
        lines = headers.decode().split("\r\n")
        fields = dict(line.lower().split(":", 1) for line in lines[1:] if ":" in line)
        body = await self.reader.readexactly(int(fields["content-length"]))
        return int(lines[0].split()[1]), json.loads(body), len(headers) + len(body)

    async def close(self):
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass


class Subscriber:
    def __init__(self, name, symbol, output, origin, warmup, duration, expected_projection):
        self.name, self.symbol, self.origin = name, symbol, origin
        self.warmup, self.duration = warmup, duration
        self.expected_projection = expected_projection
        self.path = output / f"{name}-frames.jsonl.gz"
        self.initial = asyncio.Event()
        self.ready = asyncio.Event()
        self.closed_by_harness = False
        self.failure = None
        self.close_code = None
        self.close_reason = None
        self.previous = None
        self.initial_sequence = None
        self.live_frames = 0
        self.measurement_frames = 0
        self.first_live_sequence = None
        self.last_live_sequence = None
        self.stream_id = None
        self._gzip = None
        self.task = None

    async def open(self, port):
        self.reader, self.writer = await asyncio.open_connection("127.0.0.1", port)
        key = base64.b64encode(os.urandom(16)).decode()
        request = (f"GET /market-data?symbol={self.symbol} HTTP/1.1\r\nHost: localhost\r\n"
                   "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
                   f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Protocol: {PROTOCOL}\r\n\r\n")
        self.writer.write(request.encode())
        await self.writer.drain()
        headers = (await self.reader.readuntil(b"\r\n\r\n")).decode().lower()
        if not headers.startswith("http/1.1 101") or f"sec-websocket-protocol: {PROTOCOL}" not in headers:
            raise RuntimeError(f"{self.name}: websocket upgrade failed: {headers!r}")
        self._gzip = gzip.open(self.path, "wt")
        self.task = asyncio.create_task(self._read())
        await asyncio.wait_for(self.initial.wait(), 10)

    async def _frame(self):
        first, second = await self.reader.readexactly(2)
        opcode, masked, length = first & 0x0f, second & 0x80, second & 0x7f
        if length == 126:
            length = struct.unpack("!H", await self.reader.readexactly(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", await self.reader.readexactly(8))[0]
        mask = await self.reader.readexactly(4) if masked else None
        payload = await self.reader.readexactly(length)
        if mask:
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        return opcode, payload

    async def _send(self, opcode, payload=b""):
        mask = os.urandom(4)
        size = len(payload)
        header = bytes([0x80 | opcode])
        if size < 126:
            header += bytes([0x80 | size])
        elif size < 65536:
            header += bytes([0x80 | 126]) + struct.pack("!H", size)
        else:
            header += bytes([0x80 | 127]) + struct.pack("!Q", size)
        encoded = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.writer.write(header + mask + encoded)
        await self.writer.drain()

    async def _read(self):
        try:
            while True:
                opcode, payload = await self._frame()
                if opcode == 1:
                    record = json.loads(payload)
                    now = time.perf_counter() - self.origin
                    sequence = record["sequence"]
                    if self.previous is None:
                        if (type(record.get("version")) is not int or record["version"] != 1 or
                                record.get("type") != "l2_snapshot" or
                                record.get("reason") != "initial" or record.get("symbol") != self.symbol or
                                type(record.get("depth")) is not int or record["depth"] != MARKET_DATA_DEPTH):
                            raise RuntimeError("first frame was not an initial L2 snapshot")
                        if type(sequence) is not int or sequence < 0:
                            raise RuntimeError("initial snapshot had an invalid sequence")
                        try:
                            stream_id = record["stream_id"]
                            if not isinstance(stream_id, str) or str(uuid.UUID(stream_id)) != stream_id:
                                raise ValueError
                        except (KeyError, ValueError, AttributeError):
                            raise RuntimeError("initial snapshot had an invalid stream ID") from None
                        projection = {"bids": record.get("bids"), "asks": record.get("asks")}
                        if projection != self.expected_projection:
                            raise RuntimeError("initial snapshot did not match the pre-load book projection")
                        self.initial_sequence = sequence
                        self.stream_id = stream_id
                        self.previous = sequence
                        self.initial.set()
                    else:
                        if (type(record.get("version")) is not int or record["version"] != 1 or
                                type(sequence) is not int or sequence < 0 or
                                record.get("type") not in {"trade", "bbo", "l2_snapshot"}):
                            raise RuntimeError("live frame had invalid protocol fields")
                        if record.get("stream_id") != self.stream_id or record.get("symbol") != self.symbol:
                            raise RuntimeError("stream or symbol changed")
                        if sequence != self.previous + 1:
                            raise RuntimeError(f"noncontiguous sequence {self.previous} -> {sequence}")
                        self.previous = sequence
                        self.live_frames += 1
                        self.first_live_sequence = self.first_live_sequence or sequence
                        self.last_live_sequence = sequence
                        if self.warmup <= now < self.warmup + self.duration:
                            self.measurement_frames += 1
                    self._gzip.write(json.dumps({"received_seconds": now, "frame": record}, separators=(",", ":")) + "\n")
                elif opcode == 8:
                    self.close_code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None
                    self.close_reason = payload[2:].decode("utf-8", "replace") if len(payload) >= 2 else ""
                    if not self.closed_by_harness:
                        raise RuntimeError(f"server close {self.close_code} {self.close_reason!r}")
                    return
                elif opcode == 9:
                    await self._send(10, payload)
                elif opcode not in (10,):
                    raise RuntimeError(f"unexpected WebSocket opcode {opcode}")
        except asyncio.IncompleteReadError:
            if not self.closed_by_harness:
                self.failure = "socket EOF before harness close"
        except Exception as error:
            self.failure = str(error)
        finally:
            self.initial.set()
            self.ready.set()
            if self._gzip:
                self._gzip.close()

    async def close(self):
        self.closed_by_harness = True
        try:
            await self._send(8, struct.pack("!H", 1000))
            await asyncio.wait_for(self.ready.wait(), 5)
        except (OSError, asyncio.TimeoutError):
            pass
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass
        if self.task:
            await self.task

    def summary(self):
        return {"name": self.name, "symbol": self.symbol, "initial_sequence": self.initial_sequence,
                "first_live_sequence": self.first_live_sequence, "last_live_sequence": self.last_live_sequence,
                "live_frames": self.live_frames, "measurement_frames": self.measurement_frames,
                "measurement_frame_rate": self.measurement_frames / self.duration, "stream_id": self.stream_id,
                "close_code": self.close_code, "close_reason": self.close_reason, "failure": self.failure,
                "raw_frames": self.path.name}


def aggregate(samples, seconds):
    completed = [sample for sample in samples if sample["outcome"] == "response"]
    counts = {}
    for sample in samples:
        key = f"{sample['outcome']}:{sample['status']}:{sample['code']}"
        counts[key] = counts.get(key, 0) + 1
    return {"offered": len(samples), "dispatched": sum(s["dispatch"] is not None for s in samples),
            "completed": len(completed), "http_200": sum(s["status"] == 200 for s in samples),
            "http_503": sum(s["status"] == 503 for s in samples),
            "domain_errors": sum(s["status"] in (404, 409, 422) for s in samples),
            "timeouts": sum(s["outcome"] == "timeout" for s in samples),
            "connection_errors": sum(s["outcome"] == "connection_error" for s in samples),
            "generator_capacity": sum(s["outcome"] == "generator_capacity" for s in samples),
            "counts": counts,
            "http_wall_ms": quantiles([(s["completion"] - s["dispatch"]) * 1000 for s in completed]),
            "offered_response_ms": quantiles([(s["completion"] - s["scheduled"]) * 1000 for s in completed]),
            "scheduling_lag_ms": quantiles([(s["dispatch"] - s["scheduled"]) * 1000 for s in samples if s["dispatch"] is not None]),
            "missing_fraction": 1 - len(completed) / len(samples),
            "successful_rps": sum(s["status"] == 200 for s in samples) / seconds,
            "completed_rps": len(completed) / seconds}


def records_per_symbol(count):
    return sum((1, 1, 3, 1)[index % 4] for index in range(count))


def l2_projection(book):
    def levels(side):
        return [{"price_ticks": level["price_ticks"],
                 "quantity": str(sum(order["remaining"] for order in level["orders"]))}
                for level in side[:MARKET_DATA_DEPTH]]
    return {"bids": levels(book["bids"]), "asks": levels(book["asks"])}


async def run(args):
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    binary = str(Path(args.server).resolve())
    invocation = [Path(args.server).name, "--listen", "127.0.0.1:0", "--symbols", ",".join(SYMBOLS),
                  "--queue-capacity", str(args.queue), "--in-flight", str(args.in_flight),
                  "--connections", "1024", "--body-limit", "16384", "--market-data-ring-capacity", "1024",
                  "--market-data-connections", "128"]
    process = await asyncio.create_subprocess_exec(binary, *invocation[1:], stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)
    clients, subscribers = [], []
    try:
        ready = json.loads(await process.stdout.readline())
        port = int(ready["listening"].split(":")[-1])
        clients = await asyncio.gather(*(HttpConnection().open(port) for _ in range(args.clients)))
        offers = int(args.rate * (args.warmup + args.duration))
        pool = math.ceil(math.ceil(offers / 16) * 1.1)
        preload = [(symbol, index + 1, 100) for symbol in SYMBOLS for index in range(pool)]
        preload += [(symbol, 1_000_000 + index, -100) for symbol in SYMBOLS for index in range(pool)]
        async def preload_worker(client, offset):
            for symbol, order_id, tick in preload[offset::len(clients)]:
                status, body, _ = await client.request("POST", "/orders", command(symbol, order_id, price=tick, owner=1))
                assert status == 200, (status, body)
        await asyncio.gather(*(preload_worker(client, index) for index, client in enumerate(clients)))
        async def snapshots(name):
            values, control = {}, await HttpConnection().open(port)
            try:
                for symbol in SYMBOLS:
                    status, body, _ = await control.request("GET", f"/book?symbol={symbol}")
                    assert status == 200
                    values[symbol] = body
            finally:
                await control.close()
            with gzip.open(output / f"{name}.json.gz", "wt") as file:
                json.dump(values, file, separators=(",", ":"))
            return values
        before = await snapshots("preload")
        for symbol in SYMBOLS:
            assert sum(len(level["orders"]) for level in before[symbol]["bids"]) == pool * 2
        origin = time.perf_counter()
        subscribers = [Subscriber(f"subscriber-{index + 1:02d}", symbol, output, origin, args.warmup,
                                  args.duration, l2_projection(before[symbol]))
                       for index, symbol in enumerate(["A", "A", "B", "B", "C", "C", "D", "D"])]
        await asyncio.gather(*(subscriber.open(port) for subscriber in subscribers))
        free, samples, pending, cancels = deque(clients), [], set(), [0] * 4
        warm_count = int(args.rate * args.warmup)
        async def dispatch(client, sample, method, path, body):
            try:
                status, result, nbytes = await asyncio.wait_for(client.request(method, path, body), args.timeout)
                sample.update(status=status, code=result.get("error", {}).get("code", ""), bytes=nbytes, outcome="response")
            except asyncio.TimeoutError:
                sample["outcome"] = "timeout"
                await client.close(); await client.open(port)
            except (OSError, asyncio.IncompleteReadError, ValueError, KeyError) as error:
                sample.update(outcome="connection_error", code=type(error).__name__)
                await client.close(); await client.open(port)
            finally:
                sample["completion"] = time.perf_counter() - origin
                free.append(client)
        for index in range(offers):
            scheduled = index / args.rate
            delay = origin + scheduled - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            symbol = SYMBOLS[index % 4]
            category = ("limit", "limit", "market", "delete")[(index // 4) % 4]
            sample = {"index": index, "phase": "warmup" if index < warm_count else "measurement",
                      "scheduled": scheduled, "dispatch": None, "completion": None, "category": category,
                      "endpoint": "DELETE" if category == "delete" else "POST", "status": 0,
                      "code": "", "bytes": 0, "outcome": "generator_capacity"}
            samples.append(sample)
            if category == "delete":
                order_id = 1_000_000 + cancels[index % 4]; cancels[index % 4] += 1
                method, path, body = "DELETE", f"/orders/{order_id}?symbol={symbol}", None
            else:
                order_id = (2_000_000 if category == "limit" else 3_000_000) + index
                method, path, body = "POST", "/orders", command(symbol, order_id, kind="limit" if category == "limit" else "market")
            if not free:
                continue
            client = free.popleft(); sample["dispatch"] = time.perf_counter() - origin
            task = asyncio.create_task(dispatch(client, sample, method, path, body))
            pending.add(task); task.add_done_callback(pending.discard)
        await asyncio.gather(*pending)
        drained_seconds = time.perf_counter() - origin
        expected_live = {symbol: records_per_symbol(sum(index % len(SYMBOLS) == position
                                                         for index in range(offers)))
                         for position, symbol in enumerate(SYMBOLS)}
        await asyncio.wait_for(asyncio.gather(*(wait_for_records(s, expected_live[s.symbol]) for s in subscribers)), 15)
        after = await snapshots("final")
        with gzip.open(output / "samples.csv.gz", "wt", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(samples[0])); writer.writeheader(); writer.writerows(samples)
        await asyncio.gather(*(subscriber.close() for subscriber in subscribers))
        await asyncio.gather(*(client.close() for client in clients)); clients = []
        process.send_signal(signal.SIGTERM)
        stdout, stderr = await process.communicate()
        (output / "server.stderr.txt").write_bytes(stderr)
        (output / "server.stdout.txt").write_bytes(json.dumps(ready).encode() + b"\n" + stdout)
        assert process.returncode == 0, stderr
        metrics = json.loads(stdout.splitlines()[-1])
        measured = [sample for sample in samples if sample["phase"] == "measurement"]
        summary = aggregate(measured, args.duration)
        warmup = aggregate(samples[:warm_count], args.warmup) if warm_count else None
        subscribers_summary = [subscriber.summary() for subscriber in subscribers]
        git_worktree = "dirty" if subprocess.check_output(
            ["git", "status", "--porcelain"], text=True
        ).strip() else "clean"
        summary.update({"version": VERSION, "release_profile": "release", "server": metrics,
                        "websocket": {"healthy_subscribers": len(subscribers), "expected_live_frames_per_symbol": expected_live,
                                      "subscribers": subscribers_summary,
                                      "resync_closes": sum(s["close_code"] == 1013 or s["close_reason"] == "resync_required" for s in subscribers_summary),
                                      "measurement_disconnects": sum(s["failure"] is not None for s in subscribers_summary),
                                      "post_measurement_client_closes": len(subscribers)},
                        "warmup": warmup, "drained_seconds": drained_seconds,
                        "final_orders": {symbol: sum(len(level["orders"]) for level in after[symbol]["bids"] + after[symbol]["asks"]) for symbol in SYMBOLS},
                        "reconciliation": {"commands_admitted_minus_completed": metrics["commands_admitted"] - metrics["commands_completed"],
                                             "preload_commands": len(preload), "load_admitted": metrics["commands_admitted"] - len(preload),
                                             "load_engine_completed": metrics["commands_completed"] - len(preload),
                                             "load_http_200": sum(sample["status"] == 200 for sample in samples)},
                        "configuration": {"rate": args.rate, "warmup": args.warmup, "duration": args.duration,
                                          "clients": args.clients, "timeout": args.timeout, "queue": args.queue,
                                          "in_flight": args.in_flight, "invocation": invocation,
                                          "pool_per_symbol": pool, "subscribers": "two healthy readers per A,B,C,D",
                                          "version": VERSION},
                        "environment": {"python": platform.python_version(), "os": platform.platform(), "logical_cpus": os.cpu_count(),
                                        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                                        "git_worktree": git_worktree,
                                        "rust": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                                        "binary_sha256": hashlib.file_digest(open(binary, "rb"), "sha256").hexdigest()},
                        "runtime_notes": {"placement": "loopback, generator and server colocated on one host", "affinity": "not pinned", "debug_request_logging": False,
                                          "power_thermal": "not controlled", "capacity_claim": "baseline evidence only; no maximum QPS established"}})
        summary["gate_demonstrated"] = (summary["offered"] == args.rate * args.duration and summary["dispatched"] == summary["offered"] and
            summary["completed"] == summary["offered"] and summary["http_200"] == summary["offered"] and summary["http_503"] == 0 and
            summary["domain_errors"] == 0 and summary["timeouts"] == 0 and summary["connection_errors"] == 0 and summary["generator_capacity"] == 0 and
            summary["http_wall_ms"]["p90"] <= 1000 and metrics["overload"] == 0 and summary["reconciliation"]["commands_admitted_minus_completed"] == 0 and
            len({s["stream_id"] for s in subscribers_summary}) == 1 and
            all(s["failure"] is None and s["live_frames"] == expected_live[s["symbol"]] and
                s["initial_sequence"] + expected_live[s["symbol"]] == s["last_live_sequence"] ==
                metrics["market_data_symbols"][s["symbol"]]["sequence"]
                for s in subscribers_summary))
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({"output": str(output), "gate_demonstrated": summary["gate_demonstrated"], "http_p90_ms": summary["http_wall_ms"]["p90"]}, indent=2), flush=True)
        assert summary["gate_demonstrated"], summary
    finally:
        await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)
        await asyncio.gather(*(subscriber.close() for subscriber in subscribers), return_exceptions=True)
        if process.returncode is None:
            process.send_signal(signal.SIGTERM)
            await process.communicate()


async def wait_for_records(subscriber, expected):
    while subscriber.live_frames < expected and subscriber.failure is None:
        await asyncio.sleep(.002)
    if subscriber.failure:
        raise RuntimeError(f"{subscriber.name}: {subscriber.failure}")
    if subscriber.live_frames != expected:
        raise RuntimeError(f"{subscriber.name}: expected {expected} live frames, got {subscriber.live_frames}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="target/release/trade-http")
    parser.add_argument("--output", required=True)
    parser.add_argument("--rate", type=int, default=500)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--duration", type=float, default=120)
    parser.add_argument("--clients", type=int, default=128)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--queue", type=int, default=256)
    parser.add_argument("--in-flight", type=int, default=512)
    args = parser.parse_args()
    if min(args.rate, args.duration, args.clients, args.timeout, args.queue, args.in_flight) <= 0 or args.warmup < 0:
        parser.error("positive values required; warmup may be zero")
    asyncio.run(run(args))
