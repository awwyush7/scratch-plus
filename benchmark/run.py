#!/usr/bin/env python3
"""Phase 5 benchmark — Sequential HTTP/1.1 vs Classic Pipelining vs Pipelining+.

Two servers are started:
  http_server   — plain HTTPServer (serial, in-order responses); used by
                  Sequential and Classic-pipeline modes.
  pp_server     — PipeliningPlusServer (concurrent, out-of-order); used by
                  Pipelining+ mode.

Workloads
---------
W1  all-fast         8× GET /fast
W2  slow-then-fast   1× GET /slow?ms=500  + 7× GET /fast
W3  mixed-delay      /slow?ms=100, /fast, /slow?ms=200, /fast, /slow?ms=300, /fast, /slow?ms=50, /fast
W4  large-then-small 1× GET /large (64 KB body) + 7× GET /small

Metrics
-------
  Total time, per-request latency, p50, p95, connections used,
  bytes sent, bytes received.

Run from the project root:
    python benchmark/run.py
"""

import json
import socket
import statistics
import sys
import threading
import time

sys.path.insert(0, ".")

from scratchplus.http import HTTPResponse, HTTPServer, json_response
from scratchplus.pipelining_plus import PipeliningPlusServer

# ---------------------------------------------------------------------------
# Routes (registered on both server types)
# ---------------------------------------------------------------------------

_LARGE_BODY = b"A" * 65_536  # 64 KB
_SMALL_BODY = json.dumps({"route": "small"}).encode()


def _register_routes_http(server: HTTPServer) -> None:
    @server.route("GET", "/fast")
    def fast(req):
        return json_response({"route": "fast"})

    @server.route("GET", "/slow")
    def slow(req):
        ms = int(req.query_params.get("ms", "500"))
        time.sleep(ms / 1000.0)
        return json_response({"route": "slow", "ms": ms})

    @server.route("GET", "/large")
    def large(req):
        return HTTPResponse(200, {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(_LARGE_BODY)),
            "Connection": "keep-alive",
        }, _LARGE_BODY)

    @server.route("GET", "/small")
    def small(req):
        return HTTPResponse(200, {
            "Content-Type": "application/json",
            "Content-Length": str(len(_SMALL_BODY)),
            "Connection": "keep-alive",
        }, _SMALL_BODY)


def _register_routes_pp(server: PipeliningPlusServer) -> None:
    @server.route("GET", "/fast")
    def fast(req):
        return json_response({"route": "fast"})

    @server.route("GET", "/slow")
    def slow(req):
        ms = int(req.query_params.get("ms", "500"))
        time.sleep(ms / 1000.0)
        return json_response({"route": "slow", "ms": ms})

    @server.route("GET", "/large")
    def large(req):
        return HTTPResponse(200, {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(_LARGE_BODY)),
            "Connection": "keep-alive",
        }, _LARGE_BODY)

    @server.route("GET", "/small")
    def small(req):
        return HTTPResponse(200, {
            "Content-Type": "application/json",
            "Content-Length": str(len(_SMALL_BODY)),
            "Connection": "keep-alive",
        }, _SMALL_BODY)


# ---------------------------------------------------------------------------
# Counting socket wrapper
# ---------------------------------------------------------------------------

class _CountingSocket:
    """Raw socket that accumulates bytes_sent / bytes_recv."""

    def __init__(self, host: str, port: int, timeout: float = 15.0) -> None:
        s = socket.create_connection((host, port), timeout=timeout)
        s.settimeout(timeout)
        self._s = s
        self.bytes_sent = 0
        self.bytes_recv = 0

    def sendall(self, data: bytes) -> None:
        self.bytes_sent += len(data)
        self._s.sendall(data)

    def recv(self, n: int) -> bytes:
        data = self._s.recv(n)
        self.bytes_recv += len(data)
        return data

    def close(self) -> None:
        self._s.close()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _make_request(method: str, path: str, req_id: str | None = None,
                  keep_alive: bool = True) -> bytes:
    headers = [
        f"{method} {path} HTTP/1.1",
        "Host: 127.0.0.1",
        f"Connection: {'keep-alive' if keep_alive else 'close'}",
        "Content-Length: 0",
    ]
    if req_id is not None:
        headers.append(f"Request-ID: {req_id}")
    return ("\r\n".join(headers) + "\r\n\r\n").encode()


def _recv_one(sock: _CountingSocket, buf: bytearray) -> tuple[int, str, bytes]:
    """Read one HTTP response from sock into buf. Returns (status, req_id, body)."""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("unexpected EOF reading headers")
        buf.extend(chunk)

    idx = buf.index(b"\r\n\r\n")
    header_block = bytes(buf[:idx]).decode()
    del buf[:idx + 4]

    lines = header_block.split("\r\n")
    status = int(lines[0].split(" ", 2)[1])
    hdrs: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            k, _, v = line.partition(":")
            hdrs[k.strip().lower()] = v.strip()

    clen = int(hdrs.get("content-length", 0))
    while len(buf) < clen:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("unexpected EOF reading body")
        buf.extend(chunk)

    body = bytes(buf[:clen])
    del buf[:clen]
    return status, hdrs.get("request-id", ""), body


# ---------------------------------------------------------------------------
# BenchResult
# ---------------------------------------------------------------------------

class BenchResult:
    def __init__(self, latencies: list[float], connections: int,
                 bytes_sent: int, bytes_recv: int) -> None:
        self.latencies = latencies
        self.total_s = max(latencies)
        self.connections = connections
        self.bytes_sent = bytes_sent
        self.bytes_recv = bytes_recv

    @property
    def p50(self) -> float:
        return statistics.median(self.latencies)

    @property
    def p95(self) -> float:
        s = sorted(self.latencies)
        return s[max(0, int(len(s) * 0.95) - 1)]


# ---------------------------------------------------------------------------
# Client modes
# ---------------------------------------------------------------------------

def run_sequential(host: str, port: int, requests: list[tuple[str, str]]) -> BenchResult:
    """One request per connection; no pipelining. Uses plain HTTPServer."""
    latencies: list[float] = []
    total_sent = 0
    total_recv = 0

    for method, path in requests:
        sock = _CountingSocket(host, port)
        raw = _make_request(method, path, keep_alive=False)
        t0 = time.monotonic()
        sock.sendall(raw)
        buf = bytearray()
        _recv_one(sock, buf)
        latencies.append(time.monotonic() - t0)
        total_sent += sock.bytes_sent
        total_recv += sock.bytes_recv
        sock.close()

    return BenchResult(latencies, len(requests), total_sent, total_recv)


def run_classic_pipeline(host: str, port: int, requests: list[tuple[str, str]]) -> BenchResult:
    """Send all on one keep-alive connection; receive IN ORDER (HOL blocking).
    Uses plain HTTPServer — responses are always serialized on the server side,
    so a slow response blocks every subsequent one on the wire."""
    sock = _CountingSocket(host, port)
    t0 = time.monotonic()

    for method, path in requests:
        sock.sendall(_make_request(method, path, keep_alive=True))

    buf = bytearray()
    latencies: list[float] = []
    for _ in requests:
        _recv_one(sock, buf)
        latencies.append(time.monotonic() - t0)

    result = BenchResult(latencies, 1, sock.bytes_sent, sock.bytes_recv)
    sock.close()
    return result


def run_pipelining_plus(host: str, port: int, requests: list[tuple[str, str]]) -> BenchResult:
    """Send all with Request-IDs; receive out-of-order. Uses PipeliningPlusServer."""
    sock = _CountingSocket(host, port)
    t0 = time.monotonic()
    send_times: dict[str, float] = {}

    for i, (method, path) in enumerate(requests):
        req_id = f"req-{i + 1}"
        send_times[req_id] = time.monotonic() - t0
        sock.sendall(_make_request(method, path, req_id=req_id, keep_alive=True))

    buf = bytearray()
    latencies: list[float] = []
    for _ in requests:
        _, req_id, _ = _recv_one(sock, buf)
        recv_time = time.monotonic() - t0
        latencies.append(recv_time - send_times.get(req_id, 0.0))

    result = BenchResult(latencies, 1, sock.bytes_sent, sock.bytes_recv)
    sock.close()
    return result


# ---------------------------------------------------------------------------
# Workloads
# ---------------------------------------------------------------------------

WORKLOADS: list[tuple[str, list[tuple[str, str]]]] = [
    (
        "W1  all-fast",
        [("GET", "/fast")] * 8,
    ),
    (
        "W2  slow-then-fast",
        [("GET", "/slow?ms=500")] + [("GET", "/fast")] * 7,
    ),
    (
        "W3  mixed-delay",
        [
            ("GET", "/slow?ms=100"), ("GET", "/fast"),
            ("GET", "/slow?ms=200"), ("GET", "/fast"),
            ("GET", "/slow?ms=300"), ("GET", "/fast"),
            ("GET", "/slow?ms=50"),  ("GET", "/fast"),
        ],
    ),
    (
        "W4  large-then-small",
        [("GET", "/large")] + [("GET", "/small")] * 7,
    ),
]


# ---------------------------------------------------------------------------
# HOL-blocking demonstration
# ---------------------------------------------------------------------------

def hol_demo(http_host: str, http_port: int,
             pp_host: str, pp_port: int) -> None:
    """Concrete proof: same workload, two servers, two response orderings."""
    reqs = [("GET", "/slow?ms=500")] + [("GET", "/fast")] * 3

    print("=" * 68)
    print("HOL-BLOCKING DEMONSTRATION")
    print("  Workload: 1× slow (500 ms) + 3× fast, single TCP connection")
    print()

    # Classic pipeline — HTTPServer, serial per-connection processing
    sock = _CountingSocket(http_host, http_port)
    t0 = time.monotonic()
    for m, p in reqs:
        sock.sendall(_make_request(m, p, keep_alive=True))
    buf = bytearray()
    classic_times: list[float] = []
    for _ in reqs:
        _recv_one(sock, buf)
        classic_times.append(time.monotonic() - t0)
    sock.close()

    labels = ["slow"] + ["fast"] * 3
    print("  Classic pipeline (HTTPServer — in-order, serial):")
    for i, (t, lbl) in enumerate(zip(classic_times, labels)):
        print(f"    req-{i+1} ({lbl:<4}) done at +{t * 1000:6.1f} ms")

    print()

    # Pipelining+ — PipeliningPlusServer, concurrent per-request threads
    sock = _CountingSocket(pp_host, pp_port)
    t0 = time.monotonic()
    ids: list[str] = []
    for i, (m, p) in enumerate(reqs):
        rid = f"req-{i + 1}"
        ids.append(rid)
        sock.sendall(_make_request(m, p, req_id=rid, keep_alive=True))
    buf = bytearray()
    plus_times: dict[str, float] = {}
    plus_order: list[str] = []
    for _ in reqs:
        _, rid, _ = _recv_one(sock, buf)
        plus_times[rid] = time.monotonic() - t0
        plus_order.append(rid)
    sock.close()

    print("  Pipelining+ (PipeliningPlusServer — concurrent, out-of-order):")
    for i, rid in enumerate(ids):
        lbl = labels[i]
        t = plus_times.get(rid, 0.0)
        print(f"    {rid} ({lbl:<4}) done at +{t * 1000:6.1f} ms")

    classic_fast_first = classic_times[1]
    plus_fast_first = min(plus_times[rid] for rid in ids[1:])
    penalty_ms = (classic_fast_first - plus_fast_first) * 1000
    print()
    arrival = " → ".join(plus_order)
    print(f"  Pipelining+ arrival order : {arrival}")
    print(f"  HOL penalty on first fast : {penalty_ms:.0f} ms")
    print(f"  (classic: fast requests wait for slow to finish;")
    print(f"   pipelining+: fast responses fly past slow)")
    print()


# ---------------------------------------------------------------------------
# Benchmark loop
# ---------------------------------------------------------------------------

_RUNS = 3


def _median_run(results: list[BenchResult]) -> BenchResult:
    totals = [r.total_s for r in results]
    med = statistics.median(totals)
    return min(results, key=lambda r: abs(r.total_s - med))


def run_all(http_host: str, http_port: int,
            pp_host: str, pp_port: int) -> dict:
    modes = [
        ("Sequential",       lambda h, p, rq: run_sequential(http_host, http_port, rq)),
        ("Classic pipeline", lambda h, p, rq: run_classic_pipeline(http_host, http_port, rq)),
        ("Pipelining+",      lambda h, p, rq: run_pipelining_plus(pp_host, pp_port, rq)),
    ]

    all_results: dict = {}
    for wl_name, wl_reqs in WORKLOADS:
        all_results[wl_name] = {}
        for mode_name, mode_fn in modes:
            mode_fn(None, None, wl_reqs)  # warm-up (discarded)
            runs = [mode_fn(None, None, wl_reqs) for _ in range(_RUNS)]
            all_results[wl_name][mode_name] = _median_run(runs)

    return all_results


def print_results(all_results: dict) -> None:
    cols = ["Sequential", "Classic pipeline", "Pipelining+"]

    print("=" * 68)
    print("BENCHMARK RESULTS")
    print("=" * 68)

    for wl_name, wl_data in all_results.items():
        print()
        print(f"Workload: {wl_name}")
        print("-" * 68)
        hdr = f"{'Metric':<26}" + "".join(f"{c:>14}" for c in cols)
        print(hdr)
        print("-" * 68)

        def row(label: str, getter) -> None:
            vals = [getter(wl_data[c]) for c in cols]
            print(f"  {label:<24}" + "".join(f"{v:>14}" for v in vals))

        row("Total time (ms)",  lambda r: f"{r.total_s * 1000:.1f}")
        row("p50 latency (ms)", lambda r: f"{r.p50 * 1000:.1f}")
        row("p95 latency (ms)", lambda r: f"{r.p95 * 1000:.1f}")
        row("Connections used", lambda r: str(r.connections))
        row("Bytes sent",       lambda r: str(r.bytes_sent))
        row("Bytes received",   lambda r: str(r.bytes_recv))
        print("-" * 68)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Plain HTTP/1.1 server — serial, in-order responses per connection
    http_server = HTTPServer(port=0, conn_timeout=60.0)
    _register_routes_http(http_server)
    http_server.start()
    threading.Thread(target=http_server.serve_forever, daemon=True).start()

    # Pipelining+ server — concurrent, out-of-order responses
    pp_server = PipeliningPlusServer(port=0, conn_timeout=60.0)
    _register_routes_pp(pp_server)
    pp_server.start()
    threading.Thread(target=pp_server.serve_forever, daemon=True).start()

    http_host, http_port = "127.0.0.1", http_server.port
    pp_host, pp_port = "127.0.0.1", pp_server.port

    print(f"HTTPServer        on 127.0.0.1:{http_port}  (sequential + classic pipeline)")
    print(f"PipeliningPlus    on 127.0.0.1:{pp_port}  (pipelining+)")
    print()

    try:
        hol_demo(http_host, http_port, pp_host, pp_port)
        all_results = run_all(http_host, http_port, pp_host, pp_port)
        print_results(all_results)
    finally:
        http_server.stop()
        pp_server.stop()

    print()
    print("Done.")


if __name__ == "__main__":
    main()
