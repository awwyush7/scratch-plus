"""Pipelining+ / HTTP/1.15 — demo client and benchmark.

Starts an embedded server, then:
  1. Runs an out-of-order demo: one slow + one fast request on a single
     TCP connection; shows the fast response arriving before the slow one.
  2. Runs a benchmark comparing three modes over a workload of
     1 slow (500 ms) + 4 fast requests:

     Sequential   – one request at a time (classic HTTP/1.0 style)
     Classic pipe – send all, receive in order (HOL blocking)
     Pipelining+  – send all, receive by Request-ID as each finishes

Usage:
    python examples/pipelining_plus_client.py
"""

import json
import socket
import sys
import threading
import time

sys.path.insert(0, ".")

from examples.pipelining_plus_server import build_server


# ---------------------------------------------------------------------------
# Pipelining+ client
# ---------------------------------------------------------------------------

class PipeliningPlusClient:
    """Sends requests with Request-ID headers; receives responses in any order."""

    def __init__(self, host: str, port: int, timeout: float = 10.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._buf = bytearray()
        self._counter = 0

    def send(self, method: str, path: str, body: bytes = b"") -> str:
        self._counter += 1
        req_id = f"req-{self._counter}"
        raw = (
            f"{method} {path} HTTP/1.1\r\n"
            f"Host: 127.0.0.1\r\n"
            f"Connection: keep-alive\r\n"
            f"Request-ID: {req_id}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"\r\n"
        ).encode() + body
        self._sock.sendall(raw)
        return req_id

    def recv_one(self) -> tuple[int, str, bytes]:
        """Read one complete response. Returns (status, request_id, body)."""
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("unexpected EOF")
            self._buf.extend(chunk)

        idx = self._buf.index(b"\r\n\r\n")
        header_block = bytes(self._buf[:idx]).decode()
        del self._buf[: idx + 4]

        lines = header_block.split("\r\n")
        status = int(lines[0].split(" ", 2)[1])
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()

        clen = int(headers.get("content-length", 0))
        while len(self._buf) < clen:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("unexpected EOF in body")
            self._buf.extend(chunk)

        body = bytes(self._buf[:clen])
        del self._buf[:clen]
        return status, headers.get("request-id", ""), body

    def recv_all(self, n: int) -> dict[str, tuple[int, bytes]]:
        """Read n responses; return {request_id: (status, body)}."""
        return {rid: (s, b) for s, rid, b in (self.recv_one() for _ in range(n))}

    def close(self) -> None:
        self._sock.close()


# ---------------------------------------------------------------------------
# Classic HTTP/1.1 client helpers (for comparison benchmarks)
# ---------------------------------------------------------------------------

class _SequentialClient:
    """One request at a time; send → wait for full response → next."""

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port

    def run(self, requests: list[tuple[str, str]]) -> list[tuple[float, float]]:
        """Return list of (send_time, recv_time) for each request."""
        results = []
        t0 = time.monotonic()
        for method, path in requests:
            sock = socket.create_connection((self._host, self._port), timeout=10.0)
            sock.settimeout(10.0)
            raw = (
                f"{method} {path} HTTP/1.1\r\n"
                f"Host: 127.0.0.1\r\n"
                f"Connection: close\r\n"
                f"Content-Length: 0\r\n"
                f"\r\n"
            ).encode()
            t_send = time.monotonic() - t0
            sock.sendall(raw)
            buf = bytearray()
            while b"\r\n\r\n" not in buf:
                buf.extend(sock.recv(4096))
            idx = buf.index(b"\r\n\r\n")
            header_block = bytes(buf[:idx]).decode()
            del buf[: idx + 4]
            headers = {}
            for line in header_block.split("\r\n")[1:]:
                if ":" in line:
                    k, _, v = line.partition(":")
                    headers[k.strip().lower()] = v.strip()
            clen = int(headers.get("content-length", 0))
            while len(buf) < clen:
                buf.extend(sock.recv(4096))
            sock.close()
            t_recv = time.monotonic() - t0
            results.append((t_send, t_recv))
        return results


class _ClassicPipelineClient:
    """Send all requests, receive responses in order (HOL blocking demo)."""

    def run(self, host: str, port: int, requests: list[tuple[str, str]]) -> list[tuple[float, float]]:
        t0 = time.monotonic()
        sock = socket.create_connection((host, port), timeout=10.0)
        sock.settimeout(10.0)
        for method, path in requests:
            sock.sendall((
                f"{method} {path} HTTP/1.1\r\n"
                f"Host: 127.0.0.1\r\n"
                f"Connection: keep-alive\r\n"
                f"Content-Length: 0\r\n"
                f"\r\n"
            ).encode())
        results = []
        buf = bytearray()
        for _ in requests:
            while b"\r\n\r\n" not in buf:
                buf.extend(sock.recv(4096))
            idx = buf.index(b"\r\n\r\n")
            headers = {}
            for line in bytes(buf[:idx]).decode().split("\r\n")[1:]:
                if ":" in line:
                    k, _, v = line.partition(":")
                    headers[k.strip().lower()] = v.strip()
            del buf[: idx + 4]
            clen = int(headers.get("content-length", 0))
            while len(buf) < clen:
                buf.extend(sock.recv(4096))
            del buf[:clen]
            results.append((0.0, time.monotonic() - t0))
        sock.close()
        return results


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def run_demo(host: str, port: int) -> None:
    print("=" * 60)
    print("DEMO: out-of-order response delivery")
    print("  Req1  GET /slow?ms=400   (400 ms)")
    print("  Req2  GET /fast          (  0 ms)")
    print()
    print("With classic pipelining, Resp2 is blocked until Resp1 finishes.")
    print("With Pipelining+, Resp2 arrives ~immediately while slow is running.")
    print()

    client = PipeliningPlusClient(host, port)
    t0 = time.monotonic()

    slow_id = client.send("GET", "/slow?ms=400")
    fast_id = client.send("GET", "/fast")

    arrival_order: list[tuple[float, str, str]] = []
    for _ in range(2):
        status, req_id, body = client.recv_one()
        elapsed = time.monotonic() - t0
        route = json.loads(body)["route"]
        arrival_order.append((elapsed, req_id, route))
        print(f"  [+{elapsed*1000:6.1f} ms]  {req_id}  route={route}")

    client.close()

    first_rid = arrival_order[0][1]
    if first_rid == fast_id:
        print()
        print("  ✓ Fast response arrived FIRST — HOL blocking eliminated.")
    else:
        print()
        print("  (slow arrived first — recheck server concurrency)")
    print()


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

def run_benchmark(host: str, port: int) -> None:
    SLOW_MS = 500
    FAST_COUNT = 4
    requests = [("GET", f"/slow?ms={SLOW_MS}")] + [("GET", "/fast")] * FAST_COUNT
    total = len(requests)

    print("=" * 60)
    print(f"BENCHMARK: {total} requests  (1× slow@{SLOW_MS}ms + {FAST_COUNT}× fast)")
    print()

    # --- Sequential ---
    seq_client = _SequentialClient(host, port)
    seq_results = seq_client.run(requests)
    seq_total = seq_results[-1][1]
    fast_done_seq = max(t for _, t in seq_results[1:])
    print(f"  Sequential")
    print(f"    Total time           : {seq_total*1000:6.1f} ms")
    print(f"    Time to all fast done: {fast_done_seq*1000:6.1f} ms")
    print()

    # --- Classic pipeline ---
    pipe_client = _ClassicPipelineClient()
    pipe_results = pipe_client.run(host, port, requests)
    pipe_total = pipe_results[-1][1]
    fast_done_pipe = max(t for _, t in pipe_results[1:])
    print(f"  Classic pipeline (HOL blocking)")
    print(f"    Total time           : {pipe_total*1000:6.1f} ms")
    print(f"    Time to all fast done: {fast_done_pipe*1000:6.1f} ms")
    print()

    # --- Pipelining+ ---
    t0 = time.monotonic()
    pp_client = PipeliningPlusClient(host, port)
    ids = []
    for method, path in requests:
        ids.append(pp_client.send(method, path))
    slow_id = ids[0]
    fast_ids = set(ids[1:])

    recv_times: dict[str, float] = {}
    for _ in range(total):
        _, req_id, _ = pp_client.recv_one()
        recv_times[req_id] = time.monotonic() - t0
    pp_client.close()

    pp_total = max(recv_times.values())
    fast_done_pp = max(recv_times[rid] for rid in fast_ids)
    print(f"  Pipelining+ / HTTP/1.15")
    print(f"    Total time           : {pp_total*1000:6.1f} ms")
    print(f"    Time to all fast done: {fast_done_pp*1000:6.1f} ms")
    print()

    speedup = fast_done_pipe / fast_done_pp if fast_done_pp > 0 else float("inf")
    print(f"  Pipelining+ delivers fast responses {speedup:.0f}× sooner than classic pipeline.")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    server = build_server(port=0)
    server.start()
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

    host, port = "127.0.0.1", server.port
    print(f"Server on port {port}")
    print()

    try:
        run_demo(host, port)
        run_benchmark(host, port)
    finally:
        server.stop()


if __name__ == "__main__":
    main()
