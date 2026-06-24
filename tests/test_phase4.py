"""Phase 4 tests — Pipelining+ / HTTP/1.15: out-of-order responses, Request-ID mapping."""

import json
import socket
import threading
import time

import pytest

from scratchplus.http import HTTPRequest, json_response
from scratchplus.pipelining_plus import PipeliningPlusServer


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def pp_server():
    server = PipeliningPlusServer(port=0, conn_timeout=10.0)

    @server.route("GET", "/fast")
    def fast(req: HTTPRequest):
        return json_response({"route": "fast"})

    @server.route("GET", "/slow")
    def slow(req: HTTPRequest):
        ms = int(req.query_params.get("ms", "300"))
        time.sleep(ms / 1000)
        return json_response({"route": "slow", "delay_ms": ms})

    @server.route("POST", "/echo")
    def echo(req: HTTPRequest):
        return json_response({"body": req.body.decode("utf-8", errors="replace")})

    server.start()
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield server
    server.stop()
    t.join(timeout=2)


# ---------------------------------------------------------------------------
# Minimal client (inline — same pattern as phase 3 tests)
# ---------------------------------------------------------------------------

class _PPClient:
    def __init__(self, host: str, port: int, timeout: float = 10.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._buf = bytearray()
        self._counter = 0

    def send(self, method: str, path: str, body: bytes = b"", req_id: str | None = None) -> str:
        if req_id is None:
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
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            assert chunk, "unexpected EOF"
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
            assert chunk, "unexpected EOF in body"
            self._buf.extend(chunk)
        body = bytes(self._buf[:clen])
        del self._buf[:clen]
        return status, headers.get("request-id", ""), body

    def close(self) -> None:
        self._sock.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_request_id_echoed(pp_server):
    """Every response must echo back the exact Request-ID from the request."""
    c = _PPClient("127.0.0.1", pp_server.port)
    ids_sent = [c.send("GET", "/fast", req_id=f"alpha-{i}") for i in range(3)]
    ids_received = []
    for _ in range(3):
        _, rid, _ = c.recv_one()
        ids_received.append(rid)
    c.close()
    assert set(ids_sent) == set(ids_received), (
        f"Echoed IDs {ids_received} don't match sent IDs {ids_sent}"
    )


def test_out_of_order_fast_before_slow(pp_server):
    """
    Pipeline: Req1=slow(300ms), Req2=fast.
    Pipelining+ must deliver Resp2 (/fast) BEFORE Resp1 (/slow).
    The fast response should arrive well under 150 ms; the slow one at ~300 ms.
    """
    SLOW_MS = 300
    c = _PPClient("127.0.0.1", pp_server.port)

    t0 = time.monotonic()
    slow_id = c.send("GET", f"/slow?ms={SLOW_MS}")
    fast_id = c.send("GET", "/fast")

    status1, rid1, body1 = c.recv_one()
    t1 = time.monotonic() - t0

    status2, rid2, body2 = c.recv_one()
    t2 = time.monotonic() - t0

    c.close()

    assert status1 == status2 == 200

    # First response to arrive must be the fast one
    assert rid1 == fast_id, (
        f"Expected fast response first, got Request-ID={rid1!r}. "
        "HOL blocking still present — server may not be concurrent."
    )
    assert rid2 == slow_id

    # Fast response arrived quickly
    assert t1 < 0.15, f"Fast response took {t1*1000:.1f} ms, expected < 150 ms"

    # Slow response arrived after the delay
    assert t2 >= SLOW_MS / 1000 * 0.85, (
        f"Slow response arrived too early: {t2*1000:.1f} ms, expected ≥ {SLOW_MS*0.85:.0f} ms"
    )

    # Correct body mapping
    assert json.loads(body1)["route"] == "fast"
    assert json.loads(body2)["route"] == "slow"


def test_request_id_mapping_many(pp_server):
    """
    Pipeline 5 requests: 1 slow + 4 fast.
    After receiving all 5 responses (in any order), every Request-ID must
    map to the correct route.
    """
    SLOW_MS = 300
    c = _PPClient("127.0.0.1", pp_server.port)

    slow_id = c.send("GET", f"/slow?ms={SLOW_MS}")
    fast_ids = {c.send("GET", "/fast") for _ in range(4)}

    results: dict[str, str] = {}  # request_id → route
    for _ in range(5):
        _, rid, body = c.recv_one()
        results[rid] = json.loads(body)["route"]

    c.close()

    assert results[slow_id] == "slow", f"slow_id mapped to wrong route: {results[slow_id]}"
    for fid in fast_ids:
        assert results[fid] == "fast", f"fast_id {fid} mapped to wrong route: {results[fid]}"


def test_no_request_id_still_works(pp_server):
    """Requests without Request-ID are handled normally; responses have no Request-ID header."""
    sock = socket.create_connection(("127.0.0.1", pp_server.port), timeout=5.0)
    sock.settimeout(5.0)
    sock.sendall((
        "GET /fast HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Connection: keep-alive\r\n"
        "Content-Length: 0\r\n"
        "\r\n"
    ).encode())
    buf = bytearray()
    while b"\r\n\r\n" not in buf:
        buf.extend(sock.recv(4096))
    idx = buf.index(b"\r\n\r\n")
    header_block = bytes(buf[:idx]).decode().lower()
    sock.close()
    assert "200" in header_block.split("\r\n")[0] or "200" in bytes(buf[:idx]).decode().split()[1]
    assert "request-id" not in header_block


def test_concurrent_handlers_do_not_interleave(pp_server):
    """
    Send 6 pipelined requests; read all responses.
    Every response must be a self-contained, parseable HTTP message —
    meaning the send lock prevented any byte-level interleaving.
    """
    SLOW_MS = 200
    c = _PPClient("127.0.0.1", pp_server.port)

    ids = []
    ids.append(c.send("GET", f"/slow?ms={SLOW_MS}"))
    for _ in range(5):
        ids.append(c.send("GET", "/fast"))

    received: dict[str, bytes] = {}
    for _ in range(6):
        status, rid, body = c.recv_one()
        assert status == 200
        data = json.loads(body)  # would raise if body bytes were corrupted/interleaved
        received[rid] = body

    c.close()

    assert set(received.keys()) == set(ids), "Some responses are missing"


def test_post_echo_request_id(pp_server):
    """POST /echo with Request-ID; body content must survive round-trip correctly."""
    c = _PPClient("127.0.0.1", pp_server.port)
    payload = b"hello pipelining+"
    rid = c.send("POST", "/echo", body=payload)
    _, echoed_rid, body = c.recv_one()
    c.close()

    assert echoed_rid == rid
    assert json.loads(body)["body"] == payload.decode()
