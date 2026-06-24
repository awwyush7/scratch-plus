"""Phase 3 tests — HTTP/1.1 classic pipelining: in-order responses and HOL blocking."""

import json
import socket
import threading
import time

import pytest

from scratchplus.http import HTTPRequest, HTTPServer, json_response, text_response


# ---------------------------------------------------------------------------
# Fixture (same routes as phase 2, reused by example client too)
# ---------------------------------------------------------------------------

@pytest.fixture
def http_server():
    server = HTTPServer(port=0, conn_timeout=10.0)

    @server.route("GET", "/fast")
    def fast(req: HTTPRequest):
        return json_response({"route": "fast"})

    @server.route("GET", "/slow")
    def slow(req: HTTPRequest):
        time.sleep(0.30)
        return json_response({"route": "slow"})

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
# Pipelining client — sends N requests before reading any responses
# ---------------------------------------------------------------------------

class _PipelinedClient:
    """Fires all requests immediately, then reads responses in arrival order."""

    def __init__(self, host: str, port: int, timeout: float = 10.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._buf = bytearray()

    def send(self, method: str, path: str, body: bytes = b"", extra_headers: dict | None = None) -> None:
        hdrs = {
            "Host": "127.0.0.1",
            "Connection": "keep-alive",
            "Content-Length": str(len(body)),
        }
        if extra_headers:
            hdrs.update(extra_headers)
        lines = "\r\n".join(f"{k}: {v}" for k, v in hdrs.items())
        raw = f"{method} {path} HTTP/1.1\r\n{lines}\r\n\r\n".encode() + body
        self._sock.sendall(raw)

    def recv_one(self) -> tuple[int, dict[str, str], bytes]:
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            assert chunk, "unexpected EOF reading response"
            self._buf.extend(chunk)

        idx = self._buf.index(b"\r\n\r\n")
        header_data = bytes(self._buf[:idx]).decode()
        del self._buf[: idx + 4]

        lines = header_data.split("\r\n")
        status = int(lines[0].split(" ", 2)[1])
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()

        clen = int(headers.get("content-length", 0))
        while len(self._buf) < clen:
            chunk = self._sock.recv(4096)
            assert chunk, "unexpected EOF reading response body"
            self._buf.extend(chunk)

        body = bytes(self._buf[:clen])
        del self._buf[:clen]
        return status, headers, body

    def close(self) -> None:
        self._sock.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_pipeline_responses_arrive_in_order(http_server):
    """
    Pipeline 3 distinct POST /echo requests; responses must echo them in order.
    Sending all requests before reading any response is what makes this pipelining.
    """
    c = _PipelinedClient("127.0.0.1", http_server.port)

    # Fire all 3 requests without waiting for any response
    for i in range(3):
        c.send("POST", "/echo", body=f"req-{i}".encode())

    # Read responses — must arrive in request order
    for i in range(3):
        status, _, body = c.recv_one()
        assert status == 200
        assert json.loads(body)["body"] == f"req-{i}"

    c.close()


def test_pipeline_hol_blocking(http_server):
    """
    Pipeline: /slow (300 ms) → /fast → /fast.

    HOL-blocking properties to verify:
      1. Resp1 (/slow) takes ≥ 300 ms — the slow route ran.
      2. Resp2 and Resp3 (/fast) are delivered quickly AFTER Resp1, not before.
      3. The two fast responses arrive within 150 ms of Resp1 (they were queued,
         not re-processed after a second round-trip).
    """
    SLOW_S = 0.30
    c = _PipelinedClient("127.0.0.1", http_server.port)

    t0 = time.monotonic()

    # Pipeline all three requests at once
    c.send("GET", "/slow")
    c.send("GET", "/fast")
    c.send("GET", "/fast")

    status1, _, body1 = c.recv_one()
    t1 = time.monotonic()

    status2, _, body2 = c.recv_one()
    t2 = time.monotonic()

    status3, _, body3 = c.recv_one()
    t3 = time.monotonic()

    c.close()

    # Correct status codes
    assert status1 == status2 == status3 == 200

    # Responses carry the correct route identifiers (proves order)
    assert json.loads(body1)["route"] == "slow"
    assert json.loads(body2)["route"] == "fast"
    assert json.loads(body3)["route"] == "fast"

    # Resp1 took at least the slow delay
    elapsed_resp1 = t1 - t0
    assert elapsed_resp1 >= SLOW_S * 0.85, (
        f"Expected resp1 after ~{SLOW_S}s, got {elapsed_resp1:.3f}s"
    )

    # HOL: resp2 and resp3 could not have arrived before resp1
    assert t2 >= t1, "resp2 arrived before resp1 — order violated"
    assert t3 >= t2, "resp3 arrived before resp2 — order violated"

    # Fast responses are delivered promptly once the slow one unblocks
    fast_lag = t3 - t1
    assert fast_lag < 0.15, (
        f"Fast responses took {fast_lag:.3f}s after unblocking — expected < 0.15s"
    )
