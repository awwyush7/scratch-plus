"""Phase 2 tests — HTTP/1.1 subset: routes, headers, keep-alive, connection-close."""

import json
import socket
import threading
import time

import pytest

from scratchplus.http import HTTPRequest, HTTPServer, json_response, text_response


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def http_server():
    server = HTTPServer(port=0, conn_timeout=5.0)

    @server.route("GET", "/")
    def index(req: HTTPRequest):
        return text_response("Hello from Scratch+!")

    @server.route("GET", "/health")
    def health(req: HTTPRequest):
        return json_response({"status": "ok"})

    @server.route("GET", "/delay")
    def delay(req: HTTPRequest):
        ms = int(req.query_params.get("ms", 0))
        ms = max(0, min(ms, 5_000))
        time.sleep(ms / 1000)
        return json_response({"slept_ms": ms})

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
# Raw HTTP client helper (preserves buffer across requests for keep-alive)
# ---------------------------------------------------------------------------

class _Client:
    """Minimal stateful HTTP/1.1 client over a raw socket."""

    def __init__(self, host: str, port: int, timeout: float = 5.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._buf = bytearray()

    def request(
        self,
        method: str,
        path: str,
        body: bytes = b"",
        headers: dict | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        hdrs = {
            "Host": "127.0.0.1",
            "Content-Length": str(len(body)),
            "Connection": "keep-alive",
        }
        if headers:
            hdrs.update(headers)
        header_str = "\r\n".join(f"{k}: {v}" for k, v in hdrs.items())
        raw = f"{method} {path} HTTP/1.1\r\n{header_str}\r\n\r\n".encode() + body
        self._sock.sendall(raw)
        return self._read_response()

    def _read_response(self) -> tuple[int, dict[str, str], bytes]:
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            assert chunk, "unexpected EOF reading response headers"
            self._buf.extend(chunk)

        idx = self._buf.index(b"\r\n\r\n")
        header_data = bytes(self._buf[:idx]).decode()
        del self._buf[: idx + 4]

        lines = header_data.split("\r\n")
        status = int(lines[0].split(" ", 2)[1])
        resp_headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                resp_headers[k.strip().lower()] = v.strip()

        content_length = int(resp_headers.get("content-length", 0))
        while len(self._buf) < content_length:
            chunk = self._sock.recv(4096)
            assert chunk, "unexpected EOF reading response body"
            self._buf.extend(chunk)

        body = bytes(self._buf[:content_length])
        del self._buf[:content_length]
        return status, resp_headers, body

    def recv_eof(self) -> bool:
        """Return True if server closed the connection (EOF)."""
        try:
            return self._sock.recv(1) == b""
        except (socket.timeout, OSError):
            return False

    def close(self) -> None:
        self._sock.close()


# ---------------------------------------------------------------------------
# Route tests
# ---------------------------------------------------------------------------

def test_get_root_status_and_body(http_server):
    c = _Client("127.0.0.1", http_server.port)
    status, hdrs, body = c.request("GET", "/")
    assert status == 200
    assert "text/plain" in hdrs["content-type"]
    assert b"Hello from Scratch+" in body
    c.close()


def test_get_root_content_length(http_server):
    c = _Client("127.0.0.1", http_server.port)
    status, hdrs, body = c.request("GET", "/")
    assert int(hdrs["content-length"]) == len(body)
    c.close()


def test_get_health_json(http_server):
    c = _Client("127.0.0.1", http_server.port)
    status, hdrs, body = c.request("GET", "/health")
    assert status == 200
    assert hdrs["content-type"] == "application/json"
    assert json.loads(body) == {"status": "ok"}
    c.close()


def test_get_delay_sleeps(http_server):
    c = _Client("127.0.0.1", http_server.port, timeout=10.0)
    t0 = time.monotonic()
    status, hdrs, body = c.request("GET", "/delay?ms=200")
    elapsed = time.monotonic() - t0
    assert status == 200
    assert json.loads(body) == {"slept_ms": 200}
    assert elapsed >= 0.18
    c.close()


def test_get_delay_zero(http_server):
    c = _Client("127.0.0.1", http_server.port)
    status, hdrs, body = c.request("GET", "/delay")
    assert status == 200
    assert json.loads(body) == {"slept_ms": 0}
    c.close()


def test_post_echo_body(http_server):
    payload = b'{"hello": "world"}'
    c = _Client("127.0.0.1", http_server.port)
    status, hdrs, body = c.request(
        "POST", "/echo", body=payload, headers={"Content-Type": "application/json"}
    )
    assert status == 200
    assert json.loads(body)["body"] == payload.decode()
    c.close()


def test_post_echo_empty_body(http_server):
    c = _Client("127.0.0.1", http_server.port)
    status, hdrs, body = c.request("POST", "/echo")
    assert status == 200
    assert json.loads(body)["body"] == ""
    c.close()


def test_404_unknown_route(http_server):
    c = _Client("127.0.0.1", http_server.port)
    status, hdrs, body = c.request("GET", "/not-a-route")
    assert status == 404
    c.close()


# ---------------------------------------------------------------------------
# Keep-alive tests
# ---------------------------------------------------------------------------

def test_keep_alive_reuses_connection(http_server):
    c = _Client("127.0.0.1", http_server.port)

    status1, hdrs1, body1 = c.request("GET", "/health")
    assert status1 == 200
    assert hdrs1["connection"] == "keep-alive"

    status2, hdrs2, body2 = c.request("GET", "/")
    assert status2 == 200
    assert hdrs2["connection"] == "keep-alive"
    assert b"Hello" in body2

    status3, hdrs3, body3 = c.request("POST", "/echo", body=b"ping")
    assert status3 == 200
    assert json.loads(body3)["body"] == "ping"

    c.close()


def test_connection_close_header(http_server):
    c = _Client("127.0.0.1", http_server.port)
    status, hdrs, body = c.request("GET", "/health", headers={"Connection": "close"})
    assert status == 200
    assert hdrs["connection"] == "close"
    # Server must close after one response
    assert c.recv_eof()
    c.close()


def test_keep_alive_many_requests(http_server):
    """Ten sequential requests on a single TCP connection all succeed."""
    c = _Client("127.0.0.1", http_server.port)
    for i in range(10):
        status, _, body = c.request("GET", "/health")
        assert status == 200
        assert json.loads(body) == {"status": "ok"}
    c.close()
