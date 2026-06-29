"""Phase 7 tests — Classic WebSocket: handshake, text echo, ping/pong, close."""

import base64
import os
import socket
import struct
import threading

import pytest

from scratchplus.websocket import (
    OP_CLOSE, OP_PING, OP_PONG, OP_TEXT,
    WSServer, compute_accept, make_client_frame,
)


# ---------------------------------------------------------------------------
# Test fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def ws_server():
    server = WSServer(port=0, conn_timeout=10.0)

    @server.route("/echo")
    def echo(ws):
        while True:
            frame = ws.recv_frame()
            if frame is None:
                break
            opcode, payload = frame
            if opcode == OP_TEXT:
                ws.send_text(payload.decode())

    server.start()
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield server
    server.stop()
    t.join(timeout=2)


# ---------------------------------------------------------------------------
# Raw WebSocket client helpers (mirrors what ws_client.py does)
# ---------------------------------------------------------------------------

def _connect(port: int, path: str = "/echo") -> tuple[socket.socket, str]:
    """Perform HTTP upgrade handshake; return (sock, sec_key) on success."""
    raw_key = base64.b64encode(os.urandom(16)).decode()
    sock = socket.create_connection(("127.0.0.1", port), timeout=5.0)
    sock.sendall((
        f"GET {path} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {raw_key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n"
    ).encode())

    buf = b""
    while b"\r\n\r\n" not in buf:
        buf += sock.recv(4096)
    return sock, raw_key, buf[:buf.index(b"\r\n\r\n")].decode()


def _recv_frame(sock: socket.socket) -> tuple[int, bytes]:
    def exact(n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("eof")
            buf += chunk
        return buf

    b0, b1 = exact(2)
    opcode = b0 & 0x0F
    plen = b1 & 0x7F
    if plen == 126:
        plen = struct.unpack("!H", exact(2))[0]
    elif plen == 127:
        plen = struct.unpack("!Q", exact(8))[0]
    return opcode, exact(plen)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_handshake_101(ws_server):
    """Server must respond with 101 Switching Protocols."""
    _, _, headers = _connect(ws_server.port)
    assert "101" in headers.split("\r\n")[0]


def test_handshake_accept_key(ws_server):
    """Sec-WebSocket-Accept must be the correct SHA-1+base64 of the client key."""
    sock, raw_key, headers = _connect(ws_server.port)
    sock.close()
    expected = compute_accept(raw_key)
    accept_line = next(
        (l for l in headers.split("\r\n") if l.lower().startswith("sec-websocket-accept:")),
        None,
    )
    assert accept_line is not None, "Sec-WebSocket-Accept header missing"
    got = accept_line.split(":", 1)[1].strip()
    assert got == expected, f"accept key mismatch: {got!r} != {expected!r}"


def test_text_echo(ws_server):
    """Server must echo back every text frame verbatim."""
    sock, _, _ = _connect(ws_server.port)
    try:
        for msg in ["hello", "world", "🌐"]:
            sock.sendall(make_client_frame(OP_TEXT, msg.encode()))
            opcode, payload = _recv_frame(sock)
            assert opcode == OP_TEXT, f"Expected text frame, got opcode {opcode:#x}"
            assert payload.decode() == msg
    finally:
        sock.close()


def test_ping_pong(ws_server):
    """Server must reply to a ping with a pong carrying the same payload."""
    sock, _, _ = _connect(ws_server.port)
    try:
        ping_payload = b"scratch-plus"
        sock.sendall(make_client_frame(OP_PING, ping_payload))
        opcode, payload = _recv_frame(sock)
        assert opcode == OP_PONG, f"Expected pong (0xA), got {opcode:#x}"
        assert payload == ping_payload, f"Pong payload mismatch: {payload!r}"
    finally:
        sock.close()


def test_clean_close(ws_server):
    """Client close frame must be echoed with a close frame from the server."""
    sock, _, _ = _connect(ws_server.port)
    close_payload = struct.pack("!H", 1000) + b"bye"
    sock.sendall(make_client_frame(OP_CLOSE, close_payload))
    sock.settimeout(3.0)
    opcode, _ = _recv_frame(sock)
    assert opcode == OP_CLOSE, f"Expected close frame (0x8), got {opcode:#x}"
    sock.close()


def test_ping_after_echo(ws_server):
    """Ping/pong works interleaved with text echo."""
    sock, _, _ = _connect(ws_server.port)
    try:
        # echo first
        sock.sendall(make_client_frame(OP_TEXT, b"hi"))
        opcode, payload = _recv_frame(sock)
        assert opcode == OP_TEXT and payload == b"hi"

        # then ping
        sock.sendall(make_client_frame(OP_PING, b"chk"))
        opcode, payload = _recv_frame(sock)
        assert opcode == OP_PONG and payload == b"chk"

        # then echo again
        sock.sendall(make_client_frame(OP_TEXT, b"after ping"))
        opcode, payload = _recv_frame(sock)
        assert opcode == OP_TEXT and payload == b"after ping"
    finally:
        sock.close()


def test_unknown_path_sends_close(ws_server):
    """Connecting to an unregistered path must result in a close frame."""
    sock, _, headers = _connect(ws_server.port, path="/nosuchpath")
    try:
        assert "101" in headers.split("\r\n")[0], "Expected upgrade even for unknown path"
        sock.settimeout(3.0)
        opcode, _ = _recv_frame(sock)
        assert opcode == OP_CLOSE, f"Expected close frame, got opcode {opcode:#x}"
    finally:
        sock.close()


def test_compute_accept_known_value():
    """RFC 6455 §1.3 sample: known key → known accept value."""
    key = "dGhlIHNhbXBsZSBub25jZQ=="
    expected = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    assert compute_accept(key) == expected
