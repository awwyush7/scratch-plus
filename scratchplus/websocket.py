"""Classic WebSocket — RFC 6455: HTTP upgrade handshake + text/ping/pong/close frames."""

import base64
import hashlib
import os
import socket
import struct
from typing import Callable

from .http import _parse_request
from .log import get_logger
from .tcp import Connection, TCPServer

log = get_logger("websocket")

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# Frame opcodes
OP_CONT  = 0x0
OP_TEXT  = 0x1
OP_BIN   = 0x2
OP_CLOSE = 0x8
OP_PING  = 0x9
OP_PONG  = 0xA


# ---------------------------------------------------------------------------
# Accept-key computation
# ---------------------------------------------------------------------------

def compute_accept(sec_key: str) -> str:
    """Sec-WebSocket-Key → Sec-WebSocket-Accept (SHA-1 + magic GUID, base64)."""
    digest = hashlib.sha1((sec_key + _WS_GUID).encode()).digest()
    return base64.b64encode(digest).decode()


# ---------------------------------------------------------------------------
# Frame I/O helpers
# ---------------------------------------------------------------------------

def _fill(conn: Connection, n: int) -> bool:
    """Ensure conn.rbuf holds at least n bytes. Returns False on EOF/error."""
    while len(conn.rbuf) < n:
        try:
            data = conn.recv()
        except (socket.timeout, OSError):
            return False
        if not data:
            return False
    return True


def read_frame(conn: Connection) -> "tuple[int, bytes] | None":
    """
    Read one complete WebSocket frame from conn.
    Returns (opcode, payload) with client masking already removed.
    Returns None on connection close or error.
    """
    if not _fill(conn, 2):
        return None

    b0 = conn.rbuf.read(1)[0]
    b1 = conn.rbuf.read(1)[0]

    opcode = b0 & 0x0F
    masked = bool(b1 & 0x80)
    payload_len = b1 & 0x7F

    if payload_len == 126:
        if not _fill(conn, 2):
            return None
        payload_len = struct.unpack("!H", conn.rbuf.read(2))[0]
    elif payload_len == 127:
        if not _fill(conn, 8):
            return None
        payload_len = struct.unpack("!Q", conn.rbuf.read(8))[0]

    mask_key = b""
    if masked:
        if not _fill(conn, 4):
            return None
        mask_key = conn.rbuf.read(4)

    if not _fill(conn, payload_len):
        return None
    raw = bytearray(conn.rbuf.read(payload_len))

    if masked:
        for i in range(len(raw)):
            raw[i] ^= mask_key[i % 4]

    return opcode, bytes(raw)


def write_frame(conn: Connection, opcode: int, payload: bytes) -> None:
    """
    Write one WebSocket frame to conn (server→client, always unmasked per RFC 6455 §5.1).
    Sets FIN bit; no fragmentation.
    """
    b0 = 0x80 | opcode  # FIN=1
    n = len(payload)
    if n < 126:
        header = bytes([b0, n])
    elif n < 65536:
        header = bytes([b0, 126]) + struct.pack("!H", n)
    else:
        header = bytes([b0, 127]) + struct.pack("!Q", n)
    conn.send(header + payload)
    conn.flush()


# ---------------------------------------------------------------------------
# Client-side framing helpers (for ws_client.py / tests)
# ---------------------------------------------------------------------------

def make_client_frame(opcode: int, payload: bytes) -> bytes:
    """Build a masked frame as a client would send it (MASK bit set, 4-byte mask key)."""
    mask_key = os.urandom(4)
    masked = bytearray(payload)
    for i in range(len(masked)):
        masked[i] ^= mask_key[i % 4]

    n = len(payload)
    if n < 126:
        header = bytes([0x80 | opcode, 0x80 | n])
    elif n < 65536:
        header = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack("!H", n)
    else:
        header = bytes([0x80 | opcode, 0x80 | 127]) + struct.pack("!Q", n)

    return header + mask_key + bytes(masked)


# ---------------------------------------------------------------------------
# WSConnection — server-side session object
# ---------------------------------------------------------------------------

class WSConnection:
    """Wraps a TCP Connection for WebSocket send/recv after the handshake."""

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._closed = False

    def send_text(self, text: str) -> None:
        write_frame(self._conn, OP_TEXT, text.encode())

    def send_binary(self, data: bytes) -> None:
        write_frame(self._conn, OP_BIN, data)

    def send_ping(self, payload: bytes = b"") -> None:
        write_frame(self._conn, OP_PING, payload)

    def send_pong(self, payload: bytes = b"") -> None:
        write_frame(self._conn, OP_PONG, payload)

    def send_close(self, code: int = 1000, reason: str = "") -> None:
        if self._closed:
            return
        self._closed = True
        body = struct.pack("!H", code) + reason.encode()
        write_frame(self._conn, OP_CLOSE, body)

    def recv_frame(self) -> "tuple[int, bytes] | None":
        """Read the next frame. Auto-handles ping (sends pong) and close echo."""
        while True:
            frame = read_frame(self._conn)
            if frame is None:
                return None
            opcode, payload = frame
            if opcode == OP_PING:
                self.send_pong(payload)
                continue
            if opcode == OP_CLOSE:
                if not self._closed:
                    self.send_close(1000)
                return None
            return opcode, payload

    @property
    def closed(self) -> bool:
        return self._closed


# ---------------------------------------------------------------------------
# WSServer
# ---------------------------------------------------------------------------

WSHandler = Callable[[WSConnection], None]


class WSServer:
    """WebSocket server built on TCPServer. Handles HTTP upgrade per RFC 6455."""

    def __init__(self, host: str = "", port: int = 0, conn_timeout: float = 30.0) -> None:
        self._tcp = TCPServer(host, port, conn_timeout)
        self._routes: dict[str, WSHandler] = {}

    @property
    def port(self) -> int:
        return self._tcp.port

    def route(self, path: str) -> Callable:
        def decorator(fn: WSHandler) -> WSHandler:
            self._routes[path] = fn
            return fn
        return decorator

    def start(self) -> None:
        self._tcp.start()

    def serve_forever(self) -> None:
        self._tcp.serve_forever(self._handle_conn)

    def stop(self) -> None:
        self._tcp.stop()

    def _handle_conn(self, conn: Connection) -> None:
        req = _parse_request(conn)
        if req is None:
            return

        upgrade = req.headers.get("upgrade", "").lower()
        connection = req.headers.get("connection", "").lower()
        sec_key = req.headers.get("sec-websocket-key", "")

        if upgrade != "websocket" or "upgrade" not in connection or not sec_key:
            conn.send(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 11\r\n\r\nBad Request")
            conn.flush()
            return

        accept = compute_accept(sec_key)
        conn.send(
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"Upgrade: websocket\r\n"
            b"Connection: Upgrade\r\n"
            + f"Sec-WebSocket-Accept: {accept}\r\n".encode()
            + b"\r\n"
        )
        conn.flush()
        log.debug("WebSocket upgrade complete for %s %s", conn, req.path)

        handler = self._routes.get(req.path)
        ws = WSConnection(conn)
        if handler is None:
            ws.send_close(1011, "no handler for path")
            return

        try:
            handler(ws)
        except (OSError, BrokenPipeError):
            pass
        finally:
            if not ws.closed:
                ws.send_close(1000)
