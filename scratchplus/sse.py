"""Classic SSE: text/event-stream, data/event/id fields, heartbeat comments, reconnect."""

import threading
from typing import Callable

from .http import HTTPRequest, _parse_request
from .log import get_logger
from .tcp import Connection, TCPServer

log = get_logger("sse")


class SSEWriter:
    """Writes SSE-formatted frames to a live TCP connection."""

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._lock = threading.Lock()

    def send_event(self, data: str, *, event: str | None = None, id: str | None = None) -> None:
        """Send a single SSE event. Multi-line data is split across data: lines."""
        parts: list[str] = []
        if id is not None:
            parts.append(f"id: {id}")
        if event is not None:
            parts.append(f"event: {event}")
        for line in (data.splitlines() or [""]):
            parts.append(f"data: {line}")
        parts.append("")  # blank line → event boundary
        chunk = "\n".join(parts) + "\n"
        with self._lock:
            self._conn.send(chunk.encode())
            self._conn.flush()

    def send_comment(self, text: str = "") -> None:
        """Send a heartbeat comment (: text)."""
        with self._lock:
            self._conn.send(f": {text}\n".encode())
            self._conn.flush()

    def send_retry(self, ms: int) -> None:
        """Advise the client to wait ms milliseconds before reconnecting."""
        with self._lock:
            self._conn.send(f"retry: {ms}\n\n".encode())
            self._conn.flush()


SSEHandler = Callable[[HTTPRequest, SSEWriter], None]


class SSEServer:
    """Minimal SSE server built on TCPServer. Each route runs a streaming handler."""

    def __init__(self, host: str = "", port: int = 0, conn_timeout: float = 30.0) -> None:
        self._tcp = TCPServer(host, port, conn_timeout)
        self._routes: dict[str, SSEHandler] = {}

    @property
    def port(self) -> int:
        return self._tcp.port

    def route(self, path: str) -> Callable:
        def decorator(fn: SSEHandler) -> SSEHandler:
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

        handler = self._routes.get(req.path)
        if handler is None:
            conn.send(b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found")
            conn.flush()
            return

        conn.send(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: keep-alive\r\n"
            b"X-Accel-Buffering: no\r\n"
            b"\r\n"
        )
        conn.flush()

        writer = SSEWriter(conn)
        try:
            handler(req, writer)
        except (OSError, BrokenPipeError):
            pass
