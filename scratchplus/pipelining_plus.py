"""Pipelining+ / HTTP/1.15: concurrent per-request dispatch, out-of-order responses."""

import threading
from typing import Callable

from .http import HTTPRequest, HTTPResponse, _parse_request, text_response
from .log import get_logger
from .tcp import Connection, TCPServer

log = get_logger("pipelining_plus")


class PipeliningPlusServer:
    """
    HTTP/1.15 server.

    One TCP connection carries multiple pipelined requests.  Each request
    carries a ``Request-ID`` header; the server dispatches each to its own
    thread and sends COMPLETE responses in whatever order they finish.  The
    send lock guarantees that response bytes from different handlers never
    interleave on the wire.  The client identifies each response by the
    echoed ``Request-ID`` header.

    This is NOT HTTP/2: there are no frames, no streams, no flow control.
    The only addition over classic pipelining is the concurrency on the server
    side and the Request-ID echo that lets the client match out-of-order
    responses to their requests.
    """

    def __init__(
        self,
        host: str = "",
        port: int = 0,
        conn_timeout: float = 30.0,
    ) -> None:
        self._tcp = TCPServer(host, port, conn_timeout)
        self._routes: dict[tuple[str, str], Callable[[HTTPRequest], HTTPResponse]] = {}

    @property
    def port(self) -> int:
        return self._tcp.port

    def route(self, method: str, path: str) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._routes[(method.upper(), path)] = fn
            return fn
        return decorator

    def start(self) -> None:
        self._tcp.start()

    def serve_forever(self) -> None:
        self._tcp.serve_forever(self._handle_conn)

    def stop(self) -> None:
        self._tcp.stop()

    def _handle_conn(self, conn: Connection) -> None:
        # One lock per connection; guarantees complete (not interleaved) responses.
        send_lock = threading.Lock()
        pending: list[threading.Thread] = []

        def dispatch(req: HTTPRequest) -> None:
            request_id = req.headers.get("request-id", "")
            handler = self._routes.get((req.method, req.path))
            if handler is None:
                resp = text_response("Not Found", 404)
            else:
                try:
                    resp = handler(req)
                except Exception:
                    log.exception("Handler error for %s %s", req.method, req.path)
                    resp = text_response("Internal Server Error", 500)

            if request_id:
                resp.headers["Request-ID"] = request_id
            resp.headers["Connection"] = "keep-alive"

            raw = resp.encode()
            with send_lock:
                try:
                    conn.send(raw)
                    conn.flush()
                except OSError:
                    pass  # client disconnected; drop silently

        # Reader loop: parse one complete request, immediately hand it to a
        # fresh thread.  No waiting for the previous handler to finish.
        while True:
            req = _parse_request(conn)
            if req is None:
                break
            t = threading.Thread(target=dispatch, args=(req,), daemon=True)
            pending.append(t)
            t.start()

        for t in pending:
            t.join(timeout=30.0)
