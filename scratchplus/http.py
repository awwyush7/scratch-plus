"""HTTP/1.1 subset: request parser, response builder, router, keep-alive loop."""

import json
import socket
import time
import urllib.parse
from dataclasses import dataclass
from typing import Callable

from .log import get_logger
from .tcp import Connection, TCPServer

log = get_logger("http")

_HEADER_END = b"\r\n\r\n"
_MAX_HEADER_BYTES = 64 * 1024

_REASON: dict[int, str] = {
    200: "OK",
    201: "Created",
    204: "No Content",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    500: "Internal Server Error",
}


@dataclass
class HTTPRequest:
    method: str
    path: str
    query_string: str
    query_params: dict[str, str]
    headers: dict[str, str]  # lowercase keys
    body: bytes
    version: str


@dataclass
class HTTPResponse:
    status: int
    headers: dict[str, str]
    body: bytes

    def encode(self) -> bytes:
        reason = _REASON.get(self.status, "Unknown")
        status_line = f"HTTP/1.1 {self.status} {reason}"
        header_lines = "\r\n".join(f"{k}: {v}" for k, v in self.headers.items())
        return f"{status_line}\r\n{header_lines}\r\n\r\n".encode() + self.body


def json_response(data, status: int = 200, keep_alive: bool = True) -> HTTPResponse:
    body = json.dumps(data).encode()
    return HTTPResponse(
        status,
        {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "Connection": "keep-alive" if keep_alive else "close",
        },
        body,
    )


def text_response(text: str, status: int = 200, keep_alive: bool = True) -> HTTPResponse:
    body = text.encode()
    return HTTPResponse(
        status,
        {
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(body)),
            "Connection": "keep-alive" if keep_alive else "close",
        },
        body,
    )


def _parse_request(conn: Connection) -> "HTTPRequest | None":
    """Read and parse one HTTP request from conn. Returns None on EOF/timeout/error."""
    while conn.rbuf.find(_HEADER_END) == -1:
        try:
            data = conn.recv()
        except (socket.timeout, OSError):
            return None
        if not data:
            return None
        if len(conn.rbuf) > _MAX_HEADER_BYTES:
            return None

    raw = conn.rbuf.read_until(_HEADER_END)
    header_block = raw[:-4]
    lines = header_block.split(b"\r\n")

    parts = lines[0].decode().split(" ", 2)
    if len(parts) != 3:
        return None
    method, raw_path, version = parts

    path, _, query_string = raw_path.partition("?")
    query_params = dict(urllib.parse.parse_qsl(query_string))

    headers: dict[str, str] = {}
    for line in lines[1:]:
        if b":" in line:
            k, _, v = line.partition(b":")
            headers[k.strip().lower().decode()] = v.strip().decode()

    body = b""
    content_length = int(headers.get("content-length", 0))
    if content_length > 0:
        while len(conn.rbuf) < content_length:
            try:
                data = conn.recv()
            except (socket.timeout, OSError):
                break
            if not data:
                break
        body = conn.rbuf.read(content_length)

    return HTTPRequest(method, path, query_string, query_params, headers, body, version)


Handler = Callable[[HTTPRequest], HTTPResponse]


class HTTPServer:
    """HTTP/1.1 server with keep-alive, built on TCPServer."""

    def __init__(self, host: str = "", port: int = 0, conn_timeout: float = 30.0) -> None:
        self._tcp = TCPServer(host, port, conn_timeout)
        self._routes: dict[tuple[str, str], Handler] = {}

    @property
    def port(self) -> int:
        return self._tcp.port

    def route(self, method: str, path: str) -> Callable:
        def decorator(fn: Handler) -> Handler:
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
        while True:
            req = _parse_request(conn)
            if req is None:
                break

            keep_alive = (
                req.version == "HTTP/1.1"
                and req.headers.get("connection", "").lower() != "close"
            )

            handler = self._routes.get((req.method, req.path))
            if handler is None:
                resp = text_response("Not Found", status=404, keep_alive=keep_alive)
            else:
                try:
                    resp = handler(req)
                except Exception:
                    log.exception("Handler error for %s %s", req.method, req.path)
                    resp = text_response("Internal Server Error", status=500, keep_alive=keep_alive)
                resp.headers["Connection"] = "keep-alive" if keep_alive else "close"

            conn.send(resp.encode())
            conn.flush()

            if not keep_alive:
                break
