"""TCP core: connection abstraction, blocking server, async server."""

import asyncio
import socket
import threading
from typing import Callable

from .buffer import ByteBuffer
from .log import get_logger

log = get_logger("tcp")

RECV_SIZE = 4096


class Connection:
    """Wraps a client socket with read/write ByteBuffers and a timeout."""

    def __init__(self, sock: socket.socket, addr: tuple, timeout: float = 30.0) -> None:
        self.sock = sock
        self.addr = addr
        self.rbuf = ByteBuffer()
        self._wbuf = ByteBuffer()
        sock.settimeout(timeout)

    def recv(self, n: int = RECV_SIZE) -> bytes:
        """Read up to n bytes from the socket; empty bytes signals EOF."""
        data = self.sock.recv(n)
        if data:
            self.rbuf.write(data)
        return data

    def send(self, data: bytes) -> None:
        """Queue data for sending."""
        self._wbuf.write(data)

    def flush(self) -> None:
        """Drain the write buffer to the socket."""
        while len(self._wbuf):
            chunk = self._wbuf.read(RECV_SIZE)
            self.sock.sendall(chunk)

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    def __repr__(self) -> str:
        return f"Connection({self.addr})"


class TCPServer:
    """Blocking TCP server: one daemon thread per accepted connection."""

    def __init__(
        self,
        host: str = "",
        port: int = 0,
        conn_timeout: float = 30.0,
        backlog: int = 5,
    ) -> None:
        self.host = host
        self.port = port
        self.conn_timeout = conn_timeout
        self.backlog = backlog
        self._sock: socket.socket | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        """Bind and listen. Updates self.port to the actual port (useful for port=0)."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(self.backlog)
        # 1-second accept timeout so serve_forever can check the stop flag
        s.settimeout(1.0)
        self._sock = s
        self.port = s.getsockname()[1]
        log.info("Listening on %s:%d", self.host or "0.0.0.0", self.port)

    def serve_forever(self, handler: Callable[["Connection"], None]) -> None:
        """Accept loop — blocks until stop() is called."""
        assert self._sock is not None, "call start() first"
        while not self._stop.is_set():
            try:
                client_sock, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn = Connection(client_sock, addr, timeout=self.conn_timeout)
            log.debug("Accepted %s", conn)
            t = threading.Thread(target=self._dispatch, args=(conn, handler), daemon=True)
            t.start()

    def _dispatch(self, conn: Connection, handler: Callable) -> None:
        try:
            handler(conn)
        except Exception:
            log.exception("Handler error on %s", conn)
        finally:
            conn.close()
            log.debug("Closed %s", conn)

    def stop(self) -> None:
        self._stop.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass


class AsyncTCPServer:
    """Async TCP server built on asyncio.start_server."""

    def __init__(
        self,
        host: str = "",
        port: int = 0,
        conn_timeout: float = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.conn_timeout = conn_timeout
        self._server: asyncio.Server | None = None
        # threading.Event lets the fixture thread wait for the server to bind
        self._started = threading.Event()

    async def start_and_serve(
        self,
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter, float], None],
    ) -> None:
        """Bind, listen, and serve until stop() is called (or the task is cancelled)."""
        self._server = await asyncio.start_server(
            self._wrap(handler),
            self.host or "0.0.0.0",
            self.port,
        )
        self.port = self._server.sockets[0].getsockname()[1]
        log.info("Async listening on port %d", self.port)
        self._started.set()
        try:
            async with self._server:
                await self._server.serve_forever()
        except asyncio.CancelledError:
            pass

    def _wrap(self, handler):
        timeout = self.conn_timeout

        async def _h(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            addr = writer.get_extra_info("peername")
            log.debug("Async connection from %s", addr)
            try:
                await handler(reader, writer, timeout)
            except Exception:
                log.exception("Async handler error for %s", addr)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

        return _h

    def stop(self) -> None:
        """Thread-safe: close the server (serve_forever will return)."""
        if self._server:
            self._server.close()
