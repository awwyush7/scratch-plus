"""Classic multiplexing toy: framed streams over one TCP connection."""

import socket
import struct
import threading
from typing import Callable

from .log import get_logger
from .tcp import TCPServer, Connection

log = get_logger("multiplexing")

# ---- Frame types -----------------------------------------------------------

FRAME_DATA = 0
FRAME_HEADERS = 1
FRAME_END = 2

# ---- Wire format -----------------------------------------------------------
# [stream_id: 4B uint][frame_type: 1B][payload_len: 4B uint][payload: N bytes]

_HDR_FMT = "!IBI"
_HDR_SIZE = struct.calcsize(_HDR_FMT)  # 9 bytes


def encode_frame(stream_id: int, frame_type: int, payload: bytes = b"") -> bytes:
    return struct.pack(_HDR_FMT, stream_id, frame_type, len(payload)) + payload


# ---- Connection wrapper ----------------------------------------------------

class MuxConn:
    """
    Wraps a socket with frame-level send/receive.

    send_frame holds a lock only for the duration of one sendall call so that
    DATA frames from different streams can interleave freely on the wire.
    This is the key difference from Pipelining+, which holds the send lock
    for an entire response.
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._lock = threading.Lock()

    def send_frame(self, stream_id: int, frame_type: int, payload: bytes = b"") -> None:
        frame = encode_frame(stream_id, frame_type, payload)
        with self._lock:
            self._sock.sendall(frame)

    def recv_frame(self) -> tuple[int, int, bytes] | None:
        """Read one complete frame; return (stream_id, frame_type, payload) or None on EOF."""
        raw = self._recvexact(_HDR_SIZE)
        if raw is None:
            return None
        stream_id, frame_type, payload_len = struct.unpack(_HDR_FMT, raw)
        payload = self._recvexact(payload_len) if payload_len else b""
        if payload is None:
            return None
        return stream_id, frame_type, payload

    def _recvexact(self, n: int) -> bytes | None:
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass


# ---- Server ----------------------------------------------------------------

class MuxServer:
    """
    Toy multiplexing server.

    One TCP connection carries multiple concurrent logical streams, each
    identified by a stream_id.  A stream begins with a HEADERS frame whose
    payload is the request path.  The server dispatches each stream to a
    handler thread; handlers send DATA frames and finish with an END frame.

    Because the per-frame lock is released between frames, DATA frames from
    concurrent streams can interleave on the wire — neither stream waits for
    the other to finish before its bytes can flow.
    """

    def __init__(self, host: str = "", port: int = 0, conn_timeout: float = 30.0) -> None:
        self._tcp = TCPServer(host, port, conn_timeout)
        self._routes: dict[str, Callable[[int, "MuxConn"], None]] = {}

    @property
    def port(self) -> int:
        return self._tcp.port

    def route(self, path: str) -> Callable:
        def decorator(fn: Callable) -> Callable:
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
        mux = MuxConn(conn.sock)
        pending: list[threading.Thread] = []

        while True:
            frame = mux.recv_frame()
            if frame is None:
                break
            stream_id, frame_type, payload = frame
            if frame_type != FRAME_HEADERS:
                continue
            path = payload.decode()
            handler = self._routes.get(path)

            def dispatch(sid=stream_id, h=handler, p=path) -> None:
                try:
                    if h is None:
                        mux.send_frame(sid, FRAME_DATA, b"404 Not Found")
                    else:
                        h(sid, mux)
                except Exception:
                    log.exception("Handler error stream=%d path=%s", sid, p)
                finally:
                    mux.send_frame(sid, FRAME_END)

            t = threading.Thread(target=dispatch, daemon=True)
            pending.append(t)
            t.start()

        for t in pending:
            t.join(timeout=10.0)
