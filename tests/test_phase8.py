"""Phase 8 tests — Classic Multiplexing Toy: frames, stream reassembly, interleaving."""

import socket
import threading
import time
from collections import defaultdict

import pytest

from scratchplus.multiplexing import (
    FRAME_DATA,
    FRAME_END,
    FRAME_HEADERS,
    MuxConn,
    MuxServer,
    encode_frame,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _client(port: int) -> MuxConn:
    sock = socket.create_connection(("127.0.0.1", port), timeout=5.0)
    return MuxConn(sock)


def _collect(mux: MuxConn, expect_streams: set[int]) -> dict[int, bytes]:
    """Read frames until all expected stream_ids have received END."""
    bodies: dict[int, list[bytes]] = defaultdict(list)
    done: set[int] = set()
    while done != expect_streams:
        frame = mux.recv_frame()
        assert frame is not None, "Connection closed before all streams completed"
        stream_id, frame_type, payload = frame
        if frame_type == FRAME_END:
            done.add(stream_id)
        elif frame_type == FRAME_DATA:
            bodies[stream_id].append(payload)
    return {sid: b"".join(chunks) for sid, chunks in bodies.items()}


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mux_server():
    server = MuxServer(port=0, conn_timeout=5.0)

    @server.route("/echo")
    def echo(stream_id: int, mux: MuxConn) -> None:
        # Just re-emit the path as DATA so we know which stream handled it
        mux.send_frame(stream_id, FRAME_DATA, b"echo")

    @server.route("/fast")
    def fast(stream_id: int, mux: MuxConn) -> None:
        mux.send_frame(stream_id, FRAME_DATA, b"fast-chunk-1")
        mux.send_frame(stream_id, FRAME_DATA, b"fast-chunk-2")

    @server.route("/slow")
    def slow(stream_id: int, mux: MuxConn) -> None:
        for i in range(3):
            time.sleep(0.04)
            mux.send_frame(stream_id, FRAME_DATA, f"slow-{i}".encode())

    server.start()
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield server
    server.stop()
    t.join(timeout=2)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_encode_frame_round_trip():
    """encode_frame + recv_frame must reproduce (stream_id, frame_type, payload)."""
    raw = encode_frame(42, FRAME_DATA, b"hello")
    sock_a, sock_b = socket.socketpair()
    try:
        sock_a.sendall(raw)
        mux = MuxConn(sock_b)
        result = mux.recv_frame()
        assert result == (42, FRAME_DATA, b"hello")
    finally:
        sock_a.close()
        sock_b.close()


def test_single_stream_reassembly(mux_server):
    """A single stream's DATA frames reassemble into the full body."""
    mux = _client(mux_server.port)
    mux.send_frame(1, FRAME_HEADERS, b"/fast")
    bodies = _collect(mux, {1})
    mux.close()
    assert bodies[1] == b"fast-chunk-1fast-chunk-2"


def test_two_streams_independent_reassembly(mux_server):
    """Two concurrent streams each reassemble correctly, regardless of interleaving."""
    mux = _client(mux_server.port)
    mux.send_frame(1, FRAME_HEADERS, b"/slow")
    mux.send_frame(2, FRAME_HEADERS, b"/fast")
    bodies = _collect(mux, {1, 2})
    mux.close()
    assert bodies[1] == b"slow-0slow-1slow-2"
    assert bodies[2] == b"fast-chunk-1fast-chunk-2"


def test_fast_stream_completes_before_slow(mux_server):
    """The fast stream's END frame must arrive before the slow stream's END."""
    mux = _client(mux_server.port)
    mux.send_frame(1, FRAME_HEADERS, b"/slow")
    mux.send_frame(2, FRAME_HEADERS, b"/fast")

    end_times: dict[int, float] = {}
    bodies: dict[int, list[bytes]] = defaultdict(list)
    pending = {1, 2}
    while pending:
        frame = mux.recv_frame()
        assert frame is not None
        stream_id, frame_type, payload = frame
        if frame_type == FRAME_END:
            end_times[stream_id] = time.monotonic()
            pending.discard(stream_id)
        elif frame_type == FRAME_DATA:
            bodies[stream_id].append(payload)

    mux.close()
    assert end_times[2] < end_times[1], (
        f"/fast (stream 2) should finish before /slow (stream 1): "
        f"fast={end_times[2]:.3f} slow={end_times[1]:.3f}"
    )


def test_stream_ids_are_independent(mux_server):
    """Different stream IDs do not interfere — each gets its own handler."""
    mux = _client(mux_server.port)
    mux.send_frame(10, FRAME_HEADERS, b"/echo")
    mux.send_frame(20, FRAME_HEADERS, b"/echo")
    bodies = _collect(mux, {10, 20})
    mux.close()
    assert bodies[10] == b"echo"
    assert bodies[20] == b"echo"


def test_unknown_route_sends_end(mux_server):
    """A HEADERS frame for an unknown path still gets an END frame (404 data + END)."""
    mux = _client(mux_server.port)
    mux.send_frame(99, FRAME_HEADERS, b"/no-such-route")
    bodies = _collect(mux, {99})
    mux.close()
    assert b"404" in bodies[99]


def test_three_streams_all_complete(mux_server):
    """Three concurrent streams all reach END and bodies are correct."""
    mux = _client(mux_server.port)
    mux.send_frame(1, FRAME_HEADERS, b"/slow")
    mux.send_frame(2, FRAME_HEADERS, b"/fast")
    mux.send_frame(3, FRAME_HEADERS, b"/echo")
    bodies = _collect(mux, {1, 2, 3})
    mux.close()
    assert bodies[1] == b"slow-0slow-1slow-2"
    assert bodies[2] == b"fast-chunk-1fast-chunk-2"
    assert bodies[3] == b"echo"
