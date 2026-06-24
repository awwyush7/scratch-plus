"""Phase 1 tests — TCP core: blocking server and async server round-trip."""

import asyncio
import socket
import threading
import time

import pytest

from scratchplus.tcp import RECV_SIZE, AsyncTCPServer, Connection, TCPServer


# ---------------------------------------------------------------------------
# Shared echo handlers
# ---------------------------------------------------------------------------

def _blocking_echo(conn: Connection) -> None:
    while True:
        try:
            data = conn.recv()
        except (socket.timeout, OSError):
            break
        if not data:
            break
        conn.send(data)
        conn.flush()


async def _async_echo(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    timeout: float,
) -> None:
    while True:
        try:
            data = await asyncio.wait_for(reader.read(RECV_SIZE), timeout=timeout)
        except asyncio.TimeoutError:
            break
        if not data:
            break
        writer.write(data)
        await writer.drain()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def echo_server():
    """Blocking TCPServer on an ephemeral port."""
    server = TCPServer(port=0, conn_timeout=5.0)
    server.start()
    t = threading.Thread(target=server.serve_forever, args=(_blocking_echo,), daemon=True)
    t.start()
    yield server
    server.stop()
    t.join(timeout=2)


@pytest.fixture
def async_echo_server():
    """AsyncTCPServer on an ephemeral port, driven by a background event loop."""
    server = AsyncTCPServer(port=0, conn_timeout=5.0)
    loop = asyncio.new_event_loop()

    def _run() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.start_and_serve(_async_echo))

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    assert server._started.wait(timeout=5), "async server did not start in time"
    yield server
    loop.call_soon_threadsafe(server.stop)
    t.join(timeout=2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _send_recv(host: str, port: int, payload: bytes) -> bytes:
    with socket.create_connection((host, port), timeout=5) as s:
        s.sendall(payload)
        received = bytearray()
        while len(received) < len(payload):
            chunk = s.recv(len(payload) - len(received))
            assert chunk, "unexpected EOF from server"
            received.extend(chunk)
        return bytes(received)


# ---------------------------------------------------------------------------
# Blocking server tests
# ---------------------------------------------------------------------------

def test_blocking_echo_roundtrip(echo_server):
    assert _send_recv("127.0.0.1", echo_server.port, b"hello") == b"hello"


def test_blocking_echo_multiple_messages(echo_server):
    with socket.create_connection(("127.0.0.1", echo_server.port), timeout=5) as s:
        for msg in [b"foo", b"bar", b"baz"]:
            s.sendall(msg)
            got = bytearray()
            while len(got) < len(msg):
                chunk = s.recv(len(msg) - len(got))
                assert chunk
                got.extend(chunk)
            assert bytes(got) == msg


def test_blocking_echo_large_payload(echo_server):
    payload = b"x" * 65536
    assert _send_recv("127.0.0.1", echo_server.port, payload) == payload


def test_blocking_graceful_close(echo_server):
    with socket.create_connection(("127.0.0.1", echo_server.port), timeout=5) as s:
        s.sendall(b"ping")
        got = bytearray()
        while len(got) < 4:
            chunk = s.recv(4 - len(got))
            assert chunk
            got.extend(chunk)
        assert bytes(got) == b"ping"
    # Connection closed by client — server should handle it without crashing
    time.sleep(0.05)
    # Server still accepts new connections
    assert _send_recv("127.0.0.1", echo_server.port, b"still alive") == b"still alive"


def test_blocking_concurrent_clients(echo_server):
    results = {}
    errors = []

    def client(i: int) -> None:
        try:
            msg = f"client{i}".encode()
            results[i] = _send_recv("127.0.0.1", echo_server.port, msg)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=client, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert not errors
    for i in range(5):
        assert results[i] == f"client{i}".encode()


def test_connection_timeout(echo_server):
    """Server closes an idle connection after conn_timeout (5 s in fixture).
    We just verify the server doesn't block forever on an idle socket.
    The test itself uses a short-lived connection, not the 5-s timeout."""
    # Create a connection, send nothing, then let it drop — server should not hang.
    s = socket.create_connection(("127.0.0.1", echo_server.port), timeout=5)
    s.close()
    time.sleep(0.05)
    assert _send_recv("127.0.0.1", echo_server.port, b"ok") == b"ok"


# ---------------------------------------------------------------------------
# Async server tests
# ---------------------------------------------------------------------------

def test_async_echo_roundtrip(async_echo_server):
    assert _send_recv("127.0.0.1", async_echo_server.port, b"hello async") == b"hello async"


def test_async_echo_large_payload(async_echo_server):
    payload = b"a" * 32768
    assert _send_recv("127.0.0.1", async_echo_server.port, payload) == payload


def test_async_echo_concurrent_clients(async_echo_server):
    results = {}
    errors = []

    def client(i: int) -> None:
        try:
            msg = f"async{i}".encode()
            results[i] = _send_recv("127.0.0.1", async_echo_server.port, msg)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=client, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert not errors
    for i in range(5):
        assert results[i] == f"async{i}".encode()


def test_async_graceful_close(async_echo_server):
    with socket.create_connection(("127.0.0.1", async_echo_server.port), timeout=5) as s:
        s.sendall(b"bye")
        got = bytearray()
        while len(got) < 3:
            chunk = s.recv(3 - len(got))
            assert chunk
            got.extend(chunk)
        assert bytes(got) == b"bye"
    time.sleep(0.05)
    assert _send_recv("127.0.0.1", async_echo_server.port, b"still up") == b"still up"
