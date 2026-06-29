"""Phase 6 tests — Classic SSE: well-formed events, heartbeats, reconnect (Last-Event-ID)."""

import socket
import threading
import time

import pytest

from scratchplus.http import HTTPRequest
from scratchplus.sse import SSEServer, SSEWriter


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def sse_server():
    server = SSEServer(port=0, conn_timeout=10.0)

    @server.route("/events")
    def counter(req: HTTPRequest, writer: SSEWriter) -> None:
        last_id = req.headers.get("last-event-id", "")
        start = int(last_id) + 1 if last_id.isdigit() else 1

        writer.send_retry(1000)
        for i in range(start, start + 3):
            writer.send_comment("heartbeat")
            writer.send_event(f"msg {i}", event="tick", id=str(i))
        writer.send_event("bye", event="done")

    server.start()
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield server
    server.stop()
    t.join(timeout=2)


# ---------------------------------------------------------------------------
# Minimal SSE reader
# ---------------------------------------------------------------------------

def _raw_sse(port: int, last_event_id: str = "", timeout: float = 5.0):
    """
    Connect to /events, return (response_headers_str, list_of_parsed_events).
    Each event is a dict with keys: id, event, data, comment, retry.
    """
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    request_lines = [
        "GET /events HTTP/1.1",
        f"Host: 127.0.0.1:{port}",
        "Accept: text/event-stream",
        "Connection: keep-alive",
    ]
    if last_event_id:
        request_lines.append(f"Last-Event-ID: {last_event_id}")
    request_lines += ["", ""]
    sock.sendall("\r\n".join(request_lines).encode())

    buf = bytearray()
    while b"\r\n\r\n" not in buf:
        buf.extend(sock.recv(4096))
    idx = buf.index(b"\r\n\r\n")
    headers_raw = bytes(buf[:idx]).decode()
    buf = buf[idx + 4:]

    # Read the full stream body
    sock.settimeout(timeout)
    try:
        while True:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf.extend(chunk)
            except socket.timeout:
                break
    finally:
        sock.close()

    # Parse SSE frames
    events: list[dict] = []
    current: dict = {}
    for raw_line in bytes(buf).decode("utf-8", errors="replace").splitlines():
        line = raw_line.rstrip("\r")
        if line == "":
            if current:
                events.append(current)
                current = {}
        elif line.startswith(":"):
            events.append({"comment": line[1:].lstrip()})
        elif line.startswith("retry:"):
            events.append({"retry": int(line[6:].strip())})
        elif ":" in line:
            field, _, value = line.partition(":")
            current[field.strip()] = value.lstrip()

    if current:
        events.append(current)

    return headers_raw, events


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_content_type_event_stream(sse_server):
    """Response must have Content-Type: text/event-stream."""
    headers, _ = _raw_sse(sse_server.port)
    header_lower = headers.lower()
    assert "200" in headers.split("\r\n")[0]
    assert "text/event-stream" in header_lower


def test_events_have_required_fields(sse_server):
    """Each non-comment event must have at least a data field."""
    _, events = _raw_sse(sse_server.port)
    data_events = [e for e in events if "data" in e]
    assert len(data_events) >= 4, f"Expected at least 4 data events, got: {data_events}"
    for ev in data_events:
        assert ev["data"], f"Empty data field in event: {ev}"


def test_events_have_id_and_event_fields(sse_server):
    """The 'tick' events must carry both id and event fields."""
    _, events = _raw_sse(sse_server.port)
    tick_events = [e for e in events if e.get("event") == "tick"]
    assert len(tick_events) == 3, f"Expected 3 tick events, got: {tick_events}"
    for ev in tick_events:
        assert "id" in ev, f"Missing id in tick event: {ev}"
        assert ev["id"].isdigit(), f"id is not numeric: {ev}"


def test_heartbeat_comments_emitted(sse_server):
    """Server must emit at least one comment (heartbeat) line."""
    _, events = _raw_sse(sse_server.port)
    comments = [e for e in events if "comment" in e]
    assert len(comments) >= 3, f"Expected >= 3 heartbeat comments, got: {comments}"
    for c in comments:
        assert c["comment"] == "heartbeat"


def test_retry_field_present(sse_server):
    """Server must send a retry: directive."""
    _, events = _raw_sse(sse_server.port)
    retries = [e for e in events if "retry" in e]
    assert retries, "No retry directive found in SSE stream"
    assert retries[0]["retry"] == 1000


def test_reconnect_last_event_id(sse_server):
    """
    After first stream (ids 1–3), reconnect with Last-Event-ID: 2.
    Server must resume from id 3 (not restart from 1).
    """
    _, first_events = _raw_sse(sse_server.port)
    tick_events = [e for e in first_events if e.get("event") == "tick"]
    assert tick_events, "No tick events in first stream"

    # Reconnect from id 2: server should send ids 3, 4, 5
    _, second_events = _raw_sse(sse_server.port, last_event_id="2")
    second_tick_ids = [int(e["id"]) for e in second_events if e.get("event") == "tick"]
    assert second_tick_ids, "No tick events in reconnected stream"
    assert min(second_tick_ids) == 3, (
        f"Expected reconnect to start from id 3, got ids: {second_tick_ids}"
    )


def test_done_event_closes_stream(sse_server):
    """Last emitted event must be the 'done' event."""
    _, events = _raw_sse(sse_server.port)
    data_events = [e for e in events if "data" in e]
    assert data_events[-1].get("event") == "done"
    assert data_events[-1]["data"] == "bye"


def test_unknown_path_returns_404(sse_server):
    """Requests to an unregistered path must get a 404."""
    sock = socket.create_connection(("127.0.0.1", sse_server.port), timeout=5.0)
    sock.sendall(b"GET /missing HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
    buf = bytearray()
    sock.settimeout(3.0)
    try:
        while b"\r\n\r\n" not in buf:
            buf.extend(sock.recv(4096))
    except socket.timeout:
        pass
    finally:
        sock.close()
    status_line = bytes(buf).split(b"\r\n")[0].decode()
    assert "404" in status_line
