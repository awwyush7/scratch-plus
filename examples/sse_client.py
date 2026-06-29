"""SSE example client: prints streamed events, then reconnects once with Last-Event-ID.

Run: python examples/sse_client.py [port]   (default 8080)
"""

import socket
import sys

HOST = "127.0.0.1"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080


def connect_and_read(port: int, last_event_id: str = "") -> str:
    """Open a raw SSE connection, print all events, return the last seen event ID."""
    sock = socket.create_connection((HOST, port), timeout=10)
    request_lines = [
        f"GET /events HTTP/1.1",
        f"Host: {HOST}:{port}",
        "Accept: text/event-stream",
        "Cache-Control: no-cache",
        "Connection: keep-alive",
    ]
    if last_event_id:
        request_lines.append(f"Last-Event-ID: {last_event_id}")
    request_lines.append("")
    request_lines.append("")
    sock.sendall("\r\n".join(request_lines).encode())

    buf = bytearray()
    while b"\r\n\r\n" not in buf:
        buf.extend(sock.recv(4096))
    idx = buf.index(b"\r\n\r\n")
    status_line = bytes(buf[:idx]).decode().split("\r\n")[0]
    print(f"  [response] {status_line}")
    buf = buf[idx + 4:]

    current_id = last_event_id
    pending_fields: list[str] = []

    sock.settimeout(5.0)
    try:
        while True:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf.extend(chunk)
            except socket.timeout:
                break

            while b"\n" in buf:
                nl = buf.index(b"\n")
                raw_line = bytes(buf[:nl]).decode("utf-8", errors="replace").rstrip("\r")
                buf = buf[nl + 1:]

                if raw_line == "":
                    if pending_fields:
                        print(f"  [event]     {' | '.join(pending_fields)}")
                    pending_fields = []
                elif raw_line.startswith(":"):
                    print(f"  [heartbeat]{raw_line[1:]}")
                elif raw_line.startswith("id:"):
                    current_id = raw_line[3:].strip()
                    pending_fields.append(f"id={current_id}")
                elif raw_line.startswith("event:"):
                    pending_fields.append(f"event={raw_line[6:].strip()}")
                elif raw_line.startswith("data:"):
                    pending_fields.append(f"data={raw_line[5:].strip()}")
                elif raw_line.startswith("retry:"):
                    print(f"  [retry]     {raw_line[6:].strip()} ms")
    finally:
        sock.close()

    return current_id


if __name__ == "__main__":
    print("=== First connection ===")
    last_id = connect_and_read(PORT)

    print(f"\n=== Reconnect with Last-Event-ID: {last_id!r} ===")
    connect_and_read(PORT, last_id)
