"""Minimal raw-socket WebSocket client — run with: python examples/ws_client.py

Connects to the echo server, sends two text messages, one ping, then closes.
No external libraries — all framing is done by hand.
"""

import base64
import os
import socket
import struct
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scratchplus.websocket import (
    OP_CLOSE, OP_PONG, OP_TEXT,
    compute_accept, make_client_frame,
)

HOST = "127.0.0.1"
PORT = 8765
PATH = "/echo"


def recv_frame(sock: socket.socket) -> tuple[int, bytes]:
    """Read one unmasked WebSocket frame from a raw socket."""
    def recv_exact(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("connection closed mid-frame")
            buf += chunk
        return buf

    b0, b1 = recv_exact(2)
    opcode = b0 & 0x0F
    payload_len = b1 & 0x7F

    if payload_len == 126:
        payload_len = struct.unpack("!H", recv_exact(2))[0]
    elif payload_len == 127:
        payload_len = struct.unpack("!Q", recv_exact(8))[0]

    return opcode, recv_exact(payload_len)


def main() -> None:
    # --- HTTP upgrade handshake ---
    raw_key = base64.b64encode(os.urandom(16)).decode()
    expected_accept = compute_accept(raw_key)

    sock = socket.create_connection((HOST, PORT), timeout=10.0)
    handshake = (
        f"GET {PATH} HTTP/1.1\r\n"
        f"Host: {HOST}:{PORT}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {raw_key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n"
    )
    sock.sendall(handshake.encode())

    # Read response headers
    buf = b""
    while b"\r\n\r\n" not in buf:
        buf += sock.recv(4096)
    headers_raw = buf[:buf.index(b"\r\n\r\n")].decode()
    print(f"[client] server response:\n{headers_raw}\n")

    status = headers_raw.split("\r\n")[0]
    assert "101" in status, f"Expected 101, got: {status}"

    # Verify accept key
    for line in headers_raw.split("\r\n"):
        if line.lower().startswith("sec-websocket-accept:"):
            got = line.split(":", 1)[1].strip()
            assert got == expected_accept, f"Accept mismatch: {got!r} != {expected_accept!r}"
            print(f"[client] Sec-WebSocket-Accept verified: {got}")

    # --- Text echo ---
    for msg in ["Hello, WebSocket!", "second message"]:
        sock.sendall(make_client_frame(OP_TEXT, msg.encode()))
        opcode, payload = recv_frame(sock)
        assert opcode == OP_TEXT
        assert payload.decode() == msg
        print(f"[client] echo OK: {payload.decode()!r}")

    # --- Ping / Pong ---
    ping_data = b"ping-payload"
    sock.sendall(make_client_frame(0x9, ping_data))  # opcode 9 = ping
    opcode, payload = recv_frame(sock)
    assert opcode == OP_PONG, f"Expected pong (0xA), got {opcode:#x}"
    assert payload == ping_data
    print(f"[client] pong received with payload {payload!r}")

    # --- Clean close ---
    close_frame = make_client_frame(OP_CLOSE, struct.pack("!H", 1000) + b"bye")
    sock.sendall(close_frame)
    opcode, _ = recv_frame(sock)
    assert opcode == OP_CLOSE, f"Expected close (0x8), got {opcode:#x}"
    print("[client] clean close handshake complete")

    sock.close()
    print("[client] done")


if __name__ == "__main__":
    main()
