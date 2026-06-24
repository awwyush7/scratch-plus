"""Phase 3 example — HTTP/1.1 classic pipelining with HOL-blocking demo.

Starts an embedded server, then pipelines three requests on one TCP connection:
  Req1  GET /slow   (300 ms simulated delay)
  Req2  GET /fast
  Req3  GET /fast

All three requests are sent before reading a single response.
The server processes them in order, so /fast responses are held up
until /slow finishes — application-level head-of-line (HOL) blocking.

Usage:
    python examples/pipelining_client.py
"""

import json
import socket
import sys
import threading
import time

sys.path.insert(0, ".")

from scratchplus.http import HTTPRequest, HTTPServer, json_response


# ---------------------------------------------------------------------------
# Embedded server with /slow and /fast routes
# ---------------------------------------------------------------------------

def make_server() -> HTTPServer:
    server = HTTPServer(port=0, conn_timeout=10.0)

    @server.route("GET", "/slow")
    def slow(req: HTTPRequest):
        time.sleep(0.30)
        return json_response({"route": "slow", "delay_ms": 300})

    @server.route("GET", "/fast")
    def fast(req: HTTPRequest):
        return json_response({"route": "fast", "delay_ms": 0})

    return server


# ---------------------------------------------------------------------------
# Minimal pipelining client
# ---------------------------------------------------------------------------

class PipelinedClient:
    def __init__(self, host: str, port: int) -> None:
        self._sock = socket.create_connection((host, port), timeout=10.0)
        self._sock.settimeout(10.0)
        self._buf = bytearray()

    def send(self, method: str, path: str) -> None:
        raw = (
            f"{method} {path} HTTP/1.1\r\n"
            f"Host: 127.0.0.1\r\n"
            f"Connection: keep-alive\r\n"
            f"Content-Length: 0\r\n"
            f"\r\n"
        ).encode()
        self._sock.sendall(raw)

    def recv_one(self) -> tuple[int, bytes]:
        while b"\r\n\r\n" not in self._buf:
            self._buf.extend(self._sock.recv(4096))
        idx = self._buf.index(b"\r\n\r\n")
        header_data = bytes(self._buf[:idx]).decode()
        del self._buf[: idx + 4]
        lines = header_data.split("\r\n")
        status = int(lines[0].split()[1])
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        clen = int(headers.get("content-length", 0))
        while len(self._buf) < clen:
            self._buf.extend(self._sock.recv(4096))
        body = bytes(self._buf[:clen])
        del self._buf[:clen]
        return status, body

    def close(self) -> None:
        self._sock.close()


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def main() -> None:
    server = make_server()
    server.start()
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

    port = server.port
    print(f"Server listening on port {port}")
    print()

    client = PipelinedClient("127.0.0.1", port)

    print("=== Pipelining: fire all three requests before reading any response ===")
    print("  Req1  GET /slow   (300 ms)")
    print("  Req2  GET /fast")
    print("  Req3  GET /fast")
    print()

    t0 = time.monotonic()

    # Send all requests without waiting
    client.send("GET", "/slow")
    client.send("GET", "/fast")
    client.send("GET", "/fast")
    t_sent = time.monotonic()
    print(f"  [+{(t_sent - t0)*1000:6.1f} ms]  all three requests sent")

    # Now read responses in order
    for req_num in (1, 2, 3):
        status, body = client.recv_one()
        t_recv = time.monotonic()
        data = json.loads(body)
        print(
            f"  [+{(t_recv - t0)*1000:6.1f} ms]  Resp{req_num}  "
            f"status={status}  route={data['route']}"
        )

    client.close()
    server.stop()

    print()
    print("HOL-blocking observed: Resp2 and Resp3 (/fast) could not arrive")
    print("until Resp1 (/slow, 300 ms) was sent — even though they completed faster.")


if __name__ == "__main__":
    main()
