"""Pipelining+ / HTTP/1.15 — standalone server.

Routes
------
GET  /fast            – instant JSON response
GET  /slow?ms=<n>     – sleeps ms milliseconds (default 500), then responds
GET  /health          – liveness check

Run:
    python examples/pipelining_plus_server.py [port]

The server exits on Ctrl-C.
"""

import json
import sys
import time

sys.path.insert(0, ".")

from scratchplus.http import HTTPRequest, json_response
from scratchplus.pipelining_plus import PipeliningPlusServer


def build_server(port: int = 8080) -> PipeliningPlusServer:
    server = PipeliningPlusServer(port=port, conn_timeout=60.0)

    @server.route("GET", "/health")
    def health(req: HTTPRequest):
        return json_response({"status": "ok"})

    @server.route("GET", "/fast")
    def fast(req: HTTPRequest):
        return json_response({"route": "fast", "delay_ms": 0})

    @server.route("GET", "/slow")
    def slow(req: HTTPRequest):
        ms = int(req.query_params.get("ms", "500"))
        time.sleep(ms / 1000)
        return json_response({"route": "slow", "delay_ms": ms})

    return server


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    server = build_server(port)
    server.start()
    print(f"Pipelining+ server listening on port {server.port}")
    print("Press Ctrl-C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.stop()
        print("\nStopped.")
