"""Phase 2 example: HTTP/1.1 server with four routes.

Usage:
    python examples/http_server.py          # listens on :8080
    python examples/http_server.py 9090     # custom port

Routes:
    GET  /           → "Hello from Scratch+!"
    GET  /health     → {"status": "ok"}
    GET  /delay?ms=N → sleeps N ms, returns {"slept_ms": N}
    POST /echo       → {"body": "<request body>"}
"""

import sys
import time

sys.path.insert(0, ".")

from scratchplus.http import HTTPRequest, HTTPServer, json_response, text_response


def make_app(port: int = 8080) -> HTTPServer:
    server = HTTPServer(port=port)

    @server.route("GET", "/")
    def index(req: HTTPRequest) -> None:
        return text_response("Hello from Scratch+!")

    @server.route("GET", "/health")
    def health(req: HTTPRequest) -> None:
        return json_response({"status": "ok"})

    @server.route("GET", "/delay")
    def delay(req: HTTPRequest) -> None:
        ms = int(req.query_params.get("ms", 0))
        ms = max(0, min(ms, 10_000))
        time.sleep(ms / 1000)
        return json_response({"slept_ms": ms})

    @server.route("POST", "/echo")
    def echo(req: HTTPRequest) -> None:
        return json_response({"body": req.body.decode("utf-8", errors="replace")})

    return server


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    server = make_app(port)
    server.start()
    print(f"Listening on http://127.0.0.1:{server.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.stop()
