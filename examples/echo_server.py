"""
Echo server — Phase 1 example.

Blocking mode (default):
    python examples/echo_server.py
    python examples/echo_server.py --mode blocking --port 9000

Async mode:
    python examples/echo_server.py --mode async --port 9000
"""

import argparse
import asyncio
import socket
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scratchplus.tcp import RECV_SIZE, AsyncTCPServer, Connection, TCPServer


# ---------------------------------------------------------------------------
# Blocking handler (runs in its own thread, one per connection)
# ---------------------------------------------------------------------------

def blocking_echo_handler(conn: Connection) -> None:
    while True:
        try:
            data = conn.recv()
        except (socket.timeout, OSError):
            break
        if not data:
            break
        conn.send(data)
        conn.flush()


# ---------------------------------------------------------------------------
# Async handler (runs as a coroutine in the event loop)
# ---------------------------------------------------------------------------

async def async_echo_handler(
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
# Entry points
# ---------------------------------------------------------------------------

def run_blocking(host: str, port: int) -> None:
    server = TCPServer(host=host, port=port, conn_timeout=30.0)
    server.start()
    print(f"[blocking] echo server on {host or '0.0.0.0'}:{server.port}  (Ctrl-C to stop)")
    try:
        server.serve_forever(blocking_echo_handler)
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


def run_async(host: str, port: int) -> None:
    server = AsyncTCPServer(host=host, port=port, conn_timeout=30.0)

    async def _main() -> None:
        print(f"[async] echo server starting on {host or '0.0.0.0'}:{port}  (Ctrl-C to stop)")
        await server.start_and_serve(async_echo_handler)

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Echo server")
    parser.add_argument("--mode", choices=["blocking", "async"], default="blocking")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--host", default="")
    args = parser.parse_args()

    if args.mode == "blocking":
        run_blocking(args.host, args.port)
    else:
        run_async(args.host, args.port)
