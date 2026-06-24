"""
Echo client — Phase 1 example.

Connects to the echo server, reads lines from stdin, sends them, and prints
what the server echoes back.

Usage:
    python examples/echo_client.py --host 127.0.0.1 --port 9000
    echo "hello world" | python examples/echo_client.py --port 9000
"""

import argparse
import socket
import sys


def run(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=10) as s:
        print(f"Connected to {host}:{port} — type lines, Ctrl-D/Ctrl-C to quit")
        try:
            for line in sys.stdin:
                payload = line.encode()
                s.sendall(payload)
                # recv the same number of bytes back
                received = bytearray()
                while len(received) < len(payload):
                    chunk = s.recv(len(payload) - len(received))
                    if not chunk:
                        print("Server closed connection.")
                        return
                    received.extend(chunk)
                sys.stdout.write(f"echo: {received.decode()}")
                sys.stdout.flush()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Echo client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    run(args.host, args.port)
