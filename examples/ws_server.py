"""WebSocket echo server — run with: python examples/ws_server.py"""

import threading
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scratchplus.websocket import WSConnection, WSServer, OP_TEXT, OP_BIN

PORT = 8765


def main() -> None:
    server = WSServer(port=PORT, conn_timeout=60.0)

    @server.route("/echo")
    def echo(ws: WSConnection) -> None:
        print(f"[server] client connected")
        while True:
            frame = ws.recv_frame()
            if frame is None:
                print("[server] connection closed")
                break
            opcode, payload = frame
            if opcode == OP_TEXT:
                text = payload.decode()
                print(f"[server] echo text: {text!r}")
                ws.send_text(text)
            elif opcode == OP_BIN:
                ws.send_binary(payload)

    server.start()
    print(f"WebSocket echo server listening on ws://127.0.0.1:{PORT}/echo")
    print("Press Ctrl-C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
