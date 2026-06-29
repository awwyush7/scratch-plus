"""Phase 8 example: multiplexing toy server.

Two streams can progress concurrently over one TCP connection.
/slow sends its data in 3 chunks (50 ms apart); /fast replies immediately.
"""

import time

from scratchplus.multiplexing import FRAME_DATA, MuxConn, MuxServer

server = MuxServer(port=9080)


@server.route("/fast")
def fast(stream_id: int, mux: MuxConn) -> None:
    mux.send_frame(stream_id, FRAME_DATA, b"fast-chunk-1")
    mux.send_frame(stream_id, FRAME_DATA, b"fast-chunk-2")


@server.route("/slow")
def slow(stream_id: int, mux: MuxConn) -> None:
    for i in range(3):
        time.sleep(0.05)
        mux.send_frame(stream_id, FRAME_DATA, f"slow-chunk-{i + 1}".encode())


if __name__ == "__main__":
    server.start()
    print(f"Mux server listening on port {server.port}")
    server.serve_forever()
