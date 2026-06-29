"""Phase 8 example: multiplexing toy client.

Opens two concurrent logical streams over one TCP connection:
  Stream 1 → /slow   (3 × 50 ms chunks)
  Stream 2 → /fast   (2 chunks, immediate)

Frames arrive interleaved; the client reassembles each stream independently.
"""

import socket
import threading
import time
from collections import defaultdict

from scratchplus.multiplexing import FRAME_END, FRAME_HEADERS, MuxConn, encode_frame

HOST, PORT = "127.0.0.1", 9080


def main() -> None:
    sock = socket.create_connection((HOST, PORT), timeout=10.0)
    mux = MuxConn(sock)

    streams: dict[int, list[bytes]] = defaultdict(list)
    done: dict[int, threading.Event] = {1: threading.Event(), 2: threading.Event()}

    def reader() -> None:
        while True:
            frame = mux.recv_frame()
            if frame is None:
                break
            stream_id, frame_type, payload = frame
            if frame_type == FRAME_END:
                if stream_id in done:
                    done[stream_id].set()
            else:
                streams[stream_id].append(payload)
                print(f"  [stream {stream_id}] DATA: {payload.decode()}")

    t = threading.Thread(target=reader, daemon=True)
    t.start()

    # Open both streams immediately — HEADERS for stream 1 (/slow) then stream 2 (/fast)
    t0 = time.monotonic()
    mux.send_frame(1, FRAME_HEADERS, b"/slow")
    mux.send_frame(2, FRAME_HEADERS, b"/fast")

    done[2].wait(timeout=5.0)
    fast_done = time.monotonic() - t0
    done[1].wait(timeout=5.0)
    slow_done = time.monotonic() - t0

    mux.close()

    print(f"\nStream 2 (/fast) completed in {fast_done * 1000:.0f} ms")
    print(f"Stream 1 (/slow) completed in {slow_done * 1000:.0f} ms")
    print(f"\nStream 1 body: {b''.join(streams[1]).decode()}")
    print(f"Stream 2 body: {b''.join(streams[2]).decode()}")

    assert fast_done < slow_done, "/fast must finish before /slow"
    print("\nInterleaving verified: /fast completed before /slow.")


if __name__ == "__main__":
    main()
