"""SSE example server: emits a counter stream with heartbeats and reconnect support.

Run: python examples/sse_server.py
Then open http://127.0.0.1:8080/events in a browser or run examples/sse_client.py.
"""

import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from scratchplus.http import HTTPRequest
from scratchplus.sse import SSEServer, SSEWriter

server = SSEServer(port=8080)


@server.route("/events")
def counter_stream(req: HTTPRequest, writer: SSEWriter) -> None:
    # Resume from Last-Event-ID on reconnect
    last_id = req.headers.get("last-event-id", "")
    start = int(last_id) + 1 if last_id.isdigit() else 1

    writer.send_retry(3000)  # advise 3 s reconnect delay
    for i in range(start, start + 5):
        writer.send_comment("heartbeat")
        writer.send_event(f"tick {i}", event="tick", id=str(i))
        time.sleep(0.5)
    writer.send_event("stream complete", event="done")


if __name__ == "__main__":
    server.start()
    print(f"SSE server listening on http://127.0.0.1:{server.port}/events")
    server.serve_forever()
