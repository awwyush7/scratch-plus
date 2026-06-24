# Scratch+ — from-scratch networking/protocols lab

Implement classic networking/web protocols from scratch, then add small experimental
"+" extensions to understand protocol design tradeoffs.

**Implementation language: Python 3, standard library only** (socket, selectors,
threading, asyncio). No web frameworks, no third-party HTTP libs — the point is to
build the protocol machinery by hand. Tests may use pytest.

Each phase should: read this SPEC and the existing code, implement the phase, add a
runnable example and tests, document the design tradeoffs learned (this is a learning
lab), and commit.

## References (for behavior, not code to copy)
- HTTP/1.1: follow RFC 9112 concepts — message parsing, framing, connection
  management, persistence, pipelining.
- HTTP/2: later, only as inspiration for multiplexing — streams, frames, concurrent
  exchanges.
- WebSocket: later, RFC 6455 — HTTP upgrade handshake + message framing over TCP.
- SSE: later, EventSource / text/event-stream behavior.

## Phase 0: Repo Setup
Base repo, tooling, docs, examples.
Deliverables: basic README, project roadmap (ROADMAP.md), docs/ folder, common
utilities (e.g. a `scratchplus/` package), test setup (pytest), benchmark/ folder.

## Phase 1: TCP Core
From-scratch TCP server foundation.
Features: TCP listener, accept loop, connection abstraction, read/write buffers,
connection timeout, graceful close, simple echo server, blocking server first,
async/threaded version later.
Output: examples/echo_server, examples/echo_client.

## Phase 2: Classic HTTP/1.1 Subset
Minimal HTTP/1.1 server on top of the TCP core.
Features: parse request line, parse headers, parse Content-Length, route handling,
response builder, keep-alive support, connection close support, basic static
response, basic JSON response.
Out of scope initially: chunked encoding, compression, caching, TLS, full RFC
compliance.
Output routes: GET / , GET /health , GET /delay?ms=1000 , POST /echo.

## Phase 3: Classic HTTP/1.1 Pipelining
Normal HTTP/1.1 pipelining.
Behavior: client sends multiple requests on the same TCP connection without waiting;
server reads multiple requests; server returns responses in the same order as
requests; demonstrate application-level head-of-line (HOL) blocking.
Example: client sends Req1 /slow, Req2 /fast, Req3 /fast → server responds Resp1
/slow, Resp2 /fast, Resp3 /fast (in order; /slow blocks the rest).

## Phase 4: Pipelining+ / HTTP/1.15 (the extension)
Allow pipelined requests with OUT-OF-ORDER complete responses using Request-IDs.
Request adds `Request-ID:` header; responses echo `Request-ID:` and may return in any
order as each completes.
Rules: one TCP connection; multiple pipelined requests; each request has a
Request-ID; server can process requests concurrently; server can send COMPLETE
responses out of order; client maps response→request by Request-ID; NO interleaved
response chunks; NOT full HTTP/2-style multiplexing.
Output: examples/pipelining_plus_server, examples/pipelining_plus_client, and a
benchmark comparing sequential HTTP/1.1 vs classic pipelining vs Pipelining+.

## Phase 5: Benchmarking and Reports
Repeatable benchmarks.
Workloads: all fast; one slow then many fast; mixed delay; large response then small.
Metrics: total completion time, per-request latency, p50/p95, connection count,
bytes sent/received, HOL-blocking demonstration.
Output: benchmark script + markdown report (graphs optional).

## Phase 6: Classic SSE
Features: text/event-stream, data:/event:/id: fields, heartbeat comments, reconnect
demo. Keep extension folder empty: sse_plus/.

## Phase 7: Classic WebSocket
Features: HTTP upgrade handshake, Sec-WebSocket-Key/Accept, text frames, ping/pong,
close frame. Keep extension folder empty: websocket_plus/.

## Phase 8: Classic Multiplexing Toy
After HTTP/1.15 is complete. Define frame header (stream ID, frame type, payload
length), DATA/HEADERS/END frames. Keep extension folder empty: multiplexing_plus/.
