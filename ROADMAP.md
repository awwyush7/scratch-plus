# Scratch+ Roadmap

- [x] **Phase 0** — Repo Setup: README, ROADMAP, docs/, scratchplus/ package, pytest, benchmark/
- [x] **Phase 1** — TCP Core: listener, accept loop, connection abstraction, read/write buffers, timeout, graceful close, echo server (blocking → async/threaded)
- [x] **Phase 2** — Classic HTTP/1.1 Subset: request/response parsing, routing, keep-alive, JSON/static responses
- [x] **Phase 3** — Classic HTTP/1.1 Pipelining: multi-request on one connection, in-order responses, HOL-blocking demo
- [x] **Phase 4** — Pipelining+ / HTTP/1.15: out-of-order responses via Request-ID, concurrent processing, benchmark vs classic
- [x] **Phase 5** — Benchmarking and Reports: repeatable workloads, latency metrics, p50/p95, HOL-blocking measurements
- [x] **Phase 6** — Classic SSE: text/event-stream, data/event/id fields, heartbeat, reconnect demo
- [x] **Phase 7** — Classic WebSocket: HTTP upgrade, Sec-WebSocket-Key/Accept, text frames, ping/pong, close frame
- [x] **Phase 8** — Classic Multiplexing Toy: frame header (stream ID, type, payload length), DATA/HEADERS/END frames; true interleaving contrast with Pipelining+
