# Scratch+

From-scratch networking/protocols lab. Implements classic networking protocols
using Python 3 standard library only, then adds small experimental "+" extensions
to understand protocol design tradeoffs.

## Project structure

```
scratchplus/      # shared utilities (logging, buffers, helpers)
tests/            # pytest test suite
examples/         # runnable examples per phase
benchmark/        # benchmark scripts and reports
docs/             # design notes and tradeoff writeups
```

## Phases

| Phase | Topic |
|-------|-------|
| 0 | Repo Setup |
| 1 | TCP Core (echo server, async variant) |
| 2 | Classic HTTP/1.1 Subset |
| 3 | Classic HTTP/1.1 Pipelining + HOL-blocking demo |
| 4 | Pipelining+ / HTTP/1.15 (out-of-order responses via Request-ID) |
| 5 | Benchmarking and Reports |
| 6 | Classic SSE (text/event-stream, heartbeat, reconnect) |
| 7 | Classic WebSocket (upgrade handshake, frames, ping/pong) |
| 8 | Classic Multiplexing Toy (binary frames, stream IDs, true interleaving) |

See [ROADMAP.md](ROADMAP.md) for phase checklist and [SPEC.md](SPEC.md) for full spec.

## Running tests

```bash
python -m pytest
```

## Rules

- Python 3, standard library only (socket, selectors, threading, asyncio)
- No web frameworks, no third-party HTTP libs
- Each phase: implement → test → document tradeoffs → commit
