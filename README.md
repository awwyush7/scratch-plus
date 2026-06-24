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

See [ROADMAP.md](ROADMAP.md) for phase checklist and [SPEC.md](SPEC.md) for full spec.

## Running tests

```bash
python -m pytest
```

## Rules

- Python 3, standard library only (socket, selectors, threading, asyncio)
- No web frameworks, no third-party HTTP libs
- Each phase: implement → test → document tradeoffs → commit
