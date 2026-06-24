# TCP: Blocking (threaded) vs Async — tradeoffs

## What we built

**Blocking / thread-per-connection (`TCPServer`)**  
Each accepted connection gets its own daemon thread. The socket has a settimeout so
`recv()` raises `socket.timeout` after the idle period instead of blocking forever.

**Async / coroutine-per-connection (`AsyncTCPServer`)**  
A single asyncio event loop drives all connections as coroutines. No threads for I/O;
blocking waits are replaced with `await` suspensions. `asyncio.wait_for` enforces
per-connection timeout without a per-socket OS timer.

---

## Tradeoff table

| Dimension | Blocking (threaded) | Async |
|---|---|---|
| **Concurrency model** | OS threads; each blocked on its socket | Single-threaded cooperative multitasking |
| **Memory per connection** | ~8 MB stack per thread (OS default) | ~1–2 KB per coroutine frame |
| **Max connections** | ~1 000–10 000 before thread overhead hurts | Hundreds of thousands (memory bound, not thread bound) |
| **CPU-bound work** | Can use multiple cores via threads (GIL notwithstanding for I/O-bound) | Blocks the event loop; must push to a thread pool (`run_in_executor`) |
| **Code style** | Sequential — easy to read, debug, and profile | Requires async/await throughout; stack traces are harder to read |
| **Timeout mechanism** | `socket.settimeout` — OS-level | `asyncio.wait_for` — runtime-level; doesn't exist until awaited |
| **When to choose** | Few hundred connections, simple protocol, learner-friendly | High fan-out servers (proxies, chat, push), I/O-heavy workloads |

---

## Why blocking is a good starting point for learning

- You can set a breakpoint inside the handler and step through a real connection.
- Each connection's state lives in local variables on the call stack, not scattered
  across coroutine frames. "What is this connection doing?" is answered by `pdb bt`.
- Errors surface as ordinary Python exceptions with ordinary tracebacks.

## Why async is the right tool at scale

A thread costs ~1 MB of RAM and an OS scheduler slot. A coroutine costs a few KB of
heap. At 10 000 simultaneous connections the threading model needs ~10 GB just for
stacks; the async model needs a few hundred MB for coroutine frames.

Async also makes it easier to express fan-out: one coroutine can `await` many I/O
sources without spawning threads. This maps naturally to multiplexed protocols
(HTTP/2, WebSocket) that we'll implement in later phases.

---

## The GIL and where threads still win

Python's Global Interpreter Lock means two threads cannot execute Python bytecode
simultaneously. For **I/O-bound** work this barely matters: the GIL is released
during system calls (`recv`, `send`, `sleep`), so threads genuinely overlap on I/O.
For **CPU-bound** handlers (e.g., computing a response) you need `ProcessPoolExecutor`
or a compiled extension regardless of threading vs async.

---

## Connection timeout: how each model implements it

**Blocking:** `sock.settimeout(n)` tells the OS to unblock `recv()` with
`BlockingIOError` / `socket.timeout` after `n` seconds of no data. This fires even
if the handler is not running (the thread is blocked inside `recv`).

**Async:** `asyncio.wait_for(reader.read(...), timeout=n)` wraps the coroutine in a
task with a deadline. If the deadline fires, `asyncio.TimeoutError` is raised at the
`await` site. This is purely runtime-managed — it only advances while the event loop
is running, which is fine for I/O-bound servers.
