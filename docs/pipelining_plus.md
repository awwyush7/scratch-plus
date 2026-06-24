# Pipelining+ / HTTP/1.15 — Design Notes

## Problem: HOL blocking in classic pipelining

HTTP/1.1 pipelining lets a client send multiple requests on one TCP connection
without waiting for each response.  The server, however, must reply **in order**:
responses to later requests cannot be delivered until all earlier responses have
been sent.  This is called **head-of-line (HOL) blocking**.

```
Client            Server
──────            ──────
→ Req1 /slow
→ Req2 /fast
→ Req3 /fast
                  ← (processing /slow... 300 ms)
                  ← Resp1 /slow   ← only now can the client read anything
                  ← Resp2 /fast
                  ← Resp3 /fast
```

The two fast requests finished on the server immediately but could not be
delivered because the in-order constraint blocked them behind the slow one.

## Solution: out-of-order responses identified by Request-ID

Pipelining+ adds a single header convention:

- The client attaches a `Request-ID:` header to every request.
- The server echos it verbatim in the corresponding response.
- Responses may arrive in **any order** — whichever handler finishes first sends first.
- The client maps each response to its request by comparing `Request-ID` values.

```
Client                  Server
──────                  ──────
→ Req1 /slow  (Request-ID: req-1)
→ Req2 /fast  (Request-ID: req-2)
→ Req3 /fast  (Request-ID: req-3)
                        (dispatches all three to threads concurrently)
← Resp2 /fast (Request-ID: req-2)   ← arrives ~immediately
← Resp3 /fast (Request-ID: req-3)   ← arrives ~immediately
← Resp1 /slow (Request-ID: req-1)   ← arrives after 300 ms
```

## Implementation

### Reader loop (single thread)

One thread reads the TCP stream sequentially.  It calls `_parse_request` in a
loop, which consumes exactly one complete HTTP request from the connection's
read buffer each iteration.  This is safe because `_parse_request` is the only
reader of `conn.rbuf`.

### Per-request dispatcher threads

Each parsed request is immediately handed to a fresh `threading.Thread`.  The
reader loop does not wait for that thread to finish before reading the next
request — this is what enables concurrency.

### Send lock — the framing rule

A single `threading.Lock` per connection (`send_lock`) is acquired before any
response bytes are written and released only after the socket is fully flushed.

```python
with send_lock:
    conn.send(resp.encode())
    conn.flush()
```

This ensures that response bytes from two concurrent handlers never interleave
on the wire.  Without this, the client might receive garbled HTTP messages
that mix bytes from two responses.

**Rule: one complete response at a time.**  This is the only framing guarantee
Pipelining+ provides.  There are no frames, no stream IDs, no flow control —
just a lock and a header.

## Tradeoffs vs HTTP/2

| Property            | Pipelining+                      | HTTP/2                            |
|---------------------|----------------------------------|-----------------------------------|
| Framing             | Complete responses, no chunks    | Fixed-size frames with stream IDs |
| Multiplexing        | Single stream; lock per response | True multiplexing; no HOL         |
| Flow control        | None                             | Per-stream and connection-level   |
| Header compression  | None                             | HPACK                             |
| Server push         | No                               | Yes                               |
| Complexity          | ~60 lines of code                | RFC 7540 (96 pages)               |
| Interoperability    | Custom (needs client support)    | Standard; every browser supports  |
| HOL elimination     | Yes — for large response gap     | Yes                               |
| Head-of-body block  | Yes — one big response still     | No — chunked per frame            |
|                     | blocks smaller ones on the wire  |                                   |

### When Pipelining+ is enough

Pipelining+ eliminates HOL blocking at the **response granularity**: once a
fast handler finishes, it grabs the lock and sends immediately.  A large slow
response body will, however, delay smaller responses that finish after it grabs
the lock — you get application-level concurrency but not byte-level
multiplexing.

For workloads where responses are small (typical APIs, JSON endpoints) and the
bottleneck is handler latency (database queries, external calls), Pipelining+
captures most of HTTP/2's latency benefit with a fraction of the protocol
complexity.

### What HTTP/2 adds that Pipelining+ can't

HTTP/2 frames every response into small chunks interleaved across streams.  A
10 MB download in stream 1 does not block a 1 KB response in stream 2 at the
TCP level.  Pipelining+ cannot do this: once the send lock is held by the large
response, all other handlers wait.

## Benchmark interpretation

Running `python examples/pipelining_plus_client.py` produces three sections:

1. **Demo** — 1 slow + 1 fast.  Confirms Resp2 (/fast) arrives before Resp1 (/slow).
2. **Benchmark** — 1 slow@500ms + 4 fast.

Expected numbers (loopback, no load):

| Mode             | Total time | Time to all fast done |
|------------------|------------|----------------------|
| Sequential       | ~505 ms    | ~505 ms              |
| Classic pipeline | ~505 ms    | ~505 ms              |
| Pipelining+      | ~505 ms    | ~5 ms                |

Total time is similar across all modes (dominated by the slow request).
The win is in **fast-request latency**: Pipelining+ delivers all four fast
responses ~100× sooner than the other two modes.
