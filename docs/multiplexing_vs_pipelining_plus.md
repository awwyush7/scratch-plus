# Multiplexing vs Pipelining+ — Design Contrast

## Phase 4 recap: Pipelining+ (HTTP/1.15)

Pipelining+ sends COMPLETE responses out of order, identified by a `Request-ID`
header.  The key framing rule: one complete response at a time.

```
Client                      Server
──────                      ──────
→ Req1 /slow  (ID: 1)
→ Req2 /fast  (ID: 2)
                            (threads running concurrently)
← FULL Resp2 (ID: 2)       ← /fast wins the send lock, sends everything
← FULL Resp1 (ID: 1)       ← /slow eventually finishes
```

**The lock is held for the entire response.**  If /fast's response is 1 MB
and /slow is 1 KB, /slow's bytes cannot flow until the 1 MB upload is done.
This is byte-level HOL blocking — eliminated at the response level but not at
the byte level.

## Phase 8: True multiplexing via frames

Instead of sending complete responses, the server breaks each response into
small DATA frames, each tagged with a stream_id.  The send lock is held only
for one `sendall()` call (one frame), so frames from different streams
interleave freely on the wire.

```
Client                       Server
──────                       ──────
→ HEADERS stream=1 /slow
→ HEADERS stream=2 /fast
                             (two handler threads running)
← DATA    stream=2 chunk-1  ← /fast sends immediately
← DATA    stream=2 chunk-2
← END     stream=2
← DATA    stream=1 chunk-1  ← /slow sends its first chunk
← DATA    stream=1 chunk-2
← DATA    stream=1 chunk-3
← END     stream=1
```

The client accumulates DATA frames per stream_id and reconstructs the full
body when it sees END.

## Frame format

```
┌─────────────────────┬────────────┬─────────────────────┬───────────┐
│   stream_id (4 B)   │ type (1 B) │ payload_len (4 B)   │ payload   │
└─────────────────────┴────────────┴─────────────────────┴───────────┘
 9-byte fixed header                                       variable
```

Frame types: `HEADERS = 1`, `DATA = 0`, `END = 2`.

A stream lifecycle:
1. Client sends `HEADERS` → server sees a new stream, spawns a handler.
2. Handler sends ≥0 `DATA` frames.
3. Handler sends `END` → client knows the stream is complete.

## Comparison table

| Property              | Pipelining+ (Phase 4)              | Multiplexing toy (Phase 8)             |
|-----------------------|------------------------------------|----------------------------------------|
| Framing unit          | Complete response                  | Small DATA frames                      |
| Send lock duration    | Entire response body               | One frame (~9 B + payload)             |
| Stream ID             | Request-ID header (string)         | Binary stream_id in frame header       |
| Byte-level HOL        | Yes (large resp blocks small ones) | No (frames interleave per-frame)       |
| Partial body delivery | No                                 | Yes (client sees chunks as they arrive)|
| Flow control          | None                               | None (toy)                             |
| Header compression    | None                               | None (toy)                             |
| Complexity            | ~60 lines                          | ~110 lines                             |

## Why framing matters

Pipelining+ solves **application-level** HOL blocking: a slow handler no longer
blocks a fast one.  But it cannot solve **wire-level** HOL blocking: once the
send lock is held for a large body, all other complete responses queue up.

Multiplexing solves both.  By interleaving frames, a 10 MB download on stream 1
does not delay a 1 KB reply on stream 2 at the TCP byte level.  Each frame from
each stream makes progress independently.

This is the core insight behind HTTP/2: not just concurrency of handlers, but
byte-level multiplexing so no single response monopolizes the wire.

## What this toy omits (versus real HTTP/2)

- **HPACK header compression** — HTTP/2 compresses pseudo-headers; here headers
  are plain UTF-8 paths.
- **Flow control** — HTTP/2 has per-stream and connection-level windows; here
  handlers send without back-pressure.
- **Stream priorities and dependencies** — HTTP/2 can weight streams; here all
  streams are equal.
- **PUSH_PROMISE** — server push; not implemented.
- **Settings negotiation** — HTTP/2 starts with a SETTINGS frame; here the
  frame format is agreed out-of-band.
- **TLS / ALPN** — HTTP/2 in practice runs over TLS; this toy is plaintext TCP.
