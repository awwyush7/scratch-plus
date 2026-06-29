# Phase 5 Benchmark Report

## Setup

Two embedded servers share the same route set (`/fast`, `/slow?ms=N`, `/large`, `/small`):

| Server | Behaviour |
|--------|-----------|
| `HTTPServer` | Serial per-connection — each request waits for the previous response before the next handler runs. Used for **Sequential** and **Classic pipeline** modes. |
| `PipeliningPlusServer` | One thread per request, send lock per connection — responses complete out-of-order and are matched by `Request-ID`. Used for **Pipelining+** mode. |

Each workload runs 3 times; the median run is reported.  
Machine: MacBook, Python 3.13, loopback (127.0.0.1).

---

## Workloads

| ID | Description |
|----|-------------|
| W1 | 8 × GET /fast |
| W2 | 1 × GET /slow?ms=500 + 7 × GET /fast |
| W3 | /slow?ms=100, /fast, /slow?ms=200, /fast, /slow?ms=300, /fast, /slow?ms=50, /fast |
| W4 | 1 × GET /large (64 KB) + 7 × GET /small |

---

## HOL-Blocking Demonstration

Workload: 1 × slow (500 ms) + 3 × fast, one TCP connection.

**Classic pipeline** (HTTPServer — serial, in-order):

```
req-1 (slow)  done at +  506.1 ms
req-2 (fast)  done at +  506.2 ms
req-3 (fast)  done at +  506.8 ms
req-4 (fast)  done at +  506.8 ms
```

**Pipelining+** (concurrent, out-of-order):

```
req-1 (slow)  done at +  506.3 ms
req-2 (fast)  done at +    2.4 ms  ← arrives first
req-3 (fast)  done at +    2.8 ms
req-4 (fast)  done at +    3.0 ms
Arrival order: req-2 → req-3 → req-4 → req-1
```

**HOL penalty on first fast request: 504 ms.**  
Classic pipeline forces every subsequent response to queue behind the slow one. Pipelining+ dispatches all requests concurrently; fast responses bypass the slow one entirely.

---

## Results Tables

### W1 — all-fast (8 requests)

| Metric | Sequential | Classic pipeline | Pipelining+ |
|--------|-----------|-----------------|-------------|
| Total time (ms) | 0.4 | 0.6 | 0.7 |
| p50 latency (ms) | 0.3 | 0.5 | 0.5 |
| p95 latency (ms) | 0.4 | 0.5 | 0.7 |
| Connections | 8 | 1 | 1 |
| Bytes sent | 616 | 656 | 808 |
| Bytes received | 856 | 896 | 1048 |

> All-fast: no meaningful difference. Pipelining+ carries a tiny overhead from `Request-ID` headers (+192 B sent, +192 B received vs classic pipeline).

---

### W2 — slow-then-fast (1 × 500 ms slow + 7 fast)

| Metric | Sequential | Classic pipeline | Pipelining+ |
|--------|-----------|-----------------|-------------|
| Total time (ms) | 504.4 | 505.4 | 506.4 |
| p50 latency (ms) | 0.8 | **505.0** | **2.7** |
| p95 latency (ms) | 1.1 | 505.3 | 3.1 |
| Connections | 8 | 1 | 1 |
| Bytes sent | 623 | 663 | 815 |
| Bytes received | 867 | 907 | 1059 |

> **Key result.** Classic pipeline p50 = 505 ms: the slow request blocks every fast response on the wire. Pipelining+ p50 = 2.7 ms — the 7 fast requests complete in ~3 ms while the slow request is still running. **187× lower median latency for fast requests.**

---

### W3 — mixed-delay (4 slow + 4 fast, interleaved)

| Metric | Sequential | Classic pipeline | Pipelining+ |
|--------|-----------|-----------------|-------------|
| Total time (ms) | 304.7 | **666.0** | **306.0** |
| p50 latency (ms) | 28.6 | 462.2 | 29.2 |
| p95 latency (ms) | 203.3 | 666.0 | 206.1 |
| Connections | 8 | 1 | 1 |
| Bytes sent | 643 | 683 | 835 |
| Bytes received | 899 | 939 | 1091 |

> **Classic pipeline total = 666 ms** — delays accumulate (100 + 200 + 300 + 50 = 650 ms of sleep, plus each fast request waits for the preceding slow one). **Pipelining+ total = 306 ms** — bounded by the longest single delay (300 ms), not their sum. **2.2× faster overall.**

---

### W4 — large-then-small (64 KB + 7 small)

| Metric | Sequential | Classic pipeline | Pipelining+ |
|--------|-----------|-----------------|-------------|
| Total time (ms) | 0.6 | 0.7 | 1.5 |
| p50 latency (ms) | 0.2 | 0.7 | 1.4 |
| p95 latency (ms) | 0.2 | 0.7 | 1.5 |
| Connections | 8 | 1 | 1 |
| Bytes sent | 624 | 664 | 816 |
| Bytes received | 66393 | 66433 | 66585 |

> Loopback bandwidth is not a bottleneck at 64 KB. No meaningful HOL effect since the large response is not slow — it completes quickly regardless of order. Pipelining+ overhead is visible (~1ms extra) but trivial.

---

## Analysis: why Pipelining+ wins on slow-then-fast (W2)

Classic HTTP/1.1 pipelining allows the client to send all requests without waiting for individual responses, but the **server must respond in the same order it received requests**. If request 1 takes 500 ms, requests 2–8 cannot be delivered to the client until request 1's response is written to the wire — they sit in a queue behind it. This is **application-level head-of-line (HOL) blocking**.

Pipelining+ breaks this constraint with two additions:

1. **Per-request concurrency** — each incoming request is handed to its own thread immediately, without waiting for preceding handlers to complete.
2. **Request-ID correlation** — every request carries a `Request-ID` header; every response echoes it. The client matches responses to requests by ID, so it does not need them to arrive in order.

The consequence: in W2, the 7 fast requests complete in ~3 ms and their responses are sent immediately. The slow request is still sleeping. The client already has 7 responses back while waiting for the 8th. Classic pipelining cannot do this — the fast responses are physically blocked on the write side until the slow response is written first.

**W3 amplifies this** because delays are interleaved. Classic pipeline stacks them serially (666 ms); Pipelining+ overlaps them, bounded by the single longest request (306 ms ≈ 300 ms slow + overhead).

**Trade-off:** Pipelining+ requires the client to implement Request-ID tracking and response reordering. It also adds ~190 B of header overhead per request pair. For workloads with uniform, fast responses (W1, W4), the benefit is negligible and the overhead is visible. The extension pays off only when response times are heterogeneous — exactly the real-world case for APIs that mix cheap reads with slow computations.
