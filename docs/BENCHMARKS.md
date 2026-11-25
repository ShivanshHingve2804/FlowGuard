# FlowGuard Verified Benchmarks & Test Evidence

All measurements below were captured inside Docker (`python:3.12-slim` + `redis:7-alpine`) using `flowguard.cli.benchmark` and the automated `pytest` suite.

---

## 1. Automated Verification Summary

- **Lint & Format:** `ruff check .` (0 issues), `ruff format --check .` (23 files formatted)
- **Static Typing:** `mypy src/` (0 issues across 14 source files)
- **Test Suite:** `pytest -v --cov=flowguard --cov-fail-under=85` → **32 / 32 passed in `6.49s` (`89.58%` coverage)**
  - Includes multi-threaded contention across all 4 in-memory algorithms, 64-stripe key isolation, LRU eviction, fractional window precision, pure ASGI middleware, `@rate_limit` sync/async routes, and live **Redis 7 Lua cross-client atomic contention** (`25` capacity across `80` concurrent threads on two independent clients → exact `25` winners).

---

## 2. In-Memory Striped Backend (`64` Stripes, `50` Threads, `16` Keys)

Command:
```bash
python -m flowguard.cli.benchmark --algorithm all --requests 20000 --concurrency 50 --keys 16
```

| Algorithm | Allowed | Rejected | Throughput (QPS) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| `token_bucket` | 20,000 | 0 | **37,549.9** | 0.0038 | 9.1355 | 11.6461 |
| `leaky_bucket` | 20,000 | 0 | **38,190.3** | 0.0035 | 0.0421 | 12.6261 |
| `sliding_window_log` | 20,000 | 0 | **28,768.5** | 0.0058 | 8.1577 | 26.9032 |
| `sliding_window_counter` | 20,000 | 0 | **39,716.8** | 0.0052 | 0.0415 | 16.9906 |

---

## 3. Distributed Redis 7 Lua Backend (`20` Threads, `16` Keys)

Command:
```bash
python -m flowguard.cli.benchmark --algorithm all --requests 2000 --concurrency 20 --keys 16 --redis-url redis://redis:6379/0
```

| Algorithm | Allowed | Rejected | Throughput (QPS) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| `token_bucket` | 2,000 | 0 | **3,911.4** | 4.1375 | 8.4373 | 13.5646 |
| `leaky_bucket` | 2,000 | 0 | **4,294.7** | 4.0732 | 7.7840 | 10.8160 |
| `sliding_window_log` | 2,000 | 0 | **3,478.7** | 4.5077 | 10.4835 | 21.1715 |
| `sliding_window_counter` | 2,000 | 0 | **2,927.6** | 4.9291 | 14.4766 | 37.3323 |

> **Scope note:** Redis timings reflect per-request network round-trips from a Docker container to `redis:7-alpine` over the local bridge network (`50`/s vs in-memory ~`38,000` QPS, trading single-node speed for cross-process linearizability).
