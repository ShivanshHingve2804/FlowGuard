# FlowGuard API & Middleware Reference

## 1. Core Limiters (Sync & Async)

All limiters implement `BaseRateLimiter`:

```python
result = limiter.acquire(key: str, tokens: int = 1) -> RateLimitResult
await limiter.aacquire(key: str, tokens: int = 1) -> RateLimitResult
limiter.reset(key: str) -> None
await limiter.areset(key: str) -> None
```

### In-Memory Limiters

```python
from flowguard import (
    LeakyBucketLimiter,
    SlidingWindowCounterLimiter,
    SlidingWindowLogLimiter,
    TokenBucketLimiter,
)

tb = TokenBucketLimiter(capacity=100, refill_rate=10.0, max_keys=10_000, stripes=64)
lb = LeakyBucketLimiter(capacity=100, leak_rate=10.0)
swl = SlidingWindowLogLimiter(limit=100, window=60.0)
swc = SlidingWindowCounterLimiter(limit=100, window=60.0)
```

### Distributed Redis Limiters

```python
from flowguard import (
    RedisLeakyBucketLimiter,
    RedisSlidingWindowCounterLimiter,
    RedisSlidingWindowLogLimiter,
    RedisTokenBucketLimiter,
)

dist_tb = RedisTokenBucketLimiter(
    capacity=100,
    refill_rate=10.0,
    redis_url="redis://localhost:6379/0",
    key_prefix="flowguard",
    fail_open=False,
)
```

### Rule-Based Factory

```python
from flowguard import RateLimitRule, create_limiter

rule = RateLimitRule(
    name="public-api",
    capacity=60,
    period_seconds=60.0,
    algorithm="sliding_window_counter",
)
limiter = create_limiter(rule=rule, redis_url="redis://localhost:6379/0")
```

---

## 2. Pure ASGI Middleware (`FlowGuardMiddleware`)

```python
from fastapi import FastAPI, Request
from flowguard import FlowGuardMiddleware, RedisTokenBucketLimiter

app = FastAPI()
limiter = RedisTokenBucketLimiter(capacity=100, refill_rate=10.0)

app.add_middleware(
    FlowGuardMiddleware,
    limiter=limiter,
    exclude_paths={"/health", "/metrics"},
    trust_proxy_headers=False,
    key_func=lambda req: (
        req.headers.get("X-API-Key") or (req.client.host if req.client else "anon")
    ),
)
```

### Response Headers & `429` Semantics

| Header | Emitted On | Value |
|---|---|---|
| `X-RateLimit-Limit` | `200` and `429` | Configured bucket/window cap |
| `X-RateLimit-Remaining` | `200` and `429` | Remaining capacity (`>= 0`) |
| `X-RateLimit-Reset` | `200` and `429` | Seconds until full recovery (`ceil`, `"0"` if rate is `0`) |
| `Retry-After` | `429` (when finite) | Whole seconds until next token fits (`ceil >= 1`) |

Rejected requests return HTTP `429`:
```json
{
  "detail": "Too Many Requests",
  "retry_after": 1.0
}
```
(`retry_after` is `null` when refill/leak rate is `0`.)

---

## 3. Route Decorator (`@rate_limit`)

```python
from fastapi import FastAPI, Request
from flowguard import rate_limit

app = FastAPI()


@app.get("/login")
@rate_limit(
    limit=5,
    window=60.0,
    algorithm="sliding_window_log",
    key_func=lambda req: req.headers.get("X-User-Id", "anon"),
)
async def login(request: Request):
    return {"status": "ok"}
```

---

## 4. Interactive Dashboard & CLI

| Endpoint / Command | Purpose |
|---|---|
| `GET /` | Real-time visualization dashboard across all 4 algorithms |
| `GET /health` | Container liveness probe (`{"status": "healthy"}`) |
| `POST /api/acquire/{algorithm}` | Acquire 1 token and record timeline telemetry |
| `POST /api/burst/{algorithm}` | Burst up to 200 sequential acquires |
| `POST /api/benchmark` | Isolated multi-threaded benchmark via `asyncio.to_thread` |
| `POST /api/reset` | Reconfigure capacity, rate, and window across dashboard limiters |
| `python -m flowguard.cli.benchmark --algorithm all --requests 20000 --concurrency 50 --keys 16` | Multi-key throughput & latency CLI benchmark |
