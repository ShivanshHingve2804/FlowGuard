"""
FlowGuard Interactive Dashboard — FastAPI Demo Application.

Serves a real-time web dashboard for visualizing, testing, and
benchmarking all four rate-limiting algorithms side by side.
"""

import time
import json
import asyncio
from pathlib import Path
from typing import Dict, Any
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.algorithms.leaky_bucket import LeakyBucketLimiter
from flowguard.algorithms.sliding_window_log import SlidingWindowLogLimiter
from flowguard.algorithms.sliding_window_counter import SlidingWindowCounterLimiter
from flowguard.models import RateLimitExceeded


# ── Global limiter instances (reset-able via API) ──────────────────────

_limiters: Dict[str, Any] = {}
_request_log: list = []          # recent request history for the timeline
_MAX_LOG = 500                   # keep last N entries


def _create_limiters(
    capacity: int = 10,
    refill_rate: float = 2.0,
    window: float = 5.0,
):
    """(Re-)create all four limiter instances."""
    global _limiters, _request_log
    _limiters = {
        "token_bucket": TokenBucketLimiter(capacity=capacity, refill_rate=refill_rate),
        "leaky_bucket": LeakyBucketLimiter(capacity=capacity, leak_rate=refill_rate),
        "sliding_window_log": SlidingWindowLogLimiter(limit=capacity, window=window),
        "sliding_window_counter": SlidingWindowCounterLimiter(limit=capacity, window=window),
    }
    _request_log.clear()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _create_limiters()
    yield


app = FastAPI(
    title="FlowGuard Dashboard",
    description="Interactive rate-limiter visualisation",
    version="1.0.0",
    lifespan=lifespan,
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── API Routes ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the dashboard HTML."""
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/api/acquire/{algorithm}")
async def acquire(algorithm: str, request: Request):
    """Attempt to acquire a token from the specified algorithm."""
    if algorithm not in _limiters:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown algorithm: {algorithm}. Use: {list(_limiters.keys())}"},
        )

    limiter = _limiters[algorithm]
    key = "dashboard-user"
    ts = time.time()

    try:
        result = limiter.acquire(key)
        entry = {
            "timestamp": ts,
            "algorithm": algorithm,
            "allowed": True,
            "remaining": result.remaining,
            "limit": result.limit,
            "current_usage": result.current_usage,
            "reset_after": round(result.reset_after_seconds, 3),
            "retry_after": 0.0,
        }
    except RateLimitExceeded as exc:
        r = exc.result
        entry = {
            "timestamp": ts,
            "algorithm": algorithm,
            "allowed": False,
            "remaining": r.remaining,
            "limit": r.limit,
            "current_usage": r.current_usage,
            "reset_after": round(r.reset_after_seconds, 3),
            "retry_after": round(r.retry_after_seconds, 3),
        }

    _request_log.append(entry)
    if len(_request_log) > _MAX_LOG:
        _request_log[:] = _request_log[-_MAX_LOG:]

    return entry


@app.post("/api/burst/{algorithm}")
async def burst(algorithm: str, request: Request):
    """Fire N requests as fast as possible and return all results."""
    body = await request.json()
    count = min(int(body.get("count", 20)), 200)

    results = []
    for _ in range(count):
        resp = await acquire(algorithm, request)
        if isinstance(resp, dict):
            results.append(resp)
        else:
            results.append(resp.body.decode() if hasattr(resp, "body") else str(resp))
    return {"results": results}


@app.post("/api/benchmark")
async def benchmark(request: Request):
    """Run a multi-threaded benchmark across all algorithms."""
    body = await request.json()
    total = min(int(body.get("requests", 100)), 5000)
    workers = min(int(body.get("workers", 10)), 50)

    # Reset limiters with the same capacity for fair comparison
    _create_limiters(capacity=50, refill_rate=10.0, window=5.0)

    loop = asyncio.get_event_loop()
    results: Dict[str, Any] = {}

    for algo_name, limiter in _limiters.items():
        limiter.reset("bench")
        allowed = 0
        rejected = 0
        latencies = []

        def _worker(_):
            nonlocal allowed, rejected
            t0 = time.perf_counter()
            try:
                limiter.acquire("bench")
                elapsed = (time.perf_counter() - t0) * 1_000_000  # µs
                latencies.append(elapsed)
                return True
            except RateLimitExceeded:
                elapsed = (time.perf_counter() - t0) * 1_000_000
                latencies.append(elapsed)
                return False

        with ThreadPoolExecutor(max_workers=workers) as pool:
            outcomes = list(pool.map(_worker, range(total)))

        allowed = sum(1 for o in outcomes if o)
        rejected = total - allowed
        latencies.sort()

        results[algo_name] = {
            "total": total,
            "allowed": allowed,
            "rejected": rejected,
            "avg_latency_us": round(sum(latencies) / len(latencies), 2) if latencies else 0,
            "p50_latency_us": round(latencies[len(latencies) // 2], 2) if latencies else 0,
            "p95_latency_us": round(latencies[int(len(latencies) * 0.95)], 2) if latencies else 0,
            "p99_latency_us": round(latencies[int(len(latencies) * 0.99)], 2) if latencies else 0,
        }

    # Restore default limiters
    _create_limiters()
    return {"benchmark": results, "config": {"requests": total, "workers": workers}}


@app.post("/api/reset")
async def reset_limiters(request: Request):
    """Reset all limiters with optional new configuration."""
    body = await request.json()
    cap = int(body.get("capacity", 10))
    rate = float(body.get("refill_rate", 2.0))
    win = float(body.get("window", 5.0))
    _create_limiters(capacity=cap, refill_rate=rate, window=win)
    return {"status": "reset", "capacity": cap, "refill_rate": rate, "window": win}


@app.get("/api/log")
async def get_log():
    """Return recent request log."""
    return {"log": _request_log[-100:]}
