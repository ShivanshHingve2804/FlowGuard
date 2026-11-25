"""
FlowGuard Interactive Dashboard — FastAPI Demo Application.

Serves a real-time web dashboard for visualizing, testing, and
benchmarking all four rate-limiting algorithms side by side.
"""

from __future__ import annotations

import asyncio
import math
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from flowguard.algorithms.base import BaseRateLimiter
from flowguard.algorithms.leaky_bucket import LeakyBucketLimiter
from flowguard.algorithms.sliding_window_counter import SlidingWindowCounterLimiter
from flowguard.algorithms.sliding_window_log import SlidingWindowLogLimiter
from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.models import RateLimitExceeded

_limiters: dict[str, BaseRateLimiter] = {}
_request_log: list[dict[str, Any]] = []
_MAX_LOG = 500


def _build_limiters(
    capacity: int = 10,
    refill_rate: float = 2.0,
    window: float = 5.0,
) -> dict[str, BaseRateLimiter]:
    return {
        "token_bucket": TokenBucketLimiter(capacity=capacity, refill_rate=refill_rate),
        "leaky_bucket": LeakyBucketLimiter(capacity=capacity, leak_rate=refill_rate),
        "sliding_window_log": SlidingWindowLogLimiter(limit=capacity, window=window),
        "sliding_window_counter": SlidingWindowCounterLimiter(limit=capacity, window=window),
    }


def _create_limiters(
    capacity: int = 10,
    refill_rate: float = 2.0,
    window: float = 5.0,
) -> None:
    """(Re-)create all four interactive dashboard limiter instances."""
    global _limiters
    _limiters = _build_limiters(capacity=capacity, refill_rate=refill_rate, window=window)
    _request_log.clear()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _create_limiters()
    yield


app = FastAPI(
    title="FlowGuard Dashboard",
    description="Interactive rate-limiter visualisation",
    version="0.2.0",
    lifespan=lifespan,
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _get_limiters() -> dict[str, BaseRateLimiter]:
    if not _limiters:
        _create_limiters()
    return _limiters


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe for container orchestration."""
    return {"status": "healthy"}


@app.get("/", response_class=HTMLResponse)
async def root() -> HTMLResponse:
    """Serve the dashboard HTML."""
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/api/acquire/{algorithm}", response_model=None)
async def acquire(algorithm: str) -> JSONResponse | dict[str, Any]:
    """Attempt to acquire a token from the specified algorithm."""
    limiters = _get_limiters()
    if algorithm not in limiters:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown algorithm: {algorithm}. Use: {list(limiters.keys())}"},
        )

    limiter = limiters[algorithm]
    key = "dashboard-user"
    ts = time.time()

    try:
        result = await limiter.aacquire(key)
        entry: dict[str, Any] = {
            "timestamp": ts,
            "algorithm": algorithm,
            "allowed": True,
            "remaining": result.remaining,
            "limit": result.limit,
            "current_usage": result.current_usage,
            "reset_after": round(result.reset_after_seconds, 3)
            if math.isfinite(result.reset_after_seconds)
            else None,
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
            "reset_after": round(r.reset_after_seconds, 3)
            if math.isfinite(r.reset_after_seconds)
            else None,
            "retry_after": r.retry_after_json,
        }

    _request_log.append(entry)
    if len(_request_log) > _MAX_LOG:
        _request_log[:] = _request_log[-_MAX_LOG:]

    return entry


@app.post("/api/burst/{algorithm}", response_model=None)
async def burst(algorithm: str, request: Request) -> JSONResponse | dict[str, Any]:
    """Fire N requests as fast as possible and return all results."""
    limiters = _get_limiters()
    if algorithm not in limiters:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown algorithm: {algorithm}. Use: {list(limiters.keys())}"},
        )

    body = await request.json()
    count = max(1, min(int(body.get("count", 20)), 200))

    results = []
    for _ in range(count):
        results.append(await acquire(algorithm))
    return {"results": results}


def _run_bench_sync(total: int, workers: int) -> dict[str, dict[str, Any]]:
    bench_limiters = _build_limiters(capacity=50, refill_rate=10.0, window=5.0)
    results: dict[str, dict[str, Any]] = {}

    for algo_name, limiter in bench_limiters.items():

        def _worker(_: int) -> tuple[bool, float]:
            t0 = time.perf_counter()
            try:
                limiter.acquire("bench")
                return True, (time.perf_counter() - t0) * 1_000_000
            except RateLimitExceeded:
                return False, (time.perf_counter() - t0) * 1_000_000

        with ThreadPoolExecutor(max_workers=workers) as pool:
            outcomes = list(pool.map(_worker, range(total)))

        allowed = sum(1 for ok, _ in outcomes if ok)
        latencies = sorted(lat for _, lat in outcomes)
        results[algo_name] = {
            "total": total,
            "allowed": allowed,
            "rejected": total - allowed,
            "avg_latency_us": round(sum(latencies) / len(latencies), 2) if latencies else 0.0,
            "p50_latency_us": round(latencies[len(latencies) // 2], 2) if latencies else 0.0,
            "p95_latency_us": round(latencies[int(len(latencies) * 0.95)], 2) if latencies else 0.0,
            "p99_latency_us": round(latencies[int(len(latencies) * 0.99)], 2) if latencies else 0.0,
        }
    return results


@app.post("/api/benchmark")
async def benchmark(request: Request) -> dict[str, Any]:
    """Run an isolated multi-threaded benchmark off the ASGI event loop."""
    body = await request.json()
    total = max(1, min(int(body.get("requests", 100)), 5000))
    workers = max(1, min(int(body.get("workers", 10)), 50))

    results = await asyncio.to_thread(_run_bench_sync, total, workers)
    return {"benchmark": results, "config": {"requests": total, "workers": workers}}


@app.post("/api/reset", response_model=None)
async def reset_limiters(request: Request) -> JSONResponse | dict[str, Any]:
    """Reset interactive limiters with validated configuration."""
    body = await request.json()
    cap = int(body.get("capacity", 10))
    rate = float(body.get("refill_rate", 2.0))
    win = float(body.get("window", 5.0))
    try:
        _create_limiters(capacity=cap, refill_rate=rate, window=win)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    return {"status": "reset", "capacity": cap, "refill_rate": rate, "window": win}


@app.get("/api/log")
async def get_log() -> dict[str, list[dict[str, Any]]]:
    """Return recent request log."""
    return {"log": _request_log[-100:]}
