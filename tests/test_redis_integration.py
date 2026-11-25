"""Live Redis integration tests verifying atomic Lua scripts across concurrent clients."""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import time
import uuid

import pytest
import redis
import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.testclient import TestClient

from flowguard.algorithms import (
    RedisLeakyBucketLimiter,
    RedisSlidingWindowCounterLimiter,
    RedisSlidingWindowLogLimiter,
    RedisTokenBucketLimiter,
)
from flowguard.middleware import FlowGuardMiddleware
from flowguard.models import RateLimitExceeded

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture
def live_prefix() -> str:
    prefix = f"flowguard_test:{uuid.uuid4().hex[:8]}"
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    client.ping()
    try:
        yield prefix
    finally:
        keys = list(client.scan_iter(match=f"{prefix}:*"))
        if keys:
            client.delete(*keys)
        client.close()


def test_live_redis_all_four_algorithms(live_prefix: str) -> None:
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        tb = RedisTokenBucketLimiter(
            capacity=3, refill_rate=20.0, redis_client=client, key_prefix=live_prefix
        )
        assert tb.acquire("u1").remaining == 2
        assert tb.acquire("u1").remaining == 1
        assert tb.acquire("u1").remaining == 0
        with pytest.raises(RateLimitExceeded):
            tb.acquire("u1")
        time.sleep(0.12)
        assert tb.acquire("u1").allowed is True

        lb = RedisLeakyBucketLimiter(
            capacity=2, leak_rate=20.0, redis_client=client, key_prefix=live_prefix
        )
        assert lb.acquire("u2").remaining == 1
        assert lb.acquire("u2").remaining == 0
        with pytest.raises(RateLimitExceeded):
            lb.acquire("u2")
        time.sleep(0.12)
        assert lb.acquire("u2").allowed is True

        swl = RedisSlidingWindowLogLimiter(
            limit=2, window=0.15, redis_client=client, key_prefix=live_prefix
        )
        assert swl.acquire("u3").remaining == 1
        assert swl.acquire("u3").remaining == 0
        with pytest.raises(RateLimitExceeded):
            swl.acquire("u3")
        time.sleep(0.2)
        assert swl.acquire("u3").allowed is True

        swc = RedisSlidingWindowCounterLimiter(
            limit=2, window=0.2, redis_client=client, key_prefix=live_prefix
        )
        assert swc.acquire("u4").remaining == 1
        assert swc.acquire("u4").remaining == 0
        with pytest.raises(RateLimitExceeded):
            swc.acquire("u4")
    finally:
        client.close()


def test_live_redis_cross_process_atomic_contention(live_prefix: str) -> None:
    """Two separate limiter instances + clients racing 80 threads must allow exact capacity."""
    c1 = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    c2 = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        limiter_a = RedisTokenBucketLimiter(
            capacity=25, refill_rate=0.0, redis_client=c1, key_prefix=live_prefix
        )
        limiter_b = RedisTokenBucketLimiter(
            capacity=25, refill_rate=0.0, redis_client=c2, key_prefix=live_prefix
        )

        def hit(idx: int) -> bool:
            target = limiter_a if idx % 2 == 0 else limiter_b
            try:
                target.acquire("shared-global-key")
                return True
            except RateLimitExceeded:
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            outcomes = list(pool.map(hit, range(80)))

        assert sum(1 for ok in outcomes if ok) == 25
    finally:
        c1.close()
        c2.close()


async def test_live_redis_async_and_asgi_middleware(live_prefix: str) -> None:
    async_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        limiter = RedisSlidingWindowLogLimiter(
            limit=10, window=10.0, async_redis_client=async_client, key_prefix=live_prefix
        )

        async def hit() -> bool:
            try:
                await limiter.aacquire("async-shared")
                return True
            except RateLimitExceeded:
                return False

        results = await asyncio.gather(*(hit() for _ in range(25)))
        assert sum(1 for ok in results if ok) == 10
    finally:
        await async_client.aclose()


def test_live_redis_asgi_middleware(live_prefix: str) -> None:
    mw_limiter = RedisTokenBucketLimiter(
        capacity=2, refill_rate=0.0, redis_url=REDIS_URL, key_prefix=live_prefix
    )
    app = FastAPI()
    app.add_middleware(FlowGuardMiddleware, limiter=mw_limiter)

    @app.get("/api")
    async def api_route() -> dict[str, str]:
        return {"status": "ok"}

    with TestClient(app) as client:
        assert client.get("/api").headers["X-RateLimit-Remaining"] == "1"
        assert client.get("/api").headers["X-RateLimit-Remaining"] == "0"
        r429 = client.get("/api")
        assert r429.status_code == 429
        assert r429.headers["X-RateLimit-Remaining"] == "0"
