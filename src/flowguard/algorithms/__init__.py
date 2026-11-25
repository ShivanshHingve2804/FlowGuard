"""Rate limiting algorithms (in-memory striped and distributed Redis Lua)."""

from __future__ import annotations

import os
from typing import Any

from flowguard.models import AlgorithmName, RateLimitRule

from .base import BaseRateLimiter
from .leaky_bucket import LeakyBucketLimiter
from .redis_backend import (
    BaseRedisRateLimiter,
    RedisLeakyBucketLimiter,
    RedisSlidingWindowCounterLimiter,
    RedisSlidingWindowLogLimiter,
    RedisTokenBucketLimiter,
)
from .sliding_window_counter import SlidingWindowCounterLimiter
from .sliding_window_log import SlidingWindowLogLimiter
from .token_bucket import TokenBucketLimiter


def create_limiter(
    algorithm: AlgorithmName = "token_bucket",
    *,
    limit: int | None = None,
    window: float | None = None,
    rate: float | None = None,
    rule: RateLimitRule | None = None,
    redis_url: str | None = None,
    redis_client: Any = None,
    async_redis_client: Any = None,
    key_prefix: str = "flowguard",
    fail_open: bool = False,
) -> BaseRateLimiter:
    """Build an in-memory or Redis-backed rate limiter from parameters or a `RateLimitRule`."""
    if rule is not None:
        algo = rule.algorithm
        cap = rule.capacity
        win = rule.period_seconds
        eff_rate = rule.rate if rate is None else rate
    else:
        algo = algorithm
        raw_default = os.getenv("DEFAULT_RATE_LIMIT", "100").split("/", 1)[0].strip()
        cap = limit if limit is not None else int(raw_default)
        win = window if window is not None else 60.0
        eff_rate = (cap / win) if rate is None else rate

    use_redis = any(v is not None for v in (redis_url, redis_client, async_redis_client))
    if use_redis:
        common = {
            "redis_client": redis_client,
            "async_redis_client": async_redis_client,
            "redis_url": redis_url,
            "key_prefix": key_prefix,
            "fail_open": fail_open,
        }
        if algo == "token_bucket":
            return RedisTokenBucketLimiter(capacity=cap, refill_rate=eff_rate, **common)
        if algo == "leaky_bucket":
            return RedisLeakyBucketLimiter(capacity=cap, leak_rate=eff_rate, **common)
        if algo == "sliding_window_log":
            return RedisSlidingWindowLogLimiter(limit=cap, window=win, **common)
        if algo == "sliding_window_counter":
            return RedisSlidingWindowCounterLimiter(limit=cap, window=win, **common)
        raise ValueError(f"Unsupported algorithm: {algo}")

    if algo == "token_bucket":
        return TokenBucketLimiter(capacity=cap, refill_rate=eff_rate)
    if algo == "leaky_bucket":
        return LeakyBucketLimiter(capacity=cap, leak_rate=eff_rate)
    if algo == "sliding_window_log":
        return SlidingWindowLogLimiter(limit=cap, window=win)
    if algo == "sliding_window_counter":
        return SlidingWindowCounterLimiter(limit=cap, window=win)
    raise ValueError(f"Unsupported algorithm: {algo}")


__all__ = [
    "BaseRateLimiter",
    "BaseRedisRateLimiter",
    "TokenBucketLimiter",
    "LeakyBucketLimiter",
    "SlidingWindowLogLimiter",
    "SlidingWindowCounterLimiter",
    "RedisTokenBucketLimiter",
    "RedisLeakyBucketLimiter",
    "RedisSlidingWindowLogLimiter",
    "RedisSlidingWindowCounterLimiter",
    "create_limiter",
]
