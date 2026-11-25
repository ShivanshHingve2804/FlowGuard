"""Thread-safety, striped locking, and async concurrency tests."""

import asyncio
import concurrent.futures

import pytest

from flowguard.algorithms import (
    LeakyBucketLimiter,
    SlidingWindowCounterLimiter,
    SlidingWindowLogLimiter,
    TokenBucketLimiter,
)
from flowguard.models import RateLimitExceeded


@pytest.mark.parametrize(
    "factory",
    [
        lambda: TokenBucketLimiter(capacity=100, refill_rate=0.0),
        lambda: LeakyBucketLimiter(capacity=100, leak_rate=0.0),
        lambda: SlidingWindowLogLimiter(limit=100, window=60.0),
        lambda: SlidingWindowCounterLimiter(limit=100, window=60.0),
    ],
)
def test_all_algorithms_thread_safe_single_key(factory) -> None:
    limiter = factory()

    def worker(_: int) -> bool:
        try:
            limiter.acquire("hot-key")
            return True
        except RateLimitExceeded:
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        outcomes = list(executor.map(worker, range(180)))

    assert sum(1 for ok in outcomes if ok) == 100


def test_striped_isolation_across_many_keys() -> None:
    limiter = TokenBucketLimiter(capacity=5, refill_rate=0.0, stripes=64, max_keys=10_000)
    keys = [f"tenant-{i}" for i in range(50)]

    def worker(idx: int) -> bool:
        key = keys[idx % len(keys)]
        try:
            limiter.acquire(key)
            return True
        except RateLimitExceeded:
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        outcomes = list(executor.map(worker, range(50 * 8)))

    assert sum(1 for ok in outcomes if ok) == 250


async def test_async_gather_concurrency() -> None:
    limiter = SlidingWindowLogLimiter(limit=40, window=60.0)

    async def hit() -> bool:
        try:
            await limiter.aacquire("async-hot")
            return True
        except RateLimitExceeded:
            return False

    results = await asyncio.gather(*(hit() for _ in range(75)))
    assert sum(1 for ok in results if ok) == 40
