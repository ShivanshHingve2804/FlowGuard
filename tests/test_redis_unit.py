"""Unit tests for Redis-backed limiters (keys, Lua contract, fail-open/closed)."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from flowguard.algorithms import (
    RedisLeakyBucketLimiter,
    RedisSlidingWindowCounterLimiter,
    RedisSlidingWindowLogLimiter,
    RedisTokenBucketLimiter,
    create_limiter,
)
from flowguard.models import RateLimitExceeded


class _FakeScript:
    def __init__(self, response: list[Any] | Exception) -> None:
        self.response = response
        self.calls: list[tuple[Sequence[str], Sequence[Any]]] = []

    def __call__(self, keys: Sequence[str], args: Sequence[Any]) -> list[Any]:
        self.calls.append((keys, args))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class _FakeAsyncScript:
    def __init__(self, response: list[Any] | Exception) -> None:
        self.response = response
        self.calls: list[tuple[Sequence[str], Sequence[Any]]] = []

    async def __call__(self, keys: Sequence[str], args: Sequence[Any]) -> list[Any]:
        self.calls.append((keys, args))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class _FakeSyncRedis:
    def __init__(self, script: _FakeScript) -> None:
        self._script = script
        self.deleted: list[tuple[str, ...]] = []

    def register_script(self, _lua: str) -> _FakeScript:
        return self._script

    def delete(self, *keys: str) -> int:
        self.deleted.append(keys)
        return len(keys)


class _FakeAsyncRedis:
    def __init__(self, script: _FakeAsyncScript) -> None:
        self._script = script
        self.deleted: list[tuple[str, ...]] = []

    def register_script(self, _lua: str) -> _FakeAsyncScript:
        return self._script

    async def delete(self, *keys: str) -> int:
        self.deleted.append(keys)
        return len(keys)


def test_redis_cluster_hash_tags_and_infinite_sentinel() -> None:
    script = _FakeScript([0, 0, "-1.000000", "-1.000000", 5])
    fake_client = _FakeSyncRedis(script)
    limiter = RedisTokenBucketLimiter(
        capacity=5,
        refill_rate=0.0,
        redis_client=fake_client,
        key_prefix="fg",  # type: ignore[arg-type]
    )

    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.acquire("user:42", tokens=2)

    assert math.isinf(exc_info.value.retry_after)
    assert exc_info.value.result.retry_after_json is None
    assert script.calls[0][0] == ["fg:tb:{user:42}"]
    assert script.calls[0][1][:3] == [5, 0.0, 2]

    limiter.reset("user:42")
    assert fake_client.deleted == [("fg:tb:{user:42}",)]


def test_redis_sliding_window_log_two_cluster_tagged_keys() -> None:
    script = _FakeScript([1, 8, "9.500000", "0.000000", 2])
    fake_client = _FakeSyncRedis(script)
    limiter = RedisSlidingWindowLogLimiter(
        limit=10,
        window=10.0,
        redis_client=fake_client,
        key_prefix="fg",  # type: ignore[arg-type]
    )
    res = limiter.acquire("tenant-a", tokens=2)
    assert res.allowed is True
    assert res.remaining == 8
    assert script.calls[0][0] == ["fg:swl:{tenant-a}", "fg:swl:{tenant-a}:seq"]

    limiter.reset("tenant-a")
    assert fake_client.deleted == [("fg:swl:{tenant-a}", "fg:swl:{tenant-a}:seq")]


def test_redis_fail_open_vs_fail_closed() -> None:
    failing_script = _FakeScript(RedisConnectionError("redis down"))
    fake_client = _FakeSyncRedis(failing_script)

    closed_limiter = RedisLeakyBucketLimiter(
        capacity=5,
        leak_rate=1.0,
        redis_client=fake_client,
        fail_open=False,  # type: ignore[arg-type]
    )
    with pytest.raises(RedisConnectionError):
        closed_limiter.acquire("k")

    open_limiter = RedisSlidingWindowCounterLimiter(
        limit=5,
        window=10.0,
        redis_client=fake_client,
        fail_open=True,  # type: ignore[arg-type]
    )
    res = open_limiter.acquire("k")
    assert res.allowed is True
    assert res.remaining == 5


async def test_redis_async_client_and_factory() -> None:
    script = _FakeAsyncScript([1, 4, "1.000000", "0.000000", 1])
    fake_async = _FakeAsyncRedis(script)
    limiter = create_limiter("token_bucket", limit=5, window=5.0, async_redis_client=fake_async)
    assert isinstance(limiter, RedisTokenBucketLimiter)
    res = await limiter.aacquire("async-user")
    assert res.allowed is True
    assert res.remaining == 4
    await limiter.areset("async-user")
    assert fake_async.deleted == [("flowguard:tb:{async-user}",)]
