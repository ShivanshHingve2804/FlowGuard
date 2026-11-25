"""Distributed Redis-backed rate limiters using atomic Lua scripts and server TIME."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import redis
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from flowguard.algorithms.base import (
    BaseRateLimiter,
    validate_acquire_request,
    validate_non_negative_float,
    validate_positive_float,
    validate_positive_int,
)
from flowguard.models import RateLimitExceeded, RateLimitResult

logger = logging.getLogger(__name__)

_TOKEN_BUCKET_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000.0
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local current_tokens = tonumber(data[1])
local last_ts = tonumber(data[2])

if current_tokens == nil or last_ts == nil then
    current_tokens = capacity
    last_ts = now
end

local elapsed = math.max(0.0, now - last_ts)
local filled = math.min(capacity, current_tokens + elapsed * refill_rate)

local allowed = 0
local retry_after = 0.0
if filled >= requested then
    current_tokens = filled - requested
    allowed = 1
else
    current_tokens = filled
    if refill_rate > 0 then
        retry_after = (requested - filled) / refill_rate
    else
        retry_after = -1.0
    end
end

local reset_after = 0.0
if current_tokens < capacity then
    if refill_rate > 0 then
        reset_after = (capacity - current_tokens) / refill_rate
    else
        reset_after = -1.0
    end
end

redis.call(
    'HSET',
    KEYS[1],
    'tokens', string.format('%.6f', current_tokens),
    'ts', string.format('%.6f', now)
)
if ttl_ms > 0 then
    redis.call('PEXPIRE', KEYS[1], ttl_ms)
end

local remaining = math.max(0, math.floor(current_tokens))
return {
    allowed,
    remaining,
    string.format('%.6f', reset_after),
    string.format('%.6f', retry_after),
    capacity - remaining
}
"""

_LEAKY_BUCKET_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000.0
local capacity = tonumber(ARGV[1])
local leak_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'water', 'ts')
local water = tonumber(data[1])
local last_ts = tonumber(data[2])

if water == nil or last_ts == nil then
    water = 0.0
    last_ts = now
end

local elapsed = math.max(0.0, now - last_ts)
water = math.max(0.0, water - elapsed * leak_rate)

local allowed = 0
local retry_after = 0.0
if water + requested <= capacity then
    water = water + requested
    allowed = 1
else
    if leak_rate > 0 then
        retry_after = (water + requested - capacity) / leak_rate
    else
        retry_after = -1.0
    end
end

local reset_after = 0.0
if water > 0 then
    if leak_rate > 0 then
        reset_after = water / leak_rate
    else
        reset_after = -1.0
    end
end

redis.call(
    'HSET',
    KEYS[1],
    'water', string.format('%.6f', water),
    'ts', string.format('%.6f', now)
)
if ttl_ms > 0 then
    redis.call('PEXPIRE', KEYS[1], ttl_ms)
end

local remaining = math.max(0, math.floor(capacity - water))
return {
    allowed,
    remaining,
    string.format('%.6f', reset_after),
    string.format('%.6f', retry_after),
    capacity - remaining
}
"""

_SLIDING_WINDOW_LOG_LUA = """
local t = redis.call('TIME')
local now_us = tonumber(t[1]) * 1000000 + tonumber(t[2])
local limit = tonumber(ARGV[1])
local window_sec = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local window_us = math.max(1, math.floor(window_sec * 1000000))
local cutoff = now_us - window_us

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
local current_usage = tonumber(redis.call('ZCARD', KEYS[1]))

local allowed = 0
local retry_after = 0.0
if current_usage + requested <= limit then
    local seq = tonumber(redis.call('INCRBY', KEYS[2], requested))
    for i = 0, requested - 1 do
        local member = tostring(now_us) .. ':' .. tostring(seq - i)
        redis.call('ZADD', KEYS[1], now_us, member)
    end
    current_usage = current_usage + requested
    local ttl_ms = math.max(1000, math.ceil(window_sec * 1000) + 1000)
    redis.call('PEXPIRE', KEYS[1], ttl_ms)
    redis.call('PEXPIRE', KEYS[2], ttl_ms)
    allowed = 1
else
    local need_evict = (current_usage + requested) - limit
    local idx = math.max(0, need_evict - 1)
    local target = redis.call('ZRANGE', KEYS[1], idx, idx, 'WITHSCORES')
    if #target >= 2 then
        retry_after = math.max(0.0, (tonumber(target[2]) + window_us - now_us) / 1000000.0)
    else
        retry_after = window_sec
    end
end

local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local reset_after = 0.0
if #oldest >= 2 then
    reset_after = math.max(0.0, (tonumber(oldest[2]) + window_us - now_us) / 1000000.0)
end

local remaining = math.max(0, limit - current_usage)
return {
    allowed,
    remaining,
    string.format('%.6f', reset_after),
    string.format('%.6f', retry_after),
    current_usage
}
"""

_SLIDING_WINDOW_COUNTER_LUA = """
local t = redis.call('TIME')
local now_us = tonumber(t[1]) * 1000000 + tonumber(t[2])
local limit = tonumber(ARGV[1])
local window_sec = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local window_us = math.max(1, math.floor(window_sec * 1000000))

local win_idx = math.floor(now_us / window_us)
local win_start_us = win_idx * window_us
local elapsed_us = now_us - win_start_us

local data = redis.call('HMGET', KEYS[1], 'prev', 'curr', 'idx')
local prev_count = tonumber(data[1]) or 0
local curr_count = tonumber(data[2]) or 0
local stored_idx = tonumber(data[3])

if stored_idx == nil then
    stored_idx = win_idx
elseif win_idx > stored_idx then
    local diff = win_idx - stored_idx
    if diff == 1 then
        prev_count = curr_count
    else
        prev_count = 0
    end
    curr_count = 0
    stored_idx = win_idx
end

local weight = (window_us - elapsed_us) / window_us
local estimated = math.floor(prev_count * weight) + curr_count
local reset_after = math.max(0.0, (window_us - elapsed_us) / 1000000.0)
local allowed = 0
local retry_after = 0.0

if estimated + requested <= limit then
    curr_count = curr_count + requested
    estimated = estimated + requested
    allowed = 1
else
    if curr_count + requested > limit then
        local remaining_curr = limit - requested
        local extra_wait = 0.0
        if curr_count > 0 and remaining_curr < curr_count then
            local next_weight = (remaining_curr + 1) / curr_count
            extra_wait = math.max(0.0, (1.0 - next_weight) * window_sec)
        end
        retry_after = reset_after + extra_wait
    else
        local max_prev = limit - requested - curr_count
        if prev_count > 0 then
            local target_weight = (max_prev + 1) / prev_count
            local target_elapsed_us = math.max(0.0, (1.0 - target_weight) * window_us)
            retry_after = math.min(reset_after, math.max(0.0, (target_elapsed_us - elapsed_us) / 1000000.0))
        else
            retry_after = reset_after
        end
    end
end

redis.call('HSET', KEYS[1], 'prev', prev_count, 'curr', curr_count, 'idx', stored_idx)
local ttl_ms = math.max(1000, math.ceil(window_sec * 2000) + 1000)
redis.call('PEXPIRE', KEYS[1], ttl_ms)

local remaining = math.max(0, limit - estimated)
return {
    allowed,
    remaining,
    string.format('%.6f', reset_after),
    string.format('%.6f', retry_after),
    estimated
}
"""


class BaseRedisRateLimiter(BaseRateLimiter):
    """Shared base for Redis Lua atomic rate limiters (sync + async)."""

    _lua_script: str = ""
    _algo_tag: str = ""

    def __init__(
        self,
        *,
        limit: int,
        redis_client: redis.Redis | None = None,
        async_redis_client: aioredis.Redis | None = None,
        redis_url: str | None = None,
        key_prefix: str = "flowguard",
        fail_open: bool = False,
    ) -> None:
        super().__init__()
        self._limit = validate_positive_int(limit, "limit")
        self._redis_url = redis_url or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
        self._sync_client = redis_client
        self._async_client = async_redis_client
        self._owns_sync_client = False
        self._owns_async_client = False
        self._sync_script: Any = None
        self._async_script: Any = None
        self.key_prefix = key_prefix
        self.fail_open = fail_open

    def _keys_for(self, key: str) -> list[str]:
        return [f"{self.key_prefix}:{self._algo_tag}:{{{key}}}"]

    def _script_args(self, tokens: int) -> list[Any]:
        raise NotImplementedError

    def _get_sync_script(self) -> Any:
        if self._sync_client is None:
            self._sync_client = redis.Redis.from_url(self._redis_url, decode_responses=True)
            self._owns_sync_client = True
        if self._sync_script is None:
            self._sync_script = self._sync_client.register_script(self._lua_script)
        return self._sync_script

    def _get_async_script(self) -> Any:
        if self._async_client is None:
            self._async_client = aioredis.from_url(self._redis_url, decode_responses=True)
            self._owns_async_client = True
        if self._async_script is None:
            self._async_script = self._async_client.register_script(self._lua_script)
        return self._async_script

    def _build_result(self, raw: list[Any]) -> RateLimitResult:
        allowed = bool(int(raw[0]))
        remaining = int(raw[1])
        reset_raw = float(raw[2])
        retry_raw = float(raw[3])
        current_usage = int(raw[4])

        reset_after = float("inf") if reset_raw < 0 else max(0.0, reset_raw)
        retry_after = float("inf") if retry_raw < 0 else max(0.0, retry_raw)

        result = RateLimitResult(
            allowed=allowed,
            limit=self._limit,
            remaining=remaining,
            reset_after_seconds=reset_after,
            retry_after_seconds=retry_after,
            current_usage=current_usage,
        )
        if not allowed:
            raise RateLimitExceeded(result)
        return result

    def _fail_open_result(self) -> RateLimitResult:
        return RateLimitResult(
            allowed=True,
            limit=self._limit,
            remaining=self._limit,
            reset_after_seconds=0.0,
            retry_after_seconds=0.0,
            current_usage=0,
        )

    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        validate_acquire_request(key, tokens, self._limit)
        try:
            script = self._get_sync_script()
            raw = script(keys=self._keys_for(key), args=self._script_args(tokens))
        except RedisError as exc:
            if self.fail_open:
                logger.warning("Redis unavailable during acquire (fail_open=True): %s", exc)
                return self._fail_open_result()
            raise
        return self._build_result(raw)

    async def aacquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        validate_acquire_request(key, tokens, self._limit)
        if self._async_client is None and self._sync_client is not None:
            return await asyncio.to_thread(self.acquire, key, tokens)
        try:
            script = self._get_async_script()
            raw = await script(keys=self._keys_for(key), args=self._script_args(tokens))
        except RedisError as exc:
            if self.fail_open:
                logger.warning("Redis unavailable during aacquire (fail_open=True): %s", exc)
                return self._fail_open_result()
            raise
        return self._build_result(raw)

    def reset(self, key: str) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        try:
            if self._sync_client is None:
                self._sync_client = redis.Redis.from_url(self._redis_url, decode_responses=True)
                self._owns_sync_client = True
            self._sync_client.delete(*self._keys_for(key))
        except RedisError as exc:
            if self.fail_open:
                logger.warning("Redis unavailable during reset (fail_open=True): %s", exc)
                return
            raise

    async def areset(self, key: str) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        if self._async_client is None and self._sync_client is not None:
            await asyncio.to_thread(self.reset, key)
            return
        try:
            if self._async_client is None:
                self._async_client = aioredis.from_url(self._redis_url, decode_responses=True)
                self._owns_async_client = True
            await self._async_client.delete(*self._keys_for(key))
        except RedisError as exc:
            if self.fail_open:
                logger.warning("Redis unavailable during areset (fail_open=True): %s", exc)
                return
            raise

    def close(self) -> None:
        """Close owned synchronous Redis connection pool."""
        if self._owns_sync_client and self._sync_client is not None:
            self._sync_client.close()
            self._sync_client = None
            self._sync_script = None

    async def aclose(self) -> None:
        """Close owned asynchronous Redis connection pool."""
        if self._owns_async_client and self._async_client is not None:
            await self._async_client.aclose()
            self._async_client = None
            self._async_script = None


class RedisTokenBucketLimiter(BaseRedisRateLimiter):
    """Distributed Token Bucket rate limiter backed by Redis Lua."""

    _lua_script = _TOKEN_BUCKET_LUA
    _algo_tag = "tb"

    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        *,
        redis_client: redis.Redis | None = None,
        async_redis_client: aioredis.Redis | None = None,
        redis_url: str | None = None,
        key_prefix: str = "flowguard",
        fail_open: bool = False,
    ) -> None:
        self.capacity = validate_positive_int(capacity, "capacity")
        self.refill_rate = validate_non_negative_float(refill_rate, "refill_rate")
        super().__init__(
            limit=self.capacity,
            redis_client=redis_client,
            async_redis_client=async_redis_client,
            redis_url=redis_url,
            key_prefix=key_prefix,
            fail_open=fail_open,
        )

    def _script_args(self, tokens: int) -> list[Any]:
        ttl_ms = (
            max(60_000, int((self.capacity / self.refill_rate) * 2000) + 1000)
            if self.refill_rate > 0
            else 0
        )
        return [self.capacity, self.refill_rate, tokens, ttl_ms]


class RedisLeakyBucketLimiter(BaseRedisRateLimiter):
    """Distributed metered Leaky Bucket rate limiter backed by Redis Lua."""

    _lua_script = _LEAKY_BUCKET_LUA
    _algo_tag = "lb"

    def __init__(
        self,
        capacity: int,
        leak_rate: float,
        *,
        redis_client: redis.Redis | None = None,
        async_redis_client: aioredis.Redis | None = None,
        redis_url: str | None = None,
        key_prefix: str = "flowguard",
        fail_open: bool = False,
    ) -> None:
        self.capacity = validate_positive_int(capacity, "capacity")
        self.leak_rate = validate_non_negative_float(leak_rate, "leak_rate")
        super().__init__(
            limit=self.capacity,
            redis_client=redis_client,
            async_redis_client=async_redis_client,
            redis_url=redis_url,
            key_prefix=key_prefix,
            fail_open=fail_open,
        )

    def _script_args(self, tokens: int) -> list[Any]:
        ttl_ms = (
            max(60_000, int((self.capacity / self.leak_rate) * 2000) + 1000)
            if self.leak_rate > 0
            else 0
        )
        return [self.capacity, self.leak_rate, tokens, ttl_ms]


class RedisSlidingWindowLogLimiter(BaseRedisRateLimiter):
    """Distributed exact Sliding Window Log rate limiter backed by Redis ZSET + Lua."""

    _lua_script = _SLIDING_WINDOW_LOG_LUA
    _algo_tag = "swl"

    def __init__(
        self,
        limit: int,
        window: float,
        *,
        redis_client: redis.Redis | None = None,
        async_redis_client: aioredis.Redis | None = None,
        redis_url: str | None = None,
        key_prefix: str = "flowguard",
        fail_open: bool = False,
    ) -> None:
        self.limit = validate_positive_int(limit, "limit")
        self.window_size = validate_positive_float(window, "window")
        super().__init__(
            limit=self.limit,
            redis_client=redis_client,
            async_redis_client=async_redis_client,
            redis_url=redis_url,
            key_prefix=key_prefix,
            fail_open=fail_open,
        )

    def _keys_for(self, key: str) -> list[str]:
        base = f"{self.key_prefix}:{self._algo_tag}:{{{key}}}"
        return [base, f"{base}:seq"]

    def _script_args(self, tokens: int) -> list[Any]:
        return [self.limit, self.window_size, tokens]


class RedisSlidingWindowCounterLimiter(BaseRedisRateLimiter):
    """Distributed Sliding Window Counter rate limiter backed by Redis Hash + Lua."""

    _lua_script = _SLIDING_WINDOW_COUNTER_LUA
    _algo_tag = "swc"

    def __init__(
        self,
        limit: int,
        window: float,
        *,
        redis_client: redis.Redis | None = None,
        async_redis_client: aioredis.Redis | None = None,
        redis_url: str | None = None,
        key_prefix: str = "flowguard",
        fail_open: bool = False,
    ) -> None:
        self.limit = validate_positive_int(limit, "limit")
        self.window_size = validate_positive_float(window, "window")
        super().__init__(
            limit=self.limit,
            redis_client=redis_client,
            async_redis_client=async_redis_client,
            redis_url=redis_url,
            key_prefix=key_prefix,
            fail_open=fail_open,
        )

    def _script_args(self, tokens: int) -> list[Any]:
        return [self.limit, self.window_size, tokens]
