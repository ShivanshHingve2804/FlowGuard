"""Abstract base classes, striped locking, and validation helpers for rate limiters."""

import math
import time
import zlib
from abc import ABC, abstractmethod
from collections import OrderedDict
from contextlib import contextmanager
from threading import RLock
from typing import Callable, Generic, Iterator, TypeVar

from flowguard.models import RateLimitResult

StateT = TypeVar("StateT")


def validate_positive_int(value: int, name: str) -> int:
    """Ensure integer parameters such as capacity/limit are strictly positive."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def validate_positive_float(value: float, name: str) -> float:
    """Ensure float parameters such as window duration are finite and positive."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive number")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise ValueError(f"{name} must be a positive finite number")
    return result


def validate_non_negative_float(value: float, name: str) -> float:
    """Ensure rate parameters are finite and non-negative."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a non-negative number")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise ValueError(f"{name} must be a non-negative finite number")
    return result


def validate_acquire_request(key: str, tokens: int, limit: int) -> None:
    """Validate key and requested token count against limiter capacity."""
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens <= 0:
        raise ValueError("tokens must be a positive integer")
    if tokens > limit:
        raise ValueError(f"Requested tokens ({tokens}) exceed limiter limit ({limit})")


class StripedStateStore(Generic[StateT]):
    """Thread-safe sharded LRU store isolating lock contention across keys."""

    def __init__(self, stripes: int = 64, max_keys: int | None = 10_000) -> None:
        validate_positive_int(stripes, "stripes")
        if max_keys is not None:
            validate_positive_int(max_keys, "max_keys")
        self._stripes = stripes
        self._locks = tuple(RLock() for _ in range(stripes))
        self._buckets: list[OrderedDict[str, StateT]] = [OrderedDict() for _ in range(stripes)]
        self._per_stripe_cap = (
            max(1, math.ceil(max_keys / stripes)) if max_keys is not None else None
        )

    def _stripe_index(self, key: str) -> int:
        return zlib.crc32(key.encode("utf-8")) % self._stripes

    @contextmanager
    def lock_bucket(self, key: str) -> Iterator[OrderedDict[str, StateT]]:
        """Acquire only the stripe lock responsible for `key`."""
        idx = self._stripe_index(key)
        lock = self._locks[idx]
        bucket = self._buckets[idx]
        with lock:
            yield bucket
            if key in bucket:
                bucket.move_to_end(key)
            if self._per_stripe_cap is not None:
                while len(bucket) > self._per_stripe_cap:
                    bucket.popitem(last=False)

    def delete(self, key: str) -> None:
        """Delete a single key under its stripe lock."""
        idx = self._stripe_index(key)
        with self._locks[idx]:
            self._buckets[idx].pop(key, None)

    def clear(self) -> None:
        """Clear all stripes in fixed lock order to avoid deadlocks."""
        for lock, bucket in zip(self._locks, self._buckets):
            with lock:
                bucket.clear()


class BaseRateLimiter(ABC):
    """Abstract base class for synchronous and asynchronous rate limiters."""

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic

    def _now(self) -> float:
        return self._clock()

    @abstractmethod
    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        """Synchronously attempt to acquire `tokens` for `key`."""

    async def aacquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        """Asynchronously attempt to acquire `tokens` for `key`."""
        return self.acquire(key, tokens=tokens)

    @abstractmethod
    def reset(self, key: str) -> None:
        """Synchronously clear state for `key`."""

    async def areset(self, key: str) -> None:
        """Asynchronously clear state for `key`."""
        self.reset(key)
