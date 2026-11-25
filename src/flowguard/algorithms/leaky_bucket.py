"""In-memory metered Leaky Bucket rate limiting algorithm."""

from dataclasses import dataclass
from typing import Callable

from flowguard.algorithms.base import (
    BaseRateLimiter,
    StripedStateStore,
    validate_acquire_request,
    validate_non_negative_float,
    validate_positive_int,
)
from flowguard.models import RateLimitExceeded, RateLimitResult


@dataclass
class LeakyBucketState:
    water_level: float
    last_leak_timestamp: float


class LeakyBucketLimiter(BaseRateLimiter):
    """Thread-safe metered Leaky Bucket limiter draining at `leak_rate` tokens/sec."""

    def __init__(
        self,
        capacity: int,
        leak_rate: float,
        *,
        clock: Callable[[], float] | None = None,
        max_keys: int | None = 10_000,
        stripes: int = 64,
    ) -> None:
        super().__init__(clock=clock)
        self.capacity = validate_positive_int(capacity, "capacity")
        self.leak_rate = validate_non_negative_float(leak_rate, "leak_rate")
        self._store: StripedStateStore[LeakyBucketState] = StripedStateStore(
            stripes=stripes, max_keys=max_keys
        )

    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        validate_acquire_request(key, tokens, self.capacity)

        with self._store.lock_bucket(key) as bucket:
            now = self._now()
            state = bucket.get(key)
            if state is None:
                state = LeakyBucketState(water_level=0.0, last_leak_timestamp=now)
                bucket[key] = state

            elapsed = max(0.0, now - state.last_leak_timestamp)
            state.water_level = max(0.0, state.water_level - elapsed * self.leak_rate)
            state.last_leak_timestamp = now

            if state.water_level + tokens <= self.capacity:
                state.water_level += tokens
                allowed = True
                retry_after = 0.0
            else:
                allowed = False
                needed_leak = (state.water_level + tokens) - self.capacity
                retry_after = needed_leak / self.leak_rate if self.leak_rate > 0 else float("inf")

            reset_after = (
                state.water_level / self.leak_rate
                if self.leak_rate > 0
                else (0.0 if state.water_level <= 0.0 else float("inf"))
            )
            remaining = max(0, int(self.capacity - state.water_level))
            current_usage = self.capacity - remaining

            result = RateLimitResult(
                allowed=allowed,
                limit=self.capacity,
                remaining=remaining,
                reset_after_seconds=max(0.0, reset_after),
                retry_after_seconds=max(0.0, retry_after),
                current_usage=current_usage,
            )

            if not allowed:
                raise RateLimitExceeded(result)
            return result

    def reset(self, key: str) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        self._store.delete(key)
