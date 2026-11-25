"""In-memory Token Bucket rate limiting algorithm."""

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
class TokenBucketState:
    current_tokens: float
    last_refill_timestamp: float


class TokenBucketLimiter(BaseRateLimiter):
    """Thread-safe Token Bucket limiter supporting bursts up to `capacity`."""

    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        *,
        clock: Callable[[], float] | None = None,
        max_keys: int | None = 10_000,
        stripes: int = 64,
    ) -> None:
        super().__init__(clock=clock)
        self.capacity = validate_positive_int(capacity, "capacity")
        self.refill_rate = validate_non_negative_float(refill_rate, "refill_rate")
        self._store: StripedStateStore[TokenBucketState] = StripedStateStore(
            stripes=stripes, max_keys=max_keys
        )

    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        validate_acquire_request(key, tokens, self.capacity)

        with self._store.lock_bucket(key) as bucket:
            now = self._now()
            state = bucket.get(key)
            if state is None:
                state = TokenBucketState(
                    current_tokens=float(self.capacity),
                    last_refill_timestamp=now,
                )
                bucket[key] = state

            elapsed = max(0.0, now - state.last_refill_timestamp)
            new_tokens = min(
                float(self.capacity), state.current_tokens + elapsed * self.refill_rate
            )

            if new_tokens >= tokens:
                state.current_tokens = new_tokens - tokens
                state.last_refill_timestamp = now
                allowed = True
                retry_after = 0.0
            else:
                state.current_tokens = new_tokens
                state.last_refill_timestamp = now
                allowed = False
                needed_tokens = tokens - new_tokens
                retry_after = (
                    needed_tokens / self.refill_rate if self.refill_rate > 0 else float("inf")
                )

            reset_after = (
                (self.capacity - state.current_tokens) / self.refill_rate
                if self.refill_rate > 0
                else (0.0 if state.current_tokens >= self.capacity else float("inf"))
            )
            remaining = max(0, int(state.current_tokens))
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
