"""In-memory Sliding Window Log rate limiting algorithm (exact precision)."""

from collections import deque
from dataclasses import dataclass, field
from typing import Callable

from flowguard.algorithms.base import (
    BaseRateLimiter,
    StripedStateStore,
    validate_acquire_request,
    validate_positive_float,
    validate_positive_int,
)
from flowguard.models import RateLimitExceeded, RateLimitResult


@dataclass
class WindowLogState:
    entries: deque[tuple[float, int]] = field(default_factory=deque)
    total_count: int = 0


class SlidingWindowLogLimiter(BaseRateLimiter):
    """Exact Sliding Window Log limiter backed by striped LRU buckets."""

    def __init__(
        self,
        limit: int,
        window: float,
        *,
        clock: Callable[[], float] | None = None,
        max_keys: int | None = 10_000,
        stripes: int = 64,
    ) -> None:
        super().__init__(clock=clock)
        self.limit = validate_positive_int(limit, "limit")
        self.window_size = validate_positive_float(window, "window")
        self._store: StripedStateStore[WindowLogState] = StripedStateStore(
            stripes=stripes, max_keys=max_keys
        )

    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        validate_acquire_request(key, tokens, self.limit)

        with self._store.lock_bucket(key) as bucket:
            now = self._now()
            state = bucket.get(key)
            if state is None:
                state = WindowLogState()
                bucket[key] = state

            cutoff = now - self.window_size
            while state.entries and state.entries[0][0] <= cutoff:
                _, count = state.entries.popleft()
                state.total_count -= count

            if state.total_count + tokens <= self.limit:
                if state.entries and state.entries[-1][0] == now:
                    ts, count = state.entries[-1]
                    state.entries[-1] = (ts, count + tokens)
                else:
                    state.entries.append((now, tokens))
                state.total_count += tokens
                allowed = True
                retry_after = 0.0
            else:
                allowed = False
                need_evict = (state.total_count + tokens) - self.limit
                accumulated = 0
                target_ts = now
                for ts, count in state.entries:
                    accumulated += count
                    if accumulated >= need_evict:
                        target_ts = ts
                        break
                retry_after = (
                    (target_ts + self.window_size - now) if state.entries else self.window_size
                )

            reset_after = (state.entries[0][0] + self.window_size - now) if state.entries else 0.0
            current_usage = state.total_count
            if current_usage == 0:
                bucket.pop(key, None)

            result = RateLimitResult(
                allowed=allowed,
                limit=self.limit,
                remaining=max(0, self.limit - current_usage),
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
