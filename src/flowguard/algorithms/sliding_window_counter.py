"""In-memory Sliding Window Counter rate limiting algorithm."""

import math
from dataclasses import dataclass
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
class WindowState:
    previous_count: int = 0
    current_count: int = 0
    current_window_index: int = 0


class SlidingWindowCounterLimiter(BaseRateLimiter):
    """Memory-efficient Sliding Window Counter using integer window indices."""

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
        self._store: StripedStateStore[WindowState] = StripedStateStore(
            stripes=stripes, max_keys=max_keys
        )

    def _window_index(self, now: float) -> int:
        # Epsilon prevents IEEE-754 drift (e.g., 0.3 / 0.1 == 2.9999999999999996)
        return math.floor((now / self.window_size) + 1e-9)

    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        validate_acquire_request(key, tokens, self.limit)

        with self._store.lock_bucket(key) as bucket:
            now = self._now()
            window_idx = self._window_index(now)
            state = bucket.get(key)

            if state is None:
                state = WindowState(current_window_index=window_idx)
                bucket[key] = state
            elif window_idx > state.current_window_index:
                delta_windows = window_idx - state.current_window_index
                state.previous_count = state.current_count if delta_windows == 1 else 0
                state.current_count = 0
                state.current_window_index = window_idx
            elif window_idx < state.current_window_index:
                window_idx = state.current_window_index

            window_start = window_idx * self.window_size
            elapsed_in_window = max(0.0, min(self.window_size, now - window_start))
            weight = (self.window_size - elapsed_in_window) / self.window_size
            estimated_count = int(state.previous_count * weight) + state.current_count
            reset_after = max(0.0, self.window_size - elapsed_in_window)

            if estimated_count + tokens <= self.limit:
                state.current_count += tokens
                estimated_count += tokens
                allowed = True
                retry_after = 0.0
            else:
                allowed = False
                if state.current_count + tokens > self.limit:
                    # Must wait into the next window before current_count rotates to previous_count.
                    remaining_curr = self.limit - tokens
                    extra_wait = 0.0
                    if state.current_count > 0 and remaining_curr < state.current_count:
                        next_weight = (remaining_curr + 1) / state.current_count
                        extra_wait = max(0.0, (1.0 - next_weight) * self.window_size)
                    retry_after = reset_after + extra_wait
                else:
                    max_prev = self.limit - tokens - state.current_count
                    if state.previous_count > 0:
                        target_weight = (max_prev + 1) / state.previous_count
                        target_elapsed = max(0.0, (1.0 - target_weight) * self.window_size)
                        retry_after = min(reset_after, max(0.0, target_elapsed - elapsed_in_window))
                    else:
                        retry_after = reset_after

            result = RateLimitResult(
                allowed=allowed,
                limit=self.limit,
                remaining=max(0, self.limit - estimated_count),
                reset_after_seconds=reset_after,
                retry_after_seconds=max(0.0, retry_after),
                current_usage=estimated_count,
            )

            if not allowed:
                raise RateLimitExceeded(result)
            return result

    def reset(self, key: str) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        self._store.delete(key)
