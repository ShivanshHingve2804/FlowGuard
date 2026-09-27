import time
from typing import Dict
from dataclasses import dataclass

from flowguard.algorithms.base import BaseRateLimiter
from flowguard.models import RateLimitResult, RateLimitExceeded

@dataclass
class WindowState:
    previous_count: int = 0
    current_count: int = 0
    current_window_start: float = 0.0

class SlidingWindowCounterLimiter(BaseRateLimiter):
    """Sliding Window Counter rate limiting algorithm (Memory-efficient)."""
    
    def __init__(self, limit: int, window: float) -> None:
        super().__init__()
        self.limit = limit
        self.window_size = window
        self._state: Dict[str, WindowState] = {}
        
    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        with self._lock:
            now = time.time()
            state = self._state.get(key)
            
            current_window_start = (now // self.window_size) * self.window_size
            
            if state is None:
                state = WindowState(current_window_start=current_window_start)
                self._state[key] = state
                
            if state.current_window_start != current_window_start:
                time_diff = current_window_start - state.current_window_start
                if time_diff >= 2 * self.window_size:
                    state.previous_count = 0
                else:
                    state.previous_count = state.current_count
                    
                state.current_count = 0
                state.current_window_start = current_window_start
                
            time_into_window = now - current_window_start
            weight = (self.window_size - time_into_window) / self.window_size
            
            estimated_count = int(state.previous_count * weight) + state.current_count
            
            if estimated_count + tokens <= self.limit:
                state.current_count += tokens
                allowed = True
                retry_after = 0.0
                estimated_count += tokens
            else:
                allowed = False
                needed_reduction = (estimated_count + tokens) - self.limit
                reduction_rate = state.previous_count / self.window_size if state.previous_count > 0 else 1.0
                retry_after = needed_reduction / reduction_rate if state.previous_count > 0 else self.window_size
                
            reset_after = self.window_size - time_into_window
            
            result = RateLimitResult(
                allowed=allowed,
                limit=self.limit,
                remaining=max(0, self.limit - estimated_count),
                reset_after_seconds=max(0.0, reset_after),
                retry_after_seconds=max(0.0, retry_after),
                current_usage=estimated_count
            )
            
            if not allowed:
                raise RateLimitExceeded(result)
            return result

    def reset(self, key: str) -> None:
        with self._lock:
            self._state.pop(key, None)
