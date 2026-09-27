import time
from typing import Dict
from collections import deque

from flowguard.algorithms.base import BaseRateLimiter
from flowguard.models import RateLimitResult, RateLimitExceeded

class SlidingWindowLogLimiter(BaseRateLimiter):
    """Sliding Window Log rate limiting algorithm (Exact precision)."""
    
    def __init__(self, limit: int, window: float) -> None:
        super().__init__()
        self.limit = limit
        self.window_size = window
        self._state: Dict[str, deque[float]] = {}
        
    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        with self._lock:
            now = time.time()
            
            if key not in self._state:
                self._state[key] = deque()
                
            log = self._state[key]
            
            window_start = now - self.window_size
            while log and log[0] <= window_start:
                log.popleft()
                
            current_usage = len(log)
            
            if current_usage + tokens <= self.limit:
                for _ in range(tokens):
                    log.append(now)
                allowed = True
                retry_after = 0.0
                current_usage += tokens
            else:
                allowed = False
                if len(log) > 0:
                    oldest = log[0]
                    retry_after = oldest + self.window_size - now
                else:
                    retry_after = self.window_size
                    
            if len(log) > 0:
                oldest = log[0]
                reset_after = oldest + self.window_size - now
            else:
                reset_after = 0.0
                
            result = RateLimitResult(
                allowed=allowed,
                limit=self.limit,
                remaining=max(0, self.limit - current_usage),
                reset_after_seconds=max(0.0, reset_after),
                retry_after_seconds=max(0.0, retry_after),
                current_usage=current_usage
            )
            
            if not allowed:
                raise RateLimitExceeded(result)
            return result

    def reset(self, key: str) -> None:
        with self._lock:
            self._state.pop(key, None)
