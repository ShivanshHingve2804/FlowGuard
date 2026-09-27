import time
from typing import Dict
from collections import deque

from flowguard.algorithms.base import BaseRateLimiter
from flowguard.models import RateLimitResult

class SlidingWindowLogLimiter(BaseRateLimiter):
    """Sliding Window Log rate limiting algorithm (Exact precision)."""
    
    def __init__(self, limit: int, window_size: float) -> None:
        """
        Initialize the Sliding Window Log limiter.
        
        Args:
            limit (int): Maximum number of requests allowed in the window.
            window_size (float): The time window in seconds.
        """
        super().__init__()
        self.limit = limit
        self.window_size = window_size
        self._state: Dict[str, deque[float]] = {}
        
    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        """Attempt to record a request in the sliding log."""
        with self._lock:
            now = time.monotonic()
            
            if key not in self._state:
                self._state[key] = deque()
                
            log = self._state[key]
            
            # Prune old entries
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
                
            return RateLimitResult(
                allowed=allowed,
                remaining=max(0, self.limit - current_usage),
                reset_after_seconds=max(0.0, reset_after),
                retry_after_seconds=max(0.0, retry_after),
                current_usage=current_usage
            )

    def reset(self, key: str) -> None:
        """Reset the log for a key."""
        with self._lock:
            self._state.pop(key, None)
