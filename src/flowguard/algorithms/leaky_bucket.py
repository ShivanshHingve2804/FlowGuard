import time
from typing import Dict
from dataclasses import dataclass

from flowguard.algorithms.base import BaseRateLimiter
from flowguard.models import RateLimitResult

@dataclass
class LeakyBucketState:
    water_level: float
    last_leak_timestamp: float

class LeakyBucketLimiter(BaseRateLimiter):
    """Leaky Bucket rate limiting algorithm."""
    
    def __init__(self, capacity: int, leak_rate: float) -> None:
        """
        Initialize the Leaky Bucket limiter.
        
        Args:
            capacity (int): Maximum buffer size (water level).
            leak_rate (float): Rate at which the bucket leaks (tokens per second).
        """
        super().__init__()
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._state: Dict[str, LeakyBucketState] = {}
        
    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        """Attempt to add tokens to the leaky bucket."""
        with self._lock:
            now = time.monotonic()
            state = self._state.get(key)
            
            if state is None:
                state = LeakyBucketState(water_level=0.0, last_leak_timestamp=now)
                self._state[key] = state
                
            elapsed = now - state.last_leak_timestamp
            
            # Leak water
            leaked = elapsed * self.leak_rate
            state.water_level = max(0.0, state.water_level - leaked)
            state.last_leak_timestamp = now
            
            if state.water_level + tokens <= self.capacity:
                state.water_level += tokens
                allowed = True
                retry_after = 0.0
            else:
                allowed = False
                needed_leak = (state.water_level + tokens) - self.capacity
                retry_after = needed_leak / self.leak_rate if self.leak_rate > 0 else float('inf')
                
            reset_after = state.water_level / self.leak_rate if self.leak_rate > 0 else float('inf')
            
            return RateLimitResult(
                allowed=allowed,
                remaining=int(self.capacity - state.water_level),
                reset_after_seconds=max(0.0, reset_after),
                retry_after_seconds=max(0.0, retry_after),
                current_usage=int(state.water_level)
            )

    def reset(self, key: str) -> None:
        """Reset the bucket state for a key."""
        with self._lock:
            self._state.pop(key, None)
