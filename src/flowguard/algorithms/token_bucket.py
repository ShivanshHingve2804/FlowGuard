import time
from typing import Dict
from dataclasses import dataclass

from flowguard.algorithms.base import BaseRateLimiter
from flowguard.models import RateLimitResult

@dataclass
class TokenBucketState:
    current_tokens: float
    last_refill_timestamp: float

class TokenBucketLimiter(BaseRateLimiter):
    """Token Bucket rate limiting algorithm."""
    
    def __init__(self, capacity: int, refill_rate: float) -> None:
        """
        Initialize the Token Bucket limiter.
        
        Args:
            capacity (int): Maximum burst size / bucket capacity.
            refill_rate (float): Number of tokens added to the bucket per second.
        """
        super().__init__()
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._state: Dict[str, TokenBucketState] = {}
        
    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        """Attempt to acquire tokens from the bucket."""
        with self._lock:
            now = time.monotonic()
            state = self._state.get(key)
            
            if state is None:
                state = TokenBucketState(
                    current_tokens=self.capacity,
                    last_refill_timestamp=now
                )
                self._state[key] = state
                
            elapsed = now - state.last_refill_timestamp
            
            # Refill tokens based on elapsed time
            new_tokens = min(self.capacity, state.current_tokens + elapsed * self.refill_rate)
            
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
                retry_after = needed_tokens / self.refill_rate if self.refill_rate > 0 else float('inf')
                
            reset_after = (self.capacity - state.current_tokens) / self.refill_rate if self.refill_rate > 0 else float('inf')
            
            return RateLimitResult(
                allowed=allowed,
                remaining=int(state.current_tokens),
                reset_after_seconds=max(0.0, reset_after),
                retry_after_seconds=max(0.0, retry_after),
                current_usage=int(self.capacity - state.current_tokens)
            )
            
    def reset(self, key: str) -> None:
        """Reset the bucket state for a key."""
        with self._lock:
            self._state.pop(key, None)
