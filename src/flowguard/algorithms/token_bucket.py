import time
from typing import Dict
from dataclasses import dataclass

from flowguard.algorithms.base import BaseRateLimiter
from flowguard.models import RateLimitResult, RateLimitExceeded

@dataclass
class TokenBucketState:
    current_tokens: float
    last_refill_timestamp: float

class TokenBucketLimiter(BaseRateLimiter):
    """Token Bucket rate limiting algorithm."""
    
    def __init__(self, capacity: int, refill_rate: float) -> None:
        super().__init__()
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._state: Dict[str, TokenBucketState] = {}
        
    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        with self._lock:
            now = time.time()
            state = self._state.get(key)
            
            if state is None:
                state = TokenBucketState(
                    current_tokens=self.capacity,
                    last_refill_timestamp=now
                )
                self._state[key] = state
                
            elapsed = now - state.last_refill_timestamp
            
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
            
            result = RateLimitResult(
                allowed=allowed,
                limit=self.capacity,
                remaining=int(state.current_tokens),
                reset_after_seconds=max(0.0, reset_after),
                retry_after_seconds=max(0.0, retry_after),
                current_usage=int(self.capacity - state.current_tokens)
            )
            
            if not allowed:
                raise RateLimitExceeded(result)
            return result
            
    def reset(self, key: str) -> None:
        with self._lock:
            self._state.pop(key, None)
