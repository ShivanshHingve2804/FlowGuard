"""
Token Bucket tests.
"""
import pytest
from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.models import RateLimitExceeded

def test_initial_capacity_allows_burst(mock_clock):
    limiter = TokenBucketLimiter(capacity=5, refill_rate=1.0)
    for _ in range(5):
        result = limiter.acquire("test")
        assert result.remaining >= 0
        
def test_exhaustion_returns_429(mock_clock):
    limiter = TokenBucketLimiter(capacity=2, refill_rate=1.0)
    limiter.acquire("test")
    limiter.acquire("test")
    with pytest.raises(RateLimitExceeded):
        limiter.acquire("test")
        
def test_refill_over_time(mock_clock):
    limiter = TokenBucketLimiter(capacity=2, refill_rate=1.0)
    limiter.acquire("test")
    limiter.acquire("test")
    mock_clock.advance(1.1)
    result = limiter.acquire("test")
    assert result.remaining == 0
    
def test_retry_after_calculation(mock_clock):
    limiter = TokenBucketLimiter(capacity=1, refill_rate=1.0)
    limiter.acquire("test")
    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.acquire("test")
    assert exc_info.value.retry_after > 0
