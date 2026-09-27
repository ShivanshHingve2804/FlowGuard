"""
Tests for other algorithms.
"""
import pytest
from flowguard.algorithms.leaky_bucket import LeakyBucketLimiter
from flowguard.algorithms.sliding_window_log import SlidingWindowLogLimiter
from flowguard.algorithms.sliding_window_counter import SlidingWindowCounterLimiter
from flowguard.models import RateLimitExceeded

def test_leaky_bucket(mock_clock):
    limiter = LeakyBucketLimiter(capacity=2, leak_rate=1.0)
    limiter.acquire("test")
    limiter.acquire("test")
    with pytest.raises(RateLimitExceeded):
        limiter.acquire("test")
    mock_clock.advance(1.1)
    limiter.acquire("test")

def test_sliding_window_log(mock_clock):
    limiter = SlidingWindowLogLimiter(limit=2, window=1.0)
    limiter.acquire("test")
    limiter.acquire("test")
    with pytest.raises(RateLimitExceeded):
        limiter.acquire("test")
    mock_clock.advance(1.1)
    limiter.acquire("test")
    
def test_sliding_window_counter(mock_clock):
    limiter = SlidingWindowCounterLimiter(limit=2, window=1.0)
    limiter.acquire("test")
    limiter.acquire("test")
    with pytest.raises(RateLimitExceeded):
        limiter.acquire("test")
    mock_clock.advance(1.1)
    limiter.acquire("test")
