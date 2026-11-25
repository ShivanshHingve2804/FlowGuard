"""Token Bucket tests (sync, async, validation, eviction, and edge cases)."""

import math

import pytest

from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.models import RateLimitExceeded


def test_initial_capacity_allows_burst(mock_clock) -> None:
    limiter = TokenBucketLimiter(capacity=5, refill_rate=1.0, clock=mock_clock)
    for expected_rem in (4, 3, 2, 1, 0):
        result = limiter.acquire("test")
        assert result.allowed is True
        assert result.remaining == expected_rem
        assert result.remaining + result.current_usage == result.limit


def test_exhaustion_returns_429(mock_clock) -> None:
    limiter = TokenBucketLimiter(capacity=2, refill_rate=1.0, clock=mock_clock)
    limiter.acquire("test")
    limiter.acquire("test")
    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.acquire("test")
    assert exc_info.value.status_code == 429
    assert exc_info.value.result.allowed is False
    assert exc_info.value.result.remaining == 0


def test_refill_over_time(mock_clock) -> None:
    limiter = TokenBucketLimiter(capacity=2, refill_rate=1.0, clock=mock_clock)
    limiter.acquire("test")
    limiter.acquire("test")
    mock_clock.advance(1.1)
    result = limiter.acquire("test")
    assert result.remaining == 0


def test_retry_after_calculation(mock_clock) -> None:
    limiter = TokenBucketLimiter(capacity=1, refill_rate=1.0, clock=mock_clock)
    limiter.acquire("test")
    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.acquire("test")
    assert exc_info.value.retry_after == pytest.approx(1.0)


def test_zero_refill_rate_infinite_retry_and_reset(mock_clock) -> None:
    limiter = TokenBucketLimiter(capacity=2, refill_rate=0.0, clock=mock_clock)
    limiter.acquire("k", tokens=2)
    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.acquire("k")
    assert math.isinf(exc_info.value.retry_after)
    assert exc_info.value.result.retry_after_json is None

    limiter.reset("k")
    res = limiter.acquire("k")
    assert res.remaining == 1


def test_backward_clock_jump_clamped(mock_clock) -> None:
    limiter = TokenBucketLimiter(capacity=3, refill_rate=1.0, clock=mock_clock)
    mock_clock.advance(10.0)
    limiter.acquire("k")
    mock_clock.advance(-5.0)
    res = limiter.acquire("k")
    assert res.remaining == 1


def test_validation_and_lru_eviction(mock_clock) -> None:
    with pytest.raises(ValueError):
        TokenBucketLimiter(capacity=0, refill_rate=1.0)
    with pytest.raises(ValueError):
        TokenBucketLimiter(capacity=5, refill_rate=-1.0)

    limiter = TokenBucketLimiter(
        capacity=2, refill_rate=0.0, clock=mock_clock, max_keys=1, stripes=1
    )
    with pytest.raises(ValueError):
        limiter.acquire("", tokens=1)
    with pytest.raises(ValueError):
        limiter.acquire("k1", tokens=0)
    with pytest.raises(ValueError):
        limiter.acquire("k1", tokens=3)

    limiter.acquire("k1", tokens=2)
    limiter.acquire("k2", tokens=1)
    # k1 was evicted by LRU max_keys=1
    assert limiter.acquire("k1", tokens=2).remaining == 0


async def test_async_acquire_and_reset(mock_clock) -> None:
    limiter = TokenBucketLimiter(capacity=2, refill_rate=0.0, clock=mock_clock)
    r1 = await limiter.aacquire("async-k")
    assert r1.remaining == 1
    await limiter.areset("async-k")
    r2 = await limiter.aacquire("async-k")
    assert r2.remaining == 1
