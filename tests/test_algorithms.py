"""Algorithm, factory, and top-level export tests for FlowGuard."""

import math

import pytest

import flowguard
from flowguard import (
    FlowGuardMiddleware,
    LeakyBucketLimiter,
    RateLimitExceeded,
    RateLimitRule,
    SlidingWindowCounterLimiter,
    SlidingWindowLogLimiter,
    TokenBucketLimiter,
    create_limiter,
    rate_limit,
)


def test_top_level_exports_are_real_implementations() -> None:
    assert FlowGuardMiddleware is flowguard.middleware.FlowGuardMiddleware
    assert rate_limit is flowguard.middleware.rate_limit


def test_leaky_bucket_leak_zero_rate_and_reset(mock_clock) -> None:
    limiter = LeakyBucketLimiter(capacity=2, leak_rate=1.0, clock=mock_clock)
    limiter.acquire("test")
    limiter.acquire("test")
    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.acquire("test")
    assert exc_info.value.retry_after == pytest.approx(1.0)

    mock_clock.advance(1.1)
    res = limiter.acquire("test")
    assert res.allowed is True
    assert res.remaining + res.current_usage == res.limit

    zero_limiter = LeakyBucketLimiter(capacity=1, leak_rate=0.0, clock=mock_clock)
    zero_limiter.acquire("k")
    with pytest.raises(RateLimitExceeded) as z_exc:
        zero_limiter.acquire("k")
    assert math.isinf(z_exc.value.retry_after)
    zero_limiter.reset("k")
    assert zero_limiter.acquire("k").allowed is True


def test_sliding_window_log_exact_and_multi_token_retry(mock_clock) -> None:
    limiter = SlidingWindowLogLimiter(limit=3, window=10.0, clock=mock_clock)
    limiter.acquire("u1")
    mock_clock.advance(2.0)
    limiter.acquire("u1")
    mock_clock.advance(2.0)
    limiter.acquire("u1")

    # Need 2 tokens -> must wait until second oldest entry (at t=2.0) expires at t=12.0 (8s from t=4.0)
    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.acquire("u1", tokens=2)
    assert exc_info.value.retry_after == pytest.approx(8.0)

    mock_clock.advance(8.0)
    res = limiter.acquire("u1", tokens=2)
    assert res.current_usage == 3
    assert res.remaining == 0

    limiter.reset("u1")
    assert limiter.acquire("u1").current_usage == 1


def test_sliding_window_counter_fractional_precision_and_rollover(mock_clock) -> None:
    # Verify 0.1s window does not suffer 0.3 // 0.1 IEEE-754 drift
    limiter = SlidingWindowCounterLimiter(limit=2, window=0.1, clock=mock_clock)
    mock_clock.advance(0.2)
    limiter.acquire("k")
    mock_clock.advance(0.1)  # now = 0.30000000000000004 -> should be window index 3
    limiter.acquire("k")
    with pytest.raises(RateLimitExceeded):
        limiter.acquire("k")

    # Jump >2 windows clears previous count cleanly
    mock_clock.advance(0.5)
    assert limiter.acquire("k", tokens=2).remaining == 0

    # When current_count == limit, retry_after must cross into next window (> reset_after)
    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.acquire("k")
    assert exc_info.value.retry_after >= exc_info.value.result.reset_after_seconds


def test_rate_limit_rule_and_factory() -> None:
    rule = RateLimitRule(capacity=10, period_seconds=2.0, name="api", algorithm="leaky_bucket")
    assert rule.rate == pytest.approx(5.0)
    limiter = create_limiter(rule=rule)
    assert isinstance(limiter, LeakyBucketLimiter)
    assert isinstance(create_limiter("token_bucket", limit=5, window=1.0), TokenBucketLimiter)
    assert isinstance(
        create_limiter("sliding_window_log", limit=5, window=1.0), SlidingWindowLogLimiter
    )
    assert isinstance(
        create_limiter("sliding_window_counter", limit=5, window=1.0), SlidingWindowCounterLimiter
    )
