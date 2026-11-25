"""FlowGuard data models, rule schemas, and exceptions."""

import math
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, Field

AlgorithmName = Literal[
    "token_bucket",
    "leaky_bucket",
    "sliding_window_log",
    "sliding_window_counter",
]


class RateLimitRule(BaseModel):
    """Validated configuration rule for creating rate limiters."""

    capacity: int = Field(..., gt=0, description="Maximum allowed tokens or requests")
    period_seconds: float = Field(..., gt=0.0, description="Replenishment or window duration (s)")
    name: str = Field("default", min_length=1, description="Logical rule identifier")
    algorithm: AlgorithmName = Field("token_bucket", description="Target rate-limiting algorithm")

    @property
    def rate(self) -> float:
        """Return average replenishment/leak rate in tokens per second."""
        return self.capacity / self.period_seconds


@dataclass
class RateLimitResult:
    """Result of a rate limit evaluation."""

    allowed: bool
    limit: int
    remaining: int
    reset_after_seconds: float
    retry_after_seconds: float = 0.0
    current_usage: int = 0

    @property
    def retry_after(self) -> float:
        """Alias for retry_after_seconds."""
        return self.retry_after_seconds

    @property
    def retry_after_json(self) -> float | None:
        """JSON-safe retry delay (`None` when infinite, e.g. zero refill rate)."""
        if math.isinf(self.retry_after_seconds) or math.isnan(self.retry_after_seconds):
            return None
        return round(max(0.0, self.retry_after_seconds), 6)

    def to_headers(self, *, include_retry_after: bool = True) -> dict[str, str]:
        """Build standard HTTP rate-limit headers without overflowing on infinite durations."""
        reset_value = (
            "0"
            if math.isinf(self.reset_after_seconds) or math.isnan(self.reset_after_seconds)
            else str(max(0, math.ceil(self.reset_after_seconds)))
        )
        headers = {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Reset": reset_value,
        }
        if (
            include_retry_after
            and not self.allowed
            and not math.isinf(self.retry_after_seconds)
            and not math.isnan(self.retry_after_seconds)
        ):
            headers["Retry-After"] = str(max(1, math.ceil(self.retry_after_seconds)))
        return headers


class RateLimitExceeded(Exception):
    """Exception raised when a rate limit is exceeded."""

    def __init__(self, result: RateLimitResult) -> None:
        retry_label = (
            "inf"
            if math.isinf(result.retry_after_seconds)
            else f"{result.retry_after_seconds:.2f}s"
        )
        super().__init__(f"Rate limit of {result.limit} exceeded. Retry after {retry_label}")
        self.result = result
        self.status_code = 429

    @property
    def retry_after(self) -> float:
        """Convenience accessor for retry_after."""
        return self.result.retry_after_seconds
