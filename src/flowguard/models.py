"""FlowGuard data models and exceptions."""

from dataclasses import dataclass
from pydantic import BaseModel, Field


class RateLimitRule(BaseModel):
    """Configuration for a rate limit rule."""
    capacity: int = Field(..., description="Maximum allowed tokens/requests")
    period_seconds: float = Field(..., description="Time period in seconds")
    name: str = Field("default", description="Name of the rule")


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


class RateLimitExceeded(Exception):
    """Exception raised when a rate limit is exceeded."""
    def __init__(self, result: RateLimitResult):
        super().__init__(f"Rate limit of {result.limit} exceeded. Retry after {result.retry_after_seconds:.2f}s")
        self.result = result
        self.status_code = 429

    @property
    def retry_after(self) -> float:
        """Convenience accessor for retry_after."""
        return self.result.retry_after_seconds
