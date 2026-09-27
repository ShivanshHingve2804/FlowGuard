from pydantic import BaseModel, Field
from dataclasses import dataclass

class RateLimitRule(BaseModel):
    """Configuration for a rate limit rule."""
    capacity: int = Field(..., description="Maximum allowed tokens/requests")
    period_seconds: float = Field(..., description="Time period in seconds")
    name: str = Field("default", description="Name of the rule")

@dataclass
class RateLimitResult:
    """Result of a rate limit evaluation."""
    allowed: bool
    remaining: int
    reset_after_seconds: float
    retry_after_seconds: float = 0.0
    current_usage: int = 0

class RateLimitExceeded(Exception):
    """Exception raised when a rate limit is exceeded."""
    def __init__(self, result: RateLimitResult):
        super().__init__("Rate limit exceeded")
        self.result = result
        self.status_code = 429
