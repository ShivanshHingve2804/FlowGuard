from abc import ABC, abstractmethod
from typing import Dict, Any
from threading import RLock

from flowguard.models import RateLimitResult

class BaseRateLimiter(ABC):
    """Abstract base class for all rate limiting algorithms."""
    
    def __init__(self) -> None:
        """Initialize the rate limiter with a reentrant lock."""
        self._lock = RLock()
        
    @abstractmethod
    def acquire(self, key: str, tokens: int = 1) -> RateLimitResult:
        """
        Attempt to acquire tokens for a specific key.
        
        Args:
            key (str): The unique identifier for the limit (e.g., IP, user ID).
            tokens (int, optional): Number of tokens to acquire. Defaults to 1.
            
        Returns:
            RateLimitResult: The result of the rate limit evaluation.
        """
        pass
        
    @abstractmethod
    def reset(self, key: str) -> None:
        """
        Reset the rate limit state for a specific key.
        
        Args:
            key (str): The unique identifier to reset.
        """
        pass
