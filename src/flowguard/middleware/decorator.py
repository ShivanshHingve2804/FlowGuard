"""
Decorator for route-level rate limiting.
"""
import functools
import asyncio
from typing import Callable, Any
from fastapi import Request, HTTPException
from flowguard.models import RateLimitExceeded

def rate_limit(limit: int = 10, window: int = 60, algorithm: str = "token_bucket"):
    """
    Rate limit decorator.
    
    Args:
        limit: The maximum number of requests allowed.
        window: The time window in seconds.
        algorithm: The rate limiting algorithm to use.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            request = kwargs.get("request") or next((arg for arg in args if isinstance(arg, Request)), None)
            if not request:
                raise ValueError("Request object not found in arguments")
                
            limiter = getattr(request.app.state, "limiter", None)
            if not limiter:
                raise RuntimeError("Limiter not found in app state")
                
            key = request.client.host if request.client else "127.0.0.1"
            try:
                limiter.acquire(key)
            except RateLimitExceeded as e:
                raise HTTPException(
                    status_code=429,
                    detail={"error": "Too Many Requests", "retry_after": e.retry_after},
                    headers={"Retry-After": str(int(e.retry_after))}
                )
            return await func(*args, **kwargs)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            request = kwargs.get("request") or next((arg for arg in args if isinstance(arg, Request)), None)
            if not request:
                raise ValueError("Request object not found in arguments")
                
            limiter = getattr(request.app.state, "limiter", None)
            if not limiter:
                raise RuntimeError("Limiter not found in app state")
                
            key = request.client.host if request.client else "127.0.0.1"
            try:
                limiter.acquire(key)
            except RateLimitExceeded as e:
                raise HTTPException(
                    status_code=429,
                    detail={"error": "Too Many Requests", "retry_after": e.retry_after},
                    headers={"Retry-After": str(int(e.retry_after))}
                )
            return func(*args, **kwargs)
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator
