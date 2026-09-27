"""
FastAPI/Starlette middleware for FlowGuard rate limiting.
"""
from typing import Callable, Awaitable, Optional, Any
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from flowguard.models import RateLimitExceeded

class FlowGuardMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: Any,
        limiter: Any,
        key_func: Optional[Callable[[Request], str]] = None
    ):
        """
        Initialize the middleware.
        
        Args:
            app: The ASGI application.
            limiter: The rate limiter instance.
            key_func: Function to extract the rate limit key from the request.
        """
        super().__init__(app)
        self.limiter = limiter
        self.key_func = key_func or self._default_key_func

    def _default_key_func(self, request: Request) -> str:
        """Default key function using client IP."""
        return request.client.host if request.client else "127.0.0.1"

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Process the request."""
        key = self.key_func(request)
        
        try:
            result = self.limiter.acquire(key)
        except RateLimitExceeded as e:
            return JSONResponse(
                status_code=429,
                content={"detail": "Too Many Requests", "retry_after": e.retry_after},
                headers={"Retry-After": str(int(e.retry_after))}
            )

        response = await call_next(request)
        
        if result:
            response.headers["X-RateLimit-Limit"] = str(result.limit)
            response.headers["X-RateLimit-Remaining"] = str(result.remaining)
            response.headers["X-RateLimit-Reset"] = str(int(result.reset_time))
            
        return response
