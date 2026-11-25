"""Pure ASGI FastAPI/Starlette middleware for FlowGuard rate limiting."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Callable

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from flowguard.algorithms.base import BaseRateLimiter
from flowguard.models import RateLimitExceeded


def default_key_func(request: Request, *, trust_proxy_headers: bool = False) -> str:
    """Extract client key from forwarded headers (when trusted) or peer socket IP."""
    if trust_proxy_headers:
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            first_hop = forwarded_for.split(",", 1)[0].strip()
            if first_hop:
                return first_hop
        real_ip = request.headers.get("x-real-ip")
        if real_ip and real_ip.strip():
            return real_ip.strip()
    return request.client.host if request.client else "127.0.0.1"


class FlowGuardMiddleware:
    """Zero-overhead pure ASGI middleware enforcing FlowGuard rate limits."""

    def __init__(
        self,
        app: ASGIApp,
        limiter: BaseRateLimiter,
        key_func: Callable[[Request], str] | None = None,
        exclude_paths: Iterable[str] | None = None,
        trust_proxy_headers: bool = False,
        tokens: int = 1,
    ) -> None:
        self.app = app
        self.limiter = limiter
        self.trust_proxy_headers = trust_proxy_headers
        self.tokens = tokens
        self.exclude_paths = frozenset(exclude_paths or ())
        self.key_func = key_func or (
            lambda req: default_key_func(req, trust_proxy_headers=self.trust_proxy_headers)
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in self.exclude_paths:
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        key = self.key_func(request)

        try:
            result = await self.limiter.aacquire(key, tokens=self.tokens)
        except RateLimitExceeded as exc:
            response = JSONResponse(
                status_code=429,
                content={
                    "detail": "Too Many Requests",
                    "retry_after": exc.result.retry_after_json,
                },
                headers=exc.result.to_headers(include_retry_after=True),
            )
            await response(scope, receive, send)
            return

        rate_headers = result.to_headers(include_retry_after=False)

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for header_name, header_value in rate_headers.items():
                    headers[header_name] = header_value
            await send(message)

        await self.app(scope, receive, send_with_headers)
