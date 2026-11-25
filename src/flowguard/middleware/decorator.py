"""Route-level rate limiting decorator for FastAPI routes and functions."""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, ParamSpec, TypeVar

from fastapi import HTTPException, Request

from flowguard.algorithms import create_limiter
from flowguard.algorithms.base import BaseRateLimiter
from flowguard.middleware.fastapi_middleware import default_key_func
from flowguard.models import AlgorithmName, RateLimitExceeded, RateLimitRule

P = ParamSpec("P")
R = TypeVar("R")


def _extract_request(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Request | None:
    req = kwargs.get("request")
    if isinstance(req, Request):
        return req
    for arg in args:
        if isinstance(arg, Request):
            return arg
    return None


def _resolve_key(
    route_scope: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    key_func: Callable[..., str] | None,
    trust_proxy_headers: bool,
) -> str:
    request = _extract_request(args, kwargs)
    if request is not None:
        raw_key = (
            key_func(request)
            if key_func is not None
            else default_key_func(request, trust_proxy_headers=trust_proxy_headers)
        )
    elif key_func is not None:
        raw_key = key_func(*args, **kwargs)
    else:
        raw_key = "global"
    return f"{route_scope}:{raw_key}"


def _build_http_429(exc: RateLimitExceeded) -> HTTPException:
    return HTTPException(
        status_code=429,
        detail={
            "detail": "Too Many Requests",
            "retry_after": exc.result.retry_after_json,
        },
        headers=exc.result.to_headers(include_retry_after=True),
    )


def rate_limit(
    limit: int = 10,
    window: float = 60.0,
    algorithm: AlgorithmName = "token_bucket",
    *,
    rate: float | None = None,
    tokens: int = 1,
    limiter: BaseRateLimiter | None = None,
    rule: RateLimitRule | None = None,
    key_func: Callable[..., str] | None = None,
    scope: str | None = None,
    trust_proxy_headers: bool = False,
    redis_url: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Enforce per-route or per-function rate limiting across sync and async callables."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        route_limiter = limiter or create_limiter(
            algorithm=algorithm,
            limit=limit,
            window=window,
            rate=rate,
            rule=rule,
            redis_url=redis_url,
        )
        route_scope = scope or f"{func.__module__}.{func.__qualname__}"

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
                key = _resolve_key(route_scope, args, kwargs, key_func, trust_proxy_headers)
                try:
                    await route_limiter.aacquire(key, tokens=tokens)
                except RateLimitExceeded as exc:
                    raise _build_http_429(exc) from exc
                return await func(*args, **kwargs)

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            key = _resolve_key(route_scope, args, kwargs, key_func, trust_proxy_headers)
            try:
                route_limiter.acquire(key, tokens=tokens)
            except RateLimitExceeded as exc:
                raise _build_http_429(exc) from exc
            return func(*args, **kwargs)

        return sync_wrapper

    return decorator
