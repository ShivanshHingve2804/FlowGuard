"""Pytest fixtures for FlowGuard."""

import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.middleware.fastapi_middleware import FlowGuardMiddleware


class MockClock:
    """Deterministic controllable clock for rate-limiter unit tests."""

    def __init__(self, initial: float = 0.0) -> None:
        self.time = initial

    def __call__(self) -> float:
        return self.time

    def advance(self, seconds: float) -> None:
        self.time += seconds


@pytest.fixture
def mock_clock(monkeypatch: pytest.MonkeyPatch) -> MockClock:
    clock = MockClock(0.0)
    monkeypatch.setattr(time, "monotonic", clock)
    monkeypatch.setattr(time, "time", clock)
    return clock


@pytest.fixture
def token_bucket() -> TokenBucketLimiter:
    return TokenBucketLimiter(capacity=10, refill_rate=1.0)


@pytest.fixture
def app(token_bucket: TokenBucketLimiter) -> FastAPI:
    fastapi_app = FastAPI()
    fastapi_app.add_middleware(FlowGuardMiddleware, limiter=token_bucket)

    @fastapi_app.get("/")
    def read_root() -> dict[str, str]:
        return {"Hello": "World"}

    return fastapi_app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)
