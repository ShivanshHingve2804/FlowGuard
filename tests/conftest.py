"""
Pytest fixtures.
"""
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.algorithms.leaky_bucket import LeakyBucketLimiter
from flowguard.algorithms.sliding_window_log import SlidingWindowLogLimiter
from flowguard.algorithms.sliding_window_counter import SlidingWindowCounterLimiter
from flowguard.middleware.fastapi_middleware import FlowGuardMiddleware

@pytest.fixture
def mock_clock(monkeypatch):
    class MockClock:
        def __init__(self):
            self.time = 0.0
        def __call__(self):
            return self.time
        def advance(self, seconds: float):
            self.time += seconds
            
    clock = MockClock()
    import time
    monkeypatch.setattr(time, "time", clock)
    return clock

@pytest.fixture
def token_bucket():
    return TokenBucketLimiter(capacity=10, refill_rate=1.0)

@pytest.fixture
def app(token_bucket):
    fastapi_app = FastAPI()
    fastapi_app.add_middleware(FlowGuardMiddleware, limiter=token_bucket)
    
    @fastapi_app.get("/")
    def read_root():
        return {"Hello": "World"}
        
    return fastapi_app

@pytest.fixture
def client(app):
    return TestClient(app)
