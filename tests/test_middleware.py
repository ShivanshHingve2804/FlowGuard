"""Tests for pure ASGI FlowGuardMiddleware, @rate_limit decorator, demo app, and CLI."""

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from examples.demo_app import app as demo_app
from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.cli.benchmark import main as bench_main
from flowguard.middleware import FlowGuardMiddleware, rate_limit


def test_successful_request_headers(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["X-RateLimit-Limit"] == "10"
    assert response.headers["X-RateLimit-Remaining"] == "9"
    assert "X-RateLimit-Reset" in response.headers


def test_rate_limit_exceeded_headers_and_zero_rate_no_overflow() -> None:
    app = FastAPI()
    limiter = TokenBucketLimiter(capacity=2, refill_rate=0.0)
    app.add_middleware(FlowGuardMiddleware, limiter=limiter, exclude_paths={"/health"})

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"ok": "true"}

    @app.get("/items")
    def items() -> dict[str, str]:
        return {"items": "ok"}

    client = TestClient(app)
    for _ in range(5):
        assert client.get("/health").status_code == 200

    assert client.get("/items").status_code == 200
    assert client.get("/items").status_code == 200
    blocked = client.get("/items")
    assert blocked.status_code == 429
    assert blocked.json() == {"detail": "Too Many Requests", "retry_after": None}
    assert blocked.headers["X-RateLimit-Limit"] == "2"
    assert blocked.headers["X-RateLimit-Remaining"] == "0"
    assert "Retry-After" not in blocked.headers


def test_proxy_header_trust_flag() -> None:
    app_untrusted = FastAPI()
    app_untrusted.add_middleware(
        FlowGuardMiddleware,
        limiter=TokenBucketLimiter(capacity=1, refill_rate=0.0),
        trust_proxy_headers=False,
    )

    @app_untrusted.get("/ping")
    def ping_u() -> dict[str, str]:
        return {"ok": "1"}

    client_u = TestClient(app_untrusted)
    assert client_u.get("/ping", headers={"X-Forwarded-For": "1.1.1.1"}).status_code == 200
    # Spoofed X-Forwarded-For must NOT bypass limit when trust_proxy_headers=False
    assert client_u.get("/ping", headers={"X-Forwarded-For": "2.2.2.2"}).status_code == 429

    app_trusted = FastAPI()
    app_trusted.add_middleware(
        FlowGuardMiddleware,
        limiter=TokenBucketLimiter(capacity=1, refill_rate=0.0),
        trust_proxy_headers=True,
    )

    @app_trusted.get("/ping")
    def ping_t() -> dict[str, str]:
        return {"ok": "1"}

    client_t = TestClient(app_trusted)
    assert client_t.get("/ping", headers={"X-Forwarded-For": "1.1.1.1"}).status_code == 200
    assert client_t.get("/ping", headers={"X-Forwarded-For": "2.2.2.2"}).status_code == 200
    assert client_t.get("/ping", headers={"X-Forwarded-For": "2.2.2.2"}).status_code == 429


def test_rate_limit_decorator_sync_async_and_isolation() -> None:
    app = FastAPI()

    @app.get("/a")
    @rate_limit(limit=2, window=60.0, algorithm="sliding_window_log")
    async def route_a(request: Request) -> dict[str, str]:
        return {"route": "a"}

    @app.get("/b")
    @rate_limit(limit=1, window=60.0, algorithm="token_bucket")
    def route_b(request: Request) -> dict[str, str]:
        return {"route": "b"}

    @app.get("/tenant")
    @rate_limit(
        limit=1,
        window=60.0,
        algorithm="sliding_window_counter",
        key_func=lambda req: req.headers.get("X-Tenant", "anon"),
    )
    async def route_tenant(request: Request) -> dict[str, str]:
        return {"ok": "tenant"}

    client = TestClient(app)
    assert client.get("/a").status_code == 200
    assert client.get("/a").status_code == 200
    blocked_a = client.get("/a")
    assert blocked_a.status_code == 429
    assert "Retry-After" in blocked_a.headers

    # Separate route limiter is unaffected by /a exhaustion
    assert client.get("/b").status_code == 200
    assert client.get("/b").status_code == 429

    assert client.get("/tenant", headers={"X-Tenant": "t1"}).status_code == 200
    assert client.get("/tenant", headers={"X-Tenant": "t2"}).status_code == 200
    assert client.get("/tenant", headers={"X-Tenant": "t2"}).status_code == 429


def test_demo_app_and_cli_benchmark(capsys) -> None:
    with TestClient(demo_app) as demo_client:
        assert demo_client.get("/health").status_code == 200
        assert demo_client.get("/").status_code == 200
        assert (
            demo_client.post(
                "/api/reset", json={"capacity": 3, "refill_rate": 0.0, "window": 5}
            ).status_code
            == 200
        )
        burst = demo_client.post("/api/burst/token_bucket", json={"count": 5})
        assert burst.status_code == 200
        outcomes = [item["allowed"] for item in burst.json()["results"]]
        assert outcomes == [True, True, True, False, False]
        bench = demo_client.post("/api/benchmark", json={"requests": 40, "workers": 4})
        assert bench.status_code == 200
        assert "token_bucket" in bench.json()["benchmark"]
        assert demo_client.get("/api/log").status_code == 200

    exit_code = bench_main(
        ["--algorithm", "all", "--requests", "80", "--concurrency", "4", "--keys", "4"]
    )
    assert exit_code == 0
    captured = capsys.readouterr().out
    assert "token_bucket" in captured
    assert "sliding_window_counter" in captured
