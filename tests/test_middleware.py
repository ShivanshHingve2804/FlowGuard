"""
Middleware tests.
"""
def test_successful_request_headers(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "X-RateLimit-Limit" in response.headers
    assert "X-RateLimit-Remaining" in response.headers
    assert "X-RateLimit-Reset" in response.headers

def test_rate_limit_exceeded(client):
    for _ in range(10):
        client.get("/")
    response = client.get("/")
    assert response.status_code == 429
    assert response.json()["detail"] == "Too Many Requests"
    assert "Retry-After" in response.headers
