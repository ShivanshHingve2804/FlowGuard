# FlowGuard

FlowGuard is a modern, high-performance rate limiting library for Python. 

## Features
- Multiple Algorithms: Token Bucket, Leaky Bucket, Sliding Window Log, Sliding Window Counter
- Async and Sync Support
- FastAPI Middleware
- Thread-safe implementations

## Quickstart

```python
from fastapi import FastAPI
from flowguard.middleware import FlowGuardMiddleware
from flowguard.algorithms.token_bucket import TokenBucketLimiter

app = FastAPI()
limiter = TokenBucketLimiter(capacity=100, refill_rate=10.0)
app.add_middleware(FlowGuardMiddleware, limiter=limiter)
```

## Algorithms Comparison

| Algorithm | Memory Usage | Burst Handling | Use Case |
|-----------|--------------|----------------|----------|
| Token Bucket | Low | Yes | API Rate Limiting |
| Leaky Bucket | Low | No | Traffic Shaping |
| Sliding Window Log | High | Yes | Precise Limiting |
| Sliding Window Counter | Low | Yes | Distributed Scaling |

## Benchmark

```bash
make benchmark
```
