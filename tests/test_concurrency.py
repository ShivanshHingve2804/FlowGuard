"""
Concurrency tests.
"""
import concurrent.futures
from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.models import RateLimitExceeded

def test_token_bucket_thread_safety():
    limiter = TokenBucketLimiter(capacity=100, refill_rate=0.0)
    key = "concurrent_key"
    
    def worker():
        try:
            limiter.acquire(key)
            return True
        except RateLimitExceeded:
            return False
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        results = list(executor.map(lambda _: worker(), range(150)))
        
    allowed = sum(1 for r in results if r)
    assert allowed == 100
