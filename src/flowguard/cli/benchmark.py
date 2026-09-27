"""
CLI Benchmark script for FlowGuard.
"""
import concurrent.futures
import time
import argparse
from typing import List, Tuple
from flowguard.algorithms.token_bucket import TokenBucketLimiter
from flowguard.models import RateLimitExceeded

def benchmark(num_requests: int, concurrency: int) -> None:
    """Run the benchmark."""
    limiter = TokenBucketLimiter(capacity=num_requests // 2, refill_rate=num_requests // 4)
    key = "benchmark_key"
    
    allowed = 0
    rejected = 0
    latencies: List[float] = []
    
    def worker() -> Tuple[bool, float]:
        start_time = time.perf_counter()
        try:
            limiter.acquire(key)
            latency = (time.perf_counter() - start_time) * 1000
            return True, latency
        except RateLimitExceeded:
            latency = (time.perf_counter() - start_time) * 1000
            return False, latency
            
    print(f"Starting benchmark with {num_requests} requests and concurrency {concurrency}...")
    start_total = time.perf_counter()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker) for _ in range(num_requests)]
        for future in concurrent.futures.as_completed(futures):
            success, latency = future.result()
            latencies.append(latency)
            if success:
                allowed += 1
            else:
                rejected += 1
                
    total_time = time.perf_counter() - start_total
    qps = num_requests / total_time
    avg_latency = sum(latencies) / len(latencies)
    latencies.sort()
    p95_latency = latencies[int(len(latencies) * 0.95)]
    
    print("-" * 50)
    print("Benchmark Results")
    print("-" * 50)
    print(f"Total Requests  : {num_requests}")
    print(f"Concurrency     : {concurrency}")
    print(f"Allowed         : {allowed}")
    print(f"Rejected        : {rejected}")
    print(f"Allowed Ratio   : {allowed / num_requests * 100:.2f}%")
    print(f"Throughput (QPS): {qps:.2f}")
    print(f"Avg Latency     : {avg_latency:.4f} ms")
    print(f"P95 Latency     : {p95_latency:.4f} ms")
    print("-" * 50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FlowGuard Benchmark")
    parser.add_argument("--requests", type=int, default=10000, help="Number of requests to simulate")
    parser.add_argument("--concurrency", type=int, default=50, help="Number of worker threads")
    args = parser.parse_args()
    benchmark(args.requests, args.concurrency)
