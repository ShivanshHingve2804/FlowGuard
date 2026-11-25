"""CLI multi-threaded benchmark runner for FlowGuard algorithms."""

from __future__ import annotations

import argparse
import concurrent.futures
import math
import time
from dataclasses import dataclass
from typing import Sequence

from flowguard.algorithms import create_limiter
from flowguard.algorithms.base import BaseRateLimiter
from flowguard.models import AlgorithmName, RateLimitExceeded


@dataclass(frozen=True)
class BenchmarkStats:
    algorithm: str
    backend: str
    requests: int
    concurrency: int
    keys: int
    allowed: int
    rejected: int
    elapsed_seconds: float
    qps: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, math.floor((len(sorted_values) - 1) * pct))
    return sorted_values[idx]


def run_benchmark(
    algorithm: AlgorithmName = "token_bucket",
    *,
    num_requests: int = 10_000,
    concurrency: int = 50,
    keys: int = 1,
    redis_url: str | None = None,
) -> BenchmarkStats:
    """Execute a multi-threaded benchmark and return throughput/latency metrics."""
    if num_requests <= 0:
        raise ValueError("requests must be > 0")
    if concurrency <= 0:
        raise ValueError("concurrency must be > 0")
    if keys <= 0:
        raise ValueError("keys must be > 0")

    capacity = max(1, num_requests // 2)
    window = 4.0
    refill_rate = max(1.0, num_requests / 4.0)
    limiter: BaseRateLimiter = create_limiter(
        algorithm=algorithm,
        limit=capacity,
        window=window,
        rate=refill_rate,
        redis_url=redis_url,
    )
    key_pool = [f"bench-{i}" for i in range(keys)]
    for key in key_pool:
        limiter.reset(key)

    def worker(idx: int) -> tuple[bool, float]:
        target_key = key_pool[idx % keys]
        t0 = time.perf_counter()
        try:
            limiter.acquire(target_key)
            return True, (time.perf_counter() - t0) * 1000.0
        except RateLimitExceeded:
            return False, (time.perf_counter() - t0) * 1000.0

    start_total = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        outcomes = list(executor.map(worker, range(num_requests)))
    total_time = max(1e-9, time.perf_counter() - start_total)

    if hasattr(limiter, "close"):
        limiter.close()

    allowed = sum(1 for ok, _ in outcomes if ok)
    latencies = sorted(lat for _, lat in outcomes)
    return BenchmarkStats(
        algorithm=algorithm,
        backend="redis" if redis_url else "memory",
        requests=num_requests,
        concurrency=concurrency,
        keys=keys,
        allowed=allowed,
        rejected=num_requests - allowed,
        elapsed_seconds=total_time,
        qps=num_requests / total_time,
        avg_ms=sum(latencies) / len(latencies),
        p50_ms=_percentile(latencies, 0.50),
        p95_ms=_percentile(latencies, 0.95),
        p99_ms=_percentile(latencies, 0.99),
    )


def format_report(stats_list: Sequence[BenchmarkStats]) -> str:
    """Format benchmark results into a readable CLI table."""
    lines = ["-" * 92]
    lines.append(
        f"{'Algorithm':<24} {'Backend':<8} {'Allowed':>8} {'Rejected':>8} "
        f"{'QPS':>10} {'p50(ms)':>9} {'p95(ms)':>9} {'p99(ms)':>9}"
    )
    lines.append("-" * 92)
    for s in stats_list:
        lines.append(
            f"{s.algorithm:<24} {s.backend:<8} {s.allowed:>8} {s.rejected:>8} "
            f"{s.qps:>10.1f} {s.p50_ms:>9.4f} {s.p95_ms:>9.4f} {s.p99_ms:>9.4f}"
        )
    lines.append("-" * 92)
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FlowGuard Multi-Threaded Benchmark")
    parser.add_argument(
        "--algorithm",
        choices=[
            "all",
            "token_bucket",
            "leaky_bucket",
            "sliding_window_log",
            "sliding_window_counter",
        ],
        default="token_bucket",
        help="Algorithm to benchmark (or 'all')",
    )
    parser.add_argument("--requests", type=int, default=10000, help="Total requests to run")
    parser.add_argument("--concurrency", type=int, default=50, help="Worker thread count")
    parser.add_argument("--keys", type=int, default=1, help="Distinct keys to distribute load")
    parser.add_argument("--redis-url", type=str, default=None, help="Optional Redis URL")
    args = parser.parse_args(argv)

    algos: list[AlgorithmName] = (
        [
            "token_bucket",
            "leaky_bucket",
            "sliding_window_log",
            "sliding_window_counter",
        ]
        if args.algorithm == "all"
        else [args.algorithm]
    )
    results = [
        run_benchmark(
            algo,
            num_requests=args.requests,
            concurrency=args.concurrency,
            keys=args.keys,
            redis_url=args.redis_url,
        )
        for algo in algos
    ]
    print(format_report(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
