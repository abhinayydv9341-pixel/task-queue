"""End-to-end latency benchmark: one-job-per-round-trip dequeue vs batched dequeue.

Enqueues a burst of synthetic jobs (each sleeps `--work-ms` to stand in for I/O-bound
work), runs one worker pool until the queue drains, and reads every job's
`finished_at - run_at` from Redis. Each configuration runs `--repeat` times on a clean
key prefix; the reported numbers are medians across runs.

    python bench/latency.py --redis-url redis://localhost:6379/0
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
import uuid
from pathlib import Path

import redis

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from taskqueue import RetryPolicy, TaskQueue, TaskRegistry, Worker  # noqa: E402


def run_once(client: redis.Redis, *, jobs: int, concurrency: int, batch_size: int, work_ms: float) -> dict:
    prefix = f"bench-{uuid.uuid4().hex[:8]}"
    queue = TaskQueue(client, "bench", prefix=prefix, retry_policy=RetryPolicy(jitter=0))
    registry = TaskRegistry()
    registry.register("work", lambda _: time.sleep(work_ms / 1000))

    ids: list[str] = []
    for start in range(0, jobs, 1000):
        ids += [j.id for j in queue.enqueue_many([{"task": "work"} for _ in range(min(1000, jobs - start))])]

    worker = Worker(queue, registry, concurrency=concurrency, batch_size=batch_size, poll_interval=0.01)
    t0 = time.perf_counter()
    worker.start()
    while queue.counts()["completed"] < jobs:
        time.sleep(0.01)
    elapsed = time.perf_counter() - t0
    worker.stop()

    pipe = client.pipeline(transaction=False)
    for job_id in ids:
        pipe.hmget(queue.keys.job(job_id), "run_at", "finished_at")
    rows = pipe.execute()
    latencies = sorted(int(f) - int(r) for r, f in rows)

    for key in client.scan_iter(f"{prefix}:*", count=1000):
        client.delete(key)

    return {
        "p50": latencies[len(latencies) // 2],
        "p95": latencies[int(len(latencies) * 0.95)],
        "throughput": jobs / elapsed,
        "dequeue_calls": worker.dequeue_calls,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--jobs", type=int, default=5000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--work-ms", type=float, default=2.0)
    parser.add_argument("--batch-sizes", default="1,16", help="comma separated; the first is the baseline")
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()

    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    client.ping()
    batch_sizes = [int(b) for b in args.batch_sizes.split(",")]

    results = {}
    for batch in batch_sizes:
        runs = [
            run_once(client, jobs=args.jobs, concurrency=args.concurrency, batch_size=batch, work_ms=args.work_ms)
            for _ in range(args.repeat)
        ]
        results[batch] = {k: statistics.median(r[k] for r in runs) for k in runs[0]}

    base = results[batch_sizes[0]]
    print(
        f"\n{args.jobs} jobs x {args.work_ms} ms work, concurrency {args.concurrency}, "
        f"median of {args.repeat} runs, Python {sys.version.split()[0]}\n"
    )
    print("| dequeue batch size | p50 latency (ms) | p95 latency (ms) | throughput (jobs/s) | dequeue round trips | p50 vs baseline |")
    print("|---|---|---|---|---|---|")
    for batch, r in results.items():
        change = (r["p50"] - base["p50"]) / base["p50"] * 100 if base["p50"] else 0.0
        print(
            f"| {batch} | {r['p50']:.0f} | {r['p95']:.0f} | {r['throughput']:.0f} | {r['dequeue_calls']:.0f} | "
            f"{'baseline' if batch == batch_sizes[0] else f'{change:+.1f}%'} |"
        )


if __name__ == "__main__":
    main()
