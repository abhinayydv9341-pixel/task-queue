"""Command line entry point.

    python -m taskqueue worker --app examples.tasks:registry --pool default=8 --pool emails=2
    python -m taskqueue api --port 8000
    python -m taskqueue enqueue send_email '{"to": "a@example.com"}' --queue emails --priority 5
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import signal
import threading

import redis

from .queue import TaskQueue
from .retry import RetryPolicy
from .worker import TaskRegistry, Worker, run_pools


def load_registry(path: str) -> TaskRegistry:
    module_name, _, attr = path.partition(":")
    registry = getattr(importlib.import_module(module_name), attr or "registry")
    if not isinstance(registry, TaskRegistry):
        raise SystemExit(f"{path} is not a TaskRegistry")
    return registry


def parse_pool(spec: str) -> tuple[str, int]:
    name, _, concurrency = spec.partition("=")
    try:
        return name, int(concurrency or 4)
    except ValueError:
        raise argparse.ArgumentTypeError(f"pool must look like QUEUE=CONCURRENCY, got {spec!r}") from None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="taskqueue")
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--prefix", default=os.environ.get("TQ_PREFIX", "tq"))
    sub = parser.add_subparsers(dest="command", required=True)

    w = sub.add_parser("worker", help="run one or more worker pools")
    w.add_argument("--app", required=True, help="module:attribute of a TaskRegistry")
    w.add_argument("--pool", action="append", type=parse_pool, default=None, help="QUEUE=CONCURRENCY (repeatable)")
    w.add_argument("--batch-size", type=int, default=None, help="max jobs leased per dequeue (default: concurrency)")
    w.add_argument("--lease-ms", type=int, default=30_000)
    w.add_argument("--retry-base-ms", type=int, default=1_000)
    w.add_argument("--retry-max-ms", type=int, default=300_000)

    a = sub.add_parser("api", help="serve the REST API and dashboard")
    a.add_argument("--host", default="0.0.0.0")
    a.add_argument("--port", type=int, default=8000)
    a.add_argument("--dashboard-dir", default=os.environ.get("DASHBOARD_DIR", "dashboard/dist"))

    e = sub.add_parser("enqueue", help="enqueue a single job")
    e.add_argument("task")
    e.add_argument("payload", nargs="?", default="null", help="JSON payload")
    e.add_argument("--queue", default="default")
    e.add_argument("--priority", type=int, default=0)
    e.add_argument("--delay-ms", type=int, default=0)
    e.add_argument("--max-retries", type=int, default=3)

    args = parser.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    client = redis.Redis.from_url(args.redis_url, decode_responses=True)

    if args.command == "worker":
        registry = load_registry(args.app)
        policy = RetryPolicy(base_delay_ms=args.retry_base_ms, max_delay_ms=args.retry_max_ms)
        workers = [
            Worker(
                TaskQueue(client, name, prefix=args.prefix, retry_policy=policy),
                registry,
                concurrency=concurrency,
                batch_size=args.batch_size,
                lease_ms=args.lease_ms,
            )
            for name, concurrency in (args.pool or [("default", 4)])
        ]
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        run_pools(workers, stop)

    elif args.command == "api":
        import uvicorn

        from .api import create_app

        uvicorn.run(create_app(client, prefix=args.prefix, dashboard_dir=args.dashboard_dir), host=args.host, port=args.port)

    elif args.command == "enqueue":
        job = TaskQueue(client, args.queue, prefix=args.prefix).enqueue(
            args.task,
            json.loads(args.payload),
            priority=args.priority,
            delay_ms=args.delay_ms,
            max_retries=args.max_retries,
        )
        print(job.id)


if __name__ == "__main__":
    main()
