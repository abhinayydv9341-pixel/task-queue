from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import redis

from .job import Job
from .queue import Keys, TaskQueue

log = logging.getLogger("taskqueue.worker")

Handler = Callable[[Any], Any]


class PermanentError(Exception):
    """Raise from a task to dead-letter the job immediately instead of retrying it."""


class TaskRegistry:
    """Maps task names to handler functions: `@registry.task()` or `@registry.task("name")`."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def task(self, name: str | None = None) -> Callable[[Handler], Handler]:
        def decorator(fn: Handler) -> Handler:
            self.register(name or fn.__name__, fn)
            return fn

        return decorator

    def register(self, name: str, fn: Handler) -> None:
        if name in self._handlers:
            raise ValueError(f"task {name!r} is already registered")
        self._handlers[name] = fn

    def get(self, name: str) -> Handler | None:
        return self._handlers.get(name)

    def names(self) -> list[str]:
        return sorted(self._handlers)


class Worker:
    """A worker pool consuming one queue with a fixed number of concurrent slots.

    * A fetch thread leases jobs in batches sized to the free slots (one round trip per
      batch, not per job) and hands them to a thread pool of `concurrency` threads.
    * A heartbeat thread extends leases of running jobs, reaps jobs whose worker died,
      and publishes this pool's utilisation for the dashboard.
    """

    def __init__(
        self,
        queue: TaskQueue,
        registry: TaskRegistry,
        *,
        concurrency: int = 4,
        batch_size: int | None = None,
        lease_ms: int = 30_000,
        poll_interval: float = 0.05,
        heartbeat_interval: float | None = None,
        worker_id: str | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.queue = queue
        self.registry = registry
        self.concurrency = concurrency
        self.batch_size = max(1, batch_size or concurrency)
        self.lease_ms = lease_ms
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval or max(0.05, lease_ms / 3000)
        self.id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{queue.name}-{uuid.uuid4().hex[:6]}"

        self._slots = threading.Semaphore(concurrency)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._running: dict[str, Job] = {}
        self._executor: ThreadPoolExecutor | None = None
        self._threads: list[threading.Thread] = []
        self.started_at: int | None = None
        self.processed = 0
        self.failed = 0
        self.dequeue_calls = 0

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> Worker:
        self.started_at = self.queue.clock()
        self._stop.clear()
        self._executor = ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix=f"tq-{self.queue.name}")
        self._threads = [
            threading.Thread(target=self._fetch_loop, name=f"tq-fetch-{self.queue.name}", daemon=True),
            threading.Thread(target=self._heartbeat_loop, name=f"tq-heartbeat-{self.queue.name}", daemon=True),
        ]
        for t in self._threads:
            t.start()
        log.info("worker %s started on %r (concurrency=%d, batch=%d)", self.id, self.queue.name, self.concurrency, self.batch_size)
        return self

    def stop(self, timeout: float | None = 30) -> None:
        """Graceful shutdown: stop fetching, let running jobs finish, deregister."""
        self._stop.set()
        for t in self._threads:
            t.join(timeout)
        if self._executor:
            self._executor.shutdown(wait=True)
        self._deregister()
        log.info("worker %s stopped (processed=%d failed=%d)", self.id, self.processed, self.failed)

    @property
    def busy(self) -> int:
        with self._lock:
            return len(self._running)

    # ------------------------------------------------------------------ fetching

    def _acquire_slots(self) -> int:
        """Blocks until at least one slot is free, then grabs as many more as are free (up to batch_size)."""
        while not self._stop.is_set():
            if self._slots.acquire(timeout=0.1):
                n = 1
                while n < self.batch_size and self._slots.acquire(blocking=False):
                    n += 1
                return n
        return 0

    def _fetch_loop(self) -> None:
        while not self._stop.is_set():
            n = self._acquire_slots()
            if n == 0:
                return
            try:
                jobs = self.queue.dequeue(n, lease_ms=self.lease_ms, worker_id=self.id)
                self.dequeue_calls += 1
            except redis.RedisError:
                log.exception("dequeue failed")
                jobs = []
            for _ in range(n - len(jobs)):
                self._slots.release()
            if not jobs:
                self._stop.wait(self.poll_interval)
                continue
            with self._lock:
                for job in jobs:
                    self._running[job.id] = job
            for job in jobs:
                assert self._executor is not None
                self._executor.submit(self._execute, job)

    def _execute(self, job: Job) -> None:
        try:
            handler = self.registry.get(job.task)
            if handler is None:
                self.queue.fail(job, f"UnknownTask: no handler registered for {job.task!r}", retryable=False)
                self._count("failed")
                return
            try:
                result = handler(job.payload)
            except PermanentError as exc:
                self.queue.fail(job, f"PermanentError: {exc}", retryable=False)
                self._count("failed")
            except Exception as exc:  # noqa: BLE001 - any task error is a failed attempt
                outcome = self.queue.fail(job, f"{type(exc).__name__}: {exc}")
                self._count("failed")
                log.warning("job %s (%s) attempt %d failed -> %s: %s", job.id, job.task, job.attempts, outcome, exc)
            else:
                if not self.queue.complete(job, result):
                    log.warning("job %s finished after its lease was lost; result discarded", job.id)
                self._count("processed")
        except redis.RedisError:
            # The lease will expire and another worker will retry the job.
            log.exception("could not record outcome of job %s", job.id)
        finally:
            with self._lock:
                self._running.pop(job.id, None)
            self._slots.release()

    def _count(self, attr: str) -> None:
        with self._lock:
            setattr(self, attr, getattr(self, attr) + 1)

    # ------------------------------------------------------------------ heartbeat

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_interval):
            self.heartbeat()

    def heartbeat(self) -> None:
        try:
            with self._lock:
                running = list(self._running.values())
            for job in running:
                self.queue.extend_lease(job, self.lease_ms)
            self.queue.reap_expired()
            self._register()
        except redis.RedisError:
            log.exception("heartbeat failed")

    def info(self) -> dict[str, Any]:
        busy = self.busy
        return {
            "id": self.id,
            "queue": self.queue.name,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "concurrency": self.concurrency,
            "batch_size": self.batch_size,
            "busy": busy,
            "utilisation": busy / self.concurrency,
            "processed": self.processed,
            "failed": self.failed,
            "started_at": self.started_at,
            "last_seen": self.queue.clock(),
            "heartbeat_interval_ms": int(self.heartbeat_interval * 1000),
        }

    def _register(self) -> None:
        self.queue.redis.hset(self.queue.keys.workers, self.id, json.dumps(self.info()))

    def _deregister(self) -> None:
        try:
            self.queue.redis.hdel(self.queue.keys.workers, self.id)
        except redis.RedisError:
            pass


def list_workers(client: redis.Redis, prefix: str = "tq", now: int | None = None) -> list[dict[str, Any]]:
    """Workers that heartbeated recently. Records of crashed workers are pruned."""
    now = now if now is not None else int(time.time() * 1000)
    key = Keys(prefix).workers
    alive, stale = [], []
    for worker_id, raw in client.hgetall(key).items():
        info = json.loads(raw)
        if now - info["last_seen"] > max(3 * info.get("heartbeat_interval_ms", 1000), 5000):
            stale.append(worker_id)
        else:
            alive.append(info)
    if stale:
        client.hdel(key, *stale)
    return sorted(alive, key=lambda w: (w["queue"], w["id"]))


def run_pools(workers: list[Worker], stop_event: threading.Event) -> None:
    """Runs several worker pools in one process until `stop_event` is set."""
    for w in workers:
        w.start()
    try:
        while not stop_event.wait(0.5):
            pass
    finally:
        for w in workers:
            w.stop()
