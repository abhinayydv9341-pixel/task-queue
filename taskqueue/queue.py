from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable
from typing import Any

import redis

from . import scripts
from .job import MAX_PRIORITY, PRIORITY_STRIDE, Job, JobStatus, new_job_id, priority_score, validate_priority
from .retry import RetryPolicy

THROUGHPUT_BUCKET_S = 5
THROUGHPUT_BUCKET_TTL_S = 3600
LATENCY_SAMPLES = 1000
LEASE_LOST_ERROR = "LeaseExpired: worker stopped heartbeating (crashed or stalled)"

JOB_STATES = ("ready", "delayed", "inflight", "dead", "completed")


def now_ms() -> int:
    return int(time.time() * 1000)


class Keys:
    """All Redis keys live under one prefix so several deployments can share a Redis."""

    def __init__(self, prefix: str = "tq") -> None:
        self.prefix = prefix
        self.queues = f"{prefix}:queues"
        self.workers = f"{prefix}:workers"
        self.job_prefix = f"{prefix}:job:"

    def job(self, job_id: str) -> str:
        return f"{self.job_prefix}{job_id}"

    def q(self, queue: str, part: str) -> str:
        return f"{self.prefix}:q:{queue}:{part}"

    def throughput(self, queue: str, bucket: int) -> str:
        return f"{self.prefix}:q:{queue}:tput:{bucket}"


class TaskQueue:
    """A named queue backed by Redis sorted sets.

    ready     ZSET  job id -> priority score (see job.priority_score)
    delayed   ZSET  job id -> eligible-at ms (initial delay and retry back-off)
    inflight  ZSET  job id -> lease deadline ms
    dead      ZSET  job id -> dead-lettered-at ms
    completed ZSET  job id -> finished-at ms (bounded index of recent completions)
    """

    def __init__(
        self,
        client: redis.Redis,
        name: str = "default",
        *,
        prefix: str = "tq",
        retry_policy: RetryPolicy | None = None,
        clock: Callable[[], int] = now_ms,
        job_ttl_s: int = 24 * 3600,
        keep_completed: int = 1000,
    ) -> None:
        if not name or ":" in name:
            raise ValueError("queue name must be non-empty and must not contain ':'")
        self.redis = client
        self.name = name
        self.keys = Keys(prefix)
        self.retry_policy = retry_policy or RetryPolicy()
        self.clock = clock
        self.job_ttl_s = job_ttl_s
        self.keep_completed = keep_completed

        self.k_ready = self.keys.q(name, "ready")
        self.k_delayed = self.keys.q(name, "delayed")
        self.k_inflight = self.keys.q(name, "inflight")
        self.k_dead = self.keys.q(name, "dead")
        self.k_completed = self.keys.q(name, "completed")
        self.k_stats = self.keys.q(name, "stats")
        self.k_latency = self.keys.q(name, "latency")

        self._dequeue = client.register_script(scripts.DEQUEUE)
        self._complete = client.register_script(scripts.COMPLETE)
        self._fail = client.register_script(scripts.FAIL)
        self._extend = client.register_script(scripts.EXTEND)
        self._requeue_dead = client.register_script(scripts.REQUEUE_DEAD)

    # ------------------------------------------------------------------ producing

    def _build(self, task: str, payload: Any, priority: int, delay_ms: int, max_retries: int, job_id: str | None) -> Job:
        if not task:
            raise ValueError("task name is required")
        validate_priority(priority)
        if delay_ms < 0:
            raise ValueError("delay_ms must be >= 0")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        now = self.clock()
        return Job(
            id=job_id or new_job_id(),
            queue=self.name,
            task=task,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
            status=(JobStatus.SCHEDULED if delay_ms else JobStatus.QUEUED).value,
            created_at=now,
            run_at=now + delay_ms,
        )

    def _stage(self, pipe: redis.client.Pipeline, job: Job) -> None:
        pipe.hset(self.keys.job(job.id), mapping=job.to_hash())
        if job.run_at > job.created_at:
            pipe.zadd(self.k_delayed, {job.id: job.run_at})
        else:
            pipe.zadd(self.k_ready, {job.id: priority_score(job.priority, job.run_at)})

    def enqueue(
        self,
        task: str,
        payload: Any = None,
        *,
        priority: int = 0,
        delay_ms: int = 0,
        max_retries: int = 3,
        job_id: str | None = None,
    ) -> Job:
        """Adds a job. `priority` 0-9 (9 runs first); `delay_ms` schedules it for later."""
        return self.enqueue_many(
            [dict(task=task, payload=payload, priority=priority, delay_ms=delay_ms, max_retries=max_retries, job_id=job_id)]
        )[0]

    def enqueue_many(self, specs: Iterable[dict[str, Any]]) -> list[Job]:
        """Enqueues several jobs atomically in a single MULTI/EXEC round trip."""
        jobs = [
            self._build(
                s["task"],
                s.get("payload"),
                s.get("priority", 0),
                s.get("delay_ms", 0),
                s.get("max_retries", 3),
                s.get("job_id"),
            )
            for s in specs
        ]
        if not jobs:
            return []
        pipe = self.redis.pipeline(transaction=True)
        for job in jobs:
            self._stage(pipe, job)
        pipe.sadd(self.keys.queues, self.name)
        pipe.hincrby(self.k_stats, "enqueued", len(jobs))
        pipe.execute()
        return jobs

    # ------------------------------------------------------------------ consuming

    def dequeue(self, count: int = 1, *, lease_ms: int = 30_000, worker_id: str = "anonymous") -> list[Job]:
        """Atomically leases up to `count` jobs, highest priority first.

        A leased job stays in the in-flight set until it is completed, failed, or its lease
        expires (then `reap_expired` hands it to another worker).
        """
        if count < 1:
            raise ValueError("count must be >= 1")
        rows = self._dequeue(
            keys=[self.k_ready, self.k_delayed, self.k_inflight],
            args=[self.clock(), count, lease_ms, worker_id, self.keys.job_prefix, MAX_PRIORITY, PRIORITY_STRIDE],
        )
        return [Job.from_flat(row) for row in rows]

    def complete(self, job: Job, result: Any = None) -> bool:
        """Acks a job. Returns False if the lease was lost (the job was redelivered elsewhere)."""
        now = self.clock()
        bucket = (now // 1000) // THROUGHPUT_BUCKET_S
        ok = self._complete(
            keys=[
                self.k_inflight,
                self.keys.job(job.id),
                self.k_stats,
                self.k_completed,
                self.k_latency,
                self.keys.throughput(self.name, bucket),
            ],
            args=[
                job.id,
                job.attempts,
                now,
                json.dumps(result),
                self.job_ttl_s,
                self.keep_completed,
                THROUGHPUT_BUCKET_TTL_S,
                LATENCY_SAMPLES,
            ],
        )
        return bool(ok)

    def fail(self, job: Job, error: str, *, retryable: bool = True, only_if_expired: bool = False) -> str:
        """Records a failed attempt.

        Returns "retry" (re-scheduled with exponential back-off), "dead" (moved to the
        dead-letter queue after `max_retries` retries, or because it isn't retryable) or
        "stale" (this worker no longer owns the job).
        """
        now = self.clock()
        if retryable and job.attempts <= job.max_retries:
            retry_at = now + self.retry_policy.delay_ms(job.attempts)
        else:
            retry_at = -1
        outcome = self._fail(
            keys=[self.k_inflight, self.k_delayed, self.k_dead, self.keys.job(job.id), self.k_stats],
            args=[job.id, job.attempts, now, error[:2000], retry_at, 1 if only_if_expired else 0],
        )
        return {1: "retry", 2: "dead"}.get(int(outcome), "stale")

    def extend_lease(self, job: Job, lease_ms: int) -> bool:
        """Heartbeat for a long-running job. False means the lease was already lost."""
        ok = self._extend(keys=[self.k_inflight, self.keys.job(job.id)], args=[job.id, job.attempts, self.clock() + lease_ms])
        return bool(ok)

    def reap_expired(self, limit: int = 100) -> dict[str, int]:
        """Recovers jobs whose worker died mid-flight.

        Their leases have expired, so they're treated as a failed attempt: retried with
        back-off, or dead-lettered once retries are exhausted. Safe to run from every worker
        concurrently: the FAIL script re-checks the lease atomically.
        """
        now = self.clock()
        ids = self.redis.zrangebyscore(self.k_inflight, "-inf", now, start=0, num=limit)
        outcome = {"retry": 0, "dead": 0, "stale": 0}
        for job_id in ids:
            data = self.redis.hgetall(self.keys.job(job_id))
            if not data:
                self.redis.zrem(self.k_inflight, job_id)
                continue
            outcome[self.fail(Job.from_hash(data), LEASE_LOST_ERROR, only_if_expired=True)] += 1
        return outcome

    def retry_dead(self, job_id: str) -> bool:
        """Moves a dead-lettered job back to the ready set with a fresh retry budget."""
        data = self.redis.hgetall(self.keys.job(job_id))
        if not data:
            return False
        now = self.clock()
        ok = self._requeue_dead(
            keys=[self.k_dead, self.k_ready, self.keys.job(job_id)],
            args=[job_id, priority_score(int(data["priority"]), now), now],
        )
        return bool(ok)

    # ------------------------------------------------------------------ inspection

    def get_job(self, job_id: str) -> Job | None:
        data = self.redis.hgetall(self.keys.job(job_id))
        return Job.from_hash(data) if data else None

    def _state_key(self, state: str) -> str:
        if state not in JOB_STATES:
            raise ValueError(f"state must be one of {', '.join(JOB_STATES)}")
        return {
            "ready": self.k_ready,
            "delayed": self.k_delayed,
            "inflight": self.k_inflight,
            "dead": self.k_dead,
            "completed": self.k_completed,
        }[state]

    def jobs(self, state: str, limit: int = 50) -> list[Job]:
        """Jobs in a state: ready in dequeue order, others most recent first."""
        key = self._state_key(state)
        ids = (
            self.redis.zrange(key, 0, limit - 1)
            if state in ("ready", "delayed", "inflight")
            else self.redis.zrevrange(key, 0, limit - 1)
        )
        if not ids:
            return []
        pipe = self.redis.pipeline(transaction=False)
        for job_id in ids:
            pipe.hgetall(self.keys.job(job_id))
        return [Job.from_hash(d) for d in pipe.execute() if d]

    def counts(self) -> dict[str, int]:
        pipe = self.redis.pipeline(transaction=False)
        for state in JOB_STATES:
            pipe.zcard(self._state_key(state))
        return dict(zip(JOB_STATES, pipe.execute()))

    def stats(self) -> dict[str, int]:
        raw = self.redis.hgetall(self.k_stats)
        return {k: int(raw.get(k, 0)) for k in ("enqueued", "completed", "failed", "retried", "dead")}

    def throughput(self, window_s: int = 300) -> list[dict[str, int]]:
        """Completions per THROUGHPUT_BUCKET_S-second bucket over the last `window_s` seconds."""
        current = (self.clock() // 1000) // THROUGHPUT_BUCKET_S
        n = max(1, window_s // THROUGHPUT_BUCKET_S)
        buckets = list(range(current - n + 1, current + 1))
        values = self.redis.mget([self.keys.throughput(self.name, b) for b in buckets])
        return [
            {"t": b * THROUGHPUT_BUCKET_S * 1000, "completed": int(v or 0)} for b, v in zip(buckets, values, strict=True)
        ]

    def latency(self) -> dict[str, float | int | None]:
        """End-to-end latency (eligible -> completed) percentiles over recent completions."""
        samples = sorted(int(x) for x in self.redis.lrange(self.k_latency, 0, -1))
        if not samples:
            return {"samples": 0, "p50": None, "p95": None, "p99": None}

        def pct(p: float) -> int:
            return samples[min(len(samples) - 1, int(p * len(samples)))]

        return {"samples": len(samples), "p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)}

    def summary(self) -> dict[str, Any]:
        return {"name": self.name, "counts": self.counts(), "stats": self.stats(), "latency_ms": self.latency()}

    @staticmethod
    def list_names(client: redis.Redis, prefix: str = "tq") -> list[str]:
        return sorted(client.smembers(Keys(prefix).queues))
