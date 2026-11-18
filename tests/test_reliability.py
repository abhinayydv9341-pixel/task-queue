"""At-least-once delivery: a job leased by a worker that dies is never lost."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

from taskqueue import RetryPolicy, TaskQueue, TaskRegistry, Worker
from taskqueue.queue import LEASE_LOST_ERROR

from .conftest import REDIS_URL, real_redis_only

ROOT = Path(__file__).resolve().parent.parent


def test_expired_lease_is_reaped_and_redelivered(queue, clock):
    job = queue.enqueue("t")
    queue.dequeue(lease_ms=5_000, worker_id="crashed-worker")  # ...and the worker dies

    clock.advance(5_001)
    assert queue.reap_expired() == {"retry": 1, "dead": 0, "stale": 0}
    stored = queue.get_job(job.id)
    assert stored.status == "scheduled"
    assert stored.last_error == LEASE_LOST_ERROR

    clock.advance(1_000)  # back-off for attempt 1
    [redelivered] = queue.dequeue(worker_id="healthy-worker")
    assert redelivered.id == job.id
    assert redelivered.attempts == 2
    assert queue.complete(redelivered) is True


def test_live_lease_is_not_reaped(queue, clock):
    queue.enqueue("t")
    queue.dequeue(lease_ms=5_000)
    clock.advance(4_999)
    assert queue.reap_expired() == {"retry": 0, "dead": 0, "stale": 0}
    assert queue.counts()["inflight"] == 1


def test_heartbeat_extends_lease(queue, clock):
    queue.enqueue("t")
    [job] = queue.dequeue(lease_ms=5_000)
    for _ in range(5):
        clock.advance(4_000)
        assert queue.extend_lease(job, 5_000) is True
    assert queue.reap_expired()["retry"] == 0
    assert queue.complete(job) is True


def test_stale_worker_cannot_ack_a_redelivered_job(queue, clock):
    """Worker A stalls past its lease; the job goes to worker B. A's late ack must not count."""
    job = queue.enqueue("t")
    [as_seen_by_a] = queue.dequeue(lease_ms=1_000, worker_id="A")
    clock.advance(1_001)
    queue.reap_expired()
    clock.advance(1_000)
    [as_seen_by_b] = queue.dequeue(lease_ms=1_000, worker_id="B")

    assert queue.complete(as_seen_by_a, "from A") is False
    assert queue.fail(as_seen_by_a, "late failure from A") == "stale"
    assert queue.extend_lease(as_seen_by_a, 1_000) is False
    assert queue.complete(as_seen_by_b, "from B") is True
    assert queue.get_job(job.id).result == "from B"
    assert queue.stats()["completed"] == 1


def test_reaper_does_not_steal_a_job_whose_lease_was_just_extended(queue, clock):
    queue.enqueue("t")
    [job] = queue.dequeue(lease_ms=1_000)
    clock.advance(1_001)
    queue.extend_lease(job, 1_000)  # heartbeat lands just before the reaper runs
    # Simulate the reaper having read a stale snapshot: call fail with only_if_expired.
    assert queue.fail(job, LEASE_LOST_ERROR, only_if_expired=True) == "stale"
    assert queue.counts()["inflight"] == 1


def test_job_that_keeps_crashing_workers_ends_in_dead_letter(queue, clock):
    """A poison message that kills every worker must not loop forever."""
    job = queue.enqueue("t", max_retries=2)
    for _ in range(3):
        clock.advance(60_000)
        assert len(queue.dequeue(lease_ms=1_000)) == 1
        clock.advance(1_001)
        queue.reap_expired()
    assert queue.get_job(job.id).status == "dead"
    assert queue.stats()["dead"] == 1


def test_many_crashed_leases_are_all_recovered(queue, clock):
    jobs = queue.enqueue_many([{"task": "t", "payload": i} for i in range(50)])
    queue.dequeue(50, lease_ms=1_000)
    clock.advance(1_001)
    assert queue.reap_expired(limit=100)["retry"] == 50
    clock.advance(1_000)
    redelivered = queue.dequeue(100)
    assert {j.id for j in redelivered} == {j.id for j in jobs}


def test_worker_thread_crash_recovered_by_another_worker(live_queue):
    """In-process version: worker A leases jobs and is killed without acking; worker B finishes them."""
    jobs = live_queue.enqueue_many([{"task": "noop", "payload": i} for i in range(10)])
    live_queue.dequeue(10, lease_ms=200, worker_id="killed")  # A leases everything, then dies

    registry = TaskRegistry()
    registry.register("noop", lambda p: p)
    b = Worker(live_queue, registry, concurrency=4, lease_ms=200, heartbeat_interval=0.05).start()
    try:
        deadline = time.time() + 10
        while live_queue.counts()["completed"] < 10 and time.time() < deadline:
            time.sleep(0.05)
    finally:
        b.stop()
    for job in jobs:
        stored = live_queue.get_job(job.id)
        assert stored.status == "completed"
        assert stored.attempts == 2
        assert stored.result == job.payload


CRASHING_WORKER = textwrap.dedent(
    """
    import os, sys, time
    import redis
    from taskqueue import RetryPolicy, TaskQueue, TaskRegistry, Worker

    client = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    queue = TaskQueue(client, "crashy", retry_policy=RetryPolicy(base_delay_ms=10, jitter=0))
    registry = TaskRegistry()
    seen = 0

    @registry.task()
    def work(payload):
        global seen
        seen += 1
        time.sleep(0.02)
        if seen == 5:
            os._exit(137)  # hard crash mid-job: no ack, no cleanup, leases left dangling
        return payload

    Worker(queue, registry, concurrency=4, lease_ms=500).start()
    time.sleep(30)
    """
)


@real_redis_only
def test_hard_killed_worker_process_loses_no_jobs(client):
    """A real worker *process* dies with jobs in flight; a second process completes every job."""
    queue = TaskQueue(client, "crashy", retry_policy=RetryPolicy(base_delay_ms=10, jitter=0))
    jobs = queue.enqueue_many([{"task": "work", "payload": i} for i in range(40)])

    env = {**os.environ, "REDIS_URL": REDIS_URL, "PYTHONPATH": str(ROOT)}
    crashed = subprocess.run([sys.executable, "-c", CRASHING_WORKER], env=env, timeout=30)
    assert crashed.returncode == 137
    assert queue.counts()["inflight"] > 0, "the crash should have left leased jobs behind"

    registry = TaskRegistry()
    registry.register("work", lambda p: p)
    survivor = Worker(queue, registry, concurrency=8, lease_ms=500, heartbeat_interval=0.1).start()
    try:
        deadline = time.time() + 20
        while queue.counts()["completed"] < len(jobs) and time.time() < deadline:
            time.sleep(0.1)
    finally:
        survivor.stop()

    statuses = [queue.get_job(j.id).status for j in jobs]
    assert statuses.count("completed") == len(jobs)
    assert queue.counts() == {"ready": 0, "delayed": 0, "inflight": 0, "dead": 0, "completed": len(jobs)}
    # Jobs in flight during the crash were delivered twice: at-least-once, never zero.
    assert any(queue.get_job(j.id).attempts > 1 for j in jobs)
