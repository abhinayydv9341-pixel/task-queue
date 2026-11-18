from __future__ import annotations

import threading
import time

import pytest

from taskqueue import PermanentError, TaskQueue, TaskRegistry, Worker, list_workers


def wait_until(predicate, timeout=10.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise AssertionError("condition not met in time")


@pytest.fixture
def registry():
    return TaskRegistry()


def test_registry_rejects_duplicates(registry):
    @registry.task()
    def a(payload):
        return payload

    with pytest.raises(ValueError):
        registry.register("a", lambda p: p)
    assert registry.names() == ["a"]


def test_worker_processes_jobs(live_queue, registry):
    registry.register("double", lambda p: p * 2)
    jobs = live_queue.enqueue_many([{"task": "double", "payload": i} for i in range(20)])
    worker = Worker(live_queue, registry, concurrency=4).start()
    try:
        wait_until(lambda: live_queue.counts()["completed"] == 20)
    finally:
        worker.stop()
    assert [live_queue.get_job(j.id).result for j in jobs] == [i * 2 for i in range(20)]
    assert worker.processed == 20


def test_flaky_job_is_retried_until_it_succeeds(live_queue, registry):
    calls = []

    @registry.task()
    def flaky(payload):
        calls.append(time.monotonic())
        if len(calls) < 3:
            raise ConnectionError("try again")
        return "finally"

    job = live_queue.enqueue("flaky", max_retries=5)
    worker = Worker(live_queue, registry, concurrency=1).start()
    try:
        wait_until(lambda: live_queue.get_job(job.id).status == "completed")
    finally:
        worker.stop()
    done = live_queue.get_job(job.id)
    assert done.attempts == 3
    assert done.result == "finally"
    assert done.last_error == "ConnectionError: try again"
    assert live_queue.stats()["retried"] == 2
    # back-off of 10ms then 20ms between attempts
    assert calls[1] - calls[0] >= 0.009 and calls[2] - calls[1] >= 0.019


def test_unknown_task_and_permanent_errors_are_dead_lettered(live_queue, registry):
    @registry.task()
    def charge(payload):
        raise PermanentError("card declined")

    unknown = live_queue.enqueue("no_such_task", max_retries=5)
    permanent = live_queue.enqueue("charge", max_retries=5)
    worker = Worker(live_queue, registry, concurrency=2).start()
    try:
        wait_until(lambda: live_queue.counts()["dead"] == 2)
    finally:
        worker.stop()
    assert live_queue.get_job(unknown.id).attempts == 1
    assert "UnknownTask" in live_queue.get_job(unknown.id).last_error
    assert live_queue.get_job(permanent.id).last_error == "PermanentError: card declined"


def test_concurrency_limit_is_respected(live_queue, registry):
    lock = threading.Lock()
    state = {"now": 0, "peak": 0}

    @registry.task()
    def slow(payload):
        with lock:
            state["now"] += 1
            state["peak"] = max(state["peak"], state["now"])
        time.sleep(0.03)
        with lock:
            state["now"] -= 1

    live_queue.enqueue_many([{"task": "slow"} for _ in range(30)])
    worker = Worker(live_queue, registry, concurrency=3).start()
    try:
        wait_until(lambda: live_queue.counts()["completed"] == 30)
    finally:
        worker.stop()
    assert state["peak"] == 3


def test_batched_dequeue_fills_all_free_slots_in_one_round_trip(client, registry, fast_retry):
    release = threading.Event()
    registry.register("block", lambda p: release.wait(5))

    def leases_needed_to_fill_8_slots(batch_size):
        q = TaskQueue(client, f"batch{batch_size}", retry_policy=fast_retry)
        q.enqueue_many([{"task": "block"} for _ in range(16)])
        w = Worker(q, registry, concurrency=8, batch_size=batch_size).start()
        try:
            wait_until(lambda: w.busy == 8)
            return w.dequeue_calls
        finally:
            release.set()
            w.stop()
            release.clear()

    assert leases_needed_to_fill_8_slots(1) == 8
    assert leases_needed_to_fill_8_slots(8) == 1


def test_high_priority_jobs_overtake_a_backlog(live_queue, registry):
    order = []
    registry.register("record", lambda p: order.append(p))
    live_queue.enqueue_many([{"task": "record", "payload": f"low{i}", "priority": 0} for i in range(10)])
    live_queue.enqueue("record", "urgent", priority=9)
    worker = Worker(live_queue, registry, concurrency=1, batch_size=1).start()
    try:
        wait_until(lambda: len(order) == 11)
    finally:
        worker.stop()
    assert order[0] == "urgent"


def test_worker_publishes_heartbeat_and_deregisters(live_queue, registry):
    started = threading.Event()
    release = threading.Event()

    @registry.task()
    def block(payload):
        started.set()
        release.wait(5)

    live_queue.enqueue("block")
    worker = Worker(live_queue, registry, concurrency=4, heartbeat_interval=0.05, worker_id="w-test").start()
    try:
        assert started.wait(5)
        wait_until(lambda: any(w["busy"] == 1 for w in list_workers(live_queue.redis)))
        [info] = list_workers(live_queue.redis)
        assert info["id"] == "w-test"
        assert info["queue"] == "live"
        assert info["concurrency"] == 4
        assert info["utilisation"] == 0.25
    finally:
        release.set()
        worker.stop()
    assert list_workers(live_queue.redis) == []


def test_stale_worker_records_are_pruned(client):
    import json

    client.hset("tq:workers", "ghost", json.dumps({"id": "ghost", "queue": "q", "last_seen": 0, "heartbeat_interval_ms": 1000}))
    assert list_workers(client, now=60_000) == []
    assert client.hlen("tq:workers") == 0


def test_graceful_stop_finishes_running_jobs(live_queue, registry):
    started = threading.Event()

    @registry.task()
    def slow(payload):
        started.set()
        time.sleep(0.3)
        return "done"

    job = live_queue.enqueue("slow")
    worker = Worker(live_queue, registry, concurrency=1).start()
    assert started.wait(5)
    worker.stop()
    assert live_queue.get_job(job.id).status == "completed"


def test_separate_pools_have_independent_concurrency(client, registry, fast_retry):
    lock = threading.Lock()
    peaks = {"emails": [0, 0], "reports": [0, 0]}

    def make(queue_name):
        def handler(payload):
            with lock:
                p = peaks[queue_name]
                p[0] += 1
                p[1] = max(p[1], p[0])
            time.sleep(0.02)
            with lock:
                peaks[queue_name][0] -= 1

        return handler

    registry.register("email", make("emails"))
    registry.register("report", make("reports"))
    emails = TaskQueue(client, "emails", retry_policy=fast_retry)
    reports = TaskQueue(client, "reports", retry_policy=fast_retry)
    emails.enqueue_many([{"task": "email"} for _ in range(30)])
    reports.enqueue_many([{"task": "report"} for _ in range(10)])

    pools = [Worker(emails, registry, concurrency=5).start(), Worker(reports, registry, concurrency=1).start()]
    try:
        wait_until(lambda: emails.counts()["completed"] == 30 and reports.counts()["completed"] == 10)
    finally:
        for p in pools:
            p.stop()
    assert peaks["emails"][1] == 5
    assert peaks["reports"][1] == 1
