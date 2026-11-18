from __future__ import annotations

import pytest

from taskqueue import TaskQueue


def test_enqueue_persists_job(queue):
    job = queue.enqueue("send_email", {"to": "a@example.com"})
    stored = queue.get_job(job.id)
    assert stored.task == "send_email"
    assert stored.payload == {"to": "a@example.com"}
    assert stored.status == "queued"
    assert stored.attempts == 0
    assert queue.counts()["ready"] == 1


def test_dequeue_leases_job(queue, clock):
    job = queue.enqueue("t")
    [leased] = queue.dequeue(lease_ms=5_000, worker_id="w1")
    assert leased.id == job.id
    assert leased.status == "running"
    assert leased.attempts == 1
    assert leased.worker_id == "w1"
    assert leased.started_at == clock.now
    assert queue.counts() == {"ready": 0, "delayed": 0, "inflight": 1, "dead": 0, "completed": 0}
    assert queue.redis.zscore(queue.k_inflight, job.id) == clock.now + 5_000


def test_dequeue_empty_queue(queue):
    assert queue.dequeue(5) == []


def test_higher_priority_runs_first(queue):
    low = queue.enqueue("t", priority=0)
    high = queue.enqueue("t", priority=9)
    mid = queue.enqueue("t", priority=5)
    assert [j.id for j in queue.dequeue(3)] == [high.id, mid.id, low.id]


def test_fifo_within_same_priority(queue, clock):
    ids = []
    for _ in range(5):
        ids.append(queue.enqueue("t", priority=3).id)
        clock.advance(1)
    assert [j.id for j in queue.dequeue(5)] == ids


def test_old_low_priority_job_still_waits_for_new_high_priority(queue, clock):
    old_low = queue.enqueue("t", priority=1)
    clock.advance(10 * 24 * 3600 * 1000)  # ten days later
    new_high = queue.enqueue("t", priority=2)
    assert [j.id for j in queue.dequeue(2)] == [new_high.id, old_low.id]


def test_delayed_job_is_not_dequeued_early(queue, clock):
    job = queue.enqueue("t", delay_ms=10_000)
    assert queue.get_job(job.id).status == "scheduled"
    assert queue.counts()["delayed"] == 1
    clock.advance(9_999)
    assert queue.dequeue() == []


def test_delayed_job_is_dequeued_once_due(queue, clock):
    job = queue.enqueue("t", delay_ms=10_000)
    clock.advance(10_000)
    [leased] = queue.dequeue()
    assert leased.id == job.id
    assert queue.counts()["delayed"] == 0


def test_promoted_delayed_job_respects_priority(queue, clock):
    waiting_low = queue.enqueue("t", priority=0)
    delayed_high = queue.enqueue("t", priority=9, delay_ms=1_000)
    clock.advance(1_000)
    assert [j.id for j in queue.dequeue(2)] == [delayed_high.id, waiting_low.id]


def test_batch_dequeue_takes_up_to_count_in_priority_order(queue):
    jobs = [queue.enqueue("t", priority=p) for p in (1, 7, 3, 9, 5)]
    batch = queue.dequeue(3)
    assert [j.priority for j in batch] == [9, 7, 5]
    assert queue.counts()["ready"] == 2
    assert queue.counts()["inflight"] == 3
    assert {j.id for j in jobs} >= {j.id for j in batch}


def test_batch_dequeue_with_fewer_jobs_than_requested(queue):
    queue.enqueue("t")
    queue.enqueue("t")
    assert len(queue.dequeue(10)) == 2


def test_complete_records_result_and_stats(queue, clock):
    job = queue.enqueue("t")
    clock.advance(250)
    [leased] = queue.dequeue()
    clock.advance(750)
    assert queue.complete(leased, {"ok": True}) is True
    done = queue.get_job(job.id)
    assert done.status == "completed"
    assert done.result == {"ok": True}
    assert done.finished_at == clock.now
    assert done.to_dict()["latency_ms"] == 1_000
    assert queue.counts()["inflight"] == 0
    assert queue.counts()["completed"] == 1
    assert queue.stats()["completed"] == 1


def test_completed_job_hash_expires(queue, client):
    queue.enqueue("t")
    [leased] = queue.dequeue()
    queue.complete(leased)
    assert 0 < client.ttl(queue.keys.job(leased.id)) <= queue.job_ttl_s


def test_completed_index_is_bounded(client, clock, no_jitter):
    q = TaskQueue(client, "bounded", clock=clock, retry_policy=no_jitter, keep_completed=3)
    for _ in range(5):
        q.enqueue("t")
    for job in q.dequeue(5):
        clock.advance(1)
        q.complete(job)
    assert q.counts()["completed"] == 3


def test_failed_job_is_retried_with_exponential_backoff(queue, clock):
    job = queue.enqueue("t", max_retries=3)
    expected_delays = [1_000, 2_000, 4_000]
    for attempt, delay in enumerate(expected_delays, start=1):
        [leased] = queue.dequeue()
        assert leased.attempts == attempt
        assert queue.fail(leased, "boom") == "retry"
        stored = queue.get_job(job.id)
        assert stored.status == "scheduled"
        assert stored.retry_at == clock.now + delay
        clock.advance(delay - 1)
        assert queue.dequeue() == [], "must not run before back-off elapses"
        clock.advance(1)


def test_job_is_dead_lettered_after_max_retries(queue, clock):
    job = queue.enqueue("t", max_retries=2)
    outcomes = []
    for _ in range(3):
        [leased] = queue.dequeue()
        outcomes.append(queue.fail(leased, "ValueError: nope"))
        clock.advance(60_000)
    assert outcomes == ["retry", "retry", "dead"]
    dead = queue.get_job(job.id)
    assert dead.status == "dead"
    assert dead.attempts == 3
    assert dead.last_error == "ValueError: nope"
    assert queue.counts()["dead"] == 1
    assert queue.dequeue() == []


def test_non_retryable_failure_goes_straight_to_dead_letter(queue):
    queue.enqueue("t", max_retries=5)
    [leased] = queue.dequeue()
    assert queue.fail(leased, "PermanentError: bad input", retryable=False) == "dead"
    assert queue.counts()["dead"] == 1


def test_zero_retries_means_one_attempt(queue):
    queue.enqueue("t", max_retries=0)
    [leased] = queue.dequeue()
    assert queue.fail(leased, "x") == "dead"


def test_stats_counters(queue, clock):
    for _ in range(3):
        queue.enqueue("t", max_retries=1)
    a, b, c = queue.dequeue(3)
    queue.complete(a)
    queue.fail(b, "x")  # retry
    queue.fail(c, "x", retryable=False)  # dead
    assert queue.stats() == {"enqueued": 3, "completed": 1, "failed": 2, "retried": 1, "dead": 1}


def test_retry_dead_requeues_with_fresh_budget(queue, clock):
    job = queue.enqueue("t", max_retries=0, priority=4)
    [leased] = queue.dequeue()
    queue.fail(leased, "x")
    assert queue.retry_dead(job.id) is True
    requeued = queue.get_job(job.id)
    assert requeued.status == "queued"
    assert requeued.attempts == 0
    assert queue.counts()["dead"] == 0
    [again] = queue.dequeue()
    assert again.id == job.id and again.attempts == 1


def test_retry_dead_rejects_jobs_that_are_not_dead(queue):
    job = queue.enqueue("t")
    assert queue.retry_dead(job.id) is False
    assert queue.retry_dead("does-not-exist") is False


@pytest.mark.parametrize("priority", [-1, 10, 1.5, True])
def test_invalid_priority_is_rejected(queue, priority):
    with pytest.raises(ValueError):
        queue.enqueue("t", priority=priority)


def test_invalid_arguments(queue, client):
    with pytest.raises(ValueError):
        queue.enqueue("")
    with pytest.raises(ValueError):
        queue.enqueue("t", delay_ms=-1)
    with pytest.raises(ValueError):
        queue.dequeue(0)
    with pytest.raises(ValueError):
        TaskQueue(client, "bad:name")


def test_enqueue_many_is_one_atomic_batch(queue):
    jobs = queue.enqueue_many([{"task": "t", "payload": i, "priority": i % 10} for i in range(20)])
    assert len(jobs) == 20
    assert queue.counts()["ready"] == 20
    assert queue.stats()["enqueued"] == 20


def test_payload_round_trips_as_json(queue):
    payload = {"nested": {"list": [1, 2.5, "three", None, True]}, "unicode": "héllo ✓"}
    queue.enqueue("t", payload)
    [leased] = queue.dequeue()
    assert leased.payload == payload


def test_jobs_listing_by_state(queue, clock):
    a = queue.enqueue("t", priority=1)
    b = queue.enqueue("t", priority=2)
    c = queue.enqueue("t", delay_ms=5_000)
    assert [j.id for j in queue.jobs("ready")] == [b.id, a.id]
    assert [j.id for j in queue.jobs("delayed")] == [c.id]
    with pytest.raises(ValueError):
        queue.jobs("bogus")


def test_throughput_buckets_count_completions(queue, clock):
    for _ in range(4):
        queue.enqueue("t")
    for job in queue.dequeue(4):
        queue.complete(job)
    series = queue.throughput(window_s=60)
    assert len(series) == 12
    assert series[-1]["completed"] == 4
    assert sum(p["completed"] for p in series[:-1]) == 0


def test_latency_percentiles(queue, clock):
    assert queue.latency()["p50"] is None
    for i in range(1, 11):
        queue.enqueue("t")
        [job] = queue.dequeue()
        clock.advance(i * 10)
        queue.complete(job)
    lat = queue.latency()
    assert lat["samples"] == 10
    assert lat["p50"] == 60
    assert lat["p99"] == 100


def test_queue_names_are_discoverable(client, clock):
    TaskQueue(client, "emails", clock=clock).enqueue("t")
    TaskQueue(client, "media", clock=clock).enqueue("t")
    assert TaskQueue.list_names(client) == ["emails", "media"]


def test_queues_are_isolated(client, clock):
    a = TaskQueue(client, "a", clock=clock)
    b = TaskQueue(client, "b", clock=clock)
    a.enqueue("t")
    assert b.dequeue() == []
    assert len(a.dequeue()) == 1
