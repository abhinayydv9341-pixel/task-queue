from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from taskqueue import TaskQueue, TaskRegistry, Worker
from taskqueue.api import create_app


@pytest.fixture
def api(client):
    return TestClient(create_app(client))


def test_health(api):
    assert api.get("/api/health").json() == {"status": "ok"}


def test_enqueue_and_fetch_job(api):
    res = api.post("/api/queues/emails/jobs", json={"task": "send_email", "payload": {"to": "x"}, "priority": 7})
    assert res.status_code == 201
    job = res.json()
    assert job["status"] == "queued" and job["priority"] == 7

    fetched = api.get(f"/api/jobs/{job['id']}").json()
    assert fetched["payload"] == {"to": "x"}
    assert api.get("/api/jobs/nope").status_code == 404


@pytest.mark.parametrize(
    "body",
    [{"task": "t", "priority": 10}, {"task": "t", "delay_ms": -1}, {"task": ""}, {"payload": 1}],
)
def test_enqueue_validation(api, body):
    assert api.post("/api/queues/q/jobs", json=body).status_code == 422


def test_bad_queue_name(api):
    assert api.post("/api/queues/bad name!/jobs", json={"task": "t"}).status_code == 422


def test_queue_summary_and_job_listing(api, client):
    q = TaskQueue(client, "media")
    q.enqueue("resize", priority=1)
    q.enqueue("resize", delay_ms=60_000)
    summary = api.get("/api/queues/media").json()
    assert summary["counts"]["ready"] == 1
    assert summary["counts"]["delayed"] == 1
    assert summary["stats"]["enqueued"] == 2
    assert len(summary["throughput"]) == 60

    assert len(api.get("/api/queues/media/jobs?state=delayed").json()) == 1
    assert api.get("/api/queues/media/jobs?state=nonsense").status_code == 422
    assert api.get("/api/queues/missing").status_code == 404


def test_retry_dead_letter_via_api(api, client):
    q = TaskQueue(client, "billing")
    job = q.enqueue("charge", max_retries=0)
    [leased] = q.dequeue()
    q.fail(leased, "boom")

    dead = api.get("/api/queues/billing/jobs?state=dead").json()
    assert [j["id"] for j in dead] == [job.id]
    res = api.post(f"/api/jobs/{job.id}/retry")
    assert res.status_code == 200 and res.json()["status"] == "queued"
    assert api.post(f"/api/jobs/{job.id}/retry").status_code == 409


def test_overview_reports_queues_workers_and_utilisation(api, client):
    q = TaskQueue(client, "default")
    q.enqueue("t")
    registry = TaskRegistry()
    worker = Worker(q, registry, concurrency=4, worker_id="w1")
    worker.heartbeat()  # publish the record without starting threads
    try:
        body = api.get("/api/overview").json()
        assert [x["name"] for x in body["queues"]] == ["default"]
        assert body["queues"][0]["counts"]["ready"] == 1
        assert body["totals"] == {"workers": 1, "capacity": 4, "busy": 0, "utilisation": 0.0}
        assert api.get("/api/workers").json()[0]["id"] == "w1"
    finally:
        worker.stop()
