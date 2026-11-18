"""REST API polled by the monitoring dashboard (and usable for producing jobs over HTTP)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import redis
from fastapi import FastAPI, HTTPException, Query
from fastapi import Path as PathParam
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .job import MAX_PRIORITY, MIN_PRIORITY
from .queue import JOB_STATES, TaskQueue
from .worker import list_workers

QUEUE_NAME = r"^[A-Za-z0-9_.-]{1,64}$"


class EnqueueRequest(BaseModel):
    task: str = Field(min_length=1, max_length=200)
    payload: Any = None
    priority: int = Field(0, ge=MIN_PRIORITY, le=MAX_PRIORITY)
    delay_ms: int = Field(0, ge=0)
    max_retries: int = Field(3, ge=0, le=100)


def create_app(client: redis.Redis, *, prefix: str = "tq", dashboard_dir: str | Path | None = None) -> FastAPI:
    app = FastAPI(title="Distributed Task Queue", version="1.0.0")

    def queue(name: str) -> TaskQueue:
        return TaskQueue(client, name, prefix=prefix)

    def known_queue(name: str) -> TaskQueue:
        if name not in TaskQueue.list_names(client, prefix):
            raise HTTPException(404, f"queue {name!r} not found")
        return queue(name)

    def find_job(job_id: str):
        for name in TaskQueue.list_names(client, prefix):
            job = queue(name).get_job(job_id)
            if job:
                return job
        raise HTTPException(404, "job not found")

    @app.get("/api/health")
    def health() -> dict[str, str]:
        client.ping()
        return {"status": "ok"}

    @app.get("/api/overview")
    def overview(window_s: int = Query(300, ge=10, le=3600)) -> dict[str, Any]:
        """Everything the dashboard renders, in one poll."""
        queues = []
        for name in TaskQueue.list_names(client, prefix):
            q = queue(name)
            queues.append({**q.summary(), "throughput": q.throughput(window_s)})
        workers = list_workers(client, prefix)
        capacity = sum(w["concurrency"] for w in workers)
        busy = sum(w["busy"] for w in workers)
        return {
            "queues": queues,
            "workers": workers,
            "totals": {
                "workers": len(workers),
                "capacity": capacity,
                "busy": busy,
                "utilisation": busy / capacity if capacity else 0.0,
            },
        }

    @app.get("/api/queues")
    def list_queues() -> list[dict[str, Any]]:
        return [queue(n).summary() for n in TaskQueue.list_names(client, prefix)]

    @app.get("/api/queues/{name}")
    def get_queue(name: str, window_s: int = Query(300, ge=10, le=3600)) -> dict[str, Any]:
        q = known_queue(name)
        return {**q.summary(), "throughput": q.throughput(window_s)}

    @app.get("/api/queues/{name}/jobs")
    def list_jobs(
        name: str,
        state: str = Query("ready", pattern="^(" + "|".join(JOB_STATES) + ")$"),
        limit: int = Query(50, ge=1, le=500),
    ) -> list[dict[str, Any]]:
        return [j.to_dict() for j in known_queue(name).jobs(state, limit)]

    @app.post("/api/queues/{name}/jobs", status_code=201)
    def enqueue(body: EnqueueRequest, name: str = PathParam(pattern=QUEUE_NAME)) -> dict[str, Any]:
        job = queue(name).enqueue(
            body.task, body.payload, priority=body.priority, delay_ms=body.delay_ms, max_retries=body.max_retries
        )
        return job.to_dict()

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        return find_job(job_id).to_dict()

    @app.post("/api/jobs/{job_id}/retry")
    def retry_job(job_id: str) -> dict[str, Any]:
        job = find_job(job_id)
        if not queue(job.queue).retry_dead(job_id):
            raise HTTPException(409, "only dead-lettered jobs can be retried")
        return queue(job.queue).get_job(job_id).to_dict()

    @app.get("/api/workers")
    def workers() -> list[dict[str, Any]]:
        return list_workers(client, prefix)

    if dashboard_dir and Path(dashboard_dir).is_dir():
        app.mount("/", StaticFiles(directory=dashboard_dir, html=True), name="dashboard")

    return app

