from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any

MIN_PRIORITY = 0
MAX_PRIORITY = 9

# Ready-set score = (MAX_PRIORITY - priority) * PRIORITY_STRIDE + eligible_at_ms.
# Higher priority sorts first; within a priority, older jobs sort first (FIFO).
# 10^13 ms is ~317 years, so timestamps never cross into the next priority band,
# and the largest score (~9.2e13) stays exactly representable as a double.
PRIORITY_STRIDE = 10**13


class JobStatus(str, Enum):
    QUEUED = "queued"  # in the ready set, waiting for a worker
    SCHEDULED = "scheduled"  # delayed: initial delay or retry back-off
    RUNNING = "running"  # leased by a worker (in-flight)
    COMPLETED = "completed"
    DEAD = "dead"  # exhausted retries; parked in the dead-letter queue


def priority_score(priority: int, eligible_at_ms: int) -> int:
    return (MAX_PRIORITY - priority) * PRIORITY_STRIDE + eligible_at_ms


def validate_priority(priority: int) -> int:
    if not isinstance(priority, int) or isinstance(priority, bool) or not MIN_PRIORITY <= priority <= MAX_PRIORITY:
        raise ValueError(f"priority must be an integer in [{MIN_PRIORITY}, {MAX_PRIORITY}]")
    return priority


def new_job_id() -> str:
    return uuid.uuid4().hex


def _int(value: str | None) -> int | None:
    return int(value) if value not in (None, "") else None


@dataclass
class Job:
    id: str
    queue: str
    task: str
    payload: Any = None
    priority: int = 0
    max_retries: int = 3
    attempts: int = 0
    status: str = JobStatus.QUEUED.value
    created_at: int = 0
    run_at: int = 0  # when the job first became eligible to run (latency is measured from here)
    retry_at: int | None = None
    started_at: int | None = None
    finished_at: int | None = None
    worker_id: str | None = None
    last_error: str | None = None
    result: Any = None

    def to_hash(self) -> dict[str, str]:
        """Fields written to Redis when the job is created."""
        return {
            "id": self.id,
            "queue": self.queue,
            "task": self.task,
            "payload": json.dumps(self.payload),
            "priority": str(self.priority),
            "max_retries": str(self.max_retries),
            "attempts": str(self.attempts),
            "status": self.status,
            "created_at": str(self.created_at),
            "run_at": str(self.run_at),
        }

    @classmethod
    def from_hash(cls, data: dict[str, str]) -> Job:
        return cls(
            id=data["id"],
            queue=data["queue"],
            task=data["task"],
            payload=json.loads(data["payload"]) if data.get("payload") else None,
            priority=int(data.get("priority", 0)),
            max_retries=int(data.get("max_retries", 0)),
            attempts=int(data.get("attempts", 0)),
            status=data.get("status", JobStatus.QUEUED.value),
            created_at=int(data.get("created_at", 0)),
            run_at=int(data.get("run_at", 0)),
            retry_at=_int(data.get("retry_at")),
            started_at=_int(data.get("started_at")),
            finished_at=_int(data.get("finished_at")),
            worker_id=data.get("worker_id") or None,
            last_error=data.get("last_error") or None,
            result=json.loads(data["result"]) if data.get("result") else None,
        )

    @classmethod
    def from_flat(cls, flat: list[str]) -> Job:
        """Builds a job from a flat [field, value, field, value, ...] HGETALL reply (as returned by Lua)."""
        return cls.from_hash(dict(zip(flat[::2], flat[1::2])))

    def to_dict(self) -> dict[str, Any]:
        latency = self.finished_at - self.run_at if self.finished_at and self.status == JobStatus.COMPLETED else None
        return {
            "id": self.id,
            "queue": self.queue,
            "task": self.task,
            "payload": self.payload,
            "priority": self.priority,
            "max_retries": self.max_retries,
            "attempts": self.attempts,
            "status": self.status,
            "created_at": self.created_at,
            "run_at": self.run_at,
            "retry_at": self.retry_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "worker_id": self.worker_id,
            "last_error": self.last_error,
            "result": self.result,
            "latency_ms": latency,
        }
