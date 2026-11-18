# Distributed Task Queue & Job Scheduler

A background job processing system built on Redis sorted sets. It supports job priorities, delayed scheduling, worker pools with configurable concurrency, retries with exponential back-off, a dead-letter queue, and at-least-once delivery when a worker crashes. A React dashboard polls a REST API to show queue depth, job statuses, throughput and per-worker utilisation in real time.

**Stack:** Python 3.11+ · Redis 7 (Lua scripts) · FastAPI · React 18 + Recharts (Vite) · pytest · Docker Compose · GitHub Actions

```
 producers ──enqueue──►  Redis                                 ◄──poll── React dashboard
 (lib / CLI / REST)      ┌────────────────────────────────┐              │
                         │ ready     ZSET priority+FIFO   │        FastAPI /api/*
                         │ delayed   ZSET run-at ms       │              │
                         │ inflight  ZSET lease deadline  │◄─────────────┘
                         │ dead      ZSET  (DLQ)          │
                         │ job:<id>  HASH                 │
                         └──────┬──────────────▲──────────┘
                    batched     │              │ ack / fail / heartbeat / reap
                    dequeue     ▼              │
                 ┌─────────────────────────────┴───┐
                 │ Worker pool  (queue=emails, 8×) │   … one pool per queue,
                 │ fetch thread → thread pool      │     any number of processes
                 │ heartbeat thread                │
                 └─────────────────────────────────┘
```

## Features

- **Priorities.** Each job has a priority from 0 to 9 (9 runs first). Jobs with the same priority run in FIFO order. Both are encoded in a single sorted-set score: `(9 - priority) · 10¹³ + eligible_at_ms`.
- **Delayed scheduling.** `delay_ms` puts a job in the `delayed` set. When it becomes due, the dequeue script moves it to `ready`, in the same atomic call.
- **Batched dequeue.** A Lua script leases up to *N* jobs in one round trip. A worker asks for as many jobs as it has free slots (up to `batch_size`), instead of one per call.
- **Configurable concurrency per worker pool.** Each `Worker` consumes one queue with a fixed number of thread slots. One process can run several pools (`--pool emails=8 --pool reports=2`), and you can start as many processes as you need.
- **Exponential back-off retries.** The delay is `base · factor^(attempt-1)`, capped, with ±jitter. After `max_retries` the job moves to the **dead-letter queue**. Raising `PermanentError` (or calling an unknown task) skips retries and dead-letters the job immediately. Dead jobs can be re-queued from the API or the dashboard.
- **At-least-once delivery.** A leased job lives in `inflight`, scored by its lease deadline. Workers heartbeat to extend their leases. If a worker dies, its leases expire and any worker's reaper re-schedules those jobs as a failed attempt. A crash can't loop forever: repeated crashes on the same job use up its retries and it ends in the DLQ. A stalled worker's late ack is rejected because acks carry the attempt number (fencing).
- **Monitoring.** Per-queue counters, completions per 5-second bucket, and an end-to-end latency sample window (p50/p95/p99). Worker heartbeat records report busy/concurrency (utilisation) and are pruned when a worker stops heartbeating.

## Quick start

```bash
docker compose up --build
```

Then open <http://localhost:8000>. Compose starts Redis, the API (which also serves the dashboard), two worker processes each running four pools, and a producer that generates a synthetic workload. Some demo tasks fail on purpose, so you can watch retries and the dead-letter queue.

### Without Docker

```bash
pip install -e ".[dev]"
python -m taskqueue api --port 8000                          # REST API (+ dashboard if built)
python -m taskqueue worker --app examples.tasks:registry --pool emails=8 --pool media=4
python -m taskqueue enqueue send_email '{"to":"a@example.com"}' --queue emails --priority 9

cd dashboard && npm install && npm run dev                   # dashboard dev server, proxies /api to :8000
```

## Library usage

```python
import redis
from taskqueue import TaskQueue, TaskRegistry, Worker, RetryPolicy, PermanentError

client = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
emails = TaskQueue(client, "emails", retry_policy=RetryPolicy(base_delay_ms=1000, max_delay_ms=300_000))

# produce
emails.enqueue("send_email", {"to": "a@example.com"}, priority=9)
emails.enqueue("send_email", {"to": "b@example.com"}, delay_ms=60_000, max_retries=5)

# consume
registry = TaskRegistry()

@registry.task()
def send_email(payload):
    if not payload.get("to"):
        raise PermanentError("no recipient")   # dead-letter now, don't retry
    ...                                        # any other exception = retry with back-off
    return {"sent": True}                      # stored as the job's result

worker = Worker(emails, registry, concurrency=8, batch_size=8, lease_ms=30_000).start()
...
worker.stop()   # graceful: stops fetching and finishes running jobs
```

## Job lifecycle

```
enqueue ──► queued ──dequeue──► running ──complete──► completed
   │          ▲                   │  │
   │ delay    │ due               │  └─ lease expired (worker crashed) ─┐
   ▼          │                   │                                      │
scheduled ────┘ ◄── back-off ◄────┴─ fail, attempts ≤ max_retries ◄──────┘
                                  │
                                  └─ fail, retries exhausted / PermanentError ──► dead ──retry──► queued
```

Every transition (dequeue, complete, fail/retry/dead-letter, lease extension, DLQ re-queue) is a single Lua script, so a job is never in two sets at once and never "popped but not leased". The scripts are in [`taskqueue/scripts.py`](taskqueue/scripts.py).

## REST API

| Method & path | Description |
| --- | --- |
| `GET /api/overview` | Everything the dashboard needs in one poll: every queue's counts, stats, latency and throughput series, plus the worker list and total utilisation |
| `GET /api/queues` | Per-queue summary |
| `GET /api/queues/{name}` | Summary and throughput series (`?window_s=300`) |
| `GET /api/queues/{name}/jobs?state=ready\|delayed\|inflight\|dead\|completed&limit=50` | Jobs in a state |
| `POST /api/queues/{name}/jobs` | `{ task, payload, priority, delay_ms, max_retries }` → `201` job |
| `GET /api/jobs/{id}` | A single job: attempts, last error, result, latency |
| `POST /api/jobs/{id}/retry` | Re-queue a dead-lettered job (`409` if it isn't dead) |
| `GET /api/workers` | Live workers: concurrency, busy, utilisation, processed, failed, last heartbeat |
| `GET /api/health` | Redis ping |

## Dashboard

The dashboard is in `dashboard/` (React + Recharts) and polls `/api/overview` every 2 seconds, pausing while the tab is hidden. It shows:
- summary tiles: queued, running, completed, retried, dead-lettered, and cluster utilisation
- one card per queue with ready/delayed/running/dead counts and p50/p95 latency
- a stacked chart of throughput per queue
- a queue-depth history chart
- a worker table with utilisation bars
- a job browser with a tab per state, last error, and a **Retry** button for dead-lettered jobs

## Benchmark

[`bench/latency.py`](bench/latency.py) enqueues a burst of synthetic I/O-bound jobs, drains them with one worker pool, and reads each job's `finished_at - run_at` from Redis. It compares one job per dequeue round trip against batched dequeue:

```bash
python bench/latency.py --jobs 5000 --concurrency 16 --work-ms 2 --batch-sizes 1,16 --repeat 3
```

It prints a markdown table with p50/p95 latency, throughput, the number of dequeue round trips, and the p50 change against the baseline. CI runs it against a real Redis and posts the table to the job summary.

## Tests

```bash
pytest                                   # uses fakeredis (with Lua) when no Redis is configured
REDIS_URL=redis://localhost:6379/15 pytest   # against real Redis; also runs the process-kill test
```

The suite has over 60 unit and integration tests:

| File | Covers |
| --- | --- |
| `test_queue.py` | priority order, FIFO, delayed promotion, batch dequeue, completion and result storage, back-off timing, dead-lettering, DLQ re-queue, stats, throughput buckets, latency percentiles, validation |
| `test_retry.py` | exponential growth, cap, jitter bounds, invalid policies |
| `test_reliability.py` | lease expiry and redelivery, heartbeats, fencing of stale acks, reaper/heartbeat race, poison jobs ending in the DLQ, recovery of 50 crashed leases, and a **real worker process hard-killed (`os._exit`) mid-batch** with no jobs lost |
| `test_worker.py` | end-to-end processing, flaky-job retries, permanent errors, the concurrency cap, batched vs single dequeue round trips, priority overtaking a backlog, heartbeat and utilisation records, graceful shutdown, independent pools |
| `test_api.py` | every endpoint, validation errors, DLQ retry over HTTP, the overview payload |

[GitHub Actions](.github/workflows/ci.yml) has three jobs:
- runs ruff and pytest against both a real Redis service container and fakeredis, then runs the benchmark
- builds the dashboard
- starts the Compose stack and checks that jobs actually complete

## Project layout

```
taskqueue/
  job.py        Job model, statuses, priority scoring
  queue.py      TaskQueue: enqueue, batched dequeue, ack/fail, leases, reaper, metrics
  scripts.py    Lua scripts for atomic state transitions
  retry.py      exponential back-off policy with jitter
  worker.py     Worker pool, TaskRegistry, heartbeats, worker registry
  api.py        FastAPI app (also serves the built dashboard)
  __main__.py   CLI: worker / api / enqueue
dashboard/      React monitoring dashboard (Vite)
examples/       demo tasks and a synthetic load producer
bench/          latency benchmark
tests/          pytest suite
```

## License

MIT
