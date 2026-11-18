"""Fixtures.

Tests run against a real Redis when REDIS_URL is set (CI does this), otherwise against
fakeredis with Lua support, so `pytest` works with no services running.
"""

from __future__ import annotations

import os

import pytest
import redis

from taskqueue import RetryPolicy, TaskQueue

REDIS_URL = os.environ.get("REDIS_URL")
real_redis_only = pytest.mark.skipif(not REDIS_URL, reason="needs a real Redis (set REDIS_URL)")


class FakeClock:
    """Millisecond clock the tests move by hand, so leases and back-off are deterministic."""

    def __init__(self, start: int = 1_700_000_000_000) -> None:
        self.now = start

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


@pytest.fixture
def client():
    if REDIS_URL:
        conn = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    else:
        import fakeredis

        conn = fakeredis.FakeRedis(decode_responses=True)
    conn.flushdb()
    yield conn
    conn.flushdb()
    conn.close()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def no_jitter() -> RetryPolicy:
    return RetryPolicy(base_delay_ms=1_000, factor=2, max_delay_ms=60_000, jitter=0)


@pytest.fixture
def queue(client, clock, no_jitter) -> TaskQueue:
    """Queue on a fake clock: nothing happens with time unless the test advances it."""
    return TaskQueue(client, "default", retry_policy=no_jitter, clock=clock)


@pytest.fixture
def fast_retry() -> RetryPolicy:
    return RetryPolicy(base_delay_ms=10, factor=2, max_delay_ms=50, jitter=0)


@pytest.fixture
def live_queue(client, fast_retry) -> TaskQueue:
    """Queue on the real clock, for tests that run actual worker threads."""
    return TaskQueue(client, "live", retry_policy=fast_retry)
