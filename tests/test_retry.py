from __future__ import annotations

import random

import pytest

from taskqueue import RetryPolicy


def test_delays_double_each_attempt():
    policy = RetryPolicy(base_delay_ms=500, factor=2, max_delay_ms=1_000_000, jitter=0)
    assert [policy.delay_ms(n) for n in range(1, 6)] == [500, 1_000, 2_000, 4_000, 8_000]


def test_delay_is_capped():
    policy = RetryPolicy(base_delay_ms=1_000, factor=3, max_delay_ms=10_000, jitter=0)
    assert policy.delay_ms(10) == 10_000
    assert policy.delay_ms(10_000) == 10_000  # no overflow on absurd attempt counts


def test_jitter_stays_within_bounds():
    policy = RetryPolicy(base_delay_ms=1_000, factor=2, jitter=0.25, rng=random.Random(42))
    samples = [policy.delay_ms(3) for _ in range(500)]
    assert all(3_000 <= s <= 5_000 for s in samples)
    assert len(set(samples)) > 50, "jitter should actually spread retries out"


@pytest.mark.parametrize(
    "kwargs",
    [{"base_delay_ms": -1}, {"factor": 0.5}, {"jitter": 1.5}, {"max_delay_ms": -5}],
)
def test_invalid_policy(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_attempt_must_be_positive():
    with pytest.raises(ValueError):
        RetryPolicy().delay_ms(0)
