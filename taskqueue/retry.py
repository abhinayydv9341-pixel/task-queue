from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass
class RetryPolicy:
    """Exponential back-off: delay(n) = min(max_delay, base * factor^(n-1)), +/- jitter.

    `attempt` is the attempt that just failed (1 for the first try), so with the defaults a
    job is retried after ~1s, 2s, 4s, 8s, ... capped at 5 minutes. Jitter spreads retries
    of jobs that failed together (e.g. a downstream outage) so they don't stampede back at
    the same instant.
    """

    base_delay_ms: int = 1_000
    factor: float = 2.0
    max_delay_ms: int = 300_000
    jitter: float = 0.1  # fraction of the delay, applied symmetrically
    rng: random.Random = field(default_factory=random.Random, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.base_delay_ms < 0 or self.max_delay_ms < 0:
            raise ValueError("delays must be non-negative")
        if self.factor < 1:
            raise ValueError("factor must be >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")

    def delay_ms(self, attempt: int) -> int:
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        # Cap the exponent before exponentiating so huge attempt counts don't overflow.
        raw = self.base_delay_ms * self.factor ** min(attempt - 1, 64)
        delay = min(float(self.max_delay_ms), raw)
        if self.jitter:
            delay *= 1 + self.rng.uniform(-self.jitter, self.jitter)
        return max(0, int(delay))
