"""Decide which failures deserve another attempt, and how long to wait.

Only errors a retry can plausibly fix are retried. A constraint violation or a
bad column stays failed and is reported immediately.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

# 40001 serialization failure, 40P01 deadlock, 55P03 lock not available,
# 57P01 admin shutdown, 08006 connection failure.
TRANSIENT_SQLSTATES = frozenset({"40001", "40P01", "55P03", "57P01", "08006"})


def sqlstate(exc: BaseException) -> str | None:
    """Return the Postgres error code if the driver attached one."""
    value = getattr(exc, "sqlstate", None)
    return value if isinstance(value, str) else None


def is_retryable(exc: BaseException, transient: frozenset[str] = TRANSIENT_SQLSTATES) -> bool:
    return sqlstate(exc) in transient


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff bounded by ``max_attempts`` (first try included)."""

    max_attempts: int = 1
    delay: float = 2.0
    factor: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.delay < 0:
            raise ValueError("delay must not be negative")
        if self.factor < 1:
            raise ValueError("factor must be at least 1")

    def wait_before(self, attempt: int) -> float:
        """Seconds to wait after failed ``attempt``, before the next one."""
        return self.delay * (self.factor ** (attempt - 1))

    def pause(self, attempt: int, sleep=time.sleep) -> float:
        seconds = self.wait_before(attempt)
        if seconds:
            sleep(seconds)
        return seconds
