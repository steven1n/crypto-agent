"""Shared helpers for turning exchange timestamps into normalized event metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic_ns


@dataclass(frozen=True, slots=True)
class ReceiveContext:
    wall_time: datetime
    monotonic_ns: int

    @classmethod
    def now(cls) -> ReceiveContext:
        return cls(wall_time=datetime.now(UTC), monotonic_ns=monotonic_ns())

    def estimated_latency_ms(self, exchange_timestamp_ms: int) -> float:
        """Wall-clock latency estimate; may be negative when local clock is skewed."""

        local_timestamp_ms = self.wall_time.timestamp() * 1_000
        return local_timestamp_ms - exchange_timestamp_ms
