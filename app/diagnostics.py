"""Bounded runtime counters, latency distributions, and scheduling lag."""

from __future__ import annotations

import asyncio
import math
from collections import Counter, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from statistics import median
from typing import Any


class BoundedDistribution:
    def __init__(self, capacity: int = 6_000) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.values: deque[float] = deque(maxlen=capacity)
        self.count = 0
        self.total = 0.0
        self.maximum: float | None = None

    def add(self, value: float) -> None:
        if not math.isfinite(value):
            return
        self.values.append(value)
        self.count += 1
        self.total += value
        self.maximum = value if self.maximum is None else max(value, self.maximum)

    def summary(self) -> dict[str, Any]:
        values = sorted(self.values)
        return {
            "count": self.count,
            "retained": len(values),
            "mean": self.total / self.count if self.count else None,
            "maximum": self.maximum,
            "p50": median(values) if values else None,
            "p95": values[int((len(values) - 1) * 0.95)] if values else None,
            "p99": values[int((len(values) - 1) * 0.99)] if values else None,
            "quantile_scope": "bounded most recent observations",
        }


@dataclass
class SymbolStatistics:
    counts: Counter[str] = field(default_factory=Counter)
    distributions: dict[str, BoundedDistribution] = field(
        default_factory=lambda: {
            name: BoundedDistribution()
            for name in (
                "book_age_ms",
                "raw_event_lag_ms",
                "corrected_event_lag_ms",
                "spread_bps",
                "event_receive_interval_ms",
                "event_loop_lag_ms",
            )
        }
    )
    last_event_ns: int | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "counts": dict(self.counts),
            "distributions": {name: data.summary() for name, data in self.distributions.items()},
        }


def scheduling_lag_ms(expected_ns: int, actual_ns: int) -> float:
    return max(0.0, (actual_ns - expected_ns) / 1_000_000)


async def detect_event_loop_stalls(
    stop: asyncio.Event,
    distribution: BoundedDistribution,
    *,
    interval_seconds: float = 1.0,
    sample_handler: Callable[[float], Awaitable[None]] | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        expected = loop.time() + interval_seconds
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            lag = scheduling_lag_ms(int(expected * 1e9), int(loop.time() * 1e9))
            distribution.add(lag)
            if sample_handler is not None:
                await sample_handler(lag)
