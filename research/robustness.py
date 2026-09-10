"""Dependence-aware descriptive statistics with deterministic time-block bootstrap."""

from __future__ import annotations

import bisect
import math
import random
from collections import defaultdict
from dataclasses import dataclass
from statistics import fmean, median
from typing import Any

from research.diagnostics import _pearson

COST_GRID_BPS = (0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 12.0)
AUTOCORRELATION_LAGS_MS = (100, 250, 500, 1_000, 2_000, 5_000)


@dataclass
class CorrelationMoments:
    """Mergeable centered moments: session correlations without retaining session rows."""

    n: int = 0
    mean_x: float = 0.0
    mean_y: float = 0.0
    m2_x: float = 0.0
    m2_y: float = 0.0
    cross: float = 0.0

    def add(self, x: float, y: float) -> None:
        self.n += 1
        dx, dy = x - self.mean_x, y - self.mean_y
        self.mean_x += dx / self.n
        self.mean_y += dy / self.n
        self.m2_x += dx * (x - self.mean_x)
        self.m2_y += dy * (y - self.mean_y)
        self.cross += dx * (y - self.mean_y)

    def merge(self, other: CorrelationMoments) -> None:
        if not other.n:
            return
        n = self.n + other.n
        weight = self.n * other.n / n
        dx, dy = other.mean_x - self.mean_x, other.mean_y - self.mean_y
        self.m2_x += other.m2_x + dx * dx * weight
        self.m2_y += other.m2_y + dy * dy * weight
        self.cross += other.cross + dx * dy * weight
        self.mean_x += dx * other.n / n
        self.mean_y += dy * other.n / n
        self.n = n

    def pearson(self) -> float | None:
        if self.n < 3 or self.m2_x <= 0 or self.m2_y <= 0:
            return None
        return max(-1.0, min(1.0, self.cross / math.sqrt(self.m2_x * self.m2_y)))


def non_overlapping_indices(timestamps: tuple[int, ...], horizon_ms: int) -> tuple[int, ...]:
    if horizon_ms <= 0:
        raise ValueError("horizon must be positive")
    output: list[int] = []
    next_ns: int | None = None
    for index, timestamp in enumerate(timestamps):
        if index and timestamp <= timestamps[index - 1]:
            raise ValueError("timestamps must be strictly increasing")
        if next_ns is None or timestamp >= next_ns:
            output.append(index)
            next_ns = timestamp + horizon_ms * 1_000_000
    return tuple(output)


def autocorrelation(
    timestamps: tuple[int, ...], values: tuple[float | None, ...], lag_ms: int
) -> float | None:
    if len(timestamps) != len(values) or lag_ms <= 0:
        raise ValueError("invalid autocorrelation inputs")
    left: list[float] = []
    right: list[float] = []
    for index, timestamp in enumerate(timestamps):
        previous = bisect.bisect_right(timestamps, timestamp - lag_ms * 1_000_000, hi=index) - 1
        if previous < 0:
            continue
        # Do not bridge gaps with a stale proxy for the requested lag.
        if timestamp - timestamps[previous] > (lag_ms + 150) * 1_000_000:
            continue
        a, b = values[previous], values[index]
        if a is not None and b is not None and math.isfinite(a) and math.isfinite(b):
            left.append(a)
            right.append(b)
    return _pearson(left, right)


def effective_sample_size(values: tuple[float, ...], max_lag: int = 50) -> float:
    """Initial-positive-sequence approximation on equally sampled observations."""
    n = len(values)
    if n < 3:
        return float(n)
    if max(values) == min(values):
        return 1.0
    total = 0.0
    for lag in range(1, min(max_lag + 1, n // 3)):
        rho = _pearson(list(values[:-lag]), list(values[lag:]))
        if rho is None or rho <= 0:
            break
        total += rho
    return max(1.0, min(float(n), n / (1 + 2 * total)))


@dataclass(frozen=True)
class BootstrapInterval:
    estimate_bps: float | None
    low_bps: float | None
    high_bps: float | None
    usable_replications: int
    block_count: int
    block_length_ms: int
    replications: int
    seed: int
    method: str = "non-overlapping time-block resampling; descriptive approximate interval"


def block_bootstrap(
    timestamps: tuple[int, ...],
    signal: tuple[float, ...],
    future_return: tuple[float, ...],
    *,
    threshold: float = 0.6,
    block_length_ms: int = 10_000,
    replications: int = 200,
    seed: int = 42,
    difference: bool = False,
) -> BootstrapInterval:
    if len(timestamps) != len(signal) or len(signal) != len(future_return):
        raise ValueError("bootstrap arrays differ in length")
    if block_length_ms <= 0 or replications < 1 or threshold <= 0:
        raise ValueError("invalid bootstrap configuration")
    # Preaggregate each time block; bootstrap cost is blocks × replications,
    # not raw rows × replications. No IID row resampling.
    blocks: dict[int, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
    origin = timestamps[0] if timestamps else 0
    for timestamp, x, y in zip(timestamps, signal, future_return, strict=True):
        block = blocks[(timestamp - origin) // (block_length_ms * 1_000_000)]
        if x >= threshold:
            block[0] += y * 10_000
            block[1] += 1
        elif x <= -threshold:
            block[2] += y * 10_000
            block[3] += 1
    values = list(blocks.values())

    def estimate(selected: list[list[float]]) -> float | None:
        sums = [sum(row[column] for row in selected) for column in range(4)]
        if not sums[1] or (difference and not sums[3]):
            return None
        return sums[0] / sums[1] - (sums[2] / sums[3] if difference else 0)

    point = estimate(values)
    draws: list[float] = []
    rng = random.Random(seed)
    if len(values) >= 2:
        for _ in range(replications):
            result = estimate([values[rng.randrange(len(values))] for _ in values])
            if result is not None:
                draws.append(result)
    draws.sort()
    return BootstrapInterval(
        point,
        draws[int((len(draws) - 1) * 0.025)] if draws else None,
        draws[int((len(draws) - 1) * 0.975)] if draws else None,
        len(draws),
        len(values),
        block_length_ms,
        replications,
        seed,
    )


def cost_sensitivity(
    edge_bps: float | None, costs: tuple[float, ...] = COST_GRID_BPS
) -> list[dict[str, float | None]]:
    if any(cost < 0 or not math.isfinite(cost) for cost in costs):
        raise ValueError("cost grid must be nonnegative and finite")
    return [
        {
            "round_trip_cost_bps": cost,
            "gross_edge_bps": edge_bps,
            "net_expected_bps": None if edge_bps is None else edge_bps - cost,
            "edge_cost_ratio": None if edge_bps is None or cost == 0 else edge_bps / cost,
            "break_even_cost_bps": edge_bps,
        }
        for cost in costs
    ]


def session_stability(statistics: tuple[float | None, ...]) -> dict[str, Any]:
    observed = [x for x in statistics if x is not None and math.isfinite(x)]
    center = median(observed) if observed else None
    return {
        "sessions": len(statistics),
        "usable_sessions": len(observed),
        "per_session": statistics,
        "median": center,
        "min": min(observed) if observed else None,
        "max": max(observed) if observed else None,
        "fraction_same_sign_as_median": None
        if center is None or center == 0
        else fmean(float(value * center > 0) for value in observed),
    }


def volatility_boundaries(training_values: tuple[float, ...]) -> tuple[float, float]:
    if not training_values:
        raise ValueError("volatility boundaries require training observations")
    values = sorted(training_values)
    return values[(len(values) - 1) // 3], values[2 * (len(values) - 1) // 3]


def volatility_regime(value: float | None, boundaries: tuple[float, float]) -> str:
    if value is None:
        return "UNAVAILABLE"
    return "LOW" if value <= boundaries[0] else "MEDIUM" if value <= boundaries[1] else "HIGH"
