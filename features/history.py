"""Bounded monotonic mid-price history, returns, and realized volatility."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class MidObservation:
    monotonic_ns: int
    price: float


class MidPriceHistory:
    """Observed-update history; no interpolation or future-boundary lookup."""

    def __init__(self, retention_seconds: float = 15.0, *, max_observations: int = 200_000) -> None:
        if retention_seconds < 10:
            raise ValueError("retention_seconds must cover the 10-second horizon")
        self._retention_ns = int(retention_seconds * 1_000_000_000)
        self._max_observations = max_observations
        self._observations: deque[MidObservation] = deque()

    def __len__(self) -> int:
        return len(self._observations)

    def clear(self) -> None:
        self._observations.clear()

    def add(self, monotonic_timestamp_ns: int, mid_price: Decimal) -> None:
        if mid_price <= 0:
            raise ValueError("mid_price must be positive")
        if self._observations and monotonic_timestamp_ns < self._observations[-1].monotonic_ns:
            raise ValueError("mid-price observations must be monotonic")
        observation = MidObservation(monotonic_timestamp_ns, float(mid_price))
        if self._observations and monotonic_timestamp_ns == self._observations[-1].monotonic_ns:
            self._observations[-1] = observation
        else:
            self._observations.append(observation)
        cutoff = monotonic_timestamp_ns - self._retention_ns
        while self._observations and self._observations[0].monotonic_ns < cutoff:
            self._observations.popleft()
        if len(self._observations) > self._max_observations:
            raise RuntimeError("mid history exceeded bounded capacity")

    def _current(self, now_ns: int) -> MidObservation | None:
        for observation in reversed(self._observations):
            if observation.monotonic_ns <= now_ns:
                return observation
        return None

    def historical_at_or_before(self, boundary_ns: int) -> MidObservation | None:
        for observation in reversed(self._observations):
            if observation.monotonic_ns <= boundary_ns:
                return observation
        return None

    def log_return(self, horizon_ms: int, *, now_ns: int) -> float | None:
        if horizon_ms <= 0:
            raise ValueError("horizon_ms must be positive")
        current = self._current(now_ns)
        historical = self.historical_at_or_before(now_ns - horizon_ms * 1_000_000)
        if current is None or historical is None:
            return None
        return math.log(current.price / historical.price)

    def realized_volatility(self, window_ms: int, *, now_ns: int) -> float | None:
        """Non-annualized sqrt(sum(r_i^2)) over observed consecutive updates."""

        if window_ms <= 0:
            raise ValueError("window_ms must be positive")
        boundary_ns = now_ns - window_ms * 1_000_000
        observations = [
            item for item in self._observations if boundary_ns <= item.monotonic_ns <= now_ns
        ]
        if len(observations) < 2:
            return None
        squared_returns = (
            math.log(current.price / previous.price) ** 2
            for previous, current in zip(observations, observations[1:], strict=False)
        )
        return math.sqrt(sum(squared_returns))
