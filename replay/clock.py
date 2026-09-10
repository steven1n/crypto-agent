"""Explicit clock advanced only by recorded replay events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class WallClockStep:
    previous_wall_time: datetime
    current_wall_time: datetime
    capture_seq: int | None
    monotonic_ns: int
    backward_ms: float
    monotonic_elapsed_ms: float
    wall_minus_monotonic_step_ms: float


class ReplayClock:
    def __init__(self) -> None:
        self._wall_time: datetime | None = None
        self._monotonic_ns: int | None = None
        self.backward_step_count = 0
        self.largest_backward_step: WallClockStep | None = None
        self.last_backward_step: WallClockStep | None = None

    def advance(
        self, *, wall_time: datetime, monotonic_ns: int, capture_seq: int | None = None
    ) -> None:
        if wall_time.tzinfo is None:
            raise ValueError("replay wall time must be timezone-aware")
        if self._monotonic_ns is not None and monotonic_ns < self._monotonic_ns:
            raise ValueError(
                f"negative replay monotonic delta: {monotonic_ns - self._monotonic_ns}"
            )
        if self._wall_time is not None and wall_time < self._wall_time:
            assert self._monotonic_ns is not None
            backward_ms = (self._wall_time - wall_time).total_seconds() * 1_000
            elapsed_ms = (monotonic_ns - self._monotonic_ns) / 1_000_000
            step = WallClockStep(
                self._wall_time,
                wall_time,
                capture_seq,
                monotonic_ns,
                backward_ms,
                elapsed_ms,
                -backward_ms - elapsed_ms,
            )
            self.backward_step_count += 1
            self.last_backward_step = step
            if (
                self.largest_backward_step is None
                or backward_ms > self.largest_backward_step.backward_ms
            ):
                self.largest_backward_step = step
        # Civil time is an observation, not a logical ordering constraint.
        # Preserve it exactly, including backward adjustments. Diagnostic memory is O(1).
        self._wall_time = wall_time
        self._monotonic_ns = monotonic_ns

    def wall_time(self) -> datetime:
        if self._wall_time is None:
            raise RuntimeError("replay clock has not been advanced")
        return self._wall_time

    def monotonic_ns(self) -> int:
        if self._monotonic_ns is None:
            raise RuntimeError("replay clock has not been advanced")
        return self._monotonic_ns
