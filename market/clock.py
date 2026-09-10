"""Robust exchange clock-offset estimation using public server time."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from statistics import median
from time import monotonic_ns
from typing import Protocol


class ServerTimeProvider(Protocol):
    async def server_time_ms(self) -> int: ...


@dataclass(frozen=True, slots=True)
class ClockOffsetSample:
    offset_ms: float
    rtt_ms: float
    sampled_at: datetime
    local_send: datetime | None = None
    local_receive: datetime | None = None
    server_time_ms: int | None = None
    send_monotonic_ns: int | None = None
    receive_monotonic_ns: int | None = None
    accepted: bool = True


ClockSampleHandler = Callable[[ClockOffsetSample], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ClockSyncStatus:
    offset_ms: float | None
    rtt_ms: float | None
    sample_count: int
    last_updated: datetime | None
    healthy: bool
    successes: int
    failures: int
    offset_mad_ms: float | None = None
    min_rtt_ms: float | None = None
    offset_drift_ms: float | None = None


def datetime_to_unix_ms(value: datetime) -> float:
    if value.tzinfo is None:
        raise ValueError("wall-clock datetime must be timezone-aware")
    return value.timestamp() * 1_000


class ExchangeClockOffsetEstimator:
    """NTP-style midpoint estimator with a rolling median.

    Sign convention: ``offset = exchange_clock - local_clock``. Therefore an
    exchange timestamp is converted into the local clock domain by subtracting
    the offset, and corrected event lag is ``local_receive - exchange + offset``.
    """

    def __init__(
        self,
        provider: ServerTimeProvider,
        *,
        sample_count: int = 5,
        max_rtt_ms: float = 1_000.0,
        refresh_interval_seconds: float = 300.0,
        wall_clock: Callable[[], datetime] | None = None,
        monotonic_clock_ns: Callable[[], int] = monotonic_ns,
        sample_handler: ClockSampleHandler | None = None,
    ) -> None:
        if sample_count <= 0:
            raise ValueError("sample_count must be positive")
        if max_rtt_ms <= 0 or refresh_interval_seconds <= 0:
            raise ValueError("clock timing limits must be positive")
        self._provider = provider
        self._target_sample_count = sample_count
        self._max_rtt_ms = max_rtt_ms
        self._refresh_interval_seconds = refresh_interval_seconds
        self._wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self._monotonic_clock_ns = monotonic_clock_ns
        self._sample_handler = sample_handler
        self._samples: deque[ClockOffsetSample] = deque(maxlen=sample_count)
        self._successes = 0
        self._failures = 0
        self._stop_event = asyncio.Event()
        self._sample_lock = asyncio.Lock()

    async def sample_once(self) -> ClockOffsetSample | None:
        async with self._sample_lock:
            local_send = self._wall_clock()
            send_monotonic_ns = self._monotonic_clock_ns()
            try:
                server_time_ms = await self._provider.server_time_ms()
            except Exception:
                self._failures += 1
                return None
            local_receive = self._wall_clock()
            receive_monotonic_ns = self._monotonic_clock_ns()
            rtt_ms = max(0.0, (receive_monotonic_ns - send_monotonic_ns) / 1_000_000)
            midpoint_ms = (datetime_to_unix_ms(local_send) + datetime_to_unix_ms(local_receive)) / 2
            sample = ClockOffsetSample(
                offset_ms=server_time_ms - midpoint_ms,
                rtt_ms=rtt_ms,
                sampled_at=local_receive,
                local_send=local_send,
                local_receive=local_receive,
                server_time_ms=server_time_ms,
                send_monotonic_ns=send_monotonic_ns,
                receive_monotonic_ns=receive_monotonic_ns,
                accepted=rtt_ms <= self._max_rtt_ms,
            )
            if sample.accepted:
                self._samples.append(sample)
                self._successes += 1
            else:
                self._failures += 1
            if self._sample_handler is not None:
                await self._sample_handler(sample)
            return sample if sample.accepted else None

    async def synchronize(self, samples: int | None = None) -> ClockSyncStatus:
        count = samples if samples is not None else self._target_sample_count
        if count <= 0:
            raise ValueError("samples must be positive")
        for _ in range(count):
            await self.sample_once()
        return self.status()

    def status(self) -> ClockSyncStatus:
        if not self._samples:
            return ClockSyncStatus(
                offset_ms=None,
                rtt_ms=None,
                sample_count=0,
                last_updated=None,
                healthy=False,
                successes=self._successes,
                failures=self._failures,
            )
        return ClockSyncStatus(
            offset_ms=median(sample.offset_ms for sample in self._samples),
            rtt_ms=median(sample.rtt_ms for sample in self._samples),
            sample_count=len(self._samples),
            last_updated=self._samples[-1].sampled_at,
            healthy=True,
            successes=self._successes,
            failures=self._failures,
            offset_mad_ms=median(
                abs(sample.offset_ms - median(s.offset_ms for s in self._samples))
                for sample in self._samples
            ),
            min_rtt_ms=min(sample.rtt_ms for sample in self._samples),
            offset_drift_ms=self._samples[-1].offset_ms - self._samples[0].offset_ms,
        )

    def corrected_event_lag_ms(
        self,
        *,
        local_receive_time: datetime,
        exchange_event_time: datetime,
    ) -> float | None:
        offset = self.status().offset_ms
        if offset is None:
            return None
        raw_lag = datetime_to_unix_ms(local_receive_time) - datetime_to_unix_ms(exchange_event_time)
        return raw_lag + offset

    async def run(self) -> None:
        while not self._stop_event.is_set():
            await self.synchronize()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._refresh_interval_seconds
                )
            except TimeoutError:
                pass

    async def stop(self) -> None:
        self._stop_event.set()
