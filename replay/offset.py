"""Recorded clock samples replayed without network access."""

from collections import deque
from datetime import datetime
from statistics import median

from market.clock import ClockOffsetSample, ClockSyncStatus, datetime_to_unix_ms


class RecordedClockOffset:
    def __init__(self, sample_count: int = 5) -> None:
        self.samples: deque[ClockOffsetSample] = deque(maxlen=sample_count)
        self.successes = 0
        self.failures = 0

    def add(self, sample: ClockOffsetSample) -> None:
        if sample.accepted:
            self.samples.append(sample)
            self.successes += 1
        else:
            self.failures += 1

    def status(self) -> ClockSyncStatus:
        samples = self.samples
        return ClockSyncStatus(
            offset_ms=median(s.offset_ms for s in samples) if samples else None,
            rtt_ms=median(s.rtt_ms for s in samples) if samples else None,
            sample_count=len(samples),
            last_updated=samples[-1].sampled_at if samples else None,
            healthy=bool(samples),
            successes=self.successes,
            failures=self.failures,
            offset_mad_ms=median(
                abs(s.offset_ms - median(x.offset_ms for x in samples)) for s in samples
            )
            if samples
            else None,
            min_rtt_ms=min(s.rtt_ms for s in samples) if samples else None,
            offset_drift_ms=samples[-1].offset_ms - samples[0].offset_ms if samples else None,
        )

    def corrected_event_lag_ms(
        self, *, local_receive_time: datetime, exchange_event_time: datetime
    ) -> float | None:
        offset = self.status().offset_ms
        return (
            None
            if offset is None
            else (
                datetime_to_unix_ms(local_receive_time)
                - datetime_to_unix_ms(exchange_event_time)
                + offset
            )
        )
