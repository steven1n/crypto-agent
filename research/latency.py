"""Bounded offline attribution evidence, never latency clamping or causal claims."""

from dataclasses import asdict
from typing import Any

from app.diagnostics import BoundedDistribution
from market.clock import ClockOffsetSample
from market.events import BookUpdateEvent, TradeEvent
from recorder.raw_models import RuntimeSample
from replay.models import ReplayItem
from replay.offset import RecordedClockOffset


class LatencyDiagnostics:
    def __init__(self, clock_sample_count: int = 5) -> None:
        self.offsets = RecordedClockOffset(clock_sample_count)
        self.distributions = {
            name: BoundedDistribution()
            for name in (
                "raw_event_lag_ms",
                "corrected_event_lag_ms",
                "depth_corrected_lag_ms",
                "trade_corrected_lag_ms",
                "receive_interval_ms",
                "event_loop_lag_ms",
            )
        }
        self.tails: list[dict[str, Any]] = []
        self.previous_ns: int | None = None
        self.first_offset: float | None = None
        self.last_offset: float | None = None

    def accept(self, item: ReplayItem) -> None:
        payload = item.payload
        if isinstance(payload, ClockOffsetSample):
            self.offsets.add(payload)
            if payload.accepted:
                if self.first_offset is None:
                    self.first_offset = payload.offset_ms
                self.last_offset = payload.offset_ms
        elif isinstance(payload, RuntimeSample):
            self.distributions["event_loop_lag_ms"].add(payload.event_loop_lag_ms)
            for tail in self.tails:
                age_ms = (item.monotonic_ns - tail["monotonic_ns"]) / 1e6
                if 0 <= age_ms <= 1_000 + payload.event_loop_lag_ms:
                    tail["nearby_heartbeat_lag_ms"] = payload.event_loop_lag_ms
        elif isinstance(payload, (TradeEvent, BookUpdateEvent)):
            raw = (payload.local_receive_time - payload.exchange_event_time).total_seconds() * 1000
            corrected = self.offsets.corrected_event_lag_ms(
                local_receive_time=payload.local_receive_time,
                exchange_event_time=payload.exchange_event_time,
            )
            interval = (
                None if self.previous_ns is None else (item.monotonic_ns - self.previous_ns) / 1e6
            )
            self.previous_ns = item.monotonic_ns
            self.distributions["raw_event_lag_ms"].add(raw)
            if interval is not None:
                self.distributions["receive_interval_ms"].add(interval)
            if corrected is not None:
                self.distributions["corrected_event_lag_ms"].add(corrected)
                kind = "trade" if isinstance(payload, TradeEvent) else "depth"
                self.distributions[f"{kind}_corrected_lag_ms"].add(corrected)
                if len(self.tails) < 50 or corrected > self.tails[-1]["corrected_lag_ms"]:
                    self.tails.append(
                        {
                            "capture_seq": item.capture_seq,
                            "monotonic_ns": item.monotonic_ns,
                            "local_time": item.local_time.isoformat(),
                            "kind": kind,
                            "raw_lag_ms": raw,
                            "corrected_lag_ms": corrected,
                            "preceding_receive_interval_ms": interval,
                            "nearby_heartbeat_lag_ms": None,
                        }
                    )
                    self.tails.sort(key=lambda row: row["corrected_lag_ms"], reverse=True)
                    del self.tails[50:]

    def report(self) -> dict[str, Any]:
        return {
            "clock": asdict(self.offsets.status()),
            "first_to_last_sample_offset_drift_ms": None
            if self.first_offset is None or self.last_offset is None
            else self.last_offset - self.first_offset,
            "distributions": {
                name: values.summary() for name, values in self.distributions.items()
            },
            "largest_corrected_lag_observations": self.tails,
            "attribution": "Heartbeat proximity and receive bursts are evidence, not proof. "
            "RTT asymmetry, exchange E/T semantics and external buffering remain confounded.",
        }
