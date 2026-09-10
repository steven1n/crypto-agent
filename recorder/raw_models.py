"""Capture envelope and lifecycle records for durable raw datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from market.bootstrap import BootstrapSnapshot
from market.clock import ClockOffsetSample
from market.events import BookUpdateEvent, TradeEvent
from market.orderbook import TrustedBookSnapshot


class RawEventType(StrEnum):
    TRADE = "trade"
    DEPTH = "depth"
    TRUSTED_BOOK = "trusted_book"
    STREAM_LIFECYCLE = "stream_lifecycle"
    BOOK_LIFECYCLE = "book_lifecycle"
    CLOCK_SAMPLE = "clock_sample"
    BOOTSTRAP = "orderbook_bootstrap_snapshot"
    RUNTIME_SAMPLE = "runtime_sample"


@dataclass(frozen=True, slots=True)
class CaptureMetadata:
    capture_seq: int
    capture_session_id: str
    exchange: str
    market_type: str


@dataclass(frozen=True, slots=True)
class LifecycleRecord:
    symbol: str
    component: str
    kind: str
    local_time: datetime
    monotonic_ns: int
    reason: str | None = None
    resync_count: int = 0
    sequence_gap_count: int = 0
    healthy: bool = True
    connection_generation: int = 0
    sync_generation: int = 0


@dataclass(frozen=True, slots=True)
class RuntimeSample:
    local_time: datetime
    monotonic_ns: int
    event_loop_lag_ms: float
    raw_queue_depth: int
    feature_queue_depth: int


RawPayload = (
    TradeEvent
    | BookUpdateEvent
    | TrustedBookSnapshot
    | LifecycleRecord
    | ClockOffsetSample
    | BootstrapSnapshot
    | RuntimeSample
)


@dataclass(frozen=True, slots=True)
class CapturedRawRecord:
    metadata: CaptureMetadata
    event_type: RawEventType
    symbol: str
    exchange_time: datetime | None
    local_time: datetime
    monotonic_ns: int
    payload: RawPayload


class CaptureSequencer:
    """Single-process monotonically increasing capture order."""

    def __init__(self, start: int = 0) -> None:
        if start < 0:
            raise ValueError("capture sequence start cannot be negative")
        self._next_value = start

    def next(self) -> int:
        value = self._next_value
        self._next_value += 1
        return value
