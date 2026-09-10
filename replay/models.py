"""Typed replay stream records and integrity status."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from features.models import FeatureSnapshot
from market.bootstrap import BootstrapSnapshot
from market.clock import ClockOffsetSample
from market.events import BookUpdateEvent, TradeEvent
from market.orderbook import TrustedBookSnapshot
from recorder.raw_models import LifecycleRecord, RuntimeSample


class ReplayEventType(StrEnum):
    TRADE = "trade"
    DEPTH = "depth"
    TRUSTED_BOOK = "trusted_book"
    STREAM_LIFECYCLE = "stream_lifecycle"
    BOOK_LIFECYCLE = "book_lifecycle"
    CLOCK_SAMPLE = "clock_sample"
    FEATURE_TICK = "feature_tick"
    BOOTSTRAP = "orderbook_bootstrap_snapshot"
    RUNTIME_SAMPLE = "runtime_sample"


ReplayPayload = (
    TradeEvent
    | BookUpdateEvent
    | TrustedBookSnapshot
    | LifecycleRecord
    | ClockOffsetSample
    | BootstrapSnapshot
    | RuntimeSample
    | FeatureSnapshot
    | None
)


@dataclass(frozen=True, slots=True)
class ReplayItem:
    capture_seq: int
    capture_session_id: str
    event_type: ReplayEventType
    symbol: str
    local_time: datetime
    monotonic_ns: int
    payload: ReplayPayload


@dataclass(frozen=True, slots=True)
class ReplayDataQuality:
    valid: bool
    event_count: int
    first_capture_seq: int | None
    last_capture_seq: int | None
    sequence_gap_count: int
    errors: tuple[str, ...]


class ReplayIntegrityError(RuntimeError):
    """Recorded data cannot be replayed without inventing or skipping state."""
