"""Speed-independent deterministic replay into the live FeatureEngine."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum

from features.engine import FeatureEngine
from features.models import FeatureSnapshot
from market.clock import ClockOffsetSample
from market.events import BookUpdateEvent, TradeEvent
from market.orderbook import TrustedBookSnapshot
from recorder.raw_models import LifecycleRecord
from replay.clock import ReplayClock
from replay.models import (
    ReplayDataQuality,
    ReplayEventType,
    ReplayIntegrityError,
    ReplayItem,
)
from replay.offset import RecordedClockOffset


class ReplayMode(StrEnum):
    FAST = "FAST"
    STEP = "STEP"


ReplayConsumer = Callable[[ReplayItem], None]


@dataclass(frozen=True, slots=True)
class ReplayResult:
    features: tuple[FeatureSnapshot, ...]
    recorded_features: tuple[FeatureSnapshot, ...]
    data_quality: ReplayDataQuality


class ReplayEngine:
    def __init__(
        self,
        events: Iterable[ReplayItem],
        clock: ReplayClock,
        feature_engine: FeatureEngine,
        *,
        consumers: tuple[ReplayConsumer, ...] = (),
        mode: ReplayMode = ReplayMode.FAST,
        retain_results: bool = True,
        feature_handler: Callable[[FeatureSnapshot, FeatureSnapshot | None], None] | None = None,
        clock_offset: RecordedClockOffset | None = None,
    ) -> None:
        self._events = events
        self._iterator = iter(events)
        self._retain_results = retain_results
        self._feature_handler = feature_handler
        self._clock_offset = clock_offset
        self._first_seq: int | None = None
        self._last_seq: int | None = None
        self._previous_event: ReplayItem | None = None
        self._clock = clock
        self._feature_engine = feature_engine
        self._consumers = consumers
        self.mode = mode
        self._index = 0
        self._features: list[FeatureSnapshot] = []
        self._recorded_features: list[FeatureSnapshot] = []
        self._last_depth_u: int | None = None
        self._sequence_gap_count = 0
        if isinstance(events, tuple):
            self._validate_ordering()

    def _validate_ordering(self) -> None:
        previous_seq: int | None = None
        previous_ns: int | None = None
        session: str | None = None
        symbol: str | None = None
        for event in self._events:
            if previous_seq is not None and event.capture_seq <= previous_seq:
                raise ReplayIntegrityError(
                    f"duplicate or inverted capture_seq: {event.capture_seq}"
                )
            if previous_ns is not None and event.monotonic_ns < previous_ns:
                raise ReplayIntegrityError("impossible negative monotonic delta")
            if session is not None and event.capture_session_id != session:
                raise ReplayIntegrityError("multiple capture sessions require explicit merge")
            if symbol is not None and event.symbol != symbol:
                raise ReplayIntegrityError("symbol mismatch in replay stream")
            previous_seq = event.capture_seq
            previous_ns = event.monotonic_ns
            session = event.capture_session_id
            symbol = event.symbol

    def step(self) -> bool:
        event = next(self._iterator, None)
        if event is None:
            return False
        previous = self._previous_event
        if previous is not None and (
            event.capture_seq <= previous.capture_seq
            or event.monotonic_ns < previous.monotonic_ns
            or event.symbol != previous.symbol
            or event.capture_session_id != previous.capture_session_id
        ):
            raise ReplayIntegrityError("invalid streaming replay order/session/symbol")
        self._previous_event = event
        if self._first_seq is None:
            self._first_seq = event.capture_seq
        self._last_seq = event.capture_seq
        self._index += 1
        self._clock.advance(
            wall_time=event.local_time,
            monotonic_ns=event.monotonic_ns,
            capture_seq=event.capture_seq,
        )
        for consumer in self._consumers:
            consumer(event)

        payload = event.payload
        if isinstance(payload, ClockOffsetSample) and self._clock_offset is not None:
            self._clock_offset.add(payload)
        if event.event_type is ReplayEventType.TRADE:
            if not isinstance(payload, TradeEvent):
                raise ReplayIntegrityError("trade record has wrong payload")
            self._feature_engine.on_trade(payload)
        elif event.event_type is ReplayEventType.DEPTH:
            if not isinstance(payload, BookUpdateEvent):
                raise ReplayIntegrityError("depth record has wrong payload")
            if (
                self._last_depth_u is not None
                and payload.previous_final_update_id != self._last_depth_u
            ):
                self._sequence_gap_count += 1
                raise ReplayIntegrityError(
                    "depth sequence gap: "
                    f"previous u={self._last_depth_u}, pu={payload.previous_final_update_id}"
                )
            self._last_depth_u = payload.final_update_id
            self._feature_engine.on_market_activity(event.monotonic_ns)
        elif event.event_type is ReplayEventType.TRUSTED_BOOK:
            if not isinstance(payload, TrustedBookSnapshot):
                raise ReplayIntegrityError("trusted-book record has wrong payload")
            self._feature_engine.on_market_activity(
                payload.event_receive_monotonic_ns or event.monotonic_ns
            )
            self._feature_engine.on_book_snapshot(payload)
        elif event.event_type is ReplayEventType.STREAM_LIFECYCLE:
            if not isinstance(payload, LifecycleRecord):
                raise ReplayIntegrityError("stream lifecycle has wrong payload")
            connected = payload.kind.upper() == "CONNECTED"
            self._feature_engine.on_connection(connected, monotonic_timestamp_ns=event.monotonic_ns)
            if not connected:
                self._last_depth_u = None
        elif event.event_type is ReplayEventType.BOOK_LIFECYCLE:
            if not isinstance(payload, LifecycleRecord):
                raise ReplayIntegrityError("book lifecycle has wrong payload")
            if not payload.healthy:
                self._feature_engine.on_book_invalidated()
                self._last_depth_u = None
        elif event.event_type is ReplayEventType.FEATURE_TICK:
            if payload is not None and not isinstance(payload, FeatureSnapshot):
                raise ReplayIntegrityError("feature tick has wrong payload")
            feature = self._feature_engine.build_snapshot(now_ns=event.monotonic_ns)
            if feature is not None and self._feature_handler is not None:
                self._feature_handler(feature, payload)
            if feature is not None and self._retain_results:
                self._features.append(feature)
            if isinstance(payload, FeatureSnapshot) and self._retain_results:
                self._recorded_features.append(payload)
        return True

    def run(self) -> ReplayResult:
        while self.step():
            pass
        first = self._first_seq
        last = self._last_seq
        return ReplayResult(
            features=tuple(self._features),
            recorded_features=tuple(self._recorded_features),
            data_quality=ReplayDataQuality(
                valid=True,
                event_count=self._index,
                first_capture_seq=first,
                last_capture_seq=last,
                sequence_gap_count=self._sequence_gap_count,
                errors=(),
            ),
        )
