"""Health-gated, exchange-independent real-time feature engine."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic_ns
from typing import Protocol

from features.book import calculate_book_features
from features.history import MidPriceHistory
from features.models import (
    FeatureHealth,
    FeatureInvalidReason,
    FeatureSnapshot,
    ReturnFeatures,
    VolatilityFeatures,
)
from features.trade_flow import TradeFlowTracker
from market.clock import ClockSyncStatus
from market.events import TradeEvent
from market.orderbook import OrderBookState, TrustedBookSnapshot

FeatureHandler = Callable[[FeatureSnapshot], Awaitable[None]]


class ClockOffsetProvider(Protocol):
    def status(self) -> ClockSyncStatus: ...

    def corrected_event_lag_ms(
        self, *, local_receive_time: datetime, exchange_event_time: datetime
    ) -> float | None: ...


@dataclass(frozen=True, slots=True)
class FeatureEngineDiagnostics:
    feature_snapshots_emitted: int
    feature_snapshots_invalid: int
    book_snapshots_processed: int
    trade_events_processed: int
    clock_sync_successes: int
    clock_sync_failures: int
    clock_offset_ms: float | None
    clock_rtt_ms: float | None
    mid_history_size: int
    trade_history_size: int
    stream_connected: bool
    history_generation: int | None
    clock_sync: ClockSyncStatus | None


class FeatureEngine:
    """Computes point-in-time features from trusted book and normalized trades only."""

    def __init__(
        self,
        symbol: str,
        *,
        interval_ms: int = 100,
        book_stale_after_ms: int = 3_000,
        trade_stale_after_ms: int = 5_000,
        mid_history_seconds: float = 15.0,
        trade_history_seconds: float = 15.0,
        imbalance_levels: tuple[int, ...] = (1, 5, 10),
        clock_offset: ClockOffsetProvider | None = None,
        monotonic_clock_ns: Callable[[], int] = monotonic_ns,
        wall_clock: Callable[[], datetime] | None = None,
    ) -> None:
        if interval_ms <= 0 or book_stale_after_ms <= 0 or trade_stale_after_ms <= 0:
            raise ValueError("feature cadence and staleness limits must be positive")
        if trade_history_seconds < 10:
            raise ValueError("trade history must cover the 10-second window")
        self.symbol = symbol.strip().upper()
        self._interval_ns = interval_ms * 1_000_000
        self._book_stale_after_ms = book_stale_after_ms
        self._trade_stale_after_ms = trade_stale_after_ms
        self._imbalance_levels = imbalance_levels
        self._clock_offset = clock_offset
        self._monotonic_clock_ns = monotonic_clock_ns
        self._wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self._mid_history = MidPriceHistory(mid_history_seconds)
        self._trade_flow = TradeFlowTracker()
        self._latest_book: TrustedBookSnapshot | None = None
        self._history_generation: int | None = None
        self._book_integrity_valid = False
        self._stream_connected = False
        self._last_market_activity_ns: int | None = None
        self._next_emission_ns: int | None = None
        self._stop_event = asyncio.Event()
        self._feature_snapshots_emitted = 0
        self._invalid_snapshots_emitted = 0
        self._book_snapshots_processed = 0
        self._trade_events_processed = 0

    def on_connection(self, connected: bool, *, monotonic_timestamp_ns: int) -> None:
        self._stream_connected = connected
        if connected:
            self._last_market_activity_ns = monotonic_timestamp_ns
            return
        self._last_market_activity_ns = None
        self._invalidate_histories()

    def on_market_activity(self, monotonic_timestamp_ns: int) -> None:
        if (
            self._last_market_activity_ns is None
            or monotonic_timestamp_ns >= self._last_market_activity_ns
        ):
            self._last_market_activity_ns = monotonic_timestamp_ns

    def on_book_invalidated(self) -> None:
        """Revoke temporal continuity immediately on desync or resync."""

        if self._book_integrity_valid or len(self._mid_history):
            self._mid_history.clear()
        self._book_integrity_valid = False

    def _invalidate_histories(self) -> None:
        self._mid_history.clear()
        self._trade_flow.clear()
        self._book_integrity_valid = False

    def on_book_snapshot(self, snapshot: TrustedBookSnapshot) -> None:
        if snapshot.symbol != self.symbol:
            raise ValueError(f"book symbol {snapshot.symbol!r} does not match {self.symbol!r}")
        if self._history_generation != snapshot.resync_count:
            self._mid_history.clear()
            self._history_generation = snapshot.resync_count
        self._latest_book = snapshot
        self._book_snapshots_processed += 1
        self._book_integrity_valid = snapshot.is_healthy and snapshot.state is OrderBookState.SYNCED
        if self._book_integrity_valid:
            observation_ns = (
                snapshot.event_receive_monotonic_ns
                if snapshot.event_receive_monotonic_ns is not None
                else snapshot.timestamp_monotonic_ns
            )
            self._mid_history.add(observation_ns, snapshot.mid_price)
        else:
            self._mid_history.clear()

    def on_trade(self, trade: TradeEvent) -> None:
        if trade.symbol != self.symbol:
            raise ValueError(f"trade symbol {trade.symbol!r} does not match {self.symbol!r}")
        self.on_market_activity(trade.local_receive_monotonic_ns)
        self._trade_flow.add(trade)
        self._trade_events_processed += 1

    def _trade_stream_age_ms(self, now_ns: int) -> float | None:
        if self._last_market_activity_ns is None:
            return None
        return max(0.0, (now_ns - self._last_market_activity_ns) / 1_000_000)

    def _book_age_ms(self, snapshot: TrustedBookSnapshot, now_ns: int) -> float:
        elapsed = max(0.0, (now_ns - snapshot.timestamp_monotonic_ns) / 1_000_000)
        reported_age = snapshot.book_age_ms + elapsed
        if snapshot.event_receive_monotonic_ns is None:
            return reported_age
        direct_age = max(0.0, (now_ns - snapshot.event_receive_monotonic_ns) / 1_000_000)
        return max(reported_age, direct_age)

    def _corrected_exchange_lag_ms(self, snapshot: TrustedBookSnapshot) -> float | None:
        if (
            self._clock_offset is None
            or snapshot.event_receive_time is None
            or snapshot.exchange_event_time is None
        ):
            return None
        return self._clock_offset.corrected_event_lag_ms(
            local_receive_time=snapshot.event_receive_time,
            exchange_event_time=snapshot.exchange_event_time,
        )

    def build_snapshot(self, *, now_ns: int | None = None) -> FeatureSnapshot | None:
        snapshot = self._latest_book
        if snapshot is None:
            return None
        current_ns = self._monotonic_clock_ns() if now_ns is None else now_ns
        book_age_ms = self._book_age_ms(snapshot, current_ns)
        trade_age_ms = self._trade_stream_age_ms(current_ns)
        trade_stream_healthy = (
            self._stream_connected
            and trade_age_ms is not None
            and trade_age_ms <= self._trade_stale_after_ms
        )

        reasons: list[FeatureInvalidReason] = []
        if not self._book_integrity_valid:
            reasons.append(FeatureInvalidReason.BOOK_UNHEALTHY)
        if book_age_ms > self._book_stale_after_ms:
            reasons.append(FeatureInvalidReason.BOOK_STALE)
        if snapshot.spread <= 0 or snapshot.best_bid_price >= snapshot.best_ask_price:
            reasons.append(FeatureInvalidReason.INVALID_SPREAD)
        if snapshot.best_bid_quantity <= 0 or snapshot.best_ask_quantity <= 0:
            reasons.append(FeatureInvalidReason.INVALID_TOP_OF_BOOK)
        if not trade_stream_healthy:
            reasons.append(FeatureInvalidReason.TRADE_STREAM_UNHEALTHY)

        book = calculate_book_features(snapshot, self._imbalance_levels)
        returns = ReturnFeatures(
            return_250ms=self._mid_history.log_return(250, now_ns=current_ns),
            return_1s=self._mid_history.log_return(1_000, now_ns=current_ns),
            return_5s=self._mid_history.log_return(5_000, now_ns=current_ns),
            return_10s=self._mid_history.log_return(10_000, now_ns=current_ns),
        )
        volatility = VolatilityFeatures(
            realized_volatility_1s=self._mid_history.realized_volatility(1_000, now_ns=current_ns),
            realized_volatility_5s=self._mid_history.realized_volatility(5_000, now_ns=current_ns),
            realized_volatility_10s=self._mid_history.realized_volatility(
                10_000, now_ns=current_ns
            ),
        )
        clock_status = None if self._clock_offset is None else self._clock_offset.status()
        result = FeatureSnapshot(
            symbol=self.symbol,
            event_time=snapshot.exchange_event_time,
            local_receive_time=snapshot.event_receive_time,
            created_at=self._wall_clock(),
            monotonic_ns=current_ns,
            book_last_update_id=snapshot.last_update_id,
            book=book,
            returns=returns,
            volatility=volatility,
            trade_flow=self._trade_flow.snapshot(now_ns=current_ns, available=trade_stream_healthy),
            health=FeatureHealth(
                book_age_ms=book_age_ms,
                trade_stream_age_ms=trade_age_ms,
                corrected_exchange_lag_ms=self._corrected_exchange_lag_ms(snapshot),
                book_healthy=self._book_integrity_valid
                and book_age_ms <= self._book_stale_after_ms,
                trade_stream_healthy=trade_stream_healthy,
                clock_sync_healthy=clock_status is not None and clock_status.healthy,
            ),
            feature_valid=not reasons,
            invalid_reasons=tuple(reasons),
        )
        self._feature_snapshots_emitted += 1
        if reasons:
            self._invalid_snapshots_emitted += 1
        return result

    def build_if_due(self, *, now_ns: int | None = None) -> FeatureSnapshot | None:
        current_ns = self._monotonic_clock_ns() if now_ns is None else now_ns
        if self._next_emission_ns is None:
            self._next_emission_ns = current_ns
        if current_ns < self._next_emission_ns:
            return None
        missed_intervals = (current_ns - self._next_emission_ns) // self._interval_ns
        self._next_emission_ns += (missed_intervals + 1) * self._interval_ns
        return self.build_snapshot(now_ns=current_ns)

    async def run(self, handler: FeatureHandler) -> None:
        self._stop_event.clear()
        while not self._stop_event.is_set():
            current_ns = self._monotonic_clock_ns()
            feature = self.build_if_due(now_ns=current_ns)
            if feature is not None:
                await handler(feature)
            deadline = self._next_emission_ns
            delay = self._interval_ns / 1_000_000_000
            if deadline is not None:
                delay = max(0.0, (deadline - self._monotonic_clock_ns()) / 1_000_000_000)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except TimeoutError:
                pass

    async def stop(self) -> None:
        self._stop_event.set()

    def diagnostics(self) -> FeatureEngineDiagnostics:
        clock_status = None if self._clock_offset is None else self._clock_offset.status()
        return FeatureEngineDiagnostics(
            feature_snapshots_emitted=self._feature_snapshots_emitted,
            feature_snapshots_invalid=self._invalid_snapshots_emitted,
            book_snapshots_processed=self._book_snapshots_processed,
            trade_events_processed=self._trade_events_processed,
            clock_sync_successes=0 if clock_status is None else clock_status.successes,
            clock_sync_failures=0 if clock_status is None else clock_status.failures,
            clock_offset_ms=None if clock_status is None else clock_status.offset_ms,
            clock_rtt_ms=None if clock_status is None else clock_status.rtt_ms,
            mid_history_size=len(self._mid_history),
            trade_history_size=self._trade_flow.history_size,
            stream_connected=self._stream_connected,
            history_generation=self._history_generation,
            clock_sync=clock_status,
        )
