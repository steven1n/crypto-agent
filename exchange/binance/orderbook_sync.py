"""Failure-closed Binance USD-M snapshot and diff-depth synchronization."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic_ns
from typing import Protocol

import structlog

from exchange.binance.market_ws import calculate_backoff
from market.bootstrap import BootstrapSnapshot
from market.events import BookLevel, BookUpdateEvent, MarketSnapshot
from market.orderbook import (
    CrossedBookError,
    InvalidBookDataError,
    LocalOrderBook,
    OrderBookError,
    OrderBookState,
    SequenceGapError,
    TrustedBookSnapshot,
)

logger = structlog.get_logger(__name__)

EventKey = tuple[int, int, int, tuple[BookLevel, ...], tuple[BookLevel, ...]]


class SnapshotProvider(Protocol):
    async def book_snapshot(self, symbol: str, *, limit: int = 1_000) -> MarketSnapshot: ...


class SynchronizationError(OrderBookError):
    """Base class for Binance synchronization failures."""


class SnapshotBridgeError(SynchronizationError):
    """Buffered events cannot bridge the REST snapshot update ID."""


class BufferOverflowError(SynchronizationError):
    """The bounded pre-snapshot depth-event buffer overflowed."""


class StaleUpdateError(SynchronizationError):
    """A non-duplicate event is behind the active book sequence."""


@dataclass(frozen=True, slots=True)
class OrderBookDiagnostics:
    state: OrderBookState
    is_healthy: bool
    is_synced: bool
    last_update_id: int | None
    last_event_monotonic_ns: int | None
    book_age_ms: float | None
    resync_count: int
    last_desync_reason: str | None
    sync_success_count: int
    sync_failure_count: int
    sequence_gap_count: int
    buffer_overflow_count: int
    crossed_book_count: int
    events_applied: int
    events_discarded_as_old: int
    current_buffer_size: int
    last_sync_duration_ms: float | None


def is_snapshot_bridge(event: BookUpdateEvent, snapshot_last_update_id: int) -> bool:
    """Current USD-M rule: ``U <= lastUpdateId <= u`` (no Spot-style +1)."""

    return (
        event.first_update_id <= snapshot_last_update_id
        and event.final_update_id >= snapshot_last_update_id
    )


class BinanceBookSynchronizer:
    """Builds and owns the only trusted local book for one Binance symbol.

    REST I/O happens outside the synchronization lock. Candidate books are
    built and replayed while the active reference remains untrusted or points
    at the last known state. Only a completely validated candidate is promoted.
    """

    def __init__(
        self,
        symbol: str,
        snapshot_provider: SnapshotProvider,
        *,
        snapshot_limit: int = 1_000,
        max_buffered_events: int = 50_000,
        max_stored_levels: int | None = None,
        stale_after_ms: int = 3_000,
        resync_initial_seconds: float = 0.5,
        resync_max_seconds: float = 10.0,
        clock_ns: Callable[[], int] = monotonic_ns,
        bootstrap_handler: Callable[[BootstrapSnapshot], Awaitable[None]] | None = None,
        offset_provider: Callable[[], float | None] | None = None,
        hard_level_limit: int = 100_000,
    ) -> None:
        if max_buffered_events <= 0:
            raise ValueError("max_buffered_events must be positive")
        if stale_after_ms <= 0:
            raise ValueError("stale_after_ms must be positive")
        self.symbol = symbol.strip().upper()
        self._snapshot_provider = snapshot_provider
        self._snapshot_limit = snapshot_limit
        self._max_buffered_events = max_buffered_events
        self._max_stored_levels = max_stored_levels
        self._stale_after_ns = stale_after_ms * 1_000_000
        self._resync_initial_seconds = resync_initial_seconds
        self._resync_max_seconds = resync_max_seconds
        self._clock_ns = clock_ns
        self._bootstrap_handler = bootstrap_handler
        self._offset_provider = offset_provider
        self._hard_level_limit = hard_level_limit
        self.connection_generation = 0
        self.sync_generation = 0

        self._lock = asyncio.Lock()
        self._state = OrderBookState.EMPTY
        self._connected = False
        self._ever_connected = False
        self._active_book: LocalOrderBook | None = None
        self._candidate_book: LocalOrderBook | None = None
        self._candidate_snapshot_id: int | None = None
        self._buffer: deque[BookUpdateEvent] = deque()
        self._sync_epoch = 0
        self._sync_task: asyncio.Task[None] | None = None
        self._sync_started_ns: int | None = None
        self._synced_event = asyncio.Event()

        self._recent_event_keys: deque[EventKey] = deque(maxlen=4_096)
        self._recent_event_key_set: set[EventKey] = set()
        self._last_desync_reason: str | None = None
        self._sync_success_count = 0
        self._sync_failure_count = 0
        self._resync_count = 0
        self._sequence_gap_count = 0
        self._buffer_overflow_count = 0
        self._crossed_book_count = 0
        self._events_applied = 0
        self._events_discarded_as_old = 0
        self._last_sync_duration_ms: float | None = None

    @staticmethod
    def _event_key(event: BookUpdateEvent) -> EventKey:
        return (
            event.first_update_id,
            event.final_update_id,
            event.previous_final_update_id,
            event.bids,
            event.asks,
        )

    def _remember_event(self, event: BookUpdateEvent) -> None:
        key = self._event_key(event)
        if key in self._recent_event_key_set:
            return
        if len(self._recent_event_keys) == self._recent_event_keys.maxlen:
            removed = self._recent_event_keys.popleft()
            self._recent_event_key_set.discard(removed)
        self._recent_event_keys.append(key)
        self._recent_event_key_set.add(key)

    def _clear_recent_events(self) -> None:
        self._recent_event_keys.clear()
        self._recent_event_key_set.clear()

    @property
    def state(self) -> OrderBookState:
        self._refresh_staleness()
        return self._state

    @property
    def is_synced(self) -> bool:
        return self.state is OrderBookState.SYNCED

    @property
    def is_healthy(self) -> bool:
        return self.state is OrderBookState.SYNCED

    @property
    def last_update_id(self) -> int | None:
        return None if self._active_book is None else self._active_book.last_update_id

    @property
    def last_event_monotonic_ns(self) -> int | None:
        return None if self._active_book is None else self._active_book.last_event_monotonic_ns

    @property
    def book_age_ms(self) -> float | None:
        last_ns = self.last_event_monotonic_ns
        if last_ns is None:
            return None
        return max(0.0, (self._clock_ns() - last_ns) / 1_000_000)

    def _refresh_staleness(self) -> None:
        if self._state is not OrderBookState.SYNCED:
            return
        last_ns = self.last_event_monotonic_ns
        if last_ns is None or self._clock_ns() - last_ns > self._stale_after_ns:
            self._state = OrderBookState.STALE
            self._synced_event.clear()
            logger.warning(
                "stale_book_detected",
                symbol=self.symbol,
                book_age_ms=self.book_age_ms,
            )

    def diagnostics(self) -> OrderBookDiagnostics:
        self._refresh_staleness()
        return OrderBookDiagnostics(
            state=self._state,
            is_healthy=self._state is OrderBookState.SYNCED,
            is_synced=self._state is OrderBookState.SYNCED,
            last_update_id=self.last_update_id,
            last_event_monotonic_ns=self.last_event_monotonic_ns,
            book_age_ms=self.book_age_ms,
            resync_count=self._resync_count,
            last_desync_reason=self._last_desync_reason,
            sync_success_count=self._sync_success_count,
            sync_failure_count=self._sync_failure_count,
            sequence_gap_count=self._sequence_gap_count,
            buffer_overflow_count=self._buffer_overflow_count,
            crossed_book_count=self._crossed_book_count,
            events_applied=self._events_applied,
            events_discarded_as_old=self._events_discarded_as_old,
            current_buffer_size=len(self._buffer),
            last_sync_duration_ms=self._last_sync_duration_ms,
        )

    def trusted_snapshot(self, levels: int | None = None) -> TrustedBookSnapshot | None:
        self._refresh_staleness()
        book = self._active_book
        if self._state is not OrderBookState.SYNCED or book is None:
            return None
        view = book.snapshot(levels)
        age = self.book_age_ms
        assert age is not None
        return TrustedBookSnapshot(
            symbol=view.symbol,
            timestamp=datetime.now(UTC),
            timestamp_monotonic_ns=self._clock_ns(),
            last_update_id=view.last_update_id,
            best_bid_price=view.best_bid_price,
            best_bid_quantity=view.best_bid_quantity,
            best_ask_price=view.best_ask_price,
            best_ask_quantity=view.best_ask_quantity,
            bids=view.bids,
            asks=view.asks,
            spread=view.spread,
            mid_price=view.mid_price,
            spread_bps=view.spread_bps,
            exchange_event_time=view.exchange_event_time,
            event_receive_time=view.event_receive_time,
            event_receive_monotonic_ns=view.event_receive_monotonic_ns,
            state=self._state,
            is_healthy=True,
            book_age_ms=age,
            resync_count=self._resync_count,
        )

    async def wait_until_synced(self, timeout_seconds: float) -> None:
        await asyncio.wait_for(self._synced_event.wait(), timeout=timeout_seconds)

    async def start(self) -> None:
        await self.on_connected()

    async def on_connected(self) -> None:
        async with self._lock:
            if self._state is OrderBookState.STOPPED:
                return
            reconnect = self._ever_connected
            self.connection_generation += 1
            self._connected = True
            self._ever_connected = True
            reason = "WebSocket reconnect" if reconnect else "initial WebSocket connection"
            self._begin_sync_locked(reason=reason, resync=reconnect, clear_buffer=True)

    async def on_disconnected(self, reason: str = "WebSocket disconnected") -> None:
        async with self._lock:
            if self._state is OrderBookState.STOPPED:
                return
            self._connected = False
            self._invalidate_sync_locked(reason)
            self._buffer.clear()
            logger.warning("orderbook_desynced", symbol=self.symbol, reason=reason)

    async def stop(self) -> None:
        task: asyncio.Task[None] | None
        async with self._lock:
            self._state = OrderBookState.STOPPED
            self._connected = False
            self._synced_event.clear()
            self._buffer.clear()
            self._candidate_book = None
            self._candidate_snapshot_id = None
            self._sync_epoch += 1
            task = self._sync_task
            self._sync_task = None
            if task is not None:
                task.cancel()
        if task is not None:
            with suppress(asyncio.CancelledError):
                await task

    async def on_depth_event(self, event: BookUpdateEvent) -> None:
        if event.symbol != self.symbol:
            raise InvalidBookDataError(
                f"event symbol {event.symbol!r} does not match {self.symbol!r}"
            )
        async with self._lock:
            if self._state is OrderBookState.STOPPED:
                return
            if not self._connected:
                return

            self._refresh_staleness()
            if self._state in {OrderBookState.SYNCED, OrderBookState.STALE}:
                self._apply_live_event_locked(event)
                return

            if len(self._buffer) >= self._max_buffered_events:
                error = BufferOverflowError(
                    f"depth buffer exceeded {self._max_buffered_events} events"
                )
                self._buffer_overflow_count += 1
                self._sync_failure_count += 1
                logger.error(
                    "orderbook_resync_failed",
                    symbol=self.symbol,
                    reason=str(error),
                    buffer_size=len(self._buffer),
                )
                self._buffer.clear()
                self._buffer.append(event)
                self._begin_sync_locked(reason=str(error), resync=True, clear_buffer=False)
                return

            self._buffer.append(event)
            if self._candidate_book is not None:
                try:
                    self._try_promote_candidate_locked()
                except OrderBookError as exc:
                    self._record_sync_failure_locked(exc)
                    self._begin_sync_locked(
                        reason=str(exc),
                        resync=True,
                        clear_buffer=not isinstance(exc, SnapshotBridgeError),
                    )

    def _invalidate_sync_locked(self, reason: str) -> None:
        self._state = OrderBookState.DESYNCED
        self._last_desync_reason = reason
        self._synced_event.clear()
        self._candidate_book = None
        self._candidate_snapshot_id = None
        self._sync_epoch += 1
        if self._sync_task is not None:
            self._sync_task.cancel()
            self._sync_task = None

    def _begin_sync_locked(self, *, reason: str, resync: bool, clear_buffer: bool) -> None:
        self._invalidate_sync_locked(reason)
        if clear_buffer:
            self._buffer.clear()
        self._clear_recent_events()
        self._sync_started_ns = self._clock_ns()
        if resync:
            self._resync_count += 1
            self._state = OrderBookState.RESYNCING
            logger.warning(
                "orderbook_resync_started",
                symbol=self.symbol,
                reason=reason,
                resync_count=self._resync_count,
            )
        else:
            self._state = OrderBookState.BUFFERING
            logger.info("orderbook_buffering_started", symbol=self.symbol, reason=reason)
        epoch = self._sync_epoch
        self._sync_task = asyncio.create_task(
            self._snapshot_worker(epoch), name=f"{self.symbol}-orderbook-snapshot"
        )

    async def _snapshot_worker(self, epoch: int) -> None:
        attempt = 0
        try:
            while True:
                attempt += 1
                async with self._lock:
                    if (
                        epoch != self._sync_epoch
                        or not self._connected
                        or self._state is OrderBookState.STOPPED
                    ):
                        return
                    self._state = OrderBookState.SNAPSHOT_LOADING
                    self.sync_generation += 1
                    buffer_size = len(self._buffer)
                logger.info(
                    "snapshot_requested",
                    symbol=self.symbol,
                    limit=self._snapshot_limit,
                    buffer_size=buffer_size,
                    attempt=attempt,
                )

                try:
                    request_start_ns = self._clock_ns()
                    request_start_time = datetime.now(UTC)
                    snapshot = await self._snapshot_provider.book_snapshot(
                        self.symbol, limit=self._snapshot_limit
                    )
                    retry = False
                    async with self._lock:
                        if epoch != self._sync_epoch:
                            return
                        if self._bootstrap_handler is not None:
                            await self._bootstrap_handler(
                                BootstrapSnapshot(
                                    snapshot=snapshot,
                                    connection_generation=self.connection_generation,
                                    sync_generation=self.sync_generation,
                                    request_start_time=request_start_time,
                                    request_start_monotonic_ns=request_start_ns,
                                    response_receive_time=snapshot.created_at,
                                    response_receive_monotonic_ns=snapshot.created_monotonic_ns,
                                    snapshot_limit=self._snapshot_limit,
                                    resync_reason=self._last_desync_reason,
                                    clock_offset_ms=(
                                        None
                                        if self._offset_provider is None
                                        else self._offset_provider()
                                    ),
                                    buffered_final_update_ids=tuple(
                                        event.final_update_id for event in self._buffer
                                    ),
                                )
                            )
                        logger.info(
                            "snapshot_received",
                            symbol=self.symbol,
                            lastUpdateId=snapshot.last_update_id,
                            buffer_size=len(self._buffer),
                        )
                        candidate = LocalOrderBook(
                            self.symbol,
                            max_stored_levels=self._max_stored_levels,
                            hard_level_limit=self._hard_level_limit,
                        )
                        candidate.load_snapshot(snapshot)
                        candidate.require_bootstrap_coverage(self._snapshot_limit)
                        self._candidate_book = candidate
                        self._candidate_snapshot_id = snapshot.last_update_id
                        self._state = OrderBookState.SYNCING
                        self._try_promote_candidate_locked()
                        return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    async with self._lock:
                        if epoch != self._sync_epoch:
                            return
                        error = (
                            exc
                            if isinstance(exc, OrderBookError)
                            else SynchronizationError(
                                f"snapshot request failed: {type(exc).__name__}: {exc}"
                            )
                        )
                        self._record_sync_failure_locked(error)
                        self._candidate_book = None
                        self._candidate_snapshot_id = None
                        self._state = OrderBookState.RESYNCING
                        retry = True

                if retry:
                    delay = calculate_backoff(
                        self._resync_initial_seconds,
                        self._resync_max_seconds,
                        attempt,
                    )
                    await asyncio.sleep(delay)
        finally:
            current = asyncio.current_task()
            if self._sync_task is current:
                self._sync_task = None

    def _record_sync_failure_locked(self, error: OrderBookError) -> None:
        self._sync_failure_count += 1
        self._last_desync_reason = str(error)
        self._synced_event.clear()
        if isinstance(error, SequenceGapError):
            self._sequence_gap_count += 1
            logger.error(
                "sequence_gap_detected",
                symbol=self.symbol,
                previous_u=error.expected_previous_u,
                pu=error.actual_pu,
            )
        if isinstance(error, CrossedBookError):
            self._crossed_book_count += 1
            logger.error("crossed_book_detected", symbol=self.symbol, reason=str(error))
        logger.error(
            "orderbook_resync_failed",
            symbol=self.symbol,
            reason=str(error),
            buffer_size=len(self._buffer),
        )

    def _try_promote_candidate_locked(self) -> None:
        candidate = self._candidate_book
        snapshot_id = self._candidate_snapshot_id
        if candidate is None or snapshot_id is None:
            return

        while self._buffer and self._buffer[0].final_update_id < snapshot_id:
            old = self._buffer.popleft()
            self._events_discarded_as_old += 1
            logger.debug(
                "orderbook_bootstrap_event_discarded",
                symbol=self.symbol,
                U=old.first_update_id,
                u=old.final_update_id,
                lastUpdateId=snapshot_id,
            )
        if not self._buffer:
            self._state = OrderBookState.SYNCING
            return

        bridge = self._buffer[0]
        if not is_snapshot_bridge(bridge, snapshot_id):
            raise SnapshotBridgeError(
                "snapshot bridge missing: "
                f"U={bridge.first_update_id}, u={bridge.final_update_id}, "
                f"lastUpdateId={snapshot_id}"
            )
        logger.info(
            "bridge_event_found",
            symbol=self.symbol,
            U=bridge.first_update_id,
            u=bridge.final_update_id,
            pu=bridge.previous_final_update_id,
            lastUpdateId=snapshot_id,
        )

        events = tuple(self._buffer)
        applied: list[BookUpdateEvent] = []
        previous_u: int | None = None
        seen_during_replay: set[EventKey] = set()
        for event in events:
            key = self._event_key(event)
            if key in seen_during_replay:
                self._events_discarded_as_old += 1
                continue
            if previous_u is not None:
                if event.final_update_id <= previous_u:
                    raise StaleUpdateError(
                        "out-of-order buffered event: "
                        f"U={event.first_update_id}, u={event.final_update_id}, "
                        f"previous_u={previous_u}"
                    )
                if event.previous_final_update_id != previous_u:
                    raise SequenceGapError(
                        expected_previous_u=previous_u,
                        actual_pu=event.previous_final_update_id,
                    )
            candidate.apply_event(
                event,
                expected_previous_update_id=previous_u,
            )
            previous_u = event.final_update_id
            seen_during_replay.add(key)
            applied.append(event)

        self._active_book = candidate
        self._candidate_book = None
        self._candidate_snapshot_id = None
        self._buffer.clear()
        self._clear_recent_events()
        for event in applied:
            self._remember_event(event)
        self._events_applied += len(applied)
        self._sync_success_count += 1
        self._state = OrderBookState.SYNCED
        if self._sync_started_ns is not None:
            self._last_sync_duration_ms = (self._clock_ns() - self._sync_started_ns) / 1_000_000
        self._synced_event.set()
        logger.info(
            "orderbook_synced",
            symbol=self.symbol,
            last_update_id=candidate.last_update_id,
            events_applied=len(applied),
            sync_duration_ms=self._last_sync_duration_ms,
        )
        if self._resync_count:
            logger.info(
                "orderbook_resync_succeeded",
                symbol=self.symbol,
                last_update_id=candidate.last_update_id,
                resync_count=self._resync_count,
            )

    def _apply_live_event_locked(self, event: BookUpdateEvent) -> None:
        active = self._active_book
        if active is None or active.last_update_id is None:
            self._begin_sync_locked(
                reason="active book missing while processing depth event",
                resync=True,
                clear_buffer=True,
            )
            self._buffer.append(event)
            return

        key = self._event_key(event)
        if key in self._recent_event_key_set:
            self._events_discarded_as_old += 1
            return
        if event.final_update_id <= active.last_update_id:
            error = StaleUpdateError(
                "non-duplicate stale event after synchronization: "
                f"U={event.first_update_id}, u={event.final_update_id}, "
                f"last_update_id={active.last_update_id}"
            )
            self._record_sync_failure_locked(error)
            self._buffer.clear()
            self._buffer.append(event)
            self._begin_sync_locked(reason=str(error), resync=True, clear_buffer=False)
            return

        try:
            # LocalOrderBook.apply_event validates against copies and commits at
            # the end, so the active instance remains unchanged on failure.
            active.apply_event(
                event,
                expected_previous_update_id=active.last_update_id,
            )
        except OrderBookError as exc:
            self._record_sync_failure_locked(exc)
            self._buffer.clear()
            self._buffer.append(event)
            self._begin_sync_locked(reason=str(exc), resync=True, clear_buffer=False)
            return

        self._remember_event(event)
        self._events_applied += 1
        self._state = OrderBookState.SYNCED
        self._synced_event.set()
