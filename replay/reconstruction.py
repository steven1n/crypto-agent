"""Rebuild from REST plus raw diff events, without using trusted book prices."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass

from exchange.binance.orderbook_sync import is_snapshot_bridge
from market.bootstrap import BootstrapSnapshot
from market.events import BookUpdateEvent
from market.orderbook import LocalOrderBook, OrderBookError, SequenceGapError, TrustedBookSnapshot
from replay.models import ReplayEventType, ReplayIntegrityError, ReplayItem


@dataclass(frozen=True)
class ReconstructionResult:
    snapshots_compared: int
    mismatches: int
    first_mismatch: str | None
    sequence_gap_count: int
    bootstrap_snapshots: int

    @property
    def valid(self) -> bool:
        return self.snapshots_compared > 0 and self.mismatches == 0 and self.sequence_gap_count == 0


class RawReconstructionValidator:
    def __init__(self) -> None:
        self.recent: OrderedDict[int, BookUpdateEvent] = OrderedDict()
        self.book: LocalOrderBook | None = None
        self.bridge_id: int | None = None
        self.compared = self.mismatches = self.gaps = self.bootstraps = 0
        self.first: str | None = None
        self.last_seq = -1
        self.symbol: str | None = None

    def _apply(self, event: BookUpdateEvent) -> None:
        book = self.book
        if book is None:
            return
        if self.bridge_id is not None:
            if event.final_update_id < self.bridge_id:
                return
            if not is_snapshot_bridge(event, self.bridge_id):
                self.book = None
                return
            book.apply_event(event, expected_previous_update_id=None)
            self.bridge_id = None
            return
        if event.final_update_id == book.last_update_id:
            return
        try:
            book.apply_event(event, expected_previous_update_id=book.last_update_id)
        except SequenceGapError:
            self.gaps += 1
            self.book = None
        except OrderBookError:
            self.book = None

    def accept(self, event: ReplayItem) -> None:
        if event.capture_seq <= self.last_seq or (
            self.symbol is not None and self.symbol != event.symbol
        ):
            raise ReplayIntegrityError("invalid reconstruction ordering/symbol")
        self.last_seq, self.symbol = event.capture_seq, event.symbol
        payload = event.payload
        if event.event_type is ReplayEventType.STREAM_LIFECYCLE:
            self.book = None
            self.bridge_id = None
            self.recent.clear()
        elif isinstance(payload, BookUpdateEvent):
            self.recent[payload.final_update_id] = payload
            if len(self.recent) > 50_000:
                self.recent.popitem(last=False)
            self._apply(payload)
        elif isinstance(payload, BootstrapSnapshot):
            self.bootstraps += 1
            self.book = LocalOrderBook(event.symbol)
            self.book.load_snapshot(payload.snapshot)
            self.book.require_bootstrap_coverage(payload.snapshot_limit)
            self.bridge_id = payload.snapshot.last_update_id
            for update_id in payload.buffered_final_update_ids:
                if update_id not in self.recent:
                    raise ReplayIntegrityError(f"missing buffered raw depth {update_id}")
                self._apply(self.recent[update_id])
        elif isinstance(payload, TrustedBookSnapshot):
            self.compared += 1
            if self.book is None or self.bridge_id is not None:
                error = "trusted snapshot has no reconstructed synchronized book"
            else:
                view = self.book.snapshot(max(len(payload.bids), len(payload.asks)))
                keys = (
                    "last_update_id",
                    "bids",
                    "asks",
                    "mid_price",
                    "spread",
                    "spread_bps",
                    "best_bid_price",
                    "best_ask_price",
                    "best_bid_quantity",
                    "best_ask_quantity",
                )
                error = ", ".join(
                    key for key in keys if getattr(view, key) != getattr(payload, key)
                )
            if error:
                self.mismatches += 1
                if self.first is None:
                    self.first = f"capture_seq={event.capture_seq}: {error}"

    def result(self) -> ReconstructionResult:
        return ReconstructionResult(
            self.compared, self.mismatches, self.first, self.gaps, self.bootstraps
        )


def validate_raw_reconstruction(events: Iterable[ReplayItem]) -> ReconstructionResult:
    validator = RawReconstructionValidator()
    for event in events:
        validator.accept(event)
    return validator.result()
