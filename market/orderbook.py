"""Exchange-independent, precision-safe in-memory L2 order book."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from time import monotonic_ns

from market.events import BookLevel, BookUpdateEvent, MarketSnapshot


class BookSide(StrEnum):
    BID = "BID"
    ASK = "ASK"


class OrderBookState(StrEnum):
    EMPTY = "EMPTY"
    BUFFERING = "BUFFERING"
    SNAPSHOT_LOADING = "SNAPSHOT_LOADING"
    SYNCING = "SYNCING"
    SYNCED = "SYNCED"
    STALE = "STALE"
    DESYNCED = "DESYNCED"
    RESYNCING = "RESYNCING"
    STOPPED = "STOPPED"


class OrderBookError(RuntimeError):
    """Base class for local order-book integrity failures."""


class InvalidBookDataError(OrderBookError):
    """A snapshot or update contains an invalid price or quantity."""


class CrossedBookError(OrderBookError):
    """The best bid is greater than or equal to the best ask."""


class EmptyBookSideError(OrderBookError):
    """A validated order book unexpectedly has an empty side."""


class SequenceGapError(OrderBookError):
    """A depth event doesn't continue the previously applied event."""

    def __init__(self, *, expected_previous_u: int, actual_pu: int) -> None:
        super().__init__(
            f"sequence gap: expected pu={expected_previous_u}, received pu={actual_pu}"
        )
        self.expected_previous_u = expected_previous_u
        self.actual_pu = actual_pu


@dataclass(frozen=True, slots=True)
class OrderBookView:
    symbol: str
    timestamp: datetime
    timestamp_monotonic_ns: int
    last_update_id: int
    best_bid_price: Decimal
    best_bid_quantity: Decimal
    best_ask_price: Decimal
    best_ask_quantity: Decimal
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    spread: Decimal
    mid_price: Decimal
    spread_bps: Decimal
    exchange_event_time: datetime | None
    event_receive_time: datetime | None
    event_receive_monotonic_ns: int | None


@dataclass(frozen=True, slots=True)
class TrustedBookSnapshot(OrderBookView):
    state: OrderBookState
    is_healthy: bool
    book_age_ms: float
    resync_count: int


class LocalOrderBook:
    """Deterministic L2 state using absolute quantities.

    The default retains every known level. ``max_stored_levels`` is optional
    because pruning deep levels can make them unavailable after nearer levels
    are later deleted. When configured, only the best N known levels per side
    are retained; deletion of an already-pruned level remains a valid no-op.
    """

    def __init__(
        self, symbol: str, *, max_stored_levels: int | None = None, hard_level_limit: int = 100_000
    ) -> None:
        if max_stored_levels is not None and max_stored_levels <= 0:
            raise ValueError("max_stored_levels must be positive or None")
        self.symbol = symbol.strip().upper()
        self.max_stored_levels = max_stored_levels
        self.hard_level_limit = hard_level_limit
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self.last_update_id: int | None = None
        self.last_event_monotonic_ns: int | None = None
        self.last_event_exchange_time: datetime | None = None
        self.last_event_receive_time: datetime | None = None
        self._bootstrap_bid_boundary: Decimal | None = None
        self._bootstrap_ask_boundary: Decimal | None = None
        self._coverage_levels = 10

    def clone(self) -> LocalOrderBook:
        clone = LocalOrderBook(
            self.symbol,
            max_stored_levels=self.max_stored_levels,
            hard_level_limit=self.hard_level_limit,
        )
        clone._bids = self._bids.copy()
        clone._asks = self._asks.copy()
        clone.last_update_id = self.last_update_id
        clone.last_event_monotonic_ns = self.last_event_monotonic_ns
        clone.last_event_exchange_time = self.last_event_exchange_time
        clone.last_event_receive_time = self.last_event_receive_time
        clone._bootstrap_bid_boundary = self._bootstrap_bid_boundary
        clone._bootstrap_ask_boundary = self._bootstrap_ask_boundary
        clone._coverage_levels = self._coverage_levels
        return clone

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self.last_update_id = None
        self.last_event_monotonic_ns = None
        self.last_event_exchange_time = None
        self.last_event_receive_time = None
        self._bootstrap_bid_boundary = self._bootstrap_ask_boundary = None

    def require_bootstrap_coverage(self, snapshot_limit: int, levels: int = 10) -> None:
        """Resnapshot if requested top levels move beyond truncated REST coverage."""
        self._bootstrap_bid_boundary = (
            min(self._bids) if len(self._bids) >= snapshot_limit else None
        )
        self._bootstrap_ask_boundary = (
            max(self._asks) if len(self._asks) >= snapshot_limit else None
        )
        self._coverage_levels = levels

    def _validate_coverage(
        self, bids: dict[Decimal, Decimal], asks: dict[Decimal, Decimal]
    ) -> None:
        bid, ask = self._bootstrap_bid_boundary, self._bootstrap_ask_boundary
        if (bid is not None and sum(p >= bid for p in bids) < self._coverage_levels) or (
            ask is not None and sum(p <= ask for p in asks) < self._coverage_levels
        ):
            raise InvalidBookDataError("top depth escaped known REST coverage; resnapshot required")

    @staticmethod
    def _validate_level(price: Decimal, quantity: Decimal, *, snapshot: bool) -> None:
        if price <= 0:
            raise InvalidBookDataError(f"price must be positive, received {price}")
        if quantity < 0:
            raise InvalidBookDataError(f"quantity cannot be negative, received {quantity}")
        if snapshot and quantity == 0:
            raise InvalidBookDataError("snapshot quantities must be positive")

    @staticmethod
    def _validated_levels(levels: tuple[BookLevel, ...]) -> dict[Decimal, Decimal]:
        result: dict[Decimal, Decimal] = {}
        for price, quantity in levels:
            LocalOrderBook._validate_level(price, quantity, snapshot=True)
            if price in result:
                raise InvalidBookDataError(f"duplicate snapshot price level: {price}")
            result[price] = quantity
        return result

    @staticmethod
    def _validate_book(bids: dict[Decimal, Decimal], asks: dict[Decimal, Decimal]) -> None:
        if not bids:
            raise EmptyBookSideError("bid side is empty")
        if not asks:
            raise EmptyBookSideError("ask side is empty")
        best_bid = max(bids)
        best_ask = min(asks)
        if best_bid >= best_ask:
            raise CrossedBookError(
                f"crossed book: best bid {best_bid} is not below best ask {best_ask}"
            )

    def _prune(self, levels: dict[Decimal, Decimal], side: BookSide) -> None:
        if len(levels) > self.hard_level_limit:
            raise InvalidBookDataError("hard order-book level limit exceeded; resnapshot required")
        limit = self.max_stored_levels
        if limit is None or len(levels) <= limit:
            return
        reverse = side is BookSide.BID
        retained = set(sorted(levels, reverse=reverse)[:limit])
        for price in tuple(levels):
            if price not in retained:
                del levels[price]

    def load_snapshot(self, snapshot: MarketSnapshot) -> None:
        if snapshot.symbol != self.symbol:
            raise InvalidBookDataError(
                f"snapshot symbol {snapshot.symbol!r} does not match {self.symbol!r}"
            )
        if snapshot.last_update_id < 0:
            raise InvalidBookDataError("snapshot last_update_id cannot be negative")
        bids = self._validated_levels(snapshot.bids)
        asks = self._validated_levels(snapshot.asks)
        self._prune(bids, BookSide.BID)
        self._prune(asks, BookSide.ASK)
        self._validate_book(bids, asks)
        self._bids = bids
        self._asks = asks
        self._bootstrap_bid_boundary = self._bootstrap_ask_boundary = None
        self.last_update_id = snapshot.last_update_id
        self.last_event_monotonic_ns = None
        self.last_event_exchange_time = None
        self.last_event_receive_time = None

    def _updated_side(
        self,
        side: BookSide,
        price: Decimal,
        quantity: Decimal,
    ) -> tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]]:
        self._validate_level(price, quantity, snapshot=False)
        bids = self._bids.copy()
        asks = self._asks.copy()
        levels = bids if side is BookSide.BID else asks
        if quantity == 0:
            levels.pop(price, None)
        else:
            levels[price] = quantity
        self._prune(levels, side)
        self._validate_book(bids, asks)
        self._validate_coverage(bids, asks)
        return bids, asks

    def apply_bid(self, price: Decimal, quantity: Decimal) -> None:
        bids, asks = self._updated_side(BookSide.BID, price, quantity)
        self._bids, self._asks = bids, asks

    def apply_ask(self, price: Decimal, quantity: Decimal) -> None:
        bids, asks = self._updated_side(BookSide.ASK, price, quantity)
        self._bids, self._asks = bids, asks

    def apply_event(
        self,
        event: BookUpdateEvent,
        *,
        expected_previous_update_id: int | None = None,
    ) -> None:
        if event.symbol != self.symbol:
            raise InvalidBookDataError(
                f"event symbol {event.symbol!r} does not match {self.symbol!r}"
            )
        if event.first_update_id > event.final_update_id:
            raise InvalidBookDataError(
                f"event U={event.first_update_id} exceeds u={event.final_update_id}"
            )
        if (
            expected_previous_update_id is not None
            and event.previous_final_update_id != expected_previous_update_id
        ):
            raise SequenceGapError(
                expected_previous_u=expected_previous_update_id,
                actual_pu=event.previous_final_update_id,
            )

        bids = self._bids.copy()
        asks = self._asks.copy()
        for price, quantity in event.bids:
            self._validate_level(price, quantity, snapshot=False)
            if quantity == 0:
                bids.pop(price, None)
            else:
                bids[price] = quantity
        for price, quantity in event.asks:
            self._validate_level(price, quantity, snapshot=False)
            if quantity == 0:
                asks.pop(price, None)
            else:
                asks[price] = quantity
        self._prune(bids, BookSide.BID)
        self._prune(asks, BookSide.ASK)
        self._validate_book(bids, asks)
        self._validate_coverage(bids, asks)
        self._bids, self._asks = bids, asks
        self.last_update_id = event.final_update_id
        self.last_event_monotonic_ns = event.local_receive_monotonic_ns
        self.last_event_exchange_time = event.exchange_event_time
        self.last_event_receive_time = event.local_receive_time

    def best_bid(self) -> BookLevel | None:
        if not self._bids:
            return None
        price = max(self._bids)
        return price, self._bids[price]

    def best_ask(self) -> BookLevel | None:
        if not self._asks:
            return None
        price = min(self._asks)
        return price, self._asks[price]

    def mid_price(self) -> Decimal | None:
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is None or ask is None:
            return None
        return (bid[0] + ask[0]) / Decimal(2)

    def spread(self) -> Decimal | None:
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is None or ask is None:
            return None
        return ask[0] - bid[0]

    def spread_bps(self) -> Decimal | None:
        spread = self.spread()
        mid = self.mid_price()
        if spread is None or mid is None or mid == 0:
            return None
        return spread / mid * Decimal(10_000)

    def depth(self, side: BookSide, levels: int | None = None) -> tuple[BookLevel, ...]:
        if levels is not None and levels < 0:
            raise ValueError("levels cannot be negative")
        source = self._bids if side is BookSide.BID else self._asks
        ordered = tuple(
            (price, source[price]) for price in sorted(source, reverse=side is BookSide.BID)
        )
        return ordered if levels is None else ordered[:levels]

    def snapshot(self, levels: int | None = None) -> OrderBookView:
        self._validate_book(self._bids, self._asks)
        if self.last_update_id is None:
            raise InvalidBookDataError("book has no update ID")
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        spread = self.spread()
        mid = self.mid_price()
        spread_bps = self.spread_bps()
        assert best_bid is not None
        assert best_ask is not None
        assert spread is not None
        assert mid is not None
        assert spread_bps is not None
        now_ns = monotonic_ns()
        return OrderBookView(
            symbol=self.symbol,
            timestamp=datetime.now(UTC),
            timestamp_monotonic_ns=now_ns,
            last_update_id=self.last_update_id,
            best_bid_price=best_bid[0],
            best_bid_quantity=best_bid[1],
            best_ask_price=best_ask[0],
            best_ask_quantity=best_ask[1],
            bids=self.depth(BookSide.BID, levels),
            asks=self.depth(BookSide.ASK, levels),
            spread=spread,
            mid_price=mid,
            spread_bps=spread_bps,
            exchange_event_time=self.last_event_exchange_time,
            event_receive_time=self.last_event_receive_time,
            event_receive_monotonic_ns=self.last_event_monotonic_ns,
        )
