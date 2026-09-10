"""Normalized market events consumed by downstream research components."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

BookLevel = tuple[Decimal, Decimal]


def utc_from_milliseconds(timestamp_ms: int) -> datetime:
    """Convert Unix milliseconds without introducing floating-point rounding."""

    seconds, milliseconds = divmod(timestamp_ms, 1_000)
    return datetime.fromtimestamp(seconds, tz=UTC) + timedelta(milliseconds=milliseconds)


class AggressorSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class TradeSource(StrEnum):
    AGGREGATE = "AGGREGATE"
    INDIVIDUAL = "INDIVIDUAL"


@dataclass(frozen=True, slots=True, kw_only=True)
class MarketEvent:
    symbol: str
    exchange_event_time: datetime
    local_receive_time: datetime
    local_receive_monotonic_ns: int
    estimated_latency_ms: float


@dataclass(frozen=True, slots=True, kw_only=True)
class TradeEvent(MarketEvent):
    aggregate_trade_id: int
    price: Decimal
    quantity: Decimal
    first_trade_id: int
    last_trade_id: int
    trade_time: datetime
    aggressor_side: AggressorSide
    buyer_is_maker: bool
    source: TradeSource = TradeSource.AGGREGATE


@dataclass(frozen=True, slots=True, kw_only=True)
class BookUpdateEvent(MarketEvent):
    transaction_time: datetime
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class BestBidAskEvent(MarketEvent):
    transaction_time: datetime
    update_id: int
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal


@dataclass(frozen=True, slots=True, kw_only=True)
class MarketSnapshot:
    """Exchange-independent point-in-time L2 representation."""

    symbol: str
    last_update_id: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    created_at: datetime
    created_monotonic_ns: int
    exchange_event_time: datetime | None = None
    exchange_transaction_time: datetime | None = None


NormalizedMarketEvent = TradeEvent | BookUpdateEvent | BestBidAskEvent
