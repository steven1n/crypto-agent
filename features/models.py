"""Immutable feature-domain models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class FeatureInvalidReason(StrEnum):
    BOOK_UNHEALTHY = "BOOK_UNHEALTHY"
    BOOK_STALE = "BOOK_STALE"
    INVALID_SPREAD = "INVALID_SPREAD"
    INVALID_TOP_OF_BOOK = "INVALID_TOP_OF_BOOK"
    TRADE_STREAM_UNHEALTHY = "TRADE_STREAM_UNHEALTHY"


@dataclass(frozen=True, slots=True)
class DepthMetrics:
    levels: int
    bid_volume: Decimal
    ask_volume: Decimal
    imbalance: float | None


@dataclass(frozen=True, slots=True)
class BookFeatures:
    best_bid_price: Decimal
    best_bid_quantity: Decimal
    best_ask_price: Decimal
    best_ask_quantity: Decimal
    mid_price: Decimal
    spread: Decimal
    spread_bps: float
    microprice: Decimal | None
    microprice_offset: Decimal | None
    microprice_offset_bps: float | None
    depth: tuple[DepthMetrics, ...]

    def at_depth(self, levels: int) -> DepthMetrics:
        try:
            return next(item for item in self.depth if item.levels == levels)
        except StopIteration as exc:
            raise KeyError(f"depth level {levels} was not calculated") from exc


@dataclass(frozen=True, slots=True)
class ReturnFeatures:
    return_250ms: float | None
    return_1s: float | None
    return_5s: float | None
    return_10s: float | None


@dataclass(frozen=True, slots=True)
class VolatilityFeatures:
    realized_volatility_1s: float | None
    realized_volatility_5s: float | None
    realized_volatility_10s: float | None


@dataclass(frozen=True, slots=True)
class TradeFlowMetrics:
    window_ms: int
    available: bool
    aggressive_buy_volume: Decimal | None
    aggressive_sell_volume: Decimal | None
    signed_volume: Decimal | None
    total_volume: Decimal | None
    trade_event_count: int | None
    trade_imbalance: float | None
    buy_ratio: float | None
    volume_weighted_price: Decimal | None


@dataclass(frozen=True, slots=True)
class FeatureHealth:
    book_age_ms: float
    trade_stream_age_ms: float | None
    corrected_exchange_lag_ms: float | None
    book_healthy: bool
    trade_stream_healthy: bool
    clock_sync_healthy: bool


@dataclass(frozen=True, slots=True)
class FeatureSnapshot:
    symbol: str
    event_time: datetime | None
    local_receive_time: datetime | None
    created_at: datetime
    monotonic_ns: int
    book_last_update_id: int
    book: BookFeatures
    returns: ReturnFeatures
    volatility: VolatilityFeatures
    trade_flow: tuple[TradeFlowMetrics, ...]
    health: FeatureHealth
    feature_valid: bool
    invalid_reasons: tuple[FeatureInvalidReason, ...]

    def trade_flow_window(self, window_ms: int) -> TradeFlowMetrics:
        try:
            return next(item for item in self.trade_flow if item.window_ms == window_ms)
        except StopIteration as exc:
            raise KeyError(f"trade-flow window {window_ms} was not calculated") from exc
