from datetime import UTC, datetime, timedelta
from decimal import Decimal

from features.models import (
    BookFeatures,
    DepthMetrics,
    FeatureHealth,
    FeatureSnapshot,
    ReturnFeatures,
    TradeFlowMetrics,
    VolatilityFeatures,
)
from market.events import (
    AggressorSide,
    BookLevel,
    BookUpdateEvent,
    MarketSnapshot,
    TradeEvent,
)
from market.orderbook import OrderBookState, TrustedBookSnapshot

NOW = datetime(2026, 9, 4, tzinfo=UTC)


def market_snapshot(
    *,
    symbol: str = "BTCUSDC",
    last_update_id: int = 100,
    bids: tuple[BookLevel, ...] = ((Decimal("100"), Decimal("1")),),
    asks: tuple[BookLevel, ...] = ((Decimal("102"), Decimal("1")),),
    monotonic_ns: int = 1_000_000_000,
) -> MarketSnapshot:
    return MarketSnapshot(
        symbol=symbol,
        last_update_id=last_update_id,
        bids=bids,
        asks=asks,
        created_at=NOW,
        created_monotonic_ns=monotonic_ns,
    )


def depth_event(
    *,
    symbol: str = "BTCUSDC",
    first_update_id: int,
    final_update_id: int,
    previous_final_update_id: int,
    bids: tuple[BookLevel, ...] = (),
    asks: tuple[BookLevel, ...] = (),
    monotonic_ns: int | None = None,
) -> BookUpdateEvent:
    return BookUpdateEvent(
        symbol=symbol,
        exchange_event_time=NOW,
        local_receive_time=NOW,
        local_receive_monotonic_ns=(
            monotonic_ns if monotonic_ns is not None else final_update_id * 1_000_000
        ),
        estimated_latency_ms=1.0,
        transaction_time=NOW,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
        bids=bids,
        asks=asks,
    )


def trusted_book(
    *,
    symbol: str = "BTCUSDC",
    last_update_id: int = 101,
    bids: tuple[BookLevel, ...] = (
        (Decimal("100"), Decimal("2")),
        (Decimal("99"), Decimal("3")),
        (Decimal("98"), Decimal("4")),
        (Decimal("97"), Decimal("5")),
        (Decimal("96"), Decimal("6")),
    ),
    asks: tuple[BookLevel, ...] = (
        (Decimal("102"), Decimal("1")),
        (Decimal("103"), Decimal("2")),
        (Decimal("104"), Decimal("3")),
        (Decimal("105"), Decimal("4")),
        (Decimal("106"), Decimal("5")),
    ),
    monotonic_ns: int = 1_000_000_000,
    book_age_ms: float = 0.0,
    resync_count: int = 0,
    healthy: bool = True,
) -> TrustedBookSnapshot:
    best_bid_price, best_bid_quantity = bids[0]
    best_ask_price, best_ask_quantity = asks[0]
    spread = best_ask_price - best_bid_price
    mid = (best_bid_price + best_ask_price) / Decimal(2)
    return TrustedBookSnapshot(
        symbol=symbol,
        timestamp=NOW,
        timestamp_monotonic_ns=monotonic_ns,
        last_update_id=last_update_id,
        best_bid_price=best_bid_price,
        best_bid_quantity=best_bid_quantity,
        best_ask_price=best_ask_price,
        best_ask_quantity=best_ask_quantity,
        bids=bids,
        asks=asks,
        spread=spread,
        mid_price=mid,
        spread_bps=spread / mid * Decimal(10_000),
        exchange_event_time=NOW,
        event_receive_time=NOW,
        event_receive_monotonic_ns=monotonic_ns,
        state=OrderBookState.SYNCED if healthy else OrderBookState.DESYNCED,
        is_healthy=healthy,
        book_age_ms=book_age_ms,
        resync_count=resync_count,
    )


def trade_event(
    *,
    symbol: str = "BTCUSDC",
    monotonic_ns: int,
    side: AggressorSide = AggressorSide.BUY,
    price: Decimal = Decimal("101"),
    quantity: Decimal = Decimal("1"),
    aggregate_trade_id: int = 1,
) -> TradeEvent:
    return TradeEvent(
        symbol=symbol,
        exchange_event_time=NOW,
        local_receive_time=NOW,
        local_receive_monotonic_ns=monotonic_ns,
        estimated_latency_ms=0,
        aggregate_trade_id=aggregate_trade_id,
        price=price,
        quantity=quantity,
        first_trade_id=aggregate_trade_id,
        last_trade_id=aggregate_trade_id,
        trade_time=NOW,
        aggressor_side=side,
        buyer_is_maker=side is AggressorSide.SELL,
    )


def feature_snapshot(
    *,
    symbol: str = "BTCUSDT",
    monotonic_ns: int,
    mid: Decimal = Decimal("100"),
    microprice_offset_bps: float = 0.0,
    imbalance_l1: float = 0.0,
    imbalance_l5: float = 0.0,
    imbalance_l10: float = 0.0,
    trade_imbalance_1s: float | None = 0.0,
    return_250ms: float | None = None,
    return_1s: float | None = None,
    rv_5s: float | None = None,
    valid: bool = True,
) -> FeatureSnapshot:
    spread = Decimal("0.10")
    bid = mid - spread / 2
    ask = mid + spread / 2
    microprice_offset = mid * Decimal(str(microprice_offset_bps)) / Decimal(10_000)

    def flow(window_ms: int, imbalance: float | None) -> TradeFlowMetrics:
        if imbalance is None:
            return TradeFlowMetrics(
                window_ms=window_ms,
                available=False,
                aggressive_buy_volume=None,
                aggressive_sell_volume=None,
                signed_volume=None,
                total_volume=None,
                trade_event_count=None,
                trade_imbalance=None,
                buy_ratio=None,
                volume_weighted_price=None,
            )
        buy = Decimal(1) + Decimal(str(imbalance))
        sell = Decimal(1) - Decimal(str(imbalance))
        return TradeFlowMetrics(
            window_ms=window_ms,
            available=True,
            aggressive_buy_volume=buy,
            aggressive_sell_volume=sell,
            signed_volume=buy - sell,
            total_volume=buy + sell,
            trade_event_count=2,
            trade_imbalance=imbalance,
            buy_ratio=float(buy / (buy + sell)),
            volume_weighted_price=mid,
        )

    created = NOW + timedelta(microseconds=monotonic_ns / 1_000)
    return FeatureSnapshot(
        symbol=symbol,
        event_time=created,
        local_receive_time=created,
        created_at=created,
        monotonic_ns=monotonic_ns,
        book_last_update_id=monotonic_ns,
        book=BookFeatures(
            best_bid_price=bid,
            best_bid_quantity=Decimal(1),
            best_ask_price=ask,
            best_ask_quantity=Decimal(1),
            mid_price=mid,
            spread=spread,
            spread_bps=float(spread / mid * Decimal(10_000)),
            microprice=mid + microprice_offset,
            microprice_offset=microprice_offset,
            microprice_offset_bps=microprice_offset_bps,
            depth=(
                DepthMetrics(1, Decimal(1), Decimal(1), imbalance_l1),
                DepthMetrics(5, Decimal(5), Decimal(5), imbalance_l5),
                DepthMetrics(10, Decimal(10), Decimal(10), imbalance_l10),
            ),
        ),
        returns=ReturnFeatures(return_250ms, return_1s, None, None),
        volatility=VolatilityFeatures(None, rv_5s, None),
        trade_flow=(
            flow(1_000, trade_imbalance_1s),
            flow(5_000, trade_imbalance_1s),
            flow(10_000, trade_imbalance_1s),
        ),
        health=FeatureHealth(0, 0, 0, True, True, True),
        feature_valid=valid,
        invalid_reasons=(),
    )
