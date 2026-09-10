"""Pure top-of-book and depth feature calculations."""

from __future__ import annotations

from decimal import Decimal

from features.models import BookFeatures, DepthMetrics
from market.orderbook import TrustedBookSnapshot


def calculate_microprice(snapshot: TrustedBookSnapshot) -> Decimal | None:
    """Return quantity-weighted top-of-book microprice.

    ``(ask_price * bid_qty + bid_price * ask_qty) / (bid_qty + ask_qty)``
    """

    total_quantity = snapshot.best_bid_quantity + snapshot.best_ask_quantity
    if total_quantity <= 0:
        return None
    return (
        snapshot.best_ask_price * snapshot.best_bid_quantity
        + snapshot.best_bid_price * snapshot.best_ask_quantity
    ) / total_quantity


def calculate_depth_metrics(snapshot: TrustedBookSnapshot, levels: int) -> DepthMetrics:
    if levels <= 0:
        raise ValueError("levels must be positive")
    bid_volume = sum((quantity for _, quantity in snapshot.bids[:levels]), Decimal(0))
    ask_volume = sum((quantity for _, quantity in snapshot.asks[:levels]), Decimal(0))
    total_volume = bid_volume + ask_volume
    imbalance = None if total_volume == 0 else float((bid_volume - ask_volume) / total_volume)
    return DepthMetrics(
        levels=levels,
        bid_volume=bid_volume,
        ask_volume=ask_volume,
        imbalance=imbalance,
    )


def calculate_book_features(
    snapshot: TrustedBookSnapshot,
    imbalance_levels: tuple[int, ...] = (1, 5, 10),
) -> BookFeatures:
    microprice = calculate_microprice(snapshot)
    microprice_offset = None if microprice is None else microprice - snapshot.mid_price
    microprice_offset_bps = (
        None
        if microprice_offset is None or snapshot.mid_price <= 0
        else float(microprice_offset / snapshot.mid_price * Decimal(10_000))
    )
    return BookFeatures(
        best_bid_price=snapshot.best_bid_price,
        best_bid_quantity=snapshot.best_bid_quantity,
        best_ask_price=snapshot.best_ask_price,
        best_ask_quantity=snapshot.best_ask_quantity,
        mid_price=snapshot.mid_price,
        spread=snapshot.spread,
        spread_bps=float(snapshot.spread / snapshot.mid_price * Decimal(10_000)),
        microprice=microprice,
        microprice_offset=microprice_offset,
        microprice_offset_bps=microprice_offset_bps,
        depth=tuple(calculate_depth_metrics(snapshot, level) for level in imbalance_levels),
    )
