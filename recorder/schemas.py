"""Stable flattened Parquet schema for immutable feature snapshots."""

from __future__ import annotations

from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

from features.models import FeatureSnapshot, TradeFlowMetrics
from recorder.versions import DATASET_VERSION, FEATURE_SCHEMA_VERSION

FEATURE_SCHEMA = pa.schema(
    [
        pa.field("dataset_version", pa.int16(), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("capture_session_id", pa.string(), nullable=False),
        pa.field("capture_seq", pa.int64(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("market_type", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC")),
        pa.field("local_receive_time", pa.timestamp("us", tz="UTC")),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("monotonic_ns", pa.int64(), nullable=False),
        pa.field("book_last_update_id", pa.int64(), nullable=False),
        pa.field("best_bid_price", pa.string(), nullable=False),
        pa.field("best_bid_quantity", pa.string(), nullable=False),
        pa.field("best_ask_price", pa.string(), nullable=False),
        pa.field("best_ask_quantity", pa.string(), nullable=False),
        pa.field("mid_price", pa.string(), nullable=False),
        pa.field("spread", pa.string(), nullable=False),
        pa.field("spread_bps", pa.float64(), nullable=False),
        pa.field("microprice", pa.string()),
        pa.field("microprice_offset", pa.string()),
        pa.field("microprice_offset_bps", pa.float64()),
        *(
            field
            for level in (1, 5, 10)
            for field in (
                pa.field(f"bid_volume_l{level}", pa.string(), nullable=False),
                pa.field(f"ask_volume_l{level}", pa.string(), nullable=False),
                pa.field(f"book_imbalance_l{level}", pa.float64()),
            )
        ),
        pa.field("return_250ms", pa.float64()),
        pa.field("return_1s", pa.float64()),
        pa.field("return_5s", pa.float64()),
        pa.field("return_10s", pa.float64()),
        pa.field("realized_volatility_1s", pa.float64()),
        pa.field("realized_volatility_5s", pa.float64()),
        pa.field("realized_volatility_10s", pa.float64()),
        *(
            field
            for seconds in (1, 5, 10)
            for field in (
                pa.field(f"aggressive_buy_volume_{seconds}s", pa.string()),
                pa.field(f"aggressive_sell_volume_{seconds}s", pa.string()),
                pa.field(f"signed_volume_{seconds}s", pa.string()),
                pa.field(f"total_volume_{seconds}s", pa.string()),
                pa.field(f"trade_event_count_{seconds}s", pa.int64()),
                pa.field(f"trade_imbalance_{seconds}s", pa.float64()),
                pa.field(f"buy_ratio_{seconds}s", pa.float64()),
                pa.field(f"volume_weighted_price_{seconds}s", pa.string()),
            )
        ),
        pa.field("book_age_ms", pa.float64(), nullable=False),
        pa.field("trade_stream_age_ms", pa.float64()),
        pa.field("corrected_exchange_lag_ms", pa.float64()),
        pa.field("book_healthy", pa.bool_(), nullable=False),
        pa.field("trade_stream_healthy", pa.bool_(), nullable=False),
        pa.field("clock_sync_healthy", pa.bool_(), nullable=False),
        pa.field("feature_valid", pa.bool_(), nullable=False),
        pa.field("invalid_reasons", pa.list_(pa.string()), nullable=False),
    ]
)

# Version 1 counted source messages too, but used an ambiguous column name.
# A reader renames explicitly; raw trade_source remains authoritative.
FEATURE_SCHEMA_V1 = pa.schema(
    [
        pa.field(
            field.name.replace("trade_event_count", "aggregate_trade_event_count"),
            field.type,
            nullable=field.nullable,
        )
        for field in FEATURE_SCHEMA
    ]
)


def feature_schema(version: int) -> pa.Schema:
    if version == 1:
        return FEATURE_SCHEMA_V1
    if version == FEATURE_SCHEMA_VERSION:
        return FEATURE_SCHEMA
    raise ValueError(f"unsupported feature schema version: {version}")


def _flow_columns(flow: TradeFlowMetrics) -> dict[str, Any]:
    suffix = f"{flow.window_ms // 1_000}s"
    return {
        f"aggressive_buy_volume_{suffix}": (
            None if flow.aggressive_buy_volume is None else str(flow.aggressive_buy_volume)
        ),
        f"aggressive_sell_volume_{suffix}": (
            None if flow.aggressive_sell_volume is None else str(flow.aggressive_sell_volume)
        ),
        f"signed_volume_{suffix}": (
            None if flow.signed_volume is None else str(flow.signed_volume)
        ),
        f"total_volume_{suffix}": (None if flow.total_volume is None else str(flow.total_volume)),
        f"trade_event_count_{suffix}": flow.trade_event_count,
        f"trade_imbalance_{suffix}": flow.trade_imbalance,
        f"buy_ratio_{suffix}": flow.buy_ratio,
        f"volume_weighted_price_{suffix}": (
            None if flow.volume_weighted_price is None else str(flow.volume_weighted_price)
        ),
    }


def flatten_feature(
    snapshot: FeatureSnapshot,
    *,
    capture_session_id: str = "standalone",
    capture_seq: int = 0,
    exchange: str = "binance",
    market_type: str = "usdm",
) -> dict[str, Any]:
    """Flatten nested domain groups while retaining exact decimals as strings."""

    row: dict[str, Any] = {
        "dataset_version": DATASET_VERSION,
        "schema_version": FEATURE_SCHEMA_VERSION,
        "capture_session_id": capture_session_id,
        "capture_seq": capture_seq,
        "exchange": exchange,
        "market_type": market_type,
        "symbol": snapshot.symbol,
        "event_time": snapshot.event_time,
        "local_receive_time": snapshot.local_receive_time,
        "created_at": snapshot.created_at,
        "monotonic_ns": snapshot.monotonic_ns,
        "book_last_update_id": snapshot.book_last_update_id,
        "best_bid_price": str(snapshot.book.best_bid_price),
        "best_bid_quantity": str(snapshot.book.best_bid_quantity),
        "best_ask_price": str(snapshot.book.best_ask_price),
        "best_ask_quantity": str(snapshot.book.best_ask_quantity),
        "mid_price": str(snapshot.book.mid_price),
        "spread": str(snapshot.book.spread),
        "spread_bps": snapshot.book.spread_bps,
        "microprice": (None if snapshot.book.microprice is None else str(snapshot.book.microprice)),
        "microprice_offset": (
            None
            if snapshot.book.microprice_offset is None
            else str(snapshot.book.microprice_offset)
        ),
        "microprice_offset_bps": snapshot.book.microprice_offset_bps,
        "return_250ms": snapshot.returns.return_250ms,
        "return_1s": snapshot.returns.return_1s,
        "return_5s": snapshot.returns.return_5s,
        "return_10s": snapshot.returns.return_10s,
        "realized_volatility_1s": snapshot.volatility.realized_volatility_1s,
        "realized_volatility_5s": snapshot.volatility.realized_volatility_5s,
        "realized_volatility_10s": snapshot.volatility.realized_volatility_10s,
        "book_age_ms": snapshot.health.book_age_ms,
        "trade_stream_age_ms": snapshot.health.trade_stream_age_ms,
        "corrected_exchange_lag_ms": snapshot.health.corrected_exchange_lag_ms,
        "book_healthy": snapshot.health.book_healthy,
        "trade_stream_healthy": snapshot.health.trade_stream_healthy,
        "clock_sync_healthy": snapshot.health.clock_sync_healthy,
        "feature_valid": snapshot.feature_valid,
        "invalid_reasons": [reason.value for reason in snapshot.invalid_reasons],
    }
    for level in (1, 5, 10):
        depth = snapshot.book.at_depth(level)
        row[f"bid_volume_l{level}"] = str(depth.bid_volume)
        row[f"ask_volume_l{level}"] = str(depth.ask_volume)
        row[f"book_imbalance_l{level}"] = depth.imbalance
    for flow in snapshot.trade_flow:
        row.update(_flow_columns(flow))
    return row
