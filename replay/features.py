"""Load recorded feature ticks and compare online/offline feature calculations."""

from __future__ import annotations

import math
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from features.models import (
    BookFeatures,
    DepthMetrics,
    FeatureHealth,
    FeatureInvalidReason,
    FeatureSnapshot,
    ReturnFeatures,
    TradeFlowMetrics,
    VolatilityFeatures,
)
from recorder.catalog import DatasetCatalog
from recorder.schemas import feature_schema
from recorder.versions import DATASET_VERSION, FEATURE_SCHEMA_VERSION
from replay.models import ReplayEventType, ReplayIntegrityError, ReplayItem


def _optional_decimal(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def decode_feature(row: dict[str, Any]) -> FeatureSnapshot:
    if row.get("schema_version") not in (1, FEATURE_SCHEMA_VERSION):
        raise ReplayIntegrityError("unsupported feature schema version")
    if row.get("schema_version") == 1:
        row = {
            key.replace("aggregate_trade_event_count", "trade_event_count"): value
            for key, value in row.items()
        }
    depths = tuple(
        DepthMetrics(
            levels=level,
            bid_volume=Decimal(row[f"bid_volume_l{level}"]),
            ask_volume=Decimal(row[f"ask_volume_l{level}"]),
            imbalance=row[f"book_imbalance_l{level}"],
        )
        for level in (1, 5, 10)
    )
    flows = tuple(
        TradeFlowMetrics(
            window_ms=seconds * 1_000,
            available=row[f"total_volume_{seconds}s"] is not None,
            aggressive_buy_volume=_optional_decimal(row[f"aggressive_buy_volume_{seconds}s"]),
            aggressive_sell_volume=_optional_decimal(row[f"aggressive_sell_volume_{seconds}s"]),
            signed_volume=_optional_decimal(row[f"signed_volume_{seconds}s"]),
            total_volume=_optional_decimal(row[f"total_volume_{seconds}s"]),
            trade_event_count=row[f"trade_event_count_{seconds}s"],
            trade_imbalance=row[f"trade_imbalance_{seconds}s"],
            buy_ratio=row[f"buy_ratio_{seconds}s"],
            volume_weighted_price=_optional_decimal(row[f"volume_weighted_price_{seconds}s"]),
        )
        for seconds in (1, 5, 10)
    )
    return FeatureSnapshot(
        symbol=row["symbol"],
        event_time=row["event_time"],
        local_receive_time=row["local_receive_time"],
        created_at=row["created_at"],
        monotonic_ns=int(row["monotonic_ns"]),
        book_last_update_id=int(row["book_last_update_id"]),
        book=BookFeatures(
            best_bid_price=Decimal(row["best_bid_price"]),
            best_bid_quantity=Decimal(row["best_bid_quantity"]),
            best_ask_price=Decimal(row["best_ask_price"]),
            best_ask_quantity=Decimal(row["best_ask_quantity"]),
            mid_price=Decimal(row["mid_price"]),
            spread=Decimal(row["spread"]),
            spread_bps=float(row["spread_bps"]),
            microprice=_optional_decimal(row["microprice"]),
            microprice_offset=_optional_decimal(row["microprice_offset"]),
            microprice_offset_bps=row["microprice_offset_bps"],
            depth=depths,
        ),
        returns=ReturnFeatures(
            return_250ms=row["return_250ms"],
            return_1s=row["return_1s"],
            return_5s=row["return_5s"],
            return_10s=row["return_10s"],
        ),
        volatility=VolatilityFeatures(
            realized_volatility_1s=row["realized_volatility_1s"],
            realized_volatility_5s=row["realized_volatility_5s"],
            realized_volatility_10s=row["realized_volatility_10s"],
        ),
        trade_flow=flows,
        health=FeatureHealth(
            book_age_ms=float(row["book_age_ms"]),
            trade_stream_age_ms=row["trade_stream_age_ms"],
            corrected_exchange_lag_ms=row["corrected_exchange_lag_ms"],
            book_healthy=bool(row["book_healthy"]),
            trade_stream_healthy=bool(row["trade_stream_healthy"]),
            clock_sync_healthy=bool(row["clock_sync_healthy"]),
        ),
        feature_valid=bool(row["feature_valid"]),
        invalid_reasons=tuple(FeatureInvalidReason(value) for value in row["invalid_reasons"]),
    )


def load_feature_ticks(
    data_directory: Path,
    *,
    capture_session_id: str,
    symbol: str,
    catalog: DatasetCatalog | None = None,
) -> tuple[ReplayItem, ...]:
    active_catalog = catalog or DatasetCatalog(data_directory)
    manifests = tuple(
        item
        for item in active_catalog.for_session(capture_session_id)
        if item.event_type == "feature" and item.symbol == symbol.strip().upper()
    )
    if not manifests:
        raise ReplayIntegrityError("missing recorded feature ticks")
    ticks: list[ReplayItem] = []
    for manifest in manifests:
        if manifest.dataset_version != DATASET_VERSION or manifest.schema_version not in (
            1,
            FEATURE_SCHEMA_VERSION,
        ):
            raise ReplayIntegrityError("feature dataset/schema version mismatch")
        path = data_directory / manifest.file_path
        try:
            table = pq.ParquetFile(path).read()
        except Exception as exc:
            raise ReplayIntegrityError(f"corrupt feature Parquet file: {path}") from exc
        if (
            table.schema != feature_schema(manifest.schema_version)
            or table.num_rows != manifest.row_count
        ):
            raise ReplayIntegrityError(f"feature schema or catalog count mismatch: {path}")
        for row in table.to_pylist():
            feature = decode_feature(row)
            ticks.append(
                ReplayItem(
                    capture_seq=int(row["capture_seq"]),
                    capture_session_id=row["capture_session_id"],
                    event_type=ReplayEventType.FEATURE_TICK,
                    symbol=row["symbol"],
                    local_time=row["created_at"],
                    monotonic_ns=int(row["monotonic_ns"]),
                    payload=feature,
                )
            )
    ticks.sort(key=lambda item: item.capture_seq)
    return tuple(ticks)


def merge_replay_streams(*streams: tuple[ReplayItem, ...]) -> tuple[ReplayItem, ...]:
    merged = sorted(
        (item for stream in streams for item in stream),
        key=lambda item: item.capture_seq,
    )
    sequences = [item.capture_seq for item in merged]
    if any(
        current == previous for previous, current in zip(sequences, sequences[1:], strict=False)
    ):
        raise ReplayIntegrityError("duplicate capture_seq across replay streams")
    return tuple(merged)


def feature_equivalence_errors(
    actual: FeatureSnapshot,
    recorded: FeatureSnapshot,
    *,
    absolute_tolerance: float = 1e-12,
) -> tuple[str, ...]:
    """Compare every field; Decimal, identity and validity fields are exact."""
    errors: list[str] = []

    def compare(left: Any, right: Any, path: str) -> None:
        if isinstance(left, dict) and isinstance(right, dict):
            for key in sorted(left.keys() | right.keys()):
                compare(left.get(key), right.get(key), f"{path}.{key}".lstrip("."))
        elif isinstance(left, (tuple, list)) and isinstance(right, (tuple, list)):
            if len(left) != len(right):
                errors.append(path)
            else:
                for index, (a, b) in enumerate(zip(left, right, strict=True)):
                    compare(a, b, f"{path}[{index}]")
        elif isinstance(left, float) and isinstance(right, float):
            if not math.isclose(left, right, rel_tol=0, abs_tol=absolute_tolerance):
                errors.append(path)
        elif left != right:
            errors.append(path)

    compare(asdict(actual), asdict(recorded), "")
    return tuple(errors)
