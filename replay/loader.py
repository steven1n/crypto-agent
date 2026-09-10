"""Fail-closed loader for cataloged versioned raw Parquet datasets."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from market.bootstrap import BootstrapSnapshot
from market.clock import ClockOffsetSample
from market.events import (
    AggressorSide,
    BookLevel,
    BookUpdateEvent,
    MarketSnapshot,
    TradeEvent,
    TradeSource,
)
from market.orderbook import OrderBookState, TrustedBookSnapshot
from recorder.catalog import DatasetCatalog, DatasetFileManifest
from recorder.raw_models import LifecycleRecord, RawEventType, RawPayload, RuntimeSample
from recorder.raw_schemas import raw_schema
from recorder.versions import DATASET_VERSION
from replay.models import ReplayEventType, ReplayIntegrityError, ReplayItem


def _levels(value: list[dict[str, Any]]) -> tuple[BookLevel, ...]:
    return tuple((Decimal(str(item["price"])), Decimal(str(item["quantity"]))) for item in value)


def _event_latency_ms(local_time: datetime, exchange_time: datetime) -> float:
    return (local_time - exchange_time).total_seconds() * 1_000


def _decode_payload(event_type: RawEventType, row: dict[str, Any]) -> RawPayload:
    exchange_time = row["exchange_time"]
    local_time = row["local_time"]
    monotonic_ns = int(row["monotonic_ns"])
    if event_type is RawEventType.BOOTSTRAP:
        return BootstrapSnapshot(
            snapshot=MarketSnapshot(
                symbol=row["symbol"],
                last_update_id=row["last_update_id"],
                bids=_levels(row["bids"]),
                asks=_levels(row["asks"]),
                created_at=row["response_receive_time"],
                created_monotonic_ns=row["response_receive_monotonic_ns"],
                exchange_event_time=exchange_time,
                exchange_transaction_time=row["exchange_transaction_time"],
            ),
            connection_generation=row["connection_generation"],
            sync_generation=row["sync_generation"],
            request_start_time=row["request_start_time"],
            request_start_monotonic_ns=row["request_start_monotonic_ns"],
            response_receive_time=row["response_receive_time"],
            response_receive_monotonic_ns=row["response_receive_monotonic_ns"],
            snapshot_limit=row["snapshot_limit"],
            resync_reason=row["resync_reason"],
            clock_offset_ms=row["clock_offset_ms"],
            buffered_final_update_ids=tuple(row["buffered_final_update_ids"]),
        )
    if event_type is RawEventType.TRADE:
        assert exchange_time is not None
        return TradeEvent(
            symbol=row["symbol"],
            exchange_event_time=exchange_time,
            local_receive_time=local_time,
            local_receive_monotonic_ns=monotonic_ns,
            estimated_latency_ms=_event_latency_ms(local_time, exchange_time),
            aggregate_trade_id=int(row["aggregate_trade_id"]),
            price=Decimal(row["price"]),
            quantity=Decimal(row["quantity"]),
            first_trade_id=int(row["first_trade_id"]),
            last_trade_id=int(row["last_trade_id"]),
            trade_time=row["trade_time"],
            aggressor_side=AggressorSide(row["aggressor_side"]),
            buyer_is_maker=bool(row["buyer_is_maker"]),
            source=TradeSource(row["trade_source"]),
        )
    if event_type is RawEventType.DEPTH:
        assert exchange_time is not None
        return BookUpdateEvent(
            symbol=row["symbol"],
            exchange_event_time=exchange_time,
            local_receive_time=local_time,
            local_receive_monotonic_ns=monotonic_ns,
            estimated_latency_ms=_event_latency_ms(local_time, exchange_time),
            transaction_time=row["transaction_time"],
            first_update_id=int(row["first_update_id"]),
            final_update_id=int(row["final_update_id"]),
            previous_final_update_id=int(row["previous_final_update_id"]),
            bids=_levels(row["bids"]),
            asks=_levels(row["asks"]),
        )
    if event_type is RawEventType.TRUSTED_BOOK:
        bids = _levels(row["bids"])
        asks = _levels(row["asks"])
        if not bids or not asks:
            raise ReplayIntegrityError("recorded trusted book has an empty side")
        best_bid_price, best_bid_quantity = bids[0]
        best_ask_price, best_ask_quantity = asks[0]
        mid = (best_bid_price + best_ask_price) / Decimal(2)
        spread = best_ask_price - best_bid_price
        return TrustedBookSnapshot(
            symbol=row["symbol"],
            timestamp=row["snapshot_time"],
            timestamp_monotonic_ns=int(row["snapshot_monotonic_ns"]),
            last_update_id=int(row["last_update_id"]),
            best_bid_price=best_bid_price,
            best_bid_quantity=best_bid_quantity,
            best_ask_price=best_ask_price,
            best_ask_quantity=best_ask_quantity,
            bids=bids,
            asks=asks,
            spread=spread,
            mid_price=mid,
            spread_bps=spread / mid * Decimal(10_000),
            exchange_event_time=exchange_time,
            event_receive_time=row["event_receive_time"],
            event_receive_monotonic_ns=row["event_receive_monotonic_ns"],
            state=OrderBookState(row["state"]),
            is_healthy=bool(row["is_healthy"]),
            book_age_ms=float(row["book_age_ms"]),
            resync_count=int(row["resync_count"]),
        )
    if event_type in {
        RawEventType.STREAM_LIFECYCLE,
        RawEventType.BOOK_LIFECYCLE,
    }:
        return LifecycleRecord(
            symbol=row["symbol"],
            component=row["component"],
            kind=row["kind"],
            local_time=local_time,
            monotonic_ns=monotonic_ns,
            reason=row["reason"],
            resync_count=int(row["resync_count"]),
            sequence_gap_count=int(row["sequence_gap_count"]),
            healthy=bool(row["healthy"]),
            connection_generation=row.get("connection_generation", 0),
            sync_generation=row.get("sync_generation", 0),
        )
    if event_type is RawEventType.RUNTIME_SAMPLE:
        return RuntimeSample(
            local_time,
            monotonic_ns,
            row["event_loop_lag_ms"],
            row["raw_queue_depth"],
            row["feature_queue_depth"],
        )
    if event_type is RawEventType.CLOCK_SAMPLE:
        return ClockOffsetSample(
            offset_ms=float(row["offset_ms"]),
            rtt_ms=float(row["rtt_ms"]),
            sampled_at=row["sampled_at"],
            local_send=row.get("local_send"),
            local_receive=row.get("local_receive"),
            server_time_ms=row.get("server_time_ms"),
            send_monotonic_ns=row.get("send_monotonic_ns"),
            receive_monotonic_ns=row.get("receive_monotonic_ns"),
            accepted=row.get("accepted", True),
        )
    raise ReplayIntegrityError(f"unsupported event type: {event_type}")


class ReplayDatasetLoader:
    def __init__(self, data_directory: Path, catalog: DatasetCatalog | None = None) -> None:
        self._data_directory = data_directory
        self._catalog = catalog or DatasetCatalog(data_directory)

    def load(
        self,
        *,
        capture_session_id: str,
        symbol: str,
        start_local_time: datetime | None = None,
        end_local_time: datetime | None = None,
        required_event_types: frozenset[RawEventType] = frozenset(
            {
                RawEventType.DEPTH,
                RawEventType.TRUSTED_BOOK,
                RawEventType.STREAM_LIFECYCLE,
            }
        ),
    ) -> tuple[ReplayItem, ...]:
        normalized_symbol = symbol.strip().upper()
        manifests = tuple(
            item
            for item in self._catalog.for_session(capture_session_id)
            if item.symbol == normalized_symbol
        )
        raw_names = {item.value for item in RawEventType}
        manifests = tuple(item for item in manifests if item.event_type in raw_names)
        available_types = {RawEventType(item.event_type) for item in manifests}
        missing = required_event_types - available_types
        if missing:
            names = ", ".join(sorted(item.value for item in missing))
            raise ReplayIntegrityError(f"missing required dataset event types: {names}")

        items: list[ReplayItem] = []
        for manifest in manifests:
            items.extend(
                self._load_file(
                    manifest,
                    normalized_symbol,
                    start_local_time,
                    end_local_time,
                )
            )
        items.sort(key=lambda item: item.capture_seq)
        previous_seq: int | None = None
        previous_ns: int | None = None
        for item in items:
            if previous_seq is not None and item.capture_seq == previous_seq:
                raise ReplayIntegrityError(f"duplicate capture_seq: {item.capture_seq}")
            if previous_ns is not None and item.monotonic_ns < previous_ns:
                raise ReplayIntegrityError("impossible negative monotonic delta")
            previous_seq = item.capture_seq
            previous_ns = item.monotonic_ns
        return tuple(items)

    def _load_file(
        self,
        manifest: DatasetFileManifest,
        symbol: str,
        start_local_time: datetime | None,
        end_local_time: datetime | None,
    ) -> list[ReplayItem]:
        if manifest.dataset_version != DATASET_VERSION:
            raise ReplayIntegrityError(f"unsupported dataset version: {manifest.dataset_version}")
        event_type = RawEventType(manifest.event_type)
        try:
            schema = raw_schema(event_type, manifest.schema_version)
        except ValueError as exc:
            raise ReplayIntegrityError(str(exc)) from exc
        path = self._data_directory / manifest.file_path
        if not path.is_file():
            raise ReplayIntegrityError(f"missing cataloged file: {path}")
        try:
            table = pq.ParquetFile(path).read()
        except Exception as exc:
            raise ReplayIntegrityError(f"corrupt Parquet file {path}: {exc}") from exc
        if table.schema != schema:
            raise ReplayIntegrityError(f"Parquet schema mismatch: {path}")
        if table.num_rows != manifest.row_count:
            raise ReplayIntegrityError(f"catalog row count mismatch: {path}")
        rows = table.to_pylist()
        sequences = [int(row["capture_seq"]) for row in rows]
        if any(
            current <= previous for previous, current in zip(sequences, sequences[1:], strict=False)
        ):
            raise ReplayIntegrityError(f"capture ordering inversion inside {path}")

        result: list[ReplayItem] = []
        for row in rows:
            if row["symbol"] != symbol:
                raise ReplayIntegrityError(f"symbol mismatch inside {path}")
            local_time = row["local_time"]
            if start_local_time is not None and local_time < start_local_time:
                continue
            if end_local_time is not None and local_time > end_local_time:
                continue
            result.append(
                ReplayItem(
                    capture_seq=int(row["capture_seq"]),
                    capture_session_id=row["capture_session_id"],
                    event_type=ReplayEventType(event_type.value),
                    symbol=row["symbol"],
                    local_time=local_time,
                    monotonic_ns=int(row["monotonic_ns"]),
                    payload=_decode_payload(event_type, row),
                )
            )
        return result
