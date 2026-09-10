"""Versioned flat Arrow schemas and serializers for normalized source records."""

from __future__ import annotations

from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

from market.bootstrap import BootstrapSnapshot
from market.clock import ClockOffsetSample
from market.events import BookUpdateEvent, TradeEvent
from market.orderbook import TrustedBookSnapshot
from recorder.raw_models import CapturedRawRecord, LifecycleRecord, RawEventType, RuntimeSample
from recorder.versions import (
    BOOTSTRAP_SCHEMA_VERSION,
    CLOCK_SCHEMA_VERSION,
    DATASET_VERSION,
    LIFECYCLE_SCHEMA_VERSION,
    RAW_DEPTH_SCHEMA_VERSION,
    RAW_TRADE_SCHEMA_VERSION,
    TRUSTED_BOOK_SCHEMA_VERSION,
)

UTC_TS = pa.timestamp("us", tz="UTC")
LEVELS = pa.list_(pa.struct([pa.field("price", pa.string()), pa.field("quantity", pa.string())]))

COMMON_FIELDS = [
    pa.field("dataset_version", pa.int16(), nullable=False),
    pa.field("schema_version", pa.int16(), nullable=False),
    pa.field("capture_session_id", pa.string(), nullable=False),
    pa.field("capture_seq", pa.int64(), nullable=False),
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("market_type", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("exchange_time", UTC_TS),
    pa.field("local_time", UTC_TS, nullable=False),
    pa.field("monotonic_ns", pa.int64(), nullable=False),
]

TRADE_SCHEMA = pa.schema(
    [
        *COMMON_FIELDS,
        pa.field("trade_source", pa.string(), nullable=False),
        pa.field("aggregate_trade_id", pa.int64(), nullable=False),
        pa.field("price", pa.string(), nullable=False),
        pa.field("quantity", pa.string(), nullable=False),
        pa.field("first_trade_id", pa.int64(), nullable=False),
        pa.field("last_trade_id", pa.int64(), nullable=False),
        pa.field("trade_time", UTC_TS, nullable=False),
        pa.field("aggressor_side", pa.string(), nullable=False),
        pa.field("buyer_is_maker", pa.bool_(), nullable=False),
    ]
)

DEPTH_SCHEMA = pa.schema(
    [
        *COMMON_FIELDS,
        pa.field("transaction_time", UTC_TS, nullable=False),
        pa.field("first_update_id", pa.int64(), nullable=False),
        pa.field("final_update_id", pa.int64(), nullable=False),
        pa.field("previous_final_update_id", pa.int64(), nullable=False),
        pa.field("bids", LEVELS, nullable=False),
        pa.field("asks", LEVELS, nullable=False),
    ]
)

TRUSTED_BOOK_SCHEMA = pa.schema(
    [
        *COMMON_FIELDS,
        pa.field("last_update_id", pa.int64(), nullable=False),
        pa.field("snapshot_time", UTC_TS, nullable=False),
        pa.field("snapshot_monotonic_ns", pa.int64(), nullable=False),
        pa.field("event_receive_time", UTC_TS),
        pa.field("event_receive_monotonic_ns", pa.int64()),
        pa.field("bids", LEVELS, nullable=False),
        pa.field("asks", LEVELS, nullable=False),
        pa.field("state", pa.string(), nullable=False),
        pa.field("is_healthy", pa.bool_(), nullable=False),
        pa.field("book_age_ms", pa.float64(), nullable=False),
        pa.field("resync_count", pa.int64(), nullable=False),
    ]
)

LIFECYCLE_SCHEMA = pa.schema(
    [
        *COMMON_FIELDS,
        pa.field("component", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("reason", pa.string()),
        pa.field("resync_count", pa.int64(), nullable=False),
        pa.field("sequence_gap_count", pa.int64(), nullable=False),
        pa.field("healthy", pa.bool_(), nullable=False),
        pa.field("connection_generation", pa.int64(), nullable=False),
        pa.field("sync_generation", pa.int64(), nullable=False),
    ]
)

CLOCK_SCHEMA = pa.schema(
    [
        *COMMON_FIELDS,
        pa.field("offset_ms", pa.float64(), nullable=False),
        pa.field("rtt_ms", pa.float64(), nullable=False),
        pa.field("sampled_at", UTC_TS, nullable=False),
        pa.field("local_send", UTC_TS),
        pa.field("local_receive", UTC_TS),
        pa.field("server_time_ms", pa.int64()),
        pa.field("send_monotonic_ns", pa.int64()),
        pa.field("receive_monotonic_ns", pa.int64()),
        pa.field("accepted", pa.bool_(), nullable=False),
    ]
)

BOOTSTRAP_SCHEMA = pa.schema(
    [
        *COMMON_FIELDS,
        pa.field("connection_generation", pa.int64(), nullable=False),
        pa.field("sync_generation", pa.int64(), nullable=False),
        pa.field("request_start_time", UTC_TS, nullable=False),
        pa.field("request_start_monotonic_ns", pa.int64(), nullable=False),
        pa.field("response_receive_time", UTC_TS, nullable=False),
        pa.field("response_receive_monotonic_ns", pa.int64(), nullable=False),
        pa.field("exchange_transaction_time", UTC_TS),
        pa.field("last_update_id", pa.int64(), nullable=False),
        pa.field("bids", LEVELS, nullable=False),
        pa.field("asks", LEVELS, nullable=False),
        pa.field("snapshot_limit", pa.int64(), nullable=False),
        pa.field("resync_reason", pa.string()),
        pa.field("clock_offset_ms", pa.float64()),
        pa.field("buffered_final_update_ids", pa.list_(pa.int64()), nullable=False),
    ]
)

RUNTIME_SCHEMA = pa.schema(
    [
        *COMMON_FIELDS,
        pa.field("event_loop_lag_ms", pa.float64(), nullable=False),
        pa.field("raw_queue_depth", pa.int64(), nullable=False),
        pa.field("feature_queue_depth", pa.int64(), nullable=False),
    ]
)

SCHEMAS = {
    RawEventType.RUNTIME_SAMPLE: RUNTIME_SCHEMA,
    RawEventType.BOOTSTRAP: BOOTSTRAP_SCHEMA,
    RawEventType.TRADE: TRADE_SCHEMA,
    RawEventType.DEPTH: DEPTH_SCHEMA,
    RawEventType.TRUSTED_BOOK: TRUSTED_BOOK_SCHEMA,
    RawEventType.STREAM_LIFECYCLE: LIFECYCLE_SCHEMA,
    RawEventType.BOOK_LIFECYCLE: LIFECYCLE_SCHEMA,
    RawEventType.CLOCK_SAMPLE: CLOCK_SCHEMA,
}

SCHEMA_VERSIONS = {
    RawEventType.RUNTIME_SAMPLE: 1,
    RawEventType.BOOTSTRAP: BOOTSTRAP_SCHEMA_VERSION,
    RawEventType.TRADE: RAW_TRADE_SCHEMA_VERSION,
    RawEventType.DEPTH: RAW_DEPTH_SCHEMA_VERSION,
    RawEventType.TRUSTED_BOOK: TRUSTED_BOOK_SCHEMA_VERSION,
    RawEventType.STREAM_LIFECYCLE: LIFECYCLE_SCHEMA_VERSION,
    RawEventType.BOOK_LIFECYCLE: LIFECYCLE_SCHEMA_VERSION,
    RawEventType.CLOCK_SAMPLE: CLOCK_SCHEMA_VERSION,
}


def raw_schema(event_type: RawEventType, version: int) -> pa.Schema:
    schema = SCHEMAS[event_type]
    if version == SCHEMA_VERSIONS[event_type]:
        return schema
    if version == 1 and event_type is RawEventType.CLOCK_SAMPLE:
        return pa.schema(list(schema)[: len(COMMON_FIELDS) + 3])
    if version == 1 and event_type in (RawEventType.STREAM_LIFECYCLE, RawEventType.BOOK_LIFECYCLE):
        return pa.schema(list(schema)[:-2])
    raise ValueError(f"unsupported {event_type} schema version {version}")


def _levels(value: tuple[tuple[Any, Any], ...]) -> list[dict[str, str]]:
    return [{"price": str(price), "quantity": str(quantity)} for price, quantity in value]


def flatten_raw(record: CapturedRawRecord) -> dict[str, Any]:
    metadata = record.metadata
    row: dict[str, Any] = {
        "dataset_version": DATASET_VERSION,
        "schema_version": SCHEMA_VERSIONS[record.event_type],
        "capture_session_id": metadata.capture_session_id,
        "capture_seq": metadata.capture_seq,
        "exchange": metadata.exchange,
        "market_type": metadata.market_type,
        "symbol": record.symbol,
        "exchange_time": record.exchange_time,
        "local_time": record.local_time,
        "monotonic_ns": record.monotonic_ns,
    }
    payload = record.payload
    if isinstance(payload, TradeEvent):
        row.update(
            trade_source=payload.source.value,
            aggregate_trade_id=payload.aggregate_trade_id,
            price=str(payload.price),
            quantity=str(payload.quantity),
            first_trade_id=payload.first_trade_id,
            last_trade_id=payload.last_trade_id,
            trade_time=payload.trade_time,
            aggressor_side=payload.aggressor_side.value,
            buyer_is_maker=payload.buyer_is_maker,
        )
    elif isinstance(payload, BookUpdateEvent):
        row.update(
            transaction_time=payload.transaction_time,
            first_update_id=payload.first_update_id,
            final_update_id=payload.final_update_id,
            previous_final_update_id=payload.previous_final_update_id,
            bids=_levels(payload.bids),
            asks=_levels(payload.asks),
        )
    elif isinstance(payload, TrustedBookSnapshot):
        row.update(
            last_update_id=payload.last_update_id,
            snapshot_time=payload.timestamp,
            snapshot_monotonic_ns=payload.timestamp_monotonic_ns,
            event_receive_time=payload.event_receive_time,
            event_receive_monotonic_ns=payload.event_receive_monotonic_ns,
            bids=_levels(payload.bids),
            asks=_levels(payload.asks),
            state=payload.state.value,
            is_healthy=payload.is_healthy,
            book_age_ms=payload.book_age_ms,
            resync_count=payload.resync_count,
        )
    elif isinstance(payload, LifecycleRecord):
        row.update(
            component=payload.component,
            kind=payload.kind,
            reason=payload.reason,
            resync_count=payload.resync_count,
            sequence_gap_count=payload.sequence_gap_count,
            healthy=payload.healthy,
            connection_generation=payload.connection_generation,
            sync_generation=payload.sync_generation,
        )
    elif isinstance(payload, RuntimeSample):
        row.update(
            event_loop_lag_ms=payload.event_loop_lag_ms,
            raw_queue_depth=payload.raw_queue_depth,
            feature_queue_depth=payload.feature_queue_depth,
        )
    elif isinstance(payload, ClockOffsetSample):
        row.update(
            offset_ms=payload.offset_ms,
            rtt_ms=payload.rtt_ms,
            sampled_at=payload.sampled_at,
            local_send=payload.local_send,
            local_receive=payload.local_receive,
            server_time_ms=payload.server_time_ms,
            send_monotonic_ns=payload.send_monotonic_ns,
            receive_monotonic_ns=payload.receive_monotonic_ns,
            accepted=payload.accepted,
        )
    elif isinstance(payload, BootstrapSnapshot):
        row.update(
            connection_generation=payload.connection_generation,
            sync_generation=payload.sync_generation,
            request_start_time=payload.request_start_time,
            request_start_monotonic_ns=payload.request_start_monotonic_ns,
            response_receive_time=payload.response_receive_time,
            response_receive_monotonic_ns=payload.response_receive_monotonic_ns,
            exchange_transaction_time=payload.snapshot.exchange_transaction_time,
            last_update_id=payload.snapshot.last_update_id,
            bids=_levels(payload.snapshot.bids),
            asks=_levels(payload.snapshot.asks),
            snapshot_limit=payload.snapshot_limit,
            resync_reason=payload.resync_reason,
            clock_offset_ms=payload.clock_offset_ms,
            buffered_final_update_ids=list(payload.buffered_final_update_ids),
        )
    else:  # pragma: no cover - closed union guard
        raise TypeError(f"unsupported raw payload: {type(payload).__name__}")
    return row
