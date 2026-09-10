import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from app.diagnostics import BoundedDistribution, detect_event_loop_stalls, scheduling_lag_ms
from features.history import MidPriceHistory
from features.trade_flow import RollingTradeWindow
from market.clock import ExchangeClockOffsetEstimator
from market.orderbook import InvalidBookDataError, LocalOrderBook
from recorder.raw_models import CapturedRawRecord, CaptureMetadata, RawEventType
from recorder.raw_schemas import flatten_raw, raw_schema
from recorder.schemas import FEATURE_SCHEMA, FEATURE_SCHEMA_V1, flatten_feature
from replay.features import decode_feature, feature_equivalence_errors
from replay.loader import _decode_payload
from tests.helpers import NOW, feature_snapshot, market_snapshot, trade_event
from tests.test_clock import CallableClock, ServerTime, wall_time


def test_diagnostic_buffers_are_bounded_but_cumulative_counts_survive():
    distribution = BoundedDistribution(10)
    for i in range(1000):
        distribution.add(float(i))
    distribution.add(float("nan"))
    assert len(distribution.values) == 10 and distribution.summary()["count"] == 1000
    assert distribution.summary()["maximum"] == 999
    assert distribution.summary()["p50"] == 994.5


@pytest.mark.parametrize("actual, expected_lag", [(90, 0), (100, 0), (250, 150)])
def test_event_loop_lag_uses_monotonic_expected_wake(actual, expected_lag):
    assert scheduling_lag_ms(100_000_000, actual * 1_000_000) == expected_lag


async def test_event_loop_heartbeat_emits_persistable_samples_and_stops(monkeypatch):
    stop, distribution, recorded = asyncio.Event(), BoundedDistribution(), []

    async def timeout(awaitable, **kwargs):
        awaitable.close()
        raise TimeoutError

    async def record(value):
        recorded.append(value)
        stop.set()

    monkeypatch.setattr("app.diagnostics.asyncio.wait_for", timeout)
    await detect_event_loop_stalls(stop, distribution, sample_handler=record)
    assert len(recorded) == distribution.count == 1


def test_histories_fail_closed_at_hard_memory_capacity():
    mid = MidPriceHistory(max_observations=2)
    mid.add(0, Decimal(100))
    mid.add(1, Decimal(100))
    with pytest.raises(RuntimeError, match="bounded capacity"):
        mid.add(2, Decimal(100))
    flow = RollingTradeWindow(1000, max_observations=1)
    flow.add(trade_event(monotonic_ns=0))
    with pytest.raises(RuntimeError, match="bounded capacity"):
        flow.add(trade_event(monotonic_ns=1))


def test_orderbook_hard_limit_does_not_silently_prune():
    book = LocalOrderBook("BTCUSDC", hard_level_limit=1)
    with pytest.raises(InvalidBookDataError, match="limit|capacity"):
        book.load_snapshot(
            market_snapshot(bids=((Decimal(100), Decimal(1)), (Decimal(99), Decimal(1))))
        )


def test_finite_rest_depth_coverage_is_revoked_before_unknown_levels_are_trusted():
    from tests.helpers import depth_event

    book = LocalOrderBook("BTCUSDC")
    book.load_snapshot(
        market_snapshot(bids=((Decimal(100), Decimal(1)), (Decimal(99), Decimal(1))))
    )
    book.require_bootstrap_coverage(snapshot_limit=2, levels=1)
    with pytest.raises(InvalidBookDataError, match="coverage"):
        book.apply_event(
            depth_event(
                first_update_id=99,
                final_update_id=101,
                previous_final_update_id=98,
                bids=(
                    (Decimal(100), Decimal(0)),
                    (Decimal(99), Decimal(0)),
                    (Decimal(98), Decimal(1)),
                ),
            )
        )


async def test_clock_preserves_rejected_samples_and_robust_drift():
    samples = []

    async def record(sample):
        samples.append(sample)

    estimator = ExchangeClockOffsetEstimator(
        ServerTime([1060, 2070, 3500, 4090]),
        sample_count=3,
        max_rtt_ms=50,
        wall_clock=CallableClock(
            iter(wall_time(t) for t in (1000, 1100, 2000, 2100, 3000, 3100, 4000, 4100))
        ),
        monotonic_clock_ns=CallableClock(
            iter(
                (
                    0,
                    10_000_000,
                    20_000_000,
                    40_000_000,
                    50_000_000,
                    150_000_000,
                    160_000_000,
                    170_000_000,
                )
            )
        ),
        sample_handler=record,
    )
    await estimator.synchronize(samples=4)
    status = estimator.status()
    assert len(samples) == 4 and samples[2].accepted is False
    assert status.offset_ms == 20 and status.offset_mad_ms == 10
    assert status.min_rtt_ms == 10 and status.offset_drift_ms == 30
    assert status.successes == 3 and status.failures == 1
    assert samples[0].local_send == wall_time(1000)
    assert samples[0].server_time_ms == 1060


def test_feature_v1_migration_is_explicit_and_new_schema_has_semantic_name():
    feature = feature_snapshot(monotonic_ns=0)
    row = flatten_feature(feature, capture_session_id="s", capture_seq=0)
    assert "trade_event_count_1s" in FEATURE_SCHEMA.names
    assert "aggregate_trade_event_count_1s" in FEATURE_SCHEMA_V1.names
    old = {k.replace("trade_event_count", "aggregate_trade_event_count"): v for k, v in row.items()}
    old["schema_version"] = 1
    assert decode_feature(old) == decode_feature(row) == feature
    with pytest.raises(Exception, match="schema version"):
        decode_feature({**row, "schema_version": 99})


@pytest.mark.parametrize("source", ["AGGREGATE", "INDIVIDUAL"])
def test_raw_trade_source_roundtrip_is_never_silently_reinterpreted(source):
    from market.events import TradeSource

    trade = replace(trade_event(monotonic_ns=1), source=TradeSource(source))
    row = flatten_raw(
        CapturedRawRecord(
            CaptureMetadata(0, "s", "binance", "usdm"),
            RawEventType.TRADE,
            trade.symbol,
            NOW,
            NOW,
            1,
            trade,
        )
    )
    decoded = _decode_payload(RawEventType.TRADE, row)
    assert decoded.source == trade.source
    assert row["trade_source"] == source
    with pytest.raises(ValueError, match="unsupported"):
        raw_schema(RawEventType.TRADE, 99)


def test_feature_equivalence_checks_counts_long_horizons_validity_and_decimals():
    feature = feature_snapshot(monotonic_ns=0)
    assert feature_equivalence_errors(
        feature, replace(feature, returns=replace(feature.returns, return_10s=0.1))
    )
    assert feature_equivalence_errors(feature, replace(feature, feature_valid=False))
    flow = replace(feature.trade_flow[0], trade_event_count=99)
    assert feature_equivalence_errors(
        feature, replace(feature, trade_flow=(flow, *feature.trade_flow[1:]))
    )
    assert feature_equivalence_errors(
        feature,
        replace(
            feature,
            book=replace(
                feature.book,
                best_bid_quantity=feature.book.best_bid_quantity + Decimal("0.00000000000001"),
            ),
        ),
    )
