from decimal import Decimal

from features.engine import FeatureEngine
from features.models import FeatureInvalidReason
from market.events import AggressorSide
from tests.helpers import trade_event, trusted_book


def connected_engine() -> FeatureEngine:
    engine = FeatureEngine("BTCUSDC")
    engine.on_connection(True, monotonic_timestamp_ns=0)
    engine.on_market_activity(0)
    return engine


def test_engine_emits_healthy_empty_trade_windows_as_known_zero() -> None:
    engine = connected_engine()
    engine.on_book_snapshot(trusted_book(monotonic_ns=0))

    feature = engine.build_snapshot(now_ns=100_000_000)

    assert feature is not None
    assert feature.feature_valid
    assert feature.trade_flow_window(1_000).available
    assert feature.trade_flow_window(1_000).total_volume == Decimal(0)
    assert feature.trade_flow_window(1_000).trade_event_count == 0
    assert feature.returns.return_250ms is None


def test_engine_health_gates_stale_book_and_stream() -> None:
    engine = connected_engine()
    engine.on_book_snapshot(trusted_book(monotonic_ns=0))

    feature = engine.build_snapshot(now_ns=6_000_000_000)

    assert feature is not None
    assert not feature.feature_valid
    assert FeatureInvalidReason.BOOK_STALE in feature.invalid_reasons
    assert FeatureInvalidReason.TRADE_STREAM_UNHEALTHY in feature.invalid_reasons
    assert feature.trade_flow_window(1_000).total_volume is None


def test_zero_top_quantity_is_invalid_and_does_not_fabricate_ratios() -> None:
    engine = connected_engine()
    engine.on_book_snapshot(
        trusted_book(
            bids=((Decimal("100"), Decimal("0")),),
            asks=((Decimal("101"), Decimal("0")),),
            monotonic_ns=0,
        )
    )

    feature = engine.build_snapshot(now_ns=0)

    assert feature is not None
    assert not feature.feature_valid
    assert FeatureInvalidReason.INVALID_TOP_OF_BOOK in feature.invalid_reasons
    assert feature.book.microprice is None
    assert feature.book.at_depth(1).imbalance is None


def test_disconnect_invalidates_temporal_history_and_flow() -> None:
    engine = connected_engine()
    engine.on_book_snapshot(trusted_book(monotonic_ns=0))
    engine.on_trade(trade_event(monotonic_ns=500_000_000, side=AggressorSide.BUY))
    engine.on_book_snapshot(
        trusted_book(
            bids=((Decimal("109"), Decimal("1")),),
            asks=((Decimal("111"), Decimal("1")),),
            monotonic_ns=1_000_000_000,
        )
    )
    before = engine.build_snapshot(now_ns=1_000_000_000)
    assert before is not None
    assert before.returns.return_1s is not None

    engine.on_connection(False, monotonic_timestamp_ns=1_100_000_000)
    after = engine.build_snapshot(now_ns=1_100_000_000)

    assert after is not None
    assert not after.feature_valid
    assert after.returns.return_1s is None
    assert after.trade_flow_window(1_000).total_volume is None


def test_new_resync_generation_cannot_mix_pre_gap_returns() -> None:
    engine = connected_engine()
    engine.on_book_snapshot(trusted_book(monotonic_ns=0, resync_count=0))
    engine.on_book_snapshot(
        trusted_book(
            bids=((Decimal("109"), Decimal("1")),),
            asks=((Decimal("111"), Decimal("1")),),
            monotonic_ns=1_000_000_000,
            resync_count=0,
        )
    )
    before = engine.build_snapshot(now_ns=1_000_000_000)
    assert before is not None and before.returns.return_1s is not None

    engine.on_book_invalidated()
    engine.on_book_snapshot(
        trusted_book(
            bids=((Decimal("119"), Decimal("1")),),
            asks=((Decimal("121"), Decimal("1")),),
            monotonic_ns=2_000_000_000,
            resync_count=1,
        )
    )
    after = engine.build_snapshot(now_ns=2_000_000_000)

    assert after is not None
    assert after.returns.return_1s is None
    assert engine.diagnostics().mid_history_size == 1


def test_cadence_is_anchored_and_skips_missed_slots_without_bursting() -> None:
    engine = connected_engine()
    engine.on_book_snapshot(trusted_book(monotonic_ns=0))

    assert engine.build_if_due(now_ns=0) is not None
    assert engine.build_if_due(now_ns=99_999_999) is None
    assert engine.build_if_due(now_ns=100_000_000) is not None
    assert engine.build_if_due(now_ns=450_000_000) is not None
    assert engine.build_if_due(now_ns=499_999_999) is None
    assert engine.build_if_due(now_ns=500_000_000) is not None


def test_trades_aggregate_into_all_feature_windows() -> None:
    engine = connected_engine()
    engine.on_book_snapshot(trusted_book(monotonic_ns=0))
    engine.on_trade(
        trade_event(
            monotonic_ns=100_000_000,
            side=AggressorSide.SELL,
            quantity=Decimal("2.5"),
        )
    )

    feature = engine.build_snapshot(now_ns=100_000_000)

    assert feature is not None
    assert all(flow.aggressive_sell_volume == Decimal("2.5") for flow in feature.trade_flow)
    assert all(flow.trade_event_count == 1 for flow in feature.trade_flow)
