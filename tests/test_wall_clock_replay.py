import math
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from features.engine import FeatureEngine
from replay.clock import ReplayClock
from replay.engine import ReplayEngine
from replay.models import ReplayIntegrityError
from research.labels import LabelGenerator
from tests.helpers import NOW, feature_snapshot, trade_event, trusted_book
from tests.test_replay import replay_events


def test_recorded_wall_rollback_preserves_time_and_elapsed() -> None:
    clock = ReplayClock()
    a = NOW.replace(hour=17, minute=11, second=15, microsecond=500000)
    b = a - timedelta(milliseconds=297)
    clock.advance(wall_time=a, monotonic_ns=10_000_000_000, capture_seq=100)
    clock.advance(wall_time=b, monotonic_ns=10_100_000_000, capture_seq=101)
    assert clock.wall_time() == b
    assert clock.monotonic_ns() == 10_100_000_000
    assert clock.backward_step_count == 1
    step = clock.last_backward_step
    assert step is not None
    assert step.capture_seq == 101 and step.previous_wall_time == a
    assert step.current_wall_time == b and step.monotonic_elapsed_ms == 100
    assert step.backward_ms == 297
    assert step.wall_minus_monotonic_step_ms == -397


def test_monotonic_inversion_still_fails_without_mutating_clock() -> None:
    clock = ReplayClock()
    clock.advance(wall_time=NOW, monotonic_ns=10_100_000_000)
    with pytest.raises(ValueError, match="negative replay monotonic"):
        clock.advance(wall_time=NOW - timedelta(seconds=1), monotonic_ns=10_000_000_000)
    assert clock.wall_time() == NOW
    assert clock.monotonic_ns() == 10_100_000_000
    assert clock.backward_step_count == 0
    with pytest.raises(ValueError, match="timezone-aware"):
        clock.advance(wall_time=NOW.replace(tzinfo=None), monotonic_ns=10_200_000_000)


@pytest.mark.parametrize("sequence", [0, -1])
@pytest.mark.parametrize("streaming", [False, True])
def test_capture_corruption_remains_fail_closed(sequence: int, streaming: bool) -> None:
    events = (replay_events()[0], replace(replay_events()[1], capture_seq=sequence))
    with pytest.raises(ReplayIntegrityError):
        ReplayEngine(
            iter(events) if streaming else events, ReplayClock(), FeatureEngine("BTCUSDT")
        ).run()


def test_live_feature_equivalence_with_rollback_and_window_expiry() -> None:
    # Independent live-style clock injection, not a first replay used as its own oracle.
    wall = NOW
    live = FeatureEngine("BTCUSDT", wall_clock=lambda: wall)
    live.on_connection(True, monotonic_timestamp_ns=0)
    live.on_book_snapshot(trusted_book(symbol="BTCUSDT", monotonic_ns=0))
    events = list(replay_events())
    wall = events[2].local_time
    expected_first = live.build_snapshot(now_ns=100_000_000)
    live.on_trade(trade_event(symbol="BTCUSDT", monotonic_ns=150_000_000))
    live.on_market_activity(200_000_000)
    live.on_book_snapshot(trusted_book(symbol="BTCUSDT", monotonic_ns=200_000_000))
    wall = NOW - timedelta(milliseconds=197)
    expected_second = live.build_snapshot(now_ns=200_000_000)
    events[2] = replace(events[2], payload=expected_first)
    events[4] = replace(events[4], local_time=wall)
    events[5] = replace(events[5], local_time=wall, payload=expected_second)
    # Time expiry remains monotonic even though civil time is behind the original wall time.
    wall = NOW - timedelta(milliseconds=100)
    expected_expired = live.build_snapshot(now_ns=2_000_000_000)
    events.append(
        replace(
            events[5],
            capture_seq=6,
            local_time=wall,
            monotonic_ns=2_000_000_000,
            payload=expected_expired,
        )
    )
    clock = ReplayClock()
    engine = FeatureEngine(
        "BTCUSDT", monotonic_clock_ns=clock.monotonic_ns, wall_clock=clock.wall_time
    )
    observed: list[int] = []
    result = ReplayEngine(
        iter(events), clock, engine, consumers=(lambda e: observed.append(e.capture_seq),)
    ).run()
    assert result.features == (expected_first, expected_second, expected_expired)
    assert result.recorded_features == result.features
    assert observed == list(range(7))
    assert result.features[1].trade_flow_window(1000).trade_event_count == 1
    assert result.features[2].trade_flow_window(1000).trade_event_count == 0
    assert result.features[2].health.book_age_ms == 1800
    assert clock.backward_step_count == 1


def test_rollback_does_not_bypass_invalid_or_lifecycle_label_boundaries() -> None:
    features = tuple(
        replace(
            feature_snapshot(monotonic_ns=i * 250_000_000),
            created_at=NOW - timedelta(milliseconds=i * 100),
        )
        for i in range(6)
    )
    assert LabelGenerator().generate(features)[0].future_return_500ms == 0
    invalid = (features[0], replace(features[1], feature_valid=False), *features[2:])
    assert LabelGenerator().generate(invalid)[0].future_return_500ms is None
    assert (
        LabelGenerator()
        .generate(features, integrity_boundaries_ns=(100_000_000,))[0]
        .future_return_500ms
        is None
    )
    engine = FeatureEngine("BTCUSDT", wall_clock=lambda: NOW - timedelta(seconds=1))
    engine.on_connection(True, monotonic_timestamp_ns=0)
    engine.on_book_snapshot(trusted_book(symbol="BTCUSDT", monotonic_ns=0))
    engine.on_trade(trade_event(symbol="BTCUSDT", monotonic_ns=0))
    engine.on_connection(False, monotonic_timestamp_ns=100_000_000)
    assert engine.diagnostics().mid_history_size == 0
    assert engine.diagnostics().trade_history_size == 0
    engine.on_connection(True, monotonic_timestamp_ns=200_000_000)
    engine.on_book_snapshot(
        trusted_book(symbol="BTCUSDT", monotonic_ns=200_000_000, resync_count=1)
    )
    snapshot = engine.build_snapshot(now_ns=300_000_000)
    assert snapshot is not None
    assert snapshot.returns.return_250ms is None
    assert snapshot.trade_flow_window(1000).trade_event_count == 0


def test_returns_volatility_and_staleness_ignore_civil_rollback() -> None:
    clock = ReplayClock()
    engine = FeatureEngine("BTCUSDT", wall_clock=clock.wall_time)
    engine.on_connection(True, monotonic_timestamp_ns=0)
    engine.on_book_snapshot(trusted_book(symbol="BTCUSDT", monotonic_ns=0))
    clock.advance(wall_time=NOW, monotonic_ns=0)
    newer = trusted_book(
        symbol="BTCUSDT",
        monotonic_ns=500_000_000,
        bids=((Decimal("101"), Decimal("2")),),
        asks=((Decimal("103"), Decimal("1")),),
    )
    clock.advance(wall_time=NOW - timedelta(seconds=1), monotonic_ns=500_000_000)
    engine.on_book_snapshot(newer)
    engine.on_market_activity(500_000_000)
    feature = engine.build_snapshot(now_ns=500_000_000)
    assert feature is not None
    expected = math.log(102 / 101)
    assert feature.returns.return_250ms == pytest.approx(expected)
    assert feature.volatility.realized_volatility_1s == pytest.approx(abs(expected))
    clock.advance(wall_time=NOW - timedelta(milliseconds=900), monotonic_ns=4_000_000_000)
    stale = engine.build_snapshot(now_ns=4_000_000_000)
    assert stale is not None and not stale.feature_valid
    assert stale.health.book_age_ms == 3500
    assert stale.volatility.realized_volatility_1s is None


def test_backward_diagnostics_retain_only_largest_and_last() -> None:
    clock = ReplayClock()
    clock.advance(wall_time=NOW, monotonic_ns=0)
    clock.advance(wall_time=NOW - timedelta(seconds=2), monotonic_ns=1)
    largest = clock.largest_backward_step
    for i in range(1, 1001):
        clock.advance(wall_time=NOW - timedelta(seconds=2, milliseconds=i), monotonic_ns=i + 1)
    assert clock.backward_step_count == 1001
    assert clock.largest_backward_step is largest
    assert clock.last_backward_step is not None
    assert clock.last_backward_step.backward_ms == 1
    assert len(vars(clock)) == 5
