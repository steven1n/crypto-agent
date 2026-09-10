from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from execution.costs import TransactionCostModel
from features.engine import FeatureEngine
from recorder.raw_models import LifecycleRecord
from replay.clock import ReplayClock
from replay.engine import ReplayEngine, ReplayMode
from replay.features import feature_equivalence_errors
from replay.models import ReplayEventType, ReplayIntegrityError, ReplayItem
from research.evaluation import evaluate_strategy
from strategies.baselines import BookImbalanceStrategy
from tests.helpers import NOW, trade_event, trusted_book


def replay_events(recorded_feature=None):  # type: ignore[no-untyped-def]
    return (
        ReplayItem(
            0,
            "session",
            ReplayEventType.STREAM_LIFECYCLE,
            "BTCUSDT",
            NOW,
            0,
            LifecycleRecord("BTCUSDT", "websocket", "CONNECTED", NOW, 0),
        ),
        ReplayItem(
            1,
            "session",
            ReplayEventType.TRUSTED_BOOK,
            "BTCUSDT",
            NOW,
            0,
            trusted_book(symbol="BTCUSDT", monotonic_ns=0),
        ),
        ReplayItem(
            2,
            "session",
            ReplayEventType.FEATURE_TICK,
            "BTCUSDT",
            NOW + timedelta(milliseconds=100),
            100_000_000,
            recorded_feature,
        ),
        ReplayItem(
            3,
            "session",
            ReplayEventType.TRADE,
            "BTCUSDT",
            NOW + timedelta(milliseconds=150),
            150_000_000,
            trade_event(symbol="BTCUSDT", monotonic_ns=150_000_000),
        ),
        ReplayItem(
            4,
            "session",
            ReplayEventType.TRUSTED_BOOK,
            "BTCUSDT",
            NOW + timedelta(milliseconds=200),
            200_000_000,
            trusted_book(symbol="BTCUSDT", monotonic_ns=200_000_000),
        ),
        ReplayItem(
            5,
            "session",
            ReplayEventType.FEATURE_TICK,
            "BTCUSDT",
            NOW + timedelta(milliseconds=200),
            200_000_000,
            None,
        ),
    )


def run_replay(mode: ReplayMode):  # type: ignore[no-untyped-def]
    clock = ReplayClock()
    engine = FeatureEngine(
        "BTCUSDT",
        monotonic_clock_ns=clock.monotonic_ns,
        wall_clock=clock.wall_time,
    )
    replay = ReplayEngine(replay_events(), clock, engine, mode=mode)
    if mode is ReplayMode.STEP:
        while replay.step():
            pass
    return replay.run()


def test_replay_is_repeatable_and_speed_mode_independent() -> None:
    first = run_replay(ReplayMode.FAST)
    second = run_replay(ReplayMode.FAST)
    stepped = run_replay(ReplayMode.STEP)

    assert first == second == stepped
    assert len(first.features) == 2
    assert first.features[1].trade_flow_window(1_000).trade_event_count == 1


def test_replay_signals_trades_and_metrics_are_repeatable() -> None:
    costs = TransactionCostModel(
        maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("2"),
        entry_slippage_bps=Decimal("0.5"),
        exit_slippage_bps=Decimal("0.5"),
    )

    def execute(mode: ReplayMode):  # type: ignore[no-untyped-def]
        features = run_replay(mode).features
        return evaluate_strategy(
            features,
            BookImbalanceStrategy(threshold=0.1),
            costs,
            holding_period_ms=100,
            fixed_notional=Decimal("1000"),
        )

    first = execute(ReplayMode.FAST)
    second = execute(ReplayMode.FAST)
    stepped = execute(ReplayMode.STEP)

    assert first.signals == second.signals == stepped.signals
    assert first.trades == second.trades == stepped.trades
    assert first.metrics == second.metrics == stepped.metrics


def test_replay_preserves_exact_capture_order_for_consumers() -> None:
    observed: list[int] = []
    clock = ReplayClock()
    engine = FeatureEngine(
        "BTCUSDT", monotonic_clock_ns=clock.monotonic_ns, wall_clock=clock.wall_time
    )
    ReplayEngine(
        replay_events(), clock, engine, consumers=(lambda item: observed.append(item.capture_seq),)
    ).run()

    assert observed == [0, 1, 2, 3, 4, 5]


def test_feature_replay_equivalence_uses_float_tolerance_only() -> None:
    expected = run_replay(ReplayMode.FAST).features[0]
    events = list(replay_events())
    events[2] = replace(events[2], payload=expected)
    clock = ReplayClock()
    engine = FeatureEngine(
        "BTCUSDT", monotonic_clock_ns=clock.monotonic_ns, wall_clock=clock.wall_time
    )
    result = ReplayEngine(tuple(events), clock, engine).run()

    assert len(result.recorded_features) == 1
    assert feature_equivalence_errors(result.features[0], result.recorded_features[0]) == ()


def test_replay_rejects_duplicate_sequence_and_negative_monotonic_delta() -> None:
    events = list(replay_events())
    duplicate = replace(events[1], capture_seq=0)
    clock = ReplayClock()
    engine = FeatureEngine("BTCUSDT")
    with pytest.raises(ReplayIntegrityError, match="capture_seq"):
        ReplayEngine((events[0], duplicate), clock, engine)

    inversion = replace(events[1], monotonic_ns=-1)
    with pytest.raises(ReplayIntegrityError, match="negative monotonic"):
        ReplayEngine((events[0], inversion), ReplayClock(), FeatureEngine("BTCUSDT"))
