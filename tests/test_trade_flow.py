from decimal import Decimal

from features.trade_flow import TradeFlowTracker
from market.events import AggressorSide
from tests.helpers import trade_event


def test_rolling_windows_have_deterministic_inclusive_boundaries() -> None:
    tracker = TradeFlowTracker()
    tracker.add(trade_event(monotonic_ns=0, side=AggressorSide.BUY, quantity=Decimal("1")))
    tracker.add(
        trade_event(
            monotonic_ns=500_000_000,
            side=AggressorSide.SELL,
            quantity=Decimal("2"),
        )
    )
    tracker.add(
        trade_event(
            monotonic_ns=2_000_000_000,
            side=AggressorSide.BUY,
            quantity=Decimal("3"),
        )
    )

    one, five, ten = tracker.snapshot(now_ns=2_000_000_000, available=True)
    assert one.total_volume == Decimal("3")
    assert one.trade_event_count == 1
    assert five.total_volume == Decimal("6")
    assert ten.total_volume == Decimal("6")

    at_boundary = tracker.snapshot(now_ns=10_000_000_000, available=True)[2]
    assert at_boundary.total_volume == Decimal("6")
    after_boundary = tracker.snapshot(now_ns=10_000_000_001, available=True)[2]
    assert after_boundary.total_volume == Decimal("5")


def test_exact_one_five_ten_second_contents_at_eleven_seconds() -> None:
    tracker = TradeFlowTracker()
    observations = (
        (0, AggressorSide.BUY, "1"),
        (500_000_000, AggressorSide.SELL, "2"),
        (2_000_000_000, AggressorSide.BUY, "3"),
        (6_000_000_000, AggressorSide.SELL, "4"),
        (11_000_000_000, AggressorSide.BUY, "5"),
    )
    for event_id, (timestamp, side, quantity) in enumerate(observations):
        tracker.add(
            trade_event(
                monotonic_ns=timestamp,
                side=side,
                quantity=Decimal(quantity),
                aggregate_trade_id=event_id,
            )
        )

    one, five, ten = tracker.snapshot(now_ns=11_000_000_000, available=True)

    assert (one.aggressive_buy_volume, one.aggressive_sell_volume) == (
        Decimal("5"),
        Decimal("0"),
    )
    assert (five.aggressive_buy_volume, five.aggressive_sell_volume) == (
        Decimal("5"),
        Decimal("4"),
    )
    assert (ten.aggressive_buy_volume, ten.aggressive_sell_volume) == (
        Decimal("8"),
        Decimal("4"),
    )
    assert (
        one.trade_event_count,
        five.trade_event_count,
        ten.trade_event_count,
    ) == (1, 2, 3)
    assert tracker.history_size == 3


def test_trade_flow_arithmetic_and_vwap_are_exact() -> None:
    tracker = TradeFlowTracker()
    tracker.add(
        trade_event(
            monotonic_ns=1,
            side=AggressorSide.BUY,
            price=Decimal("100"),
            quantity=Decimal("2"),
        )
    )
    tracker.add(
        trade_event(
            monotonic_ns=2,
            side=AggressorSide.SELL,
            price=Decimal("110"),
            quantity=Decimal("1"),
        )
    )
    flow = tracker.snapshot(now_ns=2, available=True)[0]

    assert flow.aggressive_buy_volume == Decimal("2")
    assert flow.aggressive_sell_volume == Decimal("1")
    assert flow.signed_volume == Decimal("1")
    assert flow.trade_imbalance == 1 / 3
    assert flow.buy_ratio == 2 / 3
    assert flow.volume_weighted_price == Decimal("103.3333333333333333333333333")


def test_healthy_empty_window_is_zero_but_unavailable_window_is_none() -> None:
    tracker = TradeFlowTracker()
    available = tracker.snapshot(now_ns=0, available=True)[0]
    unavailable = tracker.snapshot(now_ns=0, available=False)[0]

    assert available.trade_event_count == 0
    assert available.total_volume == Decimal(0)
    assert available.trade_imbalance is None
    assert unavailable.trade_event_count is None
    assert unavailable.total_volume is None
