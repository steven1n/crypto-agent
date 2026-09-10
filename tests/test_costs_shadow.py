from decimal import Decimal

import pytest

from execution.costs import ExecutionAssumption, TransactionCostModel
from execution.shadow import ExitReason, ShadowExecutionSimulator
from signals.models import Signal, SignalSide
from tests.helpers import feature_snapshot


def signal(side: SignalSide, monotonic_ns: int = 0) -> Signal:
    feature = feature_snapshot(monotonic_ns=monotonic_ns)
    return Signal(
        symbol=feature.symbol,
        side=side,
        strategy_name="test",
        created_time=feature.created_at,
        monotonic_ns=monotonic_ns,
        score=1,
        evidence=(),
        expected_horizon_ms=1_000,
    )


def cost_model(
    assumption: ExecutionAssumption = ExecutionAssumption.TAKER_TAKER,
) -> TransactionCostModel:
    return TransactionCostModel(
        maker_fee_bps=Decimal("0.5"),
        taker_fee_bps=Decimal("1"),
        entry_slippage_bps=Decimal("2"),
        exit_slippage_bps=Decimal("2"),
        assumption=assumption,
        cross_spread=False,
    )


@pytest.mark.parametrize(
    ("side", "exit_mid"),
    [(SignalSide.LONG, Decimal("101")), (SignalSide.SHORT, Decimal("99"))],
)
def test_long_short_gross_pnl_symmetry_and_explicit_costs(
    side: SignalSide, exit_mid: Decimal
) -> None:
    entry = feature_snapshot(monotonic_ns=0, mid=Decimal("100"))
    exit_feature = feature_snapshot(monotonic_ns=1_000_000_000, mid=exit_mid)
    simulator = ShadowExecutionSimulator(
        cost_model(), fixed_notional=Decimal("1000"), holding_period_ms=1_000
    )
    assert simulator.enter(signal(side), entry)

    trade = simulator.observe(exit_feature)

    assert trade is not None
    assert trade.gross_pnl == Decimal("10")
    assert trade.fees > 0
    assert trade.slippage_cost > 0
    assert trade.net_pnl == trade.gross_pnl - trade.fees - trade.slippage_cost
    assert trade.exit_reason is ExitReason.TIME_STOP
    assert trade.holding_time_ms == 1_000


def test_maker_taker_does_not_pretend_entry_filled() -> None:
    feature = feature_snapshot(monotonic_ns=0)
    simulator = ShadowExecutionSimulator(
        cost_model(ExecutionAssumption.MAKER_TAKER),
        fixed_notional=Decimal("1000"),
        holding_period_ms=1_000,
    )

    assert not simulator.enter(signal(SignalSide.LONG), feature)
    assert simulator.enter(signal(SignalSide.LONG), feature, maker_filled=True)


def test_spread_crossing_is_added_to_configured_slippage() -> None:
    model = TransactionCostModel(
        maker_fee_bps=Decimal(0),
        taker_fee_bps=Decimal(0),
        entry_slippage_bps=Decimal(0),
        exit_slippage_bps=Decimal(0),
        cross_spread=True,
    )
    entry, exit_price = model.executable_prices(
        side=SignalSide.LONG,
        entry_mid=Decimal("100"),
        exit_mid=Decimal("100"),
        entry_spread_bps=2,
        exit_spread_bps=2,
    )

    assert entry == Decimal("100.01")
    assert exit_price == Decimal("99.99")


def test_maker_taker_does_not_cross_entry_spread() -> None:
    model = TransactionCostModel(
        maker_fee_bps=Decimal(0),
        taker_fee_bps=Decimal(0),
        entry_slippage_bps=Decimal(0),
        exit_slippage_bps=Decimal(0),
        assumption=ExecutionAssumption.MAKER_TAKER,
        cross_spread=True,
    )

    entry, exit_price = model.executable_prices(
        side=SignalSide.LONG,
        entry_mid=Decimal("100"),
        exit_mid=Decimal("100"),
        entry_spread_bps=2,
        exit_spread_bps=2,
    )

    assert entry == Decimal("100")
    assert exit_price == Decimal("99.99")
