from signals.models import SignalSide
from strategies.baselines import (
    BookImbalanceStrategy,
    CombinedBaselineStrategy,
    MicropriceMomentumStrategy,
    TradeFlowStrategy,
)
from tests.helpers import feature_snapshot


def test_four_baselines_emit_only_explicit_symmetric_signals() -> None:
    long_feature = feature_snapshot(
        monotonic_ns=0,
        microprice_offset_bps=0.06,
        imbalance_l1=0.7,
        imbalance_l5=0.7,
        trade_imbalance_1s=0.7,
    )
    short_feature = feature_snapshot(
        monotonic_ns=1,
        microprice_offset_bps=-0.06,
        imbalance_l1=-0.7,
        imbalance_l5=-0.7,
        trade_imbalance_1s=-0.7,
    )
    strategies = (
        MicropriceMomentumStrategy(),
        BookImbalanceStrategy(),
        TradeFlowStrategy(),
        CombinedBaselineStrategy(),
    )

    for strategy in strategies:
        long_signal = strategy.evaluate(long_feature)
        short_signal = strategy.evaluate(short_feature)
        assert long_signal is not None and long_signal.side is SignalSide.LONG
        assert short_signal is not None and short_signal.side is SignalSide.SHORT
        assert long_signal.strategy_name == strategy.name
        assert long_signal.evidence


def test_invalid_feature_never_emits_signal() -> None:
    feature = feature_snapshot(monotonic_ns=0, imbalance_l5=1, valid=False)
    assert BookImbalanceStrategy().evaluate(feature) is None
