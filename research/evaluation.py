"""Chronological baseline evaluation using offline-only shadow execution."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from execution.costs import TransactionCostModel
from execution.shadow import ShadowExecutionSimulator, ShadowTrade
from features.models import FeatureSnapshot
from research.metrics import StrategyMetrics, calculate_strategy_metrics
from signals.models import Signal
from strategies.base import Strategy

FIXED_TIME_STOP_HORIZONS_MS = (250, 500, 1_000, 2_000, 5_000)


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    strategy_name: str
    holding_period_ms: int
    signals: tuple[Signal, ...]
    trades: tuple[ShadowTrade, ...]
    metrics: StrategyMetrics


def evaluate_strategy(
    features: tuple[FeatureSnapshot, ...],
    strategy: Strategy,
    cost_model: TransactionCostModel,
    *,
    holding_period_ms: int,
    fixed_notional: Decimal,
    force_close_at_end: bool = False,
    maker_entry_filled: bool = False,
) -> EvaluationResult:
    simulator = ShadowExecutionSimulator(
        cost_model,
        fixed_notional=fixed_notional,
        holding_period_ms=holding_period_ms,
    )
    signals: list[Signal] = []
    for feature in features:
        simulator.observe(feature)
        if simulator.has_position:
            continue
        signal = strategy.evaluate(feature)
        if signal is None:
            continue
        signals.append(signal)
        simulator.enter(signal, feature, maker_filled=maker_entry_filled)
    if force_close_at_end and simulator.has_position and features:
        simulator.close_end_of_data(features[-1])
    trades = simulator.trades
    return EvaluationResult(
        strategy_name=strategy.name,
        holding_period_ms=holding_period_ms,
        signals=tuple(signals),
        trades=trades,
        metrics=calculate_strategy_metrics(trades, signal_count=len(signals)),
    )


def evaluate_fixed_horizons(
    features: tuple[FeatureSnapshot, ...],
    strategy: Strategy,
    cost_model: TransactionCostModel,
    *,
    fixed_notional: Decimal,
    horizons_ms: tuple[int, ...] = FIXED_TIME_STOP_HORIZONS_MS,
    force_close_at_end: bool = False,
    maker_entry_filled: bool = False,
) -> tuple[EvaluationResult, ...]:
    """Evaluate a fixed, declared horizon set without parameter search."""
    if not horizons_ms or any(horizon <= 0 for horizon in horizons_ms):
        raise ValueError("at least one positive holding horizon is required")
    return tuple(
        evaluate_strategy(
            features,
            strategy,
            cost_model,
            holding_period_ms=horizon,
            fixed_notional=fixed_notional,
            force_close_at_end=force_close_at_end,
            maker_entry_filled=maker_entry_filled,
        )
        for horizon in horizons_ms
    )
