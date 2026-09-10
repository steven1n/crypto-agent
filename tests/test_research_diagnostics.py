from dataclasses import replace
from decimal import Decimal

import pytest

from execution.costs import TransactionCostModel
from research.dataset import ResearchRow, join_features_and_labels
from research.diagnostics import (
    analyze_feature,
    data_quality_report,
    directional_hit_rates,
)
from research.evaluation import (
    FIXED_TIME_STOP_HORIZONS_MS,
    evaluate_fixed_horizons,
    evaluate_strategy,
)
from research.labels import LabelGenerator
from strategies.baselines import BookImbalanceStrategy
from tests.helpers import feature_snapshot


def research_rows() -> tuple[ResearchRow, ...]:
    features = tuple(
        feature_snapshot(
            monotonic_ns=index * 100_000_000,
            imbalance_l5=(index - 10) / 10,
        )
        for index in range(21)
    )
    generated = LabelGenerator().generate(features)
    labels = tuple(
        replace(label, future_return_250ms=feature.book.at_depth(5).imbalance * 0.001)
        for feature, label in zip(features, generated, strict=True)
    )
    return join_features_and_labels(features, labels)


def test_predictive_correlations_bins_and_directional_hit_rates() -> None:
    rows = research_rows()

    diagnostic = analyze_feature(
        rows,
        feature_name="imbalance_l5",
        label_name="future_return_250ms",
        bin_count=5,
    )
    hit_rates = directional_hit_rates(
        rows,
        feature_name="imbalance_l5",
        label_name="future_return_250ms",
        thresholds=(0.2, 0.6),
    )

    assert diagnostic.sample_count == 21
    assert diagnostic.pearson_correlation == pytest.approx(1)
    assert diagnostic.spearman_correlation == pytest.approx(1)
    assert len(diagnostic.bins) == 5
    assert all(item.hit_rate == 1 for item in hit_rates if item.sample_count)


def test_data_quality_invariants() -> None:
    features = tuple(row.feature for row in research_rows())

    report = data_quality_report(features)

    assert report.valid
    assert report.rows == 21
    assert not report.invariant_errors


def test_strategy_evaluation_reports_gross_net_and_break_even_cost() -> None:
    features = (
        feature_snapshot(monotonic_ns=0, mid=Decimal("100"), imbalance_l5=0.8),
        feature_snapshot(monotonic_ns=1_000_000_000, mid=Decimal("101"), imbalance_l5=0),
        feature_snapshot(monotonic_ns=2_000_000_000, mid=Decimal("101"), imbalance_l5=-0.8),
        feature_snapshot(monotonic_ns=3_000_000_000, mid=Decimal("103"), imbalance_l5=0),
    )
    costs = TransactionCostModel(
        maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("2"),
        entry_slippage_bps=Decimal("1"),
        exit_slippage_bps=Decimal("1"),
        cross_spread=False,
    )

    result = evaluate_strategy(
        features,
        BookImbalanceStrategy(threshold=0.6),
        costs,
        holding_period_ms=1_000,
        fixed_notional=Decimal("1000"),
    )

    assert result.metrics.trade_count == 2
    assert result.metrics.long_count == 1
    assert result.metrics.short_count == 1
    assert result.metrics.gross_pnl < 0
    assert result.metrics.net_pnl < result.metrics.gross_pnl
    assert result.metrics.fees > 0
    assert result.metrics.slippage_cost > 0
    assert result.metrics.break_even_round_trip_cost_bps == pytest.approx(
        sum(trade.gross_edge_bps for trade in result.trades) / 2
    )


def test_predefined_fixed_horizons_are_evaluated_without_search() -> None:
    features = tuple(
        feature_snapshot(
            monotonic_ns=index * 250_000_000,
            mid=Decimal("100"),
            imbalance_l5=0.8 if index == 0 else 0,
        )
        for index in range(21)
    )
    costs = TransactionCostModel(
        maker_fee_bps=Decimal(0),
        taker_fee_bps=Decimal(0),
        entry_slippage_bps=Decimal(0),
        exit_slippage_bps=Decimal(0),
        cross_spread=False,
    )

    results = evaluate_fixed_horizons(
        features,
        BookImbalanceStrategy(threshold=0.6),
        costs,
        fixed_notional=Decimal("1000"),
    )

    assert tuple(result.holding_period_ms for result in results) == FIXED_TIME_STOP_HORIZONS_MS
