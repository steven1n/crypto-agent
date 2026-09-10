import math
import random
from dataclasses import replace
from datetime import timedelta

import pytest

from research.batch import HORIZONS, BatchConfiguration, segment_diagnostics
from research.dataset import join_features_and_labels
from research.labels import LabelGenerator
from research.robustness import (
    COST_GRID_BPS,
    CorrelationMoments,
    autocorrelation,
    block_bootstrap,
    cost_sensitivity,
    effective_sample_size,
    non_overlapping_indices,
    session_stability,
    volatility_boundaries,
    volatility_regime,
)
from research.splits import chronological_split
from tests.helpers import NOW, feature_snapshot


@pytest.mark.parametrize("horizon", [250, 500, 1000, 2000, 5000, 10000])
def test_nonoverlap_spacing(horizon):
    times = tuple(i * 100_000_000 for i in range(200))
    indices = non_overlapping_indices(times, horizon)
    assert all(
        times[b] - times[a] >= horizon * 1_000_000
        for a, b in zip(indices, indices[1:], strict=False)
    )
    assert len(indices) <= math.ceil(20000 / horizon)


def test_nonoverlap_rejects_disordered_or_duplicate_timestamps():
    with pytest.raises(ValueError, match="increasing"):
        non_overlapping_indices((1, 1), 1000)


def test_time_autocorrelation_and_gap_handling():
    times = tuple(i * 100_000_000 for i in range(100))
    assert autocorrelation(times, tuple(float(i) for i in range(100)), 1000) == pytest.approx(1)
    assert autocorrelation((0, 10_000_000_000), (1.0, 2.0), 100) is None
    assert autocorrelation(times, (1.0,) * 100, 100) is None


def test_effective_count_is_approximate_bounded_and_detects_dependence():
    rng = random.Random(7)
    iid = tuple(rng.gauss(0, 1) for _ in range(1000))
    assert 300 < effective_sample_size(iid) <= 1000
    assert 1 <= effective_sample_size(tuple(float(i) for i in range(1000))) < 30
    assert effective_sample_size((1.0,) * 1000) == 1
    assert effective_sample_size(()) == 0


def test_time_block_bootstrap_is_deterministic_and_not_iid_resampling():
    timestamps = tuple(i * 100_000_000 for i in range(1000))
    x = tuple(1.0 if i % 2 else -1.0 for i in range(1000))
    y = tuple((1 if i % 2 else -1) * (1 + i // 100) / 10000 for i in range(1000))
    first = block_bootstrap(timestamps, x, y, replications=100, seed=42, difference=True)
    assert first == block_bootstrap(timestamps, x, y, replications=100, seed=42, difference=True)
    assert first.block_count == 10 and first.usable_replications == 100
    assert first.low_bps < first.estimate_bps < first.high_bps
    assert first != block_bootstrap(timestamps, x, y, replications=100, seed=43, difference=True)
    one_block = block_bootstrap(timestamps[:5], x[:5], y[:5])
    assert one_block.low_bps is one_block.high_bps is None


def test_cost_grid_is_hypothetical_and_break_even_is_explicit():
    result = cost_sensitivity(0.4)
    assert tuple(row["round_trip_cost_bps"] for row in result) == COST_GRID_BPS
    assert result[1]["net_expected_bps"] == pytest.approx(-0.1)
    assert cost_sensitivity(0.4, (11.0,))[0]["edge_cost_ratio"] == pytest.approx(0.4 / 11)
    assert all(row["break_even_cost_bps"] == 0.4 for row in result)
    assert cost_sensitivity(None)[0]["net_expected_bps"] is None
    with pytest.raises(ValueError):
        cost_sensitivity(1, (-1.0,))


def test_volatility_buckets_use_supplied_train_cutpoints_and_stability_retains_signs():
    cuts = volatility_boundaries((1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0))
    assert cuts == (3.0, 5.0)
    assert [volatility_regime(v, cuts) for v in (1.0, 4.0, 100.0)] == ["LOW", "MEDIUM", "HIGH"]
    result = session_stability((0.08, 0.03, -0.01, None))
    assert result["median"] == 0.03
    assert result["fraction_same_sign_as_median"] == pytest.approx(2 / 3)
    assert result["usable_sessions"] == 3


def test_mergeable_correlation_matches_whole_session():
    whole, a, b = CorrelationMoments(), CorrelationMoments(), CorrelationMoments()
    for i in range(100):
        x, y = float(i), math.sin(i) + 0.1 * i
        whole.add(x, y)
        (a if i < 50 else b).add(x, y)
    a.merge(b)
    assert a.n == 100 and a.pearson() == pytest.approx(whole.pearson(), abs=1e-12)


def test_labels_do_not_bridge_invalid_ticks_or_long_disconnect_gap():
    features = tuple(feature_snapshot(monotonic_ns=i * 100_000_000) for i in range(20))
    invalid = features[:5] + (replace(features[5], feature_valid=False),) + features[6:]
    assert LabelGenerator().generate(invalid)[0].future_return_1s is None
    gap = (features[0], feature_snapshot(monotonic_ns=10_000_000_000))
    assert LabelGenerator().generate(gap)[0].future_return_250ms is None


def test_split_purges_actual_future_observation_when_irregular_cadence_overshoots():
    times = (0, 100, 200, 300, 400, 1000, 1100, 1200, 2000, 2100)
    features = tuple(feature_snapshot(monotonic_ns=i * 1_000_000) for i in times)
    rows = join_features_and_labels(features, LabelGenerator().generate(features))
    split = chronological_split(
        rows, train_fraction=0.5, validation_fraction=0.3, max_label_horizon_ms=150
    )
    assert split.train[-1].feature.monotonic_ns == 200_000_000
    assert split.validation[-1].feature.monotonic_ns == 1_000_000_000


def test_hour_context_retains_future_labels_and_all_fixed_baselines_and_horizons():
    features = tuple(
        replace(
            feature_snapshot(monotonic_ns=i * 100_000_000),
            created_at=NOW + timedelta(hours=1, seconds=-2 + i / 10),
        )
        for i in range(140)
    )
    report, rows = segment_diagnostics(features, NOW, BatchConfiguration(bootstrap_replications=5))
    assert len(rows) == 20
    assert report["relationships"]["imbalance_l5"]["1000"]["raw_sample_count"] == 20
    assert set(report["baseline_edge_vs_cost"]) == {
        "microprice_momentum",
        "book_imbalance",
        "trade_flow",
        "combined_baseline",
    }
    for baseline in report["baseline_edge_vs_cost"].values():
        assert set(baseline["horizons"]) == {str(h) for h in HORIZONS}
    assert (
        report["baseline_edge_vs_cost"]["book_imbalance"]["unchanged_parameters"]["threshold"]
        == 0.6
    )


def test_bootstrap_configuration_rejects_blocks_shorter_than_maximum_label():
    with pytest.raises(ValueError, match="10s"):
        BatchConfiguration(block_length_ms=1000)


def test_labels_do_not_cross_short_resync_without_invalid_feature_ticks():
    features = tuple(feature_snapshot(monotonic_ns=i * 100_000_000) for i in range(20))
    labels = LabelGenerator().generate(features, integrity_boundaries_ns=(550_000_000,))
    assert labels[0].future_return_1s is None
    assert labels[0].future_return_250ms is not None
    assert labels[6].future_return_1s is not None
