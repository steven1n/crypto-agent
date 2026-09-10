import ast
import math
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from research.dataset import join_features_and_labels
from research.labels import LabelGenerator
from research.splits import chronological_split
from tests.helpers import feature_snapshot


def test_future_label_uses_first_observation_at_or_after_boundary() -> None:
    features = (
        feature_snapshot(monotonic_ns=0, mid=Decimal("100")),
        feature_snapshot(monotonic_ns=400_000_000, mid=Decimal("104")),
        feature_snapshot(monotonic_ns=600_000_000, mid=Decimal("106")),
    )

    label = LabelGenerator().generate(features)[0]

    assert label.future_return_500ms == pytest.approx(math.log(1.06))
    assert label.future_return_250ms == pytest.approx(math.log(1.04))
    assert LabelGenerator().generate(features)[-1].future_return_10s is None


def test_future_excursions_preserve_favorable_and_adverse_path() -> None:
    features = (
        feature_snapshot(monotonic_ns=0, mid=Decimal("100")),
        feature_snapshot(monotonic_ns=400_000_000, mid=Decimal("102")),
        feature_snapshot(monotonic_ns=700_000_000, mid=Decimal("99")),
        feature_snapshot(monotonic_ns=1_000_000_000, mid=Decimal("101")),
    )
    label = LabelGenerator().generate(features)[0]

    assert label.future_max_up_bps_1s == pytest.approx(math.log(1.02) * 10_000)
    assert label.future_max_down_bps_1s == pytest.approx(math.log(0.99) * 10_000)


def test_walk_forward_split_purges_forward_label_boundary_crossing() -> None:
    features = tuple(feature_snapshot(monotonic_ns=index * 1_000_000_000) for index in range(100))
    rows = join_features_and_labels(features, LabelGenerator().generate(features))

    split = chronological_split(rows, max_label_horizon_ms=10_000)

    assert len(split.train) == 50
    assert len(split.validation) == 10
    assert len(split.test) == 20
    assert split.train[-1].feature.monotonic_ns + split.purge_window_ns < 60_000_000_000
    assert split.validation[-1].feature.monotonic_ns + split.purge_window_ns < 80_000_000_000


def test_strategy_modules_have_no_research_label_import() -> None:
    strategy_directory = Path(__file__).parents[1] / "strategies"
    imported_modules: set[str] = set()
    for path in strategy_directory.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.add(node.module)

    assert not any(name.startswith("research") for name in imported_modules)


def test_join_rejects_temporal_misalignment() -> None:
    feature = feature_snapshot(monotonic_ns=0)
    label = LabelGenerator().generate((feature, feature_snapshot(monotonic_ns=1)))[0]
    with pytest.raises(ValueError, match="alignment"):
        join_features_and_labels((replace(feature, monotonic_ns=2),), (label,))
