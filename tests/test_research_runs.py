import json
from decimal import Decimal
from pathlib import Path

from execution.costs import TransactionCostModel
from research.dataset import join_features_and_labels
from research.evaluation import evaluate_strategy
from research.labels import LabelGenerator
from research.runs import ResearchRunWriter, reproducibility_fingerprint
from research.splits import chronological_split
from strategies.baselines import BookImbalanceStrategy
from tests.helpers import feature_snapshot


def test_fingerprint_excludes_creation_time_and_is_order_stable() -> None:
    first = reproducibility_fingerprint(
        dataset_manifest={"b": 2, "a": 1},
        strategy_configuration={"threshold": 0.6},
        transaction_cost_configuration={"fee": 2},
        replay_configuration={"mode": "FAST"},
    )
    second = reproducibility_fingerprint(
        dataset_manifest={"a": 1, "b": 2},
        strategy_configuration={"threshold": 0.6},
        transaction_cost_configuration={"fee": 2},
        replay_configuration={"mode": "FAST"},
    )
    assert first == second


def test_research_run_writes_machine_readable_artifacts(tmp_path: Path) -> None:
    features = tuple(
        feature_snapshot(
            monotonic_ns=index * 1_000_000_000,
            imbalance_l5=0.8 if index % 2 == 0 else 0,
        )
        for index in range(100)
    )
    labels = LabelGenerator().generate(features)
    split = chronological_split(join_features_and_labels(features, labels))
    costs = TransactionCostModel(
        maker_fee_bps=Decimal(1),
        taker_fee_bps=Decimal(2),
        entry_slippage_bps=Decimal(1),
        exit_slippage_bps=Decimal(1),
        cross_spread=False,
    )
    evaluation = evaluate_strategy(
        features,
        BookImbalanceStrategy(),
        costs,
        holding_period_ms=1_000,
        fixed_notional=Decimal(1000),
    )

    artifacts = ResearchRunWriter(tmp_path).write(
        evaluation,
        dataset_ids=("dataset-a",),
        dataset_time_range=("start", "end"),
        strategy_configuration={"threshold": 0.6},
        transaction_cost_configuration={"taker_fee_bps": 2},
        replay_configuration={"mode": "FAST"},
        split=split,
        data_quality_status={"valid": True},
        dataset_manifest={"dataset_version": 1},
        run_id="run-a",
    )

    assert artifacts.config_path.is_file()
    assert artifacts.metrics_path.is_file()
    assert artifacts.trades_path.is_file()
    assert artifacts.signals_path.is_file()
    assert artifacts.summary_path.is_file()
    config = json.loads(artifacts.config_path.read_text(encoding="utf-8"))
    assert config["reproducibility_fingerprint"] == artifacts.fingerprint
    assert config["split_boundaries"][0]["name"] == "TRAIN"
