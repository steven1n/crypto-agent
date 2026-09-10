"""Machine-readable, reproducibly fingerprinted research-run artifacts."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from recorder.catalog import git_revision, stable_fingerprint
from research.evaluation import EvaluationResult
from research.splits import WalkForwardSplit


def reproducibility_fingerprint(
    *,
    dataset_manifest: object,
    strategy_configuration: object,
    transaction_cost_configuration: object,
    replay_configuration: object,
) -> str:
    return stable_fingerprint(
        {
            "dataset_manifest": dataset_manifest,
            "strategy": strategy_configuration,
            "transaction_costs": transaction_cost_configuration,
            "replay": replay_configuration,
        }
    )


@dataclass(frozen=True, slots=True)
class ResearchRunArtifacts:
    run_id: str
    directory: Path
    fingerprint: str
    config_path: Path
    metrics_path: Path
    trades_path: Path
    signals_path: Path
    summary_path: Path


class ResearchRunWriter:
    def __init__(self, data_directory: Path) -> None:
        self._runs_directory = data_directory / "research" / "runs"

    def write(
        self,
        evaluation: EvaluationResult,
        *,
        dataset_ids: tuple[str, ...],
        dataset_time_range: tuple[str, str],
        strategy_configuration: object,
        transaction_cost_configuration: object,
        replay_configuration: object,
        split: WalkForwardSplit,
        data_quality_status: object,
        dataset_manifest: object,
        run_id: str | None = None,
    ) -> ResearchRunArtifacts:
        actual_run_id = run_id or uuid.uuid4().hex
        directory = self._runs_directory / actual_run_id
        if directory.exists():
            raise FileExistsError(f"research run already exists: {directory}")
        directory.mkdir(parents=True)
        created_at = datetime.now(UTC).isoformat()
        fingerprint = reproducibility_fingerprint(
            dataset_manifest=dataset_manifest,
            strategy_configuration=strategy_configuration,
            transaction_cost_configuration=transaction_cost_configuration,
            replay_configuration=replay_configuration,
        )
        config = {
            "run_id": actual_run_id,
            "created_at": created_at,
            "reproducibility_fingerprint": fingerprint,
            "dataset_ids": dataset_ids,
            "dataset_time_range": dataset_time_range,
            "software_revision": git_revision(),
            "strategy_name": evaluation.strategy_name,
            "strategy_configuration": strategy_configuration,
            "transaction_cost_configuration": transaction_cost_configuration,
            "replay_configuration": replay_configuration,
            "split_boundaries": [asdict(item) for item in split.boundaries],
            "purge_window_ns": split.purge_window_ns,
            "data_quality_status": data_quality_status,
        }
        metrics = asdict(evaluation.metrics)
        config_path = directory / "config.json"
        metrics_path = directory / "metrics.json"
        trades_path = directory / "trades.parquet"
        signals_path = directory / "signals.parquet"
        summary_path = directory / "summary.md"
        self._write_json_atomic(config_path, config)
        self._write_json_atomic(metrics_path, metrics)
        self._write_parquet_atomic(
            trades_path,
            [self._jsonable(asdict(trade)) for trade in evaluation.trades],
        )
        self._write_parquet_atomic(
            signals_path,
            [
                {
                    **self._jsonable(asdict(signal)),
                    "evidence": json.dumps(
                        self._jsonable([asdict(item) for item in signal.evidence]),
                        sort_keys=True,
                    ),
                }
                for signal in evaluation.signals
            ],
        )
        summary_path.write_text(
            "\n".join(
                (
                    f"# Research run {actual_run_id}",
                    "",
                    f"- Fingerprint: `{fingerprint}`",
                    f"- Strategy: `{evaluation.strategy_name}`",
                    f"- Holding period: {evaluation.holding_period_ms} ms",
                    f"- Signals: {evaluation.metrics.signal_count}",
                    f"- Trades: {evaluation.metrics.trade_count}",
                    f"- Gross PnL: {evaluation.metrics.gross_pnl}",
                    f"- Net PnL: {evaluation.metrics.net_pnl}",
                    "",
                    "No profitability or statistical-significance claim is implied.",
                )
            ),
            encoding="utf-8",
        )
        return ResearchRunArtifacts(
            run_id=actual_run_id,
            directory=directory,
            fingerprint=fingerprint,
            config_path=config_path,
            metrics_path=metrics_path,
            trades_path=trades_path,
            signals_path=signals_path,
            summary_path=summary_path,
        )

    @classmethod
    def _jsonable(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: cls._jsonable(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._jsonable(item) for item in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    @classmethod
    def _write_json_atomic(cls, path: Path, value: object) -> None:
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(
            json.dumps(cls._jsonable(value), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, path)

    @staticmethod
    def _write_parquet_atomic(path: Path, rows: list[dict[str, Any]]) -> None:
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        table = (
            pa.Table.from_pylist(rows) if rows else pa.table({"empty": pa.array([], pa.bool_())})
        )
        pq.write_table(table, temporary, compression="zstd")
        os.replace(temporary, path)
