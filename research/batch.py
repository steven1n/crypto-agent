"""Reproducible streaming batch validation and descriptive robustness research."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean, median
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from features.engine import FeatureEngine
from features.models import FeatureSnapshot
from recorder.catalog import DatasetFileManifest, stable_fingerprint
from recorder.durability import CaptureSafetyError, file_sha256, write_json_atomic
from recorder.frozen import select_manifests, verify_frozen
from recorder.raw_models import LifecycleRecord
from replay.clock import ReplayClock
from replay.engine import ReplayEngine
from replay.features import feature_equivalence_errors
from replay.models import ReplayEventType, ReplayItem
from replay.offset import RecordedClockOffset
from replay.reconstruction import RawReconstructionValidator
from replay.streaming import iter_session
from research.dataset import ResearchRow, flatten_research_row, join_features_and_labels
from research.diagnostics import _pearson, analyze_feature, data_quality_report, feature_value
from research.labels import LabelGenerator
from research.latency import LatencyDiagnostics
from research.robustness import (
    AUTOCORRELATION_LAGS_MS,
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
from signals.models import SignalSide
from strategies.baselines import (
    BookImbalanceStrategy,
    CombinedBaselineStrategy,
    MicropriceMomentumStrategy,
    TradeFlowStrategy,
)

FEATURES = (
    "microprice_offset_bps",
    "imbalance_l1",
    "imbalance_l5",
    "imbalance_l10",
    "trade_imbalance_1s",
    "trade_imbalance_5s",
    "return_250ms",
    "return_1s",
    "rv_1s",
)
HORIZONS = {
    250: "future_return_250ms",
    500: "future_return_500ms",
    1_000: "future_return_1s",
    2_000: "future_return_2s",
    5_000: "future_return_5s",
    10_000: "future_return_10s",
}


@dataclass(frozen=True)
class BatchConfiguration:
    block_length_ms: int = 10_000
    bootstrap_replications: int = 200
    seed: int = 42
    cost_grid_bps: tuple[float, ...] = COST_GRID_BPS
    taker_taker_cost_bps: float = 11.0
    maker_taker_cost_bps: float = 8.0
    max_hour_features: int = 100_000

    def __post_init__(self) -> None:
        if self.block_length_ms < 10_000 or self.bootstrap_replications < 1:
            raise ValueError("blocks must cover maximum 10s label horizon; replications positive")
        if min(self.taker_taker_cost_bps, self.maker_taker_cost_bps) < 0:
            raise ValueError("hypothetical costs must be nonnegative")


def _hour(timestamp: datetime) -> datetime:
    return timestamp.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


@dataclass
class HourBuffer:
    """Source-ordered hour fragments, with a monotonic future-label suffix.

    A civil hour may recur. Never group noncontiguous runs or discard an earlier
    civil hour: doing so would remove future context for still-pending rows.
    """

    capacity: int
    rows: list[FeatureSnapshot] = field(default_factory=list)
    prefix_size: int = 0
    source_rows: int = 0
    emitted_rows: int = 0
    excluded_before_start: int = 0
    excluded_at_or_after_end: int = 0
    _last_ns: int | None = None

    def append(self, feature: FeatureSnapshot) -> None:
        if self._last_ns is not None and feature.monotonic_ns <= self._last_ns:
            raise CaptureSafetyError("analysis feature timestamps must strictly increase")
        if len(self.rows) >= self.capacity:
            raise CaptureSafetyError("hour feature buffer exceeds declared memory bound")
        if not self.rows or (
            self.prefix_size == len(self.rows)
            and _hour(feature.created_at) == _hour(self.rows[0].created_at)
        ):
            self.prefix_size += 1
        self.rows.append(feature)
        self._last_ns = feature.monotonic_ns
        self.source_rows += 1

    def ready(self) -> bool:
        return (
            bool(self.rows)
            and self.prefix_size < len(self.rows)
            and (
                self.rows[-1].monotonic_ns - self.rows[self.prefix_size - 1].monotonic_ns
                >= max(HORIZONS) * 1_000_000
            )
        )

    def finish(
        self,
        output: list[dict[str, Any]],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> None:
        """Validate exact identity/order before retiring only the emitted prefix."""
        prefix = self.rows[: self.prefix_size]
        selected = [
            f
            for f in prefix
            if (start is None or f.created_at >= start) and (end is None or f.created_at < end)
        ]
        expected = [(f.monotonic_ns, f.created_at) for f in selected]
        observed = [(row["monotonic_ns"], row["created_at"]) for row in output]
        if observed != expected:
            raise CaptureSafetyError("analysis row accounting: missing/duplicate/reordered row")
        self.emitted_rows += len(output)
        self.excluded_before_start += sum(
            start is not None and f.created_at < start for f in prefix
        )
        self.excluded_at_or_after_end += sum(
            end is not None and f.created_at >= end for f in prefix
        )
        del self.rows[: self.prefix_size]
        self.prefix_size = 0
        for feature in self.rows:
            if _hour(feature.created_at) != _hour(self.rows[0].created_at):
                break
            self.prefix_size += 1

    def accounting(self) -> dict[str, int]:
        excluded = self.excluded_before_start + self.excluded_at_or_after_end
        if self.rows or self.source_rows != self.emitted_rows + excluded:
            raise CaptureSafetyError("analysis session row accounting failed")
        return {
            "source_rows": self.source_rows,
            "eligible_source_rows": self.source_rows - excluded,
            "emitted_analysis_rows": self.emitted_rows,
            "intentionally_excluded_rows": excluded,
            "excluded_before_start": self.excluded_before_start,
            "excluded_at_or_after_end": self.excluded_at_or_after_end,
        }


def hour_analysis_rows(
    features: tuple[FeatureSnapshot, ...],
    target_hour: datetime,
    *,
    integrity_boundaries_ns: tuple[int, ...] = (),
    selection_end_ns: int | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> tuple[ResearchRow, ...]:
    """Label in source order using the full suffix, then select only this fragment."""
    # Civil range filtering can be noncontiguous after rollback. Retain invalid
    # ticks in label context, and do not bridge a deliberately excluded interval.
    excluded = tuple(
        f.monotonic_ns
        for f in features
        if (start is not None and f.created_at < start) or (end is not None and f.created_at >= end)
    )
    boundaries = (
        tuple(sorted((*integrity_boundaries_ns, *excluded)))
        if excluded
        else integrity_boundaries_ns
    )
    labels = LabelGenerator().generate(features, integrity_boundaries_ns=boundaries)
    return tuple(
        row
        for row in join_features_and_labels(features, labels)
        if _hour(row.feature.created_at) == target_hour
        and (selection_end_ns is None or row.feature.monotonic_ns <= selection_end_ns)
        and (start is None or row.feature.created_at >= start)
        and (end is None or row.feature.created_at < end)
    )


def segment_diagnostics(
    features: tuple[FeatureSnapshot, ...],
    target_hour: datetime,
    configuration: BatchConfiguration,
    *,
    integrity_boundaries_ns: tuple[int, ...] = (),
    selection_end_ns: int | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = hour_analysis_rows(
        features,
        target_hour,
        integrity_boundaries_ns=integrity_boundaries_ns,
        selection_end_ns=selection_end_ns,
        start=start,
        end=end,
    )
    quality = data_quality_report(tuple(row.feature for row in rows))
    if not quality.valid:
        raise CaptureSafetyError(f"feature quality failure: {quality.invariant_errors[:5]}")
    healthy = tuple(row for row in rows if row.feature.feature_valid)
    timestamps = tuple(row.feature.monotonic_ns for row in healthy)
    # Explicitly descriptive; formal evaluation must call volatility_boundaries
    # with TRAIN values and reuse those cutpoints for validation/test.
    rv = tuple(
        value for row in healthy if (value := feature_value(row.feature, "rv_1s")) is not None
    )
    cuts = volatility_boundaries(rv) if rv else (0.0, 0.0)
    relationships: dict[str, Any] = {}
    for name in FEATURES:
        relationships[name] = {}
        signal = tuple(feature_value(row.feature, name) for row in healthy)
        for horizon, label_name in HORIZONS.items():
            future = tuple(getattr(row.label, label_name) for row in healthy)
            diagnostic = analyze_feature(healthy, feature_name=name, label_name=label_name)
            indices = non_overlapping_indices(timestamps, horizon)
            pairs = [
                (signal[i], future[i])
                for i in indices
                if signal[i] is not None and future[i] is not None
            ]
            x = [float(a) for a, _ in pairs if a is not None]
            y = [float(b) for _, b in pairs if b is not None]
            n_eff = min(effective_sample_size(tuple(x)), effective_sample_size(tuple(y)))
            condition = [
                (row.feature.monotonic_ns, float(s), float(f))
                for row, s, f in zip(healthy, signal, future, strict=True)
                if s is not None and f is not None
            ]
            moments = CorrelationMoments()
            for _, signal_value, future_value in condition:
                moments.add(signal_value, future_value)
            positive = [f * 10_000 for _, s, f in condition if s >= 0.6]
            negative = [-f * 10_000 for _, s, f in condition if s <= -0.6]
            edge = fmean(positive + negative) if positive or negative else None
            # Threshold .6 only has semantic meaning for bounded imbalance signals.
            directional = "imbalance" in name
            regime_results = {}
            for regime in ("LOW", "MEDIUM", "HIGH"):
                selected = tuple(
                    row
                    for row in healthy
                    if volatility_regime(feature_value(row.feature, "rv_1s"), cuts) == regime
                )
                item = analyze_feature(selected, feature_name=name, label_name=label_name)
                regime_results[regime] = {
                    "n": item.sample_count,
                    "pearson": item.pearson_correlation,
                }
            result: dict[str, Any] = {
                "raw_sample_count": diagnostic.sample_count,
                "correlation_moments": asdict(moments),
                "all_snapshot_pearson": diagnostic.pearson_correlation,
                "all_snapshot_spearman": diagnostic.spearman_correlation,
                "non_overlapping_sample_count": len(pairs),
                "non_overlapping_pearson": _pearson(x, y),
                "approximate_effective_sample_count": n_eff,
                "effective_count_method": (
                    "minimum initial-positive ACF estimate for nonoverlapping x/y; "
                    "approximate, not inferential proof"
                ),
                "conditional_bins": [asdict(bin_) for bin_ in diagnostic.bins],
                "signal_autocorrelation": {
                    str(lag): autocorrelation(timestamps, signal, lag)
                    for lag in AUTOCORRELATION_LAGS_MS
                },
                "return_autocorrelation": {
                    str(lag): autocorrelation(timestamps, future, lag)
                    for lag in AUTOCORRELATION_LAGS_MS
                },
                "volatility_regimes": regime_results,
            }
            if directional:
                result["condition"] = "LONG signal >= 0.6; SHORT signal <= -0.6 (fixed)"
                result["conditional_count"] = len(positive) + len(negative)
                result["gross_predicted_movement_bps"] = edge
                result["break_even_cost_bps"] = edge
                result["cost_sensitivity"] = cost_sensitivity(edge, configuration.cost_grid_bps)
                result["execution_assumptions"] = {
                    "TAKER_TAKER": cost_sensitivity(edge, (configuration.taker_taker_cost_bps,))[0],
                    "MAKER_TAKER": cost_sensitivity(edge, (configuration.maker_taker_cost_bps,))[0],
                    "note": "hypothetical all-in costs; maker fills are not assumed or simulated",
                }
            if name == "imbalance_l5":
                t = tuple(item[0] for item in condition)
                sx = tuple(item[1] for item in condition)
                fy = tuple(item[2] for item in condition)
                result["block_bootstrap_high_mean"] = asdict(
                    block_bootstrap(
                        t,
                        sx,
                        fy,
                        block_length_ms=configuration.block_length_ms,
                        replications=configuration.bootstrap_replications,
                        seed=configuration.seed,
                    )
                )
                result["block_bootstrap_high_minus_low"] = asdict(
                    block_bootstrap(
                        t,
                        sx,
                        fy,
                        block_length_ms=configuration.block_length_ms,
                        replications=configuration.bootstrap_replications,
                        seed=configuration.seed,
                        difference=True,
                    )
                )
            relationships[name][str(horizon)] = result
    baseline_edges: dict[str, Any] = {}
    for strategy in (
        MicropriceMomentumStrategy(),
        BookImbalanceStrategy(),
        TradeFlowStrategy(),
        CombinedBaselineStrategy(),
    ):
        evaluated = [(row, strategy.evaluate(row.feature)) for row in healthy]
        horizons = {}
        for horizon, label_name in HORIZONS.items():
            movements = [
                getattr(row.label, label_name)
                * 10_000
                * (1 if signal.side is SignalSide.LONG else -1)
                for row, signal in evaluated
                if signal is not None and getattr(row.label, label_name) is not None
            ]
            gross = fmean(movements) if movements else None
            horizons[str(horizon)] = {
                "conditional_count": len(movements),
                "gross_predicted_movement_bps": gross,
                "break_even_cost_bps": gross,
                "cost_sensitivity": cost_sensitivity(gross, configuration.cost_grid_bps),
                "TAKER_TAKER": cost_sensitivity(gross, (configuration.taker_taker_cost_bps,))[0],
                "MAKER_TAKER": cost_sensitivity(gross, (configuration.maker_taker_cost_bps,))[0],
            }
        baseline_edges[strategy.name] = {
            "unchanged_parameters": asdict(strategy),
            "horizons": horizons,
        }
    observed_features = [row.feature for row in rows]
    spreads = sorted(feature.book.spread_bps for feature in observed_features)
    unchanged = sum(
        a.book.mid_price == b.book.mid_price
        for a, b in zip(observed_features, observed_features[1:], strict=False)
    )
    comparison = {
        "feature_count": len(rows),
        "valid_feature_count": len(healthy),
        "median_spread_bps": median(spreads) if spreads else None,
        "p95_spread_bps": spreads[int((len(spreads) - 1) * 0.95)] if spreads else None,
        "median_depth": {
            str(level): {
                side: median(
                    float(getattr(f.book.at_depth(level), f"{side}_volume"))
                    for f in observed_features
                )
                for side in ("bid", "ask")
            }
            for level in (1, 5, 10)
        }
        if rows
        else {},
        "median_rv_1s": median(rv) if rv else None,
        "unchanged_tick_fraction": unchanged / max(1, len(rows) - 1),
        "price_update_fraction": 1 - unchanged / max(1, len(rows) - 1),
    }
    return {
        "hour_utc": target_hour.isoformat(),
        "data_quality": asdict(quality),
        "comparison": comparison,
        "volatility_cutpoints": cuts,
        "volatility_regime_scope": (
            "descriptive per-hour quantiles of contemporaneous/past RV; not formal test fitting"
        ),
        "relationships": relationships,
        "baseline_edge_vs_cost": baseline_edges,
        "edge_scope": "signed future log-price movement at fixed signals; not fills, trades or PnL",
    }, [flatten_research_row(row) for row in rows]


def run_batch(
    root: Path,
    *,
    symbol: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    frozen_path: Path | None = None,
    configuration: BatchConfiguration | None = None,
) -> Path:
    configuration = configuration or BatchConfiguration()
    if start is not None and end is not None and start >= end:
        raise ValueError("--from must precede --to")
    frozen = None if frozen_path is None else verify_frozen(root, frozen_path)
    entries = (
        select_manifests(root, symbol=symbol, start=start, end=end)
        if frozen is None
        else tuple(
            DatasetFileManifest.from_dict(item["manifest"])
            for item in frozen["files"]
            if symbol is None or item["manifest"]["symbol"] == symbol.upper()
        )
    )
    if not entries:
        raise ValueError("no matching completed dataset sessions")
    source_root = Path(__file__).resolve().parent.parent
    source_fingerprint = stable_fingerprint(
        {
            str(path.relative_to(source_root)): file_sha256(path)
            for module in ("research", "replay", "features", "market", "recorder", "strategies")
            for path in sorted((source_root / module).rglob("*.py"))
        }
    )
    fingerprint = stable_fingerprint(
        {
            "files": [asdict(e) for e in entries],
            "configuration": asdict(configuration),
            "from": start,
            "to": end,
            "frozen": None if frozen is None else frozen["dataset_id"],
            "source_fingerprint": source_fingerprint,
            "session_summary_sha256": {
                session: file_sha256(root / "sessions" / session / "session-summary.json")
                for session in sorted({e.capture_session_id for e in entries})
            },
        }
    )
    directory = root / "research" / "batches" / fingerprint
    groups: dict[tuple[str, str], list[DatasetFileManifest]] = defaultdict(list)
    for entry in entries:
        groups[(entry.capture_session_id, entry.symbol)].append(entry)
    sessions: list[dict[str, Any]] = []
    for (session, current_symbol), group in sorted(groups.items()):
        summary_path = root / "sessions" / session / "session-summary.json"
        if not summary_path.exists():
            raise CaptureSafetyError(
                f"session {session} has no final summary; explicit legacy validation required"
            )
        summary = json.loads(summary_path.read_text())
        symbol_summary = summary.get("symbols", {}).get(current_symbol, {})
        if summary.get("status") != "FINALIZED" or not symbol_summary.get("dataset_valid"):
            raise CaptureSafetyError(f"session {session}/{current_symbol} is active or invalid")
        clock = ReplayClock()
        settings = summary["configuration"]
        offsets = RecordedClockOffset(settings["clock_sync_sample_count"])
        engine = FeatureEngine(
            current_symbol,
            interval_ms=settings["feature_interval_ms"],
            book_stale_after_ms=settings["orderbook_stale_after_ms"],
            trade_stale_after_ms=settings["trade_stale_after_ms"],
            mid_history_seconds=settings["mid_history_seconds"],
            trade_history_seconds=settings["trade_history_seconds"],
            imbalance_levels=tuple(settings["feature_imbalance_levels"]),
            clock_offset=offsets,
            monotonic_clock_ns=clock.monotonic_ns,
            wall_clock=clock.wall_time,
        )
        reconstruction = RawReconstructionValidator()
        latency = LatencyDiagnostics(settings["clock_sync_sample_count"])
        hour_buffer = HourBuffer(configuration.max_hour_features)
        buffer = hour_buffer.rows
        integrity_boundaries: list[int] = []
        capture_sequences: dict[int, int] = {}
        reports: list[dict[str, Any]] = []
        digest = hashlib.sha256()
        compared = 0

        def flush(
            buffer: list[FeatureSnapshot] = buffer,
            reports: list[dict[str, Any]] = reports,
            session: str = session,
            current_symbol: str = current_symbol,
            integrity_boundaries: list[int] = integrity_boundaries,
            capture_sequences: dict[int, int] = capture_sequences,
            hour_buffer: HourBuffer = hour_buffer,
        ) -> None:
            if not buffer:
                return
            current_hour = _hour(buffer[0].created_at)
            prefix_end_ns = buffer[hour_buffer.prefix_size - 1].monotonic_ns
            eligible = tuple(
                f
                for f in buffer
                if (start is None or f.created_at >= start) and (end is None or f.created_at < end)
            )
            selected = tuple(f for f in eligible if f.monotonic_ns <= prefix_end_ns)
            analytical_rows: list[dict[str, Any]] = []
            if selected:
                report, analytical_rows = segment_diagnostics(
                    tuple(buffer),
                    current_hour,
                    configuration,
                    integrity_boundaries_ns=tuple(integrity_boundaries),
                    selection_end_ns=prefix_end_ns,
                    start=start,
                    end=end,
                )
                # Reject lost/duplicate rows before publishing any fragment files.
                hour_buffer.finish(analytical_rows, start=start, end=end)
                first_seq = capture_sequences[selected[0].monotonic_ns]
                last_seq = capture_sequences[selected[-1].monotonic_ns]
                key = (
                    f"{session}-{current_symbol}-{current_hour.strftime('%Y%m%dT%H')}"
                    f"-seq{first_seq}"
                )
                report["partition"] = {
                    "policy": "contiguous source-order UTC-hour fragment; hours may recur",
                    "first_capture_seq": first_seq,
                    "last_capture_seq": last_seq,
                }
                write_json_atomic(directory / "segments" / f"{key}.json", report)
                write_json_atomic(
                    directory / "edge-vs-cost" / f"{key}.json", report["baseline_edge_vs_cost"]
                )
                # Derived analytical rows are an explicit feature/label join, never live input.
                output = directory / "labels" / f"{key}.parquet"
                for row in analytical_rows:
                    row["capture_session_id"] = session
                    row["capture_seq"] = capture_sequences[row["monotonic_ns"]]
                output.parent.mkdir(parents=True, exist_ok=True)
                temporary = output.with_suffix(".tmp")
                pq.write_table(pa.Table.from_pylist(analytical_rows), temporary, compression="zstd")
                temporary.replace(output)
                reports.append(
                    {
                        "path": f"segments/{key}.json",
                        "hour_utc": report["hour_utc"],
                        "partition": report["partition"],
                        "comparison": report["comparison"],
                        "relationships": {
                            name: {
                                h: {
                                    "n": item["raw_sample_count"],
                                    "pearson": item["all_snapshot_pearson"],
                                    "edge_bps": item.get("gross_predicted_movement_bps"),
                                    "moments": item["correlation_moments"],
                                }
                                for h, item in horizons.items()
                            }
                            for name, horizons in report["relationships"].items()
                        },
                    }
                )
            else:
                hour_buffer.finish([], start=start, end=end)
            if buffer:
                integrity_boundaries[:] = [
                    t for t in integrity_boundaries if t >= buffer[0].monotonic_ns
                ]
            else:
                integrity_boundaries.clear()
            retained_ns = {f.monotonic_ns for f in buffer}
            for timestamp in tuple(capture_sequences):
                if timestamp not in retained_ns:
                    del capture_sequences[timestamp]

        def integrity_boundary(
            item: ReplayItem,
            boundaries: list[int] = integrity_boundaries,
            capture_sequences: dict[int, int] = capture_sequences,
        ) -> None:
            if item.event_type is ReplayEventType.FEATURE_TICK:
                capture_sequences[item.monotonic_ns] = item.capture_seq
            if isinstance(item.payload, LifecycleRecord) and not item.payload.healthy:
                boundaries.append(item.monotonic_ns)
                if len(boundaries) > configuration.max_hour_features:
                    raise CaptureSafetyError("integrity boundary buffer exceeds memory bound")

        def feature_handler(
            feature: FeatureSnapshot,
            recorded: FeatureSnapshot | None,
            buffer: list[FeatureSnapshot] = buffer,
            update_digest: Callable[[bytes], None] = digest.update,
            flush_handler: Callable[[], None] = flush,
            hour_buffer: HourBuffer = hour_buffer,
        ) -> None:
            nonlocal compared
            if recorded is None:
                raise CaptureSafetyError("recorded feature tick missing its snapshot")
            errors = feature_equivalence_errors(feature, recorded)
            if errors:
                raise CaptureSafetyError(
                    f"feature replay mismatch at {feature.monotonic_ns}: {errors}"
                )
            compared += 1
            update_digest(json.dumps(asdict(feature), sort_keys=True, default=str).encode())
            hour_buffer.append(feature)
            while hour_buffer.ready():
                flush_handler()

        replay = ReplayEngine(
            iter_session(root, tuple(group)),
            clock,
            engine,
            consumers=(reconstruction.accept, latency.accept, integrity_boundary),
            retain_results=False,
            feature_handler=feature_handler,
            clock_offset=offsets,
        ).run()
        while buffer:
            flush()
        accounting = hour_buffer.accounting()
        raw_result = reconstruction.result()
        if compared != sum(e.row_count for e in group if e.event_type == "feature"):
            raise CaptureSafetyError("replay failed to reproduce every recorded feature tick")
        if not raw_result.valid:
            raise CaptureSafetyError(f"raw reconstruction invalid: {raw_result}")

        def session_correlation(
            name: str, horizon: int, reports: list[dict[str, Any]] = reports
        ) -> float | None:
            combined = CorrelationMoments()
            for report in reports:
                combined.merge(
                    CorrelationMoments(**report["relationships"][name][str(horizon)]["moments"])
                )
            return combined.pearson()

        session_stats = {
            name: {str(h): session_correlation(name, h) for h in HORIZONS} for name in FEATURES
        }
        sessions.append(
            {
                "session": session,
                "symbol": current_symbol,
                "replay": asdict(replay.data_quality),
                "recomputed_features": compared,
                "row_accounting": accounting,
                "wall_clock_diagnostics": {
                    "backward_step_count": clock.backward_step_count,
                    "largest_backward_step": (
                        None
                        if clock.largest_backward_step is None
                        else asdict(clock.largest_backward_step)
                    ),
                    "last_backward_step": (
                        None
                        if clock.last_backward_step is None
                        else asdict(clock.last_backward_step)
                    ),
                },
                "reconstruction": asdict(raw_result),
                "latency_diagnostics": latency.report(),
                "feature_output_sha256": digest.hexdigest(),
                "capture_summary": symbol_summary,
                "segments": reports,
                "session_statistic_method": (
                    "session Pearson from merged centered hourly moments; "
                    "no session row concatenation"
                ),
                "session_statistics": session_stats,
            }
        )
    stability = {
        s: {
            name: {
                str(h): session_stability(
                    tuple(
                        item["session_statistics"][name][str(h)]
                        for item in sessions
                        if item["symbol"] == s
                    )
                )
                for h in HORIZONS
            }
            for name in FEATURES
        }
        for s in sorted({item["symbol"] for item in sessions})
    }
    report = {
        "batch_fingerprint": fingerprint,
        "source_fingerprint": source_fingerprint,
        "frozen_dataset_id": None if frozen is None else frozen["dataset_id"],
        "configuration": asdict(configuration),
        "scope": "descriptive robustness; all fixed horizons, no threshold fitting",
        "session_boundary_policy": (
            "replay each independently; no labels across restarts; "
            "forward context retained across UTC hours"
        ),
        "sessions": sessions,
        "session_stability": stability,
        "dataset_valid": True,
    }
    path = directory / "batch-report.json"
    write_json_atomic(path, report)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, default=Path("data"))
    parser.add_argument("--symbol", choices=("BTCUSDT", "BTCUSDC"))
    parser.add_argument("--from", dest="start", type=datetime.fromisoformat)
    parser.add_argument("--to", dest="end", type=datetime.fromisoformat)
    parser.add_argument("--frozen", type=Path)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--bootstrap-replications", type=int, default=200)
    parser.add_argument("--block-length-ms", type=int, default=10_000)
    parser.add_argument("--taker-taker-cost-bps", type=float, default=11)
    parser.add_argument("--maker-taker-cost-bps", type=float, default=8)
    args = parser.parse_args()
    for value in (args.start, args.end):
        if value is not None and value.tzinfo is None:
            parser.error("--from/--to require an explicit timezone, e.g. 2026-09-05T00:00:00+00:00")
    print(
        run_batch(
            args.data_directory,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            frozen_path=args.frozen,
            configuration=BatchConfiguration(
                seed=args.seed,
                bootstrap_replications=args.bootstrap_replications,
                block_length_ms=args.block_length_ms,
                taker_taker_cost_bps=args.taker_taker_cost_bps,
                maker_taker_cost_bps=args.maker_taker_cost_bps,
            ),
        )
    )


if __name__ == "__main__":
    main()
