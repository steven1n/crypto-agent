"""Transparent predictive and data-quality diagnostics without parameter fitting."""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import fmean, median, stdev

from features.models import FeatureSnapshot
from research.dataset import ResearchRow


@dataclass(frozen=True, slots=True)
class ConditionalBin:
    bin_index: int
    sample_count: int
    feature_min: float
    feature_max: float
    mean_future_return: float
    standard_deviation: float | None
    standard_error: float | None


@dataclass(frozen=True, slots=True)
class PredictiveDiagnostic:
    feature_name: str
    label_name: str
    sample_count: int
    pearson_correlation: float | None
    spearman_correlation: float | None
    bins: tuple[ConditionalBin, ...]


@dataclass(frozen=True, slots=True)
class DirectionalHitRate:
    feature_name: str
    label_name: str
    threshold: float
    direction: str
    sample_count: int
    hit_rate: float | None
    mean_future_return: float | None


@dataclass(frozen=True, slots=True)
class NumericSummary:
    name: str
    rows: int
    missing_percentage: float
    nan_count: int
    inf_count: int
    minimum: float | None
    median: float | None
    p95: float | None
    p99: float | None
    maximum: float | None


@dataclass(frozen=True, slots=True)
class DataQualityReport:
    valid: bool
    rows: int
    summaries: tuple[NumericSummary, ...]
    invariant_errors: tuple[str, ...]


def feature_value(feature: FeatureSnapshot, name: str) -> float | None:
    values = {
        "microprice_offset_bps": feature.book.microprice_offset_bps,
        "imbalance_l1": feature.book.at_depth(1).imbalance,
        "imbalance_l5": feature.book.at_depth(5).imbalance,
        "imbalance_l10": feature.book.at_depth(10).imbalance,
        "trade_imbalance_1s": feature.trade_flow_window(1_000).trade_imbalance,
        "trade_imbalance_5s": feature.trade_flow_window(5_000).trade_imbalance,
        "return_250ms": feature.returns.return_250ms,
        "return_1s": feature.returns.return_1s,
        "rv_1s": feature.volatility.realized_volatility_1s,
        "rv_5s": feature.volatility.realized_volatility_5s,
        "rv_10s": feature.volatility.realized_volatility_10s,
    }
    if name not in values:
        raise KeyError(f"unknown feature: {name}")
    return values[name]


def label_value(row: ResearchRow, name: str) -> float | None:
    if not name.startswith("future_") or not hasattr(row.label, name):
        raise KeyError(f"unknown label: {name}")
    value = getattr(row.label, name)
    return None if value is None else float(value)


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) < 2:
        return None
    mean_left = fmean(left)
    mean_right = fmean(right)
    numerator = sum((x - mean_left) * (y - mean_right) for x, y in zip(left, right, strict=True))
    left_variance = sum((value - mean_left) ** 2 for value in left)
    right_variance = sum((value - mean_right) ** 2 for value in right)
    denominator = math.sqrt(left_variance * right_variance)
    return None if denominator == 0 else numerator / denominator


def _ranks(values: list[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        average_rank = (index + end - 1) / 2 + 1
        for original_index, _ in ordered[index:end]:
            ranks[original_index] = average_rank
        index = end
    return ranks


def analyze_feature(
    rows: tuple[ResearchRow, ...],
    *,
    feature_name: str,
    label_name: str,
    bin_count: int = 10,
) -> PredictiveDiagnostic:
    if bin_count <= 0:
        raise ValueError("bin_count must be positive")
    pairs = [
        (feature_value(row.feature, feature_name), label_value(row, label_name)) for row in rows
    ]
    observed = [(x, y) for x, y in pairs if x is not None and y is not None]
    left = [float(x) for x, _ in observed]
    right = [float(y) for _, y in observed]
    ordered = sorted(zip(left, right, strict=True), key=lambda pair: pair[0])
    bins: list[ConditionalBin] = []
    for bin_index in range(bin_count):
        start = len(ordered) * bin_index // bin_count
        end = len(ordered) * (bin_index + 1) // bin_count
        portion = ordered[start:end]
        if not portion:
            continue
        x_values = [item[0] for item in portion]
        y_values = [item[1] for item in portion]
        deviation = stdev(y_values) if len(y_values) >= 2 else None
        bins.append(
            ConditionalBin(
                bin_index=bin_index,
                sample_count=len(portion),
                feature_min=min(x_values),
                feature_max=max(x_values),
                mean_future_return=fmean(y_values),
                standard_deviation=deviation,
                standard_error=(
                    None if deviation is None else deviation / math.sqrt(len(y_values))
                ),
            )
        )
    return PredictiveDiagnostic(
        feature_name=feature_name,
        label_name=label_name,
        sample_count=len(observed),
        pearson_correlation=_pearson(left, right),
        spearman_correlation=_pearson(_ranks(left), _ranks(right)),
        bins=tuple(bins),
    )


def directional_hit_rates(
    rows: tuple[ResearchRow, ...],
    *,
    feature_name: str,
    label_name: str,
    thresholds: tuple[float, ...] = (0.2, 0.4, 0.6),
) -> tuple[DirectionalHitRate, ...]:
    output: list[DirectionalHitRate] = []
    for threshold in thresholds:
        if threshold <= 0:
            raise ValueError("directional thresholds must be positive")
        for direction in ("LONG", "SHORT"):
            selected: list[float] = []
            for row in rows:
                feature = feature_value(row.feature, feature_name)
                future = label_value(row, label_name)
                if feature is None or future is None:
                    continue
                if (direction == "LONG" and feature >= threshold) or (
                    direction == "SHORT" and feature <= -threshold
                ):
                    selected.append(future)
            hits = sum((value > 0 if direction == "LONG" else value < 0) for value in selected)
            output.append(
                DirectionalHitRate(
                    feature_name=feature_name,
                    label_name=label_name,
                    threshold=threshold,
                    direction=direction,
                    sample_count=len(selected),
                    hit_rate=None if not selected else hits / len(selected),
                    mean_future_return=None if not selected else fmean(selected),
                )
            )
    return tuple(output)


def _quantile(sorted_values: list[float], fraction: float) -> float | None:
    if not sorted_values:
        return None
    return sorted_values[int((len(sorted_values) - 1) * fraction)]


def data_quality_report(features: tuple[FeatureSnapshot, ...]) -> DataQualityReport:
    names = (
        "microprice_offset_bps",
        "imbalance_l1",
        "imbalance_l5",
        "imbalance_l10",
        "trade_imbalance_1s",
        "trade_imbalance_5s",
        "return_250ms",
        "return_1s",
        "rv_5s",
    )
    summaries: list[NumericSummary] = []
    errors: list[str] = []
    row_count = len(features)
    for name in names:
        raw = [feature_value(feature, name) for feature in features]
        nan_count = sum(value is not None and math.isnan(value) for value in raw)
        inf_count = sum(value is not None and math.isinf(value) for value in raw)
        finite = sorted(value for value in raw if value is not None and math.isfinite(value))
        summaries.append(
            NumericSummary(
                name=name,
                rows=row_count,
                missing_percentage=(
                    0.0
                    if row_count == 0
                    else (row_count - len(finite) - nan_count - inf_count) / row_count * 100
                ),
                nan_count=nan_count,
                inf_count=inf_count,
                minimum=None if not finite else min(finite),
                median=None if not finite else median(finite),
                p95=_quantile(finite, 0.95),
                p99=_quantile(finite, 0.99),
                maximum=None if not finite else max(finite),
            )
        )
    for index, feature in enumerate(features):
        if feature.book.spread_bps < 0:
            errors.append(f"row {index}: negative spread")
        if feature.book.best_bid_price <= 0 or feature.book.best_ask_price <= 0:
            errors.append(f"row {index}: nonpositive price")
        microprice = feature.book.microprice
        if microprice is not None and not (
            feature.book.best_bid_price <= microprice <= feature.book.best_ask_price
        ):
            errors.append(f"row {index}: microprice outside BBO")
        for level in (1, 5, 10):
            depth = feature.book.at_depth(level)
            if depth.bid_volume < 0 or depth.ask_volume < 0:
                errors.append(f"row {index}: negative depth")
            if depth.imbalance is not None and not -1 <= depth.imbalance <= 1:
                errors.append(f"row {index}: imbalance outside range")
        for window in (1_000, 5_000, 10_000):
            imbalance = feature.trade_flow_window(window).trade_imbalance
            if imbalance is not None and not -1 <= imbalance <= 1:
                errors.append(f"row {index}: trade imbalance outside range")
    if any(summary.nan_count or summary.inf_count for summary in summaries):
        errors.append("NaN or Inf in critical numeric features")
    return DataQualityReport(
        valid=not errors,
        rows=row_count,
        summaries=tuple(summaries),
        invariant_errors=tuple(errors),
    )
