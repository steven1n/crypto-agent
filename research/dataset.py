"""Deterministic feature/label analytical-table alignment."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from features.models import FeatureSnapshot
from recorder.schemas import flatten_feature
from research.labels import LabelSnapshot


@dataclass(frozen=True, slots=True)
class ResearchRow:
    feature: FeatureSnapshot
    label: LabelSnapshot


def join_features_and_labels(
    features: tuple[FeatureSnapshot, ...], labels: tuple[LabelSnapshot, ...]
) -> tuple[ResearchRow, ...]:
    if len(features) != len(labels):
        raise ValueError("feature and label counts differ")
    rows: list[ResearchRow] = []
    for feature, label in zip(features, labels, strict=True):
        if feature.symbol != label.symbol or feature.monotonic_ns != label.monotonic_ns:
            raise ValueError("feature/label alignment mismatch")
        rows.append(ResearchRow(feature, label))
    return tuple(rows)


def flatten_research_row(row: ResearchRow) -> dict[str, Any]:
    result = flatten_feature(row.feature)
    result.update(
        {
            name: getattr(row.label, name)
            for name in row.label.__dataclass_fields__
            if name not in {"symbol", "monotonic_ns"}
        }
    )
    return result
