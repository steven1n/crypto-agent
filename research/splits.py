"""Chronological walk-forward splits with forward-label boundary purging."""

from __future__ import annotations

import bisect
from dataclasses import dataclass

from research.dataset import ResearchRow


@dataclass(frozen=True, slots=True)
class SplitBoundary:
    name: str
    start_monotonic_ns: int
    end_monotonic_ns: int
    row_count: int


@dataclass(frozen=True, slots=True)
class WalkForwardSplit:
    train: tuple[ResearchRow, ...]
    validation: tuple[ResearchRow, ...]
    test: tuple[ResearchRow, ...]
    purge_window_ns: int
    boundaries: tuple[SplitBoundary, ...]


def chronological_split(
    rows: tuple[ResearchRow, ...],
    *,
    train_fraction: float = 0.6,
    validation_fraction: float = 0.2,
    max_label_horizon_ms: int = 10_000,
) -> WalkForwardSplit:
    if not rows:
        raise ValueError("cannot split an empty research dataset")
    if not 0 < train_fraction < 1 or not 0 < validation_fraction < 1:
        raise ValueError("split fractions must be inside (0, 1)")
    if train_fraction + validation_fraction >= 1:
        raise ValueError("train and validation fractions must leave a test partition")
    timestamps = [row.feature.monotonic_ns for row in rows]
    if any(
        current <= previous for previous, current in zip(timestamps, timestamps[1:], strict=False)
    ):
        raise ValueError("research rows must be strictly chronological")
    train_cut = max(1, int(len(rows) * train_fraction))
    validation_cut = max(train_cut + 1, int(len(rows) * (train_fraction + validation_fraction)))
    if validation_cut >= len(rows):
        raise ValueError("not enough rows for all three chronological partitions")
    purge_ns = max_label_horizon_ms * 1_000_000
    if max_label_horizon_ms <= 0:
        raise ValueError("label horizon must be positive")

    train = tuple(
        row
        for row in rows[:train_cut]
        if bisect.bisect_left(timestamps, row.feature.monotonic_ns + purge_ns) < train_cut
    )
    validation = tuple(
        row
        for row in rows[train_cut:validation_cut]
        if bisect.bisect_left(timestamps, row.feature.monotonic_ns + purge_ns) < validation_cut
    )
    test = tuple(rows[validation_cut:])

    def boundary(name: str, partition: tuple[ResearchRow, ...]) -> SplitBoundary:
        if not partition:
            raise ValueError(f"purge window leaves {name} partition empty")
        return SplitBoundary(
            name=name,
            start_monotonic_ns=partition[0].feature.monotonic_ns,
            end_monotonic_ns=partition[-1].feature.monotonic_ns,
            row_count=len(partition),
        )

    return WalkForwardSplit(
        train=train,
        validation=validation,
        test=test,
        purge_window_ns=purge_ns,
        boundaries=(
            boundary("TRAIN", train),
            boundary("VALIDATION", validation),
            boundary("TEST", test),
        ),
    )
