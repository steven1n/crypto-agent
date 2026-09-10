"""Bounded k-way merge of cataloged session streams."""

from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from recorder.catalog import DatasetFileManifest
from recorder.durability import validate_file
from recorder.raw_models import RawEventType
from replay.features import decode_feature
from replay.loader import _decode_payload
from replay.models import ReplayEventType, ReplayIntegrityError, ReplayItem


def iter_session(root: Path, manifests: tuple[DatasetFileManifest, ...]) -> Iterator[ReplayItem]:
    """At most one 512-row batch per event type; files are opened sequentially."""
    if len({(m.symbol, m.capture_session_id) for m in manifests}) != 1:
        raise ReplayIntegrityError("streaming replay requires one session and symbol")
    kinds = {m.event_type for m in manifests}
    if not {"depth", "trusted_book", "stream_lifecycle", "feature"}.issubset(kinds):
        raise ReplayIntegrityError("missing required stream in session")
    groups: dict[str, list[DatasetFileManifest]] = defaultdict(list)
    for manifest in manifests:
        groups[manifest.event_type].append(manifest)

    def stream(entries: list[DatasetFileManifest]) -> Iterator[ReplayItem]:
        previous = -1
        for manifest in sorted(entries, key=lambda m: m.first_capture_seq):
            validate_file(root, manifest)
            parquet = pq.ParquetFile(root / manifest.file_path)
            for batch in parquet.iter_batches(batch_size=512):
                for row in batch.to_pylist():
                    sequence = int(row["capture_seq"])
                    if sequence <= previous:
                        raise ReplayIntegrityError(
                            "overlapping files or duplicate capture sequence"
                        )
                    previous = sequence
                    feature = manifest.event_type == "feature"
                    yield ReplayItem(
                        sequence,
                        manifest.capture_session_id,
                        ReplayEventType.FEATURE_TICK
                        if feature
                        else ReplayEventType(manifest.event_type),
                        manifest.symbol,
                        row["created_at"] if feature else row["local_time"],
                        row["monotonic_ns"],
                        decode_feature(row)
                        if feature
                        else _decode_payload(RawEventType(manifest.event_type), row),
                    )

    previous_seq = previous_ns = -1
    for event in heapq.merge(
        *(stream(entries) for entries in groups.values()), key=lambda e: e.capture_seq
    ):
        if event.capture_seq <= previous_seq or event.monotonic_ns < previous_ns:
            raise ReplayIntegrityError("capture sequence duplicate or negative monotonic delta")
        previous_seq, previous_ns = event.capture_seq, event.monotonic_ns
        yield event
