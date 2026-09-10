"""Validated, atomic offline Parquet fragment compaction."""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]


class CompactionError(RuntimeError):
    """Source fragments are unsafe or incompatible."""


@dataclass(frozen=True, slots=True)
class CompactionResult:
    output_path: Path
    input_files: int
    input_rows: int
    output_rows: int
    sources_removed: bool


def compact_parquet_fragments(
    source_paths: tuple[Path, ...],
    output_path: Path,
    *,
    ordering_keys: tuple[str, ...] = ("capture_seq",),
    remove_sources: bool = False,
    minimum_source_age_seconds: float = 5.0,
) -> CompactionResult:
    """Merge compatible fragments without deleting sources by default.

    A nonzero age guard avoids files that an active recorder may still own.
    Destructive removal happens only after the atomically installed output has
    been read back and compared with the canonical sorted input table.
    """

    if not source_paths:
        raise ValueError("at least one source fragment is required")
    if minimum_source_age_seconds < 0:
        raise ValueError("minimum_source_age_seconds cannot be negative")
    resolved_output = output_path.resolve()
    resolved_sources = tuple(path.resolve() for path in source_paths)
    if len(set(resolved_sources)) != len(resolved_sources):
        raise CompactionError("duplicate source fragment")
    if resolved_output in resolved_sources:
        raise CompactionError("output path cannot also be a source")
    if output_path.exists():
        raise CompactionError(f"output already exists: {output_path}")

    if remove_sources:
        from recorder.frozen import reject_frozen_sources

        reject_frozen_sources(source_paths)
        if any(
            (parent / "catalog").is_dir()
            for source in source_paths
            for parent in source.resolve().parents
        ):
            raise CompactionError(
                "cataloged sources require new-version compaction via recorder.compact"
            )

    now = time.time()
    for source in source_paths:
        if not source.is_file():
            raise CompactionError(f"missing source fragment: {source}")
        age = now - source.stat().st_mtime
        if age < minimum_source_age_seconds:
            raise CompactionError(f"source may still be active ({age:.3f}s old): {source}")

    try:
        tables = [pq.ParquetFile(path).read() for path in source_paths]
    except Exception as exc:
        raise CompactionError(f"cannot read source fragment: {exc}") from exc
    reference_schema = tables[0].schema
    if any(table.schema != reference_schema for table in tables[1:]):
        raise CompactionError("source Parquet schemas are incompatible")
    missing_keys = [key for key in ordering_keys if key not in reference_schema.names]
    if missing_keys:
        raise CompactionError(f"ordering columns missing: {missing_keys}")

    combined = pa.concat_tables(tables)
    canonical = combined.sort_by([(key, "ascending") for key in ordering_keys])
    input_rows = sum(table.num_rows for table in tables)
    if canonical.num_rows != input_rows:
        raise CompactionError("row count changed while combining fragments")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        pq.write_table(canonical, temporary, compression="zstd")
        validated = pq.ParquetFile(temporary).read()
        if validated.num_rows != input_rows:
            raise CompactionError("compacted output row-count validation failed")
        if validated.schema != reference_schema:
            raise CompactionError("compacted output schema validation failed")
        if not validated.equals(canonical):
            raise CompactionError("compacted output row-content validation failed")
        os.replace(temporary, output_path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    if remove_sources:
        for source in source_paths:
            source.unlink()
    return CompactionResult(
        output_path=output_path,
        input_files=len(source_paths),
        input_rows=input_rows,
        output_rows=input_rows,
        sources_removed=remove_sources,
    )
