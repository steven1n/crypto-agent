"""Compact inactive fragments into a NEW dataset directory; retain all originals."""

import argparse
import json
import shutil
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from recorder.catalog import DatasetCatalog, DatasetFileManifest, stable_fingerprint
from recorder.compaction import compact_parquet_fragments
from recorder.durability import (
    DatasetLock,
    file_sha256,
    finalize_parquet,
    validate_file,
    write_json_atomic,
)


def compact_dataset(
    root: Path,
    destination: Path,
    *,
    max_fragments: int = 100,
    max_input_bytes: int = 64_000_000,
    max_input_rows: int = 200_000,
) -> int:
    if destination.exists() or destination.resolve().is_relative_to(root.resolve()):
        raise ValueError("output must be a new directory outside the source dataset")
    if min(max_fragments, max_input_bytes, max_input_rows) < 1:
        raise ValueError("compaction bounds must be positive")
    with DatasetLock(root), DatasetLock(destination):
        entries = DatasetCatalog(root).entries()
        for entry in entries:
            validate_file(root, entry)
        catalog = DatasetCatalog(destination)
        groups: dict[tuple[str, str, str, int], list[DatasetFileManifest]] = defaultdict(list)
        for entry in entries:
            groups[
                (entry.capture_session_id, entry.symbol, entry.event_type, entry.schema_version)
            ].append(entry)
        written = 0
        for group in groups.values():
            group.sort(key=lambda e: e.first_capture_seq)
            subsets: list[list[DatasetFileManifest]] = []
            pending: list[DatasetFileManifest] = []
            rows = size = 0
            for entry in group:
                if entry.file_size > max_input_bytes or entry.row_count > max_input_rows:
                    raise ValueError(
                        "single fragment exceeds compaction bounds; raise explicit limits"
                    )
                if pending and (
                    len(pending) >= max_fragments
                    or size + entry.file_size > max_input_bytes
                    or rows + entry.row_count > max_input_rows
                ):
                    subsets.append(pending)
                    pending, size, rows = [], 0, 0
                pending.append(entry)
                size += entry.file_size
                rows += entry.row_count
            if pending:
                subsets.append(pending)
            for subset in subsets:
                first = subset[0]
                path = (
                    destination / Path(first.file_path).parent / f"compacted-{written:06d}.parquet"
                )
                working = destination / f".compacting-{written:06d}.parquet"
                compact_parquet_fragments(
                    tuple(root / e.file_path for e in subset), working, minimum_source_age_seconds=0
                )
                table = pq.ParquetFile(working).read().replace_schema_metadata(None)
                manifest = replace(
                    first,
                    dataset_id=stable_fingerprint([e.dataset_id for e in subset]),
                    file_path=path.relative_to(destination).as_posix(),
                    file_size=0,
                    content_sha256=None,
                    row_count=sum(e.row_count for e in subset),
                    last_capture_seq=subset[-1].last_capture_seq,
                    end_local_time=max(e.end_local_time for e in subset),
                    end_exchange_time=max(
                        (e.end_exchange_time for e in subset if e.end_exchange_time), default=None
                    ),
                    data_healthy=all(e.data_healthy for e in subset),
                )
                catalog.register(finalize_parquet(table, path, manifest))
                working.unlink()
                written += 1
        if (root / "sessions").exists():
            shutil.copytree(root / "sessions", destination / "sessions")
            finalized = catalog.entries()
            for summary_path in (destination / "sessions").glob("*/session-summary.json"):
                summary = json.loads(summary_path.read_text())
                source_summary = root / "sessions" / summary_path.parent.name / summary_path.name
                summary["compaction_provenance"] = {
                    "source_summary_sha256": file_sha256(source_summary),
                    "storage_projection_scope": "original capture layout, before compaction",
                }
                for symbol, stats in summary.get("symbols", {}).items():
                    selected = [
                        e
                        for e in finalized
                        if e.symbol == symbol
                        and e.capture_session_id == summary["capture_session_id"]
                    ]
                    stats["original_capture_bytes"] = stats["bytes"]
                    stats["bytes"] = sum(e.file_size for e in selected)
                    stats["files"] = len(selected)
                write_json_atomic(summary_path, summary)
        return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--max-fragments", type=int, default=100)
    parser.add_argument("--max-input-bytes", type=int, default=64_000_000)
    parser.add_argument("--max-input-rows", type=int, default=200_000)
    args = parser.parse_args()
    print(
        {
            "files_finalized": compact_dataset(
                args.data_directory,
                args.output_directory,
                max_fragments=args.max_fragments,
                max_input_bytes=args.max_input_bytes,
                max_input_rows=args.max_input_rows,
            )
        }
    )


if __name__ == "__main__":
    main()
