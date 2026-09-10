"""Atomic, recoverable files, exclusive dataset ownership and disk guards."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import uuid
from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, BinaryIO

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from recorder.catalog import DatasetCatalog, DatasetFileManifest


class CaptureSafetyError(RuntimeError):
    """Continuing capture would hide data loss or compromise integrity."""


class BackpressureState(StrEnum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


def backpressure_state(queued: int, capacity: int) -> BackpressureState:
    if capacity <= 0 or queued < 0:
        raise ValueError("invalid queue size")
    ratio = queued / capacity
    if ratio >= 0.9:
        return BackpressureState.CRITICAL
    return BackpressureState.WARNING if ratio >= 0.7 else BackpressureState.NORMAL


@dataclass
class DiskGuard:
    root: Path
    min_free_bytes: int = 2_000_000_000
    degraded: bool = False

    def check(self) -> int:
        self.root.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(self.root).free
        if free < self.min_free_bytes:
            self.degraded = True
            raise CaptureSafetyError(f"free disk {free} below reserve {self.min_free_bytes}")
        return free


class DatasetLock:
    """OS releases flock on crash; the lock file itself is intentionally persistent."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._file: BinaryIO | None = None

    def __enter__(self) -> DatasetLock:
        self.root.mkdir(parents=True, exist_ok=True)
        self._file = (self.root / ".capture.lock").open("a+b")
        try:
            fcntl.flock(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._file.close()
            self._file = None
            raise CaptureSafetyError(
                "dataset is owned by an active capture/maintenance process"
            ) from exc
        return self

    def __exit__(self, *args: object) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json_atomic(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, default=str, allow_nan=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    sync_directory(path.parent)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def finalize_parquet(
    table: pa.Table, path: Path, manifest: DatasetFileManifest, guard: DiskGuard | None = None
) -> DatasetFileManifest:
    if guard is not None:
        guard.check()
    if path.exists():
        raise CaptureSafetyError(f"refusing to overwrite finalized file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    # The footer survives a crash between rename and catalog registration.
    metadata = {
        **(table.schema.metadata or {}),
        b"capture_manifest": json.dumps(asdict(manifest), sort_keys=True).encode(),
    }
    pq.write_table(table.replace_schema_metadata(metadata), temporary, compression="zstd")
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    readback = pq.ParquetFile(temporary)
    if readback.metadata.num_rows != manifest.row_count:
        raise CaptureSafetyError("row count changed during finalization")
    os.replace(temporary, path)
    sync_directory(path.parent)
    return replace(manifest, file_size=path.stat().st_size, content_sha256=file_sha256(path))


def expected_schema(event_type: str, version: int) -> pa.Schema:
    from recorder.raw_models import RawEventType
    from recorder.raw_schemas import raw_schema
    from recorder.schemas import feature_schema

    return (
        feature_schema(version)
        if event_type == "feature"
        else raw_schema(RawEventType(event_type), version)
    )


def validate_file(root: Path, manifest: DatasetFileManifest) -> None:
    from recorder.versions import DATASET_VERSION

    if manifest.dataset_version != DATASET_VERSION or manifest.row_count <= 0:
        raise CaptureSafetyError("unsupported dataset version or empty finalized file")
    path = (root / manifest.file_path).resolve()
    if not path.is_relative_to(root.resolve()):
        raise CaptureSafetyError("manifest path escapes dataset root")
    if not path.is_file():
        raise CaptureSafetyError(f"missing cataloged file: {manifest.file_path}")
    if manifest.content_sha256 and file_sha256(path) != manifest.content_sha256:
        raise CaptureSafetyError(f"file checksum mismatch: {manifest.file_path}")
    parquet = pq.ParquetFile(path)
    if parquet.schema_arrow != expected_schema(manifest.event_type, manifest.schema_version):
        raise CaptureSafetyError(f"schema mismatch: {manifest.file_path}")
    if parquet.metadata.num_rows != manifest.row_count or path.stat().st_size != manifest.file_size:
        raise CaptureSafetyError(f"file size/row count mismatch: {manifest.file_path}")
    previous = -1
    first = None
    for batch in parquet.iter_batches(
        columns=["capture_seq", "symbol", "capture_session_id", "schema_version", "dataset_version"]
    ):
        for row in batch.to_pylist():
            if (
                row["capture_seq"] <= previous
                or row["symbol"] != manifest.symbol
                or row["capture_session_id"] != manifest.capture_session_id
                or row["schema_version"] != manifest.schema_version
                or row["dataset_version"] != manifest.dataset_version
            ):
                raise CaptureSafetyError(f"invalid row identity/order: {manifest.file_path}")
            previous = row["capture_seq"]
            if first is None:
                first = previous
    if first != manifest.first_capture_seq or previous != manifest.last_capture_seq:
        raise CaptureSafetyError(
            f"capture sequence bounds differ from manifest: {manifest.file_path}"
        )


@dataclass(frozen=True)
class RecoverySummary:
    reconciled: tuple[str, ...]
    quarantined: tuple[str, ...]
    invalid: tuple[str, ...]
    interrupted_sessions: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.invalid


def recover_dataset(root: Path, *, repair: bool = False) -> RecoverySummary:
    """Caller holds DatasetLock for repair. Unknown orphan data is never guessed."""
    catalog = DatasetCatalog(root)
    try:
        entries = catalog.entries()
    except Exception as exc:
        return RecoverySummary((), (), (f"unreadable catalog: {exc}",), ())
    invalid: list[str] = []
    if any(root.glob(".compacting*")):
        invalid.append("unfinished compaction workspace; do not treat output as complete")
    reconciled: list[str] = []
    quarantined: list[str] = []
    interrupted: list[str] = []
    ids = [entry.dataset_id for entry in entries]
    paths = [entry.file_path for entry in entries]
    if len(set(ids)) != len(ids) or len(set(paths)) != len(paths):
        invalid.append("duplicate dataset IDs or file paths in manifest")
    for entry in entries:
        try:
            validate_file(root, entry)
        except Exception as exc:
            invalid.append(str(exc))
    for folder in (root / "raw", root / "features", root / "catalog", root / "sessions"):
        if not folder.exists():
            continue
        for path in sorted(folder.rglob("*.tmp")):
            relative = str(path.relative_to(root))
            quarantined.append(relative)
            if repair:
                destination = root / "quarantine" / f"{uuid.uuid4().hex}-{path.name}"
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.replace(path, destination)
        for path in sorted(folder.rglob("*.parquet")):
            relative = path.relative_to(root).as_posix()
            if relative in paths:
                continue
            try:
                metadata = pq.ParquetFile(path).schema_arrow.metadata or {}
                if b"capture_manifest" not in metadata:
                    raise CaptureSafetyError(f"orphan without embedded manifest: {relative}")
                recovered = DatasetFileManifest.from_dict(json.loads(metadata[b"capture_manifest"]))
                if recovered.file_path != relative or recovered.dataset_id in ids:
                    raise CaptureSafetyError(f"ambiguous orphan identity: {relative}")
                recovered = replace(
                    recovered, file_size=path.stat().st_size, content_sha256=file_sha256(path)
                )
                validate_file(root, recovered)
                reconciled.append(relative)
                if repair:
                    catalog.register(recovered)
                ids.append(recovered.dataset_id)
            except Exception as exc:
                invalid.append(str(exc))
    for path in sorted((root / "sessions").glob("*/session-summary.json")):
        state: dict[str, Any] = json.loads(path.read_text())
        if state.get("status") == "RUNNING":
            interrupted.append(state["capture_session_id"])
            if repair:
                state.update(
                    status="INTERRUPTED",
                    dataset_valid=False,
                    invalid_reasons=["process interrupted before graceful finalization"],
                )
                write_json_atomic(path, state)
    return RecoverySummary(
        tuple(reconciled), tuple(quarantined), tuple(invalid), tuple(interrupted)
    )
