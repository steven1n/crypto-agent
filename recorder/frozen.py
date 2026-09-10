"""Immutable content-addressed dataset selections for reproducible research."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from recorder.catalog import DatasetCatalog, DatasetFileManifest, stable_fingerprint
from recorder.durability import (
    CaptureSafetyError,
    DatasetLock,
    file_sha256,
    validate_file,
    write_json_atomic,
)


def select_manifests(
    root: Path,
    *,
    symbol: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> tuple[DatasetFileManifest, ...]:
    """Select entire sessions overlapping the UTC interval, retaining bootstrap context."""
    entries = DatasetCatalog(root).entries()
    sessions = {
        e.capture_session_id
        for e in entries
        if (symbol is None or e.symbol == symbol.upper())
        and (start is None or datetime.fromisoformat(e.end_local_time) >= start)
        and (end is None or datetime.fromisoformat(e.start_local_time) < end)
    }
    return tuple(
        sorted(
            (
                e
                for e in entries
                if e.capture_session_id in sessions
                and (symbol is None or e.symbol == symbol.upper())
            ),
            key=lambda e: (e.capture_session_id, e.first_capture_seq, e.file_path),
        )
    )


def freeze_dataset(root: Path, entries: tuple[DatasetFileManifest, ...]) -> Path:
    if not entries:
        raise ValueError("cannot freeze an empty dataset")
    entries = tuple(
        sorted(entries, key=lambda e: (e.capture_session_id, e.first_capture_seq, e.file_path))
    )
    if len({e.dataset_id for e in entries}) != len(entries):
        raise CaptureSafetyError("duplicate file IDs in frozen selection")
    with DatasetLock(root):
        for entry in entries:
            validate_file(root, entry)
        sessions = sorted({entry.capture_session_id for entry in entries})
        summaries = []
        for session in sessions:
            summary_path = root / "sessions" / session / "session-summary.json"
            if not summary_path.exists():
                raise CaptureSafetyError(f"session {session} lacks a final session summary")
            summary = json.loads(summary_path.read_text())
            if summary.get("status") != "FINALIZED" or not summary.get("dataset_valid"):
                raise CaptureSafetyError(f"session {session} is unfinished or invalid")
            summaries.append({"session": session, "sha256": file_sha256(summary_path)})
        files = [
            {"manifest": asdict(e), "sha256": file_sha256(root / e.file_path)} for e in entries
        ]
        payload = {
            "dataset_version": 1,
            "files": files,
            "session_summaries": summaries,
            "symbols": sorted({e.symbol for e in entries}),
            "schema_versions": {
                kind: sorted({e.schema_version for e in entries if e.event_type == kind})
                for kind in sorted({e.event_type for e in entries})
            },
            "time_range": [
                min(e.start_local_time for e in entries),
                max(e.end_local_time for e in entries),
            ],
            "health_status": "VALID",
        }
        fingerprint = stable_fingerprint(payload)
        frozen = {**payload, "dataset_id": fingerprint, "manifest_fingerprint": fingerprint}
        path = root / "frozen" / f"{fingerprint}.json"
        if path.exists():
            if json.loads(path.read_text()) != frozen:
                raise CaptureSafetyError("immutable frozen manifest was modified")
        else:
            write_json_atomic(path, frozen)
        return path


def verify_frozen(root: Path, path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(path.read_text())
    core = {k: v for k, v in payload.items() if k not in ("dataset_id", "manifest_fingerprint")}
    expected = stable_fingerprint(core)
    if payload["dataset_id"] != expected or payload["manifest_fingerprint"] != expected:
        raise CaptureSafetyError("frozen manifest fingerprint mismatch")
    for item in payload["files"]:
        manifest = DatasetFileManifest.from_dict(item["manifest"])
        validate_file(root, manifest)
        if file_sha256(root / manifest.file_path) != item["sha256"]:
            raise CaptureSafetyError("frozen file contents changed")
    for item in payload["session_summaries"]:
        if (
            file_sha256(root / "sessions" / item["session"] / "session-summary.json")
            != item["sha256"]
        ):
            raise CaptureSafetyError("frozen session summary changed")
    return payload


def reject_frozen_sources(paths: tuple[Path, ...]) -> None:
    resolved = {str(path.resolve()) for path in paths}
    roots = {
        parent
        for path in paths
        for parent in path.resolve().parents
        if (parent / "catalog").is_dir()
    }
    for root in roots:
        for path in (root / "frozen").glob("*.json"):
            payload = json.loads(path.read_text())
            if any(
                str((root / item["manifest"]["file_path"]).resolve()) in resolved
                for item in payload["files"]
            ):
                raise CaptureSafetyError(
                    "frozen source cannot be removed or recompacted; create a new dataset version"
                )
