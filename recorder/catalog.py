"""Small atomic JSON dataset catalog with no external service dependency."""

from __future__ import annotations

import hashlib
import json
import subprocess
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def stable_fingerprint(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def git_revision(work_directory: Path | None = None) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=work_directory,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    revision = result.stdout.strip()
    return revision or None


@dataclass(frozen=True, slots=True)
class DatasetFileManifest:
    dataset_id: str
    dataset_version: int
    schema_version: int
    exchange: str
    market_type: str
    symbol: str
    start_exchange_time: str | None
    end_exchange_time: str | None
    start_local_time: str
    end_local_time: str
    first_capture_seq: int
    last_capture_seq: int
    row_count: int
    event_type: str
    file_path: str
    file_size: int
    created_at: str
    capture_session_id: str
    software_revision: str | None
    configuration_fingerprint: str
    clock_offset_median_ms: float | None
    clock_rtt_median_ms: float | None
    resync_count: int
    sequence_gap_count: int
    data_healthy: bool
    content_sha256: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> DatasetFileManifest:
        return cls(**value)


class DatasetCatalog:
    """Catalog entries are rewritten atomically as one deterministic JSON document."""

    def __init__(self, data_directory: Path) -> None:
        self.root = data_directory
        self.path = data_directory / "catalog" / "manifest.json"
        self._lock = threading.Lock()

    def entries(self) -> tuple[DatasetFileManifest, ...]:
        with self._lock:
            return self._read_unlocked()

    def _read_unlocked(self) -> tuple[DatasetFileManifest, ...]:
        if not self.path.exists():
            return ()
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("dataset catalog root must be a list")
        return tuple(DatasetFileManifest.from_dict(item) for item in raw)

    def register(self, manifest: DatasetFileManifest) -> None:
        from recorder.durability import write_json_atomic

        with self._lock:
            entries = list(self._read_unlocked())
            if any(item.dataset_id == manifest.dataset_id for item in entries):
                raise ValueError(f"duplicate dataset_id: {manifest.dataset_id}")
            entries.append(manifest)
            entries.sort(key=lambda item: (item.first_capture_seq, item.file_path))
            write_json_atomic(self.path, [asdict(item) for item in entries])

    def for_session(self, capture_session_id: str) -> tuple[DatasetFileManifest, ...]:
        return tuple(
            item for item in self.entries() if item.capture_session_id == capture_session_id
        )


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()
