"""Non-blocking, bounded and batched Parquet feature recorder."""

from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]

from features.models import FeatureSnapshot
from recorder.catalog import DatasetCatalog, DatasetFileManifest, git_revision, stable_fingerprint
from recorder.durability import DiskGuard, finalize_parquet
from recorder.schemas import FEATURE_SCHEMA, flatten_feature
from recorder.versions import DATASET_VERSION, FEATURE_SCHEMA_VERSION


class RecorderBufferFullError(RuntimeError):
    """The recorder cannot keep up without dropping observations."""


@dataclass(frozen=True, slots=True)
class FeatureRecorderDiagnostics:
    queued: int
    rows_written: int
    files_written: int
    batches_written: int
    last_error: str | None


@dataclass(frozen=True, slots=True)
class FeatureRecord:
    snapshot: FeatureSnapshot
    capture_seq: int


_STOP = object()


class FeatureParquetRecorder:
    """Moves serialization and disk I/O off the market-data callback path."""

    def __init__(
        self,
        data_directory: Path,
        *,
        batch_size: int = 500,
        flush_interval_seconds: float = 1.0,
        queue_size: int = 10_000,
        run_id: str | None = None,
        capture_session_id: str | None = None,
        exchange: str = "binance",
        market_type: str = "usdm",
        configuration: object | None = None,
        catalog: DatasetCatalog | None = None,
        software_revision: str | None = None,
        disk_guard: DiskGuard | None = None,
    ) -> None:
        if batch_size <= 0 or flush_interval_seconds <= 0 or queue_size <= 0:
            raise ValueError("recorder limits must be positive")
        self._data_directory = data_directory
        self._disk_guard = disk_guard
        self._batch_size = batch_size
        self._flush_interval_seconds = flush_interval_seconds
        self._queue: asyncio.Queue[FeatureRecord | object] = asyncio.Queue(queue_size)
        self._run_id = run_id or uuid.uuid4().hex[:12]
        self.capture_session_id = capture_session_id or self._run_id
        self._exchange = exchange
        self._market_type = market_type
        self._configuration_fingerprint = stable_fingerprint(configuration or {})
        self._catalog = catalog or DatasetCatalog(data_directory)
        self._software_revision = software_revision or git_revision()
        self._worker: asyncio.Task[None] | None = None
        self._rows_written = 0
        self._files_written = 0
        self._batches_written = 0
        self._last_error: str | None = None
        self._part = 0
        self._next_sequence = 0
        self._last_capture_seq: int | None = None

    async def start(self) -> None:
        if self._worker is not None:
            raise RuntimeError("feature recorder is already started")
        self._worker = asyncio.create_task(self._run(), name="feature-parquet-recorder")

    async def record(self, snapshot: FeatureSnapshot, *, capture_seq: int | None = None) -> None:
        worker = self._worker
        if worker is None:
            raise RuntimeError("feature recorder has not been started")
        if worker.done():
            await worker
            raise RuntimeError("feature recorder stopped unexpectedly")
        sequence = self._next_sequence if capture_seq is None else capture_seq
        if capture_seq is None:
            self._next_sequence += 1
        if self._last_capture_seq is not None and sequence <= self._last_capture_seq:
            raise ValueError("feature capture_seq must be strictly increasing")
        self._last_capture_seq = sequence
        try:
            self._queue.put_nowait(FeatureRecord(snapshot, sequence))
        except asyncio.QueueFull as exc:
            raise RecorderBufferFullError(
                f"feature recorder queue reached {self._queue.maxsize} rows"
            ) from exc

    async def stop(self) -> None:
        worker = self._worker
        if worker is None:
            return
        if not worker.done():
            put = asyncio.create_task(self._queue.put(_STOP))
            await asyncio.wait({put, worker}, return_when=asyncio.FIRST_COMPLETED)
            if not put.done():
                put.cancel()
                await asyncio.gather(put, return_exceptions=True)
        try:
            await worker
        finally:
            self._worker = None

    async def _run(self) -> None:
        batch: list[FeatureRecord] = []
        stopping = False
        flush_deadline: float | None = None
        loop = asyncio.get_running_loop()
        try:
            while not stopping:
                timeout = self._flush_interval_seconds
                if flush_deadline is not None:
                    timeout = max(0.0, flush_deadline - loop.time())
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                except TimeoutError:
                    if batch:
                        await self._flush(batch)
                        batch = []
                    flush_deadline = None
                    continue
                self._queue.task_done()
                if item is _STOP:
                    stopping = True
                else:
                    assert isinstance(item, FeatureRecord)
                    if not batch:
                        flush_deadline = loop.time() + self._flush_interval_seconds
                    batch.append(item)
                if len(batch) >= self._batch_size:
                    await self._flush(batch)
                    batch = []
                    flush_deadline = None
            if batch:
                await self._flush(batch)
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: {exc}"
            raise

    async def _flush(self, batch: list[FeatureRecord]) -> None:
        partitions: dict[tuple[str, str], list[FeatureRecord]] = defaultdict(list)
        for record in batch:
            snapshot = record.snapshot
            partitions[(snapshot.created_at.date().isoformat(), snapshot.symbol)].append(record)

        for (date, symbol), records in partitions.items():
            records.sort(key=lambda item: item.capture_seq)
            directory = (
                self._data_directory
                / "features"
                / f"exchange={self._exchange}"
                / f"symbol={symbol}"
                / f"date={date}"
            )
            self._part += 1
            path = directory / f"features-{self._run_id}-part-{self._part:06d}.parquet"
            manifest = await asyncio.to_thread(self._serialize_write, path, records)
            await asyncio.to_thread(self._catalog.register, manifest)
            self._files_written += 1
            self._rows_written += len(records)
        self._batches_written += 1

    def _serialize_write(self, path: Path, records: list[FeatureRecord]) -> DatasetFileManifest:
        table = pa.Table.from_pylist(
            [
                flatten_feature(
                    record.snapshot,
                    capture_session_id=self.capture_session_id,
                    capture_seq=record.capture_seq,
                    exchange=self._exchange,
                    market_type=self._market_type,
                )
                for record in records
            ],
            schema=FEATURE_SCHEMA,
        )
        return self._write_atomic(table, path, records)

    def _write_atomic(
        self, table: pa.Table, path: Path, records: list[FeatureRecord]
    ) -> DatasetFileManifest:
        snapshots = [item.snapshot for item in records]
        exchange_times = [item.event_time for item in snapshots if item.event_time]
        local_times = [item.created_at for item in snapshots]
        relative_path = path.relative_to(self._data_directory).as_posix()
        manifest = DatasetFileManifest(
            dataset_id=stable_fingerprint(
                {
                    "session": self.capture_session_id,
                    "path": relative_path,
                    "rows": len(records),
                }
            ),
            dataset_version=DATASET_VERSION,
            schema_version=FEATURE_SCHEMA_VERSION,
            exchange=self._exchange,
            market_type=self._market_type,
            symbol=snapshots[0].symbol,
            start_exchange_time=(None if not exchange_times else min(exchange_times).isoformat()),
            end_exchange_time=(None if not exchange_times else max(exchange_times).isoformat()),
            start_local_time=min(local_times).isoformat(),
            end_local_time=max(local_times).isoformat(),
            first_capture_seq=records[0].capture_seq,
            last_capture_seq=records[-1].capture_seq,
            row_count=len(records),
            event_type="feature",
            file_path=relative_path,
            file_size=0,
            created_at=datetime.now(UTC).isoformat(),
            capture_session_id=self.capture_session_id,
            software_revision=self._software_revision,
            configuration_fingerprint=self._configuration_fingerprint,
            clock_offset_median_ms=None,
            clock_rtt_median_ms=None,
            resync_count=0,
            sequence_gap_count=0,
            data_healthy=all(item.feature_valid for item in snapshots),
        )
        return finalize_parquet(table, path, manifest, self._disk_guard)

    def diagnostics(self) -> FeatureRecorderDiagnostics:
        return FeatureRecorderDiagnostics(
            queued=self._queue.qsize(),
            rows_written=self._rows_written,
            files_written=self._files_written,
            batches_written=self._batches_written,
            last_error=self._last_error,
        )
