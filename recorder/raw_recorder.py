"""Buffered, versioned recording of normalized source data and lifecycle state."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median

import pyarrow as pa  # type: ignore[import-untyped]

from market.bootstrap import BootstrapSnapshot
from market.clock import ClockOffsetSample
from market.events import BookUpdateEvent, TradeEvent
from market.orderbook import TrustedBookSnapshot
from recorder.catalog import DatasetCatalog, DatasetFileManifest, git_revision, stable_fingerprint
from recorder.durability import DiskGuard, finalize_parquet
from recorder.raw_models import (
    CapturedRawRecord,
    CaptureMetadata,
    LifecycleRecord,
    RawEventType,
    RuntimeSample,
)
from recorder.raw_schemas import SCHEMA_VERSIONS, SCHEMAS, flatten_raw
from recorder.versions import DATASET_VERSION


class RawRecorderBufferFullError(RuntimeError):
    """Raw data cannot be queued without an explicit loss."""


@dataclass(frozen=True, slots=True)
class RawRecorderDiagnostics:
    queued: int
    rows_written: int
    files_written: int
    batches_written: int
    last_capture_seq: int | None
    last_error: str | None


_STOP = object()


class RawParquetRecorder:
    def __init__(
        self,
        data_directory: Path,
        *,
        capture_session_id: str,
        exchange: str = "binance",
        market_type: str = "usdm",
        batch_size: int = 2_000,
        flush_interval_seconds: float = 2.0,
        queue_size: int = 100_000,
        configuration: object | None = None,
        catalog: DatasetCatalog | None = None,
        software_revision: str | None = None,
        disk_guard: DiskGuard | None = None,
    ) -> None:
        if batch_size <= 0 or flush_interval_seconds <= 0 or queue_size <= 0:
            raise ValueError("raw recorder limits must be positive")
        if not capture_session_id:
            raise ValueError("capture_session_id is required")
        self._data_directory = data_directory
        self._disk_guard = disk_guard
        self.capture_session_id = capture_session_id
        self._exchange = exchange
        self._market_type = market_type
        self._batch_size = batch_size
        self._flush_interval_seconds = flush_interval_seconds
        self._queue: asyncio.Queue[CapturedRawRecord | object] = asyncio.Queue(queue_size)
        self._configuration_fingerprint = stable_fingerprint(configuration or {})
        self._catalog = catalog or DatasetCatalog(data_directory)
        self._software_revision = software_revision or git_revision()
        self._worker: asyncio.Task[None] | None = None
        self._last_capture_seq: int | None = None
        self._rows_written = 0
        self._files_written = 0
        self._batches_written = 0
        self._last_error: str | None = None
        self._part = 0
        self._clock_offsets: deque[float] = deque(maxlen=1_000)
        self._clock_rtts: deque[float] = deque(maxlen=1_000)
        self._resync_count: dict[str, int] = {}
        self._sequence_gap_count: dict[str, int] = {}

    async def start(self) -> None:
        if self._worker is not None:
            raise RuntimeError("raw recorder is already started")
        self._worker = asyncio.create_task(self._run(), name="raw-parquet-recorder")

    def _metadata(self, capture_seq: int) -> CaptureMetadata:
        return CaptureMetadata(
            capture_seq=capture_seq,
            capture_session_id=self.capture_session_id,
            exchange=self._exchange,
            market_type=self._market_type,
        )

    async def _record(self, record: CapturedRawRecord) -> None:
        worker = self._worker
        if worker is None:
            raise RuntimeError("raw recorder has not been started")
        if worker.done():
            await worker
            raise RuntimeError("raw recorder stopped unexpectedly")
        sequence = record.metadata.capture_seq
        if self._last_capture_seq is not None and sequence <= self._last_capture_seq:
            raise ValueError(
                f"capture_seq must increase: previous={self._last_capture_seq}, current={sequence}"
            )
        self._last_capture_seq = sequence
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull as exc:
            raise RawRecorderBufferFullError(
                f"raw recorder queue reached {self._queue.maxsize} rows"
            ) from exc

    async def record_market(self, event: TradeEvent | BookUpdateEvent, *, capture_seq: int) -> None:
        event_type = RawEventType.TRADE if isinstance(event, TradeEvent) else RawEventType.DEPTH
        await self._record(
            CapturedRawRecord(
                metadata=self._metadata(capture_seq),
                event_type=event_type,
                symbol=event.symbol,
                exchange_time=event.exchange_event_time,
                local_time=event.local_receive_time,
                monotonic_ns=event.local_receive_monotonic_ns,
                payload=event,
            )
        )

    async def record_bootstrap(
        self,
        snapshot: BootstrapSnapshot,
        *,
        capture_seq: int,
        local_time: datetime,
        monotonic_ns: int,
    ) -> None:
        await self._record(
            CapturedRawRecord(
                metadata=self._metadata(capture_seq),
                event_type=RawEventType.BOOTSTRAP,
                symbol=snapshot.snapshot.symbol,
                exchange_time=snapshot.snapshot.exchange_event_time,
                local_time=local_time,
                monotonic_ns=monotonic_ns,
                payload=snapshot,
            )
        )

    async def record_trusted_book(self, snapshot: TrustedBookSnapshot, *, capture_seq: int) -> None:
        await self._record(
            CapturedRawRecord(
                metadata=self._metadata(capture_seq),
                event_type=RawEventType.TRUSTED_BOOK,
                symbol=snapshot.symbol,
                exchange_time=snapshot.exchange_event_time,
                local_time=snapshot.timestamp,
                monotonic_ns=snapshot.timestamp_monotonic_ns,
                payload=snapshot,
            )
        )

    async def record_lifecycle(
        self,
        event: LifecycleRecord,
        *,
        capture_seq: int,
        book: bool = False,
    ) -> None:
        self._resync_count[event.symbol] = max(
            self._resync_count.get(event.symbol, 0), event.resync_count
        )
        self._sequence_gap_count[event.symbol] = max(
            self._sequence_gap_count.get(event.symbol, 0), event.sequence_gap_count
        )
        await self._record(
            CapturedRawRecord(
                metadata=self._metadata(capture_seq),
                event_type=(RawEventType.BOOK_LIFECYCLE if book else RawEventType.STREAM_LIFECYCLE),
                symbol=event.symbol,
                exchange_time=None,
                local_time=event.local_time,
                monotonic_ns=event.monotonic_ns,
                payload=event,
            )
        )

    async def record_clock_sample(
        self,
        sample: ClockOffsetSample,
        *,
        symbol: str,
        capture_seq: int,
        monotonic_timestamp_ns: int,
    ) -> None:
        if sample.accepted:
            self._clock_offsets.append(sample.offset_ms)
            self._clock_rtts.append(sample.rtt_ms)
        await self._record(
            CapturedRawRecord(
                metadata=self._metadata(capture_seq),
                event_type=RawEventType.CLOCK_SAMPLE,
                symbol=symbol,
                exchange_time=None,
                local_time=sample.sampled_at,
                monotonic_ns=monotonic_timestamp_ns,
                payload=sample,
            )
        )

    async def record_runtime_sample(
        self, sample: RuntimeSample, *, symbol: str, capture_seq: int
    ) -> None:
        await self._record(
            CapturedRawRecord(
                self._metadata(capture_seq),
                RawEventType.RUNTIME_SAMPLE,
                symbol,
                None,
                sample.local_time,
                sample.monotonic_ns,
                sample,
            )
        )

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
        batch: list[CapturedRawRecord] = []
        flush_deadline: float | None = None
        stopping = False
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
                    assert isinstance(item, CapturedRawRecord)
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

    async def _flush(self, batch: list[CapturedRawRecord]) -> None:
        partitions: dict[tuple[RawEventType, str, str, str], list[CapturedRawRecord]] = defaultdict(
            list
        )
        for record in batch:
            utc_time = record.local_time.astimezone(UTC)
            partitions[
                (
                    record.event_type,
                    record.symbol,
                    utc_time.date().isoformat(),
                    f"{utc_time.hour:02d}",
                )
            ].append(record)

        for (event_type, symbol, date, hour), records in partitions.items():
            records.sort(key=lambda item: item.metadata.capture_seq)
            directory = (
                self._data_directory
                / "raw"
                / f"exchange={self._exchange}"
                / f"market={self._market_type}"
                / f"symbol={symbol}"
                / f"date={date}"
                / f"hour={hour}"
            )
            self._part += 1
            path = directory / (
                f"{event_type.value}-{self.capture_session_id}-part-{self._part:06d}.parquet"
            )
            manifest = await asyncio.to_thread(
                self._serialize_write,
                path,
                event_type,
                records,
            )
            await asyncio.to_thread(self._catalog.register, manifest)
            self._files_written += 1
            self._rows_written += len(records)
        self._batches_written += 1

    def _serialize_write(
        self, path: Path, event_type: RawEventType, records: list[CapturedRawRecord]
    ) -> DatasetFileManifest:
        table = pa.Table.from_pylist(
            [flatten_raw(record) for record in records], schema=SCHEMAS[event_type]
        )
        return self._write_atomic(table, path, event_type, records)

    def _write_atomic(
        self,
        table: pa.Table,
        path: Path,
        event_type: RawEventType,
        records: list[CapturedRawRecord],
    ) -> DatasetFileManifest:
        exchange_times = [item.exchange_time for item in records if item.exchange_time]
        local_times = [item.local_time for item in records]
        relative_path = path.relative_to(self._data_directory).as_posix()
        data_healthy = all(
            not isinstance(item.payload, LifecycleRecord) or item.payload.healthy
            for item in records
        )
        manifest = DatasetFileManifest(
            dataset_id=stable_fingerprint(
                {
                    "session": self.capture_session_id,
                    "path": relative_path,
                    "rows": len(records),
                }
            ),
            dataset_version=DATASET_VERSION,
            schema_version=SCHEMA_VERSIONS[event_type],
            exchange=self._exchange,
            market_type=self._market_type,
            symbol=records[0].symbol,
            start_exchange_time=(None if not exchange_times else min(exchange_times).isoformat()),
            end_exchange_time=(None if not exchange_times else max(exchange_times).isoformat()),
            start_local_time=min(local_times).isoformat(),
            end_local_time=max(local_times).isoformat(),
            first_capture_seq=records[0].metadata.capture_seq,
            last_capture_seq=records[-1].metadata.capture_seq,
            row_count=len(records),
            event_type=event_type.value,
            file_path=relative_path,
            file_size=0,
            created_at=datetime.now(UTC).isoformat(),
            capture_session_id=self.capture_session_id,
            software_revision=self._software_revision,
            configuration_fingerprint=self._configuration_fingerprint,
            clock_offset_median_ms=(
                None if not self._clock_offsets else median(self._clock_offsets)
            ),
            clock_rtt_median_ms=(None if not self._clock_rtts else median(self._clock_rtts)),
            resync_count=self._resync_count.get(records[0].symbol, 0),
            sequence_gap_count=self._sequence_gap_count.get(records[0].symbol, 0),
            data_healthy=data_healthy,
        )
        return finalize_parquet(table, path, manifest, self._disk_guard)

    def diagnostics(self) -> RawRecorderDiagnostics:
        return RawRecorderDiagnostics(
            queued=self._queue.qsize(),
            rows_written=self._rows_written,
            files_written=self._files_written,
            batches_written=self._batches_written,
            last_capture_seq=self._last_capture_seq,
            last_error=self._last_error,
        )
