"""One supervised public-data runtime with isolated symbol pipelines."""

from __future__ import annotations

import argparse
import asyncio
import signal
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic_ns
from typing import Any

import structlog

from app.diagnostics import BoundedDistribution, SymbolStatistics, detect_event_loop_stalls
from config.settings import Settings
from exchange.base import StreamLifecycleEvent, StreamLifecycleKind
from exchange.binance.market_ws import BinanceMarketWebSocket
from exchange.binance.orderbook_sync import BinanceBookSynchronizer
from exchange.binance.rest import BinancePublicRestClient
from features.engine import FeatureEngine
from features.models import FeatureSnapshot
from market.bootstrap import BootstrapSnapshot
from market.clock import ClockOffsetSample, ExchangeClockOffsetEstimator
from market.events import BookUpdateEvent, NormalizedMarketEvent, TradeEvent
from recorder.catalog import DatasetCatalog
from recorder.durability import (
    BackpressureState,
    CaptureSafetyError,
    DatasetLock,
    DiskGuard,
    backpressure_state,
    recover_dataset,
    write_json_atomic,
)
from recorder.feature_recorder import FeatureParquetRecorder
from recorder.raw_models import CaptureSequencer, LifecycleRecord, RuntimeSample
from recorder.raw_recorder import RawParquetRecorder

log = structlog.get_logger(__name__)


class SymbolPipeline:
    def __init__(self, runtime: CaptureRuntime, symbol: str, rest: BinancePublicRestClient) -> None:
        self.runtime, self.symbol = runtime, symbol
        settings = runtime.settings
        self.statistics = SymbolStatistics()
        self.stream = BinanceMarketWebSocket(
            symbol,
            base_url=settings.binance_ws_base_url,
            stale_after_seconds=settings.ws_stale_after_seconds,
            reconnect_initial_seconds=settings.ws_reconnect_initial_seconds,
            reconnect_max_seconds=settings.ws_reconnect_max_seconds,
        )
        self.book = BinanceBookSynchronizer(
            symbol,
            rest,
            max_buffered_events=settings.orderbook_max_buffered_events,
            stale_after_ms=settings.orderbook_stale_after_ms,
            resync_initial_seconds=settings.orderbook_resync_initial_seconds,
            resync_max_seconds=settings.orderbook_resync_max_seconds,
            bootstrap_handler=self.bootstrap,
            offset_provider=lambda: runtime.clock.status().offset_ms,
            hard_level_limit=settings.orderbook_hard_level_limit,
        )
        self.features = FeatureEngine(
            symbol,
            interval_ms=settings.feature_interval_ms,
            book_stale_after_ms=settings.orderbook_stale_after_ms,
            trade_stale_after_ms=settings.trade_stale_after_ms,
            mid_history_seconds=settings.mid_history_seconds,
            trade_history_seconds=settings.trade_history_seconds,
            imbalance_levels=settings.feature_imbalance_levels,
            clock_offset=runtime.clock,
        )
        self._last_state: tuple[str, int] | None = None

    async def bootstrap(self, snapshot: BootstrapSnapshot) -> None:
        if not self.runtime.accepting:
            return
        try:
            await self.runtime.raw.record_bootstrap(
                snapshot,
                capture_seq=self.runtime.sequence.next(),
                local_time=datetime.now(UTC),
                monotonic_ns=monotonic_ns(),
            )
            self.statistics.counts["bootstrap_snapshots"] += 1
        except Exception as exc:
            self.runtime.fail(exc)
            raise

    async def record_book_state(self) -> None:
        diagnostics = self.book.diagnostics()
        state = (diagnostics.state.value, self.book.sync_generation)
        if state == self._last_state:
            return
        self._last_state = state
        await self.runtime.raw.record_lifecycle(
            LifecycleRecord(
                symbol=self.symbol,
                component="orderbook",
                kind=state[0],
                local_time=datetime.now(UTC),
                monotonic_ns=monotonic_ns(),
                reason=diagnostics.last_desync_reason,
                resync_count=diagnostics.resync_count,
                sequence_gap_count=diagnostics.sequence_gap_count,
                healthy=diagnostics.is_healthy,
                connection_generation=self.book.connection_generation,
                sync_generation=self.book.sync_generation,
            ),
            capture_seq=self.runtime.sequence.next(),
            book=True,
        )

    async def market_event(self, event: NormalizedMarketEvent) -> None:
        if not self.runtime.accepting or not isinstance(event, (TradeEvent, BookUpdateEvent)):
            return
        try:
            await self.runtime.raw.record_market(event, capture_seq=self.runtime.sequence.next())
            self.features.on_market_activity(event.local_receive_monotonic_ns)
            stats = self.statistics
            if stats.last_event_ns is not None:
                stats.distributions["event_receive_interval_ms"].add(
                    (event.local_receive_monotonic_ns - stats.last_event_ns) / 1e6
                )
            stats.last_event_ns = event.local_receive_monotonic_ns
            stats.distributions["raw_event_lag_ms"].add(event.estimated_latency_ms)
            corrected = self.runtime.clock.corrected_event_lag_ms(
                local_receive_time=event.local_receive_time,
                exchange_event_time=event.exchange_event_time,
            )
            if corrected is not None:
                stats.distributions["corrected_event_lag_ms"].add(corrected)
            if isinstance(event, TradeEvent):
                stats.counts["trades_received"] += 1
                self.features.on_trade(event)
            else:
                stats.counts["depth_events_received"] += 1
                await self.book.on_depth_event(event)
                trusted = self.book.trusted_snapshot(
                    levels=max(self.runtime.settings.feature_imbalance_levels)
                )
                if trusted is None:
                    self.features.on_book_invalidated()
                else:
                    self.features.on_book_snapshot(trusted)
                    await self.runtime.raw.record_trusted_book(
                        trusted, capture_seq=self.runtime.sequence.next()
                    )
                    stats.counts["trusted_books_emitted"] += 1
                await self.record_book_state()
        except Exception as exc:
            self.runtime.fail(exc)
            raise

    async def lifecycle(self, event: StreamLifecycleEvent) -> None:
        if not self.runtime.accepting:
            return
        try:
            connected = event.kind is StreamLifecycleKind.CONNECTED
            self.statistics.counts[
                "websocket_connections" if connected else "websocket_disconnects"
            ] += 1
            if connected and event.connection_generation > 1:
                self.statistics.counts["websocket_reconnects"] += 1
            await self.runtime.raw.record_lifecycle(
                LifecycleRecord(
                    self.symbol,
                    "websocket",
                    event.kind.value,
                    datetime.now(UTC),
                    event.monotonic_ns,
                    reason=event.reason,
                    healthy=connected,
                    connection_generation=event.connection_generation,
                    sync_generation=self.book.sync_generation,
                ),
                capture_seq=self.runtime.sequence.next(),
            )
            if connected:
                await self.book.on_connected()
            else:
                await self.book.on_disconnected(event.reason or "connection lost")
            self.features.on_connection(connected, monotonic_timestamp_ns=event.monotonic_ns)
            await self.record_book_state()
        except Exception as exc:
            self.runtime.fail(exc)
            raise

    async def feature(self, snapshot: FeatureSnapshot) -> None:
        if not self.runtime.accepting:
            return
        await self.runtime.feature_recorder.record(
            snapshot, capture_seq=self.runtime.sequence.next()
        )
        self.statistics.counts["features_emitted"] += 1
        self.statistics.counts["feature_invalid_count"] += int(not snapshot.feature_valid)
        self.statistics.distributions["book_age_ms"].add(snapshot.health.book_age_ms)
        self.statistics.distributions["spread_bps"].add(snapshot.book.spread_bps)


class CaptureRuntime:
    def __init__(self, settings: Settings, rest: BinancePublicRestClient) -> None:
        if settings.feature_imbalance_levels != (1, 5, 10):
            raise CaptureSafetyError(
                "recorded feature schema supports exactly L1/L5/L10; extra depths need a new schema"
            )
        self.settings = settings
        self.session_id = uuid.uuid4().hex
        self.sequence = CaptureSequencer()
        self.catalog = DatasetCatalog(settings.data_directory)
        self.disk = DiskGuard(settings.data_directory, int(settings.min_free_disk_gb * 1e9))
        self.stop_requested = asyncio.Event()
        self.accepting = True
        self.invalid_reasons: list[str] = []
        self.started_at = datetime.now(UTC)
        self.started_ns = monotonic_ns()
        self.event_loop_lag = BoundedDistribution()
        self.clock = ExchangeClockOffsetEstimator(
            rest,
            sample_count=settings.clock_sync_sample_count,
            max_rtt_ms=settings.clock_sync_max_rtt_ms,
            refresh_interval_seconds=settings.clock_sync_interval_seconds,
            sample_handler=self.clock_sample,
        )
        self.raw = RawParquetRecorder(
            settings.data_directory,
            batch_size=settings.raw_recorder_batch_size,
            flush_interval_seconds=settings.raw_recorder_flush_interval_seconds,
            queue_size=settings.raw_recorder_queue_size,
            capture_session_id=self.session_id,
            catalog=self.catalog,
            configuration=settings.model_dump(mode="json"),
            disk_guard=self.disk,
        )
        self.feature_recorder = FeatureParquetRecorder(
            settings.data_directory,
            batch_size=settings.feature_recorder_batch_size,
            flush_interval_seconds=settings.feature_recorder_flush_interval_seconds,
            queue_size=settings.feature_recorder_queue_size,
            capture_session_id=self.session_id,
            catalog=self.catalog,
            configuration=settings.model_dump(mode="json"),
            disk_guard=self.disk,
        )
        self.pipelines = {symbol: SymbolPipeline(self, symbol, rest) for symbol in settings.symbols}
        self.summary_path = (
            settings.data_directory / "sessions" / self.session_id / "session-summary.json"
        )
        self.recovery: dict[str, Any] = {}
        self._last_health_ns = self.started_ns
        self._last_health_counts: dict[str, dict[str, int]] = {}
        self._free_disk_bytes: int | None = None

    def fail(self, error: Exception) -> None:
        reason = f"{type(error).__name__}: {error}"
        if reason not in self.invalid_reasons:
            self.invalid_reasons.append(reason)
        self.stop_requested.set()

    async def clock_sample(self, sample: ClockOffsetSample) -> None:
        if not self.accepting:
            return
        for symbol in self.pipelines:
            await self.raw.record_clock_sample(
                sample,
                symbol=symbol,
                capture_seq=self.sequence.next(),
                monotonic_timestamp_ns=monotonic_ns(),
            )

    async def runtime_sample(self, lag: float) -> None:
        if not self.accepting:
            return
        for symbol in self.pipelines:
            self.pipelines[symbol].statistics.distributions["event_loop_lag_ms"].add(lag)
            sample = RuntimeSample(
                datetime.now(UTC),
                monotonic_ns(),
                lag,
                self.raw.diagnostics().queued,
                self.feature_recorder.diagnostics().queued,
            )
            await self.raw.record_runtime_sample(
                sample, symbol=symbol, capture_seq=self.sequence.next()
            )

    def health(self) -> dict[str, Any]:
        elapsed = (monotonic_ns() - self.started_ns) / 1e9
        now_ns = monotonic_ns()
        window_seconds = max((now_ns - self._last_health_ns) / 1e9, 0.001)
        report = {
            "capture_session_id": self.session_id,
            "uptime_seconds": elapsed,
            "clock": asdict(self.clock.status()),
            "event_loop_lag_ms": self.event_loop_lag.summary(),
            "raw_recorder": asdict(self.raw.diagnostics()),
            "feature_recorder": asdict(self.feature_recorder.diagnostics()),
            "backpressure": {
                "raw": backpressure_state(
                    self.raw.diagnostics().queued, self.settings.raw_recorder_queue_size
                ).value,
                "features": backpressure_state(
                    self.feature_recorder.diagnostics().queued,
                    self.settings.feature_recorder_queue_size,
                ).value,
            },
            "free_disk_bytes": self._free_disk_bytes,
            "symbols": {
                symbol: {
                    **pipeline.statistics.summary(),
                    "book": asdict(pipeline.book.diagnostics()),
                    "connection": asdict(pipeline.stream.health()),
                    "rates_per_second": {
                        name: count / max(elapsed, 0.001)
                        for name, count in pipeline.statistics.counts.items()
                    },
                    "rolling_rates_per_second": {
                        name: (count - self._last_health_counts.get(symbol, {}).get(name, 0))
                        / window_seconds
                        for name, count in pipeline.statistics.counts.items()
                    },
                    "rate_window_seconds": window_seconds,
                }
                for symbol, pipeline in self.pipelines.items()
            },
        }
        self._last_health_ns = now_ns
        self._last_health_counts = {s: dict(p.statistics.counts) for s, p in self.pipelines.items()}
        return report

    async def supervise(self) -> None:
        loop = asyncio.get_running_loop()
        next_report = loop.time() + self.settings.health_report_interval_seconds
        while not self.stop_requested.is_set():
            try:
                free = await asyncio.to_thread(self.disk.check)
                self._free_disk_bytes = free
                for diagnostics, capacity in (
                    (self.raw.diagnostics(), self.settings.raw_recorder_queue_size),
                    (
                        self.feature_recorder.diagnostics(),
                        self.settings.feature_recorder_queue_size,
                    ),
                ):
                    pressure = backpressure_state(diagnostics.queued, capacity)
                    if diagnostics.last_error:
                        raise CaptureSafetyError(diagnostics.last_error)
                    if pressure is BackpressureState.CRITICAL:
                        raise CaptureSafetyError(
                            f"recorder CRITICAL: {diagnostics.queued}/{capacity}"
                        )
                if loop.time() >= next_report:
                    health = {**self.health(), "free_disk_bytes": free}
                    log.info("capture_health", **health)
                    await asyncio.to_thread(
                        write_json_atomic, self.summary_path.parent / "latest-health.json", health
                    )
                    next_report = loop.time() + self.settings.health_report_interval_seconds
            except Exception as exc:
                self.fail(exc)
                return
            try:
                await asyncio.wait_for(self.stop_requested.wait(), timeout=1)
            except TimeoutError:
                pass

    async def run(self) -> dict[str, Any]:
        # Ownership covers recovery, writers, summary, and final stream closure.
        with DatasetLock(self.settings.data_directory):
            recovery = await asyncio.to_thread(
                recover_dataset, self.settings.data_directory, repair=True
            )
            self.recovery = asdict(recovery)
            log.info("capture_recovery", **self.recovery)
            if not recovery.valid:
                raise CaptureSafetyError(f"startup integrity errors: {recovery.invalid}")
            await asyncio.to_thread(self.disk.check)
            await asyncio.to_thread(
                write_json_atomic,
                self.summary_path,
                {
                    "capture_session_id": self.session_id,
                    "status": "RUNNING",
                    "dataset_valid": False,
                    "started_at": self.started_at.isoformat(),
                    "configuration": self.settings.model_dump(mode="json"),
                },
            )
            await self.raw.start()
            await self.feature_recorder.start()
            stream_tasks = [
                asyncio.create_task(
                    p.stream.run(p.market_event, lifecycle_handler=p.lifecycle),
                    name=f"stream-{p.symbol}",
                )
                for p in self.pipelines.values()
            ]
            feature_tasks = [
                asyncio.create_task(p.features.run(p.feature), name=f"features-{p.symbol}")
                for p in self.pipelines.values()
            ]
            clock_task = asyncio.create_task(self.clock.run(), name="clock-offset")
            supervisor = asyncio.create_task(self.supervise(), name="capture-supervisor")
            stalls = asyncio.create_task(
                detect_event_loop_stalls(
                    self.stop_requested, self.event_loop_lag, sample_handler=self.runtime_sample
                )
            )
            wait_stop = asyncio.create_task(self.stop_requested.wait())
            duration = self.settings.capture_duration_seconds
            timer = (
                None
                if duration is None
                else asyncio.get_running_loop().call_later(duration, self.stop_requested.set)
            )
            tasks = [*stream_tasks, *feature_tasks, clock_task, supervisor, stalls, wait_stop]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task is not wait_stop:
                        try:
                            await task
                        except Exception as exc:
                            self.fail(exc)
                        else:
                            if not self.stop_requested.is_set():
                                self.fail(
                                    CaptureSafetyError(
                                        f"task exited unexpectedly: {task.get_name()}"
                                    )
                                )
            except asyncio.CancelledError:
                self.fail(CaptureSafetyError("capture task was cancelled"))
                raise
            finally:
                if timer is not None:
                    timer.cancel()
                self.accepting = False
                self.stop_requested.set()
                ended_at = datetime.now(UTC)
                for pipeline in self.pipelines.values():
                    await pipeline.features.stop()
                    await pipeline.book.stop()  # Cancels in-flight REST work before finalization.
                clock_task.cancel()
                await asyncio.gather(
                    *feature_tasks,
                    clock_task,
                    supervisor,
                    stalls,
                    wait_stop,
                    return_exceptions=True,
                )
                for pipeline in self.pipelines.values():
                    try:
                        await self.raw.record_lifecycle(
                            LifecycleRecord(
                                pipeline.symbol,
                                "capture",
                                "CAPTURE_STOPPED",
                                ended_at,
                                monotonic_ns(),
                                healthy=False,
                                connection_generation=pipeline.book.connection_generation,
                                sync_generation=pipeline.book.sync_generation,
                            ),
                            capture_seq=self.sequence.next(),
                        )
                    except Exception as exc:
                        self.fail(exc)
                for recorder in (self.raw, self.feature_recorder):
                    try:
                        await recorder.stop()
                    except Exception as exc:
                        self.fail(exc)
                try:
                    summary = await asyncio.to_thread(self.finish_summary, ended_at)
                finally:
                    for pipeline in self.pipelines.values():
                        await pipeline.stream.stop()
                    await asyncio.gather(*stream_tasks, return_exceptions=True)
            return summary

    def finish_summary(self, ended_at: datetime) -> dict[str, Any]:
        manifests = self.catalog.for_session(self.session_id)
        duration = (ended_at - self.started_at).total_seconds()
        symbols: dict[str, Any] = {}
        for symbol, pipeline in self.pipelines.items():
            entries = [entry for entry in manifests if entry.symbol == symbol]
            book = pipeline.book.diagnostics()
            invalid = list(self.invalid_reasons)
            if not pipeline.statistics.counts["features_emitted"]:
                invalid.append("no feature snapshots")
            if book.sequence_gap_count:
                invalid.append("depth sequence gaps")
            if book.crossed_book_count or book.buffer_overflow_count:
                invalid.append("book integrity failure")
            for kind, counter in (
                ("trade", "trades_received"),
                ("depth", "depth_events_received"),
                ("feature", "features_emitted"),
                ("trusted_book", "trusted_books_emitted"),
                ("orderbook_bootstrap_snapshot", "bootstrap_snapshots"),
            ):
                if (
                    sum(e.row_count for e in entries if e.event_type == kind)
                    != pipeline.statistics.counts[counter]
                ):
                    invalid.append(f"recorded row count differs from received count: {kind}")
            bytes_written = sum(entry.file_size for entry in entries)
            symbols[symbol] = {
                **pipeline.statistics.summary(),
                "started_at": self.started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "duration_seconds": duration,
                "rows_by_event_type": {
                    kind: sum(entry.row_count for entry in entries if entry.event_type == kind)
                    for kind in sorted({entry.event_type for entry in entries})
                },
                "files": len(entries),
                "bytes": bytes_written,
                "trades_per_second": pipeline.statistics.counts["trades_received"]
                / max(duration, 0.001),
                "depth_events_per_second": pipeline.statistics.counts["depth_events_received"]
                / max(duration, 0.001),
                "resyncs": book.resync_count,
                "sequence_gaps": book.sequence_gap_count,
                "crossed_books": book.crossed_book_count,
                "buffer_overflows": book.buffer_overflow_count,
                "clock": asdict(self.clock.status()),
                "connection_generation": pipeline.book.connection_generation,
                "sync_generation": pipeline.book.sync_generation,
                "storage_projection": {
                    kind: {
                        "gb_per_hour": sum(e.file_size for e in entries if e.event_type == kind)
                        / max(duration, 0.001)
                        * 3600
                        / 1e9,
                        "gb_per_day": sum(e.file_size for e in entries if e.event_type == kind)
                        / max(duration, 0.001)
                        * 86400
                        / 1e9,
                    }
                    for kind in sorted({e.event_type for e in entries})
                },
                "dataset_valid": not invalid,
                "invalid_reasons": invalid,
            }
        result = {
            "capture_session_id": self.session_id,
            "status": "FINALIZED",
            "started_at": self.started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "configuration": self.settings.model_dump(mode="json"),
            "symbols": symbols,
            "dataset_valid": all(value["dataset_valid"] for value in symbols.values()),
            "invalid_reasons": sorted(
                set(
                    self.invalid_reasons
                    + [
                        f"{symbol}: {reason}"
                        for symbol, stats in symbols.items()
                        for reason in stats["invalid_reasons"]
                    ]
                )
            ),
            "recovery": self.recovery,
            "clock": asdict(self.clock.status()),
            "event_loop_lag_ms": self.event_loop_lag.summary(),
            "disk_degraded": self.disk.degraded,
        }
        write_json_atomic(self.summary_path, result)
        return result


async def run_capture(settings: Settings) -> dict[str, Any]:
    async with BinancePublicRestClient(
        base_url=settings.binance_rest_base_url, timeout_seconds=settings.rest_timeout_seconds
    ) as rest:
        # Startup network outages are retried while SIGINT/SIGTERM remain responsive.
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        while not stop.is_set():
            try:
                for symbol in settings.symbols:
                    await rest.validate_symbol(symbol)
                break
            except Exception as exc:
                log.warning("startup_network_retry", error=str(exc))
                try:
                    await asyncio.wait_for(stop.wait(), timeout=5)
                except TimeoutError:
                    pass
        if stop.is_set():
            return {"status": "STOPPED_BEFORE_CAPTURE", "dataset_valid": False}
        runtime = CaptureRuntime(settings, rest)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, runtime.stop_requested.set)
        return await runtime.run()


def main() -> None:
    from app.main import configure_logging

    parser = argparse.ArgumentParser(description="Public-data multi-symbol capture")
    parser.add_argument("--symbols", default=None, help="comma-separated BTCUSDT,BTCUSDC")
    parser.add_argument("--data-directory", type=Path)
    parser.add_argument("--duration", type=float, help="seconds; omitted means continuous")
    parser.add_argument("--rotation-seconds", type=float)
    args = parser.parse_args()
    overrides: dict[str, Any] = {}
    if args.symbols:
        overrides["symbols"] = args.symbols
    if args.data_directory:
        overrides["data_directory"] = args.data_directory
    if args.duration is not None:
        overrides["capture_duration_seconds"] = args.duration
    if args.rotation_seconds is not None:
        overrides["raw_recorder_flush_interval_seconds"] = args.rotation_seconds
        overrides["feature_recorder_flush_interval_seconds"] = args.rotation_seconds
    settings = Settings(**overrides)
    configure_logging(settings.log_level.value)
    summary = asyncio.run(run_capture(settings))
    log.info("capture_finished", summary=summary)
    if not summary.get("dataset_valid"):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
