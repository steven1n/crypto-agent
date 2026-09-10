import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from time import monotonic_ns

import pytest

from app.capture import CaptureRuntime
from config.settings import Settings
from exchange.base import StreamLifecycleEvent, StreamLifecycleKind
from exchange.binance.market_ws import ConnectionHealth
from market.events import TradeSource
from recorder.catalog import DatasetCatalog
from recorder.durability import DatasetLock, recover_dataset
from recorder.frozen import freeze_dataset, verify_frozen
from replay.reconstruction import validate_raw_reconstruction
from replay.streaming import iter_session
from research.batch import BatchConfiguration, run_batch
from tests.helpers import depth_event, market_snapshot, trade_event


class FakeRest:
    async def server_time_ms(self):
        return int(datetime.now(UTC).timestamp() * 1000)

    async def book_snapshot(self, symbol, *, limit=1_000):
        await asyncio.sleep(0.001)
        return replace(
            market_snapshot(symbol=symbol),
            created_at=datetime.now(UTC),
            created_monotonic_ns=monotonic_ns(),
        )


def test_capture_rejects_unrecordable_extra_feature_depths_before_start(tmp_path):
    from recorder.durability import CaptureSafetyError

    with pytest.raises(CaptureSafetyError, match="extra depths"):
        CaptureRuntime(
            Settings(
                _env_file=None, data_directory=tmp_path, feature_imbalance_levels=(1, 5, 10, 20)
            ),
            FakeRest(),
        )
    assert not list(tmp_path.iterdir())


class FakeStream:
    reconnect_symbol = None
    stall_snapshot = False

    def __init__(self, symbol, **kwargs):
        self.symbol = symbol
        self.stopped = asyncio.Event()
        self.generation = 1

    async def run(self, handler, *, lifecycle_handler):
        async def lifecycle(connected):
            await lifecycle_handler(
                StreamLifecycleEvent(
                    kind=StreamLifecycleKind.CONNECTED
                    if connected
                    else StreamLifecycleKind.DISCONNECTED,
                    connection_generation=self.generation,
                    monotonic_ns=monotonic_ns(),
                )
            )

        await lifecycle(True)
        index = 0
        reconnected = False
        while not self.stopped.is_set():
            if index == 4 and self.symbol == self.reconnect_symbol and not reconnected:
                await lifecycle(False)
                self.generation += 1
                await lifecycle(True)
                index = 0
                reconnected = True
            depth = replace(
                depth_event(
                    symbol=self.symbol,
                    first_update_id=99 if index == 0 else 101 + index,
                    final_update_id=101 + index,
                    previous_final_update_id=98 if index == 0 else 100 + index,
                    monotonic_ns=monotonic_ns(),
                ),
                local_receive_time=datetime.now(UTC),
                exchange_event_time=datetime.now(UTC),
            )
            await handler(depth)
            trade = replace(
                trade_event(
                    symbol=self.symbol, monotonic_ns=monotonic_ns(), aggregate_trade_id=index
                ),
                local_receive_time=datetime.now(UTC),
                exchange_event_time=datetime.now(UTC),
                source=TradeSource.INDIVIDUAL,
            )
            await handler(trade)
            index += 1
            await asyncio.sleep(0.01)
        await lifecycle(False)

    def health(self):
        return ConnectionHealth(True, False, 0, 0, 0, 0, self.generation - 1, None)

    async def stop(self):
        self.stopped.set()


@pytest.fixture
async def captured_dataset(tmp_path, monkeypatch):
    monkeypatch.setattr("app.capture.BinanceMarketWebSocket", FakeStream)
    monkeypatch.setattr(FakeStream, "reconnect_symbol", None)
    settings = Settings(
        _env_file=None,
        data_directory=tmp_path,
        capture_duration_seconds=0.35,
        feature_interval_ms=20,
        raw_recorder_batch_size=1000,
        feature_recorder_batch_size=100,
        raw_recorder_flush_interval_seconds=0.05,
        feature_recorder_flush_interval_seconds=0.05,
        min_free_disk_gb=0,
        health_report_interval_seconds=0.1,
    )
    runtime = CaptureRuntime(settings, FakeRest())
    summary = await runtime.run()
    assert summary["dataset_valid"], summary
    return tmp_path, runtime, summary


async def test_multisymbol_capture_bootstrap_rotation_and_graceful_finalization(captured_dataset):
    root, runtime, summary = captured_dataset
    assert set(summary["symbols"]) == {"BTCUSDT", "BTCUSDC"}
    all_entries = DatasetCatalog(root).entries()
    for symbol, stats in summary["symbols"].items():
        assert stats["rows_by_event_type"]["orderbook_bootstrap_snapshot"] == 1
        assert stats["rows_by_event_type"]["feature"] > 0
        assert stats["rows_by_event_type"]["trade"] > 0
        assert stats["connection_generation"] == 1
        assert stats["sync_generation"] == 1
        assert stats["files"] > 7  # rotation, not just one fragment per type
        entries = tuple(e for e in all_entries if e.symbol == symbol)
        result = validate_raw_reconstruction(iter_session(root, entries))
        assert result.valid, result
        assert result.snapshots_compared > 0
        assert result.mismatches == result.sequence_gap_count == 0
    assert runtime.raw.diagnostics().queued == 0
    assert runtime.feature_recorder.diagnostics().queued == 0
    assert not list(root.rglob("*.tmp"))
    with DatasetLock(root):
        assert recover_dataset(root).valid


async def test_symbol_reconnect_does_not_reset_other_symbol(tmp_path, monkeypatch):
    monkeypatch.setattr("app.capture.BinanceMarketWebSocket", FakeStream)
    monkeypatch.setattr(FakeStream, "reconnect_symbol", "BTCUSDC")
    settings = Settings(
        _env_file=None,
        data_directory=tmp_path,
        capture_duration_seconds=0.3,
        feature_interval_ms=20,
        min_free_disk_gb=0,
    )
    runtime = CaptureRuntime(settings, FakeRest())
    summary = await runtime.run()
    usdt, usdc = summary["symbols"]["BTCUSDT"], summary["symbols"]["BTCUSDC"]
    assert usdt["connection_generation"] == usdt["sync_generation"] == 1
    assert usdc["connection_generation"] == usdc["sync_generation"] == 2
    assert usdc["resyncs"] == 1 and usdt["resyncs"] == 0
    assert all(e.resync_count == 0 for e in runtime.catalog.entries() if e.symbol == "BTCUSDT")
    assert usdt["counts"]["features_emitted"] > 0 and usdc["counts"]["features_emitted"] > 0
    for symbol in ("BTCUSDT", "BTCUSDC"):
        entries = tuple(e for e in runtime.catalog.entries() if e.symbol == symbol)
        result = validate_raw_reconstruction(iter_session(tmp_path, entries))
        assert result.valid, result


async def test_restart_gets_new_session_and_freeze_is_repeatable(captured_dataset, monkeypatch):
    root, runtime, _ = captured_dataset
    restarted = CaptureRuntime(runtime.settings, FakeRest())
    assert restarted.session_id != runtime.session_id
    await restarted.run()
    entries = DatasetCatalog(root).entries()
    assert len({entry.capture_session_id for entry in entries}) == 2
    frozen = freeze_dataset(root, entries)
    assert freeze_dataset(root, entries) == frozen
    data = verify_frozen(root, frozen)
    assert data["manifest_fingerprint"] == data["dataset_id"]
    file = root / entries[0].file_path
    file.write_bytes(file.read_bytes() + b"changed")
    with pytest.raises(Exception, match="checksum|size"):
        verify_frozen(root, frozen)


async def test_batch_consumes_both_symbols_and_repeats_exactly(captured_dataset):
    root, _, _ = captured_dataset
    configuration = BatchConfiguration(bootstrap_replications=10)
    path = run_batch(root, configuration=configuration)
    first = path.read_bytes()
    assert run_batch(root, configuration=configuration).read_bytes() == first
    report = json.loads(first)
    assert len(report["sessions"]) == 2
    assert report["dataset_valid"]
    assert all(s["reconstruction"]["mismatches"] == 0 for s in report["sessions"])
    assert all(s["recomputed_features"] > 0 for s in report["sessions"])


async def test_shutdown_cancels_pending_snapshot_request(tmp_path, monkeypatch):
    monkeypatch.setattr("app.capture.BinanceMarketWebSocket", FakeStream)

    class SlowRest(FakeRest):
        cancelled = False

        async def book_snapshot(self, symbol, *, limit=1_000):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    rest = SlowRest()
    settings = Settings(
        _env_file=None, data_directory=tmp_path, capture_duration_seconds=0.05, min_free_disk_gb=0
    )
    result = await asyncio.wait_for(CaptureRuntime(settings, rest).run(), timeout=3)
    assert rest.cancelled
    assert not result["dataset_valid"]
    assert (tmp_path / "sessions" / result["capture_session_id"] / "session-summary.json").exists()


async def test_bootstrap_contains_exact_levels_timing_and_generation(captured_dataset):
    from market.bootstrap import BootstrapSnapshot

    root, runtime, _ = captured_dataset
    entries = tuple(e for e in runtime.catalog.entries() if e.symbol == "BTCUSDT")
    bootstrap = next(
        item.payload
        for item in iter_session(root, entries)
        if isinstance(item.payload, BootstrapSnapshot)
    )
    assert bootstrap.snapshot.bids == market_snapshot().bids
    assert bootstrap.snapshot.asks == market_snapshot().asks
    assert bootstrap.snapshot.last_update_id == 100
    assert bootstrap.snapshot_limit == 1000
    assert bootstrap.response_receive_time >= bootstrap.request_start_time
    assert bootstrap.response_receive_monotonic_ns >= bootstrap.request_start_monotonic_ns
    assert bootstrap.connection_generation == bootstrap.sync_generation == 1
    assert bootstrap.resync_reason == "initial WebSocket connection"


async def test_raw_reconstruction_detects_one_decimal_quantity_difference(captured_dataset):
    from decimal import Decimal

    from market.orderbook import TrustedBookSnapshot

    root, runtime, _ = captured_dataset
    entries = tuple(e for e in runtime.catalog.entries() if e.symbol == "BTCUSDT")
    stream = list(iter_session(root, entries))
    index = next(
        i for i, item in enumerate(stream) if isinstance(item.payload, TrustedBookSnapshot)
    )
    payload = stream[index].payload
    stream[index] = replace(
        stream[index],
        payload=replace(
            payload, best_bid_quantity=payload.best_bid_quantity + Decimal("0.00000000000000001")
        ),
    )
    result = validate_raw_reconstruction(stream)
    assert not result.valid and result.mismatches == 1
    assert "best_bid_quantity" in result.first_mismatch


async def test_batch_catalog_selection_retains_session_bootstrap(captured_dataset):
    from datetime import timedelta

    from recorder.frozen import select_manifests

    root, runtime, summary = captured_dataset
    start = datetime.fromisoformat(summary["started_at"]) + timedelta(seconds=0.1)
    selected = select_manifests(root, symbol="BTCUSDT", start=start)
    assert {e.symbol for e in selected} == {"BTCUSDT"}
    assert any(e.event_type == "orderbook_bootstrap_snapshot" for e in selected)
    assert {e.capture_session_id for e in selected} == {runtime.session_id}
    assert not select_manifests(root, start=start + timedelta(days=1))


async def test_frozen_dataset_compacts_only_into_new_version(captured_dataset, tmp_path):
    from recorder.compact import compact_dataset
    from recorder.compaction import compact_parquet_fragments
    from recorder.durability import CaptureSafetyError

    root, runtime, _ = captured_dataset
    entries = runtime.catalog.entries()
    frozen = freeze_dataset(root, entries)
    assert freeze_dataset(root, tuple(reversed(entries))) == frozen
    with pytest.raises(CaptureSafetyError, match="frozen"):
        compact_parquet_fragments(
            (root / entries[0].file_path,),
            root / "bad.parquet",
            remove_sources=True,
            minimum_source_age_seconds=0,
        )
    destination = root.parent / (root.name + "-compacted")
    assert compact_dataset(root, destination) > 0
    assert recover_dataset(destination).valid
    compacted = DatasetCatalog(destination).entries()
    assert sum(e.row_count for e in compacted) == sum(e.row_count for e in entries)
    assert len(compacted) < len(entries)
    new_frozen = freeze_dataset(destination, compacted)
    assert new_frozen.name != frozen.name
    verify_frozen(root, frozen)
    report = json.loads(
        run_batch(
            destination, configuration=BatchConfiguration(bootstrap_replications=5)
        ).read_text()
    )
    assert report["dataset_valid"]


async def test_runtime_critical_queue_degrades_session_and_stops(tmp_path, monkeypatch):
    from recorder.raw_recorder import RawParquetRecorder

    monkeypatch.setattr("app.capture.BinanceMarketWebSocket", FakeStream)
    original = RawParquetRecorder.diagnostics
    monkeypatch.setattr(
        RawParquetRecorder, "diagnostics", lambda self: replace(original(self), queued=90_000)
    )
    runtime = CaptureRuntime(
        Settings(
            _env_file=None,
            data_directory=tmp_path,
            capture_duration_seconds=0.1,
            min_free_disk_gb=0,
        ),
        FakeRest(),
    )
    summary = await runtime.run()
    assert not summary["dataset_valid"]
    assert any("CRITICAL" in reason for reason in summary["invalid_reasons"])


async def test_connection_source_clock_is_preserved_before_first_trade(tmp_path):
    from time import monotonic_ns

    from recorder.raw_models import LifecycleRecord

    runtime = CaptureRuntime(
        Settings(_env_file=None, data_directory=tmp_path, min_free_disk_gb=0), FakeRest()
    )
    await runtime.raw.start()
    source_ns = monotonic_ns() - 500_000
    await runtime.pipelines["BTCUSDC"].lifecycle(
        StreamLifecycleEvent(
            kind=StreamLifecycleKind.CONNECTED, connection_generation=1, monotonic_ns=source_ns
        )
    )
    await runtime.pipelines["BTCUSDC"].book.stop()
    await runtime.raw.stop()
    from replay.loader import ReplayDatasetLoader

    items = ReplayDatasetLoader(tmp_path).load(
        capture_session_id=runtime.session_id, symbol="BTCUSDC", required_event_types=frozenset()
    )
    event = next(
        i
        for i in items
        if isinstance(i.payload, LifecycleRecord) and i.payload.component == "websocket"
    )
    assert event.monotonic_ns == source_ns
