import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from recorder.catalog import DatasetCatalog
from recorder.compaction import CompactionError, compact_parquet_fragments
from recorder.durability import (
    BackpressureState,
    CaptureSafetyError,
    DatasetLock,
    DiskGuard,
    backpressure_state,
    recover_dataset,
    validate_file,
    write_json_atomic,
)
from recorder.raw_recorder import RawParquetRecorder
from tests.helpers import trade_event


@pytest.mark.parametrize(
    ("size", "expected"),
    [
        (0, "NORMAL"),
        (69, "NORMAL"),
        (70, "WARNING"),
        (89, "WARNING"),
        (90, "CRITICAL"),
        (100, "CRITICAL"),
    ],
)
def test_backpressure_levels(size, expected):
    assert backpressure_state(size, 100) is BackpressureState(expected)


def test_disk_reserve_is_checked_before_any_file_creation(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "recorder.durability.shutil.disk_usage", lambda _: type("Usage", (), {"free": 10})()
    )
    guard = DiskGuard(tmp_path, min_free_bytes=11)
    with pytest.raises(CaptureSafetyError, match="reserve"):
        guard.check()
    assert guard.degraded
    assert not list(tmp_path.rglob("*.parquet"))


def test_dataset_exclusive_ownership(tmp_path):
    with DatasetLock(tmp_path):
        with pytest.raises(CaptureSafetyError, match="owned"):
            with DatasetLock(tmp_path):
                pass
    with DatasetLock(tmp_path):
        pass


async def test_startup_quarantines_temp_and_reconciles_only_embedded_manifest(tmp_path):
    recorder = RawParquetRecorder(tmp_path, capture_session_id="session", batch_size=10)
    await recorder.start()
    await recorder.record_market(trade_event(monotonic_ns=1), capture_seq=0)
    await recorder.stop()
    catalog = DatasetCatalog(tmp_path)
    entry = catalog.entries()[0]
    write_json_atomic(catalog.path, [])  # crash between rename and catalog update
    temporary = tmp_path / "raw" / ".unfinished.tmp"
    temporary.write_bytes(b"partial parquet")
    with DatasetLock(tmp_path):
        recovery = recover_dataset(tmp_path, repair=True)
    assert recovery.valid
    assert recovery.reconciled == (entry.file_path,)
    assert len(recovery.quarantined) == 1
    assert not temporary.exists()
    assert len(list((tmp_path / "quarantine").iterdir())) == 1
    validate_file(tmp_path, catalog.entries()[0])


async def test_recovery_reports_missing_duplicate_and_invalid_schema(tmp_path):
    recorder = RawParquetRecorder(tmp_path, capture_session_id="s")
    await recorder.start()
    await recorder.record_market(trade_event(monotonic_ns=1), capture_seq=0)
    await recorder.stop()
    catalog = DatasetCatalog(tmp_path)
    entry = catalog.entries()[0]
    path = tmp_path / entry.file_path
    original = path.read_bytes()
    path.unlink()
    assert not recover_dataset(tmp_path).valid
    path.write_bytes(original)
    doc = json.loads(catalog.path.read_text())
    write_json_atomic(catalog.path, doc + doc)
    assert "duplicate" in recover_dataset(tmp_path).invalid[0]
    write_json_atomic(catalog.path, doc)
    pq.write_table(pa.table({"unknown": [1]}), path)
    assert not recover_dataset(tmp_path).valid


def test_unknown_orphan_is_not_silently_cataloged(tmp_path):
    folder = tmp_path / "raw"
    folder.mkdir()
    pq.write_table(pa.table({"unknown": [1]}), folder / "orphan.parquet")
    result = recover_dataset(tmp_path, repair=True)
    assert not result.valid
    assert DatasetCatalog(tmp_path).entries() == ()


def test_interrupted_session_is_marked_invalid(tmp_path):
    path = tmp_path / "sessions" / "s" / "session-summary.json"
    write_json_atomic(path, {"capture_session_id": "s", "status": "RUNNING"})
    result = recover_dataset(tmp_path, repair=True)
    assert result.interrupted_sessions == ("s",)
    state = json.loads(path.read_text())
    assert state["status"] == "INTERRUPTED"
    assert not state["dataset_valid"]


async def test_disk_writer_failure_does_not_deadlock_shutdown(tmp_path, monkeypatch):
    recorder = RawParquetRecorder(
        tmp_path,
        capture_session_id="s",
        batch_size=1,
        queue_size=2,
        disk_guard=DiskGuard(tmp_path, 100),
    )
    monkeypatch.setattr(
        "recorder.durability.shutil.disk_usage", lambda _: type("Usage", (), {"free": 0})()
    )
    await recorder.start()
    await recorder.record_market(trade_event(monotonic_ns=1), capture_seq=0)
    import asyncio

    with pytest.raises(CaptureSafetyError):
        await asyncio.wait_for(recorder.stop(), timeout=2)
    assert not DatasetCatalog(tmp_path).entries()


async def test_cataloged_source_removal_is_rejected_before_output(tmp_path):
    recorder = RawParquetRecorder(tmp_path, capture_session_id="s")
    await recorder.start()
    await recorder.record_market(trade_event(monotonic_ns=1), capture_seq=0)
    await recorder.stop()
    entry = DatasetCatalog(tmp_path).entries()[0]
    output = tmp_path / "new.parquet"
    with pytest.raises(CompactionError, match="new-version"):
        compact_parquet_fragments(
            (tmp_path / entry.file_path,), output, remove_sources=True, minimum_source_age_seconds=0
        )
    assert not output.exists()


async def test_shutdown_drains_high_queue_without_losing_rows(tmp_path):
    recorder = RawParquetRecorder(
        tmp_path, capture_session_id="s", queue_size=100, batch_size=20, flush_interval_seconds=60
    )
    await recorder.start()
    for i in range(85):
        await recorder.record_market(trade_event(monotonic_ns=i), capture_seq=i)
    assert recorder.diagnostics().queued == 85
    await recorder.stop()
    assert recorder.diagnostics().queued == 0
    assert recorder.diagnostics().rows_written == 85
    assert len(DatasetCatalog(tmp_path).entries()) == 5


async def test_atomic_rename_precedes_catalog_and_partial_writer_is_not_healthy(
    tmp_path, monkeypatch
):
    import os

    original_replace = os.replace
    renames = []

    def observed_replace(source, destination):
        if str(destination).endswith(".parquet"):
            assert not DatasetCatalog(tmp_path).entries()
            assert pq.ParquetFile(source).metadata.num_rows == 1
            renames.append(destination)
        original_replace(source, destination)

    monkeypatch.setattr("recorder.durability.os.replace", observed_replace)
    recorder = RawParquetRecorder(tmp_path, capture_session_id="s")
    await recorder.start()
    await recorder.record_market(trade_event(monotonic_ns=1), capture_seq=0)
    await recorder.stop()
    assert len(renames) == len(DatasetCatalog(tmp_path).entries()) == 1


async def test_write_interruption_leaves_only_quarantinable_temp(tmp_path, monkeypatch):
    def partial_write(table, destination, **kwargs):
        destination.write_bytes(b"PAR1 interrupted")
        raise OSError("simulated power loss")

    monkeypatch.setattr("recorder.durability.pq.write_table", partial_write)
    recorder = RawParquetRecorder(tmp_path, capture_session_id="s")
    await recorder.start()
    await recorder.record_market(trade_event(monotonic_ns=1), capture_seq=0)
    with pytest.raises(OSError, match="power loss"):
        await recorder.stop()
    assert not DatasetCatalog(tmp_path).entries()
    assert not list(tmp_path.rglob("*.parquet"))
    assert len(recover_dataset(tmp_path, repair=True).quarantined) == 1


def test_unreadable_catalog_is_reported_invalid_not_guessed(tmp_path):
    catalog = DatasetCatalog(tmp_path)
    catalog.path.parent.mkdir()
    catalog.path.write_text("{partial")
    result = recover_dataset(tmp_path, repair=True)
    assert not result.valid and "unreadable catalog" in result.invalid[0]
