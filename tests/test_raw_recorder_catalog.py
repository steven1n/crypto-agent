from datetime import timedelta
from pathlib import Path

import pytest

from market.clock import ClockOffsetSample
from recorder.catalog import DatasetCatalog
from recorder.raw_models import LifecycleRecord
from recorder.raw_recorder import RawParquetRecorder
from recorder.versions import DATASET_VERSION
from replay.loader import ReplayDatasetLoader
from replay.models import ReplayEventType, ReplayIntegrityError
from tests.helpers import NOW, depth_event, trade_event, trusted_book


async def test_raw_recorder_catalog_and_loader_round_trip(tmp_path: Path) -> None:
    catalog = DatasetCatalog(tmp_path)
    recorder = RawParquetRecorder(
        tmp_path,
        capture_session_id="session-a",
        batch_size=10,
        flush_interval_seconds=60,
        catalog=catalog,
        configuration={"symbol": "BTCUSDT"},
        software_revision=None,
    )
    await recorder.start()
    await recorder.record_lifecycle(
        LifecycleRecord(
            symbol="BTCUSDT",
            component="websocket",
            kind="CONNECTED",
            local_time=NOW,
            monotonic_ns=0,
        ),
        capture_seq=0,
    )
    await recorder.record_market(
        depth_event(
            symbol="BTCUSDT",
            first_update_id=100,
            final_update_id=101,
            previous_final_update_id=99,
            monotonic_ns=1_000,
        ),
        capture_seq=1,
    )
    await recorder.record_trusted_book(
        trusted_book(symbol="BTCUSDT", monotonic_ns=2_000), capture_seq=2
    )
    await recorder.record_market(trade_event(symbol="BTCUSDT", monotonic_ns=3_000), capture_seq=3)
    await recorder.record_clock_sample(
        ClockOffsetSample(20, 10, NOW + timedelta(microseconds=4)),
        symbol="BTCUSDT",
        capture_seq=4,
        monotonic_timestamp_ns=4_000,
    )
    await recorder.stop()

    entries = catalog.entries()
    assert sum(item.row_count for item in entries) == 5
    assert all(item.dataset_version == DATASET_VERSION for item in entries)
    assert all((tmp_path / item.file_path).stat().st_size == item.file_size for item in entries)
    assert {item.symbol for item in entries} == {"BTCUSDT"}
    assert recorder.diagnostics().rows_written == 5

    replay = ReplayDatasetLoader(tmp_path, catalog).load(
        capture_session_id="session-a", symbol="BTCUSDT"
    )
    assert [item.capture_seq for item in replay] == [0, 1, 2, 3, 4]
    assert [item.event_type for item in replay] == [
        ReplayEventType.STREAM_LIFECYCLE,
        ReplayEventType.DEPTH,
        ReplayEventType.TRUSTED_BOOK,
        ReplayEventType.TRADE,
        ReplayEventType.CLOCK_SAMPLE,
    ]

    depth_manifest = next(item for item in entries if item.event_type == "depth")
    depth_path = tmp_path / depth_manifest.file_path
    original = depth_path.read_bytes()
    depth_path.unlink()
    with pytest.raises(ReplayIntegrityError, match="missing cataloged file"):
        ReplayDatasetLoader(tmp_path, catalog).load(
            capture_session_id="session-a", symbol="BTCUSDT"
        )
    depth_path.write_bytes(b"not parquet")
    with pytest.raises(ReplayIntegrityError, match="corrupt Parquet"):
        ReplayDatasetLoader(tmp_path, catalog).load(
            capture_session_id="session-a", symbol="BTCUSDT"
        )
    depth_path.write_bytes(original)


async def test_raw_recorder_rejects_duplicate_capture_sequence(tmp_path: Path) -> None:
    recorder = RawParquetRecorder(tmp_path, capture_session_id="session-b")
    await recorder.start()
    event = trade_event(symbol="BTCUSDT", monotonic_ns=1)
    await recorder.record_market(event, capture_seq=1)
    try:
        try:
            await recorder.record_market(event, capture_seq=1)
        except ValueError as exc:
            assert "capture_seq" in str(exc)
        else:
            raise AssertionError("duplicate capture sequence was accepted")
    finally:
        await recorder.stop()
