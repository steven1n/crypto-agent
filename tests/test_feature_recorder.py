from pathlib import Path

import pyarrow.parquet as pq

from features.engine import FeatureEngine
from recorder.feature_recorder import FeatureParquetRecorder
from recorder.schemas import FEATURE_SCHEMA
from tests.helpers import trusted_book


def make_feature():  # type: ignore[no-untyped-def]
    engine = FeatureEngine("BTCUSDC")
    engine.on_connection(True, monotonic_timestamp_ns=0)
    engine.on_market_activity(0)
    engine.on_book_snapshot(trusted_book(monotonic_ns=0))
    feature = engine.build_snapshot(now_ns=1)
    assert feature is not None
    return feature


def read_feature_files(tmp_path: Path):  # type: ignore[no-untyped-def]
    paths = sorted(
        tmp_path.glob("features/exchange=binance/symbol=BTCUSDC/date=*/features-test-*.parquet")
    )
    return paths, [pq.ParquetFile(path).read() for path in paths]


async def test_recorder_batches_and_writes_stable_parquet(tmp_path: Path) -> None:
    recorder = FeatureParquetRecorder(
        tmp_path,
        batch_size=2,
        flush_interval_seconds=60,
        queue_size=10,
        run_id="test",
    )
    await recorder.start()
    feature = make_feature()
    await recorder.record(feature)
    await recorder.record(feature)
    await recorder.record(feature)
    await recorder.stop()

    paths, tables = read_feature_files(tmp_path)
    assert len(paths) == 2
    assert sum(table.num_rows for table in tables) == 3
    assert tables[0].schema == FEATURE_SCHEMA
    assert tables[0].column("best_bid_price").to_pylist() == ["100", "100"]
    assert tables[0].column("feature_valid").to_pylist() == [True, True]
    assert tables[0].column("capture_seq").to_pylist() == [0, 1]
    assert recorder.diagnostics().rows_written == 3
    assert (tmp_path / "catalog" / "manifest.json").is_file()
