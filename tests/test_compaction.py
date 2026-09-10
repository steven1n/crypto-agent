from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from recorder.compaction import compact_parquet_fragments


def test_compaction_preserves_rows_and_orders_without_removing_sources(
    tmp_path: Path,
) -> None:
    schema = pa.schema([pa.field("capture_seq", pa.int64()), pa.field("value", pa.string())])
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"capture_seq": 3, "value": "c"}, {"capture_seq": 1, "value": "a"}],
            schema=schema,
        ),
        first,
    )
    pq.write_table(
        pa.Table.from_pylist([{"capture_seq": 2, "value": "b"}], schema=schema),
        second,
    )
    output = tmp_path / "compacted.parquet"

    result = compact_parquet_fragments((first, second), output, minimum_source_age_seconds=0)

    assert result.input_rows == result.output_rows == 3
    assert first.exists() and second.exists()
    assert pq.ParquetFile(output).read().to_pylist() == [
        {"capture_seq": 1, "value": "a"},
        {"capture_seq": 2, "value": "b"},
        {"capture_seq": 3, "value": "c"},
    ]


def test_compaction_removes_sources_only_in_explicit_mode(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    pq.write_table(pa.table({"capture_seq": [1]}), source)

    result = compact_parquet_fragments(
        (source,),
        tmp_path / "output.parquet",
        remove_sources=True,
        minimum_source_age_seconds=0,
    )

    assert result.sources_removed
    assert not source.exists()
