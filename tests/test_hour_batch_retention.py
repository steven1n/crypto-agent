import json
import random
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq
import pytest

import research.batch as batch
from config.settings import Settings
from recorder.catalog import DatasetFileManifest
from recorder.durability import CaptureSafetyError, write_json_atomic
from recorder.raw_models import LifecycleRecord
from replay.engine import ReplayResult
from replay.models import ReplayDataQuality, ReplayEventType, ReplayItem
from replay.reconstruction import ReconstructionResult
from research.batch import (
    BatchConfiguration,
    HourBuffer,
    _hour,
    hour_analysis_rows,
    segment_diagnostics,
)
from research.labels import LabelGenerator
from tests.helpers import NOW, feature_snapshot


def features_at(walls):
    return tuple(
        replace(
            feature_snapshot(monotonic_ns=i * 100_000_000),
            created_at=datetime.fromisoformat(f"2026-09-07T{wall}+00:00"),
        )
        for i, wall in enumerate(walls)
    )


def collect(features, boundaries=(), start=None, end=None):
    pending = HourBuffer(1000)
    emitted, fragments = [], []

    def flush():
        hour = _hour(pending.rows[0].created_at)
        _, rows = segment_diagnostics(
            tuple(pending.rows),
            hour,
            BatchConfiguration(bootstrap_replications=1),
            integrity_boundaries_ns=boundaries,
            selection_end_ns=pending.rows[pending.prefix_size - 1].monotonic_ns,
            start=start,
            end=end,
        )
        assert all(_hour(row["created_at"]) == hour for row in rows)
        pending.finish(rows, start=start, end=end)
        emitted.extend(rows)
        fragments.append((hour, rows))

    for f in features:
        pending.append(f)
        while pending.ready():
            flush()
        assert (
            len(emitted)
            + len(pending.rows)
            + pending.excluded_before_start
            + pending.excluded_at_or_after_end
        ) == pending.source_rows
    while pending.rows:
        flush()
    return emitted, fragments, pending.accounting()


@pytest.mark.parametrize(
    "walls",
    [
        ["08:59:59.900", "09:00:00.100", "09:00:00.200"],
        ["09:11:15.953", "09:11:15.755"],
        ["09:00:00.100", "08:59:59.803"],
        ["09:00:00.100", "08:59:59.803", "08:59:59.900", "09:00:00.020"],
        ["09:59:59.900", "10:00:00.100"],
    ],
)
def test_civil_rollback_exactly_once_in_source_order(walls):
    features = features_at(walls)
    rows, _, accounting = collect(features)
    assert [(r["monotonic_ns"], r["created_at"]) for r in rows] == [
        (f.monotonic_ns, f.created_at) for f in features
    ]
    assert accounting["eligible_source_rows"] == accounting["emitted_analysis_rows"] == len(walls)
    assert accounting["intentionally_excluded_rows"] == 0


def test_interleaved_hours_keep_future_context_and_invalid_lifecycle_boundaries():
    first = features_at(["08:59:59.900", "09:00:00.100", "08:59:59.803", "09:00:00.020"])
    rest = tuple(
        replace(
            feature_snapshot(monotonic_ns=i * 100_000_000),
            created_at=first[-1].created_at + timedelta(seconds=i / 10),
        )
        for i in range(4, 140)
    )
    features = first + rest
    features = features[:5] + (replace(features[5], feature_valid=False),) + features[6:]
    boundaries = (850_000_000,)
    expected = LabelGenerator().generate(features, integrity_boundaries_ns=boundaries)
    rows, fragments, _ = collect(features, boundaries)
    assert len(fragments) == 4
    for row, label in zip(rows, expected, strict=True):
        assert all(row[k] == v for k, v in asdict(label).items())
    assert rows[0]["future_return_500ms"] is None
    assert rows[0]["future_return_250ms"] == 0


@pytest.mark.parametrize("corruption", ["missing", "duplicate", "reordered", "wall_rewrite"])
def test_output_accounting_fails_closed(corruption):
    pending = HourBuffer(10)
    for f in features_at(["09:11:15.953", "09:11:15.755"]):
        pending.append(f)
    rows = [{"monotonic_ns": f.monotonic_ns, "created_at": f.created_at} for f in pending.rows]
    if corruption == "missing":
        rows.pop()
    elif corruption == "duplicate":
        rows.append(rows[0])
    elif corruption == "reordered":
        rows.reverse()
    else:
        rows[1] = {**rows[1], "created_at": NOW}
    with pytest.raises(CaptureSafetyError, match="accounting"):
        pending.finish(rows)
    assert len(pending.rows) == 2 and pending.emitted_rows == 0


def test_session_accounting_exclusions_and_pending_rows():
    features = features_at(["09:00:00.100", "08:59:59.803", "09:00:00.200", "10:00:00.000"])
    start = datetime(2026, 9, 7, 9, tzinfo=UTC)
    end = start + timedelta(hours=1)
    rows, _, counts = collect(features, start=start, end=end)
    assert len(rows) == 2
    assert counts == {
        "source_rows": 4,
        "eligible_source_rows": 2,
        "emitted_analysis_rows": 2,
        "intentionally_excluded_rows": 2,
        "excluded_before_start": 1,
        "excluded_at_or_after_end": 1,
    }
    pending = HourBuffer(10)
    pending.append(features[0])
    with pytest.raises(CaptureSafetyError, match="accounting"):
        pending.accounting()


def test_monotonic_readiness_and_bounded_capacity():
    pending = HourBuffer(4)
    a, b = features_at(["09:00:00.100", "08:59:59.803"])
    pending.append(a)
    pending.append(b)
    assert not pending.ready()
    pending.append(replace(b, monotonic_ns=10_000_000_000))
    assert pending.ready()  # No future civil-hour watermark required.
    with pytest.raises(CaptureSafetyError, match="strictly increase"):
        pending.append(b)
    pending.append(replace(b, monotonic_ns=11_000_000_000))
    with pytest.raises(CaptureSafetyError, match="memory bound"):
        pending.append(replace(b, monotonic_ns=12_000_000_000))


@pytest.mark.parametrize("invalid_middle", [False, True])
def test_civil_range_exclusion_remains_a_label_boundary(invalid_middle):
    a, b, c = features_at(["09:00:00.100", "08:59:59.803", "09:00:00.200"])
    b = replace(b, feature_valid=not invalid_middle)
    c = replace(c, monotonic_ns=500_000_000)
    start = _hour(a.created_at)
    rows = hour_analysis_rows((a, b, c), start, start=start)
    assert [row.feature.monotonic_ns for row in rows] == [a.monotonic_ns, c.monotonic_ns]
    assert rows[0].label.future_return_500ms is None


def test_many_hour_revisits_never_duplicate_or_lose_rows():
    rng = random.Random(42)
    features = tuple(
        replace(
            feature_snapshot(monotonic_ns=i * 100_000_000),
            created_at=NOW + timedelta(hours=rng.randrange(-10, 10)),
        )
        for i in range(240)
    )
    pending = HourBuffer(200)
    output = []

    def flush():
        rows = [
            {"monotonic_ns": f.monotonic_ns, "created_at": f.created_at}
            for f in pending.rows[: pending.prefix_size]
        ]
        pending.finish(rows)
        output.extend(rows)

    for f in features:
        pending.append(f)
        while pending.ready():
            flush()
    while pending.rows:
        flush()
    assert [r["monotonic_ns"] for r in output] == [f.monotonic_ns for f in features]
    assert pending.accounting()["emitted_analysis_rows"] == 240


@pytest.mark.parametrize("flush_before_end", [False, True])
def test_run_batch_hour_revisit_writes_distinct_files(tmp_path, monkeypatch, flush_before_end):
    """Exercise real batch selection/writes/accounting; fake only upstream replay."""
    features = features_at(["09:00:00.100", "08:59:59.803", "08:59:59.900", "09:00:00.020"])
    if flush_before_end:
        features = (*features[:-1], replace(features[-1], monotonic_ns=10_200_000_000))
    settings = Settings(_env_file=None).model_dump(mode="json")
    write_json_atomic(
        tmp_path / "sessions" / "s" / "session-summary.json",
        {
            "status": "FINALIZED",
            "symbols": {"BTCUSDT": {"dataset_valid": True}},
            "configuration": settings,
        },
    )
    manifest = DatasetFileManifest(
        "f",
        1,
        2,
        "binance",
        "usdm",
        "BTCUSDT",
        None,
        None,
        features[0].created_at.isoformat(),
        features[-1].created_at.isoformat(),
        0,
        6,
        4,
        "feature",
        "unused.parquet",
        0,
        NOW.isoformat(),
        "s",
        None,
        "config",
        None,
        None,
        0,
        0,
        True,
    )
    events = [
        ReplayItem(
            i * 2, "s", ReplayEventType.FEATURE_TICK, "BTCUSDT", f.created_at, f.monotonic_ns, f
        )
        for i, f in enumerate(features)
    ]
    boundary = ReplayItem(
        1,
        "s",
        ReplayEventType.BOOK_LIFECYCLE,
        "BTCUSDT",
        NOW,
        50_000_000,
        LifecycleRecord("BTCUSDT", "book", "DESYNCED", NOW, 50_000_000, healthy=False),
    )
    events.insert(1, boundary)
    monkeypatch.setattr(batch, "select_manifests", lambda *a, **k: (manifest,))
    monkeypatch.setattr(batch, "iter_session", lambda *a: iter(events))

    class Replay:
        def __init__(self, events, *a, consumers, feature_handler, **k):
            self.events, self.consumers, self.handler = events, consumers, feature_handler

        def run(self):
            for event in self.events:
                for consumer in self.consumers:
                    consumer(event)
                if event.event_type is ReplayEventType.FEATURE_TICK:
                    self.handler(event.payload, event.payload)
            return ReplayResult((), (), ReplayDataQuality(True, len(events), 0, 6, 0, ()))

    monkeypatch.setattr(batch, "ReplayEngine", Replay)
    monkeypatch.setattr(batch.RawReconstructionValidator, "accept", lambda *a: None)
    monkeypatch.setattr(
        batch.RawReconstructionValidator,
        "result",
        lambda *a: ReconstructionResult(1, 0, None, 0, 1),
    )
    path = batch.run_batch(tmp_path, configuration=BatchConfiguration(bootstrap_replications=1))
    report = json.loads(path.read_text())
    session = report["sessions"][0]
    assert session["row_accounting"]["emitted_analysis_rows"] == 4
    assert len(session["segments"]) == 3
    assert len({s["path"] for s in session["segments"]}) == 3
    rows = []
    for segment in session["segments"]:
        name = segment["path"].split("/")[-1].replace(".json", ".parquet")
        rows.extend(pq.ParquetFile(path.parent / "labels" / name).read().to_pylist())
    assert [r["capture_seq"] for r in rows] == [0, 2, 4, 6]
    assert [r["created_at"] for r in rows] == [f.created_at for f in features]
    assert rows[0]["future_return_250ms"] is None
