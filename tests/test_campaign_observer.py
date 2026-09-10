"""Fast observer checks; long market capture is not a unit test."""

from pathlib import Path

import pytest

from app.campaign_observer import (
    capture_environment,
    freeze_configuration,
    observe,
    parse_process_sample,
)
from config.settings import Settings
from recorder.catalog import stable_fingerprint


def test_ps_units() -> None:
    assert parse_process_sample(" 1024  12.5\n") == {
        "rss_bytes": 1048576,
        "cpu_percent_ps": 12.5,
    }


@pytest.mark.parametrize("ceiling", [0, -1, float("nan"), float("inf")])
def test_rss_ceiling_rejects_invalid_values(tmp_path: Path, ceiling: float) -> None:
    with pytest.raises(ValueError, match="RSS ceiling"):
        observe(tmp_path / "invalid", 7200, 60, ceiling)
    assert not (tmp_path / "invalid").exists()


def test_rss_ceiling_frozen_without_changing_controls(tmp_path: Path) -> None:
    settings = Settings(data_directory=tmp_path)
    original = freeze_configuration(settings, 60)
    guarded = freeze_configuration(settings, 60, 1024)
    assert guarded["observer_rss_ceiling_bytes"] == 1_024_000_000
    assert original["controls_fingerprint"] == guarded["controls_fingerprint"]
    assert original["configuration_fingerprint"] != guarded["configuration_fingerprint"]


def test_rss_ceiling_stops_child_and_marks_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    import app.campaign_observer as observer

    source = tmp_path / "source"
    (source / "app").mkdir(parents=True)
    (source / "app" / "__init__.py").write_text("")
    (source / "app" / "capture.py").write_text("import time\ntime.sleep(30)\n")
    (source / "pyproject.toml").write_text("")
    monkeypatch.setattr(observer, "SOURCE_ROOT", source)
    monkeypatch.setattr(observer, "resource_sample", lambda *args: {"rss_bytes": 1_000_000})
    output = tmp_path / "checkpoint"
    assert observe(output, 7200, 0.01, 1) == 2
    status = json.loads((output / "observer-status.json").read_text())
    assert status["rss_limit_triggered"]
    assert len(status["warnings"]) == 1
    assert "resource gate failed" in status["warnings"][0]
    assert status["ended_at"]


def test_frozen_configuration_is_deterministic(tmp_path: Path) -> None:
    settings = Settings(data_directory=tmp_path, capture_duration_seconds=7200)
    first = freeze_configuration(settings, 60)
    assert first == freeze_configuration(settings, 60)
    fingerprint = first.pop("configuration_fingerprint")
    assert fingerprint == stable_fingerprint(first)
    assert first["transport_constants"]["trade_source"] == "INDIVIDUAL"
    assert first["research_controls"]["horizons_ms"] == [250, 500, 1000, 2000, 5000, 10000]
    assert first["research_controls"]["configuration"]["seed"] == 42
    assert first["research_controls"]["configuration"]["bootstrap_replications"] == 200


def test_controls_unchanged_between_stages(tmp_path: Path) -> None:
    first = freeze_configuration(
        Settings(data_directory=tmp_path / "2h", capture_duration_seconds=7200), 60
    )
    later = freeze_configuration(
        Settings(data_directory=tmp_path / "6h", capture_duration_seconds=21600), 60
    )
    assert first["controls_fingerprint"] == later["controls_fingerprint"]
    assert first["configuration_fingerprint"] != later["configuration_fingerprint"]


def test_environment_removes_case_insensitive_overrides(tmp_path: Path) -> None:
    settings = Settings(data_directory=tmp_path, capture_duration_seconds=7200)
    env = capture_environment(
        settings, {"symbols": "WRONG", "FEATURE_INTERVAL_MS": "999", "custom_proxy": "private"}
    )
    assert "symbols" not in env
    assert env["SYMBOLS"] == "BTCUSDT,BTCUSDC"
    assert env["FEATURE_INTERVAL_MS"] == "100"
    assert env["custom_proxy"] == "private"
    assert "ORDERBOOK_MAX_STORED_LEVELS" not in env


def test_configuration_rejects_nonpositive_sampling() -> None:
    with pytest.raises(ValueError, match="positive"):
        freeze_configuration(Settings(), 0)


def test_observer_refuses_existing_directory(tmp_path: Path) -> None:
    with pytest.raises(FileExistsError):
        observe(tmp_path, 7200, 60)


def test_observer_rejects_zero_duration_before_creating_directory(tmp_path: Path) -> None:
    output = tmp_path / "new"
    with pytest.raises(ValueError):
        observe(output, 0, 60)
    assert not output.exists()


def test_environment_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(data_directory=tmp_path, capture_duration_seconds=7200)
    env = capture_environment(settings, {})
    for name in tuple(__import__("os").environ):
        if name.lower() in Settings.model_fields:
            monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)  # As in production, child cwd has no .env.
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert Settings().model_dump(mode="json") == settings.model_dump(mode="json")


def test_observer_fake_capture_exits_without_market_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    import app.campaign_observer as observer

    source = tmp_path / "source"
    (source / "app").mkdir(parents=True)
    (source / "app" / "__init__.py").write_text("")
    (source / "app" / "capture.py").write_text("print('fake transport complete')\n")
    (source / "pyproject.toml").write_text("")
    monkeypatch.setattr(observer, "SOURCE_ROOT", source)
    # This tests child lifecycle, not OS permissions; keep resource aggregation real.
    # test_ps_units separately covers the real sampler's output parsing and units.
    monkeypatch.setattr(
        observer,
        "process_sample",
        lambda pid: {"rss_bytes": 1_048_576, "cpu_percent_ps": 0.0},
    )
    output = tmp_path / "checkpoint"
    assert observe(output, 7200, 0.01) == 0  # Fake child exits immediately, not after 2h.
    status = json.loads((output / "observer-status.json").read_text())
    assert status["status"] == "CAPTURE_ENDED_PENDING_VALIDATION"
    assert status["source_unchanged"]
    assert status["returncode"] == 0
    assert "fake transport complete" in (output / "capture.log").read_text()
