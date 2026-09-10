"""External operational observer; launches the existing capture CLI unchanged.

No stage promotion or research decisions are automated here. Every completed
capture still needs verify -> freeze -> batch before starting the next stage.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import signal
import subprocess
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

from config.settings import Settings
from recorder.catalog import stable_fingerprint
from recorder.durability import file_sha256, write_json_atomic
from recorder.raw_schemas import SCHEMA_VERSIONS
from recorder.versions import FEATURE_SCHEMA_VERSION
from research.batch import FEATURES, HORIZONS, BatchConfiguration
from strategies.baselines import (
    BookImbalanceStrategy,
    CombinedBaselineStrategy,
    MicropriceMomentumStrategy,
    TradeFlowStrategy,
)

SOURCE_ROOT = Path(__file__).resolve().parent.parent
RESEARCH_MODULES = ("research", "replay", "features", "market", "recorder", "strategies")
SOURCE_MODULES = (*RESEARCH_MODULES, "app", "config", "exchange", "execution", "signals")


def source_files(modules: tuple[str, ...]) -> dict[str, str]:
    return {
        str(path.relative_to(SOURCE_ROOT)): file_sha256(path)
        for module in modules
        for path in sorted((SOURCE_ROOT / module).rglob("*.py"))
    }


def freeze_configuration(
    settings: Settings, sample_seconds: float, max_rss_mb: float | None = None
) -> dict[str, Any]:
    """Include hardcoded transport settings, not just environment configuration."""
    if not math.isfinite(sample_seconds) or sample_seconds <= 0:
        raise ValueError("resource sampling interval must be positive")
    if max_rss_mb is not None and (not math.isfinite(max_rss_mb) or max_rss_mb <= 0):
        raise ValueError("RSS ceiling must be finite and positive")
    settings_json = settings.model_dump(mode="json")
    controls = {
        key: value
        for key, value in settings_json.items()
        if key not in ("data_directory", "capture_duration_seconds")
    }
    research = {
        "configuration": asdict(BatchConfiguration()),
        "horizons_ms": list(HORIZONS),
        "features": list(FEATURES),
        "strategies": [
            asdict(strategy)
            for strategy in (
                MicropriceMomentumStrategy(),
                BookImbalanceStrategy(),
                TradeFlowStrategy(),
                CombinedBaselineStrategy(),
            )
        ],
    }
    transport = {
        "depth_subscription": "@depth@100ms",
        "trade_subscription": "@trade",
        "trade_source": "INDIVIDUAL",
        "additional_subscription": "@bookTicker (not a feature input)",
        "rest_snapshot_limit": 1000,
        "websocket_max_queue": 64,
        "websocket_max_size_bytes": 1048576,
        "websocket_ping_interval_seconds": 20,
        "websocket_ping_timeout_seconds": 20,
        "websocket_close_timeout_seconds": 10,
        "diagnostic_rolling_capacity": 6000,
        "feature_history_hard_capacity": 200000,
        "clock_estimator": "rolling median; reject RTT above configured maximum",
    }
    files = source_files(SOURCE_MODULES)
    files["pyproject.toml"] = file_sha256(SOURCE_ROOT / "pyproject.toml")
    config = {
        "settings": settings_json,
        "transport_constants": transport,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "raw_schema_versions": {kind.value: version for kind, version in SCHEMA_VERSIONS.items()},
        "research_controls": research,
        "source_files_sha256": files,
        "source_fingerprint": stable_fingerprint(files),
        "research_source_fingerprint": stable_fingerprint(source_files(RESEARCH_MODULES)),
        "controls_fingerprint": stable_fingerprint(
            {"capture": controls, "transport": transport, "research": research}
        ),
        "resource_sample_seconds": sample_seconds,
        "resource_quantiles": "sampled observations, not continuous process maxima",
        "symbol_roles": {
            "BTCUSDT": "primary market-discovery research feed",
            "BTCUSDC": "potential execution-research feed; no venue recommendation",
        },
    }
    if max_rss_mb is not None:
        config["observer_rss_ceiling_bytes"] = int(max_rss_mb * 1_000_000)
        config["observer_rss_policy"] = "sampled child RSS; SIGTERM and await graceful finalization"
    return {**config, "configuration_fingerprint": stable_fingerprint(config)}


def capture_environment(settings: Settings, inherited: dict[str, str]) -> dict[str, str]:
    """Freeze every Settings field, removing case-insensitive inherited overrides.

    Child cwd is a newly created directory with no .env. None uses the existing
    Settings default; all non-None fields are provided explicitly as environment.
    No proxy credentials or other inherited environment is written to artifacts.
    """
    values = settings.model_dump(mode="json")
    env = {key: value for key, value in inherited.items() if key.lower() not in values}
    for name, value in values.items():
        if value is None:
            if Settings.model_fields[name].default is not None:
                raise ValueError(f"cannot preserve nullable setting: {name}")
            continue
        env[name.upper()] = (
            ",".join(str(item) for item in value)
            if isinstance(value, list)
            else str(value).lower()
            if isinstance(value, bool)
            else str(value)
        )
    env["PYTHONPATH"] = str(SOURCE_ROOT)
    env["PYTHONUNBUFFERED"] = "1"
    return env


def parse_process_sample(output: str) -> dict[str, float | int]:
    """macOS/POSIX ps RSS is KiB; %CPU is OS-defined, not interval CPU time."""
    rss, cpu = output.strip().split()
    return {"rss_bytes": int(rss) * 1024, "cpu_percent_ps": float(cpu)}


def process_sample(pid: int) -> dict[str, float | int]:
    result = subprocess.run(
        ["ps", "-o", "rss=,pcpu=", "-p", str(pid)],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    return parse_process_sample(result.stdout)


def resource_sample(pid: int, dataset: Path, started: float) -> dict[str, Any]:
    sample: dict[str, Any] = {
        "utc": datetime.now(UTC).isoformat(),
        "elapsed_seconds": monotonic() - started,
        "pid": pid,
        **process_sample(pid),
        "free_disk_bytes": shutil.disk_usage(dataset.parent).free,
    }
    manifest = dataset / "catalog" / "manifest.json"
    sample["catalog_bytes"] = manifest.stat().st_size if manifest.exists() else 0
    # Reading atomic snapshots does not acquire the active recorder's dataset lock.
    entries = json.loads(manifest.read_text()) if manifest.exists() else []
    sample["catalog_entries"] = len(entries)
    sample["finalized_bytes"] = sum(entry["file_size"] for entry in entries)
    sample["health"] = [
        json.loads(path.read_text())
        for path in sorted((dataset / "sessions").glob("*/latest-health.json"))
    ]
    return sample


def observe(
    output: Path, duration: float, sample_seconds: float, max_rss_mb: float | None = None
) -> int:
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("capture duration must be finite and positive")
    output = output.resolve()
    settings = Settings(
        symbols=("BTCUSDT", "BTCUSDC"),
        data_directory=output / "dataset",
        capture_duration_seconds=duration,
    )
    config = freeze_configuration(settings, sample_seconds, max_rss_mb)
    output.mkdir(parents=True, exist_ok=False)  # Never append to an old checkpoint.
    write_json_atomic(output / "frozen-configuration.json", config)
    environment = capture_environment(settings, dict(os.environ))
    command = [sys.executable, "-m", "app.capture"]
    started = monotonic()
    status: dict[str, Any] = {
        "status": "STARTING",
        "observer_pid": os.getpid(),
        "started_at": datetime.now(UTC).isoformat(),
        "command": command,
        "cwd": str(output),
        "configuration_fingerprint": config["configuration_fingerprint"],
        "target_duration_seconds": duration,
        "warnings": [],
    }
    write_json_atomic(output / "observer-status.json", status)
    with (output / "capture.log").open("x") as log, (output / "resources.jsonl").open("x") as rows:
        process = subprocess.Popen(command, cwd=output, env=environment, stdout=log, stderr=log)
        status.update(status="CAPTURING", capture_pid=process.pid)
        write_json_atomic(output / "observer-status.json", status)
        previous_handlers = {}

        def stop_child(signum: int, _frame: object) -> None:
            status["operator_stop_signal"] = signum
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)

        for sig in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[sig] = signal.signal(sig, stop_child)
        awake: subprocess.Popen[bytes] | None = None
        try:
            if sys.platform == "darwin":
                # Scoped to this child PID; does not change system power settings.
                awake = subprocess.Popen(["caffeinate", "-i", "-w", str(process.pid)])
            while process.poll() is None:
                try:
                    row = resource_sample(process.pid, settings.data_directory, started)
                except subprocess.CalledProcessError:
                    if process.poll() is not None:
                        break
                    raise
                rows.write(json.dumps(row, sort_keys=True, allow_nan=False) + "\n")
                rows.flush()
                os.fsync(rows.fileno())
                ceiling = config.get("observer_rss_ceiling_bytes")
                if ceiling is not None and row["rss_bytes"] >= ceiling:
                    status["warnings"].append(
                        f"RSS safety stop: {row['rss_bytes']} bytes >= {ceiling}; "
                        "resource gate failed; do not promote this checkpoint"
                    )
                    status.update(status="STOPPING_RSS_LIMIT", rss_limit_triggered=True)
                    write_json_atomic(output / "observer-status.json", status)
                    if process.poll() is None:
                        process.terminate()
                    break  # finally waits for the existing graceful shutdown, never SIGKILL.
                try:
                    process.wait(timeout=sample_seconds)
                except subprocess.TimeoutExpired:
                    pass
        except BaseException as exc:
            status["warnings"].append(f"observer failure: {type(exc).__name__}: {exc}")
            if process.poll() is None:
                process.terminate()
            raise
        finally:
            # Do not kill the recorder during Parquet finalization.
            process.wait()
            if awake is not None:
                awake.wait(timeout=10)
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)
            current = source_files(SOURCE_MODULES)
            current["pyproject.toml"] = file_sha256(SOURCE_ROOT / "pyproject.toml")
            unchanged = current == config["source_files_sha256"]
            status.update(
                status="CAPTURE_ENDED_PENDING_VALIDATION",
                ended_at=datetime.now(UTC).isoformat(),
                returncode=process.returncode,
                source_unchanged=unchanged,
            )
            if not unchanged:
                status["warnings"].append(
                    "source changed during capture; investigate before research"
                )
            write_json_atomic(output / "observer-status.json", status)
    return 2 if status["warnings"] or process.returncode else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", required=True, type=Path, help="must not exist")
    parser.add_argument("--duration", type=float, default=7200, help="capture seconds; default 2h")
    parser.add_argument("--sample-seconds", type=float, default=60)
    parser.add_argument(
        "--max-rss-mb",
        type=float,
        default=None,
        help="optional sampled capture-process RSS ceiling in decimal MB; graceful stop on breach",
    )
    args = parser.parse_args()
    raise SystemExit(
        observe(args.output_directory, args.duration, args.sample_seconds, args.max_rss_mb)
    )


if __name__ == "__main__":
    main()
