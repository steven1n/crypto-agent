"""One fixed-control campaign; orchestration only, no core semantics changed."""

import json
import os
import platform
import signal
import subprocess
import sys
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

import pyarrow as pa

from app.campaign_observer import capture_environment, freeze_configuration, resource_sample
from config.settings import Settings
from recorder.catalog import stable_fingerprint
from recorder.durability import DatasetLock, file_sha256, recover_dataset, write_json_atomic
from recorder.frozen import freeze_dataset, select_manifests, verify_frozen

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data/clean-6h-stage-b-20260909"
DATA = OUT / "dataset"
TOOLS = ROOT / "data/resource-soak-tools-20260908"
sys.path.insert(0, str(TOOLS))
from run_soak import files_now, native_sample
from finish_soak import slope, stats

LOCAL = ZoneInfo("Asia/Singapore")


def read(path):
    return json.loads(path.read_text())


def save(name, value):
    write_json_atomic(OUT / name, value)


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def research_controls_equal(left: object, right: object) -> bool:
    """Compare strict JSON values, preserving sequence order and every key."""

    def same(a: object, b: object) -> bool:
        if isinstance(a, dict) and isinstance(b, dict):
            return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
        if isinstance(a, list) and isinstance(b, list):
            return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b, strict=True))
        if isinstance(a, bool) or isinstance(b, bool):
            return type(a) is type(b) and a == b
        return a == b

    # No default=str, rounding, array sorting, or non-finite JSON extensions.
    # JSON round-trip removes tuple/list distinctions; equal JSON numbers such
    # as 11 and 11.0 remain equal without conflating boolean true with number 1.
    return same(
        json.loads(json.dumps(left, allow_nan=False)),
        json.loads(json.dumps(right, allow_nan=False)),
    )


def instrument_hashes():
    paths = [Path(__file__).resolve(), TOOLS / "capture_probe.py", TOOLS / "run_soak.py", TOOLS / "finish_soak.py"]
    return {str(p.relative_to(ROOT)): file_sha256(p) for p in paths}


def unchanged(config):
    require(files_now() == config["source_files_sha256"], "core source changed")
    require(instrument_hashes() == config["instrumentation_sha256"], "campaign instrumentation changed")


def prepare():
    now = datetime.now(LOCAL)
    cutoff = now.replace(hour=23, minute=0, second=0, microsecond=0)
    require(now + timedelta(seconds=21600, minutes=30) < cutoff, "insufficient pre-midnight network window")
    prior = read(ROOT / "data/campaign-20260905/stage-b-6h/frozen-configuration.json")
    settings = Settings(_env_file=None, **{**prior["settings"], "data_directory": DATA, "capture_duration_seconds": 21600})
    config = freeze_configuration(settings, 60, 1024)
    require(config["controls_fingerprint"] == prior["controls_fingerprint"], "accepted controls differ")
    config.pop("configuration_fingerprint")
    config.update(instrumentation_sha256=instrument_hashes(), environment={
        "python": sys.version, "pyarrow": pa.__version__, "os": platform.platform(),
        "allocator": pa.default_memory_pool().backend_name, "executable": sys.executable,
    }, prepared_at_utc=datetime.now(UTC).isoformat(), prepared_at_local=now.isoformat(),
        capture_hard_stop_local=cutoff.isoformat(), policy="one session; no restart; capture then gated offline phases")
    config["configuration_fingerprint"] = stable_fingerprint(config)
    OUT.mkdir(exist_ok=False)
    save("frozen-configuration.json", config)
    save("summary.json", {"status": "PREPARED", "configuration_fingerprint": config["configuration_fingerprint"],
        "source_fingerprint": config["source_fingerprint"], "controls_fingerprint": config["controls_fingerprint"],
        "target_duration_seconds": 21600, "symbols": list(settings.symbols), "stage_b_decision": None})
    return settings, config


def capture(settings, config, state):
    started = monotonic()
    frozen_hash = file_sha256(OUT / "frozen-configuration.json")
    state.update(status="CAPTURING", observer_pid=os.getpid(), warnings=[],
        capture_launch_utc=datetime.now(UTC).isoformat(), capture_launch_local=datetime.now(LOCAL).isoformat())
    with (OUT / "capture.log").open("x") as log, (OUT / "resources.jsonl").open("x") as samples:
        child = subprocess.Popen([sys.executable, str(TOOLS / "capture_probe.py")], cwd=OUT,
            env=capture_environment(settings, dict(os.environ)), stdout=log, stderr=log)
        state["capture_pid"] = child.pid
        save("summary.json", state)
        awake = subprocess.Popen(["caffeinate", "-i", "-w", str(child.pid)])

        def stop(signum, frame):
            state["warnings"].append(f"operator signal {signum}")
            if child.poll() is None:
                child.terminate()

        old_handlers = {s: signal.signal(s, stop) for s in (signal.SIGINT, signal.SIGTERM)}
        try:
            peak = 0
            while child.poll() is None:
                tick = monotonic()
                try:
                    row = resource_sample(child.pid, DATA, started)
                    row.update(native_sample(child.pid))
                except Exception:
                    if child.poll() is not None:
                        break
                    raise
                peak = max(peak, row["rss_bytes"])
                row.update(rss_mb=row["rss_bytes"] / 1e6, sampled_peak_rss_bytes=peak)
                samples.write(json.dumps(row, allow_nan=False) + "\n")
                samples.flush()
                unchanged(config)
                require(file_sha256(OUT / "frozen-configuration.json") == frozen_hash, "configuration changed")
                require(row["rss_bytes"] < config["observer_rss_ceiling_bytes"], "capture RSS safety ceiling")
                require(row["free_disk_bytes"] >= settings.min_free_disk_gb * 1e9, "disk safety")
                require(datetime.now(LOCAL) < datetime.fromisoformat(config["capture_hard_stop_local"]), "network cutoff guard")
                require(monotonic() - started < 22500, "capture startup/finalization exceeded 15-minute allowance")
                for health in row["health"]:
                    require("CRITICAL" not in health["backpressure"].values(), "CRITICAL recorder pressure")
                    require(not health["raw_recorder"]["last_error"] and not health["feature_recorder"]["last_error"], "writer error")
                state.update(last_sample_utc=row["utc"], last_rss_mb=row["rss_mb"])
                save("summary.json", state)
                try:
                    child.wait(timeout=max(0.1, 60 - (monotonic() - tick)))
                except subprocess.TimeoutExpired:
                    pass
        except BaseException as exc:
            state["warnings"].append(repr(exc))
            if child.poll() is None:
                child.terminate()
        finally:
            code = child.wait()  # Never SIGKILL a finalizing recorder.
            awake.wait()
            for s, handler in old_handlers.items():
                signal.signal(s, handler)
            state.update(capture_exit_code=code, capture_process_end_utc=datetime.now(UTC).isoformat())
            save("summary.json", state)
    unchanged(config)
    require(code == 0 and not state["warnings"], "capture failed or stopped by safety/operator")
    summaries = list((DATA / "sessions").glob("*/session-summary.json"))
    require(len(summaries) == 1, "expected exactly one capture session")
    session = read(summaries[0])
    require(session["status"] == "FINALIZED" and session["dataset_valid"] and not session["disk_degraded"], "session not valid/finalized")
    require(set(session["symbols"]) == {"BTCUSDT", "BTCUSDC"}, "symbols differ")
    for value in session["symbols"].values():
        require(value["duration_seconds"] >= 21540, "capture shorter than six-hour tolerance")
        require(value["sequence_gaps"] == 0, "sequence gaps")
    state.update(capture_session_id=session["capture_session_id"], session_summary=str(summaries[0]),
        session=session, status="CAPTURE_FINALIZED")
    save("summary.json", state)


def worker(phase, symbol):
    config = read(OUT / "frozen-configuration.json")
    unchanged(config)
    entries = select_manifests(DATA, symbol=symbol)
    require(bool(entries), "no input manifests")
    if phase == "raw":
        from replay.reconstruction import validate_raw_reconstruction
        from replay.streaming import iter_session
        result = asdict(validate_raw_reconstruction(iter_session(DATA, entries)))
        save(f"raw-{symbol}.json", result)
        require(result["bootstrap_snapshots"] > 0 and result["snapshots_compared"] > 0
            and result["mismatches"] == 0 and result["sequence_gap_count"] == 0
            and result["first_mismatch"] is None, "raw reconstruction failed")
    elif phase == "features":
        from features.engine import FeatureEngine
        from replay.clock import ReplayClock
        from replay.offset import RecordedClockOffset
        from replay.engine import ReplayEngine
        from replay.features import feature_equivalence_errors
        from replay.streaming import iter_session
        cfg = config["settings"]
        clock = ReplayClock()
        offsets = RecordedClockOffset(cfg["clock_sync_sample_count"])
        engine = FeatureEngine(symbol, interval_ms=cfg["feature_interval_ms"],
            book_stale_after_ms=cfg["orderbook_stale_after_ms"], trade_stale_after_ms=cfg["trade_stale_after_ms"],
            mid_history_seconds=cfg["mid_history_seconds"], trade_history_seconds=cfg["trade_history_seconds"],
            imbalance_levels=tuple(cfg["feature_imbalance_levels"]), clock_offset=offsets,
            monotonic_clock_ns=clock.monotonic_ns, wall_clock=clock.wall_time)
        result = {"source_feature_rows": sum(e.row_count for e in entries if e.event_type == "feature"),
            "snapshots_compared": 0, "mismatch_count": 0, "decimal_differences": 0,
            "float_differences": 0, "maximum_float_error": 0, "first_mismatch": None,
            "absolute_float_tolerance": 0, "retain_results": False}

        def compare(actual, recorded):
            errors = ("missing recorded",) if recorded is None else feature_equivalence_errors(actual, recorded, absolute_tolerance=0)
            if errors:
                result.update(mismatch_count=1, first_mismatch=list(errors))
                save(f"features-{symbol}.json", result)
                raise RuntimeError(f"feature mismatch: {errors}")
            result["snapshots_compared"] += 1

        replay = ReplayEngine(iter_session(DATA, entries), clock, engine, retain_results=False,
            feature_handler=compare, clock_offset=offsets).run()
        require(replay.data_quality.valid, "replay data quality invalid")
        result["row_accounting_passed"] = result["source_feature_rows"] == result["snapshots_compared"]
        save(f"features-{symbol}.json", result)
        require(result["row_accounting_passed"], "feature accounting mismatch")
    unchanged(config)


def offline(command, label, state, config):
    unchanged(config)
    state.update(status=label, phase_started_utc=datetime.now(UTC).isoformat())
    save("summary.json", state)
    started = monotonic()
    with (OUT / f"{label}.log").open("x") as log, (OUT / f"{label}-process.jsonl").open("x") as samples:
        child = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=log)
        state["offline_pid"] = child.pid
        save("summary.json", state)
        try:
            while child.poll() is None:
                try:
                    row = native_sample(child.pid)
                except OSError:
                    if child.poll() is not None:
                        break
                    raise
                samples.write(json.dumps({"elapsed_seconds": monotonic()-started, **row}) + "\n")
                samples.flush()
                require(row["native_resident_bytes"] < 6_000_000_000, "offline RSS safety ceiling")
                unchanged(config)
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    pass
        except BaseException:
            if child.poll() is None:
                child.terminate()
            child.wait()
            raise
        code = child.wait()
    state.setdefault("phase_results", {})[label] = {"exit_code": code, "wall_seconds": monotonic()-started}
    save("summary.json", state)
    require(code == 0, f"{label} failed; no automatic retry")


def resources(state):
    rows = [json.loads(line) for line in (OUT / "resources.jsonl").read_text().splitlines()]
    end = rows[-1]["elapsed_seconds"]
    state["resource_result"] = {
        "gate": "RESOURCE_GATE_WARNING — LONG-RUN BOUND NOT ESTABLISHED",
        "samples_artifact": str(OUT / "resources.jsonl"),
        "sampled_rss_mb": stats([r["rss_mb"] for r in rows]),
        "final_sample_rss_mb": rows[-1]["rss_mb"],
        "hourly_nearest": {str(h): min(rows, key=lambda r: abs(r["elapsed_seconds"]-h*3600)) for h in range(7)},
        "slopes_MB_per_hour": {"final_3h": slope(rows, max(0,end-10800),end), "final_2h": slope(rows,max(0,end-7200),end)},
        "process": {k: stats([r[k] for r in rows]) for k in ("cpu_percent_ps","open_fds","threads","sockets")},
        "storage": {k: {"last":rows[-1][k],"per_hour_slope":slope(rows,0,end,k)} for k in ("catalog_entries","catalog_bytes","finalized_bytes")},
        "limitation": "minute samples not continuous maxima; final-resource-gauges.json records live getrusage peak",
    }


def run():
    settings, config = prepare()
    state = read(OUT / "summary.json")
    awake = subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])
    try:
        capture(settings, config, state)
        resources(state)
        with DatasetLock(DATA):
            recovery = recover_dataset(DATA, repair=False)
            state["recovery"] = asdict(recovery)
            require(recovery.valid, "ambiguous recovery")
        for phase in ("raw", "features"):
            for symbol in settings.symbols:
                offline([sys.executable, str(Path(__file__).resolve()), phase, symbol], f"{phase}-{symbol}", state, config)
        state["integrity"] = {s:{p:read(OUT/f"{p}-{s}.json") for p in ("raw","features")} for s in settings.symbols}
        unchanged(config)
        state["status"] = "FREEZING"
        save("summary.json", state)
        path = freeze_dataset(DATA, select_manifests(DATA))
        frozen = verify_frozen(DATA, path)
        frozen_hash = file_sha256(path)
        state["frozen"] = {"path":str(path),"dataset_id":frozen["dataset_id"],
            "manifest_fingerprint":frozen["manifest_fingerprint"],"sha256":frozen_hash,
            "files":len(frozen["files"]),"bytes":sum(e["manifest"]["file_size"] for e in frozen["files"]),
            "row_count":sum(e["manifest"]["row_count"] for e in frozen["files"]),"verification_passed":True}
        offline([sys.executable,"-m","research.batch","--data-directory",str(DATA),"--frozen",str(path)], "STAGE_B_BATCH", state, config)
        reports = list((DATA / "research/batches").glob("*/batch-report.json"))
        require(len(reports)==1, "expected one fixed-control batch report")
        batch = read(reports[0])
        require(batch["dataset_valid"] and batch["frozen_dataset_id"]==frozen["dataset_id"], "batch dataset mismatch")
        # Compare the complete persisted JSON values, not tuple/list implementation details.
        require(
            research_controls_equal(
                batch["configuration"], config["research_controls"]["configuration"]
            ),
            "research controls changed",
        )
        require(file_sha256(path)==frozen_hash, "frozen manifest changed")
        unchanged(config)
        state.update(status="REPORT_READY_PENDING_INTERPRETATION", batch_report=str(reports[0]),
            batch_fingerprint=batch["batch_fingerprint"], finished_utc=datetime.now(UTC).isoformat(),
            stage_b_decision=None, next_action="human-readable fixed-control review only; no further capture")
    except BaseException as exc:
        state.update(status="STOPPED_INCOMPLETE", error=repr(exc), stage_b_decision=None,
            stopped_utc=datetime.now(UTC).isoformat(), next_action="investigate evidence; no automatic retry/freeze/research")
        raise
    finally:
        save("summary.json", state)
        if awake.poll() is None:
            awake.terminate()
        awake.wait()


if __name__ == "__main__":
    if len(sys.argv)==3:
        worker(sys.argv[1],sys.argv[2])
    else:
        run()
