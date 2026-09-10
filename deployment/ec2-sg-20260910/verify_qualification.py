"""Post-stop exact qualification only; no labels/bootstrap/strategy statistics."""

import json
import math
import shutil
from collections import Counter, defaultdict
from dataclasses import asdict
from decimal import Decimal

from qualify import ROOT, save

from app.campaign_observer import freeze_configuration
from config.settings import Settings
from features.engine import FeatureEngine
from market.clock import ClockOffsetSample
from market.events import BookUpdateEvent, TradeEvent
from recorder.durability import DatasetLock, recover_dataset
from recorder.frozen import select_manifests
from recorder.raw_models import RuntimeSample
from replay.clock import ReplayClock
from replay.engine import ReplayEngine
from replay.features import feature_equivalence_errors
from replay.offset import RecordedClockOffset
from replay.reconstruction import validate_raw_reconstruction
from replay.streaming import iter_session


def stats(values):
    a = sorted(values)
    if not a:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "max": None}
    return {
        "n": len(a),
        "min": a[0],
        "max": a[-1],
        **{f"p{p}": a[int((len(a) - 1) * p / 100)] for p in (50, 95, 99)},
    }


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify(state):
    require(state["status"] == "CAPTURE_STOPPED_PENDING_INTEGRITY", "capture not stopped")
    require(state["capture_wall_seconds"] >= 890, "capture ended materially before 900 seconds")
    require(state["capture_exit_code"] == 0 and state["source_unchanged"], "capture/source failure")
    settings = Settings(_env_file=None, **state["configuration"]["settings"])
    require(
        freeze_configuration(settings, 60)["source_files_sha256"]
        == state["configuration"]["source_files_sha256"],
        "source changed",
    )
    sessions = state["session_summaries"]
    require(len(sessions) == 1, "expected exactly one session")
    session = sessions[0]
    require(
        session["status"] == "FINALIZED"
        and session["dataset_valid"]
        and not session["disk_degraded"],
        "invalid capture",
    )
    require(
        session["configuration"] == state["configuration"]["settings"],
        "effective capture settings changed",
    )
    root = ROOT / "dataset"
    with DatasetLock(root):
        recovery = recover_dataset(root, repair=False)
        state["post_capture_recovery"] = asdict(recovery)
        require(recovery.valid, "recovery ambiguity")
        entries = select_manifests(root)
        require({e.symbol for e in entries} == {"BTCUSDT", "BTCUSDC"}, "missing symbol")
        groups = {s: tuple(e for e in entries if e.symbol == s) for s in ("BTCUSDT", "BTCUSDC")}
        state["raw_reconstruction"] = {}
        state["latency"] = {}
        state["recorded_runtime_queues"] = {}
        # Finish both symbols' raw validation BEFORE any feature replay.
        for symbol, selected in groups.items():
            values = defaultdict(list)
            counts = Counter()
            offset = RecordedClockOffset(settings.clock_sync_sample_count)

            def events(selected=selected, counts=counts, offset=offset, values=values):
                for event in iter_session(root, selected):
                    counts[event.event_type.value] += 1
                    payload = event.payload
                    if isinstance(payload, ClockOffsetSample):
                        offset.add(payload)
                        if payload.accepted:
                            values["accepted_clock_offset_ms"].append(payload.offset_ms)
                            values["accepted_clock_rtt_ms"].append(payload.rtt_ms)
                    elif isinstance(payload, (TradeEvent, BookUpdateEvent)):
                        kind = "trade" if isinstance(payload, TradeEvent) else "depth"
                        raw = (
                            payload.local_receive_time - payload.exchange_event_time
                        ).total_seconds() * 1000
                        values[kind + "_raw_receive_lag_ms"].append(raw)
                        lag = offset.corrected_event_lag_ms(
                            local_receive_time=payload.local_receive_time,
                            exchange_event_time=payload.exchange_event_time,
                        )
                        if lag is not None:
                            values[kind + "_corrected_receive_lag_ms"].append(lag)
                    elif isinstance(payload, RuntimeSample):
                        values["event_loop_lag_ms"].append(payload.event_loop_lag_ms)
                        values["raw_queue"].append(payload.raw_queue_depth)
                        values["feature_queue"].append(payload.feature_queue_depth)
                    yield event

            result = validate_raw_reconstruction(events())
            state["raw_reconstruction"][symbol] = {
                **asdict(result),
                "source_event_counts": dict(counts),
                "source_events": sum(counts.values()),
            }
            state["latency"][symbol] = {k: stats(v) for k, v in values.items() if "queue" not in k}
            state["recorded_runtime_queues"][symbol] = {
                k: stats(values[k]) for k in ("raw_queue", "feature_queue")
            }
            save("summary.json", state)
            print(json.dumps({"raw_symbol": symbol, **asdict(result)}), flush=True)
            require(
                result.valid and result.bootstrap_snapshots > 0 and result.first_mismatch is None,
                "raw reconstruction failed",
            )
        state["feature_equivalence"] = {}
        for symbol, selected in groups.items():
            clock = ReplayClock()
            offset = RecordedClockOffset(settings.clock_sync_sample_count)
            engine = FeatureEngine(
                symbol,
                interval_ms=settings.feature_interval_ms,
                book_stale_after_ms=settings.orderbook_stale_after_ms,
                trade_stale_after_ms=settings.trade_stale_after_ms,
                mid_history_seconds=settings.mid_history_seconds,
                trade_history_seconds=settings.trade_history_seconds,
                imbalance_levels=settings.feature_imbalance_levels,
                clock_offset=offset,
                monotonic_clock_ns=clock.monotonic_ns,
                wall_clock=clock.wall_time,
            )
            report = {
                "snapshots_compared": 0,
                "mismatch_count": 0,
                "Decimal_differences": 0,
                "float_differences": 0,
                "maximum_float_error": 0.0,
                "first_mismatch": None,
            }

            def fields(a, b, report=report):
                if isinstance(a, dict) and isinstance(b, dict):
                    for k in a.keys() | b.keys():
                        fields(a.get(k), b.get(k))
                elif isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
                    for x, y in zip(a, b, strict=False):
                        fields(x, y)
                elif isinstance(a, Decimal) or isinstance(b, Decimal):
                    report["Decimal_differences"] += int(a != b)
                elif isinstance(a, float) or isinstance(b, float):
                    if a != b:
                        report["float_differences"] += 1
                        if (
                            isinstance(a, (float, int))
                            and isinstance(b, (float, int))
                            and math.isfinite(a)
                            and math.isfinite(b)
                        ):
                            report["maximum_float_error"] = max(
                                report["maximum_float_error"], abs(a - b)
                            )
                        else:
                            report["non_numeric_or_nonfinite_float_difference"] = True

            def compare(actual, recorded, report=report, fields=fields):
                require(recorded is not None, "missing recorded feature")
                report["snapshots_compared"] += 1
                errors = feature_equivalence_errors(actual, recorded, absolute_tolerance=0)
                fields(asdict(actual), asdict(recorded))
                if errors:
                    report["mismatch_count"] += 1
                    if report["first_mismatch"] is None:
                        report["first_mismatch"] = {
                            "row": report["snapshots_compared"],
                            "fields": errors,
                        }

            result = ReplayEngine(
                iter_session(root, selected),
                clock,
                engine,
                retain_results=False,
                feature_handler=compare,
                clock_offset=offset,
            ).run()
            report["source_feature_rows"] = sum(
                e.row_count for e in selected if e.event_type == "feature"
            )
            report["data_quality"] = asdict(result.data_quality)
            report["retain_results"] = False
            state["feature_equivalence"][symbol] = report
            save("summary.json", state)
            print(json.dumps({"feature_symbol": symbol, **report}), flush=True)
            require(
                result.data_quality.valid
                and report["snapshots_compared"] == report["source_feature_rows"] > 0
                and report["mismatch_count"]
                == report["Decimal_differences"]
                == report["float_differences"]
                == report["maximum_float_error"]
                == 0
                and report["first_mismatch"] is None,
                "feature equivalence failed",
            )
        state["storage"] = {
            "files": len(entries),
            "bytes": sum(e.file_size for e in entries),
            "disk": shutil.disk_usage(ROOT)._asdict(),
        }
    samples = [json.loads(line) for line in (ROOT / "resources.jsonl").read_text().splitlines()]
    useful = [s for s in samples if "rss_bytes" in s]
    state["resource_observations"] = {
        k: stats([s[k] for s in useful])
        for k in (
            "rss_bytes",
            "vmhwm_bytes",
            "fd_count",
            "threads",
            "socket_count",
            "cpu_percent_lifetime_one_core",
        )
    }
    state["resource_observations"].update(
        sample_artifact="resources.jsonl",
        sample_errors=[s for s in samples if "sample_error" in s],
        initial_post_startup_sample=next((s for s in useful if s["elapsed_seconds"] >= 30), None),
        last_live_sample=useful[-1] if useful else None,
        scope="15-minute warmup only; no long-run boundedness conclusion",
    )
    health = [h for s in useful for h in s["health"]]
    state["sampled_pressure_states"] = {
        k: dict(Counter(h["backpressure"][k] for h in health)) for k in ("raw", "features")
    }
    require(
        all(not h[k]["last_error"] for h in health for k in ("raw_recorder", "feature_recorder")),
        "writer error",
    )
    require(
        all(v != "CRITICAL" for h in health for v in h["backpressure"].values()),
        "observed critical pressure",
    )
    require(
        freeze_configuration(settings, 60)["source_files_sha256"]
        == state["configuration"]["source_files_sha256"],
        "source changed during verification",
    )
    state["decision"] = "LINUX_LIVE_QUALIFICATION_PASS — READY_FOR_RESOURCE_SOAK"
    state["status"] = "QUALIFICATION_COMPLETE"


if __name__ == "__main__":
    state = json.loads((ROOT / "summary.json").read_text())
    try:
        verify(state)
    except Exception as exc:
        state["decision"] = "LINUX_LIVE_QUALIFICATION_FAIL — INVESTIGATION_REQUIRED"
        state["qualification_error"] = repr(exc)
        state["status"] = "QUALIFICATION_FAILED"
        raise
    finally:
        save("summary.json", state)
