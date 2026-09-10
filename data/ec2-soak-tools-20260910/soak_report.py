"""Fixed operational gate summary; never computes alpha or starts work."""

import json

from soak_capture import ROOT, save

from recorder.durability import file_sha256

PASS = "RESOURCE_GATE_PASS — READY_FOR_24H_CAMPAIGN"
WARNING = "RESOURCE_GATE_WARNING — LONG-RUN BOUND NOT ESTABLISHED"
FAIL = "RESOURCE_GATE_FAIL — RESOURCE_OR_CORRECTNESS_FAILURE"


def slope(rows, start, end, field="rss_bytes", scale=1e6):
    points = [
        (r["elapsed_seconds"] / 3600, r[field] / scale)
        for r in rows
        if start <= r["elapsed_seconds"] <= end
    ]
    if len(points) < 3:
        return None
    mx = sum(x for x, y in points) / len(points)
    my = sum(y for x, y in points) / len(points)
    denom = sum((x - mx) ** 2 for x, y in points)
    return sum((x - mx) * (y - my) for x, y in points) / denom if denom else None


def evaluate(state, rows):
    good = [r for r in rows if "rss_bytes" in r]
    if not good:
        return {"decision": FAIL, "reasons": ["no live resource observations"]}
    end = good[-1]["elapsed_seconds"]
    checkpoints = {}
    for name, target in [("10m", 600)] + [(f"{h}h", h * 3600) for h in range(1, 7)]:
        nearest = min(good, key=lambda r: abs(r["elapsed_seconds"] - target))
        checkpoints[name] = (
            {
                k: nearest[k]
                for k in (
                    "elapsed_seconds",
                    "rss_bytes",
                    "vmhwm_bytes",
                    "fd_count",
                    "threads",
                    "socket_count",
                )
            }
            if abs(nearest["elapsed_seconds"] - target) <= 90
            else None
        )
    late = {
        name: slope(good, start, end)
        for name, start in [
            ("10m_to_end", 600),
            ("1h_to_end", 3600),
            ("2h_to_end", 7200),
            ("final_3h", end - 10800),
            ("final_2h", end - 7200),
        ]
    }
    deltas = {
        f"{h - 1}h_to_{h}h": (
            checkpoints[f"{h}h"]["rss_bytes"] - checkpoints[f"{h - 1}h"]["rss_bytes"]
        )
        / 1e6
        if checkpoints[f"{h}h"] and checkpoints[f"{h - 1}h"]
        else None
        for h in range(2, 7)
    }
    warning = []
    failure = []
    if state.get("decision") != "INTEGRITY_PASS_PENDING_RESOURCE_REVIEW":
        failure.append("capture/integrity verification did not pass")
    if state.get("safety_stop") or state.get("observer_error"):
        failure.append("observer safety/error")
    if end < 21500:
        failure.append("not a completed 6-hour live observation")
    peak = max(r["rss_bytes"] for r in good)
    if peak >= 3e9:
        warning.append("RSS not comfortably below 3 GB")
    if late["final_3h"] is None or late["final_3h"] > 15:
        warning.append("final 3h slope exceeds +15 MB/h or unavailable")
    if late["final_2h"] is None or late["final_2h"] > 20:
        warning.append("final 2h slope exceeds +20 MB/h or unavailable")
    if deltas["5h_to_6h"] is None or (
        deltas["5h_to_6h"] > 20 and deltas["5h_to_6h"] > (deltas["4h_to_5h"] or 0)
    ):
        warning.append("late hourly acceleration/coverage requires review")
    before = state.get("provenance", {}).get("preflight", {}).get("pressure_before_capture", {})
    after = good[-1].get("memory_pressure", {})
    oom_delta = after.get("oom_kill", 0) - before.get("oom_kill", 0) if before and after else None
    psi_delta = {}
    if oom_delta is None:
        warning.append("OOM telemetry unavailable")
    elif oom_delta > 0:
        failure.append("host OOM kill counter increased during capture")
    if "memory_psi" in before and "memory_psi" in after:
        psi_delta = {
            key: after["memory_psi"][key]["total"] - before["memory_psi"][key]["total"]
            for key in ("some", "full")
        }
        if any(v > 0 for v in psi_delta.values()):
            warning.append("host memory pressure observed; cannot attribute causally")
    else:
        warning.append("memory PSI unavailable")
    post = [r for r in good if r["elapsed_seconds"] >= 600]
    counts = (
        {
            key: {
                "min": min(r[key] for r in post),
                "max": max(r[key] for r in post),
                "final": post[-1][key],
                "late_2h_slope_per_hour": slope(good, end - 7200, end, key, 1),
            }
            for key in ("fd_count", "threads", "socket_count")
        }
        if post
        else {}
    )
    for key in ("fd_count", "threads"):
        if not counts or (counts[key]["late_2h_slope_per_hour"] or 0) > 0.5:
            warning.append(key + " sustained-growth ambiguity")
    health = [h for r in good for h in r.get("health", [])]
    if not health:
        warning.append("no recorder health snapshots")
    for h in health:
        if any(h[k].get("last_error") for k in ("raw_recorder", "feature_recorder")):
            failure.append("writer error")
        if "CRITICAL" in h["backpressure"].values():
            failure.append("CRITICAL pressure observed")
        if "WARNING" in h["backpressure"].values():
            warning.append("WARNING pressure observed")
    if any("sample_error" in r for r in rows):
        warning.append("resource sample errors")
    if any(
        b["elapsed_seconds"] - a["elapsed_seconds"] > 180
        for a, b in zip(good, good[1:], strict=False)
    ):
        warning.append("resource telemetry gaps")
    queue = state.get("recorded_runtime_queues", {})
    for metrics in queue.values():
        if metrics["raw_queue"]["max"] >= 10000 or metrics["feature_queue"]["max"] >= 1000:
            warning.append("queue reached >=10% capacity")
    decision = FAIL if failure else WARNING if warning else PASS
    return {
        "decision": decision,
        "failure_reasons": sorted(set(failure)),
        "warning_reasons": sorted(set(warning)),
        "rss_checkpoints": checkpoints,
        "first_observed_at_or_after_10m": next(
            (r["elapsed_seconds"] for r in good if r["elapsed_seconds"] >= 600), None
        ),
        "baseline_note": "10-minute post-warmup observation, not proven stable",
        "rss_slopes_MB_per_hour": late,
        "hourly_rss_deltas_MB": deltas,
        "any_negative_hourly_delta": any(v < 0 for v in deltas.values() if v is not None),
        "final_live_rss_bytes": good[-1]["rss_bytes"],
        "max_sampled_rss_bytes": peak,
        "max_observed_VmHWM_bytes": max(r["vmhwm_bytes"] for r in good),
        "fd_thread_socket_post10m": counts,
        "oom_kill_delta": oom_delta,
        "memory_psi_stall_microseconds_delta": psi_delta,
        "memory_event_scope": "host /proc counters, not proof of allocator causality",
        "live_start_free_bytes": state.get("provenance", {}).get("root_disk", {}).get("free"),
        "final_live_free_bytes": good[-1]["free_disk_bytes"],
        "samples": len(rows),
        "telemetry_end_seconds": end,
        "gate_rules": (
            "user fixed late 3h <=15MB/h, late 2h <=20MB/h; WARNING on missing/ambiguous "
            "evidence, pressure, or final-hour growth >20MB and greater than preceding hour"
        ),
    }


def main():
    state = json.loads((ROOT / "summary.json").read_text())
    path = ROOT / "resources.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
    gate = evaluate(state, rows)
    state["resource_gate"] = gate
    state["decision"] = gate["decision"]
    state["status"] = "REPORT_READY_PENDING_REVIEW"
    if state.get("storage") and state.get("capture_wall_seconds"):
        state["storage"].update(
            rate_denominator="observer monotonic duration, including startup/finalization",
            gb_per_hour=state["storage"]["bytes"] / state["capture_wall_seconds"] * 3600 / 1e9,
            projected_gb_per_day=state["storage"]["bytes"]
            / state["capture_wall_seconds"]
            * 86400
            / 1e9,
        )
    sessions = state.get("session_summaries", [])
    state["lifecycle_summary"] = {
        symbol: {
            "connections": metrics["counts"].get("websocket_connections", 0),
            "reconnects": metrics["counts"].get("websocket_reconnects", 0),
            "disconnects": metrics["counts"].get("websocket_disconnects", 0),
            "resyncs": metrics["resyncs"],
            "bootstrap_snapshots": metrics["counts"].get("bootstrap_snapshots", 0),
            "invalid_feature_ticks": metrics["counts"].get("feature_invalid_count", 0),
            "invalid_feature_ratio": metrics["counts"].get("feature_invalid_count", 0)
            / max(metrics["counts"].get("features_emitted", 0), 1),
            "sequence_gaps": metrics["sequence_gaps"],
        }
        for session in sessions
        for symbol, metrics in session["symbols"].items()
    }
    state["artifact_sha256"] = {
        p.name: file_sha256(p)
        for p in (
            ROOT / "resources.jsonl",
            ROOT / "capture.log",
            ROOT / "frozen-configuration.json",
        )
        if p.exists()
    }
    state["mac_comparison"] = {
        "decision_unchanged": WARNING,
        "reference": "2026-09-09 Mac 6h accepted report",
        "Mac_last_sampled_RSS_MB": 602.161,
        "Mac_final3h_slope_MB_per_hour": 47.03,
        "Mac_final2h_slope_MB_per_hour": 61.74,
        "Mac_FD_max": 12,
        "Mac_threads_max": 5,
        "limits": (
            "Different sessions/environments/market workloads. No causal attribution. "
            "Mac remains WARNING. No equivalent full raw per-event latency comparison."
        ),
    }
    save("summary.json", state)
    text = "# Singapore EC2 6-hour resource soak — 2026-09-10\n\n" + gate["decision"] + "\n\n"
    text += (
        "No Stage B, optimization, freeze, resize, new campaign or trading. "
        "Quantiles are sampled, not continuous maxima. Ten-minute baseline is not "
        "proof of stability; VmHWM excludes unsampled final flush peaks. "
        "CPU is lifetime average per one core. Clock/runtime rows are shared observations.\n\n"
    )
    for title, key in [
        ("Host and configuration", "provenance"),
        ("Frozen controls and schemas", "configuration"),
        ("Capture/session counts and lifecycle", "session_summaries"),
        ("Coverage ratios and reconnects", "lifecycle_summary"),
        ("Resource gate, RSS slopes and hourly deltas", "resource_gate"),
        ("Native resource observations", "resource_observations"),
        ("Queue occupancy", "recorded_runtime_queues"),
        ("Pressure states", "sampled_pressure_states"),
        ("Disk/files", "storage"),
        ("Latency and clocks", "latency"),
        ("Raw reconstruction", "raw_reconstruction"),
        ("Exact feature replay", "feature_equivalence"),
        ("Mac comparison (separate result)", "mac_comparison"),
    ]:
        text += (
            "## "
            + title
            + "\n\n```json\n"
            + json.dumps(state.get(key), indent=2, sort_keys=True)
            + "\n```\n\n"
        )
    text += (
        "Only after review may the next campaign be explicitly authorized. "
        "No next campaign was launched.\n\n" + gate["decision"] + "\n"
    )
    (ROOT / "ec2-singapore-resource-soak-6h-20260910.md").write_text(text)


if __name__ == "__main__":
    main()
