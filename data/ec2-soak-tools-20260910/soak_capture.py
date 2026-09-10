"""One explicitly authorized 21600s Linux resource soak; no restart/research."""

import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow
from soak_preflight import preflight, pressure_sample

from app.campaign_observer import capture_environment, freeze_configuration
from config.settings import Settings
from recorder.durability import file_sha256

ROOT = Path("/var/lib/crypto-capture/work/ec2-singapore-resource-soak-6h-20260910")
TOOLS = Path("/opt/crypto-deployment/soak6h-20260910")


def save(name, value):
    path = ROOT / name
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w") as f:
        json.dump(value, f, indent=2, sort_keys=True, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


def command(*args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=15)
    return {"exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}


def sample(pid, start):
    proc = Path("/proc") / str(pid)
    status = dict(line.split(":", 1) for line in (proc / "status").read_text().splitlines())
    stat = (proc / "stat").read_text().rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    age = float(Path("/proc/uptime").read_text().split()[0]) - int(stat[19]) / ticks
    links = [os.readlink(p) for p in (proc / "fd").iterdir()]
    manifest = ROOT / "dataset/catalog/manifest.json"
    entries = json.loads(manifest.read_text()) if manifest.exists() else []
    return {
        "utc": datetime.now(UTC).isoformat(),
        "elapsed_seconds": time.monotonic() - start,
        "pid": pid,
        "process_start_ticks": int(stat[19]),
        "memory_pressure": pressure_sample(),
        "rss_bytes": int(status["VmRSS"].split()[0]) * 1024,
        "vmhwm_bytes": int(status["VmHWM"].split()[0]) * 1024,
        "vmsize_bytes": int(status["VmSize"].split()[0]) * 1024,
        "cpu_percent_lifetime_one_core": (int(stat[11]) + int(stat[12]))
        / ticks
        / max(age, 0.001)
        * 100,
        "fd_count": len(links),
        "socket_count": sum(x.startswith("socket:") for x in links),
        "threads": len(list((proc / "task").iterdir())),
        "files": len(entries),
        "finalized_bytes": sum(e["file_size"] for e in entries),
        "catalog_bytes": manifest.stat().st_size if manifest.exists() else 0,
        "free_disk_bytes": shutil.disk_usage(ROOT).free,
        "health": [
            json.loads(p.read_text())
            for p in sorted((ROOT / "dataset/sessions").glob("*/latest-health.json"))
        ],
    }


def main():
    ROOT.mkdir(exist_ok=False)
    settings = Settings(_env_file=None, **json.loads((TOOLS / "capture-settings.json").read_text()))
    config = freeze_configuration(settings, 60)
    preflight_result = preflight(settings, config)
    save("preflight.json", preflight_result)
    save("frozen-configuration.json", config)
    provenance = {
        "instance_id_operator_supplied": "i-0b357fe2738d4a496",
        "public_ip_operational_metadata": "54.179.165.51",
        "qualification_start_utc": datetime.now(UTC).isoformat(),
        "hostname": platform.node(),
        "os_release": Path("/etc/os-release").read_text(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "meminfo": Path("/proc/meminfo").read_text(),
        "root_disk": shutil.disk_usage("/")._asdict(),
        "python": sys.version,
        "pyarrow": pyarrow.__version__,
        "glibc": platform.libc_ver(),
        "arrow_allocator": pyarrow.default_memory_pool().backend_name,
        "time_sync": command("timedatectl", "show", "-p", "Timezone", "-p", "NTPSynchronized"),
        "wheel_manifest_sha256": file_sha256(Path("/opt/crypto-deployment/linux-wheels.sha256")),
        "source_fingerprint": config["source_fingerprint"],
        "configuration_fingerprint": config["configuration_fingerprint"],
        "controls_fingerprint": config["controls_fingerprint"],
        "preflight": preflight_result,
        "runner_sha256": file_sha256(Path(__file__)),
    }
    save("host-provenance.json", provenance)
    state = {"status": "CAPTURING", "provenance": provenance, "configuration": config}
    env = capture_environment(
        settings, {"PATH": "/usr/bin:/bin", "TZ": "UTC", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    )
    start = time.monotonic()
    with (ROOT / "capture.log").open("x") as log, (ROOT / "resources.jsonl").open("x") as samples:
        process = subprocess.Popen(
            [sys.executable, "-m", "app.capture"], cwd=ROOT, env=env, stdout=log, stderr=log
        )
        state["capture_pid"] = process.pid
        save("summary.json", state)

        def forward_stop(signum, frame):
            state["operator_stop_signal"] = signum
            if process.poll() is None:
                process.terminate()

        previous = {
            sig: signal.signal(sig, forward_stop) for sig in (signal.SIGINT, signal.SIGTERM)
        }
        try:
            while process.poll() is None:
                try:
                    row = sample(process.pid, start)
                except (OSError, KeyError, ValueError) as exc:
                    row = {"utc": datetime.now(UTC).isoformat(), "sample_error": repr(exc)}
                if row.get("rss_bytes", 0) >= 8_000_000_000:
                    state["safety_stop"] = "external observer RSS reached 8 GB machine-safety limit"
                    process.terminate()
                samples.write(json.dumps(row, sort_keys=True) + "\n")
                samples.flush()
                os.fsync(samples.fileno())
                print(
                    json.dumps(
                        {
                            k: row.get(k)
                            for k in ("elapsed_seconds", "rss_bytes", "files", "sample_error")
                        }
                    ),
                    flush=True,
                )
                try:
                    process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    pass
        except BaseException as exc:
            state["observer_error"] = repr(exc)
        finally:
            if process.poll() is None:
                process.terminate()
            state["capture_exit_code"] = process.wait()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    state["capture_wall_seconds"] = time.monotonic() - start
    state["ended_utc"] = datetime.now(UTC).isoformat()
    state["source_unchanged"] = (
        freeze_configuration(settings, 60)["source_files_sha256"] == config["source_files_sha256"]
    )
    state["session_summaries"] = [
        json.loads(p.read_text())
        for p in (ROOT / "dataset/sessions").glob("*/session-summary.json")
    ]
    state["status"] = "CAPTURE_STOPPED_PENDING_INTEGRITY"
    save("summary.json", state)
    print(
        json.dumps({"status": state["status"], "capture_exit_code": state["capture_exit_code"]}),
        flush=True,
    )


if __name__ == "__main__":
    main()
