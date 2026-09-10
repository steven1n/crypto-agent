"""Read-only provenance checks and separate optional offline wheel rebuild."""

import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

import pyarrow

from recorder.durability import file_sha256

BASE = Path("/opt/crypto-deployment")
SOURCE = "2822085ea27f48d346d3f61f4dafd6a0a1c70c5620400fa281a943de3efadf93"
CONTROLS = "cc225b299a296c97d626bda39f7ee9045638ee298084f44727e3abbb34bfbb2d"
WHEELS = "32fe5492f036b16dd8f7e9cb6584f9a9b8e0bff07d17a2de31669ed5731cf103"


def pressure_sample():
    vm = dict(x.split() for x in Path("/proc/vmstat").read_text().splitlines())
    mem = dict(x.split(":", 1) for x in Path("/proc/meminfo").read_text().splitlines())
    result = {
        "oom_kill": int(vm["oom_kill"]),
        "mem_available_bytes": int(mem["MemAvailable"].split()[0]) * 1024,
    }
    try:
        result["memory_psi"] = {
            line.split()[0]: {
                k: float(v) for k, v in (part.split("=") for part in line.split()[1:])
            }
            for line in Path("/proc/pressure/memory").read_text().splitlines()
        }
    except OSError as exc:
        result["psi_error"] = repr(exc)
    return result


def preflight(settings, config):
    if config["source_fingerprint"] != SOURCE or config["controls_fingerprint"] != CONTROLS:
        raise RuntimeError("source or controls provenance drift")
    if settings.capture_duration_seconds != 21600 or settings.trading_enabled:
        raise RuntimeError("incorrect duration or trading enabled")
    if (
        platform.python_version() != "3.14.4"
        or pyarrow.__version__ != "23.0.1"
        or pyarrow.default_memory_pool().backend_name != "mimalloc"
    ):
        raise RuntimeError("accepted runtime version/allocator drift")
    if platform.machine() != "x86_64" or os.cpu_count() != 4:
        raise RuntimeError("host architecture/CPU drift")
    if (
        platform.release() != "7.0.0-1006-aws"
        or platform.libc_ver() != ("glibc", "2.43")
        or 'VERSION_ID="26.04"' not in Path("/etc/os-release").read_text()
    ):
        raise RuntimeError("OS/kernel/glibc provenance drift")
    mem = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    if not 15e9 <= int(mem["MemTotal"].split()[0]) * 1024 <= 18e9:
        raise RuntimeError("host RAM drift")
    if shutil.disk_usage("/").free < 5_000_000_000:
        raise RuntimeError("insufficient operational free-disk headroom")
    if file_sha256(BASE / "linux-wheels.sha256") != WHEELS:
        raise RuntimeError("wheel manifest drift")
    wheels = subprocess.run(
        ["sha256sum", "-c", "linux-wheels.sha256"],
        cwd=BASE,
        text=True,
        capture_output=True,
        check=True,
    )
    timed = subprocess.run(
        ["timedatectl", "show", "-p", "Timezone", "-p", "NTPSynchronized"],
        text=True,
        capture_output=True,
        check=True,
    )
    if "NTPSynchronized=yes" not in timed.stdout or "Timezone=Etc/UTC" not in timed.stdout:
        raise RuntimeError("timezone/NTP preflight failed")
    base = "http://169.254.169.254/latest/"
    req = urllib.request.Request(
        base + "api/token", method="PUT", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        token = response.read().decode()
    req = urllib.request.Request(
        base + "dynamic/instance-identity/document", headers={"X-aws-ec2-metadata-token": token}
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        identity = json.load(response)
    if identity["instanceId"] != "i-0b357fe2738d4a496" or identity["region"] != "ap-southeast-1":
        raise RuntimeError("instance identity drift")
    result = {
        "instance_identity": {
            k: identity[k]
            for k in ("instanceId", "instanceType", "region", "availabilityZone", "imageId")
        },
        "wheel_verification": wheels.stdout,
        "time_sync": timed.stdout,
        "observer_files_sha256": {
            p.name: file_sha256(p) for p in Path(__file__).parent.glob("*.py")
        },
    }
    # Separate fresh environment, local retained wheels only; never touch active .venv.
    target = Path(tempfile.mkdtemp(prefix="crypto-soak-wheel-check-"))
    commands = [
        [sys.executable, "-m", "venv", str(target)],
        [
            str(target / "bin/python"),
            "-m",
            "pip",
            "install",
            "--no-index",
            "--find-links",
            str(BASE / "wheels"),
            "-r",
            str(BASE / "requirements-mac-pins.txt"),
        ],
        [str(target / "bin/python"), "-m", "pip", "check"],
        [
            str(target / "bin/python"),
            "-c",
            "import pyarrow; print(pyarrow.__version__,pyarrow.default_memory_pool().backend_name)",
        ],
    ]
    rebuild = {"path": str(target), "success": True, "steps": []}
    for args in commands:
        try:
            completed = subprocess.run(args, text=True, capture_output=True, timeout=180)
            rebuild["steps"].append(
                {
                    "command": args,
                    "exit_code": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                }
            )
            if completed.returncode:
                rebuild["success"] = False
                break
        except Exception as exc:
            rebuild["success"] = False
            rebuild["error"] = repr(exc)
            break
    result["optional_throwaway_rebuild"] = rebuild
    result["pressure_before_capture"] = pressure_sample()
    return result
