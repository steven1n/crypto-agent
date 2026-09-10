"""Single authorized remote sequence; never retries capture or verification."""

import json
import subprocess
import sys
import traceback
from pathlib import Path

import soak_capture
import soak_report


def main():
    root = soak_capture.ROOT
    if root.exists():
        print("Refusing existing output; no restart or overwrite", file=sys.stderr)
        return 2
    try:
        soak_capture.main()
        state = json.loads((root / "summary.json").read_text())
        if (
            state["capture_exit_code"] != 0
            or state.get("observer_error")
            or state.get("safety_stop")
        ):
            raise RuntimeError("capture/observer failed; no verification")
        with (root / "verification.log").open("x") as log:
            result = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("soak_verify.py"))],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        if result.returncode:
            raise RuntimeError("integrity verification failed; no further verification")
    except Exception as exc:
        traceback.print_exc()
        if root.exists():
            state = (
                json.loads((root / "summary.json").read_text())
                if (root / "summary.json").exists()
                else {}
            )
            state.update(
                status="STOPPED_INCOMPLETE", decision=soak_report.FAIL, runner_error=repr(exc)
            )
            soak_capture.save("summary.json", state)
        else:
            return 2
    soak_report.main()
    return (
        0 if json.loads((root / "summary.json").read_text())["decision"] != soak_report.FAIL else 2
    )


if __name__ == "__main__":
    raise SystemExit(main())
