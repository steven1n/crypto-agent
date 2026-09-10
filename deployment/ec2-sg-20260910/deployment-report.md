# Singapore EC2 installation — 2026-09-10

Instance supplied by operator: i-0b357fe2738d4a496, 54.179.165.51. SSH login ubuntu succeeded using the operator-supplied private key; key permissions tightened to 0600. Private key was not copied to the server. First connection used SSH trust-on-first-use, not independently attested host identity.

Observed: Ubuntu 26.04 LTS, x86_64, 4 logical CPUs, 15782 MiB RAM, Python 3.14.4. This is approximately 16 GiB, not the stated 8 GB. Root filesystem 19 GB, initially 17 GB available. NTP synchronized, timezone Etc/UTC. Binance public futures time endpoint HTTP 200. This does not prove WebSocket capture suitability.

Installed source under /opt/crypto-agent and recreated Linux .venv using the 28 exact installed Mac dependency versions listed in requirements-mac-pins.txt. pip check passed. Linux binary wheels are retained at /opt/crypto-deployment/wheels with accompanying linux-wheels.sha256. No Mac virtualenv, .env, historical market dataset or trading credentials uploaded. One campaign.py is included solely because comparison regression tests AST-read its guard; it is NOT a Linux launcher. No packaging build/editable installation was needed: use the existing python -m entry points with the source root on PYTHONPATH.

Installed Ubuntu python3-venv and rsync (including their package-manager-selected Python patch dependencies). Created non-login capture service account and /var/lib/crypto-capture/work. No service was started/enabled and no live capture was launched. No source semantics or research controls changed.

Linux validation: pytest 239 passed in 9.37s, exit 0; Ruff lint pass; format 109 files already formatted; strict mypy 68 source files pass; capture --help pass. Initial lint erroneously included ignored campaign tooling because .gitignore/Git context were missing from deployment; copying unchanged .gitignore and initializing an empty Git repository restored the existing discovery scope without changing code. Final chained lint/format/mypy/help/diff command exited 0. Empty Git diff is not provenance proof: checksum-mode rsync independently found no changed file contents in uploaded source/tests/docs/README/pyproject (directory mtimes differ).

Not yet qualified for long capture: no live smoke, raw reconstruction or feature equivalence on this host; no second-venv hash-locked rebuild; no full Linux resource telemetry gate; no campaign configuration frozen because none started. Disk is smaller than the existing multi-day deployment plan. No 6h/24h, historical batch/replay/reconstruction, optimization or trading ran. Deterministic unit tests used synthetic fixtures only.

Next step: a separately authorized short Linux capture qualification, not a long campaign. Decide disk expansion before multi-day retention. Existing historical Mac evidence remains on the Mac untouched.
