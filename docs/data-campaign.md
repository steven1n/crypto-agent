# Public-data collection campaign — 2026-09-05

This is an operational campaign, not another architecture milestone. No result
from earlier minute-long smoke samples counts as campaign evidence.

## Inspection decision

The existing `app.capture`, `recorder.verify`, `recorder.compact`, and
`research.batch` commands can conduct the capture/verify/freeze/research cycle.
Freeze currently requires completed sessions. Use separate approximately 2-hour,
6-hour, and 24-hour sessions instead of adding active time-slice freezing.

Only one source module was added: `app/campaign_observer.py`. The concrete gap was
the absence of persistent process RSS/CPU measurements, which cannot be recovered
from the existing market records after capture. The external observer launches
the unchanged capture CLI, freezes effective settings and source hashes before
launch, samples RSS/CPU/catalog/disk/health every 60 seconds, and forwards graceful
stop signals. Tests cover configuration fingerprints, control preservation,
environment roundtrips, process units, safe output ownership, and a fake child.
No existing capture, recorder, research, strategy, or threshold code was changed.

The existing batch report does **not** yet supply every requested campaign table:
notably feature-to-feature correlation including RV_5s, non-overlap Spearman,
full pooled/session rank statistics, and expanded latency-tail surroundings.
These are offline checkpoint analysis tasks, recoverable from persisted raw and
feature data, not reasons to alter the capture pipeline. They must not be marked
complete merely because the existing batch command succeeded.

## Frozen controls and provenance

Each stage has `frozen-configuration.json` created before its child starts.
It contains all effective Settings, hardcoded transport limits/subscriptions,
schema versions, four strategy dataclass defaults, all six horizons, the cost
grid, bootstrap 10-second / 200 / seed 42 controls, full source file hashes, the
existing research source fingerprint, an expanded capture source fingerprint,
and a configuration fingerprint. `controls_fingerprint` excludes only output
path and requested capture duration to allow comparison between stages.

Do not edit source during a capture. The observer compares source hashes again
at exit. Runtime startup records the actual session ID and start time; the
observer start time is not a substitute for market coverage start time.

Child cwd is its new stage directory with no `.env`, with every effective
non-null setting supplied explicitly in its environment. Nullable settings use
their checked null defaults. Inherited credentials/proxy variables are not
written to the configuration artifact. No authenticated exchange interface is used.

## Operational commands

Run from the repository root, using the existing virtual environment. Paths below
are exact for Stage A; do not rerun the observer into an existing stage directory.

```sh
.venv/bin/python -m app.campaign_observer --output-directory data/campaign-20260905/stage-a-2h --duration 7200
.venv/bin/python -m recorder.verify --data-directory data/campaign-20260905/stage-a-2h/dataset --reconstruct
.venv/bin/python -m recorder.verify --data-directory data/campaign-20260905/stage-a-2h/dataset --freeze
```

Use the actual frozen JSON path returned by freeze with the implemented
`research.batch --data-directory PATH --frozen FROZEN_JSON` command. Do not invent
a frozen ID flag. Use defaults: all six horizons, all baseline thresholds,
10-second blocks, 200 replications, seed 42, and the entire cost grid.

Compaction uses `recorder.compact --data-directory SOURCE --output-directory NEW`.
Keep its default bounded fragment/row/byte limits initially. Verify, reconstruct,
freeze and replay the new directory separately; compare row counts and feature
digests and re-verify the original frozen fingerprint. Never replace the source.

## Checkpoint gates and remaining work

1. Observe active capture, using PID ownership plus `observer-status.json`, not
   filename presence alone. Read `resources.jsonl` incrementally. Do not run
   recovery/freeze against an active dataset. Do not start duplicate captures.
2. At end, require finalized healthy session summaries and unchanged source and
   effective settings. Time verification and catalog scan separately.
3. Require REST+diff reconstruction: zero Decimal mismatches and zero sequence
   gaps. Freeze only after integrity passes. Full feature replay must pass before
   interpreting any predictive/profitability result. Preserve failing artifacts.
4. Produce resource report: sampled initial/1h/2h/later/peak RSS and MB/hour trend,
   OS %CPU, catalog/file/storage growth, sampled queue pressure durations and
   percentiles. Runtime queue/loop observations are also recorded every second.
   Sampled maxima are not exact continuous maxima. Initial process RSS may be
   sampled before Python imports complete; also report a post-warmup baseline.
   Bounded rolling buffers also fill during the first minutes; show both full-run
   and post-warmup slopes before interpreting retained growth as a leak.
5. Investigate material sustained memory growth or CRITICAL pressure before
   proceeding. Existing capture disk/backpressure/sequence safety remains active.
   A growing source catalog is a candidate retained object, not proof of a leak.
6. From full raw data produce hourly clock medians/MAD/RTT/offset changes and
   accepted RTT-offset correlation; report all requested latency quantiles by
   trade/depth, neighboring arrival intervals, nearby loop lag/clock/generations.
   Do not clamp outliers or assert unproven causality. Track clock sample age:
   the current estimator retains its previous offset during REST outages.
7. Generate all requested hourly/session/pooled feature, dependence and baseline
   gross-bps/cost tables, including negative and null results. Keep invalid ticks,
   session/connection/sync boundaries and known unobserved duration separate.
   Do not bridge labels through outages. No threshold/horizon selection.
8. Compaction experiment only on a completed validated frozen checkpoint, in a
   new directory. Measure wall time/size/file reduction and peak RSS if feasible.
9. Advance to Stage B (21600 seconds), then Stage C (86400 seconds), only after
   reviewing the preceding checkpoint. Compare the same controls fingerprint.
   If controls must change, document the reason and begin a new session.
10. Write machine-readable and human-readable 2h/6h/24h comparison artifacts. Do
    not assign final A/B/C/D outcome until materially larger data is analyzed.
    No live trading under any outcome. Only C/D supported by dependence-aware
    out-of-sample evidence permits recommending realistic maker fill simulation.

## Runtime assumptions

For Stage B, the observer additionally supports `--max-rss-mb 1024`: on a sampled
capture-process RSS breach it sends SIGTERM, waits for graceful finalization and
records a warning/nonzero observer exit. The ceiling is frozen in operational
configuration, not in strategy controls. A healthy partial dataset does not
override an observer resource warning or satisfy a full-duration checkpoint.
This minimal change addresses the Stage A memory-risk observation; see
`data/campaign-20260905/stage-b-readiness.md` for evidence and limitations.

The laptop was on AC power at preflight. The observer uses macOS `caffeinate -i`
scoped to the capture child, without altering global power settings. This does
not protect against shutdown, reboot, lid-close sleep, loss of network or killing
the process. Keep the computer powered, connected, and open. Scheduled Codex
follow-ups additionally require the local app/host to be available.

Dataset validity means internally consistent recorded coverage, not uninterrupted
market observation. Stage completion is not statistical significance.

## Initial quality gates

After adding the observer: `pytest -q`: **185 passed**; `ruff check .`: pass;
`ruff format --check .`: **98 files already formatted**; strict mypy on all
source packages: **68 source files, no issues**; `git diff --check`: pass.
Git source files are still untracked; no commit was manufactured. Explicit file
content hashes, not a nonexistent Git revision, provide capture provenance.

## Stage A launch (not a completed checkpoint)

Actual session: `2745dfa6173d43ad94d02f0cfc359000`.
Start: **2026-09-05 05:26:32.305741 UTC**. Requested duration: 7200 seconds.
Target end: approximately **07:26:32 UTC / 15:26:32 Asia/Singapore**.
Observer PID 11820; capture PID 11822 (verify ownership before any signal).
The child session's effective configuration exactly matched the saved freeze.
The existing research-source fingerprint remains
`f3d096752bfaacd8821ecf752ebcd1f79d8b65a49fd7a90e7464364654d50374`.
Full configuration/source/control fingerprints are in the stage's frozen JSON
and `data/campaign-20260905/campaign-plan.json`.

The first health report at about 60 seconds showed both books SYNCED, both feature
engines emitting, one bootstrap each, zero gaps/resyncs, and both recorder queues
NORMAL. This is launch verification only. It establishes neither long-run
reliability nor predictive value. Stage B and C remain gated and not started.

Initially a current-thread follow-up was active every 30 minutes. Official scheduled-task
guidance was used to keep continuation attached to this task and to make the
local-host/app availability requirement explicit:
[scheduled tasks](https://learn.chatgpt.com/docs/automations?surface=app).

User scheduling change on 2026-09-07: the 30-minute agent polling is canceled.
The same task now checks once at **19:25 Asia/Singapore**, after Stage B's expected
19:17:56 completion. If work is still active, inspect status without mutating an
active dataset and schedule one later completion check. Do not restore periodic
mid-capture agent polling. Program-internal resource sampling, health reporting
and safety stops remain unchanged. Later capture stages use the same
post-expected-completion follow-up policy.

Latest user instruction, 2026-09-07: **cancel scheduling and validate directly**.
The existing automation was deleted. All older scheduling instructions above are
historical, not authorization to recreate it. The already-running Stage B offline
verification pipeline continues without a scheduled wake-up; do not duplicate it
or launch a new capture. See the campaign plan and verification-progress.json.
