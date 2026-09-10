# Crypto Agent

Crypto Agent is a public-market-data and offline quantitative-research system for Binance
USDⓈ-M perpetual futures. It records versioned normalized data, deterministically replays the
same `FeatureEngine` used during capture, creates forward labels, and evaluates transparent
shadow strategies after explicit transaction costs.

This repository contains no authenticated API, secret handling, order placement, leverage,
LLM, ML model, or parameter optimizer. `TRADING_ENABLED=true` is rejected.

## Architecture

```text
One CaptureRuntime: session ID, sequencer, REST client, clock, supervised recorders
  ├─ BTCUSDT: socket → synchronizer/book → trade windows/history → FeatureEngine
  └─ BTCUSDC: socket → synchronizer/book → trade windows/history → FeatureEngine
        ↓                                      ↓
REST bootstrap + diff depth + trades     trusted books + feature ticks
        └──────── atomic Parquet / catalog ─────┘
                           ↓
session-by-session streaming replay + raw reconstruction equivalence
                           ↓
hour/session diagnostics → dependence checks → fixed-signal edge/cost tables
```

The raw capture stores every normalized diff-depth event and every trusted top-10 book snapshot.
Every REST bootstrap is also persisted, including all returned levels, request/response wall
and monotonic times, exchange E/T if available, `lastUpdateId`, limit, reason, offset, and
connection/sync generation. `buffered_final_update_ids` identifies raw diffs already queued
when REST completed. Reconstruction uses only these REST/diff records and `U/u/pu`, never
recorded trusted prices; trusted books are the independent comparison target. All prices,
quantities and reconstructed Decimal-derived values compare exactly.

Both symbols are captured simultaneously through `SYMBOLS=BTCUSDT,BTCUSDC`. Symbol identity is carried
through raw rows, features, labels, catalog entries, signals, trades, and research runs. There is
no cross-symbol price substitution or lead/lag logic. BTCUSDT is the primary discovery/research
feed; BTCUSDC is a candidate for future execution research, not an assumed cheaper venue.
Each pipeline owns its connection, book, sequence state, trade windows, feature history, health
and counters. Book resyncs stay local; shared recorder/disk integrity failures stop the dataset
session fail-closed. The clock is intentionally shared because both feeds use the same server.

## Dataset layout and catalog

```text
data/
  catalog/manifest.json
  raw/exchange=binance/market=usdm/symbol=BTCUSDT/date=YYYY-MM-DD/hour=HH/
    trade-*.parquet
    depth-*.parquet
    trusted_book-*.parquet
    stream_lifecycle-*.parquet
    book_lifecycle-*.parquet
    clock_sample-*.parquet
    orderbook_bootstrap_snapshot-*.parquet
    runtime_sample-*.parquet
  features/exchange=binance/symbol=BTCUSDT/date=YYYY-MM-DD/
    features-*.parquet
  research/
    batches/<fingerprint>/
      batch-report.json
      segments/<session-symbol-UTC-hour>.json
      labels/<session-symbol-UTC-hour>.parquet
      edge-vs-cost/<session-symbol-UTC-hour>.json
    runs/<run_id>/
      config.json
      metrics.json
      trades.parquet
      signals.parquet
      summary.md
    reports/
  sessions/<capture-session-id>/session-summary.json
  sessions/<capture-session-id>/latest-health.json
  frozen/<dataset-fingerprint>.json
  quarantine/
```

Each manifest entry includes dataset/schema versions, exchange, market type, symbol, exchange
and local time ranges, capture sequence range, rows, event type, path, size, creation time,
session ID, optional Git revision, configuration fingerprint, clock summary, resync/gap counts,
and a health flag. The JSON catalog and all recorder Parquet fragments are committed by atomic
rename. Git is optional; unavailable revisions are `null`.

Every file has a SHA-256 content checksum and an embedded provisional manifest in its Parquet
footer. Readers select an exact Arrow schema by event type/version; unknown schemas are invalid.

Feature schema v2 renames `aggregate_trade_event_count_{1,5,10}s` to
`trade_event_count_{1,5,10}s`. The v1 reader explicitly maps the names without changing values;
it does not convert an aggregate message into individual fills. **Version alone does not prove
trade source**: earlier v1 feature captures also used the individual feed. Consult the companion
raw `trade_source`; a legacy dataset without source provenance remains unidentified and is not
silently labeled INDIVIDUAL. Historical AGGREGATE rows remain AGGREGATE. Raw trade schema v1 keeps
the historical `aggregate_trade_id` column for compatibility; for INDIVIDUAL it is Binance `t`
and equals first/last trade ID. No historical file is rewritten by migration.

Lifecycle v2 adds connection/sync generations; clock v2 adds full timing and acceptance fields.
Readers retain explicit v1 support. Bootstrap and runtime heartbeat schemas begin at v1.

The live client accepts both Binance aggregate-trade and individual-trade payloads. Current live
capture subscribes to the richer individual `@trade` feed and records `trade_source=INDIVIDUAL`;
each message maps to one normalized trade with identical first/last trade IDs. Zero-price,
zero-quantity non-market markers are rejected before feature processing. Existing aggregate
payloads remain supported and are tagged `AGGREGATE`.

## Deterministic ordering and time

`capture_seq` is allocated from one process-wide sequencer immediately before every raw,
lifecycle, clock, trusted-book, or feature record. It is strictly increasing within one capture
session and is the canonical cross-stream replay key. Parquet filenames never determine order.
Within depth events, Binance `U/u/pu` continuity remains mandatory.
Restart creates a new session UUID and resets process sequence. Reconnect keeps the session
but increments that symbol's connection generation. Each REST synchronization attempt gets a
new sync generation, including retries on the same connection. Original connection-event
monotonic time is preserved, so reconnect health ages reproduce before the first new trade.

The loader fails closed on missing required event types/files, unsupported versions, schema or
catalog count mismatch, corrupt Parquet, symbol mismatch, in-file ordering inversion, duplicate
capture sequence, negative monotonic deltas, and L2 sequence gaps. A strategy result is separate
from replay/data-quality validity.

`ReplayClock` owns replay wall time and monotonic nanoseconds. It advances only from recorded
events; historical timing logic never calls the real monotonic clock. `FAST` consumes all events
without sleeping and `STEP` advances one event at a time. Both modes therefore produce the same
features, signals, shadow trades, and metrics.

When raw and recorded features coexist, replay recomputes at each recorded feature tick and
compares **every** snapshot field: identity/time, all book/depth fields, all horizons/windows,
trade event counts/volumes/VWAP, validity and health. `Decimal` fields match exactly; derived floats use an absolute tolerance of
`1e-12`.

## Features and labels

The feature definitions remain point-in-time:

- `mid = (best_bid + best_ask) / 2`
- `spread_bps = spread / mid × 10,000`
- `microprice = (ask × bid_qty + bid × ask_qty) / (bid_qty + ask_qty)`
- `imbalance_N = (bid_volume_N - ask_volume_N) / (bid_volume_N + ask_volume_N)`
- historical return uses the observation at or before `t-h`
- realized volatility uses observed consecutive log returns without interpolation
- trade imbalance uses aggressive buy/sell volume over 1 s, 5 s, and 10 s

Forward labels are isolated in `research/labels.py` and cover 250 ms, 500 ms, 1 s, 2 s, 5 s,
and 10 s:

```text
future_log_return_h = ln(first_mid_at_or_after(t+h) / mid_t)
```

Favorable/adverse log-return excursions in basis points are generated for 1 s, 2 s, and 5 s.
Missing tail labels remain `None`; they are never filled with zero. Strategy modules import only
`FeatureSnapshot` and `Signal` types and never receive `LabelSnapshot`. Tests separately prove
that feature history cannot use an observation after its historical boundary while labels may
intentionally use the first observation after a future boundary.

Research datasets split chronologically into 60% train, 20% validation, and 20% test by default.
The actual first-at-or-after future observation is purged if it crosses validation/test,
including irregular cadence. Batch labels also stop at lifecycle integrity boundaries, invalid
feature ticks, gaps over 3 seconds and session ends; short resyncs are not bridged just because
their gap is less than 3 seconds. Future context is retained across ordinary UTC-hour boundaries.
There is no random shuffle and no parameter fitting on test data.

## Diagnostics and shadow evaluation

The diagnostics module reports Pearson and tied-rank Spearman correlation, quantile-bin sample
count/mean/standard deviation/standard error, and symmetric directional hit rates at fixed
thresholds 0.2, 0.4, and 0.6. Data quality covers missing/NaN/Inf rates, min/median/p95/p99/max,
positive prices, nonnegative quantities/depth, spread, microprice-inside-BBO, and imbalance
bounds. A nonzero correlation alone is not treated as statistical significance.

Four deliberately fixed baselines are included:

- microprice momentum: microprice offset and L1 imbalance agree;
- book imbalance: symmetric L5 threshold;
- trade flow: symmetric 1-second aggressive-flow threshold;
- combined baseline: microprice, L5 book, and 1-second trade flow all agree.

They emit immutable `Signal` objects only. The offline simulator allows one fixed-notional LONG
or SHORT position and exits at a fixed 250 ms, 500 ms, 1 s, 2 s, or 5 s time stop. It tracks entry
and exit mids/executable prices, quantity, gross PnL, fees, slippage, net PnL, holding time, MFE,
MAE, and reason.

For side sign `s` (`+1` LONG, `-1` SHORT) and quantity `q = notional / entry_mid`:

```text
gross_pnl = s × q × (exit_mid - entry_mid)
fees = q × (entry_exec × entry_fee_bps + exit_exec × exit_fee_bps) / 10,000
slippage_cost = gross_pnl - executable_pnl_before_fees
net_pnl = gross_pnl - slippage_cost - fees
```

`TAKER_TAKER` uses taker fees on both legs and, when enabled, crosses half the observed spread on
each leg. `MAKER_TAKER` uses maker entry/taker exit fees, does not cross the entry spread, and
rejects entry unless the caller explicitly supplies a maker-fill observation. Fee and slippage
values are configuration, not hardcoded promotional exchange rates.

Reports include gross and net PnL, side counts, costs, per-trade distribution, win/loss metrics,
profit factor, drawdown, consecutive losses, holding time, MFE/MAE, turnover, cost/gross-edge
ratio, gross edge per trade in basis points, and break-even round-trip cost. Edge is evaluated in
underlying-price basis points before any leverage discussion.

## Long-duration durability and health

Capture obtains an OS dataset lock before recovery and holds it through shutdown. Startup
validates finalized files and manifests: content hashes, exact schema, row identity/order,
sequence bounds, missing files and duplicate IDs/paths. Interrupted `.tmp` files are moved to
quarantine, not repaired into healthy Parquet. A finalized orphan is reconciled only when its
embedded manifest and contents validate. Unknown or ambiguous corruption is reported invalid;
capture refuses to start. An old RUNNING session becomes INTERRUPTED/invalid, while a new
session may capture independently. Recovery never calls the interrupted session complete.

Rotation uses whichever comes first: row count or elapsed buffer age. Default shared raw
buffer: 20,000 records / 30 seconds; feature buffer: 6,000 / 60 seconds. Partitions split each
batch by symbol/type/hour (features by date). These conservative bounds avoid the many tiny
files generated by 1-second flushing. The measured smoke storage projection includes a more
aggressive 10-second policy; compare actual bytes after a longer capture before tuning defaults.
Serialization, compression, disk I/O and catalog registration execute in worker threads, not
the WebSocket receive callback. Finalization is temp write → footer/read-back → fsync → atomic
rename → directory fsync → catalog update. An interrupted file cannot be cataloged healthy.

Queues have hard capacities (raw 100,000; features 10,000). WARNING starts at 70%, CRITICAL at
90%. The 1-second supervisor stops capture and marks it degraded at CRITICAL; a sudden full
queue raises immediately, never drops silently. `MIN_FREE_DISK_GB=2` is checked before each
new file and by the supervisor. Below reserve, no new Parquet is opened; data remaining in
memory may be unflushable, so the session stays invalid. No automatic deletion occurs.

SIGINT/SIGTERM stop admission and feature work, cancel pending REST/clock work, drain/finalize
available recorder buffers, update the catalog and session summary, then close sockets. A
failed summary write still closes streams and leaves the RUNNING marker for startup recovery.
SIGKILL/power loss cannot preserve RAM-only records; the durable prefix survives and the
interrupted session is invalid. Supervise the command externally if OS-level auto-restart is
desired; this milestone does not install a system service or start days of unattended capture.

Each 60-second health report includes rolling/cumulative symbol rates, book/socket health,
lag and spread quantiles, generations, resync/gap counters, clock status and queue pressure.
Quantiles use the last 6,000 observations, explicitly labeled; counts/mean/max are cumulative.
REST clock samples preserve send/receive/server times, RTT, offset and accepted/rejected status.
Raw wall lag and corrected lag are separate and may be negative; neither is clamped. Clock
median/MAD/min-RTT/drift are rolling diagnostics, not claims of a precise one-way latency.
Network clock failures increment counters; no server timestamp is fabricated for failed requests.
Timestamped event-loop heartbeat samples are persisted each second. Offline latency reports
retain bounded top-tail observations with interarrival gaps and nearby heartbeat lag, allowing
inspection of local stalls versus buffered bursts without claiming causal identification.

Socket receive inactivity triggers reconnect (default 5 seconds); malformed market messages
fail capture closed rather than silently skip. Known non-market zero markers remain excluded.
WebSocket queues, book buffers, duplicate caches, clock/latency samples and recorder batches
are bounded. Mid/trade windows have hard 200,000-observation caps and fail closed if exceeded.
Known book levels are retained up to 100,000 per side, then resnapshot; no silent deep pruning.
The capture schema supports exactly L1/L5/L10; extra configured feature depths are rejected
before startup instead of being silently lost during flattening.
If trusted top-10 depth leaves the price range covered by a truncated REST snapshot, resnapshot
is required. Catalog/manifest metadata scales with file count and should be monitored on multi-day runs.

`session-summary.json` contains per-symbol counts, rates, files/bytes, invalid features,
generations, clock/lag/spread/book-age summaries, recovery/disk warnings and validity/reasons;
it contains no profitability metrics. `dataset_valid` means an internally consistent capture
with explicit outages, not uninterrupted coverage or statistical quality. Batch independently
requires raw reconstruction and full feature equivalence before marking research valid.

## Compaction and immutable freezes

`recorder.compaction.compact_parquet_fragments` merges only compatible schemas, sorts by declared
canonical keys (`capture_seq` by default), verifies row count/content/schema after a read-back,
and commits through a temporary file plus atomic rename. Sources are retained by default.
Cataloged/frozen source removal is prohibited. `python -m recorder.compact` writes a new
directory/version while retaining every original. Batches are capped by fragment count,
compressed input bytes (64 MB) and input rows (200,000); decompression can require more memory.
The new layout gets updated counts and provenance. A frozen selection lists files/checksums,
schema versions, symbols/time range, final summary hashes and health, under a content-addressed
dataset ID. Repeated identical selection gives the same fingerprint regardless of input order.
Capture may append new sessions outside a frozen selection; frozen files/summaries are not
changed. Verification detects modifications; this is application-enforced immutability, not WORM storage.

## Install and run capture

Python 3.12 or newer is required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install '.[dev]'
cp .env.example .env
python -m app.capture --symbols BTCUSDT,BTCUSDC --data-directory data/capture-20260905
```

Default capture is simultaneous BTCUSDT/BTCUSDC, continuous until stopped. `SYMBOL` is retained
for legacy configuration, but `SYMBOLS`/`--symbols` controls the recorder. For a bounded smoke:

```bash
python -m app.capture --duration 120 --rotation-seconds 10 --data-directory data/smoke
python -m recorder.verify --data-directory data/smoke --reconstruct
python -m recorder.verify --data-directory data/smoke --repair
python -m recorder.verify --data-directory data/smoke --freeze
python -m research.batch --data-directory data/smoke --frozen data/smoke/frozen/<fingerprint>.json
python -m research.batch --data-directory data/capture-20260905 --symbol BTCUSDT \
  --from 2026-09-05T00:00:00+00:00 --to 2026-09-06T00:00:00+00:00 \
  --block-length-ms 10000 --bootstrap-replications 200 --seed 42 \
  --taker-taker-cost-bps 11 --maker-taker-cost-bps 8
python -m recorder.compact --data-directory data/smoke --output-directory data/smoke-v2
```

Inspect recovery first; `--repair` only performs the safe cases above. Validation/maintenance
lock out active capture. No command submits orders. Fee numbers are hypothetical all-in
round-trip assumptions, not current Binance fees or promises of maker fills.

## Batch robustness research

Batch queries catalog sessions overlapping the requested timezone-aware range. It streams
Parquet batches via a heap merge by capture sequence, replays each session/symbol independently,
reconstructs raw books and recomputes all features. It retains at most one UTC hour plus 10s
future context (hard 100,000 features). Legacy sessions missing bootstrap/final-summary proof
are rejected, not silently promoted. Failed quality/equivalence checks abort the batch.

Every predefined feature/horizon is reported: all-row Pearson/Spearman versus sampling at
intervals at least H; ACF at 100/250/500/1000/2000/5000 ms; raw, nonoverlap and approximate
effective counts; deterministic time-block bootstrap for L5 high-imbalance mean and high-minus-low
mean (default 200 replications, seed 42, 10s blocks). ACF uses at-or-before lag matches with at
most 150ms tolerance. ESS is the minimum initial-positive ACF estimate for sampled x/y, not a
formal inferential guarantee. Irregular cadence and remaining dependence still limit inference.

Regime diagnostics use contemporaneous/past RV but **descriptive per-hour cutpoints**; these
are explicitly not formal held-out evaluation. For formal work use `volatility_boundaries`
only on TRAIN and reuse those fixed values on validation/test. Session correlations combine
centered hourly moments, retaining all per-hour results without concatenating full sessions.
Across-session median/min/max and same-sign fraction are reported separately for each symbol.

All four existing baselines are evaluated with unchanged default parameters, as conditional
signed future price movement only—no fills or trading simulation. Every horizon has an
edge/cost table, edge/cost ratio, break-even cost and net sensitivity at
`0, 0.5, 1, 2, 3, 5, 8, 10, 12` bps. No best threshold or best horizon is selected.
Cross-symbol comparison retains trade/depth rates, spread/depth/RV, unchanged ticks, price-update
frequency, lag and resync/gap rates. A short comparison cannot establish venue suitability.

Batch artifacts identify input manifests/checksums, session summary hashes, research source
fingerprint, configuration and frozen ID. Repeated runs produce identical JSON and feature
digests; analytical Parquet joins preserve source session/sequence identity. Prefer this command
for long datasets; the tuple-based programmatic API below is intended for small fixtures.

Programmatic replay starts from cataloged session and symbol selection:

```python
from pathlib import Path

from features.engine import FeatureEngine
from replay.clock import ReplayClock
from replay.engine import ReplayEngine
from replay.features import load_feature_ticks, merge_replay_streams
from replay.loader import ReplayDatasetLoader

root = Path("data")
raw = ReplayDatasetLoader(root).load(capture_session_id="...", symbol="BTCUSDT")
ticks = load_feature_ticks(root, capture_session_id="...", symbol="BTCUSDT")
events = merge_replay_streams(raw, ticks)
clock = ReplayClock()
engine = FeatureEngine("BTCUSDT", monotonic_clock_ns=clock.monotonic_ns, wall_clock=clock.wall_time)
result = ReplayEngine(events, clock, engine).run()
```

## Quality gates

```bash
pytest -q
ruff check .
ruff format --check .
mypy app config exchange execution features market recorder replay research signals strategies
git diff --check
```

Tests use fake transports and injected clocks unless explicitly performing a manual public-data
smoke capture. No test needs an API key.

## Limitations and next milestone

Short smoke samples cannot establish significance, stationarity, or profitability. REST depth
is finite, and byte/memory/CPU behavior is not yet soak-tested for days. Startup validates the
catalog linearly and catalog updates rewrite JSON; very large archives may eventually justify
an indexed catalog. Compression ratio and activity can change sharply by regime. Clock midpoint
estimation cannot separate route asymmetry from exchange timestamp semantics. Bootstrap intervals
are approximate, and one or two short sessions do not validate inference or venue suitability.
The legacy shadow maker model still needs explicit fills and does not model queue position.

The next milestone is to collect materially larger, clean BTCUSDT and BTCUSDC datasets across
multiple market regimes before changing thresholds or adding strategies.
