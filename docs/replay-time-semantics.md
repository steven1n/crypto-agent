# Replay time semantics

Canonical event ordering is `capture_seq`, strictly increasing within the selected
session stream (per-symbol projections may have gaps in this global sequence).
Recorded monotonic nanoseconds must be nondecreasing; equal timestamps are legal
for different events. Negative monotonic deltas, duplicate/backward sequence IDs,
and unexpected session/symbol changes remain fail-closed.

Local wall time is a recorded civil-time observation. It must be timezone-aware,
but may move backward. Replay preserves it exactly: no clamping, sorting by wall
time, timestamp rewriting, source rewrite, or synthetic offset correction.
Exchange event and transaction timestamps are also preserved; they retain their
exchange-event/latency meaning, not local scheduling authority.

`ReplayClock` retains an O(1) summary: backward-step count, last step, and largest
observed backward step. Each includes the previous/current wall timestamp,
capture sequence, monotonic timestamp, observed backward difference, monotonic
elapsed time, and wall-minus-monotonic change. These are diagnostics, not proof
of the external clock-adjustment mechanism. They do not invalidate market data
or reset feature state by themselves. Batch session reports expose this summary.

Two magnitudes must not be confused: a recorded wall difference of -198.5 ms
while monotonic time advances 98.8 ms implies a wall-minus-monotonic change near
-297.3 ms. The requested synthetic example (-297 ms wall difference with +100 ms
monotonic progression) instead implies a -397 ms wall-minus-monotonic change.

## Usage audit

| Scope | Existing clock semantics / action |
| --- | --- |
| Replay iteration and scheduling | Capture sequence and recorded monotonic time. FAST/STEP do not schedule from wall timestamps. Preserve integrity guards; pass sequence to clock diagnostics. |
| Mid-price history and returns | `features/history.py` uses monotonic observation times for retention and historical lookup. No change. |
| Realized volatility | Monotonic observation-window selection. No change. |
| Trade-flow windows | `features/trade_flow.py` expires by receive monotonic time; exact Decimal aggregates unchanged. No change. |
| Feature cadence and staleness | `features/engine.py` deadlines, market-activity age, book age and stale thresholds use monotonic time. No change. |
| Future labels | `research/labels.py` bisects strictly increasing monotonic times, rejects invalid ticks, gaps over 3 seconds, and supplied lifecycle boundaries. No change. |
| Lifecycle and rolling state | Disconnect clears price and trade histories; book invalidation/resync clears price continuity. Book-only resync does not erase the independent trade stream. Wall rollback adds no new lifecycle event. No change. |
| Train/validation/test | `research/splits.py` orders/purges on monotonic timestamps. No change. |
| Dependence diagnostics and bootstrap | Monotonic feature times supplied to sampling, ACF and time blocks. No change. |
| Latency | Wall-minus-exchange timestamps are intentionally used for lag; interarrival and heartbeat proximity use monotonic time. Neither lag nor source timestamp is clamped by this fix. |
| Civil UTC reporting | Batch hour labels and from/to filters use recorded UTC; no change to research controls. See limitation below. |

## Civil-hour batching limitation

Historical finding below: the separate UTC-hour retention correction is now
documented in `utc-hour-retention.md`. ReplayClock semantics were not changed by
that correction; the original wall-clock acceptance evidence remains immutable.

The existing bounded batch buffer flushes a UTC hour after its civil 10-second
lookahead, and retains rows whose UTC hour is later. A synthetic predicate audit
demonstrates that even a 297 ms rollback across an hour boundary can leave a row
neither selected nor retained when the buffer begins in the later hour. Revisiting
an already flushed hour can also require revised reporting partitions. The audit
is saved in the campaign's `civil-hour-audit.json`. This is a separate reporting
time-semantics limitation, not permission to reorder events or reset monotonic
feature history. The known 09:11:15 UTC rollback is wholly inside one hour. Its
targeted and full feature replay are therefore independently testable without
changing hourly reporting or any strategy parameters. A separate post-batch audit
checks that every original feature has a derived row and no emitted label crosses
an invalid/lifecycle boundary. Full acceptance evidence
must distinguish feature equivalence from unrestricted civil-hour robustness.

## Frozen acceptance

Capture source fingerprint and repaired replay source fingerprint are different
by design and must both be preserved. The campaign acceptance harness records the
three authorized core-file hashes, checks unchanged fixed research configuration,
verifies the existing frozen files, performs prefix-warmed targeted replay before
full feature replay, and only then starts the fixed-control batch. It never starts
a capture. Full reconstruction and batch results must be reported as pending until
that process actually succeeds.
