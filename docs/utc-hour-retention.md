# UTC-hour rollback retention

ReplayClock is closed and unchanged. This correction is confined to
`research/batch.py`, its deterministic tests, and offline regression evidence.

## Root cause

Previously, `flush()` chose the UTC hour of `buffer[0].created_at`, selected every
eligible row in that hour, and retained only rows whose civil hour was **greater**
than that hour. For 09:00:00.100 followed by 08:59:59.803, the second row satisfied
neither condition. The algorithm incorrectly assumed civil hours were globally
monotonic in capture order.

Changing `>` to `!=` alone is insufficient: selecting noncontiguous rows can
remove a future observation needed to label an earlier pending row, and writing
the same hour a second time can overwrite the first hour's output filename.

## Small source-ordered correction

`HourBuffer` owns the existing bounded feature buffer and row counters; there is
no new service, database or replay architecture.

1. Append once in existing source order. Feature monotonic timestamps must be
   strictly increasing. The replay sequence checks are unchanged.
2. The pending partition is the **contiguous prefix** sharing the first row's UTC
   hour. A later recurrence of that hour belongs to a separate fragment.
3. Keep all subsequent rows as future-label context, in their original order.
   Flush only after recorded monotonic time reaches the last prefix row plus the
   existing maximum label horizon (10 seconds), or at session end. This replaces
   the incorrect civil-time lookahead assumption, not any research horizon.
4. `hour_analysis_rows()` labels from the complete context before selecting the
   prefix. No selected row is retired while an earlier source row still needs it
   as future context.
5. Retire only that prefix. Every other row remains in the buffer exactly once,
   regardless of its wall-clock hour. Drain successive ready prefixes, or all
   remaining prefixes at end of input.
6. Filenames include session, symbol, UTC hour and the fragment's first eligible
   capture sequence. Recurring hours cannot overwrite an earlier fragment.

Reporting preserves the recorded UTC hour. A repeated hour has multiple explicitly
identified source-order fragments, not multiple independent capture sessions.
Per-fragment diagnostics are not represented as one complete pooled civil-hour
statistic; `partition` metadata makes that distinction explicit. Existing session
moment aggregation includes all fragments. Within the unaffected frozen dataset,
fragment membership still matches the prior hourly partitions exactly.

Recorded wall times are neither changed nor used to sort input. Exchange clocks,
FeatureEngine state, strategies, costs, bootstrap parameters and horizons are
unchanged. The existing memory limit remains fail-closed; arbitrary clock steps
do not justify unbounded buffering or silently evicting records.

## Accounting and exclusions

Before publishing a fragment, output `(monotonic_ns, created_at)` identities must
match the expected eligible prefix exactly and in order. Missing, duplicate,
reordered or wall-rewritten rows raise `CaptureSafetyError`. Strict input identity
progression plus prefix consumption prevents cross-fragment duplicates without
an unbounded seen-row cache.

At session completion require:

```
source_rows = emitted_analysis_rows + intentionally_excluded_rows
eligible_source_rows = source_rows - intentionally_excluded_rows
eligible_source_rows = emitted_analysis_rows
pending_rows = 0
```

The only intentional exclusions are `created_at < --from` and
`created_at >= --to`. These categories are disjoint for the CLI's valid start/end
range. Invalid features are still emitted and counted; missing future labels do
not exclude their source rows. With no range filters, all source rows are eligible.

Civil filtering can become noncontiguous after rollback. Excluded observations
stay in label context and additionally mark a label boundary. Original invalid
ticks, gaps and lifecycle boundaries remain visible. Thus removing an output row
by UTC range cannot permit labels to bridge the excluded/invalid interval. At
session end, unavailable future labels remain null rather than inventing data.

## Verification scope

Synthetic tests cover normal hour progression, within-hour and cross-hour rollback,
repeated hours, multiple rolled-back rows, forward transitions, pre-EOF flushing,
separate output files, exact identity accounting, bounded capacity, invalid ticks,
lifecycle boundaries and civil-range exclusions.

The frozen regression is feature-only: stream recorded FeatureSnapshots into the
same buffer and `hour_analysis_rows()` used by production, regenerate labels with
recorded boundaries, and compare every output field with the already accepted
analysis Parquet rows. At most the configured buffer and one reference hour are
held in memory. No order-book reconstruction, FeatureEngine replay, strategy
execution, bootstrap or profitability interpretation is repeated.

The original frozen and accepted batch artifacts are not rewritten. A future full
batch under the repaired source has a different source-derived fingerprint and
sequence-suffixed fragment paths/accounting metadata. This bounded regression is
not misrepresented as a newly completed full statistical batch.
