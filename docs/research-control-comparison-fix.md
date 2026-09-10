# Research-control comparison fix

## Outcome

The comparison fix is implemented and repository acceptance is now complete. The initial gate results below are preserved as the earlier attempt; the explicitly authorized blocker closeout at the end supersedes that attempt. All 21 focused regressions and all 239 repository tests pass.

## Original cause and exact path

`data/clean-stage-b-tools-20260909/campaign.py::run`, after the existing batch exits, compared `batch["configuration"]` directly with the in-memory `config["research_controls"]["configuration"]`. The latter contains a tuple cost grid; JSON restores it as a list. Python dictionary equality therefore raised `research controls changed` despite equal serialized values.

The complete compared object contains: `block_length_ms`, `bootstrap_replications`, `cost_grid_bps`, `maker_taker_cost_bps`, `max_hour_features`, `seed`, and `taker_taker_cost_bps`. Feature lists, horizons and baseline definitions are separate frozen control fields; this fix does not broaden the original guard or change those definitions. Existing source/control capture checks remain in place. Tests additionally prove that the comparator rejects threshold/horizon changes when such fields participate in the compared object.

## Minimal fix and canonical semantics

The guard now calls the wrapper-local `research_controls_equal` on the same two complete objects. Each operand is serialized/deserialized with standard JSON and `allow_nan=False`; then recursive value equality preserves all dictionary keys and array positions. Boolean values are compared as booleans, never as numbers.

- Tuples become JSON arrays recursively, including nested tuples and values already converted with `dataclasses.asdict`.
- Dictionary insertion order is irrelevant; missing keys and explicit null remain different.
- Array order is unchanged; no sorting, numeric-string coercion, rounding or tolerance.
- Equal JSON numbers such as `8`/`8.0` and `11`/`11.0` compare equal. Actual differences, including adjacent representable floats and large integers, remain unequal.
- Strings, booleans and null preserve their JSON types. Unsupported/non-finite values raise rather than being stringified or accepted fuzzily.
- Any unequal valid control object still triggers the original `RuntimeError("research controls changed")`. No warning-only fallback was introduced.

The existing `recorder.catalog.stable_fingerprint` was inspected and **left unchanged**. It normalizes tuple/list serialization, but hashes `8` and `8.0` differently. The real saved campaign/batch also contains these equal integer/float fee-default representations. Comparing those fingerprints would retain a false failure. Its `default=str` behavior also permits non-JSON objects to become strings. Therefore a strict JSON-value comparator is appropriate here; no existing fingerprint definition or artifact identity changed.

## Regression coverage

`tests/test_research_control_comparison.py` adds 21 tests: tuple/list, nested tuple/list, dictionary order, numeric change, array order, missing key, strategy threshold, horizon, bootstrap repetitions, seed, cost-grid value, string/number, no float rounding, explicit null/missing, boolean/number, dataclass-asdict values, exact September 9 complete object shape (including equal integer fee defaults), three non-finite cases, and large-integer difference preservation.

Tests compile only the actual wrapper comparison helper, `require` definition and real closeout guard from its AST. They do not import the operational wrapper or launch any campaign. The historical-shape fixture is embedded and does not depend on mutable historical data. Equivalent inputs are checked for non-mutation.

## Quality gates — exact final results

| Check | Result |
|---|---|
| Focused regression tests | **21 passed in 0.21s** |
| `pytest -q` | **1 failed, 238 passed in 9.72s**, exit 1 |
| `ruff check .` | **All checks passed**, exit 0 |
| `ruff format --check .` | **1 file would be reformatted, 107 files already formatted**, exit 1 |
| `mypy --strict app config exchange execution features market recorder replay research signals strategies` | **Success: no issues found in 68 source files**, exit 0 |
| `mypy --strict tests/test_research_control_comparison.py` | **Success: no issues found in 1 source file**, exit 0 |
| `git diff --check` | pass, exit 0 |

The existing failing test is `tests/test_campaign_observer.py::test_observer_fake_capture_exits_without_market_data`: its normal OS sampling invokes `ps`, which this sandbox refuses with `PermissionError: [Errno 1] Operation not permitted`. No permission bypass or test skip was used.

The formatting failure is the pre-existing code block around line 243 of `docs/vps-deployment-readiness.md`. This unrelated document was not reformatted. The campaign wrapper lives under ignored `data/`; ordinary repository lint/type-check discovery does not cover that entire existing operational script. Its actual changed guard/helper are exercised directly by the new tests. `git diff --check` alone is not a content audit of untracked/ignored files.

## Historical evidence and scope

SHA256 was compared before/after for 36 protected files, including the original summary/configuration, traceback log, frozen manifest, original accepted Stage B reports, batch and segment JSON. **Zero changes.** Original `STOPPED_INCOMPLETE` and its recorded error remain untouched. The wrapper file itself is intentionally updated for future use; its old hash in historical configuration is not rewritten.

Only the wrapper comparison, its focused test file and this new report were changed. No research parameter, fingerprint algorithm or core runtime module was changed. No batch, replay, reconstruction, label/bootstrap generation, capture, optimization or trading was run outside the explicitly requested deterministic unit tests.

Follow-up required: rerun the full suite in an environment that permits its existing OS-sampling test, and resolve the unrelated formatting gate with appropriate scope. Do not rerun the historical six-hour batch.

## Acceptance blocker closeout — 2026-09-10

The comparator and its focused tests were not changed. Focused tests ran first: 21 passed in 0.17s, exit 0, confirming normalized container equality and fail-closed rejection of actual changes.

The fake-capture observer test is a lifecycle test: a real short-lived fake child must exit successfully, produce its log, preserve source identity and leave CAPTURE_ENDED_PENDING_VALIDATION with returncode 0. Only its process_sample boundary is monkeypatched to deterministic RSS/CPU values. Resource aggregation, subprocess lifecycle and every original assertion remain real/unchanged. There is no skip and no production exception handling or monitoring change. Existing test_ps_units retains representative ps-output parsing coverage, including whitespace, CPU and KiB-to-byte conversion. Production process_sample still executes real ps unchanged; this task did not execute an OS integration probe.

The VPS document was formatted by the configured Ruff formatter only. Its embedded Python changed quotes, whitespace and wrapping, not commands, values or semantics. The exact formatter diff follows below.

### Final quality gates

| Check | Exact result | Exit code |
|---|---|---|
| Focused comparison tests after cleanup | 21 passed in 0.20s | 0 |
| pytest -q | 239 passed in 9.77s | 0 |
| ruff check . | All checks passed! | 0 |
| ruff format --check . | 109 files already formatted | 0 |
| mypy --strict app config exchange execution features market recorder replay research signals strategies | Success: no issues found in 68 source files | 0 |
| git diff --check | No output / passed | 0 |
| Protected historical SHA256 comparison | 36 checked, zero changes | 0 |

The same 36 historical hashes from the previous acceptance were rechecked before and after cleanup, including original STOPPED_INCOMPLETE summary, traceback, frozen manifest, configurations, accepted Stage B reports and batch/segment outputs. None changed. The wrapper and comparison-test hashes also match their pre-cleanup values. Only tests/test_campaign_observer.py, docs/vps-deployment-readiness.md and this fix acceptance report were changed in this cleanup. No historical workload, capture, batch, reconstruction, bootstrap or research computation was launched; only requested deterministic unit tests ran.

### Exact VPS formatting diff

```diff
--- docs/vps-deployment-readiness.md
+++ docs/vps-deployment-readiness.md
@@ -240,36 +240,51 @@
 from replay.offset import RecordedClockOffset
 from replay.streaming import iter_session
 
-root = Path('/var/lib/crypto-capture/smoke-001/dataset')
+root = Path("/var/lib/crypto-capture/smoke-001/dataset")
 entries = select_manifests(root)
 assert entries
 for session, symbol in sorted({(e.capture_session_id, e.symbol) for e in entries}):
-    summary = json.loads((root/'sessions'/session/'session-summary.json').read_text())
-    assert summary['status'] == 'FINALIZED' and summary['dataset_valid']
-    cfg = summary['configuration']
-    selected = tuple(e for e in entries if (e.capture_session_id,e.symbol)==(session,symbol))
+    summary = json.loads((root / "sessions" / session / "session-summary.json").read_text())
+    assert summary["status"] == "FINALIZED" and summary["dataset_valid"]
+    cfg = summary["configuration"]
+    selected = tuple(e for e in entries if (e.capture_session_id, e.symbol) == (session, symbol))
     clock = ReplayClock()
-    offsets = RecordedClockOffset(cfg['clock_sync_sample_count'])
-    engine = FeatureEngine(symbol, interval_ms=cfg['feature_interval_ms'],
-        book_stale_after_ms=cfg['orderbook_stale_after_ms'],
-        trade_stale_after_ms=cfg['trade_stale_after_ms'],
-        mid_history_seconds=cfg['mid_history_seconds'],
-        trade_history_seconds=cfg['trade_history_seconds'],
-        imbalance_levels=tuple(cfg['feature_imbalance_levels']), clock_offset=offsets,
-        monotonic_clock_ns=clock.monotonic_ns, wall_clock=clock.wall_time)
-    counts = {'compared':0, 'tolerance_mismatch_rows':0, 'exact_difference_rows':0}
+    offsets = RecordedClockOffset(cfg["clock_sync_sample_count"])
+    engine = FeatureEngine(
+        symbol,
+        interval_ms=cfg["feature_interval_ms"],
+        book_stale_after_ms=cfg["orderbook_stale_after_ms"],
+        trade_stale_after_ms=cfg["trade_stale_after_ms"],
+        mid_history_seconds=cfg["mid_history_seconds"],
+        trade_history_seconds=cfg["trade_history_seconds"],
+        imbalance_levels=tuple(cfg["feature_imbalance_levels"]),
+        clock_offset=offsets,
+        monotonic_clock_ns=clock.monotonic_ns,
+        wall_clock=clock.wall_time,
+    )
+    counts = {"compared": 0, "tolerance_mismatch_rows": 0, "exact_difference_rows": 0}
+
     def compare(actual, recorded):
         assert recorded is not None
-        counts['compared'] += 1
-        counts['tolerance_mismatch_rows'] += bool(feature_equivalence_errors(actual,recorded))
-        counts['exact_difference_rows'] += bool(feature_equivalence_errors(actual,recorded,absolute_tolerance=0))
-    result = ReplayEngine(iter_session(root,selected),clock,engine,
-        retain_results=False,feature_handler=compare,clock_offset=offsets).run()
-    expected = sum(e.row_count for e in selected if e.event_type=='feature')
-    print(json.dumps({'session':session,'symbol':symbol,'source_rows':expected,**counts}))
-    assert result.data_quality.valid and expected == counts['compared'] > 0
-    assert counts['tolerance_mismatch_rows'] == 0
-    assert counts['exact_difference_rows'] == 0
+        counts["compared"] += 1
+        counts["tolerance_mismatch_rows"] += bool(feature_equivalence_errors(actual, recorded))
+        counts["exact_difference_rows"] += bool(
+            feature_equivalence_errors(actual, recorded, absolute_tolerance=0)
+        )
+
+    result = ReplayEngine(
+        iter_session(root, selected),
+        clock,
+        engine,
+        retain_results=False,
+        feature_handler=compare,
+        clock_offset=offsets,
+    ).run()
+    expected = sum(e.row_count for e in selected if e.event_type == "feature")
+    print(json.dumps({"session": session, "symbol": symbol, "source_rows": expected, **counts}))
+    assert result.data_quality.valid and expected == counts["compared"] > 0
+    assert counts["tolerance_mismatch_rows"] == 0
+    assert counts["exact_difference_rows"] == 0
 ```
 
 严格全字段exact差异为0足以证明Decimal及float差异均0。若只有float精度差异，默认文档容差1e-12仍适用，但本保守资格脚本会停下供人工列出Decimal/float字段和最大误差，不能自动忽略。检查两symbol均有输出。不要对多日数据误用这段为无限时测试；这里仅900秒session。依赖/硬件变化时重新验收。
```

RESEARCH_CONTROL_COMPARISON_FIXED — FUTURE CLOSEOUT SAFE
