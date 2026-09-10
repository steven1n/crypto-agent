# Singapore EC2 6h resource soak — IN PROGRESS

This is a launch record, not resource acceptance. No PASS/WARNING/FAIL is issued yet.

- Session: d112a383865e46b0926bb6ad6288a769.
- Observer start UTC: 2026-09-10T08:52:39.865847+00:00 (16:52:39 Singapore).
- Expected capture end: approximately 2026-09-10 14:52:40 UTC / 22:52:40 Singapore, followed by graceful finalization and integrity verification.
- Instance: i-0b357fe2738d4a496, ap-southeast-1b, t3.xlarge; IMDSv2 verified.
- Source: 2822085ea27f48d346d3f61f4dafd6a0a1c70c5620400fa281a943de3efadf93.
- Controls: cc225b299a296c97d626bda39f7ee9045638ee298084f44727e3abbb34bfbb2d.
- Configuration: 3a4bc2af4be409cb95b60f5caafd3015a91073d9baae044498e6b9276a214ba8.
- Capture observer SHA256: d51d42182a8d4edae6536d865ae34f89e54456ee3aed4bba5ea4b024aea56fc5.
- 28 retained wheels passed SHA256 verification. Separate throwaway venv rebuilt offline from exact pins and retained wheels; pip check/import passed. Accepted production venv unchanged.
- Source/config/environment preflight enforced. Only output directory and duration changed from accepted capture settings. Additional /proc PSI/OOM observation is external, not hot-path instrumentation.
- At first 60s health both books SYNCED, both emitted 598 features, zero gaps, queues NORMAL.

Remote artifacts:

- /var/lib/crypto-capture/work/ec2-singapore-resource-soak-6h-20260910/summary.json
- same directory resources.jsonl, capture.log, preflight.json, host-provenance.json, frozen-configuration.json, dataset/
- /opt/crypto-deployment/soak6h-20260910/runner.log
- after capture: verification.log and generated resource report in campaign directory

One detached remote process runs 21600s capture → raw validation for both symbols → retain_results=False exact feature replay → resource gate report. No automatic restart or merge. Invalid raw integrity stops before feature acceptance. An external 8 GB RSS machine-safety stop protects the host; this is not the 3 GB PASS criterion and does not change capture settings. Existing production disk/queue/writer fail-closed behavior remains unchanged.

Local tools under data/ec2-soak-tools-20260910 are campaign-only. Python compilation, local project-scope Ruff and five synthetic gate checks passed. Remote default/tool-directory lint uses different global rules/import-root classification and reported style issues; no operational defect was reproduced, no active tool/source files were reformatted after launch. Do not misreport this as a new full repository quality-gate pass.

Resource acceptance must use user thresholds: final3h RSS slope <=15 MB/h, final2h <=20 MB/h, comfortably below3GB, no late acceleration/OOM/pressure/writer/disk/correctness failures, bounded FD/threads and low queues. Missing or ambiguous evidence yields WARNING, not forced PASS. Host PSI is not a causal diagnosis. Raw telemetry is retained for review. Mac remains RESOURCE_GATE_WARNING independently.

Heartbeat ec2-6h-resource-soak-closeout follows this thread hourly, does not duplicate computation, and must delete itself after reviewed report delivery. No new6h/24h, Stage B, freeze, compaction, resize, service enablement, optimization or trading is authorized by this follow-up. The final reviewed report will replace this launch record; do not overwrite historical Mac or 15-minute qualification reports.
