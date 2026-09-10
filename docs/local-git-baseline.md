# Local Git baseline

The initial local baseline records source, tests, documentation, deployment configuration
and selected operational scripts without changing runtime semantics. No remote or push
is configured by this work. Git identity uses the existing local configuration.

Do not commit private keys, .env files, virtual environments, generated market datasets,
Parquet files or generated qualification summaries. .env.example remains versioned.
The baseline is a source snapshot, not a backup of research datasets; retain those and
their frozen manifests separately.

The data/ directory remains ignored to preserve existing tooling discovery behavior.
The following source files are deliberately force-added, not the containing datasets:

- data/clean-stage-b-tools-20260909/campaign.py: the comparison regression tests read its
  real guard using AST; it is not a portable live-capture launcher.
- data/ec2-soak-tools-20260910/*.py and capture-settings.json: exact Linux soak tooling
  and non-secret fixed configuration. These are operational source, not market data.

Already tracked scripts continue to appear in git diff/status despite data/ being ignored.
New scripts under that directory require explicit review and targeted git add -f.
Never force-add data/ wholesale. A clone does not contain historical datasets referenced
by campaign-specific tools; use the core CLI and separately supplied data instead.

Useful checks before a local commit:

```sh
git status --short
git diff
git diff --cached --stat
git diff --cached --check
```

Live campaign source/config fingerprints remain authoritative. A local Git commit does
not authorize deploying changed files, changing an active campaign, starting capture,
or publishing private project content. Generated in-progress reports may legitimately
change after campaign closeout; commit their finalized versions separately.
