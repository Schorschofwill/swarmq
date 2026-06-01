---
title: "uv.lock drifts silently from pyproject optional extras; UV_FROZEN CI won't catch it"
date: 2026-06-01
problem_type: tooling_decision
component: tooling
module: "build / dependency-management"
severity: low
applies_when: "Adding or changing dependencies in pyproject.toml — especially optional extras — in a uv-managed project whose CI runs with UV_FROZEN=1"
related_components:
  - "tooling"
  - "testing_framework"
tags:
  - "uv"
  - "uv-lock"
  - "lockfile"
  - "uv-frozen"
  - "pyproject"
  - "optional-extras"
  - "ci"
  - "dependency-management"
  - "dishka"
---

# uv.lock drifts silently from pyproject optional extras; UV_FROZEN CI won't catch it

## Context

The `dishka` DI feature added an optional extra to `pyproject.toml`
(`dishka = ["dishka>=1.10.1"]`, and the `test` extra was made to depend on
dishka) but `uv.lock` was never regenerated in that change. So `uv.lock` was
out of sync with `pyproject.toml` — it contained no `dishka` entry at all.

This sat undetected for weeks across many green pipelines. It only surfaced when
an unrelated command implicitly ran `uv lock`, which produced a one-line working-
tree change (`Added dishka v1.10.1`) seemingly out of nowhere. The natural
assumption — "CI uses `UV_FROZEN=1`, so the lock must match pyproject" — is
wrong, and understanding *why* is the durable lesson.

## Guidance

**Regenerate and commit `uv.lock` in the same change whenever you touch
`pyproject.toml` dependencies — including optional extras that no CI job
installs.** Do not rely on a `UV_FROZEN`/frozen-install CI gate to catch lock
drift: frozen mode only fails when an install *resolves against* the missing
piece. An optional extra that no job installs is never resolved, so its absence
from the lock is invisible to CI.

Verify locally before committing:

```bash
uv lock            # regenerate; with no pyproject change this is a no-op
uv lock --check    # exit 0 ⇒ lock is consistent with pyproject (CI-equivalent check)
git diff uv.lock   # confirm the diff is only what you intended
```

For projects that want this enforced rather than remembered, add a CI step that
runs `uv lock --check` (it validates the *whole* pyproject↔lock consistency
regardless of which extras a job installs). **Critical gotcha: that check is a
no-op under `UV_FROZEN=1`** — with frozen set, `uv lock --check` validates only
lock *validity*, not up-to-dateness, prints a warning, and exits 0 even when the
lock is stale. The CI guard must run with `UV_FROZEN=0` (or `--no-frozen`) or it
silently catches nothing — the very trap that hid this drift. This repo now has
a blocking `lock-check` job (`.gitlab-ci.yml`) that overrides `UV_FROZEN: "0"`
and needs no dependency install (`before_script: []`).

## Why This Matters

- **Silent drift compounds.** A lock missing one extra today becomes a lock
  missing several after the next few merges, and the eventual regeneration
  produces a noisy, hard-to-review diff entangled with whatever change happened
  to trigger it.
- **`UV_FROZEN` is a weaker guarantee than it looks.** It guarantees "don't
  re-resolve during this install," not "the lock matches pyproject." The blocking
  jobs here install `--extra test --extra reload`; `--extra dishka` is installed
  by no blocking job, so frozen mode had nothing to complain about. The gap is
  structural, not a one-off.
- **Reproducibility.** A consumer who runs `uv sync --extra dishka` off a lock
  that lacks dishka gets a re-resolve (or, in frozen mode, an error) — exactly
  the non-reproducible behavior the lockfile exists to prevent.

## When to Apply

- Any edit to `[project.dependencies]` or `[project.optional-dependencies]` in
  `pyproject.toml`.
- Reviewing a dependency/feature PR: if `pyproject.toml` changed and `uv.lock`
  did not, treat it as a red flag and ask for a regenerated lock.
- Onboarding a uv + `UV_FROZEN` CI setup: do not assume the frozen gate covers
  extra/lock consistency — add `uv lock --check` if you want that covered.

## Examples

Before (the drift, as it sat on `main`):

```toml
# pyproject.toml
[project.optional-dependencies]
dishka = ["dishka>=1.10.1"]
```
```text
# uv.lock  →  grep 'name = "dishka"'  ⇒  0 matches  (extra declared, never locked)
```

After (`uv lock`, sole change):

```text
$ uv lock
Resolved 89 packages in 480ms
Added dishka v1.10.1
$ uv lock --check
Resolved 89 packages in 1ms          # exit 0 — consistent
$ grep -c 'name = "dishka"' uv.lock
5
```

The fix commit touched only `uv.lock` (+16/-1) — proof the lock was the single
out-of-sync artifact, not a broader dependency change.

## Related

- [../architecture-patterns/dishka-di-integration-async-task-queue.md](../architecture-patterns/dishka-di-integration-async-task-queue.md)
  — introduced the `swarmq[dishka]` optional extra (pinned `>=1.10.1`); this is
  the extra whose package was missing from `uv.lock`.
