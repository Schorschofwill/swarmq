---
title: "Sunset reminder: CLI surface whitelist + _DLQNotFound rename (KP-2 / M-02 / M-03)"
date: 2026-05-26
sunset_date: 2026-12-31
# ``trigger_date`` mirrors ``sunset_date`` for compatibility with the
# plan-specified frontmatter field name; tooling may consume either.
trigger_date: 2026-12-31
category: patterns
module: cli
problem_type: deferred_decision
component: cli-surface
applies_when:
  - "Reviewing the frozen surface whitelist in tests/unit/test_cli_module_surface.py near or after 2026-12-31"
  - "A ce-review run flags KP-2 (rename _DLQNotFound -> _DLQNotFoundError) or M-02/M-03 (ambient re-exports) as open findings"
  - "Considering an API-break commit that removes underscore-prefixed CLI symbols from the public surface"
root_cause: api_compat_freeze
resolution_type: scheduled_reevaluation
severity: low
tags:
  - cli
  - surface-whitelist
  - api-compat
  - sunset
  - deferred-decision
  - kp-2
  - m-02
  - m-03
parent_plan: docs/plans/2026-04-23-001-fix-cli-refactor-residuals-plan.md
related:
  - docs/plans/2026-04-22-001-refactor-cli-split-plan.md
  - docs/plans/2026-05-26-001-fix-cli-residuals-dod-followups-plan.md
---

## Why this exists

The cli-residuals plan (`2026-04-23-001`) deliberately deferred three
findings that would be API-breaks for any external consumer who
patches the CLI surface:

- **KP-2** — rename `_DLQNotFound` → `_DLQNotFoundError`.
- **M-02 / M-03** — trim ambient re-exports (`Path`, `Sequence`, etc.)
  from `tests/unit/test_cli_module_surface.py::_CLI_PUBLIC_SURFACE`.

The underscore prefix signals "internal", but no inventory of external
consumers has been done. Decision E6 in the parent plan set a sunset
date of **2026-12-31** to bound the freeze: if no consumer surfaces
via Issue / Docs / Slack by then, the renames and trim move forward
in a separate plan.

## What to do on or after 2026-12-31

1. **Inventory consumer evidence.**
   - `gh search issues "_DLQNotFound OR ValkeyBackend OR RabbitMQBroker" --repo <repo>` — look for users patching these names through the CLI namespace.
   - `grep -rn "patch.*swarmq.cli" docs/ website/` — internal docs that prescribe the pattern.
   - Slack archive (last 6 months) for `_DLQNotFound`, `swarmq.cli.ValkeyBackend`.

2. **If no consumers:** open a new plan
   `<date>-fix-cli-surface-whitelist-trim-plan.md` that does the
   rename + whitelist trim in one go. Mark this file as `status:
   resolved` and link the resolving plan.

3. **If consumers exist:** push the sunset out by 12 months
   (`sunset_date: 2027-12-31`), document the consumer rationale here,
   and add an entry to `website/reference/changelog.md` warning that
   the freeze is conditional.

## Triggers that should surface this file early

- A ce-review run re-flagging KP-2, M-02, or M-03 — the reviewer should
  find this file via the `kp-2` / `m-02` / `m-03` tags and reply with
  "deferred until `sunset_date`".
- Any plan that touches `tests/unit/test_cli_module_surface.py::_CLI_PUBLIC_SURFACE`.
- A `ce-compound-refresh` run that scans `docs/solutions/patterns/`
  for `sunset_date` fields past `date.today()`.

## Why this isn't just an Issue

GitHub issues drift out of view. `docs/solutions/patterns/` is indexed
by `ce-learnings-researcher` and surfaces in plan/review workflows
that match by tag — the same workflows that would otherwise re-discover
KP-2 cold and re-debate it.
