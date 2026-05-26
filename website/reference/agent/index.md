# Agent reference

This section is written for AI agents, coding assistants, and
automation scripts that need to use SwarmQ **without reading the
source code**. Every page in this bucket holds to three rules:

1. **Shape before prose.** Tables, enums, and JSON fragments come
   first; natural language is supporting context.
2. **Canonical, not illustrative.** Every field, type, and enum
   value is the exact one SwarmQ emits or accepts — no
   "for example" omissions.
3. **Stable IDs and anchors.** Section slugs do not change without
   a semver-bumped entry in the [changelog](../changelog.md). An
   agent can deep-link to `#task-enqueued` and expect it to
   resolve.

## What lives here

- [Message formats](message-formats.md) — the wire schemas for
  task messages, results, progress updates, scheduler entries,
  DLQ envelopes, and cancel flags.
- [Decision trees](decision-trees.md) — Mermaid flowcharts for
  the non-obvious choices: which queue, which retry mode, when
  a task is expired, when a lock releases, when a unique flag
  releases.
- [Error catalog (machine-readable)](error-catalog-machine.md) —
  the same exceptions as the [human error catalog](../errors.md),
  but in a strict tabular shape with stable structured keys
  (`exception_class`, `exit_code_on_cli`, `retriable`,
  `dlq_on_uncaught`).

## How to use this section

Agents that need SwarmQ behaviour should:

1. Read [message formats](message-formats.md) once per session to
   ground the payload shape of every signal and stored artefact.
2. Pull the relevant [decision tree](decision-trees.md) when
   choosing among features (retry config, unique vs lock, when to
   emit `Retry` vs `NoRetry`).
3. Cross-reference the
   [error catalog](error-catalog-machine.md) when interpreting
   raised exceptions or CLI exit codes.

For ground-truth Python signatures, the
[API reference](../api.md) remains canonical — this section
paraphrases nothing it cannot back with a wire format.

## How to consume the docs programmatically

The site exposes two generated files at the domain root:

- `/llms.txt` — single-file TOC of every page, Diátaxis-grouped,
  with relative links. Sections match the navigation groups:
  Introduction, Tutorials, How-to guides, Reference, Explanation.
  The Agent reference pages appear under Reference.
- `/llms-full.txt` — the entire site concatenated into one
  newline-separated Markdown document. Large; suitable for a
  one-shot grounding pass.

Both files are regenerated on every docs build.

## Versioning

The library follows [Semantic Versioning](https://semver.org/).
For agents:

- **Patch versions (0.x.Y).** No wire-format changes. Existing
  integrations continue to work.
- **Minor versions (0.X.y).** Additive wire-format changes
  (new optional headers, new signals, new enum values). Old
  clients keep working; new fields should be read with
  `get()`-style defaults to stay forward-compatible.
- **Major versions (X.y.z).** Breaking changes are allowed. Each
  release enumerates them in the [changelog](../changelog.md)
  under a `### Changed` or `### Removed` section.

Pre-`1.0.0`, minor releases may contain breaking changes — SwarmQ
is currently in that pre-release window. The changelog is the
authoritative source.

## See also

- [`/llms.txt`](/llms.txt) — TOC for agents
- [`/llms-full.txt`](/llms-full.txt) — full-text dump for grounding
- [API reference](../api.md) — Python signatures
- [Error catalog (human)](../errors.md) — prose-first version of
  the error table
