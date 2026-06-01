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

Two machine-readable surfaces ship at the site root and under
`reference/agent/spec/`:

- `/llms.txt` — an [llmstxt.org](https://llmstxt.org/) index: a
  one-line summary, links to the key documentation pages, and links
  to every generated spec file below.
- **Generated spec (JSON)** — structured, source-of-truth data an
  agent can parse without reading prose or source:
    - [`spec/index.json`](spec/index.json) — version + manifest.
    - [`spec/errors.json`](spec/errors.json) — every `SwarmQError`
      subclass with `retriable` / `dlq_on_uncaught` / exit code
      (the strict form of [the error catalog](error-catalog-machine.md)).
    - [`spec/config.json`](spec/config.json) — every config option
      with type, default, and environment variable.
    - [`spec/cli.json`](spec/cli.json) — every CLI command, flag,
      and exit code.
    - [`spec/messages/result.schema.json`](spec/messages/result.schema.json)
      and its siblings — JSON Schema (Draft 2020-12) per wire format.

The spec JSON is generated from the code by `python -m swarmq.agent_spec`
and pinned to it by tests, so it cannot silently drift from reality
between releases. Regenerate it for any installed version with that
same command.

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

- [`/llms.txt`](/llms.txt) — agent index (docs + generated spec)
- [Generated spec index](spec/index.json) — machine-readable JSON
- [API reference](../api.md) — Python signatures
- [Error catalog (human)](../errors.md) — prose-first version of
  the error table
