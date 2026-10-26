# Agent reference

This section is for AI agents, coding assistants and automation scripts
that need to use SwarmQ **without reading the source code**. Every page
here follows three rules:

1. **Shape before prose.** Tables, enums and JSON fragments come first;
   prose only supports them.
2. **Canonical, not illustrative.** Every field, type and enum value is
   the exact one SwarmQ emits or accepts, with nothing left out.
3. **Stable anchors.** Section slugs change only with an entry in the
   [changelog](../changelog.md). An agent can deep-link to
   [`message-formats.md#task_enqueued`](message-formats.md#task_enqueued)
   and expect it to resolve.

The pages describe SwarmQ 0.14.4.

## What lives here

- [Message formats](message-formats.md): the wire schemas of task
  messages and their AMQP headers, results, progress updates, scheduler
  entries, DLQ envelopes, cancel and dedup flags, signal payloads and
  the `swarmq worker --check` payload.
- [Decision trees](decision-trees.md): Mermaid flowcharts for the
  non-obvious choices, such as which queue, which retry setup, unique
  task or lock, and when a message goes to the DLQ.
- [Error catalog (machine-readable)](error-catalog-machine.md): the
  exceptions of the [human error catalog](../errors.md) in a strict
  table with stable keys (`exception_class`, `exit_code_on_cli`,
  `retriable`, `dlq_on_uncaught`), plus the CLI exit codes.

## How to use this section

1. Read the [message formats](message-formats.md) once per session to
   ground the shape of every signal and stored artefact.
2. Pull the matching [decision tree](decision-trees.md) when choosing
   between features (retry setup, unique task or lock, `Retry` or
   `NoRetry`).
3. Look up raised exceptions and CLI exit codes in the
   [error catalog](error-catalog-machine.md).

For Python signatures the [API reference](../api.md) stays canonical;
this section states nothing it cannot back with a wire format.

## Machine-readable files

Two machine-readable surfaces ship with the site:

- [`llms.txt`](../../llms.txt) at the site root: an
  [llmstxt.org](https://llmstxt.org/) index with a one-line summary,
  links to the key documentation pages and links to every generated
  spec file below.
- **Generated spec (JSON)** under `reference/agent/spec/`, parseable
  without reading prose or source:
    - [`spec/index.json`](spec/index.json): version and manifest.
    - [`spec/errors.json`](spec/errors.json): every `SwarmQError`
      subclass with `retriable`, `dlq_on_uncaught` and exit code (the
      strict form of [the error catalog](error-catalog-machine.md)).
    - [`spec/config.json`](spec/config.json): every configuration
      option with type, default and environment variable.
    - [`spec/cli.json`](spec/cli.json): every CLI command, flag and
      exit code.
    - [`spec/messages/`](spec/messages/task-message.schema.json): one
      JSON Schema (Draft 2020-12) per wire format:
      `task-message.schema.json`, `result.schema.json`,
      `progress.schema.json` and `schedule-definition.schema.json`.

`python -m swarmq.agent_spec` generates the spec from the code, and a
test pins the checked-in files to a fresh generation, so they cannot
drift from the code between releases. The same command regenerates the
spec for any installed version.

## Versioning

SwarmQ follows [Semantic Versioning](https://semver.org/). For agents:

- **Patch versions (0.x.Y)** change no wire format. Existing
  integrations keep working.
- **Minor versions (0.X.y)** add to the wire format (new optional
  headers, signals, enum values or fields). Old clients keep working;
  read new fields with `get()`-style defaults to stay
  forward-compatible.
- **Major versions (X.y.z)** may break compatibility. Each release
  lists the breaks in the [changelog](../changelog.md).

Before `1.0.0`, minor releases may also contain breaking changes, and
SwarmQ is in that window. The changelog, with its upgrade notes per
release, is the authoritative source.

## See also

- [`llms.txt`](../../llms.txt): agent index (docs and generated spec)
- [Generated spec index](spec/index.json): machine-readable JSON
- [API reference](../api.md): Python signatures
- [Error catalog (human)](../errors.md): the prose version of the
  error table
