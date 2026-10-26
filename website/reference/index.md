# Reference

Canonical specification of every public class, method, command,
configuration key, and error class. Read these pages when you need
the exact signature or behaviour; for learning, the guides are the
better start.

- [API](api.md) — `SwarmQ`, `Task`, `TaskInfo`, `Middleware`,
  `Signal`, workflows, health and metrics, the exception hierarchy,
  and the rest of the Python surface.
- [CLI](cli.md) — the `swarmq` binary, its subcommands `worker`,
  `schedule`, `dlq`, `inspect` and `check`, and its exit codes.
- [Configuration](configuration.md) — every `SWARMQ_*` environment
  variable, the `SwarmQConfig` kwargs, the metric catalog, and every
  `TaskInfo` field with its validation rules.
- [Logging](logging.md) — logger names, fields, the event catalog,
  integration recipes for stdlib and structlog apps, and the logging
  options of `swarmq worker`.
- [Error catalog](errors.md) — every exception the library raises
  with **When raised**, **Cause**, and **Recovery**.
- [Changelog](changelog.md) — version-by-version release notes in
  [Keep a Changelog](https://keepachangelog.com/) format.
- [Agent reference](agent/index.md) — message formats, decision trees
  and the machine-readable error catalog for agents and scripts.
