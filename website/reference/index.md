# Reference

Canonical specification of every public class, method, command,
configuration key, and error class. Read these pages when you need
the exact signature or behaviour — not when you are learning.

- [API](api.md) — `SwarmQ`, `Task`, `TaskInfo`, `Middleware`,
  `Signal`, `ProgressInfo`, and the rest of the Python surface.
- [CLI](cli.md) — the `swarmq` binary and its subcommands:
  `worker`, `schedule`, `dlq`, `inspect`.
- [Configuration](configuration.md) — environment variables,
  `SwarmQConfig` kwargs, and every `TaskInfo` field with its
  validation rules.
- [Logging](logging.md) — logger names, fields, the event catalog,
  integration recipes for stdlib and structlog apps, and the logging
  options of `swarmq worker`.
- [Error catalog](errors.md) — every exception the library raises
  with **When raised**, **Cause**, and **Recovery**.
- [Changelog](changelog.md) — version-by-version release notes in
  [Keep a Changelog](https://keepachangelog.com/) format.
