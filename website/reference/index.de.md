# Referenz

Kanonische Spezifikation jeder öffentlichen Klasse, Methode, jedes
Befehls, Konfigurationsschlüssels und jeder Fehlerklasse. Lies diese
Seiten, wenn du die exakte Signatur oder das Verhalten brauchst —
nicht, wenn du gerade lernst.

- [API](api.md) — `SwarmQ`, `Task`, `TaskInfo`, `Middleware`,
  `Signal`, `ProgressInfo` und der Rest der Python-Oberfläche.
- [CLI](cli.md) — das `swarmq`-Binary und seine Subcommands:
  `worker`, `schedule`, `dlq`, `inspect`.
- [Konfiguration](configuration.md) — Environment-Variablen,
  `SwarmQConfig`-kwargs und jedes `TaskInfo`-Feld mit seinen
  Validierungsregeln.
- [Logging](logging.md) — Logger-Namen, Felder, der Event-Katalog,
  Integrationsrezepte für stdlib- und structlog-Apps und die
  Logging-Optionen von `swarmq worker`.
- [Fehlerkatalog](errors.md) — jede Exception, die die Library
  raist, mit **Wann geraist**, **Ursache** und **Recovery**.
- [Changelog](changelog.md) — Release-Notes pro Version im
  [Keep-a-Changelog](https://keepachangelog.com/)-Format.
