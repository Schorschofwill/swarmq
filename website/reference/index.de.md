# Referenz

Kanonische Spezifikation jeder öffentlichen Klasse und Methode, jedes
Befehls, jedes Konfigurationsschlüssels und jeder Fehlerklasse. Lies
diese Seiten, wenn du die exakte Signatur oder das genaue Verhalten
brauchst — zum Lernen sind die Anleitungen da.

- [API](api.md) — `SwarmQ`, `Task`, `TaskInfo`, `Middleware`,
  `Signal`, Workflows, Health und Metriken, die Exception-Hierarchie
  und der Rest der Python-Oberfläche.
- [CLI](cli.md) — das `swarmq`-Binary, seine Subcommands `worker`,
  `schedule`, `dlq`, `inspect` und `check` und seine Exit-Codes.
- [Konfiguration](configuration.md) — jede `SWARMQ_*`-Umgebungsvariable,
  die `SwarmQConfig`-kwargs, der Metrik-Katalog und jedes
  `TaskInfo`-Feld mit seinen Validierungsregeln.
- [Logging](logging.md) — Logger-Namen, Felder, der Event-Katalog,
  Integrationsrezepte für stdlib- und structlog-Apps und die
  Logging-Optionen von `swarmq worker`.
- [Fehlerkatalog](errors.md) — jede Exception, die die Library
  wirft, mit **Wann geraist**, **Ursache** und **Recovery**.
- [Changelog](changelog.md) — Release-Notes pro Version im
  [Keep-a-Changelog](https://keepachangelog.com/)-Format.
- [Agent-Referenz](agent/index.md) — Nachrichtenformate,
  Entscheidungsbäume und der maschinenlesbare Fehlerkatalog für Agents
  und Skripte.
