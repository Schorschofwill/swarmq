# Agent-Referenz

Dieser Abschnitt ist für KI-Agenten, Coding-Assistenten und
Automation-Skripte geschrieben, die SwarmQ **ohne Lesen des
Source-Codes** nutzen müssen. Jede Seite in diesem Bucket hält
sich an drei Regeln:

1. **Form vor Prosa.** Tabellen, Enums und JSON-Fragmente kommen
   zuerst; natürliche Sprache ist stützender Kontext.
2. **Kanonisch, nicht illustrativ.** Jedes Feld, jeder Typ und
   jeder Enum-Wert ist exakt der, den SwarmQ emittiert oder
   akzeptiert — keine „zum Beispiel"-Auslassungen.
3. **Stabile IDs und Anker.** Section-Slugs ändern sich nicht ohne
   einen semver-bumped Eintrag im [Changelog](../changelog.md).
   Ein Agent kann per Deep-Link auf `#task-enqueued` verweisen und
   darauf vertrauen, dass es auflöst.

## Was hier lebt

- [Nachrichtenformate](message-formats.md) — die Wire-Schemata für
  Task-Nachrichten, Ergebnisse, Progress-Updates, Scheduler-
  Einträge, DLQ-Envelopes und Cancel-Flags.
- [Entscheidungsbäume](decision-trees.md) — Mermaid-Flowcharts für
  die nicht-offensichtlichen Wahlen: welche Queue, welcher
  Retry-Modus, wann ein Task expired ist, wann ein Lock
  freigegeben wird, wann ein Unique-Flag freigegeben wird.
- [Fehlerkatalog (maschinenlesbar)](error-catalog-machine.md) —
  dieselben Exceptions wie im
  [menschlichen Fehlerkatalog](../errors.md), aber in strikt
  tabellarischer Form mit stabilen strukturierten Keys
  (`exception_class`, `exit_code_on_cli`, `retriable`,
  `dlq_on_uncaught`).

## Wie du diesen Abschnitt nutzt

Agenten, die SwarmQ-Verhalten brauchen, sollten:

1. Einmal pro Session die
   [Nachrichtenformate](message-formats.md) lesen, um die
   Payload-Form jedes Signals und gespeicherten Artefakts zu
   verankern.
2. Den relevanten [Entscheidungsbaum](decision-trees.md) ziehen,
   wenn zwischen Features zu wählen ist (Retry-Config, Unique vs
   Lock, wann `Retry` vs `NoRetry` emittieren).
3. Den [Fehlerkatalog](error-catalog-machine.md) querreferenzieren,
   wenn raised Exceptions oder CLI-Exit-Codes interpretiert werden.

Für Python-Ground-Truth-Signaturen bleibt die
[API-Referenz](../api.md) kanonisch — dieser Abschnitt
paraphrasiert nichts, was er nicht mit einem Wire-Format stützen
kann.

## Wie du die Docs programmatisch konsumierst

Zwei maschinenlesbare Flächen liegen im Site-Root und unter
`reference/agent/spec/`:

- `/llms.txt` — ein [llmstxt.org](https://llmstxt.org/)-Index: eine
  Einzeiler-Zusammenfassung, Links auf die wichtigsten Doku-Seiten
  und Links auf jede der unten generierten Spec-Dateien.
- **Generierte Spec (JSON)** — strukturierte Source-of-Truth-Daten,
  die ein Agent ohne Prosa- oder Quellcode-Lesen parsen kann:
    - [`spec/index.json`](spec/index.json) — Version + Manifest.
    - [`spec/errors.json`](spec/errors.json) — jede `SwarmQError`-
      Subklasse mit `retriable` / `dlq_on_uncaught` / Exit-Code
      (die strikte Form des [Fehlerkatalogs](error-catalog-machine.md)).
    - [`spec/config.json`](spec/config.json) — jede Config-Option
      mit Typ, Default und Umgebungsvariable.
    - [`spec/cli.json`](spec/cli.json) — jeder CLI-Befehl, jedes
      Flag und jeder Exit-Code.
    - [`spec/messages/result.schema.json`](spec/messages/result.schema.json)
      und Geschwister — JSON-Schema (Draft 2020-12) je Wire-Format.

Die Spec-JSON wird per `python -m swarmq.agent_spec` aus dem Code
generiert und per Tests daran verankert, kann also zwischen Releases
nicht stillschweigend von der Realität abweichen. Mit demselben Befehl
für jede installierte Version neu erzeugbar.

## Versionierung

Die Library folgt [Semantic Versioning](https://semver.org/).
Für Agenten:

- **Patch-Versionen (0.x.Y).** Keine Wire-Format-Änderungen.
  Bestehende Integrationen laufen weiter.
- **Minor-Versionen (0.X.y).** Additive Wire-Format-Änderungen
  (neue optionale Header, neue Signals, neue Enum-Werte). Alte
  Clients laufen weiter; neue Felder sollten mit `get()`-Style-
  Defaults gelesen werden, um vorwärtskompatibel zu bleiben.
- **Major-Versionen (X.y.z).** Breaking Changes sind erlaubt.
  Jedes Release listet sie im [Changelog](../changelog.md) unter
  `### Changed`- oder `### Removed`-Abschnitt auf.

Pre-`1.0.0` dürfen Minor-Releases Breaking Changes enthalten —
SwarmQ ist derzeit in diesem Pre-Release-Fenster. Der Changelog
ist die autoritative Quelle.

## Siehe auch

- [`/llms.txt`](/llms.txt) — Agent-Index (Docs + generierte Spec)
- [Generierter Spec-Index](spec/index.json) — maschinenlesbares JSON
- [API-Referenz](../api.md) — Python-Signaturen
- [Fehlerkatalog (menschlich)](../errors.md) — prosa-erste Version
  der Fehlertabelle
