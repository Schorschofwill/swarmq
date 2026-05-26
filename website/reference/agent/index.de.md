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

Die Site exponiert zwei generierte Dateien im Domain-Root:

- `/llms.txt` — Single-File-TOC jeder Seite, Diátaxis-gruppiert,
  mit relativen Links. Sektionen entsprechen den Navigations-
  gruppen: Introduction, Tutorials, How-to-Guides, Reference,
  Explanation. Die Agent-Reference-Seiten erscheinen unter
  Reference.
- `/llms-full.txt` — die gesamte Site in einem newline-getrennten
  Markdown-Dokument zusammengeführt. Groß; geeignet für einen
  Einmalige-Verankerungs-Pass.

Beide Dateien werden bei jedem Docs-Build neu generiert.

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

- [`/llms.txt`](/llms.txt) — TOC für Agenten
- [`/llms-full.txt`](/llms-full.txt) — Full-Text-Dump für
  Grundlagenverankerung
- [API-Referenz](../api.md) — Python-Signaturen
- [Fehlerkatalog (menschlich)](../errors.md) — prosa-erste Version
  der Fehlertabelle
