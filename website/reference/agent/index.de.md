# Agent-Referenz

Dieser Abschnitt richtet sich an KI-Agenten, Coding-Assistenten und
Automatisierungsskripte, die SwarmQ **ohne Blick in den Quellcode**
nutzen sollen. Jede Seite hier folgt drei Regeln:

1. **Form vor Prosa.** Tabellen, Enums und JSON-Fragmente kommen
   zuerst; Text stützt sie nur.
2. **Verbindlich, nicht beispielhaft.** Jedes Feld, jeder Typ und
   jeder Enum-Wert ist genau der, den SwarmQ ausgibt oder annimmt,
   ohne Auslassungen.
3. **Stabile Anker.** Abschnitts-Slugs ändern sich nur mit einem
   Eintrag im [Changelog](../changelog.md). Ein Agent kann per
   Deep-Link auf
   [`message-formats.md#task_enqueued`](message-formats.md#task_enqueued)
   verweisen und sich darauf verlassen, dass er auflöst.

Die Seiten beschreiben SwarmQ 0.14.4.

## Was hier steht

- [Nachrichtenformate](message-formats.md): die Wire-Schemata von
  Task-Nachrichten und ihren AMQP-Headern, Ergebnissen,
  Fortschrittsmeldungen, Scheduler-Einträgen, DLQ-Envelopes, Cancel-
  und Dedup-Flags, Signal-Payloads und dem Payload von
  `swarmq worker --check`.
- [Entscheidungsbäume](decision-trees.md): Mermaid-Flowcharts für die
  nicht offensichtlichen Wahlen, etwa welche Queue, welche
  Retry-Einstellung, Unique-Task oder Lock, und wann eine Nachricht in
  die DLQ geht.
- [Fehlerkatalog (maschinenlesbar)](error-catalog-machine.md): die
  Exceptions des [menschlichen Fehlerkatalogs](../errors.md) als
  strikte Tabelle mit stabilen Schlüsseln (`exception_class`,
  `exit_code_on_cli`, `retriable`, `dlq_on_uncaught`), dazu die
  Exit-Codes der CLI.

## So nutzt du diesen Abschnitt

1. Lies die [Nachrichtenformate](message-formats.md) einmal je
   Sitzung, um die Form jedes Signals und jedes gespeicherten Artefakts
   zu kennen.
2. Zieh den passenden [Entscheidungsbaum](decision-trees.md) heran,
   wenn du zwischen Features wählst (Retry-Einstellung, Unique-Task
   oder Lock, `Retry` oder `NoRetry`).
3. Schlag geworfene Exceptions und Exit-Codes der CLI im
   [Fehlerkatalog](error-catalog-machine.md) nach.

Für Python-Signaturen bleibt die [API-Referenz](../api.md)
maßgeblich; dieser Abschnitt behauptet nichts, was er nicht mit einem
Wire-Format belegen kann.

## Maschinenlesbare Dateien

Zwei maschinenlesbare Quellen liegen bei der Site:

- [`llms.txt`](../../llms.txt) im Site-Root: ein
  [llmstxt.org](https://llmstxt.org/)-Index mit einer
  Zusammenfassung in einer Zeile, Links auf die wichtigsten
  Doku-Seiten und Links auf jede der unten genannten Spec-Dateien.
- **Generierte Spec (JSON)** unter `reference/agent/spec/`, lesbar ohne
  Prosa oder Quellcode:
    - [`spec/index.json`](spec/index.json): Version und Manifest.
    - [`spec/errors.json`](spec/errors.json): jede Unterklasse von
      `SwarmQError` mit `retriable`, `dlq_on_uncaught` und Exit-Code
      (die strikte Form des [Fehlerkatalogs](error-catalog-machine.md)).
    - [`spec/config.json`](spec/config.json): jede
      Konfigurationsoption mit Typ, Default und Umgebungsvariable.
    - [`spec/cli.json`](spec/cli.json): jeder CLI-Befehl, jedes Flag
      und jeder Exit-Code.
    - [`spec/messages/`](spec/messages/task-message.schema.json): je
      Wire-Format ein JSON-Schema (Draft 2020-12):
      `task-message.schema.json`, `result.schema.json`,
      `progress.schema.json` und `schedule-definition.schema.json`.

`python -m swarmq.agent_spec` erzeugt die Spec aus dem Code, und ein
Test hält die eingecheckten Dateien gleich einer frischen Generierung;
zwischen Releases können sie also nicht vom Code abweichen. Derselbe
Befehl erzeugt die Spec für jede installierte Version neu.

## Versionierung

SwarmQ folgt [Semantic Versioning](https://semver.org/). Für Agenten:

- **Patch-Versionen (0.x.Y)** ändern kein Wire-Format. Bestehende
  Integrationen laufen weiter.
- **Minor-Versionen (0.X.y)** ergänzen das Wire-Format (neue optionale
  Header, Signale, Enum-Werte oder Felder). Alte Clients laufen weiter;
  lies neue Felder mit Defaults im Stil von `get()`, um
  vorwärtskompatibel zu bleiben.
- **Major-Versionen (X.y.z)** dürfen die Kompatibilität brechen. Jedes
  Release nennt die Brüche im [Changelog](../changelog.md).

Vor `1.0.0` dürfen auch Minor-Releases Breaking Changes enthalten, und
SwarmQ ist in diesem Fenster. Maßgeblich ist der Changelog mit seinen
Upgrade-Hinweisen je Release.

## Siehe auch

- [`llms.txt`](../../llms.txt): Agent-Index (Doku und generierte Spec)
- [Generierter Spec-Index](spec/index.json): maschinenlesbares JSON
- [API-Referenz](../api.md): Python-Signaturen
- [Fehlerkatalog (menschlich)](../errors.md): die Prosa-Fassung der
  Fehlertabelle
