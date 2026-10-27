# Methodenkatalog

Der Katalog führt je Säule die Methoden der tiefen Runden und die Module, gegen die sie laufen (Plan `docs/plans/2026-10-24-1118-chore-qualitaetsablauf-plan.md`, R1, R12, KTD8). Jede Kombination aus Methode und Modul hat im Sättigungsbuch `saettigungsbuch.toml` ihre Läufe. Den Stand berechnet `uv run just saettigung` (`scripts/saettigung.py`), gespeichert wird er nicht.

Regeln:

- Eine Kombination ist gesättigt, wenn ihre letzten zwei Läufe mit Prüfumfang größer 0 keinen bestätigten Befund brachten (R14). Bestätigt sind FEHLER, LÜCKE und VERBESSERUNG. ENTSCHEIDUNG und NICHT HALTBAR zählen nicht.
- Ändern sich seit dem Commit des letzten Laufs mindestens 20 % der heutigen Zeilen eines Moduls, ist die Kombination wieder offen. Je Datei zählt das Größere aus hinzugefügten und gelöschten Zeilen (`git diff --numstat`).
- Eine Säule ist gesättigt, wenn alle ihre Kombinationen gesättigt sind. Eine neue Methode und ein neues oder umbenanntes Modul beginnen offen.
- In den Säulen Angriffssicherheit und Performance beginnt jede Untersuchung bei einem Signal aus dem Buch (R15). Die Lese-Methode gibt es nur bei Fehler und Zuverlässigkeit (R17).
- Ein Lauf nennt seinen Prüfumfang als Zahl mit der Einheit aus der Spalte „Prüfumfang“. Ein Lauf ohne nachgewiesenen Prüfumfang wird mit `umfang = 0` eingetragen und zählt nicht.

Der Guard `tests/unit/test_saettigungsbuch.py` prüft, dass jede Methode, jedes Modul und jede Befund-ID des Buchs hier bzw. in einem Register unter `docs/befunde/` steht und dass die Modulliste zu `src/swarmq/` passt.

## Fehler

| ID | Methode | Prüfumfang | Vorgehen |
|---|---|---|---|
| M-F-01 | Lese-Methode | gelesene Dateien | Agenten lesen den Code aus einem Blickwinkel, etwa Regressionen, Absturz zwischen zwei Schritten, Nebenläufigkeit, Grenzwerte oder Doku-Zusagen (Runden 2 und 3). |
| M-F-02 | Zustandsmodelle | Beispiele | Hypothesis-Zustandsmaschinen gegen ein Referenzmodell (Runde 4, Spur Z). |
| M-F-03 | Mutation, Unit-bewertet | Mutanten | mutmut gegen `tests/unit`, danach Sichtung der Überlebenden (Runde 2; Runde 4, Lauf A). |
| M-F-04 | Mutation, Integration-bewertet | Mutanten | mutmut gegen `tests/unit` und `tests/integration`, danach Sichtung (Runde 4, Lauf B). |
| M-F-05 | Fuzzing | Beispiele | Hypothesis-Fuzz-Ziele mit zufälligem Profil und vielen Beispielen (Runde 4, Spur F). |
| M-F-06 | Konkurrenz-Katalog | gesichtete Issues | Bekannte Fehler aus den Trackern von Celery (`Issue Type: Bug Report`, `Issue Type: Regression`), Dramatiq (`bug`, `rabbitmq`, `redis`), taskiq und taskiq-aio-pika (`bug`, `brokers`, `worker`) und arq (ohne Labels, Suchwörter lost, duplicate, retry, abort, health) werden als Prüffälle gegen swarmq gestellt (R16, KTD11). Jedes Issue wird als anwendbar oder nicht anwendbar festgehalten. |

## Zuverlässigkeit

| ID | Methode | Prüfumfang | Vorgehen |
|---|---|---|---|
| M-Z-01 | Lese-Methode | gelesene Dateien | Agenten lesen den Code mit Blick auf Zustellgarantien und Wiederherstellung, etwa Lebenszyklus, Zurückgestelltes, Änderungen über Versionen und Umgebung („Betrieb über Zeit“). |
| M-Z-02 | Störungsinjektion | Störungsläufe | Störungen unter Last in der Loadsuite und `tests/chaos` (Worker-Abbruch, Broker-Neustart, Netztrennung; Runde 4, Spur I). |
| M-Z-03 | Betriebsabläufe | Abläufe | Betriebsabläufe gegen den Teststack, etwa Rolling Deploy, Neustart und Skalierung (Runde 4, Spur B). |

## Angriffssicherheit

| ID | Methode | Prüfumfang | Vorgehen |
|---|---|---|---|
| M-A-01 | Erreichbarkeit eines Scanner-Signals | Signale | Ein Treffer von ruff (`S`-Regeln) oder `uv audit` wird über einen erreichbaren Pfad nachgewiesen (öffentliche API, eingehende Nachricht, Konfiguration, CLI) oder mit Grund verworfen (R4, AE1). |
| M-A-02 | Fuzzing mit Fremddaten | Beispiele | Fuzz-Ziele mit Eingaben, die von außen kommen: eingehende Nachrichten, Header, Konfiguration, CLI-Argumente. |
| M-A-03 | Konkurrenz-CVEs | gesichtete CVEs | OSV-Abfrage je PyPI-Paket von Celery, Dramatiq, arq, taskiq und taskiq-aio-pika; eine anwendbare CVE wird ein Signal (KTD11). |

## Performance

| ID | Methode | Prüfumfang | Vorgehen |
|---|---|---|---|
| M-P-01 | Profil und A/B-Messung | Messpaare | Ab einem Signal (Profiler-Hotspot, Loadsuite-Auffälligkeit) wird mit `scripts/ab_messung.py` nach R5 gemessen, mit Profil je Seite (KTD12). |
| M-P-02 | Kostenmetrik | Szenarien | Abweichungen der Zählmetriken gegen die Basislinie (`tests/kosten/basislinie.toml`) werden auf ihre Ursache untersucht (R7). |
| M-P-03 | Konkurrenz-Performance | gesichtete Issues | Performance-Probleme aus den Trackern der Konkurrenz (wie M-F-06) werden als Signal gegen swarmq gestellt (KTD11). |

## Module

Die Pakete und Module direkt unter `src/swarmq/`, ohne `__init__.py` und `py.typed`.

- `backend`
- `broker`
- `cli`
- `client`
- `contracts`
- `lua`
- `process_manager`
- `scheduler`
- `worker`
- `_http`
- `agent_spec`
- `app`
- `backoff`
- `cancellation`
- `config`
- `config_validation`
- `cron_syntax`
- `dishka`
- `exceptions`
- `header_text`
- `headers`
- `health`
- `hot_reload`
- `identity`
- `keyspace`
- `limits`
- `locking`
- `logging`
- `logging_batch`
- `logging_drain`
- `logging_file`
- `logging_fork`
- `logging_forksource`
- `logging_format`
- `logging_queue`
- `logging_settings`
- `logging_stdlib`
- `metrics`
- `middleware`
- `progress`
- `queue_names`
- `ready_depth`
- `registry`
- `seconds`
- `serialization`
- `shutdown_budget`
- `signals`
- `slots`
- `supervision`
- `task`
- `valkey_policy`
- `workflow`
- `workflow_enqueue`
- `workflow_failure`
- `workflow_orchestration`
- `workflow_state`
- `workflow_topology`

## Entfallene Module

Module, die umbenannt oder entfernt wurden, mit Nachfolger. Alte Läufe dürfen sie weiter nennen; sie zählen für keine Kombination mehr.

Bisher keine.
