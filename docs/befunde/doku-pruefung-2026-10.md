<!-- Befundregister. IDs DP-nn sind die Ticketnummern für Nachweis-Tests (AGENTS.md: kein Skip ohne Ticket). -->
# Befundregister „Doku-Prüfung“ – swarmq 0.14.4, 2026-10-26

**Anlass.** Bei der Überarbeitung der Nutzer-Doku auf den Stand 0.14.4 (MR !123, Branch `docs/doku-ueberarbeitung`) haben die Doku-Agenten jede Aussage gegen den Code geprüft. Dabei fielen Abweichungen auf, die nicht die Doku, sondern den Code betreffen. Die Doku in MR !123 beschreibt das Ist-Verhalten; die Verdachte wurden getrennt gesammelt (V1 bis V7).

**Gegenprüfung** nach `docs/befunde/README.md` am 2026-10-26 durch drei Prüfer, die die Verdachte nicht gefunden haben (V1; V2 bis V4; V5 bis V7). Proben nur auf Unit-Ebene ohne Dienste. Die Rate-Limit-Probe führt das echte `sliding_window_counter.lua` über lupa mit simulierter Zeit aus.

**Status (2026-10-26).** Georg hat entschieden: alle FEHLER beheben (Branch `fix/doku-pruefung`), die LÜCKEN V3b (E-087) und V4a/V4b (E-086) wie empfohlen.

**Status (2026-10-26, Umsetzung).** Alle sieben Befunde behoben auf Branch `fix/doku-pruefung` (auf `docs/doku-ueberarbeitung`, MR !123), Nachweise ohne `xfail` grün: DP-01 `591ece7` (Lua) und `beead6f` (Jitter), DP-02 `0682a48`, DP-03 mit E-086/E-087 `5bec2e2`, DP-04 bis DP-06 `b017bac` und `b6ee56f` (Hilfe-Snapshots), DP-07 `5d6aa95`; Aufräumen V5d/V7 `3faf884`. Geplant als 0.15.0 (E-086 und E-087 sind breaking). Die Commits gelten für den Stand vor dem Merge.

Zum Jitter von DP-01: Die Probe verglich mit dem echten Skript (`100/m`, Rückstand 2000, 10 min) fünf Varianten. Ohne Jitter 100 je Minute bei 272 000 Prüfungen (vorher 17 400 bei Ø 48); Jitter bis zur Wartezeit änderte daran nichts; bis zu einem Fenster 27 500 Prüfungen, aber bis zu einem Fenster zusätzlicher Wartezeit auch bei `/h` und `/d`; gewählt ist (Fenster / Limit) · 2^n, höchstens ein Fenster: 100 je Minute bei 36 000 Prüfungen, `10/s` und `1000/h` auf Limitniveau.

## Übersicht

| Einstufung | Anzahl | Einträge |
|---|---|---|
| FEHLER | 7 | DP-01 (P2), DP-02 (P3), DP-03 bis DP-07 (P4) |
| FEHLER ohne ID (Aufräumen, kein Verhalten) | 2 | V5d, V7 |
| LÜCKE | 3 | V3b → E-087, V4a und V4b → E-086 |
| NICHT HALTBAR | 3 | V3c, V5b, V6 (Teil `ModuleNotFoundError`/`TaskNotFoundError`) |

## FEHLER

| ID | Verdacht | P | Befund | Quelle der Erwartung | Fundstelle |
|---|---|---|---|---|---|
| DP-01 | V1 | P2 | Bei Rückstand erreicht ein Rate-Limit nur etwa die Hälfte seines Durchsatzes (`100/m`: Ø 48–56 je Minute, abwechselnd etwa 100 und 10). Abgewiesene Tasks warten bis zum Ende des **festen** Fensters und kommen gesammelt an der Grenze zurück, wo das Vorfenster noch voll zählt. | Doku auf `main` (`website/guides/rate-limiting.md:58-61`): Wartende werden frei, „when enough headroom reappears. There is no hard edge at the minute mark.“; Recherche `docs/recherche-rate-limiting-locking-dedup.md:554` („geschätzte Zeit bis ein Slot frei wird“) | `src/swarmq/lua/sliding_window_counter.lua:48-52`, `worker/gates/rate_limit.py:116-117` (kein Jitter) |
| DP-02 | V2 | P3 | `schedule_many()` kodiert die Nachrichten vor `pre_enqueue`; In-place-Änderungen einer Middleware an `kwargs` kommen nicht an (bei `schedule()` schon), `post_enqueue` sieht nie veröffentlichte `kwargs`. | E-059 (jeder Enqueue läuft durch `pre_enqueue`; Nachtrag 2026-10-15: In-place-Änderungen an `kwargs` werden gespeichert) | `src/swarmq/client/schedule.py:601-624` |
| DP-03 | V3a | P4 | `TaskInfo(result_ttl=-1)` wird angenommen und scheitert erst beim Speichern (`PX -1000`), der Task läuft dann bis zum Zustelllimit; `-0.5` wird still zu `0` („nicht speichern“). | `SwarmQConfig.result_ttl` prüft den Rohwert (`config_validation.py:167`, E-055 Nachtrag 2026-10-15, E-063) | `src/swarmq/task.py:173` |
| DP-04 | V5a | P4 | Hilfetext `--concurrency`: „divided by --processes“; geteilt wird durch `--max-processes`, wenn gesetzt. | `website/reference/cli.md`, Hilfe zu `--max-processes` | `src/swarmq/cli/parser.py:127-131`, `cli/pool.py:39-40` |
| DP-05 | V5c | P4 | Hilfetext `schedule list` nennt den Key `sq:sched:defs`; er heißt `sq:{sched}:defs` (Hash-Tag). | `keyspace.py:143-144`, `website/reference/cli.md` | `src/swarmq/cli/parser_ops.py:42` |
| DP-06 | V5e | P4 | Hinweis bei `reload_aborted_alive_children` rät „re-send SIGHUP“, der Supervisor endet danach aber mit Exit 0. | `website/reference/logging.md`, `guides/hot-reload.md`, E-018 Nachtrag F3-42 | `src/swarmq/process_manager/reload.py:147-154` |
| DP-07 | V6 | P4 | Agent-Spec `errors.json` nennt für `ConfigurationError` nur Exit 78; `schedule`, `dlq`, `inspect` und `worker --check` enden mit 1. | `website/reference/cli.md` (Exit-Codes), `website/reference/agent/error-catalog-machine.md` | `src/swarmq/agent_spec.py:86-95` |

**Aufräumen ohne ID (keine Verhaltensänderung, kein Nachweis-Test nach AGENTS.md):**
- V5d: Kommentar in `src/swarmq/config.py` nennt ein Tripel mit `.normal`; die Normal-Queue ist die Basis-Queue.
- V7: `scripts/run_mutation.sh` (mutmut-2-Optionen, durch `just mutation-*` ersetzt) entfernen; Docstring von `scripts/flaky_quarantine.py` und die Marker-Texte in `pyproject.toml` ohne nightly/weekly; im Stress-Job der `.gitlab-ci.yml` den Nightly-Kommentar und die nie greifende Schedule-Regel streichen; in E-001 den erledigten Satz zum Widerspruch mit `delivery-semantics.md` als erledigt markieren (die Frage FS-01 bleibt offen).

## LÜCKEN (entschieden 2026-10-26)

| Verdacht | Frage | Entscheidung |
|---|---|---|
| V3b | `TaskInfo(max_retries=-1)` wirkt wie `0`. | E-087: `ValueError` |
| V4a | `SWARMQ_PRIORITIES=on` (jeder unbekannte bool-Wert) gilt still als „aus“; `SWARMQ_SCHEDULER` ist streng. | E-086: `on`/`off` dazu, Unbekanntes ist ein `ConfigurationError` |
| V4b | Loglevel eingebettet und als Flag nur in Großbuchstaben, über `SWARMQ_LOG_LEVEL` im Worker in jeder Schreibung. | E-086: überall ohne Rücksicht auf die Schreibung |

## NICHT HALTBAR

| Verdacht | Begründung |
|---|---|
| V3c | Negatives `retry_max_delay`: schon in Runde 3 als R3C-02 verworfen (`fehlersuche-runde-3-2026-10.md`), kein neues Argument. |
| V5b | Hilfetext `dlq list` „Drain-and-republish“ beschreibt genau das Verhalten aus E-028. |
| V6 (Teil) | `ModuleNotFoundError` in `cli.json` grob, aber nicht falsch; `TaskNotFoundError` unter `when: schedule` stimmt (im Worker nur als `error_class` der DLQ-Kopie). |

## Offen außerhalb dieser Runde

- FS-01 (`fehlersuche-runde-2-2026-10.md`, Frage 1): Soll ein Drain-Abbruch weiter ein Fehlerergebnis schreiben (E-001, KTD2)? Georg entscheidet.
