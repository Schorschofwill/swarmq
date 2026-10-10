---
title: Behebung der Befunde „Betrieb über Zeit“ (BZ-01 bis BZ-28) – Plan 0.12.0
type: fix
date: 2026-10-09
revision: 4 (… F1–F11 sowie Spike-Ergebnisse S1–S9 aus MR 1 eingearbeitet)
artifact_readiness: implementation-ready
execution: code
---

# Behebung der Befunde „Betrieb über Zeit“ – Plan 0.12.0

## Goal Capsule

- **Objective:** Alle 28 Befunde aus `docs/befunde/betrieb-ueber-zeit-2026-10.md` beheben. Ausnahme ist BZ-20: Dort gilt eine dokumentierte Grenze mit Warnung.
  - **Hauptbedingung:** Kein Fix erzeugt einen neuen Fehler. Das gilt auch im Mischbetrieb mit 0.10/0.11, in Bestandsdeployments, beim Rollback und über Wochen Betrieb.
- **Authority:**
  - Bei Konflikten gilt zuerst das Register (Abschnitt „Entscheidungen“) zusammen mit Abschnitt 1 dieses Plans.
  - Danach gelten die Querschnittsregeln Q1–Q18, zuletzt die MR-Beschreibungen.
  - AGENTS.md gilt für jede Unit.
- **Abnahme:**
  - Abgenommen wird über die Nachweis-Tests (`@pytest.mark.befund`, `xfail(strict=True)`). Ein Fix ist fertig, wenn sein Nachweis grün ist und der Marker entfernt ist.
  - Widerspricht ein Nachweis der Entscheidung oder dem Design, wird er im selben MR **vor** dem Fix umgeschrieben (Abschnitt 6).
- **Ausführung:**
  - Phase 0 umfasst Refactor, Werkzeuge und Spikes, ohne Verhaltensänderung.
  - Danach folgen MR 2–19 in der Reihenfolge aus Abschnitt 4. Die Nummern sind zugleich die Reihenfolge.
  - Gearbeitet wird lokal auf Branches. Push, MR, Merge, Tag und Release nur nach Georgs ausdrücklicher Freigabe.
- **Stop conditions:** Das betroffene MR wird angehalten und notiert, unabhängige MRs laufen weiter. Anlässe:
  - Ein Spike widerspricht einer Annahme des Plans.
  - Die Mischbetrieb-Suite zeigt Verlust oder Doppelausführung, die nicht als Grenze in Abschnitt 7 steht.
  - Ein bestehender Test muss geändert werden, der nicht in Abschnitt 6 steht.
  - Ein bestehender Golden-Eintrag ändert sich, ohne Q3-Ausnahme zu sein.
  - Der Loadsuite-Durchsatz sinkt um mehr als 10 %, oder die Keyzahl wächst ohne Plateau.
  - Es taucht eine echte Produktfrage auf.
- **Tail ownership:** Georg. Den PyPI-Upload führt er per `!` aus.

---

## 1. Entscheidungen

Grundlage ist der Abschnitt „Entscheidungen“ im Register. Georg hat am 09.10. abends ergänzt:

| ID | Präzisierung |
|---|---|
| BZ-04 | Default `strict` mit lautem Upgrade-Hinweis. Abschalter: `SWARMQ_VALKEY_EVICTION_CHECK=warn\|off`. Ist die Policy **nicht prüfbar**, blockiert das nie und erzeugt nur eine Warnung. |
| BZ-05 | Gemessen wird ab dem letzten **Halterwechsel**. Die DLQ greift erst, wenn derselbe Halter länger als TTL + 60 s hält. `max_age` bleibt die harte Obergrenze. |
| BZ-08 (Cron) | Ein Replay eines Cron-Laufs läuft. |
| BZ-10 | Der Namensraum gilt für alle Valkey-Keys, Pub/Sub-Kanäle, den Leader und den Kontrollkanal. Queue-Namen bleiben ohne Präfix; für RabbitMQ gilt „eigener vhost je App“. |
| BZ-11 | Termine werden zurückgehalten, nicht verworfen. |
| BZ-12 | Die Restlücke wird akzeptiert. Die Doku empfiehlt `max_age` für Cron- und kritische Tasks. |
| BZ-15 | `x-delivery-limit` wird tolerant behandelt: Es gilt der vorhandene Wert, dazu eine WARNING. Andere **erkannte** Argumentkonflikte führen zu Exit 78. |
| BZ-18 | `SWARMQ_SHUTDOWN_TIMEOUT` ist das Gesamtbudget ab SIGTERM. |
| BZ-19 | `auto`: Neue Queues werden at-least-once angelegt, Bestandsqueues behalten ihre Argumente. |
| BZ-20 | Dokumentierte Grenze, weil globales QoS bei Quorum-Queues nicht geht. |
| BZ-25 | Weiter erneuern, solange der Handler lebt, auch über `hard_timeout + 30 s` hinaus. |
| BZ-03 (Oneshot) | Mit Backoff erneut versuchen. Ein Cron-Fire gilt als verloren und wird gezählt. |

**Technisch festgelegt** (Empfehlung, Georg kann widersprechen; \* = in Revision 2 neu oder geändert):
- **BZ-01:** 7 Stufen von 1 s bis 1 h; längere Wartezeiten bleiben in `{q}.DQ`.
- **BZ-06:** Gnadenfrist 900 s; 0 schaltet sie ab.
- **BZ-23:** Kein `task_name` in `cancel()`.
- **KTD11:** Die Leader-Versionserkennung warnt künftig auch bei Leadern < 0.12.
- **BZ-10:** Namensraum nur für neue Apps oder nach dokumentiertem Leeren.
- **Mindestversionen:** RabbitMQ 4.2/4.3, Valkey 8, Python 3.14.
- \* **BZ-23 (Rev. 3 F11):** Das Cancel-Flag wird bei der stornierten Zustellung **und** beim Abbruch während des Laufs (`worker/results.py:129,275`, `pipeline.py:589`) nicht gelöscht, sondern per `PEXPIRE … LT` auf höchstens 1 h verkürzt. Die Variante `LT` verlängert ein kürzeres Flag nie, die Abhilfe „kurze `ttl_ms`“ bleibt also intakt. Sonst laufen at-least-once-Duplikate (BZ-19) einer stornierten Nachricht doch. Changelog: Wird dieselbe `task_id` innerhalb 1 h neu eingereiht, wird sie storniert, und `cancel()` liefert dann False.
- \* **Autoscaling (BZ-01/05/06, Rev. 3 F9):** Der Autoscaler zählt nur die **Haupt-Queue**. Lock-, Guard- und Backend-Rückstellungen liegen dauerhaft in Stufen ≤ 1 min, sonst skalierten sie stundenlang auf Maximum. Kurze Retries kehren binnen Sekunden in die Haupt-Queue zurück und werden dort gezählt. Die Metrik `queue_depth{kind="pending"}` behält ihre heutige Bedeutung (Haupt-Queue + alle Delay-Queues). Neu ist `kind="delayed"` (nur Delay-Queues). Der Sampler ruft dafür die neue Broker-Methode `ready_depth(queue)` auf. Fehlt sie (Fake, Fremd-Broker), fällt er nach Q15 auf `queue_depth` zurück. Der Changelog nennt die Änderung.
- \* **Lock-Konfliktzähler (BZ-05, Rev. 3 F8):** 0.12 erhöht `x-swarmq-lock-conflict-count` nicht mehr, sonst schicken 0.11-Pods derselben App wartende 0.12-Kopien ab 10 sofort in die DLQ. Stattdessen zählt der neue Header `x-swarmq-lock-conflicts`, den 0.11 nicht liest. Aus ihm wird das Signal-kwarg `lock_conflict_count` gespeist, dessen Bedeutung damit unverändert bleibt.
- \* **Rollout-Reihenfolge:** Erst alle Apps als eigenes Deployment auf 0.11.1, dann auf 0.12. Ein Rollback per K8s oder Docker landet sonst auf 0.11.0.

---

## 2. Querschnittsregeln (Q1–Q18)

- **Q1 Wire-Kompatibilität:**
  - Neue Header, Felder und Keys sind additiv.
  - `msgspec`-Felder kommen ans Ende und haben den Default `None`.
  - Neue Signale gibt es nur als neue Enum-Werte, ohne neue kwargs an bestehenden Signalen.
- **Q2 Mischbetrieb-Suite** `tests/compat/`:
  - **Umgebung:**
    - Ein isoliertes venv für swarmq 0.11.0 bzw. 0.11.1: `uv run --no-project --isolated --with swarmq==0.11.0 --constraints <export aus uv.lock von v0.11.0>`.
    - Daraus wird einmal `sys.executable` ermittelt; die Prozesse starten direkt damit. Ein verschachteltes `uv run` würde PDEATHSIG und `crash_worker` stören.
    - Start: `python -m swarmq.cli worker --module tests.compat.<fixture> …`, cwd = Repo-Root.
    - Die Fixtures importieren nichts aus `tests.support`, was erst in 0.12 existiert.
  - **Echtheitsprüfung:** Jeder Prozess prüft `importlib.metadata.version("swarmq") == "0.11.0"` und dass `swarmq.__file__` nicht unter `<repo>/src` liegt. Sonst bricht er ab; die Suite darf nicht vakuum-grün werden.
  - **Leaderwechsel:** Zuerst den 0.11-Worker starten, dann warten, bis `sq:{sched}:leader_version == "0.11.0"`, erst dann 0.12 starten.
  - **Szenarien:**
    - Retry, Cron-Fire, Oneshot, Lock-Konflikt.
    - DLQ-Replay: UI-artig, CLI 0.11 und CLI 0.12.
    - Unbekannter Task, Leaderwechsel in beide Richtungen, Replay in-flight beim Versionswechsel.
    - Rollback auf das **vorher deployte** Image (0.11.0 und 0.11.1) nach Neuanlage von Queues.
    - Rollback mit gesetztem Namensraum: 0.11.1 muss verweigern.
    - Zwei Apps, eine davon dauerhaft auf 0.11.0.
  - **Erwartung:** neues oder unverändertes altes Verhalten. Verlust oder Doppelausführung nur, wo Abschnitt 7 es nennt.
  - **CI:** Der Job braucht Netz zu PyPI und Build-Werkzeuge für `valkey[libvalkey]`.
- **Q3 Golden-Fixtures** (`queue_args.json`, `keys.json`, `headers.txt`, `lua_contract.txt`, `channels.txt`, `values/*`):
  - Bestehende Einträge bleiben byte-identisch, neue sind additiv.
  - Jedes MR nennt jede betroffene Datei in Abschnitt 6 und im Changelog.
  - **Benannte Ausnahmen:**
    - `guard_claim` ARGV 5→6 (MR 12).
    - `values/schedule_definition.json` und die Feldliste in `test_golden_fixtures.py:49-56` bekommen neue `null`-Felder (MR 5, 6, 9). `ScheduleDefinition` hat kein `omit_defaults`.
    - Der Beschreibungstext `"delay"` in `_golden.py:205` (MR 10).
  - **Generator-Umbau in MR 8:** Deklarationen auf eigenem Channel erfassen. `quorum` entsteht im Modus `legacy`. `quorum_at_least_once` und die Stufen (MR 10) kommen über **getrennte** Aufrufe als neue Schlüssel.
- **Q4 Keyspace:**
  - Keine `sq:`-Literale außerhalb von `swarmq/keyspace.py`; ein Architekturtest prüft das.
  - Ausgenommen sind Kommentare und Docstrings, `_LOCK_KEY_PREFIX` in `locking.py` (Alias) und der Hilfetext in `cli/parser.py:299`.
- **Q5 Valkey-Wachstum:**
  - Jeder neue Key hat eine TTL oder einen benannten Aufräumer.
  - Ausnahmen: `sq:{sched}:epoch` (ein Key) und `sq:{sched}:report:known` (wird mit der Definition entfernt).
  - Jedes MR enthält eine Wachstumsschätzung für 105 Crons, 3 Apps und 1 Woche.
- **Q6 Fehlerpfade:**
  - I/O läuft mit Zeitlimit.
  - Jede Wiederhol- oder Rückstellschleife hat eine getestete Obergrenze.
  - Zustands-Header einer Schleife (`unknown-since`, `lock-wait-since`, `lock-holder*`, `lock-conflicts`) bleiben in den Kopien der **Gates** erhalten (Rate-Limit-, Guard- und Backend-Defer, Lock), sonst setzte jede fremde Rückstellung die Frist zurück (Rev. 3 F7). Entfernt werden sie nur an drei Stellen: in der fachlichen Retry-Kopie (`retry.py:129`), im DLQ-Stempel (`_publish_to_dead_letter_queue`) und von `dlq retry`. Die Obergrenze wird **mit** Kopien aus jedem Gate getestet, auch bei einem Task mit Lock **und** Rate-Limit.
- **Q7 Hot-Path:**
  - Im Normalpfad kommt kein zusätzlicher Roundtrip pro Nachricht dazu.
  - Belegt wird das per Loadsuite-Benchmark vor und nach MR 6, 8, 10, 12, 13 und 16.
- **Q8 Dateigrenzen:**
  - Jedes MR prüft alle src-Dateien gegen 800 Zeilen; `test_architecture_guards.py` prüft das bereits.
  - Gefährdet sind:
    - `lifecycle.py` 797
    - `pipeline.py` 774
    - `client/schedule.py` 762
    - `workflow_orchestration.py` 754
    - `cli/parser.py` 733
    - `supervisor.py` 721
    - `loop.py` 709
    - `config.py` 709
    - `connection.py` 550
    - `delivery.py` 427
  - Neue Logik kommt in neue Module.
- **Q9 Bestehende Tests:** Nur die in Abschnitt 6 genannten Tests ändern ihre Erwartung.
- **Q10 Marker:**
  - Ein MR entfernt die Marker seiner BZ, wie in Abschnitt 6 je Nachweis zugeordnet.
  - Gegenprobe: `pytest -m befund --runxfail` zeigt die übrigen Nachweise weiter rot.
- **Q11 Konfiguration und Spec:**
  - Jede neue `SWARMQ_*`-Variable ist ein Feld von `SwarmQConfig` und steht als Literal in `config.py` (`test_agent_spec.py:134-148`).
  - Jede neue `SwarmQError`-Unterklasse bekommt einen Eintrag in `_ERROR_SEMANTICS` (`agent_spec.py:77`).
  - Jedes MR erzeugt die Spec neu (`python -m swarmq.agent_spec`; `test_agent_spec.py:440-452`), als eigenen TDD-Zyklus.
- **Q12 Gates je MR:**
  - Ohne Rerun: Unit, Integration, e2e (inklusive `tests/integration/spikes`), `tests/compat` (`-m compat`, opt-in), Chaos, ruff, mypy, Q10. Spikes laufen zusätzlich gegen 4.2.9 (`SWARMQ_TEST_RABBITMQ42_PORT=5742`), solange der Container läuft.
  - Integrationstests nur über den Wrapper mit 5699/6399 (nie 5672/6379) und nie zwei Läufe parallel.
  - Das Ergebnis wird vor dem Commit ausdrücklich geprüft.
- **Q13 Ein roter Test zur Zeit:**
  - Der Fix-Zyklus beginnt damit, den Marker seines Nachweises zu entfernen. Der Nachweis ist dann der eine rote Test.
  - Contract-Fälle: erst die InMemory-Bindung, dann Valkey.
  - Golden-Aktualisierungen bilden einen eigenen Zyklus.
  - Signaturen werden nur additiv mit Default geändert.
- **Q14 Ausnahme-Reihenfolge:**
  - `PublishError` erbt von `DeliveryError`.
  - `PublishIndeterminate`, `SettleFailedAfterPublish`, `UnroutableMessage`, `BrokerPublishBlocked` und `MessageHeadersTooLarge` erben von `BrokerConnectionError`.
  - Jeder `except`, der zwischen diesen Klassen unterscheidet, bekommt einen Regressionstest.
- **Q15 Optionale Protocols:**
  - Nie über `isinstance` auf ein `runtime_checkable`-Protocol erkennen; `MagicMock` passt dort immer.
  - Den Aufruf in `try/except` setzen, mit Rückfall auf das sichere Verhalten.
  - Ein Test mit `MagicMock`-Store sichert das ab.
- **Q16 Reinheit:**
  - `worker`, `scheduler` und `client` importieren weder `backend.valkey` noch `broker.rabbitmq`. Das gilt auch transitiv und unter `TYPE_CHECKING` (`test_architecture_guards.py:52-56,97,102`).
  - Gemeinsame reine Helfer kommen in neutrale Module (`swarmq/keyspace.py`, `swarmq/queue_names.py`).
  - Backend- und Broker-Fähigkeiten werden über Methoden bzw. optionale Protocols angesprochen.
- **Q17 Refactor-Neutralität:**
  - Verschobener Code liest Konstanten, `time` und Logger über das Ursprungsmodul, z. B. `from swarmq.worker import lifecycle as _lc; _lc._LIFECYCLE_DISPATCH_TIMEOUT_S`. Logger-Namen bleiben gleich. So greifen bestehende `patch(...)`-Pfade und Logger-Assertions weiter.
  - Neue Untermodule werden in `_SPLIT_MODULES` eingetragen (`test_architecture_guards.py:59-75`).
  - Mixins nutzen Stubs unter `TYPE_CHECKING`, ohne zyklischen Rückimport.
- **Q18 Doku je MR:**
  - Jedes MR schreibt seinen Changelog-Eintrag (Abschnitt „Unreleased“) und seinen Doku-Abschnitt in EN und DE selbst. MR 19 konsolidiert nur.
  - **Release-Gates:**
    - Kein Release mit MR 8 oder später, bevor 0.11.1 auf PyPI liegt **und** im Deploy-Leitfaden als erster Schritt steht.
    - Kein Release eines Standes, in dem ein MR seine Voraussetzung aus Abschnitt 4 nicht erfüllt.

---

## 3. Phase 0

### 3.1 MR 0 – Refactor (Verhalten unverändert, bestehende Golden-Einträge byte-identisch)

1. **`swarmq/keyspace.py`** (neutral und rein):
   - `KeySpace(namespace=None)` bietet je Key-Familie eine Methode.
   - Die Stores bekommen `keys: KeySpace`: results, chords, execution_guard, locks (inklusive `backend/valkey/locks.py:17,58,77,99,138`), rate_limit, schedules, control.
   - `locking.py` behält `lock_key()`, `SCHEDULER_LOCK_KEY` und `_LOCK_KEY_PREFIX` als Aliase. Sie werden aus `swarmq.keyspace` gebildet, nicht aus dem Backend (Q16). `ZSET_KEY`/`DEFS_KEY` bleiben als Aliase in `backend/valkey/schedules.py:28-29`.
   - Tests: tabellengetriebener Test `KeySpace(None)` gegen `golden/keys.json`, dazu der Architekturtest für Q4. `keys.json` bekommt die fehlenden `sq:control:*` (additiv).
2. **`swarmq/queue_names.py`** (neutral und rein):
   - `queue_family(q)`, `is_delay_queue(name)` und `delay_queue_for(q, delay_ms)`; die letzte Funktion wird ab MR 10 genutzt.
   - Re-Export in `broker/rabbitmq/topology.py`. Dort kommen auch die Argumentsätze und `parse_inequivalent_arg()` hin.
3. **`config.py`:** Die `_validate_*`-Methoden ziehen nach `config_validation.py` (Mixin ohne Rückimport). `_ENV_OVERRIDES` bleibt in `config.py`.
4. **`worker/lifecycle.py` → `worker/shutdown.py`:**
   - Mixin `WorkerShutdown` nach Q17.
   - Der Patch auf `lifecycle._LIFECYCLE_DISPATCH_TIMEOUT_S` (`test_lifecycle.py:724-726`) greift weiter, die Logger-Namen bleiben.
5. **`worker/pipeline.py`** wird aufgeteilt:
   - `_release_acquired_locks` → `worker/gates/lock.py`
   - Lookup unbekannter Tasks → `worker/gates/unknown_task.py`
   - `_finalize_schedfire`, `_release_schedfire_claim`, `_maybe_release_unique` → `worker/finalize.py`
6. **`scheduler/loop.py` → `scheduler/fire.py`:**
   - Betrifft `_fire_one`, `_resolve_task_info` und `_emit_duplicate_detected`.
   - `time` wird über `loop.time` gelesen (`test_loop.py:1166`), der Logger heißt weiter `swarmq.scheduler.loop`.
7. **Weitere Splits:**
   - `client/schedule.py` → `client/schedule_resolve.py`. `ScheduleStore` (`test_schedule.py:571`) und `_ensure_schedule_store` bleiben im Ursprungsmodul.
   - `workflow_orchestration.py` → `workflow_failure.py`.
   - `cli/parser.py` → `cli/parser_ops.py`.
   - `process_manager/supervisor.py` → Mixin `restart_budget.py`.
     - `time` wird über `supervisor.time` gelesen (`test_befund_bz27…:55`, `test_befund_bz18…:84`, `test_supervisor.py:1803`).
     - `pm._maybe_restart` und `pm._hang_timestamps` bleiben Attribute.
8. `_SPLIT_MODULES` und `test_cli_module_surface.py:184` werden nachgezogen (Abschnitt 6).

### 3.2 MR 1 – Werkzeuge und Spikes

- **Mischbetrieb-Suite:** nach Q2 mit den Grundszenarien. Die Basislinie ist grün gegen den heutigen Code.
- **`tests/support/wait.py`** mit `wait_until`. Die `asyncio.sleep`-Polls in den Nachweisen werden darauf umgestellt:
  - Umgestellt: e2e :198/:219, BZ-17 :119, BZ-20 :111, BZ-14 :119, BZ-16 :87/:164, BZ-08 :83.
  - Ausgenommen, weil sie echte Zeit brauchen: BZ-25 :116, BZ-23 e2e :311.
- **`tests/support/queues.py`** mit `delete_queue_family()`. Die rund 20 Testdateien mit eigenen Löschlisten werden darauf umgestellt.
- **Brokerversionen für die Spikes:**
  - 4.3.6 auf dem Testbroker 5699.
  - 4.2.9 über den Compose-Dienst `rabbitmq42` (5742/15742). Gestartet wird **nur dieser Dienst** (`docker compose up -d rabbitmq42`), denn dieselbe Compose-Datei bindet auch 5672/6379.
  - Ist der Start nicht erlaubt, laufen die Spikes nur auf 4.3.6, und die Prüfung auf 4.2.9 wird für Georg vermerkt.

### 3.3 Spikes (als Integrationstests im Repo)

| Spike | Frage | Betrifft |
|---|---|---|
| S1 | Texte von 406 „inequivalent arg“ für delivery-limit, queue-type, dead-letter-strategy und overflow, in beide Richtungen („current is none“ und „current is at-least-once“). Wirksamer Default von `x-delivery-limit` ohne Argument (20?). Bedeutung von -1 und 0. Verhältnis einer `delivery-limit`-Policy zum Argument. | MR 8 |
| S2 | at-least-once mit Per-Message-TTL über den Default-Exchange, auch bei Verfall am Kopf. Publish auf eine Queue mit `reject-publish` und `max-length`-Policy. | MR 8, 10 |
| S3 | Globales QoS beim Consume einer Quorum-Queue. | MR 18 |
| S4 | Delivery-Count bei `nack(requeue=True)`, bei Channel-Abbruch und bei `basic.get` ohne Ack mit Rückgabe, **auf einer XQ ohne Argumente** (Default-Limit). | MR 13, 16 |
| S5 | Bleibt ein client-gesetztes `x-death` beim Republish erhalten, oder wird es entfernt? Was trägt ein Shovel- oder UI-Republish nach einem Broker-DLX? | MR 12 |
| S6 | `PublishError` ist eine `DeliveryError` (statisch belegt). Zugriffspfad zur aiormq-Verbindung (`conn.transport.connection`), auch nach robustem Reconnect. Lässt sich der Blocked-Zustand **synchron** lesen? | MR 6, 16 |
| S7 | Ändert ein wiederholter Broker-DLX in `x-death` nur `count` oder auch `time`? | MR 12 |
| S8 | 406- und 403-Texte, die keine Argumentkonflikte sind: exklusive Queue, `max-queues` erreicht, ACCESS_REFUSED. | MR 8, 10 |
| S9 | Stufen mit Per-Message-TTL bei gemischten Delays in einer Stufe: Bestätigung des Deckels beim Vierfachen. | MR 10 |

---

## 4. Reihenfolge

| # | MR | BZ | Voraussetzung |
|---|---|---|---|
| 0 | Refactor | – | – |
| 1 | Werkzeuge + Spikes | – | 0 |
| 2 | Kleinkram | BZ-26, BZ-24, BZ-17 | 0 |
| 3 | Terminale Pfade | BZ-13, BZ-22, BZ-29 (+ Pfad beim Lock-Deckel, Teil BZ-05) | 0, 1 (S4) |
| 4 | Scheduler-Basis | BZ-28, BZ-07 | 0 |
| 5 | Stabile Oneshot-ID | BZ-02, BZ-23, KTD11 ≥ 0.12 | 4 |
| 6 | Unroutable | BZ-03 | 1 (S6), 3, 5 |
| 7 | Rollback-Ziel 0.11.1 (Branch von `v0.11.0`) | – | – |
| 8 | Queue-Deklaration | BZ-15, BZ-19 | 6, 7, S1, S2, S8 |
| 9 | Cron-Dedup | BZ-09 | 4, 6 |
| 10 | Delay-Stufen | BZ-01 | 6, 8, S9 |
| 11 | Unbekannte Tasks + verwaiste Crons | BZ-06, BZ-11 | 3, 4, 10 |
| 12 | Ausführungsidentität | BZ-08, BZ-12 | 10, S5, S7 |
| 13 | Locks | BZ-25, BZ-04 (Laufzeit), BZ-05 | 3, 10, S4 |
| 14 | Valkey-Umgebung | BZ-04 | 0 |
| 15 | Mehrere Apps | BZ-10 | 6, 9, 14 |
| 16 | Verbindungen | BZ-16, BZ-21 | 6, 12, S4, S6 |
| 17 | Prozess-Lebenszyklus | BZ-18, BZ-27 | 13 |
| 18 | Beobachtung | BZ-14, BZ-20 | 10, 14, 15, S3 |
| 19 | Doku-Konsolidierung, Changelog, RC | Doku-Befunde | alle |

**Gründe für die Reihenfolge:**
- **Terminal-Helfer vor Unroutable:** Ein unroutbarer Workflow-Advance muss terminal scheitern. Sonst läuft der Schritt bis zum Delivery-Limit erneut.
- **Stabile ID vor Unroutable:** Ein verworfener Oneshot braucht sie für sein Fehlerergebnis.
- **Unroutable vor allem, was neue Queues nutzt.**
- **Deklaration vor den Stufen:** Die Stufen entstehen gleich mit den endgültigen Argumenten.
- **Stufen vor Identität:** `is_delay_queue` muss alle Stufen kennen.
- **Identität vor nicht-zerstörendem Lesen:** Ein wiederholter `dlq retry` darf nicht doppelt ausführen.
- **Locks vor Shutdown-Budget:** Der Keeper muss den Drain überdauern.
- **0.11.1 vor jedem Release ab MR 8.**

---

## 5. Units

### MR 2 – BZ-26, BZ-24, BZ-17

- **BZ-26** wird in `calculate_backoff` behoben (`backoff.py:7-36`), nicht in `backend_defer_delay`.
  - Der Exponent wird gedeckelt: `min(attempt, 64)`.
  - Der Jitter nutzt `u = jitter − 1` aus der **bestehenden** Ziehung `uniform(1,2)`. Es gibt keinen zusätzlichen `random`-Aufruf, denn der Nachweis patcht `backoff.random.uniform`.
  - Oberhalb von `max_delay` gilt `max_delay·(0,6+0,2u)`.
  - `fixed` bleibt unverändert.
- **BZ-24:**
  - `client_task_defaults_unresolved` (WARNING) erscheint einmal je Task-Name und Client-Instanz, wenn der Producer ohne TaskInfo sendet (keine Registry oder Name nicht in der Registry); kein Abschalter. (Rev. 4: Ausnahme „`queue=`/`priority=` gesetzt“ entfernt, unerreichbar: ohne Registry ist `queue=` Pflicht, `priority=` gibt es nicht.)
  - Die Menge der gemeldeten Namen ist auf 1000 gedeckelt.
- **BZ-17:**
  - `bounded_header_text()` begrenzt Traceback- und Lock-Render-Header auf 8 KiB: 6 KiB vom Anfang, 2 KiB vom Ende.
  - An der Broker-Grenze greift ein Schutz bei ½ `frame_max`: zuerst kürzen, dann `MessageHeadersTooLarge` (mit `_ERROR_SEMANTICS`, Q11).
  - Der Nachweis wird umgeschrieben: Er prüft die Tiefe der **Haupt-Queue** per passivem Declare, nicht `queue_depth` (das zählt `.DQ` mit).

### MR 3 – BZ-13, BZ-22, BZ-29 (+ Pfad beim Lock-Deckel)

- **BZ-29 (neu aus Spike S4):** Auf RabbitMQ 4.3 erhöht `nack(requeue=True)` den `x-delivery-count` nicht. Es zählt nur `x-acquired-count`, der bei der zweiten Zustellung mit 1 beginnt. Die vier `redeliver`-Pfade (`guarded_broker.py:71`, `gates/__init__.py:233,238,248`) laufen deshalb auf 4.3 ohne Grenze und ohne Pause, auf 4.2 enden sie nach rund 20 schnellen Runden still im Broker-DLX. Fix:
  - `DeliveryCountGate` zählt `max(x-delivery-count, x-acquired-count)`. Je nach Zähler heißt der `error_type` weiter `WorkerHungError` oder neu `RedeliveryLimitError` (Eintrag in `_ERROR_SEMANTICS`, Q11).
  - `redeliver` wartet vor dem `nack` `min(0,1·2^k, 5 s)` mit k = max(Zähler), maximal 5 s, und hält dabei den Slot. Damit verteilen sich die rund 20 Versuche auf mehr als eine Minute, und ein kurzer Publish-Ausfall schickt nichts sofort in die DLQ. Abbruch der Wartezeit beim Shutdown.
  - Das Gate nutzt die Konfiguration bzw. ab MR 8 das effektive Limit.
  - Nachweis-Test `tests/unit/worker/gates/test_befund_bz29_redeliver_limit.py` zuerst (xfail strict), Register-Eintrag BZ-29.
  - Folgefehler-Prüfung: Auf 4.2 zählt `x-acquired-count` nicht (None), dort bleibt das Verhalten gleich, nur mit Backoff. Der Hang-Pfad (Channel-Abbruch) zählt auf beiden Versionen wie bisher.

- **`worker/terminal.py`:** `fail_workflow_step()` (aus `retry.py:209-245`) und `GateDeps.fail_workflow`. Nutzer:
  - Expiry.
  - Lock-Render-Fehler **und Lock-Konflikt-Deckel** über `RetryHandler.dead_letter(extra_headers=…)`. Das liefert `TASK_DLQ`, Ergebnis, Workflow-Fail und Unique-Freigabe. Der Deckel bleibt vorerst bei 10.
  - `route_via_dlx`, best effort.
- **Q10-Ausnahme:** Die BZ-05-Nachweise 2/3 (`test_befund_bz05…:312-335`, `TASK_DLQ`/Ergebnis beim Deckel) werden hier grün; ihre Marker entfallen in MR 3. MR 13 schreibt sie auf `LockWaitExceeded` um.
- **`workflow_state_missing`** (WARNING) mit Feld `workflow_id`.
- **Schutz:** `fail()` markiert keinen Schritt, der schon erledigt ist (Test).
- **BZ-22:** `Terminal.release_only()` nach erfolgreichem `dead_letter`, mit Owner-Check.
- **Nachweis BZ-13, Test 1:** wird auf Grace=0 umgestellt, damit er nach MR 11 nicht vakuum-grün wird. Für Unbekannt und Lock-Deckel kommen echte Tests auf den DLQ-Zweig dazu.
- **Folgefehler:** Ist Valkey beim Fail-Mark nicht erreichbar, wird neu zugestellt. Das begrenzt das Delivery-Limit (Test).

### MR 4 – BZ-28, BZ-07

- **BZ-28:**
  - `sched_poll.lua` (KEYS und ARGV unverändert) liest seitenweise.
  - ZREM für Waisen und für Einträge, bei denen `pcall(cjson.decode)` `disabled==true` liefert.
  - Python dekodiert zeilenweise; eine kaputte Definition wird geparkt.
- **BZ-07:**
  - Fix über `take_handover_marker`: Protocol in `schedules.py:225`; die Implementierung `get_and_del_schedfire_handover_marker` (`:390`) bleibt als Alias.
  - Fester Key `sq:{sched}:handover` plus Legacy-Key, gelesen per `GETDEL`.
  - Ein 0.11-Handover wird über den zuletzt gesehenen Owner erkannt.
  - Direkt nach der Übernahme folgt ein Renew.
  - Das InMemory-Doppel wird nachgezogen.
- **Golden:** `keys.json` bekommt `sq:{sched}:handover` (additiv, Abschnitt 6).

### MR 5 – BZ-02, BZ-23, KTD11

- **Version:** `pyproject.toml` auf `0.12.0.dev0` (S-Befund aus MR 1). Sonst meldet der Arbeitsstand 0.11.0 und gilt für KTD11 selbst als veraltet. Die compat-Suite erkennt die Versionen weiter über die PID.
- **BZ-02:** neues Feld `ScheduleDefinition.task_id`. Fallback ist das Suffix von `oneshot:<uuid>`, sonst `uuid7()`. Crons bekommen je Fire eine neue ID.
- **BZ-23:**
  - Das Flag lebt `max(24 h, result_ttl)` plus `fire_ts − now` plus `max_age + 300` (falls bekannt).
  - Bei der stornierten Zustellung und beim Abbruch während des Laufs wird es per `PEXPIRE … LT` **auf höchstens 1 h verkürzt statt gelöscht** (Abschnitt 1).
- **KTD11:**
  - Die Schwelle `TIMEZONE_FLOOR` (`leader_version.py:22`) wird zu einer allgemeinen Mindestversion 0.12.
  - Der Hinweis (`:135-140`) wird versionsneutral formuliert, ebenso `metrics.py:219`, `signals.py:118-120`, `website/reference/logging.md:699` und `website/reference/configuration.md:139`.
  - Die WARNING kommt weiter einmal je Leaderwechsel (`test_leader_version.py:65`), die Gauge bleibt.
- **Golden:** Werte-Datei und Feldliste (Q3-Ausnahme).

### MR 6 – BZ-03

- **Broker:**
  - Publish-Channels mit `on_return_raises=True`.
  - `PublishError` wird **vor** `DeliveryError` gefangen und sofort zu `UnroutableMessage`.
  - Eintrag in `_ERROR_SEMANTICS`.
- **Leader, Cron:**
  - Ein unroutbarer Fire ist verloren: Commit, Zähler.
  - Das Log wird gedrosselt, einmal je Cron und 10 min.
  - Bei einem fremd gemeldeten Cron zusätzlich `scheduler_fire_unroutable_foreign`.
- **Leader, Oneshot:**
  - Commit mit Score `now + 60 s`, damit der Poll nicht verstopft.
  - Neues Definitionsfeld `unroutable_since`. Gemessen wird ab dem ersten unroutbaren Versuch, nicht ab der Fälligkeit.
  - Nach `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` (Default 86400) wird der Oneshot verworfen: Event, Metrik, Fehlerergebnis unter `task_id`, bei `cli_unique` die Unique-Freigabe.
- **Worker:**
  - Die eigene Queue wird einmal auf einem **eigenen** Wegwerf-Channel neu deklariert (MR 8), dann folgt ein zweiter Versuch.
  - Im Hot-Path wird jede Ausnahme daraus zu `BrokerConnectionError`.
- **Workflow-Advance:** `UnroutableMessage` führt sofort zu `fail_workflow_step` (MR 3), ohne Redeliver.
- **Doku und Changelog:**
  - „Worker vor Producern“ gilt **dauerhaft für jede neue Queue**. Ein Producer bekommt sonst `UnroutableMessage`, wo die Nachricht vorher still verloren ging.
  - Ein Prioritäts-Publish ohne `--priorities` wirft jetzt; die Texte in `lifecycle.py:310-330` und `parser.py:258` werden angepasst.

### MR 7 – Patch 0.11.1 (Branch `release/0.11.1` von `v0.11.0`)

- **Declare** (Obergrenze an Versuchen wie in MR 8):
  - Bei 406 auf `x-dead-letter-strategy`/`x-overflow`: vorhandene Werte übernehmen.
  - Bei 406 auf `x-delivery-limit`: vorhandenen Wert übernehmen, mit WARNING.
  - Alles andere bleibt wie in 0.11.0.
- **Namensraum:** Ist `SWARMQ_NAMESPACE` gesetzt, folgt `ConfigurationError` (Exit 78, „Namensraum erfordert ≥ 0.12“). Ein Rollback darf nicht still in den Zustand ohne Präfix fallen.
- **Release** nur mit Georgs Freigabe, und **vor** jedem Release ab MR 8.

### MR 8 – BZ-15, BZ-19

- **Eigener Deklarations-Channel:**
  - Deklarationen laufen immer auf einem **eigenen** Wegwerf-Channel, nie auf dem Publish-Channel. Ein 406 würde dort parallele Publishes mit offenen Confirms abreißen.
  - Das Ergebnis wird je Queue gecacht. Bei `UnroutableMessage` und NOT_FOUND wird der Eintrag verworfen; das Neu-Deklarieren aus MR 6 und MR 10 umgeht den Cache ausdrücklich. Test: Queue gelöscht → Unroutable → neu deklariert (Rev. 3 F5).
  - Die Unit-Tests auf `_publish_channel.declare_queue` und der Golden-Generator werden nachgezogen (Abschnitt 6).
- **Kette je Queue:**
  1. Passives Declare.
     - Existiert die Queue, folgt die Deklaration mit **Legacy-Argumenten plus bisherigem Delivery-Limit**. So entsteht für Bestandsqueues keine 406-Zeile.
     - Existiert sie nicht, wird sie mit den Zielargumenten angelegt. Bei `auto` sind das at-least-once und `reject-publish`, nicht für `.XQ`.
  2. Erkannter „inequivalent arg“:
     - `x-dead-letter-strategy`/`x-overflow`: den **vorhandenen** Wert übernehmen, ob `none` oder `at-least-once`. Ist der vorhandene Wert `at-least-once`, wird sofort der vollständige 0.12-Satz versucht (Strategy und `reject-publish` zusammen).
     - `x-delivery-limit`: den vorhandenen Wert aus dem 406-Text übernehmen (tolerant). Das passive declare-ok liefert keine Argumente; das „bisherige Limit“ in Schritt 1 ist deshalb der Konfigurationswert, der tatsächliche Wert kommt nur aus dem 406-Text (Rev. 3 F4).
     - Jedes Argument wird höchstens einmal übernommen. Obergrenze: 1 + Zahl der tolerierten Argumente = 4 Versuche, damit endet die Kette garantiert.
     - Für von 0.12 angelegte Queues kostet das je Start bis zu zwei 406 je Queue. „Keine 406-Zeile“ gilt nur für Legacy-Queues. Die Warnung bleibt auf eine je Prozess gedrosselt.
  3. Andere **erkannte** Konflikte → `QueueArgumentsMismatch`, Exit 78. Ein unerkannter oder anderer 406/403 bleibt `BrokerConnectionError` (S8).
- **Warnung:**
  - Höchstens **eine** `queue_dead_letter_at_most_once` je Prozess, mit Liste der Queues.
  - Text: „Policy `dead-letter-strategy` setzen **oder** `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=legacy`“. Eine Policy ist nicht erkennbar; nur der Abschalter bringt die Warnung zum Schweigen.
- **Effektives Delivery-Limit je Queue:**
  - Bestimmt über `delivery.queue`, nicht `ctx.queue`, damit die Prioritätsbänder stimmen.
  - Werte < 0 (−1) gelten als unbegrenzt, dann gilt die Konfiguration. **0 heißt „keine Wiederzustellung“** (S1); das Gate dead-lettert dann bei der ersten Wiederzustellung.
  - „none“ gilt ab RabbitMQ 4 als 20 (S1 belegt auf 4.2.9 und 4.3.6).
  - Eine Policy und das Argument wirken als Minimum (S1). Eine `delivery-limit`-Policy bleibt ohne Management-API unsichtbar; die Doku verbietet sie weiterhin.
  - Sonst gilt der Wert der Queue.
  - Das Gate nutzt `min(config, effektiv)`.
  - Eine `delivery-limit`-**Policy** ist ohne Management-API nicht sichtbar. Die Doku verbietet sie für swarmq-Queues.
  - Test: Queue 20, Konfiguration 50 → das Gate dead-lettert bei 19.
- **Env (Konfigurationsfeld):** `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=auto|at-least-once|legacy`.
- **Changelog:** `reject-publish` plus `max-length`-Policy bedeutet Nack statt drop-head. Der Publish scheitert nach 5 Versuchen mit `BrokerConnectionError`.
- **Rollback:** auf 0.11.1 (MR 7). Q2-Szenario „vorher deploytes Image“.

### MR 9 – BZ-09

- **Felder:** Neu ist nur `unique_for`. `unique_until` existiert bereits (`definition.py:69`).
- **Fingerabdruck:** enthält beide Werte nur, wenn `unique_for` gesetzt ist. Sonst bleibt er unverändert; ein Test pinnt den Hex-Wert.
- **Beim Feuern:**
  - Ist `definition.unique_for` gesetzt, kommen beide Werte aus der Definition, sonst beide aus der Registry.
  - Test: Registry mit `unique_until="start"`, Alt-Definition.
- **Unique-Freigabe bei Publish-Fehler:**
  - Nur wenn das SETNX **in genau diesem Fire** beansprucht wurde (`loop.py` else-Zweig).
  - Nicht für fremd beanspruchte `cli_unique`-Flags.
  - Nicht bei `PublishIndeterminate` (Q14).

### MR 10 – BZ-01

- **Ort:** Die Stufen `{q}.DQ.1s` … `.1h`, die Stufenwahl und die Selbstheilung liegen in `RabbitMQBroker.declare_queue`/`retry_later`; dort testet der Nachweis.
  - Gewählt wird die kleinste Stufe ≥ delay. Über 1 h → `{q}.DQ`.
- **Start:** Ein Fehler beim Deklarieren einer Stufe (Limit, Rechte, 406/403) ist **nicht fatal**. Es folgen der Rückfall auf `{q}.DQ`, die WARNING `delay_tier_unavailable` und eine Gauge.
- **Unroutable im Betrieb:** einmal neu deklarieren, sonst `.DQ`.
- **Tiefe:**
  - Die Familie wird über einen Channel abgefragt, der nach NOT_FOUND neu geöffnet wird. Fehlende Stufen zählen als 0.
  - Der Autoscaler zählt nur die Haupt-Queue (`ready_depth`, Abschnitt 1). `kind="pending"` bleibt Haupt-Queue + alle Delay-Queues, neu ist `kind="delayed"`.
- **Grenzen** (Abschnitt 7):
  - Über 1 h bleibt das Head-of-Line-Blocking.
  - Innerhalb einer Stufe bis zum Vierfachen der Wartezeit.
  - at-least-once-Duplikate ohne Guard.
- **Doku:** Queue-Zahlen (7 Stufen je Queue, mit Prioritäten 21), Rechte und Policy-Muster `\.DQ(\.\w+)?$`.

### MR 11 – BZ-06, BZ-11

- **BZ-06:**
  - Das Gate liefert bei der Rückstellung **`Redelivered`**, und der Aufrufer übernimmt dieses Outcome. Sonst schreibt das `finally` den Schedfire-Completed-Marker und gibt das Unique-Flag frei (`pipeline.py:551-556, 651`); die Kopie würde dann beim kundigen Worker als Duplikat verworfen.
  - Die Kopie entsteht über `republish_headers`, das Claim-Token reist mit. Zustands-Header nach Q6.
  - Nach Ablauf der Frist: `dead_letter` mit Fehlerergebnis, Workflow-Fail, Unique-Freigabe, `TASK_DLQ`, DLQ-TTL und `TASK_REJECTED`.
  - Gauge bzw. Counter für die Rückstellungen.
  - Test: unbekannter Cron-Fire → K bleibt nicht completed, das Flag bleibt gehalten.
- **BZ-11:**
  - **Neuer Key `sq:{sched}:report:known`** (Set der je gemeldeten Cron-Namen):
    - Er wird nur zusammen mit der Definition entfernt (Prune bzw. CLI).
    - 0.11 kennt ihn nicht und löscht ihn darum nicht.
    - Die bestehenden Meldungen und ihr Aufräumen bleiben **unverändert**, also kein Wachstum über Fingerabdrücke.
  - **Ein Cron gilt als verwaist**, wenn alle fünf Bedingungen erfüllt sind:
    - die Meldungen sind **in der aktuellen Amtszeit** geladen (Zeitstempel je Amtszeit);
    - der Name steht in `known`;
    - er ist nicht lebend gemeldet;
    - er ist kein eigener statischer Cron;
    - die Definition hat `source == "static"` (Rev. 3 F10). Sonst würde eine später per CLI angelegte gleichnamige Definition dauerhaft zurückgehalten, wenn ein 0.11-Leader den statischen Vorgänger ohne `known` gelöscht hat.
  - Nach dem Erwerb wartet der Poll höchstens 5 s auf das erste Adopt, danach gilt fail-open.
  - Zurückgehalten wird per Commit auf `now + seen_interval`.
  - Das Log `scheduler_cron_held_unowned` erscheint nur **beim Übergang** (gehalten bzw. wieder frei). Dazu die Gauge `swarmq_scheduler_crons_held` und das Signal `SCHEDULER_CRON_UNOWNED` beim Übergang.
- **Nachweis:** Der BZ-11-e2e-Test wird geteilt. Der Leader-Teil braucht eine „je gemeldet“-Meldung (`e2e/…:248-258`).

### MR 12 – BZ-08, BZ-12: Ausführungsidentität (Revision 2, vereinfachtes Design)

Dieses Design ersetzt das aus Revision 1 (Belege, Basis-Header, Vorrangregeln). Dort haben die Prüfer Verlust- und Doppelpfade gefunden (A1–A3, U17).

- **Identität:**
  - Sie besteht aus `task_id`, `x-swarmq-replay-id` (H) und **n**.
  - **n = `x-swarmq-xdeath-base` + Summe der `count`-Werte aller `x-death`-Einträge, deren Queue keine Delay-Queue ist (`is_delay_queue`)** (Rev. 3 F1).
  - **Jede** Kopie, die swarmq selbst publiziert (`republish_headers`, DLQ-Stempel, CLI-Weiterleitung), setzt `x-swarmq-xdeath-base = n` und **entfernt `x-death`**. Damit stimmt n, egal ob der Broker client-gesetztes `x-death` erhält, entfernt oder beim nächsten Dead-Lettern neu aufbaut. Die Gabel nach S5 entfällt. Kein Code liest heute `x-death` (nur ein Kommentar in `admin.py:56`), und 0.11 ignoriert es.
  - Restlücke: Ein UI-Move setzt keine Base. Die erste Ausführung danach zählt das rohe `x-death`, wie heute.
  - Format: Bei n = 0 wie heute, `task_id` bzw. `task_id|H`, also byte-gleich zu 0.11. Bei n > 0 kommt `#n` dazu.
  - Die Länge ist gedeckelt.
- **Neue Generation beim Dead-Lettern durch den Worker:**
  - An **einer** Stelle: `_publish_to_dead_letter_queue` in der Broker-Schicht (`delivery.py:176-181`). Damit sind alle Pfade erfasst (Expiry, DLX-Fallback, Retry-DLQ).
  - Gesetzt werden `x-swarmq-replay-id = "g" + sha1(aktuelle Identität)[:39]` und `x-swarmq-dlq-gen = <Wert von x-swarmq-dead-lettered-at>`. Beide entstehen im selben Aufruf (Rev. 3 F2).
  - Deterministisch: Zwei DLQ-Kopien derselben Ausführung, etwa nach `PublishIndeterminate`, tragen dieselbe ID. Jede neue Ausführungsgeneration bekommt eine neue ID.
- **Broker-DLX** (`route_via_dlx`, Delivery-Limit, Ablauf in der Haupt-Queue):
  - Die Header bleiben, `x-death` wächst. Ein Replay hat daher n+1 und eine neue Identität.
  - Eine Neuzustellung des Originals hat n und bleibt geschützt.
  - at-least-once-Duplikate tragen dieselben Header und damit dieselbe Identität.
  - S5 (MR 1) hat belegt: Ein Broker-DLX baut `x-death` **neu** auf, client-gesetzte Einträge verschwinden. Ein Client-Republish lässt es unverändert. Ein wiederholter reiner Broker-DLX erhöht nur `count` (S7). Die Base-Variante ist damit notwendig und korrekt; ein Integrationstest prüft die Kette Kopie → `.DQ` → Konsum.
- **CLI `dlq retry`:**
  - Ist `x-swarmq-dlq-gen` **gleich** `x-swarmq-dead-lettered-at` (frisch von einem 0.12-Worker dead-gelettert), **bleibt `x-swarmq-replay-id` unverändert**. Hat ein 0.11-Worker die Nachricht erneut dead-gelettert, stempelt er `dead-lettered-at` neu; die Gleichheit bricht, und die CLI vergibt eine neue `uuid4`. Sonst würde die CLI eine schon `done` gesetzte ID behalten (Rev. 3 F2).
    - Ein wiederholter `dlq retry` derselben XQ-Nachricht ergibt dieselbe Identität; der zweite Lauf wird verworfen. UI-Move verhält sich genauso.
  - Fehlt der Header (Bestand, 0.11-Worker), vergibt die CLI wie heute eine neue `uuid4`. Damit bleibt `test_rabbitmq_dlq.py:258-282` gültig, auch mit `headers={}`.
  - Die CLI entfernt zusätzlich die Zustands-Header (Q6) und setzt `x-swarmq-enqueued-at = now`. Sonst läuft ein Replay mit `max_age` sofort wieder ab (A6). Für UI-Replays wird diese Grenze dokumentiert.
- **Done-TTL für Replay-Identitäten** (H gesetzt oder n > 0): `max(execution_guard_done_ttl_s, dead_letter_ttl_s)`, bei `dead_letter_ttl_s = None` stattdessen `max(done_ttl, execution_guard_backlog_ttl_s)` (Rev. 3 F3). Sonst würde ein erneuter Retry am nächsten Tag doppelt laufen (A4). Die TTL reist je Lease mit, denn `_state_ttl_ms` ist heute identitätsunabhängig (`execution_guard.py:241-244`). Das Wachstum ist durch die Zahl der Replays begrenzt (Q5).
- **Cron-Replay:** Bei einer Replay-Identität nutzt das Schedfire-Gate den Key `K|<Identität ohne task_id>`. Normale Doppel-Fires (n = 0, kein H) behalten K.
- **BZ-12, Done-Marker:**
  - Wird zurückgestellt, weil die Ausführung fremd läuft, verlängert der **Claim-Pfad** in `guard_claim.lua` (ARGV[6]) den State auf `max(SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S=7 d, max_age+300)`.
  - `guard_finish.lua` verkürzt nie: Es vergleicht die PTTL im Skript, denn heute setzt es `PEXPIRE` unbedingt (`guard_finish.lua:23-31`). ARGV bleibt unverändert.
  - Ein gescheiterter Ack und `SettleFailedAfterPublish` beenden mit `extended`.
  - Das InMemory-Doppel bildet das nach.
- **BZ-12, Fired-Marker:** TTL = `max(base, 2·wait + 300)`, gedeckelt bei `max(base, max_age + 300)` und 7 d, nie unter `scheduler_dedup_ttl`.
- **Tests:**
  - Tabellentest über diese Sequenzen:
    - Broker-DLX → UI → Exception → Worker-DLQ → CLI (A1)
    - Worker-DLQ → UI → `route_via_dlx` → Replay (A2)
    - 0.11-DLQ ohne Generation, zweimal CLI (A3)
    - `PublishIndeterminate`-Doppelkopie
    - `dlq list` dazwischen
    - at-least-once-Duplikat
    - Retry über jede Stufe
    - 0.11-replay-id in-flight
  - Der Nachweis BZ-08 wird umgeschrieben: Die DLQ-Kopie trägt jetzt eine Generations-ID. Geprüft wird, dass ein Replay ohne ID-Änderung läuft, statt `"x-swarmq-replay-id" not in headers`.
- **Mischbetrieb** (Abschnitt 7):
  - 0.11-Worker sehen nur `task|H` und ignorieren n.
  - Ein Broker-DLX-Replay an einen 0.11-Worker wird deshalb wie heute verworfen. Ebenso ein Cron-Replay: Das Schedfire-Gate von 0.11 verwirft ihn (Verlust, keine Doppelausführung).
  - Ein Replay in-flight beim Versionswechsel kann doppelt laufen (n = 0 gegenüber n > 0). Die Generations-ID ist für beide Versionen gleich.

### MR 13 – BZ-25, BZ-04 (Laufzeit), BZ-05

- **BZ-25** (Revision 2, wegen R4 und dem Takt des Nachweises):
  - **Verlängerung:** Der Keeper verlängert **nicht** auf die volle TTL. Bis zur ursprünglichen Frist `t0 + TTL` bleibt alles wie heute. Erst wenn ein Lock in weniger als einem Fenster abläuft und der Handler noch lebt, verlängert er in kurzen Schritten (Fenster = 3 × Takt, Takt `min(max(ttl/3, 50 ms), 20 s)`).
    - Folge: Ein per SIGKILL beendeter Halter hinterlässt den Lock höchstens bis `t0 + TTL` wie heute, bei Überziehung höchstens ein Fenster länger.
  - **Lua:** `lock_extend.lua` ergänzt `lock_renew.lua`, der Leader bleibt unverändert. Mutex: `PEXPIRE`. Semaphor: Score anheben **und** `PEXPIRE` auf den ZSET. Beides verkürzt nie. Der Score kommt aus Server-`TIME`, wie in `locks.py:56-57`.
  - **Gemischte TTLs auf einem Semaphor (Rev. 3 F6):** `lock_acquire.lua` setzt `PEXPIRE` künftig nur noch, wenn der neue Wert größer als die aktuelle PTTL ist (ARGV unverändert, der Inhalt des Skripts ändert sich). Sonst verkürzt ein Erwerber mit kurzer TTL den Key unter einem Langläufer weg. Ein Erwerber mit kurzer TTL räumt Member weiterhin nach seiner eigenen TTL ab. Deshalb gelten gemischte `hard_timeout` unter einem Semaphor-Namen als Grenze (Abschnitt 7), ebenso 0.11-Erwerber, die weiter verkürzen. Test: A (TTL 1 h) und B (TTL 60 s) auf einem Semaphor; nach 70 s ist A noch Halter.
  - **Ende des Keepers:** Eine Lease fällt beim **Freigabeversuch** aus dem Keeper, auch wenn der Versuch scheitert. Der Keeper läuft bis zur letzten Lease oder bis das Backend schließt, nicht über `shutdown_event`.
  - `compute_lock_ttl_ms` wird über `worker.gates.lock` gelesen (Patch-Pfad des Nachweises).
  - **Events:**
    - `task_lock_lost` mit Feld `lock_name` und `event=lost_while_running|lost_at_release`; geprüft wird mit `is False`.
    - `task_lock_held_past_hard_timeout`.
- **BZ-05:**
  - **Fingerprint:** nur die Menge der Owner, ohne Score und PTTL. Sonst sähe jede Erneuerung wie ein Halterwechsel aus.
  - **Stillstand:** beobachtete PTTL des Keys + 60 s, nicht die eigene TTL. PTTL −2 (Key weg) heißt: kein Halter, also kein Stillstand. PTTL −1 (ohne TTL, Fremdzustand) heißt: die eigene Lock-TTL ist die Basis.
  - **Obergrenze:** `SWARMQ_LOCK_WAIT_MAX_S` (Konfigurationsfeld, Default 21600) ab dem ersten Konflikt.
  - **Rückfall** nach Q15 (Fingerprint nicht messbar): zurückstellen bis zur Obergrenze, **kein** Deckel 10.
  - **Legacy-Zähler:** wird nicht mehr erhöht. Der neue Header `x-swarmq-lock-conflicts` speist `lock_conflict_count` (Abschnitt 1).
  - **Log:** `task_locked` nur beim ersten Konflikt und danach einmal je Minute. Das Signal `TASK_LOCKED` bleibt (API).
  - **Gauges:** wartende Konflikte und ältester Wartender je Worker.
  - **Nachweise:** BZ-05 Tests 2/3 und BZ-13 :143 werden auf `LockWaitExceeded` bzw. Stillstand umgeschrieben (Abschnitt 6).
- **Uhren:** `holder-since`, `wait-since` und `unknown-since` stammen von Worker-Wanduhren. Die Toleranz von ±60 s ist dokumentiert, der Maßstab sind Minuten.

### MR 14 – BZ-04 Valkey-Umgebung

- **`ValkeyBackend.connect()`:**
  - Prüft die Policy und **warnt** nur; daran hängt der Nachweis.
  - Quellen: `CONFIG GET`, sonst `INFO memory` (auch `maxmemory`). Ein unbekanntes `maxmemory` gilt als unsicher.
  - Zeitlimit 2 s, die Prüfung wirft nie.
  - Das Ergebnis liegt am Backend.
- **Worker:** `strict` und unsichere Policy → Exit 78. Geprüft wird vor `_wire_coordination`, über eine Backend-Methode (Q16).
- **Leader, stündlich:**
  - Erneute Prüfung, bei Befund WARNING.
  - Gauge `used_memory/maxmemory`. Bei `noeviction` hält ein OOM alle Apps an.
- **Epoch:**
  - Geprüft wird in `apply_static_definition`, im Zweig „kein Eintrag“ (`sync.py:182`).
  - Grenze: Ein RDB-Snapshot, der nach einem Valkey-Neustart geladen wird, bringt den Epoch mit zurück. Ein Verlust seit dem Snapshot bleibt dann unerkannt (Abschnitt 7).
- **`swarmq check`:**
  - Eigenes Modul `cli/env_check_cmd.py`. Ein Test sichert ab, dass `swarmq check` **nicht** als `worker --check` erkannt wird.
  - Abschnitte:
    - `valkey`: Policy, `maxmemory`, Speicher, AOF, Version, Cluster, Namensraum, Keys ohne Präfix.
    - `shutdown`.
    - `queues`: ab MR 18, einschließlich fehlender `.XQ`.
  - `cli.json` wird neu erzeugt.
- **Marker:**
  - In MR 14: `test_befund_bz04_eviction_policy.py` (2 Tests) und `test_befund_bz04_disabled_cron_data_loss.py`.
  - In MR 13: `test_befund_bz04_lock_loss.py`.
- **Doku:**
  - Vor dem Upgrade `swarmq check` in der 0.12-Version (eigenes venv, nur lesend) gegen Produktion laufen lassen, in K8s optional als Init-Container.
  - Exit 78 führt unter K8s zu CrashLoopBackOff, unter Docker mit `restart:` zu einer Neustartschleife.

### MR 15 – BZ-10

- **Namensraum:**
  - Konfigurationsfeld `namespace`.
  - `ValkeyConnection(namespace=…)` als Parameter. Ohne ausdrückliche Angabe gilt der Wert aus der Konfiguration; die Env-Variable wird über `config.py` gelesen (Q11).
  - Präfix `<ns>:sq:…` für Keys und Kanäle. Mit Validierung; die Hash-Tags bleiben unverändert.
- **Rücktritt** (`scheduler/stepdown.py`):
  - Gezählt wird `BrokerConnectionError` **ohne** `UnroutableMessage`, `MessageHeadersTooLarge` und `BrokerPublishBlocked`. Ein Broker-Alarm trifft alle Kandidaten, ein Rücktritt würde nur rotieren.
  - Rücktritt bei mindestens 2 Fehlern und mindestens `scheduler_publish_stepdown_s` (Konfigurationsfeld, Default `scheduler_lock_ttl`, 0 schaltet ab).
  - Danach Abkühlzeit 2 × Lock-TTL, ohne Handover-Marker.
- **Doku:**
  - Erst alle Komponenten auf 0.12, dann den Namensraum überall gleichzeitig setzen. Danach ist ein Rollback nur noch auf ≥ 0.12 möglich (0.11.1 verweigert, MR 7).
  - Bei getrennten vhosts ist der Namensraum Pflicht.
  - `SCHEDULER_PUBLISH_STEPDOWN_S=0` empfehlen, solange noch Apps < 0.12 auf dem Valkey laufen. Sonst wandert die Führung zu einer 0.11-App, und BZ-02/09/11 sind still aus.
- **Nachweise:** Test 4 braucht einen umgebauten Helfer `_run_two_apps` (`:267`, Warten ohne `suppress`). Tests 1, 2 und 4 bekommen einen Namensraum.

### MR 16 – BZ-16, BZ-21

- **BZ-16:**
  - Zweite Verbindung, lazy beim ersten `consume()`. Konfigurationsfeld `broker_publish_connection=separate|shared`.
  - Bei injizierter `connection=`/`connection_pool=` gilt immer `shared`, weil keine URL vorliegt.
  - Admin-Lesen läuft über die Consume-Verbindung, **falls sie offen ist**, sonst wie heute.
  - **Blocked-Erkennung:**
    - Synchron über einen Adapter (S6). Ist der Zustand nicht lesbar, gilt fail-open mit einer WARNING.
    - **Kein** `wait_for(…, 0)`.
    - Pflichttest: Eine unblockierte Verbindung liefert nie `BrokerPublishBlocked` und kostet keinen Roundtrip.
- **BZ-21:**
  - Lesen über einen eigenen Channel mit `aio_pika.queue.Queue.get` (diesen Aufruf patcht der Nachweis) und `no_ack=False`.
  - **Abhängig von S4:**
    - Zählt die Rückgabe den Delivery-Count der XQ hoch, liest `dlq list` per **Kopie ans Ende + Ack**: Confirm zuerst, die KTD6-Rest-TTL bleibt, und die Identität aus MR 12 dedupliziert.
    - Sonst halten und zurückgeben.
  - Test: 25 Listings, danach ist die Nachricht noch da.

### MR 17 – BZ-18, BZ-27

- **BZ-18:**
  - `shutdown_budget(total, supervised)`. Die Eskalation endet bei `total − 2 s`.
  - Auch eingebettet gilt: Drain ≤ total − Aufräumreserve. Das verlangt der Nachweis.
  - `ProcessManager.shutdown_grace_s` bedeutet das Gesamtbudget.
  - Der Recycling-Drain ist ausgenommen (Test).
  - Jeder Aufräumschritt hat ein Zeitlimit.
- **BZ-27:**
  - `SlotTable.lock(slot, until=math.inf)`; der Default bleibt permanent.
  - `unlock_expired(now)` mit übergebener Uhr.
  - Abkühlzeit 3600 s, ein Event und eine Gauge über `SupervisorChannel.locked_slots`.
- **e2e:** Tests mit `shutdown_timeout=5` laufen gezielt mit; Umgang laut Abschnitt 6.

### MR 18 – BZ-14, BZ-20

- **BZ-14:**
  - Primäres Kriterium: `consumer_count == 0` und `message_count > 0` aus dem passiven declare-ok (AMQP).
  - Kandidaten stammen aus `sq:{sched}:report:queues`, mit Zeitstempeln aus Valkey-`TIME`.
  - Einträge, die NOT_FOUND oder leer sind und älter als 7 d, werden per HDEL entfernt.
  - Event nur beim Übergang, dazu eine Gauge.
  - Der Nachweis wird umgeschrieben: Vorzustand ist eine gemeldete Queue, das Budget ist angepasst (`:40`).
- **BZ-20:**
  - Start-WARNING nur bei `hard_timeout × K > consumer_timeout_s`, einmal je Start.
  - Gauge und Doku.
  - Der Nachweis bleibt xfail, Begründung „Grenze“.

### MR 19 – Doku-Konsolidierung, RC

- **Deploy-Leitfaden** `website/guides/deployment.md` und `.de.md` (neu, mit Navigation):
  0. `swarmq check` ausführen.
  1. Alle Apps auf 0.11.1.
  2. Worker vor Producern, dauerhaft für neue Queues.
  3. Grace-Werte für K8s und Docker.
  4. vhost bzw. Namensraum.
  5. Rechte, Policies, Queue-Zahlen.
  6. Rollback-Regeln.
- **Doku-Befunde** D1–D15 aus dem Umgebungsentwurf.
- **Changelog** aus den Einträgen der MRs konsolidiert, mit den Abschnitten „Upgrade-Hinweise“, „Wirkt erst, wenn …“ und „Bekannte Grenzen“.
- **RC `0.12.0rc1`** (mit Freigabe):
  - Loadsuite 6 h: Durchsatz und Doppelausführungen, Szenario „Semaphor-Überlast“.
  - **Staging über 8 Tage** mit der Keyzahl je KeySpace-Familie. Nur so werden die Fristen von 24 h und 7 d beobachtbar.

---

## 6. Bestehende Tests und Golden-Dateien mit geänderter Erwartung (abschließend)

| MR | Test/Datei | Grund |
|---|---|---|
| 0 | `tests/unit/test_architecture_guards.py` (`_SPLIT_MODULES`), `tests/unit/test_cli_module_surface.py:184` | neue Untermodule |
| 0 | `golden/keys.json` (+`sq:control:*`, additiv) | Lücke im Golden |
| 2 | `tests/integration/broker/test_befund_bz17_traceback_header.py:117-122` | Tiefe der Haupt-Queue |
| 2,6,8,13,16 | `website/reference/agent/spec/*` (geprüft von `test_agent_spec.py:440-452`) | neue Fehlerklassen, Konfigurationsfelder, CLI |
| 3 | `tests/unit/worker/gates/test_lock.py:491-660`, `test_lock_dlq_ack_failed.py`, `test_expiry.py:116,153,169`, `test_pipeline.py:254`, `tests/integration/backend/test_expiry_result_store.py:93` | Ergebnis, Workflow-Fail und Freigabe in terminalen Pfaden |
| 3 | `tests/unit/worker/test_befund_bz13_workflow_hang.py` Test 1 (Grace=0) | sonst ab MR 11 vakuum-grün |
| 4 | `tests/unit/backend/test_valkey_schedules.py:243-251`, `golden/keys.json` (+handover), `tests/unit/test_keyspace.py` (`_TABLE` +1 Zeile handover; Nachtrag, von Georg am 2026-10-10 freigegeben) | doppelter Handover-Marker |
| 5 | `tests/unit/test_app.py:729-747`, Fixture `tests/unit/client/test_results.py:213-234`, Cancel-Tests, die das Löschen des Flags erwarten (grep `clear_cancel_flag` in `tests/`, auch zu `worker/results.py:129,275` und `pipeline.py:589`) | Cancel-TTL; Flag per `PEXPIRE LT` 1 h statt Löschen |
| 5 | `tests/unit/scheduler/test_leader_version.py:38,44,50-51,55,72,75-79,90`, `test_loop.py:348,353-369,375-394`, `tests/integration/test_scheduler_leader_election.py:287-310` (prüfen) | KTD11 ≥ 0.12, Hinweistext |
| 5,6,9 | `golden/values/schedule_definition.json`, Feldliste `test_golden_fixtures.py:49-56` | neue Felder (Q3-Ausnahme) |
| 8 | `tests/unit/broker/test_rabbitmq.py:306-395` (Deklaration auf `_publish_channel`, `len==3` :312), `tests/unit/broker/test_befund_bz19_dead_letter_strategy.py:312-319`, Generator `_golden.py:193-206` | eigener Deklarations-Channel, Kette |
| 10 | `test_rabbitmq.py:323-325,400-440,735-750`, `tests/unit/worker/test_worker.py:815`, `_golden.py:205`, `golden/queue_args.json` (+Stufen, getrennt); `tests/unit/process_manager/test_sampler.py` bleibt grün über den Q15-Rückfall auf `queue_depth` | Stufen, `ready_depth` |
| 11 | `test_worker.py:309,2659`, `test_pipeline.py:346,357`, `tests/e2e/test_signal_catalog.py:1110`, `tests/unit/worker/test_lifecycle_events.py:167-171`, `golden/headers.txt` | Gnadenfrist |
| 12 | `tests/unit/backend/test_valkey_execution_guard.py` (ARGV), Contract-Fälle, `golden/lua_contract.txt` (guard_claim), `golden/headers.txt`, InMemory-Broker `tests/support/inmemory/broker.py:39`, `tests/contracts/publisher_consumer.py` | Identität, Generations-ID |
| 13 | `test_lock.py:143,287,304-309,312-420,664-700,817-825`, `test_lock_dlq_ack_failed.py:77-90`, `test_worker.py:2719`, `test_pipeline.py:237-250`, `tests/e2e/test_signal_catalog.py:687` (prüfen: kwarg bleibt ≥ 1 über den neuen Header), `golden/lua_contract.txt` (+lock_extend; Inhalt lock_acquire), `golden/headers.txt` | Deckel 10 entfällt, neuer Zähler-Header, PEXPIRE nur verlängernd |
| 13 | Nachweise `test_befund_bz05_lock_conflict_cap.py:287-335`, `test_befund_bz13…:140-146` | auf `LockWaitExceeded`/Stillstand umschreiben |
| 16 | `test_rabbitmq.py:41-52,446-532,637-656,786-919`, `tests/unit/broker/test_dead_letter_ttl_header.py`, `tests/unit/broker/test_shutdown_deadline.py:84`, `tests/unit/worker/test_lifecycle.py:183-218`, `tests/unit/broker/test_consumer_watch.py:431`, `tests/integration/broker/test_rabbitmq_dlq.py` | zweite Verbindung, DLQ-Lesen |
| 17 | `test_supervisor.py:519-557,1296,1368,1552ff,1607ff`, `test_run.py:466`, `test_setpoint.py:415-423`; e2e mit `shutdown_timeout=5` nur, falls sie brechen (T anheben, wenn nur das Budget geprüft wird; Drain-Semantik ist ein Warnzeichen) | Budget, Abkühlzeit |

| 3 | Tests, die `DeliveryCountGate` nur mit `x-delivery-count` prüfen, bleiben grün; `redeliver`-Tests mit Zeitannahmen (grep `redeliver` in `tests/unit/broker`, `tests/unit/worker`) bekommen eine injizierte Wartefunktion | BZ-29 |

**Nachweise, die vor dem Fix umgeschrieben werden:**
- BZ-08: Generations-ID
- BZ-10: Tests 1, 2 und 4
- BZ-11 e2e: geteilt, „je gemeldet“
- BZ-12: Schedfire-Test
- BZ-13: Test 1
- BZ-14: Vorzustand, Budget
- BZ-17
- BZ-20: Begründung
- BZ-05 Tests 2/3 und BZ-13 :143 (MR 13)

Der BZ-16-Nachweis überspringt sich ohne lokalen Broker-Alarm. Abgenommen ist er erst mit dem lokalen Lauf über den Wrapper, ein Skip zählt nicht.

---

## 7. Bekannte Grenzen nach 0.12.0 (Changelog)

- **BZ-02/09/11:** wirken erst mit einem Leader ≥ 0.12. Rotiert die Führung zu einer 0.11-App, sind sie zeitweise aus.
- **BZ-06/12:** wirken voll erst, wenn alle Worker ≥ 0.12 sind.
- **„Worker vor Producern“:** gilt dauerhaft für neue Queues (BZ-03) und beim Rollout 0.11 → 0.12 auch für neue Tasks (BZ-06).
- **BZ-08 im Mischbetrieb:**
  - Ein Broker-DLX-Replay an einen 0.11-Worker wird verworfen. Ein Cron-Replay an einen 0.11-Worker ebenso (Verlust).
  - Ein Replay in-flight beim Versionswechsel kann doppelt laufen.
  - Bestandsnachrichten in der DLQ ohne Generation erhalten bei jedem CLI-Retry eine neue ID. Ein mehrfacher Retry ist dort weiter doppelt möglich.
  - UI-Replays mit `max_age` können sofort ablaufen.
- **BZ-12:** Ein Rückstand, der erst nach dem Lauf entsteht, ist bei Tasks ohne `max_age` nicht abgedeckt.
- **BZ-01:**
  - Über 1 h bleibt Head-of-Line-Blocking.
  - Innerhalb einer Stufe kann eine Wartezeit bis zum Vierfachen dauern.
- **BZ-19:**
  - Duplikate laufen doppelt, wenn kein Ausführungsschutz greift (FIRE_AND_FORGET, `execution_guard=False`).
  - Rollback nur auf ≥ 0.11.1.
  - `reject-publish` zusammen mit einer `max-length`-Policy führt zu Publish-Fehlern.
- **BZ-10:**
  - Bei getrennten vhosts ist der Namensraum Pflicht.
  - Nach dem Setzen des Namensraums gibt es keinen Rollback unter 0.12.
- **BZ-20:** Prefetch gilt pro Consumer.
- **BZ-21:** Parallele `dlq list` sehen jeweils Teilmengen.
- **BZ-23:** Ein sofort publizierter Task ist 24 h stornierbar.
- **BZ-04:**
  - Ein Flush bei gleichzeitigem Neustart aller Prozesse bleibt unerkannt.
  - Ein Verlust seit dem RDB-Snapshot bleibt unerkannt.
  - Die Policy wird nur stündlich geprüft.
- **KTD11:** Die Gauge steht dauerhaft, solange eine 0.11-App auf demselben Valkey läuft.
- **BZ-25/05:** Gemischte `hard_timeout` unter einem Semaphor-Namen: Ein Erwerber mit kurzer TTL räumt Member nach seiner eigenen TTL ab. 0.11-Erwerber verkürzen den Key weiter.
- **BZ-08:** Nach einem UI-Move zählt die erste Ausführung das rohe `x-death` (keine Base).
- **BZ-12/08:** Mit `dead_letter_ttl_s = none` sind Replay-Identitäten 7 d geschützt, nicht unbegrenzt.
- **Delivery-Limit per Policy:** nicht erkennbar und für swarmq-Queues nicht unterstützt.

## 8. Folgearbeit

- swarmv: Namensraum, `check`, Pin.
- Native Quorum-Prioritäten (BZ-20).
- P3-Befunde.
- SCAN in `worker_versions()`.

---

## 9. Einarbeitung der Prüfungen

Gegenlesen 1 (B1–B20) ist eingearbeitet, siehe die einzelnen MRs. B4 (Replay-Identität) ersetzt Revision 2 durch das vereinfachte Design in MR 12.

**Belastungstest, Prüfer Korrektheit (A1–C2):**

| Befund | Einarbeitung |
|---|---|
| A1 Vorrangregel verliert CLI-Replay | MR 12: neues Design ohne Basis und Vorrang |
| A2 veraltetes `-by` verdeckt jüngeres x-death | MR 12: n aus x-death plus Generations-ID |
| A3 Generationen fallen zusammen; `-by` am falschen Ort | MR 12: Generation aus der aktuellen Identität, Stempel in `_publish_to_dead_letter_queue` |
| A4 Done-Marker kürzer als XQ | MR 12: Done-TTL für Replay-Identitäten ≥ `dead_letter_ttl_s` |
| A5 Verlust im Mischbetrieb | Abschnitt 7, Q2 |
| A6 Replay mit `max_age` läuft sofort ab | MR 12: CLI setzt `enqueued-at` |
| B1 unroutbare Oneshots verstopfen den Poll | MR 6: Commit mit +60 s, `unroutable_since` |
| B2 Rückstellung unbekannter Tasks wird terminal | MR 11: `Redelivered` |
| B3 effektives Delivery-Limit unvollständig | MR 8: ≤ 0, none = 20, Policy-Doku, `delivery.queue` |
| B4 Deklaration auf dem Publish-Channel | MR 8: eigener Channel plus Cache |
| B5 0.11-Leader löscht Meldungen | MR 11: `report:known` |
| B6 Zustands-Header kleben an Kopien | Q6, MR 11/12/13 |
| B7 Unique-Freigabe zu breit | MR 9: nur bei eigenem Claim |
| C1 Keeper, Fingerprint, TTL-Basis | MR 13 |
| C2 Kleinpunkte | MR 8 (Changelog), MR 10/Abschnitt 7, MR 12 (Doppelkopie deterministisch), MR 5 (Cancel-Flag 1 h) |

**Belastungstest, Prüfer Betrieb (R1–R21):**

| Befund | Einarbeitung |
|---|---|
| R1 Rollback auf das vorher deployte Image | Abschnitt 1 (0.11.1 zuerst ausrollen), Q18-Gate, Q2 |
| R2 Rollback mit Namensraum | MR 7 (0.11.1 verweigert), MR 15 Doku |
| R3 `dlq list` über den Delivery-Count | S4, MR 16 (Kopie ans Ende), Test mit 25 Listings |
| R4 Keeper verlängert verwaiste Locks | MR 13: Verlängerung erst nahe `t0 + TTL` |
| R5 Exit 78 unter K8s/Docker | MR 14 Doku, `check` vorab |
| R6 Producer-Ausnahme dauerhaft; Zwischenstand beim Advance | MR 6 Doku; Reihenfolge (Terminal vor Unroutable) |
| R7 Konfliktzähler im Mischbetrieb | Abschnitt 1, MR 13 |
| R8 Alarmmüdigkeit | MR 8 (passiv, eine Warnung), MR 11 (Übergang), MR 2 (BZ-24), MR 18 (BZ-20), MR 6 (Drosselung) |
| R9 Falschmeldungen verwaister Queues | MR 18: `consumer_count`, TIME, HDEL |
| R10 Autoscaling durch lange Rückstellungen | Abschnitt 1, MR 10 |
| R11 Last durch Konflikte | MR 13: Drosselung, Gauges, Loadsuite-Szenario |
| R12 Stufen blockieren den Start | MR 10: nicht fatal |
| R13 406 auf dem Publish-Channel | MR 8 |
| R14 Rücktritt im Mischbetrieb und bei Alarm | MR 15 |
| R15 fail-open nach Leaderwechsel | MR 11: Amtszeit-Stempel, 5 s Warten |
| R16 Wachstum über Fingerabdrücke | MR 11: `report:known` statt Aufbewahrung |
| R17 Zwischenstände ohne Doku | Q18 |
| R18 Abnahme zu kurz | MR 19: Staging 8 d, Q2 Mehr-App |
| R19 Wanduhr | MR 13 Doku |
| R20 fehlende Signale | MR 14 (Speicher, stündlich), MR 18 (fehlende XQ), MR 11/13 (Gauges), Abschnitt 7 (RDB) |
| R21 Eskalation bis an SIGKILL | MR 17: total − 2 s |

**Belastungstest, Prüfer Umsetzung (U1–U24):**

| Befund | Einarbeitung |
|---|---|
| U1/U2 Mischbetrieb-Suite | Q2 |
| U3/U4 Spec, Env-Felder | Q11, MR 15/16 |
| U5/U6 Patch-Pfade, Guards, Zyklen | Q17, 3.1, Abschnitt 6 |
| U7 Reinheit | Q16, `swarmq/keyspace.py`, `swarmq/queue_names.py` |
| U8 BZ-26 in der falschen Funktion | MR 2 |
| U9 BZ-17-Nachweis | MR 2, Abschnitt 6 |
| U10 Abschnitt 6 zu MR 8, Gegenfall at-least-once | MR 8, Abschnitt 6 |
| U11 Golden über mehrere MRs | Q3, Abschnitt 6 |
| U12 KTD11-Tests | MR 5, Abschnitt 6 |
| U13 Lock-Deckel vs. Q10 | MR 3 (Q10-Ausnahme), MR 13 |
| U14 Keeper-Takt | MR 13 |
| U15 Ort der Eviction-Prüfung, Epoch | MR 14 |
| U16 Kollision von `check` | MR 14 |
| U17 Ort des DLQ-Stempels | MR 12 |
| U18 replay-id-Tests | MR 12, Abschnitt 6 |
| U19 Tests zu MR 16, Producer, Injektion | MR 16, Abschnitt 6 |
| U20 `SlotTable`, Budget eingebettet | MR 17, Abschnitt 6 |
| U21 Nachweise mit Umbaubedarf | Abschnitt 6 |
| U22 `pipeline.py` > 800 | 3.1 Punkt 5 |
| U23 4.2.9-Broker, weitere Spikes | 3.2, S8/S9, erweiterte S1/S2/S4 |
| U24 Referenzen | in den MRs korrigiert |

**Gezielte Prüfung Revision 2 (F1–F11):**

| Befund | Einarbeitung |
|---|---|
| F1 n sinkt, wenn der Broker client-`x-death` neu aufbaut | MR 12: immer Base-Header, Kopien entfernen `x-death`; S5 erweitert |
| F2 `dlq-gen` klebt nach 0.11-Dead-Letter | MR 12: gen = `dead-lettered-at`, CLI prüft Gleichheit |
| F3 Done-TTL bei `dead_letter_ttl_s=None`; Lua verkürzt | MR 12: Backlog-TTL als Ersatz, TTL je Lease, PTTL-Vergleich in `guard_finish.lua` |
| F4 Delivery-Limit nicht beschaffbar, 3 Versuche zu wenig | MR 8/7: Wert aus dem 406-Text, Obergrenze 4, Text korrigiert |
| F5 Deklarations-Cache ohne Invalidierung | MR 8: verwerfen bei Unroutable/NOT_FOUND, Test |
| F6 gemischte TTLs auf einem Semaphor | MR 13: `lock_acquire` verlängert nur, Score aus `TIME`, PTTL −1/−2, Abschnitt 7 |
| F7 Gate-Kopien setzen Fristen zurück | Q6: Entfernen nur bei Retry, DLQ und CLI |
| F8 Konfliktzähler: Signal und Tests | Abschnitt 1: neuer Header `x-swarmq-lock-conflicts`; Abschnitt 6 |
| F9 Autoscaling-Ziel verfehlt | Abschnitt 1, MR 10: nur Haupt-Queue über `ready_depth` |
| F10 `known` hält umgewidmete Namen | MR 11: Bedingung `source == "static"` |
| F11 Cancel-Flag verlängert; Abbruch während des Laufs | Abschnitt 1, MR 5: `PEXPIRE LT`, alle Löschstellen, Changelog |

**Spike-Ergebnisse aus MR 1 (Revision 4):** S1: `x-delivery-limit=0` heißt sofort verwerfen, −1 unbegrenzt, Default 20, Policy und Argument wirken als Minimum → MR 8 korrigiert. S3: globales QoS kostet auf 4.2 die Verbindung und wirkt auf 4.3 nicht → BZ-20 bestätigt. S4: `nack(requeue)` zählt auf 4.3 nicht → neuer Befund BZ-29 in MR 3. S5/S7: Ein Broker-DLX baut `x-death` neu auf → Base-Variante in MR 12 bestätigt. S6: Der Blocked-Zustand ist nur privat lesbar (`_Connection__connection_unblocked`), der Pfad muss je Lesen neu gegangen werden → Adapter mit fail-open (MR 16). S2/S8/S9 bestätigen die Annahmen. Shovel ist auf den Testbrokern nicht prüfbar.
