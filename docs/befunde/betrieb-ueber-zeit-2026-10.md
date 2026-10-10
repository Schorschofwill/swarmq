<!-- Befundregister. IDs BZ-nn sind die Ticketnummern fuer xfail-Marker (AGENTS.md: kein Skip ohne Ticket). -->
# Befundregister „Betrieb über Zeit“ – swarmq v0.11.0 (fb9e067), 2026-10-09

Status: alle 28 Befunde durch Tests bestätigt (2026-10-09), offen. BZ-03 präzisiert: stiller Verlust mit Erfolgsquittung, kein 120-s-Hänger. BZ-19: Verlust nur im Cluster beobachtbar, lokal belegt ist das fehlende `x-dead-letter-strategy`.

Nachweis-Tests tragen `@pytest.mark.befund("BZ-nn")` und `xfail(strict=True)`: Wird ein Befund behoben, wird sein Test XPASS (rot) und die Markierung muss entfernt werden. Alle Nachweise: `pytest -m befund --runxfail` (Integration nur über die Testports).

Quellen (Untersuchungsberichte der Sitzung): 1-lebenszyklus.md (L), 2-zurueckgestellt.md (Z), 3-aenderungen.md (D), 4-fremdkatalog.md (F), 5-umgebung.md (E).
Alle Befunde stammen aus Code- und Doku-Analyse. Keiner ist durch einen Test bestätigt. „Treffer“ = Zahl der unabhängigen Untersuchungen, die den Befund fanden.

## P1 – still falsch, Datenverlust oder Doppelausführung im Normalbetrieb

| ID | Befund | Treffer | IDs |
|---|---|---|---|
| BZ-01 | Eine einzige Delay-Queue `{queue}.DQ` mit TTL pro Nachricht. Nachrichten verfallen nur am Kopf. Ein Retry mit 3600 s vorne hält alle kürzeren Rückstellungen still fest: Lock, Rate-Limit, Backend, Guard, Cron. | 3 | F-1, Z-2, E-1 |
| BZ-02 | `schedule(delay=/eta=)` gibt ID X zurück, der Leader feuert mit neuer ID Y. Folgen: `get_result(X)` läuft in den Timeout, `cancel(X)` wirkt nicht, und das Unique-Flag wird nie freigegeben. | 2 | F-2, D-2 |
| BZ-03 | Publish an eine nicht deklarierte Queue. **Widersprüchlich:** stiller Verlust mit Quittung (F-3, E-2) oder 120-s-Hänger, der den Leader in jedem Takt blockiert (D-1). Vor dem Fix per Integrationstest klären. | 3 | F-3, E-2, D-1 |
| BZ-04 | Valkey-Eviction und -Persistenz sind weder dokumentiert noch geprüft. Die interne Recherche empfiehlt sogar `volatile-lru`, das Locks, Guard-, Unique- und Fired-Marker verdrängt; die Folge ist Doppelausführung. Ohne AOF gehen delay/eta-Tasks und CLI-Crons verloren, deaktivierte Crons kommen aktiv zurück. | 6 | F-5, L-5, Z-3, E-3, E-4, D-6 |
| BZ-05 | Lock-Konflikt (Mutex oder `max_concurrency`) landet nach etwa 51 s in der DLQ (`LOCK_CONFLICT_DLQ_CAP=10`). Dabei gibt es kein `TASK_DLQ`, kein Ergebnis und keine Workflow-Fehlermarkierung. Der Guide behauptet ein anderes Verhalten. | 1 (+D-4) | Z-1, Z-5, D-4 |
| BZ-06 | Unbekannter Task-Name geht sofort in die DLQ. Beim Rolling Deploy (neuer Producer, alter Worker) trifft das jeden neuen Task, und Workflows hängen. Eine Deploy-Reihenfolge ist nicht dokumentiert. | 2 | Z-6, D-5 |
| BZ-07 | SCAN über die ganze Valkey-DB bei der Leader-Übernahme, vor der ersten Lock-Erneuerung. Bei vielen Keys flattert der Leader (Schätzung: ab etwa 2–6 Mio. Keys). | 1 | L-1 |
| BZ-08 | Ein DLQ-Replay ohne `x-swarmq-replay-id` wird von 0.11-Workern still verworfen. Betroffen sind swarmv (pinnt <0.8), „Move messages“ in der RabbitMQ-UI, Shovel und ein altes CLI. | 1 | D-3 |
| BZ-09 | `unique_for` bei Crons hängt davon ab, welcher Worker gerade Leader ist. Mit anderer App, anderem `--module` oder getrennten CPU- und IO-Workern überlappen lange Cron-Läufe. | 1 | Z-4 |
| BZ-10 | Mehrere Apps auf einem Valkey haben keinen Namensraum. Rate-Limit- und Lock-Keys, Unique-Hash, Default-Queue `default` und der Leader werden geteilt. Ein Leader mit defektem Broker hält alle Crons aller Apps an. | 3 | D-10, Z-7, L-Nebenbefund |

## P2

| ID | Befund | IDs |
|---|---|---|
| BZ-11 | Ein entfernter Cron feuert bis zum Prune 24 h weiter. Jeder Fire landet als unregistrierter Task ohne TTL in der XQ, damit kehrt A14 zurück. | L-2, D-14 |
| BZ-12 | Done- bzw. Fired-Marker (3900 s) laufen bei mehr als 65 min Rückstand ab. Ergebnis ist Doppelausführung trotz Schutz. | L-4 |
| BZ-13 | Workflows hängen bis zur TTL, wenn ein Schritt über einen Nicht-Retry-Pfad endet. Läuft der Workflow-State ab, endet der Workflow ohne Log. | Z-5, D-4, D-7, L-8 |
| BZ-14 | Umbenannte oder verschobene Queues bleiben samt Inhalt ohne Consumer, ohne Warnung und ohne Metrik. | L-3, D-8, D-13 |
| BZ-15 | Eine Änderung von `SWARMQ_DELIVERY_LIMIT` oder `SWARMQ_QUEUE_TYPE` führt vermutlich zu PRECONDITION_FAILED und einer Neustart-Schleife. Die Doku legt die Änderung nahe. | F-10, D-9 |
| BZ-16 | Publish und Konsum laufen über eine Verbindung, `connection.blocked` wird nicht behandelt. Bei einem Broker-Alarm hängen auch die Acks. | E-5, F-12 |
| BZ-17 | Header `x-swarmq-traceback` ist unbegrenzt. Über `frame_max` schließt die Verbindung, und alle Nachbar-Tasks werden redelivered. | E-6 |
| BZ-18 | Der Shutdown dauert länger als 30 s (K8s) bzw. 10 s (Docker). SIGKILL kommt vor Lock-Freigabe, Leader-Übergabe und Guard-Abschluss. Einen Deploy-Leitfaden gibt es nicht. | F-6, E-7 |
| BZ-19 | Dead-Lettering in Quorum-Queues ist standardmäßig at-most-once, bei Leader-Wechsel gehen DQ- und XQ-Übergänge verloren. Die Doku verspricht at-least-once. | E-8 |
| BZ-20 | Prefetch gilt pro Consumer, nicht pro Worker. Bei mehreren Queues oder Prioritäten horten Worker Nachrichten, und die Zeit läuft gegen den consumer_timeout. | F-4 |
| BZ-21 | `dlq list` und `dlq inspect` entnehmen zerstörend; ein Abbruch kostet Nachrichten. | Z-9 |
| BZ-22 | Ein Unique-Task, der über `max_age` verfällt, gibt sein Flag nicht frei; neue Aufrufe werden verschluckt. | Z-16 |
| BZ-23 | Das Cancel-Flag lebt nur 24 h; länger verzögerte, stornierte Tasks laufen doch. | F-9, D-11 |
| BZ-24 | Ein Producer ohne Registry verliert still Dedup und Priorität. | Z-15 |
| BZ-25 | Locks werden weder erneuert noch gefenced; bei blockiertem Loop laufen sie still ab. | F-8 |
| BZ-26 | Retry-Jitter verschwindet am Deckel, `fixed` hat keinen Jitter, ab etwa 1024 Versuchen kommt es zu einem `OverflowError`. | F-7 |
| BZ-27 | Ein Worker-Slot bleibt nach 5 Abstürzen pro Stunde dauerhaft gesperrt. | Z-13 |
| BZ-28 | Ab 100 verwaisten oder deaktivierten Einträgen im Fire-Index feuert kein Cron mehr (Poll mit `LIMIT 100`). | L-7 |

## Nachtrag (Spike S4, 2026-10-10)

| ID | Befund | Quelle |
|---|---|---|
| BZ-29 | Auf RabbitMQ 4.3 erhöht `nack(requeue=True)` den `x-delivery-count` nicht. Die `redeliver`-Pfade laufen dort ohne Grenze und ohne Pause; auf 4.2 enden sie nach rund 20 schnellen Runden still im Broker-DLX (ohne `TASK_DLQ`). | Spike S4 (`tests/integration/spikes/test_s4_delivery_count.py`) |

## P3 (Auswahl)
- `result_ttl=0` erzeugt PX 0 (L-9).
- Bei Tasks über 65 min läuft der Guard-State mitten im Lauf ab (L-10).
- Producer-Publish über 16 MiB (F-11, Z-14).
- Kein Cluster- und kein Sentinel-Support.
- `BUSY` wird nicht als Ausfall erkannt.
- `HEALTH_HOST` 127.0.0.1.
- `max_age` vergleicht die Uhren von Producer und Worker (F-27).
- cgroup v1.
- Valkey-Pool ohne Limit.
- Nach einem Stillstand feuern alle überfälligen Crons gleichzeitig (D-16).
- Signaturänderungen werden nicht geprüft (D-12).

## Doku-Befunde
- Alte Lock-TTL-Formel in `architecture.md:115` und `locking-and-dedup.md:44` (EN/DE).
- Der Lock-Guide widerspricht dem Code.
- Doku nennt Python 3.11+, verlangt wird 3.14.
- Mindestversionen für RabbitMQ und Valkey fehlen.
- Valkey-Anforderungen fehlen: noeviction, AOF, kein Cluster.
- Ein Deploy-Leitfaden fehlt: Reihenfolge, Grace-Period, Readiness.
- `architecture.md:202` (DQ) und `:204` (Valkey-Neustart harmlos) sind falsch.
- B20 und B21 sind offen.
- Die Fehlertabelle im Workflow-Guide ist unvollständig.

## Nachweis-Tests

Status je Befund: „offen“ oder „behoben (MR n, <Commit>)“. Mit dem Fix ist der `xfail`-Marker des Nachweises entfernt.

| ID | Testdateien | Status |
|---|---|---|
| BZ-01 | `tests/integration/broker/test_befund_bz01_delay_queue_hol.py` | offen |
| BZ-02 | `tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py`, `tests/unit/scheduler/test_befund_bz02_bz07_bz09_bz28_scheduler.py` | behoben (MR 5, 0ff3af5) |
| BZ-03 | `tests/integration/broker/test_befund_bz03_unroutable_publish.py` | offen |
| BZ-04 | `tests/integration/test_befund_bz04_disabled_cron_data_loss.py`, `tests/unit/backend/test_befund_bz04_eviction_policy.py`, `tests/unit/worker/test_befund_bz04_lock_loss.py` | offen |
| BZ-05 | `tests/unit/worker/test_befund_bz05_lock_conflict_cap.py` | offen; Nachweise 2/3 (`TASK_DLQ`, Fehlerergebnis am Deckel) grün seit MR 3, a570112, Umschreiben auf `LockWaitExceeded` in MR 13 |
| BZ-06 | `tests/unit/worker/test_befund_bz06_unknown_task.py` | offen |
| BZ-07 | `tests/unit/scheduler/test_befund_bz02_bz07_bz09_bz28_scheduler.py` | behoben (MR 4, ddcb11b) |
| BZ-08 | `tests/integration/test_befund_bz08_replay_without_id.py` | offen |
| BZ-09 | `tests/unit/scheduler/test_befund_bz02_bz07_bz09_bz28_scheduler.py` | offen |
| BZ-10 | `tests/integration/test_befund_bz10_shared_valkey.py` | offen |
| BZ-11 | `tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py` | offen |
| BZ-12 | `tests/unit/worker/gates/test_befund_bz12_marker_ttl.py` | offen |
| BZ-13 | `tests/unit/worker/test_befund_bz13_workflow_hang.py` | behoben (MR 3, a570112) |
| BZ-14 | `tests/integration/broker/test_befund_bz14_orphaned_queue.py` | offen |
| BZ-15 | `tests/integration/broker/test_befund_bz15_queue_args_change.py` | offen |
| BZ-16 | `tests/integration/broker/test_befund_bz16_connection_blocked.py` | offen |
| BZ-17 | `tests/integration/broker/test_befund_bz17_traceback_header.py` | behoben (MR 2, f588215) |
| BZ-18 | `tests/unit/process_manager/test_befund_bz18_shutdown_budget.py`, `tests/unit/worker/test_befund_bz18_cleanup_after_grace.py` | offen |
| BZ-19 | `tests/unit/broker/test_befund_bz19_dead_letter_strategy.py` | offen |
| BZ-20 | `tests/integration/broker/test_befund_bz20_prefetch_per_consumer.py` | offen |
| BZ-21 | `tests/integration/broker/test_befund_bz21_dlq_destructive_read.py` | offen |
| BZ-22 | `tests/unit/worker/test_befund_bz22_unique_expiry.py` | behoben (MR 3, d4e2b81) |
| BZ-23 | `tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py` | behoben (MR 5, 660a940) |
| BZ-24 | `tests/unit/client/test_befund_bz24_registry_less_producer.py` | behoben (MR 2, 466cdd9) |
| BZ-25 | `tests/integration/test_befund_bz25_lock_lease.py` | offen |
| BZ-26 | `tests/unit/test_befund_bz26_backoff.py` | behoben (MR 2, 0fcbfe8) |
| BZ-27 | `tests/unit/process_manager/test_befund_bz27_slot_lock.py` | offen |
| BZ-28 | `tests/integration/test_befund_bz28_sched_poll_orphans.py`, `tests/unit/scheduler/test_befund_bz02_bz07_bz09_bz28_scheduler.py` | behoben (MR 4, d4719fa) |
| BZ-29 | `tests/unit/worker/gates/test_befund_bz29_redeliver_limit.py` | behoben (MR 3, 47aed19) |

## Entscheidungen (Georg, 2026-10-09)

Grundlage für den Fix-Plan 0.12.0. BZ-01, -02, -07, -13, -16, -17, -21, -22 und -28 brauchten keine Produktentscheidung.

| ID | Entscheidung |
|---|---|
| BZ-03 | Unroutbarer Publish wirft eine eigene Ausnahme (mandatory + Return-Erkennung). Der Leader loggt und zählt verlorene Cron-Fires. |
| BZ-04 | Verdrängende `maxmemory-policy`: Worker und Leader verweigern den Start (`ConfigurationError`, Exit 78), abschaltbar per Umgebungsvariable. Dazu `swarmq check`, Doku (noeviction + AOF) und Korrektur der Empfehlung `volatile-lru`. |
| BZ-05 | Lock-Konflikt wartet zeitbasiert bis `max_age` bzw. Lock-TTL + Puffer statt 10 Versuchen. Erst dann DLQ, mit `TASK_DLQ`, Fehlerergebnis und Workflow-Fail. |
| BZ-06 | Unbekannte Tasks werden mit Backoff zurückgestellt (Zähler-Header), erst nach einer Gnadenfrist (Richtwert 15 min) DLQ mit eigenem Event. Deploy-Reihenfolge wird dokumentiert. |
| BZ-08/BZ-12 | `x-death` bzw. `x-swarmq-dead-lettered-at` gilt als bewusstes Replay und umgeht den Done-Marker. Normale Neuzustellung wird weiter abgewiesen. Die Marker-TTL wird an `max_age`/Rückstand gekoppelt. |
| BZ-09 | `unique_for`/`unique_until` werden in Cron-Report und `ScheduleDefinition` mitgeführt. |
| BZ-10 | Optionaler App-Namensraum (`SWARMQ_NAMESPACE`) als Präfix für `sq:*`, Leader und Default-Queue; ohne Angabe unverändert (nicht BREAKING). Der Leader tritt nach wiederholten Publish-Fehlern zurück. |
| BZ-11 | Der Leader feuert keinen Cron, den kein lebender Worker meldet und den er selbst nicht kennt (eigenes Event). Fires über `route_via_dlx` bekommen zusätzlich eine Ablaufzeit. |
| BZ-14 | Worker tragen ihre Queues mit Heartbeat in Valkey ein. Der Leader meldet Queues ohne lebenden Consumer mit Inhalt (Event + Metrik), `swarmq check` listet sie. Keine Management-API. |
| BZ-15 | `PRECONDITION_FAILED` bei geänderten Queue-Argumenten wird erkannt: klare Meldung (alt vs. neu), Exit 78 statt Neustartschleife. Doku beschreibt die Migration. Delivery-Limit bleibt Argument. |
| BZ-18 | Drain läuft höchstens `shutdown_timeout` minus Reserve für die Aufräumphasen. Doku nennt `terminationGracePeriodSeconds` ≥ `shutdown_timeout` + 15 und Docker `stop_grace_period`. |
| BZ-19 | Neue Queues bekommen `x-dead-letter-strategy=at-least-once` (mit `overflow=reject-publish`). Für bestehende Queues Policy-Empfehlung und Erkennung über den BZ-15-Mechanismus. Doku korrigiert das Versprechen. |
| BZ-20 | `concurrency` ist Obergrenze für den ganzen Worker (gemeinsamer Kanal mit QoS). |
| BZ-23 | Das Cancel-Flag lebt bis ETA + `max_age` + Puffer (mindestens 24 h) und wird bei Zustellung gelöscht. |
| BZ-24 | Einmalige WARNING pro Task-Name, wenn ein Producer ohne TaskInfo sendet. Doku erklärt die Abhängigkeit von der Registry. |
| BZ-25 | Mutex- und Semaphore-Leases werden während des Laufs im Takt erneuert. Scheitert die Erneuerung, Event + Log. Kein Fencing-Token. |
| BZ-26 | Nur Deckel-Jitter und Overflow werden behoben. `fixed` bleibt bewusst ohne Jitter; der Nachweis dafür ist entfallen. |
| BZ-27 | Abkühlzeit statt Dauersperre (auch beim Hang-Budget), Gauge für gesperrte Slots plus Event. |
