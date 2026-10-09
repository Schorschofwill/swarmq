<!-- Befundregister. IDs BZ-nn sind die Ticketnummern fuer xfail-Marker (AGENTS.md: kein Skip ohne Ticket). -->
# Befundregister „Betrieb über Zeit“ – swarmq v0.11.0 (fb9e067), 2026-10-09

Status: offen, Nachweis-Tests auf Branch test/betrieb-ueber-zeit (pytest-Marker `befund`, xfail strict).

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
