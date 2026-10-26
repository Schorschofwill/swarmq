# Methodik des Vergleichs

Der Vergleich stellt swarmq neben Celery, Dramatiq, arq und taskiq (Plan
`docs/plans/2026-10-24-1118-chore-qualitaetsablauf-plan.md`, R19 bis R22). Er
hat zwei Teile: Performance bei gleicher Garantie (`runner.py`, U8) und
Garantien unter gleichen Störungen (`stoerungen/`, U9). Dieses Dokument
beschreibt vor allem den Garantie-Teil. Methode, Skripte, Konfiguration und Ergebnisse
sind öffentlich und nachstellbar; veröffentlicht wird auch, wo swarmq
verliert (R22).

## Parallelität im Performance-Teil

Ab Werk führen die Systeme sehr verschieden viele Tasks gleichzeitig aus
(24 Kerne: swarmq 10, arq 10, Celery 24, Dramatiq 192, taskiq 200). Ein
Durchsatz ab Werk misst deshalb zum Teil nur diese Zahl. Der Performance-Teil
läuft darum in zwei Parallelitäten (`runner.py --parallelitaet`,
`parallelitaet.py`): `gleich` setzt für jedes System 10 gleichzeitige Tasks,
in einem Prozess, wo das System es erlaubt (Celery im Prefork-Pool: zehn
Kindprozesse); `ab_werk` lässt die Werkseinstellung stehen, so wie Nutzer
die Systeme betreiben. Der Bericht weist beide getrennt aus; die
Entscheidungsregeln vergleichen bei `gleich`. Der Garantie-Teil läuft immer
ab Werk, denn dort ist das Verhalten ab Werk die Frage.

`io_bound` und `semaphore_scale` sollen den Worker messen, nicht den
Erzeuger: Sie reihen mit zehn gleichzeitigen Publishes ein, sammeln jedes
Ergebnis ab seinem Einreihen ein und fragen bis zu 200 Ergebnisse
gleichzeitig ab (`scenarios/base.py`). Bis `b18499a9` reihten beide erst alle
Tasks nacheinander ein und sammelten danach mit zehn Abfragen ein; auf
celsius lag `io_bound` damit bei fast jedem System um 200 Tasks/s, P50 bei der
halben Gesamtdauer, und die Parallelität des Workers änderte nichts. Solche
Läufe sind für `io_bound` und `semaphore_scale` nicht verwertbar, ebenso Dramatiq
im Profil `stark`: Es holte bis dahin eine Nachricht je Prozess statt je Thread.

`noop` und `latency` fragen Ergebnisse alle 1 ms ab, `io_bound` und
`semaphore_scale` alle 10 ms (dort fragen bis zu 200 gleichzeitig; enger
würde der Erzeuger selbst zur Last). Bis `94343b9f` fragten auch `noop` und
`latency` alle 10 ms. Eine einzelne Latenz maß dann vor allem, ob der Task
vor der ersten Abfrage fertig war: lokal am 2026-10-26 mit Dramatiq 2.2.1
lag dieselbe Ausführung bei rund 1 ms oder bei 11 ms, und Publisher-Confirms
des Erzeugers, die die Zustellung um rund 1 ms beschleunigen, verdreifachten
den gemessenen Durchsatz. Läufe bis `94343b9f` sind für `noop` und `latency`
nicht verwertbar. swarmq ist davon nicht betroffen: `get_result` wartet per
Pub/Sub und ignoriert das Abfrageintervall (`swarmq/client/results.py`).
Die Ergebnisdateien nennen das Intervall (`params.abfrage_ms`).

## Was die Szenarien messen

Verwertbar sind auf celsius `io_bound` ab `94343b9f` und `noop` und `latency`
ab `a9237312`. Gegen den Laptop ist celsius für jedes System 2,3- bis 3,3-mal
langsamer (`noop`, 2026-10-26); verglichen wird deshalb nur auf derselben
Maschine. Die Aufschlüsselungen unten sind Laptop-Messungen vom 2026-10-26
mit swarmq an `a9237312`.

- **`noop` misst vor allem den Queue-Typ.** Ein Task nach dem anderen wartet
  jedes Mal auf die Publisher-Bestätigung. Bei einer Quorum-Queue kommt sie
  erst nach dem Schreiben auf die Platte: swarmq braucht für `schedule()`
  2,1 ms mit Quorum-Queue und 0,45 ms mit klassischer Queue, für den ganzen
  Durchlauf 3,1 ms und 1,2 ms. Celery (`stark`) und taskiq nutzen ebenfalls
  Quorum-Queues und liegen gleichauf mit swarmq; Dramatiq nutzt klassische
  Queues.
- **Warten auf das Ergebnis.** In `noop` wartet swarmq per Pub/Sub, die
  anderen Systeme fragen alle 1 ms ab und verlieren dabei bis zu 1 ms je
  Task. In `latency` sammelt der Erzeuger für jedes System gleich
  per einzelner Abfrage ein (`timeout=0`); dort gibt es diesen Unterschied
  nicht.
- **`io_bound` begrenzt bei swarmq der Worker.** Ein Task belegt einen der
  zehn Plätze 15,1 ms, davon 10,5 ms in `run()` und 2,9 ms in sechs
  Valkey- und RabbitMQ-Aufrufen nacheinander. Der Erzeuger begrenzt nicht:
  Mit einer geteilten Pub/Sub-Verbindung statt einer Verbindung je
  `get_result` ändert sich der Durchsatz nicht (409 bis 620 gegen 492 bis
  601 Tasks/s). Ob bei Celery, Dramatiq und taskiq Worker oder Erzeuger
  begrenzt, ist nicht untersucht.

## Veröffentlichung

Veröffentlicht werden nur Wertungsläufe auf der Referenzmaschine celsius,
keine Laptop-Läufe. Die Ergebnisse erscheinen als Seite unter
`website/explanation/`, mit swarmq-Version, Maschine und Datum je Zahl und
einem Verweis auf Methodik, Rohdaten und Rezepte hier. Dazu gehören die
Grenzen jedes Systems, etwa dass Dramatiq keine Quorum-Queues kennt und sein
Profil `stark` deshalb klassische dauerhafte Queues nutzt.

## Nachstellen

```bash
benchmarks/comparison/einrichten.sh          # einmal: je System eine Umgebung (.venvs/<system>)
uv run just vergleich-garantie --runs 2      # alle Systeme, Profile und Störungen, zwei Durchgänge
uv run just vergleich-garantie --systems swarmq,celery --stoerungen sigkill   # Auswahl
```

Auf dem Messhost (celsius) läuft der Vergleich nur in Containern; dort
liegen weder Repo noch Umgebungen noch Ergebnisse. Gesteuert wird vom eigenen
Rechner über den Docker-Kontext des Hosts:

```bash
scripts/vergleich-entfernt.sh starten celsius performance --runs 3   # oder: garantie --runs 3, ab --a … --b …
scripts/vergleich-entfernt.sh status celsius
scripts/vergleich-entfernt.sh abholen celsius benchmarks/comparison/wertung/<datum>-<commit>-<teil>
```

`starten` baut aus einem sauberen Klon des aktuellen Commits das Image
`swarmq-vergleich:<commit>` (`Dockerfile`: Python und uv wie die CI, die fünf
Umgebungen aus `einrichten.sh`, Docker-CLI mit Compose) und startet den Lauf
im Hintergrund. Der Lauf-Container nutzt das Netz und den Docker-Socket des
Hosts, startet seine Dienste selbst und räumt sie am Ende ab
(`im-container.sh`). `abholen` kopiert Ergebnisse und Log und entfernt
Container und Image. Deshalb bindet kein Compose-Stack des Vergleichs eine
Datei des Hosts ein; die Proxys von Toxiproxy stehen in `stoerungen/compose.yml`.
Ins Repo kommen nur die gültigen Ergebnisdateien eines Laufs, beim
Garantie-Teil dazu Hauptbücher und Logs gepackt, ohne die Logs des
Performance-Teils (`wertung/README.md`).

Voraussetzungen: Docker mit Compose, `uv`, Python 3.14 (die Umgebungen legt
`einrichten.sh` mit `uv venv --python 3.14` an). Das Rezept startet den
eigenen Stack (Compose-Projekt `swarmq-vergleich-stoerung`), fährt alle
Kombinationen und entfernt den Stack am Ende mit `down -v`. Die Ergebnisse
liegen unter `benchmarks/comparison/stoerungen/ergebnisse/<zeitstempel>/`:
je Kombination das Hauptbuch (`.sqlite`), die Bilanz (`.json`), die
Versionen (`.meta.json`) und die Ausgaben von Erzeuger und Worker (`.log`),
dazu `lauf.json` und die Tabellen in `bilanz.md`.

Der Garantie-Teil misst keine Zeit. Er läuft deshalb auch auf einem Laptop;
die Wertung hängt nicht von der Maschine ab.

## Aufbau

| Dienst | Abbild | Rolle |
|---|---|---|
| RabbitMQ | `rabbitmq:4.3.6-management` | Broker von swarmq, Celery, Dramatiq, taskiq |
| Valkey | `valkey/valkey:8.1.10-alpine`, `--appendonly yes --appendfsync always` | Ergebnisse, bei swarmq auch Ausführungsschutz, bei arq der Broker |
| Valkey flüchtig | `valkey/valkey:8.1.10-alpine`, `--appendonly no --save ""` | nur in der Störung „Valkey-Neustart ohne Persistenz“ |
| Toxiproxy | `ghcr.io/shopify/toxiproxy:2.9.0` (wie `tests/chaos/compose.yml`) | vor allen drei Diensten |

Erzeuger und Worker erreichen die Dienste nur über Toxiproxy (Host-Ports
5686, 6386, 6387; API 18476). Die Ports meiden die Standard-, Test-, Chaos-
und Vergleichsports.

Versionen der Systeme (exakt gepinnt in `requirements-<system>.txt`, je Lauf
in `.meta.json` gespeichert):

| System | Pakete |
|---|---|
| swarmq | swarmq 0.14.4 (Stand des Repos), aio-pika 10.0.1, aiormq 7.0.0, valkey 6.1.1 |
| Celery | celery 5.6.3, kombu 5.6.2, amqp 5.4.1, redis 6.4.0 |
| Dramatiq | dramatiq 2.2.1, pika 1.4.4, redis 8.1.0 |
| arq | arq 0.28.0, redis 5.3.1 |
| taskiq | taskiq 0.13.0, taskiq-aio-pika 0.6.1, taskiq-redis 1.2.4, aio-pika 10.0.1, aiormq 7.0.0, redis 8.1.0 |

## Profile

Jedes System läuft in zwei Profilen aus `garantieprofile.py`: `ab_werk`
(Werkseinstellungen der gepinnten Version) und `stark` (die stärkste
Zustellgarantie, die sich mit swarmq vergleichen lässt). Die genauen
Schlüssel, Begründungen und Belege stehen dort. Kurz:

| System | `stark` |
|---|---|
| swarmq | feste Garantien, beide Profile gleich; frische Quorum-Queue je Lauf (E-017) |
| Celery | `task_acks_late`, `task_reject_on_worker_lost`, Quorum-Queue, `confirm_publish`, Prefetch 1 |
| Dramatiq | `confirm_delivery`, Prefetch 1 je Thread (`dramatiq_queue_prefetch` gilt je Prozess); klassische Queues (Dramatiq kennt keine Quorum-Queues) |
| arq | Werkseinstellung (arq hat keine stärkere) |
| taskiq | `--ack-type when_saved`, Quorum-Queue (beides schon Werkswert) |

Celery braucht auch `ab_werk` exklusive Kontroll- und Event-Queues, sonst
startet der Worker auf RabbitMQ 4.3 nicht; die Task-Queue bleibt ab Werk.

Jede Kombination bekommt eine frische Queue (`bench-<zufall>`). Für swarmq
ist das Pflicht: Eine Bestandsqueue behielte die schwächere
Dead-Letter-Strategie (E-017).

## Ablauf einer Kombination

Jede Kombination aus System, Profil und Störung läuft als eigener Prozess
(`stoerungen/lauf.py`) im Python der Umgebung des Systems, der Worker als
eigener Prozess über die CLI des Systems, mit Parallelität ab Werk. Die
Kombinationen laufen reihum über die Systeme.

1. Worker starten, 30 Tasks einreihen.
2. Warten, bis laut Hauptbuch ein Task läuft. Jede Störung trifft also
   nachweislich laufende Tasks.
3. Die Störung auslösen und währenddessen weiter einreihen (höchstens 30,
   alle 0,1 s ein Task, bis die Störung vorbei ist).
4. 30 weitere Tasks einreihen.
5. Ruhephase: Sie endet, wenn seit 20 s keine Ausführung dazukam und jeder
   bestätigte Publish ausgeführt ist, spätestens nach 360 s. Die Obergrenze
   liegt über der längsten Frist, nach der ein System ab Werk neu zustellt:
   arq gibt einen abgebrochenen Job erst frei, wenn sein In-Progress-Schlüssel
   abläuft (`job_timeout` 300 s + 10 s), swarmq einen Task, wenn der
   Running-Marker des Ausführungsschutzes abläuft (60 s).

Jeder Task ist dieselbe Funktion (`stoerungen/hauptbuch.py`, `garantie`):
Sie schreibt ihren Beginn ins Hauptbuch, arbeitet 1 s und schreibt am Ende
ihre Ausführung. Ein Task, den die Störung mitten im Lauf trifft, zählt also
nicht als ausgeführt. Die Tasks speichern ihr Ergebnis wie die übrigen Tasks
des Vergleichs im Ergebnis-Backend des Systems (Valkey).

Stirbt ein Worker infolge einer Störung, startet ihn der Lauf neu, wie es
ein Supervisor im Betrieb täte; die Tabelle zeigt das in der Spalte „Worker
neu gestartet“.

## Störungen

Alle Systeme bekommen in beiden Profilen dieselben Störungen (R21).

| Störung | Umsetzung |
|---|---|
| `sigkill` | SIGKILL an die Prozessgruppe des Workers (PID aus dem Adapter), also auch an geforkte Pool-Prozesse, während Tasks laufen; danach startet der Worker neu |
| `broker_neustart` | `docker compose restart` des Brokers, danach warten, bis er gesund ist. Bei arq ist der Broker Valkey (mit Persistenz) |
| `netztrennung` | Toxiproxy schaltet alle drei Proxys für 10 s ab: Offene Verbindungen brechen, neue werden abgewiesen |
| `valkey_neustart_persistent` | `docker compose restart` des Valkey mit AOF |
| `valkey_neustart_fluechtig` | Die Kombination läuft gegen das flüchtige Valkey, das neu gestartet wird und dabei alle Daten verliert |

Nicht anwendbar ist keine Störung: Alle fünf Systeme nutzen Valkey (arq als
Broker, die übrigen für Ergebnisse, swarmq zusätzlich für den
Ausführungsschutz) und alle einen Broker. Bei arq treffen Broker-Neustart
und Valkey-Neustart mit Persistenz denselben Dienst.

## Hauptbuch und Wertung

Das Hauptbuch ist eine SQLite-Datei im WAL-Modus auf dem Host, damit keine
gestörte Komponente es mitreißt (KTD14). Der Erzeuger schreibt jeden Publish
mit Ausgang:

- **bestätigt**: Der Aufruf des Systems kehrte ohne Fehler zurück.
- **unbestimmt**: swarmq meldete `PublishIndeterminate` (die Nachricht war
  gesendet, die Bestätigung fehlt; E-042), oder der Aufruf kam bei
  irgendeinem System nicht binnen 10 s zurück.
- **abgelehnt**: Der Aufruf endete mit einem anderen Fehler. Nur swarmq
  unterscheidet „nicht gesendet“ (`BrokerConnectionError`) von „womöglich
  gesendet“; bei den anderen Systemen gilt jeder Fehler als abgelehnt, so
  wie ihn ein Aufrufer liest.

Die Bilanz (`stoerungen/bilanz.py`) zählt nach der Ruhephase:

| Spalte | Regel |
|---|---|
| Verluste | bestätigter Publish ohne Ausführung |
| Duplikate | Task mit mehr als einer Ausführung |
| unbestimmt | eigene Spalte, kein Verlust (E-042) |
| abgelehnt | kein Verlust |
| Duplikate E-077 | Duplikat, dessen spätere Ausführung nach einem Valkey-Neustart ohne Persistenz lag; eigene Spalte, weil swarmq diesen Fall bewusst zulässt (E-077) |

Ein Hauptbuch ohne Publish ist ein Fehler, keine Bilanz mit „0 Verlusten“.
Eine Bilanz ohne bestätigten Publish oder ohne Ausführung heißt „leer“: Der
Lauf hat nichts gemessen.

## Ergebnisse

Probelauf auf einem Laptop (24 Kerne, Linux 6.17) am 2026-10-24, zwei
Durchgänge mit `uv run just vergleich-garantie --runs 2`, Stand swarmq 0.14.4
(Branch `qa/u9`; ein Probelauf, kein Lauf für die Veröffentlichung).
Weichen die beiden Durchgänge ab, steht „Durchgang 1 /
Durchgang 2“ in der Zelle. Jede Bilanz hat bestätigte Publishes und
Ausführungen; kein Lauf war leer, keiner scheiterte.

| System | Profil | Störung | bestätigt | ausgeführt | Verluste | Duplikate | unbestimmt | abgelehnt | Duplikate E-077 | Worker neu gestartet |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| swarmq | ab_werk | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | sigkill | 61 | 37 | 24 | 0 | 0 | 0 | 0 | nein |
| celery | stark | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | ab_werk | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | stark | sigkill | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | ab_werk | broker_neustart | 84 / 83 | 84 / 83 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | broker_neustart | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | broker_neustart | 86 | 86 | 0 | 0 | 0 | 4 | 0 | nein |
| celery | stark | broker_neustart | 87 / 86 | 87 / 86 | 0 | 24 | 0 | 3 / 4 | 0 | nein |
| dramatiq | ab_werk | broker_neustart | 66 | 66 | 0 | 31 | 0 | 24 | 0 | nein |
| dramatiq | stark | broker_neustart | 73 / 64 | 73 / 64 | 0 | 24 | 0 | 17 / 26 | 0 | nein |
| arq | ab_werk | broker_neustart | 81 / 79 | 81 / 79 | 0 | 0 | 0 | 2 / 3 | 0 | nein |
| arq | stark | broker_neustart | 80 | 80 | 0 | 0 | 0 | 2 | 0 | nein |
| taskiq | ab_werk | broker_neustart | 61 | 61 | 0 | 20 | 0 | 29 | 0 | nein |
| taskiq | stark | broker_neustart | 61 | 61 | 0 | 20 | 0 | 29 | 0 | nein |
| swarmq | ab_werk | netztrennung | 61 | 61 | 0 | 0 | 1 | 0 | 0 | nein |
| swarmq | stark | netztrennung | 61 | 61 | 0 | 0 | 1 | 0 | 0 | nein |
| celery | ab_werk | netztrennung | 61 | 62 | 0 | 0 | 1 | 1 / 0 | 0 | nein |
| celery | stark | netztrennung | 61 | 62 | 0 | 24 | 1 | 1 | 0 | nein |
| dramatiq | ab_werk | netztrennung | 61 | 61 | 0 | 31 | 0 | 29 | 0 | nein |
| dramatiq | stark | netztrennung | 61 | 61 | 0 | 8 / 6 | 0 | 29 | 0 | nein |
| arq | ab_werk | netztrennung | 61 | 61 | 0 | 1 / 0 | 0 | 29 | 0 | ja |
| arq | stark | netztrennung | 61 | 61 | 0 | 0 / 2 | 0 | 29 | 0 | ja |
| taskiq | ab_werk | netztrennung | 31 | 31 | 0 | 20 | 0 | 59 | 0 | nein |
| taskiq | stark | netztrennung | 31 | 31 | 0 | 20 | 0 | 59 | 0 | nein |
| swarmq | ab_werk | valkey_neustart_persistent | 82 / 83 | 82 / 83 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | valkey_neustart_persistent | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | valkey_neustart_persistent | 75 / 78 | 75 / 78 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | valkey_neustart_persistent | 73 / 79 | 73 / 79 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | valkey_neustart_persistent | 84 / 83 | 84 / 83 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | valkey_neustart_persistent | 83 / 82 | 83 / 82 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | valkey_neustart_persistent | 81 / 79 | 81 / 79 | 0 | 0 | 0 | 2 / 3 | 0 | nein |
| arq | stark | valkey_neustart_persistent | 81 / 74 | 81 / 74 | 0 | 0 | 0 | 2 / 0 | 0 | nein |
| taskiq | ab_werk | valkey_neustart_persistent | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | stark | valkey_neustart_persistent | 83 / 82 | 83 / 82 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | ab_werk | valkey_neustart_fluechtig | 82 / 83 | 82 / 83 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | valkey_neustart_fluechtig | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | valkey_neustart_fluechtig | 75 / 78 | 75 / 78 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | valkey_neustart_fluechtig | 78 / 74 | 78 / 74 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | valkey_neustart_fluechtig | 83 / 82 | 83 / 82 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | valkey_neustart_fluechtig | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | valkey_neustart_fluechtig | 81 / 80 | 59 | 22 / 21 | 0 | 0 | 2 / 3 | 0 | nein |
| arq | stark | valkey_neustart_fluechtig | 80 / 81 | 59 / 60 | 21 | 0 | 0 | 2 | 0 | nein |
| taskiq | ab_werk | valkey_neustart_fluechtig | 83 / 82 | 83 / 82 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | stark | valkey_neustart_fluechtig | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |

### Was die Zahlen zeigen

- **Verluste.** Celery ab Werk verliert beim Abbruch des Workers jeden Task,
  der gerade lief (24 von 61): Celery bestätigt ab Werk vor der Ausführung
  (`task_acks_late` aus). Im Profil `stark` verliert Celery nichts. arq
  verliert bei einem Valkey-Neustart ohne Persistenz alles, was noch in der
  Queue lag (21 bis 22), weil Valkey dort der Broker ist; das ist dieselbe
  Betriebsanforderung wie bei swarmq (Valkey mit Persistenz), trifft bei arq
  aber die Zustellung selbst. swarmq verliert in keiner Kombination.
- **Duplikate.** Bei Broker-Neustart und Netztrennung führen Celery
  (`stark`), Dramatiq und taskiq die Tasks, die bei der Störung liefen, ein
  zweites Mal aus: Die Ausführung war fertig, die Bestätigung an den Broker
  ging mit der Verbindung verloren, und der Broker stellte neu zu. Celery ab
  Werk hat dort keine Duplikate, weil es vorher bestätigt (und dafür beim
  Abbruch verliert). swarmq hat in keiner Kombination Duplikate; gegen
  genau diesen Fall hält es den Ausführungsschutz in Valkey (Done-Marker je
  Ausführung). Er hat eingegriffen: Im Broker-Neustart (`stark`, Durchgang
  1) war bei 10 Tasks die Bestätigung verloren (`delivery_settle_failed`),
  und ihre Neuzustellung lief nicht noch einmal (`duplicate_delivery_skipped`,
  im `.log` der Kombination).
- **Duplikate E-077.** Keine. Im Valkey-Neustart ohne Persistenz trifft die
  Störung bei swarmq laufende Tasks, aber keine Neuzustellung einer schon
  erledigten Ausführung; der von E-077 zugelassene Fall trat in diesen
  Läufen nicht ein. Die Spalte bleibt, weil er eintreten kann.
- **Unbestimmt.** swarmq meldet bei der Netztrennung einen Publish als
  `PublishIndeterminate` (gesendet, Bestätigung verloren; E-042); er wurde
  ausgeführt. Celery meldet einen Publish nicht binnen der Frist von 10 s
  zurück; auch er wurde ausgeführt (deshalb „ausgeführt“ 62 bei 61
  bestätigten).
- **Abgelehnt.** Fehler beim Einreihen sind kein Verlust, zeigen aber, ob
  der Erzeuger nach der Störung weiterarbeitet. Der Erzeuger von taskiq
  (taskiq-aio-pika) nimmt nach Broker-Neustart oder Netztrennung keinen
  Task mehr an (29 bzw. 59 abgelehnt, also jeder Publish ab der Störung).
  Dramatiq lehnt während der Störung ab und arbeitet danach wieder. swarmq
  lehnt keinen Publish ab: Es wiederholt, was nachweislich nicht gesendet
  war (E-042).
- **Worker neu gestartet.** Der arq-Worker beendet sich bei der
  Netztrennung (Verbindung zu Valkey verloren); der Lauf startet ihn neu wie
  ein Supervisor. Alle anderen Worker überstehen jede Störung selbst.

### Streuung zwischen den Durchgängen

Bei Verlusten und Duplikaten stimmen die Null- und Nicht-Null-Muster beider
Durchgänge überein, mit einer Ausnahme: arq bei der Netztrennung hat in
einem der beiden Durchgänge ein bis zwei Duplikate, im anderen keines
(`ab_werk` 1 / 0, `stark` 0 / 2). Ob ein Job doppelt läuft, hängt dort davon
ab, ob sein Ende vor oder nach dem Abbruch des Workers in Valkey ankam. Die
übrigen Unterschiede liegen in der Zahl der Publishes während der Störung
(bestätigt, abgelehnt), die von der Dauer der Störung abhängt.

### Grenzen

- Der Probelauf ist ein Laptop-Lauf; der Garantie-Teil misst keine Zeit, die
  Zahlen hängen aber von der Dauer der Störungen und der Parallelität ab
  Werk ab (24 Kerne: Celery und Dramatiq starten 24 Prozesse).
- Je Kombination laufen 61 bis 90 Tasks. Seltene Fälle (etwa die
  E-077-Duplikate) zeigen sich damit nicht zuverlässig.
- Die Ruhephase endet spätestens nach 360 s. Ein System, das erst später
  neu zustellt, erscheint mit Verlusten.
