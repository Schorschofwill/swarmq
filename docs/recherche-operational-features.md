# Recherche: Operational Features

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument fuer die Implementierung operativer Features in SwarmQ. Behandelt verzoegertes Ausfuehren, Burst-Modus, Health-Checks, Autoscaling, Prometheus-Metriken, Bulk-Enqueue, Prioritaeten und Task-Ablauf.

---

## Inhaltsverzeichnis

1. [Verzoegertes Ausfuehren (ETA / Countdown)](#1-verzoegertes-ausfuehren-eta--countdown)
2. [Burst-Modus](#2-burst-modus)
3. [Health-Check-Endpunkte](#3-health-check-endpunkte)
4. [Worker-Autoscaling](#4-worker-autoscaling)
5. [Prometheus-Metriken](#5-prometheus-metriken)
6. [Bulk-Enqueue](#6-bulk-enqueue)
7. [Task-Prioritaeten](#7-task-prioritaeten)
8. [Task-Ablauf / Max Age](#8-task-ablauf--max-age)

---

# 1. Verzoegertes Ausfuehren (ETA / Countdown)

## 1.1 Wie bestehende Bibliotheken das loesen

### Celery

- `apply_async(countdown=60)` — relative Verzoegerung in Sekunden
- `apply_async(eta=datetime(...))` — absoluter Zeitpunkt
- **Implementierung:** ETA-Tasks werden im Worker-Speicher gehalten bis sie faellig sind
- **Problem:** Bei vielen verzoegerten Tasks waechst der Speicherverbrauch unkontrolliert (Memory Bloat)
- Redis-Backend: Verwendet Sorted Sets fuer geplante Tasks

### Dramatiq

- `send_with_options(delay=10000)` — Verzoegerung in Millisekunden
- **RabbitMQ-Implementierung:** Dead-Letter-Exchange (DLX) + TTL-Pattern
  - Nachricht wird in eine Delay-Queue mit per-message TTL geschickt
  - Nach Ablauf der TTL wird die Nachricht per DLX in die Hauptqueue zurueckgeleitet
- **Redis-Implementierung:** Delayed-Queue mit Sorted Set (Score = Ausfuehrungszeitpunkt)

## 1.2 RabbitMQ-spezifische Patterns

### Pattern A: DLX + Per-Message-TTL

```
Producer → delay_exchange → delay_queue (TTL + DLX) → [TTL abgelaufen] → main_exchange → main_queue → Consumer
```

**Vorteile:**
- Kein externer Scheduler noetig, RabbitMQ verwaltet die Verzoegerung
- Keine zusaetzliche Infrastruktur

**Nachteile:**
- **Head-of-Line-Blocking:** Per-message TTL wird nur am Kopf der Queue geprueft. Eine Nachricht mit TTL=60s blockiert eine dahinterliegende mit TTL=5s
- Workaround: Mehrere Delay-Queues fuer verschiedene Bereiche (z.B. 1s, 5s, 30s, 60s, 300s, 3600s)
- Komplexe Topologie bei vielen verschiedenen Verzoegerungen
- Quorum-Queues unterstuetzen `x-message-ttl` als Queue-Policy, aber per-message `expiration` wird dort ebenfalls nur am Head geprueft

### Pattern B: Mehrere Delay-Queues (gestuft)

Statt einer einzigen Delay-Queue mit per-message TTL werden mehrere Queues mit fester TTL angelegt:

| Queue             | TTL    | Verwendung           |
|-------------------|--------|----------------------|
| `delay.1s`        | 1000   | Countdown 0-1s       |
| `delay.5s`        | 5000   | Countdown 1-5s       |
| `delay.30s`       | 30000  | Countdown 5-30s      |
| `delay.60s`       | 60000  | Countdown 30-60s     |
| `delay.300s`      | 300000 | Countdown 1-5min     |
| `delay.3600s`     | 3600000| Countdown 5-60min    |

- Vorteil: Kein Head-of-Line-Blocking (alle Nachrichten in einer Queue haben gleiche TTL)
- Nachteil: Verzoegerung ist nur grob gestuft, nicht exakt
- Nachteil: Viele Queues bei feiner Granularitaet

### Pattern C: Delayed-Message-Exchange-Plugin

- Das Community-Plugin `rabbitmq_delayed_message_exchange` wurde am 29. Januar 2026 archiviert
- **Grund:** Basiert auf Mnesia, das ab RabbitMQ 4.3/4.4 entfernt wird
- **Kein Ersatz in Open-Source-RabbitMQ** — native Delayed Queues gibt es nur in VMware Tanzu RabbitMQ (kommerziell)
- Tanzu RabbitMQ 4.2 bietet echtes Delayed-Queue-Feature mit Raft-Replikation und Indexierung nach Ausfuehrungszeitpunkt
- **Fazit:** Fuer Open-Source-RabbitMQ ist dieses Pattern keine Option mehr

### Pattern D: Valkey-basierte Verzoegerung

```
Producer → Valkey Sorted Set (Score = Timestamp) → Scheduler-Loop → RabbitMQ main_queue → Consumer
```

**Implementierung:**
1. Producer fuegt Task mit `ZADD delayed_tasks <timestamp> <task_payload>` hinzu
2. Scheduler-Loop pollt periodisch mit `ZRANGEBYSCORE delayed_tasks -inf <now> LIMIT 0 100`
3. Atomarer Claim per Lua-Script:

```lua
-- Atomares Abholen und Entfernen faelliger Tasks
local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
if #tasks > 0 then
    redis.call('ZREM', KEYS[1], unpack(tasks))
end
return tasks
```

4. Scheduler publisht geclaimte Tasks in die RabbitMQ-Hauptqueue

**Vorteile:**
- Praezise Verzoegerung (nicht gestuft)
- Kein Head-of-Line-Blocking
- Skaliert auf Millionen verzoegerter Tasks
- Funktioniert unabhaengig von RabbitMQ-Version und Queue-Typ

**Nachteile:**
- Benoetigt einen Scheduler-Prozess (bereits durch den Embedded Scheduler vorhanden!)
- Tasks liegen in Valkey, nicht in RabbitMQ — bei Valkey-Ausfall gehen verzoegerte Tasks verloren (sofern keine Persistenz)
- Polling-basiert: minimale Latenz zwischen Faelligkeit und tatsaechlicher Ausfuehrung (abhaengig vom Poll-Intervall)

## 1.3 Vergleich: RabbitMQ (DLX+TTL) vs. Valkey (Sorted Set)

| Kriterium              | RabbitMQ DLX+TTL (gestuft)     | Valkey Sorted Set             |
|------------------------|--------------------------------|-------------------------------|
| Praezision             | Grob gestuft (Queue-Grenzen)   | Millisekundengenau            |
| Head-of-Line-Blocking  | Nein (bei gestuften Queues)    | Nein                          |
| Zusaetzliche Infra     | Keine (nur Queues)             | Scheduler-Loop noetig         |
| Skalierung             | Begrenzt (viele Queues)        | Millionen Tasks in einem Set  |
| Zuverlaessigkeit       | RabbitMQ-Persistenz (robust)   | Valkey-Persistenz (konfigurierbar) |
| Komplexitaet           | Topologie-Aufwand              | Code-Aufwand                  |
| Quorum-Queue-Kompat.   | Eingeschraenkt (TTL-Verhalten) | Voll kompatibel               |

## 1.4 Empfehlung fuer SwarmQ

**Valkey-basierte Verzoegerung (Pattern D)** ist die klare Empfehlung:

1. **Embedded Scheduler existiert bereits** — der Scheduler-Loop fuer verzoegerte Tasks laesst sich direkt in den bestehenden Scheduler integrieren, der bereits mit Leader-Election und Heartbeat arbeitet
2. **Praezision** — millisekundengenaue Ausfuehrung statt gestufter Approximation
3. **Kein Vendor-Lock-in** — unabhaengig von RabbitMQ-Version, Plugin-Verfuegbarkeit oder Queue-Typ
4. **Einfache API:**
   ```python
   task.apply_async(countdown=60)        # relative Verzoegerung
   task.apply_async(eta=datetime(...))    # absoluter Zeitpunkt
   ```
5. **Poll-Intervall:** 500ms als Default, konfigurierbar. Der Scheduler pollt ohnehin fuer periodische Tasks — verzoegerte Tasks kommen ohne zusaetzlichen Loop aus.
6. **Persistenz-Garantie:** Valkey mit AOF-Persistenz (`appendfsync everysec`) bietet hinreichende Sicherheit. Bei Crash gehen maximal 1 Sekunde an verzoegerten Tasks verloren.

---

# 2. Burst-Modus

## 2.1 Wie bestehende Bibliotheken das loesen

### RQ (Redis Queue)

- `rq worker --burst` — verarbeitet alle Jobs, beendet sich dann mit Exit-Code 0
- **Implementierung:** Worker-Loop prueft nach jedem Job, ob alle Queues leer sind. Wenn ja → Graceful Shutdown
- `WorkerPool` unterstuetzt ebenfalls `--burst`
- Exit-Code 0 bei Erfolg, Exit-Code 1 bei Fehler

### Huey

- `Consumer(worker_type='process')` mit `--no-periodic` und einem aehnlichen Drain-Konzept
- Weniger explizit als RQ, aber aehnliche Idee

## 2.2 Erkennung leerer Queues in RabbitMQ

### Ansatz A: basic.get (Polling)

- `basic.get` holt eine einzelne Nachricht und liefert `GetEmpty` wenn die Queue leer ist
- **Vorteil:** Direktes Feedback ueber Leerstand
- **Nachteil:** Extrem ineffizient fuer normalen Betrieb (kein Prefetch, kein Push)
- **Fazit:** Nur als Pruefmechanismus geeignet, nicht als Haupt-Konsummethode

### Ansatz B: queue.declare (passive)

- `queue.declare` mit `passive=True` liefert `message_count` in der Antwort
- In aio-pika: `result = await channel.declare_queue('name', passive=True)` → `result.declaration_result.message_count`
- **Vorteil:** Keine Nachrichten werden konsumiert, reine Abfrage
- **Nachteil:** Eigener AMQP-Roundtrip pro Queue

### Ansatz C: Consumer mit Idle-Timeout

- Worker konsumiert normal per `basic.consume` mit Prefetch
- Parallel laeuft ein Timer, der bei Nachrichteneingang zurueckgesetzt wird
- Wenn der Timer N Sekunden ohne Nachricht ablaeuft → Burst-Bedingung erfuellt
- **Vorteil:** Kein Wechsel der Konsummethode, effizienter Betrieb
- **Nachteil:** „Leer" wird nicht sofort erkannt, sondern nach Timeout

## 2.3 Empfehlung fuer SwarmQ

**Ansatz C (Consumer mit Idle-Timeout)** kombiniert mit **Ansatz B (passive declare)** als Bestaetigung:

1. **Normaler Betrieb:** Worker konsumiert per `basic.consume` mit Prefetch
2. **Idle-Detection:** asyncio-Timer wird bei jeder verarbeiteten Nachricht zurueckgesetzt
3. **Bestaetigung:** Wenn der Idle-Timer ablaeuft, prueft der Worker einmalig per `queue.declare(passive=True)` alle ueberwachten Queues
4. **Shutdown:** Wenn alle Queues message_count=0 haben → Graceful Shutdown mit Exit-Code 0

**API und CLI:**
```bash
swarmq worker --burst                     # Standard-Timeout 5s
swarmq worker --burst --idle-timeout 10   # Benutzerdefiniertes Timeout
```

```python
worker = Worker(burst=True, burst_idle_timeout=5.0)
```

**Verhalten bei verzoegerten Tasks:**
- Burst-Modus beruecksichtigt standardmaessig **keine** verzoegerten Tasks (die liegen in Valkey, nicht in RabbitMQ)
- Optional: `--burst-drain-delayed` prueft zusaetzlich, ob verzoegerte Tasks in Valkey anstehen
- Sinnvoller Default: Burst-Modus betrachtet nur die RabbitMQ-Queues

**Exit-Codes:**
- `0` — alle Queues leer, alle laufenden Tasks erfolgreich abgeschlossen
- `1` — mindestens ein Task ist fehlgeschlagen
- `2` — Shutdown durch Signal (SIGTERM/SIGINT) waehrend der Verarbeitung

---

# 3. Health-Check-Endpunkte

## 3.1 Wie bestehende Bibliotheken das loesen

### arq

- `arq --check WorkerSettings` — CLI-Kommando, prueft ob der Worker laeuft und reagiert
- Liefert Metriken zurueck (verarbeitete Tasks, Queue-Tiefe)

### SAQ

- `--check` Flag fuer Liveness-Pruefung

### Celery + Flower

- `celery inspect ping` — prueft Worker-Responsivitaet ueber AMQP-Broadcast
- Flower HTTP-API: `/api/workers` Endpunkt mit detaillierten Worker-Infos
- Flower ist ein separater Prozess mit eigener Weboberfaeche

## 3.2 Kubernetes-Probes

Kubernetes unterscheidet drei Arten von Health-Checks:

| Probe     | Zweck                                    | Konsequenz bei Fehler          |
|-----------|------------------------------------------|--------------------------------|
| Liveness  | Lebt der Prozess noch?                   | Container wird neu gestartet   |
| Readiness | Kann der Worker neue Tasks annehmen?     | Kein Traffic mehr zum Pod      |
| Startup   | Hat die Initialisierung abgeschlossen?   | Liveness/Readiness deaktiviert |

**Wichtig fuer Task-Queue-Worker:**
- Liveness darf nicht fehlschlagen, nur weil ein langer Task laeuft
- Readiness sollte fehlschlagen, wenn Broker-Verbindung unterbrochen ist
- Startup sollte warten, bis Broker- und Backend-Verbindung stehen

## 3.3 Empfehlung fuer SwarmQ

### Eingebetteter HTTP-Server

Minimaler HTTP-Server auf Basis von `asyncio` (kein aiohttp noetig — `asyncio.start_server` mit manuellem HTTP-Parsing genuegt fuer zwei Endpunkte):

```python
worker = Worker(health_check_port=8080)  # Default: None (deaktiviert)
```

```bash
swarmq worker --health-port 8080
```

### Endpunkte

**GET /health (Liveness)**

Prueft: Event-Loop reagiert, Prozess lebt.

```json
{
  "status": "ok",
  "uptime_seconds": 3621,
  "pid": 12345
}
```

- Gibt 200 zurueck, solange der HTTP-Handler antworten kann
- Wenn der Event-Loop blockiert, kommt kein Response → Kubernetes erkennt Timeout

**GET /ready (Readiness)**

Prueft: Broker-Verbindung aktiv, Backend-Verbindung aktiv, Worker ist bereit.

```json
{
  "status": "ready",
  "broker_connected": true,
  "backend_connected": true,
  "active_tasks": 3,
  "last_task_processed_at": "2026-04-15T14:30:00Z"
}
```

- Gibt 200 nur zurueck, wenn Broker UND Backend verbunden sind
- Gibt 503 zurueck, wenn eine Verbindung fehlt

**Kubernetes-Konfiguration:**

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 2

startupProbe:
  httpGet:
    path: /ready
    port: 8080
  periodSeconds: 2
  failureThreshold: 30
```

### CLI Health-Check

```bash
swarmq worker --check --health-port 8080
```

- Verbindet sich zum lokalen Health-Endpunkt, gibt Status aus
- Exit-Code 0 wenn gesund, Exit-Code 1 wenn nicht erreichbar oder ungesund
- Verwendbar als Docker `HEALTHCHECK` oder Kubernetes `exec`-Probe

### Implementierungsdetails

- **Kein Framework noetig:** Roher asyncio TCP-Server mit minimalem HTTP/1.1-Parsing (nur GET, keine Body-Verarbeitung)
- **Thread-Sicherheit:** Health-Server laeuft im selben Event-Loop wie der Worker
- **Event-Loop-Responsivitaet:** Der Health-Endpunkt selbst testet implizit, ob der Event-Loop reagiert — wenn er blockiert, kommt kein Response
- **Overhead:** Vernachlaessigbar — keine Abhaengigkeiten, kein Routing-Framework
- **Port-Konflikt:** Bei mehreren Workern auf demselben Host muss jeder Worker einen eigenen Port nutzen. Default: `8080 + worker_index`

---

# 4. Worker-Autoscaling

## 4.1 Wie bestehende Bibliotheken das loesen

### Celery

- `--autoscale=max,min` (z.B. `--autoscale=10,3`)
- Autoscaler-Komponente im Worker-Prozess ueberwacht die Last
- **Skalierungslogik:**
  - Scale-up: Wenn alle Worker-Slots belegt sind → neuen Prozess starten
  - Scale-down: Wenn Worker-Slots laenger als `AUTOSCALE_KEEPALIVE` Sekunden idle sind → Prozess beenden
- Scale-up ist schnell (neuer Fork/Spawn), Scale-down wartet auf Idle-Timeout
- Basiert auf Pool-Groesse (Prefork-Pool), nicht auf Queue-Tiefe

## 4.2 Skalierungsmetriken

| Metrik           | Beschreibung                              | Eignung                       |
|------------------|-------------------------------------------|-------------------------------|
| Aktive Tasks     | Aktuell verarbeitete Tasks pro Worker     | Gut fuer lokale Entscheidung  |
| Queue-Tiefe      | Nachrichten in der RabbitMQ-Queue         | Gut fuer vorausschauendes Scaling |
| CPU-Auslastung   | Systemlast durch Worker-Prozesse          | Ergaenzend, nicht primaer     |
| Worker-Idle-Zeit | Zeit seit letztem verarbeitetem Task      | Gut fuer Scale-down           |

## 4.3 Empfehlung fuer SwarmQ

### ProcessManager mit Autoscaling

Der `ProcessManager` (bereits in der Worker-Architektur vorgesehen) steuert die Anzahl der Worker-Prozesse:

```bash
swarmq worker --autoscale 10,3    # max=10, min=3
swarmq worker --concurrency 5     # fest, kein Autoscaling
```

```python
worker = Worker(
    autoscale_max=10,
    autoscale_min=3,
    autoscale_cooldown=30.0,       # Sekunden zwischen Skalierungsereignissen
)
```

### Skalierungsalgorithmus

**Scale-up-Bedingung:**
1. Alle aktiven Worker haben mindestens einen laufenden Task (Auslastung > 0)
2. Diese Bedingung besteht seit `scale_up_delay` Sekunden (Default: 5s)
3. Aktuelle Worker-Anzahl < `autoscale_max`
4. Letzte Skalierung liegt laenger als `autoscale_cooldown` Sekunden zurueck

**Scale-down-Bedingung:**
1. Mindestens ein Worker hat seit `scale_down_delay` Sekunden keinen Task verarbeitet (Default: 30s)
2. Aktuelle Worker-Anzahl > `autoscale_min`
3. Letzte Skalierung liegt laenger als `autoscale_cooldown` Sekunden zurueck

**Scale-down-Ablauf:**
1. ProcessManager markiert den idle-sten Worker als "draining"
2. Worker verarbeitet keine neuen Tasks mehr (Prefetch auf 0 setzen bzw. Consumer canceln)
3. Warten bis laufende Tasks abgeschlossen sind
4. Worker-Prozess beenden

### Wichtige Design-Entscheidungen

- **Kein Queue-Tiefe-basiertes Scaling im Worker:** Queue-Tiefe eignet sich fuer externes Autoscaling (KEDA, HPA), aber nicht fuer internes — der Worker sieht nur seine eigene Last
- **Cooldown ist kritisch:** Ohne Cooldown pendelt das System bei Lastschwankungen staendig zwischen Scale-up und Scale-down (Flapping)
- **Graceful Scale-down:** Worker duerfen nie abrupt beendet werden — immer erst laufende Tasks abschliessen
- **Monitoring:** Skalierungsereignisse sollten geloggt und in Prometheus-Metriken erfasst werden

---

# 5. Prometheus-Metriken

## 5.1 Wie bestehende Bibliotheken das loesen

### Dramatiq

- Eingebaute Prometheus-Middleware (`dramatiq.middleware.Prometheus`)
- Metriken: `dramatiq_messages_total`, `dramatiq_message_duration_milliseconds`, `dramatiq_messages_retried_total`, `dramatiq_messages_rejected_total`
- HTTP-Server auf konfigurierbarem Port, Standardport 9191

### Celery + celery-exporter

- Separater Exporter-Prozess, der Celery-Events konsumiert
- Metriken: `celery_task_total`, `celery_task_runtime_seconds`, `celery_queue_length`
- Abgekoppelt vom Worker — eigener Deployment-Aufwand

### Flower

- `/metrics` Endpunkt integriert in Flower-Weboberfaeche
- Kombination aus Worker-Metriken und Queue-Metriken

## 5.2 Prometheus-Textformat

Das Prometheus-Exposition-Textformat ist einfach genug fuer eine eigene Implementierung:

```
# HELP swarmq_tasks_total Total number of tasks processed
# TYPE swarmq_tasks_total counter
swarmq_tasks_total{task="send_email",status="success"} 1027
swarmq_tasks_total{task="send_email",status="failure"} 3

# HELP swarmq_task_duration_seconds Task execution duration
# TYPE swarmq_task_duration_seconds histogram
swarmq_task_duration_seconds_bucket{task="send_email",le="0.01"} 0
swarmq_task_duration_seconds_bucket{task="send_email",le="0.1"} 512
swarmq_task_duration_seconds_bucket{task="send_email",le="1.0"} 980
swarmq_task_duration_seconds_bucket{task="send_email",le="+Inf"} 1027
swarmq_task_duration_seconds_sum{task="send_email"} 342.5
swarmq_task_duration_seconds_count{task="send_email"} 1027
```

**Escape-Regeln:**
- Label-Werte: `\` → `\\`, `"` → `\"`, Zeilenumbruch → `\n`
- HELP-Text: `\` → `\\`, Zeilenumbruch → `\n`

## 5.3 prometheus_client vs. eigene Implementierung

| Kriterium            | prometheus_client               | Eigene Implementierung         |
|----------------------|--------------------------------|-------------------------------|
| Abhaengigkeit        | Zusaetzliches Paket            | Keine                         |
| Korrektheit          | Erprobt, alle Edge-Cases       | Risiko bei Escaping, Histogrammen |
| Features             | Multiprocess-Mode, Push-GW     | Nur was wir brauchen          |
| Aufwand              | Null (pip install)             | ~200-300 Zeilen Code          |
| Wartung              | Community-maintained           | Eigene Verantwortung          |

## 5.4 Empfehlung fuer SwarmQ

**Optionale Abhaengigkeit `prometheus_client`** mit Fallback auf eigene minimale Implementierung:

```python
# swarmq/metrics.py
try:
    from prometheus_client import Counter, Histogram, Gauge, generate_latest
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False
    # Eigene minimale Implementierung (nur Textformat-Ausgabe)
```

### Metriken-Katalog

**Counter:**
- `swarmq_tasks_total{task, status}` — Gesamtzahl verarbeiteter Tasks (status: success, failure, retry, expired)
- `swarmq_tasks_retried_total{task}` — Anzahl der Wiederholungen
- `swarmq_tasks_rejected_total{task}` — Abgelehnte Tasks (z.B. nach max retries)

**Histogram:**
- `swarmq_task_duration_seconds{task}` — Ausfuehrungsdauer
  - Buckets: `[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300]`
- `swarmq_task_wait_seconds{task}` — Wartezeit in der Queue (Enqueue bis Start)

**Gauge:**
- `swarmq_active_tasks{task}` — Aktuell laufende Tasks
- `swarmq_worker_count` — Anzahl aktiver Worker-Prozesse
- `swarmq_broker_connected` — 1 wenn verbunden, 0 wenn nicht
- `swarmq_backend_connected` — 1 wenn verbunden, 0 wenn nicht

### Label-Kardinalitaet

**KRITISCH:** Nur Labels mit begrenzter Kardinalitaet verwenden:
- `task` — Taskname (begrenzt durch Code)
- `status` — Enum (success, failure, retry, expired, rejected)
- `queue` — Queue-Name (begrenzt durch Konfiguration)

**NIEMALS als Label verwenden:**
- `task_id` — Unbegrenzte Kardinalitaet, zerstoert Prometheus-Performance
- `args` / `kwargs` — Beliebige Werte
- `exception_message` — Unbegrenzte Varianz

### Exposition

Metriken werden ueber denselben HTTP-Server wie die Health-Checks bereitgestellt:

```
GET /metrics → Prometheus-Textformat (Content-Type: text/plain; version=0.004; charset=utf-8)
```

Kein separater Port, kein separater Prozess — alles auf dem Health-Check-Port.

### Multiprocess-Modus

Bei mehreren Worker-Prozessen muss jeder Prozess seine eigenen Metriken schreiben. Optionen:
- **prometheus_client Multiprocess-Mode:** Schreibt Metriken in ein gemeinsames Verzeichnis, der HTTP-Handler aggregiert beim Abruf
- **Eigene Loesung:** Jeder Worker-Prozess exponiert Metriken per Pipe/Socket an den Hauptprozess, der sie aggregiert und ueber HTTP bereitstellt

**Empfehlung:** `prometheus_client` mit `PROMETHEUS_MULTIPROC_DIR` — erprobt und genau fuer diesen Use-Case gebaut.

---

# 6. Bulk-Enqueue

## 6.1 Wie bestehende Bibliotheken das loesen

### RQ

- `queue.enqueue_many([Queue.prepare_data(func, args, kwargs), ...])` — Batch-Enqueue
- **Implementierung:** Verwendet intern eine Redis-Pipeline fuer atomare Batch-Operationen
- Alle Jobs werden in einem Roundtrip an Redis geschickt

### Celery

- `group(task.s(arg) for arg in args)()` — Celery-Group, aber kein echtes Batch-Publish
- Jeder Task wird einzeln per `basic_publish` geschickt

## 6.2 RabbitMQ-Batch-Strategien

### Strategie A: Einzelnes Publish mit Publisher Confirms (asynchron)

```python
for task in tasks:
    await channel.basic_publish(...)
# Warte auf alle Confirms
```

- **Performance:** 50.000 Nachrichten in ~6,3s (remote), ~4,1s (lokal)
- Asynchrone Confirms sind die schnellste Methode
- Nicht atomar — bei Fehler mitten in der Schleife sind einige Tasks publiziert, andere nicht

### Strategie B: Einzelnes Publish mit Batch-Confirm

```python
for task in batch:
    await channel.basic_publish(...)
    if count % batch_size == 0:
        await channel.wait_for_confirms()
```

- **Performance:** 50.000 Nachrichten in ~7,2s (remote), ~2,3s (lokal)
- Kompromiss zwischen Durchsatz und Fehlerhandling

### Strategie C: RabbitMQ-Transaktionen

- `tx.select()` → publishes → `tx.commit()`
- **Extrem langsam:** ~250x langsamer als ohne Transaktionen
- **Nicht empfohlen** — Publisher Confirms sind in jedem Szenario ueberlegen

## 6.3 Empfehlung fuer SwarmQ

**Asynchrones Batch-Publish mit Publisher Confirms + Valkey-Pipeline:**

```python
async def schedule_many(
    self,
    tasks: list[tuple[str, tuple, dict]],
) -> list[str]:
    """Bulk-Enqueue mehrerer Tasks."""
    task_ids = [generate_task_id() for _ in tasks]

    # 1. Valkey: Ergebnis-Slots per Pipeline registrieren
    async with self.backend.pipeline() as pipe:
        for task_id in task_ids:
            pipe.set(f"swarmq:result:{task_id}", "PENDING", ex=result_ttl)
        await pipe.execute()

    # 2. RabbitMQ: Tasks per basic_publish mit async Confirms
    for task_id, (name, args, kwargs) in zip(task_ids, tasks):
        message = encode_message(task_id, name, args, kwargs)
        await self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key_for(name),
            body=message,
        )

    # 3. Warte auf alle Publisher Confirms
    await self.channel.wait_for_confirms()

    return task_ids
```

**Design-Entscheidungen:**
- **Keine RabbitMQ-Transaktionen** — zu langsam, kein Vorteil gegenueber Confirms
- **Nicht atomar** — bewusste Entscheidung. Atomares Batch-Publish ist in RabbitMQ nicht performant moeglich. Bei Fehler mitten im Batch: bereits publizierte Tasks werden normal verarbeitet, fehlende koennen erneut geschickt werden (Idempotenz auf Task-Ebene).
- **Valkey-Pipeline** fuer Ergebnis-Registrierung — ein Roundtrip fuer alle SET-Operationen
- **Rueckgabe:** Liste aller `task_id`s, unabhaengig vom Erfolg einzelner Publishes
- **Fehlerbehandlung:** Bei Nack (negative Confirm) wird eine `BulkEnqueuePartialError` Exception geworfen, die die erfolgreichen und fehlgeschlagenen Task-IDs enthaelt

---

# 7. Task-Prioritaeten

## 7.1 RabbitMQ Priority Queues

### Classic Queues

- `x-max-priority` Argument bei Queue-Deklaration (empfohlen: max 10, technisch 1-255)
- Prioritaet wird per `priority`-Property auf der Nachricht gesetzt
- **Einschraenkungen:**
  - Jede Prioritaetsstufe erhoet den Speicher- und CPU-Verbrauch
  - Interaktion mit Prefetch: Bei hohem Prefetch werden Nachrichten in der Reihenfolge des Empfangs vom Consumer verarbeitet, nicht nach Prioritaet
  - Prioritaet wirkt nur innerhalb der Queue — wenn der Consumer schneller konsumiert als Nachrichten ankommen, gibt es effektiv keine Priorisierung

### Quorum Queues (ab RabbitMQ 4.0)

- **Nur zwei Prioritaetsstufen:** hoch und normal
- Nachrichten mit Prioritaet 0-4 → normal, Prioritaet > 4 → hoch
- **Keine Starvation:** Quorum Queues liefern im Verhaeltnis 2:1 (hoch zu normal), d.h. normale Nachrichten werden nie komplett ausgehungert
- Immer aktiv — keine Queue-Deklaration mit `x-max-priority` noetig
- **Kein feingranulares Prioritaetsspektrum** — nur binear (hoch/normal)

### Vergleich

| Aspekt                 | Classic Queue Priority          | Quorum Queue Priority     |
|------------------------|---------------------------------|---------------------------|
| Stufen                 | 1-255 (empf. max 10)           | 2 (hoch/normal)           |
| Starvation             | Moeglich (niedrige Prio verhungert) | Ausgeschlossen (2:1 Ratio) |
| Performance-Impact     | Spuerbar ab >5 Stufen          | Minimal                   |
| Queue-Typ              | Nur Classic                     | Nur Quorum                |
| Replikation            | Keine (oder Mirroring, deprecated) | Raft-Konsens            |

## 7.2 Alternative: Mehrere Queues fuer Prioritaetsstufen

```
high_queue   → Worker konsumiert zuerst
default_queue → Worker konsumiert wenn high_queue leer
low_queue    → Worker konsumiert wenn alle anderen leer
```

**Vorteile:**
- Funktioniert mit jedem Queue-Typ (Classic, Quorum, Stream)
- Einfach zu verstehen und zu debuggen
- Unabhaengige Monitoring-Metriken pro Prioritaet
- Kein Performance-Overhead durch Prioritaetslogik im Broker

**Nachteile:**
- Starvation moeglich (low_queue wird nie verarbeitet, wenn staendig high_queue-Nachrichten kommen)
- Komplexere Consumer-Logik (mehrere Queues managen)
- Keine feingranulare Priorisierung innerhalb einer Stufe

## 7.3 Empfehlung fuer SwarmQ

**Mehrere Queues fuer Prioritaetsstufen** — kombiniert mit gewichteter Consumption:

**Begruendung:**
1. SwarmQ verwendet Quorum Queues als Default → Classic-Queue-Prioritaeten scheiden aus
2. Quorum-Queue-Prioritaeten bieten nur 2 Stufen — zu wenig fuer viele Anwendungsfaelle
3. Separate Queues sind transparent, debugbar und funktionieren mit jedem Queue-Typ

**Drei Stufen genuegen:**

| Stufe    | Queue-Name              | Gewicht |
|----------|-------------------------|---------|
| HIGH     | `swarmq.{name}.high`    | 3       |
| DEFAULT  | `swarmq.{name}.default` | 2       |
| LOW      | `swarmq.{name}.low`     | 1       |

**Gewichtete Consumption (Anti-Starvation):**

Statt strenger Prioritaet (high immer vor default immer vor low) wird gewichtet konsumiert:
- Pro Zyklus werden Nachrichten im Verhaeltnis 3:2:1 (high:default:low) geholt
- Selbst bei voller high-Queue werden low-Tasks noch verarbeitet, nur langsamer
- Aehnlich dem Quorum-Queue-Ansatz von RabbitMQ 4.0, aber auf Anwendungsebene

**API:**

```python
@app.task(priority="high")
async def urgent_notification(user_id: str):
    ...

@app.task(priority="low")
async def generate_report(report_id: str):
    ...

# Oder dynamisch beim Enqueue:
task.apply_async(args=[...], priority="high")
```

**Implementierungsdetail:**
- Worker abonniert alle drei Queues per `basic.consume`
- Prefetch pro Queue gesteuert: high=6, default=4, low=2 (entspricht Gewichtung)
- Alternativ: Einzelner Consumer mit Prefetch auf Kanal-Ebene und Rotation zwischen den Queues

---

# 8. Task-Ablauf / Max Age

## 8.1 Wie bestehende Bibliotheken das loesen

### Dramatiq

- `max_age` Parameter in Millisekunden auf dem Actor
- **Implementierung:** Pruefung zum Zeitpunkt des Konsums
  - Nachricht enthaelt `message_timestamp` im Header
  - Worker prueft: `message_timestamp + max_age < now` → Task ueberspringen + ACK
- Einfach, zuverlaessig, aber abgelaufene Nachrichten belegen Queue-Platz bis zum Konsum

### RabbitMQ Per-Message-TTL

- `expiration` Property auf der Nachricht (String, Millisekunden)
- RabbitMQ entfernt die Nachricht aus der Queue wenn die TTL ablaeuft
- Bei konfiguriertem DLX: Nachricht wird an die Dead-Letter-Exchange weitergeleitet
- **WICHTIG:** TTL wird nur am Kopf der Queue geprueft! Nachrichten weiter hinten in der Queue koennen laenger leben als ihre TTL, bis sie an den Head-of-Queue ruecken.

## 8.2 Vergleich der Ansaetze

| Kriterium              | RabbitMQ TTL (expiration)       | Application-Level (wie Dramatiq) |
|------------------------|--------------------------------|----------------------------------|
| Queue-Platz            | Nachricht wird entfernt        | Nachricht bleibt bis Konsum      |
| Praezision             | Ungenau (Head-of-Queue-Problem)| Exakt (geprueft bei Verarbeitung)|
| Logging/Metriken       | Schwierig (Broker-seitig)      | Volle Kontrolle                  |
| DLQ-Integration        | Automatisch per DLX            | Manuell implementierbar          |
| Kompatibilitaet        | Abhaengig von Queue-Typ        | Universell                       |

## 8.3 Empfehlung fuer SwarmQ

**Beide Ansaetze kombiniert** — RabbitMQ TTL als Sicherheitsnetz, Application-Level als primaere Pruefung:

1. **Enqueue-Zeit:** `created_at` Timestamp wird in die Nachricht geschrieben
2. **RabbitMQ `expiration`:** Wird als String in Millisekunden auf der Nachricht gesetzt — dient als grobes Sicherheitsnetz, damit Nachrichten nicht ewig in der Queue liegen
3. **Application-Level-Check:** Beim Konsum prueft der Worker:
   ```python
   if msg.created_at + msg.max_age < time.monotonic():
       logger.warning("Task %s expired (age: %ds, max_age: %ds)",
                       msg.task_id, age, msg.max_age)
       metrics.tasks_total.labels(task=msg.task_name, status="expired").inc()
       await msg.ack()
       # Optional: In DLQ schieben
       if self.config.expired_to_dlq:
           await self.publish_to_dlq(msg, reason="expired")
       return
   ```

**API:**

```python
@app.task(max_age=3600)  # Task verfaellt nach 1 Stunde
async def send_verification_email(user_id: str):
    ...

# Oder dynamisch:
task.apply_async(args=[...], max_age=600)  # 10 Minuten
```

**Design-Entscheidungen:**
- **RabbitMQ-TTL als Puffer:** Auf `max_age * 1.5` gesetzt (50% laenger als das Application-Level-Limit), damit der Application-Level-Check immer zuerst greift, aber die Queue nicht endlos waechst wenn der Consumer nicht hinterherkommt
- **Abgelaufene Tasks loggen:** Immer — abgelaufene Tasks sind ein Zeichen fuer Kapazitaetsprobleme
- **Abgelaufene Tasks in DLQ:** Optional, per Konfiguration (`expired_to_dlq=True`). Nuetzlich fuer Debugging, aber nicht immer gewuenscht
- **Metriken:** Abgelaufene Tasks werden als `status="expired"` in `swarmq_tasks_total` gezaehlt
- **Zeitquelle:** `time.time()` fuer den Timestamp (nicht `monotonic_clock` — die Nachricht kommt von einem anderen Prozess/Host). Uhrzeitsynchronisation per NTP ist Voraussetzung.

---

# Quellen

- [RabbitMQ Release Information](https://www.rabbitmq.com/release-information)
- [RabbitMQ Delayed Message Exchange (archiviert)](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)
- [VMware Tanzu RabbitMQ 4.2 Delayed Queues](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-oci/4-2/tanzu-rabbitmq-oci-image/site-delayed-queues.html)
- [RabbitMQ Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)
- [RabbitMQ Priority Support in Queues](https://www.rabbitmq.com/docs/priority)
- [RabbitMQ Quorum Queue Priority Discussion #3977](https://github.com/rabbitmq/rabbitmq-server/discussions/3977)
- [RabbitMQ 4.0 Quorum Queue Features](https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0)
- [RabbitMQ Publisher Confirms Tutorial](https://www.rabbitmq.com/tutorials/tutorial-seven-java)
- [RabbitMQ Consumers Documentation](https://www.rabbitmq.com/docs/consumers)
- [CloudAMQP: basic.consume vs basic.get](https://www.cloudamqp.com/blog/rabbitmq-basic-consume-vs-rabbitmq-basic-get.html)
- [Valkey ZPOPMIN](https://valkey.io/commands/zpopmin/)
- [Prometheus Client Python](https://github.com/prometheus/client_python)
- [Prometheus Writing Client Libraries](https://prometheus.io/docs/instrumenting/writing_clientlibs/)
