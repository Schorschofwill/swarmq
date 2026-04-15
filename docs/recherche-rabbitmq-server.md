# Recherche: RabbitMQ Server-Konfiguration, Datentypen und Best Practices fuer SwarmQ

Stand: 2026-04-15 | RabbitMQ 4.x (4.0 / 4.1 / 4.2) | AMQP 0-9-1

---

## Inhaltsverzeichnis

1. [Queue-Typen -- Deep Dive](#1-queue-typen----deep-dive)
2. [Exchange-Typen und Routing-Muster](#2-exchange-typen-und-routing-muster)
3. [Message Properties und Persistenz](#3-message-properties-und-persistenz)
4. [Dead Letter Exchange (DLX) Patterns](#4-dead-letter-exchange-dlx-patterns)
5. [Performance Tuning](#5-performance-tuning)
6. [Sicherheit](#6-sicherheit)
7. [Monitoring und Observability](#7-monitoring-und-observability)
8. [Hochverfuegbarkeit](#8-hochverfuegbarkeit)
9. [Docker/Container Best Practices](#9-dockercontainer-best-practices)
10. [Konkrete Konfigurationsempfehlung fuer SwarmQ](#10-konkrete-konfigurationsempfehlung-fuer-swarmq)

---

## 1. Queue-Typen -- Deep Dive

### 1.1 Classic Queues

#### Versionshistorie

Ab RabbitMQ 4.0 existiert nur noch **CQv2** (Classic Queue Version 2). Die aeltere CQv1 wurde entfernt. Ebenso wurde **Classic Queue Mirroring** (HA-Mirroring) in 4.0 vollstaendig entfernt. Wer Replikation benoetigt, muss Quorum Queues verwenden.

#### Interne Architektur (CQv2)

Die Persistenzschicht besteht aus zwei Komponenten:

1. **Queue Index:** Speichert Metadaten (Sequenznummern, Delivery-Tags, Zustand). Verbraucht wenig Speicher.
2. **Per-Queue Message Store (neu in CQv2):** Kleine Nachrichten werden in einem dedizierten Store pro Queue gespeichert. Grosse Nachrichten gehen in den **Shared Message Store**, der von allen Queues geteilt wird.

**Speicherverhalten des Per-Queue Store:**
- Write Buffer: 512 KB
- Read Cache: 512 KB
- Gesamt pro Queue: bis zu 1 MB RAM-Overhead
- Beim Flush: Cache wird geleert, Write Buffer wird zum Cache

**Verbesserungen in CQv2 gegenueber CQv1:**
- Recovery-Zeit beim Node-Boot ist ca. 2x schneller (ab RabbitMQ 4.0)
- Bessere Speichereffizienz durch Per-Queue Store statt Shared Store fuer kleine Nachrichten
- Einheitliche Implementierung (kein v1/v2-Umschalten mehr ab 4.0)

#### Memory vs. Disk Verhalten

Classic Queues halten Nachrichten standardmaessig im RAM und schreiben sie bei Speicherdruck auf Disk. Das fuehrt zu:
- Hohem Speicherverbrauch bei grossen Backlogs
- Ploetzlichen Latenzspitzen wenn unter Speicherdruck auf Disk gewechselt wird

**Lazy Mode** (`x-queue-mode: lazy`):
- Nachrichten werden sofort auf Disk geschrieben
- Vorteil: Deutlich groessere Backlogs moeglich, vorhersagbarer Speicherverbrauch
- Nachteil: Hoeherer Latenz durch Disk I/O
- Ab RabbitMQ 3.12: `x-queue-mode: lazy` wird auch fuer CQv2 unterstuetzt

#### Mirroring (entfernt ab 4.0)

Classic Queue Mirroring (auch "HA-Queues" genannt) wurde mit RabbitMQ 4.0 **vollstaendig entfernt**. Es war seit 3.8 zugunsten von Quorum Queues deprecated. Gruende:
- Chain-Replikation war langsam und unzuverlaessig
- Kein echtes Consensus-Protokoll -- Split-Brain moeglich
- Keine At-Least-Once Garantie beim Dead-Lettering
- Synchronisation blockierte die Queue waehrend der Replikation

**Ersatz: Quorum Queues** (siehe 1.2)

#### Wann Classic Queues verwenden

| Szenario | Begruendung |
|---|---|
| **Entwicklung/Testen** | Einfacher, kein Cluster noetig, funktioniert auf Single-Node |
| **Temporaere Queues** | `exclusive=True` oder `auto_delete=True` (Quorum Queues unterstuetzen das nicht) |
| **Nachrichtenprioritaet (klassisch)** | Classic Queues unterstuetzen 0-255 Prioritaetslevel mit strikter Reihenfolge |
| **Global QoS** | Nur Classic Queues unterstuetzen `global_=True` bei `set_qos()` |
| **Per-Message TTL mit exakter Auswertung** | Classic Queues werten TTL an der Queue-Spitze aus |

### 1.2 Quorum Queues

#### Raft Consensus -- Funktionsweise

Quorum Queues basieren auf dem **Raft Consensus Algorithm**. Jede Quorum Queue ist ein eigenstaendiger Raft-Cluster:

**Architektur:**
- **Leader:** Empfaengt alle Schreiboperationen (publish, ack, nack)
- **Follower:** Replizieren den Zustand des Leaders
- **Multi-Raft:** RabbitMQ nutzt eine Multi-Raft-Implementierung, bei der die Kommunikation mehrerer Raft-Cluster gebatcht wird, um den Netzwerk-Overhead zu reduzieren

**Nachrichtenverarbeitung:**
1. Nachricht kommt beim Leader an
2. Leader schreibt in sein Write-Ahead Log (WAL) und persistiert auf Disk
3. **Parallel:** Leader sendet Replikations-Befehle an alle Follower
4. Follower bestaetigen den Empfang
5. Sobald eine **Mehrheit** (Quorum = N/2+1) bestaetigt hat, gilt die Nachricht als "committed"
6. Erst dann wird der Publisher Confirm an den Client gesendet

**Leader Election:**
- Bei Leader-Ausfall waehlen die Follower per Raft-Protokoll einen neuen Leader
- Nur Follower mit dem aktuellsten Log koennen Leader werden
- Wahl dauert typischerweise 1-3 Sekunden

#### Replication Factor

| Cluster-Groesse | Replication Factor | Tolerierte Ausfaelle | Quorum |
|---|---|---|---|
| 1 Node | 1 | 0 | 1 |
| 3 Nodes | 3 (Default) | 1 | 2 |
| 5 Nodes | 5 | 2 | 3 |
| 7 Nodes | 7 | 3 | 4 |

**Empfehlung:** 3 oder 5 Replicas. Mehr als 5 bringt wenig Gewinn bei deutlich mehr Overhead.

**Was passiert bei Node-Ausfaellen:**
- **1 Node down (3er-Cluster):** Queue bleibt verfuegbar. Neuer Leader wird gewaehlt wenn noetig.
- **2 Nodes down (3er-Cluster):** Queue wird **unavailable**. Kein Quorum mehr moeglich.
- **Wieder-Beitritt:** Ausgefallener Node synchronisiert sich automatisch mit dem Leader.

#### Performance-Charakteristik vs. Classic Queues

| Metrik | Classic Queue | Quorum Queue |
|---|---|---|
| **Publish-Latenz** | Niedriger | Hoeher (Replikation) |
| **Throughput (Single Node)** | Hoeher | Ca. 70-80% von Classic |
| **Throughput (Cluster)** | Kein nativer Support | Gut (repliziert) |
| **Speicherverbrauch** | Variabel (RAM-basiert) | Vorhersagbar (lazy-artig) |
| **Recovery-Zeit** | Schnell | Abhaengig von Log-Groesse |
| **Latenz-Varianz** | Kann schwanken | Stabiler |

**Wichtig:** Moderne Quorum Queues (ab 3.12+) sind deutlich schneller als fruehe Versionen. Der Performance-Unterschied zu Classic Queues ist in vielen Szenarien vernachlaessigbar.

#### Delivery Limit (Poison Message Handling)

Ab RabbitMQ 4.0: **Default Delivery Limit = 20**

Das bedeutet: Eine Nachricht, die 20 Mal redelivered (nack + requeue) wurde, wird automatisch:
- An die Dead Letter Exchange (DLX) weitergeleitet, **wenn konfiguriert**
- **Verworfen (dropped)**, wenn keine DLX konfiguriert ist

**KRITISCH fuer SwarmQ:** Immer eine DLX konfigurieren! Ohne DLX gehen Nachrichten nach 20 Zustellversuchen verloren.

```
# Konfigurierbar pro Queue:
x-delivery-limit: 5     # SwarmQ-Empfehlung: 5 (konfigurierbar)
```

#### Dead Lettering -- At-Least-Once (ab RabbitMQ 3.13)

Quorum Queues verwenden **intern Publisher Confirms** fuer Dead-Lettering. Das bedeutet:
- Die Nachricht wird erst aus der Quorum Queue entfernt, wenn die DLQ den Empfang bestaetigt hat
- **At-Least-Once Garantie:** Keine Nachricht geht beim Dead-Lettering verloren

Classic Queues haben diese Garantie **nicht** -- dort ist Dead-Lettering "best effort".

```
# Explizit aktivieren (empfohlen):
x-dead-letter-strategy: at-least-once
```

#### Speicher-Management

Quorum Queues verhalten sich aehnlich wie Lazy Queues:
- Nachrichten werden schnell auf Disk persistiert
- Kein expliziter Lazy Mode noetig
- In-Memory wird nur ein begrenztes Fenster gehalten
- **Prefetch 0 wird auf max. 2000 begrenzt** (anders als Classic Queues wo 0 = unlimited)

#### Einschraenkungen

| Feature | Status |
|---|---|
| **Prioritaet (klassisch, 0-255)** | Nicht unterstuetzt |
| **Prioritaet (4.0+)** | Nur 2 Level: normal (0-4) und high (5+) |
| **Global QoS** | Nicht unterstuetzt (nur per-Consumer) |
| **exclusive** | Nicht unterstuetzt |
| **auto_delete** | Nicht unterstuetzt |
| **Non-durable** | Nicht unterstuetzt (immer durable) |
| **TTL exakte Auswertung** | Lazy evaluation (nicht an Queue-Spitze) |
| **Poison Message ohne DLX** | Nachrichten werden nach Delivery-Limit verworfen |

#### Prioritaet in Quorum Queues (ab 4.0)

Quorum Queues haben ein **2-Level-Prioritaetssystem:**

- **Normal:** Nachrichten mit Prioritaet 0-4 (oder ohne Prioritaet)
- **High:** Nachrichten mit Prioritaet 5+

**Auslieferungsverhaeltnis:** 2 High-Priority Nachrichten pro 1 Normal-Priority Nachricht. Das verhindert Starvation -- normale Nachrichten werden immer zugestellt, auch wenn viele High-Priority Nachrichten in der Queue sind.

**Unterschied zu Classic Queues:** Classic Queues liefern immer zuerst alle hoeherprioren Nachrichten aus (strikte Prioritaet), was zu Starvation fuehren kann.

Ab RabbitMQ 4.2: **Strikte Prioritaets-Queues** mit per-Priority Message Counts, korrekter Redelivery-Reihenfolge und prioritaetsbewusstem Message-Ablauf.

#### Wann Quorum Queues die RICHTIGE Wahl sind

- **Produktion mit Ausfallsicherheit:** Replikation + Raft = keine Datenverluste
- **Task Queues:** Genau der Anwendungsfall von SwarmQ
- **At-Least-Once Delivery:** Garantierte Zustellung
- **Dead Letter Queues:** At-Least-Once DLQ (ab 3.13)
- **Poison Message Handling:** Automatisches Delivery Limit
- **Cluster-Betrieb:** Native Replikation ohne Mirroring

### 1.3 Streams

#### Grundkonzept

Streams sind ein **Append-Only Log** mit nicht-destruktivem Lesen:
- Nachrichten werden nie aus dem Stream entfernt (nur per Retention Policy)
- Mehrere Consumer koennen denselben Stream an verschiedenen Offsets lesen
- Consumer koennen Nachrichten **replaten** (zurueck zu einem frueheren Offset springen)

#### Unterschiede zu Queues

| Merkmal | Queue | Stream |
|---|---|---|
| **Semantik** | Nachricht wird konsumiert und entfernt | Nachricht bleibt erhalten |
| **Consumer** | Nachricht an genau einen Consumer | Alle Consumer lesen alle Nachrichten |
| **Replay** | Nicht moeglich | Jederzeit moeglich |
| **Throughput** | Gut | Sehr hoch (Millionen msg/s) |
| **Ack-Semantik** | Consumer ackt einzelne Nachrichten | Consumer trackt Offset |
| **Speicher** | Nachrichten werden nach Ack entfernt | Retention-basiert (Zeit/Groesse) |

#### Streams fuer Task Queues -- Bewertung

**Streams sind NICHT geeignet fuer SwarmQ:**

1. **Kein exklusiver Konsum:** Alle Consumer lesen alle Nachrichten. Bei einem Task-Queue soll aber jeder Task von genau einem Worker verarbeitet werden.
2. **Kein Ack-Mechanismus:** Es gibt kein "diese Nachricht wurde verarbeitet". Der Consumer trackt nur seinen Offset.
3. **Kein Dead-Lettering:** Fehlgeschlagene Nachrichten koennen nicht an eine DLX weitergeleitet werden.
4. **Kein Requeue:** Nachrichten koennen nicht zurueck in den Stream gelegt werden.
5. **Overhead:** Nachrichten werden nie geloescht, was fuer eine Task Queue unnoetig Speicherplatz verbraucht.

**Streams sind geeignet fuer:**
- Event Sourcing
- Audit Logs
- Fan-Out an viele Consumer
- Replay-Szenarien (z.B. Daten-Reprozessierung)

**Fazit fuer SwarmQ:** Streams kommen nicht in Frage. Quorum Queues sind die richtige Wahl.

### 1.4 Zusammenfassende Empfehlung fuer SwarmQ

| Umgebung | Queue-Typ | Begruendung |
|---|---|---|
| **Produktion** | Quorum Queues | Replikation, At-Least-Once DLQ, Delivery Limit |
| **Entwicklung (Single Node)** | Quorum Queues | Funktionieren auch auf Single-Node (Replication Factor 1) |
| **Tests (CI/CD)** | Quorum Queues | Konsistentes Verhalten wie in Produktion |
| **Temporaere Queues** | Classic Queues | Fuer exclusive/auto_delete Szenarien |

SwarmQ sollte **Quorum Queues als Default** verwenden und Classic Queues nur fuer Spezialfaelle unterstuetzen (z.B. temporaere Reply-Queues bei RPC-Pattern).

---

## 2. Exchange-Typen und Routing-Muster

### 2.1 Alle Exchange-Typen im Ueberblick

#### a) Direct Exchange

- **Routing:** Exakter Match des Routing-Keys mit dem Binding-Key
- **Performance:** Schnell -- einfacher String-Vergleich
- **Anwendung:** Task-Queue mit benannten Queues

```
Exchange: swarmq.tasks (direct)
    |
    |-- routing_key="default"    --> Queue: swarmq.tasks.default
    |-- routing_key="high-prio"  --> Queue: swarmq.tasks.high-prio
    |-- routing_key="cpu"        --> Queue: swarmq.tasks.cpu
```

#### b) Topic Exchange

- **Routing:** Pattern-basiert mit Wildcards (`*` = ein Wort, `#` = null oder mehr)
- **Performance:** Etwas langsamer als Direct (Pattern-Matching statt String-Vergleich), aber seit RabbitMQ 2.4.0 wurde der Algorithmus um Faktor 60 optimiert. In der Praxis kein signifikanter Unterschied.
- **Anwendung:** Hierarchisches Routing (z.B. `tasks.payment.high`)

```
Exchange: swarmq.events (topic)
    |
    |-- routing_key="tasks.*.completed" --> Queue fuer alle Task-Completions
    |-- routing_key="tasks.payment.#"   --> Queue fuer alle Payment-Events
```

#### c) Fanout Exchange

- **Routing:** Broadcast an alle gebundenen Queues, Routing-Key wird ignoriert
- **Performance:** Am schnellsten (kein Matching noetig)
- **Anwendung:** Broadcast-Notifications, Worker-Shutdown-Signale

```
Exchange: swarmq.broadcast (fanout)
    |
    |-- --> Queue: worker-1-notifications
    |-- --> Queue: worker-2-notifications
    |-- --> Queue: worker-3-notifications
```

#### d) Headers Exchange

- **Routing:** Basierend auf Message-Headers statt Routing-Key
- **Performance:** Langsamer (Header-Vergleich)
- **Anwendung:** Komplexes attributbasiertes Routing

```
Exchange: swarmq.headers (headers)
    |
    |-- x-match=all, priority=high, type=payment --> Queue: payment-high-prio
```

#### e) Default Exchange (nameless, "")

- **Routing:** Automatisches Direct-Routing: Routing-Key = Queue-Name
- **Performance:** Identisch mit Direct Exchange
- **Besonderheit:** Immer vorhanden, muss nicht deklariert werden, kann nicht geloescht/modifiziert werden

```
Default Exchange ("")
    |
    |-- routing_key="swarmq.tasks.default"    --> Queue: swarmq.tasks.default
    |-- routing_key="swarmq.tasks.high-prio"  --> Queue: swarmq.tasks.high-prio
```

### 2.2 Performance-Vergleich

| Exchange-Typ | Matching | Relative Performance | Overhead |
|---|---|---|---|
| **Default** | Routing-Key = Queue-Name | Schnellste | Minimal |
| **Fanout** | Kein Matching | Sehr schnell | Minimal |
| **Direct** | Exakter String-Match | Schnell | Gering |
| **Topic** | Pattern-Match | Gut (optimiert seit 2.4.0) | Moderat |
| **Headers** | Multi-Attribut-Match | Langsamer | Hoeher |

### 2.3 Empfehlung fuer SwarmQ

**Primaer: Default Exchange fuer Task-Dispatch**

Fuer SwarmQ's einfaches Queue-Routing (Task an eine bestimmte Queue) ist die **Default Exchange** optimal:

- Kein zusaetzlicher Exchange muss deklariert werden
- Routing-Key = Queue-Name (z.B. `swarmq.tasks.default`)
- Minimaler Overhead
- Einfachste Konfiguration

**Alternative: Direct Exchange fuer explizites Routing**

Falls spaeter erweitertes Routing gewuenscht ist (z.B. ein Task geht an mehrere Queues), kann ein Direct Exchange verwendet werden. Die Migration ist einfach.

**Zukunft: Topic Exchange fuer Monitoring-Events**

Fuer Task-Lifecycle-Events (z.B. `task.completed`, `task.failed`) kann spaeter ein Topic Exchange ergaenzt werden. Das betrifft aber nicht den Kern-Task-Dispatch.

**Begruendung gegen andere Typen:**
- **Fanout:** Nicht geeignet fuer Task-Routing (ein Task soll an genau eine Queue)
- **Headers:** Unnoetiger Overhead fuer den Anwendungsfall
- **Topic:** Moeglich, aber Overkill fuer einfaches Queue-Routing. Lohnt sich erst bei hierarchischem Routing (z.B. `tasks.payment.high`)

---

## 3. Message Properties und Persistenz

### 3.1 AMQP 0-9-1 Basic Properties

Die 14 vordefinierten Message Properties in AMQP 0-9-1:

| Property | Typ | SwarmQ-Nutzung | Wert |
|---|---|---|---|
| `delivery_mode` | uint8 | **Ja** | `2` (persistent) fuer AT_LEAST_ONCE, `1` (transient) fuer FIRE_AND_FORGET |
| `content_type` | string | **Ja** | `"application/json"` |
| `content_encoding` | string | **Ja** | `"utf-8"` |
| `message_id` | string | **Ja** | UUID7 der Task-ID |
| `correlation_id` | string | **Spaeter** | Fuer Result-Korrelation (z.B. bei `get_result()`) |
| `reply_to` | string | **Spaeter** | Fuer RPC-Pattern (Reply-Queue-Name) |
| `expiration` | string | **Ja** | Per-Message TTL basierend auf `hard_timeout` (in ms als String) |
| `priority` | uint8 | **Optional** | Fuer Quorum Queues: 0-4 = normal, 5+ = high |
| `headers` | table | **Ja** | Custom Metadata (Retry-Count, Original-Queue, etc.) |
| `timestamp` | uint64 | **Ja** | Unix Timestamp der Nachrichtenerstellung |
| `type` | string | **Ja** | Task-Name (z.B. `"example_task"`) |
| `app_id` | string | **Optional** | `"swarmq"` |
| `cluster_id` | string | Nein | Deprecated |
| `user_id` | string | Nein | Fuer Auth-Validierung, nicht fuer SwarmQ relevant |

### 3.2 delivery_mode im Detail

| Mode | Wert | Verhalten | SwarmQ-Mapping |
|---|---|---|---|
| **Transient** | 1 | Nachricht nur im RAM. Geht bei Broker-Restart verloren. | `FIRE_AND_FORGET` |
| **Persistent** | 2 | Nachricht wird auf Disk geschrieben. Ueberlebt Broker-Restart. | `AT_LEAST_ONCE` |

**Voraussetzung fuer Persistenz:** Alle drei muessen `durable=True` bzw. persistent sein:
1. Exchange: `durable=True`
2. Queue: `durable=True`
3. Message: `delivery_mode=2`

**Performance-Impact von delivery_mode=2:**
- Ca. 10-20% Throughput-Reduktion durch Disk I/O
- Bei Quorum Queues minimal, da ohnehin auf Disk geschrieben wird
- Bei `FIRE_AND_FORGET`: `delivery_mode=1` spart Disk I/O (sinnvoll da Nachricht ohnehin verloren gehen darf)

### 3.3 Empfohlene Message Properties fuer SwarmQ

```python
# AT_LEAST_ONCE Task Message
message = aio_pika.Message(
    body=json_payload,                              # JSON-encoded Task
    delivery_mode=aio_pika.DeliveryMode.PERSISTENT, # delivery_mode=2
    content_type="application/json",
    content_encoding="utf-8",
    message_id=str(task_id),                        # UUID7
    type=task_name,                                  # "example_task"
    timestamp=datetime.now(UTC),
    expiration=hard_timeout * 1000,                  # Millisekunden
    headers={
        "swarmq-version": "1",
        "swarmq-queue": queue_name,
        "swarmq-delivery": "at_least_once",
    },
)

# FIRE_AND_FORGET Task Message
message = aio_pika.Message(
    body=json_payload,
    delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT, # delivery_mode=1
    content_type="application/json",
    content_encoding="utf-8",
    message_id=str(task_id),
    type=task_name,
    timestamp=datetime.now(UTC),
    headers={
        "swarmq-version": "1",
        "swarmq-queue": queue_name,
        "swarmq-delivery": "fire_and_forget",
    },
)
```

### 3.4 Priority -- Praktische Limits

| Queue-Typ | Prioritaets-Level | Verhalten |
|---|---|---|
| **Classic Queue** | 0-255 (konfigurierbar, max empfohlen: 10) | Strikte Reihenfolge (hoeher = zuerst) |
| **Quorum Queue (4.0+)** | 2 Level: normal (0-4) und high (5+) | Ratio 2:1 (high:normal), keine Starvation |
| **Quorum Queue (4.2+)** | Strikt mit per-Priority Counts | Verbesserte Reihenfolge |

**Empfehlung fuer SwarmQ:** Das 2-Level-System der Quorum Queues ist fuer eine Task Queue ausreichend. Falls feingranulare Prioritaeten benoetigt werden, separate Queues verwenden (z.B. `swarmq.tasks.high`, `swarmq.tasks.default`, `swarmq.tasks.low`) statt Message-Prioritaeten.

---

## 4. Dead Letter Exchange (DLX) Patterns

### 4.1 DLX Setup

```
┌─────────────────────────┐     reject/expire/limit     ┌─────────────────────┐
│  swarmq.tasks.default   │ ──────────────────────────>  │     swarmq.dlx      │
│  (Quorum Queue)         │                              │  (Direct Exchange)  │
│                         │                              │                     │
│  x-dead-letter-exchange │                              │                     │
│  = "swarmq.dlx"         │                              └──────────┬──────────┘
│                         │                                         │
│  x-dead-letter-routing  │                  routing_key="swarmq.tasks.default.dead"
│  -key = "swarmq.tasks.  │                                         │
│  default.dead"          │                              ┌──────────▼──────────┐
│                         │                              │ swarmq.tasks.       │
│  x-delivery-limit = 5   │                              │ default.dead        │
│                         │                              │ (Classic/Quorum Q)  │
└─────────────────────────┘                              └─────────────────────┘
```

### 4.2 Wann werden Nachrichten dead-lettered?

| Ausloeser | Beschreibung | SwarmQ-Relevanz |
|---|---|---|
| **Rejection** | `reject(requeue=False)` oder `nack(requeue=False)` | Task-Fehler nach Verarbeitung |
| **Expiration** | Per-Message TTL oder Queue-TTL abgelaufen | Tasks die zu lange in der Queue warten |
| **Max-Length** | Queue-Laengenlimit ueberschritten (`x-max-length`) | Backpressure |
| **Delivery Limit** | Nachricht wurde x-mal redelivered (Quorum Queues) | Poison Messages |

### 4.3 Retry-Pattern mit DLX + TTL (Delayed Retry)

Dieses Pattern nutzt eine Retry-Queue mit TTL, die Nachrichten nach Ablauf zurueck an die Haupt-Queue leitet:

```
┌──────────────────┐   reject    ┌──────────────────┐   TTL abgelaufen  ┌──────────────────┐
│  swarmq.tasks.   │ ─────────>  │  swarmq.retry.   │ ────────────────> │  swarmq.tasks.   │
│  default         │             │  default         │                   │  default         │
│                  │             │                  │                   │                  │
│  DLX = retry.ex  │             │  x-message-ttl   │                   │                  │
│                  │             │  = 30000 (30s)   │                   │                  │
│                  │             │                  │                   │                  │
│                  │             │  DLX = tasks.ex  │                   │                  │
└──────────────────┘             └──────────────────┘                   └──────────────────┘
```

**Ablauf:**
1. Task schlaegt fehl -> `reject(requeue=False)` -> Nachricht geht an Retry Exchange
2. Nachricht landet in Retry-Queue mit TTL (z.B. 30 Sekunden)
3. Nach TTL-Ablauf -> Nachricht geht an DLX der Retry-Queue -> zurueck an Task-Queue
4. Task wird erneut verarbeitet

### 4.4 Multi-Level Retry mit steigenden Delays

Fuer exponentielle Backoff-Strategien werden mehrere Retry-Queues mit unterschiedlichen TTLs erstellt:

```
Fehlgeschlagener Task
    │
    ├── 1. Versuch fehlgeschlagen -> retry-30s Queue (TTL=30s)
    │       └── nach 30s zurueck an Task-Queue
    │
    ├── 2. Versuch fehlgeschlagen -> retry-120s Queue (TTL=2min)
    │       └── nach 2min zurueck an Task-Queue
    │
    ├── 3. Versuch fehlgeschlagen -> retry-600s Queue (TTL=10min)
    │       └── nach 10min zurueck an Task-Queue
    │
    └── 4. Versuch fehlgeschlagen -> Dead Letter Queue (endgueltig)
```

**Tracking der Retry-Anzahl:**
- RabbitMQ fuegt automatisch einen `x-death` Header hinzu, der die Anzahl der Dead-Letter-Zyklen zaehlt
- Der Header enthaelt: `count`, `reason`, `queue`, `exchange`, `routing-keys`, `time`
- SwarmQ kann diesen Header auswerten um die aktuelle Retry-Stufe zu bestimmen

**ACHTUNG:** Beim Dead-Lettering wird der `expiration` Header entfernt und durch `original-expiration` ersetzt. Ein Task mit 1-Stunden-TTL koennte nach dem Retry ohne TTL in der Queue landen und dort unbegrenzt bleiben. SwarmQ muss den TTL nach dem Retry ggf. neu setzen.

### 4.5 Delayed Retry in Quorum Queues (ab RabbitMQ 4.2)

RabbitMQ 4.2 fuehrt **native Delayed Retry** fuer Quorum Queues ein -- ohne separate Retry-Queues:

```
# Queue Arguments:
x-delayed-retry-type: "failed"    # disabled | all | failed | returned
x-delayed-retry-min: 10000         # Minimale Verzoegerung in ms (10s)
x-delayed-retry-max: 300000        # Maximale Verzoegerung in ms (5min)

# Oder per Policy:
delayed-retry-type: "failed"
delayed-retry-min: 10000
delayed-retry-max: 300000
```

**Retry-Typen:**
- `disabled` (Default): Kein Delayed Retry
- `all`: Alle redelivered Nachrichten werden verzoegert
- `failed`: Nur nack'd Nachrichten werden verzoegert
- `returned`: Nur returned (mandatory) Nachrichten werden verzoegert

**Vorteil:** Kein separates Retry-Queue-Setup noetig. Der Backoff wird automatisch basierend auf der Delivery-Count berechnet.

**Empfehlung fuer SwarmQ:**
- Wenn RabbitMQ 4.2+ garantiert ist: Native Delayed Retry nutzen
- Sonst: Multi-Level Retry mit DLX + TTL implementieren
- In beiden Faellen: Immer eine finale DLQ fuer dauerhaft fehlgeschlagene Tasks

### 4.6 DLQ Monitoring

Wichtige Metriken fuer Dead Letter Queues:
- **Queue Depth der DLQ:** Wie viele fehlgeschlagene Tasks gibt es?
- **Eingangsrate der DLQ:** Steigt die Fehlerrate?
- **Alter der Nachrichten in der DLQ:** Wie lange liegen fehlgeschlagene Tasks schon?

**Empfehlung:** Alarm wenn DLQ-Tiefe einen Schwellwert ueberschreitet (z.B. > 100 Nachrichten).

### 4.7 At-Least-Once Dead Lettering (Quorum Queues 3.13+)

In Quorum Queues ist Dead-Lettering **at-least-once zuverlaessig:**
- Die Quorum Queue behaelt die Nachricht so lange, bis die DLQ den Empfang bestaetigt
- Intern wird ein Publisher Confirm zwischen Quorum Queue und DLQ verwendet
- Die Nachricht wird erst nach Bestaetigung aus der Quorum Queue entfernt

Classic Queues bieten diese Garantie nicht -- dort ist Dead-Lettering "fire-and-forget".

```
# Explizit aktivieren (empfohlen):
x-dead-letter-strategy: at-least-once
```

---

## 5. Performance Tuning

### 5.1 Prefetch (QoS)

#### Empfohlene Werte fuer SwarmQ

| Task-Typ | Verarbeitungszeit | Empfohlener Prefetch | Begruendung |
|---|---|---|---|
| **I/O-bound async** (HTTP, DB) | 10-100ms | 10-50 | Viele Tasks koennen parallel auf I/O warten |
| **I/O-bound async** (langsam) | > 100ms | 5-20 | Weniger vorausladen |
| **CPU-bound** | Variabel | 1-2 | Nicht mehr als verarbeitet werden kann |
| **Gemischt** | Variabel | 5-10 | Kompromiss |

**Empfehlung fuer SwarmQ:**
- **Async Worker:** `prefetch_count=10` als Default (konfigurierbar)
- **CPU Worker:** `prefetch_count=1`
- Empirisches Tuning: Wert schrittweise erhoehen und Throughput/Latenz messen

#### Global vs. Per-Consumer Prefetch

| Modus | Beschreibung | Quorum Queue Support |
|---|---|---|
| **Per-Consumer** (`global_=False`) | Limit pro Consumer auf dem Channel | **Ja** |
| **Global** (`global_=True`) | Limit fuer alle Consumer auf dem Channel | **Nein** |

**KRITISCH:** Quorum Queues unterstuetzen **kein Global QoS**. Ein Versuch mit `global_=True` fuehrt zu einem Channel-Fehler. Da SwarmQ Quorum Queues als Default nutzt, immer `global_=False` (Default) verwenden.

**Quorum Queue Spezialfall:** `prefetch_count=0` wird automatisch auf max. 2000 begrenzt.

#### Fair Dispatching

RabbitMQ verteilt Nachrichten Round-Robin an Consumer. Mit Prefetch > 1 kann es passieren, dass ein langsamer Consumer viele Nachrichten vorhaelt, waehrend schnelle Consumer idle sind.

**Loesung:** Prefetch Count angemessen niedrig halten. Bei `prefetch_count=1` ist die Verteilung am fairsten, aber der Throughput am niedrigsten.

### 5.2 Connection und Channel Management

#### Connections pro Prozess

| Regel | Empfehlung |
|---|---|
| **Ein Connection pro Prozess** | Standard-Empfehlung, SwarmQ-Default |
| **Mehrere Connections** | Nur wenn > 100 Channels pro Connection noetig |
| **Connection-Sharing ueber Prozesse** | **VERBOTEN** -- nicht fork-safe |

**SwarmQ:** Ein Worker-Prozess = Eine `RobustConnection`.

#### Channels pro Connection

| Limit | Wert | Quelle |
|---|---|---|
| **Protokoll-Maximum** | 65535 Channels pro Connection | AMQP 0-9-1 Spec |
| **Praktische Empfehlung** | Max. 100 Channels pro Connection | RabbitMQ Team |
| **SwarmQ-Bedarf** | ~10-20 Channels (Consumers + Publisher Pool + Admin) | Geschaetzt |

**Channel-Aufteilung fuer SwarmQ:**
- 1 Channel pro Queue-Consumer (z.B. 3 Queues = 3 Channels)
- 5-10 Channels im Publisher-Pool fuer `broker.schedule()`
- 1 Admin-Channel fuer Queue/Exchange-Deklaration

#### Heartbeat Tuning

| Parameter | Default | Empfehlung |
|---|---|---|
| `heartbeat` | 60 Sekunden | 60s fuer die meisten Szenarien |
| Heartbeat-Interval | `heartbeat / 2` = 30s | Automatisch |
| Stuck-Erkennung | `heartbeat * 3` = 180s | Automatisch |

**Wann aendern:**
- **Cloud/Container:** 30s (schnellere Erkennung von Connection-Verlust)
- **Langsame Netzwerke:** 120s (vermeidet False Positives)
- **Niemals 0** setzen (deaktiviert Heartbeat, tote Connections werden nicht erkannt)

### 5.3 Publisher Performance

#### Publisher Confirms vs. Transactions

| Methode | Throughput (typisch) | Latenz | Atomaritaet |
|---|---|---|---|
| **Kein Confirm** | ~50.000 msg/s | Minimal | Keine |
| **Publisher Confirms** | ~30.000-40.000 msg/s | Niedrig | Pro Nachricht |
| **Transactions** | ~5.000-10.000 msg/s | Hoch | Pro Transaktion |

**Empfehlung fuer SwarmQ:** Immer Publisher Confirms (Default in aio-pika).

#### Batch Publishing

aio-pika hat kein natives Batch-Publish, aber mit `asyncio.gather()` lassen sich mehrere Publishes parallel ausfuehren:

```python
# Parallelisiertes Publishing
await asyncio.gather(*[
    exchange.publish(msg, routing_key=key)
    for msg, key in messages
])
```

#### mandatory Flag

| Wert | Verhalten | Performance-Impact |
|---|---|---|
| `True` | Fehler wenn keine Queue den Routing-Key matcht | Gering (nur Pruefung) |
| `False` | Nachricht wird still verworfen wenn kein Match | Keiner |

**Empfehlung fuer SwarmQ:** `mandatory=True` fuer Task-Nachrichten. Der Publisher soll wissen wenn eine Queue nicht existiert.

#### Flow Control

RabbitMQ hat mehrere Schutzmechanismen:
1. **Credit-based Flow Control:** Bremst gezielt einzelne schnelle Publisher
2. **Memory Alarm:** Bei >40% RAM (konfigurierbar) werden ALLE Publisher blockiert
3. **Disk Alarm:** Bei wenig Disk Space werden ALLE Publisher blockiert

aiormq erkennt `Connection.Blocked` / `Connection.Unblocked` automatisch und pausiert Publishing.

### 5.4 Consumer Performance

#### Prefetch-Optimierung

**Faustregel:** Prefetch = erwartete Anzahl gleichzeitiger Tasks die der Worker verarbeiten kann.

Fuer SwarmQ's Async Worker mit `max_concurrent_tasks=10`:
- `prefetch_count=10` -- Worker bekommt genau so viele Nachrichten wie er verarbeiten kann
- `prefetch_count=15-20` -- Etwas Puffer fuer Pipeline-Effekt (naechste Nachricht ist schon da)

#### Mehrere Consumer pro Queue vs. Single Consumer

| Strategie | Throughput | Reihenfolge | Empfehlung |
|---|---|---|---|
| **Mehrere Worker auf einer Queue** | Hoch | Keine Garantie | **Standard fuer SwarmQ** |
| **Single Active Consumer** | Niedriger | Garantiert | Fuer strikt geordnete Verarbeitung |
| **Consumer Priorities** | Variabel | Abhaengig | Fuer Failover-Szenarien |

#### Single Active Consumer (SAC)

```python
# Queue mit Single Active Consumer
queue = await channel.declare_queue(
    "swarmq.tasks.ordered",
    durable=True,
    arguments={
        "x-queue-type": "quorum",
        "x-single-active-consumer": True,
    },
)
```

SAC-Verhalten:
- Nur ein Consumer ist aktiv, alle anderen sind Standby
- Bei Ausfall des aktiven Consumers wird der naechste aktiviert
- Ab RabbitMQ 4.0: Consumer-Prioritaet wird bei SAC-Auswahl beruecksichtigt

#### Consumer Timeout

RabbitMQ erzwingt ein Timeout fuer unbestaetigte Nachrichten:
- **Default:** 30 Minuten
- Bei Timeout: Channel wird mit `PRECONDITION_FAILED` geschlossen
- Konfigurierbar per Queue oder global

**Relevanz fuer SwarmQ:** `hard_timeout` eines Tasks muss **unter** dem Consumer Timeout liegen, oder das Consumer Timeout muss entsprechend erhoeht werden.

```
# rabbitmq.conf -- Consumer Timeout erhoehen
consumer_timeout = 3600000   # 1 Stunde in ms
```

### 5.5 Cluster Performance

#### Queue Leadership und Locality

- Jede Quorum Queue hat einen Leader auf einem bestimmten Node
- Nachrichten werden immer zum Leader geroutet
- Publisher und Consumer, die auf demselben Node wie der Queue-Leader sind, haben niedrigere Latenz

**Empfehlung:** Queue-Leader ueber Nodes verteilen. RabbitMQ macht das automatisch, aber man kann es per Policy steuern:

```
# rabbitmq.conf -- Queue Leader Balancing
queue_leader_locator = balanced   # Default: client-local
```

#### Inter-Node Traffic

- Jede Quorum Queue repliziert zu N-1 Followers
- Bei 100 Queues auf 3 Nodes: 100 Raft-Cluster mit Multi-Raft-Batching
- **Empfehlung:** Nicht mehr als ~1000 Quorum Queues pro Cluster

#### Memory und Disk Alarms

| Alarm | Default-Schwelle | Wirkung |
|---|---|---|
| **Memory Alarm** | 40% des RAM (`vm_memory_high_watermark`) | Alle Publisher blockiert |
| **Disk Alarm** | 50 MB freier Speicher (`disk_free_limit`) | Alle Publisher blockiert |

**Empfehlung fuer Produktion:**
```
# rabbitmq.conf
vm_memory_high_watermark.relative = 0.4       # 40% RAM
disk_free_limit.relative = 1.5                  # 1.5x RAM-Groesse
```

---

## 6. Sicherheit

### 6.1 Authentifizierung und Autorisierung

#### Default Guest Account

- **Default:** `guest:guest` (Username:Passwort)
- **Einschraenkung:** Kann sich nur von `localhost` verbinden (Sicherheits-Feature)
- **Produktion:** IMMER deaktivieren oder Passwort aendern

```bash
# Guest-Account deaktivieren
rabbitmqctl delete_user guest

# Oder: Neuen Admin-User erstellen
rabbitmqctl add_user admin <sicheres_passwort>
rabbitmqctl set_user_tags admin administrator
rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"
```

#### Virtual Hosts fuer Isolation

VHosts sind logische Namespaces. Queues, Exchanges und Bindings existieren nur innerhalb eines VHosts.

```bash
# SwarmQ VHost erstellen
rabbitmqctl add_vhost swarmq

# User mit Berechtigungen fuer SwarmQ VHost
rabbitmqctl add_user swarmq_app <passwort>
rabbitmqctl set_permissions -p swarmq swarmq_app "swarmq\..*" "swarmq\..*" "swarmq\..*"
```

#### Berechtigungen: configure, write, read

| Berechtigung | Bedeutung | Regex-Beispiel |
|---|---|---|
| **configure** | Queues/Exchanges deklarieren und loeschen | `"swarmq\..*"` |
| **write** | Nachrichten publizieren (Exchange + Routing) | `"swarmq\..*"` |
| **read** | Nachrichten konsumieren (Queue + Consumer) | `"swarmq\..*"` |

**Empfehlung fuer SwarmQ:** Getrennte User fuer verschiedene Rollen:
- `swarmq_publisher`: nur `write` auf Task-Exchanges
- `swarmq_worker`: `read` auf Task-Queues + `write` fuer Results + `configure` fuer Queue-Deklaration
- `swarmq_admin`: alle Berechtigungen (fuer Setup/Monitoring)

#### OAuth 2.0 / LDAP

- **OAuth 2.0:** Plugin `rabbitmq_auth_backend_oauth2` -- Integration mit Identity Providern
- **LDAP:** Plugin `rabbitmq_auth_backend_ldap` -- Enterprise-Integration
- **Empfehlung:** In der Kombination mit Caching-Backend (`rabbitmq_auth_backend_cache`) nutzen um LDAP-Server nicht zu ueberlasten

### 6.2 TLS/SSL

#### Client-Verbindungen

```
# rabbitmq.conf -- TLS fuer Client-Verbindungen
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/certs/ca.pem
ssl_options.certfile   = /etc/rabbitmq/certs/server.pem
ssl_options.keyfile    = /etc/rabbitmq/certs/server.key
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = true

# Optional: Unverschluesselte Verbindungen deaktivieren
listeners.tcp = none
```

#### Inter-Node Kommunikation

```
# rabbitmq.conf -- TLS fuer Cluster-Traffic
cluster_formation.ssl.cacertfile = /etc/rabbitmq/certs/ca.pem
cluster_formation.ssl.certfile   = /etc/rabbitmq/certs/node.pem
cluster_formation.ssl.keyfile    = /etc/rabbitmq/certs/node.key
```

#### Performance-Impact

TLS verursacht ca. 10-20% Throughput-Reduktion. Fuer Produktion trotzdem empfohlen.

### 6.3 Netzwerk-Sicherheit

#### Ports und Firewall

| Port | Dienst | Empfehlung |
|---|---|---|
| **5672** | AMQP (Client-Verbindungen) | Nur fuer Application-Server oeffnen |
| **5671** | AMQPS (TLS) | Nur fuer Application-Server oeffnen |
| **15672** | Management UI/API | Nur fuer Admins, am besten hinter VPN |
| **25672** | Inter-Node (Cluster) | Nur zwischen Cluster-Nodes |
| **4369** | EPMD (Erlang Port Mapper) | Nur zwischen Cluster-Nodes |
| **15692** | Prometheus Metrics | Nur fuer Monitoring-System |

#### Erlang Cookie

- Shared Secret fuer Node-zu-Node Authentifizierung
- Gespeichert in `/var/lib/rabbitmq/.erlang.cookie`
- **Berechtigungen:** `chmod 400` -- nur RabbitMQ-User darf lesen
- In Docker: Als Secret mounten, nicht als Environment Variable

### 6.4 Nachrichten-Sicherheit

| Aspekt | Status | Empfehlung |
|---|---|---|
| **Verschluesselung** | Keine eingebaute Payload-Verschluesselung | TLS fuer Transport, App-Level-Verschluesselung fuer sensible Daten |
| **Payload-Validierung** | Nicht am Broker | Validierung in SwarmQ-Worker (msgspec) |
| **Nachrichtengroesse** | Default Max: 128 MiB | Nachrichten < 1 MiB halten. Grosse Daten als Referenz (S3, DB) |

---

## 7. Monitoring und Observability

### 7.1 Wichtige Metriken

#### Queue-Metriken

| Metrik | Prometheus-Name | Alarm-Schwelle |
|---|---|---|
| **Messages Ready** | `rabbitmq_queue_messages_ready` | > 10.000 (abhaengig von Workload) |
| **Messages Unacked** | `rabbitmq_queue_messages_unacked` | > 1.000 |
| **Queue Depth (gesamt)** | `rabbitmq_queue_messages` | > 50.000 |
| **Publish Rate** | `rabbitmq_queue_messages_published_total` | Monitoring |
| **Deliver Rate** | `rabbitmq_queue_messages_delivered_total` | Monitoring |
| **Ack Rate** | `rabbitmq_queue_messages_acknowledged_total` | Monitoring |
| **Redeliver Rate** | `rabbitmq_queue_messages_redelivered_total` | > 100/min (Poison Messages?) |
| **Consumer Count** | `rabbitmq_queue_consumers` | == 0 (Queue ohne Consumer!) |

#### Node-Metriken

| Metrik | Prometheus-Name | Alarm-Schwelle |
|---|---|---|
| **Memory Used** | `rabbitmq_process_resident_memory_bytes` | > 80% von `vm_memory_high_watermark` |
| **Disk Free** | `rabbitmq_disk_space_available_bytes` | < 2x `disk_free_limit` |
| **File Descriptors** | `rabbitmq_process_open_fds` | > 80% von `rabbitmq_process_max_fds` |
| **Connection Count** | `rabbitmq_connections` | > 1000 |
| **Channel Count** | `rabbitmq_channels` | > 5000 |

#### Consumer-Metriken

| Metrik | Bedeutung | Alarm |
|---|---|---|
| **Consumer Utilization** | 0-1, wie oft der Consumer beschaeftigt ist | < 0.5 (Consumer ist oft idle, Prefetch zu niedrig?) |

### 7.2 Management API Endpoints

```bash
# Cluster-Ueberblick
GET /api/overview

# Alle Queues mit Metriken
GET /api/queues/{vhost}

# Einzelne Queue
GET /api/queues/{vhost}/{queue_name}

# Connections
GET /api/connections

# Channels
GET /api/channels

# Nodes
GET /api/nodes
```

### 7.3 Prometheus/Grafana Integration

RabbitMQ hat ein eingebautes Prometheus-Plugin:

```
# rabbitmq.conf -- Prometheus Plugin aktivieren
# (wird separat per enabled_plugins aktiviert)

# Prometheus Endpoint konfigurieren
prometheus.return_per_object_metrics = true
prometheus.path = /metrics
prometheus.tcp.port = 15692
```

```bash
# Plugin aktivieren
rabbitmq-plugins enable rabbitmq_prometheus
```

**Effizienter Metrics-Abruf:**
```
# Nur Queue-Metriken (bis zu 60x effizienter als volle Metriken):
GET /metrics/detailed?family=queue_coarse_metrics&family=queue_consumer_count
```

### 7.4 Health Check Endpoints

```bash
# Basis Health Check: Ist der Node am laufen?
rabbitmq-diagnostics check_running

# Port-Connectivity: Sind alle kritischen Ports erreichbar?
rabbitmq-diagnostics check_port_connectivity

# Cluster Health: Sind alle erwarteten Nodes im Cluster?
rabbitmq-diagnostics check_if_node_is_quorum_critical

# Alarme pruefen
rabbitmq-diagnostics check_alarms

# Fuer Docker Health Check:
rabbitmq-diagnostics -q ping
```

### 7.5 Alarm-Schwellwerte (Empfehlung)

| Alarm | Bedingung | Schwere |
|---|---|---|
| **DLQ nicht leer** | DLQ Depth > 0 | Warning |
| **DLQ waechst** | DLQ Depth > 100 | Critical |
| **Queue ohne Consumer** | Consumer Count == 0 fuer > 5min | Critical |
| **Queue-Backlog** | Messages Ready > 10.000 | Warning |
| **Memory Alarm** | Memory > 90% Watermark | Critical |
| **Disk Alarm** | Disk Free < 2x Limit | Critical |
| **Unacked Messages** | Unacked > Prefetch * Consumer Count * 2 | Warning |

---

## 8. Hochverfuegbarkeit

### 8.1 Quorum Queues fuer HA

Quorum Queues sind der primaere HA-Mechanismus ab RabbitMQ 4.0:
- Automatische Replikation per Raft
- Leader Election bei Node-Ausfall
- Keine manuelle Konfiguration von Mirroring noetig

**Minimum fuer HA:** 3 Nodes (toleriert 1 Ausfall)
**Empfohlen:** 5 Nodes (toleriert 2 Ausfaelle)

### 8.2 Node Failure Handling

| Szenario | Verhalten | Dauer |
|---|---|---|
| **1 Node down (3er-Cluster)** | Leader Election, Queue bleibt verfuegbar | 1-3 Sekunden |
| **2 Nodes down (3er-Cluster)** | Queue wird unavailable (kein Quorum) | Sofort |
| **Node kommt zurueck** | Automatische Synchronisation | Abhaengig von Datenvolumen |

### 8.3 Network Partition Handling

#### Strategien

| Strategie | Verhalten | Empfehlung |
|---|---|---|
| **ignore** (Default) | Partitions werden nicht behandelt, Split-Brain moeglich | **Nie in Produktion** |
| **pause_minority** | Minority-Seite pausiert, Majority arbeitet weiter | **Empfohlen fuer SwarmQ** |
| **autoheal** | Automatische Auswahl einer "Gewinner-Seite", Verlierer werden neugestartet | Wenn Verfuegbarkeit > Konsistenz |
| **pause-if-all-down** | Pausiert wenn alle gelisteten Nodes unerreichbar | Spezialfaelle |

```
# rabbitmq.conf
cluster_partition_handling = pause_minority
```

**WICHTIG ab RabbitMQ 4.3:** Wenn Khepri (neuer Metadata-Store, Ersatz fuer Mnesia) verwendet wird, ist `cluster_partition_handling` **nicht mehr relevant**, da Khepri eigene Raft-basierte Partition-Handling hat.

#### Empfehlung fuer SwarmQ

- **pause_minority** mit einer ungeraden Anzahl Nodes (3, 5, 7)
- Da SwarmQ "Ausfallsicherheit hat hoechste Prioritaet": Konsistenz > Verfuegbarkeit
- Quorum Queues auf der Majority-Seite bleiben automatisch verfuegbar

### 8.4 Backup und Restore

```bash
# Definitionen exportieren (Queues, Exchanges, Bindings, Users, Vhosts)
rabbitmqctl export_definitions /backup/definitions.json

# Definitionen importieren
rabbitmqctl import_definitions /backup/definitions.json

# Mnesia-Daten sind in /var/lib/rabbitmq/mnesia/
# Bei Quorum Queues: Raft-Log ist ebenfalls dort
```

**Backup-Strategie:**
1. **Definitionen** regelmaessig exportieren (automatisierbar)
2. **Nachrichten** werden durch Quorum Queue Replikation geschuetzt
3. **Volume-Backups** von `/var/lib/rabbitmq/` fuer Disaster Recovery

---

## 9. Docker/Container Best Practices

### 9.1 Offizielles Docker Image

```
# Standard (ohne Management UI)
rabbitmq:4.2

# Mit Management UI + Prometheus Plugin
rabbitmq:4.2-management
```

### 9.2 docker-compose.yml fuer Entwicklung

```yaml
services:
  rabbitmq:
    image: rabbitmq:4.2-management
    container_name: swarmq-rabbitmq
    hostname: swarmq-rabbit
    ports:
      - "5672:5672"     # AMQP
      - "15672:15672"   # Management UI
      - "15692:15692"   # Prometheus Metrics
    environment:
      RABBITMQ_DEFAULT_USER: swarmq
      RABBITMQ_DEFAULT_PASS: swarmq_dev_pass
      RABBITMQ_DEFAULT_VHOST: swarmq
      RABBITMQ_NODENAME: rabbit@swarmq-rabbit
      RABBITMQ_VM_MEMORY_HIGH_WATERMARK: 0.4
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
      - ./rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./rabbitmq/enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: "2.0"
        reservations:
          memory: 512M
          cpus: "0.5"

volumes:
  rabbitmq_data:
    driver: local
```

### 9.3 Volume Mounts

| Mount | Pfad im Container | Zweck |
|---|---|---|
| **Daten** | `/var/lib/rabbitmq` | Queue-Daten, Raft-Logs, Mnesia -- MUSS persistiert werden |
| **Konfiguration** | `/etc/rabbitmq/rabbitmq.conf` | Server-Konfiguration (read-only) |
| **Plugins** | `/etc/rabbitmq/enabled_plugins` | Plugin-Liste (read-only) |
| **Zertifikate** | `/etc/rabbitmq/certs/` | TLS-Zertifikate (read-only) |

**KRITISCH:** `RABBITMQ_NODENAME` muss konsistent sein (inklusive Hostname), damit Daten nach Container-Neustart wiedergefunden werden. Format: `rabbit@<hostname>`. Deshalb immer `hostname` im docker-compose setzen.

### 9.4 Environment Variables

| Variable | Wert | Beschreibung |
|---|---|---|
| `RABBITMQ_DEFAULT_USER` | `swarmq` | Initialer Admin-User |
| `RABBITMQ_DEFAULT_PASS` | `<sicher>` | Initiales Passwort |
| `RABBITMQ_DEFAULT_VHOST` | `swarmq` | Initialer VHost |
| `RABBITMQ_NODENAME` | `rabbit@<hostname>` | Konsistenter Node-Name |
| `RABBITMQ_VM_MEMORY_HIGH_WATERMARK` | `0.4` | Memory Watermark |
| `RABBITMQ_ERLANG_COOKIE` | `<secret>` | Fuer Clustering (als Docker Secret bevorzugt) |

### 9.5 Health Check Konfiguration

```yaml
# Gute Health Check Konfiguration:
healthcheck:
  test: ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"]
  interval: 30s    # Alle 30 Sekunden pruefen
  timeout: 10s     # Max 10s fuer den Check
  retries: 5       # 5 Fehlversuche bevor "unhealthy"
  start_period: 30s # 30s Wartezeit nach Container-Start
```

**Warum `check_port_connectivity` statt `check_running`?**
- `check_running` prueft nur ob der Erlang-Prozess laeuft
- `check_port_connectivity` prueft ob die Ports tatsaechlich erreichbar sind (AMQP, Clustering, etc.)
- Letzteres ist aussagekraeftiger fuer die tatsaechliche Bereitschaft

---

## 10. Konkrete Konfigurationsempfehlung fuer SwarmQ

### 10.1 rabbitmq.conf

```ini
# === SwarmQ RabbitMQ Konfiguration ===
# Fuer Entwicklung und Produktion (Anpassungen per Umgebung)

# --- Netzwerk ---
listeners.tcp.default = 5672
# listeners.ssl.default = 5671  # TLS fuer Produktion aktivieren

# --- Ressourcen ---
vm_memory_high_watermark.relative = 0.4
disk_free_limit.relative = 1.5

# --- Consumer Timeout ---
# Muss hoeher sein als der laengste hard_timeout in SwarmQ
consumer_timeout = 3600000   # 1 Stunde

# --- Queue Leader Verteilung ---
queue_leader_locator = balanced

# --- Logging ---
log.console = true
log.console.level = info
log.file.level = warning

# --- Management ---
management.tcp.port = 15672

# --- Prometheus ---
prometheus.return_per_object_metrics = true
prometheus.tcp.port = 15692

# --- Partition Handling (nur relevant bei Clustering ohne Khepri) ---
cluster_partition_handling = pause_minority

# --- Default Queue Typ ---
# Ab 4.0 empfohlen: Quorum als Default fuer neue Queues
default_queue_type = quorum
```

### 10.2 enabled_plugins

```
[rabbitmq_management,rabbitmq_prometheus].
```

### 10.3 docker-compose.yml (Entwicklung)

```yaml
services:
  rabbitmq:
    image: rabbitmq:4.2-management
    container_name: swarmq-rabbitmq
    hostname: swarmq-rabbit
    ports:
      - "5672:5672"
      - "15672:15672"
      - "15692:15692"
    environment:
      RABBITMQ_DEFAULT_USER: swarmq
      RABBITMQ_DEFAULT_PASS: swarmq_dev_pass
      RABBITMQ_DEFAULT_VHOST: swarmq
      RABBITMQ_NODENAME: rabbit@swarmq-rabbit
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
      - ./rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./rabbitmq/enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: unless-stopped

  valkey:
    image: valkey/valkey:8.1
    container_name: swarmq-valkey
    ports:
      - "6379:6379"
    volumes:
      - valkey_data:/data
    healthcheck:
      test: ["CMD", "valkey-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

volumes:
  rabbitmq_data:
  valkey_data:
```

### 10.4 Queue Declaration Parameter

```python
# === Task-Queue (Quorum Queue) ===
async def declare_task_queue(channel, queue_name: str, delivery_limit: int = 5):
    return await channel.declare_queue(
        f"swarmq.tasks.{queue_name}",
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "swarmq.dlx",
            "x-dead-letter-routing-key": f"swarmq.tasks.{queue_name}.dead",
            "x-dead-letter-strategy": "at-least-once",
            "x-delivery-limit": delivery_limit,
            "x-overflow": "reject-publish",
            "x-max-length": 100000,
        },
    )

# === Dead Letter Queue (fuer jede Task-Queue eine) ===
async def declare_dlq(channel, queue_name: str):
    return await channel.declare_queue(
        f"swarmq.tasks.{queue_name}.dead",
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            # DLQ-Nachrichten nach 7 Tagen loeschen
            "x-message-ttl": 7 * 24 * 60 * 60 * 1000,
        },
    )
```

### 10.5 Exchange Setup

```python
async def setup_exchanges(channel):
    # DLX Exchange (empfaengt fehlgeschlagene Nachrichten)
    dlx = await channel.declare_exchange(
        "swarmq.dlx",
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    return dlx

async def setup_queue_with_dlx(channel, queue_name: str, delivery_limit: int = 5):
    dlx = await setup_exchanges(channel)

    # Task-Queue deklarieren
    task_queue = await declare_task_queue(channel, queue_name, delivery_limit)

    # DLQ deklarieren und an DLX binden
    dlq = await declare_dlq(channel, queue_name)
    await dlq.bind(dlx, routing_key=f"swarmq.tasks.{queue_name}.dead")

    return task_queue, dlq
```

### 10.6 Publishing-Muster

```python
async def publish_task(channel, queue_name: str, task_id: str, task_name: str,
                       args: list, kwargs: dict, delivery: str, hard_timeout: int):
    """Task an Queue senden"""
    payload = msgspec.json.encode({
        "task": {
            "id": task_id,
            "queue": queue_name,
            "name": task_name,
        },
        "args": args,
        "kwargs": kwargs,
    })

    is_persistent = delivery == "at_least_once"

    message = aio_pika.Message(
        body=payload,
        delivery_mode=(
            aio_pika.DeliveryMode.PERSISTENT if is_persistent
            else aio_pika.DeliveryMode.NOT_PERSISTENT
        ),
        content_type="application/json",
        content_encoding="utf-8",
        message_id=task_id,
        type=task_name,
        timestamp=datetime.now(UTC),
        expiration=hard_timeout * 1000 if hard_timeout else None,
        headers={
            "swarmq-version": "1",
            "swarmq-queue": queue_name,
            "swarmq-delivery": delivery,
        },
    )

    # Default Exchange: Routing-Key = Queue-Name
    await channel.default_exchange.publish(
        message,
        routing_key=f"swarmq.tasks.{queue_name}",
        mandatory=True,
        timeout=10,
    )
```

### 10.7 User/VHost Setup (Produktion)

```bash
#!/bin/bash
# setup-rabbitmq.sh -- Initiales Setup fuer SwarmQ

# VHost erstellen
rabbitmqctl add_vhost swarmq

# Application User (Worker + Publisher)
rabbitmqctl add_user swarmq_app "<sicheres_passwort>"
rabbitmqctl set_permissions -p swarmq swarmq_app \
    "swarmq\..*" \  # configure: nur swarmq.* Ressourcen
    "swarmq\..*" \  # write: nur swarmq.* Ressourcen
    "swarmq\..*"    # read: nur swarmq.* Ressourcen

# Monitoring User (nur lesen)
rabbitmqctl add_user swarmq_monitor "<passwort>"
rabbitmqctl set_user_tags swarmq_monitor monitoring
rabbitmqctl set_permissions -p swarmq swarmq_monitor \
    "" \            # configure: nichts
    "" \            # write: nichts
    "swarmq\..*"    # read: nur swarmq.* Ressourcen

# Guest-Account loeschen
rabbitmqctl delete_user guest
```

### 10.8 Namenskonvention

| Ressource | Pattern | Beispiel |
|---|---|---|
| **Task Queue** | `swarmq.tasks.{queue_name}` | `swarmq.tasks.default` |
| **Dead Letter Queue** | `swarmq.tasks.{queue_name}.dead` | `swarmq.tasks.default.dead` |
| **Retry Queue** | `swarmq.retry.{queue_name}.{delay_s}s` | `swarmq.retry.default.30s` |
| **DLX Exchange** | `swarmq.dlx` | `swarmq.dlx` |
| **Retry Exchange** | `swarmq.retry` | `swarmq.retry` |
| **VHost** | `swarmq` oder konfigurierbar | `swarmq` |

---

## Zusammenfassung der Kernentscheidungen fuer SwarmQ

| Entscheidung | Wahl | Begruendung |
|---|---|---|
| **Queue-Typ** | Quorum Queue | Replikation, At-Least-Once DLQ, Delivery Limit, Ausfallsicherheit |
| **Exchange** | Default Exchange | Einfachstes Routing, kein Setup noetig |
| **DLX** | Direct Exchange "swarmq.dlx" | Fuer DLQ-Routing |
| **Delivery Mode** | Persistent fuer AT_LEAST_ONCE | Ueberlebt Broker-Restart |
| **Prefetch** | 10 (Async), 1 (CPU) | Balance zwischen Throughput und Fairness |
| **Dead Lettering** | At-Least-Once Strategy | Keine Nachricht geht beim DLQ-Transfer verloren |
| **Delivery Limit** | 5 (konfigurierbar) | Poison Messages nach 5 Versuchen in DLQ |
| **Partition Handling** | pause_minority | Konsistenz > Verfuegbarkeit |
| **Retry** | Native (4.2+) oder DLX+TTL (aeltere Versionen) | Exponentieller Backoff |
| **VHost** | Eigener VHost "swarmq" | Isolation |

---

## Quellen

- [RabbitMQ Quorum Queues Dokumentation](https://www.rabbitmq.com/docs/quorum-queues)
- [RabbitMQ Classic Queues Dokumentation](https://www.rabbitmq.com/docs/classic-queues)
- [RabbitMQ Streams Dokumentation](https://www.rabbitmq.com/docs/streams)
- [RabbitMQ Exchanges Dokumentation](https://www.rabbitmq.com/docs/exchanges)
- [RabbitMQ Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx)
- [RabbitMQ Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch)
- [RabbitMQ Configuration](https://www.rabbitmq.com/docs/configure)
- [RabbitMQ Production Deployment Guidelines](https://www.rabbitmq.com/docs/production-checklist)
- [RabbitMQ Monitoring with Prometheus](https://www.rabbitmq.com/docs/prometheus)
- [RabbitMQ Monitoring](https://www.rabbitmq.com/docs/monitoring)
- [RabbitMQ TLS Support](https://www.rabbitmq.com/docs/ssl)
- [RabbitMQ Authentication and Authorization](https://www.rabbitmq.com/docs/access-control)
- [RabbitMQ Network Partitions](https://www.rabbitmq.com/docs/partitions)
- [RabbitMQ Priority Support](https://www.rabbitmq.com/docs/priority)
- [RabbitMQ Consumers](https://www.rabbitmq.com/docs/consumers)
- [RabbitMQ 4.0 Quorum Queue Features (Blog)](https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0)
- [RabbitMQ 3.13 Classic Queues Changes (Blog)](https://www.rabbitmq.com/blog/2024/01/11/3.13-release)
- [At-Least-Once Dead Lettering (Blog)](https://www.rabbitmq.com/blog/2022/03/29/at-least-once-dead-lettering)
- [Migrating to Quorum Queues in 2025 (Blog)](https://www.rabbitmq.com/blog/2025/07/29/latest-benefits-of-rmq-and-migrating-to-qq-along-the-way)
- [Quorum Queues Internals -- Deep Dive (CloudAMQP)](https://www.cloudamqp.com/blog/quorum-queues-internals-a-deep-dive.html)
- [RabbitMQ Queue Types Explained (CloudAMQP)](https://www.cloudamqp.com/blog/rabbitmq-queue-types.html)
- [RabbitMQ Best Practices Part 1 (CloudAMQP)](https://www.cloudamqp.com/blog/part1-rabbitmq-best-practice.html)
- [RabbitMQ Best Practices Part 2 -- High Performance (CloudAMQP)](https://www.cloudamqp.com/blog/part2-rabbitmq-best-practice-for-high-performance.html)
- [Optimizing Prefetch Count (CloudAMQP)](https://www.cloudamqp.com/blog/how-to-optimize-the-rabbitmq-prefetch-count.html)
- [RabbitMQ Streams and Replay (CloudAMQP)](https://www.cloudamqp.com/blog/rabbitmq-streams-and-replay-features-part-1-when-to-use-rabbitmq-streams.html)
- [DLX Retry Approaches (Jack Vanlightly)](https://jack-vanlightly.com/blog/2017/3/24/rabbitmq-delayed-retry-approaches-that-work)
- [Key Metrics for RabbitMQ Monitoring (Datadog)](https://www.datadoghq.com/blog/rabbitmq-monitoring/)
- [RabbitMQ Queues vs Streams (SeventhState)](https://seventhstate.io/rabbitmq-queues-vs-streams/)
- [RabbitMQ Quorum Queues Explained (SeventhState)](https://seventhstate.io/rabbitmq-quorum-queues-explained/)
- [RabbitMQ Docker Hub](https://hub.docker.com/_/rabbitmq)
- [RabbitMQ Delayed Queues (Broadcom TechDocs)](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-rpm/4-2/tanzu-rabbitmq-rpm-offering/site-delayed-queues.html)
- [RabbitMQ 4.2 Release Notes (Broadcom TechDocs)](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/open-source-rabbitmq/4-2/opn-src-rabbitmq/site-release-notes.html)
- [AMQP 0-9-1 Reference Guide](https://www.rabbitmq.com/amqp-0-9-1-reference)
