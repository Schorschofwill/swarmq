# Recherche: Embedded Scheduler mit Leader Election

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument für die Implementierung des SwarmQ Embedded Schedulers. Enthält alle Erkenntnisse aus der Analyse bestehender Lösungen, Distributed-Lock-Mechanismen und Architektur-Entscheidungen.

---

## Inhaltsverzeichnis

1. [Problemstellung](#1-problemstellung)
2. [Wie bestehende Frameworks scheitern](#2-wie-bestehende-frameworks-scheitern)
3. [Ansatz 1: Valkey-basierter Distributed Lock (EMPFOHLEN)](#3-ansatz-1-valkey-basierter-distributed-lock-empfohlen)
4. [Ansatz 2: RabbitMQ Single Active Consumer](#4-ansatz-2-rabbitmq-single-active-consumer)
5. [Ansatz 3: APScheduler 4.x Stil](#5-ansatz-3-apscheduler-4x-stil)
6. [Embedded Scheduler Patterns](#6-embedded-scheduler-patterns)
7. [Praktische Implementierungsdetails](#7-praktische-implementierungsdetails)
8. [Bekannte Bugs und Lehren aus RedBeat](#8-bekannte-bugs-und-lehren-aus-redbeat)
9. [Entscheidung und Begründung](#9-entscheidung-und-begründung)

---

# 1. Problemstellung

Der Benutzer soll keine Sorgen haben, einen separaten Scheduler-Prozess zu starten. Bei bestehenden Frameworks (Celery Beat, TaskIQ Scheduler) muss der Benutzer:

1. Einen separaten Prozess starten und überwachen
2. Sicherstellen, dass nur eine Instanz läuft (kein eingebauter Schutz)
3. Manuell für Failover sorgen (Supervisor, systemd, Kubernetes)

**Gewünschtes Verhalten bei SwarmQ:**
- Worker starten → Scheduler läuft automatisch mit
- Mehrere Worker → nur ein Scheduler aktiv
- Scheduler-Worker crasht → anderer Worker übernimmt automatisch

---

# 2. Wie bestehende Frameworks scheitern

## 2.1 Celery Beat

- **Separater Prozess** (`celery beat`), den der Benutzer manuell starten muss
- **Kein Schutz gegen mehrere Instanzen** — offenes Issue #251 seit 2010
- Wenn N Beat-Instanzen laufen → jeder Schedule wird N-mal ausgeführt
- Problematisch bei AWS ECS / Kubernetes (automatische Container-Skalierung)
- Offizielle Empfehlung: "Stelle sicher dass nur ein Beat läuft" → Single Point of Failure

**Community-Workarounds:**
- `single-beat` (ybrs/single-beat): Externes Tool mit Redis-Lock
- `celery-beatx`: Fügt Lock-Mechanismus hinzu
- `django-celery-beat`: Speichert Schedules in DB, löst Multi-Instanz-Problem aber nicht

## 2.2 TaskIQ

- **Separater Prozess** (`taskiq scheduler module:scheduler`)
- **Keine Single-Instance-Garantie** — Doku warnt explizit: *"Please always run only one instance of the scheduler!"*
- N Scheduler → N-fache Ausführung
- `ListRedisScheduleSource` für dynamische Schedules, aber kein Schutz gegen Mehrfachausführung

## 2.3 Dramatiq

- **Kein eingebauter Scheduler** — nur Community-Lösungen:
  - `periodiq`: Separater Prozess, keine Multi-Instanz-Koordination
  - `dramatiq-crontab`: Django-Integration mit Redis-Lock-Backend
  - APScheduler-Integration: Externer Prozess triggert Dramatiq-Actors

## 2.4 RedBeat (Celery) — am nächsten an unserer Lösung

Ersetzt den Default-Scheduler von Celery Beat, speichert alles in Redis.

**Lock-Mechanismus:**
- Lock-Key: `<prefix>:lock` (default: `redbeat:lock`)
- Lock-TTL: 5× Loop-Interval (default 25 Minuten)
- Aktiver Scheduler verlängert Lock periodisch

**Schwerwiegende Bugs (Lehren für SwarmQ):**
- **Issue #127:** `pexpire` wird OHNE Ownership-Check aufgerufen. Nach Netzwerkunterbrechung kann ein alter Leader den Lock eines neuen Leaders verlängern → zwei aktive Scheduler, doppelte Task-Ausführung.
- **Issue #208:** Bei Redis-Verbindungsfehlern während `lock.acquire()` fängt Celery die Exception ab und der Scheduler läuft ohne Lock weiter. `scheduler.lock` bleibt `None`, was als "kein Lock konfiguriert" interpretiert wird statt "Lock-Erwerb fehlgeschlagen".

## 2.5 APScheduler 4.x — einziges Framework mit nativer Multi-Scheduler-Koordination

Grundlegend neue Architektur in v4 mit zwei Konzepten:
1. **Data Store:** Speichert Tasks, Schedules, Jobs (SQLAlchemy, MongoDB)
2. **Event Broker:** Koordiniert Scheduler und Worker (Redis, AsyncPG, MQTT)

Atomare Claim-Operationen stellen sicher, dass ein Schedule nur einmal verarbeitet wird. Elegant aber komplex — für SwarmQ's Use Case Over-Engineering.

---

# 3. Ansatz 1: Valkey-basierter Distributed Lock (EMPFOHLEN)

## 3.1 Grundmechanismus: SET NX mit TTL

```
SET swarmq:scheduler:lock <unique_worker_id> NX PX 30000
```

- `NX` — nur setzen wenn Key nicht existiert
- `PX 30000` — TTL von 30 Sekunden
- `<unique_worker_id>` — eindeutige Worker-ID (z.B. `hostname:pid:uuid`)

**Ablauf:**
1. Jeder Worker versucht beim Start, den Lock zu erwerben
2. Genau ein Worker erhält den Lock und wird Scheduler-Leader
3. Der Leader verlängert den Lock periodisch (Heartbeat)
4. Alle anderen Worker prüfen periodisch, ob sie den Lock übernehmen können
5. Wenn der Leader crasht, läuft der TTL ab und ein anderer Worker übernimmt

## 3.2 Lua-Scripts für atomare Operationen

### Lock verlängern (nur wenn man noch Besitzer ist)

```lua
-- KEYS[1] = lock key, ARGV[1] = worker_id, ARGV[2] = ttl_ms
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
```

### Lock freigeben (nur eigenen Lock löschen)

```lua
-- KEYS[1] = lock key, ARGV[1] = worker_id
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
```

**Hinweis:** Ab Redis 8.4+ gibt es `DELEX key IFEQ value` als nativen Befehl. Für ältere Versionen und Valkey sind die Lua-Scripts nötig.

**KRITISCH:** Ohne Ownership-Check bei der Verlängerung entsteht der RedBeat-Bug #127 — ein alter Leader verlängert den Lock eines neuen Leaders.

## 3.3 Redlock — NICHT relevant für SwarmQ

Redlock (N unabhängige Redis-Instanzen, Mehrheit für Lock-Erwerb) löst das Problem von Redis-Replikation bei Failover zwischen Master/Replica. Bei Single-Node Valkey: kein Mehrwert, unnötige Komplexität.

Martin Kleppmanns Kritik unterscheidet:
- **Locks für Effizienz:** Verhindert doppelte Arbeit. Wenn Lock versagt → Arbeit passiert doppelt. Einfacher Redis-Lock reicht.
- **Locks für Korrektheit:** Verhindert Datenkorruption. Braucht Fencing-Tokens und Konsens-Systeme.

**SwarmQ-Scheduler = Effizienz-Lock.** Doppelte Task-Einreihung ist tolerierbar (Tasks sind per Design idempotent bei `AT_LEAST_ONCE`).

## 3.4 Vollständiger Pseudocode

```python
import asyncio
import uuid
from valkey.asyncio import Valkey

LOCK_KEY = "swarmq:scheduler:lock"
LOCK_TTL_MS = 30_000
HEARTBEAT_INTERVAL = 10
CANDIDATE_CHECK_INTERVAL = 5

EXTEND_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""

RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

class EmbeddedScheduler:
    def __init__(self, valkey: Valkey, schedules: list):
        self.valkey = valkey
        self.worker_id = str(uuid.uuid4())
        self.is_leader = False
        self.schedules = schedules
        self._extend_sha = None
        self._release_sha = None

    async def start(self):
        self._extend_sha = await self.valkey.script_load(EXTEND_SCRIPT)
        self._release_sha = await self.valkey.script_load(RELEASE_SCRIPT)
        await asyncio.gather(
            self._election_loop(),
            self._scheduler_loop(),
        )

    async def _try_acquire(self) -> bool:
        result = await self.valkey.set(
            LOCK_KEY, self.worker_id, nx=True, px=LOCK_TTL_MS
        )
        return result is not None

    async def _try_extend(self) -> bool:
        result = await self.valkey.evalsha(
            self._extend_sha, 1, LOCK_KEY, self.worker_id, str(LOCK_TTL_MS)
        )
        return result == 1

    async def _release(self):
        await self.valkey.evalsha(
            self._release_sha, 1, LOCK_KEY, self.worker_id
        )

    async def _election_loop(self):
        while True:
            if self.is_leader:
                extended = await self._try_extend()
                if not extended:
                    self.is_leader = False  # Lock verloren
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            else:
                acquired = await self._try_acquire()
                if acquired:
                    self.is_leader = True
                await asyncio.sleep(CANDIDATE_CHECK_INTERVAL)

    async def _scheduler_loop(self):
        while True:
            if self.is_leader:
                # Zusätzliche Sicherheitsprüfung VOR Dispatch
                current = await self.valkey.get(LOCK_KEY)
                if current == self.worker_id.encode():
                    await self._dispatch_due_schedules()
            await asyncio.sleep(1)

    async def _dispatch_due_schedules(self):
        for schedule in self.schedules:
            last_run = await self.valkey.get(
                f"swarmq:schedule:{schedule.name}:last_run"
            )
            next_due = croniter(schedule.cron, float(last_run or 0)).get_next()
            if next_due <= time.time():
                await broker.schedule(schedule.task, schedule.args, **schedule.kwargs)
                await self.valkey.set(
                    f"swarmq:schedule:{schedule.name}:last_run",
                    str(time.time()),
                )

    async def shutdown(self):
        if self.is_leader:
            await self._release()
        self.is_leader = False
```

## 3.5 Bewertung

| Kriterium | Bewertung |
|---|---|
| **Zuverlässigkeit** | Hoch für Effizienz-Locks. Kurzzeitig 0 oder 2 Leader möglich, aber Tasks sind idempotent. |
| **Komplexität** | Niedrig. ~150 Zeilen Code. Zwei Lua-Scripts. |
| **Failover-Geschwindigkeit** | TTL-abhängig. Bei TTL=30s und Heartbeat=10s: Failover in 20-30 Sekunden. |
| **Betriebsaufwand** | Minimal. Valkey ist ohnehin das Backend. Kein zusätzlicher Dienst. |
| **Broker-Unabhängigkeit** | Ja — funktioniert auch wenn später Valkey als Broker kommt. |

---

# 4. Ansatz 2: RabbitMQ Single Active Consumer

## 4.1 Mechanismus

RabbitMQ bietet seit Version 3.8 das Feature `x-single-active-consumer`:

```python
channel.queue_declare(
    queue="swarmq-scheduler-leader",
    arguments={"x-single-active-consumer": True}
)
```

Alle Worker konsumieren von dieser Queue. RabbitMQ garantiert, dass nur ein Consumer aktiv ist. Fällt dieser weg, wird automatisch der nächste aktiviert.

## 4.2 Pattern für Scheduler-Leadership

```python
async def scheduler_leader_loop(channel):
    await channel.declare_queue(
        "swarmq-scheduler-leader",
        arguments={"x-single-active-consumer": True}
    )
    # Nur der aktive Consumer erhält Messages
    # Wer eine Message erhält, ist der Leader
    async for message in queue:
        await dispatch_due_schedules()
        await channel.publish(
            "swarmq-scheduler-leader",
            body=b"tick",
            delay=60_000,
        )
```

## 4.3 Classic vs. Quorum Queues

- **Classic Queues:** Initialer Consumer wird zufällig gewählt
- **Quorum Queues:** Unterstützen Consumer-Prioritäten; neuer Consumer mit höherer Priorität kann aktiven ersetzen

## 4.4 Limitierungen

- SAC und Exclusive Consumers schließen sich gegenseitig aus
- Kann nicht via Policy aktiviert werden (nur bei Queue-Deklaration)
- Messages werden immer an aktiven Consumer geliefert, auch wenn dieser "busy" ist

## 4.5 Bewertung

| Kriterium | Bewertung |
|---|---|
| **Zuverlässigkeit** | Sehr hoch. RabbitMQ garantiert Single Active Consumer. |
| **Komplexität** | Niedrig, aber "Tick"-Message-Pattern ist unüblich. |
| **Failover-Geschwindigkeit** | Schnell (<5 Sekunden, abhängig von Heartbeat-Timeout). |
| **Betriebsaufwand** | Minimal, aber Schedule-State muss trotzdem in Valkey. |
| **Broker-Unabhängigkeit** | NEIN — bei Broker-Wechsel (Valkey als Broker) müsste der Mechanismus neu implementiert werden. |

---

# 5. Ansatz 3: APScheduler 4.x Stil

Grundlegend neues Subsystem mit Data Store und Event Broker. Mehrere Scheduler können gegen denselben Store laufen, atomare Claim-Operationen verhindern Doppelausführung.

**Bewertung:** Elegant aber Over-Engineering für SwarmQ's Use Case. Hohe Komplexität, erhebliche zusätzliche Dependencies.

---

# 6. Embedded Scheduler Patterns

## 6.1 Konzept

Statt separatem Prozess läuft der Scheduler als asyncio-Task innerhalb jedes Worker-Prozesses:

```python
class Worker:
    async def start(self):
        await asyncio.gather(
            self._consume_tasks(),
            self._embedded_scheduler.start(),
        )

    async def shutdown(self):
        await self._embedded_scheduler.shutdown()
        await self._drain_tasks()
```

## 6.2 Heartbeat/Lease-Pattern

```
                TTL = 30s
                |<----------------------->|
Leader:   [ACQUIRE]---[EXTEND]---[EXTEND]---[EXTEND]---...
                       t=10s     t=20s      t=30s

Candidate:  [TRY]...[TRY]...[TRY]...[TRY]...
              |       |       |       |
           FAIL    FAIL    FAIL    FAIL
```

## 6.3 Wichtige Regeln (Amazon Builders Library)

1. **NICHT den Heartbeat in einem Background-Thread machen.** Wenn der Heartbeat-Thread läuft aber der Scheduler haengt, wird Leadership künstlich aufrechterhalten.
2. **Lokale verstrichene Zeit verwenden, nicht Wall-Clock-Time.** Vermeidet Clock-Drift- und NTP-Probleme.
3. **VOR jeder Aktion mit Seiteneffekten prüfen, ob man noch Leader ist.** Nicht nur beim Heartbeat.
4. **Idempotenz der Downstream-Aktionen annehmen.** Ein neuer Leader muss partiell abgeschlossene Arbeit sicher wiederholen können.

## 6.4 Fencing-Tokens

Monoton steigende Zahl bei jedem Lock-Erwerb. Schützt gegen GC-Pause-Szenario:

```
Worker A: [ACQUIRE, token=33] ... [GC-Pause 35s] ... [dispatcht mit token=33]
Worker B:                [Lock expired] [ACQUIRE, token=34] [dispatcht mit token=34]
```

**Für SwarmQ NICHT nötig**, da:
- Scheduler schiebt Tasks in RabbitMQ-Queue (kein geteilter mutabler State)
- Doppelte Task-Einreihung ist tolerierbar (idempotente Tasks)
- Zusätzlicher Valkey-Roundtrip pro Dispatch überwiegt den Nutzen

---

# 7. Praktische Implementierungsdetails

## 7.1 Lock-TTL vs. Heartbeat-Intervall

| Konfiguration | Failover-Zeit | Redis-Last | Empfehlung |
|---|---|---|---|
| TTL=15s, Heartbeat=5s | 10-15s | Höher | Zu aggressiv, empfindlich gegen Jitter |
| TTL=30s, Heartbeat=10s | 20-30s | Moderat | **EMPFOHLEN** |
| TTL=60s, Heartbeat=20s | 40-60s | Niedrig | Konservativ, akzeptabel für Cron |

**Begründung:** Cron-Tasks haben Minutengenauigkeit. Failover von 20-30s ist völlig akzeptabel.

## 7.2 Split-Brain-Szenarien

**Wann kann es zwei Leader geben?**

1. **GC-Pause / Event-Loop-Stall:** Leader wird für >TTL blockiert. Lock läuft ab, neuer Leader gewählt. Alter Leader "erwacht" und denkt er ist noch Leader.
   - **Mitigation:** VOR jedem Dispatch `GET swarmq:scheduler:lock` prüfen ob eigene Worker-ID noch gesetzt ist.

2. **Netzwerkpartition:** Leader kann Valkey nicht erreichen, Lock läuft ab. Alter Leader kann auch keine Tasks dispatchen (vermutlich auch kein Broker-Zugang).

3. **Valkey-Restart ohne Persistenz:** Lock geht verloren, alle Worker konkurrieren. SETNX garantiert: nur einer gewinnt.

## 7.3 Clock-Drift zwischen Workern

**Für Lock:** Irrelevant — Valkey verwaltet TTL serverseitig.

**Für Cron-Auswertung:** Relevant! Letzter Ausführungszeitpunkt muss in Valkey persistiert werden (nicht im Worker-Speicher), damit nach Leader-Wechsel korrekt weitergearbeitet wird:

```python
# Pro Schedule in Valkey:
# swarmq:schedule:{name}:last_run → Unix-Timestamp

last_run = await valkey.get(f"swarmq:schedule:{name}:last_run")
next_due = croniter(cron_expr, float(last_run or 0)).get_next()
if next_due <= time.time():
    await dispatch_task(schedule)
    await valkey.set(f"swarmq:schedule:{name}:last_run", str(time.time()))
```

## 7.4 Failover-Dauer nach Crash

| Szenario | Failover-Zeit |
|---|---|
| Leader crasht (SIGKILL) | Max. volle TTL (30s) |
| Leader fährt sauber herunter (SIGTERM) | Sofort (Lock wird freigegeben) |
| Leader verliert Netzwerk zu Valkey | TTL (30s) |
| Valkey restartet | Millisekunden (alle Worker konkurrieren, SETNX entscheidet) |

## 7.5 Gleichzeitiger Neustart aller Worker

Szenario: Deployment, alle Worker starten gleichzeitig.

- Alle Worker versuchen `SET ... NX`. Valkey garantiert Atomarität — genau einer gewinnt.
- Wenn Lock noch von vorheriger Instanz gehalten (TTL nicht abgelaufen): max. 30s Verzögerung.
- **Mitigation:** Beim Deployment zuerst Graceful Shutdown (Lock wird freigegeben), dann neue Worker starten.

## 7.6 Valkey-Persistenz

Wenn Valkey ohne Persistenz (RDB/AOF) läuft und restartet, gehen alle Locks und Schedule-State verloren. Alle Worker konkurrieren sofort um neuen Lock. `last_run`-Timestamps sind ebenfalls verloren → Missed-Run-Handling greift.

**Empfehlung:** Valkey sollte mit AOF oder RDB-Persistenz konfiguriert sein.

---

# 8. Bekannte Bugs und Lehren aus RedBeat

## 8.1 Bug #127: Lock-Verlängerung ohne Ownership-Check

**Problem:** `pexpire` wird ohne zu prüfen aufgerufen, ob man noch der Lock-Besitzer ist.

**Szenario:**
1. Scheduler A hält Lock
2. Netzwerkunterbrechung — Scheduler A kann Lock nicht verlängern
3. Lock läuft ab, Scheduler B erwirbt Lock
4. Netzwerk kommt zurück, Scheduler A ruft `pexpire` auf → verlängert den Lock von B
5. Beide Scheduler denken, sie sind Leader → doppelte Ausführung

**Lösung für SwarmQ:** Lua-Script das Ownership prüft bevor `pexpire` aufgerufen wird.

## 8.2 Bug #208: Lock-Fehler wird als "kein Lock konfiguriert" interpretiert

**Problem:** Bei Redis-Verbindungsfehlern während `lock.acquire()` bleibt `scheduler.lock = None`. Der Code interpretiert `None` als "Lock deaktiviert" statt "Lock-Erwerb fehlgeschlagen" → Scheduler läuft ohne Lock.

**Lösung für SwarmQ:** Klare Unterscheidung zwischen:
- Lock deaktiviert (bewusste Konfiguration)
- Lock-Erwerb fehlgeschlagen (Fehler → Scheduler darf NICHT starten)
- Lock erfolgreich erworben (normaler Betrieb)

## 8.3 Zusammenfassung der Regeln

1. Lock-Verlängerung IMMER mit Ownership-Check (Lua-Script)
2. Lock-Erwerb-Fehler = Scheduler darf nicht laufen (nicht ignorieren)
3. VOR jedem Dispatch: nochmal prüfen ob man noch Leader ist
4. Lock-Freigabe: nur eigenen Lock löschen (Lua-Script)
5. `is_leader` als lokaler State reicht nicht — immer gegen Valkey verifizieren bei Aktionen

---

# 9. Entscheidung und Begründung

## Gewählt: Valkey-basierter Distributed Lock mit Embedded Scheduler

## Begründung

| Faktor | Valkey Lock | RabbitMQ SAC | APScheduler Stil |
|---|---|---|---|
| **Zuverlässigkeit** | Hoch (Effizienz-Lock) | Sehr hoch | Sehr hoch |
| **Komplexität** | ~150 LOC | Niedrig-Mittel | Hoch (neues Subsystem) |
| **Failover** | 20-30s | <5s | Abhängig von Implementierung |
| **Zusätzliche Deps** | Keine | Keine | Erheblich |
| **Broker-unabhängig** | Ja (nur Valkey) | Nein (RabbitMQ-spezifisch) | Teilweise |
| **Schedule-Persistenz** | Valkey | Valkey (trotzdem nötig) | Eigener Data Store |
| **Debugging** | `GET swarmq:scheduler:lock` | RabbitMQ Management UI | Komplex |

**Entscheidende Faktoren:**
1. Valkey ist ohnehin als Backend da — keine neue Infrastruktur
2. Broker-unabhängig — funktioniert auch wenn später Valkey als Broker kommt
3. Niedrige Komplexität bei ausreichender Zuverlässigkeit
4. Bekannte Bugs aus RedBeat sind dokumentiert und vermeidbar
5. 20-30s Failover ist für Cron-Tasks (Minutengenauigkeit) akzeptabel
