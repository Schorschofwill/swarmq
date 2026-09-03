# Worker-Architektur: Multiprocessing + asyncio + Signal-Handling

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument fuer die Implementierung der SwarmQ Worker-Architektur. Basiert auf Recherche der Python-Dokumentation, Analyse von TaskIQ/Dramatiq, und verifiziert durch Prototyp-Tests.
**Prototyp:** `prototypes/worker_prototype.py` (alle 6 Signal-Tests bestanden)

---

## Inhaltsverzeichnis

1. [Architektur-Uebersicht](#1-architektur-uebersicht)
2. [Process-Start-Methoden](#2-process-start-methoden)
3. [Signal-Handling](#3-signal-handling)
4. [Graceful Shutdown](#4-graceful-shutdown)
5. [Process-Lifecycle-Management](#5-process-lifecycle-management)
6. [Inter-Process-Communication](#6-inter-process-communication)
7. [Bekannte Pitfalls](#7-bekannte-pitfalls)
8. [Erkenntnisse aus TaskIQ und Dramatiq](#8-erkenntnisse-aus-taskiq-und-dramatiq)
9. [Verifizierte Patterns (Prototyp-Tests)](#9-verifizierte-patterns)
10. [Konkrete Implementierungsempfehlung](#10-konkrete-implementierungsempfehlung)

---

# 1. Architektur-Uebersicht

```
                    +--------------------------+
                    |     ProcessManager       |
                    |  (Main-Prozess, KEIN     |
                    |   asyncio Event-Loop)    |
                    |                          |
                    |  signal.signal(SIGTERM)  |
                    |  signal.signal(SIGINT)   |
                    |  signal.signal(SIGHUP)   |
                    |  signal.signal(SIGCHLD)  |
                    |                          |
                    |  Monitor-Loop:           |
                    |  - is_alive() polling    |
                    |  - Auto-Restart          |
                    |  - Backoff-Logik         |
                    +----+--------+-------+----+
                         |        |       |
                    spawn|   spawn|  spawn|
                         v        v       v
              +----------+  +----------+  +----------+
              | Worker 1 |  | Worker 2 |  | Worker N |
              |          |  |          |  |          |
              | asyncio  |  | asyncio  |  | asyncio  |
              | .run()   |  | .run()   |  | .run()   |
              |          |  |          |  |          |
              | SIGTERM: |  | SIGTERM: |  | SIGTERM: |
              | add_sig_ |  | add_sig_ |  | add_sig_ |
              | handler()|  | handler()|  | handler()|
              |          |  |          |  |          |
              | SIGINT:  |  | SIGINT:  |  | SIGINT:  |
              | SIG_IGN  |  | SIG_IGN  |  | SIG_IGN  |
              +----------+  +----------+  +----------+
                   |              |             |
                   v              v             v
              [RabbitMQ -- Tasks holen via aio-pika]
              [Valkey -- Results speichern]
```

**Kern-Entscheidungen:**
- ProcessManager hat **keinen** asyncio Event-Loop (reines `signal.signal()` + `time.sleep()`)
- Jeder Worker hat seinen **eigenen** asyncio Event-Loop via `asyncio.run()`
- Kommunikation Parent → Worker: `multiprocessing.Event` (Shutdown)
- Worker sind autonom: holen Tasks selbst vom Broker, speichern Results selbst

---

# 2. Process-Start-Methoden

## 2.1 Vergleich

| Methode | Mechanismus | Sicherheit mit asyncio | Geschwindigkeit |
|---------|-------------|----------------------|-----------------|
| `fork` | `os.fork()`, Copy-on-Write | **UNSICHER** — kopiert Event-Loop-State, Locks, FDs | Schnell |
| `spawn` | Neuer Python-Interpreter + Pickle | **SICHER** — komplett frischer Prozess | Langsam |
| `forkserver` | Forkt von dediziertem Server-Prozess | **SICHER** (wenn vor Threading gestartet) | Mittel |

## 2.2 Warum `fork` mit asyncio gefaehrlich ist

1. **Event-Loop-State:** asyncio-Loop hat internen State (Callbacks, registrierte FDs, Timer). Wird 1:1 kopiert, aber OS-Ressourcen (epoll-FDs, Sockets) werden geteilt oder sind ungueltig.
2. **Locks in korruptem Zustand:** Wenn `fork()` waehrend ein Thread einen Lock haelt (Logging-Lock, asyncio-interner Lock), ist er im Child permanent gesperrt — der Thread existiert nicht.
3. **File-Descriptors:** Offene Connections (DB, Redis) werden geteilt. Zwei Prozesse auf demselben Socket = korrupte Daten.
4. **Python 3.12+:** `DeprecationWarning` bei `fork` mit aktiven Threads.
5. **Python 3.14:** Default wechselt von `fork` zu `forkserver` auf POSIX.

## 2.3 Empfehlung fuer SwarmQ

```python
# IMMER expliziten Context verwenden, nie set_start_method()
ctx = multiprocessing.get_context("spawn")
process = ctx.Process(target=worker_entry, args=(shutdown_event,), daemon=False)
process.start()
```

**Warum `spawn` und nicht `forkserver` — fuer Worker:**
- Komplett frischer Interpreter — kein korrupter State moeglich
- Worker starten selten (nur bei Start/Restart) — Startup-Overhead irrelevant
- Keine Probleme mit nicht-picklebaren globalen Objekten

**Wo diese Begruendung nicht mehr traegt:** Sie steht und faellt mit
„starten selten". Fuer den CPU-Worker-Modus (`docs/cpu-worker-machbarkeit.md`)
gilt das Gegenteil — dort entsteht **ein Rechenkind je Task**, und der
Startup-Overhead ist die entscheidende Groesse. Gemessen am 2026-08-31:

| Variante | Kindstart (Median) |
|---|---|
| `spawn`, ohne Import | 34 ms |
| `spawn`, mit swarmq-Import | 114 ms |
| **`forkserver` + Preload** | **6 ms** |

Bei 6 ms wird das Recycling-Problem gegenstandslos statt geloest: man nimmt
fuer jeden Task ein frisches Kind, und kein Leak ueberlebt je einen Task.
Bei 114 ms waere das unbezahlbar. Der Sicherheitseinwand aus 2.2 bleibt
dabei gewahrt, weil der Forkserver **nur importiert und nie rechnet** — er
hatte im Test 1 Thread, obwohl numpy und Pillow vorgeladen waren. Genau
diese Bedingung muss die Implementierung erzwingen.

Fuer Worker-Subprozesse bleibt `spawn` die Wahl; fuer Rechenkinder ist es
`forkserver`.

**Warum `daemon=False`:**
- Daemon-Prozesse werden sofort getoetet wenn Parent endet — kein Graceful Shutdown
- Wir wollen kontrolliertes Shutdown mit Task-Drain

---

# 3. Signal-Handling

## 3.1 Zwei verschiedene APIs fuer zwei verschiedene Kontexte

| API | Kontext | Erlaubte Ops | Wann |
|-----|---------|-------------|------|
| `signal.signal()` | Parent (kein Event-Loop) | Nur signal-safe: Flag setzen, `os.write()` Pipe, `Event.set()` | ProcessManager |
| `loop.add_signal_handler()` | Worker (mit Event-Loop) | Alles — wird als normaler Callback ausgefuehrt | AsyncWorker |

## 3.2 Signal-Verteilung

| Signal | Parent-Prozess | Worker-Prozess |
|--------|---------------|----------------|
| **SIGINT** (Ctrl+C) | Shutdown einleiten, Event setzen | **IGNORIEREN** (`signal.SIG_IGN`) |
| **SIGTERM** | Shutdown einleiten, Event setzen | `loop.add_signal_handler()` → lokales Shutdown-Event |
| **SIGHUP** | Reload-Flag setzen, Shutdown einleiten | Ignoriert (Parent kuemmert sich) |
| **SIGCHLD** | Zombie-Vermeidung via `os.waitpid()` | Nicht relevant |

## 3.3 Warum SIGINT im Worker ignorieren

Ctrl+C sendet SIGINT an die **gesamte Prozessgruppe** — Parent UND alle Worker gleichzeitig. Wenn jeder Prozess unabhaengig reagiert, entstehen Race-Conditions. Stattdessen:

1. Worker ignorieren SIGINT (`signal.signal(signal.SIGINT, signal.SIG_IGN)`)
2. Nur der Parent reagiert
3. Parent setzt `multiprocessing.Event`
4. Worker bemerken das Event und fahren koordiniert runter

**Verifiziert in Test 5:** Worker lief weiter nachdem SIGINT direkt an ihn gesendet wurde.

## 3.4 Signal-Blocking vor Worker-Start (Dramatiq-Pattern)

```python
# VOR dem Spawnen: Signale blockieren
signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGINT, signal.SIGTERM, signal.SIGHUP))

# Worker starten (erben die Block-Maske)
for _ in range(num_workers):
    process.start()

# Signal-Handler im Parent registrieren
signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, handler)

# DANACH: Signale entblocken
signal.pthread_sigmask(signal.SIG_UNBLOCK, (signal.SIGINT, signal.SIGTERM, signal.SIGHUP))
```

**Warum:** Ohne Blocking kann ein Signal in der Luecke zwischen `Process.start()` und Handler-Setup eintreffen — dann wird der Default-Handler ausgefuehrt (Prozess-Tod).

## 3.5 uvloop-Einschraenkung

uvloop reserviert SIGCHLD intern fuer Subprocess-Tracking. `loop.add_signal_handler(signal.SIGCHLD, ...)` wirft `RuntimeError`. SIGCHLD nur im Parent via `signal.signal()` behandeln, nie in Workern.

---

# 4. Graceful Shutdown

## 4.1 Dreistufiges Shutdown-Modell

```
Signal empfangen (SIGTERM/SIGINT)
  |
  v
[Stufe 1: Cooperative Shutdown — 10s]
  Parent setzt multiprocessing.Event
  Worker bemerken Event (Polling im Executor, 0.5s Intervall)
  Worker setzen lokales asyncio.Event
  Task-Loop nimmt keine neuen Tasks an
  Laufende Tasks: Soft-Timeout (5s) zum Fertigwerden
  |
  |-- Alle Tasks fertig? → Sauberer Exit
  |
  v
[Stufe 2: SIGTERM — 5s]
  Parent sendet SIGTERM an verbleibende Worker
  Worker: add_signal_handler setzt Shutdown-Event (Notfall)
  Verbleibende Tasks werden cancelled (asyncio.Task.cancel())
  Cancelled Tasks koennen finally-Bloecke ausfuehren
  |
  |-- Alle Prozesse beendet? → Exit
  |
  v
[Stufe 3: SIGKILL]
  Parent sendet SIGKILL — sofortige Terminierung
  Keine Cleanup-Moeglichkeit
```

**Verifiziert in Test 1+6:** Nach SIGTERM wurden 12 Tasks noch sauber abgeschlossen (Soft-Shutdown funktioniert).

## 4.2 Worker-interner Shutdown: `asyncio.wait()` statt `asyncio.gather()`

```python
# EMPFOHLEN: asyncio.wait() — hat timeout, gibt (done, pending) zurueck
done, pending = await asyncio.wait(active_tasks, timeout=5.0, return_when=asyncio.ALL_COMPLETED)
for task in pending:
    task.cancel()

# NICHT: asyncio.gather() — kein Timeout, blockiert
results = await asyncio.gather(*tasks, return_exceptions=True)  # Kein Timeout!
```

## 4.3 Cooperative Shutdown via asyncio.Event (besser als Task.cancel())

```python
# Task-Loop prueft Shutdown-Event VOR dem Annehmen neuer Tasks
while not self._local_shutdown.is_set():
    task = await get_next_from_broker()
    await process_task(task)

# Laufende Tasks laufen zu Ende — kein Cancel noetig
# Nur bei Timeout: dann cancel als Fallback
```

## 4.4 Unfertige Tasks: NACK an Broker

```python
async def _run_single_task(self, task_msg):
    try:
        result = await user_code(task_msg)
        await broker.ack(task_msg)  # Erfolgreich
    except asyncio.CancelledError:
        await broker.nack(task_msg)  # Zurueck in die Queue!
        raise
```

**Verifiziert:** Der Prototyp zeigt "wuerde NACK an Broker senden" bei gecancelten Tasks.

---

# 5. Process-Lifecycle-Management

## 5.1 Monitor-Loop im Parent

```python
def _monitor_loop(self):
    while not shutting_down:
        # Tote Worker erkennen
        for pid, info in list(workers.items()):
            if not info.process.is_alive():
                handle_dead_worker(pid, info)

        # Fehlende Worker nachstarten
        while len(workers) < num_workers:
            start_worker()

        time.sleep(0.5)  # Polling-Intervall
```

## 5.2 Auto-Restart mit Backoff

```python
MAX_RESTARTS = 5
RESTART_WINDOW = 60.0  # Sekunden

def maybe_restart(old_info):
    now = time.monotonic()
    if now - old_info.last_restart > RESTART_WINDOW:
        restart_count = 0  # Reset nach Fenster
    else:
        restart_count = old_info.restart_count + 1

    if restart_count >= MAX_RESTARTS:
        log.error("Zu viele Restarts — gebe auf")
        return

    new_worker = start_worker()
    new_worker.restart_count = restart_count
```

**Verifiziert in Test 3:** Getoeteter Worker wurde automatisch neugestartet, 2 Worker wieder aktiv.

## 5.3 Zombie-Vermeidung

```python
# SIGCHLD-Handler im Parent:
def _on_sigchld(signum, frame):
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
        except ChildProcessError:
            break

# PLUS: process.join(timeout=1) im Monitor-Loop fuer tote Worker
```

## 5.4 Exit-Codes

| Code | Bedeutung | Restart? |
|------|-----------|----------|
| 0 | Normaler Shutdown | Nein |
| 1 | Unbehandelter Fehler | Ja (mit Backoff) |
| 2 | Restart angefordert | Ja (sofort) |
| -9 (137) | SIGKILL / OOM-Killer | Ja (mit Backoff) |
| -15 | SIGTERM | Nein (kontrolliert) |

## 5.5 Bekanntes Problem: `is_alive()` Verzoegerung bei `spawn`

Im Prototyp-Test beobachtet: Worker beendet `asyncio.run()` und loggt "beendet", aber `process.is_alive()` gibt fuer einige Sekunden noch `True` zurueck. Das liegt am Teardown des `spawn`-Prozesses (Interpreter-Shutdown, FD-Cleanup). 

**Loesung fuer SwarmQ:** Im Shutdown nicht nur `is_alive()` pruefen, sondern zusaetzlich `process.join(timeout=0.5)` aufrufen — das wartet aktiv auf den Prozess-Exit.

---

# 6. Inter-Process-Communication

## 6.1 Nur das Noetigste

| Kanal | Richtung | Mechanismus | Zweck |
|-------|----------|------------|-------|
| Shutdown | Parent → Worker | `multiprocessing.Event` | Einziger IPC-Kanal |
| Tasks | Broker → Worker | RabbitMQ (extern) | Kein internes IPC noetig |
| Results | Worker → Backend | Valkey (extern) | Kein internes IPC noetig |
| Logs | Worker → stdout | Standard-Streams | Optional: QueueHandler |

**Kein Shared State.** Jeder Worker ist autonom.

## 6.2 multiprocessing.Event fuer Shutdown

```python
# Parent setzt:
shutdown_event.set()  # Signal-safe!

# Worker pollt (Event ist nicht async-nativ):
async def _monitor_mp_event(self):
    loop = asyncio.get_running_loop()
    while not local_shutdown.is_set():
        is_set = await loop.run_in_executor(None, shutdown_event.wait, 0.5)
        if is_set:
            local_shutdown.set()
```

**Warum Polling:** `multiprocessing.Event` ist nicht awaitable. Der `run_in_executor`-Wrapper blockiert einen Thread-Pool-Thread fuer maximal 0.5s pro Iteration — vernachlaessigbarer Overhead.

---

# 7. Bekannte Pitfalls

## 7.1 Fork + Threading = Deadlock

`fork()` kopiert Locks im gesperrten Zustand. Thread der den Lock halten wuerde existiert im Child nicht → permanenter Deadlock. **Loesung:** `spawn` verwenden.

## 7.2 Fork + asyncio Event-Loop = korrupter State

Event-Loop hat epoll-FD, Timer, Callbacks — alles wird kopiert aber OS-Ressourcen sind geteilt. **Loesung:** `spawn` verwenden.

## 7.3 SIGINT Propagation in Prozessgruppen

Ctrl+C sendet SIGINT an ALLE Prozesse in der Gruppe. Ohne explizite Ignorierung reagieren Parent und Worker gleichzeitig → Race-Conditions, doppeltes Shutdown. **Loesung:** `signal.signal(signal.SIGINT, signal.SIG_IGN)` im Worker (verifiziert in Test 5).

## 7.4 Signal-Race bei Prozess-Start

Signal kann zwischen `Process.start()` und Handler-Setup eintreffen. **Loesung:** `pthread_sigmask(SIG_BLOCK)` vor dem Starten, `SIG_UNBLOCK` nach Handler-Setup (Dramatiq-Pattern).

## 7.5 Logging in spawn-Prozessen

Bei `spawn` wird ein frischer Interpreter gestartet — Logging muss im Worker neu konfiguriert werden (`logging.basicConfig(force=True)`). Fuer zentralisiertes Logging: `QueueHandler` + `QueueListener` verwenden.

## 7.6 `asyncio.shield()` schuetzt NICHT bei Shutdown

`asyncio.all_tasks()` findet den inneren Task und `task.cancel()` cancelt ihn trotzdem. **Loesung:** Cooperative Shutdown via Event statt shield.

## 7.7 Connections NICHT im Parent oeffnen

Bei `spawn` werden Connections nicht geerbt (frischer Prozess). Aber selbst mit `fork`/`forkserver` waeren geteilte Connections korrupt. **Immer:** Connections erst im Worker-Prozess oeffnen.

```python
def worker_entry(shutdown_event):
    async def _run():
        broker = await connect_to_rabbitmq()  # HIER, nicht im Parent
        backend = await connect_to_valkey()   # HIER, nicht im Parent
        try:
            await process_tasks(broker, backend)
        finally:
            await broker.close()
            await backend.close()
    asyncio.run(_run())
```

---

# 8. Erkenntnisse aus TaskIQ und Dramatiq

## 8.1 Von Dramatiq uebernehmen

| Pattern | Beschreibung | Warum |
|---------|-------------|-------|
| **Signal-Blocking vor Fork** | `pthread_sigmask(SIG_BLOCK)` vor Worker-Start | Eliminiert Race-Conditions |
| **SIGINT im Worker ignorieren** | Nur Parent reagiert auf Ctrl+C | Verhindert doppeltes Shutdown |
| **Shutdown-Reihenfolge** | Worker-Tasks zuerst, dann Consumer/Heartbeats | Broker-Connection bleibt offen waehrend Drain |
| **Message-Requeuing** | Unverarbeitete Messages aktiv zurueck in Queue | Keine Message-Verluste bei Shutdown |
| **execvp fuer Reload** | `os.execvp()` bei SIGHUP statt Fork-Neustart | Code-Reload ohne PID-Wechsel |
| **Python 3.14 Start-Method** | Expliziter Umgang mit forkserver-Default | Zukunftssicher |
| **Zentralisiertes Logging** | stdout/stderr ueber Pipe zum Master | Keine Log-Vermischung |

## 8.2 Von TaskIQ uebernehmen

| Pattern | Beschreibung | Warum |
|---------|-------------|-------|
| **Auto-Restart mit max_fails** | Tote Worker automatisch neustarten, mit Limit | Kein Restart-Loop, kein externer Supervisor noetig |
| **asyncio-nativer Worker** | Eigener Event-Loop pro Worker, optional uvloop | Passt zu SwarmQ's async-first Architektur |
| **Backpressure via Semaphore** | `max_async_tasks` und `max_prefetch` | Verhindert Worker-Ueberlastung |
| **Hardkill-Counter** | N-maliges Ctrl+C eskaliert von Soft zu Hard | Benutzer hat Kontrolle |
| **Ack-Typen** | WHEN_RECEIVED / WHEN_EXECUTED / WHEN_SAVED | Passt zu SwarmQ's DeliveryMode |

## 8.3 Fehler vermeiden

| Fehler | Quelle | SwarmQ-Loesung |
|--------|--------|---------------|
| Gleicher Signal-Handler prueft `current_process().name` | TaskIQ | Separate Handler pro Prozessrolle |
| 1s-Polling fuer Worker-Health | TaskIQ | 0.5s-Polling + SIGCHLD |
| Keine Signal-Blockierung beim Fork | TaskIQ | pthread_sigmask (Dramatiq-Pattern) |
| Kein Auto-Restart | Dramatiq | Auto-Restart mit Backoff + max_restarts |
| ctypes-Buffer fuer IPC | Dramatiq | multiprocessing.Event (einfacher, sicherer) |
| `multiprocessing.Event` erzeugt aber nie uebergeben | TaskIQ | Nur erzeugen was gebraucht wird |

---

# 9. Verifizierte Patterns (Prototyp-Tests)

Alle Tests mit `prototypes/worker_prototype.py` und `prototypes/test_signals.sh`.

| # | Test | Ergebnis | Detail |
|---|------|----------|--------|
| 1 | SIGTERM → Graceful Shutdown | **PASS** | 22 Tasks abgeschlossen, "SIGTERM empfangen" → "Shutdown-Event vom Parent" → "Alle Tasks sauber beendet" |
| 2 | SIGINT → Graceful Shutdown | **PASS** | 22 Tasks abgeschlossen, identischer Flow wie SIGTERM |
| 3 | Worker-Kill → Auto-Restart | **PASS** | Worker per SIGKILL getoetet, ProcessManager startet neuen Worker, 2 Worker wieder aktiv |
| 4 | SIGHUP → Reload | **PASS** | "SIGHUP empfangen — Reload angefordert" |
| 5 | Worker ignoriert SIGINT | **PASS** | SIGINT direkt an Worker gesendet, Worker laeuft weiter |
| 6 | Laufende Tasks bei Shutdown | **PASS** | 12 Tasks nach SIGTERM noch abgeschlossen (3s-Tasks, Soft-Timeout greift) |

### Shutdown-Flow im Detail (aus Test 1 Logs)

```
08:31:24.465 [MainProcess] Parent: SIGTERM empfangen — leite Shutdown ein
08:31:24.465 [Worker-0]    Worker 0: Shutdown-Event vom Parent empfangen
08:31:24.465 [Worker-1]    Worker 1: Shutdown-Event vom Parent empfangen
08:31:24.522 [MainProcess] Shutdown: Warte auf 2 Worker (max 10s)...
08:31:24.790 [Worker-0]    Worker 0: Alle Tasks sauber beendet
08:31:24.790 [Worker-0]    Worker 0 beendet. Tasks abgeschlossen: 15
08:31:24.798 [Worker-1]    Worker 1: Alle Tasks sauber beendet
08:31:24.798 [Worker-1]    Worker 1 beendet. Tasks abgeschlossen: 15
08:31:39.840 [MainProcess] Alle Worker beendet
```

**Latenz:** ~325ms vom Signal bis alle Tasks drained + Worker beendet.

### Bekannte Einschraenkung im Prototyp

Nach Worker-Exit (`asyncio.run()` abgeschlossen) dauert der `spawn`-Prozess-Teardown einige Sekunden. Der ProcessManager eskaliert dadurch unnoetig zu SIGTERM/SIGKILL. **Fix fuer Produktion:** `process.join(timeout=0.5)` in der Shutdown-Wait-Loop aufrufen statt nur `is_alive()` pruefen.

---

# 10. Konkrete Implementierungsempfehlung fuer SwarmQ

## 10.1 Dateien und Klassen

```
swarmq/
  worker/
    __init__.py
    process_manager.py    # ProcessManager (Main-Prozess, kein asyncio)
    async_worker.py       # AsyncWorker (Worker-Prozess, mit asyncio)
    entry.py              # worker_entry() — spawn Target-Funktion
    shutdown.py           # ShutdownManager, Signal-Setup-Helfer
    signals.py            # block_signals(), unblock_signals()
```

## 10.2 Start-Sequenz

```python
def main():
    # 1. CLI-Args parsen
    # 2. Logging konfigurieren
    # 3. ProcessManager erstellen
    manager = ProcessManager(num_workers=N, ...)

    # 4. Signal-Blocking
    block_signals()

    # 5. Worker spawnen (erben Block-Maske)
    manager.start_workers()

    # 6. Signal-Handler im Parent registrieren
    manager.setup_signals()

    # 7. Signale entblocken
    unblock_signals()

    # 8. Monitor-Loop (blockiert)
    manager.monitor_loop()

    # 9. Shutdown (dreistufig)
    manager.shutdown()
```

## 10.3 Worker Entry-Point

```python
def worker_entry(worker_id, shutdown_event, config):
    """Wird in neuem Prozess via spawn ausgefuehrt."""
    # 1. Logging neu konfigurieren (spawn = frisch)
    setup_logging()

    # 2. SIGINT ignorieren
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # 3. Optional: uvloop
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

    # 4. asyncio.run() — Worker-Lifetime
    async def _run():
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, on_sigterm)

        broker = await connect_broker(config)
        backend = await connect_backend(config)
        try:
            worker = AsyncWorker(broker, backend, shutdown_event)
            await worker.run()
        finally:
            await broker.close()
            await backend.close()

    asyncio.run(_run())
```

## 10.4 Konfigurierbare Parameter

| Parameter | Default | Beschreibung |
|-----------|---------|-------------|
| `num_workers` | CPU-Kerne | Anzahl Worker-Prozesse |
| `max_async_tasks` | 10 | Concurrent Tasks pro Worker (Semaphore) |
| `max_prefetch` | 2 | Prefetch-Buffer (Backpressure) |
| `soft_shutdown_timeout` | 30s | Warten auf laufende Tasks |
| `hard_shutdown_timeout` | 5s | Warten nach Task-Cancel |
| `max_restarts` | 5 | Max Neustarts pro Worker |
| `restart_window` | 60s | Zeitfenster fuer Restart-Zaehlung |
| `monitor_interval` | 0.5s | Health-Check-Frequenz |
| `mp_event_poll` | 0.5s | Polling-Intervall fuer Shutdown-Event |

---

## Quellen

### Python-Dokumentation
- multiprocessing: docs.python.org/3/library/multiprocessing.html
- asyncio Signal-Handling: docs.python.org/3/library/asyncio-eventloop.html#unix-signals
- Python 3.14 What's New: docs.python.org/3/whatsnew/3.14.html
- CPython Issue #84559: fork start method is broken

### Referenz-Implementierungen
- TaskIQ ProcessManager: github.com/taskiq-python/taskiq → `taskiq/cli/worker/process_manager.py`
- TaskIQ Worker-Start: `taskiq/cli/worker/run.py`
- Dramatiq CLI/Master: github.com/Bogdanp/dramatiq → `dramatiq/cli.py`
- Dramatiq Worker/Threads: `dramatiq/worker.py`
- Dramatiq ShutdownNotifications: `dramatiq/middleware/shutdown.py`

### Sonstiges
- Graceful Shutdowns with asyncio (roguelynn.com)
- Signal handling with async multiprocesses (Medium)
- uvloop SIGCHLD Issue #409
