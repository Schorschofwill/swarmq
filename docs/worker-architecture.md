# Worker-Architektur: Multiprocessing + asyncio + Signal-Handling

**Erstellt:** 2026-04-15 · **Überarbeitet:** 2026-10-24 (Stand 0.14.4)
**Zweck:** Referenzdokument für die Prozess- und Signalarchitektur des Workers.
Die Abschnitte 1, 4, 5, 6 und 11–13 beschreiben die heutige Implementierung.
Die Abschnitte 2, 3, 7 und 8 sind die Recherche vom April 2026 (Python-Doku,
TaskIQ, Dramatiq); wo die Implementierung davon abweicht, steht es dort.
Die Abschnitte 9 und 10 sind historisch (Prototyp und erster Entwurf).
**Prototyp:** `prototypes/worker_prototype.py` mit `prototypes/test_signals.sh`
(alle 6 Signal-Tests bestanden, April 2026).
**Nutzerdoku:** `website/reference/cli.md` (`swarmq worker`),
`website/reference/configuration.md`, `website/guides/cpu-mode.md`.

---

## Inhaltsverzeichnis

1. [Architektur-Übersicht](#1-architektur-übersicht)
2. [Process-Start-Methoden](#2-process-start-methoden)
3. [Signal-Handling](#3-signal-handling)
4. [Graceful Shutdown](#4-graceful-shutdown)
5. [Process-Lifecycle-Management](#5-process-lifecycle-management)
6. [Inter-Process-Communication](#6-inter-process-communication)
7. [Bekannte Pitfalls](#7-bekannte-pitfalls)
8. [Erkenntnisse aus TaskIQ und Dramatiq](#8-erkenntnisse-aus-taskiq-und-dramatiq)
9. [Verifizierte Patterns (Prototyp-Tests)](#9-verifizierte-patterns-prototyp-tests)
10. [Ursprüngliche Implementierungsempfehlung (historisch)](#10-ursprüngliche-implementierungsempfehlung-historisch)
11. [Supervisor als Standard, Lebenszeichen, Recycling](#11-supervisor-als-standard-lebenszeichen-recycling)
12. [CPU-Modus](#12-cpu-modus)
13. [Autoskalierung und Kontrollkanal](#13-autoskalierung-und-kontrollkanal)

---

# 1. Architektur-Übersicht

```
                 +-----------------------------------+
                 |  Supervisor (ProcessManager)      |
                 |  Hauptprozess, KEIN dauerhafter   |
                 |  asyncio-Event-Loop               |
                 |                                   |
                 |  signal.signal(SIGTERM, SIGINT)   |
                 |    -> Herunterfahren              |
                 |  signal.signal(SIGHUP)            |
                 |    -> Reload vormerken            |
                 |  (kein SIGCHLD-Handler)           |
                 |                                   |
                 |  Monitor-Loop alle 0,5 s:         |
                 |  Lebenszeichen, Eskalation,       |
                 |  tote Worker, Slot-Abkühlung,     |
                 |  Reload-Probe, Sollwert,          |
                 |  Nachfüllen, Autoskalierung       |
                 +---+-------------+-------------+---+
                     |             |             |
                spawn|        spawn|        spawn|   Slot 0..N-1
                     v             v             v
              +-----------+  +-----------+  +-----------+
              | Worker 0  |  | Worker 1  |  | Worker N  |
              | asyncio   |  | asyncio   |  | asyncio   |
              | .run()    |  | .run()    |  | .run()    |
              |           |  |           |  |           |
              | SIGTERM + |  | SIGTERM + |  | SIGTERM + |
              | SIGINT:   |  | SIGINT:   |  | SIGINT:   |
              | add_sig_  |  | add_sig_  |  | add_sig_  |
              | handler() |  | handler() |  | handler() |
              +-----+-----+  +-----------+  +-----------+
                    |  nur --pool cpu:
                    v
              Forkserver (nur Import, 1 Thread)
                    | fork je Task (oder je Slot mit DI)
                    v
              Rechenkinder
                   |              |             |
                   v              v             v
              [RabbitMQ -- Tasks holen via aio-pika]
              [Valkey -- Results, Locks, Scheduler, Sollwerte]
```

**Kern-Entscheidungen (umgesetzt):**
- Jeder `swarmq worker` läuft seit 0.5.0 unter dem Supervisor, auch mit
  einem Prozess (Abschnitt 11).
- Der Supervisor hat keinen dauerhaften Event-Loop: `signal.signal()` und
  `time.sleep()`. Für die wenigen asynchronen Aufrufe (Queue-Tiefe messen,
  Sollwert lesen und schreiben) öffnet er je Aufruf ein eigenes
  `asyncio.run` mit eigener Zeitgrenze.
- Jeder Worker hat seinen eigenen Event-Loop via `asyncio.run()`.
- Parent → Worker: `multiprocessing.Event` (Herunterfahren); Worker →
  Supervisor: `SupervisorChannel` (Lebenszeichen, Recycling-Absicht,
  Bestätigung; Abschnitt 6).
- Worker sind autonom: Sie holen Tasks selbst vom Broker und speichern
  Results selbst.
- Jedes Worker-Kind bindet sich per Parent-Death-Signal (`prctl`, SIGTERM)
  an den Supervisor und drainiert, wenn dieser hart stirbt.

**Module (`src/swarmq/`):**

| Modul | Inhalt |
|---|---|
| `process_manager/supervisor.py` | `ProcessManager`: Start, Signale, Monitor-Loop, Herunterfahren, Exit-Codes |
| `process_manager/restart_budget.py` | Lebenszeichen prüfen, hängende Worker ersetzen, Neustart-Budget |
| `process_manager/slots.py` | Slot-Tabelle (frei, aktiv, Übergang, gesperrt) und `WorkerInfo` |
| `process_manager/worker_target.py` | Signalblockade beim Start, Kindziel `run_worker_entry`, Parent-Death-Signal |
| `process_manager/entry.py` | `worker_subprocess_entry`: Logging, Shutdown-Watcher, Lebenszeichen, Exit-Codes des Kindes |
| `process_manager/reload.py` | SIGHUP-Reload: Probe der `--log-config`, `execvpe` |
| `process_manager/autoscale.py`, `sampler.py` | Autoskalierung nach Queue-Tiefe |
| `process_manager/setpoint.py`, `setpoint_store.py` | Kontrollkanal: Sollwert lesen, anwenden, Status melden |
| `process_manager/run.py`, `config.py`, `check.py` | Einstieg aus der CLI, `WorkerConfig`, `--check` |
| `supervision.py` | `SupervisorChannel`, Exit-Code-Konstanten, `SHUTDOWN_TERM_S` |
| `shutdown_budget.py` | Phasen des Herunterfahrens im Gesamtbudget |
| `worker/lifecycle.py`, `worker/shutdown.py` | Start und Drain des Workers im Kind |
| `worker/recycling.py` | Selbst-Recycling nach Taskzahl oder RSS |
| `worker/compute/` | CPU-Modus: Forkserver, Slot-Pool, Rechenkind |
| `slots.py` | Pool-Modus und Slot-Voreinstellung (zugeteilte CPUs) |

---

# 2. Process-Start-Methoden

*Recherche April 2026, ergänzt um die Messung für den CPU-Modus.*

## 2.1 Vergleich

| Methode | Mechanismus | Sicherheit mit asyncio | Geschwindigkeit |
|---------|-------------|----------------------|-----------------|
| `fork` | `os.fork()`, Copy-on-Write | **UNSICHER** — kopiert Event-Loop-State, Locks, FDs | Schnell |
| `spawn` | Neuer Python-Interpreter + Pickle | **SICHER** — komplett frischer Prozess | Langsam |
| `forkserver` | Forkt von dediziertem Server-Prozess | **SICHER** (wenn vor Threading gestartet) | Mittel |

## 2.2 Warum `fork` mit asyncio gefährlich ist

1. **Event-Loop-State:** Der asyncio-Loop hat internen State (Callbacks, registrierte FDs, Timer). Er wird 1:1 kopiert, aber OS-Ressourcen (epoll-FDs, Sockets) werden geteilt oder sind ungültig.
2. **Locks in korruptem Zustand:** Hält beim `fork()` ein Thread einen Lock (Logging-Lock, asyncio-interner Lock), ist er im Kind dauerhaft gesperrt — der Thread existiert dort nicht.
3. **File-Descriptors:** Offene Verbindungen (DB, Redis) werden geteilt. Zwei Prozesse auf demselben Socket = korrupte Daten.
4. **Python 3.12+:** `DeprecationWarning` bei `fork` mit aktiven Threads.
5. **Python 3.14:** Default wechselt auf POSIX von `fork` zu `forkserver`.

## 2.3 Entscheidung für SwarmQ

```python
# Immer expliziter Context, nie set_start_method()
ctx = multiprocessing.get_context("spawn")
process = ctx.Process(target=run_worker_entry, args=(...), daemon=False)
process.start()
```

So steht es in `ProcessManager.__init__` und `_start_worker`.

**Warum `spawn` für Worker-Prozesse:**
- Komplett frischer Interpreter — kein korrupter State möglich.
- Worker starten selten (Start, Neustart, Recycling) — der Startaufwand ist unerheblich.
- Keine Probleme mit nicht picklebaren globalen Objekten.

**Wo diese Begründung nicht trägt:** Sie steht und fällt mit „starten
selten“. Im CPU-Modus (`docs/cpu-worker-machbarkeit.md`, Abschnitt 12) gilt
das Gegenteil — dort entsteht **ein Rechenkind je Task**, und der
Startaufwand ist die entscheidende Größe. Gemessen am 2026-08-31:

| Variante | Kindstart (Median) |
|---|---|
| `spawn`, ohne Import | 34 ms |
| `spawn`, mit swarmq-Import | 114 ms |
| **`forkserver` + Preload** | **6 ms** |

Bei 6 ms wird das Recycling-Problem gegenstandslos statt gelöst: Jeder Task
bekommt ein frisches Kind, und kein Leck überlebt ihn. Bei 114 ms wäre das
unbezahlbar. Der Sicherheitseinwand aus 2.2 bleibt gewahrt, weil der
Forkserver **nur importiert und nie rechnet** — im Test hatte er einen
Thread, obwohl numpy und Pillow vorgeladen waren. Die Implementierung
erzwingt diese Bedingung (Abschnitt 12).

Für Worker-Prozesse bleibt es bei `spawn`, für Rechenkinder bei `forkserver`.

**Warum `daemon=False`:**
- Daemon-Prozesse werden sofort getötet, wenn der Parent endet — kein Graceful Shutdown.
- Gewollt ist ein kontrolliertes Herunterfahren mit Drain.

---

# 3. Signal-Handling

## 3.1 Zwei APIs für zwei Kontexte

| API | Kontext | Erlaubte Ops | Wo |
|-----|---------|-------------|------|
| `signal.signal()` | Supervisor (kein Event-Loop) | Nur signal-sicher: Flag setzen, `Event.set()` | `ProcessManager._setup_signals` |
| `loop.add_signal_handler()` | Worker (mit Event-Loop) | Alles — läuft als normaler Callback | `worker/lifecycle.py` |

## 3.2 Signal-Verteilung (Stand 0.14.4)

| Signal | Supervisor | Worker-Prozess |
|--------|---------------|----------------|
| **SIGTERM** | Herunterfahren: Flag setzen, Shutdown-Event setzen, ausstehenden Reload verwerfen | `add_signal_handler` → `_request_shutdown` (idempotent): Drain |
| **SIGINT** (Ctrl+C) | wie SIGTERM | wie SIGTERM |
| **SIGHUP** | Nur Reload vormerken; der Monitor-Loop prüft ggf. `--log-config` (Probe außerhalb des Handlers), stoppt dann die Worker und führt `execvpe` aus. SIGTERM/SIGINT verwerfen einen ausstehenden oder beschlossenen Reload | kein eigener Handler (Supervisor kümmert sich) |
| **SIGCHLD** | **kein Handler** (siehe 5.3) | – |

Der Handler im Supervisor loggt nicht (Mutex der Log-Queue); das Signal
wird hinterlegt und im Monitor-Loop genau einmal geloggt.

## 3.3 SIGINT im Worker: Empfehlung und Umsetzung

Ctrl+C sendet SIGINT an die **gesamte Prozessgruppe** — Supervisor und alle
Worker gleichzeitig. Der Prototyp ließ die Worker SIGINT deshalb ignorieren
(`signal.SIG_IGN`), damit nur der Parent reagiert (Test 5).

**Umgesetzt ist es anders:** Der Worker behandelt SIGINT wie SIGTERM. Das
ist unschädlich, weil `_request_shutdown` idempotent ist: Ob das Signal
direkt ankommt oder über das Shutdown-Event des Supervisors (das der Worker
per SIGTERM an sich selbst umsetzt, 6.2), der Drain beginnt genau einmal.
Ein Worker ohne Supervisor (eingebettet per `SwarmQ.start_worker()`) bleibt
so per Ctrl+C beendbar.

## 3.4 Signal-Blocking vor Worker-Start (Dramatiq-Pattern, umgesetzt)

`ProcessManager.run` und `process_manager/worker_target.py`:

```python
block_signals()          # SIGINT, SIGTERM, SIGHUP per pthread_sigmask blockieren
self._start_all_workers()
self._setup_signals()    # Handler im Supervisor registrieren
unblock_signals()
```

**Warum:** Ohne Blockade kann ein Signal in der Lücke zwischen
`Process.start()` und dem Handler-Setup eintreffen — dann greift der
Default-Handler (Prozess-Tod).

`spawn` vererbt die Signalmaske. Das Kindziel `run_worker_entry` hebt sie
deshalb als Erstes auf, auch für einen fremden Entry, den ein Betreiber dem
`ProcessManager` gibt; sonst bliebe das SIGTERM des Shutdown-Watchers
anhängig, und der Prozess endete erst per SIGKILL (B15 des
Loadsuite-Backlogs). Danach setzt es das Parent-Death-Signal.

Beim Reload blockiert `process_manager/reload.py` SIGTERM/SIGINT unmittelbar
vor `execvpe` und prüft erst danach, ob inzwischen ein Shutdown-Signal kam
(dann kein Reload). Ein Signal nach dieser Prüfung bleibt über `execve`
hinweg anhängig; der neue Supervisor bekommt es, sobald er die Signale
freigibt.

## 3.5 uvloop-Einschränkung

uvloop reserviert SIGCHLD intern für das Subprocess-Tracking;
`loop.add_signal_handler(signal.SIGCHLD, ...)` wirft `RuntimeError`.
SwarmQ installiert uvloop nicht und registriert SIGCHLD nirgends; der
Hinweis gilt für Apps, die uvloop selbst einsetzen.

---

# 4. Graceful Shutdown

## 4.1 Gesamtbudget ab SIGTERM (Stand 0.14.4)

Seit BZ-18 (0.12.0) ist `SWARMQ_SHUTDOWN_TIMEOUT` (Vorgabe 30 s,
`SwarmQConfig.shutdown_timeout`) das **Gesamtbudget ab SIGTERM**: Bis dahin
müssen Drain, Sperrenfreigabe, Leader-Übergabe, Verbindungsschluss und die
Eskalation des Supervisors fertig sein, bevor Kubernetes oder Docker selbst
SIGKILL schicken. `swarmq.shutdown_budget.shutdown_budget(total, supervised)`
teilt es auf:

```
SIGTERM/SIGINT an den Supervisor (t = 0)
  |
  v
[Phase 1: kooperativ, bis sigterm_at = total - 2 s - 5 s]
  Supervisor setzt das Shutdown-Event
  Worker bemerken es (Watcher, Takt 0,3 s), schicken sich SIGTERM, drainieren:
  keine neuen Nachrichten, laufende Tasks dürfen bis drain_until fertig werden
  (drain_until = sigkill_at - 5 s Aufräumreserve)
  |-- alle Worker beendet? -> Ende
  v
[Phase 2: SIGTERM an Überlebende, bis sigkill_at = total - 2 s]
  |-- alle beendet? -> Ende
  v
[Phase 3: SIGKILL an Überlebende, Einsammeln bis total]
```

Mit der Vorgabe 30 s: SIGTERM bei 23 s, SIGKILL bei 28 s, Drain-Ende im
Worker bei 23 s. Bei kleinen Budgets schrumpfen die Reserven anteilig
(höchstens die Hälfte des Rests), `0` heißt sofort (E-018/F3-42).
Eingebettet (ohne Supervisor) gilt `finish_by = total`.

`SHUTDOWN_TERM_S` (5 s) ist zusätzlich die SIGTERM-Frist für Worker, die
der Supervisor im laufenden Betrieb ersetzt (hängend, Recycling).

## 4.2 Worker-interner Shutdown: `asyncio.wait()` statt `asyncio.gather()`

*Muster aus der Recherche.*

```python
# asyncio.wait() hat einen Timeout und liefert (done, pending)
done, pending = await asyncio.wait(active_tasks, timeout=5.0, return_when=asyncio.ALL_COMPLETED)
for task in pending:
    task.cancel()

# Nicht: asyncio.gather() — kein Timeout, blockiert
results = await asyncio.gather(*tasks, return_exceptions=True)
```

## 4.3 Cooperative Shutdown via asyncio.Event (besser als Task.cancel())

*Muster aus der Recherche.*

```python
# Der Task-Loop prüft das Shutdown-Event vor dem Annehmen neuer Tasks
while not self._local_shutdown.is_set():
    task = await get_next_from_broker()
    await process_task(task)

# Laufende Tasks laufen zu Ende; Cancel nur als Rückfall nach dem Timeout
```

## 4.4 Unfertige Tasks: zurück an den Broker

*Muster aus der Recherche.* Ein abgebrochener, nicht bestätigter Task darf
nicht verloren gehen; er geht an den Broker zurück und wird erneut
zugestellt.

```python
async def _run_single_task(self, task_msg):
    try:
        result = await user_code(task_msg)
        await broker.ack(task_msg)
    except asyncio.CancelledError:
        await broker.nack(task_msg)  # zurück in die Queue
        raise
```

---

# 5. Process-Lifecycle-Management

## 5.1 Monitor-Loop im Supervisor (Stand 0.14.4)

`ProcessManager._monitor_loop` läuft alle `MONITOR_INTERVAL` = 0,5 s:

1. `_check_heartbeats` — Recycling-Absichten beantworten (Ersatz starten,
   bestätigen), hängende Worker ersetzen (Abschnitt 11).
2. `_escalate_terminating` — SIGKILL für ersetzte Worker, die SIGTERM
   überlebt haben (`SHUTDOWN_TERM_S`).
3. `_check_dead_workers` — tote Worker einsammeln (`is_alive()`,
   `join(timeout=1)`), Exit-Code auswerten (5.4), ggf. neu starten.
4. `_unlock_expired_slots` — Slots nach der Abkühlzeit freigeben.
5. Konfigurationsfehler: Ende mit 78 (unter `--reload` warten auf die
   nächste Dateiänderung); alle Slots gesperrt: Ende mit 1; alle Worker
   sauber beendet (`--burst`): Ende mit 0.
6. `reloader.step()` — Reload-Probe starten oder pollen, nie blockierend.
7. `setpoint.tick` — Kontrollkanal (Abschnitt 13).
8. Nachfüllen bis zum Ziel (ohne Autoskalierung: Startzahl minus gesperrte
   und sauber beendete Slots).
9. `_autoscale_tick` — nur mit `--autoscale`.

## 5.2 Neustart-Budget

Kein Backoff mit Wartezeit, sondern ein gleitendes Fenster je Slot
(`process_manager/restart_budget.py`, `_maybe_restart`):

- `MAX_RESTARTS` = 5 Neustarts je `RESTART_WINDOW` = 3600 s.
- Ist das Budget erschöpft, wird der Slot für `SLOT_COOLDOWN_S` = 1 h
  gesperrt (BZ-27, seit 0.12; vorher dauerhaft) und startet danach mit
  leerer Historie neu.
- Sind ohne Autoskalierung alle Slots gesperrt (bzw. gesperrt oder sauber
  beendet) und kein Worker mehr am Leben, endet der Supervisor mit 1
  (`all_worker_slots_exhausted`), damit systemd/Kubernetes ihre eigene
  Neustart-Strategie anwenden.

Ersatz hängender Worker zählt nicht hierher, sondern gegen ein eigenes
Budget (Abschnitt 11).

## 5.3 Zombie-Vermeidung: bewusst kein SIGCHLD-Handler

Der Prototyp sammelte Kinder im SIGCHLD-Handler mit
`os.waitpid(-1, WNOHANG)` ein. Das kollidierte mit dem `waitpid` je PID, das
`multiprocessing` selbst ausführt: Gewann der Handler, bekam `Popen.poll()`
`ECHILD`, `is_alive()` blieb für tote Worker dauerhaft `True` — kein
Neustart, und das Herunterfahren wartete immer die volle Frist. Heute sammelt
ausschließlich die `multiprocessing`-API ein (`is_alive()`, `join()`), binnen
eines `MONITOR_INTERVAL`.

## 5.4 Exit-Codes

Stand 0.14.4 (`ProcessManager._check_dead_workers` und `_handle_exit_code`
in `process_manager/supervisor.py`, Konstanten in `supervision.py`). Der
ursprüngliche Prototyp kannte noch einen Code 2 für „Restart angefordert“,
den es nicht mehr gibt (Reload läuft per `SIGHUP` und `execvpe`).

| Code des Workers | Bedeutung | Neustart? |
|------|-----------|----------|
| 0 | Normales Ende (Burst-Drain o. Ä.) | Nein, der Slot wird stillgelegt; sind alle so beendet, endet der Supervisor mit 0 |
| 78 | Konfigurationsfehler (`EX_CONFIG`, `worker_config_error`) | Nein, der Supervisor endet mit 78; unter `--reload` wartet er auf die nächste Dateiänderung |
| 75 | Recycling ohne Bestätigung (`EX_TEMPFAIL`, `worker_recycle_noack`) | Ja (Neustart-Budget) |
| 71 | CPU-Pool hat seine Fork-Quelle verworfen (`ForkSourceRejected`, `worker_fork_source_rejected`) | Ja, mit frischem Forkserver (Neustart-Budget) |
| 1 / sonstige | Unbehandelter Fehler | Ja (Neustart-Budget, 5.2) |
| -9 | SIGKILL / OOM-Killer | Ja (Neustart-Budget) |
| -15 | SIGTERM | Ja, außer der Supervisor hat den Worker selbst beendet (`expected_shutdown`: Herunterskalieren, Recycling, Ersatz) |

Während des Herunterfahrens wird nichts neu gestartet.

Hängt nach dem Leeren der Log-Queue noch ein aufgegebener Log-Thread, endet
der Worker-Prozess per `os._exit` mit genau diesem Code (`logging_drain.py`).

## 5.5 `is_alive()`-Verzögerung bei `spawn`

Im Prototyp beobachtet: Der Worker beendet `asyncio.run()` und loggt
„beendet“, aber `process.is_alive()` liefert noch einige Sekunden `True`
(Interpreter-Shutdown, FD-Cleanup). Umgesetzt ist der Fix aus dem
Prototyp: Der Supervisor ruft `join()` mit Timeout auf, statt nur
`is_alive()` zu prüfen (`_check_dead_workers`, `_join_all`).

---

# 6. Inter-Process-Communication

## 6.1 Kanäle (Stand 0.14.4)

| Kanal | Richtung | Mechanismus | Zweck |
|-------|----------|------------|-------|
| Shutdown | Supervisor → alle Worker | `multiprocessing.Event` | Herunterfahren, Reload |
| `SupervisorChannel` | beide Richtungen, je Wert ein Schreiber | drei `multiprocessing.Value` je Worker: `heartbeat` (Worker), `intent` (Worker: draining / Recycling), `ack` (Supervisor: Ersatz gestartet); dazu ein gemeinsamer Wert für gesperrte Slots (Metrik) | Lebenszeichen, Recycling-Handshake |
| Rechenkind | Loop-Prozess ↔ Rechenkind | eine `Pipe` je Kind, Leser-Thread im Loop-Prozess | Ready-Handshake, Ergebnis, Fortschritt; EOF = Todesmeldung |
| Queue-Tiefe | Broker → Supervisor | Broker-Abfrage über eine Factory aus der CLI (`sampler.py`) | Autoskalierung |
| Sollwert / Status | Valkey ↔ Supervisor | `sq:control:setpoint:<gruppe>`, `sq:control:status:<gruppe>:<supervisor>` | Kontrollkanal |
| Tasks | Broker → Worker | RabbitMQ | kein internes IPC nötig |
| Results | Worker → Backend | Valkey | kein internes IPC nötig |
| Logs | jeder Prozess → stderr | eigene Log-Queue je Prozess (7.5) | kein zentraler Log-Kanal |

Kein gemeinsamer Anwendungszustand: Jeder Worker ist autonom.

## 6.2 Shutdown-Event im Worker

Der Prototyp pollte das Event per `run_in_executor` mit 0,5 s Timeout.
Umgesetzt ist ein asyncio-Task in `process_manager/entry.py`:

```python
async def _watch_shutdown():
    while True:
        if channel is not None and now >= next_beat:
            channel.beat()                       # Lebenszeichen aus demselben Tick
        if shutdown_event.is_set():
            os.kill(os.getpid(), signal.SIGTERM) # bestehender Signal-Pfad des Workers
            return
        await asyncio.sleep(0.3)
```

Das nicht blockierende `is_set()` braucht keinen Thread. Das Lebenszeichen
kommt bewusst aus demselben Event-Loop-Tick: Ein blockierter Loop schreibt
keins mehr, und genau das erkennt der Supervisor.

---

# 7. Bekannte Pitfalls

## 7.1 Fork + Threading = Deadlock

`fork()` kopiert Locks im gesperrten Zustand; der Thread, der sie freigäbe,
existiert im Kind nicht → dauerhafter Deadlock. **Lösung:** `spawn` für
Worker; im CPU-Modus ein Forkserver, der nachweislich nur einen Thread hat
(Abschnitt 12).

## 7.2 Fork + asyncio Event-Loop = korrupter State

Der Event-Loop hat epoll-FD, Timer, Callbacks — alles wird kopiert, aber
OS-Ressourcen sind geteilt. **Lösung:** wie 7.1; der Forkserver startet
keinen Event-Loop.

## 7.3 SIGINT-Propagation in Prozessgruppen

Ctrl+C sendet SIGINT an alle Prozesse der Gruppe. Reagieren Parent und
Worker unabhängig, drohen doppeltes Herunterfahren und Races. **Lösung in
SwarmQ:** idempotentes `_request_shutdown` im Worker (3.3), statt SIGINT zu
ignorieren.

## 7.4 Signal-Race bei Prozess-Start

Ein Signal kann zwischen `Process.start()` und Handler-Setup eintreffen.
**Lösung:** `pthread_sigmask(SIG_BLOCK)` vor dem Start, `SIG_UNBLOCK` nach
dem Handler-Setup, und im Kind die geerbte Maske aufheben (3.4).

## 7.5 Logging in spawn-Prozessen

Bei `spawn` startet ein frischer Interpreter — das Logging muss in jedem
Kind neu eingerichtet werden. Umsetzung in SwarmQ (Plan 2026-10-06-0831):

- **Die Bibliothek konfiguriert nie.** `SwarmQ()` lässt Root-Handler, Root-Level und structlog-Konfiguration unberührt. Konfiguriert wird nur in den Einstiegspunkten, je Prozess genau einmal per `swarmq.logging.configure_logging(settings)`: im Supervisor (`swarmq worker`), im Worker-Subprozess (`process_manager/entry.py`, **vor** dem App-Import) und im Rechenkind (`worker/compute/child.py`). Ein zweiter Aufruf im selben Prozess ist ein No-op; neu aufgebaut wird nur über `shutdown_logging()` und danach `configure_logging()`.
- **Einmal auflösen, aufgelöst weitergeben.** Der Supervisor löst die Einstellungen einmal auf (CLI > Env > `SwarmQConfig` > Default) und gibt sie als pickle-sicheres `LoggingSettings` über `WorkerConfig` bzw. `ComputeJob` weiter. Die Kinder lesen kein Env. Eine `--log-config`-Datei prüft der Supervisor einmal und reicht die geprüfte Konfiguration als JSON-String weiter; Kinder lesen die Datei nie. Vor einem Reload (`SIGHUP`) prüft der laufende Supervisor die Datei erneut, außerhalb des Signal-Handlers per `Popen`-Probe, die der Monitor-Loop pollt (`process_manager/reload.py`, Plan 2026-10-06-1811 KTD8): ungültig → `log_config_reload_failed`, Frist von 30 s überschritten → `log_config_reload_probe_timeout`, jeweils kein `execvpe`. SIGTERM/SIGINT gewinnen immer gegen einen Reload.
- **Nicht blockierend.** Eingebaut schreibt Root über einen `QueueHandler` mit begrenzter Queue (`--log-queue-size`, Vorgabe 10000) an einen `QueueListener`-Thread (`swarmq-log-lsnr`, höchstens 15 Zeichen für `/proc/<pid>/task/*/comm`), der rendert und nach stderr schreibt. Der Listener startet lazy beim ersten Eintrag; im Forkserver (markiert von `worker/compute/preload.py`) startet er nie, Records gehen dort synchron hinaus — der Forkserver bleibt so single-threaded. Volle Queue → Eintrag verwerfen statt blockieren (WARNING+ haben 10 % Reserve); der Aufrufer zählt nur, der Listener meldet `log_records_dropped` höchstens 1/s als Record, Metrik `swarmq_log_records_dropped_total`. Fehler in Handlern, Filtern und Formattern fängt der Listener (`log_handler_errors`); ein toter Listener startet mit dem nächsten Eintrag neu. `os.register_at_fork` setzt Queue und Listener im Kind zurück (auch die der dictConfig-Datei); der `before`-Hook wartet höchstens 50 ms auf laufende Schreibvorgänge.
- **Leeren vor jedem Exit, begrenzt.** `shutdown_logging()` (auch per `atexit`) leert die Queue vor `execvpe`, vor Exit 78/75 und pro Job im Rechenkind vor dem Ergebnis, höchstens `log_drain_timeout` (Vorgabe 5 s) über alle Listener gemeinsam (`logging_drain.py`); der Rest zählt als verworfen, Abschlusszeile `log_drain_timeout_exceeded`. Hängt danach noch ein aufgegebener Log-Thread, enden swarmq-eigene Prozesse (`console_main`, Worker-Subprozess, Rechenkind) per `os._exit` mit dem ursprünglichen Exit-Code; eingebettete Apps nie. Bei SIGKILL ist verloren, was in der Queue liegt.
- **„App gewinnt“.** Ersetzt das Task-Modul oder ein `--configure`-Hook die Root-Handler, stoppt SwarmQ seinen Root-Listener und leitet `swarmq.*` über eine eigene Queue an die Handler der App (`logging_reconfigured_by_app`); hinzugefügte Handler → `logging_handlers_added_by_app`. Im CPU-Pool hängt `preload.py` vor dem App-Import einen thread-freien Sentinel an Root (nur wenn swarmq konfiguriert); `configure_child_logging` wendet im Kind dieselbe Regel an (`basicConfig` wirkungslos, Ersatz → App gewinnt, `addHandler` → beide schreiben) und startet ungestartete Queue-Listener der App. Grenze: Ein per Pfad gestartetes Hauptmodul wird vor dem Preload importiert.
- **Dateirotation nur außerhalb.** Mit `--log-config` wenden alle Prozesse dieselbe dictConfig an — nur Stream-Handler verwenden, Rotation per journald/logrotate (`copytruncate`)/Collector.

Nutzerdoku: `website/reference/logging.md`.

## 7.6 `asyncio.shield()` schützt NICHT beim Herunterfahren

`asyncio.all_tasks()` findet den inneren Task, und `task.cancel()` bricht
ihn trotzdem ab. **Lösung:** kooperatives Herunterfahren über ein Event
statt `shield`.

## 7.7 Verbindungen NICHT im Parent öffnen

Bei `spawn` werden Verbindungen nicht geerbt (frischer Prozess); mit
`fork`/`forkserver` wären geteilte Verbindungen korrupt. **Immer:**
Verbindungen erst im Worker-Prozess öffnen. Die kurzen Abfragen des
Supervisors (Queue-Tiefe, Sollwert) öffnen und schließen ihre Verbindung je
Aufruf in einem eigenen `asyncio.run`; es gibt keine Verbindung, die ein
Kind erben könnte.

```python
def worker_entry(shutdown_event):
    async def _run():
        broker = await connect_to_rabbitmq()  # hier, nicht im Parent
        backend = await connect_to_valkey()   # hier, nicht im Parent
        try:
            await process_tasks(broker, backend)
        finally:
            await broker.close()
            await backend.close()
    asyncio.run(_run())
```

---

# 8. Erkenntnisse aus TaskIQ und Dramatiq

*Recherche April 2026; die letzte Spalte nennt den Stand 0.14.4.*

## 8.1 Von Dramatiq

| Pattern | Beschreibung | Umgesetzt |
|---------|-------------|-------|
| **Signal-Blocking vor Fork** | `pthread_sigmask(SIG_BLOCK)` vor Worker-Start | Ja (3.4) |
| **SIGINT im Worker ignorieren** | Nur Parent reagiert auf Ctrl+C | Nein, stattdessen idempotenter Drain (3.3) |
| **Shutdown-Reihenfolge** | Worker-Tasks zuerst, dann Consumer/Heartbeats | Ja: Broker-Verbindung bleibt während des Drains offen |
| **Message-Requeuing** | Unverarbeitete Messages aktiv zurück in die Queue | Ja: Nicht bestätigte Nachrichten stellt der Broker erneut zu |
| **exec für Reload** | `os.execvpe(sys.executable, [sys.executable] + argv, env)` bei SIGHUP statt Fork-Neustart; argv ist eine Momentaufnahme von `sys.orig_argv[1:]` beim Supervisor-Start (bleibt bei `python -m swarmq.cli` korrekt), env eine Kopie von `os.environ` | Ja: Code-Reload ohne PID-Wechsel |
| **Python 3.14 Start-Method** | Expliziter Umgang mit dem forkserver-Default | Ja: immer `get_context(...)` |
| ~~Zentralisiertes Logging~~ | Dramatiq leitet stdout/stderr über eine Pipe zum Master | Nein: SwarmQ richtet das Logging je Prozess ein, jeder schreibt selbst nach stderr (7.5) |

## 8.2 Von TaskIQ

| Pattern | Beschreibung | Umgesetzt |
|---------|-------------|-------|
| **Auto-Restart mit max_fails** | Tote Worker automatisch neu starten, mit Limit | Ja (5.2) |
| **asyncio-nativer Worker** | Eigener Event-Loop pro Worker, optional uvloop | Ja, ohne uvloop |
| **Backpressure via Semaphore** | `max_async_tasks` und `max_prefetch` | Ja: `--concurrency` und Prefetch je Queue-Consumer |
| **Hardkill-Counter** | N-maliges Ctrl+C eskaliert von Soft zu Hard | Nein: Das Gesamtbudget eskaliert selbst (4.1) |
| **Ack-Typen** | WHEN_RECEIVED / WHEN_EXECUTED / WHEN_SAVED | Teilweise: `DeliveryMode.AT_LEAST_ONCE` und `FIRE_AND_FORGET` |

## 8.3 Fehler vermeiden

| Fehler | Quelle | SwarmQ-Lösung |
|--------|--------|---------------|
| Gleicher Signal-Handler prüft `current_process().name` | TaskIQ | Getrennte Handler je Prozessrolle |
| 1-s-Polling für Worker-Health | TaskIQ | 0,5-s-Monitor-Loop plus Lebenszeichen; Einsammeln über `multiprocessing`, kein SIGCHLD (5.3) |
| Keine Signal-Blockierung beim Fork | TaskIQ | `pthread_sigmask` (Dramatiq-Pattern) |
| Kein Auto-Restart | Dramatiq | Neustart im gleitenden Fenster, Slot-Sperre mit Abkühlzeit |
| ctypes-Buffer für IPC | Dramatiq | `multiprocessing.Event` und `multiprocessing.Value` mit je einem Schreiber |
| `multiprocessing.Event` erzeugt, aber nie übergeben | TaskIQ | Nur erzeugen, was gebraucht wird |

---

# 9. Verifizierte Patterns (Prototyp-Tests)

*Historisch, April 2026.* Alle Tests mit `prototypes/worker_prototype.py`
und `prototypes/test_signals.sh`.

| # | Test | Ergebnis | Detail |
|---|------|----------|--------|
| 1 | SIGTERM → Graceful Shutdown | **PASS** | 22 Tasks abgeschlossen, „SIGTERM empfangen“ → „Shutdown-Event vom Parent“ → „Alle Tasks sauber beendet“ |
| 2 | SIGINT → Graceful Shutdown | **PASS** | 22 Tasks abgeschlossen, gleicher Ablauf wie SIGTERM |
| 3 | Worker-Kill → Auto-Restart | **PASS** | Worker per SIGKILL getötet, ProcessManager startet neuen Worker, 2 Worker wieder aktiv |
| 4 | SIGHUP → Reload | **PASS** | „SIGHUP empfangen — Reload angefordert“ |
| 5 | Worker ignoriert SIGINT | **PASS** | SIGINT direkt an Worker gesendet, Worker läuft weiter (heute anders, 3.3) |
| 6 | Laufende Tasks bei Shutdown | **PASS** | 12 Tasks nach SIGTERM noch abgeschlossen (3-s-Tasks, Soft-Timeout greift) |

### Shutdown-Ablauf im Detail (aus den Logs von Test 1)

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

**Latenz:** rund 325 ms vom Signal, bis alle Tasks drainiert und die Worker
beendet waren.

### Bekannte Einschränkung im Prototyp

Nach dem Worker-Exit dauerte der Teardown des `spawn`-Prozesses einige
Sekunden; der ProcessManager eskalierte dadurch unnötig zu SIGTERM/SIGKILL.
Der Fix (`join()` mit Timeout statt nur `is_alive()`) ist umgesetzt (5.5).

---

# 10. Ursprüngliche Implementierungsempfehlung (historisch)

> **Historischer Entwurf (Stand 2026-04).** Dieser Abschnitt beschreibt die
> damalige Empfehlung, nicht die heutige Implementierung. Datei- und
> Klassennamen sowie die Parameter in 10.4 existieren so nicht; die heutigen
> Module stehen in Abschnitt 1, die Optionen in der Nutzerdoku
> (`website/reference/cli.md`, `website/reference/configuration.md`).

## 10.1 Dateien und Klassen (Entwurf)

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

## 10.2 Start-Sequenz (Entwurf)

```python
def main():
    # 1. CLI-Args parsen
    # 2. Logging konfigurieren
    # 3. ProcessManager erstellen
    manager = ProcessManager(num_workers=N, ...)

    # 4. Signal-Blocking
    block_signals()

    # 5. Worker spawnen (erben die Block-Maske)
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

## 10.3 Worker-Entry-Point (Entwurf)

```python
def worker_entry(worker_id, shutdown_event, config):
    """Wird in neuem Prozess via spawn ausgeführt."""
    # 1. Logging neu konfigurieren (spawn = frisch), aus den im Supervisor
    #    aufgelösten Settings und vor dem App-Import (siehe 7.5)
    configure_logging(config.logging_settings)

    # 2. SIGINT ignorieren (nicht umgesetzt, siehe 3.3)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # 3. Optional: uvloop (nicht umgesetzt)
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

    # 4. asyncio.run() — Lebensdauer des Workers
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

## 10.4 Konfigurierbare Parameter (Entwurf)

Historische Vorschlagsliste — keiner dieser Namen ist eine heutige Option
(heute z. B. `--processes`, `--concurrency`, `SWARMQ_SHUTDOWN_TIMEOUT`; der
Supervisor erlaubt 5 Neustarts pro Stunde und Slot).

| Parameter | Default | Beschreibung |
|-----------|---------|-------------|
| `num_workers` | CPU-Kerne | Anzahl Worker-Prozesse |
| `max_async_tasks` | 10 | Gleichzeitige Tasks pro Worker (Semaphore) |
| `max_prefetch` | 2 | Prefetch-Puffer (Backpressure) |
| `soft_shutdown_timeout` | 30s | Warten auf laufende Tasks |
| `hard_shutdown_timeout` | 5s | Warten nach Task-Cancel |
| `max_restarts` | 5 | Max. Neustarts pro Worker |
| `restart_window` | 60s | Zeitfenster für die Neustart-Zählung |
| `monitor_interval` | 0.5s | Health-Check-Frequenz |
| `mp_event_poll` | 0.5s | Polling-Intervall für das Shutdown-Event |

---

# 11. Supervisor als Standard, Lebenszeichen, Recycling

Seit 0.5.0 (Plan `docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md`,
R20–R23, R29–R31) läuft jeder `swarmq worker` unter dem Supervisor, auch
mit einem Prozess.

**Slots.** Worker belegen stabile Slots `0..N-1` (`process_manager/slots.py`);
ein Ersatz übernimmt den Slot seines Vorgängers und damit dessen
Metrik-/Health-Port (`PORT + slot`). Ein Slot ist frei, aktiv, im Übergang
(Alt- und Ersatzprozess gleichzeitig) oder gesperrt (Neustart-Budget
erschöpft, 5.2). Die Tabelle wächst zur Laufzeit nie; ihre Größe ist
`--processes`, die Obergrenze von `--autoscale` oder `--max-processes`.

**Lebenszeichen.** Der Worker schreibt alle `heartbeat_interval_s`
(Vorgabe 5 s, `SWARMQ_HEARTBEAT_INTERVAL_S`) aus dem Event-Loop in seinen
`SupervisorChannel` (6.2). Ohne Lebenszeichen über `heartbeat_timeout_s`
(Vorgabe 60 s; im Drain `shutdown_grace_s + SHUTDOWN_TERM_S`, beim
Recycling mindestens `heartbeat_timeout_s`) setzt der Supervisor
`expected_shutdown`, startet den Ersatz, sendet SIGTERM und nach
`SHUTDOWN_TERM_S` SIGKILL. Das zählt gegen ein eigenes Budget von
2 × Delivery-Limit je Stunde (Vorgabe 40), nicht gegen `MAX_RESTARTS`; ist
es erschöpft, wird der Slot für die Abkühlzeit gesperrt
(`hang_replacement_budget_exhausted`) — dann hält eine Giftnachricht die
Worker fest, und sie gehört nach dem Delivery-Limit in die DLQ.

**Recycling.** Der Worker zählt terminale Tasks und misst nach jedem
Taskende seinen RSS (`worker/recycling.py`; `SWARMQ_RECYCLE_MAX_TASKS`,
`SWARMQ_RECYCLE_RSS_MB`, im IO-Modus opt-in, im CPU-Modus Vorgabe 1000
Tasks / 250 MB). Überschreitet er eine Schwelle, meldet er die Absicht
(`intent`); der Supervisor setzt `expected_shutdown`, startet den Ersatz und
bestätigt (`ack`). Erst dann drainiert der Worker, ohne erzwungenen Abbruch.
Ohne Bestätigung endet er mit 75 und nimmt den normalen Neustart-Pfad; ohne
Supervisor endet er mit 0 für den äußeren Prozessmanager.

**Exit-Codes.** Exit 0 ohne `expected_shutdown` (Burst) wird nicht
nachgefüllt; Exit 78 (Konfigurationsfehler) beendet den Supervisor mit 78,
unter `--reload` wartet er stattdessen auf die nächste Dateiänderung;
Exit 75 (Recycling ohne Bestätigung) und Exit 71 (verworfene Fork-Quelle des
CPU-Pools, frischer Forkserver beim Neustart) nehmen den normalen
Neustart-Pfad (5.4).

**Waisen.** Stirbt der Supervisor hart (SIGKILL, OOM, `docker kill`),
bekommt jedes Worker-Kind per Parent-Death-Signal SIGTERM und drainiert.
Vorher liefen verwaiste Worker weiter und hielten per Heartbeat den
Scheduler-Leader-Lock fest.

---

# 12. CPU-Modus

Seit 0.5.0 (`--pool cpu`, `src/swarmq/worker/compute/`, Nutzerdoku
`website/guides/cpu-mode.md`). Der Loop-Prozess eines Workers rechnet nie;
jeder Task-Körper läuft in einem Rechenkind.

- **Forkserver** (`forkserver.py`): Thread-Limits und Malloc-Variablen
  stehen vor seinem Start in der Umgebung, der Preload importiert die
  Task-Module und rechnet nie. Hat der Forkserver nach dem Aufwärmen mehr
  als einen Thread, bricht der Start ab. Startet er später still neu und ist
  mehrfädig, verwirft der Pool die Kindquelle (`ForkSourceRejected`), der
  Worker hört auf zu konsumieren und endet mit 71.
- **Slot-Pool** (`pool.py`): N Rechenslots; Vorgabe sind die dem Worker
  zugeteilten CPUs (Affinität und cgroup-Quote, `swarmq/slots.py`) geteilt
  durch die Zahl der Worker-Prozesse. Ein Task ohne DI bekommt ein frisches
  Kind (`child_once`), ein Task mit DI das wiederverwendete Kind seines Slots
  (`child_loop`), das nach Taskzahl oder RSS recycelt wird.
- **Rückkanal:** eine `Pipe` je Kind (6.1); ein Dekodierfehler trifft nur
  das eine Kind. `hard_timeout` und Cancel töten das Kind synchron
  (SIGTERM, Gnade, SIGKILL). Jedes Rechenkind hat eine eigene
  Prozessgruppe, der Kill trifft per `killpg` auch Enkel; stirbt der
  Loop-Prozess, nimmt ein Parent-Death-Signal (SIGKILL) das Kind mit.
- **Grenzen:** nur POSIX, nicht mit `--autoscale` kombinierbar.

---

# 13. Autoskalierung und Kontrollkanal

**Autoskalierung** (`--autoscale MIN,MAX`, `process_manager/autoscale.py`):
Der Supervisor misst alle 10 s die Tiefe der konsumierten Queues (seit 0.12
nur die bereitliegenden Nachrichten, nicht die Delay-Queues), startet einen
Worker bei mehr als 50 Nachrichten je aktivem Worker und stoppt einen sanft
nach 60 s ohne Rückstau; zwischen zwei Entscheidungen liegen mindestens
30 s, ein Worker muss mindestens 30 s alt sein, bevor er herunterskaliert
wird. Seit 0.14.2 zählt ein drainender Worker nicht als aktiv und wird nie
erneut gewählt (E-084).

**Kontrollkanal** (`--max-processes N`, `--group LABEL`, Plan
`docs/plans/2026-09-16-1753-feat-kontrollkanal-plan.md`): Ein steuernder
Dienst schreibt je Gruppe einen Sollwert `{"min": …, "max": …}` nach
`[<namespace>:]sq:control:setpoint:<gruppe>`. Der Supervisor liest ihn vor
dem ersten Kind und dann alle 5 s, prüft ihn gegen den erklärten Spielraum
`N`, wendet ihn an (fehlende Worker sofort starten, überzählige sanft
drainieren) und meldet seine wirksamen Werte unter
`[<namespace>:]sq:control:status:<gruppe>:<supervisor>`. Ein unlesbarer oder
ungültiger Sollwert wird abgelehnt; die bisherigen Grenzen bleiben. Ohne
`--max-processes` liest und schreibt der Supervisor keine Sollwerte.

Einzelheiten: `website/reference/cli.md` (Abschnitte „Autoscaling and
draining workers“ und „Control channel“).

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
