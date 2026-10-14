# Fix-Recherche CPU-Modus und Recycling: FS-16, FS-30, FS-23, FS-24

Stand: Worktree `swarmq-fehlersuche` (906afac, v0.12.0 + Nachweise), Register `swarmq-entscheidungen` (45c6336).
Nur gelesen. Ausgeführt wurden die Unit-Tests `test_befund_fs16_kill_group.py`, `test_befund_fs30_recycling_drain.py`, `compute/test_pool.py` und `test_recycling.py` (50 passed, 3 xfailed) sowie die Proben `mb/handshake_orphan.py` und `mb/blocked_index.py`; beide geben die Ausgabe aus dem Quellbericht wieder.
Python: CPython 3.14.4 (`~/.local/share/uv/python/cpython-3.14.4-linux-x86_64-gnu/lib/python3.14/asyncio`).

Mischbetrieb vorab: Alle vier Befunde betreffen nur prozesslokalen Zustand: `SlotPool` im Loop-Prozess, Drain-Ablauf des Workers, Supervisor-Kanal mit gleicher Version auf demselben Knoten. Sie berühren keine Lua-Skripte, Header, Marker, Valkey-Keys und kein Nachrichtenformat. Ein 0.12.1-Worker kann deshalb in beiden Richtungen neben 0.12.0-Workern, -Producern und -Leadern laufen. Unten steht das nur dort noch einmal, wo es eine Besonderheit gibt.

---

## FS-16 (P2): `_kill_child` signalisiert der Prozessgruppe nichts, wenn das Kind schon tot ist

### 1. Ursache

`src/swarmq/worker/compute/pool.py`:
- `:584-585`: `if child.pid is None or not child.process.is_alive(): return`. Ist das Kind schon tot (Fall b: OOM, Segfault, `os._exit`), geht kein Signal an die Gruppe.
- `:587-589`: SIGTERM an die Gruppe. Endet das Kind innerhalb von `kill_grace_s`, kehrt `_wait_dead` mit `True` zurück, und es folgt `return`. Das SIGKILL an die Gruppe (`:590`) kommt nie (Fall a).
- `:491-508` `_finalize`: Beim Tod ohne Ergebnis (`pending` bekommt `ChildDied`, `:502-504`) wird der Gruppe gar nichts geschickt. Das ist der R9-Fall.
- `:609-617` `_signal`: `killpg` nur bei `pgid_ready` (gesetzt beim Ready, `:434`) und ohne `by_pid_only`.

Nachweis: `tests/unit/worker/compute/test_befund_fs16_kill_group.py`. Fall a: Das Kind endet sauber auf SIGTERM, der Enkel ignoriert SIGTERM. Fall b: `os._exit(1)` nach dem Enkelstart. Beide sind xfail strict.

### 2. Optionen

**A: Abschließendes Gruppen-SIGKILL („Sweep“) nach dem Kindtod, sofort.** (empfohlen)
- Neues Feld `_Child.group_killed: bool`. `_signal` setzt es, sobald `killpg(..., SIGKILL)` gesendet wurde.
- `_kill_child`: Ist das Kind tot und gilt `pgid_ready and not by_pid_only and not group_killed`, dann `killpg(pid, SIGKILL)`. `ProcessLookupError` wird ignoriert. Das gilt für den Frühausstieg `:584` und für `:588`.
- `_finalize`: derselbe Sweep, wenn das Kind ohne Ergebnis gestorben ist (`pending` nicht erledigt) oder wenn ein warmes DI-Kind ohne Ende-Signal gestorben ist (`not child.ending`, `pending is None`, `slot.reuse_child is child`). Das deckt R9 ab („Der Loop-Prozess beendet verbliebene Nachkommen des toten Kindes“).
- Folgefehler-Analyse:
  - *Aufrufer*: `_kill_child` wird aufgerufen aus `aclose` (`:189`), `run`/`_kill_child_uninterruptible` (`:251`, `:288`), `_start_child` (`:380`, `:403` mit `by_pid_only=True`, also ohne Sweep; das Kind hat vor dem Ready keinen Taskcode ausgeführt), `:407` und `_end_child` (`:580`, „forced“). `_finalize` wird aufgerufen aus EOF (`:477`), Sentinel-Gnade (`:489`) und `aclose` (`:203`). Mit dem Flag `group_killed` bekommt kein Pfad ein zweites SIGKILL, wenn die Gruppe schon eines bekam.
  - *Entscheidungen*:
    - E-036 (`test_pool.py::test_kill_slot_is_two_stage_and_waits_for_death`, markiert) erwartet genau `[SIGTERM, SIGKILL]`, alle per `killpg`. Das bleibt so: Das SIGKILL der zweiten Stufe setzt `group_killed`, und der spätere `_finalize`-Sweep (pending wird `ChildDied`) entfällt. Ohne das Flag bräche dieser Entscheidungstest.
    - E-035 (`test_immortal_child_blocks_the_slot`) ist nicht berührt, weil der Sperrpfad nur bei lebendem Kind erreicht wird.
    - E-037: Der Kindtod bleibt Retry-Pfad, das Ergebnis ändert sich nicht.
  - *Nicht markierter Test, der sich ändert*: `test_pool.py::test_cooperative_child_needs_only_sigterm` (`:494-504`) erwartet `[SIGTERM]`. Danach gilt `[SIGTERM, SIGKILL]`, wobei das SIGKILL der Sweep nach dem Tod ist. Die Anpassung ist zulässig, weil der Test keine Entscheidung festschreibt. Neue Aussage: Kooperatives Kind, also keine Wartezeit `kill_grace_s`. Das SIGKILL geht erst nach dem Tod und nur an die Gruppe (Laufzeit < `kill_grace_s`). Den Test umbenennen.
  - Ebenso bleibt `test_cancelled_run_kills_the_child_before_the_slot_is_freed` (`:648`) unverändert: Das stubborn-Kind bekommt das SIGKILL der zweiten Stufe.
  - *Bibliothek/OS*:
    - Mit Forkserver-Kontext ist das Rechenkind ein Kind des Forkservers, nicht des Loop-Prozesses. Der Forkserver reapt es, die PID-Nummer wird danach frei. Linux gibt eine PID-Nummer aber erst frei, wenn kein Task sie mehr als PID, PGID oder SID referenziert. Solange ein Enkel in der Gruppe lebt, bleibt die PGID also reserviert, und `killpg` trifft genau die alte Gruppe.
    - Ist die Gruppe leer, liefert `killpg` `ESRCH`. Ein Fehltreffer setzt voraus, dass die Gruppe leer ist, die PID-Nummer umgelaufen ist (`pid_max`, auf 64-bit-systemd typisch 4 194 304) und der neue Träger selbst `setpgid(0,0)` gemacht hat. Das Fenster liegt zwischen Todeserkennung und Sweep, also im Millisekundenbereich: vernachlässigbar.
  - *Grenzen*:
    - Ein Enkel, der sich per `setsid`/`start_new_session=True` aus der Gruppe löst, bleibt unerreichbar. Das ist ein bewusster Ausstieg der App und keine Zusage von E-036.
    - In Containern ohne reapenden PID 1 bleiben getötete Enkel als Zombies stehen. `os.kill(pid, 0)` im e2e-Test `test_kill_takes_the_grandchild_with_it` zählt sie als „lebend“. Das gilt schon heute, der Sweep ändert daran nichts.
  - *Neue Fehlermodi*:
    - Der Sweep ist synchron und ohne `await`, also ohne Cancel-Fenster.
    - Enkel mit eigenem, langsamem SIGTERM-Aufräumen werden jetzt unmittelbar nach dem Kindtod hart beendet, statt (fälschlich) weiterzulaufen. KTD3 gewährt die 1-s-Gnade dem Kind, nicht den Enkeln, das ist also vertretbar.
    - Ein Task, der absichtlich einen langlebigen Hilfsprozess in der eigenen Gruppe zurücklässt, verliert ihn bei Kill bzw. Kindtod. Genau das sagt E-036 zu.

**B: Sweep verzögert um den Rest von `kill_grace_s` (`loop.call_later`).**
- Die Enkel bekommen dieselbe Gnade wie das Kind, und `run` kehrt trotzdem sofort zurück.
- Nachteile:
  - Das Fenster für PID-Wiederverwendung wird größer, bis 1 s.
  - Der Hintergrund-Timer muss bei `aclose` abgewartet oder gestrichen werden.
  - Der Nachweis (0,1 s Fenster) müsste länger warten.
- Mehr Komplexität ohne erkennbaren Nutzen.

**C: `PR_SET_CHILD_SUBREAPER` im Forkserver oder Loop-Prozess.**
- Verwaiste Enkel würden an einen eigenen Subreaper umgehängt und könnten verfolgt und gereapt werden. Das erfasst auch `setsid`-Ausreißer und Zombies.
- Nur unter Linux, und der Forkserver müsste `SIGCHLD`/`waitpid(-1)` für fremde Kinder behandeln, was sich mit dem Exit-Reporting von `multiprocessing.forkserver` verträgt nur mit Aufwand. Aufwand L, für diesen Befund überdimensioniert.

### 3. Empfehlung

Option A.
- Sweep in `_kill_child` nach dem Tod und in `_finalize` beim Tod ohne Ergebnis bzw. ohne Ende-Signal.
- Flag `group_killed` gegen ein doppeltes SIGKILL, damit der E-036-Test unverändert grün bleibt.
- Kein Sweep nach regulärem Ende: `child_once` nach geliefertem Ergebnis, Recycling-Ende mit `ending=True`. Das ist nicht entschieden (siehe Liste unten).

Tests:
- Nachweise `test_befund_fs16_kill_group.py` (2 Tests): Marker entfernen.
- Anpassen: `test_cooperative_child_needs_only_sigterm`, nicht markiert.
- Neu (FakeCtx, < 100 ms):
  - Kindtod ohne Ergebnis mit `pgid_ready`: Genau ein `("killpg", pid, SIGKILL)` nach `ChildDied`.
  - Handshake-Fehler ohne `pgid_ready`: kein `killpg`.
  - Nach dem SIGKILL der zweiten Stufe kein weiteres SIGKILL aus `_finalize` (Regressionsschutz für den E-036-Test).
  - Idle-Tod des warmen DI-Kinds: Sweep.
  - Recycling-Ende mit `ending`: kein Sweep.

Dateien: `src/swarmq/worker/compute/pool.py`, `tests/unit/worker/compute/test_pool.py`, Nachweisdatei. Register: In E-036 R9 als Quelle für Fall b bestätigen (Gegenprüfung C), keine neue Entscheidung nötig.

### 4. Aufwand/Risiko

S / niedrig.

---

## FS-30 (P2): Recycling-Drain ohne `hard_timeout` fällt auf `shutdown_timeout` zurück

### 1. Ursache

- `src/swarmq/worker/recycling.py:77-86` `recycle_drain_timeout`: Gibt es keinen registrierten Task mit `hard_timeout`, liefert die Funktion `shutdown_timeout`. Sonst `max(hard_timeout) + 30`, wobei Tasks ohne `hard_timeout` stillschweigend ignoriert werden. Damit ist auch eine gemischte Registry betroffen.
- `:113-114`: Die Frist wird als `_drain_timeout_override` gesetzt, dann folgt `_request_shutdown()`.
- `src/swarmq/worker/shutdown.py:103-118`: `asyncio.wait(..., timeout=override)` und danach `task.cancel()`. Nach E-001 wird daraus ein Fehlerergebnis mit Retry-Verbrauch, bei `max_retries=0` DLQ und Workflow FAILED.
- Nachweis `tests/unit/worker/test_befund_fs30_recycling_drain.py` (xfail strict). Der Rückfall ist zusätzlich in `tests/unit/worker/test_recycling.py:84` festgeschrieben (`_drain_timeout_override == shutdown_timeout`). Dieser Test ist nicht markiert, und E-038 sagt ausdrücklich: „Den Test bei einem Fix anpassen.“

### Antwort auf die Kernfrage: Wie lange darf ein Recycling-Drain ohne `hard_timeout` dauern?

**Was den Drain heute tatsächlich begrenzt:**
- *Supervisor-Lebenszeichen (KTD10)* begrenzt nichts.
  - `restart_budget.py:54-58` setzt im Drain die Frist `shutdown_grace_s + SHUTDOWN_TERM_S` (≈ 35 s). Das ist aber eine **Staleness**-Frist („seit dem letzten Beat“), keine Gesamtdauer.
  - Der Worker schreibt den Beat auch im Drain weiter (`process_manager/entry.py:166-176`, „auch waehrend des Drains“). Ein gesunder Recycling-Drain kann deshalb beliebig lange laufen, ohne dass der Supervisor eingreift.
  - Nur ein blockierter Event-Loop führt zu `worker_hung_during_drain`, dann SIGTERM und SIGKILL (`:85-94`). Das gilt schon heute und ändert sich durch den Fix nicht.
  - Die in E-018 offene Notiz (`restart_budget.py:57` rechnet „Drain-Zeit“ statt Gesamtbudget) betrifft nur die Höhe dieser Staleness-Frist, nicht die Drainlänge.
- *Setpoint/Status-TTL* begrenzt ebenfalls nichts.
  - `setpoint.status_ttl_s` (`setpoint.py:205-210`) ist die TTL der Rückmeldung des **Supervisors** in seinem eigenen Abbau. Mit dem Recycling-Drain eines Workers hat sie nichts zu tun.
  - Nebenwirkung eines langen Drains: Der Slot hat zwei Halter (alter und Ersatz) und steht auf `SlotState.TRANSITION`. `_drain_one` (`setpoint.py:158-167`) und der Autoskalierer (`autoscale.py:110`) überspringen ihn. Diesen Slot kann man für die Drain-Dauer nicht herunterskalieren.
  - Zudem hält der alte Prozess seinen (wegen RSS-Schwelle hohen) Speicher, bis der Drain endet.
  - Beides gilt mit `hard_timeout` schon heute (`max + 30 s`), wird aber länger.
- *RabbitMQ-Consumer-Timeout* ist die einzige natürliche Grenze.
  - Die Konfiguration nennt sie `consumer_timeout_s` (Default 1800, `config.py:351`). swarmq setzt den Wert nicht am Broker, er spiegelt nur die Broker-Einstellung. Genutzt wird er für Startwarnungen (`wiring.py:165-178`, `prefetch.py:33-55`) und den Ausführungsschutz (`execution_guard.py:232-247`).
  - Jede Zustellung, die vor dem Recycling-Start ankam, also jede, die der Drain abwartet (nach `cancel_consume` kommen keine neuen), hat ihr Consumer-Timeout spätestens `consumer_timeout_s` nach dem Recycling-Start erreicht. Danach hat der Broker sie zurückgenommen, und ein Ack ist nicht mehr möglich. Länger zu warten bringt nichts.

**Optionen:**

**A: Frist = `max(größter hard_timeout + Puffer, consumer_timeout_s)`, sobald ein vom Worker konsumierter Task keinen `hard_timeout` hat; sonst wie heute `max(hard_timeout) + 30`.** (empfohlen)
- Verfeinerung: nur Tasks auf `config.consumed_queues` zählen, wie bei `warn_prefetch_over_consumer_timeout`. Ein registrierter, aber hier nicht konsumierter Task ohne `hard_timeout` soll die Frist nicht hochtreiben, ein hier nicht konsumierter mit großem `hard_timeout` ebenso wenig.
- Ein hängender Task ohne `hard_timeout` hält den Prozess höchstens `consumer_timeout_s` (30 min).
- Bei Fristablauf: Abbruch nach E-001. Zu diesem Zeitpunkt hat der Broker die Zustellung bereits zurückgenommen. Der Retry-Verbrauch trifft also nur Tasks, die das Consumer-Timeout ohnehin gerissen haben. Das berührt die Formulierung von E-038 („verliert keinen Retry“), siehe Liste unten.

**B: Unbegrenzt (`timeout=None`), sobald ein Task ohne `hard_timeout` konsumiert wird.**
- Erfüllt E-038 wörtlich.
- Ein hängender Task (Deadlock, ewiges Await) hält den alten Prozess samt Speicherleck aber unbegrenzt am Leben. Nur ein SIGTERM beendet ihn, und der Slot steht dauerhaft auf `TRANSITION`.
- Recycling soll gerade Speicher zurückholen. Davon rate ich ab.

**C: Frist je laufendem Handler**: Startzeit + `hard_timeout`, sonst Zustellzeit + `consumer_timeout_s`. Gewartet wird bis zur spätesten.
- Am genauesten. Braucht aber eine Zuordnung `_active_tasks` → Task-Info und Zustellzeit, die es heute nicht gibt.
- Aufwand M+, Mehrwert gegenüber A gering.

### Zwingende Begleitänderungen (unabhängig von A/B/C)

1. **SIGTERM während eines laufenden Recycling-Drains** (Nebenbefund aus MB-2).
   - `_request_shutdown` (`shutdown.py:370-378`) ist außerhalb von `RUNNING` wirkungslos. Ein SIGTERM ändert deshalb an der langen Override-Frist nichts.
   - Unter dem Supervisor setzt dessen Abbau zuerst `shutdown_event`. Der Watcher in `entry.py:177-179` schickt dem Worker dann SIGTERM, das wirkungslos bleibt. Danach eskaliert der Supervisor mit SIGTERM und SIGKILL bei `sigkill_at` (`supervisor.py:413-436`).
   - Folge: SIGKILL **ohne** Lock-Freigabe, Leader-Übergabe und Guard-Abschluss. Locks laufen per TTL ab. Die Running-Marker des Ausführungsschutzes verwaisen und zählen gegen `execution_guard_max_orphans` (3, dann DLQ). Laufende Tasks kommen als Redelivery zurück.
   - Mit FS-30 wird der Drain bis zu 30 min lang. Damit wird es sehr viel wahrscheinlicher, dass ein Deploy einen recycelnden Worker trifft.
   - Fix: Ein SIGTERM im Recycling-Drain schaltet auf das Shutdown-Budget ab diesem SIGTERM um, also `drain_until`, `finish_by` und die Publish-Endfrist ab dem Signal. Ab dort gilt E-001. E-038 erlaubt das ausdrücklich: „der erzwungene Abbruch bleibt SIGTERM vorbehalten (dort gilt E-001)“.
   - Umsetzung:
     - `_drain_in_flight` wartet zusätzlich auf ein `asyncio.Event` („recycle interrupted“).
     - Den Auslöser auf den SIGTERM-Pfad legen: entweder einen eigenen Handler statt `self._request_shutdown` in `lifecycle.py:441-442`, oder `_request_shutdown` setzt das Event nur bei gesetztem Override. `burst_monitor` (`lifecycle.py:494`) und `compute/wiring.py:156` laufen im Recycling nicht (Recycling nur im IO-Modus, Burst endet mit gesetztem Event), deshalb ist auch Variante 2 sicher.
   - Neuer Fehlermodus: Ein SIGTERM nach dem Drain, aber während des Aufräumens, setzt das Event wirkungslos. Das ist unkritisch.
2. **Publish-Endfrist und Aufräum-Budget im Recycling (Wechselwirkung FS-02 Teil b).**
   - `_execute_shutdown` setzt beim Recycling-Start `_shutdown_end = started_at + finish_by` und die Publish-Endfrist `started_at + drain_until` (`shutdown.py:269-271`). Unter dem Supervisor sind das bei 30 s Budget etwa 23 s.
   - Läuft ein Langläufer nach FS-30 durch, statt nach 30 s abgebrochen zu werden, scheitern alle seine Publishes ab Sekunde 23: Enqueue aus dem Task, Workflow-Nachfolger, ein echter Retry. Zusätzlich gehen sie wegen FS-02 Teil a mit 0-s-Deckel trotzdem auf die Leitung (`connection.py:460-471`, `delivery.py:192-213` → `PublishIndeterminate`).
   - **FS-30 allein verschlimmert damit FS-02b**: Statt ein paar Sekunden ist jetzt der ganze Rest des Drains betroffen.
   - Außerdem bekommt jeder Aufräumschritt nach einem langen Drain nur noch das Minimum `_SHUTDOWN_STEP_MIN_S = 1 s` (`shutdown.py:211-217`, `lifecycle.py:68`).
   - Fix (gehört zu FS-02b, muss aber spätestens mit FS-30 kommen):
     - Im Recycling während des Drains keine Publish-Endfrist (`None`).
     - Nach dem Drain-Ende bzw. bei SIGTERM `_shutdown_end` und die Endfrist neu ab diesem Zeitpunkt aus `shutdown_budget()` setzen.
   - Der E-018-Test `test_shutdown_budget_steps.py::test_recycling_drain_is_exempt_from_the_budget` bleibt dabei grün. Er setzt den Override als Zahl und prüft nur, dass nicht abgebrochen wird.

### 2. Folgefehler-Analyse (für A)

- *Aufrufer*: `recycle_drain_timeout` wird nur in `request_recycle` (`recycling.py:113`) verwendet, `_drain_timeout_override` nur in `_drain_in_flight` (`shutdown.py:103`) und in `start_recycle_monitor` (Init, `:40`). Achtung: `if not drain_timeout` (`shutdown.py:104`) behandelt `0.0` wie „kein Override“. Bei Option B dürfte „unbegrenzt“ deshalb weder `None` noch `0` sein, sondern bräuchte ein eigenes Flag. Option A liefert immer eine positive Zahl.
- *Entscheidungen*:
  - E-038 wird erfüllt (heute verletzt). Der konkrete Ersatzwert `consumer_timeout_s` ist eine Präzisierung, die im Register-Text nachgetragen werden sollte.
  - E-018: Ihr Test bleibt grün, und die Ausnahme „Recycling-Drain außerhalb des Budgets“ bleibt erhalten. Neu ist nur, dass ein SIGTERM das Budget wieder in Kraft setzt. Das ist mit dem Text („Gesamtbudget ab SIGTERM“) vereinbar und schließt eine Lücke.
  - E-001 bleibt unverändert: Der Abbruch bei Fristablauf oder SIGTERM ist weiter ein Fehlerergebnis. Der markierte Test `compute/test_executor.py::test_outer_cancel_during_drain_kills_child_and_returns_error_result` ist nicht berührt.
  - E-037 ist nicht berührt.
- *Bibliothek*: `asyncio.wait(timeout=…)` mit großen Werten ist unkritisch. Die Option „Event oder Tasks“ lässt sich mit `asyncio.wait({*tasks, event_task}, return_when=FIRST_COMPLETED)` in einer Schleife umsetzen. Der Event-Task muss im `finally` gestrichen werden, sonst droht ein Task-Leck (U3: Warnungen als Fehler).
- *Mischbetrieb*: Nichts geteilt. Supervisor und Worker haben auf einem Knoten dieselbe Version, und der Kanal (`supervision.py`) bleibt unverändert.
- *Wechselwirkungen*:
  - FS-02 (a und b), siehe oben: FS-02 muss vor oder mit FS-30 kommen.
  - FS-01 bzw. die E-001-Produktfrage: Fällt die Entscheidung später auf „Drain-Abbruch = Neuzustellung“, ändert sich nur, was bei Fristablauf oder SIGTERM passiert. Die Fristberechnung bleibt.
- *Neue Fehlermodi*:
  - Längere Doppelbelegung des Slots: zwei Prozesse und RSS, Slot im Zustand `TRANSITION`.
  - Ist `consumer_timeout_s` falsch konfiguriert (Broker ohne Consumer-Timeout), bricht der Drain einen über 30 min laufenden Task trotzdem nach 30 min ab. Das muss dokumentiert werden.

### 3. Empfehlung

Option A, zusammen mit SIGTERM-Umschaltung und Budget-Neuansatz, im selben MR wie oder nach FS-02b.

Tests:
- Nachweis FS-30: Marker entfernen.
- Anpassen: `test_recycling.py::test_task_count_threshold_recycles_with_ack` (`:84`), nicht markiert, laut E-038 vorgesehen. Neue Erwartung: `consumer_timeout_s` bei Registry ohne `hard_timeout`.
- Neu (Unit, < 100 ms, Events statt Sleeps):
  1. Gemischte Registry (Task mit `hard_timeout=10` plus Task ohne): Frist ≥ `consumer_timeout_s`.
  2. Task ohne `hard_timeout` auf einer nicht konsumierten Queue verlängert die Frist nicht.
  3. SIGTERM während des Recycling-Drains: Abbruch spätestens bei `drain_until` ab SIGTERM. Der Fehlerpfad nach E-001 läuft, und Locks sind vor `finish_by` frei. Aufbau wie `test_befund_bz18_cleanup_after_grace.py`.
  4. Nach dem Recycling-Drain bekommt jeder Aufräumschritt das Budget ab Drain-Ende, nicht das 1-s-Minimum.
  5. Im Recycling-Drain ist bis zum SIGTERM keine Publish-Endfrist gesetzt (Überschneidung mit dem FS-02b-Test, nur einmal schreiben).

Doku: `website/explanation/delivery-semantics.md:260-261` (und `.de`) um „ohne `hard_timeout`: `SWARMQ_CONSUMER_TIMEOUT_S`; SIGTERM beendet den Recycling-Drain im Shutdown-Budget“ ergänzen. Docstring in `recycling.py:78`.

Dateien: `src/swarmq/worker/recycling.py`, `src/swarmq/worker/shutdown.py`, ggf. `src/swarmq/worker/lifecycle.py` (Signal-Handler), Tests in `tests/unit/worker/`, Doku.

### 4. Aufwand/Risiko

M / mittel. Der Eingriff in die Abbausequenz läuft neben FS-02 im selben Code, und das Zusammenspiel mit dem Supervisor-Abbau muss stimmen.

---

## FS-23 (P3): Im Hintergrund gesperrter Slot wird weiter vergeben

### 1. Ursache

- `pool.py:593-596`: `_kill_child` setzt `slot.blocked`, wenn das Kind SIGKILL überlebt.
- `pool.py:255-258`: Nur `run()` behält den Index bei `blocked` ein, und zwar nur, wenn die Sperre entsteht, während `run` den Index hält.
- Hintergrund-Kills nach der Rückgabe des Index: `_recycle` → `_end_child` → `_kill_child(reason="forced")` (`:568`, `:580`) sowie `aclose` (`:191`, harmlos wegen `_closing`).
- `run()` prüft nach `_free.get()` (`:218-222`) nur die Poison-Marke `-1`, nicht `blocked`.
- Probe `mb/blocked_index.py`: `free queue: [0, 1]` bei `slot0.blocked: True`.
- Ein Nachweis-Test existiert noch nicht (P3).

### 2. Optionen

**A: Prüfung beim Bezug (lazy).** (empfohlen)
- In `run()` nach `_free.get()`: Ist `self._slots[index].blocked`, wird der Index verworfen (nicht zurückgelegt). Dann erneut `_usable_slots() == 0` prüfen (→ `SlotUnavailable`) und weiter `get()`.
- Ein einziger Engpass deckt alle heutigen und künftigen Hintergrund-Sperrpfade ab.
- Die Poison-Logik (`:604-607`) bleibt unverändert: Sind alle Slots gesperrt, liegt `-1` in der Queue, und der Bezug endet in `SlotUnavailable`.
- Kein Busy-Loop, weil jeder gesperrte Index höchstens einmal verworfen wird.

**B: Eager-Entfernung in `_kill_child`.**
- Beim Sperren `_free` synchron leeren und alle anderen Indizes in derselben Reihenfolge zurücklegen, ohne `await` und damit ohne Race.
- Hält `qsize()` ehrlich, deckt aber nur diesen einen Sperrpfad ab.
- Bei einem `run`, der den Index gerade hält, ist der Index nicht in der Queue, und der bestehende Pfad `:257` greift.

A und B lassen sich kombinieren. B allein reicht, A ist robuster.

- Folgefehler-Analyse:
  - *Aufrufer*: `_free.get` und `put` nur in `start`, `run`, `_kill_child` (siehe grep).
  - *Entscheidungen*:
    - E-035 (`test_immortal_child_blocks_the_slot`, markiert) prüft `all(i < 0 for i in _free(p)._queue)` nach einer Sperre im Vordergrund. Beide Optionen erhalten das.
    - Die Dauersperre selbst bleibt (E-035: „Dass eine Sperre nie aufgehoben wird, ist kein Fehler“). Der Nachweis darf nur „Index nicht mehr frei“ prüfen.
    - E-024 betrifft Supervisor-Slots, nicht diesen Pool.
  - *Wechselwirkung*:
    - FS-16 ändert dieselbe Funktion `_kill_child`, aber im Pfad „Kind tot“. Der Sperrpfad wird nur bei lebendem Kind erreicht. Kein Konflikt, im selben MR in der Reihenfolge FS-16 → FS-23.
    - Mutant `_kill_child__mutmut_48` (`and` → `or`, Poison bei *einem* gesperrten Slot) wird durch den Zusatztest unten abgesichert.
  - *Neue Fehlermodi*: Keine erkennbaren. Ein `run`, der in `_free.get()` wartet, während der Slot im Hintergrund gesperrt wird, bekommt bei A den Index, verwirft ihn und wartet weiter. Das ist korrekt.
  - *Restthema, nicht Teil dieses Fixes*: Die Concurrency von Consumer und Semaphor sinkt mit gesperrten Slots nicht. Handler warten in `_free.get()` innerhalb des `hard_timeout`-Watchdogs (`executor.py:171-176`) und können mit „hard timeout“ scheitern, ohne gerechnet zu haben. Ob die Slot-Wartezeit gegen `hard_timeout` zählen soll, ist nirgends entschieden (siehe Liste).

### 3. Empfehlung

Option A, optional zusätzlich B für ehrliches `qsize()`.

Tests, alle neu mit FakeCtx und < 100 ms:
1. Nachweis FS-23: Pool mit 2 Slots, DI-Kind auf Slot 0 mit `recycle_max_tasks=1`, altes Kind `immortal`, kleines `end_grace_s`. Nach dem Hintergrund-Recycling (`_background` leer) landen zwei folgende `run()` nie auf Slot 0, bzw. `0` liegt nicht in `_free`.
2. Pool mit 2 Slots, einer gesperrt: `run()` auf dem anderen liefert ein Ergebnis und kein `SlotUnavailable` (sichert mutmut_48 ab).
3. Beide Slots im Hintergrund gesperrt: `run()` wirft `SlotUnavailable`, `healthy` ist `False`.

Datei: `src/swarmq/worker/compute/pool.py`, `tests/unit/worker/compute/test_pool.py` bzw. eine Nachweisdatei `test_befund_fs23_*.py`.

### 4. Aufwand/Risiko

S / niedrig.

---

## FS-24 (P3): Gescheiterter Handshake: asyncio-ERROR „exception in shielded future“ und doppelt gezählter Kindtod

### 1. Ursache

- `pool.py:399`: `await asyncio.wait_for(asyncio.shield(child.ready), timeout=handshake_timeout_s)`.
- Bei Timeout oder Cancel wird nur das äußere Shield-Future abgebrochen. CPython 3.14 `asyncio/tasks.py:991-996` (`_outer_done_callback`) hängt dann `_log_on_exception` an das innere `child.ready`.
- `_kill_child` (`:403` bzw. `:407`) tötet das Kind, und das EOF ruft `_finalize` auf (`:500-501`): `child.ready.set_exception(ChildDied(...))`.
- `_log_on_exception` (`tasks.py:911-928`) meldet daraufhin über den Loop-Exception-Handler „ChildDied exception in shielded future“ (Logger `asyncio`, ERROR). Das passiert je Nachricht im Störfall „Forkserver tot oder überlastet“.
- Zweiter Teil:
  - Beim frischen Kind (`child_once`) ist `child.pending` das `fut` aus `run()` (`:224`, `:234`, `:355`). Nach dem Handshake-Fehler erwartet es niemand mehr.
  - `_finalize` (`:502-504`) zählt `stats.died += 1` und setzt `ChildDied` darauf, zusätzlich zu `_note_unavailable` (`:402`).
  - Probe: `killed=1, died=1, unavailable=1`, Loop-Fehler `['ChildDied exception in shielded future']`. Ein „Future exception was never retrieved“ für das verwaiste `fut` trat in der Probe auch nach mehrfachem `gc.collect()` nicht auf, ist aber latent möglich.
- Gleicher Mechanismus beim Abbruch im Handshake (Drain, Watchdog) und beim Ersatzkind im Recycling (`_spawn_reuse`, dort ohne `pending`, nur die Log-Meldung).
- Der bestehende Test `test_pool.py::test_handshake_timeout_is_slot_unavailable_not_child_death` (`:466-475`) prüft `died == 0` **vor** dem asynchronen `_finalize` und besteht deshalb nur wegen eines Rennens.
- Ein Nachweis-Test existiert nicht.

### 2. Optionen

**A: Handshake explizit aufgeben.** (empfohlen)
- Hilfsfunktion `_abandon_handshake(child)` in beiden `except`-Zweigen von `_start_child` (`:401-408`), **vor** dem Kill:
  - `if child.ready is not None and not child.ready.done(): child.ready.cancel()`
  - `child.pending = None`
- Wirkung:
  - `_log_on_exception` kehrt bei einem abgebrochenen Future sofort zurück (`tasks.py:912-913`).
  - `_finalize` überspringt beide `set_exception` (`:500`, `:502`). Damit kein `died`, kein verwaistes Future und kein FS-16-Sweep, der bei fehlendem `pgid_ready` ohnehin nicht greift.
- Der Shield bleibt, also keine Verhaltensänderung für den Erfolgsfall.

**B: Shield entfernen.**
- `await asyncio.wait_for(child.ready, …)`: `wait_for` (3.14: `async with timeouts.timeout(...)`, `tasks.py:486-487`) bricht beim Timeout bzw. Cancel das innere Future selbst ab.
- Zusätzlich `child.pending = None` im Fehlerpfad.
- Funktional gleichwertig. Der Shield stammt unkommentiert aus U5 (`ec52c74`), kein anderer Code wartet auf `ready` (grep). Eine Absicht ist also nicht erkennbar, aber auch nicht ausgeschlossen. A ist expliziter.

**C: Nur die Ausnahme abholen.** Done-Callback, der `ready.exception()` liest. Das behebt die Log-Meldung, nicht aber den doppelten `died`-Zähler.

- Folgefehler-Analyse:
  - *Aufrufer*: `_start_child` aus `_spawn_fresh` (`run`) und `_spawn_reuse` (`_ensure_reuse_child`, `_recycle`).
    - In `run` ist `child` beim Fehler in `_spawn_fresh` noch `None`. Damit kein zweiter Kill (`:250`) und kein Warten auf `fut`. Es bleibt ungenutzt und ohne Ausnahme, also kein never-retrieved.
    - In `_recycle` fängt `except SlotUnavailable` (`:552`) den Fehler.
  - *Späte Ready-Nachricht*: Kommt `("ready", …)` nach dem Aufgeben, aber vor dem Tod, prüft `_on_message` `not child.ready.done()` (`:436`) und setzt nichts. Es setzt aber vorher `pgid_ready = True` und `consecutive_unavailable = 0` (`:434-435`). Das gilt schon heute.
    - Folge: Ein spätes Ready eines eben getöteten Kindes setzt den Zähler für „Pool ungesund“ zurück.
    - Kleine, schon bestehende Unschärfe, mit A/B unverändert. Optional mit „ignorieren, wenn `ready` schon erledigt ist“ abfangen. Das ist eigenes Verhalten und braucht einen eigenen Test.
  - `pgid_ready = True` nach dem Aufgeben kann bewirken, dass ein laufender `_kill_child(by_pid_only=True)` korrekt nur die PID trifft. `by_pid_only` hat Vorrang (`:612`), der FS-16-Sweep muss `by_pid_only` ebenfalls respektieren.
  - *Entscheidungen*: Keine betroffen, Entscheidungsabgleich der Gegenprüfung B: „keine Entscheidung gefunden“. Maßstab ist die Linie aus Changelog rc2 (`changelog.de.md:189-194`): unabgeholte Future-Ausnahmen gelten als Fehler.
  - *Wechselwirkung*: FS-16 (Sweep in `_finalize`) und FS-24 berühren dieselbe Stelle. Mit `pending = None` vor dem Kill entsteht kein Sweep für Handshake-Kinder, was ohne `pgid_ready` auch richtig ist. Reihenfolge im MR: FS-24 vor FS-16, damit der FS-16-Test „Handshake ohne Sweep“ schon auf dem bereinigten Pfad läuft.
  - *Neue Fehlermodi*: Keine. `cancel()` auf ein noch offenes Future ist synchron und idempotent.

### 3. Empfehlung

Option A.

Tests (FakeCtx, < 100 ms):
1. Nachweis FS-24: Handshake-Timeout mit `handshake_timeout_s=0.05`, Loop-Exception-Handler per `loop.set_exception_handler` erfassen und im `finally` zurücksetzen. Auf `_finalize` warten (`_settle(lambda: not p._children)`). Erwartung: keine Loop-Fehler, `stats.died == 0`, `stats.unavailable == 1`.
2. Dasselbe für einen Cancel im Handshake (zu `test_cancel_during_handshake_kills_the_started_child`).
3. Dasselbe für ein gescheitertes Recycling-Ersatzkind (`_spawn_reuse`).
4. `test_handshake_timeout_is_slot_unavailable_not_child_death` härten: die `died`-Prüfung erst nach `_finalize` (nicht markiert, zulässig).

Datei: `src/swarmq/worker/compute/pool.py`, `tests/unit/worker/compute/test_pool.py` bzw. `test_befund_fs24_*.py`.

### 4. Aufwand/Risiko

S / niedrig.

---

## Reihenfolge und Bündelung

- **Pool-MR** (`pool.py`): FS-24 → FS-16 → FS-23. Ein Modul, drei unabhängige rote Tests. Das Flag `group_killed` (FS-16) und `_abandon_handshake` (FS-24) greifen in `_finalize` sauber ineinander.
- **Recycling-MR**: FS-30 nach oder zusammen mit FS-02 (Teil a: verstrichene Endfrist sendet nicht; Teil b: Endfrist im Recycling erst ab Drain-Ende bzw. SIGTERM). Die SIGTERM-Umschaltung gehört in diesen MR. Ohne FS-02b verschlimmert FS-30 dessen Folgen.

## Befunde, deren Fix eine Entscheidung oder offene Produktfrage berührt

- **FS-30 / E-038**: Der Ersatzwert „ohne `hard_timeout` → `consumer_timeout_s`“ ist eine Präzisierung von E-038 und sollte im Register-Text stehen. Nach Ablauf dieser Grenze verbraucht ein Task (der das Consumer-Timeout ohnehin gerissen hat) nach E-001 doch einen Retry. Das widerspricht dem Wortlaut „verliert keinen Retry“. Die Alternative „unbegrenzt“ (Option B) erfüllt den Wortlaut, lässt aber einen hängenden Task den Recycler unbegrenzt blockieren. → Kurze Bestätigung durch Georg nötig.
- **FS-30 / E-018**: Ein SIGTERM während des Recycling-Drains setzt das Gesamtbudget ab SIGTERM wieder in Kraft. Das ist mit E-018 und E-038 vereinbar, aber neues Verhalten. E-018 sollte es nennen, eine neue Entscheidung ist nicht nötig.
- **FS-30 / E-001 (Produktfrage FS-01)**: Was bei Fristablauf oder SIGTERM mit dem abgebrochenen Task passiert (Fehlerergebnis oder Neuzustellung), hängt an E-001. Der FS-30-Fix funktioniert unter der heutigen E-001 und muss bei einer Änderung nicht neu gebaut werden.
- **FS-30 / FS-02b**: FS-02b ist eine harte Abhängigkeit (Publish-Endfrist im Recycling), keine Produktfrage.
- **FS-16 / Lücke**: Ob auch nach **regulärem** Kindende (Ergebnis geliefert bzw. Recycling-Ende) verbliebene Enkel beendet werden, entscheidet E-036 nicht. Die Empfehlung lässt es aus.
- **FS-23 / Lücke**: Ob die Wartezeit auf einen freien Slot gegen `hard_timeout` zählt (der Watchdog umschließt `pool.run`) und ob die Concurrency mit gesperrten Slots sinken soll, ist nicht entschieden. Das ist nicht Teil des FS-23-Fixes.
- **FS-24**: keine.
