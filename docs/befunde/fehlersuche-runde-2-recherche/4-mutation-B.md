# Fehlersuche 4B: Überlebende Mutanten, Gruppe B (CPU-Modus, Lifecycle, Shutdown, Recycling, Prefetch)

Stand: Worktree `swarmq-fehlersuche` (v0.12.0). Grundlage: `mut-gruppe-B.txt` (558 Blöcke).
Methode: Mutanten nach Funktion gruppiert, Originalcode im Kontext gelesen, gegen Plan
`docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md` (KTD3, KTD11, R22, AE13) und
`website/` geprüft. Verdachte mit Skripten in `scratchpad/fs/mb/` nachgestellt. Es wurden nur
Unit-Bausteine und eigene Fork-Kinder verwendet, keine Integration/e2e. Alle gestarteten Prozesse
sind beendet.

Bestätigung bekannter Befunde:
- **K-1/N-5 gelten auch für den Compute-Executor.** `executor.py:199-216` fängt den äußeren Abbruch
  am Drain-Ende ab und gibt ihn nicht weiter. Daraus wird das Fehlerergebnis „compute child cancelled
  during worker drain“. Weil `uncancel()` fehlt, bleibt `cancelling()` danach `> 0`. Der Mutant
  `_await_envelope__mutmut_2` (`if run_task.done()`) ist äquivalent: Der äußere Abbruch läuft über
  `run_task` und kommt erst an, wenn dieser fertig ist.
- **Kein Gegenstück zu N-2 im CPU-Modus.** Ein scheiternder DI-Finalizer lässt das Kind ohne
  Envelope sterben. Das ergibt ein `ChildDied`-Fehlerergebnis und keinen hängenden Lock. Siehe MB-7.

---

## Teil 1: Fehlerverdachte

### MB-1 (P2): Der Kill erreicht die Prozessgruppe nicht mehr, wenn das Kind selbst schon tot ist; Enkel überleben (KTD3/AE13)

- **Fundstelle:**
  - `src/swarmq/worker/compute/pool.py:584-585`: `if child.pid is None or not child.process.is_alive(): return`
  - `pool.py:587-589`: Endet das Kind innerhalb der SIGTERM-Gnade, kommt `return` vor dem SIGKILL an die Gruppe.
  - `pool.py:249-252` / `executor.py` `ChildDied`: Nach einem Kindtod ohne Kill wird überhaupt nichts an die Gruppe geschickt.
- **Mutanten:**
  - `_kill_child__mutmut_2`: `or` → `and`. Der Pfad „Kind schon tot“ ist ungetestet.
  - `_kill_child__mutmut_10/20`: `by_pid_only=None`.
  - `_start_child__mutmut_36/78`: `by_pid_only=False`.
  - `_signal__mutmut_9`.
  - Alle haben überlebt: Kein Unit-Test prüft, *wen* der Kill trifft.
- **Szenario:**
  - KTD3 verspricht „SIGTERM an die Gruppe, … dann SIGKILL“, AE13 „kein Prozess aus der Prozessgruppe des Kindes lebt weiter“.
  - Der zweistufige Kill bricht aber ab, sobald das *Kind* tot ist.
  - **Fall a: Das Kind endet sauber auf SIGTERM.** Das ist bei `child_once` der Normalfall, wenn der Task gerade awaited. Der SIGTERM-Handler bricht den Task ab, und das Kind endet in Millisekunden. Ein Enkel, der SIGTERM ignoriert oder langsam behandelt, bekommt nie SIGKILL. Beispiele: Hilfsprozess mit eigenem Handler, `trap`-Shell, per `Popen` gestarteter Dienst. Er läuft verwaist weiter.
  - **Fall b: Das Kind stirbt von selbst.** Beispiele: OOM-Killer (trifft im CPU-Modus typischerweise das Python-Kind mit dem größten RSS), Segfault, `os._exit`. `run()` bekommt `ChildDied`, und niemand signalisiert der Gruppe. Ein laufender ffmpeg-/ghostscript-Enkel rechnet ohne Besitzer weiter. Jeder Retry startet einen weiteren.
- **Beleg:** `mb/kill_group.py`. Das Kind setzt `setpgid`, startet einen SIGTERM-festen Enkel und endet selbst auf SIGTERM.
  - Ausgabe: `_kill_child zurueck nach 0.01s, Kind lebt: False` / `Enkel … lebt noch: True (state S), pgid=<Kind-PID>`
  - Der e2e-Test `CpuSubprocess` deckt das nicht ab. Sein Enkel (`sleep 60`) stirbt an SIGTERM, und das Kind blockiert synchron in `time.sleep`, bekommt also SIGKILL.
- **Vorschlag Nachweistest (Unit, ~50 ms):** wie `mb/kill_group.py`.
  - Aufbau: `SlotPool` mit `mp.get_context("fork")`, `_Child(pgid_ready=True)`.
  - Erwartung: Nach `await pool._kill_child(...)` ist `os.killpg(child.pid, 0)` `ProcessLookupError`.
  - Zweiter Test: Kind stirbt von selbst (z. B. `os._exit(1)` nach dem Enkelstart). Nach `run()` → `ChildDied` lebt kein Gruppenmitglied mehr.
- **Fix-Richtung:**
  - Nach dem Tod des Kindes bei `pgid_ready` immer noch `killpg(pid, SIGKILL)` schicken. `ProcessLookupError` ignorieren.
  - Das auch im `ChildDied`-Pfad und in `_finalize` tun.
- **Konfidenz:** hoch für den Mechanismus. Mittel für die Häufigkeit: Fall b mit OOM ist im CPU-Modus realistisch, Fall a braucht einen SIGTERM-festen Enkel.

### MB-2 (P2): Recycling-Drain ohne registrierten `hard_timeout` fällt auf `shutdown_timeout` zurück; Langläufer verlieren beim Recycling doch einen Retry (R22)

- **Fundstelle:**
  - `src/swarmq/worker/recycling.py:79-86` (`recycle_drain_timeout`): `if not hard_timeouts: return shutdown_timeout`. Sonst `max(hard_timeout) + 30`, wobei Tasks *ohne* `hard_timeout` ignoriert werden.
  - `shutdown.py:103-118`: Override als Drain-Frist, danach `task.cancel()`.
- **Mutanten:**
  - `_recycle_drain_timeout__mutmut_7`, `_recycle_monitor__mutmut_8` (`continue` → `break`), `_recycle_monitor__mutmut_22` (`and` → `or`).
  - Den Rückfallzweig prüft kein Test. `test_recycling.py:128` testet nur den Fall mit `hard_timeout`.
- **Szenario:**
  - R22/KTD11 sagen: Das Recycling drainiert „ohne erzwungenen Abbruch“, und es „verbraucht keinen Retry eines laufenden Tasks“.
  - `hard_timeout` ist per Default `None` (`task.py:90`). Ein IO-Worker mit `SWARMQ_RECYCLE_MAX_TASKS` oder `_RSS_MB` (Normalbetrieb) und Tasks ohne `hard_timeout` drainiert beim Recycling nur `shutdown_timeout` (30 s).
  - Danach wird abgebrochen. Über N-5 wird das zu „hard timeout after None s“: Retry verbraucht, bei `max_retries=0` DLQ, `TASK_FAILED`, Workflow FAILED.
  - Dasselbe passiert in gemischten Registries: ein Task ohne `hard_timeout`, der länger läuft als `max(andere hard_timeouts) + 30`.
  - Die Doku verspricht pauschal „keeps its own drain (largest `hard_timeout`)“ (`website/explanation/delivery-semantics.md:260-261`).
  - Nebenbefund (P3): Ein SIGTERM während eines laufenden Recycling-Drains bleibt wirkungslos, weil `_request_shutdown` außerhalb von RUNNING ein No-op ist. Der lange Override bleibt stehen, entgegen „der erzwungene Abbruch bleibt SIGTERM vorbehalten“ (KTD11). Den Rest erledigt der SIGKILL der Orchestrierung.
- **Beleg:** Codelektüre, eindeutig. Ohne `hard_timeout`-Tasks ist `hard_timeouts == []`.
- **Vorschlag Nachweistest:**
  - Aufbau: IO-Worker-Fixture (wie `test_recycling_drain_is_exempt_from_the_budget`), Registry nur mit einem Task ohne `hard_timeout`, `shutdown_timeout=0.05`, `await recycling.request_recycle(worker, "tasks", 1)`.
  - Ablauf: Ein aktiver Task wartet 0.3 s auf ein Event, danach `_execute_shutdown()`.
  - Erwartung: Der Task wird nicht abgebrochen. Mindestens: `recycle_drain_timeout(worker)` liegt über `shutdown_timeout`, z. B. bei `consumer_timeout_s`.
- **Konfidenz:** hoch für den Mechanismus. Ob der Rückfall gewollt war, ist eine Produktfrage; R22 und die Doku widersprechen ihm.

### MB-3 (P3): Ein im Hintergrund blockierter Slot bleibt in der Free-Queue, und die Sperre wird nie aufgehoben

- **Fundstelle:**
  - `pool.py:593-607`: `_kill_child` setzt `slot.blocked`.
  - `pool.py:255-258`: Nur `run()` gibt den Index abhängig von `blocked` zurück.
- **Mutanten:**
  - `_kill_child__mutmut_48` (`and` → `or`: Poison schon bei *einem* blockierten Slot) hat überlebt.
  - `_end_child__mutmut_1/2`, `_recycle__mutmut_47`.
- **Szenario:**
  - `_recycle` → `_end_child(old)` → `_kill_child(reason="forced")` läuft im Hintergrund, nachdem `run()` den Slot längst zurückgegeben hat. Das gilt ebenso für `aclose`.
  - Überlebt das alte Kind den SIGKILL, ist `slot.blocked=True`, der Index liegt aber weiter in `_free`.
  - Der nächste `run()` bekommt ihn, ohne `blocked` zu prüfen, und erst danach verschwindet er. Das warme Ersatzkind des Slots bleibt bis `aclose` ungenutzt am Leben.
  - Allgemein gilt: Eine Sperre wird nie aufgehoben, auch wenn das Kind später reapbar stirbt (`_on_sentinel`/`_finalize` ändern `blocked` nicht).
  - Die Concurrency von Consumer und Semaphor sinkt nicht mit. Handler warten deshalb in `_free.get()`, und diese Wartezeit zählt bereits gegen `hard_timeout` (Watchdog um `pool.run`). Ein Task kann so mit „hard timeout“ scheitern, ohne je gerechnet zu haben.
- **Beleg:** `mb/blocked_index.py` mit einem Fake-Kind, das SIGKILL überlebt:
  `slot0.blocked: True … free queue: [0, 1] -> Slot 0 wird trotz Sperre weiter vergeben`.
- **Vorschlag Nachweistest:** wie das Skript. Erwartung: `0 not in free`.
  - Zweiter Test: Pool mit 2 Slots, einer blockiert. Ein `run()` auf dem anderen Slot liefert ein Ergebnis und kein `SlotUnavailable`. Das sichert `mutmut_48` ab.
- **Konfidenz:** hoch. Die Auswirkung ist gering, weil D-State selten ist.

### MB-4 (P3): Ein gescheiterter Handshake erzeugt eine asyncio-ERROR-Meldung „ChildDied exception in shielded future“ und zählt als Kindtod

- **Fundstelle:**
  - `pool.py:399`: `wait_for(asyncio.shield(child.ready), …)`.
  - `pool.py:500-504`: `_finalize` setzt danach `ChildDied` auf `ready` und auf das verwaiste `pending` des frischen Kinds.
- **Mutanten:** `_finalize__mutmut_19/22/28/32` und `_start_child__mutmut_71-79` haben überlebt. Kein Test beobachtet, was nach dem Handshake-Fehler passiert.
- **Szenario:**
  - Bei jedem Handshake-Timeout und jedem Abbruch im Handshake bekommt das abgeschirmte `ready`-Future später `ChildDied`, und niemand holt es ab.
  - asyncio meldet das über den Exception-Handler des Loops (ERROR, Logger `asyncio`). Das passiert genau im Störfall „Forkserver tot“, und zwar je Nachricht, was das Log flutet.
  - `stats.died` zählt den Handshake-Fehler zusätzlich als Tod. Die Stats sind aktuell nicht exportiert.
- **Beleg:** `mb/handshake_orphan.py` (Fork-Kind ohne Ready, `handshake_timeout_s=0.3`):
  `stats: … killed=1, died=1, … unavailable=1` und `loop errors: ['ChildDied exception in shielded future']`.
- **Vorschlag Nachweistest:** wie das Skript, mit `loop.set_exception_handler`. Erwartung: keine Loop-Fehler, `died == 0`.
- **Konfidenz:** hoch.

### MB-5 (P3, Ergänzung zu K-1): Im CPU-Modus läuft jeder abgebrochene Handler sicher über die 1-s-Nachlauffrist hinaus

- **Fundstelle:**
  - `shutdown.py:118`: `await asyncio.wait(pending, timeout=min(1.0, …))`.
  - `pool.py:587-591`: SIGTERM, `kill_grace_s=1.0`, SIGKILL.
- **Mutant:** `_drain_in_flight__mutmut_42` (`min(2.0, …)`) hat überlebt.
- **Szenario:**
  - Ein CPU-gebundenes Kind reagiert nicht auf SIGTERM. Der asyncio-Handler kommt nicht dran, solange der Task-Code rechnet.
  - Der Kill dauert deshalb immer mindestens `kill_grace_s` = 1 s plus Reap. Die Nachlauffrist nach `task.cancel()` ist aber genau 1 s.
  - Die Handler sind also am Fristende noch im Kill. Sie laufen danach parallel zu `executor_close`, `_release_leftover_locks`, Scheduler-Stopp und `broker_close` weiter und werden nie wieder abgewartet.
  - Erwartete Folgen:
    - Doppelte Lock-Freigabe: Shutdown und Handler-`finally` geben beide frei. Der Verlierer meldet ein falsches `task_lock_lost` („a second holder may have run in parallel“).
    - Settle und Fehlerpfad des Handlers laufen gegen die schließende Verbindung.
  - Die Kinder sind vor der Lock-Freigabe tot, weil `executor_close` vorher kommt. Eine echte Doppelausführung entsteht daraus nicht.
- **Beleg:** Codelektüre, nicht als Test nachgestellt.
- **Vorschlag Nachweistest:**
  - Aufbau: Worker-Fixture mit Fake-Executor, dessen `execute` bei Cancel noch 1.5 s braucht (Kill-Dauer), dazu ein registrierter Handler-Lock.
  - Ablauf: `shutdown_timeout=0.05`, dann `_execute_shutdown`.
  - Erwartung: kein `task_lock_lost`. Bei Ende von `_execute_shutdown` ist kein Handler mehr aktiv.
- **Konfidenz:** mittel.

### MB-6 (P3): Fortschritt aus dem Rechenkind wird ungeordnet und unbegrenzt weitergeleitet

- **Fundstelle:** `pool.py:510-520`: je `progress`/`publish`-Nachricht ein eigener `create_task(sink(msg))`.
- **Mutanten:** `_forward_progress__mutmut_1`, `set_progress_sink__mutmut_1`, `_start_compute_executor__mutmut_64` (`set_progress_sink(None)`) haben überlebt. Die Weiterleitung prüft kein Unit-Test.
- **Szenario:**
  - Im IO-Modus wartet `update_progress` jeden HSET ab. Im CPU-Modus laufen die HSET-Pipelines nebenläufig, unter Umständen auf verschiedenen Pool-Verbindungen.
  - Ein späteres `100 %` kann von einem früheren `90 %` überschrieben werden, ebenso die Pub/Sub-Deltas.
  - Ein Task, der in einer engen Schleife Fortschritt meldet, erzeugt im Loop-Prozess unbegrenzt viele Tasks.
- **Beleg:** Codelektüre. Die Umordnung ist theoretisch möglich, nicht beobachtet.
- **Vorschlag Nachweistest:**
  - Aufbau: Sink, der beim ersten Aufruf auf ein Event wartet; zwei Progress-Nachrichten über `_on_message`.
  - Erwartung: Der Sink sieht sie in Reihenfolge, z. B. über eine Queue und einen einzelnen Weiterleitungs-Task.
- **Konfidenz:** niedrig bis mittel.

### MB-7 (P3): DI-Fehler im Kind verlieren ihre Ursache, und ein erfolgreicher Task wird zum „exited without result“

- **Fundstelle:**
  - `child.py:183`: `scope.__aenter__()` liegt vor dem `try`.
  - `child.py:206-212`: `scope.__aexit__` im `finally`. Ein Fehler hier überschreibt das fertige Envelope.
- **Mutanten:** `_run_job__mutmut_72..86` (`et`/`tb`/Argumente von `__aexit__`) haben alle überlebt. R26/AE14 (Rollback mit Exception) ist in der Unit-Suite nicht abgedeckt.
- **Szenario:**
  - Scheitert der REQUEST-Provider oder ein Finalizer, endet `_run_job` mit einer Exception. `child_once` und `_serve_jobs` fangen nur `CancelledError`, das Kind stirbt.
  - Der Loop-Prozess meldet `ChildDied("… exited without result")`. Die eigentliche Ursache steht nur im stderr des Kinds.
  - Ein bereits erfolgreich gerechneter Task, dessen Commit-Finalizer scheitert, wird als Kindtod gewertet. Das ist richtigerweise ein Fehler, aber ohne `error_class`. Das warme DI-Kind geht dabei verloren.
- **Vorschlag Nachweistest:**
  - Aufbau: `run_job` mit registriertem Fake-Container, dessen `__aexit__` wirft.
  - Erwartung: ein Fehler-Envelope mit der Finalizer-Exception und kein Wurf.
  - Zusätzlich: `__aexit__` bekommt bei Task-Fehler `(type, exc, tb)` mit `tb is exc.__traceback__`.
- **Konfidenz:** hoch für den Mechanismus. Das Gewicht ist gering, es geht vor allem um Diagnose.

---

## Teil 2: Wichtige Testlücken ohne Fehler

1. **Die Untergrenze des graceful Drains ist ungetestet.**
   - `_execute_shutdown__mutmut_14` (`started_at - drain_until`, also Drain-Frist 0) und `__mutmut_10` (`_shutdown_end` in der Vergangenheit) haben überlebt.
   - Alle Budget-Tests (`test_shutdown_budget_steps.py`, `test_befund_bz18_*`) prüfen nur „endet rechtzeitig“, nie „lässt einen Task fertig werden, der vor `drain_until` endet“.
   - Eine Regression, die bei jedem Deploy alle laufenden Tasks sofort abbricht, bliebe grün.
2. **`SlotPool.aclose` mit aktivem Kind** (`aclose__mutmut_4/6/10`: `_kill_child(None)` bzw. `_end_child(None)` überleben). Der Schließpfad während einer laufenden Berechnung und mit frischem Kind ohne `reuse_child` wird nie ausgeführt.
3. **Teilblockade des Pools** (`_kill_child__mutmut_48`). Dass ein blockierter Slot die übrigen nicht lahmlegt, prüft niemand (siehe MB-3).
4. **Recycling nach Taskzahl im Pool** (`run__mutmut_37/39`: `tasks_done = 1` bzw. `+= 2` überleben). `recycle_max_tasks` (Default 1000 im CPU-Modus) wird über `run()` nie bis zur Schwelle gezählt.
5. **`_finalize`:**
   - `__mutmut_33`: Die Kanäle würden nie geschlossen, ein fd-Leck je Kind. Das betrifft lange Läufe.
   - `__mutmut_19`: Ein Tod vor Ready würde erst nach 5 s statt sofort erkannt.
   - `__mutmut_16`: `reuse_child is not child`.
6. **`ComputeExecutor._build_job`** (15 Mutanten): `task_id`, `task_name`, `queue` als `None`, `retry_count`-Default 1 und `result_ttl_ms / 1000` überleben alle. Der Job, der ins Kind geht, wird nie inhaltlich geprüft. Betroffen sind die Progress-TTL, `TaskContext.retry_count` für DI und der Logkontext.
7. **Kind-Laufzeit `child.py`** (`_serve_jobs` SIGTERM-Logik `mutmut_4-8`, `_run_job` DI-Teardown, `child_once`):
   - Die Kind-Laufzeit läuft nur im Subprozess, die Unit-Suite sieht sie nicht.
   - In-Process-Unit-Tests mit Fake-Pipes für `_serve_jobs` und `_run_job` wären billig: SIGTERM im Leerlauf, SIGTERM im Job, Ende-Signal, EOF.
8. **`recycle_monitor`** (`mutmut_8` `continue` → `break`, `mutmut_22`). Dass der Monitor über einen leeren Takt hinaus weiterläuft und nur die Taskzahl-Schwelle greift, ist ungetestet (siehe MB-2).
9. **EOF-Pfad des Leser-Threads** (`_reader__mutmut_5/21-24`). Ohne EOF fiele alles auf die Sentinel-Gnade von 2 s zurück, ohne dass ein Test es merkt. Der Kernmechanismus „Ergebnis vor Tod“ ist nur in e2e/Integration abgedeckt.
10. **Startwarnungen an Grenzwerten:**
    - `warn_prefetch_over_consumer_timeout` mit genau 2 Queues (`mutmut_2`).
    - `warn_hard_timeout_over_consumer_timeout` (`>=`).
    - `_on_decode_error` ohne `pending` (`mutmut_14` würde dort `AttributeError` im Loop-Callback werfen).

### Grobe Statistik (558 Mutanten, Heuristik plus Durchsicht)

| Kategorie | Anzahl | Einordnung |
|---|---|---|
| Logfelder, Hints, Eventnamen (`=None`, Argument entfernt) | ≈ 130 + ≈ 60 mehrzeilige Logaufrufe | Rauschen |
| `contextlib.suppress(None)`, `getattr(…, )`-Defaults, die nie greifen | ≈ 45 | äquivalent |
| Default- und Zeitkonstanten (`5.0`→`6.0`, `sleep(0.25)`→`1.25`, `join(timeout=2)`), `maxlen` | ≈ 30 | äquivalent bzw. nicht beobachtbar |
| `stats.*`-Zähler, `recent_spawn_ms`, Prozess- und Threadnamen | ≈ 30 | Diagnose, nicht exportiert |
| Grenzvergleiche auf kontinuierlichen Zeit- und RSS-Werten | ≈ 10 | äquivalent |
| Verdrahtung in `lifecycle` (`_wire_coordination`, `_start_observability`, `start`) | ≈ 90 | durch Integration/e2e abgedeckt, unit-seitig nur Mock-Verdrahtung |
| Kind-Laufzeit `child.py` (läuft nur im Subprozess) | ≈ 75 | Testlücke 7, daraus MB-7 |
| Inhaltlich relevant (Pool, Executor, Shutdown, Recycling) | ≈ 90 | daraus MB-1 bis MB-6 und Teil 2 |

Skripte: `scratchpad/fs/mb/kill_group.py`, `blocked_index.py`, `handshake_orphan.py`, `exit_time.py`.
`exit_time.py` misst die Reap-Dauer nach SIGKILL: etwa 30 ms/GB, 8 GB ≈ 0,26 s. Ein langsames
Exit großer Kinder ist damit als Ursache für `compute_child_unreaped` verworfen; dafür braucht es
echten D-State.
