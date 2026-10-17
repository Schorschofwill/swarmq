# Zustandsmodelle (Spur Z, U6 bis U9)

Plan: `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md`, R6 bis R8, KTD1 bis KTD3, KTD9. Stand: swarmq 0.12.3 (Branch `test/fehlersuche-runde-4`), Hypothesis 6.168.0.

Dieser Bericht enthält Verdachte, keine Befunde. Jeder Verdacht geht in U17 durch die Gegenprüfung nach `docs/befunde/README.md`, bevor er ins Register kommt. Je Maschine ein Abschnitt; U7 (Leader), U8 (Workflow) und U9 (Retry) ergänzen ihre Abschnitte unten.

## Gemeinsame Infrastruktur (U6)

| Datei | Inhalt |
|---|---|
| `tests/support/stateful.py` | `AsyncDriver` (hält einen `asyncio.Runner`, `run(coro)` je Regel), `AsyncStateMachine[E]` (Basisklasse: öffnet die Umgebung im eigenen Loop, `run`, Haken `close_subjects`, `teardown` schließt Subjekte, Umgebung und Loop auch nach einer werfenden Regel), Protokoll `StatefulEnv`, `InMemoryEnv` und `open_inmemory_env` |
| `tests/unit/test_stateful_driver.py` | 5 Unit-Tests des Treibers (test-first) |
| `tests/property/stateful/conftest.py` | Fixture `open_env` = `open_inmemory_env` (Store-Factory Fake) |
| `tests/integration/stateful/conftest.py` | Fixture `open_env` = `valkey_env_factory(valkey_url)` (Store-Factory Valkey), `ValkeyEnv`, `age_keys` |

Gebrauch: `run_state_machine_as_test(lambda: Maschine(open_env), settings=...)`. Die Maschine bekommt die Coroutine-Factory, nicht die Umgebung; so öffnet jedes Hypothesis-Beispiel eine frische Umgebung im eigenen Loop.

Valkey-Stufe:

- Beim Öffnen und beim Schließen leert `flush_valkey_db` die Datenbank des xdist-Prozesses; das `ValkeyBackend` bekommt einen eigenen Namensraum je Beispiel. Dass `teardown` die Datenbank wirklich leert, prüft der Isolationstest (`DBSIZE == 0` nach dem ersten Abbau).
- `advance(s)` stellt die `FakeClock` vor und kürzt in Valkey jede Frist im Namensraum um dieselbe Zeit (`PTTL`, dann `PEXPIRE` oder `DEL`). So laufen TTLs, `PEXPIRE GT` und die Owner-Prüfungen der Lua-Skripte echt, ohne reale Wartezeit. Die echte Laufzeit der Befehle kommt hinzu: Eine Frist in Valkey liegt höchstens um die seit dem Öffnen des Beispiels real vergangene Zeit unter dem Modell. `ttl_tolerance_ms` misst genau diese Zeit (`time.monotonic`) plus 50 ms. Die erste Fassung mit fester Toleranz (900 ms, gerundet auf ganze Sekunden) war unter Last flaky (Review: 2 von 6 Läufen rot, „res-b: Rest-TTL 28004 ms, Modell 30000 ms (I1)“).
- Grenze: Werte mit Serverzeit (Scores der Semaphor-ZSETs, Fenster von `sliding_window_counter.lua`) verschiebt der Zeitsprung nicht.

## Lock-Maschine (U6)

Dateien: `tests/property/stateful/lock_machine.py` (Maschine und Modell, Docstring mit Invarianten und Abweichungen), `tests/property/stateful/lock_scenarios.py` (deterministische Abläufe beider Stufen), `tests/property/stateful/test_lock_machine.py`, `tests/integration/stateful/test_lock_machine_valkey.py`.

### Subjekte

- `LockStore` der Umgebung über `backend.locks`: `InMemoryLockStore` oder `ValkeyLockStore` (`lock_acquire.lua`, `lock_release.lua`, `lock_renew.lua`, `lock_extend.lua`). Nur Mutex (`max_concurrency=1`).
- Ende eines Handlers über die echte `gates.lock.release_acquired_locks` mit `drop=keeper.drop`; ein gescheitertes Release erkennt die Maschine an der Warnung `task_lock_lost` (stage `release`).
- Je Worker (`w1`, `w2`) ein echter `LockKeeper` mit Fake-Uhr und geparktem `sleep`. Die Maschine taktet ihn mit `tick_once` so, wie seine Schleife es täte: höchstens ein Takt Abstand, bei blockiertem Loop gar nicht und nach dem Entblocken sofort.
- Unique-Claims über den echten `Client.schedule` (Registry mit Task `unique_for=60`, `unique_until="start"`) gegen `claim_unique` des Backends (`SET NX GET PX` in Valkey) und die echte `finalize.maybe_release_unique` gegen `release_unique` (`unique_release.lua`). Der Broker ist in beiden Stufen ein `InMemoryBroker`, der die Header jedes Publish festhält.
- Der Startpunkt der Pipeline (`WorkerPipeline._release_unique_on_start` und `ctx.start_release_reached = True`) ist privat; die Regel `start_pending` bildet diese zwei Zeilen nach.

### Regeln

Handler starten (Erwerb und Lease, TTL 33, 60 oder 90 s wie `hard_timeout + 30 s`), Handler beenden (`release_acquired_locks`), Erwerb mit eigenem Token wiederholen (FS-06b), Worker abstürzen (SIGKILL: kein Release, neuer Keeper), Takt, Uhr vorstellen (1 bis 70 s, in zwei von fünf Fällen mit blockiertem Loop eines Workers), Lock ohne Keeper erwerben (TTL 9, 30 oder 90 s), erneuern (`renew`, wie das Leader-Lock) und freigeben, fremder Token (`release`, `renew`, `extend`), Erwerb mit verlorener Antwort samt Rückbau (auch Rückbau vor dem Erwerb), `force_release` (gedrosselt, in einem von drei Zügen), `schedule` (Argument 1 oder 2; `unique_for` weggelassen, `None`, 30 oder 120; `unique_until` weggelassen, `start` oder `completion`), Start einer Nachricht, terminales Ende einer Nachricht, `schedule` mit verlorener Claim-Antwort.

### Invarianten mit Quelle

| ID | Invariante | Quelle |
|---|---|---|
| I1 | Halter und Rest-TTL jedes Locks im Store gleich Modell (Toleranz der Umgebung) | `LockStore`-Vertrag, `tests/contracts/coordination.py::LockStoreContract` |
| I2 | Nur der Owner gibt frei; fremder Token bei `release`, `renew`, `extend` ändert nichts; jedes `release_unique` liefert das vom Modell erwartete Ergebnis | `docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md` Abschnitt 3 („Pattern 3“ im Plan), `lock_release.lua`, `unique_release.lua` |
| I3 | Nie zwei Owner: je Lock höchstens ein laufender Handler ohne erlaubte Abweichung, und der ist Halter | E-022, `lock_acquire.lua` |
| I4 | Lease läuft nur ab, wenn kein Takt kam; nach jedem Takt Rest-TTL mindestens ein Fenster, nie verkürzt, höchstens `max(bisherige Frist, jetzt + Fenster)` | E-022, `worker/lock_keeper.py` |
| I5 | Wiederholter Erwerb mit eigenem Token gewinnt ohne neue Frist | E-022, Nachtrag FS-06b |
| I6 | Rückbau nach Ausfall (Lock und Unique-Claim) trifft nie einen fremden Lock oder ein fremdes Flag | E-052 |
| I7 | `unique_until="start"`: je Nachricht genau eine Freigabe, nach dem Start keine zweite, Ende vor dem Start gibt frei | E-034 samt Nachtrag FS-13 |
| I8 | `unique_for=None` claimt nie, auch gegen den TaskInfo-Default; ohne Hash keine Freigabe | E-033 |

### Erlaubte Abweichungen (KTD3)

| Abweichung | E-ID | Umsetzung in der Maschine |
|---|---|---|
| Lock läuft bei blockiertem Loop ab, zweiter Halter erwirbt, `task_lock_lost` | E-022 | Handler ohne Takt mindestens ein Fenster lang trägt `excused`; I3 und I4 gelten für ihn nicht mehr |
| Erwerb im Konflikt liefert `None`, der Aufrufer wartet | E-007 | erwartetes Ergebnis, kein Fehler |
| Rückbau vor dem Erwerb lässt einen verwaisten Lock bis zur TTL | E-052 (Restlücke F6) | Regel `reply_lost_acquire` mit `release_first` |
| `force_release` nimmt einem laufenden Handler den Lock | keine E-ID; Operator-Pfad ohne Owner-Check im `LockStore`-Vertrag | betroffene Handler tragen `excused` |
| Nach dem Start kein Dedup-Schutz mehr | E-034 (Bewusste Folge) | ein `schedule` mit denselben Argumenten läuft durch |

### Läufe

| Lauf | Ergebnis | Laufzeit |
|---|---|---|
| `tests/unit/test_stateful_driver.py` | 5 passed | 0,12 s |
| Fake, Profil `quick` (100 Beispiele, 30 Schritte) plus 8 deterministische Abläufe und Selbsttest | 10 passed | 0,6 bis 0,7 s |
| Fake, Profil `thorough` (10 000 Beispiele), manuell | 10 passed | 70 s |
| Fake, 3 000 Beispiele mit 100 Schritten (Suchlauf, Scratch-Skript) | keine Verletzung | 56 s |
| `uv run just property` (ganze Suite) | 46 passed | 13,4 s |
| `uv run just integration -k stateful`: Valkey, 8 Beispiele, dieselben 8 Abläufe, Selbsttest, Isolationstest | 11 passed | 2,4 s |
| nach dem Review: `uv run just integration -k lock_machine_valkey` zehnmal, jeder zweite Lauf neben `pytest tests/unit -n 4` | 10 von 10 grün | 2,6 bis 2,8 s ohne, 3,1 bis 3,5 s mit Last |
| nach dem Review: Fake, 3 000 Beispiele mit 100 Schritten | keine Verletzung | 61 s |
| Valkey mit 30 Hypothesis-Seeds nacheinander (`--hypothesis-seed=1..30`, je 8 Beispiele, über das Rezept) | 30 von 30 grün | je 1,6 bis 1,9 s |

In 100 Beispielen laufen alle 16 Regeln (gezählt mit einem Profiler: rund 2 800 Schritte, darunter 244 `advance`, 232 Handler-Starts, 179 `force_release`, 296 `schedule`), und sowohl E-022 (blockierter Loop) als auch `force_release` treten als erlaubte Abweichung auf.

Selbsttest: Ein Modell ohne Owner-Vergleich beim Freigeben (`OwnerCheckOmitted`) lässt die Maschine rot werden, deterministisch in beiden Stufen (`test_model_without_owner_check_is_caught*`). Hypothesis findet den Fehler auch selbst und schrumpft ihn auf zwei Schritte: `plain_acquire(name='res-a', ttl_ms=9000)`, `reply_lost_acquire(name='res-a', release_first=False, worker='w1')` (Meldung „Rückbau traf res-a falsch (I6)“). Der Selbsttest bleibt im Repo, statt wie im Plan nach dem Lauf entfernt zu werden.

Isolation: `test_an_example_does_not_see_the_state_of_its_predecessor` erwirbt in einer Maschine einen Lock und ein Unique-Flag, baut sie ab und öffnet eine zweite; die zweite öffnet nur auf leerer Datenbank und sieht weder Lock noch Flag.

### Ergebnis

Keine Invariante verletzt, keine Abweichung zwischen Fake und Valkey. Die deterministischen Abläufe bestätigen:

- zwei Owner mit Ablauf dazwischen nur bei blockiertem Loop (E-022), sonst hält der Keeper den Lock über jede TTL (fünfmal 40 s bei TTL 9 s);
- Takt vor Ablauf verlängert auf ein Fenster; nach Übernahme liefert `extend` `False` und der Keeper meldet `task_lock_lost` mit `stage="running"`, nach Ablauf ohne Takt mit `stage="expired"`, ohne Roundtrip;
- FS-13 ist behoben: ein terminales Ende vor dem Start gibt das Flag im start-Modus frei;
- E-034: eine Freigabe beim Start, keine zweite am Ende;
- E-033: `unique_for=None` claimt nicht und lässt das Flag des Vorgängers stehen;
- E-052: der Rückbau eines Claims mit verlorener Antwort gibt nur den eigenen Claim frei.

### Verdachte

Keine (kein `R4Z-nn` aus der Lock-Maschine). Keine Ausklammerung per `@precondition`.

Hinweis ohne Verdacht (nur Code-Lektüre, nicht gelaufen): Bei Semaphoren mit gemischten TTL unterscheiden sich Fake und Valkey. `lock_acquire.lua` räumt Member nach der TTL des jeweiligen Erwerbers ab, `lock_extend.lua` setzt den Score auf „jetzt“; `InMemoryLockStore` führt je Slot eine eigene Frist. Das Valkey-Verhalten ist entschieden (E-022, Bewusste Folgen, gemischte `hard_timeout`). Die Lock-Maschine bildet Semaphoren nicht nach; ein gemeinsames Modell bräuchte eine Entscheidung, ob der Fake die Valkey-Semantik nachbilden soll.

### Abweichungen vom Plan

- Die Maschine liegt in `tests/property/stateful/lock_machine.py`, die deterministischen Abläufe in `lock_scenarios.py`; beide Stufen importieren sie (der Plan nennt nur die Testdateien).
- `LockKeeper` nimmt den Store als `store` (Callable), nicht `store_getter`.
- Die Valkey-Stufe wartet nicht real, sondern verkürzt die Fristen in Valkey (`age_keys`); die Uhr der Lua-Skripte bleibt die echte.
- Semaphoren sind nicht nachgebildet (siehe Hinweis oben).
- Der Startpunkt der Pipeline ist nachgebildet, weil er privat ist.
- Der Selbsttest mit Modellfehler bleibt als deterministischer Test im Repo.
- Das Zeitlimit der Fake-Datei ist 60 s im Profil `quick` und 1 800 s sonst; im Profil `thorough` braucht die Maschine rund 70 s.
- Die deterministischen Abläufe nutzen `tests/unit/_log_capture.capture_swarmq_logs`, weil `swarmq_logs` nur in `tests/unit/conftest.py` liegt.
- Marker `entscheidung` nur in der Fake-Stufe und nur, wo der Ablauf genau die Entscheidung prüft: E-034 (Ende vor dem Start gibt frei, FS-13), E-033 (`unique_for=None`), E-052 (Unique-Rückbau). Ohne Marker: die Keeper-Abläufe, weil die Regel `advance` dabei die genauen Fristen des Keepers prüft (I1, I4), die E-022 nicht festlegt; „genau eine Freigabe“ (E-034), weil der Startpunkt nachgebildet ist; der Owner-Check-Ablauf (prüft Pattern 3, nicht E-052). Die Valkey-Varianten tragen keinen Marker.

### Hilfen für U7 bis U9

- Basisklasse `AsyncStateMachine[Env]` aus `tests/support/stateful.py`: `self.run(coro)` je Regel, `close_subjects()` überschreiben (Keeper, Scheduler-Loops, Broker), Konstruktor im `try` mit `self.teardown()` im Fehlerfall wie in `LockMachine`.
- Fixture `open_env` in beiden Stufen: `env.clock` (`FakeClock`), `env.backend` (`InMemoryBackend` oder `ValkeyBackend` mit `.locks`, `.schedules`, `.chords`, `.execution_guard`, `.rate_limit`), `await env.advance(s)`, `env.ttl_tolerance_ms`. Die Maschine bekommt die Factory: `run_state_machine_as_test(lambda: M(open_env), settings=...)`.
- Valkey-Einstellungen wie `VALKEY_SETTINGS` in `test_lock_machine_valkey.py` (`max_examples=8`, `deadline=None`, `stateful_step_count=30`, `suppress_health_check=[HealthCheck.too_slow]`).
- `env.ttl_tolerance_ms` ist in Valkey die seit dem Öffnen real vergangene Zeit plus 50 ms (im Fake 0); Vergleiche immer als `nominal - tol <= beobachtet <= nominal`, das Modell bleibt nominal (nie aus dem Store-Wert übernehmen).
- U9 (Rate-Limit) muss beachten: `sliding_window_counter.lua` rechnet mit Serverzeit, der Zeitsprung verschiebt das Fenster nicht.
- Den Namen `check_invariants` nicht verwenden: Hypothesis nutzt ihn intern.
- Nach dem Muster von `lock_scenarios.py` lassen sich deterministische Abläufe einmal schreiben und in beiden Stufen fahren.

## Leader- und Fire-Index-Maschine (U7)

Dateien: `tests/property/stateful/leader_machine.py` (Maschine, Modell, Wrapper der Subjekte, Docstring mit Invarianten und Abweichungen), `tests/property/stateful/leader_scenarios.py` (deterministische Abläufe), `tests/property/stateful/test_leader_machine.py`, `tests/integration/stateful/test_leader_machine_valkey.py`.

### Subjekte

- Zwei echte `SchedulerLoop` (Kandidaten `A`, `B`, feste Owner-Token) über dem gemeinsamen `ScheduleStore` und `LockStore` der Umgebung: `InMemoryScheduleStore`/`InMemoryLockStore` oder `ValkeyScheduleStore` (`sched_add/poll/commit/replan.lua`, Handover-Marker, Cron-Meldungen) und `ValkeyLockStore` (`lock_acquire/renew/release.lua`). Je Loop ein eigener `ScheduleStore` (Definitionsschicht) wie im Betrieb; er hält jeden `commit_fire` mit Halter und Score davor und danach fest.
- Publisher je Kandidat vor einem gemeinsamen `InMemoryBroker`, Fehler schaltbar: `BrokerConnectionError`, `PublishIndeterminate` mit Zustellung (Nachricht im Broker, dann Fehler) und ohne (Fehler vor dem Broker), `UnroutableMessage`. Der `InMemoryBroker` selbst kennt nur `publish_failure()` (immer `BrokerConnectionError`), deshalb der Wrapper.
- `LockStore` je Kandidat hinter einem Wrapper mit schaltbarem Valkey-Ausfall (`acquire`, `renew` und `release` werfen `BackendConnectionError`; damit ist auch der Rücktritt mit gescheiterter Freigabe erreichbar, `stepdown.py`) und Aufzeichnung von Erwerb, Renew und Freigabe.
- Oneshots über den echten `Client.schedule(delay=)`; die verlorene Antwort beim Speichern (E-058) über einen Backend-Wrapper, der `schedules.add` zweimal ausführt (die Wiederholung liefert `False`).
- Ein dritter Worker ohne Kandidatur meldet den statischen Cron `static-1` (`report_cron` wie `_report_crons`), wahlweise mit `unique_for=120`.
- Zeit: `time` in `scheduler/loop.py`, `scheduler/sync.py` und `client/schedule.py` sowie `stepdown._monotonic` laufen auf der `FakeClock`; die Uhr der Event-Loop (Zeitgrenze und Renew-Budget aus E-048) läuft echt plus dem Vorlauf der `FakeClock`. Konfiguration: Defaults (TTL 30 s, Heartbeat 10 s, `op_timeout` 5 s, Fenster 25 s, `stepdown_s` 30 s, Meldetakt 60 s), nur `scheduler_unroutable_oneshot_max_s=600`.

Die Loops laufen nicht als Tasks. Die Regeln takten Wahl, Übernahme und Poll mit denselben Aufrufen wie `_election_loop`, `_adopt_loop` (ohne BZ-14) und `_poll_loop`; diese Methoden sind privat, wie in `tests/unit/scheduler/test_loop.py`.

### Regeln

Wahl-Takt je Kandidat; Übernahme-Takt je Kandidat (nur als Leader, wie die Schleife); Poll-Takt je Kandidat, optional mit Lock-Ablauf (`expire`) oder Übernahme durch den anderen Kandidaten (`takeover`) beim ersten Publish; CLI-Cron anlegen und entfernen; `static-1` entfernen, melden (mit oder ohne `unique_for`), Meldung einstellen; Oneshot per `schedule(delay=1|30|75)`, auch mit verlorener Insert-Antwort; Uhr vorstellen (1 bis 61 s), dabei takten die nicht blockierten Kandidaten je Heartbeat Wahl, erste Übernahme der Amtszeit und Poll (`blocked` = `A`, `B`, beide oder keiner); Publish-Fehler je Kandidat setzen; Valkey-Ausfall im Lock-Pfad je Kandidat; Leader-Key verschwindet (`force_release`); `settle` (Störungen aus, Wahl bis zum Leader, Übernahme, Poll).

### Invarianten mit Quelle

| ID | Invariante | Quelle |
|---|---|---|
| L0 | Definitionen und Fire-Index im Store gleich Modell nach jeder Regel; kein Termin verschwindet oder entsteht ohne erlaubten Ausgang | `ScheduleStore`-Vertrag, E-046 |
| L1 | `commit_fire` gelingt genau für den Halter des Leader-Locks; ein abgelehnter Commit schickt den Loop in den Standby; je Fire-Identität (Name, Generation, Score) höchstens ein wirksamer Commit | E-047, `sched_commit.lua` |
| L2 | Jeder Publish trägt `Name:Score` eines fälligen Index-Eintrags vom Tick-Beginn; ein Oneshot feuert unter der `task_id` aus `schedule()`; je Poll-Takt und Kandidat höchstens ein Versuch je (Name, Score) | F3-01/E-058, BZ-02, E-047 (deckt nur zwei Leader) |
| L3 | Nach `schedule(delay=)`, auch mit verlorener Insert-Antwort, steht der Oneshot mit seinem Termin im Fire-Index | E-058 |
| L4 | Ein wirksamer Commit ohne Zustellung ist erklärt: unroutbarer Cron (nächster Termin, Signal `cron_lost`), unroutbarer Oneshot (+60 s, `unroutable_since` bleibt; nach der Frist entfernt mit Fehlerergebnis), Duplikat beim Unique-Claim nur, solange der modellierte Claim von `static-1` gehalten ist (gesetzt beim Claim mit `now + unique_for`, freigegeben bei Verbindungsfehler und unroutbar, gehalten bei Erfolg und `PublishIndeterminate`; ein Publish bei gehaltenem Claim ist ein Fehler), zurückgehaltener verwaister Cron (`now + seen_interval`, nur ohne lebende Meldung bei der letzten Übernahme der Amtszeit); sonst at-least-once | E-003, E-004, E-011/E-032, E-014, E-046 |
| L5 | Renew `False` heißt Standby; nach jedem Wahl-Takt ist ein Leader weniger als das Fenster (25 s) seit dem letzten erfolgreichen Erwerb oder Renew im Amt | E-048 samt Nachtrag FS-31 |
| L6 | Rücktritt nach Publish-Fehlern genau dann, wenn die Serie greift (≥ 2 gezählte `BrokerConnectionError` über ≥ `stepdown_s`, neue Serie nach Lücke, Amtszeit oder gelungenem Publish); danach 2 × TTL kein Erwerbsversuch | E-013, `scheduler/stepdown.py` |
| L7 | In einer neuen Amtszeit publiziert der Poll erst nach der ersten Übernahme der Meldungen, sonst frühestens nach 5 s (fail-open) | E-014, `scheduler-cron-sync-clobber`-Lösung |
| L8 | Die Übernahme ändert nur `static-1`: legt ihn bei Meldung an (bei laufender Meldung zwingend) auf den nächsten Termin, plant höchstens neu, entfernt nie | E-014, E-049 |
| L9 | Ohne Störung gibt es nach höchstens 12 Heartbeats einen Leader; nach Übernahme und Poll ist nichts mehr fällig | E-046 |

### Erlaubte Abweichungen (KTD3)

| Abweichung | E-ID | Umsetzung in der Maschine |
|---|---|---|
| Zwei Leader, doppelt publiziertes Fire (veralteter Leader, Übernahme mitten im Tick) | E-047 | Publish eines fälligen Eintrags vom Tick-Beginn ist erlaubt; nur der Commit wird geprüft (L1) |
| Rücktritt bei kurzer Störung, Wiederwahl per Selbsttreffer | E-048 | kein Fehler; L5 prüft nur die Obergrenze |
| Kein Fire nach `PublishIndeterminate` bis Sperrablauf (Cron mit `unique_for`) | E-011 | Commit mit Duplikat-Signal ist ein erklärter Ausgang (L4) |
| Kurz nicht gemeldeter Cron fällt aus | E-014 | Rückhalt nur, wenn bei der letzten Übernahme keine laufende Meldung bestand |
| Unroutbarer Cron-Lauf fehlt endgültig | E-003 | erklärt, wenn gezählt (Signal `cron_lost`) |

### Läufe

| Lauf | Ergebnis | Laufzeit |
|---|---|---|
| `uv run just property -k leader_machine` (Profil `quick`, 100 Beispiele, 30 Schritte, 10 Abläufe, 3 Sabotage-Selbsttests) | 14 passed | 1,4 s |
| `uv run just integration -k leader_machine_valkey` (8 Beispiele, 9 Abläufe, 3 Sabotage-Selbsttests, Isolationstest), dreimal nach dem Review | je 14 passed | 3,6 bis 4,0 s |
| Valkey mit 30 Hypothesis-Seeds nacheinander (`--hypothesis-seed=1..30` über das Rezept, je 8 Beispiele; vor dem Review) | 30 von 30 grün | je 2,6 bis 4,7 s |
| Fake, 5 000 Beispiele mit 100 Schritten (Suchlauf, Scratch-Skript), vor dem Review | keine Verletzung | 141 s |
| Fake, 5 000 Beispiele mit 100 Schritten, nach dem Review (Claim-Modell, ein Versuch je Identität und Takt) | keine Verletzung | 136 s |

Im Suchlauf nach dem Review traten alle Ausgänge auf: 7 107 Cron-Fires publiziert, 10 973 Oneshots publiziert, 465 unroutbare Cron-Fires (E-003), 1 390 Zurückstellungen unroutbarer Oneshots (E-004), 768 Duplikate bei gehaltenem Unique-Claim (E-011/E-032), 3 zurückgehaltene verwaiste Crons (E-014), 204 Rücktritte nach Publish-Fehlern (E-013), 427 abgelehnte Commits eines veralteten Leaders (E-047), 2 288 Polls mit Lock-Ablauf und 2 399 mit Übernahme mitten im Tick, 31 876 `settle`. Den Rücktritt mit gescheiterter Freigabe erreicht die Zufallssuche nicht (in 1 000 Beispielen keiner); der Ablauf `stepdown_with_failed_release_leaves_the_lock_to_its_ttl` deckt ihn in beiden Stufen. Das Verwerfen nach der Frist (E-004) erreicht die Zufallssuche nicht; der Ablauf `unroutable_oneshot_backs_off_and_is_dropped_with_an_error_result` deckt es in beiden Stufen.

Selbsttests im Repo (beide Stufen, deterministisch, nach dem Muster `test_model_without_owner_check_is_caught` aus U6): `sabotage_owner_check_is_caught` (Commit mit dem Token des Halters, L1), `sabotage_kept_claim_is_caught` (`_release_own_claim` leer, E-011), `sabotage_double_publish_is_caught` (`_publish_fire` publiziert zweimal, L2). Alle drei werden rot erwartet und sind es.

Sabotage-Suchläufe (Scratch-Skripte, nicht im Repo; je 2 000 Beispiele mit 40 Schritten gegen die Fakes; die letzten beiden aus dem Review):

| Sabotage | gemeldet als |
|---|---|
| Owner-Check in `commit_fire` ausgehebelt | „Commit von cand-A trotz Halter None (L1, E-047)“ |
| `_in_stepdown_cooldown` immer `False` | „Erwerbsversuch 10000 ms nach Rücktritt (E-013)“ |
| `_publish_fire` publiziert nicht, meldet aber Erfolg | „… festgeschrieben ohne Zustellung (E-046)“ |
| `_leader_tick` ignoriert Renew `False` | „Renew False, trotzdem Leader (E-048)“ und Zeitgrenze überschritten |
| `_release_own_claim` leer (vor dem Review unentdeckt) | „static-1: Duplikat ohne gehaltenen Claim, Fire verloren (E-011)“ |
| `_publish_fire` publiziert zweimal (vor dem Review unentdeckt) | „… zweimal in einem Poll-Takt (E-047)“ |
| keine Sabotage (Kontrolle) | kein Fehler |

Die deterministischen Abläufe (Fake und Valkey; der Rückhalt nur im Fake):

- Lock geht mitten im Tick an `B`: `A` publiziert, sein Commit wird abgelehnt, `A` geht in den Standby; `B` publiziert erneut (E-047) und schreibt genau einmal fest.
- Oneshot mit verlorener Insert-Antwort: Fire-Index hat den Eintrag (E-058), der Oneshot feuert unter seiner `task_id`.
- Zwei Verbindungsfehler im Abstand von `stepdown_s`: Rücktritt, Lock frei, 2 × TTL kein Erwerbsversuch, danach Wiederwahl (E-013).
- Fälliger Cron wird entfernt: kein Publish, Fire-Index leer.
- Rücktritt nach Publish-Fehlern bei Valkey-Ausfall: die Freigabe scheitert, A ist trotzdem Standby, das Lock läuft per TTL ab, erst dann übernimmt B (E-013, `stepdown.py`).
- Unroutbarer Oneshot: +60 s je Versuch mit festem `unroutable_since`, nach 600 s verworfen mit Fehlerergebnis (E-004).
- Unroutbarer Cron-Fire: gezählt und auf den nächsten Termin festgeschrieben (E-003).
- Leader-Key verschwindet: `B` übernimmt, `A`s Renew liefert `False`, `A` geht in den Standby (E-048).
- Nach Störungen auf beiden Seiten (`PublishIndeterminate`, Valkey-Ausfall) findet `settle` einen Leader, nichts bleibt fällig.
- Nur Fake: gemeldeter, dann nicht mehr gemeldeter `static-1` wird nach Ablauf der Meldung zurückgehalten (`now + 60 s`), nicht publiziert, nicht entfernt (E-014).

### Ergebnis

Keine Invariante verletzt, keine Abweichung zwischen Fake und Valkey in den gemeinsamen Abläufen und 240 Valkey-Beispielen. Kein Fehler in den Fakes gefunden; `tests/support/inmemory` ist unverändert.

### Verdachte

Keine (kein `R4Z-nn` aus der Leader-Maschine). Keine Ausklammerung per `@precondition`.

Hinweis ohne Verdacht: E-013 nennt als Neubeginn der Fehlerserie die Lücke über der Frist und die neue Amtszeit. Der Code setzt die Serie zusätzlich nach jedem gelungenen Publish zurück (`PublishStepdown.success` über `_note_publish_ok`); die Maschine bildet das nach. Das Verhalten ist gewollt und getestet (`tests/unit/scheduler/test_stepdown.py:70` `test_a_successful_publish_resets_the_series`, `:187` `test_a_successful_fire_in_between_keeps_the_leader`, ohne `entscheidung`-Marker), steht aber nicht im Register; Kandidat für einen Nachtrag zu E-013.

Grenzen der Umgebung (im Docstring): Alle Fristen und Zeitsprünge sind ganze Sekunden, die echte Laufzeit eines Beispiels bleibt unter 1 s (`ttl_tolerance_ms`). Ist die Zeitgrenze beim Renew schon erreicht, ist das Budget in `_renew_lock` negativ (`loop.py:552`): Der Fake-Renew suspendiert nicht und gelingt vor dem Abbruch, der Valkey-Renew endet als `TimeoutError`. Erreichbar nur über einen Leader, der länger als das Fenster nicht getaktet hat (blockierter Loop, E-047); kein Verdacht, eine Grenze der Fake-Stufe.

Review-Nachbesserung (P1/P2): Die Duplikat-Ausnahme (E-011) gilt nur noch bei modelliertem gehaltenem Claim, ein Doppel-Publish derselben Identität im selben Takt ist verboten, `PublishIndeterminate` ohne Zustellung stellt nichts mehr zu, die Freigabe im Lock-Wrapper beachtet den Ausfall, und drei Sabotagen sind feste Selbsttests.

### Abweichungen vom Plan

- Die Maschine taktet die privaten `_election_tick`, `_poll_tick`, `_note_adopt_ran` und `_fire_lock` (keine öffentliche Tick-API; Muster aus `tests/unit/scheduler/test_loop.py`). `adopt_reported_crons` ist öffentlich.
- Zwei Kandidaten, nicht drei.
- Publish-Fehler sitzen je Kandidat in einem Wrapper vor dem gemeinsamen `InMemoryBroker` (der Fake kennt nur `BrokerConnectionError`), nicht im Broker selbst.
- Die verlorene Insert-Antwort (E-058) kommt aus einem Backend-Wrapper, der das Insert wiederholt, nicht aus dem Fake.
- `claim_fire`/`commit_fire`/Handover des Plans: `SchedulerLoop` nutzt `claim_fire` nicht (das ist der Fire-Claim im Worker-Gate); die Maschine fährt `poll_due`, `commit_fire`, Handover-Marker, Cron-Meldungen und `replan_if_unchanged`.
- `scheduler_unroutable_oneshot_max_s` ist 600 s statt 86 400 s, damit das Verwerfen (E-004) erreichbar ist.
- „Leader-Übernahme nach Sync (nie vorher)“ ist als Invariante L7 umgesetzt (kein Publish vor der ersten Übernahme der Amtszeit, höchstens 5 s fail-open); die Maschine schreibt nie selbst in den Fire-Index.
- Der Rückhalt verwaister Crons (E-014) ist nur im Fake erreichbar: Valkey führt die Cron-Meldungen mit Serverzeit (`TIME` in `sched_report.lua`), der Zeitsprung lässt sie nicht veralten. Die Maschine prüft den Rückhalt deshalb nur in eine Richtung (Rückhalt nur ohne lebende Meldung), und der Ablauf dazu läuft nur gegen die Fakes.
- Nicht nachgebildet: ob ein doppelt publiziertes Fire (E-047, zwei Leader) doppelt ausgeführt wird; ohne Worker und Fire-Claim ist das unsichtbar, die Maschine verbietet nur zwei Versuche derselben Identität im selben Takt eines Kandidaten. Ebenso `check_orphaned_queues` (BZ-14), Unique-Oneshots (`cli_unique`), Ausfälle von Poll und Commit, Wechsel von Cron-Ausdruck und Zeitzone, Prune nach 24 h.
- Ohne `-m property` wählt `uv run --extra test pytest tests/property/stateful/test_leader_machine.py -q` nichts aus (`addopts` in `pyproject.toml`); der Lauf braucht `-m property` oder `uv run just property -k leader_machine`.

## Workflow-Maschine (U8)

Dateien: `tests/property/stateful/workflow_machine.py` (Maschine, Modell, Worker-Naht, Backend-Sicht mit Störungen, Docstring mit Invarianten und Abweichungen), `tests/property/stateful/workflow_scenarios.py` (deterministische Abläufe beider Stufen und Nachweise der Verdachte), `tests/property/stateful/test_workflow_machine.py`, `tests/integration/stateful/test_workflow_machine_valkey.py`.

### Subjekte

- Der echte `WorkflowAdvancer` (`workflow_orchestration.py`) mit dem `ChordStore` der Umgebung (`InMemoryChordStore` oder `ChordManager` über `ValkeyBackend.chords`, `chord_complete.lua`) und dem Compare-and-Set des Backends (`compare_and_store_workflow_state`, `workflow_state_cas.lua`). Je Zustellung ein eigener Advancer (Mehrprozess-Fall wie `MultiDriver`).
- Start über den echten `Client.apply` (Zustand vor der Front, deterministische Schritt-Kennungen, Fehlermarkierung bei unroutbarer Front).
- Worker-Naht über die echten `CancelGate.apply`, `ResultTail.finish` (Erfolg, Fehlerergebnis, Advance-Fehler inklusive `UnroutableMessage` und `WorkflowStateConflict`) und `RetryHandler.dead_letter` (Fail-Mark über `fail_workflow_step`, dann DLQ-Kopie), verdrahtet über ein eigenes `GateDeps` wie in `WorkerPipeline`. Der Advancer publiziert über den echten `WorkflowStepEnqueuer`.
- Broker: ein `InMemoryBroker` in beiden Stufen. Er nimmt Publishes an und hält die Verben je Zustellung fest (`ack`, `redeliver`, `retry_later`, `dead_letter`); die Maschine stellt selbst zu. Die Queue `sm-missing` gibt es nicht, ein Publish dorthin wirft `UnroutableMessage`.
- Störungen in einer Backend-Sicht je Worker: verlorene CAS-Runden (ein nebenläufiger Schreiber erhöht die Version), scheiternde Lesungen und ein scheiterndes Compare-and-Set des Zustands (`BackendConnectionError`), und für die Regel `concurrent_advance` `asyncio.sleep(0)` vor jedem Backend-Aufruf nach einem gezogenen Plan.
- Der Ausführungsschutz (E-009) ist nachgebildet: eine Kopie eines bestätigt erledigten Schritts wird verworfen.

### Regeln

Workflow starten (zufällige Topologie aus Chain, Group und Chord bis Tiefe 3, Blätter `sm_echo.s`, `.si` und selten `sm_ghost` ohne Queue; höchstens drei Workflows); Bau mit einem FAF-Schritt an zufälliger Stelle; Zustellung eines offenen Schritts mit Erfolg, Fehlerergebnis (`ResultTail`) oder Dead-Letter (`RetryHandler`); erneute Zustellung eines erledigten Schritts (Absturz vor dem Ack, gleiches Ergebnis, am Ausführungsschutz vorbei); zwei Zustellungen gleichzeitig (auch dieselbe Nachricht zweimal) mit Verschränkung an jedem Backend-Aufruf; `handle.cancel()`; `handle.cancel()` während ein Schritt läuft (nach dem Cancel-Gate); 1, 2, 24 oder 25 verlorene CAS-Runden; 1 oder 2 scheiternde Lesungen; ein scheiterndes Compare-and-Set; `drain` (alles mit Erfolg zustellen, dann Lebendigkeit).

### Invarianten mit Quelle

| ID | Invariante | Quelle |
|---|---|---|
| W1 | Ein Schritt wird erst publiziert, wenn jedes Blatt, von dem er abhängt (frühere Kettenglieder, ganzer Chord-Header), ein Erfolgsergebnis hat; der Chord-Body nie vor dem letzten Mitglied | Plan U8, R12 des Workflow-Plans 2026-05-30-001, `chord_complete.lua` |
| W2 | Jeder Pfad höchstens einmal publiziert, außer im Publish-vor-Persistenz-Fenster | Plan U8, `tests/chaos/test_workflow_chord_idempotency.py` |
| W3 | Jeder Pfad in `step_tasks` wurde vorher publiziert (außer der Anfangsfront, die `apply` bewusst vorab einträgt) | AE5, `WorkflowAdvancer.mark_enqueued` |
| W4 | Fehlerergebnis oder Dead-Letter eines Schritts ohne Neuzustellung: Workflow terminal | E-040 |
| W5 | Unroutbarer Folge-Publish mit bestätigter Zustellung: `failed` (`UnroutableMessage`); unroutbarer Start: `failed` | E-002 |
| W6 | FAF-Schritt in Chain, Group oder Chord: `WorkflowValidationError` beim Bau, nichts publiziert | E-039 |
| W7 | Terminal ist endgültig; `completed` nur mit Erfolg jedes Blatts, ohne vorheriges Cancel und mit dem Ergebnis des Referenz-Auswerters; `cancelled` nur nach Cancel (ein Schritt, der nach dem Cancel endet, schaltet nicht weiter); `failed` nur mit Ursache und Fehlertyp `TaskError` oder `UnroutableMessage` | R13 bis R17 des Workflow-Plans, `WorkflowHandle.cancel`, `_resolve_terminal` |
| W8 | Jede Zustellung endet mit genau einem Broker-Verb | `docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md` |
| W9 | Nach `drain` ist jeder Workflow terminal, außer nach Cancel | E-040 (Liveness), FS-32 |

### Erlaubte Abweichungen (KTD3)

| Abweichung | Quelle | Umsetzung in der Maschine |
|---|---|---|
| Fail-Mark scheitert vorübergehend (Backend weg, 25 CAS-Runden): Neuzustellung statt Ack | E-040 | W4 gilt nur für bestätigte Zustellungen |
| `status()` bleibt nach `handle.cancel()` `running`, wenn kein Schritt mehr fertig wird | FS-32 (LÜCKE, offen) | W9 erlaubt `running` nach Cancel |
| Doppel-Publish im Publish-vor-Persistenz-Fenster (Neuzustellung nach Publish, überlappende Advances) | keine E-ID; Workflow-Plan 2026-05-30-001 Z. 103 und 465, `chords.py`, `chord_complete.lua` | Pfade, die eine danach neu zugestellte oder eine überlappende Zustellung publizierte, sind von W2 ausgenommen; siehe R4Z-03 |

### Läufe

| Lauf | Ergebnis | Laufzeit |
|---|---|---|
| `uv run --extra test pytest tests/property/stateful/test_workflow_machine.py -q -m property` (Profil `quick`, 100 Beispiele, 30 Schritte, 11 Abläufe, Selbsttest, 3 Nachweise) | 13 passed, 3 xfailed | 1,4 bis 2,7 s |
| Fake, 17 Hypothesis-Seeds nacheinander (Profil `quick`) | nach den Ausklammerungen alle grün | je 1,1 bis 3,1 s |
| `uv run just integration -k workflow_machine_valkey` (8 Beispiele, 11 Abläufe, 2 Nachweise, Isolationstest) | 13 passed, 2 xfailed, siebenmal | 3,5 bis 5,5 s |
| Fake, 1 000 Beispiele mit 30 Schritten (Statistik, Scratch-Skript) | keine Verletzung | 21 s |
| Fake, Profil `thorough` (10 000 Beispiele, 30 Schritte), Seeds 2001 bis 2003, manuell | 3 von 3 grün | 167 bis 191 s je Lauf |
| Valkey mit 20 Hypothesis-Seeds nacheinander (`--hypothesis-seed=1..20` über das Rezept, je 8 Beispiele) | 20 von 20 grün (13 passed, 2 xfailed) | je 2,6 bis 3,0 s |
| `uv run just property -k workflow_machine` | 13 passed, 3 xfailed | 1,9 s |
| nach dem Review: `uv run just property -k workflow_machine` (mit 3 Sabotagen, eingeengter Klammer) | 16 passed, 3 xfailed | 1,2 s |
| nach dem Review: `uv run just integration -k workflow_machine_valkey` dreimal (Zufallssuche ohne `concurrent_advance`, 3 Sabotagen) | 16 passed, 2 xfailed | 3,0 bis 3,2 s |
| nach dem Review: Fake, 12 Seeds (`--hypothesis-seed=61..72`) mit eingeengter Klammer | 12 von 12 grün | je rund 1,5 s |

Statistik über 1 000 Beispiele (Zustand der Workflows beim Abbau): 441 `completed` (141 davon mit Chord), 306 `failed` (198 unroutbarer Start, 58 unroutbarer Advance, 43 Fehlerergebnis, 12 Dead-Letter), 141 `cancelled`, 766 `running` (372 nach Cancel, der Rest mit offenen Schritten bei 30 Schritten); 114 Workflows mit Doppel-Publish im erlaubten Fenster, 222 vom Ausführungsschutz verworfene Kopien.

Selbsttest: Ein Referenz-Auswerter mit falschem Endergebnis (`ReferenceOff`) lässt W7 rot werden (`test_wrong_reference_is_caught`). Dazu drei Sabotagen am Subjekt (`SABOTAGES` in `workflow_scenarios.py`, `test_sabotage_is_caught*` in beiden Stufen, je mit `patch.object` auf einem festen Ablauf): Chord-Store meldet ein Mitglied zu früh vollständig (W7, „failed ohne Ursache“), `WorkflowAdvancer.fail` ohne Wirkung (W4 beim Dead-Letter), `_enqueue_node` ohne Überspringen markierter Schritte (W2 bei der Neuzustellung des letzten Mitglieds). Alle drei werden in beiden Stufen erkannt. In der Zufallssuche (Profil `quick`) finden sich W2- und W4-Verletzungen dieser Sabotagen nur stochastisch (Prüfer-Skript im Scratchpad `u8review/sabotage.py`); verlässlich prüfen sie die festen Abläufe. Isolation (Valkey): ein zweites Beispiel öffnet nur auf leerer Datenbank und sieht den Zustand des ersten nicht.

Die deterministischen Abläufe (beide Stufen): Chord mit drei Mitgliedern, das letzte zweimal zugestellt, Body einmal; zwei von drei Mitgliedern beliebig oft neu zugestellt, kein Body; zwei Advancer auf dasselbe letzte Mitglied, eine Schreibung gewinnt, die zweite merged, Body läuft einmal; ein Mitglied im Dead-Letter, ein anderes fertig, `failed` und kein Body; Chain in Group in Chord rückwärts zugestellt, Ergebnis gleich Referenz; FAF als Chain-Glied, Gruppenmitglied und Chord-Body abgelehnt (E-039); unroutbarer Nachfolger und unroutbare Front (E-002); vorübergehend scheiterndes Fail-Mark (E-040); 25 verlorene CAS-Runden mit Neuzustellung und Abschluss; Cancel ohne weiteren Schritt bleibt `running` (FS-32).

### Verdachte

**R4Z-01: Ein Gruppenglied, dessen Marker der publizierende Advance nicht persistiert hat, kippt den Workflow beim Fan-in auf `failed`.**

- Fundstelle: `workflow_orchestration.py` `_on_group_member` → `_refresh_step_tasks` → `_ordered_member_ids` (Guard „resolved N of M member results“, Z. 657 bis 664); Ursache ist die Reihenfolge in `_enqueue_node`/`_persist` (Publish vor Persistenz) zusammen mit einem Abbruch zwischen beiden.
- Szenario (geschrumpft): `group(a1, chain(a2, a3))`. `/g0` fertig. Das Compare-and-Set scheitert einmal (`BackendConnectionError`, Valkey kurz weg). `/g1/0` fertig: der Advance publiziert `/g1/1`, `_persist` wirft, `ResultTail` stellt `/g1/0` neu zu. `/g1/1` läuft vor dieser Neuzustellung: der Fan-in der Gruppe findet `/g1/1` nicht in `step_tasks` und markiert den Workflow `failed` mit `WorkflowValidationError` („group at '' resolved 1 of 2 member results …“), obwohl kein Schritt scheiterte. Hypothesis fand es zuerst mit 25 verlorenen CAS-Runden und `chain(group(chord(group(a0), a0)))` (der Body des Chords lief vor der Neuzustellung des Header-Mitglieds).
- Beleg: `tests/property/stateful/test_workflow_machine.py::test_unmarked_member_completing_first` und `tests/integration/stateful/test_workflow_machine_valkey.py::test_unmarked_member_completing_first_against_valkey` (beide `xfail(strict=True)`, scheitern heute mit `status == failed`).
- Erwartung: Der Docstring von `_refresh_step_tasks` sagt, die frische Lesung bleibe „als Schutz gegen das Publish-vor-Persistenz-Fenster: ein Schritt wird publiziert, BEVOR sein Marker persistiert ist, und kann schneller enden als die Persistenz“ (ebenso KTD11 des Plans 2026-09-08-001). Der Docstring von `_enqueue_node` sagt, publish-then-mark verliere nie einen Schritt, schlimmstenfalls gebe es ein seltenes idempotentes Doppel-Enqueue. `website/guides/workflows.md:160-162` sagt zu, dass das letzte Glied den Fan-in genau einmal auslöst; `:232` nennt als Ursachen für `failed` nur terminal gescheiterte Schritte. Der Schutz greift nur, wenn die Persistenz vor dem Fan-in des Nachfolgers liegt; sonst endet der Workflow `failed`, obwohl kein Schritt scheiterte. Betroffen ist jedes Gruppenglied, dessen Blatt erst ein Worker-Advance publiziert (Kette als Mitglied, Chord als Mitglied, Gruppe hinter einem Kettenglied).
- Realismus: Der wahrscheinlichste Auslöser ist ein harter Worker-Tod (SIGKILL, OOM) nach dem Publish und vor der Persistenz. Die Neuzustellung des publizierenden Schritts wartet dann, weil der Ausführungsschutz den Running-Marker des toten Laufs sieht und zurückstellt (`worker/gates/execution_guard.py`, `claim.status == "running"`), während der schon publizierte Nachfolger sofort läuft und den Fan-in erreicht. Weitere Auslöser: Valkey-Aussetzer genau beim Compare-and-Set, 25 verlorene CAS-Runden, ein Nachfolger, der schneller endet als die Persistenz.
- Einstufungsvorschlag: FEHLER.
- Fix-Idee: `_terminal_result_id` leitet die Kennung eines Blatts deterministisch ab (`self._make_task_id(wf_id, path)`) statt sie in `step_tasks` nachzuschlagen; die Kennungen sind ohnehin deterministisch (`workflow_step_task_id`). Ein fehlendes Ergebnis meldet dann `_gather_results`.
- Konfidenz: hoch für den Mechanismus (deterministisch in beiden Stufen), mittel für die Häufigkeit.
- Ausklammerung: `WorkflowMachine.deliverable` (Kommentar `F4-offen (R4Z-01)`, an `deliver_pending`, `concurrent_advance`, `cancel_while_running` und in `drain`): ein Schritt ohne persistierten Marker unter einer Gruppe oder einem Chord-Header (`fan_in_above`) wird erst zugestellt, wenn der Workflow terminal oder abgebrochen ist oder die Neuzustellung den Marker nachgetragen hat. Schritte reiner Ketten laufen frei (ihr Abschluss liest keinen Marker).

**R4Z-02: Dead-Letter nach gespeichertem, aber nicht advancetem Erfolg lässt den Workflow für immer `running`.**

- Fundstelle: `worker/terminal.py` `fail_workflow_step` (Schutz `_step_succeeded`, Z. 59 bis 63) zusammen mit `worker/results.py` (Ergebnis wird vor dem Advance gespeichert; Advance-Fehler → Neuzustellung).
- Szenario (geschrumpft): Workflow aus einem Schritt. Erste Zustellung: Erfolg gespeichert, der Abschluss (`_mark_terminal` → Compare-and-Set) scheitert vorübergehend, Neuzustellung. Die Neuzustellung läuft den Task erneut und endet im Dead-Letter (z. B. `max_retries=0` oder `NoRetry`): `fail_workflow_step` sieht das alte Erfolgsergebnis, markiert nicht („Spätes Duplikat eines erledigten Schritts“), die Nachricht liegt in der DLQ, kein Advance läuft mehr. Status bleibt `running` bis zur TTL. Gleich für das letzte Glied einer Gruppe.
- Beleg: `test_dead_letter_after_unadvanced_success` in beiden Stufen (`xfail(strict=True)`; heute meldet die Maschine „dlq von M2(W1/) bestätigt, Status running (W4, E-040)“).
- Zweiter Weg ohne Wiederausführung: Scheitert der Advance wiederholt vorübergehend, läuft `redeliver_in_place` bis zum Zustelllimit; das `DeliveryCountGate` schickt die Nachricht über `RetryHandler.dead_letter` in die DLQ, `_step_succeeded` ist `True`, es gibt kein Fail-Mark. Danach überschreibt `retry.py:302` (`store_result` des Fehlerergebnisses) das Erfolgsergebnis, der Schritt gilt also auch nachträglich nicht mehr als erfolgreich.
- Erwartung: E-040, Hauptzusage („Ein Workflow läuft nie still in den `get()`-Timeout“). Dagegen steht E-040, Bewusste Folgen: „Ein Schritt, der schon erfolgreich war, markiert seinen Workflow bei einem späteren Dead-Letter nicht als gescheitert.“ Die Folge setzt stillschweigend voraus, dass der Erfolg auch advancet hat. Einstufungsvorschlag: ENTSCHEIDUNG, „Entscheidung überdenken?“ mit dem neuen Argument, dass der Schutz den einzigen verbleibenden Fortschrittsweg schließt (Zustelllimit-Pfad oben). Möglicher Ausweg: im Schutzfall statt nichts den Advance mit dem gespeicherten Ergebnis wiederholen.
- Konfidenz: hoch für den Mechanismus; realistisch nur mit Wiederausführung nach Erfolg (Ausführungsschutz ohne Done-Marker, weil die erste Zustellung neu zugestellt wurde) und terminalem Scheitern dieser Wiederausführung.
- Ausklammerung: in `deliver_pending` wird ein Dead-Letter für einen Schritt mit gespeichertem Erfolg zu einer Erfolgszustellung (Kommentar `R4Z-02`). Kein `@precondition`, weil die Art der Zustellung ein Datenwert der Regel ist.

**R4Z-03: Zwei überlappende Advances publizieren den Chord-Body (und jeden Nachfolger) zweimal.**

- Fundstelle: `workflow_orchestration.py` `_on_group_member`/`_enqueue_node`; `chord_complete.lua` meldet nach Vollständigkeit bei jedem Aufruf 1, der einzige Einmal-Schutz ist der Marker `step_tasks["/b"]`, der erst nach dem Publish persistiert wird.
- Szenario: Chord mit drei Mitgliedern, `/h/g0` und `/h/g1` fertig; zwei Zustellungen von `/h/g2` (Neuzustellung nach Kanalverlust, während die erste noch läuft) mit Verschränkung an jedem Backend-Aufruf: beide lesen den Zustand ohne `/b`, beide bekommen `record_member = True`, beide publizieren `/b`. Eine Schreibung gewinnt, die zweite merged (Version 2). Der Body läuft einmal, weil der Ausführungsschutz die Kopie verwirft (E-009). Dasselbe mit der Neuzustellung eines früheren Mitglieds parallel zum letzten.
- Beleg: `test_overlapping_advancers_publish_the_body_once` (nur Fake, `xfail(strict=True)`, deterministisch zwei Publishes); `two_advancers_on_the_last_member` in beiden Stufen (gegen Valkey ein oder zwei Publishes je nach Antwortreihenfolge).
- Erwartung: Plan U8 („Zwei Advancer auf dasselbe letzte Mitglied: ein CAS gewinnt, Callback einmal“) und die Invariante „Chord-Callback genau einmal“. Der Workflow-Plan 2026-05-30-001 nimmt ein Doppel-Enqueue nur für das Absturzfenster hin; das Register hat keine E-ID dafür. E-047 nimmt Doppel-Publishes nur für den Scheduler hin, E-009 regelt nur die Ausführung. Einstufungsvorschlag: LÜCKE, mit dem Vorschlag einer Regel fürs Register („Nachfolger at-least-once publiziert, einmal ausgeführt über den Ausführungsschutz“); als FEHLER nur, wenn ein Body ohne Ausführungsschutz (Done-Marker abgelaufen, E-010) zweimal liefe.
- Konfidenz: hoch für den Doppel-Publish, niedrig für einen Schaden.

Die Nachweise für Verdachte, die in U17 als ENTSCHEIDUNG oder LÜCKE eingestuft werden (voraussichtlich R4Z-02 und R4Z-03), sind danach in Belege umzubenennen (ohne `xfail`, das heutige Verhalten festschreibend) oder zu entfernen.

Keine Abweichung zwischen Fake und Valkey: dieselben Abläufe und Nachweise verhalten sich gleich, bis auf die Zahl der Publishes in R4Z-03, die gegen Valkey von der Antwortreihenfolge abhängt. Die Rückgabe von `chord_complete.lua` (`SCARD == N`) und `InMemoryChordStore` (`len >= N`) unterscheidet sich nur, wenn mehr als N verschiedene Mitglieder gemeldet würden; das schließen die deterministischen Kennungen aus.

Hinweis ohne Verdacht: `Client.apply` speichert den Zustand samt Anfangsfront vor dem Publish (bewusst, R18 und Docstring). Stirbt der Client-Prozess zwischen beiden, bleibt ein `running`-Zustand ohne Nachricht bis zur TTL; der Aufrufer hat dann keinen Handle. W3 nimmt die Anfangsfront aus.

### Abweichungen vom Plan

- Die Maschine fährt nicht den `Driver`/`MultiDriver` aus `tests/unit/test_workflow_advancer.py`, sondern die echte Worker-Naht (`CancelGate`, `ResultTail`, `RetryHandler`) und `Client.apply`; nur so sind E-002, E-040 und Cancel im echten Code geprüft. Je Zustellung ein frischer Advancer wie im `MultiDriver`.
- Der Plan nennt nur die Testdateien; Maschine und Abläufe liegen wie bei U6 in `workflow_machine.py` und `workflow_scenarios.py`.
- „Zähler je Chord“ im Modell: Der `ChordStore` hat keine Lese-API; die Maschine prüft die Zähler über ihre Wirkung (W1, W2, W7 mit Referenz-Ergebnis), nicht über den Store-Inhalt.
- „Chord-Callback genau einmal“ gilt in der Maschine für die Ausführung; Publishes dürfen im Publish-vor-Persistenz-Fenster doppelt sein (R4Z-03). Das Testszenario des Plans „Zwei Advancer auf dasselbe letzte Mitglied: ein CAS gewinnt, Callback einmal“ hält nur für die Ausführung, nicht für den Publish.
- „`mark_enqueued` nur mit Receipt“ ist als W3 über den gespeicherten Zustand geprüft; den `TypeError` ohne Receipt deckt schon `tests/unit/test_workflow_advancer.py::TestMarkEnqueuedRequiresReceipt`.
- Nicht nachgebildet: Ablauf von Ergebnissen und Zustand (`WorkflowResultExpired`), Middleware-Ablehnung, FAF per `sig()` zur Laufzeit, Wiederholungen vor dem Dead-Letter (`retry_later` des Retry-Pfads), Delay-Zurückstellung als echte Verzögerung (eine `retry_later`-Zustellung kommt sofort zurück in die offene Liste).
- Die Zufallssuche der Valkey-Stufe sperrt `concurrent_advance` (`SerialWorkflowMachine`, Klassenvariable `allow_concurrent = False`): die echte Antwortreihenfolge ändert dort, welche Zustellungen danach offen sind und welche Regeln erlaubt sind (`FlakyStrategyDefinition`). Die Überlappung zweier Advancer deckt in dieser Stufe der Ablauf `two_advancers_on_the_last_member`.
- Ohne `-m property` wählt `uv run --extra test pytest tests/property/stateful/test_workflow_machine.py -q` nichts aus (`addopts`); die Läufe oben tragen `-m property`.
- Keine `entscheidung`-Marker: kein Ablauf prüft genau und nur eine Regel (die E-039-, E-002- und E-040-Abläufe laufen durch die ganze Maschine samt Invarianten).

## Retry- und DLQ-Ketten-Maschine (U9)

Dateien: `tests/property/stateful/retry_chain_machine.py` (Maschine, Modell, `ChainBroker`; Docstring mit Invarianten, Abweichungen und Verdachten), `tests/property/stateful/retry_chain_scenarios.py` (13 deterministische Abläufe beider Stufen, Selbsttest, Nachweise der Verdachte), `tests/property/stateful/test_retry_chain_machine.py`, `tests/integration/stateful/test_retry_chain_machine_valkey.py`.

### Subjekte

- Die echte Worker-Pipeline über `Worker._handle_message`. Sie verdrahtet die Klassen aus dem Plan: `GateDeps`, `DeliveryCountGate`, `ExecutionGuard` (`retry_count_of`, Claim, Finish über den `GuardedBroker`), `RateLimitGate`, `RetryHandler.evaluate` und `RetryHandler.dead_letter`, dazu Ergebnis-Tail und `TaskExecutor` mit echten Soft- und Hard-Timeouts (5 ms, real abgewartet).
- Stores aus der Umgebung: `InMemoryExecutionGuardStore`/`InMemoryRateLimitStore` bzw. `ValkeyExecutionGuardStore` (`guard_claim.lua`, `guard_finish.lua`, `lock_renew.lua`) und `ValkeyRateLimitStore` (`sliding_window_counter.lua`). Ergebnisse und Cancel-Flags gehen an dasselbe Backend.
- Drei Worker auf Broker und Stores: `io` (IO-Modus), `cpu` (`ComputeExecutor` mit einem Pool ohne Kind, der `ChildDied` wirft) und `doomed` (stirbt in der Ausführung; ab dem Tod erreicht keiner seiner Aufrufe Broker, Backend oder Ausführungsschutz).
- Broker in beiden Stufen: `ChainBroker`, eine Unterklasse von `InMemoryBroker` in der Maschinendatei. Er hält jedes Verb je Zustellung fest und schließt zwei Lücken des Fakes gegenüber `RabbitMQBroker`: `dead_letter` stempelt Generation und `x-swarmq-dead-lettered-at` wie `broker/rabbitmq/delivery.py` (über die echte `identity.dead_letter_generation`), `redeliver` zählt `x-acquired-count` wie RabbitMQ 4.3. Nach Absturz oder Kanalverlust stellt er die Nachricht mit `x-delivery-count` und `x-acquired-count` plus 1 neu zu. `tests/support/inmemory` ist unverändert.
- Konfiguration: `retry_max_delay` 60 s, `delivery_limit` 4 (Schwelle 3), `execution_guard_ttl_s` 60 s, Zustands-TTLs 1 d, Waisengrenze 3, Rate-Limit `2/m`.

### Regeln

Task veröffentlichen (bis drei Ketten; die erste per `initialize`); ein bis sechs Zustellungen je Regel (`deliver_some`), jede mit `ok`, retry-fähigem Fehler, `NoRetry`, `Retry(delay)` (−5, 0, 2,5 und 30 s), Hängen (Soft- oder Hard-Timeout je nach TaskInfo), Kindtod (CPU-Worker) oder Absturz (Worker stirbt), jeweils optional mit gescheiterter Kopie (`retry_later`/`dead_letter` werfen `BrokerConnectionError`) oder Kanalverlust beim Bestätigen (`ack` scheitert, bei Kopien `SettleFailedAfterPublish`); Absturzzyklus in einem Schritt (`crash_cycle`: Absturz, sofortige Neuzustellung, Ablauf des Running-Markers); doppelte Kopie (wie nach `PublishIndeterminate`); Retry-Zähler über die UI ändern (`"abc"`, `"3"`, −2, `True`, 2,5, 1, 7, entfernt); Uhr vorstellen (1 bis 90 s); TaskInfo tauschen wie ein Deploy (`max_retries` 0 bis 3, `fixed`/`linear`/`exponential`, Basis 1 oder 90 s, Soft- oder Hard-Timeout, Rate-Limit an/aus); Replay aus der DLQ (`dlq retry`, `dlq retry` doppelt, UI-Move, UI-Move mit Text-Headern, UI-Move ohne Replay-ID). Doppelte Kopie, Header-Änderung und TaskInfo-Tausch höchstens dreimal je Beispiel, damit die Zustellregeln die Schritte bekommen.

### Modell

Je Identität (`task_id|H#n`, eigene Umsetzung von E-009): `done`, `sup`, Lease eines toten Halters, Waisen, Running-Marker bis. Rate-Limit-Zähler je Fenster mit der Formel aus `sliding_window_counter.lua`. Abgeschlossene Läufe je (Identität, Retry-Zähler) und DLQ-Kopien je Identität. Für jede Zustellung sagt `predict` in der Reihenfolge der Pipeline (Zustelllimit, Ausführungsschutz, Rate-Limit, Ausführung, Retry-Bewertung) das Verb samt Kopie, Wartezeit, Zähler, Fehlerergebnis und Folgezustand voraus; die Regel vergleicht mit dem, was der Broker sah, und schreibt erst dann das Modell fort. Retry-Zähler streng nach E-051, Broker-Zähler nachsichtig wie das Gate.

### Invarianten mit Quelle

| ID | Invariante | Quelle |
|---|---|---|
| K1 | Genau ein Vertragsverb je Zustellung, kein Verb nach dem Tod des Workers | `docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md` Abschnitt 1 |
| K2 | Verb, Kopie, Zähler und Folgezustand wie vorhergesagt: Zustelllimit vor dem Ausführungsschutz, `done`/`superseded` bestätigt ohne Lauf, `running` zurückgestellt ohne Retry-Verbrauch (Wartezeit 0,8 bis 1,2 × `backend_defer_delay`), Waisengrenze → DLQ, Rate-Limit → Kopie mit `retry_after_ms` | `gates/delivery_count.py`, `gates/execution_guard.py`, E-046 Nachtrag F3-27, E-070 |
| K3 | Hard-Timeout und Kindtod nehmen den Retry-Pfad, nie `redeliver` | E-037 |
| K4 | `Retry(delay)` zählt gegen `max_retries`, negativ → 0 ms, bei erschöpftem Budget DLQ | E-060 |
| K5 | Retry-Zähler streng: `"abc"`, `"3"`, `True`, 2,5, −2 zählen wie keiner; die Retry-Kopie trägt ein `int` | E-051 |
| K6 | `fixed` genau `min(base, retry_max_delay)`; `linear`/`exponential` in `[nominal, min(2 nominal, max)]` oder `[0,6 max, 0,8 max]` | E-023, `backoff.py` |
| K7 | Nach `max_retries` genau eine DLQ-Kopie mit Fehlerergebnis (auch bei scheiterndem DLQ-Publish liegt das Ergebnis); je Identität höchstens eine DLQ-Kopie hinter dem Ausführungsschutz | E-031 |
| K8 | Keine Kopie, die swarmq publiziert, trägt `x-delivery-count`/`x-acquired-count` | E-054 |
| K9 | Replay ist neue Identität und läuft; gleiche Identität ist Duplikat; je (Identität, Retry-Zähler) höchstens ein abgeschlossener Lauf | E-009 |
| K10 | Nach Kanalverlust beim Bestätigen setzt der Ausführungsschutz Done/Superseded; die Neuzustellung des Originals ist ein Duplikat | E-043, E-009, `worker/guarded_broker.py` |

### Erlaubte Abweichungen (KTD3)

| Abweichung | E-ID | Umsetzung in der Maschine |
|---|---|---|
| Recycling verbraucht keinen Retry | E-038 | nicht nachgebildet (Drain ohne Abbruch, die Kette sieht nichts) |
| UI-Replay mit `"3"` bekommt das volle Budget | E-051 | Regel `replay` mit `ui_move_str`, Ablauf `ui_replay_with_string_count_gets_the_full_budget` |
| Gescheiterter Retry-/DLQ-Publish: Original per `redeliver` zurück, erneuter Lauf, Fehlerergebnis liegt schon | E-041 | `copy_failure`; zählt nicht als abgeschlossener Lauf (K9) |
| Nach Absturz läuft dieselbe Ausführung erneut, sobald der Running-Marker abgelaufen ist | E-046 | Waise; Ablauf `crash_defers_the_redelivery_until_the_marker_expires` |
| UI-Move ohne Replay-ID wird als Duplikat verworfen | E-009 (Bewusste Folgen) | Regel `replay` mit `ui_move_without_replay_id` |
| Doppelausführung nach über 65 min Rückstand ohne `max_age` | E-010 | nicht erreichbar: Zustands-TTL 1 d, ein Beispiel dauert höchstens 45 min |

Keine erlaubte Abweichung, sondern Teilaspekt von R4Z-04 (keine E-ID): Eine Zustelllimit-DLQ liegt vor dem Ausführungsschutz und setzt keinen Done-Marker; eine weitere Kopie derselben Identität läuft danach. K7 zählt diese DLQ-Kopien nicht mit; der Ausschluss trägt einen Kommentar mit Verweis auf R4Z-04 (`one_guarded_dead_letter_per_identity`).

### Valkey-Stufe und Serverzeit im Rate-Limit

`RateLimiter.check` liest die Serverzeit (`TIME`) und reicht sie `sliding_window_counter.lua` als `now_ms`; `age_keys` verschiebt nur Fristen. Gelöst ohne Änderung an `src/`: Die Valkey-Stufe gibt dem `ValkeyRateLimitStore` einen Client (`FakeClockTime`), dessen `time()` die `FakeClock` liefert; alle anderen Befehle gehen an den echten Client. Das Skript rechnet Fenster und `retry_after_ms` damit exakt wie Fake und Modell, die Fenster-Keys altern mit `advance`. `test_rate_limit_store_reads_the_fake_clock` belegt, dass nach 120 s Zeitsprung ohne reale Wartezeit der Zähler wieder frei ist.

### Läufe

| Lauf | Ergebnis | Laufzeit |
|---|---|---|
| `uv run --extra test pytest tests/property/stateful/test_retry_chain_machine.py -q -m property` (Profil `quick`, 100 Beispiele, 30 Schritte; 13 Abläufe, Selbsttest, 3 Nachweise) | 15 passed, 3 xfailed | 1,2 bis 2,0 s |
| `uv run just property -k retry_chain` | 15 passed, 3 xfailed | 1,2 s |
| `uv run just property` (ganze Suite, mit U7 und U8) | 88 passed, 6 xfailed | 18,9 s |
| Fake, Profil `thorough` (10 000 Beispiele), manuell | 1 passed | 151 s |
| Suchlauf Fake, 3 000 Beispiele mit 60 Schritten (Scratch-Skript) | keine Verletzung | 79 s |
| Suchlauf Fake, 10 000 Beispiele mit 100 Schritten (Scratch-Skript) | keine Verletzung | 328 s |
| `uv run just integration -k retry_chain_machine_valkey` (8 Beispiele, dieselben 13 Abläufe, Selbsttest, Rate-Limit-Uhr, Isolation, 3 Nachweise) | 17 passed, 3 xfailed | 3,2 bis 4,8 s |
| Valkey mit 30 Hypothesis-Seeds nacheinander (`--hypothesis-seed=1..30`, über das Rezept) | 30 von 30 grün | je 2,6 bis 3,3 s |
| ruff format, ruff check, mypy (eigene Dateien und `mypy src/swarmq tests`), basedpyright (Projektkonfiguration) | grün | |

Tiefe im Profil `quick` (Nachbesserung nach Review, Zählung mit Scratch-Skript, gleiche Einstellungen: 100 Beispiele, 30 Schritte): 845 Zustellungen (rund 8,5 je Beispiel), davon Waisengrenze → DLQ 29, Zustelllimit → DLQ 3, Rate-Limit-Rückstellung 37, Zurückstellung wegen fremdem Running-Marker 253, Budget erschöpft 34, Kanalverlust beim Bestätigen 57. Mit 1 000 Beispielen: 12 616 Zustellungen, Waisengrenze 509, Zustelllimit 72, Rate-Limit 277. Nach der Nachbesserung zusätzlich: Suchlauf 3 000 Beispiele mit 60 Schritten ohne Verletzung (71 s), Valkey mit 10 Seeds 10 von 10 grün (je rund 2,4 s). Vorher (eine Zustellung je Regel, Nebenregeln unbegrenzt) erreichte `quick` rund 3 Zustellungen je Beispiel, die Waisengrenze nie und das Zustelllimit 9-mal in 1 000 Beispielen.

Abdeckung im 10 000er-Suchlauf (vor der Nachbesserung): 82 246 Zustellungen; alle Verben und Gründe kommen vor, auch die seltenen (Waisengrenze → DLQ 46-mal, Zustelllimit → DLQ 527-mal, Kanalverlust beim Bestätigen 11 978-mal über alle Verben, Rate-Limit-Rückstellung, Replays).

Selbsttest: `LenientCountModel` liest den Retry-Zähler nachsichtig (`"3"` zählt 3, gegen E-051). Der Ablauf `ui_replay_with_string_count_gets_the_full_budget` wird damit deterministisch rot (beide Stufen, `test_lenient_count_model_is_caught*`); Hypothesis findet den Modellfehler auch selbst (5 000 Beispiele, 6 verschiedene Fehlschläge, 143 s). Ohne die Ausklammerung von R4Z-04 findet Hypothesis R4Z-04 in 22 bis 57 s (5 000 Beispiele, 60 Schritte) und schrumpft auf zehn Schritte (gescheiterte Kopie → `redeliver`, Erfolg, Ack verloren, Duplikate, bis `x-acquired-count` 3).

Isolation: `test_an_example_does_not_see_the_state_of_its_predecessor` fährt eine ganze Kette bis in die DLQ, baut ab und öffnet eine zweite Maschine auf leerer Datenbank: kein Ergebnis, kein Marker, der erste Fehler wird wiederholt.

### Ergebnis

Die Kette hält K1 bis K9 in beiden Stufen; Fake und Valkey verhalten sich in allen Abläufen und Beispielen gleich. Die Abläufe bestätigen: zwei Retry-Kopien und eine DLQ-Kopie bei `max_retries=2` (Zähler 0, 1, 2); kaputte Zähler (`"abc"`, −2, `True`, 2,5) zählen 0 und die Kopie trägt 1 (E-051); `Retry(delay=-5)` gibt 0 ms und zählt (E-060); Hard-Timeout und Kindtod bei `max_retries=0` gehen direkt in die DLQ mit Fehlerergebnis (E-037); `fixed` mit Basis 90 s wartet 60 s (E-023); `dlq retry` läuft, ein zweites Exemplar derselben Wiederholung und ein UI-Move ohne Replay-ID sind Duplikate (E-009); UI-Replay mit `"3"` hat das volle Budget, ein UI-Move mit `int` 3 nicht; nach einem Absturz wird die Neuzustellung zurückgestellt, bis der Running-Marker abläuft, dann läuft sie (E-046), nach drei Waisen DLQ; das Rate-Limit stellt den dritten Lauf je Minute mit `retry_after_ms` zurück (gegen Valkey mit derselben Zahl); eine `redeliver`-Schleife endet über `x-acquired-count` am Zustelllimit (BZ-29).

### Verdachte

**R4Z-04: Am Zustelllimit wird die Neuzustellung einer abgeschlossenen oder überholten Ausführung dead-gelettert statt als Duplikat bestätigt; ein Erfolgsergebnis wird mit einem Fehler überschrieben.**

- Fundstelle: Reihenfolge in `worker/pipeline.py` `_pre_pipeline_gates` (Delivery-Count-Gate) vor `ExecutionGuard.apply` (Gate-Schleife in `_process_message`); `worker/gates/delivery_count.py` `apply` → `RetryHandler.dead_letter` → `store_result` (Fehlerergebnis) vor `dead_letter_copy`.
- Szenario (geschrumpft, deterministisch): Task läuft erfolgreich, das Ack geht verloren (Kanal weg; `GuardedBroker.ack` setzt den Done-Marker). Die Neuzustellungen werden als Duplikat bestätigt, auch deren Ack geht zweimal verloren. Die Neuzustellung mit `x-delivery-count` 3 (Limit 4) erreicht den Ausführungsschutz nicht: `WorkerHungError`-DLQ-Kopie, und `get_result` liefert jetzt „WorkerHungError: delivered 3 times …“ statt des Erfolgs. Zweite Form: Der Versuch scheiterte, die Retry-Kopie liegt (Superseded-Marker), nur das Ack des Originals ging verloren (`SettleFailedAfterPublish`); am Limit geht das Original in die DLQ und schreibt ein Fehlerergebnis, während die Kette über die Retry-Kopie weiterläuft. Ein späteres Replay der DLQ-Kopie (neue Identität) führte den erledigten Task ein zweites Mal aus. Hypothesis fand dieselbe Lage über `x-acquired-count` (gescheiterte Kopie → `redeliver`, dann verlorenes Ack).
- Beleg: `tests/property/stateful/test_retry_chain_machine.py::test_suspect[r4z_04_completed_task_redelivered_at_the_limit_stays_a_duplicate]` und `…[r4z_04_superseded_attempt_redelivered_at_the_limit_stays_a_duplicate]`, gleich in `tests/integration/stateful/test_retry_chain_machine_valkey.py::test_suspect_against_valkey[…]` (alle `xfail(strict=True)`, scheitern heute mit „Erfolgsergebnis überschrieben: 'WorkerHungError …'“ bzw. „überholter Versuch in der DLQ“).
- Erwartung: E-043 („Die Neuzustellung des Originals überspringt der Ausführungsschutz als `done`“), E-009 („Eine normale Neuzustellung derselben Ausführung … wird weiter als Duplikat verworfen“), Docstring von `GuardedBroker.ack` („die Neuzustellung wird uebersprungen statt erneut ausgefuehrt“).
- Entscheidungs-Abgleich: Das Register entscheidet die Gate-Reihenfolge nur für Ausführungsschutz, Expiry, Rate-Limit und Lock (E-070). Ursprung der Reihenfolge ist `docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md:196` (KTD5, Position: „die Erkennung hängender Worker (Delivery-Count-Gate) bleibt davor“); dieselbe KTD verlangt in der Konflikt-Zeile `:207` für Done-Marker und kleineren Retry-Zähler als den Superseded-Marker „bestätigen und überspringen (`duplicate_delivery_skipped`)“. Beides zusammen widerspricht sich am Zustelllimit; keine Regel nimmt die Folge hin.
- Folgen über die DLQ-Kopie hinaus (`RetryHandler.dead_letter`, `worker/retry.py:267-340`): Fehlerergebnis über dem Erfolg, Unique-Freigabe (`maybe_release_unique`) und `TASK_DLQ`. In der überholten Form gibt die Freigabe das Unique-Flag frei, während die Retry-Kopie noch läuft, und ein Workflow-Mitglied setzt seinen Workflow per Fail-Fast (E-040) auf `failed`, obwohl die Kette weiterläuft. In der erledigten Form verhindert `_step_succeeded` (`worker/terminal.py`) das Kippen des Workflows; Fehlerergebnis, DLQ-Kopie und `TASK_DLQ` bleiben.
- Realismus: Erfordert, dass dieselbe Nachricht das Limit erreicht: mit Default 20 rund 19 Rückgaben (Kanalverluste oder auf 4.3 `nack`-Schleifen, BZ-29), etwa während eines längeren Broker-Vorfalls, in dem Publishes scheitern und Kanäle schließen. Mit kleinem `SWARMQ_DELIVERY_LIMIT` bzw. `x-delivery-limit` (2 bis 4) genügen ein bis drei Rückgaben.
- Konfidenz: hoch für den Mechanismus (beide Stufen deterministisch), mittel für die Häufigkeit.
- Einstufungsvorschlag: FEHLER, niedrige Priorität (Realismus gering, siehe oben). Fix-Vorschlag: Das `DeliveryCountGate` liest am Limit vor dem Dead-Lettern den Zustand des Ausführungsschutzes; `done` oder `superseded` → bestätigen (`duplicate_delivery_skipped`), sonst DLQ wie heute.
- Teilaspekt: Die Zustelllimit-DLQ setzt keinen Done-Marker, eine weitere Kopie derselben Identität läuft danach (in K7 ausgenommen, siehe oben).
- Ausklammerung: `RetryChainMachine.deliverable` mit Vorbedingung an `deliver_some` (Kommentar `F4-offen (R4Z-04)`): Zustellungen, für die das Modell den Fall vorhersagt, bleiben liegen. `deliver` meldet ihn sonst als K10.

**R4Z-05: Eine über die UI neu publizierte Retry-Kopie mit Text-Zähler wird still als überholt bestätigt; die Kette endet ohne Lauf, Ergebnis und DLQ-Kopie.**

- Fundstelle: `gates/execution_guard.py` `retry_count_of` (streng, E-051) zusammen mit dem Superseded-Vergleich in `guard_claim.lua` bzw. `InMemoryExecutionGuardStore.claim`.
- Szenario (deterministisch): `max_retries=3`, der erste Versuch scheitert, die Retry-Kopie (Zähler 1) liegt; ein Operator publiziert sie über die RabbitMQ-UI neu, die UI setzt `x-swarmq-retry-count: "1"`. Die Zustellung zählt 0, der Superseded-Marker der Identität steht auf 1, der Ausführungsschutz bestätigt sie als Duplikat (`duplicate_delivery_skipped`, `reason=superseded`). Kein Lauf, kein Ergebnis, `get_result` läuft in den Timeout.
- Beleg: `test_suspect[r4z_05_republished_retry_copy_with_text_count_still_runs]` in beiden Stufen (`xfail(strict=True)`, „UI-Republish der Retry-Kopie still als superseded bestätigt, Kette beendet“).
- Erwartung als Frage: Plan U9 erwartet („Header auf `"abc"` gesetzt: nächste Zustellung zählt als 0, Kette beginnt neu“), dass die Kopie läuft. E-051 legt den strengen Vergleich aber ausdrücklich fest und nennt als Folge nur das volle Budget beim UI-Replay aus der DLQ (dort neue Identität über die Replay-ID). Soll eine Kopie ohne neue Identität laufen, oder genügt es, die Folge festzuhalten?
- Einstufungsvorschlag: ENTSCHEIDUNG; höchstens ein Nachtrag zu E-051 („eine per UI mit Text-Zähler neu publizierte Retry-Kopie gilt als überholt: kein Lauf, kein Ergebnis, keine DLQ-Kopie“). Der Nachweis prüft die Plan-Erwartung über `machine.runs` und `get_result("t1")`.
- Realismus: niedrig (manuelles UI-Publish einer Kopie, die nicht aus der DLQ kommt).
- Konfidenz: hoch für den Mechanismus. Keine Ausklammerung nötig: das Modell folgt hier dem Code.

Hinweise ohne Verdacht (nur Code-Lektüre, nicht gelaufen):

- `InMemoryBroker.dead_letter` stempelt keine Generation (`x-swarmq-replay-id`, `x-swarmq-dlq-gen`, `x-swarmq-dead-lettered-at`); ein UI-Move einer DLQ-Kopie ist im Fake deshalb ein Duplikat, gegen RabbitMQ läuft er (BZ-08). Die Maschine schließt die Lücke in `ChainBroker`; andere Tests mit dem Fake sehen sie weiter.
- `InMemoryRateLimitStore.check` rechnet `remaining` als `max - int(estimated) - 1`, `sliding_window_counter.lua` als `floor(max - estimated - 1)`; bei nicht ganzzahliger Schätzung liegt der Fake um 1 höher. Das Gate liest `remaining` nur für Log und Signal, wenn gedrosselt (dann beide 0); keine Wirkung, deshalb kein Verdacht.

### Abweichungen vom Plan

- Subjekt ist `Worker._handle_message` statt eines einzeln verdrahteten `RetryHandler.evaluate`: nur so laufen Gate-Reihenfolge, `GuardedBroker` und Ergebnis-Tail echt (R4Z-04 entsteht genau aus der Reihenfolge). Die Maschine setzt dafür die privaten `_rate_limiter`, `_lock_manager`, `_guard_store`, `_gate_deps.redeliver_pause` und beim CPU-Worker `_executor`, wie die Unit-Tests (`test_befund_fs19_counter_headers.py`, `test_pipeline_execution_guard.py`).
- Der Plan nennt nur die Testdateien; Maschine und Abläufe liegen wie bei U6 bis U8 in `retry_chain_machine.py` und `retry_chain_scenarios.py`.
- Testszenario „Header `"abc"` → Kette beginnt neu“: gilt nur für eine Zustellung ohne Superseded-Marker; für eine Retry-Kopie gilt R4Z-05.
- Zusätzlich zum Plan: Absturz eines Workers, Kanalverlust beim Bestätigen, gescheiterte Kopie, doppelte Kopie, Rate-Limit-Rückstellung, Zustelllimit; ohne sie wären K10, E-041, E-043 und E-046 nicht erreichbar.
- Ausklammerung von R4Z-04 an `deliver_some` über eine gefilterte Auswahl (`deliverable`) plus `@precondition` statt einer Vorbedingung allein, weil die auslösende Zustellung ein gezogener Datenwert der Regel ist; kein `assume()`.
- Die Valkey-Stufe ersetzt für das Rate-Limit die Serverzeit durch die `FakeClock` (siehe oben); Guard-Marker und Fenster-Keys altern über `age_keys`.
- Reale Wartezeit nur für Soft- und Hard-Timeout (je 5 ms); die Pause vor `redeliver` (BZ-29) ist abgeschaltet.
- Nicht nachgebildet: E-010 (Zustands-TTL 1 d), Recycling (E-038), Drain-Abbruch (E-001), Locks, `max_age`, Workflows, Unique-Flags, Backend-Ausfall, `PublishIndeterminate` als Ausnahme (nur die doppelte Kopie als Folge).
- Der Selbsttest mit Modellfehler bleibt als deterministischer Test im Repo.
- Keine `entscheidung`-Marker: jeder Ablauf läuft durch die ganze Maschine samt Invarianten.
- Ohne `-m property` wählt `uv run --extra test pytest tests/property/stateful/test_retry_chain_machine.py -q` nichts aus (`addopts`, Exit 5); die Läufe oben tragen `-m property` oder laufen über `uv run just property`.

### Folgeaufgaben

- Generationsstempel beim Dead-Letter (`x-swarmq-replay-id`, `x-swarmq-dlq-gen`, `x-swarmq-dead-lettered-at`) in `tests/support/inmemory/broker.py` nachziehen statt nur in `ChainBroker`; sonst behandeln andere Tests mit dem Fake einen UI-Move als Duplikat.
- `remaining` des `InMemoryRateLimitStore` an `sliding_window_counter.lua` angleichen (`floor(max - estimated - 1)`).
- Nach dem Fix von R4Z-04 die Ausklammerung in `deliverable` und den K7-Ausschluss für Zustelllimit-DLQs entfernen; die xfail-Gründe `R4Z-nn` auf die F4-IDs umstellen.
