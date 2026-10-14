# Gegenprüfung C – FS-12 bis FS-16, FS-26 bis FS-29

Worktree `swarmq-fehlersuche` @ ddeca96. Nur gelesen. Alle fünf P2-Nachweise mit `--runxfail -p no:randomly` ausgeführt: 12 Fälle, alle rot, und zwar jeweils an der Kern-Assertion. Die Vorbedingungen (Ack/DLQ, Kind tot, Opt-out beim Client wirksam usw.) halten.

---

## FS-12 – Pre-Start-`cancel(task_id)` eines Workflow-Schritts lässt den Workflow `running`

**Einstufung: FEHLER** (Konfidenz hoch). Prio P2 bleibt, der Auslöser ist aber selten (siehe unten).

**Entscheidungs-Abgleich: keine Entscheidung gefunden.**
- `docs/brainstorms/2026-05-30-workflow-primitives-requirements.md:188-196` (R16) fordert, dass ein Workflow nie still in den `get()`-Timeout läuft: „das endgültige Scheitern eines Schritts MUSS synchron … den Workflow-State als failed markieren“. Ein Cancel wird dort nicht eigens genannt.
- Commit `17dac64` (U6, „fail-fast + liveness“) hat den Cancel ausdrücklich einbezogen: „the success seam's is_error/is_cancelled branch … `reason = "task was cancelled" if is_cancelled`“. Heute steht das in `src/swarmq/worker/results.py:205-213`.
- BZ-13 (`docs/befunde/betrieb-ueber-zeit-2026-10.md:32`, Fix `a570112`) hat jeden Nicht-Retry-Pfad einzeln aufgezählt: route_via_dlx, Lock-Render, Expiry, Lock-Wartezeit. Der Pre-Start-Cancel-Pfad im Cancel-Gate fehlt in der Liste (`tests/unit/worker/test_befund_bz13_workflow_hang.py:12-17`). Laut Register brauchte BZ-13 keine Produktentscheidung (`betrieb-ueber-zeit…:120`).
- Die Tabelle *Failure modes* in `website/guides/workflows.md` nennt den Fall „einzelner Schritt per `cancel(task_id)`“ nicht. `website/reference/api.md:256-276` (`cancel()`) sagt nichts zu Workflows.
- Kein Bestandstest schreibt das heutige Verhalten fest.

**Quelle der Erwartung:** Im Nachbarpfad setzt swarmq dieselbe Invariante selbst durch (Cancel während des Laufs → `fail(..., "task was cancelled")`, `17dac64`). Dazu kommt R16. Der Nachweis erwartet genau das Verhalten dieses Nachbarpfads.

**Realismus:**
- (a) Der Test ruft den echten `Worker._handle_message` auf, nur Advancer und Backend sind gemockt. Der Advancer wird über `RetryHandler.fail_workflow`/`deps.fail_workflow` erreicht, so wie das Expiry-Gate es tut. Das ist realistisch.
- (b) Der Auslöser setzt voraus, dass jemand einen einzelnen Schritt abbricht. `children()` liefert die `task_id`s, `cancel(task_id)` ist öffentlich. Möglich ist das, häufig vermutlich nicht. Die Folge ist dafür schwer: Der Workflow hängt 24 h, ein Chord-Body läuft nie.
- (c) P2 ist vertretbar, denn BZ-13 hatte dieselbe Fehlerklasse auf P2.

**Nebenbefund (nicht im Register, Variante aus MA-1):** Nach `handle.cancel()` bleibt `status()` auf `"running"`, wenn kein Schritt mehr fertig wird. `get()` ist dagegen korrekt. `workflow_orchestration.py:172-178` prüft bewusst nur das Flag und schreibt den Zustand nicht („avoids a write race with a concurrent advance()“). Das ist eine **Entscheidung** für `get()`. Für `status()` widerspricht sie `website/guides/workflows.md:178` (`"cancelled"` als möglicher Wert). Vorschlag: als eigener P3-Punkt aufnehmen (LÜCKE).

**Test-Anpassung:** keine.

---

## FS-13 – `unique_until="start"`: terminales Ende vor dem Start gibt das Flag nicht frei

**Einstufung: FEHLER** (Konfidenz hoch). Prio P2 bleibt.

**Entscheidungs-Abgleich:**
- Bewusst entschieden ist die **Lage** der Freigabe. Commit `7ac9fd6`, P1 #4/ADV-002: „start-mode release AFTER rate-limit + lock checks … keeps the flag claimed across requeue cycles; release fires only when the task is actually about to execute“. Der Docstring in `src/swarmq/worker/pipeline.py:355-362` sagt dasselbe.
- Ebenso bewusst ist der No-op an terminalen Pfaden (`finalize.py:122-126`, „Release already happened at pickup (Unit 10)“). Grundlage sind Plan `docs/plans/2026-04-17-004-…-unique-tasks-plan.md:262-272` („terminal paths: NO additional release (already done at pickup)“) und die Anforderungen `docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md:114-118, 266-271` („Für start … ist der Flag beim Cancel-Zeitpunkt bereits released“).
- Die Prämisse dieses No-ops stimmt nicht. Schon in `34f191e` lief `_pre_start_cancel_check` vor der start-Freigabe (`worker.py:1079` gegenüber `:1121` in diesem Stand). Mit `7ac9fd6` liegen auch Lock-Wartezeit und Rate-Limit davor. Das Zusammenspiel „Freigabe später, terminaler No-op unverändert“ hat niemand entschieden, es ist eine Nebenwirkung.
- BZ-22 (Fix `d4e2b81`) hat nur den completion-Modus geheilt.

**Quelle der Erwartung:**
- `website/guides/locking-and-dedup.md:215-217`: „`"start"` — the flag releases the instant a worker picks up the message“. Eine Nachricht, die im Expiry-, Cancel- oder Delivery-Limit-Gate endet, wurde abgeholt, das Flag bleibt trotzdem stehen.
- Dazu kommt die Invariante aus BZ-22: Ein terminaler Pfad lässt kein verwaistes Flag zurück.

**Realismus:**
- (a) Der Test nutzt den echten Worker-Pfad. Der Header `x-swarmq-unique-until: start` wird vom Client wirklich gesetzt (`client/schedule.py:242, 510`). Alle fünf Fälle sind echte terminale Pfade.
- (b) Betroffen sind nur Tasks mit dem Opt-in `start`, und das auch nur auf seltenen Pfaden: `max_age`, Pre-Start-Cancel, Lock-Wartezeit-Ende, Delivery-Limit, unbekannter Task. Die Folge ist ein stilles Verschlucken bis `unique_for`. Bei kurzen TTLs, für die die Doku `start` empfiehlt, ist das Fenster klein.
- (c) P2 ist konsistent mit BZ-22 (P2).

**Test-Anpassung:** keine. Ein Fix muss `test_worker.py:2509-2516` („Only one release call in start-mode“) einhalten. Das geht mit der in MA-4 vorgeschlagenen Merkerlösung.

---

## FS-14 – `schedule(delay=/eta=, unique_for=None)` wird beim Fire trotzdem dedupliziert

**Einstufung: FEHLER** für den ausdrücklichen Opt-out (Konfidenz hoch). Prio P2 bleibt.

**Entscheidungs-Abgleich:**
- Entschieden und dokumentiert ist, dass der Leader bei Definitionen **ohne** Unique-Angabe auf seine eigene `TaskInfo` zurückfällt:
  - Plan `2026-04-17-004` Unit 9: „Scheduler-seitige Dedup für Cron/Delay/ETA-Fires“, Skizze `else: # Static cron: resolve TaskInfo and SETNX`.
  - Code-Kommentar `scheduler/fire.py:141` „Static cron or plain CLI — resolve TaskInfo and SETNX here“.
  - BZ-09/`b21cbf0`, dokumentiert in `website/guides/scheduling-and-retries.md:223-226`: „a definition without them (written before 0.12, or a CLI schedule) uses the leader's own `TaskInfo`“.
  - Anforderung `phase3-5…requirements.md:107-109`: „`unique_for` greift auch bei Cron-/Delay-/ETA-Fires“.
- Der Rückfall für `cli`-Definitionen ist also ENTSCHEIDUNG. Das ist Produktfrage 3 im Register, dort zu Recht offen gelassen.
- Ebenso ausdrücklich zugesagt ist der Opt-out je Aufruf:
  - `website/reference/api.md:122`: „Pass `None` to disable dedup for this call.“
  - `website/guides/locking-and-dedup.md:244-256`.
  - Anforderung `phase3-5…requirements.md:73-74`: „`unique_for=None` deaktiviert Dedup für diesen Call (auch wenn TaskInfo einen Default setzt)“.
  - `_resolve_unique` in `client/schedule_resolve.py:120-124`.
- Der Client speichert den Opt-out nicht (`client/schedule.py:621`, `source="cli"`, `unique_for` fehlt). Damit hebelt die Rückfallregel den Opt-out aus. Dass der Opt-out im verzögerten Pfad verloren geht, hat niemand entschieden.

**Quelle der Erwartung:** die Zusage in `api.md:122` und R3.5. Ergänzend für die Folge „verworfen ohne Ergebnis“: BZ-02 (`betrieb-ueber-zeit…:16`, `get_result(X)` darf nicht in den Timeout laufen) und die Invariante `_store_unroutable_result` im Nachbarpfad. Der Nachweis prüft den zweiten Teil nicht.

**Realismus:**
- (a) Die Definition entsteht im Test über den echten `Client.schedule(..., delay=60, unique_for=None)`, nur der `ScheduleStore` ist gemockt. Gefeuert wird über den echten `_fire_one`. Das ist realistisch, Voraussetzung ist ein Leader mit dem Task in der Registry (Normalfall).
- (b) Der Opt-out dient genau dazu, trotz laufender oder wartender Duplikate einen zweiten Lauf zu erzwingen. Gerade dann hält ein anderer Lauf das Flag, und der erzwungene, verzögerte Lauf verschwindet still.
- (c) P2 passt.

**Test-Anpassung:** keine. Hinweis für den Fix: Weil der Rückfall auf die Leader-`TaskInfo` dokumentiert ist, muss der Opt-out in der Definition persistiert werden, etwa als eigenes Feld oder Sentinel. Den Rückfall einfach zu entfernen geht nicht.

---

## FS-15 – `get_result()`: Pub/Sub nur 1 s, danach GET alle 5 ms; `polling` ignoriert; `subscribe()` ohne Frist

**Einstufung: ENTSCHEIDUNG** für Teil 1 und 2. Der Teilaspekt `subscribe()` ohne Frist ist ein **FEHLER P3** und gehört herausgelöst.

**Entscheidungs-Abgleich:**
- `docs/brainstorms/performance-optimization-requirements.md:25`, R3: „Falls nach 1 Sekunde keine Notification eintrifft, fällt der Client automatisch auf Polling (5ms-Intervall) zurück.“
- `docs/plans/2026-04-15-005-feat-pubsub-batch-performance-plan.md:27` (R3 „1s Fallback-Timeout → automatisches Polling (5ms Intervall)“), `:28` (R4 „API bleibt signatur-kompatibel“), `:255-256` („Fallback auf Polling (5ms Intervall) … `get_result(task_id, timeout=10, polling=0.2)`“), `:72` (5 ms auch für `get_results`).
- Commit `c18a144` hat `await asyncio.sleep(polling)` durch `_FALLBACK_POLL = 0.005` ersetzt. `polling` wird seitdem bewusst ignoriert.
- Nutzer-Doku `website/reference/api.md:227-229`: „push-based wait, with 5ms-interval polling as a fallback“.
- Zur Last steht in den Quellen nur „N Pub/Sub-Connections“ als Known Limitation (Plan `:76`, `:386`). Die GET-Last bei langen Wartezeiten wurde nicht betrachtet.

**Quelle der Erwartung:** Für ≤ 1 GET/0,1 s und für ein wirksames `polling` gibt es keine. Beide Nachweise prüfen also eine neue Erwartung gegen eine dokumentierte Entscheidung. Für die Frist von `subscribe()` gibt es eine Quelle: Der Docstring `backend/valkey/results.py:155-158` verspricht, dass „ein hängendes Valkey den Aufrufer nicht über `timeout` hinaus blockiert“.

**Neues Argument für „Entscheidung überdenken?“:** Die Entscheidung versteht Polling als *Rückfall bei verpasster Benachrichtigung*. Tatsächlich wird nach 1 s aber Pub/Sub gar nicht mehr gelesen, obwohl das Abonnement gesund bleibt. Für jeden Task, der länger als 1 s läuft, ist das 5-ms-Polling damit der Normalpfad (ca. 200 GET/s je Wartendem) und kein Rückfall mehr. Der Benchmark der Entscheidung (P50 12,5 ms) betraf nur kurze Tasks.

**Realismus:**
- (a) Fake-PubSub und virtuelle Uhr bilden die Zeitlogik korrekt ab.
- `subscribe()`: valkey-py `PubSub.execute_command` → `connect()` → `get_connection` mit `Retry(retries=5)` und `socket_connect_timeout=5` (`connection.py:221-227`). Bei unerreichbarem Valkey sind das real etwa 30 s und mehr über `timeout`. Bei einem Valkey, der die Verbindung annimmt, aber hängt, kommen zusätzlich `socket_timeout` (10 s) mal Retries hinzu. Der Test hängt unbegrenzt und übertreibt damit. Die Überschreitung selbst ist aber real.
- (b) Der Auslöser ist nur ein Valkey-Ausfall während `get_result`.

**Prio:** P2 → **P3**. Polling als Produktfrage oder Entscheidungsrevision, `subscribe`-Frist als P3-Fehler.

**Test-Anpassung:**
- `test_waiting_ten_seconds_polls_at_most_every_100ms` und `test_client_get_result_honours_the_polling_interval` aus dem Befund nehmen oder als Produktfrage kennzeichnen. Sie widersprechen `api.md:228` und R3/R4.
- `test_hanging_subscribe_does_not_outlast_the_timeout` bleibt. Realistischer wäre `subscribe` mit endlicher Verzögerung, etwa 30 s virtuell.

---

## FS-16 – CPU-Modus: Prozessgruppe bekommt kein SIGKILL, wenn das Kind schon tot ist

**Einstufung: FEHLER** (Konfidenz hoch für den Mechanismus). Prio P2 bleibt, eher am unteren Rand.

**Entscheidungs-Abgleich:**
- `docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:92` (R8): „Der Kill trifft das Rechenkind samt allen von ihm gestarteten Prozessen … nach dem Kill lebt kein Nachkomme des Kindes mehr.“
- `:93` (R9, Kindtod/OOM): „**Der Loop-Prozess beendet verbliebene Nachkommen des toten Kindes.**“
- `:184` (AE13): „kein Prozess aus der Prozessgruppe des Kindes lebt weiter“.
- `:259` (KTD3): „SIGTERM an die Gruppe, bis zu einer Sekunde Gnade …, dann SIGKILL“.
- `website/guides/cpu-mode.md:19`: „kills the child's process group (SIGTERM, 1 s grace, SIGKILL)“.
- Einzige Einschränkung ist KTD3: „nach einem SIGKILL des **Loop-Prozesses** sterben [Nachkommen] nur nach bestem Bemühen“. Das betrifft den Tod des Loop-Prozesses, nicht diesen Fall.
- `pool.py:11-13` kennt überlebende Enkel, behandelt aber nur deren offenes Kanal-fd (Gnadenfrist). Dass Enkel weiterlaufen dürfen, steht nirgends.
- `_kill_child` mit der Frühausstiegs-Logik stammt seit `ec52c74` unverändert aus U5. Eine Begründung gibt es nicht.

**Quelle der Erwartung:** R8, R9, AE13 und die Nutzer-Doku. Fall b (Kindtod) ist durch R9 ausdrücklich abgedeckt. Das korrigiert die Annahme in MB-1, nur KTD3/AE13 seien betroffen.

**Realismus:**
- (a) Der Test nutzt den echten `SlotPool` mit Fork, nur das Kindziel ist ersetzt. Das reale `child_once` bricht auf SIGTERM per `loop.add_signal_handler(SIGTERM, task.cancel)` ab (`child.py:230`). Wartet der Task gerade (`await proc.wait()`), endet das Kind in Millisekunden. Fall a bildet das echte Verhalten ab.
- (b) Fall a braucht einen Enkel, der SIGTERM ignoriert oder langsam behandelt. Selten, denn ImageMagick (das AE13-Beispiel) stirbt an SIGTERM. Fall b (Segfault oder `os._exit` des Python-Kinds bei laufendem Hilfsprozess) ist plausibel. Den OOM-Killer trifft im Bildfall eher der größere Enkel als das Kind.
- (c) P2 vertretbar wegen der ausdrücklichen Zusage und der unbegrenzten Laufzeit (jeder Retry startet einen weiteren Enkel). P3 wäre bei strenger Häufigkeitsbewertung ebenfalls vertretbar.

**Test-Anpassung:** keine. Das Register sollte R9 als Quelle für Fall b nachtragen.

---

## FS-26 (P3) – DI-Fehler im Kind erscheint nur als „exited without result“

**Einstufung: LÜCKE** (Konfidenz mittel). Prio P3 bleibt.

**Abgleich:**
- R26 (`cpu-plan:98`) verlangt den Scope-Abbau mit der Exception, bevor das Kind das Ergebnis meldet. Das passiert. Einen scheiternden Finalizer behandelt R26 nicht.
- KTD6 (`:262`) verlangt, dass das Kind `_teardown_di_scope` spiegelt. Im IO-Pfad propagiert ein Finalizer-Fehler ebenfalls (`pipeline.py:318-332`, siehe FS-04). Ein Referenzverhalten, an dem man sich messen könnte, gibt es also nicht.
- Keine Entscheidung gefunden.

**Sachliche Korrektur an MB-7:** „Scheitert der REQUEST-Provider“ stimmt so nicht. `dishka.AsyncContainer.__aenter__` gibt nur `self` zurück (`.venv/.../dishka/async_container.py:306-307`). Provider werden erst in `run()` über `wrap_injection` aufgelöst, liegen also im `try` und werden zum normalen Fehler-Envelope. Betroffen ist nur `__aexit__`, also ein scheiternder Finalizer bzw. `ExitError`.

**Test:** keiner vorhanden. Ein möglicher Nachweis ist wie in MB-7 beschrieben, aber nur für `__aexit__`.

---

## FS-27 (P3) – `validate_namespace("SQ")` schlägt `'sq'` vor

**Einstufung: FEHLER** (Konfidenz hoch). Prio P3 bleibt.

**Abgleich:** `keyspace.py:36-47` (Review MR 15, `a3c728d`) schließt `sq` ausdrücklich aus: „``sq`` ist ausgeschlossen, weil ``sq:sq:...`` wie ein Key ohne Namensraum aussähe“. Dieselbe Meldung empfiehlt dann `'sq'`. Direkt ausgeführt ergibt sich `…not 'sq'; write it in lowercase: 'sq'`. Die Meldung widerspricht sich selbst. Eine Entscheidung dafür gibt es nicht. `tests/unit/test_namespace_lowercase.py:33` prüft nur `App1`.

**Test:** keiner vorhanden. Ein Nachweis wäre trivial (`SQ` ohne Lowercase-Hinweis).

---

## FS-28 (P3) – AT_LEAST_ONCE-Schritt mit `result_ttl=0` meldet „is FIRE_AND_FORGET“

**Einstufung: FEHLER**, nur der Meldungstext (Konfidenz hoch). Prio P3 bleibt.

**Abgleich:** Das **Verhalten** ist entschieden und dokumentiert. `website/guides/workflows.md` (Failure modes): „A step built with `sig()` turns out to be `FIRE_AND_FORGET` (or stores no result, `result_ttl=0`) | … the workflow is `failed` (recorded error type `WorkflowValidationError`)“. Dazu der Docstring `results.py:114-119` (R4.1 U5). Der Text (`results.py:131`) nennt aber eine falsche Ursache, obwohl die Doku beide Ursachen unterscheidet. `TaskInfo(result_ttl=0)` mit AT_LEAST_ONCE ist zulässig (geprüft). Den Vorschlag „`result_ttl=0` bei `Task.s()` ablehnen“ deckt die Doku nicht ab, er ist eine Produktfrage.

**Test:** keiner vorhanden.

---

## FS-29 (P3) – Signal-Handler bleiben nach graceful Shutdown installiert

**Einstufung: LÜCKE** (Konfidenz mittel). Prio P3 bleibt.

**Abgleich:**
- Commit `3393b3c` („cancel-safe start()“) hat das Entfernen der Handler nur für den Abbruchpfad eingeführt. Der Grund war ein Leck im Abbruchpfad, der graceful Pfad wurde nicht betrachtet. Eine Entscheidung gibt es nicht.
- `website/reference/api.md:414-416`: „`start_worker()` … Returns on shutdown (SIGTERM/SIGINT) … rarely invoked directly from application code“. Eine Zusage, dass danach die vorherigen Handler gelten, gibt es nicht.

**Realismus:** `_UnixSelectorEventLoop.close()` entfernt alle Signal-Handler. Betroffen ist also nur Code, der nach `start_worker()` im **selben** Loop weiterläuft (Burst-Modus eingebettet). Dort ist SIGTERM/SIGINT danach ein No-op (`_request_shutdown` bei STOPPED). Das ist selten, aber real.

**Test:** keiner vorhanden.

---

## Übersicht

| ID | Einstufung | Prio alt→neu | Kernbeleg |
|---|---|---|---|
| FS-12 | FEHLER | P2→P2 | `17dac64` / `results.py:205-213`: Cancel während des Laufs markiert den Workflow als fehlgeschlagen; R16; BZ-13-Liste ohne Cancel-Gate |
| FS-13 | FEHLER | P2→P2 | `locking-and-dedup.md:215` „releases the instant a worker picks up“; No-op-Prämisse falsch seit `34f191e`/`7ac9fd6` |
| FS-14 | FEHLER (nur Opt-out) | P2→P2 | `api.md:122` „Pass None to disable dedup for this call“; Rückfall auf Leader-TaskInfo ist Entscheidung (`scheduling-and-retries.md:223-226`) |
| FS-15 | ENTSCHEIDUNG (+ FEHLER P3 für `subscribe`-Frist) | P2→P3 | Plan `2026-04-15-005` R3/R4, `c18a144`, `api.md:228` „5ms-interval polling as a fallback“ |
| FS-16 | FEHLER | P2→P2 | CPU-Plan R8/R9/AE13, `cpu-mode.md:19` |
| FS-26 | LÜCKE | P3→P3 | R26/KTD6 regeln Finalizer-Fehler nicht; Provider-Teil von MB-7 sachlich falsch |
| FS-27 | FEHLER | P3→P3 | `keyspace.py:36-47` schließt `sq` aus und schlägt es vor |
| FS-28 | FEHLER (Text) | P3→P3 | `workflows.md` Failure modes unterscheidet FAF und `result_ttl=0` |
| FS-29 | LÜCKE | P3→P3 | `3393b3c` entfernt Handler nur im Abbruchpfad |

Empfohlene Test-Anpassungen:
- FS-15: zwei der drei Nachweise aus dem Befund nehmen bzw. als Produktfrage führen.
- Register: R9 als Quelle für FS-16 Fall b nachtragen.
- FS-12: Nebenbefund „`status()` bleibt `running` nach `handle.cancel()`“ als P3-LÜCKE aufnehmen.

## Entscheidungsquellen außerhalb von docs/befunde (für ein Entscheidungsregister)

- `docs/brainstorms/performance-optimization-requirements.md:25-26` (R3/R4): 1 s Pub/Sub, dann 5-ms-Polling; Signatur kompatibel.
- `docs/plans/2026-04-15-005-feat-pubsub-batch-performance-plan.md:27-28, 68-76, 255-256, 386`: dieselbe Entscheidung, 5 ms auch für `get_results`, N Pub/Sub-Connections als Known Limitation.
- Commit `c18a144`: Parameter `polling` wird ignoriert (`_FALLBACK_POLL = 0.005`).
- `website/reference/api.md:227-229`: „5ms-interval polling as a fallback“ (Nutzerzusage).
- Commit `7ac9fd6` (R3.5 ce-review P1 #4/ADV-002): start-Freigabe nach Rate-Limit/Lock, Flag überlebt Requeues. Ebenda P1 #3: Unique-Freigabe nach dem DLQ-Ack. P1 #5: `unique_until` wird in der Definition persistiert.
- `docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md:73-74` (`unique_for=None` schaltet ab), `:107-118` (Scheduler-Dedup, Cancel ohne Dedup-Interaktion im start-Modus), `:210-218` (Default `completion`), `:249-255` (`unique_for=None` ignoriert `unique_until`), `:290-294` (start: Retry-Fenster ohne Dedup, dokumentierte Einschränkung).
- `docs/plans/2026-04-17-004-feat-phase3-5-unique-tasks-plan.md`, Unit 9 (Leader-SETNX für Nicht-`cli_unique`) und Unit 10 (start: terminal keine Freigabe).
- `website/guides/scheduling-and-retries.md:223-226`: Definition ohne Unique-Angabe (vor 0.12 oder CLI-Schedule) nutzt die `TaskInfo` des Leaders.
- Commit `17dac64` (U6): Ein abgebrochener Workflow-Schritt markiert den Workflow als fehlgeschlagen („task was cancelled“).
- `src/swarmq/workflow_orchestration.py:172-178`: `get()` prüft beim Workflow-Cancel nur das Flag, schreibt bewusst keinen Zustand (Write-Race mit `advance()`).
- `docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md`: R8, R9 (Nachkommen nach Kindtod beenden), R26, KTD3 (Nachkommen nach SIGKILL des Loop-Prozesses nur best effort), KTD6, AE13, AE14.
- `src/swarmq/worker/compute/pool.py:11-13`: überlebender Enkel mit offenem Kanal-fd wird per Gnadenfrist behandelt.
- Commit `3393b3c`: Teardown im Abbruchpfad (Handler entfernen) und bewusst kein Schließen von Broker/Backend.
- `src/swarmq/keyspace.py:24-28, 36-38` (Review MR 15, `a3c728d`): nur Kleinschreibung; `sq` ausgeschlossen.
- `website/guides/workflows.md` (Failure modes): FAF bzw. `result_ttl=0` als Schritt lässt den Workflow zur Laufzeit scheitern.
