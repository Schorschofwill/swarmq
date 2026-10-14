# Fehlersuche 4: Überlebende Mutanten, Gruppe A (Worker-Kern und Gates)

Stand: Worktree `swarmq-fehlersuche` (v0.12.0). Grundlage: `mut-gruppe-A.txt`, 703 überlebende Mutanten
(pipeline, retry, results, terminal, finalize, execution, decode, lock_keeper, gates/*).
Nachweise: `test_mut_a_nachweise.py` im selben Ordner. Jeder Test prüft das **korrekte** Verhalten,
ein roter Test bestätigt den Verdacht. Lauf aus dem Worktree:

```
PYTHONPATH=. uv run --extra test --extra reload pytest -p no:randomly --rootdir . -c pyproject.toml \
  <scratchpad>/fs/test_mut_a_nachweise.py
```

Ergebnis: alle 9 Nachweistests rot (MA-1: 1, MA-2: 2, MA-3: 2, MA-4: 2, MA-6: 2).
Keiner der Befunde steht in `betrieb-ueber-zeit-2026-10.md`, `1-regressionen.md`, `2-absturz.md` oder `3-nebenlaeufigkeit.md`.

---

## Teil 1: Fehlerverdachte

### MA-2 (P2, im Normalbetrieb häufig): Eine eigene `TimeoutError` des Tasks wird zu „SoftTimeoutError: soft timeout after Nones“ und ignoriert `retry_exceptions`

- **Fundstelle:** `src/swarmq/worker/execution.py:185-187` (`_await_run_result`), gleiches Muster in `src/swarmq/worker/compute/child.py:194-197` (CPU-Modus).
- **Mutant:** `execution.x.TaskExecutor._await_run_result__mutmut_5` (`SoftTimeoutError(None)`): der Fehlertext wird nirgends geprüft. Beim Lesen des Zweigs fiel auf, dass er zu weit greift.
- **Szenario:** `except TimeoutError` soll nur den Soft-Timeout aus `asyncio.wait_for` fangen. Er fängt aber jede `TimeoutError` aus `run()`: `asyncio.wait_for` im Task-Code, `asyncio.timeout`, `socket.timeout`, aiohttp-Timeouts (Unterklassen von `TimeoutError`). Das gilt auch, wenn kein `soft_timeout` gesetzt ist. Folgen:
  1. Fehlertext und `error_class` sind falsch: `"SoftTimeoutError: soft timeout after Nones"` statt der Originalmeldung, `swarmq.exceptions.SoftTimeoutError` statt `builtins.TimeoutError`.
  2. **Retry-Filter:** `task_exception` ist jetzt `SoftTimeoutError`, eine Unterklasse von `SwarmQError` und nicht von `TimeoutError`. Ein Task mit `retry_exceptions=[TimeoutError]` (oder `[aiohttp.ServerTimeoutError]`) wird beim ersten Upstream-Timeout **ohne Retry in die DLQ** geschickt. Ein Workflow-Schritt markiert dabei den ganzen Workflow als fehlgeschlagen. Genau diese Klasse listet man typischerweise in `retry_exceptions`.
- **Beleg:** `test_ma2_task_timeout_error_is_retried_per_retry_exceptions` ist rot: `dead_letter` 1×, `retry_later` 0×. `test_ma2_task_timeout_error_keeps_its_message` ist rot: gespeichert wird `'SoftTimeoutError: soft timeout after Nones'`.
- **Vorschlag:** Den Soft-Timeout selbst erkennen statt über die Ausnahmeklasse, z. B. mit `asyncio.timeout()` als Kontextmanager und `cm.expired()`, oder mit einer Prüfung, ob der Timeout aus dem `wait_for` kam. Eine fremde `TimeoutError` läuft als normaler Task-Fehler durch. Nachweistest wie oben, zusätzlich mit gesetztem `soft_timeout`, das nicht abläuft.
- **Konfidenz:** hoch.

### MA-1 (P2): Wird ein Workflow-Schritt per `cancel(task_id)` vor dem Start abgebrochen, bleibt der Workflow bis zur TTL `running`

- **Fundstelle:** `src/swarmq/worker/gates/cancel.py:47-160` (CancelGate.apply); dagegen `results.py:205-223`.
- **Mutanten:** `gates.cancel...apply__mutmut_24` (`workflow_cancel = None`) und weitere Cancel-Gate-Mutanten überleben. Kein Test prüft, was dieser Pfad mit dem Workflow macht.
- **Szenario:** `children()` liefert die `task_id` jedes Schritts, `cancel(task_id)` ist die allgemeine API. Bricht der Cancel einen **laufenden** Schritt ab, markiert `ResultTail._advance_workflow` den Workflow als fehlgeschlagen („task was cancelled“). Trifft derselbe Cancel einen **noch wartenden** Schritt, nimmt das Cancel-Gate einen anderen Weg: Cancel-Ergebnis speichern, Ack, `Terminal.already_released()`. Weder `fail` noch `advance` läuft. Der Zustand bleibt `RUNNING`, `handle.get()` endet immer wieder in `ResultNotReadyError` bis `result_ttl` (24 h), und ein Chord-Body läuft nie. Das ist dieselbe Fehlerklasse wie BZ-13, nur ist dieser Pfad im BZ-13-Fix nicht enthalten.
  - **Variante (P3):** Bei `handle.cancel()` (Workflow-Flag) liefert `get()` korrekt `TaskCancelledError` über die Flag-Prüfung. `STATUS_CANCELLED` schreibt aber nur `advance()` eines Schritts, der nach dem Cancel fertig wird (`workflow_orchestration.py:282`). Werden alle offenen Schritte schon bei der Abholung abgebrochen (kein Schritt lief gerade), meldet `status()` bis zur TTL `"running"`. Laut Doku-Tabelle ist `"cancelled"` erwartet.
- **Beleg:** `test_ma1_pre_start_task_cancel_of_workflow_step_fails_workflow` ist rot: Ack und Cancel-Ergebnis sind da, `advancer.fail` wurde nie aufgerufen. Die Variante ist per Code-Lektüre belegt (`STATUS_CANCELLED` hat genau einen Schreiber).
- **Vorschlag:** Im Cancel-Gate bei per-Task-Cancel `deps.fail_workflow(..., "task was cancelled", True)` aufrufen, wie es das Expiry-Gate tut. Bei Workflow-Cancel den Zustand terminal `cancelled` setzen (idempotent per CAS).
- **Konfidenz:** hoch für den Ablauf. Wie oft es vorkommt, hängt davon ab, ob Nutzer einzelne Schritte abbrechen.

### MA-4 (P2): `unique_until="start"`: Endet die Nachricht terminal, bevor sie startet, bleibt das Dedup-Flag bis `unique_for` stehen

- **Fundstelle:** `src/swarmq/worker/finalize.py:122-126` (`if mode == "start": return`, Begründung: „Unit 10 already released at pickup“). Freigabe beim Start in `pipeline.py:354-373`, erst **nach** Guard/Expiry/Rate-Limit/Lock.
- **Mutanten (Fall a, der Mutant wäre richtiger):** `finalize.x_maybe_release_unique__mutmut_10` (`task_info is None` → für registrierte Tasks gilt „completion“, also Freigabe), `pipeline._finalize_delivery__mutmut_14` und `retry.dead_letter__mutmut_95` (übergeben `task_info=None`, also ebenfalls Freigabe). Alle drei überleben.
- **Szenario:** Die Annahme „im start-Modus wurde schon beim Pickup freigegeben“ stimmt nur, wenn die Nachricht `_release_unique_on_start` erreicht. Die terminalen Pfade davor geben im start-Modus **nicht** frei: `max_age`-Ablauf (Expiry-Gate), Pre-Start-Cancel, Lock-Wartezeit am Ende (DLQ), Lock-Render-Fehler, unbekannter Task nach der Gnadenfrist (wenn der Header `start` trägt), Delivery-Limit. Danach liefert jedes `schedule()` mit denselben Argumenten bis `unique_for` die ID der toten Nachricht zurück, ohne einzureihen. Das ist BZ-22 für den start-Modus; der BZ-22-Fix deckt nur „completion“ ab.
- **Beleg:** `test_ma4_start_mode_flag_released_when_expired_before_start` und `test_ma4_start_mode_flag_released_on_pre_start_cancel` sind rot: `release_unique` wird nie aufgerufen.
- **Vorschlag:** Den Kontext merken lassen, ob `_release_unique_on_start` gelaufen ist. Nur dann ist start an terminalen Pfaden ein No-op, sonst wie completion freigeben (die Owner-Prüfung macht das sicher).
- **Konfidenz:** hoch.

### MA-3 (P3): Das Expiry-Gate speichert ein Fehlerergebnis auch bei `result_ttl=0` und bei FIRE_AND_FORGET

- **Fundstelle:** `src/swarmq/worker/gates/expiry.py:84-85`: `ttl_ms = (task_info.result_ttl or deps.config.result_ttl) * 1000`.
- **Mutanten:** `expiry...apply__mutmut_61` (`ttl_ms > 1`) und `_57/_59` (`/1000`, `*1001`) überleben. Weder der Überspringen-Zweig noch die TTL sind getestet.
- **Szenario:** Der Kommentar sagt: „result_ttl=0 means fire-and-forget — skip the store entirely (mirrors the main-path semantics)“. Das `or` macht aus einem ausdrücklichen `result_ttl=0` aber `config.result_ttl`. FAF ohne `result_ttl` speichert ebenfalls mit `config.result_ttl`, während `resolve_result_ttl` dafür 0 liefert. Läuft ein großer Rückstand von FAF-Nachrichten über `max_age` ab, entsteht je Nachricht ein Ergebniskey mit 24 h TTL, den niemand liest (Speicher in Valkey). Der Kommentar ist falsch, oder der Code ist es.
- **Beleg:** `test_ma3_expiry_respects_no_store[FafExpiring]` und `[NoStoreExpiring]` sind rot.
- **Vorschlag:** `resolve_result_ttl(task_info.result_ttl, config.result_ttl, fire_and_forget=...)` verwenden und bei 0 nicht speichern, wie im Haupt-Pfad. Alternativ ausdrücklich festlegen, dass der Ablauf immer speichert (wie `dead_letter`, das 0 auf `DEFAULT_RESULT_TTL_S` hebt), und den Kommentar korrigieren.
- **Konfidenz:** hoch (Verhalten belegt). Die Schwere hängt von der Absicht ab.

### MA-6 (P3): Ein unlesbarer Zähler-Header lässt die Zustellung unquittiert hängen

- **Fundstelle:** `src/swarmq/worker/gates/rate_limit.py:86` (`int(original_headers.get("x-swarmq-rate-limit-count", 0))` steht außerhalb jedes `try`) und `src/swarmq/worker/retry.py:116-119` (`retry_count = delivery.headers.get(HEADER_RETRY_COUNT, 0)`, danach `retry_count >= max_retries` ohne Typprüfung).
- **Mutanten:** Die Mutanten von `retry_count_of` und `_as_int` überleben (`or True`, `>=`). Die defensive Lesart an anderen Stellen ist kaum getestet, und dabei fiel auf, dass diese beiden Stellen gar keine haben.
- **Szenario:** Die Header sind nicht ganzzahlig, z. B. weil die DLQ-Nachricht über die RabbitMQ-UI neu publiziert wurde (Header als String) oder weil ein Fremd-Producer sie setzt. Dann wirft das Gate bzw. `evaluate` `ValueError`/`TypeError`. `contain_delivery_errors` loggt nur `delivery_handler_failed`, es gibt kein ack und kein nack. Die Nachricht bleibt unquittiert bis `consumer_timeout` (RabbitMQ-Standard 30 min). Dann schließt der Broker den Kanal, alle Nachbar-Zustellungen werden neu zugestellt, und die Schleife wiederholt sich bis zum Delivery-Limit. Beim Retry-Pfad läuft der Task bei jeder Runde **erneut** (doppelte Seiteneffekte). An anderen Stellen gilt die Regel „ein kaputter Header zählt wie keiner“ (`_as_int`, `retry_count_of`, `_as_count`).
- **Beleg:** `test_ma6_rate_limit_count_header_garbage_is_settled` und `test_ma6_string_retry_count_is_settled` sind rot: kein Broker-Verb, Log `delivery_handler_failed`.
- **Vorschlag:** Beide Stellen über `_as_count`/`retry_count_of` lesen.
- **Konfidenz:** hoch (Ablauf). Wie wahrscheinlich es ist, hängt von manuellen Replays ab.

### MA-7 (P3, Doku): Soft-Timeout „raises SoftTimeoutError inside run()“ stimmt nicht

- **Fundstelle:** `website/guides/scheduling-and-retries.md` (Abschnitt *Timeouts*): „The worker raises `SoftTimeoutError` inside `run()` so your task can checkpoint and wrap up.“ Code: `execution.py:141-142` (`asyncio.wait_for(run_coro, timeout=soft_timeout)`).
- **Szenario:** `wait_for` **bricht** `run()` beim Soft-Timeout ab, `run()` sieht also `CancelledError`. Erst danach erzeugt der Worker `SoftTimeoutError`, außerhalb des Tasks. Ein Task, der laut Doku `except SoftTimeoutError:` zum Checkpointen schreibt, erreicht diesen Zweig nie. Das gefährlichere Muster `except Exception` greift nicht, weil `CancelledError` keine `Exception` ist. Der dokumentierte Abstand `hard_timeout > soft_timeout` hat damit keine Wirkung, denn schon der Soft-Timeout beendet den Task hart.
- **Mutant:** wie bei MA-2 (`_await_run_result__mutmut_5`). Aufgefallen beim Prüfen desselben Zweigs.
- **Beleg:** Code-Lektüre (`asyncio.wait_for` in Python 3.12+ bricht das innere Awaitable ab).
- **Vorschlag:** Die Doku an das Verhalten anpassen („bricht `run()` ab, der Fehler ist `SoftTimeoutError`“) oder den Soft-Timeout wirklich als Ausnahme in den Task werfen. Mit MA-2 zusammen entscheiden.
- **Konfidenz:** hoch.

### MA-8 (P3): Der Workflow-Abbruch einer FAF-Signatur meldet „is FIRE_AND_FORGET“ auch bei AT_LEAST_ONCE mit `result_ttl=0`

- **Fundstelle:** `src/swarmq/worker/results.py:124-133`.
- **Szenario:** `_finish_without_store` greift bei jedem `result_ttl == 0`. Ein AT_LEAST_ONCE-Schritt mit ausdrücklichem `result_ttl=0` lässt den Workflow mit `WorkflowValidationError` und dem Text „task 'x' is FIRE_AND_FORGET and cannot advance a workflow“ scheitern. Das Verhalten ist dokumentiert (Tabelle *Failure modes*: „or stores no result, `result_ttl=0`“), die Meldung führt aber in die Irre.
- **Mutanten:** `results._finish_without_store__mutmut_9` (`or`) überlebt.
- **Vorschlag:** Den Text nach der Ursache unterscheiden. Optional `result_ttl=0` schon bei `Task.s()` ablehnen, wie FAF.
- **Konfidenz:** hoch (Kleinkram).

### Bestätigungen und Abgrenzung zu bekannten Befunden

- **R-3** (Expiry: Ack-Fehler nach liegender DLQ-Kopie): Die Mutanten an `expiry...apply` (`dead_letter(..., headers=...)`, Mutanten 91/92/96) überleben. Das bestätigt, dass Header und Settle-Fehler der Expiry-DLQ-Kopie ungetestet sind. Kein neuer Befund.
- **N-2** (DI-Finalizer): `_teardown_di_scope`-Mutanten überleben (u. a. `CancelledError` bei Cancel nicht übergeben). Geprüft wurde, ob ein Cancel-Exit selbst eine `CancelledError` werfen und so Semaphor und Lock-Freigabe überspringen kann: Dishka übergibt die Ausnahme per `asend`, nicht per `athrow`, also nein. Kein neuer Befund.

### Geprüft, ohne Fehler (Auswahl)

- `_nominal_defer_total` (Rechnung am Deckel richtig, nur ungetestet), `completed_marker_ttl_ms` (Mutant `or True` ist äquivalent, `int(None)` wird gefangen), `dead_letter_copy` (Rückgabewert ungenutzt, Mutanten äquivalent), `_prepare_task_instance` (FAF-Inversion äquivalent, solange `config.result_ttl` dem Standard entspricht).
- `LockKeeper.expires_by` nach dem Aufruf (konservativ richtig für „sicher abgelaufen“), `GuardedBroker` (Verb-Marker konsistent), `_check_late_cancel` (Original richtig, siehe Teil 2).
- `renew_once` mit `continue` statt `break` (Original richtig), `_emit_terminal_hooks` `and` statt `or` (`_fail` setzt immer eine Exception, also äquivalent).

---

## Teil 2: Wichtige Testlücken ohne Fehler

1. **`LockKeeper._extend`/`drop`** (`lock_keeper.py:106-111, 164-202`): `if op_timeout is not None` → `is None`, `drop` mit `if self._kept`, `window_ms*1000` überleben. Ein hängendes `extend` ohne Zeitlimit blockierte den ganzen Takt (`gather`), alle Locks liefen ab. Und das Stoppen der Schleife bei zwei gehaltenen Locks ist ungetestet. Ein Test mit zwei Leases (eine freigegeben, die andere muss weiter verlängert werden) und mit hängendem `extend` fehlt.
2. **Lock-Wartezeit über Kopien hinweg** (`gates/lock.py:306-412`): `headers[HEADER_LOCK_WAIT_GEN] = None` überlebt, ebenso `_track_holder` (`or`→`and` bei fehlendem Halter, `base_ms = … and ttl_ms`, Header-Wert ignoriert). Mit dem ersten Mutanten würde die Wartezeit bei jedem Konflikt neu beginnen, `lock_wait_max_s` griffe nie. Die Tests spritzen Header ein, statt die Kopie des Gates in die nächste Zustellung zu geben.
3. **`ExecutionGuard`-Fehlerpfade** (`execution_guard.py`): `Worker.__init__` mit `dead_letter=None` überlebt (Orphan-Limit über den Worker ungetestet), `renew_once` `continue`→`break`, `_retry_mark` (`>=`→`>`, Pause `None`), `_lease_state_ttl_ms`/`_backlog_ttl_ms` mit `max_age - 300`. Das sind die Zeitgrenzen gegen Doppelausführung; die Untergrenzen sind nur grob abgedeckt.
4. **`_check_late_cancel` / Late-Cancel-Flip** (`pipeline.py:304-316, 517-528`): `return False`→`return True` im `BackendConnectionError`-Zweig überlebt. So mutiert, würde bei einem Valkey-Ausfall jeder Task-Fehler zum Cancel und der Retry entfiele. Die Felder des Flips (`is_error=False`→`True`) sind ebenfalls ungetestet.
5. **Argumente von `advancer.advance`** (`results.py:224-229`): alle Argument-Mutanten (wf_id, step_path, task_id, result) und `step_path`-Fallback überleben in der Unit-Suite. Nur die Integration prüft, ob das richtige Ergebnis an den nächsten Schritt geht.
6. **`fail_workflow_step` bei ungültigem Workflow-Header** (`terminal.py:44-49`): `return True`→`False` überlebt. So mutiert, endete der Pfad nicht-terminal ohne Redeliver, die Nachricht bliebe unquittiert. `headers.get(...) is None` überlebt ebenfalls.
7. **TTLs gespeicherter Ergebnisse auf Gate-Pfaden** (Cancel `result_ttl*1000`, `store_cancel_result = result_ttl > 0`; Expiry `ttl_ms`; `dead_letter` mit `config.result_ttl`): `/1000`- und `*1001`-Mutanten überleben überall. Ein Faktor-1000-Fehler (Ergebnis nach Millisekunden weg) bliebe unbemerkt.
8. **Unique-Freigabe an Gate-Pfaden** (Cancel-Gate, Lock-DLX-Fallbacks, `dead_letter`, unknown-task): Argument-Mutanten von `maybe_release_unique` überleben (`task_id=None`, `task_info=None`). Siehe MA-4; die Modus-Logik ist nur am Hauptpfad getestet.
9. **`_queue_delivery_limit`** (`delivery_count.py:57-68`): `or`→`and` und `iscoroutinefunction(None)` überleben. Wie Fremd-Broker oder Mocks ins Limit fließen, ist ungetestet; ein falsches Limit führt zu verfrühtem bzw. ausbleibendem DLQ.
10. **`_teardown_di_scope`** (`pipeline.py:318-332`): `CancelledError` bei Cancel, Token-Reset und `exc_type`/`tb` überleben. Ob Finalizer Commit oder Rollback sehen, ist ungetestet (fachlich wichtig für DB-Sessions).

### Grobe Statistik (703 Mutanten)

| Klasse | Anzahl (ca.) | Bewertung |
|---|---|---|
| Felder in Log-, Signal- und Diagnose-Aufrufen (`task_id=None`, `hint=None`, entfernte kwargs) | ~235 | Rauschen |
| `what=`-Labels, `op_timeout=None` an Mocks, Initialwerte (`= None`/`""`), `cast`/`usedforsecurity`/`strict` | ~65 | äquivalent bzw. Rauschen |
| Konstanten ±1 (`*1001`, `+301`, `>= 1`, Exponent 21, `[:17]`) | ~30 | fast alle äquivalent (Grenzwerte ohne Wirkung) |
| Argument-Mutanten an Mock-Grenzen (Werte an Broker/Backend nicht geprüft) | ~250 | Testlücke, meist ohne Fehler (Teil 2) |
| inhaltliche Logik- und Bedingungs-Mutanten | ~120 | durchgesehen; daraus MA-1…MA-8 |

Die Hälfte der Überlebenden betrifft Log- und Diagnosefelder oder ist äquivalent. Echte Befunde ergaben sich dort, wo sich überlebende Mutanten in einem selten durchlaufenen terminalen Pfad häufen: Pre-Start-Cancel, Expiry, Unique-Freigabe, die Ausnahmebehandlung von `_await_run_result` und das Lesen von Headern.
