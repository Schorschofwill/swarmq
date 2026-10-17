# Fehlersuche Runde 4: Sichtung der Überlebenden, Gruppe D (Valkey-Backend)

Stand: Worktree `swarmq-fehlersuche4`, Branch `test/fehlersuche-runde-4`. Einheit U5, Gruppe D. Plan: `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md`, R17, KTD9.

## Kopf

| Punkt | Wert |
|---|---|
| Eingabe | `<scratchpad>/u4/mut-gruppe-D-schnitt.txt` aus U4 (`3b-mutation-lauf-b.md`): **234 Mutanten** nach Vorfilter, davon 141 in Lauf B überlebt (133 auch in A überlebt, 8 in A ohne Test) und 93 in B ohne Test. Die 79 vom Vorfilter aussortierten (`mut-gruppe-D-schnitt-gefiltert.txt`) wurden nicht gesichtet |
| Module | `backend/valkey/{__init__, chords, connection, control, environment, execution_guard, locks, rate_limit, script_pipeline}.py`; `schedules.py` und `results.py` sind nicht mutiert (R14) |
| Ergebnis | **3 Fehlerverdachte** (R4M-D-01 bis -03), alle mit rotem Probe-Test gegen den unmutierten Code. Von den 234 Mutanten: **93 getötet von Integrationstests außerhalb der Lauf-B-Auswahl** (Gegenlauf), **43 Testlücken**, **98 Rauschen**. Keiner der drei Verdachte kommt aus einem Mutanten selbst; alle fielen beim Lesen der ungeprüften Fehlerpfade auf (wie in Gruppe E) |
| Probe-Tests | `<scratchpad>/u5d/test_mut_d_nachweise.py`, 5 Tests ohne Dienste, alle rot an der fachlichen Stelle (Tabelle unten) |
| Gegenlauf | `<scratchpad>/u5d/gegenlauf.sh` mit `gegenlauf-liste.txt`: 31 nachgebildete Mutanten gegen die Integrationstests, die Lauf B nicht ausgeführt hat; je Mutant ein Lauf `uv run just integration <Dateien> -p mut_env -x` unter **einer** Testsperre (`scripts/with-test-lock.sh`), Seed 12345. Plugin `<scratchpad>/u5d/plug/mut_env.py` ersetzt die Funktion im Namensraum ihres Moduls (Muster aus U5/F). Fünf Basisläufe ohne Mutation grün. Logs `<scratchpad>/u5d/gl-*.log`, Übersicht `gegenlauf.log` |

`<scratchpad>` = `/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/608b3617-a1bc-47f1-bd40-e763775cafe6/scratchpad`.

Probe-Lauf aus dem Worktree:

```
PYTHONPATH=. uv run --extra test pytest -p no:randomly -n0 --rootdir . -c pyproject.toml \
  <scratchpad>/u5d/test_mut_d_nachweise.py
```

Ergebnis: 5 von 5 rot in 3,7 s.

| Test | Rot an |
|---|---|
| `test_r4m_d01_chord_store_wraps_a_transport_error` | `valkey.exceptions.ConnectionError: Error 111 connecting to 127.0.0.1:…` statt `BackendConnectionError`; die Gegenprobe im selben Test (`LockManager.acquire` auf demselben Client) wirft korrekt `BackendConnectionError` |
| `test_r4m_d01_chord_store_reports_a_self_healing_reply_as_outage` | `valkey.exceptions.OutOfMemoryError` statt `BackendConnectionError` |
| `test_r4m_d01_worker_redelivers_when_the_chord_store_is_down` | `Expected fail to not have been awaited. Awaited 1 times.` (Log `workflow_advance_failed`) |
| `test_r4m_d02_connect_wraps_a_timeout` | `valkey.exceptions.TimeoutError: Timeout connecting to server` statt `BackendConnectionError` |
| `test_r4m_d03_second_connect_after_a_failed_one_still_reports_the_outage` | `DID NOT RAISE BackendConnectionError` |

Einordnung wie in Gruppe E und F:

- **Fehlerverdacht:** Das Verhalten des **unmutierten** Codes verletzt eine Zusage; Beleg ist ein roter Probe-Test.
- **Testlücke:** Der Code ist richtig, aber weder `tests/unit` noch die gesamte Integrationssuite hält das Verhalten fest.
- **Integration (I):** Lauf B hat den Mutanten nicht getötet, weil seine Auswahl nur `tests/integration/backend` umfasste. Ein Test auf oberster Ebene von `tests/integration/` tötet ihn (im Gegenlauf belegt oder durch denselben Vertragstest wie ein belegter Nachbar). Für den Fix-Plan ist das höchstens eine Lücke der Unit-Ebene.
- **Rauschen:** äquivalent, Text für Log oder Fehlermeldung (Argument `what`), Konstante ohne Vertrag, toter Code, Zweig für einen Client mit `decode_responses=True`, den swarmq nicht baut.

**Abweichungen zwischen Plan und Code bzw. Auftrag:**

1. Plan U4 erwartet, dass Lauf B „echte Lücken und nicht nur fehlende Unit-Tests“ zeigt. Die Auswahl `tests/integration/backend` (R15) lässt aber die Vertragstests der Stores, die Lua-Tests und die Zustandsmaschinen weg. Deshalb hat diese Sichtung den Gegenlauf ergänzt. Ohne ihn wären `observe`, `renew`, `release`, `mark_superseded`, `record_member` als „auch in der Integration ungeprüft“ gezählt worden; das stimmt nicht.
2. Die Schwerpunkte aus dem Lauf-A-Bericht (Einstiege 1–7) sind unten einzeln beantwortet. Keiner davon ist ein Fehler.
3. Plan U5 Schritt 4 (Abgleich mit den Testlücken aus Runde 2: Drain-Frist 0, `lock_wait_max_s` über Kopien, Keeper-Timeout, `_check_late_cancel` bei Valkey-Ausfall): Diese Stellen liegen im Worker bzw. in `results.py`, nicht in den mutierten Modulen. Daraus folgt kein Eintrag.

---

## Teil 1: Fehlerverdachte

### R4M-D-01 (P2): `ChordManager` wandelt Treiberfehler nicht; ein kurzer Valkey-Ausfall beim letzten Gruppenglied kann einen erfolgreichen Workflow als `failed` markieren

- **Fundstelle:** `src/swarmq/backend/valkey/chords.py:41-68` (`record_and_check_complete`, `record_member`): Der Skriptaufruf steht ohne `try`. Alle anderen Koordinations-Stores wandeln an derselben Grenze: `LockManager` (`locks.py:70-73`), `ValkeyExecutionGuardStore._run` (`execution_guard.py:56-62`), `ValkeyRateLimitStore.check` (`rate_limit.py:82-87`), `ValkeySetpointStore._guarded` (`control.py:31-39`). Aufrufer: `WorkflowAdvancer._on_group_member` (`workflow_orchestration.py:395`), erreicht aus `ResultTail` (`worker/results.py:234`).
- **Mutanten:** `ChordManager.record_member__mutmut_1–10` (in A und B ohne Test), `record_and_check_complete__mutmut_16–18` (in A ohne Test, in B überlebt). Im Gegenlauf töten die Vertragstests und die Workflow-Maschine diese Mutanten (C1–C3). Keiner davon prüft den Fehlerpfad. Die Workflow-Maschine (`1-zustandsmodelle.md`, Subjekte) injiziert Störungen nur in Zustandslesen und Compare-and-Set, und zwar als bereits gewandelten `BackendConnectionError`.
- **Szenario:**
  1. Ein Gruppen- oder Chord-Kopfglied endet erfolgreich. `store_result` und das Lesen des Workflow-Zustands gelingen.
  2. Valkey fällt aus (Neustart, Failover, `LOADING`, Netzwerk) oder hängt. valkey-py wiederholt `EVALSHA chord_complete` mit der Retry-Politik aus `connection.py:247` (5 Wiederholungen, Backoff 0,1–1,6 s; Befehls- und Verbindungs-Retry sind verschachtelt, beim Verbindungsaufbau gemessen 31 Versuche und 18,6 s Backoff, siehe R4M-D-02) und wirft dann roh `valkey.exceptions.ConnectionError` bzw. `TimeoutError`. Eine Fehlerantwort, die durch Warten heilt (`OOM`, `READONLY`), kommt als roher `ResponseError`.
  3. `ResultTail` fängt für den Advance nur `BackendConnectionError`/`WorkflowStateConflict` (Redelivery), `UnroutableMessage`, `BrokerConnectionError`; der rohe Treiberfehler fällt in `except Exception` (`worker/results.py:275-290`, Log `workflow_advance_failed` auf ERROR) und wird wie ein Programmfehler behandelt: Fail-Mark des Workflows, dann Ack.
  4. Das Fail-Mark läuft sofort danach mit eigenem Retry-Budget. Ist Valkey innerhalb dieses zweiten Fensters zurück (bei Verbindungsfehlern also ein Ausfall, der länger dauert als das Retry-Fenster des Chord-Aufrufs, aber kürzer als beide zusammen, grob 20 bis 40 s; bei einem einzelnen hängenden Befehl reicht ein Zeitablauf), steht der Workflow auf `failed`, und `get()` wirft `TaskError`, obwohl jeder Schritt gelungen ist. Ist Valkey noch weg, scheitert das Fail-Mark mit `BackendConnectionError` und die Nachricht wird neu zugestellt; dann bleibt nur die falsche ERROR-Zeile.
- **Beleg:** Drei rote Probe-Tests. (a) Echter valkey-py-Client gegen einen geschlossenen lokalen Port: `LockManager.acquire` wirft `BackendConnectionError`, `ChordManager.record_member` auf demselben Client den rohen `ConnectionError`. (b) `OutOfMemoryError` aus dem Skript kommt roh heraus. (c) `ResultTail.finish` mit einem Advance, der genau diesen rohen `ConnectionError` wirft (Worker-Fixtures aus `tests/unit/worker/conftest.py`, Hilfen aus `tests/unit/worker/test_results.py`): `fail` wird aufgerufen, Ergebnis ist kein `Redelivered`.
- **Quelle der Erwartung:** (a) Der Worker behandelt einen Backend-Ausfall im Advance ausdrücklich als vorübergehend: `except (BackendConnectionError, WorkflowStateConflict)` mit „Transient — redelivery retries the idempotent advance“ (`worker/results.py:235-242`); der Kommentar davor stellt den CAS-Konflikt einem Backend-Ausfall gleich: „wie ein Backend-Ausfall behandeln, nicht fail-markieren“. (b) E-040: Fail-Mark nur, wenn ein Schritt „endgültig ohne Erfolg“ endet. (c) `website/reference/errors.md:190-200`: Verbindungsverluste zur Laufzeit holt der Reconnect nach, `BackendConnectionError` kommt erst nach erschöpftem Retry-Fenster; dazu die Fehlerfälle in `website/guides/workflows.md:227-243`, die keinen Valkey-Ausfall als Grund für `failed` nennen. (d) Gleiche Regel an anderer Stelle: `RateLimitStore.check` „must raise `BackendConnectionError` when the backend is unreachable“ (`website/reference/api.md:906-909`), dazu die Boundary-Regel aus `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md` („ValkeyBackend raises raw ValkeyError … Worker error handler never fires“).
- **Abgleich:** Register: `grep -il chord` trifft E-039 (FAF-Schritt) und E-040 (Fail-fast); keine Regel zum Fehlerpfad des Chord-Stores. Kein Test mit `entscheidung`-Marker hält rohe Treiberfehler im Advance fest; `test_workflow_advance_unexpected_error_fail_marks_and_acks` prüft das Fail-Mark für einen `RuntimeError` („advance bug“), nicht für einen Treiberfehler. `git log -S"class ChordManager"`: angelegt in e7204ce (U3 Workflow), verschoben in ff443e4 (U18); das Wrapping der anderen Stores kam später (0.9.0, Test-Liste in `tests/unit/backend/test_valkey_locks.py`), `chords.py` wurde dabei nicht angepasst. Nicht in TS-01..03 (das sind aiormq-Lecks).
- **Vorschlag:** `record_and_check_complete` wie `LockManager` wandeln: `except BackendConnectionError: raise`, `except Exception as exc: raise as_backend_error(…, exc)`. Nachweistest wie (a) und (b) in `tests/unit/backend/`, dazu ein Worker-Test, der mit einem echten `ChordManager` über einen Fake-Client den Weg bis zur Redelivery zeigt.
- **Konfidenz:** hoch, dass der Fehler roh entkommt und der Worker fail-markt. Mittel für die Häufigkeit des Schadens: Valkey muss zwischen Zustandslesen und Chord-Aufruf ausfallen und im Fenster des Fail-Marks zurückkommen (Neustart mit AOF-Laden, Failover, kurzer Netzausfall). Ein Störlauf `valkey_restart` mit Workflows (U11/U13) könnte das zeigen; ob die Störungsszenarien der Runde Workflows enthalten, habe ich nicht geprüft.

### R4M-D-02 (P3): `connect()` wandelt einen Zeitablauf nicht in `BackendConnectionError`

- **Fundstelle:** `src/swarmq/backend/valkey/connection.py:256` fängt nur `(ValkeyConnectionError, OSError)`. `valkey.exceptions.TimeoutError` erbt nicht von `ConnectionError` (beide direkt von `ValkeyError`, geprüft mit valkey-py 6.1.1). valkey-py wirft ihn bei `socket_connect_timeout` („Timeout connecting to server“, `valkey/asyncio/connection.py:317`) und bei `socket_timeout` auf dem `PING`. Ebenso entkäme ein `ResponseError` auf den `PING` (z. B. ACL `NOPERM`), der aber selten ist.
- **Mutanten:** keiner direkt. `connect__mutmut_27/28` (Fehlertext) überlebten; beim Lesen des Fehlerpfads fiel die Lücke im `except` auf.
- **Szenario:** Valkey ist beim Start nicht erreichbar, aber nicht abweisend: ein Host hinter einer Firewall, die Pakete verwirft, ein angehaltener Container, ein überlasteter Server. `ValkeyBackend.connect()` (über `SwarmQ.__aenter__`, `Worker.start`, `swarmq schedule …`, `swarmq env-check`) wirft dann roh `valkey.exceptions.TimeoutError`. Eingebettete Nutzer, die nach Doku `except BackendConnectionError` schreiben, fangen ihn nicht. Die CLI ist gedeckt: `_startup_error_classes()` (`cli/__init__.py:403-424`) nimmt die rohen valkey-Klassen ausdrücklich auf, mit Kommentar zu `get_scheduler_leader` (Decision E4). Der Supervisor-Pfad `run_multi_process_worker` (`cli/__init__.py:385`) fängt nur `BackendConnectionError`; dort endet der Kindprozess mit einem anderen Fehler, die Folge ist aber in beiden Fällen ein Neustart.
- **Beleg:** `test_r4m_d02_connect_wraps_a_timeout`: lokaler Port mit `listen(0)`, der nie antwortet, echter valkey-py-Client, Fristen auf 0,01 s und Backoff auf 0 gekürzt (Pfad unverändert). Rot mit `valkey.exceptions.TimeoutError: Timeout connecting to server`. Nebenmessung (`<scratchpad>/u5d/d02_zaehlen.py`): Bis zur Ausnahme vergehen 31 Verbindungsversuche und 18,6 s Backoff (verschachtelte Wiederholung aus Befehls- und Verbindungs-Retry). Mit den echten Fristen (5 s Connect-Timeout) sind das hochgerechnet rund 3 Minuten, bevor `connect()` überhaupt wirft. Mit einem Port, der Verbindungen annimmt, aber nie antwortet (`listen(16)`), dauerte es mit 0,05-s-Fristen 289 s (`d02_debug.py`); den Grund für die längere Dauer habe ich nicht untersucht.
- **Quelle der Erwartung:** Vertrag `ResultBackend.connect`: „Idempotent; Treiberfehler → `BackendConnectionError`“ (`src/swarmq/contracts/backend.py:16-17`). `website/reference/errors.md:190-195`: `BackendConnectionError` „Worker startup when Valkey is not reachable“. Der Unit-Test `test_connect_sets_socket_timeouts` (`tests/unit/backend/test_valkey.py:146-152`) nennt den Zweck des Connect-Timeouts: „begrenzt zusätzlich den TCP-Handshake gegen einen blackholed Host“. Genau dieser Fall endet ungewandelt.
- **Abgleich:** Register: kein Treffer für Verbindungsaufbau oder Zeitablauf beim Start (`grep -il "connect\|TimeoutError"` trifft E-006 Exit-Codes von `swarmq check` und E-048 Leader-Renew). E4 (Decision im Plan 3c, roh propagierende Fehler) gilt nur für `get_scheduler_leader`. Kein Test mit `entscheidung`-Marker berührt `connect()`. Der einzige Fehlerpfad-Test `test_connect_wraps_connection_error` prüft nur `ConnectionError`.
- **Vorschlag:** `except (ValkeyError, OSError) as exc: raise as_backend_error(str(exc), exc) from exc` wie in `_run`. Die Dauer von rund 3 Minuten bis zur Ausnahme ist eine eigene Frage (Retry-Politik beim Start); hier nur gemeldet.
- **Konfidenz:** hoch (Vertrag eindeutig, Pfad mit echtem Treiber gezeigt). Auswirkung gering, weil die CLI beides fängt.

### R4M-D-03 (P3): Ein zweites `connect()` nach gescheitertem ersten meldet Erfolg, ohne Valkey zu erreichen, und überspringt die Eviction-Prüfung

- **Fundstelle:** `src/swarmq/backend/valkey/connection.py:238-261`. `self._client` wird vor dem `PING` gesetzt und bei dessen Scheitern nicht zurückgesetzt. Der nächste `connect()` hält den Client für verbunden (`if self._client is not None: return`) und kehrt ohne `PING` und ohne `probe_eviction` zurück.
- **Mutanten:** keiner direkt; aufgefallen beim Lesen von `connect` (Mutanten `connect__mutmut_4/21–28`).
- **Szenario:** Ein eingebetteter Worker oder Producer wiederholt den Start, bis Valkey da ist (`while True: try: await worker.start() …` oder `async with app` in einer Schleife; `Worker.start` ruft `backend.connect()` vor seinem `try/finally`, schließt das Backend also nicht). Der zweite Versuch läuft durch, obwohl Valkey weiter fehlt. Wichtiger: Kommt Valkey mit `maxmemory-policy allkeys-lru` zurück, bleibt `eviction_check` `None`, und `enforce_eviction_policy` (`worker/eviction_gate.py:19-21`) lässt den Worker im Standardmodus `strict` starten. Das widerspricht E-005 (verdrängende Policy blockiert den Start). Die CLI ist nicht betroffen: Dort endet der Prozess beim ersten Fehler.
- **Beleg:** `test_r4m_d03_second_connect_after_a_failed_one_still_reports_the_outage`: geschlossener Port, erster `connect()` wirft korrekt, der zweite wirft nicht (`DID NOT RAISE`).
- **Quelle der Erwartung:** Vertrag `connect`: „Idempotent; Treiberfehler → `BackendConnectionError`“ (`contracts/backend.py:17`). Idempotent heißt: Ein zweiter Aufruf hat dieselbe Wirkung wie ein erfolgreicher erster, nicht „Erfolg ohne Wirkung“. Dazu E-005 und die Startprüfung `strict`.
- **Abgleich:** E-005 (`e005-verdraengende-valkey-policy-blockiert-start.md`) gilt; seine Tests prüfen nur den ersten `connect()`. Keine Regel zu wiederholtem `connect()` nach Fehler.
- **Vorschlag:** Bei Fehler im `try` den Client schließen und `self._client = None` setzen, oder die Prüfung „verbunden“ an einen eigenen Merker nach erfolgreichem `PING` binden.
- **Konfidenz:** hoch für das Verhalten; mittel für die Einstufung als FEHLER (der Auslöser setzt eine Wiederholschleife im selben Prozess voraus). Der Gegenprüfer sollte LÜCKE erwägen.

### Geprüft, ohne Fehler (Schwerpunkte aus dem Lauf-A-Bericht)

- **`locks.py` `now_ms` und Lua-Argumente (Einstiege 1, 2):** Die Argumente an die Skripte (`keys`, `args`, Owner-Token) tötet Lauf B bereits (`acquire__mutmut_4/17/18/21`, `ValkeyLockStore.acquire__mutmut_1–4`). Offen bleibt `acquire__mutmut_3` (`+ µs//1000` → `-`). Er verschiebt `now_ms` um bis zu 2 s nach unten. Damit verfallen Semaphor-Slots in `lock_acquire.lua` (`ZREMRANGEBYSCORE … now_ms - ttl_ms`) bis zu 2 s zu früh. Weder die Integration noch die Lock-Maschine gegen Valkey bemerkt das (Gegenlauf L1). Das Original ist richtig: **Testlücke**. `__mutmut_7` (`/` statt `//`, Bruchteil-ms) und `__mutmut_9` (`// 1001`) sind Rauschen; `extend__mutmut_17` ebenso (Gegenlauf L3). Die TTL selbst rechnet der Worker (`compute_lock_ttl_ms`, Welle 1, `docs/solutions/logic-errors/lock-ttl-shorter-than-hard-timeout-duplicate-execution-2026-06-02.md`); `locks.py` reicht sie nur durch, und das ist geprüft.
- **`execution_guard.renew` invertiert (Einstieg 4):** `renew__mutmut_16` tötet `test_renew_extends_only_for_the_owner` und `test_renew_after_expiry_fails` aus `tests/contracts/execution_guard.py` (Gegenlauf G1). Kein Fehler, keine Lücke der Integration; auf Unit-Ebene ungeprüft.
- **`claim__mutmut_3` (Einstieg 5, `backlog_ttl_ms or 1`):** Rauschen. `guard_claim.lua:44-54` setzt den Rückstand nur bei `backlog > 0` und verlängert nur; 1 ms gegen 0 ändert nichts Beobachtbares (Gegenlauf G5).
- **Fensterrechnung `rate_limit.py` (Einstiege 6, 7):** `check__mutmut_11/14/21/36` töten die Vertragstests, `test_rate_limiter_integration.py` und die Retry-Maschine (Gegenlauf R1–R4; `_14` erst nach 241 s über die Hypothesis-Maschine). Offen bleibt die Zeitbasis `check__mutmut_3/6/9` (`now_ms` falsch zusammengesetzt; Gegenlauf R6): **Testlücke**, das Original ist richtig. E-070 (Rate-Limit vor Lock) wird nicht berührt.
- **`environment.py:46` `cast(raw)` überlebt trotz `TypeError` (`_from_info__mutmut_10`):** Der `TypeError` entsteht in `_from_info` und wird von `probe_eviction` (`environment.py:66-69`) mit `except Exception: continue` verschluckt; die Prüfung endet dann mit `unknown`. Das ist so gewollt („Wirft nie“, Docstring). Dass kein Test scheitert, liegt am Test: `test_connect_tolerates_forbidden_config_get_but_says_so` (`tests/unit/backend/test_befund_bz04_eviction_policy.py:115-135`) verbietet `CONFIG GET`, liefert über `INFO` die Policy `volatile-lru` und prüft dann nur, dass irgendein Hinweis mit „maxmemory“ oder „evict“ geloggt wurde. Den gibt auch die Warnung `valkey_eviction_policy_unknown`. Der ganze INFO-Rückfall (`_from_info`, 18 Mutanten, und `probe_eviction__mutmut_9` `continue` → `break`) ist deshalb ungeprüft: **Testlücke** mit Gewicht, weil Managed-Dienste `CONFIG` per ACL sperren und dann genau dieser Pfad die Startsperre `strict` (E-005) trägt.
- **`chords.py` ohne jeden Unit-Test:** Die Logik (Rückgabe `result == 1`, Schlüssel, Argumente) prüfen die Vertragstests und die Workflow-Maschine (Gegenlauf C1–C3). Offen ist der Fehlerpfad: R4M-D-01.
- **`locks.observe` (Einstieg 3):** In beiden Läufen „ohne Test“, aber `test_observe_reports_holders_and_remaining_ttl` und `test_*_keeps_pttl` aus `tests/contracts/coordination.py` sowie die Lock-Maschine töten die Zweig-, Schlüssel- und Rückgabe-Mutanten (Gegenlauf L7–L10, L12). E-007 (Fingerprint = Menge der Owner) ist damit gedeckt. Rauschen: `transaction=False/None` (`__mutmut_3/4`, Gegenlauf L11; `GET` und `PTTL` hintereinander ohne MULTI unterscheidet kein Test, fachlich harmlos). Testlücke: `zrange(key, 0, +1)` (`__mutmut_15`, nur bei Semaphoren mit mehr als zwei Haltern sichtbar) und die Fehlerklassifikation (`__mutmut_27/29`).
- **`connection.ping` und Reconnect-Wrapping:** `ping` läuft über `_run`, das `ValkeyError`/`OSError` per `as_backend_error` wandelt; das Original ist richtig. `ping__mutmut_4` (Argumente verschoben) tötet der Vertragstest `test_connect_is_idempotent` (Gegenlauf N1). `ping__mutmut_3/6` (ohne eigene 2-s-Frist) sind Rauschen: Der einzige Aufrufer `HealthChecker.check_backend` (`health.py:125`) begrenzt selbst mit `wait_for`. Offen ist die Retry-Politik: `connect__mutmut_4` (`retry=None`) überlebt, weil `test_connect_creates_client_and_pings` nur `"retry" in call.kwargs` prüft (**Testlücke**; ohne Retry würde jeder kurze Verbindungsabbruch sofort als Ausfall gemeldet). Beim Lesen des Fehlerpfads: R4M-D-02 und R4M-D-03.
- **`control.py` (Kontrollkanal-Keys):** Alle Mutanten an `_setpoint_key`, `_status_key` und an den Schlüsseln sind getötet. Was überlebt, ist der Text `what` in `_guarded` (Fehlermeldung, Rauschen), `px=max(2, …)` (Rauschen) und der `str`-Zweig von `_decode_setpoint` (Rauschen: swarmq baut den Client ohne `decode_responses`). **Testlücke:** `_decode_setpoint__mutmut_15/16/17` (gemischte Typen: `{"min": "a", "max": 3}` oder `{"min": true, "max": 3}` würden als Sollwert akzeptiert); die Tests prüfen nur Fälle, in denen beide Werte falsch sind.
- **`script_pipeline.py`:** Rauschen: `get_connection("EVALSHA")` (`__mutmut_3/4/5`; valkey-py 6.1.1 ignoriert den Befehlsnamen, `asyncio/connection.py:1070`), `zip(strict=…)` (Längen sind immer gleich), `client=None` in `_direct` (das Skript ist am selben Client registriert), `disconnect(nowait=False)`, `_pack` für `bytes` (kein Aufrufer übergibt `bytes`). **Testlücke:** `_send__mutmut_20/22/23` (`_fail(None, exc)`): Der Originalfehler eines Transportfehlers ginge verloren, die Wartenden bekämen über `_drain` nur „script pipeline aborted“; das ist auch eine `ConnectionError`, die Einordnung als Ausfall bleibt also gleich. `_drain__mutmut_13` (`self._pending = None` nach Abbruch): Die Pipeline wäre danach unbrauchbar; erreichbar nur nach einem Abbruch des Sammlers.
- **`__init__.py` `_drop_stores__mutmut_2`:** Nach `close()` bliebe `execution_guard` als `""` statt `None` stehen; ein erneuter `connect()` derselben Instanz bekäme keinen neuen Store. **Testlücke** (Wiederverwendung nach `close()` ist vorgesehen: Kommentar `__init__.py:71`).
- **Abgleich mit den genannten Entscheidungen:** E-007, E-022 (Lease-Erneuerung: `extend__mutmut_3` tötet `test_befund_bz25_lock_lease.py`, Gegenlauf L2), E-052 (Freigabe nach Ausfall beim Erwerb: hängt an `as_backend_error` im `acquire`, geprüft) und E-070 sind nicht verletzt. E-020, E-034, E-046, E-058, E-066 betreffen `results.py` und `schedules.py`, die nicht mutiert wurden. Die Muster aus `docs/solutions/patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md` (Unique-Claims) liegen in `results.py`, außerhalb der Welle.

---

## Teil 2: Tabelle je gesichteter Funktion

Einordnung: **F** = Fehlerverdacht im Umfeld, **T** = Testlücke, **I** = von Integrationstests außerhalb der Lauf-B-Auswahl getötet, **R** = Rauschen. Zahlen sind die Suffixe `__mutmut_n`; „(G)“ heißt im Gegenlauf belegt. Status: S = in B überlebt, O = in B ohne Test.

| Modul | Funktion | Mutanten | Einordnung | Verdacht-ID |
|---|---|---|---|---|
| `__init__.py` | `ValkeyBackend._drop_stores` | S: 2 | T (1): Store nach `close()` nicht neu gebaut | – |
| `chords.py` | `chord_set_key` | O: 1–4 | R (4): kein Aufrufer in `src/` | – |
| | `ChordManager.__init__` | S: 1, 4 | R (2): `_client` ungenutzt; Backend übergibt immer `keys` | – |
| | `record_and_check_complete` | S: 16–18 | I (3): 17 (G) | R4M-D-01 (Fehlerpfad) |
| | `record_member` | O: 1–10 | I (10): 1, 3 (G) | R4M-D-01 (Fehlerpfad) |
| `connection.py` | `_is_outage_reply` | S: 8 | R (1): `maxsplit=2` äquivalent | – |
| | `ValkeyConnection.__init__` | S: 2, 3, 5, 6, 8, 9 | R (6): Namen nur im Fehlertext | – |
| | `connect` | S: 4, 21–25, 27, 28 | T (1): 4 (`retry=None`); R (7): Backoff-Werte ohne Vertrag, Fehlertext | R4M-D-02, R4M-D-03 |
| | `ping` | S: 1, 3, 4, 6–8 | I (1): 4 (G); R (5): Text, eigene Frist (3 (G), 6) | – |
| | `reconcile_namespace` | S: 6 | T (1): Rücksetzen auf „ohne Namensraum“ | – |
| `control.py` | `_decode_setpoint` | S: 3, 5–7, 15–17 | T (3): 15–17 gemischte Typen; R (4): `str`-Zweig, `errors=` | – |
| | `_guarded`, `read_setpoint`, `write_setpoint`, `clear_setpoint`, `read_status`, `write_status` | S: 1; 2, 6, 7; 7, 11, 12; 1, 5, 6; 2, 6, 7; 1, 5, 6, 22 | R (17): Text `what`, `max(2, …)` | – |
| `environment.py` | `_from_config` | S: 2–4 | T (3): Muster `maxmemory*` (Rückfall verdeckt den Fehler) | – |
| | `_from_info` | S: 1–6, 9–11, 13, 15–22 | T (17): INFO-Rückfall ungeprüft; R (1): 13 (`cast`-Typ) | – |
| | `probe_eviction` | S: 1, 9 | T (1): 9 (`continue` → `break`); R (1): 1 | – |
| | `_info_section` | S: 1, 5, 11 | T (1): 1 (Frist); R (2) | – |
| | `count_keys` | S: 10, 13, 14, 17, 18 | T (1): 18 (Merker „vollständig“ am Limit); R (4): `COUNT`-Hinweis, Grenze 17 | – |
| | `_text`, `_used_memory` | S: 2; 6, 14 | R (3) | – |
| `execution_guard.py` | `ValkeyExecutionGuardStore.__init__` | S: 1, 16 | I (1): 16 (G); R (1): `_client` ungenutzt | – |
| | `claim` | S: 3, 7, 11, 12, 20, 22, 29 | I (2): 20 (G), 22; R (5): 3 (G), Text, `cast` | – |
| | `renew` | O: 1–17 | I (13): 14, 16 (G); R (4): Text `what` | – |
| | `release` | O: 1–7 | I (6); R (1): 7 (G, `guard_finish.lua` liest ARGV[3] bei `release` nicht) | – |
| | `mark_superseded` | O: 1–6 | I (6): 5 (G) | – |
| | `_finish`, `_decode`, `state_key` | S: 1, 5, 6; 2, 3; O: 1 | R (6): Text, `str`-Zweig, kein Aufrufer | – |
| `locks.py` | `LockManager.acquire` | S: 3, 7, 9 | T (1): 3 (G, nicht getötet); R (2) | – |
| | `LockManager.extend` | S: 3, 17, 27 | I (1): 3 (G); T (1): 27; R (1): 17 (G) | – |
| | `LockManager.release`, `renew` | S: 14; 18 | T (2): Transportfehler würde zu `BackendResponseError` | – |
| | `ValkeyLockStore.renew` | S: 6 | I (1) (G) | – |
| | `force_release` | S: 3, 4, 6 | I (3): 4, 6 (G) | – |
| | `current_owner` | S: 7, 11 | R (2): `str`-Zweig | – |
| | `observe` | O: 1–18, 20–22, 24, 27, 29–47 | I (37): 5, 14, 33, 41, 44 (G); T (3): 15, 27, 29; R (2): 3, 4 (G) | – |
| | `_as_text` | O: 1–6 | I (1): 1; R (5) | – |
| `rate_limit.py` | `RateLimiter.check` | S: 3, 6, 9, 11, 13, 14, 21, 36 | I (5): 11, 14, 21, 36 (G), 13; T (3): Zeitbasis 3 (G), 6, 9 | – |
| | `ValkeyRateLimitStore.check` | S: 14–16 | I (3): 15 (G) | – |
| `script_pipeline.py` | `_send` | S: 3–5, 20, 22, 23, 27, 30, 31 | T (3): 20, 22, 23; R (6) | – |
| | `_drain` | S: 1, 13 | T (1): 13; R (1): 1 | – |
| | `_pack`, `_direct`, `_exchange` | S: 10; 4, 7; 6, 7 | R (5) | – |

Die Einordnung je Mutant steht maschinenlesbar in `<scratchpad>/u5d/einordnung.py` (prüft, dass jeder der 234 Mutanten genau eine Kategorie hat).

### Gegenlauf (Mutanten gegen die übrige Integrationssuite)

Dateien je Gruppe (alle unter `tests/integration/`): **Locks** `test_contracts_valkey.py::TestLockStoreValkey`, `test_lock_lua_scripts.py`, `test_locking_manager.py`, `test_befund_bz25_lock_lease.py`, `stateful/test_lock_machine_valkey.py`. **Guard** `…::TestExecutionGuardStoreValkey`, `test_execution_guard.py`, `stateful/test_retry_chain_machine_valkey.py`. **Rate** `…::TestRateLimitStoreValkey`, `test_rate_limit_lua.py`, `test_rate_limiter_integration.py`, `stateful/test_retry_chain_machine_valkey.py`. **Chord** `…::TestChordStoreValkey`, `test_chord_lua_scripts.py`, `test_workflow_backend.py`, `test_workflow_orchestration.py`, `stateful/test_workflow_machine_valkey.py`. **Conn** `…::TestResultBackendValkey`, `test_injected_pools.py`.

| ID | Mutant | Ergebnis |
|---|---|---|
| L0, G0, R0, C0, N0 | ohne Mutation | grün (54, 37, 31, 45, 35 Tests) |
| L1 | `LockManager.acquire__mutmut_3` | **überlebt** |
| L2 | `LockManager.extend__mutmut_3` | getötet (5 rot, u. a. Lock-Maschine) |
| L3 | `LockManager.extend__mutmut_17` | überlebt (Rauschen) |
| L4 | `ValkeyLockStore.renew__mutmut_6` | getötet (`test_renew_extends_only_for_owner`, Lock-Maschine) |
| L5, L6 | `force_release__mutmut_6`, `_4` | getötet |
| L7–L10, L12 | `observe__mutmut_5/33/41/44/14` | getötet |
| L11 | `observe__mutmut_4` | überlebt (Rauschen) |
| G1, G2 | `ValkeyExecutionGuardStore.renew__mutmut_16/14` | getötet (`test_renew_extends_only_for_the_owner`, `test_renew_after_expiry_fails`) |
| G3 | `release__mutmut_7` | überlebt (äquivalent) |
| G4 | `mark_superseded__mutmut_5` | getötet (nach 60 s) |
| G5 | `claim__mutmut_3` | überlebt (Rauschen) |
| G6, G7 | `claim__mutmut_20`, `__init____mutmut_16` | getötet |
| R1–R5 | `RateLimiter.check__mutmut_11/14/21/36`, `ValkeyRateLimitStore.check__mutmut_15` | getötet (R2 erst nach 241 s durch die Retry-Maschine) |
| R6 | `RateLimiter.check__mutmut_3` | **überlebt** |
| C1–C3 | `record_and_check_complete__mutmut_17`, `record_member__mutmut_3/1` | getötet (u. a. Workflow-Maschine, `body_never_before_the_last_member`) |
| N1 | `ping__mutmut_4` | getötet (`test_connect_is_idempotent`) |
| N2 | `ping__mutmut_3` | überlebt (Rauschen) |

Grenze: Der Mutant wird im Prozess nachgebildet (Funktion im Modul ersetzt), nicht über die Trampoline von mutmut. Für einen Mutanten innerhalb einer Funktion ist das gleichwertig. Die übrigen I-Mutanten sind nicht einzeln nachgestellt, sondern über denselben Vertragstest wie ein belegter Nachbar zugeordnet (z. B. alle `observe`-Mutanten mit falschem Schlüssel oder `None`-Rückgabe).

---

## Teil 3: Verdachtsliste (Pflichtfelder)

| ID | Fundstelle | Mutanten | Szenario (kurz) | Beleg | Quelle der Erwartung | Konfidenz | Abgleich Register / TS |
|---|---|---|---|---|---|---|---|
| R4M-D-01 | `backend/valkey/chords.py:41-68`; `worker/results.py:233-290` | `record_member__mutmut_1–10`, `record_and_check_complete__mutmut_16–18` (Fehlerpfad beim Lesen gefunden) | `ChordManager` gibt rohe valkey-Fehler weiter. Fällt Valkey beim letzten Gruppenglied kurz aus, landet der Fehler im `except Exception` des Workers: Fail-Mark statt Redelivery. Ist Valkey beim Fail-Mark zurück, ist ein erfolgreicher Workflow `failed` | 3 rote Probe-Tests (echter Client gegen geschlossenen Port; OOM-Antwort; ResultTail fail-markt) | Redelivery-Zweig für `BackendConnectionError` im Worker („Transient“); E-040; `errors.md` Reconnect-Zusage; gleiche Regel in allen anderen Stores und im `RateLimitStore`-Vertrag; Lösungsdoku Error-Wrapping 2026-04-15 | hoch (Verhalten), mittel (Häufigkeit) | keine Entscheidung zum Chord-Fehlerpfad; E-039/E-040 nicht berührt; nicht TS-01..03 |
| R4M-D-02 | `backend/valkey/connection.py:256` | – (beim Lesen von `connect__mutmut_27/28`) | `connect()` wirft bei einem Zeitablauf (Blackhole, angehaltener Server) roh `valkey.exceptions.TimeoutError`, und das erst nach rund 3 Minuten | 1 roter Probe-Test (echter Client, stummer Port) | Vertrag `ResultBackend.connect` „Treiberfehler → `BackendConnectionError`“; `errors.md` „Worker startup when Valkey is not reachable“ | hoch | keine Entscheidung; E4 gilt nur für `get_scheduler_leader`; CLI fängt beides |
| R4M-D-03 | `backend/valkey/connection.py:238-261` | – (beim Lesen von `connect`) | Nach gescheitertem `PING` bleibt der Client gesetzt; ein zweiter `connect()` meldet Erfolg ohne `PING` und ohne Eviction-Prüfung, die Startsperre `strict` (E-005) greift dann nicht | 1 roter Probe-Test | Vertrag „Idempotent; Treiberfehler → `BackendConnectionError`“; E-005 | hoch (Verhalten), mittel (Einstufung) | E-005 gilt, seine Tests prüfen nur den ersten `connect()`; keine Regel zu Wiederholung |

---

## Teil 4: Testlücken für den Fix-Plan

Nach Wirkung geordnet. Jeder Punkt ist ein Test gegen das Original, der die genannten Mutanten tötet; Unit-Ebene, wenn nicht anders gesagt.

1. **Fehlerpfade der Koordinations-Stores einheitlich:** Transportfehler, Zeitablauf, heilende Fehlerantwort (`OOM`) und `WRONGTYPE` je Store und je Methode. Die Einordnung eines Transportfehlers als Ausfall prüfen heute nur `LockManager.acquire`, der Guard-Claim (`test_transport_error_is_an_outage`) und `ValkeyRateLimitStore.check`; für `release`/`renew` gibt es nur Tests auf Fehlerantwort und beliebige Ausnahme. Betrifft `LockManager.release__mutmut_14`, `renew__mutmut_18`, `extend__mutmut_27`, `observe__mutmut_27/29` und die Tests zu R4M-D-01 (`ChordManager`). Ohne sie würde ein Transportfehler beim Freigeben als `BackendResponseError` eingeordnet; das Lock-Gate requeut dann, statt zurückzustellen.
2. **INFO-Rückfall der Eviction-Prüfung:** `CONFIG GET` verboten, `INFO memory` liefert eine verdrängende Policy → Status `unsafe` und im Modus `strict` Exit 78 (E-005). Der heutige Test prüft nur „irgendein Hinweis geloggt“. Tötet `_from_info__mutmut_1–22`, `probe_eviction__mutmut_9`, `_from_config__mutmut_2–4` (mit getrennter Prüfung, welche Quelle geantwortet hat). Dazu `_info_section` mit hängendem `INFO` (Frist, `__mutmut_1`) und `count_keys` genau am Limit (`__mutmut_18`).
3. **Zeitbasis aus `TIME`:** `LockManager.acquire` und `RateLimiter.check` reichen `now_ms = s * 1000 + µs // 1000` an das Skript (Mock auf `client.time`, Prüfung der Skriptargumente). Tötet `acquire__mutmut_3`, `RateLimiter.check__mutmut_3/6/9`. Die Integration kann diese Abweichung von bis zu 2 s nicht sehen.
4. **Retry-Politik des eigenen Clients:** `retry` ist ein `Retry` mit `retries=5` und `retry_on_error` enthält `ConnectionError`/`TimeoutError`/`OSError` (`connect__mutmut_4`). Dazu die Tests zu R4M-D-02 und R4M-D-03.
5. **`_decode_setpoint` mit gemischten Typen:** `{"min": "1", "max": 3}`, `{"min": 1, "max": true}` → `SetpointUnreadable` (`__mutmut_15–17`).
6. **Store-Neubau nach `close()`:** `close()`, `connect()`, dann `execution_guard`, `locks`, `chords` liefern neue Stores am neuen Client (`_drop_stores__mutmut_2`).
7. **`ScriptPipeline` bei Transportfehler:** Die Wartenden bekommen den ursprünglichen Fehler (`_send__mutmut_20/22/23`); nach einem Abbruch des Sammlers nimmt die Pipeline neue Aufrufe an (`_drain__mutmut_13`).
8. **Unit-Tests für `chords.py` und die Guard-Methoden `renew`, `release`, `mark_superseded`, `mark_done`, `_finish`** gegen gemockte Skripte (Argumente, Schlüssel, Rückgabe). Die Integration deckt sie über die Vertragstests; auf Unit-Ebene waren in Lauf A 93 Mutanten ohne Test (`chords.py` 42, `execution_guard.py` 51). Niedrige Priorität, weil das Verhalten geprüft ist.
9. **`reconcile_namespace` ohne ausdrücklichen Parameter mit `configured=None`** bei gesetztem Umgebungs-Namensraum (`__mutmut_6`).
10. **`observe` mit einem Semaphor mit mindestens drei Haltern** (`__mutmut_15`).
11. **Auswahl für spätere Integrations-bewertete Läufe** (Methodik, kein Test): `tests/integration/test_contracts_*.py` und die Lua- und Store-Tests der obersten Ebene gehören dazu (`3b-mutation-lauf-b.md`).

Nebenbefunde zum Aufräumen (kein Verdacht): `chords.chord_set_key` und `execution_guard.state_key`/`run_key` haben in `src/` keinen Aufrufer mehr. Die Dauer bis zur Ausnahme bei einem Blackhole-Valkey (rund 3 Minuten, verschachtelte Wiederholung) gehört als Frage zur Retry-Politik beim Start in die Gegenprüfung von R4M-D-02.

---

## Teil 5: Zusammenfassung „Fehler / Testlücke / Rauschen“ je Modul

| Modul | Mutanten | Fehler (Verdacht) | Testlücke | Integration außerhalb B | Rauschen |
|---|---:|---|---:|---:|---:|
| `__init__.py` | 1 | – | 1 | 0 | 0 |
| `chords.py` | 19 | R4M-D-01 (Fehlerpfad) | 0 | 13 | 6 |
| `connection.py` | 22 | R4M-D-02, R4M-D-03 (Fehlerpfad `connect`) | 2 | 1 | 19 |
| `control.py` | 24 | – | 3 | 0 | 21 |
| `environment.py` | 34 | – | 23 | 0 | 11 |
| `execution_guard.py` | 45 | – | 0 | 28 | 17 |
| `locks.py` | 62 | – | 7 | 43 | 12 |
| `rate_limit.py` | 11 | – | 3 | 8 | 0 |
| `script_pipeline.py` | 16 | – | 4 | 0 | 12 |
| **Summe** | **234** | **3 Verdachte** | **43** | **93** | **98** |

Die Mutation hat im Valkey-Backend keine falsche Rechnung und keine falsche Lua-Übergabe gefunden. Was die Unit-Tests auslassen, prüfen die Vertragstests gegen echtes Valkey. Offen sind zwei Dinge: die Fehlerpfade an der Treibergrenze, aus denen alle drei Verdachte stammen, und die Zeitbasis, die kein Test auf die Millisekunde prüft.
