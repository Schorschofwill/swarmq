---
title: Test-Runde – Testlücken ohne Befund-Bezug aus der Fehlersuche Runde 4 – Plan
type: test
date: 2026-10-20
origin: docs/befunde/fehlersuche-runde-4-recherche/3c-sichtung-D.md (Teil 4), 3c-sichtung-E.md (Teil 4), 3c-sichtung-F.md (Testlücken), docs/befunde/fehlersuche-runde-4-2026-10.md (Hinweise der Gegenprüfung, Deferred), docs/plans/2026-10-17-2103-fix-fehlersuche-runde-4-plan.md (KD4, Deferred)
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-plan-bootstrap
execution: code
---

# Test-Runde – Testlücken Runde 4 – Plan

## Goal Capsule

- **Objective:** Die 56 Testlücken ohne Befund-Bezug aus der Mutationssichtung der Runde 4 (D-1…D-10, E-1…E-20, T-F-01…T-F-26) und sechs Zusatzpunkte (X-1…X-6) sind gegen den Stand 0.14.1 eingeordnet. Jede offene Lücke ist danach durch einen Test geschlossen, der gegen das Original grün und gegen einen benannten Mutanten rot ist. Toter Code und das Helfer-Duplikat in `tests/compat` sind entfernt. Die Doku-Abweichungen V-C/V-D sind korrigiert.
- **Means:** Acht Testpakete (U1–U8) in zwei Wellen, jedes in einem eigenen git-Worktree; kein Paket teilt eine Test- oder Quelldatei mit einem anderen. Danach ein Aufräumpaket (U9) und ein Abschluss (U10) für Register, Befundregister und Doku.
- **Authority:** Entscheidungsregister `compound-packs/swarmq-entscheidungen/` > `docs/befunde/README.md` > AGENTS.md (TDD, Qualitäts-Gates, Testports) > dieser Plan > Sichtungen `3c-sichtung-{D,E,F}.md`.
- **Stop conditions:**
  - Ein neuer Test ist gegen das Original rot: Das ist ein Verdacht. Nicht fixen und den Test nicht abschwächen. Den Test als Probe im Scratchpad ablegen und den Fall nach `docs/befunde/README.md` melden (Gegenprüfung).
  - Ein Test ließe sich nur schreiben, wenn ein Test mit `@pytest.mark.entscheidung` geändert oder gelöscht wird: stoppen und Georg fragen.
  - Ein Paket müsste Produktionscode dauerhaft ändern (außer U9): stoppen. Die Test-Runde fixt nichts.
  - `git diff src/` ist nach einer Gegenprobe nicht leer: sofort zurücksetzen (`git checkout -- src/`), erst danach weiterarbeiten.
- **Execution profile:** Strenges TDD nach AGENTS.md mit der Abwandlung für Testlücken (Methodik unten): Test gegen das Original (grün), Mutant einbauen (rot), Mutant entfernen. Umsetzung durch Subagenten. Je Paket ein Review mit den Linsen:
  - Entscheidungs-Abgleich,
  - Mutanten-Gegenprobe tatsächlich ausgeführt,
  - Determinismus und Laufzeit,
  - Verbot 6/7 (keine privaten Methoden, kein Implementierungsdetail).
- **Tail ownership:** Push, MR und Merge erst nach Georgs Ok. Kein Release; die Runde ändert kein Verhalten.

---

## Product Contract

### Summary

Die Mutationssichtung der Runde 4 (Lauf A, Unit-bewertet, Stand 0.12.3) hat 56 Testlücken ohne Befund-Bezug geliefert. Der Fix-Plan der Runde 4 hat mit KD4 nur die Lücken mit Befund-Bezug mitgenommen (Store-Fehlerpfade, Eviction-INFO-Rückfall, `chords.py`, Retry-Politik). Seither hat sich der Code von 0.12.4 bis 0.14.1 stark geändert. Die Bestandsaufnahme vom 2026-10-20 hat jede Lücke gegen den heutigen Code und die heutigen Tests geprüft, mit Probe-Tests gegen das Original. Das Review vom selben Tag hat die Gegenproben gegen die Bestandstests nachgeprüft.

**Ergebnis für die 56 Lücken:** 24 offen, 28 teilweise, 4 geschlossen, 0 entfallen.

**Zusatzpunkte:** X-1, X-2, X-5 und X-6 sind offen. X-3 und X-4 sind toter Code ohne Aufrufer in `src/`.

**Gegengeprüfte Verdachte:**

| Verdacht | Einstufung |
|---|---|
| V-A | NICHT HALTBAR |
| V-B | LÜCKE, Produktfrage P3 |
| V-C, V-D | DOKU P3 |

Einzelheiten unter „Verdachte“.

### Problem Frame

Die Mutationssichtung hat gezeigt, wo Unit-Tests Verhalten nicht festhalten:

- periodische Takte, die nie ohne `request_check` laufen;
- Argumente, die Fakes ignorieren;
- Schleifen mit `continue`, die nur mit einem Element getestet sind;
- Grenzwerte, die nie genau getroffen werden;
- Fehlertexte und JSON-Schlüssel, die CLI und Dashboard lesen.

Einen Teil davon deckt die Integration ab, die Lauf A nicht gemessen hat. Für jede Lücke ist deshalb geprüft, ob ein Unit-Test nötig ist oder ob die Integration genügt.

Seit der Sichtung sind Teile geschlossen worden:

- die Store-Fehlerpfad-Matrix (`tests/unit/backend/test_valkey_store_error_paths.py`, U3 des Fix-Plans und 0.14.1 `b682de0`);
- der Eviction-INFO-Rückfall (`test_befund_bz04_eviction_policy.py`);
- die Retry-Politik des Clients (`test_valkey_handshake_retry.py`);
- die `chords.py`-Unit-Tests (`test_valkey_chords.py`);
- F4-03 (Reattach-TTL), F4-04 (Purge schließt den DLQ-Channel), F4-05 (`hit_limit`);
- PF-6/PF-7 (`test_cli_schedule_add_contract.py`).

Die Tabelle unten nennt für jeden Punkt, was davon übrig ist.

### Requirements

- R1. Jede offene oder teilweise Lücke der Tabelle hat nach der Runde einen Test, der gegen das Original grün ist und gegen den genannten Gegenprobe-Mutanten rot. Der Mutant wurde dazu tatsächlich eingebaut und wieder entfernt. Der Commit-Text nennt je Lücke den Mutanten und das rote Testergebnis.
- R2. Kein Test prüft eine private Methode, wenn die öffentliche API den Fall erreicht (AGENTS.md Verbot 6/7). Wo ein privater Einstieg nötig ist, nennt die Spalte „privat“ das Bestandsmuster bzw. den Seam, und der Docstring des Tests begründet ihn.
- R3. Kein Test mit `@pytest.mark.entscheidung` wird geändert oder gelöscht. Neue Tests, die entschiedenes Verhalten festhalten, tragen den Marker aus der Spalte „Marker neu“. Die Verweise im Register (`tests:` der Regel) trägt U10 nach, damit parallele Pakete keine Register-Datei teilen.
- R4. Unit-Tests bleiben unter 100 ms. Wo Zeit im Spiel ist, gilt die virtuelle Uhr `tests/support/virtual_time.py` oder der FakeClock aus `tests/unit/process_manager/_helpers.py`; kein `asyncio.sleep` in Tests. Ein Test, der dauerhaft länger braucht, gehört nach `tests/integration/` oder trägt eine Begründung (T-F-20).
- R5. Toter Code (X-3, X-4) ist entfernt. Das Duplikat in `tests/compat/test_mixed_operation.py` (X-6) nutzt die Helfer aus `_harness.py`.
- R6. Die Einstufungen der Verdachte stehen im Befundregister (U10). V-C und V-D sind in der Doku korrigiert (U10). Kein Code-Fix in dieser Runde.

### Key Decisions

- KD1. **Paketschnitt nach Testdateien, nicht nach Sichtungsgruppen.** Mehrere Lücken teilen eine Testdatei:
  - `tests/unit/broker/test_rabbitmq.py`: E-11, E-13, E-14, E-15, E-20;
  - `tests/unit/process_manager/test_setpoint.py`: T-F-05…T-F-22, T-F-19 teilweise.

  Jede Testdatei gehört genau einem Paket.
- KD2. **Mutanten sind flüchtig.** Ein Gegenprobe-Mutant wird nur im Worktree des Pakets eingebaut, der Test ausgeführt und der Mutant wieder entfernt; `git diff src/` muss danach leer sein. Deshalb dürfen zwei Pakete denselben Quellcode mutieren. Dauerhaft ändert Quellcode nur U9.
- KD3. **Register-Pflege gebündelt in U10.** Der Guard `tests/unit/test_entscheidungsregister.py` meldet nur unbekannte Marker-IDs, keine Marker ohne Eintrag in `tests:`. Die Pakete setzen die Marker, U10 trägt die Tests in die Regeldateien ein.
- KD4. **Integration genügt, wo ein Unit-Test nur Implementierungsdetails prüfen könnte.**
  - D-8 (Guard-Methoden) und E-3 (unlesbare Bodies) gelten über Vertrag bzw. Integration als geschlossen.
  - D-10 (Semaphor mit drei Haltern) wird als Vertragstest geschrieben, nicht als Prüfung von Lua-Argumenten.
- KD5. **Rauschen bleibt Rauschen.** Äquivalente Mutanten bekommen keinen Test; die Tabelle nennt sie. Beispiele:
  - `fail=None` in aio-pika,
  - `cap=3.0` im Client-Backoff,
  - `supervised=True`,
  - die Referenz auf den abgekoppelten Takt,
  - Default-Slots bei abgerundeter Zahl.
- KD6. **Verdachte werden nicht in der Runde behoben.** Kein Test in der Suite, der V-A oder V-B als Fehler zeigt. Das Verhalten zu V-B entscheidet Georg.

### Scope Boundaries

- Keine Änderung am Produktionscode außer dem Entfernen toten Codes in U9.
- Keine Mutation-Welle 3. Die dorthin verschobenen CLI-Module (`env_check_cmd.py`, `inspect_cmd.py` usw., `3c-sichtung-F.md` „Nach Welle 3 verschoben“) gehören nicht dazu.
- D-11 der Sichtung D (Auswahl für Integrations-bewertete Läufe) ist Methodik, kein Test; er bleibt für Welle 3.
- Doku-Änderungen nur in U10: V-C, V-D und das Befundregister.

#### Deferred to Follow-Up Work

- V-B nach Georgs Entscheidung (Optionen A/B/C, siehe „Verdachte“).
- V-A als optionale P3-Härtung (kein Befund).
- Die fehlenden Status-Felder in `website/reference/cli.md:248-255` (`autoscaling`, `workers`, `ttl_s`).
- Die PDEATHSIG-Nebenwirkung in `tests/unit/process_manager/test_supervisor.py::test_start_worker_unblocks_inherited_signals_before_the_entry`: Der Test setzt `PR_SET_PDEATHSIG` auf dem pytest-Prozess. U6 darf sie mitnehmen, wenn nur diese Datei betroffen ist; sonst Folgearbeit.

### Dependencies

- `main` @ 2c13c48 (0.14.1).
- Teststack auf 5699/6399 für die Integrationsläufe (U1, U2, U9) und für `uv run just all`.
- Die Probe-Tests der Bestandsaufnahme im Scratchpad sind nur Vorlage. Keine Abhängigkeit: Jedes Paket schreibt seine Tests selbst und test-first. Die Belege zu V-A und V-B sichert U10 ins Befundregister.

### Open Questions

Keine blockierenden. Für Georg: V-B (Optionen A/B/C).

### Sources

- **Sichtungen:** `docs/befunde/fehlersuche-runde-4-recherche/3c-sichtung-D.md` (Teil 4), `3c-sichtung-E.md` (Teil 4), `3c-sichtung-F.md` („Testlücken“ und „Echter `execvpe` aus einem Unit-Test“).
- **Befundregister:** `docs/befunde/fehlersuche-runde-4-2026-10.md`:
  - Punkte 10–12 („Vor dem Fix zu entscheiden“),
  - „Hinweise der Gegenprüfung ohne Einstufung“ (Lock-/Leader-Maschine, Stichprobe M, Nebenbefunde),
  - Deferred „Testlücken ohne Befund-Bezug“.
- **Pläne:** `docs/plans/2026-10-17-2103-fix-fehlersuche-runde-4-plan.md` (KD4, U3, U4, Deferred), `docs/plans/2026-10-19-1500-feat-swarmq-0-14-plan.md` (Deferred, U7 Kandidatenliste).
- **Review des Plans und Gegenprüfung der Verdachte:** 2026-10-20.

---

## Planning Contract

### Methodik je Lücke (verbindlich)

1. **Test gegen das Original schreiben.** Ein Test je Verhalten, in der Ziel-Testdatei der Tabelle, über die öffentliche API (sonst der Seam aus der Spalte „privat“). Ausführen mit `uv run --extra test pytest tests/unit/<datei>::<test> -x`. Er muss **grün** sein.
2. **Ist der neue Test gegen das Original rot, ist das ein Verdacht.** Nicht fixen, nicht abschwächen, kein `xfail`. Den Test aus der Suite nehmen und als Probe im Scratchpad ablegen. Den Fall mit Fundstelle, Quelle der Erwartung und Register-Abgleich melden; er durchläuft den Befund-Ablauf mit Gegenprüfung (`docs/befunde/README.md`).
3. **Gegenprobe:** Den Mutanten aus der Spalte „Gegenprobe-Mutant“ von Hand in den Quellcode einbauen, genau an der genannten Stelle (Code-Ausschnitt maßgeblich, Zeile nur Orientierung). Dann **nur die Ziel-Testdatei bzw. den Ziel-Test** ausführen; er **muss rot** sein. Bei mehreren genannten Mutanten jeden einzeln prüfen.
   - **Was als rot zählt:** eine fachliche Assertion. Ebenso ein `AttributeError` oder `TypeError` im beobachteten Aufruf, wenn ein mutmut-typischer Mutant (`= None`, `= ""`) dort endet.
   - **Was nicht zählt:** Import-Fehler, Fehler in fremden Tests und Rot aus `tests/unit/typing/*` (mypy-Vertragsprüfung). Wird ein Mutant nur dort rot, stattdessen „Zeile streichen“ als Mutanten wählen (Beispiel D-6).
   - **Integration und Property:** Die Gegenproben für D-10 laufen über `uv run just integration -k "contracts_valkey"` (zusätzlich über die InMemory-Seite unit), die für X-1/X-2 über `uv run just property -k "lock_machine or leader_machine"`.
   - **Virtuelle Uhr:** Ein Fristen-Mutant wie `asyncio.timeout(None)` oder `_run(None)` endet unter `run_virtual` in einem `RuntimeError` („nichts geplant“), weil die Uhr nicht weiterlaufen kann. Das zählt als rot, wenn der Test genau dieses Hängen erkennen soll. Den Mutanten trotzdem vorziehen, der eine fachliche Assertion trifft (E-10: `:191` vor `:136`).
   - **Warnung X-5:** Der Mutant `reload.py:171` (`or` → `and`) ersetzt ohne Patch den pytest-Prozess per `execvpe`. Die Gegenprobe deshalb nur mit `-k <Einzeltest>` und nur, wenn der neue Patch auf `os.execvpe` schon im Test steht. Nie die ganze Datei oder Suite mit eingebautem Mutanten laufen lassen.
4. **Mutanten entfernen:** `git checkout -- src/`, danach muss `git diff src/` leer sein. Erst dann geht es weiter.
5. **Alles grün:** `uv run just all` (Testports und Sperre setzt das Rezept). Integrationstests nur über `uv run just integration …`, nie direkt gegen 5672/6379.
6. **Commit-Text** nennt je Lücke die ID, den Test, den Mutanten und das Ergebnis der Gegenprobe (rot an welcher Assertion).
7. **Weitere Regeln:**
   - Keine Tests für private Methoden, wo die öffentliche API den Fall erreicht (Verbot 6/7).
   - Tests mit `@pytest.mark.entscheidung` nie ändern; neue Tests daneben stellen.
   - Kein `asyncio.sleep` in Tests.
   - Unit-Tests unter 100 ms.
   - `@pytest.mark.timeout(5)` für async Tests.
   - Vor dem Commit `uv run just lint` und `uv run just types`.

### Key Technical Decisions

- KTD1. **Worktrees:** je Paket `../swarmq-tl-u<n>`, Branch `test/testluecken-r4-u<n>` von `main` @ 2c13c48. Integrationsläufe aus verschiedenen Worktrees serialisiert die gemeinsame Testsperre. Vor einem Lauf `pgrep -af pytest` prüfen, weil sich die Sitzungen die Testcontainer teilen.
- KTD2. **Commits:** ein Commit je Lücke oder je zusammenhängender Lückengruppe (AGENTS.md erlaubt mehrere Zyklen in einem Commit). Je Paket ein MR.
- KTD3. **Gegenprobe ohne mutmut,** von Hand im Worktree. Ein Ersatz-Plugin, das nur Funktionen im Modulnamensraum ersetzt, greift nicht bei per monkeypatch ersetzten Modul-Globals (z. B. `supervisor.time`) und ist kein Ersatz.
- KTD4. **Neue Testdateien nur, wo keine passende existiert:**
  - `tests/unit/process_manager/test_autoscale.py` (T-F-10),
  - `tests/unit/process_manager/test_worker_target.py` (T-F-26),
  - wahlweise `tests/unit/test_cli_schedule_fire_time.py` (T-F-15).

### Tabelle aller Lücken

**Spalten:**

| Spalte | Inhalt |
|---|---|
| Status | offen / teilweise / geschlossen / entfallen |
| Fundstelle | Datei:Zeile auf 2c13c48 |
| Ziel | U = Unit, I = Integration, P = Property/stateful |
| Gegenprobe-Mutant | Code-Ausschnitt maßgeblich |
| Register | berührte E-nnn; M = Marker-Tests vorhanden, nicht ändern |
| Marker neu | E-nnn für neue Tests oder – |
| privat | nein, oder ja mit Bestandsmuster/Seam |
| Aufwand | S/M/L |

#### Gruppe D (Valkey-Backend)

| ID | Status | Fundstelle heute | Was offen ist / schließender Test | Ziel-Testdatei | Gegenprobe-Mutant | Register | Marker neu | privat | Aufw. | Paket |
|---|---|---|---|---|---|---|---|---|---|---|
| D-1 | teilweise | `backend/valkey/locks.py:58-217`, `execution_guard.py:56-62,96-106` (`peek`), `rate_limit.py:78-87`, `chords.py:62-70`, `control.py:31-39`, `connection.py:80` | **Geschlossen:** Matrix `tests/unit/backend/test_valkey_store_error_paths.py::test_driver_outage_surfaces_as_backend_connection_error`, `::test_wrongtype_reply_surfaces_as_backend_response_error`. Teil (b) „reiner `OSError`“ ist ebenfalls geschlossen: `connection.py:80` stirbt an `test_valkey_execution_guard.py::test_transport_error_is_an_outage` und `test_valkey_schedules.py::…::test_scan_transport_error_is_a_plain_outage[error1]`. **Offen:** (a) `ValkeyExecutionGuardStore.peek` (neu seit F4-02) fehlt in der Matrix. Das Gate fängt nur `BackendConnectionError` (`worker/gates/execution_guard.py:173`) | `tests/unit/backend/test_valkey_store_error_paths.py` (U) | `execution_guard.py:99-101` `await self._run("peek", self._client.hmget(...))` → `await self._client.hmget(...)` | E-079, E-081 (`current_owner` bleibt roh, nicht in die Matrix), E-077 | E-079 | nein | S | U1 |
| D-2 | teilweise | `backend/valkey/environment.py:38-41` (`_from_config`), `:44-47` (`_from_info`), `:58-77` (`probe_eviction`), `:101-107` (`_info_section`), `:138-159` (`count_keys`) | **Geschlossen:** Policy aus INFO und `strict` → Exit 78 (`test_befund_bz04_eviction_policy.py::test_connect_tolerates_forbidden_config_get_but_says_so`, `::test_strict_refuses_to_start_when_only_info_reveals_an_evicting_policy`). **Offen:** `maxmemory` aus INFO; Muster `maxmemory*` mit getrennter Quelle; Frist von `_info_section`; `count_keys` genau am Limit | `tests/unit/backend/test_valkey_environment.py` (U) | `environment.py:47` `values.get("maxmemory")` → `values.get("MAXMEMORY")`; `:39` `"maxmemory*"` → `"XXmaxmemory*XX"` (Fake beachtet das Muster per fnmatch); `:153` `return limit, True` → `return limit, False`; Frist: `:103` `asyncio.timeout(_PROBE_TIMEOUT_S)` → `asyncio.timeout(None)` (unter `run_virtual` rot als `RuntimeError` „nichts geplant“, siehe Methodik) | E-005 (M), E-006 (M) | E-005 (Teil `maxmemory`) | nein (`probe_eviction`, `describe_valkey`, `count_keys` sind öffentliche Modulfunktionen) | S | U1 |
| D-3 | offen | `locks.py:59-60` (`LockManager.acquire`), `rate_limit.py:47-50` (`RateLimiter.check`) | Kein Test prüft `now_ms = s*1000 + µs//1000` | `tests/unit/backend/test_valkey_locks.py`, `tests/unit/backend/test_valkey_rate_limit.py` (U: `client.time` → `(1_700_000_000, 999_999)`, Skriptargument `1_700_000_000_999` als `int`) | `locks.py:60` und `rate_limit.py:48` `+ int(microseconds) // 1000` → `- int(microseconds) // 1000` | E-070, E-022 (nicht berührt) | – | nein | S | U1 |
| D-4 | geschlossen | `backend/valkey/connection.py:41-45`, `:489-500` (`_build_client`) | `tests/unit/backend/test_valkey_handshake_retry.py::test_a_command_whose_connection_drops_is_retried_after_backoff`, `::test_refused_tcp_is_retried_five_times_without_outer_retry` (E-075), `tests/unit/backend/test_valkey.py::TestValkeyBackendConnect::test_connect_creates_client_and_pings`. Jedes einzelne Streichen von `ConnectionError`, `TimeoutError` oder `OSError` aus `retry_on_error` wird getötet. Äquivalent ist nur `cap=3.0` statt 2 (0,05·2⁵ = 1,6 s erreicht den Deckel nie) | – | – | E-075 (M), E-082 | – | – | – | – |
| D-5 | offen | `backend/valkey/control.py:78-89`, Prüfung `:85` (`_decode_setpoint`) | Nur Nicht-JSON getestet | `tests/unit/backend/test_valkey_control.py` (U, über `read_setpoint`, parametrisiert: `{"min":"1","max":3}`, `{"min":1,"max":true}`, `{"min":true,"max":3}`, `{"min":1,"max":"3"}` → `SetpointUnreadable`) | `control.py:85` erstes `or` → `and`; alternativ `or not isinstance(high, int)` streichen | – | – | nein | S | U1 |
| D-6 | offen | `backend/valkey/__init__.py:83-85` (`_drop_stores`), Aufruf `:68-72` | `test_valkey_connect_concurrency.py::test_connect_after_close_builds_a_new_client` prüft nur den Client | `tests/unit/backend/test_valkey.py` (U: `from_url` → `[old, new]`; nach `connect`/`close`/`connect` trifft `backend.execution_guard.peek(...)` `new.hmget`; ebenso `locks`, `rate_limit`, `chords`, `schedules`) | `__init__.py:85` Zeile `self._execution_guard = None` streichen (je Store einzeln) | E-082 | E-082 | nein | S | U1 |
| D-7 | teilweise | `backend/valkey/script_pipeline.py:89-93,101-103` (`_send`), `:78-83` (`_drain`) | **Geschlossen:** `:102` (`test_valkey_store_error_paths.py:173`) und `:92` (`test_valkey.py::TestValkeyBackendClose::test_a_store_fetched_before_close_opens_no_connection_after_it`). **Offen:** Nach einem Abbruch des Sammlers nimmt die Pipeline neue Aufrufe an | `tests/unit/backend/test_valkey_script_pipeline.py` (U: Muster `test_cancelled_exchange_drops_the_connection_and_fails_the_waiters`, danach liefert ein neuer `call` eine Antwort) | `script_pipeline.py:82` `self._pending = []` → Zeile streichen | – | – | nein | S | U1 |
| D-8 | geschlossen | `chords.py:43-81`, `execution_guard.py:80-110` | Chords: `tests/unit/backend/test_valkey_chords.py`. Guard-Methoden: Vertrag `tests/contracts/execution_guard.py` gegen Valkey in `tests/integration/test_contracts_valkey.py::TestExecutionGuardStoreValkey` (`test_renew_extends_only_for_the_owner`, `test_renew_after_expiry_fails`, `test_release_by_owner_frees_without_counting_an_orphan`, `test_superseded_counter_only_grows`); KD4 | – | – | E-077, E-052 | – | – | – | – |
| D-9 | offen | `backend/valkey/connection.py:401-418`, Zweig `:408-411`; `__init__.py:74-81` | `reconcile_namespace` wird in `tests/` nicht aufgerufen | `tests/unit/backend/test_valkey_namespace.py` (U: `SWARMQ_NAMESPACE=app1`, `ValkeyBackend(url).reconcile_namespace(None)` → `None`, danach `namespace is None`, `notify_channel_name("t1") == "sq:notify:t1"`) | `connection.py:410` `_keyspace(configured or "")` → `_keyspace(configured)` | E-012 (M) | E-012 | nein | S | U1 |
| D-10 | offen | `backend/valkey/locks.py:211` (`pipe.zrange(key, 0, -1)` in `observe`) | `tests/contracts/coordination.py::LockStoreContract::test_observe_reports_holders_and_remaining_ttl` hat nur zwei Halter | `tests/contracts/coordination.py` (neuer Vertragstest mit Ownern a/b/c bei `max_concurrency=3`; U gegen InMemory, I in `tests/integration/test_contracts_valkey.py::TestLockStoreValkey`) | `locks.py:211` `zrange(key, 0, -1)` → `zrange(key, 0, 1)`; Gegenprobe über `uv run just integration -k "contracts_valkey"` | E-007 | – | nein | S | U1 |

#### Gruppe E (Broker und Client)

| ID | Status | Fundstelle heute | Was offen ist / schließender Test | Ziel-Testdatei | Gegenprobe-Mutant | Register | Marker neu | privat | Aufw. | Paket |
|---|---|---|---|---|---|---|---|---|---|---|
| E-1 | teilweise | `broker/rabbitmq/admin.py:122-136` (`_dlq_queue`), `:247`, `:281` | **Geschlossen:** Purge (F4-04), Schließen nach Lesefehler (`test_dlq_read.py::test_own_channel_is_closed_even_when_reading_fails`), Queue-Name und `fail` über Integration. **Offen:** Verbindung geschlossen → kein Channel; fehlende XQ → Channel wird geschlossen und die Ausnahme fliegt. Den Ausnahmetyp **nicht** festschreiben (heute roher `ChannelNotFoundEntity`) | `tests/unit/broker/test_dlq_read.py` (U, Fake an der Verbindungsgrenze wie F4-04, über `list_dead_letters`) | `admin.py:129` `connection is None or connection.is_closed` → `and`; `:135` `close_channel_quietly(channel)` → `close_channel_quietly(None)` | E-028 (M) | – | nein | S | U3 |
| E-2 | teilweise | `admin.py:299` (Treffer), `:341` (`to_main`), `:515` (inspect), `:535` (retry) | **Geschlossen:** `test_dlq_read.py::test_inspect_miss_reports_hit_limit_when_an_expired_entry_used_up_a_read`, `::test_retry_miss_reports_hit_limit_and_a_hit_does_not`, Integration `test_rabbitmq_dlq.py::test_inspect_returns_full_payload_on_match`, `::test_retry_moves_matching_message_to_main`, `::test_retry_missing_returns_false_and_leaves_dlq_intact`; der Mutant an `:535` ist bereits getötet. **Offen:** Abbruch nach dem Treffer bei inspect (Treffer in der Mitte); Routing bei retry (Vorgänger in die XQ, Treffer in die Haupt-Queue) | `tests/unit/broker/test_dlq_read.py` (U: `t-0,t-1,t-2`; inspect `t-1` → `scanned == 2`; retry `t-1` → `t-0` in der XQ, `t-1` in der Haupt-Queue) | `:515` `break_after_first_match=True` → `False`; `:341` `to_main = move_to_main and (match_task_id is None or is_match)` → `… or is_match or True)` | E-028 (M) | E-028 | nein | S | U3 |
| E-3 | geschlossen | `admin.py:342-343`, `:304`, `:553-557` | Integration `tests/integration/broker/test_rabbitmq_dlq.py::test_retry_all_skips_malformed_bodies`; ein Unit-Test ist optional (KD4) | – | – | E-028 | – | – | – | – |
| E-4 | teilweise | `admin.py:353`, `:362` (`delivery_mode=PERSISTENT`), `:408` | **Geschlossen:** Zurücksetzen des Retry-Zählers (`test_rabbitmq_dlq.py::test_retry_resets_retry_count_header`). **Offen:** `PERSISTENT` der Kopien | `tests/unit/broker/test_dlq_read.py` (U) | `admin.py:353` bzw. `:362` `delivery_mode=…PERSISTENT` streichen | E-028, E-054 | – | nein | S | U3 |
| E-5 | teilweise | `admin.py:290-298`, `:488-494`, `:160-164` | **Geschlossen über Integration:** die Schlüssel außer `body_raw`. **Offen:** `body_raw` (liest `cli/dlq_cmd.py:183`); Typprüfung von `task.id`/`task.name` | `tests/unit/broker/test_dlq_read.py` (U: genaue Schlüsselmengen; id/name als Nicht-String → `None`) | `admin.py:296` `"body_raw"` → `"BODY_RAW"`; `:162` `isinstance(task_id, str)` → `isinstance(task_id, str) or True` | E-028 | – | nein | S | U3 |
| E-6 | offen | `admin.py:51`, `:63`, `:88`, `:465`, `:179`, `:296` | `test_rabbitmq.py::…::test_invalid_timestamp_counts_as_missing` tötet `:51` nicht. bytes-Schlüssel kommen mit pamqp 4.0.1 nicht vor (niedrigste Priorität) | `tests/unit/broker/test_dead_letter_ttl_header.py` (Zeitstempel/Frist), `tests/unit/broker/test_dlq_generation.py` (Generation als float), `tests/unit/broker/test_dlq_read.py` (Binär-Body, bytes-Schlüssel) (U) | `:51` `or` → `and`; `:63` `or` → `and`; `:88` `value <= 0` → `value < 0`; `:465` `remaining <= 0` → `remaining < 0`; `:296` `errors="replace"` → `errors="strict"`; `:179` bytes-Zweig ohne `decode` | E-028, E-051 | E-028 (`:51`) | nein | M | U3 |
| E-7 | teilweise | `client/workflows.py:64-159` (`apply`), Publish über `workflow_enqueue.py:39-46,112-114` | **Unit prüft nur** `ttl_ms` des Zustands; Teile decken Property-Maschine und Integration. **Offen auf Unit-Ebene:** Queue, Body (`task_id`, `args`, `kwargs`), Header `x-swarmq-workflow-id`/`-step`/`-enqueued-at`, eindeutige `workflow_id`, `step_tasks`, `op_timeout` | `tests/unit/client/test_workflows.py` (U) | `workflows.py:117` `kwargs=signature.kwargs` → `kwargs={}`; `:120` `HEADER_WORKFLOW_STEP: step_path` → `: None`; `:139` `op_timeout=self._backend_op_timeout` → `op_timeout=None` (`:86` ist bereits getötet) | E-059, E-039, E-020 (M in `test_workflows.py:234-282`) | E-020 (nur wo Cancel-Flag/TTL berührt) | nein | S | U5 |
| E-8 | teilweise | `client/workflows.py:192-210` (`workflow`) | **Geschlossen:** TTL-Teil (F4-03); `:201` `is not None` ist getötet (`test_app.py::TestSwarmQWorkflowCancelFlagTtl`). **Offen auf Unit-Ebene:** `WorkflowNotFound`, `op_timeout` beim Lesen, Handle-ID | `tests/unit/client/test_workflows.py` (U) | `:201` `if raw is None:` → `if False:`; `:200` `op_timeout=self._backend_op_timeout` → `None`; `:208` `workflow_id` (Handle) → `None` | E-020 (M) | E-020 | nein | S | U5 |
| E-9 | teilweise | `client/workflows.py:161-190` (`_mark_apply_failed`, über `apply`) | **Geschlossen:** `expected_version`, `version`, `status`, Text. **Offen:** `error["type"]`; `get_workflow_state(id, op_timeout=…)`; CAS mit `ttl_ms`/`op_timeout` | `tests/unit/client/test_workflows.py` (U, über `apply` mit `Client(backend_op_timeout=…)`) | `:186` `ttl_ms=state_ttl_ms` → `ttl_ms=None`; `:175` `op_timeout=…` → `None`; `:169` `"type"` → `"XXtypeXX"` | E-040 | – | nein | S | U5 |
| E-10 | teilweise | `broker/rabbitmq/consumer_watch.py:135-136` (`start`), `:189-201` (`_run`), `:175`, `:262`, `:332`, `:339-340`, `:156-157` | Takt nur über Integration (`test_consumer_timeout.py`, `test_consumed_queue_deleted.py`); `tick:264` gedeckt. **Offen auf Unit-Ebene:** Takt ohne `request_check`; `cancel_all`/`tick` nach Eintrag ohne Tag; `seen_on`/`missing_ticks` nach Neuanmeldung; späte Antwort nach `stop()` bei geführtem Eintrag. **Rauschen:** Referenz auf den abgekoppelten Takt (KD5). Kein Test zum Neustart nach abgekoppeltem Takt (V-A) | `tests/unit/broker/test_consumer_watch.py`; späte Antwort: `tests/unit/broker/test_consumer_watch_shutdown.py` (U, virtuelle Uhr) | Takt: zuerst `:191` `suppress(TimeoutError)` → `suppress(None)` (fachlich rot), nur hilfsweise `:136` `self._run(interval_s)` → `self._run(None)`; `:175` `continue` → `break`; `:262` `continue` → `break`; `:339` `entry.seen_on = underlay` → Zeile streichen; `:340` `entry.missing_ticks = 0` → `= 1`; `:332` `self._stopping or entry not in self._entries` → `and` | E-053 (M), E-083 (M), E-019 | E-053 (späte Antwort), E-083 (Neuanmeldung, wo die gelöschte Queue berührt ist) | nein | M | U4 |
| E-11 | teilweise | `broker/rabbitmq/connection.py:688-752` (`_ensure_channel_tracked`), `:707` (`ready()`-Frist, gilt für Publish und Consume) | **Geschlossen:** Neuöffnen geschlossener Kanäle, offener Kanal nicht ersetzt, Öffnungsfrist (F4-20/22/24, `test_publish_channel_closing.py`, `test_publish_connection_loss.py`). **Offen:** Frist auf `connection.ready()` ohne äußere Publish-Frist (Consume-Weg) | `tests/unit/broker/test_rabbitmq.py` (U) | `connection.py:707` `timeout=_CHANNEL_READY_TIMEOUT_S` → `timeout=None` | E-078 (M), E-025 | – | nein | S | U4 |
| E-12 | teilweise | `connection.py:517-534` (`set_publish_deadline`), `:536-578`, `:580-584`, `:623-651`, `:436-451` | **Geschlossen:** Hängende Close-Tasks werden abgebrochen (`test_rabbitmq.py::TestChannelCloseAfterUnblocked::test_closing_the_broker_during_the_wait_cancels_the_close_without_leftovers`). **Offen:** `_pending_channel_closes` schrumpft (weakref + gc); Frist der Sperre beim Verwurf (`publish_channel_discard_skipped`); Frist des Close; Nachziehen nach abgelaufenem Deckel | `tests/unit/broker/test_publish_deadline.py`, `tests/unit/broker/test_shutdown_deadline.py` (U) | `:584` `task.add_done_callback(self._pending_channel_closes.discard)` streichen; `:553` `timeout=_CHANNEL_CLOSE_TIMEOUT_S` → `None`; `:638` `asyncio.timeout(_CHANNEL_CLOSE_TIMEOUT_S)` → `asyncio.timeout(None)`; `:534` `continue` → `break` | E-078, E-025, E-041 (M) | – | `:534` ja: deterministisch nur über das private `_active_publish_timeouts` (Set-Reihenfolge nicht steuerbar), sonst Rauschen; Sperre über das private `_channel_lock` belegen (Seam an der Grenze); Rest nein | M | U4 |
| E-13 | teilweise | `connection.py:384` (`_connect_injected`), `:415`, `:490` | **Geschlossen:** `close` über Integration `test_injected_pools.py::test_injected_pool_roundtrip_releases_to_pool`; `:415` mit echtem aio-pika äquivalent. **Offen:** `_connect_injected` mit echtem `__aexit__` | `tests/unit/broker/test_rabbitmq.py` (U, echter `aio_pika.pool.Pool` oder `create_autospec(PoolItemContextManager)`) | `:384` `__aexit__(type(exc), exc, exc.__traceback__)` → `__aexit__(type(exc), exc)` | – | – | nein | S | U4 |
| E-14 | teilweise | `connection.py:453-467` (`close`), `:213` (Default `publish_timeout_s`) | Der Bestand prüft nur das private `_connection is None`. **Offen:** close → connect → consume auf neuem Kanal; Default | `tests/unit/broker/test_publish_connection.py`, `tests/unit/broker/test_rabbitmq.py` (U) | `:462` `self._consume_channel = None` streichen; `:213` `= _DEFAULT_PUBLISH_TIMEOUT_S` → `= None` | E-025 (M) | – | nein | S | U4 |
| E-15 | offen | `connection.py:424-434` (`ping`) | Nur Integration (`test_contracts_rabbitmq.py::TestBrokerRabbitMQ::test_ping_*`) | `tests/unit/broker/test_rabbitmq.py` (U) | `:430` `timeout=_PING_TIMEOUT_S` → `None`; `:427` `or self._connection.is_closed` streichen; `:434` `str(exc)` → `str(None)` | – | – | nein | S | U4 |
| E-16 | offen | `broker/rabbitmq/declarations.py:243-252` (`invalidate_declaration`, `:250`) | Keine zweite Rückstellung nach Invalidierung getestet. V-B im Umfeld: Der Test nutzt einen **Kanalfehler**, keinen Verbindungsabriss | `tests/unit/broker/test_delay_tiers.py` (U) | `:250` `self._ready_delay_tiers.discard(name)` → `discard(None)` | E-026, E-002 (M) | E-026 | nein | S | U3 |
| E-17 | teilweise | `broker/rabbitmq/arg_conflict.py:79-80`, `:107`, `:110`, `:132-133`, `:163-176`, `:197-199`, `:225` | **Offen:** Schließen bei werfendem oder hängendem `close()`; Fristen; `durable=True`; `recognized_mismatch` ohne `received`/`current`; Log-Ebene der zweiten Übernahme. Der Test zur DEBUG-Ebene der zweiten Übernahme tötet auch die `log=None`-Mutanten, die die Sichtung als Rauschen führte | `tests/unit/broker/test_declare_adoption.py`, `tests/unit/broker/test_queue_arg_conflict.py` (U) | `:132` `suppress(Exception)` → `suppress(None)`; `:133` `timeout=timeout_s` → `None`; `:110` `durable=True` → `False`; `:79` `None if received is None` → `None if received is None and False`; `:225` `self._info_logged = True` → `False` | E-016 (M), E-017 | E-016 | nein | M | U3 |
| E-18 | offen | `broker/rabbitmq/header_guard.py:33`, `:37`; `redeclare.py:59`; `declarations.py:116` (bereite Stufen nicht neu deklarieren) | Keine Grenzwerttests; kein `redeclare` bei geschlossener Verbindung; keine Zählung der Stufen nach unroutbarem Publish | `tests/unit/broker/test_header_guard.py`, `tests/unit/broker/test_unroutable.py`, `tests/unit/broker/test_delay_tiers.py` (U) | `header_guard.py:33` `<= limit` → `< limit`; `:37` `>` → `>=`; `redeclare.py:59` `or` → `and`; `declarations.py:116` `tier not in self._ready_delay_tiers and tier not in self._unavailable_delay_tiers` → `or` | E-002 (M), E-026 | E-026 (`:116`) | nein | S | U3 |
| E-19 | teilweise | `client/results.py:177-188`, `:65-84`, `:86-108`, `:233-251`; `client/workflows.py:212-229` (`_unpack`) | **Offen:** Weitergabe in `run`; `task_id` an `get_result`/`get_progress`; Cancel-Ergebnis ohne `task`. Der Fall ohne `fire_ts_ms` ist praktisch unerreichbar (niedrig) | `tests/unit/client/test_results.py`; `_unpack` über `WorkflowHandle` in `tests/unit/client/test_workflows.py` (U) | `results.py:187` `queue=queue` → `queue=None`; `:81` `task_id` → `None`; `:102` `task_id` → `None`; `:246` `if definition.fire_ts_ms is not None:` → `if True:`; `workflows.py:224` `data.get("task", {})` → `data.get("task", None)` | E-020, E-068 (M), E-030, E-064 | – | nein | S | U5 |
| E-20 | offen | `connection.py:202/205`, `:327`, `:343`, `:389`, `:779/794`; `admin.py:629/648` (`_passive_declarations`) | **Teilweise per `match=` gedeckt.** **Offen:** `:327`, `:389`, `:779`, `:794`, `admin.py:648`, Quellenliste in `:205`. Niedrige Priorität | `tests/unit/broker/test_rabbitmq.py`, `tests/unit/broker/test_publish_connection.py`, `tests/unit/broker/test_queue_status.py` (U) | `connection.py:794` `BrokerConnectionError(str(exc))` → `BrokerConnectionError(str(None))`; `:205` `', '.join(given)` → `'XX, XX'.join(given)` | – | – | nein | S | U4 |

#### Gruppe F (Prozessmanager und CLI)

| ID | Status | Fundstelle heute | Was offen ist / schließender Test | Ziel-Testdatei | Gegenprobe-Mutant | Register | Marker neu | privat | Aufw. | Paket |
|---|---|---|---|---|---|---|---|---|---|---|
| T-F-01 | offen | `process_manager/restart_budget.py:76`, `:89` (`_check_heartbeats`) | Kein Test mit zu überspringendem Worker vor einem hängenden | `tests/unit/process_manager/test_supervisor.py` (TestHeartbeatReplacement, U) | `:76` `continue` → `break`; `:89` `continue` → `break` | E-018-Umfeld | – | ja: `pm._check_heartbeats()` ist der Bestands-Seam von TestHeartbeatReplacement; ein öffentlicher Weg ginge nur über den Monitor-Loop mit echten Prozessen | S | U6 |
| T-F-02 | offen | `restart_budget.py:146`, `:153` (`_escalate_terminating`) | Kein Test mit zwei überfälligen Einträgen | `tests/unit/process_manager/test_supervisor.py` (U) | `:146` `continue` → `break`; `:153` `continue` → `break` | E-024 | – | ja: Einstieg über `_check_heartbeats`/`_replace_hung_worker` wie im Bestand (FakeClock-Seam) | S | U6 |
| T-F-03 | offen | `restart_budget.py:126` (`_replace_hung_worker`) | `test_slot_cooldown.py::test_hang_budget_locks_for_the_cooldown_and_the_predecessor_keeps_the_slot` prüft nur sofort und danach | `tests/unit/process_manager/test_slot_cooldown.py` (U: Uhr auf `SLOT_COOLDOWN_S - 1`, Slot noch LOCKED) | `:126` `now + self.SLOT_COOLDOWN_S` → `now - self.SLOT_COOLDOWN_S` | E-024 (M) | E-024 | ja: `_replace_hung_worker` und `_unlock_expired_slots` sind die Seams der Marker-Tests in `test_slot_cooldown.py` | S | U6 |
| T-F-04 | offen | `restart_budget.py:62` (`_heartbeat_deadline`, Zweig Recycling) | Der Bestand nutzt 70 s Stale, das liegt unter 95 | `tests/unit/process_manager/test_befund_f3_13_recycle_heartbeat_deadline.py` (U: Recycler mit Stale 100 s bei Gnade 100 s wird nicht getötet) | `:62` `self.shutdown_grace_s + self.SHUTDOWN_TERM_S` → `self.shutdown_grace_s - self.SHUTDOWN_TERM_S` | E-018 (M) | E-018 | wie Bestand der Datei (`_check_heartbeats`) | S | U6 |
| T-F-05 | teilweise | `process_manager/setpoint.py:89-126` (`apply`) | **Geschlossen:** `:126` (`test_setpoint.py::TestTick::test_autoscaler_makes_no_decision_in_the_round_of_a_change`). **Offen:** fälschlich `True` bei unread, absent, unreadable, rejected und bei **demselben Sollwert ein zweites Mal gelesen** (`:121-122`); fälschlich `False` bei entferntem Sollwert (`:105`), mit Grenzen, bei denen `reconcile` nichts tut. **Kein Test** auf „Sollwert gleich den Startgrenzen“: Der liefert heute bewusst `True` (Quellenwechsel), ein Test darauf wäre ein Fehlalarm | `tests/unit/process_manager/test_setpoint.py` (U: Rückstau 10 000; danach muss der Autoskalier-Takt in derselben Runde hochskalieren bzw. nach `:105` nicht) | `return False` → `return True` in `:96`, `:106`, `:112`, `:118`, `:122`; `:105` `return True` → `return False` | Kontrollkanal KTD3/R7 | – | ja: `pm._autoscale_tick()` ist der Bestands-Seam von `TestTick` | S | U7 |
| T-F-06 | offen | `setpoint.py:210` (`status_ttl_s`) | `TestStatus` setzt das Intervall auf 0. Berührt den offenen Punkt in E-018 (`e018…md:64`): **nur relational prüfen** (TTL > `shutdown_grace_s + SHUTDOWN_TERM_S` mit Default-Intervall), keine exakte Formel | `tests/unit/process_manager/test_setpoint.py` (U) | `:210` `+ pm.SETPOINT_SAMPLE_INTERVAL` → `- pm.SETPOINT_SAMPLE_INTERVAL` | E-018 (offener Punkt) | – (bewusst kein Marker) | nein | S | U7 |
| T-F-07 | offen | `setpoint.py:229` (`autoscaling`), `:233` (`reported_at`) | Kein Test liest `autoscaling` oder `reported_at` | `tests/unit/process_manager/test_setpoint.py` (U, parametrisiert) | `:229` `and low != high` → `or low != high`; `:233` `datetime.now(UTC)` → `datetime.now()` | R14/R15, `cli.md:253` | – | nein | S | U7 |
| T-F-08 | offen | `process_manager/supervisor.py:538-539` (`_monitor_loop`) | Kein Monitor-Loop-Test mit Autoskalierung | `tests/unit/process_manager/test_supervisor.py` (U: Rückstau 10 000, eine Runde, `time.sleep` beendet die Schleife; 2 Worker) | `:538` `self._autoscale is not None` → `is None` | R4.7 | – | ja: `_monitor_loop` mit gepatchtem `time.sleep` als Abbruch (Bestandsmuster der Monitor-Loop-Tests); `run()` würde echte Prozesse und Signale brauchen | S | U6 |
| T-F-09 | geschlossen | `process_manager/reload.py:180` (`reexec`, `finally`) | `test_reload.py::test_a_sigterm_taken_by_another_thread_is_pending_again_before_execve` tötet den Mutanten (`RecursionError`). Optionale Härtung in U6: `signal.raise_signal` als Rekorder patchen, damit der Mutant an einer Assertion statt an der Rekursion rot wird | (optional) `tests/unit/process_manager/test_reload.py` | – | KTD8 | – | – | – | (U6 optional) |
| T-F-10 | offen | `process_manager/autoscale.py:77`, `:85`, `:87`, `:88`; `setpoint.py:84`, `:147`, `:149` | **Gedeckt:** `autoscale.py:76`, `setpoint.py:143/148`. **Offen:** kein Test fährt zwei Takte nach einer Entscheidung; Leerlauffrist ungeprüft | `tests/unit/process_manager/test_autoscale.py` (neu, FakeClock auf `autoscale.time`); Setpoint-Teil in `tests/unit/process_manager/test_setpoint.py` (U) | `autoscale.py:85` `now - pm._idle_since` → `now + pm._idle_since`; `:87` `= now` → `= None`; `:77`/`:88` `= None` → `= ""`; `setpoint.py:84` `= time.monotonic()` → `= None`; `:147` `= now` → `= None`; `:149` `= None` → `= ""` (Ende in `TypeError` im zweiten Takt zählt als rot) | R4.7, Kontrollkanal | – | ja: `autoscale.tick(pm)` bzw. `pm._autoscale_tick()` wie `TestProcessManagerAutoscale` | M | U7 |
| T-F-11 | offen | `cli/check_cmd.py:127` (`_run_worker_check`; `:166`, `:173`, `:181`); `process_manager/check.py:343` | `TestPreflightCommandAgrees` deckt nur `--processes` und Ports | `tests/unit/test_cli_multiprocess.py` (neue Klasse, U, über `main([... "--check"])`) | `check_cmd.py:173` `autoscale=getattr(args, "autoscale", None)` → `autoscale=None`; `check.py:343` `autoscale=autoscale` → `None`; `check_cmd.py:181` `configure=…` → `None`; `:166` `watch_dir=…` → `None` | E-062 (M), E-069 | E-062, E-069 (`--configure`) | nein | S | U8 |
| T-F-12 | offen | `check_cmd.py:220`, `:250` | Scheinabdeckung: `test_cli_residuals.py::…::test_none_argv_uses_sys_argv` prüft nur `isinstance(result, bool)`, ohne Marker | `tests/unit/test_cli_residuals.py` (U: `sys.argv = ["swarmq","worker","--check"]`, `main(None)` mit Argparse-Fehler → JSON, Exit 1) | `:250` `sys.argv[1:]` → `sys.argv[2:]` | E-062 | E-062 | nein | S | U8 |
| T-F-13 | teilweise | `cli/dlq_cmd.py:103`, `:133-137`, `:204`, `:231-235`, `:266` | Queue-Verdrahtung über e2e (`tests/e2e/test_dlq_cli.py`). **Offen auf Unit-Ebene:** Queue, `scan_limit`, `limit` | `tests/unit/test_cli.py::TestDlqHandlerOrchestration` (U) | `:266` `purge_dead_letters(args.queue)` → `purge_dead_letters(None)`; `:136`/`:234` `scan_limit=args.scan_limit` → `None`; `:206` `limit=args.limit` → `None` | E-028 (Umfeld) | – | nein | S | U8 |
| T-F-14 | teilweise | `dlq_cmd.py:144-150`, `:241-248`, `:219`, `:60`/`:72` | **Geschlossen:** `hit_limit`/`truncated`/`scan_exhausted` (F4-05, Marker E-028). **Offen:** Nicht-gefunden-Payload, `moved: 0`, `all: true`, bytes-Header im JSON, ungültiges UTF-8 | `tests/unit/test_cli.py` (U) | `:242` `1 if found else 0` → `1 if found else 1`; `:146` `"task_id"` → `"XXtask_idXX"`; `:219` `"all": True` → `False`; `:111` Zuweisung `e["headers"] = _normalise…` streichen; `:72` `errors="replace"` → `errors="strict"` | E-028 (M) | E-028 | nein (Normalisierer über den Handler) | S | U8 |
| T-F-15 | teilweise | `cli/schedule_cmd.py:140` (`_resolve_fire_time`, `:155`, `:164-166`) | **Geschlossen:** `--at` mit Offset (`test_cli.py::TestScheduleHandlerValidation::test_schedule_add_accepts_at_just_inside_the_deadline_bound[plus-two]`, E-080). **Offen:** erster Cron-Termin in der Zukunft; „naiv = UTC“ (`cli.md:312`) mit `TZ≠UTC` | `tests/unit/test_cli_schedule_fire_time.py` (neu) oder `tests/unit/test_cli_schedule_add_contract.py` (U, InMemoryBackend, `fire_ts_ms` lesen; Zeitzonen-Fixture) | `:155` `int(time.time() * 1000)` → `int(time.time() / 1000)`; `:165` `dt.replace(tzinfo=UTC)` streichen | E-080 (M), E-065 (M) | E-080 (naiv = UTC) | nein (über `schedule add`) | S | U8 |
| T-F-16 | teilweise | `schedule_cmd.py:242` (`_run_schedule_add`), `:176` (`_render_add_result`) | **Weitgehend geschlossen** durch `tests/unit/test_cli_schedule_add_contract.py` (19 Tests, PF-6/PF-7, E-076/E-059); die Claim-TTL (`:270`) decken 11 dieser Tests. **Offen:** gespeicherte Definition (task, args, kwargs, queue, cron, `fire_ts_ms`, `source`, `dedup_hash`, `unique_until`); JSON-Felder `dedup_hash`/`unique_until`; Tabellentexte | `tests/unit/test_cli_schedule_add_contract.py` (U, InMemoryBackend) | `:285` `"cli_unique"` → `"cli"`; `:282` `queue=args.queue` → `queue=None`; `:196` `"unique_until"` → `"XXunique_untilXX"` | E-076, E-059, E-011, E-066 (M) | E-076 | nein | M | U8 |
| T-F-17 | teilweise | `schedule_cmd.py:388` (status; `:407`, `:409`, `:412-431`) | **Geschlossen:** remove über Integration `tests/integration/test_cli_schedule.py`; Verteilung `:461` über `test_cli_schedule_prune.py`, `test_cli_schedule_status.py`. **Offen:** Horizont 24 h, Sortierung und JSON-Schlüssel von `upcoming` | `tests/unit/test_cli_schedule_status.py` (U: Einträge bei jetzt+23 h und +25 h in umgekehrter Reihenfolge) | `:407` `24 * 3600 * 1000` → `24 * 3600 / 1000`; `:409` Sortierung streichen; `:420` `"fire_at"` → `"XXfire_atXX"` | E-081 (M) | – | nein | S | U8 |
| T-F-18 | teilweise | `process_manager/run.py:24` (`:43`), `:69` (`:106-126`, `:135-140`, `:156-176`, `:178-195`) | **Teilweise:** `test_run.py::test_constructs_process_manager_and_runs`, `TestControlChannelWiring`, `TestHeartbeatIntervalPassThrough`, `test_port_guard.py::TestRealStartPathEnforcesIt`. **Offen:** nur eine URL fehlt → Exit 78 statt `KeyError`; ausdrückliche Nebenläufigkeit gegen den CPU-Spielraum (E-061); `configure`, `delivery_limit`, `admin_factory` | `tests/unit/process_manager/test_run.py` (U) | `run.py:43` `or` → `and`; `:124` Ausdruck `args.concurrency if … == "explicit" else None` → `None` (nur mit `--pool cpu` sichtbar: `--concurrency 3 --max-processes 3` auf 8 CPUs käme durch); `:171` `configure=` → `None`; `:188` `delivery_limit` weglassen | E-057, E-061, E-062, E-069 (M) | E-057 (eine URL fehlt → 78), E-061 | nein | M | U8 |
| T-F-19 | offen | `process_manager/check.py:171` (`:214-215`, `:224`), `:239` (`:255`), `:282` (`:285`); `setpoint.py:25` (`:32`, `:36`) | Grenzwerte nie genau getroffen | `tests/unit/process_manager/test_port_guard.py`, `tests/unit/process_manager/test_check.py` (U8); `setpoint.validate` in `tests/unit/process_manager/test_setpoint.py` (U7) | `check.py:214` `rng[1] <= _MAX_TCP_PORT` → `<`; `:215` `continue` → `break`; `:224` `health_range[0] <= metrics_range[1]` → `<`; `:255` `<` → `<=`; `:285` `not module_name or` streichen; `setpoint.py:36` `>` → `>=` | R5/R6, Kontrollkanal R10 | – | nein | S | U8 (Ports, Spielraum, configure), U7 (`setpoint.validate`) |
| T-F-20 | teilweise | `process_manager/entry.py:130` (Rückfall `:174-176`, Takt `:188`, Watcher `:200-218`), `:56`, `:99` (`:104`) | **Teilweise:** `test_entry.py::TestHeartbeatWriter::test_channel_receives_beats_and_is_exposed` (nur der erste Schlag), `::test_shutdown_event_triggers_sigterm_on_self` (ohne Channel). **Offen:** mindestens zwei Schläge und SIGTERM mit Channel; **Rückfall-Format** `:175`, weil eine JSON-Gruppe `worker_config_error` sonst im Format `human` bekäme; Rückfall ohne `logging_settings`; Namensraum. **Zeit:** Der Watcher nutzt echtes `asyncio.sleep(0.3)` und `time.monotonic` in `asyncio.run`; `run_virtual` greift dort nicht. Deshalb einen Runner mit `_VirtualTimeLoop` (aus `tests/support/virtual_time.py`) und gepatchtem `entry.time.monotonic` bauen, oder den Test begründet nach `tests/integration/` legen | `tests/unit/process_manager/test_entry.py` (U mit virtuellem Loop; sonst I) | `entry.py:205` `next_beat = now + beat_interval` → `next_beat = None`; `:188` `or 5.0` → `and 5.0`; `:175` `format=settings.format` streichen; `:61` `format=cfg.log_format` streichen; `:104` `list(...)` → `None` | E-057 (M) | E-057 (Rückfall-Zeile) | nein | M | U8 |
| T-F-21 | offen | `setpoint.py:167` (`_drain_one`), `:185`, `:189`, `:197` | Ungeprüft: jüngster Worker nach Startzeit; Untergrenze ohne Autoskalierer; `acted`-Flag | `tests/unit/process_manager/test_setpoint.py` (U) | `:167` `max(` → `min(`; `:185` → `floor = low`; `:189`/`:197` `acted = True` → `False` | Kontrollkanal KTD11/R12 | – | ja: `setpoint.reconcile(pm)` bzw. `tick` mit `pm._autoscale_tick()` wie `TestTick` | M | U7 |
| T-F-22 | teilweise | `setpoint.py:85`, `:144`; `process_manager/setpoint_store.py:77` | **Teilweise:** Integration `tests/integration/test_cli_worker_setpoint.py::test_runtime_setpoint_applies_reverts_and_the_status_expires_after_exit`; beim Start sieht sie nur „absent“ | `tests/unit/process_manager/test_setpoint.py` (FakeStore merkt sich die Gruppe), `tests/unit/process_manager/test_setpoint_store.py` (U) | `setpoint.py:85`/`:144` `pm.group` → `None`; `setpoint_store.py:77` `read_setpoint(group)` → `read_setpoint(None)` | R5 | – | nein | S | U7 |
| T-F-23 | offen | `supervisor.py:129`, `:169`, `:417`, `:432-433` | `test_shutdown_phases.py::test_unkillable_workers_do_not_stretch_the_budget` prüft nur `<= 30`. **Rauschen:** `supervised=True` (äquivalent). `:169` nur über das private `pm._ctx`, nachrangig | `tests/unit/process_manager/test_supervisor.py` (`:129`, `:417`), `tests/unit/process_manager/test_shutdown_phases.py` (`:433`) (U) | `:129` `max_w < min_w` → `<=`; `:417` `continue` → `break`; `:433` `started + budget.total` → `started - budget.total` | BZ-18/E-018 (M) | E-018 (`:433`) | `:417`/`:433` über `_shutdown_all_workers` wie in `test_shutdown_phases.py`; `:169` ja (`pm._ctx`, nur mit Begründung) | S | U6 |
| T-F-24 | teilweise | `check_cmd.py:48`, `:67`, `:122` | **Teilweise:** Pool aus Env im Fehlerpfad; `warnings == []` nur im Erfolgspfad. **Offen:** Werte im Fehler-Payload; stderr-Umleitung | `tests/unit/test_befund_f3_19_check_argparse_payload.py` (neuer Test neben den Markertests), `tests/unit/test_cli_check_di.py` (U) | `:74` `warnings=[]` → `None`; `:57` `module=…` → `None`; `:58` `bool(...)` → `False`; `:122` `redirect_stdout(sys.stderr)` → `redirect_stdout(None)` | E-062, F3-19 (M) | E-062 | nein | S | U8 |
| T-F-25 | offen | `cli/parser_ops.py:33-37`, `:224-228` | Kein Test ohne Unterbefehl | `tests/unit/test_cli_parser.py` (U: Exit 2) | `:36` `required=True` → `False`; ebenso `:227` | `cli.md:33` | – | nein | S | U8 |
| T-F-26 | teilweise | `process_manager/worker_target.py:62-64` (`_bind_to_supervisor`) | **Geschlossen:** `:63` (`test_supervisor.py::…::test_child_target_arms_the_parent_death_signal`). **Offen:** `:64` Exit-Code 0 im Waisenfall | `tests/unit/process_manager/test_worker_target.py` (neu, U, kein Fork: `ctypes.CDLL` und `multiprocessing.parent_process` gepatcht, über `run_worker_entry`) | `:64` `SystemExit(0)` → `SystemExit(1)` | Notiz e2e-scheduler-leader-flake | – | nein | S | U6 |

#### Zusatzpunkte

| ID | Status | Fundstelle heute | Was offen ist | Ziel-Datei | Gegenprobe-Mutant | Register | Marker neu | privat | Aufw. | Paket |
|---|---|---|---|---|---|---|---|---|---|---|
| X-1 | offen | `tests/property/stateful/lock_machine.py:369-376`; Signal in `src/swarmq/worker/lock_keeper.py:155-162` (`expired`), `:192-199` (`running`); daneben `:176-186` (`lock_extend_unsupported`, `lock_extend_failed`) | Die Maschine verlangt `task_lock_lost` nicht als Pflicht. **Nötig:**<br>• `keeper.tick_once()` unter `capture_swarmq_logs()` ausführen (Muster `lock_scenarios.py:69`).<br>• Je Lease, die nach dem Takt aus `keeper.keeps` herausgefallen ist: genau ein Ereignis aus der Menge {`task_lock_lost` (Stage `running`/`expired`), `lock_extend_unsupported`}.<br>• Eine Lease, die nach `lock_extend_failed` noch gehalten wird, erzeugt kein `task_lock_lost`; behaltene Leases kein Verlustereignis.<br>• Die Bedingung über `observe` statt über das Modell formulieren (Toleranz der Valkey-Stufe), sonst wird die Valkey-Stufe flaky | `tests/property/stateful/lock_machine.py` (P; Valkey-Stufe `tests/integration/stateful/test_lock_machine_valkey.py`, I) | `lock_keeper.py:193-198` `logger.warning("task_lock_lost", …)` streichen (Pop bleibt), ebenso `:155-162`; Gegenprobe über `uv run just property -k lock_machine` | E-022 (M) | E-022 | nein (Log-Capture, `keeper.keeps`) | M | U2 |
| X-2 | offen | `tests/property/stateful/leader_machine.py:853-857`; Produktion `scheduler/unowned.py:96-106`, `:131-143`; `scheduler/loop.py:608-609` | E-014 wird nur als Erlaubnis geprüft. **Nötig:**<br>• `last_report_ms` in `report_static` setzen.<br>• Bei `adopt_tick` `unowned_at_adopt` festhalten.<br>• Danach ist jeder Commit von `static-1` der Amtszeit ein Rückhalt, beim ersten mit `SignalSeen(SCHEDULER_CRON_UNOWNED)`.<br>• Nur in der Fake-Stufe (Modulkopf `:88-90`) | `tests/property/stateful/leader_machine.py` (P) | `loop.py:608` `if self._is_unowned(schedule_name, definition):` → `if False:`; Gegenprobe über `uv run just property -k leader_machine` | E-014 (M), E-049 | E-014 | nein | M | U2 |
| X-3 | toter Code | `backend/valkey/chords.py:25-27` (`chord_set_key`), `execution_guard.py:28-29` (`run_key`), `:32-33` (`state_key`); Kommentare `chords.py:19-21`, `execution_guard.py:22-24`, `workflow_topology.py:73` | Kein Aufrufer in `src/`. `run_key` nutzt nur `tests/integration/backend/test_valkey_script_pipeline.py:24/73`; dort auf `KeySpace().guard_run(...)` umstellen | Refactor | – | – | – | – | S | U9 |
| X-4 | toter Code | `broker/rabbitmq/admin.py:183-187` (`_extract_task_id`), `:664-665` (`_passive_message_count`) | Keine Aufrufer in `src/`, `tests/`, `swarmv/src` | Refactor | – | – | – | – | S | U9 |
| X-5 | offen (eingeengt) | `tests/unit/process_manager/test_reload.py:317` (`test_without_execvpe_the_mask_is_restored_and_a_held_sigterm_delivered`) | `:310` patcht `os.execvpe` heute. Ungepatcht ist nur noch `:317`. Härtung: `patch("swarmq.process_manager.reload.os.execvpe", side_effect=AssertionError)` | `tests/unit/process_manager/test_reload.py` (U) | `reload.py:171` `or` → `and`. **Nur mit `-k <Einzeltest>` und nur mit dem Patch** (sonst wird der pytest-Prozess ersetzt; Methodik) | KTD8 | – | nein | S | U6 |
| X-6 | offen | `tests/compat/test_mixed_operation.py:84`, `:91`, `:146`, `:159`, `:173` | Duplikate von `_harness.py`: `wait_consumers` (`:372`), `wait_runs` (`:289`), `scheduler_leader` (`:304`), `wait_leader` (`:317`), `force_cron_fire` (`:333`).<br>• `_wait_consumers` prüft `>=`, `wait_consumers` dagegen `==`. Mit `==` wäre `test_mixed_operation.py:121-123` nicht deterministisch. Deshalb `_wait_consumers` mit `>=` behalten, und zwar als einzige lokale Fassung mit Kommentar; oder an der Aufrufstelle die laufende Zahl als Ziel setzen.<br>• `_wait_leader` vergleicht mit `OLD_VERSION`, `wait_leader` mit `worker.runtime.removeprefix("pypi-")`; beides ist gleichwertig.<br>• Die lokale Fixture `app` bleibt | `tests/compat/test_mixed_operation.py` (compat) | – (Refactor; die vier compat-Tests der Datei bleiben grün) | – | – | – | S | U9 |

#### Zählung

| Gruppe | offen | teilweise | geschlossen | entfallen | Summe |
|---|---:|---:|---:|---:|---:|
| D-1…D-10 | 5 | 3 | 2 | 0 | 10 |
| E-1…E-20 | 5 | 14 | 1 | 0 | 20 |
| T-F-01…T-F-26 | 14 | 11 | 1 | 0 | 26 |
| **Summe** | **24** | **28** | **4** | **0** | **56** |

Dazu X-1…X-6: X-1, X-2, X-5 und X-6 offen; X-3 und X-4 toter Code.

#### Marker neu (Grundlage für U10)

Aus der Spalte „Marker neu“: E-005 (D-2), E-012 (D-9), E-014 (X-2), E-016 (E-17), E-018 (T-F-04, T-F-23), E-020 (E-7, E-8), E-022 (X-1), E-024 (T-F-03), E-026 (E-16, E-18), E-028 (E-2, E-6, T-F-14), E-053 und E-083 (E-10), E-057 (T-F-18, T-F-20), E-061 (T-F-18), E-062 (T-F-11, T-F-12, T-F-24), E-069 (T-F-11), E-076 (T-F-16), E-079 (D-1), E-080 (T-F-15), E-082 (D-6).

U10 trägt die tatsächlich gesetzten Marker in die `tests:`-Listen dieser Regeln ein. Maßgeblich ist `grep -rn 'mark.entscheidung' tests` gegen `main`, nicht diese Liste.

### Verdachte (gegengeprüft am 2026-10-20)

- **V-A (E-10, `broker/rabbitmq/consumer_watch.py:135`): NICHT HALTBAR.**
  - **Beobachtung:** Ein erneutes `start()` nach einem `stop()`, das den Takt abgekoppelt hat, ließe den alten Takt weiterlaufen; `start()` setzt `_stopping` zurück. Die Folge wäre eine doppelte Neuanmeldung.
  - **Begründung der Einstufung:** Es gibt keinen Produktionsaufrufer für `start()` nach `stop()` ohne `broker_close`.
  - **Folge:** optional eine P3-Härtung, kein Befund. Ein Test für diesen Fall gehört nicht in die Suite (KD6).
- **V-B (E-16, `broker/rabbitmq/declarations.py:147`, `redeclare.py:48`): LÜCKE, Produktfrage P3.**
  - **Beobachtung:** Reißt beim Neu-Deklarieren einer unroutbaren Delay-Stufe die Verbindung ab, ist die Stufe aus „ready“ entfernt, aber nicht als nicht verfügbar markiert. Die Rückstellungen dieser Stufe gehen danach still nach `{q}.DQ`. Es gibt weder die WARNING `delay_tier_unavailable` noch einen Gauge-Wert (Quelle: E-026). Ursache: `_declare_delay_tier` fängt nur `AMQPChannelError` und `QueueArgumentsMismatch`.
  - **Optionen für Georg:**
    - **A:** dokumentieren.
    - **B:** transient erneut versuchen (empfohlen).
    - **C:** wie eine Ablehnung behandeln (WARNING und Gauge).
  - **Folge:** Der E-16-Test nutzt einen Kanalfehler, nicht diesen Pfad.
- **V-C (Doku): DOKU P3.**
  - **Abweichung:** `website/reference/agent/message-formats.md:512,529` (und `.de.md:518,535`), `cli.md:126` und `configuration.md:210` beschreiben `metrics_ports`/`health_ports` als „the exact ports“, Beispiel `[9000, 9001, 9002]`. Der Code (`process_manager/check.py`, `observability_port_ranges`) liefert `[erster, letzter]`.
  - **Korrektur in U10:** Portbereich `[erster, letzter]` in `message-formats(.de).md`, `cli.md:126`, `configuration.md:210`.
- **V-D (Doku): DOKU P3.**
  - **Abweichung:** `message-formats.md:524` nennt als Beispiel für `warnings` eine `hard_timeout`-Warnung, die `--check` nie erzeugt.
  - **Korrektur in U10:** Warnbeispiel `reload_burst_conflict` (`check.py:395`), auch in `message-formats.de.md`.

Beobachtungen ohne Verdacht, nur zur Kenntnis:

- `ValkeySetpointStore.read_status` (`control.py:56-62`) gibt bei Nicht-JSON einen rohen `json.JSONDecodeError` weiter.
- Der Kommentar `broker/rabbitmq/connection.py:412-413` („no-op“) ist ungenau. `__aexit__` wirft dort einen `AttributeError`, der verschluckt wird.
- Der Pre-Flight in `run.py` übergibt `configure` nicht an `check_worker_config`. `--check` fängt den Fall ab; E-062 ist nicht verletzt.
- Fehlt die XQ, fliegt bei `list`/`inspect`/`retry` ein roher `ChannelNotFoundEntity`.
- `cli.md:248-255` nennt die Status-Felder `autoscaling`, `workers` und `ttl_s` nicht (Deferred).
- Die Test-Liste in `tests/unit/client/test_results.py:549-554` steht noch auf „[ ]“.

### Sequencing

- **Welle 1:** U3, U4, U5, U6. Nur Unit; parallel.
- **Welle 2:** U1, U2, U7, U8. U1 und U2 mit Integration bzw. Property; parallel, die Integrationsläufe serialisiert die Testsperre.
- **Danach U9**, nach dem Merge von U1, U3 und U4: U9 ändert `execution_guard.py`, `chords.py` und `admin.py` dauerhaft, deren Zeilen U1, U3 und U4 als Gegenprobe-Fundstellen nutzen (U4 über E-20, `admin.py:629/648`).
- **Danach U10.**
- Keine zwei Pakete ändern dieselbe Test- oder Quelldatei. Quellcode wird nur vorübergehend mutiert (KD2).

### Risks

- **Zeilennummern verschieben sich**, sobald ein anderes Paket gemergt ist. Maßgeblich ist der Code-Ausschnitt in der Spalte „Gegenprobe-Mutant“.
- **Parallele Integrationsläufe** (U1, U2, U9) teilen die Testcontainer. Die Sperre im `just`-Rezept serialisiert sie; vor einem Lauf `pgrep -af pytest` prüfen.
- **Property-Maschinen (U2)** können durch strengere Pflichten in der Valkey-Stufe flaky werden. Gegenmittel: Bedingungen über `observe` formulieren, die Pflichten zu E-014 nur in der Fake-Stufe prüfen und die Zweige `lock_extend_*` regeln (X-1). Bei Flakiness nicht abschwächen, sondern die Ursache klären.
- **Signal-Tests (X-5, optionale Härtung T-F-09) unter xdist:** zustellen mit `signal.pthread_kill(threading.get_ident(), …)` (Muster `adf31e5`).
- **X-5-Mutant ersetzt den Testprozess:** nur mit `-k <Einzeltest>` und Patch (Methodik).
- **Zeitzonen-Test (T-F-15):** `TZ=Europe/Berlin` und `time.tzset()` im Teardown zurücksetzen.

---

## Implementation Units

| Unit | Welle | Inhalt | Testdateien (exklusiv) | Gegenprobe-Quellen (nur vorübergehend) | Integration |
|---|---|---|---|---|---|
| U3 | 1 | Broker: DLQ und Deklaration | `tests/unit/broker/test_dlq_read.py`, `test_dead_letter_ttl_header.py`, `test_dlq_generation.py`, `test_delay_tiers.py`, `test_unroutable.py`, `test_header_guard.py`, `test_declare_adoption.py`, `test_queue_arg_conflict.py` | `broker/rabbitmq/{admin,declarations,arg_conflict,header_guard,redeclare}.py` | nein |
| U4 | 1 | Broker: Verbindung und Wächter | `tests/unit/broker/test_consumer_watch.py`, `test_consumer_watch_shutdown.py`, `test_rabbitmq.py`, `test_publish_connection.py`, `test_publish_deadline.py`, `test_shutdown_deadline.py`, `test_queue_status.py` | `broker/rabbitmq/{consumer_watch,connection}.py`, `admin.py` (nur `_passive_declarations`) | nein |
| U5 | 1 | Client | `tests/unit/client/test_workflows.py`, `tests/unit/client/test_results.py` | `client/{workflows,results}.py` | nein |
| U6 | 1 | Supervisor, Restart-Budget, Reload | `tests/unit/process_manager/test_supervisor.py`, `test_slot_cooldown.py`, `test_befund_f3_13_recycle_heartbeat_deadline.py`, `test_reload.py`, `test_shutdown_phases.py`, `test_worker_target.py` (neu) | `process_manager/{restart_budget,supervisor,reload,worker_target}.py` | nein |
| U1 | 2 | Valkey-Backend | `tests/unit/backend/test_valkey_store_error_paths.py`, `test_valkey_environment.py`, `test_valkey_locks.py`, `test_valkey_rate_limit.py`, `test_valkey_control.py`, `test_valkey.py`, `test_valkey_script_pipeline.py`, `test_valkey_namespace.py`, `tests/contracts/coordination.py` | `src/swarmq/backend/valkey/*` | ja (D-10) |
| U2 | 2 | Lock- und Leader-Maschine | `tests/property/stateful/lock_machine.py`, `tests/property/stateful/leader_machine.py` | `worker/lock_keeper.py`, `scheduler/loop.py` | ja (Valkey-Stufe) |
| U7 | 2 | Sollwert und Autoskalierer | `tests/unit/process_manager/test_setpoint.py`, `test_setpoint_store.py`, `test_autoscale.py` (neu) | `process_manager/{setpoint,setpoint_store,autoscale}.py` | nein |
| U8 | 2 | CLI und Pre-Flight | `tests/unit/test_cli_multiprocess.py`, `test_cli_residuals.py`, `test_befund_f3_19_check_argparse_payload.py`, `test_cli_check_di.py`, `test_cli.py`, `test_cli_schedule_add_contract.py`, `test_cli_schedule_fire_time.py` (neu, optional), `test_cli_schedule_status.py`, `test_cli_parser.py`, `tests/unit/process_manager/test_run.py`, `test_check.py`, `test_port_guard.py`, `test_entry.py` | `cli/{check_cmd,dlq_cmd,schedule_cmd,parser_ops}.py`, `process_manager/{run,check,entry}.py` | nein (T-F-20 ggf. I, begründet) |
| U9 | danach | Aufräumen | `tests/integration/backend/test_valkey_script_pipeline.py`, `tests/compat/test_mixed_operation.py` | **dauerhaft:** `backend/valkey/chords.py`, `backend/valkey/execution_guard.py`, `broker/rabbitmq/admin.py`, Kommentar `workflow_topology.py:73` | ja (+ compat) |
| U10 | zuletzt | Register, Befundregister, Doku V-C/V-D | `compound-packs/swarmq-entscheidungen/*.md` (nur `tests:`), `docs/befunde/fehlersuche-runde-4-2026-10.md`, `website/reference/agent/message-formats(.de).md`, `website/reference/cli.md`, `website/reference/configuration.md` | – | – |

Gemeinsam genutzte Helfer werden nur gelesen, nicht geändert: `tests/unit/process_manager/_helpers.py`, `tests/support/*`, `tests/unit/_log_capture.py`. Braucht ein Paket einen neuen Helfer, legt es ihn in seiner eigenen Testdatei an.

### U3. Broker: DLQ und Deklaration (E-1, E-2, E-4, E-5, E-6, E-16, E-17, E-18) – Welle 1

- **Goal:** Getestet sind die DLQ-Lesepfade an der Verbindungsgrenze, `PERSISTENT` der Kopien, die Ausgabeschlüssel, unlesbare Header und Bodies, die Invalidierung der Delay-Stufen, `arg_conflict` und die Grenzwerte.
- **Requirements:** R1–R4.
- **Dependencies:** keine.
- **Approach:**
  - Zuerst E-1 bis E-6 mit einem Fake an der Verbindungsgrenze, angelegt in `test_dlq_read.py` nach dem Muster von `test_befund_f4_04_purge_closes_dlq_channel.py`.
  - Danach E-16, E-18, E-17.
  - E-1: Den Ausnahmetyp bei fehlender XQ nicht festschreiben.
  - E-16: Kanalfehler, kein Verbindungsabriss (V-B).
- **Verification:**
  - `uv run just unit -k "dlq or delay_tier or declare or header_guard or unroutable or arg_conflict or dead_letter"`, danach `uv run just all`.
  - Gegenproben rot, `git diff src/` leer.

### U4. Broker: Verbindung und Wächter (E-10, E-11, E-12, E-13, E-14, E-15, E-20) – Welle 1

- **Goal:** Auf Unit-Ebene getestet sind der Takt des Consumer-Wächters, die Kanal- und Pool-Grenze, der Verwurf bei der Publish-Deadline, `ping` und die Fehlertexte.
- **Requirements:** R1–R4.
- **Dependencies:** keine.
- **Approach:**
  - E-10 mit virtueller Uhr (`run_virtual`); Gegenprobe zuerst über `:191`. Kein Test zu V-A.
  - E-13 mit echtem `aio_pika.pool.Pool`.
  - E-12 `:534` nur mit Begründung des privaten Seams, sonst Rauschen.
- **Verification:**
  - `uv run just unit -k "consumer_watch or test_rabbitmq or publish_connection or publish_deadline or shutdown_deadline or queue_status"`, danach `uv run just all`.
  - Gegenproben rot.

### U5. Client (E-7, E-8, E-9, E-19) – Welle 1

- **Goal:** `apply`, `workflow`, die Fehlmarkierung und die Weitergabe in `ClientResults` sind argumentgenau geprüft.
- **Requirements:** R1–R3.
- **Dependencies:** keine.
- **Approach:**
  - Vorgehen laut Tabelle; die E-020-Marker-Tests (`test_workflows.py:234-282`) bleiben unverändert.
  - Die Test-Liste in `test_results.py:549-554` auf den aktuellen Stand bringen.
- **Verification:**
  - `uv run just unit -k "client"`, danach `uv run just all`.
  - Gegenproben rot.

### U6. Supervisor, Restart-Budget, Reload (T-F-01, T-F-02, T-F-03, T-F-04, T-F-08, T-F-23, T-F-26, X-5; optional T-F-09) – Welle 1

- **Goal:** Getestet sind Mehr-Worker-Läufe von Herzschlagprüfung und Eskalation, die Abkühlzeit bis zum Ende, die Recycling-Frist, der Autoskalierer im Monitor-Loop, die Shutdown-Grenzen, der Exit-Code im Waisenfall und der Patch auf `execvpe` in `test_reload.py:317`.
- **Requirements:** R1–R4.
- **Dependencies:** keine.
- **Approach:**
  - T-F-03 und T-F-04 als neue Tests mit Marker E-024 bzw. E-018, neben den bestehenden.
  - Private Seams nur wie in der Spalte „privat“ angegeben.
  - X-5: zuerst den Patch einfügen, dann die Gegenprobe nur mit `-k`.
  - T-F-09 ist geschlossen; die Härtung mit `signal.raise_signal` als Rekorder ist optional.
- **Verification:**
  - `uv run just unit -k "process_manager and (supervisor or slot_cooldown or f3_13 or reload or shutdown_phases or worker_target)"`, danach `uv run just all`.
  - Gegenproben rot. Die Gegenprobe zu `supervisor.py:433` im Quellcode ausführen (KTD3).

### U1. Valkey-Backend (D-1, D-2, D-3, D-5, D-6, D-7, D-9, D-10) – Welle 2

- **Goal:** Die offenen Valkey-Lücken sind geschlossen; jede Gegenprobe ist rot.
- **Requirements:** R1–R4.
- **Dependencies:** keine.
- **Approach:**
  - Reihenfolge: D-1 (`peek`), D-2, D-3, D-6, D-7 (`:82`), D-9, D-5, D-10.
  - D-2: Der Test mit Marker E-005 in `test_befund_bz04_eviction_policy.py` bleibt unberührt.
  - D-2, Frist: hängendes `INFO server` über ein Event, das nie gesetzt wird, unter virtueller Uhr.
- **Verification:**
  - `uv run just unit -k "valkey"` und `uv run just integration -k "contracts_valkey"` (D-10, auch die Gegenprobe), danach `uv run just all`.

### U2. Lock- und Leader-Maschine (X-1, X-2) – Welle 2

- **Goal:** E-022 (Signal `task_lock_lost` beim Verlust) und E-014 (verwaiste Crons zurückhalten) sind in den Zustandsmaschinen Pflicht.
- **Requirements:** R1, R3.
- **Dependencies:** keine.
- **Approach:**
  - X-1 und X-2 laut Tabelle.
  - X-1 regelt die Zweige `lock_extend_unsupported` und `lock_extend_failed` (`lock_keeper.py:176-186`) ausdrücklich.
  - Szenario-Dateien nur lesen.
- **Verification:**
  - `uv run just property -k "lock_machine or leader_machine"`, auch mit `HYPOTHESIS_PROFILE=thorough`.
  - `uv run just integration -k "lock_machine_valkey or leader_machine_valkey"`.
  - Gegenproben über `uv run just property …` rot.

### U7. Sollwert und Autoskalierer (T-F-05, T-F-06, T-F-07, T-F-10, T-F-19 Teil `setpoint.validate`, T-F-21, T-F-22) – Welle 2

- **Goal:** Getestet sind:
  - die Rückgabe von `apply`,
  - Status-TTL (relational) und Status-Felder,
  - die Folge-Takte,
  - die Auswahl beim Abbau und die Untergrenze,
  - der Gruppenschlüssel beim Lesen,
  - `setpoint.validate` an der Grenze.
- **Requirements:** R1–R4.
- **Dependencies:** keine.
- **Approach:**
  - T-F-05 nach der Formulierung in der Tabelle; kein Test auf „Sollwert gleich den Startgrenzen“.
  - T-F-06 ohne Marker und ohne exakte Formel.
  - Den FakeStore für T-F-22 lokal in `test_setpoint.py` erweitern.
  - Der Autoskalierer-Teil von T-F-10 kommt in `test_autoscale.py`.
- **Verification:**
  - `uv run just unit -k "setpoint or autoscale"`, danach `uv run just all`.
  - Gegenproben rot.

### U8. CLI und Pre-Flight (T-F-11, T-F-12, T-F-13, T-F-14, T-F-15, T-F-16, T-F-17, T-F-18, T-F-19 ohne `setpoint.validate`, T-F-20, T-F-24, T-F-25) – Welle 2

- **Goal:** Getestet sind:
  - `worker --check` mit `--autoscale`, `--configure` und `--watch-dir`,
  - die Erkennung über `sys.argv`,
  - die Queue-Verdrahtung und die JSON-Verträge der DLQ-Befehle,
  - die Fire-Zeit,
  - die gespeicherte Definition bei `schedule add`,
  - `upcoming` in `schedule status`,
  - die Verdrahtung beim Start,
  - die Grenzwerte,
  - der Herzschlag-Watcher und das Rückfall-Format,
  - der Check-Payload,
  - der Pflicht-Unterbefehl.
- **Requirements:** R1–R4.
- **Dependencies:** keine.
- **Approach:**
  - Reihenfolge: T-F-11, T-F-13, T-F-14, T-F-16, T-F-15, T-F-18, T-F-19, T-F-12, T-F-24, T-F-25, T-F-17, T-F-20.
  - T-F-20: Runner mit `_VirtualTimeLoop` und gepatchtem `entry.time.monotonic`, oder den Test begründet nach `tests/integration/`.
  - Die Marker-Tests zu E-028, E-062, E-076 und E-080 bleiben unverändert.
  - Bei Bedarf in U8a (`tests/unit/test_cli*.py`, `test_befund_f3_19_*`) und U8b (`tests/unit/process_manager/test_{run,check,port_guard,entry}.py`) teilen.
- **Verification:**
  - `uv run just unit tests/unit/test_cli.py tests/unit/test_cli_multiprocess.py tests/unit/test_cli_residuals.py tests/unit/test_cli_check_di.py tests/unit/test_cli_parser.py tests/unit/test_cli_schedule_add_contract.py tests/unit/test_cli_schedule_status.py tests/unit/test_befund_f3_19_check_argparse_payload.py tests/unit/process_manager/test_run.py tests/unit/process_manager/test_check.py tests/unit/process_manager/test_port_guard.py tests/unit/process_manager/test_entry.py`, danach `uv run just all`.
  - Gegenproben rot.

### U9. Aufräumen: toter Code und compat-Duplikat (X-3, X-4, X-6) – nach U1, U3 und U4

- **Goal:** Kein toter Code aus den Nebenbefunden mehr; `test_mixed_operation.py` nutzt die Helfer aus `_harness.py`.
- **Requirements:** R5.
- **Dependencies:** U1, U3 und U4 gemergt.
- **Files:**
  - `src/swarmq/backend/valkey/chords.py`
  - `src/swarmq/backend/valkey/execution_guard.py`
  - `src/swarmq/broker/rabbitmq/admin.py`
  - `src/swarmq/workflow_topology.py:73` (Kommentar)
  - `tests/integration/backend/test_valkey_script_pipeline.py`
  - `tests/compat/test_mixed_operation.py`
- **Approach:**
  - Refactor ohne neuen Test.
  - Vorher prüfen: `grep -rn "chord_set_key\|state_key\|run_key\|_extract_task_id\|_passive_message_count" src tests ../swarmv/src` darf nur die bekannten Stellen finden.
  - X-6: `_wait_runs`, `_leader`, `_wait_leader` und `_force_fire` durch die `_harness`-Helfer ersetzen; `_wait_consumers` mit `>=` bleibt (siehe Tabelle).
- **Verification:**
  - `uv run just all` und `uv run just integration -k "script_pipeline"`.
  - compat unter der Sperre: `scripts/with-test-lock.sh uv run --extra test --extra reload pytest tests/compat/test_mixed_operation.py -m compat -q` (Ports 5699/6399 aus `tests/support/services.py`; braucht Netz zu PyPI).
  - `uv run just lint` und `uv run just types`.

### U10. Register, Befundregister, Doku V-C/V-D – zuletzt

- **Goal:** Register, Befundregister und Doku spiegeln die Runde.
- **Requirements:** R3, R6.
- **Dependencies:** U1–U9.
- **Files:**
  - `compound-packs/swarmq-entscheidungen/e0nn-*.md` (nur die `tests:`-Listen, abgeleitet aus „Marker neu“ und dem tatsächlichen `grep`)
  - `docs/befunde/fehlersuche-runde-4-2026-10.md`
  - `website/reference/agent/message-formats.md` und `.de.md`
  - `website/reference/cli.md:126`
  - `website/reference/configuration.md:210`
- **Approach:**
  - **Befundregister:**
    - Deferred „Testlücken ohne Befund-Bezug“ auf erledigt setzen, mit Commits.
    - Den Hinweis zur Lock-/Leader-Maschine als erledigt markieren.
    - Die Nebenbefunde zum toten Code als erledigt markieren.
    - V-A bis V-D mit ihrer Einstufung eintragen, samt Belegen (Probe-Tests als Beschreibung, nicht als Scratchpad-Pfad).
    - V-B als Produktfrage P3 mit den Optionen A/B/C.
  - **Doku V-C:** Portbereich `[erster, letzter]`.
  - **Doku V-D:** Warnbeispiel `reload_burst_conflict`.
- **Verification:**
  - `uv run --extra test pytest tests/unit/test_entscheidungsregister.py -q` grün.
  - Doku-Build bzw. Link-Prüfung der Website wie üblich.
  - `uv run just all` grün.

---

## Verification Contract

- **Je Paket U1–U8:**
  - Alle neuen Tests sind gegen das Original grün.
  - Jede Gegenprobe aus der Tabelle wurde tatsächlich eingebaut und nur über die Ziel-Testdatei bzw. den Ziel-Test ausgeführt. Sie war fachlich rot (Methodik Schritt 3).
  - Danach ist `git diff src/` leer.
- **Vor jedem Commit:** `uv run just all`, `uv run just lint` und `uv run just types` grün.
- **Laufzeit:** `uv run just unit` meldet keinen neuen Test über 100 ms; Ausnahmen sind im Test begründet.
- **Integration:** nur über `uv run just integration …` bzw. die compat-Zeile mit `scripts/with-test-lock.sh`; nie gegen 5672/6379.
- **Marker:** Kein Test mit `entscheidung`-Marker wurde geändert; im Diff gegen `main` sind nur hinzugefügte Tests markiert.

## Definition of Done

- Alle 52 offenen oder teilweisen Lücken haben einen Test oder sind in der Tabelle begründet als Rauschen eingestuft bzw. als durch die Integration gedeckt. Die vier geschlossenen sind mit Test belegt.
- X-1, X-2, X-5 und X-6 sind umgesetzt; X-3 und X-4 sind entfernt.
- Die Einstufungen V-A bis V-D stehen im Befundregister. V-C und V-D sind in der Doku korrigiert. V-B liegt Georg als Produktfrage vor.
- Die Register-Verweise sind nachgetragen.
- MRs je Paket erst nach Georgs Ok.

## Status (2026-10-20)

Alle Pakete umgesetzt, lokal auf den Branches `test/tl-u1` bis `test/tl-u10` (ungepusht, MRs erst nach Georgs Ok). `test/tl-u10` enthält U1 bis U9 als Merges.

| Paket | Commits | Ergebnis |
|---|---|---|
| U1 | `886cbba` | D-1, D-2, D-3, D-5, D-6, D-9, D-10 geschlossen; D-7 nicht umgesetzt (äquivalenter Mutant) |
| U2 | `3176131` | X-1 (E-022, Invariante I9) und X-2 (E-014, Invariante L10) |
| U3 | `50ea93d` | E-1, E-2, E-4, E-5, E-6, E-16, E-17, E-18 |
| U4 | `1eae9d9` | E-10 bis E-15, E-20; kein Test zu V-A |
| U5 | `c1fa4e1` | E-7, E-8, E-9, E-19 |
| U6 | `3f4d7f8` | T-F-01 bis T-F-04, T-F-08, T-F-23, T-F-26, X-5; optionale Härtung T-F-09 nicht umgesetzt |
| U7 | `353b742` | T-F-05 bis T-F-07, T-F-10, T-F-19 (Teil `setpoint.validate`), T-F-21, T-F-22 |
| U8 | `4354a03`, `c30fe49` | T-F-11 bis T-F-20, T-F-24, T-F-25 |
| U9 | `5cdeaf9`, `37319e2` | X-3 und X-4 entfernt, X-6 umgesetzt |
| U10 | `test/tl-u10` | Register-Verweise (29 Tests in 16 Regeln), Befundregister (Abschnitt „Test-Runde (2026-10-20)“), Doku V-C und V-D |

Geschlossen: 51 der 52 offenen oder teilweisen Lücken (D 7, E 19, T-F 25); D-4, D-8, E-3 und T-F-09 waren schon geschlossen.

Abweichungen:

- **D-7 nicht umgesetzt.** Der Mutant „`self._pending = []` in `_drain` streichen“ ist äquivalent: `_fail` hat alle verbliebenen Aufrufe schon erledigt, der nächste Sammler filtert erledigte Futures heraus. Dasselbe Verhalten töten drei Bestandstests.
- **T-F-09: Härtung nicht umgesetzt.** Die Lücke ist geschlossen; die optionale Härtung mit `signal.raise_signal` als Rekorder entfällt.
- **Marker:** E-014, E-020, E-022 und E-083 aus „Marker neu“ hat kein Paket gesetzt; U2 prüft E-014 und E-022 als Invarianten der Maschinen.
- **Neue Befunde aus der Gegenprüfung (Verdacht V-E):** F4-25 (P3) und F4-26 (P2) im Autoskalierer, offen, Fix in 0.14.2 nach Georgs Freigabe. V-B ist Produktfrage PF-21 (offen bei Georg).
