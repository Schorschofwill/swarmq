# Mutation Lauf B (Spur M, U4): Valkey-Backend, Unit- und Integration-bewertet

Plan: `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md`, R15–R17, KTD4, KD2, Einheit U4 (Auswertung). Stand: swarmq 0.12.3, Branch `test/fehlersuche-runde-4`. Grundlage: lokale Kopie `mutants-b/` (nicht committet) mit `ergebnisse.txt` (Ausgabe von `mutmut results --all true`) und der mutmut-Datenbank `mutants/**/*.meta`; Vergleich mit `mutants-a/` über den Export aus U3.

Dieser Bericht enthält Kennzahlen, den Vergleich je Mutant und den Export der Schnittmenge. Die Sichtung steht in `3c-sichtung-D.md`.

## Ergebnis in Kürze

- 1113 Valkey-Mutanten: **798 getötet (71,7 %)**, 215 überlebt, 98 ohne Test, 2 timeout. Lauf A hatte für dieselben Mutanten 712 getötet (64,0 %), 254 überlebt, 146 ohne Test, 1 timeout. Die 8601 „not checked“ sind die übrigen Welle-2-Module, die das Muster `swarmq.backend.valkey.*` ausschließt.
- **Lauf B tötet 87 Mutanten mehr als Lauf A** (50 von „überlebt“, 37 von „ohne Test“). Das sind Testlücken der Unit-Ebene, kein Verdacht. Schwerpunkte: Lua-Argumente in `locks.py` (23), `chords.py` (20), `execution_guard.py` (20), `rate_limit.py` (15).
- **Schnittmenge** („in A nicht getötet und in B überlebt“ plus „in B ohne Test“): 313 Mutanten, nach demselben Vorfilter wie Lauf A **234** (141 überlebt, 93 ohne Test). Export: `<scratchpad>/u4/mut-gruppe-D-schnitt.txt`.
- **Die Testauswahl von Lauf B ist enger als „Integration“.** `tests/integration/backend` enthält 90 Tests. Die Vertragstests der Valkey-Stores (`tests/integration/test_contracts_valkey.py`), die Lua-Tests (`test_lock_lua_scripts.py`, `test_chord_lua_scripts.py`, `test_rate_limit_lua.py`), `test_execution_guard.py`, `test_locking_manager.py` und die neuen Zustandsmaschinen unter `tests/integration/stateful/` liegen eine Ebene höher und liefen nicht mit. Ein „ohne Test“ in Lauf B heißt daher „weder Unit noch `tests/integration/backend`“, nicht „keine Integration“. Der Gegenlauf in `3c-sichtung-D.md` zeigt: 93 der 234 Mutanten tötet die übrige Integrationssuite, darunter alle 47 von `ValkeyLockStore.observe`, die in beiden Läufen „ohne Test“ waren.
- Die 2 „timeout“ sind keine Zeitüberschreitung: Exit-Code 255, eine Ausnahme im Testrunner. Ursache sind offene Valkey-Verbindungen (siehe unten).

## Rahmen

| Punkt | Wert |
|---|---|
| Ort | Lastserver über Docker-Kontext `lastserver`, Compose-Projekt `swarmq-mutation`, Profil `b` (Dienste `rabbitmq` 4.3.6 und `valkey` `valkey/valkey:8` im selben Projekt, ohne Host-Ports) |
| Container | wie Lauf A: 3 CPUs, 8 GB, Abbild `ghcr.io/astral-sh/uv:python3.14-bookworm`, frischer Container mit leerem `mutants/` |
| Kinder | `mutmut run --max-children 1 'swarmq.backend.valkey.*'` (eine vhost und eine Valkey-DB für alle Kinder, KTD4) |
| Dauer | 2026-10-17, Start 13:24, Statistikphase 13:27 (`mutmut-stats.json`), Ergebnisse geholt 14:10 (Zeitstempel `mutants-b/ergebnisse.txt`). Der Auftrag nennt „bis ~14:30“; das passt zum Abbau, nicht zum Ende des Laufs. Der Lauf dauerte also höchstens 46 Minuten (mindestens 24 Mutanten pro Minute) |
| Testauswahl | `pytest_add_cli_args_test_selection = ["tests/unit", "tests/integration/backend"]` (Entrypoint für `MUTMUT_LAUF=b`, `mutants-b/pyproject.toml`); `duration_by_test` zählt 5100 Tests |
| Vorlauf | 90 Integrationstests aus `tests/integration/backend` grün (Angabe des Auftrags; die Zahl passt zum Verzeichnis: 90 gesammelt) |
| Dienste | `SWARMQ_TEST_RABBITMQ_HOST=rabbitmq`, `SWARMQ_TEST_VALKEY_HOST=valkey`, Standardports, `SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1` nur im Container (`docker/mutation/entrypoint.sh`) |
| mutmut | 3.8.0, `process_isolation = "forkserver"`, Zeitlimit-Standards wie Lauf A |
| `source_paths` | unverändert gegenüber Lauf A (KTD4); `schedules.py` und `results.py` bekommen keine Mutanten |

Export und Vorfilter: dieselben Skripte wie in U3 (`<scratchpad>/u3/export.py`, `klassifiziere.py`), angewandt auf `mutants-b/`; Ergebnis `<scratchpad>/u4/alle-b.json`, `klass-b.json`. Vergleich und Export: `<scratchpad>/u4/vergleich.py`, `schnitt.py`, `a_tot_b.py`.

`<scratchpad>` = `/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/608b3617-a1bc-47f1-bd40-e763775cafe6/scratchpad`.

## Kennzahlen je Modul (Lauf B)

| Modul | Mutanten | getötet | überlebt | ohne Test | timeout | Score B | Score A |
|---|---:|---:|---:|---:|---:|---:|---:|
| `backend/valkey/__init__.py` | 6 | 5 | 1 | 0 | 0 | 83 % | 83 % |
| `backend/valkey/chords.py` | 42 | 20 | 8 | 14 | 0 | 48 % | 0 % |
| `backend/valkey/connection.py` | 174 | 136 | 36 | 0 | 2 | 78 % | 75 % |
| `backend/valkey/control.py` | 113 | 85 | 28 | 0 | 0 | 75 % | 75 % |
| `backend/valkey/environment.py` | 232 | 167 | 65 | 0 | 0 | 72 % | 72 % |
| `backend/valkey/execution_guard.py` | 113 | 65 | 17 | 31 | 0 | 58 % | 40 % |
| `backend/valkey/locks.py` | 248 | 168 | 27 | 53 | 0 | 68 % | 58 % |
| `backend/valkey/rate_limit.py` | 73 | 59 | 14 | 0 | 0 | 81 % | 60 % |
| `backend/valkey/script_pipeline.py` | 112 | 93 | 19 | 0 | 0 | 83 % | 81 % |
| **Summe** | **1113** | **798** | **215** | **98** | **2** | **71,7 %** | **64,0 %** |

Score = getötet / Mutanten. `control.py` und `environment.py` gewinnen nichts: `tests/integration/backend/test_valkey_control.py` prüft dieselben Pfade wie die Unit-Tests, `environment.py` hat keinen Integrationstest in der Auswahl.

## Vergleich je Mutant (A gegen B)

Übergänge über alle 1113 Mutanten:

| Lauf A | Lauf B | Anzahl | Bedeutung |
|---|---|---:|---|
| getötet | getötet | 710 | – |
| getötet | timeout | 2 | `connection.__init__`, Exit 255 (unten) |
| timeout | getötet | 1 | `script_pipeline._settle__mutmut_1` |
| überlebt | getötet | 50 | Testlücke der Unit-Ebene, kein Verdacht |
| ohne Test | getötet | 37 | Testlücke der Unit-Ebene, kein Verdacht |
| überlebt | überlebt | 204 | Sichtungskandidat |
| ohne Test | überlebt | 11 | Sichtungskandidat (Funktion wird in B aufgerufen, aber nicht geprüft) |
| ohne Test | ohne Test | 98 | auch `tests/integration/backend` ruft die Funktion nicht auf |

Je Modul:

| Modul | in A nicht getötet | davon in B getötet | in A und B überlebt | A ohne Test, B überlebt | B ohne Test | Schnitt nach Vorfilter (überlebt / ohne Test) |
|---|---:|---:|---:|---:|---:|---|
| `__init__.py` | 1 | 0 | 1 | 0 | 0 | 1 / 0 |
| `chords.py` | 42 | 20 | 0 | 8 | 14 | 5 / 14 |
| `connection.py` | 44 | 8 | 36 | 0 | 0 | 22 / 0 |
| `control.py` | 28 | 0 | 28 | 0 | 0 | 24 / 0 |
| `environment.py` | 65 | 0 | 65 | 0 | 0 | 34 / 0 |
| `execution_guard.py` | 68 | 20 | 14 | 3 | 31 | 14 / 31 |
| `locks.py` | 103 | 23 | 27 | 0 | 53 | 14 / 48 |
| `rate_limit.py` | 29 | 15 | 14 | 0 | 0 | 11 / 0 |
| `script_pipeline.py` | 20 | 1 | 19 | 0 | 0 | 16 / 0 |
| **Summe** | **400** | **87** | **204** | **11** | **98** | **141 / 93** |

### In A überlebt, in B getötet (87): Testlücken der Unit-Ebene

Alle 87 waren Kandidaten nach dem Vorfilter von Lauf A. Lauf B nimmt sie aus der Sichtung.

| Modul | Funktion | Mutanten | Was nur die Integration prüft |
|---|---|---:|---|
| `locks.py` | `LockManager.acquire` | 12 | `keys`/`args` an `lock_acquire.lua` (Einstieg 2 aus Lauf A: `acquire__mutmut_17/18/21`, `now_ms * 1000 → / 1000` `__mutmut_4`) |
| | `LockManager.__init__` | 7 | registrierte Skripte |
| | `LockManager.release` | 4 | Argumente an `lock_release.lua` |
| `chords.py` | `record_and_check_complete`, `__init__` | 20 | Schlüssel und Argumente an `chord_complete.lua` (`tests/integration/backend/_golden.py`) |
| `execution_guard.py` | `_finish`, `mark_done`, `__init__`, `claim`, `run_key` | 20 | Abschluss über `guard_finish.lua`, darunter `claim__mutmut_2` (Einstieg 5: `backlog_ttl_ms and 0`) |
| `rate_limit.py` | `RateLimiter.check`, `__init__` | 15 | u. a. `check__mutmut_12` (`%` → `/`, Einstieg 6) |
| `connection.py` | `connect`, `_seal_pool`, `__init__` | 8 | Verbindungsaufbau gegen den echten Server, Sperre nach `close()` |
| `script_pipeline.py` | `_send` | 1 | Stapel gegen echte Verbindung |

### Einstiegsliste aus Lauf A, Nummern 1–7 (Gruppe D)

| # | Mutanten | Lauf B |
|---|---|---|
| 1 | `LockManager.acquire__mutmut_3/4/7`, `extend__mutmut_17` | `_4` getötet; `_3`, `_7`, `extend_17` überlebt (Sichtung) |
| 2 | `acquire__mutmut_17/18/21`, `ValkeyLockStore.acquire__mutmut_1–4` | alle getötet |
| 3 | `ValkeyLockStore.observe__mutmut_5/33` | ohne Test (Sichtung: vom Vertragstest getötet) |
| 4 | `execution_guard.renew__mutmut_16` | ohne Test (Sichtung: vom Vertragstest getötet) |
| 5 | `claim__mutmut_2/3` | `_2` getötet, `_3` überlebt (Sichtung) |
| 6 | `RateLimiter.check__mutmut_11/12/14` | `_12` getötet; `_11`, `_14` überlebt (Sichtung: vom Vertragstest getötet) |
| 7 | `check__mutmut_36` | überlebt (Sichtung: vom Vertragstest getötet) |

### Was die Auswahl `tests/integration/backend` nicht erreicht

Die 98 „ohne Test“ in B verteilen sich auf: `ValkeyLockStore.observe` (47), `locks._as_text` (6), `ValkeyExecutionGuardStore.renew` (17), `release` (7), `mark_superseded` (6), `execution_guard.state_key` (1), `ChordManager.record_member` (10), `chords.chord_set_key` (4). Die ersten sieben Funktionen prüft `tests/contracts/coordination.py` bzw. `tests/contracts/execution_guard.py` über `tests/integration/test_contracts_valkey.py`. `chord_set_key` und `state_key` haben in `src/` keinen Aufrufer.

Das ist eine Abweichung zwischen Plan und Messung, kein Fehler im Lauf: R15 und KTD4 nennen ausdrücklich `tests/integration/backend`. Ziel von U4 war, „dass Überlebende dort echte Lücken und nicht nur fehlende Unit-Tests zeigen“. Mit der Auswahl wird das nur halb erreicht. Für den Integrations-bewerteten Lauf von `broker/rabbitmq` (Welle 3) sollte die Auswahl mindestens `tests/integration/test_contracts_*.py` und die Lua- und Store-Tests auf oberster Ebene enthalten. Die Zustandsmaschinen unter `tests/integration/stateful/` sind wegen Hypothesis teurer, töten aber Mutanten, die kein Beispieltest trifft (`RateLimiter.check__mutmut_14` erst nach 241 s im Gegenlauf).

## Die 2 „timeout“

| Mutant | Fundstelle | Mutation | Exit A | Exit B |
|---|---|---|---|---|
| `connection.xǁValkeyConnectionǁ__init____mutmut_57` | `connection.py:209` | `self._owns_client = True` → `None` | 1 (getötet) | 255 |
| `connection.xǁValkeyConnectionǁ__init____mutmut_58` | `connection.py:209` | `self._owns_client = True` → `False` | 1 (getötet) | 255 |

Exit 255 ist bei mutmut 3.8 kein Zeitlimit (siehe `3a-mutation-lauf-a.md`, Tabelle der Exit-Codes): Der Forkserver-Enkel fängt eine Ausnahme aus `test_runner.run_tests` und endet mit `os._exit(-1)`. Die Datenbank hält für beide Mutanten keine Dauer fest (`durations_by_key` = 0,0).

Nachstellung auf dem Laptop (ohne mutmut, Mutant per Plugin `<scratchpad>/u5d/plug/mut_env.py` nachgebildet, `uv run just integration tests/integration/backend -n0` unter der Testsperre, Log `<scratchpad>/u5d/gl-T1.log`): Mit `_owns_client = False` schließt `close()` den selbst gebauten Client nie. Jeder Test, der ein Backend über eine URL baut, hinterlässt offene Valkey-Verbindungen. Beim Aufräumen melden sie sich als `ResourceWarning: unclosed Connection`, `unclosed transport`, `loop is closed`, und pytest wandelt das über `filterwarnings = error` in 77 Fehler beim Teardown (`PytestUnraisableExceptionWarning`). Der Lauf endet mit Exit 1, nicht nach einem Zeitlimit. In Lauf A reichte ein Unit-Test für Exit 1. In Lauf B entkommt offenbar eine der Unraisable-Ausnahmen aus `pytest.main` selbst; die Herleitung ist plausibel, aber ohne Log aus dem Container nicht belegt. Beide Mutanten gelten als erkannt; keiner ist Sichtungskandidat.

## Export der Schnittmenge

Im Scratchpad unter `<scratchpad>/u4/`:

| Datei | Inhalt |
|---|---|
| `mut-gruppe-D-schnitt.txt` | 234 Mutanten nach Vorfilter, gleiche Zeilenform wie die U3-Exporte (`Name \| Status \| Datei:Zeile \| Funktion`, darunter `-`/`+`-Zeilen); Status ist der von B, bei Abweichung mit `(A: …)`. Teil 1: in B überlebt (141; 133 in A überlebt, 8 in A ohne Test). Teil 2: in B ohne Test (93) |
| `mut-gruppe-D-schnitt-gefiltert.txt` | 79 aussortierte (V2 33, V6 25, V4 19, V7 2), mit Regel je Mutant |
| `alle-b.json`, `klass-b.json`, `schnitt.json` | alle Mutanten von B mit Status, Exit-Code und Diff; Klassifikation; Schnittmenge |

Der Vorfilter sortiert in B genau dieselben 79 Mutanten aus wie in A. Keiner der gefilterten Mutanten wurde in B getötet.

## Abweichungen zwischen Plan und Lauf

- Testauswahl: siehe Abschnitt „Was die Auswahl … nicht erreicht“. Die Schnittmenge ist deshalb größer als die echte Lücke der Integrationsebene; die Sichtung hat die übrige Integrationssuite per Gegenlauf nachgezogen.
- Plan U4 rechnete mit „512 Mutanten ohne Unit-Test“ (Zahl aus Runde 2 über die damaligen Valkey-Module). Für die Welle-2-Module waren es in Lauf A 146; Lauf B hat davon 48 erreicht (37 getötet, 11 überlebt).
- Laufzeit: höchstens 46 Minuten statt des Deckels von 48 h; Zwischenstände waren nicht nötig.
