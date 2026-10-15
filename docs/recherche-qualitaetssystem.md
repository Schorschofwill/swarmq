# Recherche: Test-, Lint-, Coverage- und CI-System von SwarmQ

Stand: 2026-10-11 | Python 3.14 | ruff 0.16.7 | mypy 2.3.1 | pytest 9.1.1 | coverage 7.16.1

Ziel: das Qualitätssystem auf den Stand der Technik bringen. Dieses Dokument
hält den gemessenen Ist-Stand, den recherchierten Stand 2026 und die Lücken
fest. Es enthält keine Umsetzung.

---

## Kurzfazit

Die **Testsuite ist überdurchschnittlich**: 3340 Unit-Tests, Property-, Fuzz-,
Chaos-, Stress- und Soak-Suiten, Contract-Tests, Architektur-Guards, mypy
strict. Die **Werkzeugschicht darum herum liegt etwa auf dem Stand von 2023**:
Coverage wird nirgends gemessen, Formatierung wird nicht durchgesetzt, es gibt
keine Hooks und keine Prüfung der Abhängigkeiten, und mehrere Regeln aus
`AGENTS.md` (Unabhängigkeit, < 100 ms pro Unit-Test) prüft nichts. Der Hebel
liegt bei Durchsetzung und Messung, nicht bei mehr Tests.

## Ist-Stand (gemessen am 2026-10-11)

| Bereich | Befund |
|---|---|
| Versionen | ruff 0.16.7 (aktuell 0.17.0), mypy 2.3.1, pytest 9.1.1, pytest-asyncio 1.4, coverage 7.16.1 (aktuell 7.16.2), mutmut 3.8 |
| Unit-Suite | 3340 passed, 2 skipped, 28 xfailed in 44 s mit einem Prozess, etwa 13 ms pro Test |
| Branch-Coverage (nur Unit) | **93 %**; schwach: `cli/inspect_cmd` 44 %, `cli/schedule_cmd` 66 %, `process_manager/worker_target` 77 % |
| Coverage in der CI | **keine**. `pytest-cov` ist installiert, wird aber nicht benutzt. Der Beitrag von Integration und e2e ist unbekannt |
| Unabhängigkeit der Tests | **verletzt**: mit `pytest-randomly` schlägt `tests/unit/test_hot_reload.py::TestResolveWatchDirs::test_installed_package_warns_with_module_name` in 3 von 3 Läufen fehl, einzeln besteht der Test. `tests/unit/test_logging_fork.py::test_the_child_does_not_report_the_drops_of_the_parent` fiel in 1 von 3 Läufen |
| Parallelisierung | `pytest-xdist -n auto`: **12 s statt 44 s**; derselbe reihenfolgeabhängige Test scheitert |
| Langsame Unit-Tests | **74 Tests ≥ 100 ms** (Call-Phase), Spitze 3,2 s (`tests/unit/typing/test_contract_shapes.py`, startet mypy). `AGENTS.md` verbietet das, kein Gate prüft es |
| Warnungen | 128 × `ResourceWarning`, alle aus `tests/unit/_architecture.py:41` (Datei wird nicht geschlossen). `filterwarnings = error` fehlt |
| pytest-Strenge | kein `strict` (pytest 9); ein vertippter Marker wird still ein neuer Marker; `xfail strict` nur per Konvention |
| Formatierung | **kein Formatter**; `ruff format --check` würde 325 von 482 Dateien ändern |
| Lint-Regeln | E, W, F, I, N, UP, B, SIM, RUF, PLR0915, C901 |
| Typen | mypy strict für `src`, `tests/support`, `tests/contracts`; Rest von `tests/` ungeprüft; kein zweiter Checker; keine Messung der Typvollständigkeit für Nutzer (`py.typed` vorhanden) |
| Abhängigkeiten | Test-Werkzeuge im Extra `test` (also Teil der veröffentlichten Metadaten), mutmut eingeschlossen; kein Renovate/Dependabot; kein Audit auf Sicherheitslücken |
| Lokale Hooks | keine `.pre-commit-config.yaml` |
| Doku | RTD baut, die CI prüft nie `mkdocs build --strict` |
| CI | GitLab, lokaler Runner (Kontingent erschöpft). Blockierend: Lint+Typen, Lock-Check, Unit (+Property, Parser-Fuzz), Integration, e2e, Secret Detection. Stress/Soak manuell, Chaos und Mutation nur von Hand |
| Release | manuell mit Token; Trusted Publishing im GitHub-Umzugsplan vorgesehen |

### Ruff gegen `ALL` (ohne D, COM812, ISC001, CPY)

Treffer je Familie:

| Familie | `src/` | `tests/` | Einordnung |
|---|---:|---:|---|
| ASYNC | 16 | 51 | fachlich wichtig für eine asyncio-Bibliothek |
| S | 78 (52 × S101 `assert`) | 6866 (fast nur S101) | `assert` in `src/` verschwindet unter `python -O` |
| BLE | 104 | 42 | pauschales `except Exception`, Begründung nötig |
| TRY / EM | 251 / ~210 | 225 | überwiegend Stil (TRY003, EM101/102) |
| TC | 159 | 117 | Importe nur für Typen, großteils autofixbar |
| PL | 227 | 1828 | darunter PLC0415 (107, gewollte lazy Importe der CLI), PLR0913, PLR2004 |
| PT | 4 | 557 | pytest-Stil |
| ARG | 10 | 516 | in Tests meist Fixtures |
| ANN401 | 263 | – | `Any` in Signaturen; neben mypy strict eine Policy-Frage |
| PERF, FURB, PTH, LOG, G, DTZ | ≤ 5 | ≤ 12 | klein |

pydocstyle (D) auf `src/`: vor allem D205 (385), D209 (337), D102 (168).

Suppressions in `src/`: 22 × `noqa`, 33 × `type: ignore`, 27 × `pragma: no cover`.

## Stand der Technik 2026 (Recherche)

Belege stammen aus den Konfigurationen von FastAPI, Pydantic, Litestar,
structlog, attrs, httpx, aiohttp und uv sowie aus Changelogs. Mit **[VW]**
markiert ist, was nur auf Vorwissen beruht.

### Lint und Format
- **Standard:** `ruff format` statt black, `ruff check` statt isort/flake8/pyupgrade. ruff 0.17.0 (09.10.2026).
- **Spitzenprojekte:** structlog und attrs nutzen `select = ["ALL"]` mit begründeten Ausnahmen (global ANN, D, FBT, A, ARG, TC, E501, PLR2004; in Tests S, TRY, EM, DTZ, PT011/012). Litestar und Pydantic nutzen breite kuratierte Listen.
- **Nicht mehr empfehlenswert:** getrennte Werkzeuge black/isort/flake8; ANN neben mypy strict.

### Typprüfung
- **Standard:** mypy strict (FastAPI, Litestar, structlog, httpx). Litestar zusätzlich `truthy-bool`, `unused-awaitable`, `possibly-undefined`; structlog `ignore-without-code`.
- **Spitzenprojekte:** zwei Checker (Litestar mypy + pyright; Pydantic pyright + mypy + pyrefly; attrs/structlog mypy für Code, pyright/pyrefly/ty für Typ-Tests).
- **Pyrefly** 1.0 seit 12.05.2026 stabil. **ty** noch 0.0.x (0.0.86), kein CI-Gate.
- `pyright --verifytypes` misst die Typvollständigkeit der öffentlichen API **[VW]**.

### Coverage
- coverage.py misst auf 3.14 standardmäßig über `sys.monitoring`, auch mit Branch-Coverage.
- Seit 7.10 `[run] patch = subprocess, _exit, execv, fork` — Subprozesse ohne `.pth`-Trick. Für swarmq entscheidend (Worker-, Forkserver- und CPU-Kinder).
- Seit 7.14 kombinieren die Report-Befehle automatisch.
- **Muster:** `branch`, `parallel`, eine Datei je Job als Artefakt, ein Kombinier-Job.
- **Ohne Drittanbieter:** Pydantic nutzt `py-cov-action/python-coverage-comment-action` (PR-Kommentar mit Diff-Coverage, Historie in einem Branch).
- `fail_under` ist uneinheitlich (Litestar 50, FastAPI 100 % über Smokeshow, httpx/Pydantic keins).

### Tests
- pytest 9: `strict = true` (bündelt `strict_config`, `strict_markers`, `strict_xfail`, `strict_parametrization_ids`) — nur mit gepinnter pytest-Version. Mit `filterwarnings = ["error"]` Standard bei Pydantic, attrs, FastAPI.
- pytest-asyncio 1.x: `asyncio_default_fixture_loop_scope` ausdrücklich setzen.
- pytest-randomly (structlog), pytest-xdist (attrs, Litestar, Pydantic).
- Mutation-Tests sind kein CI-Standard pro Push; höchstens nächtlich/wöchentlich.
- Trends: inline-snapshot (Pydantic), pytest-codspeed (structlog, attrs).

### CI auf GitHub Actions
- `astral-sh/setup-uv` v10.3; globales `permissions: contents: read`; `persist-credentials: false`; `concurrency` mit `cancel-in-progress`.
- **SHA-Pins mit Versionskommentar + zizmor + actionlint.** Im Mai 2026 wurden die Tags einer verbreiteten Action umgeschrieben, nur SHA-Pins waren geschützt.
- Ein Aggregator-Job (`re-actors/alls-green` oder `required-checks-passed`) als einziger Pflicht-Check.
- Pydantic testet 3.15 und 3.15t; free-threaded Builds werden üblich.

### Supply Chain
- Trusted Publishing über `pypa/gh-action-pypi-publish` — nur diese erzeugt **PEP-740-Attestations**, `uv publish` nicht.
- `uv audit` (OSV, SARIF) ersetzt pip-audit.
- Renovate (nutzt Astral) oder Dependabot mit Cooldown, dazu `uv lock --check`.
- GitHub Secret Scanning mit Push Protection ist für öffentliche Repos kostenlos. CodeQL und OpenSSF Scorecard verbreitet **[VW]**.

### Entwicklerfluss
- **PEP 735 `[dependency-groups]`** ist Standard (FastAPI, Pydantic, Litestar, structlog, attrs). Extras nur für Endnutzer-Features.
- Hooks: pre-commit-Konfiguration, ausgeführt mit **prek** (Rust; CPython, FastAPI, ruff nutzen es).
- Einstiegspunkt: tox 4 + tox-uv (attrs, structlog), make (Pydantic) oder direkt `uv run`; kein klarer Gewinner.
- Weitere Gates: `griffe check` gegen das letzte Tag (API-Brüche), towncrier, codespell/typos, `mkdocs build --strict` **[VW]**, deptry.

## Lücken und Empfehlungen je Bereich

### Tests
- Reihenfolge erzwingen (pytest-randomly oder xdist); die zwei gefundenen Tests sind echte Befunde.
- pytest `strict = true` und `filterwarnings = ["error"]`; die ResourceWarning in `_architecture.py` beheben.
- Die 74 Tests ≥ 100 ms nach `AGENTS.md` einsortieren; danach ein Mechanismus, der Überschreitungen meldet.
- xdist für Unit, sobald reihenfolgefest. Für Integration/e2e eigene Entscheidung (geteiltes Valkey/Broker).
- `asyncio_default_fixture_loop_scope` setzen.

### Lint und Format
- `ruff format` mit einem Umformatierungs-Commit und `.git-blame-ignore-revs`.
- Fachlich wichtige Familien zuerst: ASYNC, S, BLE, PT; dann PERF, LOG/G, DTZ, TID, TC.
- Grundsatzentscheidung: `ALL` mit begründeten Ausnahmen oder erweiterte kuratierte Liste.
- ruff auf 0.17.

### Typen
- mypy strict bleibt das Gate; zusätzliche Fehlercodes (`ignore-without-code`, `truthy-bool`, `unused-awaitable`, `possibly-undefined`, `redundant-expr`).
- Zweiter Checker für die Nutzersicht (pyright/basedpyright oder pyrefly), mindestens `--verifytypes`.
- Umfang auf ganz `tests/` ausweiten (Aufwandsfrage).

### Coverage
- Konfiguration mit `branch`, `parallel`, `patch = subprocess, fork, _exit`; Messung in Unit, Integration und e2e; kombinieren.
- Gate: Untergrenze gegen Rückschritte und Diff-Coverage für neue Zeilen (passt zur TDD-Regel „kein ungetesteter Code“).
- Bericht ohne Drittanbieter über py-cov-action.

### CI (im Zuge des GitHub-Umzugs)
- Der Umzugsplan (`docs/plans/2026-10-10-1156-chore-umzug-github-plan.md`) übernimmt die CI 1:1 (R10). Darauf aufbauend: SHA-Pins, zizmor, actionlint, Aggregator-Job, concurrency, Coverage-Kombination, xdist, Matrix (3.14, 3.14t, 3.15-dev nicht blockierend), Zeitpläne (Stress, Hypothesis `thorough`, randomisierte Reihenfolge nachts; Mutation wöchentlich), Chaos-Gate zurück in die CI (GitHub-Runner haben Docker), JUnit-Reports.

### Supply Chain
- Release über die PyPA-Action wegen der Attestations (fehlt im Umzugsplan).
- `uv audit` in der CI; Renovate oder Dependabot mit Cooldown.

### Entwicklerfluss
- `[dependency-groups]` statt Extra `test` (Gruppen etwa `test`, `lint`, `typing`, `docs`, `mutation`).
- `.pre-commit-config.yaml` mit prek.
- Ein Einstiegspunkt für lokal und CI (justfile/Makefile oder tox-uv), inklusive der Port-Wrapper 5699/6399.
- `mkdocs build --strict`, codespell/typos, griffe, towncrier, deptry.

## Vorgeschlagene Reihenfolge

1. **Stufe A, unabhängig vom CI-Host:** Dependency-Groups, `ruff format`, pytest strict + `filterwarnings`, Reihenfolge-Befunde und ResourceWarning, Coverage-Konfiguration, Hooks, Einstiegspunkt.
2. **Stufe B, Lint-Ausbau:** Regelfamilien schrittweise.
3. **Stufe C, GitHub-Umzug 1:1**, danach Sicherheit, Coverage-Gate, xdist, Matrix, Zeitpläne, `uv audit`, Renovate, Attestations.
4. **Stufe D, Spitzenklasse:** Chaos-Gate in der CI, pyright `--verifytypes`, griffe, CodSpeed, Mutation wöchentlich, Scorecard.

Entscheidung vom 2026-10-11: Begonnen wird mit ruff und mypy
(Plan `docs/plans/2026-10-11-1200-chore-ruff-mypy-ausbau-plan.md`).

## Umsetzungsstand

**Lint und Typen — erledigt** (Branch `chore/ruff-mypy-ausbau`, Plan
2026-10-11-1200): ruff 0.17 mit `select = ["ALL"]` und begründetem
Ausnahmekatalog, `ruff format` über den Bestand (`.git-blame-ignore-revs`),
mypy 2.4 strict mit Zusatzcodes über `src/` und ganz `tests/`, basedpyright
strict über `src/`, prek-Hooks (Commit: Format/Lint, Push: Typen),
Dependency-Groups `lint`/`typing`.

**Teststrenge, Coverage und Einstiegspunkt — umgesetzt** (Branch
`chore/teststrenge-coverage-just`, Plan 2026-10-13-0902, Stufe A):

- pytest `strict = true` und `filterwarnings = error`. Der GC-Lauf nach jedem
  Test, jeder Klasse und jedem Modul mit Diensten ordnet Lecks dem Test zu,
  der sie erzeugt. Dabei behoben: zwei Valkey-Lecks in swarmq
  (`ValkeyBackend.close()` mit laufenden Befehlen, Nachholversuche des
  Ausführungsschutzes nach Worker-Abbruch). Drei Lecks in aiormq/aio-pika
  sind eng ausgenommen (`docs/befunde/teststrenge-2026-10.md`, TS-01 bis
  TS-03).
- pytest-randomly in jedem Lauf, keine Reruns (pytest-rerunfailures
  entfernt). Gefundene Reihenfolge-Abhängigkeiten an der Ursache behoben
  (Supervisor-Kanal, Logging-Fork-Liste, Leader-Sperre aus e2e vor bz14,
  `sys.prefix` im Hot-Reload-Test, globale Dishka-Test-Tasks).
- Bericht über langsame Unit-Tests (> 100 ms), ohne Exit-Code-Wirkung.
- Coverage über alle Prozessmodelle (`patch = subprocess, _exit, execv,
  fork`), 96,5 % Zeilen und Zweige, Schwankung 0,01 Prozentpunkte über sechs
  Läufe. Gefunden: Scheduler meldete einen gehaltenen Cron vor dem Commit
  des Termins (behoben).
- CI: Combine-Job mit Cobertura-Bericht, Ratchet gegen den Merge-Base mit
  `main` und diff-cover 100 % für neue Zeilen; `fail_under = 96` seit
  2026-10-15 (`uv run just ratchet` aus drei `main`-Läufen: 96,53 / 96,52 /
  96,53).
- `justfile` als Einstiegspunkt für Lint, Typen und alle Suiten; feste
  Testports 5699/6399, gemeinsame Sperre, Seed immer ausgegeben.

Offen bleiben die übrigen Lücken dieses Dokuments (CI-Sharding, CI-Sicherheit,
Matrix, Supply Chain, Docstrings) und eine ungeklärte Häufung von fünf
Fehlern in einem von sechs Coverage-Läufen.
