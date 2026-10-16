# Fix-Recherche Konfiguration, TaskInfo, Backoff (F3-04, F3-08, F3-14, F3-16, F3-31)

Stand: Worktree `swarmq-fehlersuche3` (Branch `test/fehlersuche-runde-3`, HEAD `580ef3c`), Register `compound-packs/swarmq-entscheidungen/` mit den Regeln und Nachträgen vom 2026-10-15. Nur gelesen. Bibliotheken aus `.venv`: croniter 6.2.4, valkey 6.1.1, msgspec 0.21.1.

Ausgeführt: alle fünf Unit-Nachweise mit `--runxfail -p no:randomly`. Jeder scheitert aus dem genannten Grund: F3-04 92 Fälle mit `DID NOT RAISE ConfigurationError`, F3-08 2 Fälle mit `DID NOT RAISE ValueError`, F3-14 `10 Slots statt 3`, F3-16 3 Fälle `7200.0 > 3600.0`, F3-31 6 Fälle (`result_ttl bleibt 86400.0` usw.). Per Probe im `.venv` geprüft: croniter-Verhalten bei Daten ohne Termin, `math.ceil(inf/nan)`, msgspec bei `float` in einem `int`-Feld.

Architektur-Guard: `tests/unit/test_architecture_guards.py:37` (`_MAX_MODULE_LINES = 800`). Die Dateien dieses Pakets sind weit darunter: `config.py` 674, `task.py` 504, `config_validation.py` 251, `slots.py` 105, `backoff.py` 90. Keiner der Fixes braucht `worker/pipeline.py` (797 Zeilen). Der Paket-Zyklus-Guard verbietet eine Kante `swarmq` → `swarmq.scheduler`, weil `scheduler/fire.py:21` schon `swarmq.task` importiert. Das ist für F3-08 wichtig (siehe dort).

---

## F3-04 (P2): `inf`/`nan` in Frist- und TTL-Feldern der Konfiguration

### Ursache

- `config_validation.py:125-130`: Die Schleife über `_POSITIVE_FIELDS` prüft nur `value <= 0`. `inf` und `nan` bestehen das.
- `config_validation.py:163-168`: `_validate_finite` prüft nur `lock_wait_max_s` (Commit `2962bf2`, Review MR 13, P2-3). `log_drain_timeout` prüft `logging_settings.valid_drain_timeout` gesondert (`:178-183`), `scheduler_publish_stepdown_s` wird in `_validate_scheduler_relations` (`:192-195`) geprüft.
- Nicht geprüft sind außerdem Felder außerhalb von `_POSITIVE_FIELDS`: `retry_max_delay` (`config.py:216`, gar keine Prüfung, auch nicht `> 0`), `unknown_task_grace_s` (nur `< 0`, `:101-103`), `shutdown_timeout` (keine Prüfung, 0 ist bewusst erlaubt, `:77-80`), `bind_retry_budget_s` (kein Env, vom Supervisor gesetzt) und die berechneten TTLs `scheduler_dedup_ttl`, `execution_guard_done_ttl_s`.
- Folgefelder: `config.py:654-665` leitet `scheduler_dedup_ttl = max(60, retry_max_delay + 300)` und `execution_guard_done_ttl_s = max(max(retry_max_delay, consumer_timeout_s) + 300, …)` ab. `SWARMQ_CONSUMER_TIMEOUT_S=inf` macht also auch den Done-Marker unendlich. Daraus kommt der `OverflowError` vor jedem Claim im Ausführungsschutz. `dead_letter_ttl_s=inf` lässt `aio_pika.Message(expiration=…)` scheitern. `worker/recycling.py:103` nimmt `consumer_timeout_s` als Drain-Untergrenze; mit `inf` wird der Recycling-Drain unbegrenzt.
- Env-Weg: `parse_env_value` (`config.py:114-139`) nimmt `"inf"`, `"nan"`, `"Infinity"`, `"1e999"` über `float()` an.

Der Nachweis `tests/unit/test_befund_f3_04_finite_deadlines.py` prüft das entschiedene Verhalten richtig: 26 Fließkomma-Fristen, kwarg und Env, `inf` und `nan`, Meldung nennt das Feld. Er nimmt 6 Fälle aus, die heute schon über Relationsprüfungen scheitern (`_ALREADY_REJECTED`). Abweichung zur Regel: keine. Lücke im Nachweis: `shutdown_timeout`, `bind_retry_budget_s` und `result_ttl` fehlen. `result_ttl` deckt F3-31 ab (siehe unten), die anderen beiden sollte der Fix-Test ergänzen.

### Lösungsoptionen

**A – Eine Endlichkeitsprüfung über eine Feldliste in `ConfigValidation` (empfohlen).**
- Neue ClassVar `_FINITE_FIELDS` in `config.py`: `_POSITIVE_FIELDS` + `("retry_max_delay", "unknown_task_grace_s", "shutdown_timeout", "bind_retry_budget_s", "scheduler_dedup_ttl", "execution_guard_done_ttl_s")`. Reihenfolge wichtig: erst die Eingabefelder, dann die abgeleiteten. Sonst nennt die Meldung bei `retry_max_delay=inf` das Feld `scheduler_dedup_ttl` statt der Ursache. `consumer_timeout_s` steht schon in `_POSITIVE_FIELDS` und kommt so vor `execution_guard_done_ttl_s`.
- `_validate_finite` iteriert die Liste, überspringt `None` (`dead_letter_ttl_s=None` heißt unbegrenzt) und wirft `ConfigurationError(f"{name} must be a finite number, got {value}")`. Die Meldung erfüllt den E-063-Test `test_must_be_finite` (`match="lock_wait_max_s must be a finite number"`). Ganzzahlfelder in `_POSITIVE_FIELDS` (`delivery_limit`, `malloc_*` usw.) laufen mit; `math.isfinite(int)` ist immer wahr, ein `float("inf")` als kwarg wird dort ebenfalls abgefangen.
- `_validate_finite()` an den Anfang von `_validate_core` ziehen (direkt nach `_apply_pool_defaults`, vor `_validate_pool`). Dann nennen auch die 6 heute über Relationen abgelehnten Fälle das richtige Feld. Der Nachweis kann `_ALREADY_REJECTED` danach streichen (der Test trägt keinen `entscheidung`-Marker und darf erweitert werden).
- `ConfigValidation`-Stubs (`config_validation.py:20-57`) um `_FINITE_FIELDS: ClassVar[tuple[str, ...]]` ergänzen.

**B – Endlichkeit beim Env-Parsen (`_parse_float`).** Fängt kwargs nicht ab, verletzt E-063 („als Argument wie als Env-Wert“). Verworfen.

**C – Je Feld eine eigene Prüfung wie bisher.** Wiederholt das Muster aus `2962bf2` 30-mal. Verworfen.

### Folgefehler-Analyse

- **Entscheidungen.** E-063 `tests:` – `test_config_lock_wait.py::test_must_be_finite` (Meldungspräfix bleibt), `test_config_stepdown.py::test_negative_or_infinite_is_rejected` (eigene Prüfung in `_validate_scheduler_relations`, unberührt), `test_logging_settings.py::TestValidation::test_non_positive_non_numeric_or_infinite_drain_timeout_is_a_configuration_error` (eigene Prüfung, unberührt; `log_drain_timeout` nicht in die neue Liste aufnehmen, sonst ändert sich bei `inf` die Meldung). E-057 (Exit 78 für Konfigurationsfehler) gilt automatisch, weil `ConfigurationError`.
- **Bestehende Tests.** `test_config.py:275` setzt `retry_max_delay=0.0` (endlich, bleibt grün). Kein Test setzt absichtlich `inf` in einem dieser Felder (geprüft per grep).
- **Nachbarpfade mit eigenem Env-Parsing (nicht über `SwarmQConfig`):**
  - `cli/_shared.py:265-272`: Die `swarmq dlq`-Befehle lesen `SWARMQ_DEAD_LETTER_TTL_S` selbst und prüfen nur `<= 0`. `inf` geht durch und lässt das Zurücklegen von Altbestand an `aio_pika.Message` scheitern. Gehört nach E-063 in diesen Fix (`math.isfinite`, gleiche Meldung).
  - `cli/env_check_cmd.py:96-101` (`_seen_interval_s`): `inf` aus der Umgebung geht in die Berechnung des Meldefensters. Folge nur in der Diagnose; als Härtung mitnehmen oder als P3 notieren.
  - `cli/_shared.py:80-88` (`_positive_float`, genutzt für `--burst-idle-timeout`, `cli/parser.py:378`): `inf` besteht. Mit dem Fix scheitert es erst im Worker-Kind an `SwarmQConfig` (Exit 78), nicht schon beim Parsen. Empfehlung: `math.isfinite` in `_positive_float`, dann Exit 2 von argparse vor dem Start. Kleine Härtung, eigener Test.
- **Bibliotheksverhalten.** aio-pika/pamqp rechnen `expiration` in ganze Millisekunden um; `int(inf)` wirft `OverflowError`, `int(nan)` `ValueError` (in Python geprüft). valkey-py 6.1.1 verlangt für `ex`/`px` `int` oder `timedelta` (`valkey/commands/core.py:2305, 2313`).
- **Mischbetrieb 0.12.0–0.12.2.** Nur lokale Prüfung beim Start, kein Format. Eine Installation, die heute `SWARMQ_…=inf` setzt und damit „irgendwie“ läuft (etwa `SWARMQ_SCHEDULER_PRUNE_AFTER_S=inf`: nur ein Log je Takt), startet mit 0.12.3 nicht mehr (Exit 78). Das gehört als Upgrade-Hinweis in den Changelog. Rollback: Rückkehr zum alten Verhalten, keine Datenfolgen.
- **Nicht im Umfang (Hinweis).** `TaskInfo`-Felder `soft_timeout`, `hard_timeout`, `max_age`, `retry_base_delay` fallen nicht unter E-063 (Regel spricht von der Konfiguration). Per Code-Lesung: `max_age=inf` besteht `<= 0` und führt in `worker/gates/execution_guard.py:303` zu `int((inf + 300) * 1000)` → `OverflowError` je Nachricht; `retry_base_delay=nan` besteht `< 0`. Nicht per Lauf bestätigt. Kandidat für eine spätere Fehlersuche, nicht für 0.12.3. Negatives `retry_max_delay` ist laut Register (Zeile 95) bewusst kein Befund.

### Doku und Changelog

- `website/reference/configuration.md` und `.de.md`: Ein Satz vor der Tabelle „Worker behaviour“: Jede Frist, TTL und Zeitangabe muss endlich sein; `inf`/`nan` beenden den Start mit Exit 78; wo „unbegrenzt“ vorgesehen ist, heißt es `none` (bisher nur `SWARMQ_DEAD_LETTER_TTL_S`). Die Zeilen `:49-56, 66, 72, 118, 129` („Must be `> 0`“) zu „`> 0` and finite“ ergänzen. `SWARMQ_LOCK_WAIT_MAX_S` sagt es schon.
- Changelog EN/DE, Abschnitt Fixed: „`inf` and `nan` in a deadline, TTL or interval setting (`SWARMQ_CONSUMER_TIMEOUT_S=inf`, `dead_letter_ttl_s=float('inf')`, …) are now a configuration error (exit 78) instead of breaking every task at runtime. Use `none` where unlimited is supported.“ Dazu ein Upgrade-Hinweis (siehe Mischbetrieb).

### Testszenarien

1. Nachweis: alle 26 Felder × `inf`/`nan` × kwarg/Env → `ConfigurationError`, Meldung nennt das Feld. Nach dem Umzug von `_validate_finite` auch die 6 Fälle aus `_ALREADY_REJECTED`.
2. `retry_max_delay=inf` → Meldung nennt `retry_max_delay`, nicht `scheduler_dedup_ttl`. `SWARMQ_CONSUMER_TIMEOUT_S=inf` → Meldung nennt `consumer_timeout_s`.
3. `shutdown_timeout=float("inf")` → `ConfigurationError`; `shutdown_timeout=0` bleibt erlaubt.
4. `dead_letter_ttl_s=None` und `SWARMQ_DEAD_LETTER_TTL_S=none` bleiben gültig.
5. `-inf` bleibt bei der Meldung „must be > 0“ (Prüfreihenfolge).
6. `swarmq dlq …` mit `SWARMQ_DEAD_LETTER_TTL_S=inf` → `ConfigurationError` (Unit gegen `cli/_shared.py`, Broker gemockt).
7. Optional: `--burst-idle-timeout inf` → argparse-Fehler.

### Betroffene Dateien

`src/swarmq/config.py` (ClassVar `_FINITE_FIELDS`), `src/swarmq/config_validation.py` (`_validate_finite`, Stub, Aufrufstelle), `src/swarmq/cli/_shared.py` (DLQ-TTL, optional `_positive_float`), optional `src/swarmq/cli/env_check_cmd.py`; Tests `tests/unit/test_befund_f3_04_finite_deadlines.py` (xfail weg, Marker), `tests/unit/test_config.py` oder neuer Test für Szenario 2–5, Test für `cli/_shared.py`; Doku `website/reference/configuration.md`, `configuration.de.md`, `website/reference/changelog.md`, `changelog.de.md`.

### Marker nach dem Fix

Nachweis: `xfail` entfernen, `@pytest.mark.entscheidung("E-063")` ergänzen, `befund("F3-04")` bleibt. E-063: beide Testfunktionen unter `tests:` eintragen, `ohne_test:` streichen.

---

## F3-08 (P3): Cron ohne nächsten Termin wird beim `TaskInfo`-Bau angenommen

### Ursache

- `task.py:240-243`: `croniter(self.cron, datetime.now(UTC))` prüft nur die Syntax. croniter wirft erst bei `get_next()`, wenn es keinen Termin findet.
- Der Leader prüft genau das (`scheduler/sync.py:30-41`, `_validated` → `next_fire_ms` → `cron_time._iter_fires` → `croniter.get_next`) und verwirft die Meldung mit `scheduler_cron_report_invalid`. Der Cron feuert still nie.

Der Nachweis `tests/unit/test_befund_f3_08_cron_without_next_fire.py` prüft `0 0 30 2 *` und `0 0 31 4 *` → `ValueError` mit „cron“ in der Meldung. Das entspricht E-065. Er hält die Feldzahl (F3-35) korrekt heraus.

### croniter 6.2.4 (im `.venv` geprüft)

| Ausdruck | `croniter(...)` | `get_next(datetime)` | `croniter.expand(strict=True)` |
|---|---|---|---|
| `0 0 30 2 *` | ok | `CroniterBadDateError: failed to find next date` (1,2 ms) | `CroniterBadCronError: Day(s) [30] can never occur in month(s) [2]` |
| `0 0 31 4 *` | ok | `CroniterBadDateError` (0,8 ms) | `CroniterBadCronError` |
| `0 0 29 2 *` | ok | 2028-02-29 | ok |
| `0 0 30 2 1` | ok | `CroniterBadDateError` | `CroniterBadCronError` |
| `0 0 * * * 0 2020` (7 Felder, Jahr vorbei) | ok | `CroniterBadDateError` | ok |

- Keine Endlosschleife: croniter sucht höchstens `max_years_between_matches` Jahre (Default 50, `croniter.py:315-317, 728`) und wirft dann `CroniterBadDateError`. Kosten im Fehlerfall rund 1 ms, im Normalfall rund 0,1 ms. Das passt in das Budget eines Unit-Tests und in jeden Modul-Import.
- `CroniterBadDateError` erbt von `CroniterError(ValueError)` (`croniter.py:236, 256`). Der Stub `typings/croniter/__init__.pyi` deklariert die Klasse und `get_next(ret_type: type[datetime])` schon. Kein Stub-Nachtrag nötig.
- `0 0 30 2 1` würde in klassischer cron-Semantik (Tag ODER Wochentag) an Februar-Montagen feuern; croniter findet keinen Termin. Für den Fix zählt nur: Der Leader nimmt ihn genauso nicht an. Die Meldung sollte deshalb „has no next fire time“ sagen, nicht „invalid date“.

### Lösungsoptionen

**A – Nächsten Termin mit `get_next` wie der Leader berechnen (empfohlen).**

```python
try:
    croniter(self.cron, datetime.now(UTC)).get_next(datetime)
except CroniterBadDateError as exc:
    raise ValueError(f"cron never fires: {self.cron!r} has no next fire time") from exc
except (CroniterBadCronError, ValueError) as exc:
    raise ValueError(f"cron is not a valid expression: {self.cron!r} ({exc})") from exc
```

`CroniterBadDateError` muss vor `ValueError` stehen, sonst greift der allgemeine Zweig (geht auch, dann aber mit der unpassenden Meldung „not a valid expression“). Die Zone spielt keine Rolle: Ob es überhaupt einen Termin gibt, hängt nicht von der Zeitzone ab. UTC genügt; `timezone` wird erst danach normalisiert (`task.py:245-251`).

**B – `croniter.expand(strict=True)`.** Bessere Meldung, aber nicht deckungsgleich mit dem Leader: Ein 7-feldriger Ausdruck mit vergangenem Jahr besteht `strict`, der Leader verwirft ihn trotzdem. Verworfen.

**C – `swarmq.scheduler.cron_time.next_fire_ms` in `task.py` importieren.** Exakte Parität samt DST-Regel, aber Paketzyklus `swarmq` ↔ `swarmq.scheduler` (`scheduler/fire.py:21` importiert `swarmq.task`). Der Architektur-Guard schlägt an. Verworfen.

### Folgefehler-Analyse

- **Entscheidungen.** E-065 `tests:` – `test_task.py::test_taskinfo_cron_extended_{L,W,hash}_syntax_rejected` (Prüfung davor, unberührt), `test_task.py::test_taskinfo_cron_invalid_syntax_raises` (`match="cron"`, `not-a-cron` scheitert weiter im Konstruktor), `test_reports.py::TestAdoptReportedCrons::test_invalid_report_is_not_adopted_and_logged` (Leader-Pfad bleibt). Alle bleiben grün. E-057: Ein `ValueError` beim Import endet mit Exit 1, nicht 78; so gewollt.
- **Leader-Prüfung bleibt.** `sync.py:30-41` schützt weiter gegen Meldungen älterer oder fremder Worker (E-065). Nicht anfassen.
- **Abgrenzung F3-35.** Der Fix ändert die Feldzahl-Regel nicht. Nebenwirkung: Ein 7-feldriger Ausdruck mit vergangenem Jahr scheitert nun beim Import, weil er keinen Termin hat. Das folgt aus E-065 („nächster Termin“), nicht aus einer Feldzahl-Entscheidung. Der Test soll keine 6/7-Feld-Fälle enthalten.
- **Andere Pfade.** CLI `schedule add --cron` rechnet schon über `next_fire_ms` (`cli/schedule_cmd.py:146-153`) und meldet `CroniterBadDateError` als `ConfigurationError` (Exit 78). Kein Handlungsbedarf.
- **Tests mit festgehaltener Zeit.** `get_next` liest `datetime.now(UTC)`, wie der heutige Konstruktoraufruf. Kein Test baut absichtlich einen Cron ohne Termin (grep nach `30 2 *`, `31 4 *`).
- **Mischbetrieb.** Ein 0.12.3-Worker mit so einem Cron startet nicht mehr (Exit 1 beim Import). Bisher startete er, und der Cron feuerte nie. Upgrade-Hinweis im Changelog. Ältere Worker melden weiter; der Leader (jede Version) verwirft wie bisher. Rollback ohne Folgen.

### Doku und Changelog

- `website/reference/configuration.md:471` (`.de.md` entsprechend): „Standard 5-field cron expression with at least one future fire time. Extended syntax … is rejected; an expression that never fires (`0 0 30 2 *`) raises `ValueError`.“ Unter „Validation behaviour“ (`:494-505`) ein Beispiel ergänzen.
- Changelog EN/DE: Fixed – „A `TaskInfo` cron without a next fire time (`0 0 30 2 *`) now raises `ValueError` when the task module is imported, instead of being silently dropped by the scheduler leader.“ Upgrade-Hinweis: Ein Worker mit so einem Cron startet nicht mehr.

### Testszenarien

1. Nachweis: `0 0 30 2 *`, `0 0 31 4 *` → `ValueError` mit `cron`.
2. `0 0 29 2 *` (Schaltjahr) → gültig.
3. `0 0 31 * *` → gültig (Monate mit 31 Tagen).
4. Meldung enthält „no next fire time“ (eigener Test, Verhalten statt Struktur: der Nutzer sieht den Grund).
5. Mit `timezone="Europe/Berlin"` und `0 0 30 2 *` → `ValueError` vor der Zonenprüfung.

### Betroffene Dateien

`src/swarmq/task.py` (Import Zeile 16, `_validate_scheduling` `:234-243`); Tests `tests/unit/test_befund_f3_08_cron_without_next_fire.py`, `tests/unit/test_task.py` (Szenarien 2–5); Doku `website/reference/configuration.md`, `.de.md`, Changelog EN/DE.

### Marker nach dem Fix

Nachweis: `xfail` weg, `@pytest.mark.entscheidung("E-065")`. E-065 `tests:` um `tests/unit/test_befund_f3_08_cron_without_next_fire.py::test_taskinfo_rejects_cron_that_never_fires` ergänzen; den Absatz „Überdenken“ um „F3-08 behoben in 0.12.3“ nachtragen. F3-35 bleibt offen.

---

## F3-14 (P3): Eingebetteter CPU-Pool nimmt 10 Slots statt der zugeteilten CPUs

### Ursache

- `config.py:190`: `concurrency: int = 10` unabhängig vom Pool. `_apply_pool_defaults` (`config.py:559-566`) setzt im CPU-Modus nur die Recycling-Voreinstellungen.
- Die CLI löst richtig auf: `cli/pool.py:29-43` (`prepare_pool_args` → `slots.effective_concurrency`), aufgerufen in `cli/__init__.py:336-337`. Der aufgelöste Wert geht über `worker_config_kwargs` (`cli/worker_options.py:65-76`) als `resolved_fields` an `SwarmQConfig`. Der eingebettete Weg `SwarmQ(broker, backend, pool="cpu")` (`app.py:54`) bekommt den Dataclass-Default 10.
- Slots wirken in `worker/compute/wiring.py:116, 131` (Zahl der Rechenkinder) und `worker/pipeline.py:102` (Semaphor).

Der Nachweis `tests/unit/test_befund_f3_14_embedded_cpu_concurrency.py` mockt `os.sched_getaffinity` (3 CPUs) und `slots._cgroup_quota`, löscht `SWARMQ_CONCURRENCY` und prüft: ohne `concurrency` 3 Slots, mit `concurrency=10` 10. Das entspricht E-061.

### Lösungsoptionen

Ein Sentinel muss „nicht gesetzt“ von „ausdrücklich 10“ trennen. Danach wird in `_apply_pool_defaults` (läuft am Anfang von `_validate_core`, also nach den Env-Overrides) aufgelöst.

**A – Sentinel als `int`-Unterklasse (empfohlen).**

```python
class _DefaultConcurrency(int):
    """Marker: concurrency nicht gesetzt (E-061)."""

_DEFAULT_CONCURRENCY = _DefaultConcurrency(DEFAULT_IO_CONCURRENCY)
...
concurrency: int = _DEFAULT_CONCURRENCY
...
def _apply_pool_defaults(self) -> None:
    if type(self.concurrency) is _DefaultConcurrency:
        self.concurrency = effective_concurrency(None, self.pool).count
    if self.pool != "cpu":
        return
    ...
```

- Öffentlicher Typ bleibt `int`; kein Leser muss geändert werden, auch nicht `pipeline.py` (797 Zeilen).
- Typprüfung über `type(...) is`, nicht über Identität: `copy.deepcopy` erzeugt eine neue Instanz derselben Klasse.
- `SWARMQ_CONCURRENCY` (Env) und jedes kwarg liefern ein gewöhnliches `int` und gewinnen. `resolved_fields` aus der CLI ebenso.
- `slots.py` importiert nur `swarmq.exceptions`; `config.py` → `swarmq.slots` erzeugt keinen Zyklus.

**B – `concurrency: int | None = None`, Auflösung in `__post_init__`.** Saubere Semantik (`None` = Voreinstellung wie in der CLI), aber der öffentliche Feldtyp ändert sich. mypy/pyright melden dann jeden Leser: `lifecycle.py:366, 430, 449, 578`, `compute/wiring.py:116, 131`, `pipeline.py:102` (jeder braucht ein `assert` oder `cast`; `pipeline.py` liegt an der 800er-Grenze). Nutzer, die `config.concurrency` typgeprüft lesen, bekommen `int | None`. Für ein Patch-Release zu breit.

**C – `0` als „automatisch“.** Heute ist `concurrency=0` ein `ConfigurationError` (`config_validation.py:97-99`). `SWARMQ_CONCURRENCY=0` würde still zur Voreinstellung; das verdeckt einen Tippfehler (Geist von E-056). Verworfen.

### Folgefehler-Analyse

- **Entscheidungen.** E-061 `tests:` – `test_slots.py::test_effective_concurrency_cpu_uses_assigned_cpus`, `::test_effective_concurrency_explicit_wins_for_every_pool`: `slots.py` bleibt unverändert, beide grün.
- **IO-Default.** `test_config.py:18` (`cfg.concurrency == 10`) bleibt grün: `effective_concurrency(None, "io")` liefert 10.
- **CLI und Supervisor.** Unberührt: Die CLI übergibt immer ein aufgelöstes `int` (`test_cli_multiprocess.py:172`). Der Supervisor baut `SwarmQConfig` in `process_manager/run.py:150` ohne `pool` (IO, 10, wird dort nicht für Slots genutzt).
- **`SWARMQ_POOL=cpu` eingebettet.** Der Pool kommt über `_apply_env_overrides` vor `_validate_core`; die Auflösung sieht ihn. Gewollt (E-061 gilt auch so).
- **Ungültiger Pool.** `effective_concurrency(None, "gpu")` liefert 10, danach wirft `_validate_pool`. Reihenfolge unkritisch.
- **`dataclasses.replace(cfg, pool="cpu")`** auf einer schon aufgelösten IO-Konfiguration übergibt `concurrency=10` als `int` und gilt als ausdrücklich. Randfall; in `src/` nutzt niemand `replace` auf `SwarmQConfig`. In der Doku nicht erwähnen.
- **Kosten.** `os.sched_getaffinity` und ein Lesen von `/sys/fs/cgroup/cpu.max` je `SwarmQConfig` (nur wenn nicht gesetzt). Vernachlässigbar. Unit-Tests, die `SwarmQConfig()` bauen, lesen im IO-Modus nichts (`effective_concurrency` geht für `io` nicht an `resolve_default_slots`).
- **Mischbetrieb.** Nur lokal, kein Format. Rollback: wieder 10 Slots.
- **Verhaltensänderung.** Eingebettete CPU-Nutzer ohne `concurrency` bekommen weniger Slots (E-061 „Bewusste Folgen“). Changelog-Hinweis Pflicht.

### Doku und Changelog

- `website/reference/configuration.md:34` sagt es schon richtig („`10` (io) / assigned CPUs (cpu)“). Ergänzen: „also for an embedded `SwarmQConfig(pool="cpu")` since 0.12.3“ (DE analog).
- `website/guides/cpu-mode.md:18` (`.de.md` analog): Die Tabelle nennt nur `--concurrency`; einen Satz zum eingebetteten Betrieb ergänzen.
- Changelog EN/DE, Changed + Upgrade-Hinweis: „An embedded CPU pool (`SwarmQConfig(pool="cpu")` / `SwarmQ(..., pool="cpu")`) without `concurrency` now uses the CPUs assigned to the process (affinity and cgroup quota) instead of 10 slots, like `swarmq worker --pool cpu`. Set `concurrency=` to keep the old number.“

### Testszenarien

1. Nachweis: 3 zugeteilte CPUs, `pool="cpu"` → 3; `concurrency=10` → 10.
2. `SWARMQ_POOL=cpu` ohne kwargs → zugeteilte CPUs.
3. `SWARMQ_CONCURRENCY=6` mit `pool="cpu"` → 6.
4. cgroup-Quote 2 bei Affinität 8 → 2 (Quelle cgroup).
5. `pool="io"` ohne `concurrency` → 10, Typ `int` (nicht die Marker-Klasse, damit kein Leser sie sieht).
6. `concurrency=0` → weiter `ConfigurationError`.

### Betroffene Dateien

`src/swarmq/config.py` (Sentinel, Feld-Default, `_apply_pool_defaults`, Import aus `swarmq.slots`); Tests `tests/unit/test_befund_f3_14_embedded_cpu_concurrency.py`, Szenarien 2–6 in `tests/unit/test_config.py` (oder eigene Datei); Doku `website/reference/configuration.md`, `.de.md`, `website/guides/cpu-mode.md`, `.de.md`, Changelog EN/DE.

### Marker nach dem Fix

Nachweis: `xfail` weg, `@pytest.mark.entscheidung("E-061")`. E-061 `tests:` ergänzen, `ohne_test:` streichen.

---

## F3-16 (P3): `fixed`-Backoff wird nicht durch `retry_max_delay` gedeckelt

### Ursache

- `backoff.py:28-29`: `if strategy == "fixed": return base_delay`, vor jedem Deckel.
- Einziger Aufrufer: `worker/retry.py:128-130` (`calculate_backoff(retry_count, task_info.retry_backoff, task_info.retry_base_delay, deps.config.retry_max_delay)`), danach `delay_ms = int(delay_seconds * 1000)`.
- Damit hält die Invariante `scheduler_dedup_ttl >= retry_max_delay` (`config_validation.py:212-219`) für Cron-Retries mit `fixed` nicht.

Der Nachweis `tests/unit/test_befund_f3_16_fixed_backoff_cap.py` prüft `calculate_backoff(attempt, "fixed", 7200, 3600) == 3600.0` für Versuch 0, 3, 100. Das entspricht E-023 (Nachtrag 2026-10-15).

### Lösung

`return float(min(base_delay, max_delay))`. Keine Alternative nötig; E-023 legt genau das fest. Docstring `backoff.py:17-27` um „`fixed`: `base_delay`, at most `max_delay`, no jitter“ ergänzen.

### Folgefehler-Analyse

- **E-023-Test.** `test_backoff.py::test_fixed_strategy_returns_base_delay` (`base=5 < max=3600`) bleibt grün; nicht anfassen. Die BZ-26-Tests (`test_befund_bz26_backoff.py`) betreffen nur `exponential`/`linear`.
- **Bricht einen Test ohne Marker:** `tests/property/test_backoff_properties.py:132-139` `test_fixed_ignores_attempt_and_cap` erwartet `d == base` für `base ∈ [0, 1e3]`, `max_delay ∈ [0, 1e6]`. Hypothesis findet sofort `base > max_delay`. Der Test schreibt das alte, jetzt entschiedene Gegenteil fest („intentionally un-capped“) und trägt keinen `entscheidung`-Marker. Er muss im selben Commit auf `d == min(base, max_delay)` umgestellt und umbenannt werden (z. B. `test_fixed_ignores_attempt_and_respects_cap`); Modul-Docstring `:9` ebenso. Das ist eine Anpassung an die geänderte Regel, kein Abschwächen.
- **Kommentar ohne Testwirkung:** `test_backoff.py:94-97` („even if base > max_delay the formula is just base“) mit `base=5 < max=10` bleibt grün, der Kommentar wird falsch. Korrigieren; die markierte Testfunktion in derselben Datei nicht berühren.
- **e2e/compat.** Alle `retry_backoff="fixed"`-Tasks in `tests/e2e/*` und `tests/compat/_compat_tasks.py` nutzen `retry_base_delay` 0,1 bis 10 s bei `retry_max_delay` 3600: unverändert.
- **Explizites `Retry(delay=…)`.** Wird nicht gedeckelt (E-060: „genau diese Wartezeit“). Die Dedup-Invariante bleibt damit für `Retry(delay > retry_max_delay)` umgehbar. Das ist nicht F3-16 und widerspricht keiner Regel; nur als Hinweis.
- **Nebenwirkung.** `retry_base_delay=inf` in `TaskInfo` ergibt jetzt `retry_max_delay` statt `OverflowError` in `int(inf * 1000)`. `nan` bleibt `nan` (`min(nan, x)` liefert `nan`), siehe Hinweis bei F3-04.
- **Mischbetrieb.** Die Wartezeit rechnet der scheiternde Worker. Alte Worker warten länger, neue kürzer; kein Format. Rollback ohne Folgen. Tasks mit `retry_base_delay > retry_max_delay` wiederholen sich nach dem Update früher: Changelog.

### Doku und Changelog

- `website/guides/scheduling-and-retries.md:595` Tabelle: `fixed` → „`retry_base_delay`, at most `retry_max_delay`“. `:601-602`: „A computed delay above the cap becomes 0.6 to 0.8 × `retry_max_delay` (`exponential`, `linear`); `fixed` waits exactly `retry_max_delay`.“ `.de.md:619-625` analog.
- Changelog EN/DE, Fixed: „`retry_backoff="fixed"` now respects `retry_max_delay`: a `retry_base_delay` above the cap waits exactly `retry_max_delay`.“

### Testszenarien

1. Nachweis: `base=7200, max=3600` → genau 3600.0 für Versuch 0/3/100.
2. Property: `fixed` liefert `min(base, max_delay)`, unabhängig vom Versuch.
3. `base == max_delay` → genau dieser Wert.
4. `RetryHandler` mit `fixed`, `retry_base_delay=7200`, `config.retry_max_delay=3600` → publizierte Wartezeit 3 600 000 ms (Unit gegen `worker/retry.py`, Broker gemockt), damit der Aufrufer abgedeckt ist.

### Betroffene Dateien

`src/swarmq/backoff.py`; Tests `tests/unit/test_befund_f3_16_fixed_backoff_cap.py`, `tests/property/test_backoff_properties.py`, `tests/unit/test_backoff.py` (nur Kommentar), optional `tests/unit/worker/test_retry.py`; Doku `website/guides/scheduling-and-retries.md`, `.de.md`, Changelog EN/DE.

### Marker nach dem Fix

Nachweis: `xfail` weg, `@pytest.mark.entscheidung("E-023")`. E-023 `tests:` um den Nachweis ergänzen.

---

## F3-31 (P2): `float` in `result_ttl`/`unique_for` legt den Worker lahm

### Ursache

- `TaskInfo` (`task.py:92, 102, 145-167`) speichert `result_ttl` und `unique_for` ungeprüft. `_validate_unique` (`:263-267`) prüft nur `== 0` und `< 0`.
- `SwarmQConfig.result_ttl` (`config.py:188`) wird nur auf `< 0` geprüft (`config_validation.py:93-95`). Env `SWARMQ_RESULT_TTL` ist `int`-geparst (`config.py:445`), dort kommt kein `float` an.
- Die Werte gehen als `… * 1000` an valkey-py (`worker/results.py:310`, `worker/retry.py:303`, `worker/gates/cancel.py:160`, `worker/gates/expiry.py:88`, `worker/lifecycle.py:154`, `app.py:231, 238`, `client/schedule.py:252, 630`, `client/schedule_resolve.py:176`, `scheduler/fire.py:174`). valkey-py 6.1.1 wirft bei `float` in `px`/`ex` `DataError` (`valkey/commands/core.py:2305, 2313`).
- Cron-Meldungen: `scheduler/reports.py:85` übernimmt `task_info.unique_for` in die `ScheduleDefinition` (`definition.py:92`, `unique_for: int | None`). msgspec 0.21.1 kodiert `3600.0`, lehnt es beim Dekodieren aber ab (`Expected int | null, got float`, per Probe). Der Leader übernimmt den Cron nie.

Der Nachweis `tests/unit/test_befund_f3_31_float_ttl_normalized.py` prüft `TaskInfo.result_ttl`, `TaskInfo.unique_for`, `SwarmQConfig.result_ttl` mit `86400.0 → 86400` und `1.5 → 2` bzw. `0.5 → 1`, Typ `int`. Das entspricht E-055. Der Aufrufparameter `unique_for` an `schedule()` fehlt dort bewusst (Verweis auf F3-07).

### Welche Felder

Nach E-055 genau: `TaskInfo.result_ttl`, `TaskInfo.unique_for`, `SwarmQConfig.result_ttl` und der Parameter `unique_for` von `Client.schedule()`/`schedule_many()`. `SwarmQConfig` hat kein `unique_for`. Andere ganzzahlige Sekundenfelder (`soft_timeout`, `hard_timeout`, `max_age`, `shutdown_timeout`) deckt die Regel nicht. Per Code-Lesung ist `max_age` als `float` ebenfalls ein Kandidat (`client/results.py:242`: `(max_age + buffer) * 1000` wird zur TTL des Cancel-Flags); nicht bestätigt, nicht in 0.12.3 ohne Entscheidung.

### `ceil(inf)` und die Reihenfolge zu F3-04

`math.ceil(float("inf"))` wirft `OverflowError`, `math.ceil(float("nan"))` wirft `ValueError` (per Probe). Eine Normalisierung ohne Endlichkeitsprüfung würde:
- in `SwarmQConfig` einen `OverflowError` statt `ConfigurationError` werfen → Exit 1 statt 78 (gegen E-057/E-063);
- in `TaskInfo` einen `OverflowError` statt `ValueError` beim Import werfen.

Darum prüft der Helfer selbst die Endlichkeit, bevor er rundet. Dann ist F3-31 unabhängig davon, ob F3-04 schon gemergt ist. `result_ttl` steht nicht in `_POSITIVE_FIELDS` und nicht im F3-04-Nachweis; E-063 gilt aber auch für TTLs, und der Helfer erfüllt das für dieses Feld.

### Lösungsoptionen

**A – Ein gemeinsamer Helfer, angewandt in `TaskInfo`, `SwarmQConfig` und `Client.schedule` (empfohlen).**

```python
def whole_seconds(value: float | None, name: str) -> int | None:
    """E-055: ganzzahliger float -> int, echter Bruch -> aufgerundet; int bleibt."""
    if not isinstance(value, float):
        return value          # None, int und bool unverändert
    if not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number of seconds, got {value}")
    return math.ceil(value)
```

- Ort: neues stdlib-Modul `src/swarmq/seconds.py` (Präzedenz für geteilte Prüfer: `normalize_timezone` in `config.py`, `parse_rate_limit` in `limits.py`). Ein eigenes Modul hält die parallelen Units konfliktfrei (siehe Schnitt).
- `TaskInfo`: im `__post_init__` mit `object.__setattr__` (Muster `task.py:251`). In `_validate_unique` zuerst die Rohwert-Prüfungen `== 0` und `< 0` (E-056; `0.0 == 0` ist wahr, Meldung bleibt „use None to disable dedup“), dann normalisieren. So meldet `-0.5` „must be > 0“ und nicht nach dem Runden „unique_for=0“. `result_ttl` hat in `TaskInfo` keine Bereichsprüfung; nur normalisieren. `-0.5` ergibt dort `0` (ceil); kein neues Verhalten, das heute schon kaputte negative TTLs anders deutet. Randfall, nur notieren.
- `SwarmQConfig`: in `_validate_core` vor `result_ttl < 0` normalisieren, `ValueError` in `ConfigurationError` übersetzen (Muster `config.py:670-674`, `normalize_timezone`).
- `Client.schedule`/`schedule_many`: in `_resolve_unique` (`client/schedule_resolve.py:112-140`) beim ausdrücklichen Wert, nach der `<= 0`-Prüfung aus F3-07.
- `bool`: `isinstance(True, float)` ist falsch, `True` bleibt `True` (wirkt als 1 s, wie heute). E-055 regelt `bool` nicht; nicht anfassen.
- `numpy.float64` ist eine `float`-Unterklasse und wird mit erfasst. `Decimal`/`Fraction` nicht; außerhalb der Regel.
- Typannotation: `TaskInfo.__init__` hat eine eigene Signatur (`task.py:119-141`); die Parameter `result_ttl`/`unique_for` können auf `float | None` erweitert werden, die Felder bleiben `int | None`. Dann meldet mypy `timedelta(...).total_seconds()` nicht mehr. Für `SwarmQConfig` (generiertes `__init__`) bliebe nur ein breiterer Feldtyp; das ändert den Typ für alle Leser. Empfehlung: nur `TaskInfo.__init__` und `Client.schedule` erweitern, `SwarmQConfig` nicht (Laufzeit nimmt `float` trotzdem an).

**B – `isinstance(…, int)` und `ValueError`.** Von E-055 verworfen.

### Folgefehler-Analyse

- **Entscheidungen.** E-056 (`test_task.py::test_taskinfo_unique_for_zero_raises`) bleibt grün, wenn die Rohwert-Prüfung vor dem Runden steht. E-033 (`None` schaltet ab) unberührt: `None` passiert den Helfer. E-011 (Fingerabdruck mit `unique_for` nur bei gesetztem Wert): Ein normalisierter Wert ist `int`; der Fingerabdruck eines vorher nie übernommenen Float-Crons entsteht neu, ein Bestand mit `int` bleibt byte-gleich. E-031 (Fehlerergebnis trotz `result_ttl=0`) unberührt.
- **Env.** `SWARMQ_RESULT_TTL=1.5` bleibt ein `ConfigurationError` (`_parse_int`). E-055 spricht von `SwarmQConfig`-Argumenten; ob auch die Umgebung `float` annehmen soll, ist nicht entschieden. Empfehlung: unverändert lassen.
- **Mischbetrieb.** Kein neues Format. Ein 0.12.3-Worker meldet `unique_for` als `int`; Leader 0.12.0–0.12.2 übernehmen ihn (vorher scheiterte die Dekodierung). Ein alter Worker mit `float` meldet weiter einen Wert, den jeder Leader ablehnt (wie heute). Producer: normalisiertes `ttl_ms` ist `int`, für alte Worker unsichtbar. Rollback: zurück zum Fehlerbild, keine Datenfolgen.

### Doku und Changelog

- `website/reference/configuration.md:479` (`unique_for`), `:492` (`result_ttl`), Env-Tabelle `:35` (`SWARMQ_RESULT_TTL`, nur kwarg-Hinweis), `.de.md` analog: „Whole seconds; a `float` such as `timedelta(...).total_seconds()` is accepted and rounded up to the next second.“
- `website/reference/api.md:123` (`schedule(unique_for=…)`), `api.de.md` analog.
- Changelog EN/DE, Fixed: „`result_ttl` and `unique_for` accept a `float` again without stalling the worker: whole values become `int`, fractions round up (`TaskInfo`, `SwarmQConfig`, `schedule()`/`schedule_many()`).“

### Testszenarien

1. Nachweis (6 Fälle) wie vorhanden.
2. `TaskInfo(unique_for=0.0)` → `ValueError` „use None to disable dedup“; `unique_for=-0.5` → „must be > 0“.
3. `TaskInfo(result_ttl=float("inf"))`, `unique_for=float("nan")` → `ValueError` mit Feldname (kein `OverflowError`).
4. `SwarmQConfig(result_ttl=float("inf"))` → `ConfigurationError` mit `result_ttl`.
5. `TaskInfo(result_ttl=0.0)` → `0` (Typ `int`).
6. Cron-Meldung: `TaskInfo(cron="* * * * *", unique_for=3600.0)` → die gemeldete `ScheduleDefinition` dekodiert mit `unique_for == 3600` (Unit über `scheduler/reports.py`, In-Memory-Backend).
7. `schedule(…, unique_for=0.5)` → Flag-TTL 1000 ms, `int` (gehört in die F3-07-Unit, Backend gemockt).
8. `True` bleibt unverändert (Festhalten des Ist-Zustands, damit eine spätere Änderung bewusst geschieht).

### Betroffene Dateien

`src/swarmq/seconds.py` (neu), `src/swarmq/task.py` (`__init__`-Signatur, `__post_init__`/`_validate_unique`), `src/swarmq/config_validation.py` (`result_ttl`-Block `:93-95`), `src/swarmq/client/schedule_resolve.py` (mit F3-07); Tests `tests/unit/test_befund_f3_31_float_ttl_normalized.py`, `tests/unit/test_seconds.py` (neu), `tests/unit/test_task.py`, `tests/unit/test_config.py`, `tests/unit/scheduler/test_reports.py`, Client-Test bei F3-07; Doku `website/reference/configuration.md`, `.de.md`, `website/reference/api.md`, `.de.md`, Changelog EN/DE.

### Marker nach dem Fix

Nachweis: `xfail` weg, `@pytest.mark.entscheidung("E-055")`. E-055 `tests:` um die drei Testfunktionen ergänzen; `ohne_test:` erst streichen, wenn auch der `schedule()`-Test (F3-07-Unit) steht, oder bis dahin auf „schedule(): Test mit F3-07“ kürzen.

---

## Dateien je Befund

| Datei | F3-04 | F3-08 | F3-14 | F3-16 | F3-31 |
|---|---|---|---|---|---|
| `src/swarmq/config.py` | `_FINITE_FIELDS` | | Sentinel, `_apply_pool_defaults` | | |
| `src/swarmq/config_validation.py` | `_validate_finite`, Stub, Aufrufort | | | | `result_ttl`-Block `:93-95` |
| `src/swarmq/task.py` | | `_validate_scheduling`, Import | | | `__init__`, `_validate_unique`, `__post_init__` |
| `src/swarmq/seconds.py` (neu) | | | | | Helfer |
| `src/swarmq/backoff.py` | | | | `fixed`-Zweig, Docstring | |
| `src/swarmq/cli/_shared.py` | DLQ-TTL, `_positive_float` | | | | |
| `src/swarmq/cli/env_check_cmd.py` | optional | | | | |
| `src/swarmq/client/schedule_resolve.py` | | | | | `_resolve_unique` (mit F3-07) |
| `tests/unit/test_befund_f3_*` | 04 | 08 | 14 | 16 | 31 |
| `tests/unit/test_config.py` | Szenarien | | Szenarien | | Szenarien |
| `tests/unit/test_task.py` | | Szenarien | | | Szenarien |
| `tests/property/test_backoff_properties.py` | | | | anpassen | |
| `tests/unit/test_backoff.py` | | | | Kommentar | |
| `website/reference/configuration(.de).md` | Env-Tabelle | TaskInfo Cron, Validation | Env `:34` | | TaskInfo Dedup/Result, Env |
| `website/guides/cpu-mode(.de).md` | | | Tabelle `:18` | | |
| `website/guides/scheduling-and-retries(.de).md` | | | | `:595-602` | |
| `website/reference/api(.de).md` | | | | | `:123` |
| `website/reference/changelog(.de).md` | ja | ja | ja | ja | ja |
| `compound-packs/swarmq-entscheidungen/` | E-063 | E-065 | E-061 | E-023 | E-055 |

## Wechselwirkungen mit anderen Befunden

**F3-04 ↔ F3-31.** Einzige echte Reihenfolgefalle: `ceil(inf)`/`ceil(nan)`. Gelöst, wenn der Helfer die Endlichkeit selbst prüft. Dann ist die Reihenfolge der beiden Fixes egal. `result_ttl` muss nicht zusätzlich in `_FINITE_FIELDS`; schadet aber nicht, wenn es dort nach der Normalisierung geprüft würde. Nicht vor der Normalisierung, sonst zwei Meldungen für denselben Fehler in zwei Codepfaden.

**F3-04 ↔ F3-16.** Mit endlichem `retry_max_delay` (F3-04) und dem `fixed`-Deckel (F3-16) ist jede berechnete Retry-Wartezeit endlich, auch bei `retry_base_delay=inf`. Unabhängig umsetzbar.

**F3-16 ↔ F3-03 (anderes Paket).** E-060 klemmt `Retry(delay<0)` auf 0, deckelt ihn aber nicht. Kein Konflikt; `worker/retry.py` wird von F3-16 nicht geändert (nur `backoff.py`).

**F3-31 ↔ F3-07 (Client-Paket).** Beide ändern `client/schedule_resolve.py::_resolve_unique` an derselben Stelle (Bereichsprüfung, dann Normalisierung). Empfehlung: Der `schedule()`-Teil von F3-31 wird in der F3-07-Unit umgesetzt und nutzt `swarmq.seconds.whole_seconds`.

**F3-08 ↔ F3-35.** F3-35 bleibt offen. Der F3-08-Fix lehnt nebenbei 7-feldrige Ausdrücke mit vergangenem Jahr ab (kein nächster Termin), legt aber keine Feldzahl fest.

**F3-14 ↔ Supervisor/CLI.** Keine; die CLI übergibt immer einen aufgelösten Wert.

**Gemeinsame Dateien:** `config.py` (F3-04, F3-14), `config_validation.py` (F3-04, F3-31), `task.py` (F3-08, F3-31), `configuration(.de).md` und Changelog (alle).

### Vorgeschlagener Schnitt für parallele Umsetzung

| Unit | Inhalt | Dateien (Code) | Abhängigkeit |
|---|---|---|---|
| K0 | Helfer `whole_seconds` + `tests/unit/test_seconds.py` | `src/swarmq/seconds.py` | keine; klein, zuerst mergen |
| K1 Konfiguration | F3-04, dann F3-14, dann F3-31-Config-Teil | `config.py`, `config_validation.py`, `cli/_shared.py`, (`env_check_cmd.py`) | F3-31-Teil nach K0 |
| K2 TaskInfo | F3-31-TaskInfo-Teil, dann F3-08 | `task.py` | nach K0 |
| K3 Backoff | F3-16 | `backoff.py`, `tests/property/test_backoff_properties.py` | keine |
| (F3-07-Unit, Client-Paket) | F3-31-`schedule()`-Teil | `client/schedule_resolve.py` | nach K0 |

K1, K2 und K3 berühren keine gemeinsame Code- oder Testdatei. Gemeinsam bleiben nur Doku und Changelog. Vorschlag: Jede Unit ändert in `configuration(.de).md` nur ihren Abschnitt (K1: Env-Tabelle; K2: TaskInfo-Tabellen und „Validation behaviour“); den Changelog-Abschnitt 0.12.3 legt der Orchestrator einmal an und sammelt die Einträge, sonst entstehen triviale Konflikte im selben Absatz. Hinweis: Der Changelog beginnt heute mit „[0.12.2] — unreleased“; der Abschnitt für 0.12.3 fehlt noch.

## Berührungen mit Register-Entscheidungen

- Keine Regel macht einen dieser Fixes unmöglich.
- F3-16 ändert `tests/property/test_backoff_properties.py::test_fixed_ignores_attempt_and_cap`, der das alte Verhalten ohne Marker festschreibt. Das folgt aus dem E-023-Nachtrag und ist zulässig; im Commit so begründen.
- E-055 lässt offen, ob `SWARMQ_RESULT_TTL` (Env) einen `float` annehmen soll und wie `bool` zu behandeln ist. Empfehlung: beides unverändert; falls gewünscht, als Nachtrag in E-055.
- E-063 deckt `TaskInfo`-Fristen nicht. `max_age=inf` (`OverflowError` im Ausführungsschutz) und `max_age` als `float` (Cancel-Flag-TTL) sind Kandidaten für eine spätere Runde, nur per Code-Lesung gefunden.
