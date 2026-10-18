# Fehlersuche Runde 4 – Spur B: Betriebsabläufe gegen den Teststack (U15)

Stand 2026-10-17, Worktree `test/fehlersuche-runde-4`, Arbeitsstand = 0.12.3. Alle Proben liefen unter der
Testsperre (`scripts/with-test-lock.sh`) gegen `swarmq-test-rabbit` (5699/15699) und `swarmq-test-valkey`
(6399), je Probe eigene vhost `swarmq-u15-<name>` und Valkey-DB 13; vhosts und DB 13 sind danach entfernt bzw.
leer. Nie gegen 5672/6379. Die Probe-Skripte liegen im Scratchpad der Sitzung
(`…/scratchpad/u15/`: `u15common.py`, `mod/u15_tasks.py`, `p1_kontrollkanal.py`, `p2_hotreload.py`,
`p3_cli.py`, `p3b_table.py`, `p4_logrotate.py`, `p5_ersatz.py`, `p6_leader_version.py`, `p6c_workers.py`;
Rohdaten und Worker-Logs unter `u15/out/`). Last: zu Beginn hoch (Load 6–10 durch parallele Agenten),
später 1–4; Werte je Lauf unten.

Fristen aus dem Code, nicht aus der Doku: `shutdown_timeout` 30 (`config.py:234`) → `shutdown_budget(30,
supervised=True)`: `sigterm_at` 23 s, `sigkill_at` 28 s, `drain_until` 23 s (`shutdown_budget.py`);
`SHUTDOWN_TERM_S` 5 (`supervision.py:82`); `heartbeat_interval_s` 5 / `heartbeat_timeout_s` 60
(`config.py:366-367`); `scheduler_heartbeat_interval` 10 / `scheduler_lock_ttl` 30 (`config.py:286-287`);
Sollwert-Abtastung 5 s (`setpoint_store.py:DEFAULT_SAMPLE_INTERVAL_S`); Monitor-Takt 0,5 s
(`supervisor.py:79`); Hot-Reload-Entprellung 1600 ms (`hot_reload.py:191`); `log_drain_timeout` 5
(`config.py:221`).

## Übersicht

| Ablauf | Ergebnis | Verdacht |
|---|---|---|
| 1 Kontrollkanal unter Last | bestätigt; F3-41 nicht reproduziert | – |
| 2 Hot-Reload unter Last | bestätigt | R4B-01 (gering) |
| 3 CLI gegen echte Daten | bestätigt | – |
| 4 Logrotate `copytruncate` | swarmq bestätigt; Doku lückenhaft | R4B-02 |
| 5 Supervisor-Ersatz | bestätigt | – |
| 6 Mischbetrieb (`tests/compat`, E-029) | Suite grün; ein Befund in `schedule status` | R4B-03 |

## 1. Kontrollkanal unter Last

**Erwartung.** Ein Sollwert wirkt innerhalb eines Abtastintervalls; eine höhere Untergrenze startet fehlende
Worker sofort, eine niedrigere Obergrenze beendet überzählige sanft (laufender Task endet); Löschen stellt die
Startargumente her; Ablehnung bei Untergrenze < 1, Obergrenze > Spielraum, Bereich ohne Autoskalierung; die
Rückmeldung verfällt nach Gnadenfrist + Terminierungsfrist + Abtastintervall (`website/reference/cli.md`
„Control channel“, `process_manager/setpoint.py`, `tests/integration/test_cli_worker_setpoint.py`). Scale-down
gegen E-018 und F3-41 (offene LÜCKE, Runde 3).

**Aufbau.** `swarmq worker --processes 1 --max-processes 4 --group G --concurrency 4`, `SWARMQ_WORKER_ID=u15ctl`;
Producer 20 Tasks/s (0,5 s je Task) über 75 s; Folge 3..3 → 1..1 → 0..0 → 4..4 → 5..5 → 1..3 → löschen; danach
SIGTERM und Verfall der Rückmeldung. Teil B (F3-41): `--processes 2 --max-processes 3`, wiederholt 2..2 → 1..1
mit `SWARMQ_HEARTBEAT_INTERVAL_S=10`, `SWARMQ_SHUTDOWN_TIMEOUT=1` (leer und mit 5-s-Tasks) sowie
`HEARTBEAT_INTERVAL_S=15`, `SHUTDOWN_TIMEOUT=5` (mit Last), Zählung `worker_hung_during_drain`.

**Beobachtung (3 Läufe, Load zu Beginn 5,7 / 5,1 / 2,0).**

| Schritt | Lauf 1 | Lauf 2 | Lauf 3 | Status danach |
|---|---|---|---|---|
| 3..3 bis Status `occupied=3` | 2,04 s | 2,04 s | 2,04 s | `source=setpoint` |
| 1..1 bis Status `occupied=1` | 6,10 s | 5,90 s | 5,91 s | zwei `setpoint_scale_down`, Kinder enden 0,5–1,0 s danach mit Exit 0 |
| 0..0 | 2,85 s | 3,05 s | 3,06 s | `rejection: lower bound 0 is below 1`, Grenzen 1..1 bleiben |
| 4..4 | 3,05 s | 3,05 s | 2,86 s | `occupied=4` |
| 5..5 | 3,06 s | 2,85 s | 3,06 s | `rejection: … exceeds the headroom 4` |
| 1..3 | 4,88 s | 5,08 s | 5,09 s | `rejection: range 1..3 needs autoscaling …` |
| löschen | 10,16 s | – | – | `setpoint_cleared`, drei Kinder sanft beendet |

Zeiten bis zur *Rückmeldung*: Diese wird nur je Abtastung (5 s) geschrieben, die tatsächliche Wirkung ist
schneller (Log: `setpoint_scale_down` 09:13:33.127, `worker_died … expected: true` 0,5 s bzw. 1,0 s später).
Je Lauf 1500 Tasks, alle genau einmal ausgeführt und fertig, DLQ leer, keine Traceback-/Fehlerzeile.
SIGTERM: Supervisor endet nach 0,62 / 0,47 / 0,21 s mit 0; die Rückmeldung lebt danach noch 39,2 / 39,8 /
39,7 s (TTL 40 s = 30 + 5 + 5). Teil B: 32 Scale-down-Zyklen, 0 × `worker_hung_during_drain`; das beendete
Kind ist in allen Fällen 0,5 s (einmal 2,0 s) nach `setpoint_scale_down` fort, also vor dem nächsten
Lebenszeichen-Vergleich mit überschrittener Frist. Unter Last zeigen 18 von 82 bzw. 1 von 99 Tasks zwei
Ausführungen und `retry_publish_failed` (ERROR, „shutdown publish deadline has passed“): Drain-Abbruch bei
`shutdown_timeout` 1 bzw. 5 s, das Original wird neu zugestellt (E-001, Nachtrag FS-02a).

**Einstufung.** Bestätigt. Setpoint „0“ ist keine Skalierung auf null, sondern eine dokumentierte Ablehnung
(Plan nennt „Setpoint 0“ als Schritt). Die 40 s Nachlauf der Rückmeldung nach sauberem Ende entsprechen der Doku
und dem offenen Punkt in E-018 (Status-TTL rechnet `shutdown_grace_s + SHUTDOWN_TERM_S`); kein neues Argument.
F3-41 ließ sich mit realen Prozessen nicht auslösen: Der Scale-down-Drain endet bei kleinem `shutdown_timeout`
binnen eines Monitor-Takts; die Lücke braucht einen Drain, der länger lebt als
`shutdown_grace_s + 5 − (Alter des letzten Lebenszeichens)`. Bleibt offene LÜCKE ohne neuen Beleg.

**Reproduzierbarkeit.** Drei Läufe identisch (Streuung ≤ 0,2 s bis auf die 5-s-Rasterung der Rückmeldung).

## 2. Hot-Reload unter Last

**Erwartung.** Dateiänderung → SIGHUP → Worker-Stopp im Gesamtbudget (Drain höchstens bis `drain_until`,
E-018) → `execvpe` mit gleichem PID, argv- und env-Snapshot (`process_manager/reload.py`); keine
Doppelausführung, keine verlorenen Acks; Teardown ohne PENDING-Tasks, Broker/Backend nicht vom Worker
geschlossen (`docs/solutions/patterns/worker-cancel-safe-teardown-detached-tasks-2026-06-01.md`); Änderungen
während eines laufenden Reloads gehen nicht verloren.

**Aufbau.** `swarmq worker --reload --processes 2 --concurrency 4`, Watch-Dir = Modulverzeichnis im
Scratchpad; Producer 8 Tasks/s (2 s je Task) über 110 s. Reload A: eine Änderung (v1). B: zwei Änderungen
0,8 s auseinander (v2, v3). C: zwei Änderungen 3 s auseinander (v4, v5). Lauf 1 zusätzlich ein 40-s-Task vor
Reload A; Läufe 2 und 3 ohne.

**Beobachtung.**

| | Lauf 1 (40-s-Task; Load 3,2) | Lauf 2 (Load 3,7) | Lauf 3 (Load 2,6) |
|---|---|---|---|
| `reload_triggered` nach Änderung | 0,06–0,08 s | 0,06–0,10 s | 0,06–0,08 s |
| Drain (reload_requested → execvp) A/B/C | 23,34 / 23,34 / 23,34 s | 2,43 / 2,13 / 2,14 s | 2,44 / 2,11 / 2,13 s |
| execvp → neue Worker gestartet | 0,5 s | 0,5 s | 0,4–0,5 s |
| Supervisor-PID über alle Reloads | gleich | gleich | gleich |
| `shutdown_sigterm_escalation` | 1 je Reload | 0 | 0 |
| Reloads gesamt | 3 (v5 fiel in den laufenden Reload) | 4 (v5 nach dem Neustart erkannt) | 4 |
| Version der letzten 40 Läufe | v5 | v5 | v5 |
| Tasks fertig / Mehrfachausführung | 608 von 881 bei Abbruch des Wartens (264 noch in der Queue), sonst je 1 Lauf; 40-s-Task 4 Läufe, 1 fertig | 880/880, keine | 880/880, keine |

Keine Zeile mit `Traceback`, `Task was destroyed but it is pending`, `Future exception was never retrieved`,
`Unclosed`, `ResourceWarning`; kein `reload_aborted_alive_children`. Lauf 1: Der 40-s-Task wird bei jedem
Reload bei 23 s abgebrochen (`retry_publish_failed`, „shutdown publish deadline has passed“) und neu
zugestellt, wie E-001 (Nachtrag FS-02a) festlegt; dabei schickt der Supervisor bei 23,0 s SIGTERM an das Kind
(`shutdown_sigterm_escalation`, WARNING), dessen eigener Drain-Anker bis zu 0,3 s später liegt
(`process_manager/entry.py:_watch_shutdown`, Poll 0,3 s).

**Einstufung.** Bestätigt: Drain endet mit den in-flight Tasks (gut 2 s bei 2-s-Tasks) bzw. am Budget-Ende,
`execvpe` behält den PID, keine Doppelausführung außer dem entschiedenen Drain-Abbruch, keine verlorene
Änderung (B: zusammengefasst; C: entweder im neuen Abbild importiert oder vom neuen Watcher erkannt).
**Verdacht R4B-01 (gering):** `sigterm_at` und `drain_until` fallen unter dem Supervisor zusammen
(`shutdown_budget.py`: `sigterm_at = sigkill_at − 5`, `drain_until = finish_by − 5`, `finish_by = sigkill_at`),
und der Anker des Kindes liegt bis 0,3 s später. Jeder Shutdown oder Reload mit einem Task, der länger als der
Drain läuft, erzeugt deshalb `shutdown_sigterm_escalation` (WARNING), obwohl nichts hängt. Folge nur eine
irreführende Warnung; das zweite SIGTERM setzt keinen neuen Anker (E-018, Nachtrag FS-30). Quelle der
Erwartung fehlt (E-018 legt die Zeitpunkte fest, nicht die Stufe der Meldung) → vermutlich LÜCKE.

**Reproduzierbarkeit.** Drain-Zeiten über drei Läufe ± 0,3 s; R4B-01 in 3 von 3 Reloads mit Langläufer.
Nachweis-Vorschlag (U18): `tests/e2e/test_befund_r4b01_sigterm_escalation_on_drain_end.py` – Worker mit
`SWARMQ_SHUTDOWN_TIMEOUT=6`, ein Task mit 30 s, SIGTERM; Assertion: keine `shutdown_sigterm_escalation`, wenn
das Kind vor `sigkill_at` endet (nur sinnvoll, falls Georg die Erwartung festlegt).

## 3. CLI gegen echte Daten

**Erwartung.** Exitcodes laut `website/reference/cli.md` („Exit codes“, `dlq`, `schedule`, `inspect`) und E-006
(78 nur für Konfigurationsfehler); `dlq list` nicht zerstörend, parallele Listings sehen Teilmengen (E-028);
`dlq retry` ist eine neue Ausführung, wiederholtes Retry desselben Eintrags findet nichts (E-009); `inspect`
liest nur Metadaten, ohne Verbindung.

**Aufbau.** Worker (`--concurrency 16`) füllt 1000 DLQ-Einträge (`u15_maybe_fail`, `max_retries=0`, Schalter in
Valkey), dann Schalter aus und Producer 20 Tasks/s als Last, während die CLI-Befehle laufen (Lauf 1, Load 2,4);
Tabellenausgabe gesondert (`p3b_table.py`).

**Beobachtung.**

| Befehl | Exit | Ergebnis |
|---|---|---|
| `dlq list --limit 5000` zweimal parallel | 0 / 0 | 501 und 499 Einträge, disjunkt, Vereinigung 1000; DLQ danach 1000 |
| `dlq list` allein | 0 | 1000 Einträge, alle verschieden, `truncated: false`, 4,1 s |
| `dlq list --limit 100` | 0 | 100, `truncated: true`; Tabelle (`--format table`) mit Zeile „(truncated at --limit 100 …)“ |
| `dlq inspect <id>` / unbekannte ID | 0 / 3 | |
| `dlq retry <id>` während der Worker konsumiert | 0 | Task läuft erneut (Exec 2, fertig 1), DLQ 999 |
| dasselbe Retry noch einmal | 3 | |
| `dlq retry <id> --all` / ohne Argument | 2 / 2 | Meldung auf stderr |
| `dlq retry --all --limit 10` | 0 | 10 verschoben, DLQ 989, alle 10 laufen einmal |
| `inspect --module u15_tasks` / unbekanntes Modul | 0 / 1 | 3 Tasks |
| `schedule add` Cron / gleicher Name nochmal | 0 / 0 | zweites `inserted: false, already_exists: true` |
| `schedule add --at` (6 Oneshots, 2–6 s) | 0 | jeder genau einmal gefeuert |
| `schedule add` Cron `61 * * * *` / `--kwargs '[1]'` | 1 / 1 | |
| `schedule status`, `list`, `prune --orphans`, `remove`, `remove --permanent` | 0 | Leader 0.12.3 |
| `schedule remove` eines unbekannten Namens | 0 | `removed: false` (No-op) |
| `dlq list` / `schedule status` mit `SWARMQ_NAMESPACE='bad namespace!'` | 78 / 78 | |
| `dlq purge` | 0 | 989 entfernt |

Last: 800/800 Tasks fertig, keine Doppelausführung; keine Fehlerzeile im Worker außer den erwarteten
„u15 poison“-Fehlschlägen beim Füllen.

**Einstufung.** Bestätigt. Hinweis ohne Verdacht: Ohne Terminal wählt die CLI JSON (dokumentiert unter
`SWARMQ_DEFAULT_FORMAT`, `website/reference/configuration.md`). `schedule remove` eines fehlenden Namens endet
mit 0, `dlq retry/inspect` eines fehlenden Eintrags mit 3; die Doku sagt zu `schedule remove` nichts über den
Exitcode, also keine verletzte Zusage.

**Reproduzierbarkeit.** Ein Lauf (nicht zeitkritisch); Tabellenprobe dreimal identisch.

## 4. Logrotate mit `copytruncate` unter Last

**Erwartung.** `website/reference/logging.md` empfiehlt „Rotate outside the process: journald, logrotate with
`copytruncate`“ und nur Stream-Handler; Flush jeder Queue in `log_drain_timeout` (5 s) beim Ende; keine
Exception durch Rotation; Drain bei SIGTERM vollständig.

**Aufbau.** `swarmq worker --processes 2 --concurrency 8 --log-format json`, stdout+stderr in eine Datei, einmal
im Anhängemodus (`O_APPEND`, wie Shell `>>`), einmal im Schreibmodus (wie `>`). Producer 50 Tasks/s über 30 s
(1500 Tasks, je eine Logzeile `u15_line` mit Sequenznummer), alle 4 s `logrotate -f` mit `copytruncate` (7
Rotationen, je 4–87 ms), SIGTERM direkt nach dem letzten Publish. Auswertung: Sequenznummern aller Dateien
gegen die in Valkey gezählten Läufe.

**Beobachtung.**

| Lauf (Modus, Load) | Läufe | Zeilen gefunden | fehlend | NUL-Bytes | SIGTERM bis Exit |
|---|---|---|---|---|---|
| 1 append (1,9) | 1500 | 1499 | 1 (seq 194, ≈ 3,9 s nach Start = erste Rotation) | 0 | 0,47 s |
| 2 append (1,6) | 1500 | 1500 | 0 | 0 | 0,52 s |
| 3 append (1,6) | 1500 | 1500 | 0 | 0 | 0,46 s |
| 1 write (2,1) | 1500 | 1500 | 0 | 3 109 481 | 0,51 s |

Keine Exception, keine ungültige JSON-Zeile, kein `log_drain_timeout_exceeded`, Exit 0. Alle Zeilen der vor dem
SIGTERM begonnenen Tasks sind geschrieben.

**Einstufung.** swarmq bestätigt (keine Exception, Drain vollständig, keine Lücke über die Drain-Frist hinaus).
**Verdacht R4B-02 (Doku):** Die Empfehlung nennt zwei Folgen von `copytruncate` nicht: (a) Zeilen zwischen
Kopie und Kürzen gehen verloren (logrotate(8): „some logging data might be lost“; hier 1 Zeile in 21
Rotationen), (b) ohne `O_APPEND` (Shell `>` statt `>>`, systemd `StandardOutput=file:` statt `append:`)
schreibt der Prozess nach dem Kürzen an seinem alten Offset weiter, die Datei wird zur Lücke mit NUL-Bytes
(hier 3,1 MB). Quelle der Erwartung: die Empfehlung selbst in `logging.md`; keine Entscheidung im Register →
LÜCKE (Doku).

**Reproduzierbarkeit.** (b) deterministisch; (a) zufällig, 1 von 3 Läufen. Nachweis-Vorschlag: kein e2e-Test
(Verhalten von logrotate und Shell), sondern Doku-Ergänzung; falls doch: `tests/e2e/test_logrotate_copytruncate.py`
mit Worker-stdout per `open(..., "ab")` und einer `copytruncate`-Nachbildung (`shutil.copy` + `truncate`),
Assertion keine NUL-Bytes und alle Sequenzzeilen.

## 5. Supervisor-Ersatz unter Last (Rolling Deploy gleicher Stand)

**Erwartung.** Zustellung läuft weiter, kein Verlust, keine Doppelausführung (`tests/e2e/test_worker_shutdown_load.py`
für SIGTERM); der Leader gibt beim sauberen Stopp den Lock samt Handover-Marker frei, ein Standby übernimmt
beim nächsten Wahl-Takt (`scheduler/loop.py`, Takt `scheduler_heartbeat_interval` 10 s; spätestens nach
`scheduler_lock_ttl` 30 s). Der Plan nennt `heartbeat_timeout_s`; das ist die Lebenszeichen-Frist zwischen
Supervisor und Kind (60 s), nicht die Leader-Frist.

**Aufbau.** Supervisor A (`--concurrency 8`) wird Leader; Producer 20 Tasks/s über 60 s, jeder fünfte Task mit
`delay=3` (feuert nur über den Leader); nach 10 s startet B, 5 s nach dessen `worker_started` SIGTERM an A;
gemessen werden Ende von A, Zeit bis der Lock einem Kind von B gehört, größte Lücke zwischen Task-Starts.

**Beobachtung (4 Läufe, Load 4,3 / 4,5 / 2,2 / 1,2).**

| | L1 | L2 | L3 | L4 |
|---|---|---|---|---|
| A endet nach SIGTERM | – | 0,69 s | 0,69 s | 0,64 s |
| Leader bei B nach SIGTERM | 5,02 s | 5,01 s | 4,98 s | 5,01 s |
| Tasks fertig / Mehrfach | 1200/0 | 1200/0 | 1200/0 | 1200/0 |
| verzögerte Tasks fertig | 240/240 | 240/240 | 240/240 | 240/240 |
| größte Lücke zwischen Starts vor Producer-Ende | 0,13 s | < 0,13 s | < 0,13 s | < 0,13 s |

DLQ leer, keine Fehlerzeile. Die gleichbleibenden 5,0 s sind ein Artefakt der Probe: B wählt alle 10 s ab
seinem Start, SIGTERM kam 5 s nach dem Start; erwartbar sind 0–10 s.

**Einstufung.** Bestätigt.

**Reproduzierbarkeit.** Vier Läufe, Streuung 0,04 s (Leader) bzw. 0,05 s (Ende von A).

## 6. Mischbetrieb

**Erwartung.** `tests/compat` grün gegen 0.11.0, 0.11.1 und 0.12.0 von PyPI (E-008, E-009, E-011, E-013); E-029:
ein Leader < 0.12 wird einmal je Leaderwechsel als `scheduler_leader_outdated` gewarnt.
`schedule status` listet unter WORKERS „every worker whose report is live“ samt Version
(`website/reference/cli.md`, „schedule status“), damit der Betreiber Apps unter der Mindestversion findet.

**Aufbau.** (a) Suite nach `docs/testing/test-strategy.md`: `scripts/with-test-lock.sh uv run --extra test
--extra reload pytest tests/compat -m compat`, zweimal seriell, keine neue Constraints-Datei. (b) Ergänzung für
E-029, das die Suite nicht prüft: venv mit `swarmq==0.11.1` (`tests/compat/constraints-0.11.1.txt`), 0.11.1-Worker
führt, Arbeitsstand kommt dazu; `schedule status`; SIGTERM an 0.11.1. (c) zwei Supervisoren des Arbeitsstands
auf einem Host, je `--processes 2`, einmal ohne, einmal mit `SWARMQ_WORKER_ID`.

**Beobachtung.** (a) 23 passed in 89,0 s (Load 6,3) und 23 passed in 85,5 s (Seed 4242466777, Load 0,6).
(b) Genau eine WARNING `scheduler_leader_outdated` (`leader_version: 0.11.1`), auch nach der Übernahme keine
weitere; Übernahme durch den Arbeitsstand 0,74 s nach SIGTERM (schnelle Leader-Fristen der Suite);
`schedule status` zeigt `leader_version: 0.11.1`, unter `workers` aber nur `Laptop:0 0.12.3` – der
0.11.1-Worker fehlt. (c) Ohne `SWARMQ_WORKER_ID`: vier Worker-Prozesse, `workers` zeigt zwei Einträge
(`Laptop:0`, `Laptop:1`); mit IDs alle vier. Ursache: `report_worker_version` schreibt
`sq:{sched}:worker_version:<worker_id>` (`backend/valkey/schedules.py:412`, gleicher Schlüssel in 0.11.1), und
die Voreinstellung `<hostname>:<slot>` (`website/reference/configuration.md`, „worker identity“) ist für
mehrere Supervisoren auf einem Host gleich; der zuletzt Schreibende überdeckt die anderen.

**Einstufung.** (a), (b) bestätigt. **Verdacht R4B-03:** `schedule status` verschweigt Worker, wenn mehrere
Supervisoren auf einem Host ohne eigenes `SWARMQ_WORKER_ID` laufen (zwei Apps auf einem Host, Rolling Deploy
mit altem und neuem Prozess nebeneinander, mehrere systemd-Units). Gerade im Mischbetrieb kann so die
0.11-App unsichtbar sein, die E-029 und die Doku („Every version shown here should be at least the minimum
leader version“) aufspüren sollen. Quelle der Erwartung: `website/reference/cli.md` („`WORKERS` lists every
worker whose report is live“). Kein Eintrag im Register. Konfidenz hoch. Die Doku zur Worker-Identität
verlangt keine Eindeutigkeit je Host; in Containern (eigener Hostname) tritt es nicht auf.

**Reproduzierbarkeit.** Deterministisch (b und c). Nachweis-Vorschlag (U18):
`tests/e2e/test_befund_r4b03_schedule_status_workers_same_host.py` – zwei `spawn_worker` auf derselben Queue
ohne `SWARMQ_WORKER_ID`, `SWARMQ_SCHEDULER_SEEN_INTERVAL_S=2`, warten auf zwei `worker_started`, dann
`swarmq schedule status --format json`; Assertion `len(workers) == 2` (rot: 1).

## Abweichungen vom Plan

- „Setpoint 0“ ist laut Code (`setpoint.validate`, Untergrenze ≥ 1) und Doku keine Skalierung auf null,
  sondern eine Ablehnung; so geprüft.
- `swarmq inspect` verbindet sich nicht mit Broker oder Backend (`cli.md`); „inspect unter Last“ prüft nur,
  dass der Befehl neben laufender Last arbeitet.
- Leader-Wechsel „in `heartbeat_timeout_s`“: maßgeblich sind `scheduler_heartbeat_interval` (10 s) und
  `scheduler_lock_ttl` (30 s); `heartbeat_timeout_s` (60 s) betrifft Supervisor und Kind.
- Hot-Reload-Drain „bis `finish_by − 5 s`“: unter dem Supervisor ist das `drain_until` = 23 s ab Reload-Beginn,
  gemessen 23,34 s.
- Die Suite `tests/compat` prüft E-029 nicht; ergänzt durch Probe 6b mit eigenem 0.11.1-venv im Scratchpad.
- F3-41 war mit realen Prozessen nicht auslösbar (siehe Abschnitt 1).

## Offene Punkte

- R4B-01 und R4B-02 sind vermutlich LÜCKEN (Produktfrage bzw. Doku); R4B-03 ist ein FEHLER-Kandidat für die
  Gegenprüfung (U17).
- Abschnitt 3 und 6c wurden einmal gefahren (nicht zeitkritisch); Abschnitt 2 Lauf 1 hat das Warten auf den
  Rest der Queue nach 120 s abgebrochen (264 Tasks noch in der Queue, nicht verloren, aber nicht bis zum Ende
  gezählt).
- Die Rückmeldung des Kontrollkanals bleibt nach sauberem Ende 40 s stehen (E-018, offener Punkt), ohne neues
  Argument.
