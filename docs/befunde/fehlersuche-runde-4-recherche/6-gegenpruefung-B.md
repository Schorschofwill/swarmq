# Fehlersuche Runde 4 – Gegenprüfung Spur B (Betriebsabläufe, U17)

Stand 2026-10-17, Worktree `test/fehlersuche-runde-4` (= 0.12.3). Prüfer hat die Verdachte nicht gefunden;
Auftrag nach `docs/befunde/README.md`: jeden Verdacht widerlegen (Entscheidungs-Abgleich, Quelle der
Erwartung, Realismus), dann genau eine Einstufung. Grundlage: `4-betrieb.md` (U15), Probe-Skripte und
Rohdaten im Scratchpad der Sitzung (`…/scratchpad/u15/`, Logs in `out/`). Eigene Reproduktion von R4B-03
unter der Testsperre gegen 5699/6399 (Probe `p6c_workers.py` des Finders unverändert, vhost
`swarmq-u15-wk`, Valkey-DB 13), Ausgabe `…/scratchpad/u17/p6c-repro.out`.

## Ergebnis

| Verdacht | Einstufung | Prio | Kern |
|---|---|---|---|
| R4B-01 | ENTSCHEIDUNG | – (Überdenken: P4) | SIGTERM des Supervisors bei `T − 7 s` = Drain-Ende ist in E-018 und in der Doku-Tabelle festgelegt; die WARNING-Stufe ist die einzige offene Kleinigkeit |
| R4B-02 | LÜCKE (Doku) | P3 | Empfehlung `copytruncate` ohne die zwei bekannten Folgen; swarmq schreibt keine Datei, nichts zu fixen im Code |
| R4B-03 | FEHLER | P3 | `schedule status` zeigt nicht „every worker whose report is live“, wenn mehrere Supervisoren eines Hosts ohne eigenes `SWARMQ_WORKER_ID` laufen; der Kontrollkanal löst dieselbe Frage mit `:p<pid>` |

Kein Verdacht berührt F3-41. R4B-01 berührt E-018 (Zeitpunkte `sigterm_at`/`drain_until`), aber nicht dessen
offenen Restpunkt (`restart_budget._heartbeat_deadline` und Status-TTL in `setpoint.py:210` rechnen mit
`shutdown_grace_s + SHUTDOWN_TERM_S`).

---

## R4B-01 – `shutdown_sigterm_escalation` bei jedem Shutdown/Reload mit Langläufer

**Einstufung: ENTSCHEIDUNG** (E-018). Kein Befund; höchstens „Entscheidung überdenken?“ mit dem Argument
„WARNING für einen geplanten Schritt“ (P4).

**Sachverhalt bestätigt.** `shutdown_budget(30, supervised=True)`: `sigkill_at = 28`, `sigterm_at = 28 − 5 = 23`,
`finish_by = sigkill_at = 28`, `drain_until = 28 − 5 = 23` (`src/swarmq/shutdown_budget.py:50-54`).
`ProcessManager._shutdown_all_workers` wartet bis `started + sigterm_at` und loggt danach für jeden noch
lebenden Worker `shutdown_sigterm_escalation` auf WARNING (`process_manager/supervisor.py:401-411, 415-434`:
`log = logger.warning if action == "terminate" else logger.error`). Das Kind setzt seinen Anker erst nach dem
Poll von `_watch_shutdown` (bis 0,3 s, `process_manager/entry.py:192-209`) und drainiert bis `started_at +
drain_until` (`worker/shutdown.py:338-351, 366-388`). Ein Kind mit einem Task, der länger als der Drain läuft,
lebt bei `sigterm_at` also immer noch (es drainiert noch oder räumt auf). Rohdaten bestätigen das: Lauf 1
(`out/p2-rep1.log`) zeigt je Reload `shutdown_sigterm_escalation` 23,03 s nach `reload_requested`, 0,10–0,14 s
später `retry_publish_failed` des Kindes, 0,16–0,30 s danach `reload_execvp`.

**Entscheidungs-Abgleich.**
- E-018 (`compound-packs/swarmq-entscheidungen/e018-shutdown-timeout-gesamtbudget.md`): „Die Eskalation des
  Supervisors endet bei `total − 2 s` (SIGKILL), der Drain läuft höchstens bis `finish_by − 5 s`“; Nachtrag FS-30:
  „Ein zweites SIGTERM (Eskalation des Supervisors) setzt keinen neuen Anker, sonst läge das Drain-Ende hinter
  dessen SIGKILL.“ Das SIGTERM in den laufenden Shutdown ist damit eingeplant.
- Nutzer-Doku, `website/explanation/delivery-semantics.md:292-297` (Tabelle „Shutdown budget“): „Drain of
  in-flight tasks, publish deadline … until `T - 7 s`“ und „Supervisor: `SIGTERM` to workers still alive … at
  `T - 7 s`“. Beide Zeitpunkte sind ausdrücklich gleich; die Aufräumphase liegt danach („until `T - 2 s`“).
  Ebenso `:323-324`: „A second `SIGTERM` (the supervisor's escalation) does not restart that clock.“ und der
  Changelog (`website/reference/changelog.md:1280-1281`: „… `T - 7 s`, `SIGKILL` at `T - 2 s` … its worker
  drains until `T - 7 s`“).
- Plan `docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:597-604` (MR 17) legt die Eskalation fest;
  Tests mit E-018-Marker u. a. `tests/unit/test_shutdown_budget.py::test_supervised_default_budget`,
  `tests/unit/worker/test_shutdown_budget_steps.py::test_supervised_drain_ends_before_the_supervisor_kills`.

**Quelle der Erwartung.** Keine. Weder Doku noch Register sagen, dass `shutdown_sigterm_escalation` nur bei
hängenden Workern erscheint; das Ereignis steht in keinem Ereigniskatalog (`grep` über `website/` ohne Treffer),
die Stufe WARNING stammt unverändert aus 040f35b (R4.6, vor dem Gesamtbudget). Der Finder nennt selbst „Quelle
der Erwartung fehlt“.

**Realismus.** Hoch: Jeder Deploy/Reload mit einem Task, der länger als `shutdown_timeout − 7 s` läuft. Folge
ist nur Lognoise; das zweite SIGTERM ist im Kind wirkungslos (`worker/shutdown.py:510-519`, idempotent außer im
Recycling-Drain, dort nach FS-30 ohne neuen Anker). Zusätzlich erscheint im selben Fall `retry_publish_failed`
auf ERROR; das ist E-001 (Nachtrag FS-02a) und ebenfalls entschieden.

**Widerlegungsversuche.**
1. Liegt ein Defekt vor, weil der Supervisor vor dem Kind eskaliert? Nein, die Gleichzeitigkeit ist die
   dokumentierte Tabelle; der Versatz von bis zu 0,3 s (Poll) bleibt innerhalb der Aufräumreserve (Kind beendet
   in den Rohdaten 0,3 s nach dem SIGTERM, SIGKILL wäre bei +5 s).
2. Ist die Warnung ein Hinweis auf echtes Hängen? Nicht unterscheidbar: Das Ereignis trägt nur `count`; ein
   hängender Worker erzeugt dieselbe Zeile, ein wirklich hängender zusätzlich `shutdown_sigkill_escalation`
   (ERROR). Das ist das eigentliche Signal.

**Vorschlag Register-Zeile (Abschnitt ENTSCHEIDUNG).**
`R4B-01 | Supervisor loggt shutdown_sigterm_escalation (WARNING) bei jedem Shutdown/Reload, dessen Drain mit
einem Langläufer bis drain_until läuft | E-018: „Supervisor: SIGTERM to workers still alive at T - 7 s“ =
Drain-Ende T - 7 s (delivery-semantics.md:294-296); Nachtrag FS-30 „zweites SIGTERM setzt keinen neuen Anker“ |
Doku optional: Satz „ein Worker, der bis zum Drain-Ende läuft, bekommt das SIGTERM regulär
(shutdown_sigterm_escalation); ein Hängen zeigt erst shutdown_sigkill_escalation“ | schwach (P4): WARNING für
einen geplanten Schritt; Option Stufe INFO, wenn der Drain des Kindes noch innerhalb des Budgets liegt`.
Fix-Art: keine bzw. Doku; Code nur nach Georgs Entscheidung (dann Register-Nachtrag zu E-018 und Test).

---

## R4B-02 – Logrotate `copytruncate`: Zeilenverlust und NUL-Bytes nicht dokumentiert

**Einstufung: LÜCKE (Doku), P3.**

**Sachverhalt bestätigt, aber nicht in swarmq.** swarmq schreibt im Mehrprozessbetrieb keine Logdatei: Die
eingebaute Einrichtung nutzt Stream-Handler, `src/swarmq/logging_file.py` lädt nur die dictConfig-Datei
(`Path(path).open(encoding="utf-8")`, Zeile 171, lesend) und startet die Listener der `QueueHandler`. Den
Dateideskriptor und damit `O_APPEND` legt der Aufrufer fest (Shell `>`/`>>`, systemd
`StandardOutput=file:`/`append:`); in der Probe `p4_logrotate.py:39` `open(log, "ab" if mode == "append" else
"wb")`. Beide Folgen sind Eigenschaften von logrotate bzw. der Umleitung: (a) Verlust im Fenster zwischen Kopie
und Kürzen (logrotate(8): „some logging data might be lost“; 1 Zeile in 21 Rotationen, `out/p4-append-1.json`:
`missing_sample: [194]`), (b) ohne `O_APPEND` schreibt der Prozess nach dem Kürzen am alten Offset weiter
(dünnbesetzte Datei, 3,1 MB NUL in `p4-write-1`). swarmq selbst: keine Exception, kein ungültiges JSON,
vollständiger Drain.

**Entscheidungs-Abgleich.** Kein Register-Eintrag (grep `logrotate|copytruncate|rotat` über
`compound-packs/`: nichts). Die Empfehlung selbst ist geplant: `docs/plans/2026-10-06-0831-refactor-logging-haertung-plan.md:208`
„Dateirotation gehört außerhalb des Prozesses, z. B. in journald, logrotate mit `copytruncate` oder einen
Collector“; umgesetzt in a767266 (U12). Über die Folgen von `copytruncate` entscheidet der Plan nichts.
`docs/solutions/` ohne Treffer.

**Quelle der Erwartung.** Nur die Empfehlung in `website/reference/logging.md:336-341` (DE `logging.de.md:351`):
„Rotate outside the process: journald, logrotate with `copytruncate`, or a log collector.“ Sie sagt nichts über
Vollständigkeit; keine Zusage „verlustfrei“ ist verletzt. Darum kein FEHLER.

**Realismus.** (b) ist eine echte Falle (systemd `file:` statt `append:` ist verbreitet) und deterministisch;
(a) selten und klein. Wer der Empfehlung folgt, landet direkt in beiden.

**Widerlegungsversuche.**
1. Fehler im Code? Nein (siehe oben), nichts in swarmq öffnet die Datei.
2. NICHT HALTBAR, weil allgemein bekanntes logrotate-Verhalten? Nicht ganz: Die Doku empfiehlt ausgerechnet
   diese Option als Lösung; ein Halbsatz zur Voraussetzung `O_APPEND` gehört dazu.

**Produktfrage (PF).** Soll `logging.md` die Folgen von `copytruncate` nennen?
- (a) Ein Satz in der Warnbox (EN + DE): „`copytruncate` needs the output opened in append mode (`>>`, systemd
  `StandardOutput=append:`), otherwise the file fills with NUL bytes after each rotation; lines written between
  copy and truncate are lost. journald or a collector avoid both.“
- (b) `copytruncate` aus der Empfehlung streichen und nur journald/Collector nennen.
- (c) belassen.
Empfehlung: (a), reine Doku-Änderung, kein Test (Verhalten von logrotate und Shell).

**Vorschlag Register-Zeile (LÜCKE).**
`PF-n | P3 | Nennt logging.md die Folgen von logrotate copytruncate (Zeilenverlust zwischen Kopie und Kürzen;
NUL-Bytes ohne O_APPEND)? Quelle: logging.md:340 empfiehlt copytruncate ohne Einschränkung; Probe U15 Abschnitt 4 |
(a) Satz zu append-Modus und Verlustfenster, (b) copytruncate streichen, (c) belassen | (a) | offen (Georg)`.
Fix-Art: Doku.

---

## R4B-03 – `schedule status` verschweigt Worker mehrerer Supervisoren eines Hosts

**Einstufung: FEHLER, P3.**

**Sachverhalt (Code).** Der Scheduler-Loop meldet in jeder Rolle seine Version unter
`self._worker_id` (`scheduler/loop.py:345-349`), und der ist `config.worker_identity`
(`worker/lifecycle.py:179`). `SwarmQConfig.worker_identity` bildet `f"{base}:{slot}"` mit `base =
self.worker_id or socket.gethostname()` (`src/swarmq/config.py:620-633`); nicht `identity.py` (dort steht die
Ausführungsidentität eines Tasks) und nicht `leader_version.py`. Der Schlüssel ist
`sq:{sched}:worker_version:<worker_identity>` (`keyspace.py:171-175`), geschrieben per `SET … PX`
(`backend/valkey/schedules.py:412-416`), gelesen per SCAN + MGET (`:418-432`), ausgegeben von
`cli/schedule_cmd.py:342`. Zwei Supervisoren auf einem Host ohne `SWARMQ_WORKER_ID` haben dieselben Slots
`0..N-1` und überschreiben sich gegenseitig; sichtbar bleibt je Slot der zuletzt Schreibende.

**Reproduktion (eigener Lauf).** Reproduziert: 4 Kinder, 2 Einträge ohne `SWARMQ_WORKER_ID`, 4 mit (Abschnitt „Reproduktion R4B-03“ unten).

**Entscheidungs-Abgleich.**
- Register: kein Eintrag zu Worker-Version, Worker-Identität oder `SWARMQ_WORKER_ID` (grep über
  `compound-packs/` und `docs/befunde/*.md`: nichts).
- E-029 (`e029-leader-mindestversion-0-12.md`) regelt nur Warnung und Gauge für den **Leader**; die
  Leader-Erkennung ist nicht betroffen (Probe 6b: genau eine `scheduler_leader_outdated`, wie entschieden). Die
  WORKERS-Liste ist das Werkzeug, mit dem der Betreiber die Apps unter der Mindestversion findet.
- Plan/Commit der Funktion: 9dc064a „Worker melden ihre Version (sq:{sched}:worker_version:<id>)“, Doku 2e4948b
  (0.10.0, U4); keine Scope-Grenze zu mehreren Supervisoren je Host. `docs/plans/2026-10-09-2314-…:742` nennt
  nur „SCAN in `worker_versions()`“.
- Tests: `tests/unit/test_cli_schedule_status.py`, `tests/unit/backend/test_valkey_schedules.py` u. a. prüfen
  Ausgabe und Schlüssel, keiner mit `entscheidung`-Marker, keiner hält die Kollision als erwartet fest.

**Quelle der Erwartung.**
- `website/reference/cli.md:377-380`: „`WORKERS` lists every worker whose report is live (three
  `SWARMQ_SCHEDULER_SEEN_INTERVAL_S`) … Every version shown here should be at least the minimum leader version
  (currently 0.12 …)“.
- Gleiche Regel an anderer Stelle: Der Kontrollkanal meldet den Status je Supervisor unter `<supervisor>` mit
  „the supervisor id is `SWARMQ_WORKER_ID` or the hostname plus `:p<pid>`“ (`cli.md:247-249`; Code
  `process_manager/supervisor.py:144`, `process_manager/run.py:194` hängt `:p<pid>` sogar mit gesetztem
  `SWARMQ_WORKER_ID` an). swarmq rechnet also selbst mit mehreren Supervisoren je Host.
- Gegenprobe Doku zu `SWARMQ_WORKER_ID` (`website/reference/configuration.md:139, 224-242`): „Base of the worker
  identity … the default is the hostname“, Beispiel `web-3:0`, `web-3:1`. Kein Wort, dass der Wert bei mehreren
  Supervisoren je Host gesetzt werden muss; die Identität ist als Dimension für Signale, Metrik-Labels und Logs
  beschrieben („a stable identity“), nicht als eindeutiger Schlüssel. Ein Pflichtwert ist also nicht
  dokumentiert; die Doku stellt den Betreiber nicht vor die Wahl.

**Realismus.** Mittel. Tritt nicht auf in Kubernetes und Docker mit eigenem Hostnamen je Container. Tritt auf bei
mehreren systemd-Units/Apps auf einer VM, beim Rolling Deploy alter und neuer Supervisor nebeneinander auf
demselben Host (genau der Mischbetrieb, für den die Liste gedacht ist), bei Docker mit `network_mode: host`
(Hostname des Hosts; nicht geprüft). Folge: nur Beobachtbarkeit, keine Ausführung betroffen; die Leader-Warnung
nach E-029 funktioniert. Darum P3, nicht P2.

**Widerlegungsversuche.**
1. Pflicht-Konfiguration verletzt? Nein, siehe oben: nicht dokumentiert.
2. Gilt „every worker“ vielleicht je Identität, also bewusst zusammengefasst? Die Doku nennt „every worker whose
   report is live“ und ein Ziel („Every version shown here should be at least …“), das bei der Kollision gerade
   verfehlt wird: Welche Version sichtbar ist, entscheidet der Schreibzeitpunkt, nicht die älteste.
3. Altlast aus 0.11 (gleicher Schlüssel)? Erklärt die Ursache, hebt die Zusage nicht auf; ein Fix muss den
   0.11-Schlüssel weiter lesen (Mischbetrieb).

**Vorschlag Register-Zeile (P3 – FEHLER).**
`F4-nn | swarmq schedule status listet unter WORKERS nur einen Eintrag je <hostname>:<slot>, wenn mehrere
Supervisoren auf einem Host ohne eigenes SWARMQ_WORKER_ID laufen; report_worker_version überschreibt
sq:{sched}:worker_version:<worker_identity> (backend/valkey/schedules.py:412, scheduler/loop.py:345,
config.py:620-633). Im Mischbetrieb kann die 0.11-App unsichtbar sein. | cli.md:377 „WORKERS lists every worker
whose report is live“; gleiche Regel im Kontrollkanal mit :p<pid> (cli.md:247-249) | Code: Versionsschlüssel um
eine prozesseindeutige Komponente ergänzen (z. B. Owner-Token oder :p<pid> des Kindes) und Anzeige weiter
<worker_identity>; alternativ Doku: SWARMQ_WORKER_ID je Supervisor eindeutig verlangen (schwächer, Fix-Art Doku)
| R4B-03`.
Nachweis (U18) wie vom Finder vorgeschlagen: `tests/e2e/test_befund_r4b03_schedule_status_workers_same_host.py`,
zwei `spawn_worker` ohne `SWARMQ_WORKER_ID`, je `--processes 1`, `SWARMQ_SCHEDULER_SEEN_INTERVAL_S=2`,
Assertion `len(workers) == 2` (rot: 1). Mit `--processes 1` statt 2 genügt ein Kind je Supervisor; die Assertion
bleibt fachlich (Anzahl der lebenden Worker).

---

## Stichprobe der Bestätigungen (Abläufe 1–6)

| Ablauf | Gestützt auf | Bewertung |
|---|---|---|
| 1 Kontrollkanal | Zähler je `task_id` (`u15:exec:<id>`, `u15:done:<id>`, `p1_kontrollkanal.py:114-124`), Zeitstempel der Rückmeldung | tragfähig. Die Doppelausführungen in Teil B (18/82 bei `SHUTDOWN_TIMEOUT=1`) sind E-001 Nachtrag FS-02a (Drain-Abbruch → `redeliver`), richtig zugeordnet. F3-41 nicht ausgelöst: ehrlich als „ohne neuen Beleg“ geführt, kein Gegenbeleg. |
| 2 Hot-Reload | Identitäten je `task_id` (`mod/u15_tasks.py`: `incr u15:exec:<task_id>`), nicht bloße Zählung | „keine Doppelausführung“ in Läufen 2/3 tragfähig (880/880, `multi_exec` leer). **Schwach in Lauf 1:** Warten nach 120 s abgebrochen, 264 Tasks noch in der Queue; „keine verlorene Ack“ ist dort nicht belegt, nur nicht widerlegt. Läufe 2 und 3 decken es ab. Die PID-Gleichheit über `execvpe` ist beobachtet. |
| 3 CLI | Exitcodes und JSON-Ausgaben, ein Lauf | tragfähig, nicht zeitkritisch. |
| 4 Logrotate | Sequenznummern gegen Valkey-Zähler | tragfähig für swarmq; siehe R4B-02. |
| 5 Supervisor-Ersatz | Lock-Besitzer per PID (`p5_ersatz.py:25, 57`), Zähler je `task_id` | „Leader-Wechsel“ tragfähig für die Lock-Freigabe (5 s < `scheduler_lock_ttl` 30 s geht nur mit Freigabe), aber **nicht für den Handover-Marker**: `events` in `out/p5-*.json` zeigen 0 × `scheduler_leader*` in beiden Supervisoren (die Logs der Probe enthalten nur `task_succeeded` u. ä.), ob B `SCHEDULER_LEADER` statt `…_FAILOVER` sendet, ist nicht beobachtet. Die 5,0 s sind Artefakt der Phasenlage (Wahl-Takt 10 s); die Spanne „0–10 s“ ist abgeleitet, nicht gemessen. Laut Doku (`website/guides/scheduling-and-retries.md:410-425`) gibt es keine Zusage für sofortige Übernahme, also kein Verdacht. Kleine Inkonsistenz: Tabelle zeigt für L1 „A endet –“, `out/p5-1.json` hat `a_exit_s: 0.16`. |
| 6 Mischbetrieb | Suite `tests/compat` 2 × grün; 6b genau eine `scheduler_leader_outdated` | tragfähig für E-029. |

Empfehlung: Für Ablauf 5 bei Bedarf in U18 die Signale (`SCHEDULER_LEADER_HANDOVER` bei A, `SCHEDULER_LEADER`
bei B) mitschneiden; das ist kein Verdacht, nur eine Lücke der Beobachtung.

## Bezug zu F3-41 und zum E-018-Restpunkt

- F3-41 (Scale-down-Drain, Frist `shutdown_grace_s + SHUTDOWN_TERM_S` gegen Lebenszeichen): kein Verdacht der
  Spur B berührt ihn; Ablauf 1 Teil B liefert 32 Zyklen ohne `worker_hung_during_drain`, also keinen neuen
  Beleg in die eine oder andere Richtung.
- E-018-Restpunkt (`restart_budget.py`/`setpoint.py:210` mit alter Bedeutung): Ablauf 1 bestätigt nur die
  dokumentierte Status-TTL von 40 s; R4B-01 betrifft `sigterm_at`/`drain_until`, nicht diesen Punkt.

## Reproduktion R4B-03

Eigener Lauf am 2026-10-17 unter `scripts/with-test-lock.sh` (Sperre nach Ende eines Chaos-Laufs erhalten),
Probe `…/scratchpad/u15/p6c_workers.py` des Finders unverändert (vhost `swarmq-u15-wk`, Valkey-DB 13, danach
geleert), Ausgabe `…/scratchpad/u17/p6c-repro.out`, Exit 0:

| Fall | Supervisoren | Kinder | `workers` in `schedule status --format json` |
|---|---|---|---|
| ohne `SWARMQ_WORKER_ID` | 2 | 4 | 2: `Laptop:0`, `Laptop:1` |
| mit `app-a`/`app-b` | 2 | 4 | 4: `app-a:0`, `app-a:1`, `app-b:0`, `app-b:1` |

Befund des Finders reproduziert (identisch zu U15 6c). Mit gleicher Version sind die Überdeckungen unsichtbar;
im Mischbetrieb (6b des Finders) entscheidet der letzte Schreiber, welche Version erscheint.
