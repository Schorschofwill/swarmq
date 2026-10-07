# Analyse der Logging-Schwächen in swarmq 0.8.0 (2026-10-06, main 327cd0f)

Drei read-only Ursachenanalysen mit Reproduktionsskripten unter
`/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/c34caf1f-19af-4a0f-8909-0a1b7187e94d/scratchpad/analysis-{a,b,c}/`.
Alle Kernbefunde sind reproduziert (nicht reproduzierte Aussagen sind als Hypothese markiert).

## S1 Drop-Meldung blockiert den Aufrufer (Event-Loop) — REAL
- Beleg: stderr auf volle Pipe; 30 `log.warning` auf dem Loop, Leser nach 3 s → langsamster Logaufruf 2948 ms, Loop-Lag 2939 ms (human und json gleich). Ohne Leser: unbegrenzt.
- Ursache: `src/swarmq/logging.py:193` `print(self._drop_notice(), file=sys.stderr)` in `_drop` (180–193), aufgerufen aus `enqueue` (151–161) im Aufrufer-Thread. Queue ist voll gerade WEIL stderr blockiert.
- Betroffen: alle `_StructlogQueueHandler`-Nutzer (Supervisor, Worker, Compute-Kinder eingebaut; Opt-in; App-gewinnt-Übernahme).
- Zusatz (reproduziert): bei geschlossenem stderr wirft `print` ValueError; `handleError` fängt nur OSError → Exception fliegt in Anwendungscode.
- Empfehlung: Meldung in den Listener-Thread verlegen (Aufrufer zählt nur); kein nicht-blockierendes `os.write` (O_NONBLOCK teilt die File Description mit dem Listener).

## S2 Kein Zeitlimit beim Leeren (shutdown_logging/stop) — REAL
- 2.1 Toter Listener + volle Queue → `stop()` hängt für immer; atexit hängt (Exit 124 / faulthandler-Stack: `shutdown_logging → _stop_listeners → QueueListener.stop → enqueue_sentinel → queue.put`). Trigger: App-Handler, dessen `emit` wirft, oder Filter, der wirft (stdlib `_monitor` fängt nichts pro Record); bei eingebaut: geschlossenes stderr.
- 2.2 Blockiertes stderr (lebender Listener) → `shutdown_logging()` blockiert so lange wie stderr (4 s gemessen; ohne Leser Exit 124).
- 2.3 Langsamer App-Handler → Leerdauer = Records × Latenz, unbegrenzt (10 000 × 100 ms ≈ 17 min).
- Ursachen: `logging.py:62–64` `running` = `_thread is not None` (toter Thread gilt ewig als laufend, kein Neustart, Queue füllt sich, alles wird verworfen); `logging.py:84–89` blockierendes Sentinel-`put` ohne Timeout + `join` ohne Timeout; `_OptinQueueListener.handle`/`_dispatch_to_root` (237–254) ungeschützt; Datei-Modus `logging_file.py:133–140` (`put`+`join` ohne Timeout), `:160` (`_restart_lazily` nur bei `_thread is None`).
- Betroffene Aufrufstellen: atexit (logging.py:499), Worker-`finally` (entry.py:213, 123), Supervisor vor `execvpe` (supervisor.py:371 — Worker sind dann schon gestoppt → Ausfall ohne externen Eingriff), CLI-Exitpfade (cli/__init__.py:276, 285, 308), Compute-Kind pro Job vor dem Ergebnis (child.py:263–270 `_send_result` → Ergebnis wird nie gesendet), Kind-Ende (child.py:283).
- Weitere: Records in der Queue eines toten Listeners werden nicht als verworfen gezählt; `_stop_listeners` stoppt seriell (einer hängt → alle); Datei-Modus mit unbegrenzter stdlib-Queue wächst nach Listener-Tod unbegrenzt.
- Nicht reproduziert: RecursionError beim Formatieren tötet den eingebauten Listener nicht (Formatter fängt ihn).
- Empfehlung: (A) Listener stirbt nie (Exception pro Record/Handler fangen, Fehler zählen/melden; `running` = Thread lebt → Watchdog-Neustart beim nächsten Record) + (B) begrenztes Stoppen mit EINER Gesamtfrist über alle Listener (`put(timeout)`, `join(timeout)`, sonst aufgeben, Rest als verworfen zählen, eine Abschlusszeile best effort) + (C) `Queue.shutdown()` (3.13+) statt blockierendem Sentinel, danach frische Queue für Lazy-Restart + (D) dasselbe für Datei-Modus-Listener. Neue Einstellung `log_drain_timeout` (Default ~5 s).

## S3 `SwarmQ.__aexit__` leert synchron auf dem Event-Loop — REAL
- Beleg: Opt-in, App-Handler schläft 0,5 s/2 s pro Record: Logaufrufe 0,4 ms (ok), aber `__aexit__` lässt den Loop 2451 ms bzw. 11951 ms stehen; mit totem Listener + voller Queue unendlich.
- Ursache: app.py:128–130 → `_disable_log_queue` (logging.py:302–308) → `_remove_log_queue` (334–351) → `handler.listener.stop()` (blockierendes put + join).
- Betroffen: eingebettete Apps mit `log_queue=True`. Nicht: Worker/CLI (Opt-in dort wirkungslos), App-gewinnt-Übernahme (über shutdown_logging, siehe S2).
- Empfehlung: synchron in O(1) abhängen (Handler entfernen, propagate/Level zurück), Queue schließen (non-blocking), dann `await asyncio.to_thread(join, timeout)` bzw. Event; begrenzt durch `log_drain_timeout`, Rest zählen. Erneuter Erwerb während des asynchronen Leerens installiert frischen Handler/Queue. `to_thread` allein ist nur sicher, wenn `stop` selbst begrenzt ist.

## S4 Opt-in-Listener startet im Forkserver → Exit 78 mit irreführender Meldung — REAL
- Beleg (echter `swarmq worker --pool cpu`): Modul erzeugt `SwarmQ(log_queue=True)` und loggt danach auf `swarmq.*` INFO → Exit 78, Meldung beschuldigt BLAS/OpenMP-Berechnung; tatsächlich Thread `swarmq-log-list`.
- Trigger (alle drei): Opt-in installiert (`log_queue=True` oder `SWARMQ_LOG_QUEUE=1` im Env — auch ohne Argument!) + danach ein Record auf `swarmq`/`swarmq.*` + Level ≥ swarmq-Level (Default INFO). Auch `setup_dishka()` zweimal (Bibliothek loggt `dishka_container_replaced`). `SwarmQ()` selbst loggt beim Bau nicht.
- Ohne Opt-in: nichts (Forkserver unkonfiguriert, Records gehen synchron an lastResort).
- Kinder erben keinen toten Listener (Fork-Hook funktioniert); Restrisiko: Fork mit laufenden Threads (siehe R3).
- Ursache: logging.py:151–154 Lazy-Start in jedem Prozess inkl. Forkserver; KTD10/AE6 sicherten nur "kein Thread in `__init__`" zu. forkserver.py:192–198 Meldung fest auf BLAS. Testlücke: `tests/integration/test_compute_child_logging.py` Test 5 loggt vor `SwarmQ()`; `_compute_logging_driver.py:25` lässt `swarmq.worker.compute.preload` und `__main__` weg (Produktion: wiring.py:441).
- Empfehlung: (1) Im Fork-Quellprozess nie Listener-Threads starten: `swarmq.worker.compute.preload` (läuft zuerst und nur im Forkserver) setzt ein Flag in `swarmq.logging`; solange gesetzt, gibt `enqueue` synchron aus (`listener.handle(record)`); `_after_fork_in_child` löscht das Flag; `start_forkserver` stellt sicher, dass das Preload-Modul zuerst kommt. Prototyp per Monkeypatch bestätigt: 1 Thread, jede Kindzeile genau einmal. (3) Thread-bewusste Fehlermeldung: Thread-Namen aus `/proc/<pid>/task/*/comm` (comm kürzt auf 15 Zeichen → Präfix `swarmq-log-list` matchen oder Namen kürzen) → passender Hinweis (swarmq-Listener / App-QueueListener / sonst BLAS).
- Zusatz R4: `ForkserverHandle.restarted()` (forkserver.py:60–75) misst `threads` nach stillem Neustart, niemand prüft es.

## S5 Datei-Modus-QueueHandler nach `os.fork` tot — REAL, aber nur in engen Topologien
- Beleg: `configure_logging` mit `log_config_json` (QueueHandler→FileHandler), loggen, `os.fork()`: alle Kind-Records verloren; Queue wächst unbegrenzt; mit `maxsize` je Record `--- Logging error --- queue.Full` auf stderr; `shutdown_logging()` belebt nie.
- Erreichbar nur: Task-Code in IO-Worker oder Compute-Kind mit `--log-config` + QueueHandler, der selbst `os.fork()`/`get_context("fork")` nutzt. Keine swarmq-interne Topologie (spawn/forkserver/execvpe/frisch konfigurierte Kinder). Schwere: niedrig.
- Ursache: logging.py:473–492 `_after_fork_in_child` leert `_CONFIG_LISTENERS`, setzt aber `_thread`/Queue der Datei-Modus-Listener nicht zurück; logging_file.py:160–163 Neustart nur bei `_thread is None`.
- Empfehlung: im Fork-Hook für registrierte (Handler, Listener)-Paare frische Queue gleichen Typs/maxsize + `_thread=None` (Multiprocessing-Queues und nicht rekonstruierbare `"()"`-Queues auslassen). Prototyp bestätigt.

## S6 Hot-Reload-Prüfung im SIGHUP-Handler — REAL, schwerer als vermutet
- Ablauf: `_on_reload_signal` (supervisor.py:412–418) ruft `reload_precheck` (run.py:65–87, verdrahtet run.py:226 → logging_file.py:59–81 `probe_dict_config`, Unterprozess mit 30-s-Timeout, Z. 75) synchron im Signal-Handler (Hauptthread, gleicher Thread wie `_monitor_loop` supervisor.py:520).
- Probe-Dauer: normal 42–64 ms; SysLogHandler TCP auf schwarzes Loch: 30 031 ms → TimeoutExpired → gültige Konfiguration wird als "ungültig" abgelehnt.
- Szenario D: Worker während der Probe per SIGKILL getötet → `worker_died`/`restarting`/`spawned` erst nach 8,04 s (komplett eingefroren). Szenario A: Tod gar nicht bemerkt, Reload lief.
- Szenario B: SIGTERM während der Probe geht verloren → Reload + Neustart statt Beenden; Prozess lief 32 s später noch (unter systemd: Stop wird Restart, dann SIGKILL nach TimeoutStopSec).
- Szenario C: zweites SIGHUP während der Probe → verschachtelte Handler, zwei Proben parallel.
- Szenario E (schon vor der Probe vorhanden, betrifft ALLE Deployments): SIGTERM, 50 ms später SIGHUP → Supervisor startet neu statt zu beenden. `_on_reload_signal` prüft `_shutting_down` nicht; `_on_shutdown_signal` storniert keinen ausstehenden Reload.
- FileWatcher `is_busy=lambda: self._reload_requested` (supervisor.py:331) ist während der Probe False → Dateiänderung löst weiteres SIGHUP aus.
- Nicht betroffen: Heartbeats (Shared Memory) → kein falsches `worker_hung`; Hang-Erkennung nur verzögert. Eingefroren auch: Autoscale- und Setpoint-Ticks.
- Weitere: von außen getöteter Probe-Prozess (Ctrl-C in Vordergrund-Prozessgruppe) wird als "ungültige Konfiguration" gemeldet. Hypothese: Loggen aus Signal-Handlern kann deadlocken (nicht-reentranter Queue-Mutex in `enqueue`); betrifft auch den Shutdown-Handler.
- Empfehlung: Handler setzt nur Flag (`_reload_pending`), ignoriert bei `_shutting_down`; Monitor-Loop startet Probe per `Popen` (keine Threads) und pollt pro Tick mit eigener Frist; Shutdown storniert ausstehenden Reload; SIGHUPs werden zusammengefasst; `is_busy` deckt die laufende Probe ab; Probe-Timeout als eigenes Event (`log_config_reload_probe_timeout`) statt "ungültig". In-Process-Validierung verworfen (dictConfig mutiert globalen Zustand und öffnet Handler).

## S7 `basicConfig` im CPU-Pool anders als im IO-Pool — REAL, mit Folgeschäden
- Beleg (echter Worker, App-Modul ruft `basicConfig(level=INFO, format="APPFMT|…")`):
  - io: swarmq-Format, basicConfig No-op, keinerlei Event.
  - cpu: App-Format + `logging_reconfigured_by_app`.
  - `--log-format json`: im CPU-Pool KLARTEXT-Zeilen mitten im JSON-Strom.
  - `--log-config`: im CPU-Pool wird die Betreiber-Konfiguration in den Compute-Kindern IGNORIERT (Task-Logs im App-Format), Loop-Prozess im LOGCFG-Format.
  - basicConfig ohne Level: im CPU-Pool gehen INFO-Zeilen verloren (Root-Level der App = WARNING).
- Ursache: io: Worker konfiguriert Root vor App-Import (cli/worker_cmd.py:30–55, Vergleich logging.py:382–419). cpu: Forkserver-Preload (worker/compute/wiring.py:34, 58–66; worker/compute/preload.py) importiert die App mit unkonfiguriertem Root → basicConfig installiert StreamHandler und Root-Level; `configure_child_logging` (logging.py:422–448, Prüfung Z. 435 `if not settings.configure or not root_handler_snapshot()`) wertet jeden vorgefundenen Root-Handler als "App gewinnt", überspringt eingebautes Setup UND `--log-config`, wendet nur `logger_levels` an (`_apply_named_levels` logging.py:702). Hypothese: `--log-level` im Kind wird dann ignoriert.
- Doku (website/reference/logging.md ≈Z. 15, 335–342) verspricht das Gegenteil (basicConfig wirkungslos unter `swarmq worker`, gleiche Regel im CPU-Pool, `--log-config` besitzt das Logging vollständig).
- Empfehlung: Sentinel-Root-Handler (thread-frei, NullHandler-artig) im Forkserver VOR jedem App-Import (Anfang von `swarmq/worker/compute/preload.py`) → basicConfig dort ebenfalls No-op; im Kind dieselbe Regel wie im IO-Worker: nur Sentinel → entfernen und `configure_logging(settings)`; Sentinel ersetzt → App gewinnt; zusätzliche Handler → swarmq konfiguriert, behält sie, `logging_handlers_added_by_app`. Doku Z. 335–342 korrigieren.

## Weitere Befunde
- R1 (reproduziert): App mit eigenem QueueHandler (Cookbook-Muster, `listener.start()` beim Import) → CPU-Pool Exit 78 mit BLAS-Hinweis (Thread `Thread-1 (_moni`). Startet die App den Listener später (`--configure`-Hook, nur im Worker-Prozess) → Kinder erben ungestarteten Listener; "App gewinnt"-Zweig startet App-QueueHandler-Listener nie → ALLE Task- und swarmq-Zeilen der Kinder verloren, obwohl `logging_reconfigured_by_app` gemeldet wird. Fix: im Übernahme-Zweig `start_queue_listeners()` für geerbte, ungestartete App-QueueHandler; thread-bewusste Meldung (S4).
- R2 (reproduziert): Record während der Interpreter-Finalisierung (nach atexit-`shutdown_logging`, z. B. asyncio "Future exception was never retrieved", `__del__`) → Lazy-Neustart (logging.py:151–154) → `PythonFinalizationError: can't create new thread at interpreter shutdown` → `--- Logging error ---` + Record verloren; gleiches Muster in `_restart_lazily`. Fix: bei `sys.is_finalizing()` bzw. dieser Exception synchron `listener.handle(record)`.
- R3 (reproduziert als Race): Fork während der Listener mitten im Schreiben ist → im Kind `RuntimeError: reentrant call inside <_io.BufferedWriter …>` bei jedem Record (8/10 Läufen ohne Wartezeit, 0 mit 0,3 s). stdlib initialisiert Handler-Locks nach Fork neu, nicht aber den BufferedWriter-Lock. Für stderr eingebaut: 0/15, Hypothese. Mitigation: `os.register_at_fork(before=…)` erwirbt die Locks aller Listener-Ziel-Handler (laufende emits beenden), im Elternprozess wieder freigeben.
- R4: stiller Forkserver-Neustart wird nie auf Threads geprüft (siehe S4).
- R5: Integrations-Driver weicht von Produktion ab (siehe S4).
