# Changelog

Alle nennenswerten Änderungen an SwarmQ. Dieses Projekt folgt
[Keep a Changelog](https://keepachangelog.com/) und
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Geändert (breaking)

- **CPU-Pool: `logging.basicConfig()` in einem Task-Modul wirkt nicht
  mehr, und `--log-format`, `--log-level` und `--log-config` gelten in
  den Rechenkindern.** Beide Pools folgen jetzt derselben Regel „App
  gewinnt“. Bisher importierte der Forkserver deine Task-Module mit
  unkonfiguriertem Root-Logger; ein `basicConfig()` beim Import gewann
  deshalb in jedem Rechenkind: gegen `--log-format json` (Textzeilen
  mitten im JSON-Strom), gegen `--log-config` (Task-Zeilen im Format der
  App) und gegen `--log-level` (INFO-Zeilen gingen verloren, wenn
  `basicConfig` kein Level setzte). Jetzt hängt der Forkserver vor dem
  Import deiner Module einen Platzhalter-Handler an den Root-Logger (er
  startet keinen Thread und gibt WARNING und höher an `lastResort`
  weiter, wenn kein anderer Handler den Record bekommt), und jedes
  Rechenkind wendet die Regel des IO-Workers an:
  `basicConfig` wirkt nicht, ein Modul, das die Root-Handler ersetzt
  (etwa mit `logging.config.dictConfig`), gewinnt, und hinzugefügte
  Handler schreiben neben denen von SwarmQ. `logging_reconfigured_by_app`
  und `logging_handlers_added_by_app` erscheinen einmal pro Pool, vom
  Logger `swarmq.worker.compute.pool`. Jeder vom Forkserver per Fork
  gestartete Prozess beginnt ohne den Platzhalter; in Prozessen, die
  deine App selbst über den gemeinsamen Forkserver startet (etwa ein
  `ProcessPoolExecutor` mit der Standard-Startmethode `forkserver` unter
  Python 3.14), wirkt `basicConfig` also. Unter `--no-configure-logging`
  (`configure_logging=False`) wirkt `basicConfig` in beiden Pools. Das
  gilt ebenso für die Rechenkinder eines eingebetteten CPU-Pools. Ein per
  Pfad gestartetes Hauptmodul (`python app.py`) importiert der
  Forkserver vor SwarmQs Vorladen; dessen Logging beim Import gewinnt
  dort weiterhin. Garantiert gilt die Regel mit dem Befehl `swarmq` und
  mit `python -m`. **Migration:** Das Format, das du mit `basicConfig`
  setzt, gehört in eine `--log-config`-Datei; soll dein Modul das Logging
  in jedem Prozess besitzen, nimm `--no-configure-logging`. Loggt deine
  App über einen eigenen `QueueHandler`, starte dessen `QueueListener` im
  `--configure`-Hook statt beim Import; SwarmQ startet ihn dann in jedem
  Rechenkind. Details:
  [Logging](logging.md#wenn-ein-task-modul-das-logging-konfiguriert).

### Geändert

- **Der Listener-Thread heißt jetzt `swarmq-log-lsnr`** (bisher
  `swarmq-log-listener`): Linux behält 15 Zeichen eines Thread-Namens,
  und die Forkserver-Prüfung erkennt den Thread an diesem Namen.
  Betroffen ist nur Code, der den Thread über seinen Namen sucht.
- **`log_records_dropped` ist ein gewöhnlicher Record aus dem
  Listener-Thread** (Logger `swarmq.logging`, Level WARNING, Feld
  `count`), geschrieben über die eigenen Handler des Listeners,
  höchstens einmal pro Sekunde und noch einmal beim Stoppen. Bisher hat
  der loggende Code die Meldung selbst nach stderr geschrieben. Bei der
  Opt-in-Log-Queue formatieren deine Handler sie; `log_json` wählt dort
  nur das Format der neuen Zeile `log_drain_timeout_exceeded`.
- **`log_config_reload_failed` kommt jetzt vom Logger
  `swarmq.process_manager.reload`** (bisher `swarmq.process_manager.run`).
  Eine Prüfung der `--log-config`-Datei, die in ihre Frist von 30
  Sekunden läuft, wird stattdessen als `log_config_reload_probe_timeout`
  gemeldet.
- **Der Befehl `swarmq` startet über `swarmq.cli:console_main`** (bisher
  `swarmq.cli:main`), wie `python -m swarmq.cli`. Er beendet den Prozess
  hart, wenn nach dem letzten Leeren noch ein Log-Thread feststeckt
  (siehe Hinzugefügt). `swarmq.cli.main()` bleibt unverändert und endet
  nie hart.
- **Eine Logzeile je Zurückstellung, wenn das Backend vor der Ausführung
  nicht erreichbar ist.** Die Gate-Warnungen
  `cancel_check_backend_error`, `workflow_cancel_check_backend_error`,
  `cancel_store_result_failed` und `schedfire_dedup_backend_error`
  entfallen; `rate_limit_check_failed` und `lock_acquire_failed` (ERROR)
  erscheinen nur noch bei anderen Fehlern als einem unerreichbaren
  Backend. Ihre Angaben stehen jetzt in
  `task_deferred_backend_unavailable`: im neuen Feld `stage`
  (`completed_check`, `inflight_claim`, `cancel_check`,
  `workflow_cancel_check`, `cancel_store_result`, `check`, `acquire`)
  sowie, wo zutreffend, in `schedfire_key`, `workflow_id` oder
  `lock_name`. `task_defer_failed_redelivered` trägt dieselben Felder
  und den Backend-Fehler als `backend_error_type` und `backend_error`.
  Alarme auf die entfallenen Events sollten stattdessen auf
  `task_deferred_backend_unavailable` prüfen.

### Hinzugefügt

- **`log_drain_timeout`** (`--log-drain-timeout SECONDS`,
  `SWARMQ_LOG_DRAIN_TIMEOUT`, `SwarmQConfig(log_drain_timeout=...)`,
  `LoggingSettings.drain_timeout`; Vorgabe 5; Priorität CLI > Env >
  Config > Vorgabe): Obergrenze für jedes Leeren der Log-Queues — beim
  Prozessende, am Ende eines Worker-Subprozesses, bevor sich der
  Supervisor für einen Reload neu ausführt, in einem Rechenkind vor
  jedem Ergebnis, für die Listener einer `--log-config`-Datei und in
  `SwarmQ.__aexit__`. Alle Listener eines Prozesses teilen sich die Frist
  und werden parallel gestoppt; was bis dahin nicht geschrieben ist, gilt
  als verworfen. Ein Wert, der keine endliche Zahl größer als 0 und
  höchstens `threading.TIMEOUT_MAX` ist, beendet `swarmq worker` mit
  Exit `78` (bei `SwarmQConfig` ein `ConfigurationError`).
  `shutdown_logging()` nimmt ein optionales `timeout`.
- **Hartes Ende der eigenen Prozesse von SwarmQ.** Steckt nach dem
  Leeren noch ein aufgegebener Log-Thread fest, enden der Befehl
  `swarmq`, die Worker-Subprozesse und die Rechenkinder per `os._exit`
  mit dem Exit-Code, mit dem sie ohnehin geendet hätten, statt mit
  `Fatal Python error` abzubrechen (Exit `134`) oder zu hängen.
  Registrierte `atexit`-Funktionen, die noch nicht gelaufen sind, laufen
  dann nicht mehr; stdout wird nicht geleert, wenn es sich die Datei oder
  Pipe mit dem hängenden Stream teilt (`2>&1`, journald), und stdout und
  stderr werden höchstens 0,2 Sekunden lang geleert. Endet der Befehl
  `swarmq`, ein Worker-Subprozess oder ein Rechenkind mit einer
  Ausnahme, geht ihr Traceback (oder die Meldung eines `SystemExit`)
  vorher auf stderr, gekürzt auf 4096 Bytes und nur, wenn stderr nicht
  blockiert; Strg-C beendet den Befehl `swarmq` dann mit `130`. Apps,
  die SwarmQ einbetten, werden nie hart beendet.
- **Neue Events:** `log_handler_errors` (ein Handler, Filter oder
  Formatter hat geworfen; Feld `count`), `log_drain_timeout_exceeded`
  (eine Zeile direkt auf stderr, wenn ein Leeren die Frist überschritten
  hat, nur wenn stderr nicht blockiert; Felder `count`, `timeout_s`),
  `log_config_reload_probe_timeout` (Felder `path`, `timeout_s`, `hint`)
  `forkserver_restarted_multithreaded` (ERROR; Felder `pid`, `threads`,
  `thread_names`, `hint`), `compute_fork_source_rejected` (ERROR; Felder
  `reason`, `hint`) und `worker_fork_source_rejected` (Supervisor,
  WARNING; Felder `worker_id`, `pid`, `exitcode`).
- **Die Thread-Prüfung des Forkservers nennt die gefundenen Threads**
  und die wahrscheinliche Ursache: die Opt-in-Log-Queue, einen beim
  Import gestarteten logging-`QueueListener` deiner App oder sonst eine
  BLAS-/OpenMP-Rechnung. Sie läuft auch nach einem stillen Neustart des
  Forkservers, bevor das nächste Rechenkind Arbeit bekommt. Ein neu
  gestarteter Forkserver mit mehr als einem Thread wird verworfen:
  `/ready` meldet den Pool als ungesund, der Worker loggt
  `compute_fork_source_rejected`, hört auf zu konsumieren und fährt
  geordnet herunter (die laufende Nachricht geht ohne Retry-Zählung
  zurück in die Queue). Ein Worker-Subprozess endet dann mit Code `71`,
  und der Supervisor startet ihn mit frischem Forkserver neu (im
  Neustart-Budget). Mit eingebettetem CPU-Pool wirft `Worker.start()`
  nach dem Herunterfahren `swarmq.supervision.ForkSourceRejected`; ob
  neu gestartet wird, entscheidet deine App.

### Behoben

- **Ein Scheduler-Fire geht nicht mehr verloren, wenn Valkey direkt
  nach seinem Claim ausfällt.** Der Worker beansprucht einen Fire in
  Valkey, bevor er den Task ausführt. Beendete eine spätere Prüfung
  (Cancel-Check, Zustellzähler) die Zustellung, wurde der Claim nie
  freigegeben; scheiterte die Freigabe selbst (Valkey weg), sah die
  zurückgestellte, wiederholte oder neu eingereihte Kopie den Claim,
  hielt sich für ein Duplikat und bestätigte sich, der Fire lief also
  nie (der Claim lebt bis zu `SWARMQ_SCHEDULER_INFLIGHT_TTL`, Standard
  300 s). Jetzt speichert der Claim ein Owner-Token, jede Kopie
  trägt es im neuen Header `x-swarmq-schedfire-claim`, und eine Kopie,
  die ihr eigenes Token findet, übernimmt den Claim (das Token rotiert
  dabei, eine Broker-Redelivery dieser Kopie bleibt also ein Duplikat).
  Echte Duplikate (ein zweites Publish des Fires, eine Redelivery des
  Originals) werden weiter verworfen. Prüfungen, die die Zustellung vor
  dem Task beenden, geben den Claim jetzt ebenfalls frei, mit
  Owner-Prüfung. Neues Lua-Skript `sched_fire_claim.lua`;
  `ScheduleStore.claim_fire` hat die neuen optionalen Keywords `owner`
  und `takeover`, `ScheduleStore.release_fire_claim` das neue optionale
  Keyword `owner` (Stores von Dritten müssen sie annehmen). Ein
  Neueinreihen an derselben Stelle (`nack(requeue=True)`, wenn auch der
  Broker die Kopie nicht annimmt) kann kein Token tragen; scheitert dann
  auch die Freigabe, wartet dieser Fire weiter auf die Claim-TTL.
- **Retry-Kopien erben die Zustellzähler des Brokers nicht mehr.** Die
  Kopie für einen Retry, einen Rate-Limit-Treffer oder einen
  Lock-Konflikt übernahm `x-delivery-count` und `x-acquired-count` vom
  Original. RabbitMQ 4 behält einen vom Publisher gesetzten
  `x-delivery-count` bei der Zustellung bei, RabbitMQ 3.13 zählt
  zusätzlich Requeues; eine Kopie, deren geerbter Zähler
  `SWARMQ_DELIVERY_LIMIT - 1` erreichte, landete als `WorkerHungError`
  in `{queue}.XQ`, obwohl der Task noch Retries hatte. Diese Kopien sind
  jetzt neue Nachrichten mit eigenem Zustellbudget, wie die Kopien der
  Zurückstellung.
- **Ein Backend-Ausfall vor der Ausführung verbraucht das Zustellbudget
  nicht mehr.** Erreichte eine Prüfung vor dem Task (Schedule-Fire-Dedup,
  Cancel-Check, Rate-Limit, Lock) Valkey nicht, reihte der Worker die
  Nachricht an derselben Stelle neu ein (`nack(requeue=True)`), etwa alle
  1,5 s (abgewiesene Verbindung) bis 5 s (Blackhole). Auf einem Broker,
  der Requeues auf `x-delivery-limit` anrechnet (RabbitMQ 3.13),
  verschob der Broker die Nachrichten nach etwa 20 Runden nach
  `{queue}.XQ`, ohne `TASK_DLQ`, Fehlerergebnis oder Logzeile; auf
  RabbitMQ 4 kreiste die Nachricht ohne Backoff. Jetzt publiziert der
  Worker eine Kopie in die Delay-Queue und bestätigt das Original erst,
  wenn der Broker die Kopie bestätigt hat. Die Wartezeit beginnt bei
  `SWARMQ_BACKEND_DEFER_BASE_DELAY` (neu, Standard 1 s) und verdoppelt
  sich je Zurückstellung bis `SWARMQ_BACKEND_DEFER_MAX_DELAY` (neu,
  Standard 60 s); die Kopie behält alle SwarmQ-Header, aber nicht die
  Zustellzähler des Brokers (neuer Header
  `x-swarmq-backend-defer-count`). Das ist kein Retry: `max_retries`
  bleibt unberührt, `TASK_RETRYING` feuert nicht. Jede Zurückstellung
  loggt das neue WARNING-Event `task_deferred_backend_unavailable`
  (Felder `gate`, `stage`, `defer_count`, `delay_ms`, `error_type`,
  `error`; siehe Geändert).
  Nimmt auch der Broker die Kopie nicht an, reiht der Worker wie bisher
  an derselben Stelle neu ein und loggt `task_defer_failed_redelivered`.
  `ValkeyRateLimitStore.check` wirft bei Verbindungs- und
  Timeout-Fehlern jetzt `BackendConnectionError`.
- **Ein Scheduler-Fire überschreibt keine Zeitplanänderung mehr, die
  während des Fires passiert.** Ein `swarmq schedule remove`
  (deaktivieren oder `--permanent`) oder ein Redeploy mit neuer Cron
  zwischen Poll und Commit wurde rückgängig gemacht: Der Zeitplan war
  wieder aktiv, wieder angelegt oder hatte wieder die alte Cron. Der
  Commit greift jetzt nur, solange der Zeitplan noch den gefeuerten
  Score hat; `sched_commit.lua` prüft das atomar (neues optionales
  `ARGV[5]`, abwärtskompatibel), und `ScheduleStore.commit_fire` hat das
  neue optionale Keyword `fired_ts_ms`.
- **Ein abgebrochener Worker-Start lässt den eingebetteten Scheduler
  nicht mehr weiterlaufen.** Ein Abbruch von `start_worker()` oder ein
  Fehler nach dem Start des Schedulers (Executor-Start,
  Queue-Deklaration, `WORKER_ONLINE`) ließ dessen Election- und
  Poll-Tasks weiterlaufen; der Prozess erneuerte den
  Scheduler-Leader-Lock, und kein anderer Worker wurde Leader.

- **Eine volle Log-Queue hält den loggenden Code nicht mehr an.** Bei
  blockiertem stderr schrieb der Aufrufer — im Event-Loop — die
  Drop-Meldung selbst, und der Loop stand sekundenlang still (2,9 s
  gemessen; ohne Leser von stderr unbegrenzt). Bei geschlossenem stderr
  warf ein Logaufruf `ValueError` in den Anwendungscode. Der Aufrufer
  zählt jetzt nur noch.
- **Ein werfender Handler, Filter oder Formatter tötet den
  Listener-Thread nicht mehr.** Danach wurde jeder Record still
  verworfen, und das Leeren beim Prozessende hing für immer. Dasselbe
  galt für die Queue-Handler einer `--log-config`-Datei, wo eine volle
  begrenzte Queue zusätzlich einen Traceback im Aufrufer ausgab. Fehler
  werden jetzt gefangen und gezählt (`log_handler_errors`), und ein
  toter Listener startet mit dem nächsten Record neu.
- **Das Leeren der Log-Queues kann keinen Prozess mehr hängen lassen.**
  Ein blockiertes stderr oder ein langsamer Handler ließ jedes Leeren so
  lange dauern wie das Ziel: das Ende jedes SwarmQ-Prozesses, den
  Supervisor vor einem Reload (seine Worker waren schon gestoppt) und
  ein Rechenkind, dessen Ergebnis dann nie ankam. Jedes Leeren ist jetzt
  durch `log_drain_timeout` begrenzt. Records in einer Queue, deren
  Listener nicht läuft, werden innerhalb der Frist geschrieben oder
  danach als verworfen gezählt; bis das Leeren zurückkehrt, startet kein
  Record mehr einen Listener, den das Leeren sofort wieder aufgäbe.
- **`SwarmQ.__aexit__` mit `log_queue=True` hält den Event-Loop nicht
  mehr an**, während die Opt-in-Queue geleert wird (bis zu 12 s gemessen
  mit einem langsamen Handler). Wird es beim Warten abgebrochen, geht
  der Abbruch sofort weiter, und das Leeren läuft im Hintergrund bis zur
  Frist weiter.
- **`SwarmQ.__aexit__` mit `log_queue=True` wirft nicht mehr, wenn der
  Default-Executor des Event-Loops schon heruntergefahren ist**
  (`loop.shutdown_default_executor()`, wie `asyncio.run` es am Ende
  tut). Das Leeren warf `RuntimeError: Executor shutdown has been
  called` in deinen Code; jetzt wartet es auf einem eigenen Thread,
  wie bisher begrenzt durch `log_drain_timeout`.
- **CPU-Pool mit Opt-in-Log-Queue:** Ein `swarmq.*`-Record, der beim
  Import geloggt wurde, während `SwarmQ(log_queue=True)` oder
  `SWARMQ_LOG_QUEUE=1` aktiv war (auch durch zweimaliges
  `setup_dishka()`), startete im Forkserver einen Listener-Thread. Der
  Worker verweigerte dann den Start mit Exit `78` und einem Hinweis, der
  eine BLAS-Rechnung beschuldigte. Der Forkserver schreibt solche
  Records jetzt synchron.
- **CPU-Pool mit eigenem logging-`QueueHandler`:** Ein beim Import
  gestarteter `QueueListener` ließ den Worker mit demselben irreführenden
  Hinweis nicht starten; einer, der im `--configure`-Hook startete, lief
  in den Rechenkindern nie, sodass alle ihre Zeilen verloren gingen,
  obwohl `logging_reconfigured_by_app` gemeldet wurde. SwarmQ startet ihn
  jetzt in jedem Kind, und die Startmeldung nennt den Listener der App.
- **Das Logging im CPU-Pool entsprach weder dem IO-Pool noch der
  Dokumentation** (`basicConfig` gewann gegen `--log-format json`,
  `--log-config` und `--log-level`); siehe Geändert (breaking).
- **Ein still neu gestarteter Forkserver wurde nie auf Threads
  geprüft.**
- **Hot-Reload mit `--log-config`:** Die Datei wurde im
  `SIGHUP`-Signal-Handler geprüft, was die Überwachung bis zu 30 Sekunden
  einfror (tote Worker wurden nicht ersetzt, Autoskalierung und
  Kontrollkanal pausierten). Ein `SIGTERM` während der Prüfung wurde zu
  einem Neustart statt zum Herunterfahren, ein zweites `SIGHUP` startete
  eine zweite Prüfung parallel, und der Datei-Watcher löste während der
  Prüfung erneut aus. Eine Prüfung, die in ihre Frist lief oder per
  Ctrl-C abbrach, wurde als ungültige Konfiguration gemeldet. Die Prüfung
  läuft jetzt außerhalb des Signal-Handlers in einer eigenen
  Prozessgruppe; mehrere `SIGHUP`s führen zu einer Prüfung.
- **`SIGTERM` beendet den Supervisor immer.** Ein `SIGHUP`, das kurz
  nach einem `SIGTERM` eintraf, startete den Supervisor neu, statt ihn
  herunterzufahren — in jedem Deployment, mit oder ohne `--log-config`.
  Auch ein `SIGTERM` während des Worker-Stopps für einen Reload wird
  jetzt beachtet, und eines, das ein anderer Thread des Supervisors kurz
  vor dem Neuausführen annimmt, bleibt anhängig und beendet den neuen
  Supervisor.
- **Fork aus Task-Code:** Nach `os.fork()` verloren die Queue-Handler
  einer `--log-config`-Datei im Kind jeden Record (mit begrenzter Queue
  gab jeder Record einen `Logging error` aus). Ein Fork, während ein
  Listener schrieb, konnte im Kind bei jedem Record
  `RuntimeError: reentrant call` hinterlassen. Das Kind bekommt jetzt
  frische Queues, und der Fork wartet bis zu 50 ms auf laufende
  Schreibvorgänge. Eine `multiprocessing`-Queue (oder ein Manager-Proxy)
  bleibt mit dem Elternprozess geteilt: Das Kind startet keinen eigenen
  Listener, die Records des Kindes schreibt der Listener des
  Elternprozesses. Unter rohem `os.fork()` startet SwarmQ den
  Feeder-Thread der Queue im Kind neu, und ein Manager-Proxy baut eine
  eigene Verbindung zum Manager auf, statt die des Elternprozesses
  mitzubenutzen. Auf ein Ziel, das belegt bleibt, wartet nur der erste
  Fork, nicht jeder weitere erneut.
- **Records, die beim Herunterfahren des Interpreters entstehen** (etwa
  „Future exception was never retrieved“ oder aus einem `__del__`),
  lösten einen `--- Logging error ---` aus und gingen verloren; sie
  werden jetzt synchron geschrieben.
- **Eine `multiprocessing`-Queue in einer `--log-config`-Datei beim
  Prozessende:** Die Exit-Funktion von `multiprocessing` läuft meist vor
  der von SwarmQ und schloss die Pipe der Queue, während der Listener
  noch aus ihr las. Noch nicht gelesene Records gingen verloren, und der
  Listener starb mit einem `EOFError`/`OSError`-Traceback — immer wieder
  neu gestartet, oft Hunderte Tracebacks. SwarmQ leert diese Listener
  jetzt, bevor `multiprocessing` die Queue schließt. Ein Record, der
  danach noch kommt (etwa aus einer `atexit`-Funktion, die vor dem
  Import von `multiprocessing` registriert wurde), gilt ohne Traceback
  als verworfen.

## [0.8.0] — 2026-10-06

### Geändert (breaking)

- **`SwarmQ()` konfiguriert kein Logging mehr.** Eine `SwarmQ`-App lässt
  Root-Handler, Root-Level und die structlog-Konfiguration unberührt
  (bisher installierte sie einen eigenen Root-Handler und rief
  `structlog.configure` auf). SwarmQ loggt über benannte Logger
  `swarmq.<modul>` in das stdlib-`logging`: Der Event-Name ist die
  Nachricht des Records, die Felder sind `LogRecord`-Attribute. Ganz ohne
  Logging-Konfiguration erreichen WARNING und höher weiterhin Pythons
  `lastResort`-Handler — SwarmQ installiert absichtlich keinen
  `NullHandler`. Eine App mit Root-Level WARNING, die sich für ihre
  Logausgabe auf `SwarmQ()` verlassen hat, sieht SwarmQs INFO-Events
  nicht mehr. **Migration:**
    - Reine stdlib-App: Logging selbst konfigurieren, z. B.
      `logging.basicConfig(level=logging.INFO)`, mit einem Formatter, der
      die Felder zeigt — ein JSON-Formatter, der die Attribute des Records
      liest, oder `swarmq.logging.build_formatter(json=True, colors=False)`.
    - structlog-App: `structlog.stdlib.ExtraAdder()` in die
      `foreign_pre_chain` deines `ProcessorFormatter` aufnehmen. Ohne ihn
      gehen SwarmQs Felder stillschweigend verloren.
    - `log_level` und `log_json` von `SwarmQConfig` bleiben als Felder
      erhalten, steuern aber nur noch die neue Opt-in-Log-Queue
      (`log_level` setzt das Level des Loggers `swarmq`, `log_json` das
      Format der Drop-Meldung).

    Rezepte: [Logging-Referenz](logging.md#einbindung-in-deine-app).

- **Vertrag der Logausgabe.** Jede Zeile aus Prozessen, die
  `swarmq worker` startet, trägt neben `event`, `level` und `timestamp`
  den neuen Schlüssel `logger`. Umbenannte Events:
  `swarmq.dedup.detected` → `dedup_detected`,
  `swarmq.dedup.rollback_failed` → `dedup_rollback_failed`,
  `publish_notification failed` → `publish_notification_failed`,
  `publish_workflow_notification failed` →
  `publish_workflow_notification_failed` (Feld `wf_id` → `workflow_id`),
  `health_server_started`/`_stopped`/`_request_failed` und
  `metrics_server_started`/`_stopped`/`_request_failed` →
  `http_server_started`/`http_server_stopped`/`http_server_request_failed`
  mit dem Feld `server` (`health_server`/`metrics_server`). In
  `watch_dir_inside_venv_or_stdlib` heißt das Feld `module` jetzt
  `module_name`; allgemein bekommt ein Feld, dessen Name mit einem
  `LogRecord`-Attribut kollidiert, das Präfix `swarmq_` (`name` →
  `swarmq_name`). Events, die eine Exception melden, tragen `error_type`
  und `error`; einen Traceback gibt es nur bei unerwarteten Fehlern.
  Das gilt jetzt auch für `task_sent_to_dlq` (`error_type` ist der
  Klassenname der Exception, die den Task in die DLQ geschickt hat,
  `WorkerHungError` beim Erreichen des Delivery-Limits).
  Alarme und Dashboards, die Logs per grep auswerten, müssen angepasst
  werden.
- **Log-Level folgen einer Regel** (ERROR: Datenverlust oder ein
  fehlgeschlagener Task bzw. Prozess; WARNING: Degradierung, die sich
  selbst heilt; INFO: Lebenszyklus von Prozess und Task; DEBUG:
  Diagnose). `check_backend_failed` und `check_broker_failed` INFO →
  WARNING; `task_sent_to_dlq` WARNING → ERROR; Fälle, die eine TTL heilt,
  ERROR → WARNING: `shutdown_lock_release_failed`,
  `scheduler_stop_failed`, `lock_release_failed`,
  `cancel_check_backend_error`, `workflow_cancel_check_backend_error`,
  `cancel_store_result_failed`, `schedfire_dedup_backend_error`.
  `worker_died` ist INFO bei sauberem Ende oder erwartetem
  Herunterfahren, sonst WARNING.
- **Logging-Einstellungen werden einmal im Supervisor aufgelöst.**
  Priorität: CLI-Flag > Env-Var > `SwarmQConfig` > Default. Der
  Supervisor gibt die aufgelösten Einstellungen an seine
  Worker-Subprozesse und Rechenkinder weiter, die das Environment nicht
  mehr lesen; ein Flag gewinnt jetzt in jedem Prozess.

- **CLI-Optionen schlagen jetzt in allen Prozessen die `SWARMQ_*`-Umgebung,
  für alle Optionen von `swarmq worker`.** Bisher gewann in den
  Worker-Prozessen die Umgebung bei `--queues`, `--concurrency`, `--pool`,
  `--burst`, `--burst-idle-timeout`, `--priorities`, `--metrics-port` und
  `--health-port` (z. B. schlug `SWARMQ_CONCURRENCY=5` die Option
  `--concurrency 20`), während der Supervisor die Option nutzte. Jetzt wird
  jede Option einmal als CLI > Umgebung > Default aufgelöst, und Supervisor,
  Worker-Prozesse, Rechenkinder und `--check` sehen dieselben Werte. Ohne
  Flag gilt die Umgebungsvariable weiterhin.

### Hinzugefügt

- **Logging-Optionen für `swarmq worker`:** `--log-config PATH`
  (`SWARMQ_LOG_CONFIG`; eine `dictConfig`-Datei als JSON, YAML mit
  PyYAML; `disable_existing_loggers` ist ohne Angabe `false`; einmal vom
  Supervisor geprüft und vor einem Reload erneut, der mit
  `log_config_reload_failed` abbricht, wenn die Datei inzwischen ungültig
  ist), `--no-configure-logging` (`SWARMQ_CONFIGURE_LOGGING=false`),
  `--logger-level NAME=LEVEL` (wiederholbar; `SWARMQ_LOGGER_LEVELS`),
  `--log-queue-size N` (`SWARMQ_LOG_QUEUE_SIZE`, Vorgabe 10000),
  `--log-lifecycle-level LEVEL` (`SWARMQ_LOG_LIFECYCLE_LEVEL`, Vorgabe
  DEBUG) und `SWARMQ_LOG_FORMAT` (gewinnt gegen den Alias
  `SWARMQ_LOG_JSON`). `CRITICAL` ist für jedes Level erlaubt. Ungültige
  Werte oder widersprüchliche Kombinationen (`--log-config` mit einem
  ausdrücklichen Format oder mit `--no-configure-logging`) beenden den
  Lauf mit Exit 78 und genau einer Zeile `worker_config_error`; eine
  fehlerhafte Konfigurationsdatei wirft den neuen `LoggingConfigError`.
- **Lebenszyklus-Events der Tasks** über den Logger `swarmq.task`:
  `task_received` und `task_started` (Lebenszyklus-Level, Vorgabe DEBUG),
  `task_succeeded`, `task_failed` und `task_cancelled` (INFO, mit
  `duration_ms`; `task_failed` mit `error_type`/`error`; `compute_child`
  bei Ergebnissen aus dem CPU-Pool). Abschalten mit
  `--logger-level swarmq.task=WARNING`.
- **Task-Kontext an jedem Event innerhalb eines Tasks:** `task_id`,
  `task_name`, `queue`, `worker_id`, dazu neu `workflow_id` (wenn gültig)
  und `attempt` (`retry_count + 1`) sowie `compute_child` in
  Rechenkindern. Der Kontext wird pro Nachricht gebunden und danach
  wiederhergestellt; Kontextvariablen deiner App werden nicht mehr
  gelöscht.
- **Opt-in-Log-Queue für eingebettete Apps:**
  `SwarmQConfig(log_queue=True)` / `SWARMQ_LOG_QUEUE=true` leitet
  `swarmq.*`-Events über den Hintergrund-Thread `swarmq-log-listener` an
  die Root-Handler der App, sodass langsame Handler den Event-Loop nicht
  mehr blockieren. Standardmäßig aus.
- **Regel „App gewinnt“:** Ein Task-Modul oder `--configure`-Hook, das
  die Root-Handler ersetzt, behält sie; SwarmQ leitet seine eigenen
  Events über seine Queue dorthin und loggt `logging_reconfigured_by_app`
  (und schaltet `swarmq.*`-Logger wieder ein, die die `dictConfig` der
  App deaktiviert hat). Hinzugefügte Handler meldet es als WARNING
  `logging_handlers_added_by_app`.
- **Verworfene Logeinträge werden sichtbar:** Eine volle Log-Queue
  verwirft, statt zu blockieren (WARNING und höher behalten 10 %
  Reserve), meldet `log_records_dropped` höchstens einmal pro Sekunde
  (im JSON-Modus als JSON-Zeile) und zählt die Verluste in der Metrik
  `swarmq_log_records_dropped_total`.
- **Neue Events** `worker_connection_error`, `worker_recycle_ack_timeout`
  (Exit 75), `log_config_reload_failed`, `logging_reconfigured_by_app`,
  `logging_handlers_added_by_app`, `log_records_dropped`, jeweils als
  strukturierte Zeile im konfigurierten Format.
- **Öffentliche Logging-API** in `swarmq.logging`: `get_logger`,
  `build_formatter`, `configure_logging`, `shutdown_logging`,
  `load_log_config` und `LoggingConfigError`. Neue Referenzseite:
  [Logging](logging.md).
- **Weitere Exceptions direkt aus `swarmq` importierbar:**
  `LoggingConfigError`, `SoftTimeoutError` und `PublishIndeterminate`
  (dieselben Klassen wie in `swarmq.exceptions`).

### Deprecated

- `swarmq.logging.setup_logging` (Einstiegspunkte nutzen
  `configure_logging`) und `swarmq.logging.flush_logging` (jetzt ein
  Alias für `shutdown_logging`).

### Behoben

- **Hot-Reload funktioniert nach `python -m swarmq.cli`.** Der Supervisor
  startete sich mit `sys.argv` neu; nach `-m` steht dort der Pfad von
  `swarmq/cli/__main__.py`, und das neue Abbild führte die Datei als Skript
  aus — ohne Arbeitsverzeichnis in `sys.path` (`--module ... cannot be
  imported`) und mit Workern, die per spawn in der Bootstrapping-Phase
  scheiterten. Der Neustart nutzt jetzt `sys.orig_argv`, das `-m` und
  Interpreter-Flags behält; `swarmq/cli/__main__.py` ruft `main()` nur noch
  als Hauptmodul auf. Der Start über das Kommando `swarmq` ist unverändert.

### Abhängigkeiten

- `structlog` ist jetzt auf `>=24,<27` begrenzt: `swarmq.logging.get_logger`
  hängt von der Aufruftiefe von `structlog.stdlib.BoundLogger` ab, die die
  Testsuite für 26.x festhält.

## [0.7.0] — 2026-09-22

### Hinzugefügt

- **Kontrollkanal für Worker-Gruppen.** `swarmq worker --max-processes N`
  schaltet ihn ein und erklärt den Spielraum; `--group LABEL` benennt die
  Gruppe (Vorgabe: die sortierte Queue-Menge). Ein Sollwert
  `{"min": M, "max": K}` unter `sq:control:setpoint:<gruppe>` wird vor dem
  ersten Kindprozess und danach alle 5 Sekunden gelesen: Eine höhere
  Untergrenze startet Worker sofort, eine niedrigere Obergrenze beendet die
  überzähligen sanft, ein gelöschter Schlüssel stellt die Startargumente
  wieder her, die feste Form `N..N` schaltet die Autoskalierung ab.
  Sollwerte über dem Spielraum, mit verdrehten Grenzen, unlesbare und
  Bereiche auf Gruppen ohne Autoskalierung werden mit Grund abgelehnt; ein
  Sollwert lässt den Start nie scheitern. Jeder Supervisor meldet unter
  `sq:control:status:<gruppe>:<supervisor>` mit Verfallsdatum seine
  wirksamen Grenzen samt Herkunft, den zuletzt gelesenen Sollwert, einen
  Ablehnungsgrund und seine Slot-Zahlen, sodass ein steuernder Dienst
  „abgelehnt", „noch nicht angekommen" und „Backend nicht erreichbar"
  unterscheiden kann. Slot-Tabelle, Beobachtungs-Portbereiche und (mit
  `--pool cpu`) die Voreinstellung der Rechenslots richten sich beim Start
  nach dem Spielraum; die Vorflugprüfung lehnt einen Spielraum unter
  `--processes` oder der `--autoscale`-Obergrenze ab, ebenso einen, der die
  zugeteilten CPUs überbuchen würde. Ohne `--max-processes` ändert sich
  nichts. Siehe [CLI-Referenz](cli.de.md#kontrollkanal).

## [0.6.2] — 2026-09-15

### Geändert

- **README aktualisiert.** Die Paketbeschreibung auf PyPI führte unter „What's
  planned next“ Features, die längst ausgeliefert sind (Priorities, Expiry,
  Burst, Supervisor, Autoscaling, Metriken, Health, Hot Reload, Agent-Spec).
  Sie beschreibt jetzt den aktuellen Funktionsumfang, den CPU-Worker-Modus,
  die Supervisor-Vorgaben und die zwei verbliebenen Roadmap-Punkte.
- **Abhängigkeiten aktualisiert (2026-09-15).** Geprüft gegen aio-pika 10.0.1
  und aiormq 7.0.0 (pamqp 4), structlog 26.1, valkey mit libvalkey 4.1,
  msgpack 1.2, prometheus-client 0.26, watchfiles 1.2; die deklarierten
  Untergrenzen bleiben unverändert, bestehende Installationen laufen weiter.
  Entwicklungswerkzeuge: mypy 2.3, ruff 0.16 (neue Regel `RUF036`, `None`
  am Ende von Typ-Unions), pytest 9.1, hypothesis 6.168. Unit-,
  Integrations-, e2e-, Property- und Contract-Suiten sind mit dem neuen
  Stand grün.

### Behoben

- **Worker-Subprozesse sterben mit ihrem Supervisor, auch bei SIGKILL.** Ein
  hart beendeter Supervisor (Test-Teardown, OOM-Killer, `docker kill`) ließ
  seine Worker als Waisen weiterlaufen; sie erneuerten den Leader-Lock des
  Schedulers per Heartbeat und blockierten jeden späteren Leader in derselben
  Datenbank. Das Ziel des Kindprozesses setzt jetzt `PR_SET_PDEATHSIG` auf
  SIGTERM (Linux), sodass der Tod des Supervisors denselben Graceful Shutdown
  auslöst wie ein geordneter Stopp; ein Kind, dessen Supervisor zwischen
  Spawn und Setzen starb, beendet sich, statt als Waise anzulaufen. Ohne
  `prctl` bleibt das Verhalten unverändert.

## [0.6.1] — 2026-09-15

### Behoben

- **`TASK_REJECTED` trägt die Task-Identität, sobald der Body dekodiert
  wurde.** Für `reason="unregistered_task"` und
  `reason="middleware_chain_factory"` enthält das Signal jetzt `task_id` und
  `task_name`; bisher war `task_id` auf beiden Wegen `None`, obwohl der
  Worker die Nachricht schon dekodiert und die Kennung geloggt hatte. Ohne
  sie ließ sich eine abgewiesene Nachricht mit `swarmq dlq inspect` nicht im
  `.XQ` wiederfinden. `body_too_large` und `undecodable` melden weiterhin
  `None`, wie dokumentiert. Gefunden von der Lasttest-Suite am Pin v0.6.0
  (dort Backlog-Eintrag B14).
- **Der Supervisor hebt die geerbte Signalmaske auf, bevor er irgendeinen
  Worker-Entry ausführt.** `ProcessManager` blockiert SIGINT/SIGTERM/SIGHUP,
  während er Kinder startet, und `spawn` gibt diese Maske weiter. swarmqs
  eigener Kind-Entry hob sie auf; ein eigener `worker_entry`, den ein
  Betreiber dem `ProcessManager` übergibt, tat das nicht und sah das SIGTERM
  seines Shutdown-Watchers nie. Ein Graceful Stop endete deshalb erst mit
  dem SIGKILL der Orchestrierung nach dem vollen Stop-Timeout. Das Ziel des
  Kindprozesses hebt die Maske jetzt selbst auf, bevor der Entry läuft.
  Gefunden von der Lasttest-Suite (Backlog-Eintrag B15).

## [0.6.0] — 2026-09-14

### Geändert (breaking)

- **`worker_id` hat eine feste Form.** In jedem Signal, Metrik-Label und
  Logkontext ist die Worker-Identität `<basis>:<slot>` unter dem Supervisor
  und `<basis>:p<pid>` im eingebetteten `start_worker()`. `<basis>` ist
  `SWARMQ_WORKER_ID`, sonst der Hostname. Bisher war es eine undurchsichtige
  Objekt-ID, die sich mit jedem Neustart änderte. Gilt für `WORKER_*`,
  `SCHEDULER_*`, `WORKER_RECYCLING` (auch `kind="compute_child"`) und die
  Labels von `swarmq_worker_uptime_seconds` und `swarmq_scheduler_leader`.
  Dashboards können den Slot lesen; die Uptime beginnt beim Recycling eines
  Slots von vorn.
- **`--metrics-port` und `--health-port` sind Basisports.** Der Worker in
  Slot `s` bindet `basis + s`; Mehrprozess-Betrieb mit festem Port wird nicht
  mehr abgelehnt. Abgelehnt wird stattdessen ein Bereich `[basis, basis + N)`
  über 65535 hinaus oder eine Überschneidung von Metrik- und Health-Bereich.
  `N` ist `--processes` oder die Obergrenze von `--autoscale`. Prometheus
  braucht ein Target je Slot; `swarmq worker --check` gibt beide Bereiche
  aus (`metrics_ports`, `health_ports`).
- **Exit-Code 78 für Konfigurationsfehler, auf jeder Ebene.** Ein
  `ConfigurationError` oder `ModuleNotFoundError` im Elternprozess von
  `swarmq worker` erzeugt den strukturierten Logsatz `worker_config_error`
  und endet mit 78 (bisher 1). Endet ein Worker-Kind mit 78 — auch bei einem
  `ImportError` beim Import des Task-Moduls — endet der Supervisor ebenfalls
  mit 78 (bisher 1). Verbindungsfehler zu Broker oder Backend bleiben bei 1.
  Eine Orchestrierung kann jetzt „nicht wieder anfassen" von „gleich
  nochmal" unterscheiden.
- **Workflow-Schritte laufen durch die Enqueue-Middleware.** Jeder Schritt,
  den der `WorkflowAdvancer` publiziert, und jeder Schritt der Anfangsfront
  in `Client.apply` durchläuft vor dem Publish `pre_enqueue` und danach
  `post_enqueue`, in derselben Kette wie `Client.schedule`, mit
  `delivery_mode="at_least_once"` und den zusätzlichen Kontextschlüsseln
  `workflow_id` und `step_path`. `pre_enqueue` läuft für alle Schritte einer
  Front, bevor der erste publiziert wird; wirft es, wird kein Schritt dieser
  Front publiziert, der Workflow als gescheitert markiert (`error.type` =
  Name der Ausnahme) und die Nachricht bestätigt. Eine `pre_enqueue`, die
  Client-Kontext voraussetzt, kann jetzt Workflow-Schritte ablehnen.
- **Das Pseudo-Ereignis `<workflow>` entfällt.** `Client.apply` löst kein
  `TASK_ENQUEUED` mit `task_name="<workflow>"` mehr aus. Jeder Schritt löst
  `TASK_ENQUEUED` mit seiner echten Identität aus, gefolgt von
  `WORKFLOW_STEP_ENQUEUED`.

### Hinzugefügt

- **`swarmq worker --configure MODULE:CALLABLE`**: Das Callable erhält die
  `SwarmQ`-App nach ihrer Konstruktion und vor dem Start des Workers; ein
  Awaitable als Rückgabe wird abgewartet. Middleware und Signal-Handler
  registrieren, ohne einen eigenen Worker-Einstieg zu schreiben. Läuft genau
  einmal je Worker-Prozess, nie im Forkserver und nie in `schedule`, `dlq`
  oder `inspect`. Ein scheiternder Hook ist ein Konfigurationsfehler
  (Exit 78). `--check` prüft die Syntax `MODULE:CALLABLE` und die
  Auffindbarkeit des Moduls, ohne es zu importieren.
- **Signal `TASK_REJECTED`** mit `queue`, `reason`, `task_id`, `task_name`,
  `error` für jede Nachricht, die der Worker ohne Diagnose-Header an den
  Dead-Letter-Exchange des Brokers gibt. `reason` ist einer von
  `body_too_large`, `undecodable`, `unregistered_task`,
  `middleware_chain_factory`, `dlq_publish_failed`; `task_id` und
  `task_name` sind `None`, wenn der Body nie dekodiert wurde. Zähler
  `swarmq_tasks_rejected_total{queue,reason}`.
  `swarmq_tasks_total{outcome="dlq"}` zählt weiterhin nur `TASK_DLQ`. Der
  [Dead-Letter-Guide](../guides/dead-letter-queues.de.md#drei-wege-in-die-xq)
  benennt die drei Wege ins `.XQ`.
- **Signal `WORKFLOW_STEP_ENQUEUED`** mit `task_id`, `task_name`, `queue`,
  `workflow_id`, `step_path` nach jedem publizierten Workflow-Schritt,
  zusätzlich zu `TASK_ENQUEUED`.
- **`SWARMQ_WORKER_ID`** (auch `SwarmQ(worker_id=...)`) setzt die Basis der
  Worker-Identität; Standard ist der Hostname.
- **Slot-Tabelle im Supervisor.** Slots sind `0..N-1`. Der Ersatz für einen
  gestorbenen, hängenden oder recycelten Worker erhält denselben Slot und
  damit dieselben Ports; ein Scale-Up nimmt den kleinsten freien Slot, ein
  Scale-Down entfernt den jüngsten Worker nach Startzeit. Ein Slot mit
  erschöpftem Neustart-Budget wird gesperrt und nie nachgefüllt. Während
  eines rollierenden Ersatzes teilen Alt- und Neu-Prozess den Slot; der
  Autoscaler zählt ihn einmal. Der Ersatz bindet seinen Port mit
  Wiederholung; das Budget deckt das SIGKILL eines hängenden und die
  Portfreigabe eines recycelnden Vorgängers.

### Behoben

- **Verlorene Zustands-Updates von Workflows bei parallelen Zweigen.** Jede
  Schreibung des Workflow-Zustands durch den Advancer ist ein
  Compare-and-Set gegen die gelesene Version (Feld `version`; Dokumente aus
  0.5.x gelten als 0). Bei einem Konflikt liest der Advancer neu, übernimmt
  seine lokalen Änderungen (`step_tasks` additiv; `status`,
  `final_result_task_id` und `error` bei lokalem Terminalstatus) und
  schreibt erneut, bis zu 25 Runden; ein frischer Terminalstatus beendet
  den Advance ohne Schreibung. Oberhalb der Grenze wird die Nachricht neu
  zugestellt. Der bisher als `xfail` geführte Test zum verlorenen
  Schritt-Marker ist jetzt ein Regressionstest.
- **Fehlermarkierung unterscheidet Fehlertypen.** Scheitert die
  Fehlermarkierung eines Workflows an einem `BackendConnectionError`, wird
  die Nachricht (at-least-once) neu zugestellt statt bestätigt; jeder andere
  Fehler bestätigt und loggt mit Workflow-Kennung.
- **Konfigurationsfehler im Kind sind strukturierte Logsätze.** Der
  Fehlertext erscheint im konfigurierten Format (`human` oder `json`), nicht
  als rohe stderr-Zeile; der Supervisor loggt `worker_config_error`. Ein
  Kind, das mit 75 endet (Recycling ohne Bestätigung), wird als
  `worker_recycle_noack` geloggt und im Budget neu gestartet.
- **Der Log-Formatter druckt keine Tracebacks mehr.** Scheitert die
  Formatierung eines Records (etwa an einem werfenden `__repr__` in
  `record.args`), gibt der Formatter eine Zeile im konfigurierten Format mit
  `event="log_record_unformattable"`, Logger-Name, Level und Fehlertext aus.
- **Keine `RuntimeWarning` je Rechenkind.** Ein per
  `python -m paket.modul` gestarteter Worker im CPU-Modus importierte sein
  Hauptmodul in jedem Rechenkind erneut, jeweils mit einer `runpy`-Warnung.
  Der Forkserver-Preload registriert es jetzt einmal als `__main__`; der
  Kindstart bleibt bei etwa 5 ms.

## [0.5.1] — 2026-09-06

### Behoben

- Ein Fehler beim *Aufruf* von `run()` eines Tasks — falsche Argumente
  (`TypeError`) oder ein `run()`, das keine Coroutine ist — ist jetzt ein
  gewöhnlicher Task-Fehler und folgt dem Retry-/DLQ-Pfad. Bisher entkam die
  Exception dem Nachrichten-Handler, die Zustellung blieb unbestätigt, bis der
  Kanal geschlossen wurde.
- CPU-Modus: Der Forkserver-Preload enthält jetzt das Hauptmodul des
  Loop-Prozesses und die Laufzeit des Rechenkinds. Ohne sie importierte jedes
  frische Kind beides neu (etwa 60 ms je Task); der Kindstart liegt wieder bei
  etwa 5 ms wie in der Machbarkeitsstudie gemessen. `SlotPool.recent_spawn_ms`
  zeichnet Spawn- und Handshake-Zeiten zur Diagnose auf.

## [0.5.0] — 2026-09-03

### Geändert (breaking)

- **Der Supervisor ist Standard.** `swarmq worker` startet immer einen
  `ProcessManager` mit dem Worker als Subprozess, auch bei `--processes 1`.
  Zwei Prozesse statt einem; `SIGHUP` bedeutet jetzt Reload (wie bisher mit
  `--processes`); ein abstürzender Worker wird innerhalb des Budgets (5 pro
  Stunde) intern neu gestartet, bevor der Prozess endet — Alarme daher auf das
  Log-Ereignis `worker_died` statt auf Container-Neustarts. `--burst` endet
  weiterhin mit 0, sobald alle Worker leergelaufen sind.
- **Startprüfung für wirkungslose `FromDishka`-Annotationen.** Ein Parameter
  wie `db: FromDishka[Db] | None = None` hat nie injiziert und blieb still
  `None`; der Worker verweigert jetzt den Start mit einem `ConfigurationError`,
  der Task und Parameter nennt — in jedem Pool-Modus. `db: FromDishka[Db]` ohne
  `| None` und ohne Default deklarieren.
- **Konfigurationsfehler beenden den Lauf nach einem Versuch.** Ein
  Worker-Subprozess, der an seinen Startprüfungen scheitert, endet mit Code 78;
  der Supervisor startet ihn nicht neu und endet mit 1, statt das
  Neustart-Budget zu verbrennen.

### Hinzugefügt

- **CPU-Modus** (`--pool cpu`, `SWARMQ_POOL`): Task-Körper laufen in
  Rechenkindern aus einem vorgewärmten `forkserver`; der Loop-Prozess rechnet
  nie. `hard_timeout` und Cancel töten die Prozessgruppe des Kindes;
  `--concurrency` bedeutet Rechenslots und ist voreingestellt auf die dem
  Worker zugeteilten CPUs (Affinität und cgroup-Quote) geteilt durch
  `--processes`. Frisches Kind je Task ohne DI, wiederverwendetes Kind mit
  RSS-/Taskzahl-Recycling für `FromDishka`-Tasks. Siehe
  [CPU-Modus](../guides/cpu-mode.md).
- **Lebenszeichen zum Supervisor**: Ein Worker, dessen Event-Loop seinen
  Zeitstempel `SWARMQ_HEARTBEAT_TIMEOUT_S` lang (Standard 60 s) nicht mehr
  schreibt, wird ersetzt (SIGTERM, dann SIGKILL), mit eigenem Budget von
  2 × Delivery-Limit pro Stunde. Eine Nachricht, die Worker zum Hängen bringt,
  wird ab dem vorletzten Zustellversuch (`x-delivery-count`) vom Worker in
  die DLQ geführt — mit Fehlerergebnis, Workflow-Fail, Unique-Freigabe und
  `TASK_DLQ`, nicht stumm vom Broker.
- **Selbst-Recycling des IO-Workers** (`SWARMQ_RECYCLE_RSS_MB`,
  `SWARMQ_RECYCLE_MAX_TASKS`, Opt-in): Nach einem Task bittet der Worker den
  Supervisor um Ersatz, wartet auf die Bestätigung und drainiert ohne
  erzwungenen Abbruch.
- Signal `WORKER_RECYCLING` und Metrik `swarmq_recyclings_total{kind,reason}`.
- Neue Einstellungen: `SWARMQ_HEARTBEAT_INTERVAL_S`, `SWARMQ_HEARTBEAT_TIMEOUT_S`,
  `SWARMQ_CONSUMER_TIMEOUT_S` (Startwarnung, wenn ein `hard_timeout` darüber
  liegt), `SWARMQ_MALLOC_TRIM_THRESHOLD`, `SWARMQ_MALLOC_ARENA_MAX`,
  `SWARMQ_COMPUTE_THREAD_LIMIT`. `/ready` meldet im CPU-Modus den Forkserver;
  der Start warnt, wenn `queue_type` nicht `quorum` ist (kein Delivery-Limit).

## [0.4.1] — 2026-08-30

### Hinzugefügt

- **`SWARMQ_DELIVERY_LIMIT` / `SwarmQConfig.delivery_limit`** (Default `20`,
  unverändertes Verhalten) — das Zustellbudget, mit dem Quorum-Queues
  deklariert werden (`x-delivery-limit`). Ist eine Nachricht so oft zugestellt
  worden, wirft der Broker sie selbst in die Dead-Letter-Queue; das Budget ist
  das Sicherheitsnetz gegen einen Task, der den Prozess zuverlässig abschießt
  (SIGSEGV/OOM) und deshalb ewig neu zugestellt würde, ohne je `max_retries`
  zu erreichen.

  Der Default deckt drei bis vier fachliche Wiederholungen je Zustellung plus
  Puffer für Broker-Schluckauf. Dieser Puffer ist mit dem Publish-Deckel aus
  0.3.0 schmaler geworden: Bricht ein Publish im Worker an seiner Frist ab,
  wird die bearbeitete Nachricht neu zugestellt statt bestätigt — eine längere
  Broker-Störung verbraucht damit Zustellungen, die sie vorher nicht
  verbraucht hat. Höher setzen, wer längere Störungen aussitzen muss; niedriger,
  wer ein engeres Zustellfenster erzwingen will. Klassische Queues ignorieren
  die Einstellung, sie kennen das Argument nicht.

## [0.4.0] — 2026-08-30

### Geändert (breaking)

- **Eine `group`, deren Glied selbst eine `chain`, `group` oder ein `chord`
  ist, liefert keine gekürzte Ergebnisliste mehr.** Seit 0.2.0 fiel ein
  solches Glied ohne jede Fehlermeldung aus der Liste — `group(a, chain(b,
  c))` gab eine einelementige Liste zurück, während `handle.children()`
  gleichzeitig alle Schritte als abgeschlossen meldete. Derselbe Code baut
  die Eingabe eines Chord-Bodys, ein Chord über einem solchen Header hat
  also auf einer gekürzten Liste gerechnet, statt eine zurückzugeben.

  **Betroffene Versionen: 0.2.0 und 0.3.0.** Eine gekürzte Liste ist im
  Nachhinein von einer vollständigen nicht zu unterscheiden — die
  gespeicherten Ergebnisse geben das nicht her. Beantwortbar ist nur die
  Frage nach der Workflow-*Definition*: **betroffen ist jeder Lauf, dessen
  Topologie eine `group` oder einen Chord-Header mit einem Glied enthält,
  das selbst `chain`, `group` oder `chord` ist.** Wer solche Workflows
  einsetzt, muss davon ausgehen, dass frühere Ergebnisse falsch waren — und
  alles, was daraus gerechnet wurde.

- **Eine unvollständige Gliederliste ist jetzt ein Fehler, für jede Gruppe.**
  Wo die Zusammenstellung ein fehlendes Glied bisher wortlos übersprang,
  wirft sie nun `WorkflowValidationError` mit Gruppenpfad und beiden Zahlen.
  Der Guard sitzt am gemeinsamen Fan-in **aller** Gruppen und Chord-Header,
  auch der flachen — ein aus anderem Grund verlorener Schritt-Marker (etwa
  im publish-then-mark-Fenster) scheitert damit laut, wo er vorher ein
  Ergebnis lieferte. Genau das ist die Absicht: Ein falsches Ergebnis, das
  man nicht erkennen kann, ist teurer als ein sichtbarer Fehler.

- **Verschachtelte Workflows vor dem Upgrade leerfahren.** Ein Workflow, der
  über das Upgrade hinweg läuft, trifft auf den neuen Guard und scheitert,
  statt sein (falsches) gekürztes Ergebnis zu liefern. Das gilt je Fleet,
  nicht je Prozess: Solange nicht jeder Worker die neue Version fährt, kann
  ein alter Worker weiterhin still kürzen.


## [0.3.0] — 2026-08-30

**Nie auf PyPI veröffentlicht.** Getaggt, trug aber
noch die stillschweigend gekürzte `group`-Ergebnisliste, die 0.4.0 behebt —
die Veröffentlichung hätte einen wissentlich falsch rechnenden Stand
ausgeliefert. Der Eintrag bleibt fürs Protokoll; von 0.2.0 direkt auf 0.4.0
gehen und beide Abschnitte lesen.

### Geändert (breaking)

- **`swarmq worker` verweigert Mehrprozess-Betrieb mit festem
  Beobachtungs-Port.** Werden mehr als ein Worker-Prozess angefordert
  (`--processes N` mit N > 1 oder `--autoscale MIN,MAX` mit MAX > 1) und ist
  zugleich ein Metrik- oder Health-Port wirksam, scheitert jetzt der Start,
  statt still unterbesetzt weiterzulaufen. Jedes Kind band denselben Port,
  sodass nur das erste überlebte und die übrigen ihr Restart-Budget
  aufbrauchten. Geprüft wird der aufgelöste Wert: `SWARMQ_METRICS_PORT` und
  `SWARMQ_HEALTH_PORT` werden genauso abgelehnt wie die Optionen, und
  `swarmq worker --check` meldet denselben Konflikt. Entweder den festen
  Port im Mehrprozess-Betrieb weglassen oder einen einzelnen Prozess damit
  fahren — einen Port je Kindprozess gibt es noch nicht.
- **Neue Ausnahme `PublishIndeterminate`** (Unterklasse von
  `BrokerConnectionError`, bestehende Handler bleiben unberührt) für einen
  Publish, dessen Frist beim Warten auf die Broker-Bestätigung riss. In genau
  diesem Fall bleibt eine `unique_for`-Dedup-Sperre **stehen**, statt
  freigegeben zu werden: Ein Wiederholversuch wird abgewiesen, bis die Sperre
  abläuft, statt den Task ein zweites Mal auszuführen.
- **`Publisher` hat `set_publish_deadline` bekommen.** Eigene Broker, die
  `swarmq.contracts.Publisher` implementieren, sollten die Methode
  ergänzen; der Worker ruft sie über eine Schutzabfrage, eine
  Implementierung ohne sie fährt also weiterhin sauber herunter, bekommt
  aber die an das Herunterfahren gekoppelte Publish-Frist nicht.

### Hinzugefügt

- **`SWARMQ_PUBLISH_TIMEOUT_S` / `SwarmQConfig.publish_timeout_s`**
  (Default `120.0`) — Obergrenze für einen kompletten Publish inklusive
  interner Wiederholversuche. Ab Werk aktiv. Wird sie überschritten,
  scheitert der Publish mit `BrokerConnectionError` und dem ausdrücklichen
  Vorbehalt, dass die Zustellung unbestimmt ist, statt endlos auf eine
  Bestätigung zu warten, die der Transport nie liefert. Beim Herunterfahren
  gilt die kürzere aus diesem Wert und der verbleibenden Gnadenfrist.

### Behoben

- **Ein Publish konnte auf einem still toten Transport unbegrenzt hängen.**
  Es fliegt keine Ausnahme und der Kanal gilt weiter als offen, der
  bestehende Wiederholpfad griff also nie. In einem Lasttestlauf wurden aus
  einer fünfsekündigen Broker-Störung 62 Minuten Stillstand, die sich per
  SIGTERM nicht beenden ließen.
- **Jede JSON-Logzeile trug ihren Inhalt zweimal.** Die formatter-internen
  Arbeitsschlüssel (`_record`, `_from_structlog`) wurden gerendert statt
  entfernt — doppelte Logmenge und von Hand kaum lesbar. Die Textausgabe
  trug sie ebenfalls und verliert sie mit.

## [0.2.0] — 2026-08-27

### Geändert (Breaking — 0.2.0)

- **`ValkeyBackend`-Legacy-Methodenaliase entfernt.** Es gelten die
  Vertragsnamen — z. B. `setnx_unique` → `claim_unique`,
  `get_results_batch` → `get_results`, `publish_notification` →
  `notify_result`, `subscribe_and_wait` →
  `wait_for_result`/`wait_for_workflow`; Schedule-/Schedfire-Primitive
  liegen im Store `backend.schedules` (`zrangebyscore_schedules` →
  `schedules.poll_due`, …). Semantik, Key-Layout und Lua-Verträge sind
  unverändert (Golden-Fixtures). Migrationstabelle im
  0.2.0-Bruch-Inventar.
- **`RabbitMQBroker`-Admin-/Verb-Aliase entfernt.** `get_queue_depth` →
  `queue_depth`, `get_dlq_depth` → `dead_letter_depth`,
  `list_dead_letter_queue` → `list_dead_letters`, `inspect_dead_letter_message` →
  `inspect_dead_letter`, `retry_dead_letter_message` → `retry_dead_letter`,
  `retry_all_dead_letter_messages` → `retry_all_dead_letters`,
  `purge_dead_letter_queue` → `purge_dead_letters`; die Zustellungs-Verben
  heißen `redeliver`, `route_via_dlx`, `retry_later` und `dead_letter`
  (die letzten beiden bestätigen das Original selbst — separates `ack`
  entfällt).
- **Konstruktoren sind gegen die Verträge** in `swarmq.contracts`
  typisiert (`Worker`, `Client`, `SwarmQ`). `RabbitMQBroker`/
  `ValkeyBackend` implementieren sie; bestehender Code läuft unverändert.
- **Interne Modulstruktur:** `worker`, `client`, `scheduler` und
  `process_manager` sind jetzt Pakete; Klassen-Re-Exports wie
  `swarmq.locking.LockManager`, `swarmq.limits.RateLimiter` und
  `swarmq.workflow_orchestration.ChordManager` liegen kanonisch unter
  `swarmq.backend.valkey.*` (nicht Teil der Public-Oberfläche; hier
  gelistet, weil die CLI-Surface-Whitelist sie pinnt).

### Entfernt (Breaking — 0.2.0)

- **`ValkeyBackend.create_pubsub()`.** Stattdessen `wait_for_result` /
  `watch_progress` / `wait_for_workflow`; die Kanalnamen (`sq:notify:…`)
  bleiben für eigene Clients stabil.

### Behoben (0.2.0)

- **Band-Erhalt beim Retry.** Retries, Rate-Limit- und Lock-
  Rückstellungen zielen jetzt auf das Verzögerungs-/Dead-Letter-Ziel der
  **Zustellungs-Queue** — ein Retry aus `tasks.high` bleibt in
  `tasks.high`, auch wenn der Nachrichten-Body die Basis-Queue nennt
  (bisher ging das Band bei Fremd-Producer-Nachrichten verloren).
  Abgesichert durch `tests/e2e/test_retry_band.py`. Dies ist die einzige
  Verhaltensänderung der 0.2.0-Restrukturierung.

### Intern (0.2.0)

- **Vertrags-Rückgrat.** Explizite Protokolle in `swarmq.contracts`
  (Publisher/Consumer/Broker, BrokerAdmin, ResultBackend, LockStore,
  RateLimitStore, ChordStore, ScheduleStore) mit Batch-Publish-
  Andockstellen für den geplanten Bulk-Fast-Path; ein In-Memory-Stub
  in der Test-Suite implementiert dieselben Verträge, und eine
  Contract-Test-Suite läuft identisch gegen Stub, RabbitMQ und Valkey.
- **Paket-Zerlegung.** Die vier früheren Monolith-Module sind Pakete
  (`worker/`, `client/`, `scheduler/`, `process_manager/`), mit
  CI-erzwungenen Struktur-Guards (Modulgröße, Import-Zyklen,
  Import-Reinheit der Kernpakete).
- **Projektweites mypy strict.** Beide Legacy-Override-Blöcke sind
  weg; jede verbleibende `# type: ignore` trägt einen Fehlercode.

### Behoben (0.2.0, nach dem Inventar)

- **Fehlerergebnisse abgelaufener Tasks werden jetzt wirklich
  gespeichert.** Das Expiry-Gate übergab dem Result-Store ein dict
  statt kodierter Bytes; der SET scheiterte gegen echtes Valkey still
  und `get_result()` eines abgelaufenen Tasks hing bis zum Timeout
  statt `TaskError("expired")` zu liefern.

### Rollout (0.2.0)

- Gleiche Version im gesamten Worker-Fleet ausrollen.
- Keine Valkey-Migration: Key-Layout, Lua-Verträge, Channels, Header
  und Queue-Argumente sind unverändert (Golden-Fixtures).
- In-flight-0.1.2-Nachrichten bleiben gültig; ein 0.2.0-Worker
  konsumiert von 0.1.2 deklarierte Queues ohne `PRECONDITION_FAILED`.
- Rollback ist ein Redeploy von 0.1.2.

## [0.1.2] — 2026-06-07

- `py.typed`-Marker ausgeliefert, damit Type-Checker das Paket als typisiert sehen.

## [0.1.1] — 2026-06-07

- sdist per Whitelist auf `src/` getrimmt; `libvalkey` ins `fast`-Extra; Readme deklariert.

## [0.1.0] — 2026-06-07

Erstes PyPI-Release. Alles Folgende war darin enthalten.

### Performance

- **`import swarmq.cli` lädt `valkey` und `aio_pika` nicht mehr eager.**
  Der Package-Init in `swarmq/__init__.py` und die CLI-Fassade
  auflösen Public-Surfaces (`SwarmQ`, `ValkeyBackend`,
  `RabbitMQBroker`, `Metrics`, `HealthChecker`, `Middleware`,
  `Signal`, `Task` u.a.) jetzt lazy via PEP-562 `__getattr__`. Die
  Identität bleibt beim ersten Zugriff erhalten — `from swarmq
  import ValkeyBackend` liefert weiterhin das kanonische Klassen-
  Objekt, keine API-Änderung für Konsumenten. Median-`importtime`-
  Cumulative-Latency für `import swarmq.cli` sinkt von ~188 ms auf
  ~100 ms (Best Case ~78 ms). Betrifft `swarmq --help`,
  `swarmq worker --check` und jede Agent-Introspektion, die den
  CLI-Namespace ohne die schweren Backends importiert.

### Verhaltensänderungen

- **`--format`-Default ist jetzt TTY-bewusst (CLI-04).** Für
  Subcommands mit `--format`-Flag (`dlq list`, `dlq inspect`,
  `dlq retry`, `dlq purge`, `schedule list`, `schedule add`,
  `schedule remove`, `schedule status`, `inspect`) ist der
  Standard-Output jetzt `table` bei einem TTY und `json` bei
  einer Pipe/Umleitung. Vorher war der Default stets `table`.

  **Skripte, die auf dem alten Piped-Table-Default aufbauten**
  können das Altverhalten ohne Code-Änderung mit
  `SWARMQ_DEFAULT_FORMAT=table` wiederherstellen. Explizite
  `--format=table`-Flags funktionieren unverändert. Ein
  explizites `--format=…` auf der Kommandozeile schlägt die
  Env-Var immer.

  Ungültige `SWARMQ_DEFAULT_FORMAT`-Werte (Tippfehler, unbekanntes
  Format, leerer String) werden stillschweigend ignoriert — der
  CLI fällt auf den TTY-basierten Default zurück statt zu
  fehlen. Siehe [CLI-Output](configuration.md#cli-output).

### Hinzugefügt — Phase 5 Welle 2 (Dev Experience)

- **R5.3 Hot Reload** (`pip install swarmq[reload]`). `--reload`
  startet einen `watchfiles`-gestützten Daemon-Thread, der bei
  `.py`-Änderungen unter dem Task-Modul-Verzeichnis `SIGHUP` an
  den Supervisor feuert, was die bestehende R4.6-Reload-Pipeline
  (Drain → execvpe) triggert. Neue Flags: `--reload`, `--watch-dir
  PATH` (wiederholbar). Filter ignoriert `__pycache__`, `*.pyc`,
  Editor-Noise (`4913`, `*.swp`, `*.bak`, `*.tmp`). Loggt
  `hot_reload_enabled`-WARNING bei jedem Start — Developer-Only-
  Feature. Siehe [guides/hot-reload](../guides/hot-reload.md).

### Hinzugefügt — Phase 5 Welle 1 (Operations)

- **R5.1 Prometheus-Metriken** (`pip install swarmq[metrics]`).
  Sieben Metriken mit `task_name`/`queue`/`outcome`-Labels:
  `swarmq_tasks_total`, `swarmq_task_duration_seconds`,
  `swarmq_tasks_active`, `swarmq_queue_depth`,
  `swarmq_worker_uptime_seconds`, `swarmq_retries_total`,
  `swarmq_dlq_messages`. Stdlib-only HTTP-`/metrics`-Endpoint.
- **R5.2 Health- und Ready-Endpoints.** `GET /health` (Liveness,
  immer 200 solange Event-Loop lebt), `GET /ready` (200, wenn
  Broker + Backend erreichbar, 503 mit sanitisierter JSON-Body
  andernfalls). `HealthChecker`, `HealthServer`, `serve_health`
  als öffentliche API.
- **Auto-Wire.** `SwarmQConfig.metrics_port` / `health_port` (oder
  `--metrics-port` / `--health-port` auf `swarmq worker`) aktivieren
  beide Server + Signal-basierte Instrumentierung ohne Python-
  Entrypoint-Boilerplate.

### Hinzugefügt — Phase 4 (Operator-Features)

- **R4.3 Task-Priorities.** `TaskInfo(priority="high"|"normal"|"low")`
  routet zu `{queue}.{priority}`-Suffixen. Worker deklariert das
  Triplet automatisch, wenn `priorities=True` (oder `--priorities`).
  Warnt beim Start, wenn Tasks Nicht-Normal-Priority deklarieren,
  die Worker-Config aber Priorities deaktiviert hat.
- **R4.4 Task-Expiry.** `TaskInfo(max_age=seconds)` + der
  `x-swarmq-enqueued-at`-Header wird beim Publish gestempelt
  (Client + Scheduler). Worker routet abgelaufene Nachrichten
  vor Rate-Limit-/Lock-/pre_execute-Stages mit `TASK_EXPIRED`-
  Signal + `TaskError("task expired: …")`-Ergebnis in die DLQ.
- **R4.5 Burst-Mode.** `swarmq worker --burst` exited mit 0,
  sobald alle Queues `--burst-idle-timeout` Sekunden Idle waren.
  `SwarmQConfig.burst_mode` / `burst_idle_timeout` +
  `SWARMQ_BURST_MODE` / `SWARMQ_BURST_IDLE_TIMEOUT`-Env-Vars.
- **R4.6 ProcessManager.** `swarmq worker --processes N` (Alias
  `-p`) lässt N Worker-Subprozesse unter einem Supervisor laufen,
  der gecrashte Worker automatisch neu startet (Rolling-Window-
  Limit: 5 Restarts pro Stunde pro Worker). `spawn`-Context hält
  aio-pika- + Valkey-Verbindungen subprozess-scoped. SIGHUP
  triggert `os.execvp()`-basierten Reload nach graceful Drain;
  SIGTERM / SIGINT triggern denselben 3-Phasen-Shutdown
  (kooperatives Event → SIGTERM → SIGKILL) wie ein Single-
  Process-Worker.
- **R4.7 Autoscaling.** `swarmq worker --autoscale=MIN,MAX`
  skaliert die Worker-Anzahl auf Basis der Queue-Tiefe. Scale-Up,
  wenn Pending-Tasks `50 * active` übersteigen; Scale-Down nach
  60s sustained Idle; 30s Cooldown zwischen Entscheidungen.
  Queue-Tiefe per neuer `RabbitMQBroker.get_queue_depth()`
  (passives `declare(..., passive=True)` auf Main + `.DQ`).

### Hinzugefügt — Phase 3 (Komfort-Features)

- **R3.5 Unique Tasks.** `TaskInfo(unique_for=seconds,
  unique_until="start"|"completion")` dedupt auf kanonisch
  kodierten Args. Owner-Check via Lua für TTL-Expiry-Race-Schutz.
  Sowohl `Client.schedule`- als auch `Client.schedule_many`-Pfade.
- **R3.6 Task-Cancellation.** `Client.cancel(task_id)` setzt ein
  Valkey-Flag, das der Worker pre-pickup und während der
  Ausführung checkt. Dediziertes `TASK_CANCELLED`-Signal. Cancels
  propagieren als `TaskCancelledError` via `get_result`.
- **R3.7 Progress Tracking.** `Task.update_progress(current,
  total, message)` auf 500ms pro Instanz gedrosselt mit
  Force-Flush bei `current == total`. `Client.get_progress` +
  `Client.watch_progress` AsyncIterator streamt Updates via
  Pub/Sub mit HASH-Fallback bei Disconnect. ETA via linearer
  Extrapolation.
- **R3.10 DLQ-CLI.** `swarmq dlq list|inspect|retry|purge` für
  Operator-Recovery. Drain-and-Republish-Scan-Pattern, Exit-Codes
  0 (Success), 2 (Config), 3 (task_id nicht gefunden).
- **Embedded Scheduler.** Cron + Delayed (`delay=` / `eta=`) +
  Recurring mit Leader-Election-Failover. Graceful Handover ohne
  TTL-Wartezeit.
- **Full CLI.** `swarmq worker`, `swarmq schedule`, `swarmq inspect`
  — kein Python-Entrypoint für typische Deployments nötig.

### Hinzugefügt — E2E-Hardening (separate Initiative)

- **Chaos-Suite** (`tests/chaos/`): toxiproxy + Container-Restart-
  Abdeckung für Worker-SIGKILL, Broker/Backend-Restart, Netzwerk-
  Chaos (Latency/Bandwidth/Packet-Loss/Full-Down), Clock-Skew,
  Message Corruption, Byzantine Workers.
- **Property-based Tests** (`tests/property/`): Hypothesis gegen
  Dedup-Hash, Progress, Serialization, Backoff, Rate-Limiter.
- **Fuzz** (`tests/fuzz/`): Byte-Fuzz auf Message-Parser +
  AMQP-Header- / Pub/Sub-External-Publisher-Fuzz.
- **Stress** (`tests/stress/`): 10-Worker × 1000-Task exact-once,
  50-Parallel-Cancel-Winner, 200-Parallel-Progress-Throttle.
- **Long-Soak** (`tests/soak/`): 1h-Opt-in-Leak-Detection-Harness
  mit psutil-RSS- / FD- / Valkey-Conn- / Rabbit-Channel-Zählern.
- **Drei-Tier-CI** (GitHub Actions): pre-merge ≤ 5 min, nightly
  Chaos+Stress+Fuzz ≤ 30 min, weekly Mutation+Soak ≤ 4h.

### Reliability-Hardening (ce-review Follow-ups)

- **RabbitMQBroker** Channel-Recreate nach Connection-Reset;
  `_publish_with_retry`-Bounded-Retry-Wrapper erhält asyncio-
  Task-Level-Cancellation.
- **Quorum-Queue `x-delivery-limit=20`** auf Main-Queue — pathologische
  SIGSEGV-Loops terminieren jetzt in der DLQ statt in endloser
  Redelivery.
- **ValkeyBackend** Retry-Policy + `health_check_interval` auf dem
  darunterliegenden valkey-py-Client.
- **Client.watch_progress** re-subscribed bei Pub/Sub-Disconnect.
  valkey-py re-subscribed nicht automatisch.
- **Task.update_progress** asyncio.Lock um den 500ms-Throttle-
  Read-Compare-Write — raste zuvor unter `asyncio.gather`.
- **DLQ-Drain-Republish** routet durch `_publish_with_retry` —
  transiente Channel-Fehler lassen gedrainte Nachrichten nicht
  mehr stumm fallen.
- **HTTP-Oberflächen** (`/metrics`, `/health`, `/ready`) durch
  `asyncio.timeout(5.0)` begrenzt, abdeckend Request-Line +
  Header-Drain + 200-Line-Cap. Binäre Request-Lines geben 400
  zurück, keinen Log-Spam. Default-Bind `127.0.0.1`. `/ready`-
  Error-Body sanitisiert, um Host:Port-Leak zu verhindern.

## [0.x] — frühere Phasen

Phase 1 (minimaler Roundtrip) + Phase 2 (Retries, DLQ, Logging,
Graceful Shutdown) + Phase 3 Core-Komfort-Features (Middleware,
Signals, Rate-Limit, Locking) landeten im selben Branch wie die
Punkte oben. Einzelne Commit-History in git.
