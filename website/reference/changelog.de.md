# Changelog

Alle nennenswerten Änderungen an SwarmQ. Dieses Projekt folgt
[Keep a Changelog](https://keepachangelog.com/) und
[Semantic Versioning](https://semver.org/).

## [Unreleased]

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
  fehlen. Siehe [Plan 2026-04-23-001 Unit 5](…).

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
