# CLI-Referenz

Das `swarmq`-Binary wird als Console-Script installiert, wenn du
`pip install swarmq` aufrufst. Jedes Subcommand liest per Default
aus `SWARMQ_*`-Environment-Variablen; explizite Flags gewinnen.
`swarmq worker` löst seine Einstellungen einmal im Supervisor auf
(Flag, dann Env-Var, dann Default) und gibt die aufgelösten Werte an
seine Worker-Subprozesse und Rechenkinder weiter; die lesen das
Environment nicht noch einmal, ein Flag gewinnt also in jedem Prozess.
Das gilt für jede Worker-Option mit Env-Var-Gegenstück (Queues,
Concurrency, Pool, Burst, Prioritäten, Ports, Logging).
`swarmq worker --check` löst genauso auf und prüft genau die Werte, die
die Worker bekämen. Eine eingebettete `SwarmQ(...)`-App ist davon nicht
betroffen: Dort gelten zuerst die Keyword-Argumente von
`SwarmQ`/`SwarmQConfig`, und `SWARMQ_*`-Variablen überschreiben sie.

## Synopsis

```text
swarmq [--version]
swarmq worker   --module DOTTED.PATH [options]
swarmq schedule (list | add | remove | status) [options]
swarmq dlq      (list | inspect | retry | purge) [options]
swarmq inspect  --module DOTTED.PATH [--app-dir DIR] [--format FORMAT]
swarmq check    [--format FORMAT]
```

## Exit-Codes

| Code | Bedeutung |
|---|---|
| `0` | Erfolg oder Graceful Shutdown |
| `1` | Laufzeitfehler: Broker oder Backend unerreichbar, `swarmq dlq`/`schedule` zur Laufzeit gescheitert. Später erneut versuchen ist die richtige Antwort. Außerdem jeder Konfigurationsfehler von `swarmq schedule`, `swarmq dlq` und `swarmq inspect` (fehlende `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL`, ungültiges JSON in `--args`/`--kwargs`, ungültiges `--cron`/`--at`, `inspect --module` nicht gefunden oder ohne registrierte Tasks) und jeder Fehler von `swarmq worker --check`, auch argparse-Fehler. Ausnahme: Ein ungültiger `SWARMQ_NAMESPACE` beendet jeden Befehl mit `78` (siehe unten). |
| `2` | Ungültige Argumente (argparse-Usage-Fehler), auch `swarmq dlq retry` mit `task_id` und `--all` zugleich oder mit keinem von beiden (Meldung auf stderr). Nicht unter `swarmq worker --check`: Dort kommen argparse-Fehler als JSON mit Exit `1`. |
| `3` | `swarmq dlq inspect` / `swarmq dlq retry <task_id>` fand keinen passenden DLQ-Eintrag |
| `78` | Konfigurationsfehler von `swarmq worker` (`EX_CONFIG`): `ConfigurationError`, Task-Modul nicht importierbar (jeder `ImportError`, einschließlich `ModuleNotFoundError`, oder `SyntaxError` beim Import des Moduls oder seines Paket-`__init__`; andere Ausnahmen auf Modulebene bleiben `1`, ebenso ein `ImportError`, der erst zur Laufzeit des Workers auftritt), `--configure`-Hook gescheitert (auch ein `ImportError` oder `SyntaxError` im Hook-Modul), Portbereich ungültig, ungültige Logging-Einstellungen oder Logging-Konfigurationsdatei (`LoggingConfigError`). Kommt aus dem Elternprozess und aus dem Supervisor, wenn ein Worker-Kind mit `78` endet (nicht unter `--reload`, siehe dort). Ein erneuter Versuch ohne Änderung ist sinnlos. `swarmq check`: Ein Worker würde in dieser Umgebung den Start verweigern (verdrängende Valkey-`maxmemory-policy` unter `SWARMQ_VALKEY_EVICTION_CHECK=strict` oder ein `SWARMQ_*`-Wert, den der Worker ablehnt, geprüft vor dem Verbindungsaufbau). `swarmq schedule`, `dlq`, `inspect` und `check`: ein ungültiger `SWARMQ_NAMESPACE`, geprüft vor jeder Verbindung. |
| `130` | Durch Strg-C unterbrochen (`KeyboardInterrupt`, 128 + `SIGINT`), außerhalb der Behandlung durch den Befehl selbst, etwa während Module importiert werden. Ein laufender Befehl oder Worker behandelt Strg-C als Graceful Shutdown und endet mit `0`. |

`78` folgt `sysexits.h`. Jeder Konfigurationsfehler, ob er im
Elternprozess oder in einem Worker-Subprozess auftritt, beendet den
gesamten Lauf nach einem einzigen Versuch mit `78` und schreibt einen
strukturierten Logsatz `worker_config_error` im konfigurierten
`--log-format`. Verbindungsfehler bleiben bei `1`, weil dort „gleich
nochmal" richtig ist. Unter systemd `RestartPreventExitStatus=78`
setzen, damit ein kaputtes Deployment nicht in der Schleife neu
startet; in Kubernetes auf den Exit-Code statt auf Neustarts alarmieren.
Unter `--reload` beendet Exit `78` eines Worker-Kindes den Lauf nicht:
Der Supervisor loggt `worker_config_error` und startet die Worker nach
der nächsten Dateiänderung neu.
Exit `75` (`EX_TEMPFAIL`) ist intern: Ein Worker-Kind, das Recycling
angefragt und keine Bestätigung bekommen hat, loggt
`worker_recycle_ack_timeout` und endet damit, der Supervisor loggt
`worker_recycle_noack` und startet es im Budget neu;
den Aufrufer erreicht dieser Code nie. Auch Exit `71` (`EX_OSERR`) ist
intern: Ein CPU-Worker, dessen still neu gestarteter Forkserver verworfen
wurde, hört auf zu konsumieren, loggt `compute_fork_source_rejected` und
endet damit; der Supervisor loggt `worker_fork_source_rejected` und
startet ihn im Budget mit frischem Forkserver neu.

Hängt nach dem letzten Leeren noch ein aufgegebener Log-Thread, endet
der `swarmq`-Befehl per `os._exit` mit dem Exit-Code, den er sonst gehabt
hätte (ein Traceback oder die Meldung eines `SystemExit` geht vorher auf
stderr, sofern stderr nicht blockiert), statt mit `Fatal Python error`
(Exit `134`) abzubrechen oder zu hängen. Siehe
[Logging](logging.md#begrenztes-leeren).

Für `swarmq worker` fügt Level `DEBUG` (`--log-level DEBUG` oder
`SWARMQ_LOG_LEVEL=DEBUG`) den vollen Python-Traceback zu
Verbindungsfehlern (Exit `1`) und zu Konfigurationsfehlern nach dem
Logging-Setup (Exit `78`) hinzu. `schedule`, `dlq` und `inspect` haben
kein Log-Level und drucken eine einzelne Fehlerzeile.

## `swarmq worker`

Importiert ein Python-Modul, sodass seine `Task`-Subklassen
registriert werden, verbindet sich mit Broker + Backend und
konsumiert. `SWARMQ_BROKER_URL` und `SWARMQ_BACKEND_URL` müssen
gesetzt sein.

```text
swarmq worker --module DOTTED.PATH [--app-dir DIR]
              [--configure MODULE:CALLABLE]
              [--queues A,B,C]
              [--concurrency N] [--pool {io,cpu}]
              [--log-level LEVEL] [--log-format FORMAT]
              [--log-config PATH | --no-configure-logging]
              [--logger-level NAME=LEVEL]...
              [--log-queue-size N] [--log-lifecycle-level LEVEL]
              [--log-drain-timeout SECONDS]
              [--burst] [--burst-idle-timeout SECONDS]
              [--no-scheduler]
              [--priorities]
              [--metrics-port PORT] [--health-port PORT]
              [--processes N | -p N] [--autoscale MIN,MAX]
              [--max-processes N] [--group LABEL]
              [--reload] [--watch-dir PATH]...
              [--check]
```

### Optionen

| Flag | Default | Beschreibung |
|---|---|---|
| `--module DOTTED.PATH` | *pflicht* | Python-Modul mit zu registrierenden `Task`-Subklassen (z. B. `myapp.tasks`). Importiert mit `--app-dir` (Standard: das aktuelle Arbeitsverzeichnis) an erster Stelle von `sys.path`, danach der übrige Python-Pfad. |
| `--app-dir DIR` | Arbeitsverzeichnis | Verzeichnis, das vor dem Import von `--module` an die erste Stelle von `sys.path` kommt. Ohne die Option gilt das aktuelle Arbeitsverzeichnis wie bei `python -m`, außer unter `PYTHONSAFEPATH` oder `-P`; ein ausdrückliches `--app-dir` gilt immer. `--configure`, `--check`, die Worker-Kinder des Supervisors und die Rechenkinder von `--pool cpu` nutzen denselben Pfad. Siehe [Das Task-Modul finden](#das-task-modul-finden). |
| `--configure MODULE:CALLABLE` | ungesetzt | Callable, das mit der `SwarmQ`-App nach ihrer Konstruktion und vor dem Start des Workers aufgerufen wird, z. B. `myapp.hooks:setup`. Damit lassen sich Middleware und Signal-Handler registrieren, ohne einen eigenen Worker-Einstieg zu schreiben. Läuft genau einmal je Worker-Prozess, nie im Forkserver und nie in `schedule`, `dlq` oder `inspect`; ein Awaitable als Rückgabe wird abgewartet. Jede Ausnahme aus dem Hook ist ein Konfigurationsfehler (Exit `78`). Siehe [das Beispiel](#middleware-per-configure-registrieren). |
| `--queues A,B,C` | `default` | Komma-separierte Queue-Namen zum Konsumieren. Env-Var: `SWARMQ_QUEUES`. |
| `--concurrency N` | `10` (io) / zugeteilte CPUs (cpu) | Maximal parallel ausgeführte Tasks. Mit `--pool cpu` die Zahl der Rechenslots; Voreinstellung sind die dem Worker zugeteilten CPUs (Affinität und cgroup-Quote) geteilt durch `--max-processes`, falls gesetzt, sonst durch `--processes`. Env-Var: `SWARMQ_CONCURRENCY`. |
| `--pool {io,cpu}` | `io` | Ausführungspool. `cpu` führt jeden Task-Körper in einem Rechenkind aus einem vorgewärmten Forkserver aus: der Loop-Prozess rechnet nie, `hard_timeout` und Cancel töten das Kind. Nur POSIX; nicht mit `--autoscale` kombinierbar. Siehe [CPU-Modus](../guides/cpu-mode.md). |
| `--log-level LEVEL` | `INFO` | Einer aus `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Root-Level jedes SwarmQ-Prozesses. Env-Var: `SWARMQ_LOG_LEVEL`. |
| `--log-format FORMAT` | `human` | `human` oder `json` auf stderr. JSON passt zu Loki, Datadog, CloudWatch. Env-Var: `SWARMQ_LOG_FORMAT` (Alias `SWARMQ_LOG_JSON=true/false`; `SWARMQ_LOG_FORMAT` gewinnt). |
| `--log-config PATH` | nicht gesetzt | Logging-`dictConfig`-Datei (JSON; YAML mit installiertem PyYAML), die in jedem SwarmQ-Prozess (auch in den Rechenkindern) statt der eingebauten Einrichtung gilt. Der Supervisor prüft sie einmal und erneut vor jedem `SIGHUP`-Reload, in einem eigenen Prozess mit 30 Sekunden Frist, während die Überwachung weiterläuft; eine ungültige Datei (`log_config_reload_failed`) oder eine Prüfung, deren Frist abläuft (`log_config_reload_probe_timeout`), lässt die laufenden Worker weiterlaufen. Nicht kombinierbar mit einem ausdrücklichen Format oder `--no-configure-logging`. Env-Var: `SWARMQ_LOG_CONFIG`. Siehe [Logging](logging.md#eigene-dictconfig-datei-log-config). |
| `--no-configure-logging` | konfigurieren | In keinem SwarmQ-Prozess Logging einrichten; dein Task-Modul oder `--configure`-Hook ist zuständig. Env-Var: `SWARMQ_CONFIGURE_LOGGING=false`. |
| `--logger-level NAME=LEVEL` | keine | Level eines einzelnen Loggers, z. B. `aio_pika=WARNING` oder `swarmq.task=WARNING`. Wiederholbar; zuletzt angewandt. Env-Var: `SWARMQ_LOGGER_LEVELS` (komma-separiert). |
| `--log-queue-size N` | `10000` | Kapazität der prozessinternen Log-Queue; ist sie voll, werden Einträge verworfen statt zu blockieren. Env-Var: `SWARMQ_LOG_QUEUE_SIZE`. |
| `--log-lifecycle-level LEVEL` | `DEBUG` | Level der Events `task_received` und `task_started`. Env-Var: `SWARMQ_LOG_LIFECYCLE_LEVEL`. |
| `--log-drain-timeout SECONDS` | `5` | Obergrenze für jedes Leeren der Log-Queues: beim Prozessende, vor einem Reload, vor jedem Ergebnis eines Rechenkinds. Nicht rechtzeitig geschriebene Einträge gelten als verworfen. Muss größer als 0 und höchstens `threading.TIMEOUT_MAX` sein, sonst Exit `78`. Env-Var: `SWARMQ_LOG_DRAIN_TIMEOUT`. Siehe [Logging](logging.md#begrenztes-leeren). |
| `--burst` | aus | Exited sauber, sobald alle Queues `--burst-idle-timeout` Sekunden Idle waren. Env-Var: `SWARMQ_BURST_MODE`. |
| `--burst-idle-timeout SECONDS` | `5.0` | Sekunden Queue-Stille, bevor `--burst` exited. Env-Var: `SWARMQ_BURST_IDLE_TIMEOUT`. |
| `--no-scheduler` | Scheduler an | Hält diesen Worker aus der Wahl des Scheduler-Leaders heraus. Er meldet seine Crons weiter. Crons, `delay`/`eta`-Tasks und Oneshots feuern nur, solange ein anderer Worker am selben Valkey und Namensraum Leader ist. Env-Var: `SWARMQ_SCHEDULER=off` ([Scheduler abschalten](../guides/scheduling-and-retries.de.md#scheduler-auf-einzelnen-workern-abschalten)). |
| `--priorities` | aus | Auto-Deklaration und Konsum des `{queue}.high`/`.normal`/`.low`-Triplets für jede Queue. Env-Var: `SWARMQ_PRIORITIES`. Ohne das Flag wirft der Publish eines Tasks mit `TaskInfo(priority="high"\|"low")` `UnroutableMessage` (ab 0.12; vorher ging die Nachricht still verloren). |
| `--metrics-port PORT` | ungesetzt | **Basisport** für Prometheus `/metrics`: Der Worker in Slot `s` serviert auf `PORT + s`, `--processes 3 --metrics-port 9000` belegt also `9000-9002`. Braucht `pip install 'swarmq[metrics]'`. Bind-Host: `SWARMQ_METRICS_HOST` (Default `127.0.0.1`). Siehe [Basisports](configuration.de.md#observability). |
| `--health-port PORT` | ungesetzt | **Basisport** für `/health` + `/ready`, derselbe Slot-Versatz wie bei `--metrics-port`. Bind-Host: `SWARMQ_HEALTH_HOST` (Default `127.0.0.1`). |
| `--processes N`, `-p N` | `1` | Lässt N Worker-Subprozesse in den Slots `0..N-1` laufen. Seit 0.5.0 läuft jeder `swarmq worker` unter dem Supervisor, auch mit `N = 1`: Er startet gecrashte Worker im selben Slot neu (max 5 pro Stunde), sperrt einen Slot darüber hinaus für eine Abkühlzeit von 1 h (seit 0.12; vorher dauerhaft), ersetzt Worker ohne Lebenszeichen, reloaded auf `SIGHUP`, indem er sich per `os.execvpe()` mit den beim Start festgehaltenen argv (`sys.orig_argv`) und Environment neu startet – so reloaded auch `python -m swarmq.cli worker ...` –, und endet mit 0, sobald alle Worker sauber beendet sind (`--burst`). `SIGTERM` und `SIGINT` beenden den Supervisor immer, auch während ein Reload geprüft oder vorbereitet wird. Ein Konfigurationsfehler in einem beliebigen Worker beendet den Lauf nach genau einem Versuch mit Exit `78`. |
| `--autoscale MIN,MAX` | ungesetzt | Aktiviert Queue-tiefen-getriebenes Autoscaling zwischen `MIN` und `MAX` Worker-Subprozessen. Impliziert `--processes`. Seit 0.12 zählt es nur die Nachrichten, die in der Queue selbst bereitliegen, nicht die Delay-Queues ([Delay-Stufen](../explanation/delivery-semantics.md#delay-stufen)). |
| `--max-processes N` | ungesetzt | **Schaltet den Kontrollkanal ein** und erklärt den Spielraum: Ein im Backend hinterlegter Sollwert darf diesen Supervisor auf höchstens `N` Worker-Prozesse bringen. `N` muss mindestens `--processes` und die `--autoscale`-Obergrenze decken; mit `--pool cpu` darf es die zugeteilten CPUs nicht überbuchen (`N` ohne `--concurrency`, `N × --concurrency` mit). Ohne dieses Argument liest und schreibt der Supervisor keine Sollwerte und verhält sich exakt wie bisher. Siehe [Kontrollkanal](#kontrollkanal). |
| `--group LABEL` | sortierte Menge der `--queues` | Gruppenschlüssel des Kontrollkanals. Jeder Supervisor mit demselben Schlüssel (im selben Backend) folgt demselben Sollwert, jeder für sich. Deployments, die sich ein Backend teilen und dieselben Queues bedienen, brauchen verschiedene Labels, sonst steuern sie einander. |
| `--reload` | aus | Beobachtet Task-Module und triggert SIGHUP-Reload bei Änderung. Nur Developer. Braucht `pip install 'swarmq[reload]'`. Endet ein Worker-Kind mit `78` (etwa wegen eines `SyntaxError` im Task-Modul), endet der Supervisor nicht: Er loggt `worker_config_error`, startet keinen Ersatz und startet die Worker nach der nächsten Dateiänderung neu. Ein Konfigurationsfehler, den der Elternprozess beim Start erkennt, endet weiterhin mit `78`. |
| `--watch-dir PATH` | Elternverzeichnis der `--module`-Quelle | Zu überwachendes Verzeichnis (wiederholbar). Ohne `--reload` ignoriert. |
| `--check` | aus | Dry-Run-Validierung: prüft, ob `--module` importierbar ist (`--check` importiert es, Ausgaben des Moduls gehen nach stderr, und wendet die [DI-Prüfung](api.md#dependency-injection-dishka) des Worker-Starts auf die Tasks an, die der Import registriert), ob eine `--configure`-Angabe die Form `MODULE:CALLABLE` hat und ihr Modul auffindbar ist (ohne es zu importieren), mit `--reload` dass `watchfiles` installiert ist und Watch-Dirs auflösbar sind, dass die Beobachtungs-Portbereiche unter 65535 bleiben und sich nicht überschneiden, dass die Logging-Einstellungen gültig sind und eine `--log-config`-Datei lädt und von `dictConfig` angenommen wird (Probe in einem eigenen Interpreter), dass `--pool cpu` nicht mit `--autoscale` kombiniert ist und dass `--max-processes` nicht unter `--processes` oder der `--autoscale`-Obergrenze liegt und mit `--pool cpu` die zugeteilten CPUs nicht überbucht. Außerdem baut es die Worker-Konfiguration aus den `SWARMQ_*`-Variablen wie der Pre-Flight des Supervisors, sodass jeder Wert, den der Start mit `78` ablehnt, auch hier scheitert (etwa `SWARMQ_SCHEDULER=maybe` oder `SWARMQ_CONCURRENCY=0`). Fehlende `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL` prüft es nicht (`--check` läuft in Build-Schritten ohne Secrets); der Start endet dann trotzdem mit `78`. Druckt eine JSON-Zusammenfassung inklusive `metrics_ports` und `health_ports` (die genauen Ports, die die Slots binden) und exited `0`/`1` ohne Supervisor-Start oder Broker/Backend-Kontakt. Payload: [Nachrichtenformate](agent/message-formats.de.md#payload-von-swarmq-worker-check). |

### Beispiele

Minimaler lokaler Worker:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"
swarmq worker --module myapp.tasks
```

Produktiv-Setup mit JSON-Logs, drei Worker-Subprozessen, Prometheus
und Health. Die drei Slots servieren Metriken auf `9100-9102` und
Health auf `9200-9202`:

```bash
swarmq worker \
  --module myapp.tasks \
  --queues priority,default \
  --concurrency 25 \
  --processes 3 \
  --log-format json \
  --metrics-port 9100 \
  --health-port 9200
```

### Das Task-Modul finden

Das Konsolenskript `swarmq` hat das `bin`-Verzeichnis des Virtualenv an
erster Stelle von `sys.path`, nicht das Arbeitsverzeichnis. Seit diesem
Release stellen `swarmq worker` und `swarmq inspect` das
Arbeitsverzeichnis voran, wie es `python -m swarmq.cli worker` immer
tat; `--module myapp.tasks` findet also `./myapp/tasks.py` ohne
`PYTHONPATH`. Liegt der Code woanders, hilft `--app-dir DIR`:

```bash
swarmq worker --app-dir /srv/app --module myapp.tasks
```

!!! warning "Lokale Dateien überdecken installierte Pakete"
    Eine Datei im App-Verzeichnis gewinnt gegen ein installiertes Paket
    oder ein Modul der Standardbibliothek gleichen Namens: Eine
    verirrte `valkey.py`, `queue.py` oder `select.py` im
    Arbeitsverzeichnis legt den Worker lahm. `PYTHONSAFEPATH=1` (oder
    `python -P`) hält das Arbeitsverzeichnis aus `sys.path`; ein
    ausdrückliches `--app-dir` gilt trotzdem.

Wird das Modul nicht gefunden, nennt der Fehler `--app-dir` und das
zuerst durchsuchte Verzeichnis.

### Middleware per `--configure` registrieren

```python title="myapp/hooks.py"
from swarmq import Signal, SwarmQ
from swarmq.logging import get_logger

from myapp.middleware import TimingMiddleware

log = get_logger("myapp.hooks")


async def setup(app: SwarmQ) -> None:
    app.add_middleware(TimingMiddleware())

    @app.on_signal(Signal.TASK_REJECTED)
    async def on_rejected(queue, reason, **kwargs):
        log.warning("task_rejected", queue=queue, reason=reason)
```

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup
```

Der Hook kann eine gewöhnliche Funktion oder eine Coroutine-Funktion
sein. Er läuft in jedem Worker-Subprozess, nachdem die App existiert
und bevor der Worker sich verbindet; im Supervisor läuft er nicht.
Das Modul sollte deshalb beim Import keine Seiteneffekte haben.

Autoskaliertes Deployment:

```bash
swarmq worker --module myapp.tasks --autoscale 2,10
```

Burst-Mode für einen Cron-Style-Batch-Drain:

```bash
swarmq worker --module myapp.tasks --burst --burst-idle-timeout 10
```

### Kontrollkanal

Mit `--max-processes N` ändert ein Betreiber (oder ein steuernder Dienst
wie das swarmq-Dashboard) Prozesszahl und Autoscale-Grenzen einer
Worker-Gruppe im laufenden Betrieb, ohne Deploy und ohne Konsole:

```bash
swarmq worker --module myapp.tasks --autoscale 2,4 --max-processes 16 --group orders-eu
```

**Sollwert.** Der steuernde Dienst schreibt je Gruppe einen JSON-Wert nach
`[<namensraum>:]sq:control:setpoint:<gruppe>` (`<namensraum>:` nur mit `SWARMQ_NAMESPACE`), etwa `{"min": 2, "max": 8}`. Eine feste
Prozesszahl `N` ist der Sollwert `{"min": N, "max": N}` und schaltet die
Autoskalierung für die Gruppe ab. Der Supervisor liest den Schlüssel
einmal, bevor er den ersten Worker startet, und danach alle 5 Sekunden.
Eine höhere Untergrenze startet die fehlenden Worker sofort, eine
niedrigere Obergrenze beendet die überzähligen Worker sanft (der laufende
Task wird fertig, dann endet der Prozess). Wird der Schlüssel gelöscht,
gelten wieder die Startargumente. Ein neuer Deploy fährt deshalb mit dem
zuletzt gesetzten Sollwert hoch, nicht mit der Deployment-Datei.

**Ablehnung.** Ein Sollwert wird abgelehnt -- und die bisherigen Grenzen
bleiben wirksam -- wenn seine Obergrenze über dem Spielraum liegt, seine
Untergrenze unter 1, seine Grenzen verdreht sind, der Wert nicht als
`{"min": N, "max": M}` lesbar ist oder ein Bereich für eine Gruppe ohne
Autoskalierung geschrieben wird (festes `--processes` oder `--pool cpu`
nehmen nur die feste Form `N..N`). Ein Sollwert lässt den Start des
Supervisors nie scheitern: Ist das Backend beim Start nicht erreichbar,
fährt die Gruppe mit ihren Startargumenten hoch und übernimmt den
Sollwert, sobald das Backend antwortet.

**Rückmeldung.** Jeder Supervisor meldet unter
`[<namensraum>:]sq:control:status:<gruppe>:<supervisor>` (die Supervisor-Kennung ist
`SWARMQ_WORKER_ID` oder der Hostname, jeweils plus `:p<pid>`): den
Gruppenschlüssel, den zuletzt gelesenen Sollwert (oder `null`), ob der
Speicher `present`, `absent` oder `unread` (nicht erreichbar) war, die
wirksamen Grenzen samt `source` (`startup` oder `setpoint`), den
`rejection`-Grund, die Slot-Zahlen (`size`, `occupied`, `locked`) und
`reported_at`. Der Schlüssel verfällt nach Gnadenfrist des
Herunterfahrens plus Terminierungsfrist plus einem Abtastintervall; die
Meldung eines toten Supervisors verschwindet also von selbst. Ein Leser
unterscheidet damit „abgelehnt", „noch nicht angekommen" und „Backend
nicht erreichbar", ohne den Prozess anzusehen.

**Was der Spielraum dimensioniert.** Slot-Tabelle, Beobachtungs-Portbereiche
(`--metrics-port`/`--health-port` belegen `PORT..PORT+N-1`) und mit
`--pool cpu` die Voreinstellung der Rechenslots je Worker (zugeteilte
CPUs geteilt durch `N`) werden beim Start einmal aus `N` abgeleitet und
einmal geprüft. Nichts davon wächst zur Laufzeit: mehr Spielraum verlangt
einen Deploy. Das Einschalten auf einem bestehenden Deployment kann daher
eine Portänderung verlangen. Vom Neustart-Budget gesperrte Slots bleiben
gesperrt; die Rückmeldung nennt ihre Zahl.

**Sicherheit.** Wer auf das Backend schreiben darf, kann die Größe einer
Worker-Gruppe setzen. Sollwert- und Statusschlüssel liegen unter eigenen
Präfixen (`[<namensraum>:]sq:control:setpoint:` und
`[<namensraum>:]sq:control:status:`), getrennt von Task- und
Ergebnisschlüsseln, sodass ein Betreiber Schreibrechte für Sollwerte und
für Rückmeldungen getrennt vergeben kann (etwa per Valkey-ACL wie
`~<namensraum>:sq:control:setpoint:*`; ohne Namensraum
`~sq:control:setpoint:*`). Solange der steuernde Dienst keine eigenen Zugangsdaten mit
so eingeschränkten Schreibrechten hat, kann jeder Task-Erzeuger, der die
Backend-URL teilt, auch Sollwerte setzen.

## `swarmq schedule`

Verwaltet die Schedule-Definitionen des eingebetteten Schedulers in
Valkey. Braucht `SWARMQ_BACKEND_URL`; verbindet sich nicht mit dem
Broker.

### `swarmq schedule list`

```text
swarmq schedule list [--format (table|json)]
```

Listet jeden Schedule in `[<namensraum>:]sq:{sched}:defs` mit Cron, Next-Fire-Zeit,
Queue und Dedup-Zustand auf. Die Spalte `LAST_SEEN` zeigt die jüngste
[Cron-Meldung](../guides/scheduling-and-retries.md#cron-meldungen-und-leader)
eines statischen Crons (UTC), `never` für einen statischen Cron, den
nie ein Worker gemeldet hat (Kandidat für
[`schedule prune --orphans`](#swarmq-schedule-prune)), und `-` für
CLI-Schedules, die nie gemeldet werden. `--format json` ergänzt jeden
Eintrag um `last_seen_ms` (Unix-Millisekunden oder `null`).

### `swarmq schedule add`

```text
swarmq schedule add NAME
                    (--cron EXPR | --at ISO_DATETIME)
                    --task TASK_NAME
                    [--args JSON_ARRAY] [--kwargs JSON_OBJECT]
                    [--queue QUEUE]
                    [--unique-for SECONDS]
                    [--unique-until (start|completion)]
                    [--format (table|json)]
```

Legt einen neuen Schedule an (Insert-if-absent). Übergib genau
eines von `--cron` (5-Feld-Expression) oder `--at` (ISO-8601-
Einzelzeit; ohne Zeitzone als UTC gelesen; höchstens `315360000`
Sekunden voraus, sonst Exit `1` vor jeder Verbindung, seit 0.13.1). `--cron` nimmt dieselbe
Positivliste nach crontab(5) an wie `TaskInfo.cron`
([Cron-Syntax](configuration.md#scheduling-cron)); alles andere, etwa
sechs Felder oder `L`, `W`, `#`, `R`, `H`, `?` und `@reboot`, beendet den
Befehl vor jeder Verbindung mit Exit `1` (seit 0.13.0). `--args` (Default `[]`)
muss ein JSON-Array sein, `--kwargs` (Default `{}`) ein JSON-Objekt;
alles andere endet mit Exit `1`. `--queue` ist per Default `default`.
`--task` wird nicht gegen die Task-Registry geprüft: Ein Tippfehler
fällt erst auf, wenn der Schedule auf einem Worker feuert.

`--unique-for SECONDS` belegt vor dem Anlegen des Schedules ein
Dedup-Flag über Task-Name + args + kwargs; seine TTL ist `SECONDS` plus
die Zeit bis zum ersten Feuern. `SECONDS` ist höchstens `315360000`
(zehn Jahre, wie `TaskInfo.unique_for`); ein größerer Wert ist ein
Aufruffehler (Exit `2`, seit 0.13.0). Ein zweites `schedule add` mit
demselben Task und denselben args und kwargs wird, solange das Flag
gehalten wird, als Duplikat abgewiesen (`json`-Ausgabe:
`inserted: false`, `duplicate_of: <owner>`). `--unique-until`
(Default `completion`, sonst `start`) entscheidet, ob das Flag beim
Erreichen eines Endzustands oder beim Abholen durch einen Worker
freigegeben wird; es wirkt nur zusammen mit `--unique-for`.

`--format json` gibt `inserted`, `name` und `already_exists` aus, mit
`--unique-for` außerdem `dedup_hash`, `unique_until` und bei einem
Duplikat `duplicate_of`. `inserted` sagt, ob dieser Aufruf den Schedule
gespeichert hat. `already_exists` ist `true`, sobald unter dem Namen ein
Eintrag existiert, auch bei gleichem Namen und gleichen Argumenten; ein
Dedup-Duplikat unter einem anderen Namen ergibt `already_exists: false`
mit `duplicate_of` (seit 0.13.0; ob der Aufruf etwas gespeichert hat,
sagt `inserted`, nicht `already_exists`). Der Claim von `--unique-for`
wird sofort freigegeben, wenn schon ein anderer Eintrag den Namen hält
und wenn das Backend mit einem Fehler antwortet. Fällt das Backend aus
(Verbindung weg, Zeitüberschreitung), bleibt er bis zu seinem Ablauf
stehen, weil der Eintrag trotzdem gespeichert sein kann; ein erneuter
Aufruf ist bis dahin ein Duplikat. Seit 0.13.1 gilt dasselbe, wenn der
Befehl beim Speichern oder beim Lesen des Eintrags nach einem Konflikt
abgebrochen wird (Strg+C). Einen Claim, den ein früheres
`schedule add` unter demselben Namen hält, gibt der Aufruf nie frei,
auch nicht nach einem Lesefehler. Das gilt auch nach
`schedule remove`: Bis der Claim abläuft, ergibt ein erneutes Anlegen
mit gleichem Namen und gleichem Task, args und kwargs `inserted: false`,
`already_exists: false` und `duplicate_of: <eigener Name>`, weil der
Eintrag weg ist, der Claim des entfernten Schedules aber noch gehalten
wird. Beim Lesen des Eintrags unter dem
Namen kann `schedule_def_malformed_skipped` (WARNING) erscheinen, wenn
er sich nicht dekodieren lässt.

Beispiel:

```bash
swarmq schedule add nightly-billing \
  --cron '0 3 * * *' \
  --task run_billing \
  --queue priority \
  --kwargs '{"tenant": "acme"}'
```

### `swarmq schedule remove`

```text
swarmq schedule remove NAME [--permanent] [--format (table|json)]
```

Ohne `--permanent` wird der Schedule nur deaktiviert, bleibt aber
erhalten; er bleibt deaktiviert, wenn sich sein Code ändert. Mit
`--permanent` wird er hart gelöscht, und eine Markierung aus dem
Entfernen eines deaktivierten Crons (`[<namensraum>:]sq:{sched}:disabled`) wird
gelöscht, auch wenn der Cron selbst schon weg ist; die nächste Meldung
legt ihn dann aktiv an. Einen statischen Cron, den ein
lebender Worker noch meldet, übernimmt der Leader binnen eines
`SWARMQ_SCHEDULER_SEEN_INTERVAL_S` wieder; dauerhaft weg ist er erst,
wenn ihn kein Code mehr meldet (siehe
[Cron-Meldungen](../guides/scheduling-and-retries.md#cron-meldungen-und-leader)).

### `swarmq schedule status`

```text
swarmq schedule status [--limit N] [--format (table|json)]
```

Druckt den aktuellen Leader-Worker (falls vorhanden), seine
swarmq-Version, die nächsten N Schedules, die innerhalb 24 Stunden
anstehen (`--limit`, Default `10`), und die Worker, die Crons melden,
mit ihren swarmq-Versionen.

```text
LEADER: host-a:4711:3f9c2a1b
LEADER_VERSION: 0.13.0
NOW:    2026-10-20T08:00:00+00:00
UPCOMING:
  2026-10-21T01:00:00+00:00  daily_report  task=daily_report  queue=reports
WORKERS:
  worker-a  0.13.0  host-a:4711:3f9c2a1b
  worker-a  0.13.0  host-a:4802:9d01b7e4
  worker-b  0.12.3
```

`LEADER_VERSION` ist `(not reported: swarmq before 0.10.0)`, wenn der
Leader ein swarmq ohne Versionsangabe ausführt, und `-` ohne Leader.
`WORKERS` listet jeden Worker-Prozess mit lebender Meldung (drei
`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`), ohne einen `(none)`. Seit 0.13.0
meldet jeder Prozess unter eigenem Schlüssel; Worker-Prozesse mit
derselben `worker_id` (zwei Supervisoren auf einem Host, dieselbe
`SWARMQ_WORKER_ID`) erscheinen deshalb als eigene Zeilen, die dritte
Spalte ist der Prozess (das Owner-Token `host:pid:id`). Worker vor 0.13.0
melden ohne ihn und zeigen zwei Spalten. Nach einem Neustart oder Kill
eines Workers bleibt seine alte Zeile bis zu drei Meldeintervalle
sichtbar. Alle hier
gezeigten Versionen sollten mindestens die Mindestversion für Leader
erreichen (derzeit 0.12; siehe
[Leader-Version](../guides/scheduling-and-retries.md#leader-version)).
Termine erscheinen in UTC.

`--format json` ergänzt `leader`, `now_ms` und `upcoming` um die Felder
`leader_version` (String oder `null`) und `workers` (Liste von
`{"worker_id": ..., "version": ..., "process": ...}`; `process` ist
`null` für Worker vor 0.13.0).

### `swarmq schedule prune`

```text
swarmq schedule prune --orphans [--format (table|json)]
```

Entfernt jeden statischen Cron, den nie ein Worker gemeldet hat:
Altbestand aus der Zeit vor 0.10.0 und Crons von Apps, die ein swarmq
vor 0.10.0 fahren. Der Leader entfernt diese nie von selbst (er loggt
`scheduler_cron_prune_candidate`); gemeldete und dann verstummte Crons
entfernt er nach `SWARMQ_SCHEDULER_PRUNE_AFTER_S` (siehe
[Crons entfernen](../guides/scheduling-and-retries.md#crons-entfernen-die-keine-app-mehr-meldet)).
CLI-Schedules bleiben immer unberührt. Jeder Cron wird atomar samt
Termin entfernt, und nur, solange er noch keine Meldung hat. Ein
deaktivierter Cron kommt deaktiviert zurück, wenn ihn ein Worker wieder
meldet. `--orphans` ist Pflicht. Ausgabe je Cron
`removed orphan schedule 'NAME'` oder `no orphan schedules (every
static schedule has been reported)`; `--format json` gibt
`{"removed": [...]}` aus.

Nur ausführen, wenn jede App auf dem Valkey mindestens 0.10.0 fährt;
sonst werden die Crons einer älteren App entfernt und kommen erst
zurück, wenn diese App sie nach ihrem Upgrade meldet.

## `swarmq dlq`

Operiert auf `{queue}.XQ`, der Broker-Level-Dead-Letter-Queue.
Braucht `SWARMQ_BROKER_URL`; verbindet sich nicht mit dem Backend.

### `swarmq dlq list`

```text
swarmq dlq list [--queue QUEUE] [--limit N] [--format (table|json)]
```

Liest die DLQ mit Bestätigung und kopiert jede Nachricht an ihr Ende,
bevor das Original bestätigt wird; ein Leser, der abbricht, verliert
nichts ([DLQ lesen](../guides/dead-letter-queues.md#dlq-inhalte-auflisten)). Zeigt `task_id`, `task_name`, Retry-Count-
Header, Fehler-Preview, Body-Größe. `--queue` ist per Default
`default` (DLQ `default.XQ`), `--limit` `100`. Wiederholte Aufrufe sind safe:
Nachrichten bleiben mit ihrer Restfrist in der DLQ; Lesen verlängert
sie nicht, und Nachrichten mit abgelaufener Frist fallen weg (siehe
[Aufbewahrungsfrist](../guides/dead-letter-queues.md#aufbewahrungsfrist-dead-gelettete-nachrichten-verfallen)).
Die Tabellenausgabe zeigt `(truncated)`, wenn `--limit` erreicht ist;
`--format json` enthält einen `truncated: bool`-Envelope. `truncated`
(und `scan_exhausted` von `inspect` und `retry TASK_ID`) ist wahr, sobald
das Lesen am Limit endete, auch durch Lesungen abgelaufener Einträge, die
`scanned` nicht zählt (seit 0.13.0; vorher konnte ein abgelaufener
Eintrag das Limit verdecken).

Alle `dlq`-Subcommands, die Nachrichten zurücklegen, berechnen die
Restfrist aus den Headern der Nachricht, `x-swarmq-dead-lettered-at`
und `x-swarmq-dead-letter-ttl-ms`. Nur eine Nachricht ohne sie
(Altbestand, Dead-Letter-Exchange des Brokers) bekommt beim ersten
Lesen beide Header und die volle `SWARMQ_DEAD_LETTER_TTL_S` der CLI
(Standard `604800`, `none` = unbegrenzt).

### `swarmq dlq inspect`

```text
swarmq dlq inspect TASK_ID [--queue QUEUE]
                            [--scan-limit N]
                            [--format (table|json)]
```

Findet eine DLQ-Nachricht anhand `task_id` und druckt ihre vollen
Header und den dekodierten Body. Non-destruktiv. Exited `3`, wenn
die task_id nicht gefunden wird (nutze `--scan-limit`, Default
`1000`, um die Suche zu verbreitern). Endete die Suche ohne Treffer an
`--scan-limit`, sagt die Meldung das, und `--format json` enthält
`scan_exhausted: true`; `dlq retry TASK_ID` ebenso.

### `swarmq dlq retry`

```text
swarmq dlq retry (TASK_ID | --all)
                 [--queue QUEUE] [--limit N] [--scan-limit N]
                 [--format (table|json)]
```

Publisht zurück in die Haupt-Queue. `--all` bewegt jede Nachricht
(bis zu `--limit`, Default `10000`); eine einzelne `TASK_ID` wird
unter den ersten `--scan-limit` Nachrichten gesucht (Default `1000`).
Übergib genau eines von `TASK_ID` und `--all`, sonst endet der Befehl
mit Exit `2`. Der
`x-swarmq-retry-count`-Header wird zurückgesetzt, damit der Task
sein volles Retry-Budget bekommt; `x-swarmq-dead-lettered-at` und die
Frist entfallen, und eine neue `x-swarmq-replay-id` lässt die Nachricht
am [Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)
vorbei; alle anderen Header bleiben. Exited `3`, wenn ein
Einzel-Task-Retry nichts findet.

### `swarmq dlq purge`

```text
swarmq dlq purge [--queue QUEUE] [--format (table|json)]
```

Natives AMQP `queue.purge()` auf `{queue}.XQ`. Unumkehrbar — nutze
es, wenn der DLQ-Inhalt bekannt schlecht und nicht inspektionswürdig
ist.

## `swarmq inspect`

```text
swarmq inspect --module DOTTED.PATH [--app-dir DIR] [--format (table|json)]
```

Importiert ein Modul, registriert seine `Task`-Subklassen und
druckt die `TaskInfo`-Metadaten jedes Tasks — Queues, Retry-Policy,
Rate-Limit, Lock, Dedup-Settings, Cron. Read-only. Verbindet sich
nicht mit Broker oder Backend. Nützlich als Container-Healthcheck
oder in CI. `--app-dir` wirkt wie bei
[`swarmq worker`](#das-task-modul-finden).

### Beispiel

```bash
$ swarmq inspect --module myapp.tasks
TASK           QUEUES        DELIVERY       RETRIES  RATE_LIMIT  LOCK               UNIQUE_FOR  UNIQUE_UNTIL  CRON
render_invoice pdf           at_least_once  5        -           -                  -           -             -
send_email     default,mail  at_least_once  3        100/m       -                  -           -             -
sync_user      users         at_least_once  2        -           user-{user_id}     60          completion    -
```

## `swarmq check`

```text
swarmq check [--format (table|json)]
```

Prüft nur lesend die Umgebung, in der swarmq läuft (neu in 0.12). Liest
`SWARMQ_BACKEND_URL`, `SWARMQ_VALKEY_EVICTION_CHECK`,
`SWARMQ_SHUTDOWN_TIMEOUT`, `SWARMQ_NAMESPACE`, `SWARMQ_SCHEDULER` und
für den Abschnitt `queues` `SWARMQ_BROKER_URL` und `SWARMQ_SCHEDULER_SEEN_INTERVAL_S`.
Schickt an Valkey nur `CONFIG GET`, `INFO`, `SCAN` und Lesebefehle, an
RabbitMQ nur passive Declares. **Vor einem Upgrade** mit der neuen swarmq-Version in einem
eigenen virtualenv gegen Produktion laufen lassen; unter Kubernetes
optional als Init-Container ([Valkey-Anforderungen](configuration.de.md#valkey-anforderungen)).
Nicht dasselbe wie `swarmq worker --check`, das einen Worker-Start
probeweise durchläuft.

Abschnitte:

- `valkey`: `maxmemory_policy`, `maxmemory`, `used_memory`, `eviction`
  (`safe`, `unsafe`, `unknown`), das wirksame `eviction_check`,
  `aof_enabled`, `version`, `cluster_enabled`, `namespace`
  (`SWARMQ_NAMESPACE`, ohne ihn `null`) und `unprefixed_keys`: mit
  Namensraum die Zahl der Keys ohne ihn (`sq:*`, per `SCAN` gezählt bis
  10000 binnen 5 Sekunden), sonst `null`, und `unprefixed_keys_complete`
  (`false`, wenn `SCAN` das Zeitlimit erreichte: die Zahl ist dann eine
  Untergrenze). Im Cluster-Modus sieht `SCAN` nur den verbundenen Knoten.
  Werte, die Valkey nicht liefert (ACL), sind `null`.
- `shutdown`: `shutdown_timeout` und `min_grace_period_s`
  (`shutdown_timeout + 15`), die mindestens einzustellende
  `terminationGracePeriodSeconds` (Kubernetes) bzw. `stop_grace_period`
  (Docker).
- `queues` (seit 0.12, nur mit `SWARMQ_BROKER_URL`, sonst
  `checked: false`): die von Workern ab 0.12 gemeldeten Queues
  (`reported`), per passivem Declare geprüft: `orphaned` nennt Queues mit
  Nachrichten, ohne Consumer und seit drei Meldetakten
  (`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`) ohne lebende Meldung, je mit
  `queue`, `messages` und `last_reported_s_ago`;
  `missing_dead_letter_queues` nennt gemeldete Queues, deren `.XQ` fehlt
  (der Broker verwirft dorthin geleitete gescheiterte Tasks)
  ([Task in eine andere Queue verschieben](../guides/defining-tasks.de.md#task-in-eine-andere-queue-verschieben)).
- `scheduler`: `scheduler`, der Wert von
  `SWARMQ_SCHEDULER` (ohne die Variable `on`). Ob überhaupt ein Worker
  Scheduler-Leader ist, wird nicht geprüft.

`findings` nennt jedes Problem mit `section`, `level` (`error` oder
`warning`), `code` und `message`: `eviction_policy` (unter `strict` ein
Fehler, sonst eine Warnung), `eviction_policy_unknown`, `aof_disabled`,
`cluster`, `version` (älter als 8), `unprefixed_keys` (Keys ohne
Namensraum neben einem Namensraum: Apps ohne `SWARMQ_NAMESPACE` am
selben Valkey oder Daten von vor dem Setzen, die der Namensraum nicht
migriert; [Namensraum](configuration.md#namensraum-mehrere-apps-auf-einem-valkey)),
`unprefixed_keys_unknown` (`SCAN` gescheitert, verboten oder vor dem
ersten Fund am Zeitlimit), `orphaned_queue`, `dead_letter_queue_missing`
und `queues_unchecked` (die gemeldeten Queues waren aus Valkey nicht
lesbar, etwa per ACL verboten), `scheduler_disabled` (Warnung:
`SWARMQ_SCHEDULER=off`, ein Worker mit dieser Umgebung bewirbt sich
nicht um die Leader-Rolle) und `scheduler_invalid` (Fehler: ein anderer
Wert als `on` oder `off`). Eine unvollständige Zählung lautet `at least
<n> keys … (count incomplete …)`; im Cluster-Modus nennt die Meldung
zusätzlich, dass nur ein Knoten gezählt wurde.

Exit-Codes: `0`, wenn nichts blockiert (Warnungen erlaubt), `78`, wenn
ein Worker in dieser Umgebung den Start verweigern würde (verdrängende
Policy unter `SWARMQ_VALKEY_EVICTION_CHECK=strict`, ungültiger
`SWARMQ_NAMESPACE` oder `SWARMQ_SCHEDULER` oder ein anderer `SWARMQ_*`-Wert,
den der Worker ablehnt, etwa `SWARMQ_VALKEY_EVICTION_CHECK=bogus` oder
`SWARMQ_SHUTDOWN_TIMEOUT=abc`; außer `SWARMQ_SCHEDULER` wird das vor dem
Verbindungsaufbau geprüft, mit Meldung auf stderr und ohne Bericht), `1`, wenn Valkey
nicht erreichbar ist, `SWARMQ_BACKEND_URL` fehlt oder `SWARMQ_BROKER_URL`
gesetzt und der Broker nicht erreichbar ist. `swarmq check` kennt keine
Worker-Flags: Eine ungültige Variable, die ein Flag von `swarmq worker`
überstimmen würde (etwa `SWARMQ_LOG_FORMAT` unter `--log-format`), lässt
die Prüfung trotzdem mit `78` enden.

```bash
$ SWARMQ_BACKEND_URL=valkey://valkey:6379/0 swarmq check --format table
valkey  url  valkey://valkey:6379/0
valkey  eviction_check  strict
valkey  eviction  unsafe
valkey  maxmemory_policy  volatile-lru
...
ERROR  valkey: maxmemory-policy volatile-lru may evict swarmq keys (...)
WARNING  valkey: AOF is off: (...)
FAILED
```

## Globale Flags

| Flag | Beschreibung |
|---|---|
| `--version` | Druckt `swarmq <version>` und exited `0`. |
| `-h`, `--help` | Druckt die Hilfe zum aktuellen Command/Subcommand und exited `0`. |

## Siehe auch

- [Konfiguration](configuration.md) — die Env-Vars, die diese Flags schatten
- [Logging](logging.md) — Ausgabeformat, Events und Integrationsrezepte
- [Dead-Letter-Queues](../guides/dead-letter-queues.md) — Recovery-
  Workflow mit `swarmq dlq`
- [Hot Reload (nur Dev)](../guides/hot-reload.md) — wann `--reload`
  nutzen und wann nicht
