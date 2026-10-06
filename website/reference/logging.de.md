# Logging

SwarmQ loggt strukturierte Events über Pythons Standardmodul `logging`.
Als Library konfiguriert es das Logging nie: Eine App, die `SwarmQ(...)`
einbettet, behält ihre Root-Handler, ihr Root-Level und ihre
structlog-Konfiguration unverändert. Nur die Prozesse, die SwarmQ selbst
startet — der Supervisor von `swarmq worker`, seine Worker-Subprozesse
und die Rechenkinder des CPU-Pools —, richten Logging ein, und jeder Teil
dieser Einrichtung lässt sich konfigurieren oder abschalten.

| Wo SwarmQ läuft | Wer das Logging konfiguriert | Wohin SwarmQs Events gehen |
|---|---|---|
| Eingebettet in deine App (`SwarmQ(...)`, `await app.start_worker()`) | Deine App | Deine Root-Handler; ohne Handler erreichen WARNING und höher Pythons `lastResort`-Handler |
| Eingebettet, mit `log_queue=True` | Deine App | Deine Root-Handler, aufgerufen aus einem Hintergrund-Thread von SwarmQ ([Opt-in-Log-Queue](#opt-in-log-queue)) |
| `swarmq worker` (Supervisor, Worker, Rechenkinder) | SwarmQ, außer mit `--log-config` oder `--no-configure-logging` | stderr, `human` oder `json` ([Worker-Prozesse](#worker-prozesse)) |
| Rechenkinder eines eingebetteten CPU-Pools | SwarmQ, nach den Logging-Feldern von `SwarmQConfig` | stderr, nicht die Handler deiner App ([eingebetteter CPU-Pool](#eingebetteter-cpu-pool)) |

## Library-Logger und Felder

Jedes Modul loggt über einen benannten Logger `swarmq.<modul>`, etwa
`swarmq.worker.pipeline`, `swarmq.worker.retry`,
`swarmq.process_manager.supervisor` oder `swarmq.scheduler.loop`. Die
Lebenszyklus-Events der Tasks haben einen eigenen Logger, `swarmq.task`.
Alle liegen unter `swarmq`: `logging.getLogger("swarmq").setLevel(...)`
steuert die ganze Library, `logging.getLogger("swarmq.task")` nur die
Events pro Task.

Ein SwarmQ-Event ist ein gewöhnlicher `logging.LogRecord`:

- Der **Event-Name** ist die Nachricht des Records
  (`record.getMessage()` liefert `task_succeeded`).
- Jedes **Feld** ist ein Attribut des Records (`record.task_id`,
  `record.duration_ms`).
- Ein Feld, dessen Name mit einem `LogRecord`-Attribut kollidiert, wird
  mit dem Präfix `swarmq_` umbenannt (`name` wird zu `swarmq_name`,
  `module` zu `swarmq_module`). So scheitert das Logging nie an einem
  `KeyError`.

SwarmQ installiert absichtlich keinen `NullHandler`. Ganz ohne
Logging-Konfiguration erscheinen Events ab WARNING weiterhin über Pythons
`lastResort`-Handler auf stderr; INFO und DEBUG bleiben stumm, bis deine
App einen Handler und ein Level einrichtet.

`logging.disable(level)` wirkt auf SwarmQs Logger wie auf jeden anderen.

## Einbindung in deine App

SwarmQs Events erreichen die Handler, die deine App hat. Was du siehst,
hängt vom Formatter ab: Der Event-Name ist immer die Nachricht, die
Felder sieht nur ein Formatter, der die Attribute des Records liest.

### Reines stdlib-Logging

`logging.basicConfig(level=logging.INFO)` reicht, um SwarmQs
INFO-Events zu sehen. Das Standardformat zeigt aber nur den Event-Namen
(`INFO:swarmq.task:task_succeeded`). Für die Felder brauchst du einen
Formatter, der die Attribute des Records liest. Ein minimaler
JSON-Formatter:

```python
import json
import logging

_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message", "asctime",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "event": record.getMessage(),
            "level": record.levelname.lower(),
            "logger": record.name,
        }
        payload.update(
            {k: v for k, v in vars(record).items() if k not in _STANDARD}
        )
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
```

```text
{"event": "task_succeeded", "level": "info", "logger": "swarmq.task", "duration_ms": 12.5, "task_id": "…", "task_name": "send_email", …}
```

Alternativ nimmst du SwarmQs eigenen Formatter, der deine Records und
die von SwarmQ gleich rendert:

```python
from swarmq.logging import build_formatter

handler = logging.StreamHandler()
handler.setFormatter(build_formatter(json=True, colors=False))
logging.basicConfig(level=logging.INFO, handlers=[handler])
```

### structlog-Apps

Eine App, die stdlib-Records über structlogs `ProcessorFormatter`
leitet, muss `structlog.stdlib.ExtraAdder()` in die `foreign_pre_chain`
aufnehmen. Ohne ihn rendert structlog nur den Event-Namen und **verwirft
stillschweigend alle Felder** von SwarmQs Events:

```python
import logging

import structlog

formatter = structlog.stdlib.ProcessorFormatter(
    foreign_pre_chain=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.ExtraAdder(),             # nötig für SwarmQs Felder
        structlog.processors.format_exc_info,
    ],
    processors=[
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        structlog.processors.JSONRenderer(),
    ],
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
root = logging.getLogger()
root.addHandler(handler)
root.setLevel(logging.INFO)
```

SwarmQs Logger hängen nicht von `structlog.configure` ab: Deine
structlog-Konfiguration ändert SwarmQs Events nicht, und SwarmQ ändert
deine Konfiguration nicht.

### `build_formatter`

`swarmq.logging.build_formatter(json: bool, colors: bool)` liefert den
Formatter, den SwarmQ in seinen eigenen Prozessen nutzt: einen
structlog-`ProcessorFormatter`, der jeden Record (SwarmQs, deine, die
von Drittbibliotheken) mit den Schlüsseln `event`, `level`, `logger` und
`timestamp` (ISO 8601, UTC, aus dem Erzeugungszeitpunkt des Records)
plus allen Feldern rendert. `json=True` wählt den JSON-Renderer, sonst
gilt der Konsolen-Renderer, mit ANSI-Farben nur bei `colors=True`. Ein
Record, der sich nicht formatieren lässt, wird durch ein Event
`log_record_unformattable` ersetzt statt durch einen Traceback im
Logstrom. In einer dictConfig-Datei referenzierst du ihn über `"()"`
(siehe [das Beispiel](#eigene-dictconfig-datei-log-config)).

## Opt-in-Log-Queue

Standardmäßig laufen SwarmQs Events synchron durch deine Handler, im
loggenden Thread — beim Worker also im Event-Loop. Ein langsamer Handler
(Netzwerk, Datei auf einer ausgelasteten Platte) blockiert dann den Loop.
Mit `log_queue=True` (Env `SWARMQ_LOG_QUEUE=true`) verlagerst du diese
Arbeit aus dem Loop:

```python
app = SwarmQ(broker, backend, log_queue=True, log_level="INFO")
```

- SwarmQ hängt einen eigenen Queue-Handler an den Logger `swarmq`. Ein
  Hintergrund-Thread namens `swarmq-log-listener` gibt jeden Record zum
  Ausgabezeitpunkt an deine **Root**-Handler weiter. Handler-Level
  gelten, später hinzugefügte Handler (Sentry, OpenTelemetry, uvicorn)
  bekommen die Records, und ohne Handler greift weiterhin `lastResort`.
- Die Kontextvariablen des Aufrufers stehen den Filtern deiner Handler
  zur Verfügung, als liefen sie im Aufrufer.
- `log_level` setzt das Level des Loggers `swarmq`. `log_json` hat hier
  keine Wirkung: Deine Handler formatieren jeden Record, auch die
  Meldung [`log_records_dropped`](#nicht-blockierende-ausgabe-und-verworfene-eintrage).
  `log_queue_size` (Vorgabe `10000`) begrenzt die Queue.
- Handler, die du direkt an den Logger `swarmq` hängst, bleiben synchron.
  Sentrys `LoggingIntegration` läuft weiterhin im Aufrufer.
- Der Thread startet mit dem ersten Record, nicht in `SwarmQ(...)`. Die
  letzte `SwarmQ`-Instanz, die `async with` verlässt, stellt den Logger
  `swarmq` wieder her (Handler, `propagate`, Level).
- Die Option ist standardmäßig aus und wirkt nicht in Prozessen, die
  `swarmq worker` konfiguriert (eingebauter Modus oder `--log-config`);
  unter `--no-configure-logging` gilt sie.

Ohne `log_queue` haben `log_level` und `log_json` in einer eingebetteten
App keine Wirkung: Setze die Level an deinen eigenen Loggern.

## Worker-Prozesse

`swarmq worker` richtet das Logging im Supervisor, in jedem
Worker-Subprozess und in jedem Rechenkind ein. Die eingebaute
Einrichtung schreibt über eine Queue und einen Hintergrund-Thread nach
**stderr**:

- `--log-format human` (Vorgabe) nutzt den Konsolen-Renderer, mit
  ANSI-Farben nur, wenn stderr ein TTY ist.
- `--log-format json` schreibt ein JSON-Objekt pro Zeile.
- Beide tragen die Schlüssel `event`, `level`, `timestamp` und `logger`
  plus die Felder des Events. `logger` gibt es seit 0.8.0.
- `structlog.get_logger()` in deinem Task-Code rendert über dieselbe
  Ausgabe, sofern deine App structlog nicht schon selbst konfiguriert hat.

```text
{"event": "worker_started", "level": "info", "logger": "swarmq.worker.lifecycle", "timestamp": "2026-10-06T10:01:58.942714Z", "pid": 41872, …}
```

### Optionen

| Flag | Env-Var | Vorgabe | Wirkung |
|---|---|---|---|
| `--log-format {human,json}` | `SWARMQ_LOG_FORMAT` | `human` | Ausgabeformat. `SWARMQ_LOG_JSON=true/false` bleibt als Alias; sind beide gesetzt, gewinnt `SWARMQ_LOG_FORMAT`. |
| `--log-level LEVEL` | `SWARMQ_LOG_LEVEL` | `INFO` | Root-Level. `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `--log-config PATH` | `SWARMQ_LOG_CONFIG` | nicht gesetzt | Deine eigene [dictConfig-Datei](#eigene-dictconfig-datei-log-config) statt der eingebauten Einrichtung. |
| `--no-configure-logging` | `SWARMQ_CONFIGURE_LOGGING=false` | konfigurieren | [Das Logging deiner App überlassen](#logging-der-app-uberlassen), in jedem SwarmQ-Prozess. |
| `--logger-level NAME=LEVEL` (wiederholbar) | `SWARMQ_LOGGER_LEVELS="aio_pika=WARNING,valkey=ERROR"` | keine | Level einzelner Logger, zuletzt angewandt (auch nach einer dictConfig). |
| `--log-queue-size N` | `SWARMQ_LOG_QUEUE_SIZE` | `10000` | Kapazität der prozessinternen Log-Queue; eine positive ganze Zahl. |
| `--log-lifecycle-level LEVEL` | `SWARMQ_LOG_LIFECYCLE_LEVEL` | `DEBUG` | Level von `task_received` und `task_started` ([Lebenszyklus-Events](#lebenszyklus-events-der-tasks)). |

`CRITICAL` ist überall erlaubt, wo ein Level erwartet wird.

### Priorität

CLI-Flag > Umgebungsvariable > `SwarmQConfig`-Wert > Vorgabe. Der
Supervisor löst die Einstellungen **einmal** auf und gibt das Ergebnis
an seine Worker-Subprozesse und Rechenkinder weiter; die Kinder lesen
das Environment nicht noch einmal. Ein Flag auf der Kommandozeile gewinnt
deshalb in jedem Prozess: Mit `SWARMQ_LOG_FORMAT=json` und
`--log-format human` loggen Supervisor und alle Worker im Textformat.

### Konfigurationsfehler

Ungültige Logging-Einstellungen beenden `swarmq worker` mit Exit-Code
`78` und genau einer Zeile `worker_config_error` (Felder `source`,
`error_type`, `error`, `hint`; bei einer fehlerhaften Konfigurationsdatei
zusätzlich `path`). Das betrifft:

- ein ungültiges Level, Format, einen ungültigen `NAME=LEVEL`-Eintrag
  oder eine ungültige Queue-Größe (die Meldung nennt Flag oder Variable,
  aus der der Wert stammt);
- `--log-config` zusammen mit einem ausdrücklichen Format —
  `--log-format`, `SWARMQ_LOG_FORMAT` oder `SWARMQ_LOG_JSON` —, weil die
  Datei das Format festlegt;
- `--log-config` zusammen mit `--no-configure-logging` oder
  `SWARMQ_CONFIGURE_LOGGING=false`;
- eine Konfigurationsdatei, die sich nicht lesen, parsen oder anwenden
  lässt ([`LoggingConfigError`](errors.md#loggingconfigerror)).

### Eigene dictConfig-Datei (`--log-config`)

`--log-config PATH` wendet in jedem SwarmQ-Prozess eine
[`logging.config.dictConfig`](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema)-Datei
statt der eingebauten Einrichtung an.

- JSON geht immer, `.yaml`/`.yml` nur mit installiertem PyYAML (sonst
  ein Konfigurationsfehler mit Hinweis).
- Setzt die Datei `disable_existing_loggers` nicht, nimmt SwarmQ
  `false`. Die stdlib-Vorgabe `true` würde bereits importierte Logger
  stummschalten (SwarmQs, `aio_pika`).
- SwarmQ startet die Listener der `QueueHandler` aus der Datei
  (`dictConfig` legt sie an, startet sie aber nicht) und stoppt sie beim
  Prozessende, wobei die Queue geleert wird.
- Ein ausdrückliches `--log-level` setzt nach der Datei zusätzlich das
  Root-Level; auch `--logger-level`-Einträge gelten danach.
- Der Supervisor löst den Pfad auf, lädt und prüft die Datei einmal und
  gibt die geprüfte Konfiguration an seine Kinder weiter. Die Kinder
  lesen die Datei nie; weder ein anderes Arbeitsverzeichnis noch eine
  spätere Änderung der Datei wirkt sich dort aus.
- Bei einem Reload (`SIGHUP`, `--reload`) prüft der laufende Supervisor
  die Datei erneut, **bevor** er einen Worker stoppt. Ist sie inzwischen
  ungültig, bricht er den Reload mit `log_config_reload_failed` ab;
  Worker und Logging laufen unverändert weiter.
- In diesem Modus konfiguriert SwarmQ structlog für deinen Task-Code
  nicht; nutzen deine Tasks `structlog.get_logger()`, konfigurierst du
  structlog selbst.

Beispiel mit SwarmQs Formatter und einem Queue-Handler, damit der
Event-Loop nie selbst in den Stream schreibt:

```json title="logging.json"
{
  "version": 1,
  "formatters": {
    "json": {"()": "swarmq.logging.build_formatter", "json": true, "colors": false}
  },
  "handlers": {
    "stderr": {
      "class": "logging.StreamHandler",
      "stream": "ext://sys.stderr",
      "formatter": "json"
    },
    "queue": {
      "class": "logging.handlers.QueueHandler",
      "handlers": ["stderr"]
    }
  },
  "root": {"level": "INFO", "handlers": ["queue"]},
  "loggers": {
    "aio_pika": {"level": "WARNING"}
  }
}
```

```bash
swarmq worker --module myapp.tasks --log-config logging.json
```

Der `QueueHandler` der stdlib formatiert die Nachricht vor dem Einreihen;
ein Traceback landet deshalb im Text von `event` statt in einem eigenen
Schlüssel.

!!! warning "Im Mehrprozessbetrieb nur Stream-Handler"
    Supervisor und jeder Worker-Prozess wenden dieselbe Datei an. Nutze
    nur Stream-Handler (stdout/stderr) — mehrere Prozesse, die über
    `RotatingFileHandler` dieselbe Datei schreiben und rotieren,
    beschädigen sie. Rotiere außerhalb des Prozesses: journald,
    logrotate mit `copytruncate` oder ein Log-Collector.

### Logging der App überlassen

Mit `--no-configure-logging` (oder `SWARMQ_CONFIGURE_LOGGING=false`)
richtet SwarmQ in keinem seiner Prozesse etwas ein — keine Handler,
keine Level, keine structlog-Konfiguration. Dann ist dein Task-Modul
oder dein `--configure`-Hook zuständig; tut es nichts, erreichen nur
WARNING und höher `lastResort`. In diesem Modus gilt die
[Opt-in-Log-Queue](#opt-in-log-queue).

### Wenn ein Task-Modul das Logging konfiguriert

Ein Worker-Subprozess richtet das Logging ein, **bevor** er dein
Task-Modul importiert. Ändert das Modul (oder ein `--configure`-Hook)
danach das Logging, gewinnt deine App:

- **Sie ersetzt die Root-Handler** (etwa mit einer eigenen `dictConfig`):
  SwarmQ tritt zurück und hält seine eigenen Events trotzdem aus dem
  Event-Loop heraus, indem es sie über seine Queue an deine Handler
  leitet. Es loggt einmal `logging_reconfigured_by_app`. SwarmQ-Logger,
  die deine `dictConfig` deaktiviert hat, schaltet es wieder ein; die
  Anzahl steht im Feld `reenabled_loggers`.
- **Sie fügt Handler** neben denen von SwarmQ **hinzu**: Beide schreiben,
  SwarmQ entfernt nichts und loggt `logging_handlers_added_by_app` als
  WARNING mit den Handler-Typen.
- **`logging.basicConfig(...)` in einem Task-Modul wirkt nicht** unter
  `swarmq worker`, weil der Root-Logger schon einen Handler hat.

Im CPU-Pool gilt dieselbe Regel für die Rechenkinder: Handler, die dein
Modul beim Import einrichtet, erben sie vom Forkserver, und diese
gewinnen; `logging_reconfigured_by_app` erscheint dann einmal pro Pool.

Willst du das Logging ganz selbst besitzen, nimm `--log-config` oder
`--no-configure-logging`, statt beim Import Logging zu konfigurieren.

### Eingebetteter CPU-Pool

Betreibt eine eingebettete App einen CPU-Pool (`pool="cpu"`), startet
SwarmQ die Rechenkinder selbst. Sie loggen nach **stderr**, gemäß den
`SwarmQConfig`-Feldern `log_level`, `log_json` (Vorgabe `true`),
`log_config`, `configure_logging`, `logger_levels` und `log_queue_size`
— nicht über die Handler deiner App.

## Nicht blockierende Ausgabe und verworfene Einträge

Der loggende Code baut nur den Record und legt ihn in eine begrenzte
Queue. Rendern und I/O passieren im Listener-Thread. Ist die Queue voll,
verwirft SwarmQ Einträge, statt zu blockieren:

- Ab 90 % Füllstand werden nur noch Einträge unterhalb von WARNING
  verworfen; WARNING und höher nutzen die restlichen 10 % und gehen erst
  bei vollständig voller Queue verloren.
- Verworfene Einträge werden pro Prozess gezählt. Der loggende Code
  schreibt die Meldung nie selbst und wirft nie, auch nicht bei
  geschlossenem stderr. Der Listener-Thread meldet die Verluste
  höchstens einmal pro Sekunde und noch einmal beim Stoppen: als
  gewöhnlichen Record `log_records_dropped` (Logger `swarmq.logging`,
  Level WARNING, Feld `count`) über seine eigenen Handler. Die Meldung
  hat damit dasselbe Format wie jede andere Zeile, im JSON-Modus etwa
  `{"event": "log_records_dropped", "count": N, "level": "warning", "logger": "swarmq.logging", "timestamp": …}`.
- Wirft ein Handler, Filter oder Formatter, läuft der Listener weiter.
  Die Fehler werden gezählt und auf dieselbe Weise als
  `log_handler_errors` (Feld `count`) gemeldet; lässt sich auch diese
  Meldung nicht schreiben, geht sie direkt nach stderr. Ein dennoch
  gestorbener Listener-Thread startet mit dem nächsten Record neu.
- Der Worker exportiert den Zähler als Prometheus-Metrik
  `swarmq_log_records_dropped_total`. Sie umfasst nur den
  Worker-Prozess; Verluste in Rechenkindern zeigt deren Meldungszeile.

Auf jedem geordneten Exit-Pfad wird die Queue geleert. Ein per `SIGKILL`
beendeter Prozess verliert die Einträge, die noch in seiner Queue liegen.

## Task-Kontext-Felder

Innerhalb eines Tasks trägt jedes SwarmQ-Event den Task-Kontext. Das
gilt auch für deine eigenen `structlog.get_logger()`-Events in Prozessen
von `swarmq worker` und in jeder App, deren structlog-Kette
`merge_contextvars` enthält:

| Feld | Inhalt |
|---|---|
| `task_id` | ID des Tasks |
| `task_name` | Registrierter Task-Name |
| `queue` | Queue, aus der die Nachricht kam |
| `worker_id` | [Worker-Identität](configuration.md#worker-identitat) |
| `workflow_id` | Workflow, zu dem der Task gehört; nur, wenn die Nachricht eine gültige trägt |
| `attempt` | Laufender Versuch, `retry_count + 1` |
| `compute_child` | PID des Rechenkinds; nur bei Events, die im Rechenkind geloggt werden |

Der Kontext gilt für die Dauer der Nachricht und wird danach
wiederhergestellt; Kontextvariablen, die deine App selbst bindet,
bleiben erhalten.

## Lebenszyklus-Events der Tasks

Geloggt über den Logger `swarmq.task`, für IO- und CPU-Pool:

| Event | Level | Felder |
|---|---|---|
| `task_received` | Lebenszyklus-Level (Vorgabe `DEBUG`) | Task-Kontext |
| `task_started` | Lebenszyklus-Level (Vorgabe `DEBUG`) | Task-Kontext |
| `task_succeeded` | `INFO` | `duration_ms`; `compute_child` im CPU-Pool |
| `task_failed` | `INFO` | `duration_ms`, `error_type`, `error`; `compute_child` im CPU-Pool |
| `task_cancelled` | `INFO` | `duration_ms` |

`task_failed` meldet das Ergebnis einer Ausführung. Ob SwarmQ es erneut
versucht oder in die Dead-Letter-Queue legt, melden getrennt
`task_retrying` (INFO) und `task_sent_to_dlq` (ERROR).

Mit `--log-lifecycle-level INFO` machst du `task_received`/`task_started`
sichtbar; mit `--logger-level swarmq.task=WARNING` schaltest du die
Events pro Task ab. Fortschrittswarnungen (`update_progress_hset_failed`,
`update_progress_publish_failed`) teilen sich den Logger `swarmq.task`
und bleiben bei WARNING sichtbar.

## Fehlerfelder

Events, die eine Exception melden, tragen `error_type` (Klassenname der
Exception) und `error` (ihre Nachricht, bei leerer Nachricht der
Klassenname). Einen Traceback gibt es nur bei unerwarteten Fehlern —
solchen, mit denen SwarmQ nicht rechnet, etwa einem abstürzenden
Request-Handler des Health- oder Metrics-Servers.

## Event-Katalog

Die folgenden Events bilden den Logging-Vertrag für den Betrieb. Sie
sind eine Auswahl, nicht jedes Event, das SwarmQ ausgibt.

| Event | Logger | Level | Hinweise |
|---|---|---|---|
| `worker_started` | `swarmq.worker.lifecycle` | INFO | |
| `worker_died` | `swarmq.process_manager.supervisor` | INFO / WARNING | INFO bei sauberem Ende (`0`) oder erwartetem Herunterfahren, sonst WARNING. Felder `worker_id`, `pid`, `exitcode`, `expected`, `slot_state`. |
| `worker_config_error` | `swarmq.cli`, `swarmq.process_manager.entry`, `swarmq.process_manager.supervisor` | ERROR | Konfigurationsfehler, Exit `78`. Aus `swarmq.cli` (Elternprozess) und `swarmq.process_manager.entry` (Worker-Kind): Felder `source` (`supervisor`/`worker`), `error_type`, `error`, `hint`, dazu `path` bei kaputter Konfigurationsdatei und `worker_id` im Kind. Aus `swarmq.process_manager.supervisor`, wenn ein Kind mit `78` endete: Felder `worker_id`, `pid`, `exitcode`, `hint`. Traceback auf stderr nur bei Level `DEBUG` (`--log-level` oder `SWARMQ_LOG_LEVEL`). |
| `worker_connection_error` | `swarmq.cli` | ERROR | Broker oder Backend beim Start nicht erreichbar, Exit `1`. Traceback nur bei Level `DEBUG` (`--log-level` oder `SWARMQ_LOG_LEVEL`). |
| `worker_recycle_ack_timeout` | `swarmq.process_manager.entry` | ERROR | Ein recycelnder Worker bekam keine Bestätigung und endet mit `75`. |
| `log_config_reload_failed` | `swarmq.process_manager.run` | ERROR | Reload abgebrochen; Felder `path`, `error_type`, `error`, `hint`. |
| `logging_reconfigured_by_app` | `swarmq.logging`, `swarmq.worker.compute.pool` | INFO | Deine App hat die Root-Handler ersetzt; Feld `reenabled_loggers`. |
| `logging_handlers_added_by_app` | `swarmq.logging` | WARNING | Feld `handler_types`. |
| `log_records_dropped` | `swarmq.logging` | WARNING | Feld `count`; aus dem Listener-Thread, höchstens einmal pro Sekunde und noch einmal beim Stoppen. |
| `log_handler_errors` | `swarmq.logging` | WARNING | Ein Handler, Filter oder Formatter hat geworfen; Feld `count`; höchstens einmal pro Sekunde. |
| `log_record_unformattable` | Logger des Records | Level des Records | Feld `error`. |
| `task_retrying` | `swarmq.worker.retry` | INFO | Felder `retry_count`, `max_retries`, `delay_ms`. |
| `task_sent_to_dlq` | `swarmq.worker.retry` | ERROR | Der Task ging in die DLQ. Felder `error_type` (Exception-Klasse; `WorkerHungError` bei erreichtem Zustelllimit) und `error`. |
| `task_rate_limited` | `swarmq.worker.gates.rate_limit` | INFO | Felder `retry_after_ms`, `remaining`, `rate_limit_count`. |
| `task_lock_conflict` | `swarmq.worker.gates.lock` | INFO | Lock anderweitig belegt; Nachricht neu eingereiht. Felder `lock_name`, `lock_conflict_count`. |
| `task_expired` | `swarmq.worker.gates.expiry` | WARNING | Älter als `max_age`; in die DLQ. Felder `age_s`, `max_age_s`. |
| `delivery_limit_reached_dead_letter` | `swarmq.worker.gates.delivery_count` | ERROR | `delivery_limit`-mal ohne Bestätigung zugestellt; als `WorkerHungError` in die DLQ. Felder `delivery_count`, `delivery_limit`. |
| `message_body_too_large` | `swarmq.worker.decode` | ERROR | Body über `SWARMQ_MAX_MESSAGE_BODY_BYTES`; undekodiert an den DLX. Felder `body_bytes`, `cap_bytes`, `hint`. |
| `deserialize_failed` | `swarmq.worker.decode` | ERROR | Nicht dekodierbarer Body; an den DLX. |
| `task_not_found` | `swarmq.worker.pipeline` | ERROR | Task-Name in diesem Worker nicht registriert; an den DLX. |
| `retry_publish_failed` | `swarmq.worker.retry` | ERROR | Retry ließ sich nicht publizieren; die Nachricht wird neu zugestellt. |
| `dlq_publish_failed` | `swarmq.worker.retry` | ERROR | Publish in die DLQ gescheitert; die Nachricht wird neu zugestellt. |
| `result_serialization_failed` | `swarmq.worker.results` | ERROR | Task-Ergebnis nicht serialisierbar; als Fehler gespeichert. |
| `workflow_advance_failed` | `swarmq.worker.results` | ERROR | Unerwarteter Fehler beim Voranbringen eines Workflows; der Workflow wird als gescheitert markiert. Feld `workflow_id`. |
| `workflow_advance_backend_error`, `workflow_advance_broker_error` | `swarmq.worker.results` | ERROR | Backend-/Broker-Fehler (oder Zustandskonflikt) beim Voranbringen; der Schritt wird neu zugestellt. Feld `workflow_id`. |
| `middleware_hook_failed` | `swarmq.middleware` | ERROR | Ein Middleware-Hook außer `pre_enqueue` hat geworfen; mit Traceback. Felder `hook`, `middleware`. |
| `signal_handler_error` | `swarmq.signals` | WARNING | Ein Signal-Handler hat geworfen; mit Traceback. Felder `signal`, `handler`. |
| `signal_handler_timeout` | `swarmq.signals` | WARNING | Ein Lebenszyklus-Signal-Handler hat sein Zeitbudget überschritten. Felder `signal`, `handler`, `timeout_s`. |
| `worker_hung` | `swarmq.process_manager.supervisor` | WARNING | Kein Lebenszeichen; der Worker wird ersetzt. Felder `worker_id`, `pid`, `stale_s`, `deadline_s`. |
| `worker_hung_killed` | `swarmq.process_manager.supervisor` | ERROR | Ein ersetzter Worker hat SIGTERM überlebt und wurde getötet. Felder `worker_id`, `pid`. |
| `hang_replacement_budget_exhausted` | `swarmq.process_manager.supervisor` | ERROR | Zu viele hängende Worker im Fenster; der Slot wird gesperrt. Felder `limit`, `window_s`, `hint`. |
| `worker_restart_limit_exceeded` | `swarmq.process_manager.supervisor` | ERROR | Ein Slot ist binnen einer Stunde 5-mal abgestürzt und wird gesperrt. |
| `all_worker_slots_exhausted` | `swarmq.process_manager.supervisor` | ERROR | Jeder Slot hat das Neustartlimit erreicht; der Supervisor endet. |
| `reload_execvp_failed` | `swarmq.process_manager.supervisor` | ERROR | Der Neustart per `SIGHUP` ist gescheitert; der Supervisor endet mit `1`. |
| `hot_reload_enabled` | `swarmq.process_manager.supervisor` | WARNING | `--reload` ist aktiv (nur Entwicklung). Feld `watch_dirs`. |
| `compute_pool_unhealthy` | `swarmq.worker.compute.pool` | ERROR | Kein Rechenkind nutzbar; `/ready` meldet den Pool als down. |
| `compute_child_unreaped` | `swarmq.worker.compute.pool` | ERROR | Ein Rechenkind hat SIGKILL überlebt; sein Slot bleibt gesperrt. Felder `slot`, `pid`, `reason`. |
| `setpoint_applied` | `swarmq.process_manager.setpoint` | INFO | Kontrollkanal: neue Grenzen gelten. Felder `group`, `effective`. |
| `setpoint_rejected` | `swarmq.process_manager.setpoint` | WARNING | Kontrollkanal: Sollwert abgelehnt, die bisherigen Grenzen bleiben. Felder `group`, `reason`. |
| `setpoint_store_unreachable` | `swarmq.process_manager.setpoint` | WARNING | Kontrollkanal: Backend nicht erreichbar. Felder `group`, `error`. |
| `dedup_detected` | `swarmq.client.schedule`, `swarmq.scheduler.loop` | INFO | |
| `dedup_rollback_failed` | `swarmq.client.schedule` | WARNING | |
| `http_server_started`, `http_server_stopped` | `swarmq._http` | INFO | Feld `server` (`health_server`/`metrics_server`); beim Start `host`, `port`. |
| `http_server_request_failed` | `swarmq._http` | ERROR | Mit Traceback; Feld `server`. |
| `check_backend_failed`, `check_broker_failed` | `swarmq.health` | WARNING | Readiness-Prüfung fehlgeschlagen. |

## Python-API

Alles in `swarmq.logging`:

| Name | Zweck |
|---|---|
| `get_logger(name)` | Ein Logger im SwarmQ-Stil (structlog-`BoundLogger`), der Events unter `name` an das stdlib-`logging` übergibt; unabhängig von `structlog.configure`. |
| `build_formatter(json, colors)` | Der [oben](#build_formatter) beschriebene Formatter. |
| `configure_logging(settings)` | Richtet das Logging eines Prozesses so ein wie `swarmq worker`; `settings` ist ein `swarmq.logging_settings.LoggingSettings` (siehe unten). Für Einstiegspunkte gedacht, die Library ruft es nie auf. Ein zweiter Aufruf im selben Prozess ist wirkungslos. |
| `shutdown_logging()` | Leert die Log-Queues und stoppt die Listener-Threads; ein späteres `configure_logging()` baut neu auf. Läuft beim Prozessende automatisch. |
| `load_log_config(path)` | Lädt und prüft eine dictConfig-Datei (JSON, oder YAML mit PyYAML) und gibt sie als JSON-String zurück, mit `disable_existing_loggers` auf `false`, sofern die Datei es nicht setzt. Wendet nichts an; wirft `LoggingConfigError`. |
| `dropped_log_records()` | Zahl der in diesem Prozess wegen voller Queue verworfenen Log-Einträge; der Wert hinter `swarmq_log_records_dropped_total`. |
| `LoggingConfigError` | Ungültige Logging-Konfigurationsdatei; siehe [Fehlerkatalog](errors.md#loggingconfigerror). |

`LoggingSettings` ist eine eingefrorene Dataclass; jedes Feld hat einen
Default:

| Feld | Default | Bedeutung |
|---|---|---|
| `level` | `"INFO"` | Root-Level. |
| `level_explicit` | `False` | `level` auch nach einer dictConfig-Datei am Root-Logger setzen (wie ein ausdrückliches `--log-level`). |
| `format` | `"human"` | `"human"` oder `"json"` für die eingebaute Einrichtung. |
| `log_config` | `None` | Pfad einer dictConfig-Datei; ersetzt die eingebaute Einrichtung. |
| `log_config_json` | `None` | Inhalt der Datei, wie `load_log_config()` ihn liefert. Gesetzt, wird die Datei nicht noch einmal gelesen. |
| `configure` | `True` | `False` lässt `configure_logging()` nichts tun. |
| `logger_levels` | `()` | Paare `(("aio_pika", "WARNING"), ...)`, zuletzt angewandt. |
| `queue_size` | `10000` | Kapazität der Log-Queue. |
| `lifecycle_level` | `"DEBUG"` | Level von `task_received` und `task_started`. |

```python
from swarmq.logging import configure_logging, load_log_config
from swarmq.logging_settings import LoggingSettings

path = "/etc/myapp/logging.json"
configure_logging(LoggingSettings(
    log_config=path,
    log_config_json=load_log_config(path),  # einmal prüfen, früh scheitern
    logger_levels=(("aio_pika", "WARNING"),),
))
```

Veraltet: `setup_logging(log_level, log_json)` (baut die eingebaute
Einrichtung bei jedem Aufruf neu auf) und `flush_logging()` (ein Alias
für `shutdown_logging()`).

## Siehe auch

- [CLI-Referenz](cli.md#swarmq-worker) — jedes Flag von `swarmq worker`
- [Konfiguration](configuration.md#logging) — die Logging-Env-Vars und
  `SwarmQConfig`-Felder
- [Fehlerkatalog](errors.md#loggingconfigerror) — `LoggingConfigError`
- [Changelog](changelog.md#080-2026-10-06) — Migration vom bisherigen
  Verhalten
