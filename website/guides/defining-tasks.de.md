# Tasks definieren

Du willst einen Task definieren, der auf einem Worker läuft. Ein
Task in SwarmQ ist eine Python-Klasse, die von `Task` erbt und ihre
Identität mit einem `TaskInfo`-Feld deklariert.

Dieser Guide behandelt die Form einer Task-Definition, wo die
Teile leben und die Anti-Pattern, die in Produktion beißen.

## Voraussetzungen

- Ein Python-Package mit mindestens einem Modul (dem, das du an
  `--module` übergibst).
- `pip install swarmq`.

## Das Minimum

```python title="myapp/tasks.py"
from swarmq import Task, TaskInfo


class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email",
        queues=["default"],
    )

    async def run(self, to: str, subject: str) -> str:
        # Deine E-Mail-Logik hier
        return f"sent to {to}"
```

Drei Regeln werden beim Import des Moduls durchgesetzt:

1. **Von `Task` erben.** Das registriert die Klasse in der globalen
   Registry. Kein Decorator, kein manueller `register()`-Aufruf.
2. **`task_info` deklarieren.** Ohne es überspringt
   `Task.__init_subclass__` die Registrierung — deine Klasse wird
   für Worker unsichtbar.
3. **`async def run(self, ...)` definieren.** Die Signatur benennt
   die Keyword-Argumente, die deine Clients an `schedule()`
   übergeben.

## Name vs Klassenname

`TaskInfo.name` ist der Over-the-Wire-Identifier. Er ist das, was
Clients an `schedule("name", ...)` übergeben und was in Broker-
Queues, Worker-Logs und `swarmq inspect`-Output auftaucht. Der
Python-Klassenname ist für das System irrelevant — du kannst
`SendEmail` zu `EmailSender` umbenennen, ohne etwas zu zerbrechen.

**Das `name` umzubenennen ist ein Breaking Change** für bereits
eingereihte Nachrichten. Ein Worker mit neuem Code findet keinen
Task namens `send_email_v1`, wenn die Clients noch `send_email`
senden. Nutze stattdessen [Deprecation via zweiter Klasse](#einen-task-deprecaten).

## Queue-Routing

`queues` ist eine nicht-leere Liste. Das erste Element ist das
Default-Ziel für `schedule("name")`; andere sind die, an die der
Task per Override *geroutet werden kann*.

```python
task_info = TaskInfo(
    name="render_report",
    queues=["reports", "default"],
)
```

```python
# Publisht auf "reports" — das erste Element.
await app.schedule("render_report", report_id=42)

# Publisht auf "default" — expliziter Override.
await app.schedule("render_report", report_id=42, queue="default")
```

Worker-seitig konsumiert `swarmq worker --queues reports,default`
aus beiden. Siehe [Scheduling und Retries](scheduling-and-retries.md)
für das Zusammenspiel mit `delay=` / `eta=`.

### Task in eine andere Queue verschieben

Zieht ein Task in eine andere Queue um oder ändert sich `--queues` eines
Workers, bleiben Nachrichten in der alten Queue (und Retry-Kopien, die
noch in ihren Delay-Queues warten) liegen: Kein Worker konsumiert sie
mehr. Seit 0.12 meldet jeder Worker die Queues, die er konsumiert, an
Valkey (`[<namensraum>:]sq:{sched}:report:queues`, Zeitstempel von der Valkey-Uhr). Der
Scheduler-Leader prüft die gemeldeten Queues in jedem Übernahme-Takt
(`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`, Standard 60 s; nicht im Takt direkt
nach einer Übernahme) per passivem
Declare, ohne Management-API. Eine Queue gilt als verwaist, wenn sie
Nachrichten hält, keinen Consumer hat und seit drei Takten kein
lebender Worker sie gemeldet hat. Dann loggt der Leader
`scheduler_queue_orphaned` (WARNING, einmal je Übergang), sendet
`SCHEDULER_QUEUE_ORPHANED` und setzt die Gauge
`swarmq_scheduler_queues_orphaned`; darauf alarmieren. `swarmq check`
mit `SWARMQ_BROKER_URL` nennt verwaiste Queues und gemeldete Queues ohne
ihre `.XQ` ([CLI](../reference/cli.de.md#swarmq-check)).

Abhilfe: einen Worker für die alte Queue laufen lassen, bis sie leer
ist, die Nachrichten verschieben oder die Queue löschen. Grenzen:
Geprüft werden nur Queues, die ein Worker ab 0.12 gemeldet hat; eine
Queue mit Consumer (etwa einem Worker vor 0.12) gilt nie als verwaist.
Einträge fehlender oder leerer Queues, die 7 Tage niemand gemeldet hat,
werden entfernt; der ganze Key verfällt 7 Tage nach der letzten Meldung
irgendeines Workers.

## Argumente und Rückgabewerte

### Was Clients übergeben dürfen

Argumente an `run()` reisen als **JSON**, kodiert mit `msgspec`.
Einfache `str`, `int`, `float`, `bool`, `None`, `list` und `dict`
kommen unverändert an. Andere Typen werden unterwegs umgewandelt, und
der Task erhält den umgewandelten Wert, nicht den ursprünglichen Typ:

| Übergeben | Kommt in `run()` an als |
|---|---|
| `datetime`, `date`, `time` | ISO-8601-`str` (`"2026-01-02T03:04:05"`) |
| `Decimal`, `UUID` | `str` |
| `bytes` | Base64-`str` |
| `tuple`, `set`, `frozenset` | `list` |
| Dataclass, `msgspec.Struct` | `dict` |
| `Enum` | sein Wert |
| `dict` mit `int`-Schlüsseln | `dict` mit `str`-Schlüsseln (`{1: "a"}` → `{"1": "a"}`) |
| `float("nan")` | `None` |

Solche Werte in `run()` selbst zurückwandeln (etwa
`datetime.fromisoformat(...)`) oder gleich als `str` übergeben.

Ein Objekt, das `msgspec` nicht kodieren kann (eine ORM-Zeile, eine
beliebige Klasse), wird zur Schedule-Zeit abgelehnt, bevor etwas
veröffentlicht wird:

- Ein direktes `schedule()` wirft `ResultSerializationError` (der Name
  führt in die Irre; so heißt die Klasse, die der JSON-Encoder wirft).
- Mit `delay=` oder `eta=` gehen die Argumente zuerst in den
  Scheduler-Store; dort wirft dasselbe Objekt einen einfachen
  `TypeError`.
- Mit `unique_for=` lehnt der Dedup-Hash zusätzlich `bytes` und
  Objekte eigener Klassen mit `TypeError` ab.

### Was `run()` zurückgibt

Der Rückgabewert geht durch **msgpack** und wird nach Valkey
unter `sq:result:{task_id}` geschrieben. msgpack ist permissiver
als JSON: `datetime`, `date`, `time`, `Decimal`, `UUID`, `set` und
`frozenset` kommen typgleich zurück, ein `tuple` als `list`. Beliebige
Python-Objekte gehen nicht. Ist dein Rückgabewert nicht
serialisierbar, wirft der Worker `ResultSerializationError` und der
Task gilt als gescheitert.

Im Zweifel: ein `dict` aus Primitiven zurückgeben:

```python
return {"status": "sent", "message_id": message_id, "duration_ms": elapsed}
```

## Reservierte Parameternamen

Vier Parameternamen auf `run()` sind von `app.schedule()`
reserviert: `delay`, `eta`, `unique_for`, `unique_until`. Ein
Task, der einen davon deklariert, raist `ValueError` bei der
Klassen-Registrierung, mit einem Migrations-Hinweis:

```python
# ❌ Raist beim Import:
class Scheduled(Task):
    task_info = TaskInfo(name="scheduled", queues=["default"])
    async def run(self, delay: float) -> None:  # "delay" reserviert
        ...

# ✅ Umbenennen:
async def run(self, retry_delay: float) -> None:
    ...
```

Weitere Schlüsselwörter gehören zu den Aufrufen, die einen Task
starten, swarmq lehnt sie auf `run()` aber nicht ab:

- `queue` — `app.schedule()` und `app.run()` lesen es als Ziel-Queue
  und geben es nicht weiter. `schedule("export", queue="eu")` routet
  den Task in die Queue `eu`: Gibt es sie nicht, scheitert der Publish
  mit `UnroutableMessage`, sonst läuft der Task ohne das Argument
  (`TypeError` bei einem Pflichtparameter).
- `timeout`, `polling` — `app.run()` liest sie für das Warten auf das
  Ergebnis und gibt sie nicht weiter.

Benenne einen solchen Parameter um oder übergib den Wert positionell
(`schedule("export", "eu")`).

## Lifecycle-Hooks

Überschreibe `pre_execution` und `post_execution`, um auf dem Worker
vor und nach dem Task-Body Code auszuführen. `post_execution` läuft
nur, wenn `pre_execution` und `run()` erfolgreich waren. Es läuft
nicht, wenn eines davon wirft, nicht bei Soft- oder Hard-Timeout und
nicht bei einem Cancel; eine Ausnahme darin macht den Versuch zum
Fehlschlag. Nutze es für Folgearbeit nach Erfolg. Aufräumen, das bei
jedem Ausgang laufen muss, gehört in `try/finally` in `run()`:

```python
class WithSession(Task):
    task_info = TaskInfo(name="with_session", queues=["default"])

    async def run(self, user_id: int) -> dict:
        session = await open_db_session()
        try:
            return await session.fetch(user_id)
        finally:
            await session.close()
```

Mit [Dependency Injection](../reference/api.md#dependency-injection-dishka)
gehört es stattdessen in den `yield`-Finalizer des Providers; er
bekommt auch die Ausnahme, für Commit oder Rollback. Für
Querschnittsthemen, die auf **jeden** Task wirken sollen, nutze
stattdessen [Middleware](middleware.md); ihr `on_error` läuft bei
jedem Fehlschlag, nicht bei einem Cancel (dafür das Signal
`TASK_CANCELLED` behandeln).

## Progress-Reporting

Rufe `self.update_progress(current, total, message="")` innerhalb
`run()` auf, um Progress an Clients zu streamen:

```python
class RenderReport(Task):
    task_info = TaskInfo(
        name="render_report", queues=["reports"],
        hard_timeout=300, soft_timeout=240,
    )

    async def run(self, report_id: int) -> str:
        items = await fetch_items(report_id)
        total = len(items)
        for i, item in enumerate(items):
            await render_item(item)
            await self.update_progress(i + 1, total, f"item {item.id}")
        return f"reports/{report_id}.pdf"
```

Progress-Writes sind auf einen pro 500 ms pro Task-Instanz
gedrosselt; das finale Update (`current == total`) flusht immer.
Siehe
[`app.watch_progress()`](../reference/api.md#get_progress-watch_progress)
für die Consumer-Seite.

## Einen Task deprecaten

Um einen Task umzubenennen oder auszumustern, lass den alten Task
mindestens einen Release-Zyklus als Dünn-Redirect registriert:

```python
class SendEmail(Task):
    task_info = TaskInfo(name="send_email_v2", queues=["default"])

    async def run(self, to: str, subject: str) -> str:
        ...


class SendEmailLegacy(Task):
    """Veraltet — nutze send_email_v2. Für in-flight Messages behalten."""

    task_info = TaskInfo(name="send_email", queues=["default"])

    async def run(self, *args, **kwargs) -> str:
        # An die neue Implementierung delegieren.
        return await SendEmail().run(*args, **kwargs)
```

Sobald die Haupt-Queue gedraint ist und die DLQ keine
`send_email`-Einträge mehr hat (`swarmq dlq list | grep
send_email` leer ist), lösche die Legacy-Klasse.

## Anti-Pattern

### Mutable Class-Defaults

```python
class Bad(Task):
    task_info = TaskInfo(name="bad", queues=["default"])
    cache: dict = {}  # ❌ über alle Task-Runs geteilt

    async def run(self, key: str) -> str:
        self.cache[key] = "value"  # leakt für immer
        return self.cache[key]
```

Nutze stattdessen Modul-Level-Caches mit expliziten TTLs oder
speichere State in Valkey über deine eigene Connection.

### Threading oder Multiprocessing innerhalb `run()`

Spawne keine Threads oder Prozesse aus einem Task. SwarmQ läuft
schon N Tasks nebenläufig (`--concurrency`) und startet N
Worker-Prozesse (`--processes`). Aus dem Innern zu spawnen
multipliziert diesen Faktor und erschöpft Connection-Pools.

Ist ein Task CPU-bound, lass die CPU-Arbeit in einem
`concurrent.futures.ProcessPoolExecutor` laufen, der beim
Modul-Load gebaut und wiederverwendet wird — oder hebe den Task
in seine eigene Queue mit niedrigerer `--concurrency`.

### `asyncio.Task`-Handles leaken

```python
# ❌ Fire-and-forget-Tasks leaken
asyncio.create_task(some_coro())
```

Das Eltern-`run()` kehrt zurück, bevor das Kind fertig ist; das
Kind kann beim Worker-Shutdown abgebrochen werden oder unhandled
raisen und die Event-Loop des Workers crashen. `await` die
Coroutine direkt oder `schedule()` sie als ordentlichen
SwarmQ-Task.

### Globale, die von Prozess-Identität abhängen

```python
# ❌ `process_id` wird beim Modul-Import erfasst
process_id = f"worker-{os.getpid()}"
```

Mit `--processes > 1` und `--reload` ändern sich Worker-PIDs bei
jedem Re-Exec. Erfasse den Wert lazy in `run()`, wenn du ihn
brauchst.

## Verifizieren

```bash
swarmq inspect --module myapp.tasks
```

Jeder definierte Task sollte in der Tabelle mit seinen `queues`,
`delivery`, `rate_limit`, `lock`, `unique_for` / `unique_until`
und `cron`-Spalten wie deklariert auftauchen. Ein fehlender Task
bedeutet, dass sein Modul nicht importiert wurde — fixiere den
`--module`-Pfad.

## Siehe auch

- [`TaskInfo`-Felder](../reference/configuration.md#taskinfo-felder)
  — jede Option mit Typ und Default
- [Scheduling und Retries](scheduling-and-retries.md) — wie
  Retry-Policy konfiguriert wird
- [Tasks testen](testing-tasks.md) — einen `Task` ohne Broker
  unit-testen
