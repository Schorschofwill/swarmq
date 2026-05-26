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

## Argumente und Rückgabewerte

### Was Clients übergeben dürfen

Jedes Argument an `run()` muss **JSON-kompatibel** sein: es geht
durch `msgspec` auf den Draht. Erlaubte Typen:

- `str`, `int`, `float`, `bool`, `None`
- `list` / `tuple` / `dict` der obigen
- `bytes` (base64-kodiert auf dem Transport)

Nicht erlaubt:

- ORM-Row-Objekte (SQLAlchemy, Django).
- Klassen ohne JSON-kompatible Repräsentation.
- `datetime`, `Decimal`, `UUID` — vorher zu `str` kodieren (ISO 8601
  für Datetimes, Standardform für UUIDs).

Der Guardrail erscheint als `SerializationError` zur Schedule-Zeit,
nicht zur Laufzeit.

### Was `run()` zurückgibt

Der Rückgabewert geht durch **msgpack** und wird nach Valkey
unter `sq:result:{task_id}` geschrieben. msgpack ist permissiver
als JSON — verarbeitet Bytes, verschachtelte Container, Integer-
Breiten nativ — aber dennoch keine beliebigen Python-Objekte. Ist
dein Rückgabewert nicht serialisierbar, raist der Worker
`ResultSerializationError` und der Task gilt als gescheitert.

Im Zweifel: ein `dict` aus Primitiven zurückgeben:

```python
return {"status": "sent", "message_id": message_id, "duration_ms": elapsed}
```

## Reservierte Parameternamen

Vier Parameternamen auf `run()` sind von `Client.schedule()`
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

## Lifecycle-Hooks

Überschreibe `pre_execution` und `post_execution`, um auf dem
Worker vor/nach dem Task-Body Code auszuführen:

```python
class WithSession(Task):
    task_info = TaskInfo(name="with_session", queues=["default"])

    async def pre_execution(self) -> None:
        self.session = await open_db_session()

    async def run(self, user_id: int) -> dict:
        return await self.session.fetch(user_id)

    async def post_execution(self) -> None:
        await self.session.close()
```

`post_execution` läuft bei Erfolg und Fehler. Nutze es für
deterministisches Cleanup. Für Querschnittsthemen, die auf **jeden**
Task wirken sollen, nutze stattdessen [Middleware](middleware.md).

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
