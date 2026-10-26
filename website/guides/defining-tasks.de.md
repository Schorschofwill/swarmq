# Tasks definieren

Du willst einen Task definieren, der auf einem Worker läuft. Ein
SwarmQ-Task ist eine Python-Klasse, die von `Task` erbt und ihre
Identität in einer `TaskInfo` deklariert.

Dieser Guide behandelt den Aufbau einer Task-Definition, das Routing,
Argumente und Ergebnisse sowie die Anti-Pattern, die in Produktion
wehtun.

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

Beim Import des Moduls gelten drei Regeln:

1. **Von `Task` erben.** Das registriert die Klasse; es gibt keinen
   Decorator und keinen `register()`-Aufruf.
2. **`task_info` deklarieren.** Eine Klasse ohne wird nicht
   registriert und bleibt für Worker unsichtbar. Nutze eine solche
   Klasse als gemeinsame Basis für Hilfsfunktionen.
3. **`async def run(self, ...)` definieren.** Seine Parameter sind die
   Argumente, die Clients an `schedule()` übergeben, positionell oder
   als Keyword.

## Prüfung beim Import

`TaskInfo` prüft jedes Feld, wenn die Klasse definiert wird. Ein
falscher Wert wirft `ValueError` beim Import des Task-Moduls, nicht
beim ersten `schedule()`: eine leere `queues`-Liste, ein `lock` ohne
`hard_timeout`, `soft_timeout` oder `hard_timeout` nicht größer als 0,
ein Cron außerhalb der crontab(5)-Syntax oder ohne nächsten Termin,
eine Frist über zehn Jahre (`315360000` s) und mehr. `swarmq worker`
endet dann mit Exit `1` und wird bis zum Neustart-Limit neu gestartet.
Fang das vor einem Deploy ab; `--check` importiert das Modul und meldet
den Fehler als `"ok": false`:

```bash
swarmq worker --module myapp.tasks --check
```

Jedes Feld mit Typ und Standardwert:
[`TaskInfo`-Felder](../reference/configuration.md#taskinfo-felder);
die vollständige Liste der Importfehler:
[Validierungs-Verhalten](../reference/configuration.md#validierungs-verhalten).

## Name und Klassenname

`TaskInfo.name` (1 bis 255 Zeichen) ist der Bezeichner auf der
Leitung. Clients übergeben ihn an `schedule("name", ...)`, und er
erscheint in Nachrichten, Worker-Logs und `swarmq inspect`. Der
Python-Klassenname spielt keine Rolle: `SendEmail` in `EmailSender`
umzubenennen, bricht nichts.

**`name` umzubenennen ist ein Breaking Change** für Nachrichten, die
schon in der Queue liegen: Wird `send_email` zu `send_email_v2`,
registriert kein Worker mehr `send_email` für die Nachrichten, die
Clients noch senden. Halte den alten Namen am Leben, wie unter
[Einen Task ablösen](#einen-task-ablosen) beschrieben.

## Queue-Routing

`queues` ist eine nicht leere Liste. Ihr erstes Element ist das Ziel
von `schedule("name")`; ein Client kann einen einzelnen Aufruf mit
`queue=` woandershin schicken:

```python
task_info = TaskInfo(
    name="render_report",
    queues=["reports", "default"],
)
```

```python
# Veröffentlicht auf "reports", dem ersten Element.
await app.schedule("render_report", report_id=42)

# Veröffentlicht auf "default", ein ausdrücklicher Override.
await app.schedule("render_report", report_id=42, queue="default")
```

`queue=` wird nicht gegen die Liste geprüft: Jede Queue, die ein Worker
deklariert hat, funktioniert, eine Queue, die es nicht gibt, wirft
`UnroutableMessage`. Führe trotzdem jede Queue auf, auf der der Task
laufen darf; `swarmq inspect` zeigt sie. Ein Task mit
`priority="high"` oder `"low"` veröffentlicht nach `{queue}.high` oder
`{queue}.low`, auch nach einem Override.

Auf der Worker-Seite konsumiert `swarmq worker --queues reports,default`
beide (Standard: `default`). Wie `delay=` und `eta=` routen, steht unter
[Scheduling und Retries](scheduling-and-retries.md).

### Task in eine andere Queue verschieben

Zieht ein Task in eine andere Queue um oder ändert sich `--queues`
eines Workers, bleiben die Nachrichten in der alten Queue (und
Retry-Kopien, die noch in ihren Delay-Queues warten) ohne Consumer
liegen. SwarmQ meldet das:

- Jeder Worker meldet die Queues, die er konsumiert, an Valkey
  (`[<namensraum>:]sq:{sched}:report:queues`, mit der Uhr von Valkey).
- Der Scheduler-Leader prüft in jedem Übernahme-Takt
  (`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`, Standard 60 s; nicht im Takt
  direkt nach einer Übernahme) die Queues, die seit drei Takten kein
  lebender Worker gemeldet hat, per passivem Declare (ohne
  Management-API). Eine Queue mit Nachrichten und ohne Consumer gilt
  als verwaist.
- Dann loggt der Leader `scheduler_queue_orphaned` (WARNING, einmal je
  Übergang), sendet `SCHEDULER_QUEUE_ORPHANED` und setzt die Gauge
  `swarmq_scheduler_queues_orphaned`; darauf alarmieren.
  `scheduler_queue_orphan_resolved` (INFO) folgt, sobald die Queue
  wieder konsumiert wird oder leer ist.
- `swarmq check` mit `SWARMQ_BROKER_URL` nennt verwaiste Queues und
  gemeldete Queues ohne ihre `.XQ`
  ([CLI](../reference/cli.md#swarmq-check)).

Abhilfe: einen Worker für die alte Queue laufen lassen, bis sie leer
ist, die Nachrichten verschieben oder die Queue löschen. Grenzen:
Geprüft werden nur Queues, die ein Worker ab 0.12 gemeldet hat, und
eine Queue mit irgendeinem Consumer gilt nie als verwaist. Einträge
fehlender oder leerer Queues, die 7 Tage niemand gemeldet hat, werden
entfernt; der ganze Key verfällt 7 Tage nach der letzten Meldung
irgendeines Workers.

## Argumente und Rückgabewerte

### Was Clients übergeben dürfen

Argumente von `run()` reisen als **JSON**, kodiert mit `msgspec`.
`str`, `int`, `float`, `bool`, `None`, `list` und `dict` kommen
unverändert an. Andere Typen werden unterwegs umgewandelt, und der
Task erhält den umgewandelten Wert, nicht den ursprünglichen Typ:

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

Wandle solche Werte in `run()` selbst zurück (etwa mit
`datetime.fromisoformat(...)`) oder übergib sie gleich als `str`.

Ein Objekt, das `msgspec` nicht kodieren kann (eine ORM-Zeile, eine
beliebige Klasse), wird beim Einplanen abgelehnt, bevor etwas
veröffentlicht wird:

- Ein direktes `schedule()` wirft `ResultSerializationError` (der Name
  führt in die Irre; so heißt die Klasse, die der JSON-Encoder wirft).
- Mit `delay=` oder `eta=` gehen die Argumente zuerst in den
  Scheduler-Store; dort wirft dasselbe Objekt einen einfachen
  `TypeError`.
- Mit `unique_for` lehnt der Dedup-Hash zusätzlich `bytes`, Objekte
  eigener Klassen (Dataclass, `Struct`, `Enum`), `dict`-Schlüssel, die
  kein `str` sind, und `nan`/`inf` mit `TypeError` ab.

### Was `run()` zurückgibt

Der Rückgabewert wird mit **msgpack** kodiert und in Valkey unter
`[<namensraum>:]sq:result:{task_id}` gespeichert. msgpack ist
großzügiger als JSON: `datetime`, `date`, `time`, `Decimal`, `UUID`,
`set` und `frozenset` kommen mit demselben Typ zurück, ein `tuple` als
`list`. Beliebige Python-Objekte gehen nicht: Ein Rückgabewert, der
sich nicht kodieren lässt, wirft `ResultSerializationError`, und der
Versuch gilt als gescheitert.

Im Zweifel ein `dict` aus einfachen Werten zurückgeben:

```python
return {"status": "sent", "message_id": message_id, "duration_ms": elapsed}
```

## Reservierte Parameternamen

`delay`, `eta`, `unique_for` und `unique_until` sind
Keyword-Argumente von `app.schedule()`. Ein Task, dessen `run()` einen
davon deklariert, wirft beim Definieren der Klasse `ValueError` mit
einem Hinweis zum Umbenennen:

```python
# ❌ Wirft beim Import:
class Scheduled(Task):
    task_info = TaskInfo(name="scheduled", queues=["default"])

    async def run(self, delay: float) -> None:  # "delay" ist reserviert
        ...

# ✅ Umbenennen:
class Scheduled(Task):
    task_info = TaskInfo(name="scheduled", queues=["default"])

    async def run(self, retry_delay: float) -> None:
        ...
```

Drei weitere Schlüsselwörter gehören zu den Aufrufen, die einen Task
starten; swarmq lehnt sie auf `run()` aber nicht ab:

- `queue`: `app.schedule()` und `app.run()` lesen es als Ziel-Queue
  und geben es nicht weiter. `schedule("export", queue="eu")` routet
  den Task in die Queue `eu`: Gibt es sie nicht, scheitert der Publish
  mit `UnroutableMessage`, sonst läuft der Task ohne das Argument
  (`TypeError` bei einem Pflichtparameter).
- `timeout`, `polling`: `app.run()` liest sie für das Warten auf das
  Ergebnis und gibt sie nicht weiter.

Benenne einen solchen Parameter um oder übergib den Wert positionell
(`schedule("export", "eu")`).

## Lifecycle-Hooks

Überschreibe `pre_execution` und `post_execution`, um auf dem Worker
Code vor und nach `run()` auszuführen. `post_execution` läuft nur,
wenn `pre_execution` und `run()` erfolgreich waren. Es läuft nicht,
wenn eines davon wirft, nicht bei Soft- oder Hard-Timeout und nicht
bei einem Cancel; eine Ausnahme darin macht den Versuch zum
Fehlschlag. Nutze es für Folgearbeit nach Erfolg.

Aufräumen, das bei jedem Ausgang laufen muss, gehört in
`try/finally` in `run()`:

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
bekommt auch die Ausnahme, für Commit oder Rollback. Für Belange, die
für **jeden** Task gelten, nutze [Middleware](middleware.md); ihr
`on_error` läuft bei jedem Fehlschlag, nicht aber bei einem Cancel
(dafür das Signal `TASK_CANCELLED` behandeln).

## Fortschritt melden

Rufe `self.update_progress(current, total, message="")` in `run()`
auf, um Clients den Fortschritt zu melden:

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

Fortschritt ist best effort: Schreibvorgänge sind auf einen je 500 ms
und Task-Instanz gedrosselt, die letzte Meldung (`current == total`)
geht immer durch, und ein Valkey-Fehler wird geloggt
(`update_progress_hset_failed`), statt den Task scheitern zu lassen.
Der Eintrag lebt so lange wie das Ergebnis (`result_ttl`). Die
Client-Seite beschreibt
[`app.watch_progress()`](../reference/api.md#get_progress-watch_progress).

## Einen Task ablösen

Um einen Task umzubenennen oder auszumustern, lass den alten Namen
mindestens einen Release-Zyklus lang als schlanke Weiterleitung
registriert:

```python
class SendEmail(Task):
    task_info = TaskInfo(name="send_email_v2", queues=["default"])

    async def run(self, to: str, subject: str) -> str:
        ...


class SendEmailLegacy(Task):
    """Veraltet, nutze send_email_v2. Bleibt für Nachrichten unterwegs."""

    task_info = TaskInfo(name="send_email", queues=["default"])

    async def run(self, *args, **kwargs) -> str:
        # An die neue Implementierung delegieren.
        return await SendEmail().run(*args, **kwargs)
```

Rolle die Worker mit dem neuen Namen aus, bevor die Producer ihn
senden. Lösche die Legacy-Klasse, sobald die Queue leer ist und die DLQ
keine `send_email`-Einträge mehr hält (`swarmq dlq list --queue default
| grep send_email` ist leer; bei einer großen DLQ `--limit` erhöhen).
Eine Nachricht, die später für einen Namen ankommt, den kein Worker
registriert, wird `SWARMQ_UNKNOWN_TASK_GRACE_S` lang (Standard
15 Minuten) zurückgestellt und dann in die DLQ gelegt
([unregistrierte Tasks](dead-letter-queues.md#unregistrierte-tasks-und-rolling-deploys)).

## Anti-Pattern

### Veränderliche Klassen-Defaults

```python
class Bad(Task):
    task_info = TaskInfo(name="bad", queues=["default"])
    cache: dict = {}  # ❌ von allen Task-Läufen geteilt

    async def run(self, key: str) -> str:
        self.cache[key] = "value"  # wächst für immer
        return self.cache[key]
```

Der Worker erzeugt je Lauf eine neue Instanz, Klassenattribute sind
aber geteilt. Nutze einen Cache auf Modulebene mit ausdrücklicher
Größe oder TTL oder halte Zustand über eine eigene Verbindung in
Valkey.

### Threads oder Prozesse in `run()`

Starte aus einem Task keine Threads oder Prozesse. SwarmQ führt schon
`--concurrency` Tasks je Worker-Prozess aus und kann mehrere
Worker-Prozesse betreiben (`--processes`); was du darin startest,
vervielfacht das und erschöpft Connection-Pools.

Für CPU-lastige Arbeit lass den Task auf einem Worker mit
[`--pool cpu`](cpu-mode.md) laufen, der Task-Bodies in Rechenkindern
außerhalb der Event-Loop ausführt, oder gib dem Task eine eigene Queue
mit niedrigerer `--concurrency`.

### Verwaiste `asyncio.Task`-Handles

```python
# ❌ losgelassene Tasks gehen verloren
asyncio.create_task(some_coro())
```

`run()` kehrt zurück, bevor das Kind fertig ist; der Worker wartet
nicht darauf, ein Shutdown kann es abschneiden, und seine Ausnahme
geht verloren. Warte die Coroutine direkt mit `await` ab oder plane
sie mit `schedule()` als eigenen SwarmQ-Task ein.

### Globale Werte, die von der Prozess-Identität abhängen

```python
# ❌ `process_id` wird beim Import des Moduls festgehalten
process_id = f"worker-{os.getpid()}"
```

Mit `--processes > 1` und `--reload` ändern sich die PIDs der Worker
bei jedem Neustart. Lies den Wert in `run()`, wenn du ihn brauchst.

## Prüfen

```bash
swarmq inspect --module myapp.tasks
```

Jeder Task sollte mit `QUEUES`, `DELIVERY`, `RETRIES`, `RATE_LIMIT`,
`LOCK`, `UNIQUE_FOR`/`UNIQUE_UNTIL` und `CRON` wie deklariert
erscheinen (`--format json` zeigt jedes Feld). Fehlt ein Task, wurde
sein Modul nicht importiert; korrigiere den `--module`-Pfad.

## Siehe auch

- [`TaskInfo`-Felder](../reference/configuration.md#taskinfo-felder):
  jede Option mit Typ und Standardwert
- [Scheduling und Retries](scheduling-and-retries.md): Retry-Policy,
  Verzögerungen und Crons
- [Tasks testen](testing-tasks.md): einen `Task` ohne Broker
  unit-testen
