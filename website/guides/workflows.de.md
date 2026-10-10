# Workflows

Du willst mehrere Tasks als eine Arbeitseinheit laufen lassen: abrufen,
dann umwandeln, dann speichern; oder zehn Dateien parallel herunterladen
und sie zusammenführen, sobald alle zehn fertig sind. Workflows setzen
Tasks zu einer Chain, einer Group oder einem Chord zusammen, starten sie
mit einem Aufruf und geben dir ein Handle, mit dem du auf das Ganze
wartest, es inspizierst oder abbrichst.

## Voraussetzungen

- Eine `SwarmQ`-Instanz und ein laufender Worker, der die Queues
  aller Tasks des Workflows konsumiert.
- Die Task-Klassen sind importiert, **bevor** `SwarmQ(...)` konstruiert
  wird, im Client und in jedem Worker. Der Prozess, der einen Schritt
  einreiht, löst dessen Queue aus der Task-Registry auf: der Client für
  die ersten Schritte, der Worker, der den Vorgänger beendet hat, für
  jeden weiteren Schritt.

## Die drei Primitive

| Primitiv | Läuft | Ergebnis |
|---|---|---|
| `chain(a, b, c)` | nacheinander; das Ergebnis jedes Schritts wird den Argumenten des nächsten vorangestellt | das Ergebnis des letzten Schritts |
| `group(a, b, c)` | alle Glieder parallel | Liste der Glied-Ergebnisse in **Definitionsreihenfolge**, nicht in Fertigstellungsreihenfolge |
| `chord(group(a, b, c), body)` | die Header-Group parallel, dann `body` genau einmal mit der Liste der Header-Ergebnisse | das Ergebnis des Bodys |

Alle drei sind aus `swarmq` importierbar:

```python
from swarmq import chain, chord, group, sig
```

## Signaturen: `Task.s()`, `Task.si()`, `sig()`

Ein Workflow-Schritt ist eine `Signature`: ein Task-Name plus gebundene
Argumente. Du baust sie aus einer Task-Klasse:

```python title="myapp/tasks.py"
from swarmq import Task, TaskInfo


class Fetch(Task):
    task_info = TaskInfo(name="fetch", queues=["default"])

    async def run(self, url: str) -> str:
        ...


class Parse(Task):
    task_info = TaskInfo(name="parse", queues=["default"])

    async def run(self, html: str, strict: bool = False) -> dict:
        ...


class Store(Task):
    task_info = TaskInfo(name="store", queues=["default"])

    async def run(self, doc: dict) -> str:
        ...


class Notify(Task):
    task_info = TaskInfo(name="notify", queues=["default"])

    async def run(self, message: str) -> None:
        ...


class Merge(Task):
    task_info = TaskInfo(name="merge", queues=["default"])

    async def run(self, pages: list[str]) -> str:
        ...
```

- `Fetch.s("https://example.com")` — eine **veränderliche** Signatur.
  In einer Chain wird das Ergebnis des Vorgängers als erstes
  Positionsargument eingefügt: `Parse.s(strict=True)` nach `Fetch` läuft
  als `Parse.run(<html>, strict=True)`.
- `Notify.si("done")` — eine **unveränderliche** Signatur. Das Ergebnis
  des Vorgängers wird nicht eingefügt; der Schritt läuft mit genau den
  gebundenen Argumenten.
- `sig("parse", strict=True)` — eine Signatur über den Task-Namen, ohne
  Referenz auf die Klasse. Mit `immutable=True` verhält sie sich wie
  `si`. Der Task muss im einreihenden Prozess trotzdem registriert sein
  (siehe [Voraussetzungen](#voraussetzungen)).

Argumente müssen JSON-serialisierbar sein, wie bei `schedule()`.

## Zusammensetzen

```python
from swarmq import chain, chord, group

from myapp.tasks import Fetch, Merge, Notify, Parse, Store

urls = ["https://example.com/a", "https://example.com/b"]

# Sequentiell: fetch → parse → store, danach eine Benachrichtigung, die
# die gespeicherte ID ignoriert.
pipeline = chain(
    Fetch.s("https://example.com"),
    Parse.s(strict=True),
    Store.s(),
    Notify.si("page stored"),
)

# Parallel: ein Fetch pro URL; das Ergebnis ist eine Liste von Strings
# in URL-Reihenfolge.
fan_out = group(Fetch.s(u) for u in urls)

# Fan-out + Fan-in: der Body bekommt die Liste der Header-Ergebnisse als
# erstes Argument.
merged = chord(group(Fetch.s(u) for u in urls), Merge.s())
```

Der Operator `|` ist eine Kurzform:

```python
Fetch.s(u) | Parse.s()    # dasselbe wie chain(Fetch.s(u), Parse.s())
chain(...) | Store.s()    # dasselbe wie chain(..., Store.s())
group(...) | Merge.s()    # dasselbe wie chord(group(...), Merge.s())
```

`group()` nimmt Glieder als Argumente oder als ein Iterable (eine Liste
oder einen Generator-Ausdruck).

### Verschachteln

Ein Chain-Schritt, ein Group-Glied und ein Chord-Body dürfen selbst
eine Chain, Group oder ein Chord sein:

```python
# Jedes Glied der Group bekommt das Ergebnis von Fetch; das Ergebnis
# der Chain ist die Liste der Group.
chain(Fetch.s(urls[0]), group(Parse.s(), Parse.s(strict=True)))

# Ein zusammengesetztes Group-Glied trägt sein eigenes Ergebnis zur
# Liste bei: [<html von a>, <geparstes b>]
group(Fetch.s(urls[0]), chain(Fetch.s(urls[1]), Parse.s()))
```

Ein Schritt, der in einer Chain auf eine Group oder einen Chord folgt,
bekommt die ganze Liste als erstes Argument.

## Einen Workflow starten: `app.apply()`

```python
async with SwarmQ(broker, backend) as app:
    handle = await app.apply(
        chord(group(Fetch.s(u) for u in urls), Merge.s())
    )
    print(handle.workflow_id)
    result = await handle.get(timeout=60)
```

`apply()` speichert die Topologie in Valkey, publiziert die ersten
Schritte (den ersten Chain-Schritt, alle Group-Glieder oder den
Chord-Header) und gibt sofort ein `WorkflowHandle` zurück. Den Rest
treiben die Worker: Gelingt ein Schritt, reiht der Worker, der ihn
ausgeführt hat, die nächsten Schritte ein, und das zuletzt fertige
Glied einer Group oder eines Chord-Headers löst das Fan-in genau einmal
aus. Der Client-Prozess darf sich beenden; der Workflow läuft weiter.

Jeder Schritt ist eine gewöhnliche Task-Ausführung: Retries, Timeouts,
Rate-Limits und Locks aus seiner `TaskInfo` gelten. Schritt-Nachrichten
werden immer als `AT_LEAST_ONCE` publiziert.

Der Workflow-Zustand lebt so lange wie `result_ttl` der
`SwarmQ`-Instanz, die `apply()` aufgerufen hat (Default 86400 s). Setze
`result_ttl` hoch genug für den ganzen Workflow einschließlich Retries.

## Das `WorkflowHandle`

| Member | Liefert | Hinweise |
|---|---|---|
| `workflow_id` | `str` | Stabile ID; speichere sie, um dich später wieder anzuhängen. |
| `await status()` | `"running"`, `"completed"`, `"failed"` oder `"cancelled"` | |
| `await children()` | `dict[str, dict]` | Ein Eintrag pro eingereihtem Schritt, Schlüssel ist sein Topologie-Pfad (siehe unten): `{"task_id", "status", "result"?}`. `status` ist `"running"` (noch kein Ergebnis), `"completed"` (mit `result`) oder `"failed"`. Leer, bevor ein Schritt eingereiht ist. |
| `await cancel()` | `bool` | Setzt das Cancel-Flag des Workflows; `True`, wenn es neu gesetzt wurde. |
| `await get(timeout=30.0)` | das Endergebnis | Wartet, bis der Workflow terminal ist; siehe [Fehlerfälle](#fehlerfalle). |

Aus jedem Prozess mit der ID wieder anhängen:

```python
handle = await app.workflow(workflow_id)  # WorkflowNotFound, falls nicht vorhanden
print(await handle.status())
```

### Schritt-Pfade

`children()` schlüsselt Schritte nach ihrer Position in der Topologie:
Chain-Schritt `i` ist `/i`, Group-Glied `i` ist `/gi`, der Chord-Header
ist `/h` (seine Glieder `/h/g0`, `/h/g1`, …) und der Chord-Body `/b`.
Verschachtelte Pfade werden aneinandergehängt: In
`chain(Fetch.s(u), group(Parse.s(), Parse.s()))` sind die Group-Glieder
`/1/g0` und `/1/g1`. Derselbe Pfad ist der `step_path` in
Middleware-Kontexten und im Signal `WORKFLOW_STEP_ENQUEUED`.

## Abbrechen

`await handle.cancel()` ist kooperativ:

- `get()` wirft umgehend `TaskCancelledError`.
- Schritte, die noch kein Worker abgeholt hat — auch bereits
  eingereihte Group- und Chord-Glieder —, werden beim Abholen gestoppt
  und enden als abgebrochen.
- Ein Schritt, der bereits läuft, läuft zu Ende; der Worker reiht danach
  keine weiteren Schritte ein.

## Grenzen

Geprüft, wenn du den Workflow baust, bevor irgendetwas eingereiht wird;
ein Verstoß wirft `WorkflowValidationError`:

| Grenze | Wert |
|---|---|
| Verschachtelungstiefe (`max_workflow_depth`) | 10. Eine Signatur zählt 1, jede umschließende Chain, Group oder jeder Chord addiert 1. |
| Group-Größe (`max_group_size`) | 1000 Glieder pro Group. |
| Leere Zusammensetzungen | `chain()` und `group()` brauchen mindestens ein Element. |
| Chord-Header | muss eine `group(...)` sein. |
| Zustellmodus | Ein `Task.s()`/`Task.si()` eines `FIRE_AND_FORGET`-Tasks wird abgelehnt: Er bestätigt vor der Ausführung und könnte den Workflow nie weiterschalten. |

Beide Grenzen sind feste Konstanten, keine Konfigurationsoptionen.
Flache tiefere Topologien ab oder teile sehr große Fan-outs auf mehrere
Workflows auf.

## Fehlerfälle

| Was passiert | Ergebnis |
|---|---|
| Ein Schritt scheitert und hat Retries übrig | Er wird wie jeder Task wiederholt; der Workflow läuft weiter. |
| Ein Schritt scheitert terminal (Retries erschöpft, `NoRetry`, Delivery-Limit, `max_age` überschritten, Fehler im Lock-Template, Ende einer Lock-Wartezeit, Task im Worker nicht registriert) | Der Worker markiert den Workflow als `failed`, wenn er den Schritt dead-lettert; `get()` wirft `TaskError` mit der Fehlermeldung des Schritts. Es laufen keine weiteren Schritte, und ein Chord-Body läuft nie, wenn ein Header-Glied gescheitert ist. Ein spätes Duplikat eines Schritts, der schon erfolgreich lief, markiert den Workflow nicht als `failed` (`workflow_fail_skipped_step_done`). |
| Der Workflow-Zustand lief ab, während ein Schritt noch lief | Das Ergebnis des Schritts kann den Workflow nicht mehr voranbringen; der Worker loggt `workflow_state_missing` (WARNING, Feld `workflow_id`). Für lange Workflows `result_ttl` erhöhen. |
| Eine Middleware wirft in `pre_enqueue` für einen der ersten Schritte (im Client) | `apply()` wirft diese Exception; nichts wird gespeichert oder publiziert. |
| Eine Middleware wirft in `pre_enqueue` für einen späteren Schritt (im Worker) | Kein Schritt dieser Front wird publiziert; der Workflow ist `failed`, und `get()` wirft `TaskError` mit der Nachricht der Exception. Siehe [Middleware bei Workflow-Schritten](middleware.md#workflow-schritte). |
| Der Task eines Schritts ist nicht registriert | Im Client wirft `apply()` `TaskNotFoundError`. Im Worker, der den Schritt einreiht, wird der Workflow `failed`, und `get()` wirft `TaskError`. |
| Ein mit `sig()` gebauter Schritt ist `FIRE_AND_FORGET` (oder speichert kein Ergebnis, `result_ttl=0`) | Zur Laufzeit erkannt: Der Workflow ist `failed` (gespeicherter Fehlertyp `WorkflowValidationError`); `get()` wirft `TaskError`. |
| Der Workflow wird abgebrochen | `get()` wirft `TaskCancelledError`. |
| `get()` läuft in den Timeout, während der Workflow noch läuft | `ResultNotReadyError`; der Workflow läuft weiter, rufe `get()` erneut auf. |
| Der Workflow ist fertig, aber sein Endergebnis ist abgelaufen | `WorkflowResultExpired`; erhöhe `result_ttl`. |
| Ein für das Fan-in nötiges Glied-Ergebnis ist abgelaufen oder fehlt | Der Workflow wird `failed` (gespeicherter Fehlertyp `WorkflowResultExpired` oder `WorkflowValidationError`), statt eine unvollständige Liste weiterzugeben; `get()` wirft `TaskError`. |
| Der Workflow-Zustand existiert nicht mehr (nie gestartet oder abgelaufen) | `app.workflow(id)` sowie `status()`/`children()`/`get()` auf einem vorhandenen Handle werfen `WorkflowNotFound`. |

`WorkflowValidationError`, `WorkflowNotFound`, `WorkflowResultExpired`,
`TaskError`, `TaskCancelledError` und `ResultNotReadyError` sind aus
`swarmq` importierbar. `WorkflowStateConflict` (`swarmq.exceptions`)
ist intern im Worker: Nebenläufige Zweige haben ihre Zustandsänderungen
wiederholt gegenseitig überschrieben, und die Schritt-Nachricht wird neu
zugestellt — im Client-Code behandelst du sie nicht.

## Workflows beobachten

- **Logs:** Jedes Event, das während eines Schritts geloggt wird, trägt
  das [Task-Kontext-Feld](../reference/logging.md#task-kontext-felder)
  `workflow_id`.
- **Signals:** Jeder publizierte Schritt feuert `TASK_ENQUEUED` mit
  seinem echten Task-Namen und danach `WORKFLOW_STEP_ENQUEUED` mit
  `workflow_id` und `step_path` (siehe
  [Signals](signals.md#verfugbare-signals)).
- **Middleware:** `pre_enqueue`/`post_enqueue` laufen für jeden
  Schritt, mit `workflow_id` und `step_path` im Message-Kontext.

## Verifizieren

Mit einem laufenden Worker für die Queue `default`:

```python
import asyncio

from swarmq import SwarmQ, chain, group
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

from myapp.tasks import Fetch, Parse  # vor SwarmQ(...) importieren


async def main() -> None:
    async with SwarmQ(
        RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
        ValkeyBackend("valkey://localhost:6379/0"),
    ) as app:
        handle = await app.apply(
            chain(Fetch.s("https://example.com"), group(Parse.s(), Parse.s(strict=True)))
        )
        print(await handle.get(timeout=60))  # Liste mit zwei Parse-Ergebnissen
        print(await handle.status())          # completed
        print(sorted(await handle.children()))  # ['/0', '/1/g0', '/1/g1']


asyncio.run(main())
```

## Siehe auch

- [Middleware](middleware.md#workflow-schritte) — wie `pre_enqueue`
  Workflow-Schritte sieht
- [Signals](signals.md) — `WORKFLOW_STEP_ENQUEUED`
- [Fehlerkatalog](../reference/errors.md) — die Exception-Klassen
- [Zustellungssemantik](../explanation/delivery-semantics.md) — warum
  Schritte unter At-least-once-Zustellung idempotent sein müssen
