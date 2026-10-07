# Designentscheidungen

SwarmQs öffentliche Oberfläche ist bewusst klein. Jede Entscheidung
auf dieser Seite war ein expliziter Trade-off zwischen Flexibilität
und einer Library, die auch nach einem Jahr in einem echten Team
noch verständlich bleibt. Das Format: **Entscheidung**, geprüfte
Alternativen, warum SwarmQ diese gewählt hat.

## Async-first-API, kein Sync-Adapter

**Entscheidung.** Jede öffentliche Coroutine ist `async def`. Es
gibt keinen synchronen Wrapper, der im Hintergrund eine Event-Loop
laufen lässt.

**Geprüfte Alternativen.** Ein Sync-Shim (die Celery-
Kompatibilitäts-Form), damit Tasks aus jedem Python-Code geplant
werden können. Zwei APIs — eine sync, eine async — nebeneinander.

**Warum nur async.** Python-3.11+-asyncio ist für die I/O-gebundene
Arbeit, die eine Task-Queue tatsächlich verrichtet, konkurrenzfähig
zu `anyio`, uvloop und threaded Executors. Ein Sync-Shim würde ein
`asyncio.run()`-pro-Aufruf-Muster erzwingen, das in langlebigen
Prozessen Event-Loops leakt und still zerbricht, wenn es aus einer
bestehenden Loop aufgerufen wird. Beide Formen anzubieten
verdoppelt die Testmatrix und ermutigt Nutzer, die weniger sichere
zu wählen.

**Was dich das kostet.** Wenn deine App nicht schon async ist,
brauchst du einen Entrypoint. Für Einmal-Scheduler:
`asyncio.run(main())` innerhalb `main()`. Für Web-Apps: den
Async-Lifespan deines Frameworks (FastAPI, Starlette, Quart,
Django Async Views).

## Klassen-basierte `Task` mit deklarativem `TaskInfo`

**Entscheidung.** Tasks erben von `swarmq.Task` und deklarieren
ihre Identität über ein Klassen-Level-Feld `task_info =
TaskInfo(...)`. Registrierung passiert automatisch bei der
Klassendefinition.

**Geprüfte Alternativen.** Dekorierte Async-Funktionen
(`@app.task`), die Form, die Celery und arq nutzen.

**Warum Klassen.** Task-Metadaten — Queue-Routing, Retry-Policy,
Rate-Limit, Lock-Key, Timeouts, Cron-Expression, Dedup-TTL — sind
viel deklarative Config, um sie durch einen Decorator zu fädeln.
Sie auf eine Dataclass zu legen, hält Tasks inspizierbar:
`SendEmail.task_info.max_retries` ist ein einfaches Attribut-Read.
Es macht auch `swarmq inspect --module myapp.tasks` trivial: laufe
durch die registrierten Klassen und drucke ihr `task_info`.

**Was dich das kostet.** Etwas mehr Zeremonie pro Task (drei Zeilen
Klassendefinition statt eines Decorators). Im Gegenzug ist jeder
Task ein stabiler Typ, über den dein IDE, Typechecker und Tests
reasonieren können.

## JSON auf dem Draht, msgpack im Speicher

**Entscheidung.** Task-Nachrichten werden als `msgspec`-kodiertes
JSON transportiert. Task-Ergebnisse werden mit `msgpack` kodiert.
Keine Unterstützung für `pickle`, protobuf oder Avro.

**Geprüfte Alternativen.** `pickle` (Celerys Default, rundheraus
abgelehnt — es deserialisiert beliebige Klassen, jede
Deserialisierung ist ein potenzieller RCE). Protobuf (Schema-
Registry-Overhead für eine Single-Language-Library). JSON für
beides (Ergebnisse können Datetimes, Decimals, Bytes enthalten —
msgpack verarbeitet die nativ).

**Warum diese Aufteilung.** JSON-auf-dem-Draht bedeutet, dass eine
Nachricht ohne Tooling inspizierbar ist — `rabbitmqadmin get` oder
`swarmq dlq inspect` zeigen dir einen lesbaren Payload.
Msgpack-im-Speicher hält die nativen Python-Typen, die dein Task
zurückgegeben hat, intakt, ohne dass du von Hand serialisieren
musst. Beide Formate sind schnell, sicher und deterministisch.

**Was dich das kostet.** Task-Argumente müssen JSON-kompatibel sein.
Alles Exotische (NumPy-Arrays, Custom Classes) muss vor dem
Scheduling konvertiert werden. Das ist ein Feature, kein Bug: es
zwingt deinen Task-Vertrag, sprach-neutral zu bleiben.

## Separates Backend für Ergebnisse

**Entscheidung.** Ergebnisse leben in Valkey, nicht in RabbitMQ. Der
einzige Job des Brokers ist, Task-Nachrichten zu transportieren.

**Geprüfte Alternativen.** Ergebnisse in eine RPC-Reply-Queue zu
publishen (Celerys Default-Modus mit
`CELERY_TASK_IGNORE_RESULT=False`). Eine Results-Only-Queue mit
Correlation-ID-Matching.

**Warum ein separates Backend.** RabbitMQ-Queues sind schwache
Key-Value-Stores: ein Ergebnis zu konsumieren mutiert State,
Ergebnisse haben keine TTL, und die Frage „Was war das Ergebnis
von Task X" erfordert eine Suche. Valkey gibt dir `GET
sq:result:{task_id}` mit O(1)-Lookup und einer beim Schreiben
gesetzten TTL. Es ist auch der Ort, an dem Rate-Limiter,
Lock-Manager und Scheduler bereits leben — ein Connection-Pool,
nicht zwei.

**Was dich das kostet.** Einen Service mehr zu betreiben. SwarmQ
liefert eine `docker-compose.yml`, die beide für die Entwicklung
co-lokiert.

## Keine Celery-Kompatibilitäts-Shims

**Entscheidung.** SwarmQ bietet keinen Migrations-Layer für
Celery-Apps. Deine Tasks sind nicht plug-kompatibel mit
`@celery.task`.

**Geprüfte Alternativen.** Ein `swarmq.compat.celery`-Modul, das
Celerys API auf SwarmQs Interna abbildet, damit eine migrierende
Codebasis ihre bestehenden `@app.task(bind=True)`-Decorators
behalten kann.

**Warum kein Shim.** Celerys öffentliche Oberfläche ist riesig —
Request-Context, Eager-Mode, Routers, die Canvas-DSL, Custom
Serializer, Custom Result Backends, Signal-Namen, Exception-
Klassen. Einen sinnvollen Subset abzudecken lässt Nutzer mit dem
überrascht zurück, was unterstützt ist und was nicht, und der
Shim-Code ist größer als SwarmQ selbst. Ein sauberer Port, der
idiomatisches SwarmQ schreibt, ist sowohl einfacher als auch
produziert Code, der besser altert.

**Was dich das kostet.** Der initiale Port ist echte Arbeit:
schreibe `@app.task` als `class Task(swarmq.Task)` um und passe
die Task-Signatur an. Was du zurückbekommst, ist eine kleinere,
besser vorhersagbare Library.

## Ein Queue-Typ per Default: RabbitMQ-Quorum-Queues

**Entscheidung.** Queues werden als
[Quorum-Queues](https://www.rabbitmq.com/quorum-queues.html)
deklariert, außer `queue_type="classic"` ist gesetzt.

**Geprüfte Alternativen.** Classic-Mirrored-Queues (upstream
deprecated). Streams (Append-Log-Semantik, falsche Form für Tasks).

**Warum Quorum.** Raft-basierte Replikation mit deterministischem
Failover, native `x-delivery-limit`-Unterstützung, natives
DLX-Handling. Keine Mirrored-Queue-Eigenheiten, kein
Mirror-Policy-Tuning, keine Split-Brain-Edge-Cases. Für eine
Workload in der Größenordnung der SwarmQ-Ziel-Deployments sind
Quorum-Queues die Default-Empfehlung von RabbitMQ selbst.

**Was dich das kostet.** Sehr leicht höherer Per-Message-Overhead
als eine Classic-Queue auf einem einzelnen Node. Im Gegenzug ist
das Überleben eines Node-Verlusts eingebaut.

## Signals als In-Process-Pub/Sub, kein Broker

**Entscheidung.** SwarmQs `Signal`-System ist in-process:
registrierte Handler werden im Worker (oder Client) ausgeführt,
der das Event emittiert hat.

**Geprüfte Alternativen.** Lifecycle-Events an ein Broker-Topic zu
publishen, damit mehrere Services abonnieren können.

**Warum in-process.** Signals in dieser Library decken operative
Belange ab — Metriken, Logging, Tracing, Sentry-Integration. Diese
Konsumenten leben ohnehin im Worker, also ist ein Netzwerk-Hop
reiner Kostenfaktor. Ein Broker-publiziertes Event-System ist eine
separate Sorge, die Anwendungscode auf Basis des eigenen Topics
bauen kann.

**Was dich das kostet.** Nichts an der Signal-Grenze. Wenn du
cross-process-Events willst, `schedule()` du einen Folge-Task aus
deinem Handler — was die Form ist, die du ohnehin wolltest.

## Retries gehen durch eine Delay-Queue, nie `requeue=True`

**Entscheidung.** Jeder Retry publisht eine frische Nachricht in
eine Delay-Queue und lässt RabbitMQ vor der Redelivery warten.
SwarmQ ruft auf dem Primär-Retry-Pfad nie
`basic.nack(requeue=True)` auf.

**Geprüfte Alternativen.** Requeue-an-Ort-und-Stelle mit
client-seitigem Sleep vor dem Ack. Retry innerhalb des
`run()`-Aufrufs.

**Warum die Delay-Queue.** `requeue=True` stellt die Nachricht
sofort irgendeinem verfügbaren Consumer zu, was den Broker in
einer Hot-Loop ausbeutet und das Retry-Budget in Millisekunden
verbrennt. Eine Delay-Queue gibt dir gratis exponentielles /
lineares / festes Backoff und hält die Prefetch-Slots des Workers
offen für andere Arbeit.

Dasselbe gilt, wenn Valkey bei einer Prüfung vor der Ausführung
(Schedule-Fire-Dedup, Cancel-Check, Rate-Limit, Lock) nicht erreichbar
ist: Der Worker stellt eine Kopie mit eigenem Backoff über die
Delay-Queue zurück, statt an derselben Stelle neu einzureihen; auf
einem Broker, der Requeues zählt, wäre das Zustelllimit sonst nach
etwa 20 Runden aufgebraucht. Siehe
[Zustellsemantik](delivery-semantics.md#was-den-ack-abbricht).

**Was dich das kostet.** Leicht mehr AMQP-Traffic pro Retry (ein
zusätzliches Publish + ein DLX-Route). Im Gegenzug hören
Retry-Stürme auf zu existieren.

## `--module` ist zwingend

**Entscheidung.** Jeder `swarmq worker`-Aufruf verlangt
`--module package.name`. Es gibt keine Auto-Discovery von Tasks im
Working Directory.

**Geprüfte Alternativen.** Scannen von `sys.path`, Lesen einer
Config-Datei oder Übernahme von `celery -A`-Auto-Magic.

**Warum explizit.** Der Modul-Import ist, wo deine Task-Registry
gebaut wird. Explizit zu sein bedeutet:

- CI und Container-Images deklarieren es einmal, an einer Stelle.
- Ein Typo zeigt sich als `ImportError` beim Prozess-Start, nicht
  als leere Registry drei Stunden später.
- `swarmq inspect --module ...` nutzt dasselbe Argument.

**Was dich das kostet.** Ein Flag auf der Kommandozeile. Wert es.

## YAGNI bei Dingen, die offensichtlich wirken

Ein paar Features, die vernünftig klingen, sind bewusst nicht
drin:

- **Workflow-Primitive** (`chain`, `group`, `chord`). Celerys
  Canvas-DSL ist mächtig, wird aber selten gut genutzt;
  Application-Level-Orchestrierung mit `await app.run(...)`
  innerhalb eines Eltern-Tasks ist besser debugbar.
- **Multi-Broker-Routing.** Ein RabbitMQ pro SwarmQ-App. Brauchst
  du Isolation, betreibe zwei Apps.
- **Datenbank-als-Broker / DB-als-Backend.** Postgres-Queueing ist
  ein feines Werkzeug für kleine Workloads; im SwarmQ-Ziel-Scale
  ist das RabbitMQ-plus-Valkey-Paar die bessere Wahl, und beides
  zu unterstützen würde die Wartungsoberfläche verdoppeln.
- **Eingebautes Web-Dashboard.** Prometheus + deine bestehenden
  Dashboards (Grafana, Datadog, was auch immer läuft) decken das
  korrekt ab. Siehe
  [`swarmq[metrics]`](../reference/cli.md#swarmq-worker) für den
  Prometheus-Exporter.

Falls eins davon für echte Nutzer zu einem echten Blocker wird,
kann die Entscheidung überdacht werden. Bis dahin sind die
laufenden Kosten eines Features, das „irgendwann nützlich ist",
höher als die Kosten, nein zu sagen.

## Siehe auch

- [Architektur](architecture.md) — wie die Drei-Rollen-Topologie
  diese Entscheidungen umsetzt
- [Zustellungssemantik](delivery-semantics.md) — der
  at-least-once-Vertrag, auf dem das Design ruht
