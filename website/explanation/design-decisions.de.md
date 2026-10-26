# Designentscheidungen

Die öffentliche Oberfläche von SwarmQ ist absichtlich klein. Jede
Entscheidung auf dieser Seite tauscht Flexibilität gegen eine
Bibliothek, die ein Team auch nach einem Jahr im Betrieb noch
durchschaut. Jeder Abschnitt folgt demselben Aufbau: die
**Entscheidung**, die Alternativen, der Grund und was sie dich kostet.

Bewusst entschiedenes Verhalten ist in einem Entscheidungsregister im
Repository festgehalten, eine Regel je Entscheidung mit einer Kennung
wie `E-072`. Diese Seite nennt die Kennungen, wo ein Abschnitt auf
einer beruht. Überrascht dich im Betrieb etwas von dieser Liste, ist es
eine bekannte Abwägung, kein Fehler.

## At-least-once, nie exactly-once

**Entscheidung.** Jeder Task läuft mindestens einmal. SwarmQ verringert
Duplikate, wo es kann, verspricht aber keine Ausführung genau einmal.

**Geprüfte Alternativen.** Exactly-once über verteilte Transaktionen
oder einen Fencing-Dienst; at-most-once (Ack vor dem Lauf) als Default.

**Warum.** Kein System aus Queue und Worker kann exactly-once ohne Hilfe
der Anwendung garantieren: Ein Worker kann immer nach der Nebenwirkung
und vor dem Ack sterben. Arbeit still zu verlieren ist schlimmer, als
sie zweimal auszuführen. Also wählt SwarmQ at-least-once und macht
Duplikate dann selten. Der [Ausführungsschutz](delivery-semantics.md#ausfuhrungsschutz)
überspringt eine Neuzustellung einer schon erledigten Ausführung, ein
Replay aus der DLQ ist eine neue Ausführung (E-009), und ein geplantes
Fire, dessen Worker abstürzte, läuft erneut, statt verworfen zu werden
(E-046). `FIRE_AND_FORGET`-Tasks entscheiden sich ausdrücklich für
at-most-once.

**Was es dich kostet.** Task-Körper müssen idempotent sein; siehe
[Idempotenz-Erwartungen](delivery-semantics.md#idempotenz-erwartungen).

## Ack erst nach dem gespeicherten Ergebnis

**Entscheidung.** Ein Worker bestätigt eine `AT_LEAST_ONCE`-Nachricht
erst, wenn ihr terminaler Zustand dauerhaft ist: das Ergebnis in Valkey
gespeichert oder die Retry- oder Dead-Letter-Kopie vom Broker bestätigt.
Scheitert das Speichern des Ergebnisses, wiederholt der Worker es etwa
eine halbe Minute lang, bevor er die Nachricht zurückgibt (E-072). Eine
Dead-Letter-Kopie, die sicher in `{queue}.XQ` liegt, gilt als terminal,
auch wenn danach das Ack des Originals scheitert (E-043).

**Geprüfte Alternativen.** Ack beim Empfang (schnell, verliert bei
jedem Absturz Arbeit); Ack nach `run()`, aber vor dem Speichern
(verliert Ergebnisse bei einem Backend-Ausfall).

**Warum.** Das Ack ist der einzige Punkt, an dem der Broker eine
Nachricht vergisst. Liegt es hinter dem dauerhaften Speichern, bleibt
nach einem Absturz an jeder Stelle entweder die Nachricht oder das
Ergebnis, nie keins von beiden.

**Was es dich kostet.** Ein Valkey-Ausfall, der länger dauert als die
Speicherwiederholung, lässt fertige Tasks erneut laufen, und der
Task-Slot bleibt belegt, solange der Worker wiederholt.

## Retries laufen über Delay-Queues, nie `requeue=True`

**Entscheidung.** Ein Retry publiziert eine frische Kopie der Nachricht
mit eigener Ablaufzeit in eine Delay-Queue und bestätigt das Original,
sobald der Broker die Kopie bestätigt hat. RabbitMQ bringt die Kopie
nach Ablauf nach `{queue}` zurück. Sieben Delay-Stufen von 1 s bis 1 h
verhindern, dass kurze Wartezeiten hinter langen hängen (E-026).
Rückstellungen, die keine Retries sind (Rate-Limit, Lock-Konflikt,
Backend-Ausfall, Ausführungsschutz, unbekannter Task), nutzen denselben
Mechanismus.

**Geprüfte Alternativen.** `basic.nack(requeue=True)` mit einem Sleep im
Worker; Wiederholen innerhalb von `run()`; das Delayed-Message-Plugin
von RabbitMQ.

**Warum.** `requeue=True` gibt die Nachricht sofort dem nächsten freien
Consumer, das lässt den Broker heißlaufen und verbrennt das
Retry-Budget in Millisekunden. Eine Delay-Queue liefert exponentiellen,
linearen oder festen Backoff gratis (`fixed` wartet genau `base_delay`,
ohne Jitter, E-023), hält die Slots des Workers für andere Arbeit frei
und braucht kein Broker-Plugin. Ein Backend-Ausfall stellt über
dieselben Queues zurück, statt an Ort und Stelle neu einzureihen, was
das Zustelllimit nach etwa 20 Runden aufbrauchen würde.

**Was es dich kostet.** Einen zusätzlichen Publish je Retry, sieben
Delay-Stufen je Queue (21 mit Prioritäten) und innerhalb einer Stufe
eine Wartezeit bis zum Vierfachen der verlangten. Wartezeiten über eine
Stunde teilen sich `{queue}.DQ` und können sich gegenseitig blockieren.

## Async-first-API, kein Sync-Adapter

**Entscheidung.** Jede öffentliche Koroutine ist `async def`. Es gibt
keinen synchronen Wrapper, der unter der Haube eine Event-Loop startet.

**Geprüfte Alternativen.** Ein Sync-Shim, damit Tasks aus beliebigem
Python-Code eingeplant werden können; zwei parallele APIs, eine
synchron, eine asynchron.

**Warum nur async.** asyncio auf Python 3.14 ist schnell genug für die
I/O-lastige Arbeit einer Task-Queue. Ein Sync-Shim erzwingt
`asyncio.run()` je Aufruf, das lässt in langlebigen Prozessen
Event-Loops liegen und bricht, wenn es aus einer laufenden Loop
aufgerufen wird. Zwei APIs verdoppeln die Testmatrix und verleiten
dazu, die unsicherere zu wählen. Für CPU-lastige Task-Körper gibt es
den [CPU-Modus](../guides/cpu-mode.md) statt Threads.

**Was es dich kostet.** Ist deine App noch nicht async, brauchst du
einen Einstiegspunkt: `asyncio.run(main())` für Skripte, den async
Lifespan deines Frameworks für Web-Apps (FastAPI, Starlette, Quart,
Django-async-Views).

## Klassenbasierte `Task` mit deklarativem `TaskInfo`

**Entscheidung.** Tasks erben von `swarmq.Task` und deklarieren ihre
Identität in einem Klassenattribut `task_info = TaskInfo(...)`. Eine
Klasse registriert sich bei ihrer Definition, und `TaskInfo` prüft jedes
Feld beim Import des Task-Moduls (E-065).

**Geprüfte Alternativen.** Dekorierte async-Funktionen (`@app.task`),
wie bei Celery und arq.

**Warum Klassen.** Task-Metadaten (Queue, Retry-Policy, Rate-Limit,
Lock, Timeouts, Cron, Dedup, Ablauf) sind viel Konfiguration für einen
Dekorator. Auf einer Dataclass bleiben sie inspizierbar:
`SendEmail.task_info.max_retries` ist ein einfacher Attributzugriff, und
`swarmq inspect` geht einfach die registrierten Klassen durch. Die
Prüfung beim Import macht aus einem Tippfehler im Cron-Ausdruck einen
Startfehler statt eines Crons, der still nie feuert. `run()` darf keine
Parameternamen tragen, die `schedule()` selbst nutzt (`delay`, `eta`,
`unique_for`, `unique_until`); sonst scheitert schon der Bau der Klasse
(E-064).

**Was es dich kostet.** Ein paar Zeilen mehr je Task als ein Dekorator.
Dafür ist jeder Task ein stabiler Typ, mit dem IDE, Typprüfer und Tests
arbeiten können.

## JSON auf dem Draht, msgpack im Speicher

**Entscheidung.** Task-Nachrichten sind JSON, kodiert mit `msgspec`.
Ergebnisse sind msgpack. Kein `pickle`, kein Protobuf, kein Avro.

**Geprüfte Alternativen.** `pickle` (früherer Default von Celery,
sofort verworfen: Deserialisieren kann beliebigen Code ausführen).
Protobuf (eine Schema-Registry für eine Bibliothek in nur einer
Sprache). JSON für beides (Ergebnisse enthalten oft datetimes, Decimals
oder Bytes, die msgpack direkt behält).

**Warum diese Aufteilung.** Eine JSON-Nachricht ist ohne Werkzeug
lesbar: `swarmq dlq inspect` oder die RabbitMQ-Oberfläche zeigen dir den
Inhalt. msgpack behält die Python-Typen, die dein Task zurückgab, ohne
handgeschriebene Serialisierung. Beide sind schnell, sicher und
deterministisch; das hält auch den Dedup-Hash von `unique_for` stabil.

**Was es dich kostet.** Task-Argumente müssen JSON-kompatibel sein.
Wandle NumPy-Arrays oder eigene Klassen vor dem Einplanen um. Das hält
deinen Task-Vertrag sprachneutral.

## Ergebnisse in Valkey, nicht im Broker

**Entscheidung.** Ergebnisse, Fortschritt und Workflow-Zustand liegen in
Valkey. Der Broker trägt nur Task-Nachrichten.

**Geprüfte Alternativen.** RPC-Antwort-Queues; eine Ergebnis-Queue mit
Zuordnung über Correlation-IDs.

**Warum.** RabbitMQ-Queues sind schlechte Key-Value-Speicher: Ein
Ergebnis zu lesen verbraucht es, es gibt keine TTL je Key, und „was war
das Ergebnis von Task X“ verlangt eine Suche. Valkey beantwortet
`GET sq:result:{task_id}` in O(1), mit einer beim Schreiben gesetzten
TTL, und hält ohnehin Rate-Limiter, Locks und Scheduler: ein
Verbindungspool statt zwei.

**Was es dich kostet.** Einen zweiten Dienst mit Regeln: keine
verdrängende `maxmemory-policy`, AOF-Persistenz, kein Cluster-Modus
([Valkey-Anforderungen](../reference/configuration.md#valkey-anforderungen)).

## Beim Start scheitern, nicht um drei Uhr nachts

**Entscheidung.** Ein Worker verweigert den Start mit Exit-Code 78, wenn
seine Konfiguration nicht funktionieren kann: ein ungültiger Wert, ein
Import- oder Syntaxfehler im Task-Modul (E-057), Queue-Argumente, die
der vorhandenen Queue widersprechen (E-016), ein Valkey, das Keys
verdrängen darf (E-005), eine unendliche Frist (E-063). Der Supervisor
startet einen solchen Worker nicht neu. `swarmq check` und
`swarmq worker --check` sagen dieselbe Weigerung voraus, ohne zu
starten (E-006, E-062).

**Geprüfte Alternativen.** Trotzdem starten und warnen; mit Exit 1
abbrechen und den Orchestrator neu starten lassen.

**Warum.** Jeder dieser Fehler zeigt sich sonst Stunden später als
doppelte oder verlorene Arbeit. Exit 78 (`EX_CONFIG`) sagt Orchestrator
und Betreiber, dass ein Neustart nicht hilft.

**Was es dich kostet.** Ein falsch konfigurierter Rollout bleibt
sichtbar stehen: Unter Kubernetes gehen die Pods in
`CrashLoopBackOff`. Lass `swarmq check` vor dem Deployment laufen
([Deployment](../guides/deployment.md)).

## Quorum-Queues mit Dead-Lettering at-least-once

**Entscheidung.** Queues sind
[Quorum-Queues](https://www.rabbitmq.com/docs/quorum-queues), außer
`SWARMQ_QUEUE_TYPE=classic` ist gesetzt. Neue Queues bekommen
`x-dead-letter-strategy=at-least-once` mit `x-overflow=reject-publish`;
vorhandene Queues behalten ihre Argumente, und der Worker warnt (E-017).

**Geprüfte Alternativen.** Klassische gespiegelte Queues (upstream
entfernt), Streams (Append-Log-Semantik, falsche Form für Tasks),
vorhandene Queues automatisch umstellen (nur durch Löschen möglich).

**Warum Quorum.** Raft-Replikation mit vorhersehbarem Failover, ein
natives Zustelllimit und natives Dead-Lettering, ohne Tuning von
Mirror-Policies. RabbitMQ selbst empfiehlt sie für dauerhafte Arbeit.
Ihr Default-Dead-Lettering ist at-most-once und kann bei einem
Leader-Wechsel einen Retry verlieren, daher wählen neue Queues
at-least-once.

**Was es dich kostet.** Etwas mehr Aufwand je Nachricht als eine
klassische Queue auf einem Knoten. Eine vorhandene Queue umzustellen ist
ein ausdrücklicher Betreiberschritt
([Queue-Argumente ändern](delivery-semantics.md#queue-argumente-andern)).

## Workflows ohne Orchestrator

**Entscheidung.** `chain`, `group` und `chord` bauen auf gewöhnlichen
Task-Nachrichten auf. Einen Orchestrator-Prozess gibt es nicht: Der
Worker, der einen Schritt beendet, schaltet den Workflow in Valkey
weiter und publiziert die nächsten Schritte. Schritte müssen ein
Ergebnis speichern, daher werden `FIRE_AND_FORGET`-Schritte abgelehnt
(E-039). Ein Schritt, der endgültig scheitert, lässt sofort den ganzen
Workflow scheitern (E-040).

**Geprüfte Alternativen.** Ein zentraler Orchestrator-Dienst; gar keine
Workflow-Primitive, sodass die Komposition per `await app.run(...)` in
einem Eltern-Task geschieht.

**Warum.** Ein Eltern-Task, der auf seine Kinder wartet, hält die ganze
Zeit einen Slot und verliert bei jedem Absturz seinen Stand. Das
Weiterschalten im Worker braucht keinen zusätzlichen Prozess und erbt
die Zustellgarantien der Schritte. Durch das sofortige Scheitern wartet
`get()` nie auf einen Timeout bei einem Workflow, der schon verloren
ist.

**Was es dich kostet.** Feste Grenzen (Tiefe 10, 1000 Gruppenmitglieder),
einen Workflow-Zustand, der nur so lange lebt wie `result_ttl`, und eine
API-Form (`Task.s()`, `Task.si()`), die an Celerys Canvas angelehnt,
aber nicht damit kompatibel ist. Siehe [Workflows](../guides/workflows.md).

## Ein eingebetteter Scheduler mit Leader

**Entscheidung.** Jeder Worker bettet den Scheduler ein; einer davon
hält einen Leader-Lock in Valkey und publiziert fällige Crons und
verzögerte Tasks. Statische Crons kommen aus `TaskInfo`; Worker melden
sie, und der Leader hält Crons zurück oder entfernt sie, wenn kein
Worker sie mehr meldet (E-014, E-049). Ein kurzer Doppel-Leader wird
hingenommen; der Fire-Claim im Worker verwirft seine doppelten Publishes
(E-047).

**Geprüfte Alternativen.** Ein eigener Scheduler-Prozess (Celery beat),
Crons im Orchestrator (Kubernetes-`CronJob`), ein Konsensdienst für
Fencing.

**Warum.** Ein eigener Prozess ist eine Sache mehr, die man ausrollen,
überwachen und einzigartig halten muss. Jeder Worker kann binnen einer
Lock-TTL übernehmen. Echtes Fencing bräuchte einen Konsensdienst, den
SwarmQ nicht voraussetzt, und der Fire-Claim macht einen doppelten
Publish harmlos.

**Was es dich kostet.** Ein Fire kann nach einem Absturz zweimal laufen
(mindestens einmal, E-046), und Crons pausieren bei einem Failover bis
zu einer Lock-TTL. Setze `SWARMQ_SCHEDULER=off` auf Workern, die nie
führen sollen.

## Signale als In-Process-Pub/Sub, kein Broker

**Entscheidung.** Signal-Handler werden in dem Prozess abgewartet, der
das Ereignis ausgelöst hat (Worker oder Client).

**Geprüfte Alternativen.** Lebenszyklus-Ereignisse in ein Broker-Topic
publizieren, damit andere Dienste sie abonnieren können.

**Warum In-Process.** Signale decken Betriebsbelange ab: Metriken,
Logging, Tracing, Fehlerberichte. Diese Verbraucher leben ohnehin im
Worker, ein Netzwerksprung kostet also nur. Ein dienstübergreifender
Ereignisbus ist ein eigenes Thema, das du auf einem eigenen Topic bauen
kannst.

**Was es dich kostet.** Nichts an der Signalgrenze. Für Ereignisse
zwischen Prozessen plane aus deinem Handler einen Folge-Task ein.

## Ein Cancel ist kein Fehler

**Entscheidung.** Ein abgebrochener Task endet als abgebrochen: Der Task
sieht `asyncio.CancelledError`, der Aufrufer von `get_result()` bekommt
`TaskCancelledError`, die Middleware sieht weder `on_error` noch
`post_execute`, nur das Signal `TASK_CANCELLED` kommt, und kein Retry
folgt (E-068).

**Geprüfte Alternativen.** Einen Cancel als Fehler behandeln, der durch
Retry und Fehler-Hooks läuft.

**Warum.** Ein Cancel ist eine Entscheidung, keine Störung. Ihn als
Fehler zu zählen, löste Retries, Alarme und Fehler-Tracking für etwas
aus, das der Betreiber verlangt hat.

**Was es dich kostet.** Aufräumarbeit, die auch bei einem Cancel laufen
muss, gehört in einen `finally`-Block oder den Finalizer der Middleware,
nicht in `on_error`.

## `--module` ist Pflicht, Hooks kommen über `--configure`

**Entscheidung.** Jeder `swarmq worker` braucht `--module package.name`.
Tasks werden nicht automatisch gesucht. Der Worker baut seine eigene
`SwarmQ`-App; Middleware und Signal-Handler registriert ein Callable,
das du mit `--configure MODULE:CALLABLE` übergibst (E-069).

**Geprüfte Alternativen.** `sys.path` durchsuchen, eine
Konfigurationsdatei lesen, Celerys Suche per `-A app`, eine modulweite
`app` aus dem Task-Modul übernehmen.

**Warum ausdrücklich.** Beim Import des Moduls entsteht die
Task-Registry. Ist er ausdrücklich, deklarieren Container-Images ihn
einmal, ein Tippfehler scheitert beim Start mit Exit 78, statt eine
leere Registry zu hinterlassen, und `swarmq inspect` nutzt dasselbe
Argument. Eine modulweite `app` im Task-Modul gibt es noch nicht, wenn
die CLI ihre eigene baut; darauf registrierte Handler blieben still
wirkungslos.

**Was es dich kostet.** Ein Flag und, wenn du Middleware oder Signale
nutzt, eine kleine Hook-Funktion.

## Der Namensraum trennt Valkey, der vhost trennt RabbitMQ

**Entscheidung.** `SWARMQ_NAMESPACE` präfixt jeden Valkey-Key, jeden
Kanal, den Scheduler-Leader und den Kontrollkanal. Queue-Namen bekommen
nie ein Präfix; Apps, die sich einen RabbitMQ teilen, nutzen getrennte
vhosts (E-012).

**Geprüfte Alternativen.** Auch Queue-Namen präfixen.

**Warum.** Mehrere Apps auf einem Valkey teilten sich Locks,
Rate-Limits, Dedup-Claims und den Leader. RabbitMQ hat mit vhosts schon
eine eigene Trennung, und präfixierte Queue-Namen hätten jede
vorhandene Topologie gebrochen.

**Was es dich kostet.** Zwei Apps im selben vhost teilen sich ihre
Queues, auch mit verschiedenen Namensräumen. Eine App in einen
Namensraum umzuziehen braucht eine geplante Migration
([Namensraum](../reference/configuration.md#namensraum-mehrere-apps-auf-einem-valkey)).

## Keine Celery-Kompatibilitätsschicht

**Entscheidung.** SwarmQ hat keine Migrationsschicht für Celery-Apps;
Tasks sind nicht steckerkompatibel mit `@celery.task`.

**Geprüfte Alternativen.** Ein Modul `swarmq.compat.celery`, das
Celerys API auf SwarmQ abbildet.

**Warum.** Celerys Oberfläche ist riesig: Request-Kontext, Eager-Modus,
Router, Canvas, eigene Serialisierer und Ergebnis-Backends,
Signalnamen, Ausnahmeklassen. Jede brauchbare Teilmenge ließe Nutzer
raten, was unterstützt ist, und die Schicht würde größer als SwarmQ
selbst. Ein sauberer Umbau ist einfacher und altert besser.

**Was es dich kostet.** Einen echten Umbau: Schreibe jedes `@app.task`
als `Task`-Subklasse um und passe seine Signatur an.

## Bewusst weggelassen

- **Eine synchrone Client-API.** Siehe [Async-first](#async-first-api-kein-sync-adapter).
- **Routing über mehrere Broker.** Ein RabbitMQ je App; für Trennung
  zwei Apps betreiben.
- **Datenbank als Broker oder Backend.** Postgres als Queue passt zu
  kleinen Lasten; es neben RabbitMQ und Valkey zu unterstützen,
  verdoppelte den Wartungsaufwand.
- **Ein eingebautes Web-Dashboard.** SwarmQ stellt Prometheus-Metriken
  ([`--metrics-port`](../reference/cli.md#swarmq-worker)),
  Health-Endpunkte und den [Kontrollkanal](../reference/cli.md#kontrollkanal)
  bereit; Dashboards und Steuerdienste bauen außerhalb der Bibliothek
  darauf auf.
- **Valkey-Cluster-Modus.** Keys tragen Hash-Tags, aber SwarmQ läuft und
  wird getestet gegen einen einzelnen Primary.

Wird eines davon zu einem echten Hindernis, lässt sich die Entscheidung
mit einem neuen Argument überdenken. Bis dahin kostet „wahrscheinlich
irgendwann nützlich“ mehr als ein Nein.

## Siehe auch

- [Architektur](architecture.md): wie die Teile diese Entscheidungen
  umsetzen
- [Zustellungssemantik](delivery-semantics.md): der
  at-least-once-Vertrag im Detail
