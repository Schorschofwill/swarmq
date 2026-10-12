# CPU-Modus (`--pool cpu`)

SwarmQ ist reines asyncio. Ein synchroner Pillow-, numpy- oder
ImageMagick-Aufruf blockiert den Event-Loop und mit ihm alle nebenläufigen
Tasks des Workers, und ein `hard_timeout` erreicht einen blockierten Loop
nie. Der CPU-Modus verlagert Task-Körper aus dem Loop-Prozess in kurzlebige
**Rechenkinder**, die aus einem vorgewärmten `forkserver` geforkt werden.

```bash
swarmq worker --module myapp.tasks --pool cpu --queues images
```

## Was sich ändert

| Aspekt | IO-Worker (Standard) | CPU-Modus |
|---|---|---|
| Wo `run()` läuft | im Event-Loop des Workers | in einem Rechenkind, eines je Task |
| `--concurrency` | Tasks in Flight | Rechenslots; Voreinstellung = dem Worker zugeteilte CPUs (Affinität ∩ cgroup-Quote) geteilt durch `--processes` |
| `hard_timeout` | asyncio-Cancel; kann einen blockierten Loop nicht unterbrechen | tötet die Prozessgruppe des Kindes (SIGTERM, 1 s Gnade, SIGKILL); der Task folgt dem üblichen Retry-/DLQ-Pfad |
| Cancel | asyncio-Cancel | tötet das Kind; der Task endet als abgebrochen, kein Retry |
| Gates, Middleware, Signale, Ack | Loop-Prozess | Loop-Prozess — unverändert |
| `FromDishka[...]`, `Annotated[X, FromComponent(...)]`, Aliase | im Loop aufgelöst | im Kind aufgelöst; der Container entsteht beim Import, wird im Forkserver aber nie aufgelöst. Das Kind importiert das Task-Modul neu, die Annotationen müssen also auch dort bei der Klassendefinition auflösbar sein (siehe [Dependency Injection](../reference/api.md#dependency-injection-dishka)) |

Tasks **ohne** Dependency Injection bekommen ein frisches Kind je Task: Nichts
überlebt einen Task, also überlebt auch kein Leak. Tasks **mit** `FromDishka`
verwenden ein warmes Kind je Slot wieder; es wird recycelt, wenn sein RSS nach
einem Task `SWARMQ_RECYCLE_RSS_MB` überschreitet (Voreinstellung 250 MB im
CPU-Modus) oder nach `SWARMQ_RECYCLE_MAX_TASKS` Tasks (Voreinstellung 1000).
Jedes Recycling wird geloggt und gezählt
(`swarmq_recyclings_total{kind="compute_child",reason=...}`) und löst das
Signal `WORKER_RECYCLING` aus.

## Dimensionierung

- **CPU:** Slots = zugeteilte CPUs. Pillow und BLAS skalieren nur bis zur
  Kernzahl; jedes Kind bekommt einen Thread (`OMP_NUM_THREADS`,
  `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`,
  `MAGICK_THREAD_LIMIT` = `SWARMQ_COMPUTE_THREAD_LIMIT`). Eigene Werte in der
  Umgebung gewinnen.
- **Speicher:** Bedarf ≈ Slots × Recycling-Schwelle + Loop-Prozess + Forkserver
  + ein Ersatzkind, plus ruhende DI-Kinder bei gemischten Workloads. Die
  RSS-Schwelle aus dem cgroup-Limit ableiten und Reserve für den rollierenden
  Ersatz lassen.
- **Latenz vs. Durchsatz:** Ein einzelnes Bild wird mit einem Thread je Kind
  langsamer, der Durchsatz bei Vollauslastung bleibt gleich. Für
  latenzkritische Queues `SWARMQ_COMPUTE_THREAD_LIMIT` anheben.
- **Verbindungen:** N Slots bedeuten N Verbindungspools. APP-scoped Singletons
  leben so lange wie ihr Kind.

## Verhaltensänderungen für Task-Autoren

- Nach einem Kill (`hard_timeout`, Cancel, Kindtod) laufen REQUEST-Scope-
  Finalizer nur, wenn das Kind binnen einer Sekunde auf SIGTERM reagiert. Eine
  offene Datenbanktransaktion rollt der Server beim Verbindungsabbruch zurück;
  Autocommit-Folgen und externe Seiteneffekte nicht. Tasks so schneiden, dass
  ein Kill vor dem letzten Commit keinen sichtbaren Zwischenzustand hinterlässt.
- `hard_timeout` deckt Fork, DI-Aufbau und alle drei Lifecycle-Methoden, nicht
  nur `run()`. `soft_timeout` kann einen blockierenden Körper nicht unterbrechen.
- Rückgabewerte und Exceptions überqueren die Prozessgrenze per Pickle. Ein
  nicht picklebarer Rückgabewert wird als Fehlerergebnis gespeichert (kein
  Retry), genau wie heute ein nicht serialisierbarer Wert. Exceptions behalten
  ihren Typ (`Retry`, `NoRetry`, `retry_exceptions`), wenn er importierbar ist.
- `FromDishka[X] | None = None` hat nie injiziert; seit 0.5.0 verweigert der
  Worker mit solcher Annotation den Start (`ConfigurationError`), in jedem Modus.
  Das gilt ebenso für einen Alias (`Depends[X] | None`), auch unter
  `from __future__ import annotations`.

### Fire-and-forget und ein nicht verfügbarer Rechenslot

Eine Fire-and-forget-Nachricht wird bestätigt, bevor der Task-Körper
übergeben wird. Schließt das Rechenkind für diese Nachricht seinen
Ready-Handshake nie ab (etwa weil der Forkserver gestorben ist), gibt es
keine Nachricht mehr, die erneut zugestellt werden könnte: Der Worker
protokolliert `compute_slot_unavailable_fire_and_forget_dropped`, die
Nachricht ist verloren. At-least-once-Nachrichten werden stattdessen erneut
zugestellt. Wiederholte Handshake-Fehler markieren den Pool als ungesund;
`/ready` meldet dann `compute pool unhealthy`, damit Supervisor oder
Orchestrierung den Worker ersetzen können.

## Umgebung und Startprüfungen

- `MALLOC_TRIM_THRESHOLD_` (131072) und `MALLOC_ARENA_MAX` (2) werden für die
  Rechenkinder gesetzt, bevor der Forkserver startet. Sie hindern glibc daran,
  freigegebenen Speicher festzuhalten; gemessene Kosten rund 14 % Laufzeit je
  Bild. Überschreiben mit `SWARMQ_MALLOC_TRIM_THRESHOLD` / `SWARMQ_MALLOC_ARENA_MAX`.
- Der Forkserver lädt die Module der registrierten Tasks vor und darf beim
  Import nie rechnen: Nach dem Aufwärmen muss er genau einen Thread haben,
  sonst verweigert der Worker den Start (Exit `78`). Die Meldung nennt die
  zusätzlichen Threads und die wahrscheinliche Ursache: SwarmQs
  Opt-in-Log-Queue (`swarmq-log-lsnr`), einen logging-`QueueListener`, den
  deine App beim Import gestartet hat, oder sonst eine BLAS-/OpenMP-Rechnung.
  SwarmQ selbst startet im Forkserver keinen Log-Thread. Wird der
  Forkserver still neu gestartet, läuft dieselbe Prüfung erneut, bevor das
  nächste Rechenkind Arbeit bekommt; mit mehr als einem Thread loggt der
  Worker `forkserver_restarted_multithreaded`, verwirft den Forkserver und
  meldet den Pool auf `/ready` als ungesund. Danach loggt er
  `compute_fork_source_rejected`, hört auf zu konsumieren und fährt
  geordnet herunter; die laufende Nachricht geht ohne Retry-Zählung zurück
  in die Queue. Unter `swarmq worker` endet der Worker-Subprozess mit Code
  `71`, und der Supervisor startet ihn mit frischem Forkserver neu. Mit
  eingebettetem CPU-Pool wirft `Worker.start()` nach dem Herunterfahren
  `swarmq.supervision.ForkSourceRejected`, und deine App entscheidet, wie
  es weitergeht — SwarmQ beendet deinen Prozess nicht.
- Logging, das deine Task-Module beim Import einrichten, folgt derselben
  Regel „App gewinnt“ wie im IO-Pool; `logging.basicConfig()` wirkt dort
  nicht ([Logging](../reference/logging.md#wenn-ein-task-modul-das-logging-konfiguriert)).
- `--pool cpu` ist nicht mit `--autoscale` kombinierbar (spätere Stufe) und
  braucht eine POSIX-Plattform mit `forkserver`. Speicher- und Thread-Prüfung
  lesen `/proc`.
- RabbitMQs `consumer_timeout` (Standard 30 min) begrenzt jede einzelne
  Zustellung. Der Worker warnt beim Start, wenn ein `hard_timeout`
  `SWARMQ_CONSUMER_TIMEOUT_S` übersteigt; für lange Jobs beide zusammen anheben.
  Läuft ein Task trotzdem länger, schließt der Broker den Channel (bis 4.2)
  oder beendet den Consumer (ab 4.3, Quorum-Queues). Ab 0.11.0 meldet der
  Consumer-Wächter den Consumer neu an, und der Ausführungsschutz verhindert,
  dass die neu zugestellte Nachricht neben dem Original läuft; seine
  Running-Marker verlängert der Hauptprozess, nicht das Rechenkind. Details:
  [Consumer-Timeout](../explanation/delivery-semantics.md#consumer-timeout).
- `/ready` meldet einen Eintrag `consumers`: nicht bereit, sobald eine
  konsumierte Queue `SWARMQ_CONSUMER_READY_GRACE_S` (Standard 30 s) ohne
  aktiven Consumer war. `/ready` nie als Liveness-Probe nutzen.
- `/ready` meldet den Forkserver als zusätzliche Abhängigkeit.

## Verhältnis zu `--processes`

`--processes N` startet N Loop-Prozesse unter dem Supervisor, jeder mit
eigenem Forkserver und eigenen Slots; die Slot-Voreinstellung wird durch N
geteilt. Alle Variablen stehen in der [Konfiguration](../reference/configuration.md),
die Flags in der [CLI-Referenz](../reference/cli.md).
