# Recherche: Middleware-System und Signal/Event-System fuer SwarmQ

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument fuer das Design des Middleware- und Signal-Systems von SwarmQ. Enthaelt eine systematische Analyse von Dramatiq, TaskIQ, Celery und Huey, konkrete Code-Beispiele, und architektonische Empfehlungen.

---

## Inhaltsverzeichnis

### Teil 1: Middleware-System
1. [Dramatiq Middleware (primaere Referenz)](#1-dramatiq-middleware-primaere-referenz)
2. [TaskIQ Middleware](#2-taskiq-middleware)
3. [Celery: Bootsteps, Signals und Task-Hooks](#3-celery-bootsteps-signals-und-task-hooks)
4. [Best Practices fuer SwarmQ Middleware](#4-best-practices-fuer-swarmq-middleware)
5. [Empfohlene Middleware-API fuer SwarmQ](#5-empfohlene-middleware-api-fuer-swarmq)

### Teil 2: Signal/Event-System
6. [Dramatiq: Kein Signal-System](#6-dramatiq-kein-signal-system)
7. [Celery Signals](#7-celery-signals)
8. [Huey Signals](#8-huey-signals)
9. [Architektonische Analyse: Middleware vs. Signals](#9-architektonische-analyse-middleware-vs-signals)
10. [Empfohlenes Signal-System fuer SwarmQ](#10-empfohlenes-signal-system-fuer-swarmq)
11. [Synthese: Gesamtarchitektur Middleware + Signals](#11-synthese-gesamtarchitektur-middleware--signals)

---

# Teil 1: Middleware-System

# 1. Dramatiq Middleware (primaere Referenz)

## 1.1 Middleware-Basisklasse — Vollstaendige API

Dramatiq implementiert das umfangreichste Middleware-System aller untersuchten Libraries. Die Basisklasse definiert 22 Hook-Methoden, die den gesamten Lifecycle abdecken:

```python
class Middleware:
    """Basisklasse fuer Broker-Middleware. Die Default-Implementierungen
    aller Hooks sind No-Ops — Subklassen implementieren nur die Hooks
    die sie brauchen."""

    # --- Properties ---

    @property
    def actor_options(self) -> set[str]:
        """Menge der Optionen die auf jedem Actor konfiguriert werden koennen.
        Middleware kann eigene Optionen definieren die dann bei der
        Actor-Deklaration gesetzt werden koennen."""
        return set()

    @property
    def forks(self) -> list[Callable[[], int]]:
        """Liste von Funktionen die in separaten Forks des Hauptprozesses
        laufen sollen. Wird von Middleware verwendet die eigene Prozesse
        benoetigt (z.B. Prometheus HTTP-Server)."""
        return []

    # --- Message Acknowledgment ---

    def before_ack(self, broker: Broker, message: MessageProxy) -> None:
        """Vor dem Acknowledgen einer Message."""

    def after_ack(self, broker: Broker, message: MessageProxy) -> None:
        """Nach dem Acknowledgen einer Message."""

    def before_nack(self, broker: Broker, message: MessageProxy) -> None:
        """Vor dem Rejecten einer Message (Negative Acknowledgment)."""

    def after_nack(self, broker: Broker, message: MessageProxy) -> None:
        """Nach dem Rejecten einer Message."""

    # --- Actor/Queue Declaration ---

    def before_declare_actor(self, broker: Broker, actor: Actor) -> None:
        """Vor der Actor-Registrierung am Broker."""

    def after_declare_actor(self, broker: Broker, actor: Actor) -> None:
        """Nach der Actor-Registrierung."""

    def before_declare_queue(self, broker: Broker, queue_name: str) -> None:
        """Vor der Queue-Deklaration."""

    def after_declare_queue(self, broker: Broker, queue_name: str) -> None:
        """Nach der Queue-Deklaration (registriert, aber nicht
        unbedingt auf dem Server erstellt)."""

    def after_declare_delay_queue(self, broker: Broker, queue_name: str) -> None:
        """Nach der Deklaration einer Delay-Queue."""

    # --- Message Enqueue ---

    def before_enqueue(self, broker: Broker, message: Message, delay: int) -> None:
        """Vor dem Senden einer Message an den Broker (inkl. Retries)."""

    def after_enqueue(self, broker: Broker, message: Message, delay: int) -> None:
        """Nach dem Senden einer Message an den Broker."""

    # --- Message Processing ---

    def before_delay_message(self, broker: Broker, message: MessageProxy) -> None:
        """Vor dem Verzoegern einer Message im Worker-Speicher."""

    def before_process_message(self, broker: Broker, message: MessageProxy) -> None:
        """Vor der Message-Verarbeitung. Kann SkipMessage werfen
        um die Verarbeitung zu ueberspringen."""

    def after_process_message(
        self,
        broker: Broker,
        message: MessageProxy,
        *,
        result: Optional[Any] = None,
        exception: Optional[BaseException] = None,
    ) -> None:
        """Nach der Message-Verarbeitung. Erhaelt sowohl Ergebnis
        als auch Exception (bei Fehler)."""

    def after_skip_message(self, broker: Broker, message: MessageProxy) -> None:
        """Wird statt after_process_message aufgerufen wenn eine
        Message uebersprungen wurde (via SkipMessage)."""

    # --- Worker Lifecycle ---

    def after_process_boot(self, broker: Broker) -> None:
        """Direkt nach dem Start eines Subprozesses."""

    def before_worker_boot(self, broker: Broker, worker: Worker) -> None:
        """Vor dem Worker-Start."""

    def after_worker_boot(self, broker: Broker, worker: Worker) -> None:
        """Nach dem Worker-Start."""

    def before_worker_shutdown(self, broker: Broker, worker: Worker) -> None:
        """Vor dem Worker-Shutdown."""

    def after_worker_shutdown(self, broker: Broker, worker: Worker) -> None:
        """Nach dem Worker-Shutdown."""

    # --- Thread Lifecycle ---

    def after_consumer_thread_boot(self, broker: Broker, thread: ConsumerThread) -> None:
        """Nach dem Start eines Consumer-Threads."""

    def before_consumer_thread_shutdown(self, broker: Broker, thread: ConsumerThread) -> None:
        """Vor dem Shutdown eines Consumer-Threads."""

    def after_worker_thread_boot(self, broker: Broker, thread: WorkerThread) -> None:
        """Nach dem Start eines Worker-Threads."""

    def before_worker_thread_shutdown(self, broker: Broker, thread: WorkerThread) -> None:
        """Vor dem Shutdown eines Worker-Threads."""
```

**Kritische Erkenntnis:** Dramatiq hat keine Thread-Hooks fuer SwarmQ relevant (async statt Threads), aber die Ack/Nack-Hooks, Worker-Lifecycle-Hooks und Message-Processing-Hooks sind alle uebernehmbar.

## 1.2 Middleware-Ordering im Broker

Der Broker verwaltet Middleware als geordnete Liste mit `before=`/`after=`-Parametern:

```python
class Broker:
    def add_middleware(
        self,
        middleware: Middleware,
        *,
        before: type[Middleware] | None = None,
        after: type[Middleware] | None = None,
    ) -> None:
        """Fuegt Middleware hinzu.
        - Default: Am Ende der Liste angehaengt
        - before=X: Vor Middleware vom Typ X eingefuegt
        - after=X: Nach Middleware vom Typ X eingefuegt
        - Duplikate erlaubt, aber mit Warning geloggt
        - ValueError wenn Referenz-Middleware nicht gefunden"""
```

**Hook-Aufruf-Reihenfolge:**

```python
def emit_before(self, signal: str, *args, **kwargs) -> None:
    """before_*-Hooks: Vorwaerts (Registrierungsreihenfolge)"""
    for middleware in self.middleware:
        getattr(middleware, "before_" + signal)(self, *args, **kwargs)

def emit_after(self, signal: str, *args, **kwargs) -> None:
    """after_*-Hooks: Rueckwaerts (Stack-Semantik)"""
    for middleware in reversed(self.middleware):
        getattr(middleware, "after_" + signal)(self, *args, **kwargs)
```

**Fehlerbehandlung:** `MiddlewareError` wird re-raised, alle anderen Exceptions werden als CRITICAL geloggt aber geschluckt — eine einzelne defekte Middleware kann nicht den gesamten Worker crashen.

## 1.3 Default-Middlewares

Dramatiq registriert 6 Middlewares standardmaessig. Die Reihenfolge ist kritisch:

| Middleware | Zweck | Hooks verwendet |
|---|---|---|
| **AgeLimit** | Verwirft zu alte Messages | `before_process_message` (wirft `SkipMessage`) |
| **TimeLimit** | Bricht zu lang laufende Tasks ab | `before_process_message`, `after_process_message` |
| **ShutdownNotifications** | Signalisiert Worker-Shutdown an laufende Tasks | `before_process_message`, `after_process_message`, `before_worker_shutdown` |
| **Callbacks** | Success/Failure-Callbacks auf andere Actors | `after_process_message` |
| **Pipelines** | Chain-Ausfuehrung (Ergebnis → naechster Actor) | `after_process_message` |
| **Retries** | Automatische Retries mit Exponential Backoff | `after_process_message` |

### AgeLimit — SkipMessage-Pattern

```python
class AgeLimit(Middleware):
    @property
    def actor_options(self) -> set[str]:
        return {"max_age"}

    def before_process_message(self, broker, message):
        actor = broker.get_actor(message.actor_name)
        max_age = message.options.get("max_age") or actor.options.get("max_age") or self.max_age
        if max_age and current_millis() - message.message_timestamp >= max_age:
            self.logger.warning("Message %r hat max_age ueberschritten.", message.message_id)
            message.fail()
            raise SkipMessage("Message age limit exceeded")
```

**Pattern:** `before_process_message` kann `SkipMessage` werfen → Message wird uebersprungen, `after_skip_message` wird statt `after_process_message` aufgerufen.

### Retries — State ueber Message-Options

```python
class Retries(Middleware):
    def __init__(self, *, max_retries=20, min_backoff=15_000, max_backoff=604_800_000, retry_when=None):
        self.max_retries = max_retries
        self.min_backoff = min_backoff
        self.max_backoff = max_backoff
        self.retry_when = retry_when

    @property
    def actor_options(self) -> set[str]:
        return {"max_retries", "min_backoff", "max_backoff", "retry_when", "throws", "on_retry_exhausted"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if exception is None:
            return

        actor = broker.get_actor(message.actor_name)

        # "throws" — erwartete Exceptions die KEINEN Retry ausloesen
        throws = message.options.get("throws") or actor.options.get("throws", ())
        if any(isinstance(exception, t) for t in throws):
            return

        # Retry-Count inkrementieren und in message.options speichern
        retries = message.options.get("retries", 0)
        message.options["retries"] = retries + 1
        message.options["traceback"] = traceback.format_exc()

        # Retry-Limit pruefen
        max_retries = message.options.get("max_retries") or actor.options.get("max_retries") or self.max_retries
        if retries >= max_retries:
            # Alle Retries erschoepft → on_retry_exhausted-Callback
            on_exhausted = actor.options.get("on_retry_exhausted")
            if on_exhausted:
                broker.enqueue(on_exhausted.message_with_options(...))
            return

        # Exponential Backoff berechnen
        delay = min(self.min_backoff * 2**retries, self.max_backoff)
        broker.enqueue(message, delay=delay)
```

**Kritische Erkenntnis:** Retries speichern ihren State in `message.options` — nicht in der Middleware-Instanz. Das ist essentiell weil Messages zwischen Worker-Prozessen wandern koennen.

### Pipelines — Ergebnis-Weiterleitung

```python
class Pipelines(Middleware):
    @property
    def actor_options(self) -> set[str]:
        return {"pipe_target", "pipe_ignore"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if exception is not None:
            return  # Bei Fehler keine Pipeline-Weiterleitung

        pipe_target = message.options.get("pipe_target")
        if pipe_target is None:
            return

        next_message = Message(**pipe_target)
        if not message.options.get("pipe_ignore"):
            next_message.args = (*next_message.args, result)  # Ergebnis als Argument anhaengen

        broker.enqueue(next_message, delay=pipe_target.get("delay", 0))
```

### Callbacks — Success/Failure-Routing

```python
class Callbacks(Middleware):
    @property
    def actor_options(self) -> set[str]:
        return {"on_success", "on_failure"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if exception is None:
            callback_name = message.options.get("on_success") or actor.options.get("on_success")
            if callback_name:
                broker.enqueue(callback_actor.message(message.asdict(), result))
        else:
            callback_name = message.options.get("on_failure") or actor.options.get("on_failure")
            if callback_name:
                broker.enqueue(callback_actor.message(message.asdict(), type(exception).__name__, str(exception)))
```

## 1.4 actor_options — Wie Middleware eigene Optionen definiert

Das `actor_options`-Property ist eines der staerksten Features von Dramatiq. Es ermoeglicht Middleware, das Actor-/Message-Schema zu erweitern:

1. Middleware deklariert erlaubte Options-Namen via `actor_options`
2. Bei Actor-Deklaration werden alle `actor_options` aller Middlewares gesammelt
3. Benutzer kann diese Optionen beim Actor oder pro Message setzen
4. Middleware liest sie in Hooks via `message.options.get(...)` oder `actor.options.get(...)`

**Aufloesung (Dramatiq-Pattern, cascading):**
```
message.options["max_retries"]  →  actor.options["max_retries"]  →  middleware.max_retries
```

**Fuer SwarmQ:** Das Aequivalent waere `TaskInfo`-Felder die von Middleware definiert werden. Allerdings ist SwarmQ klassenbasiert — die Options sind direkt in `TaskInfo`, nicht in einem generischen Dict.

## 1.5 forks — Separate Prozesse fuer Middleware

Das `forks`-Property ermoeglicht Middleware, eigene Prozesse zu starten. Dramatiq forkt den Hauptprozess und ruft die Funktion im Kind auf:

```python
class Prometheus(Middleware):
    @property
    def forks(self):
        return [self._start_http_server]

    def _start_http_server(self):
        """Startet einen Prometheus HTTP-Server im eigenen Prozess."""
        start_http_server(self.http_port)
        # Blockiert — laeuft als eigener Prozess
```

**Fuer SwarmQ:** Weniger relevant, da asyncio-basiert. Ein Prometheus-Endpoint wuerde als asyncio-Task im Event-Loop laufen, nicht als separater Prozess.

## 1.6 Prometheus-Middleware — Metrics via Hooks

Die Prometheus-Middleware zeigt wie ein reales Monitoring-System durch Hooks implementiert wird:

| Hook | Metrik |
|---|---|
| `after_process_boot` | Initialisiert Prometheus-Registry und alle Metriken |
| `before_process_message` | Inkrementiert "in_progress"-Gauge |
| `after_process_message` | Dekrementiert "in_progress", inkrementiert Counter, berechnet Duration-Histogram |
| `after_nack` | Inkrementiert "dead_lettered"-Counter |
| `after_enqueue` | Inkrementiert "retried"-Counter (bei Retries) |
| `before_delay_message` | Inkrementiert "delayed"-Gauge |
| `after_worker_shutdown` | Markiert Prozess als tot (Multiprocess-Mode) |

**Aufgezeichnete Metriken:**
- 4 Counter: `messages_total`, `messages_errored`, `messages_retried`, `messages_dead_lettered`
- 2 Gauges: `messages_in_progress`, `messages_delayed`
- 1 Histogram: `message_duration_seconds` (19 Buckets, 5ms bis 900s)
- Alle mit Labels: `queue_name`, `actor_name`

---

# 2. TaskIQ Middleware

## 2.1 Middleware-Basisklasse

TaskIQ hat ein schlankeres System mit 8 Hooks:

```python
class TaskiqMiddleware:
    """Basisklasse fuer Middlewares."""

    def __init__(self) -> None:
        self.broker: AsyncBroker = None  # Wird via set_broker() gesetzt

    def set_broker(self, broker: "AsyncBroker") -> None:
        """Setzt die Broker-Referenz auf der Middleware."""

    # --- Lifecycle ---

    def startup(self) -> None | Coroutine:
        """Beim Broker-Startup. Sync oder async."""

    def shutdown(self) -> None | Coroutine:
        """Beim Broker-Shutdown. Sync oder async."""

    # --- Client-Seite ---

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage | Coroutine[..., TaskiqMessage]:
        """Vor dem Senden an den Worker. Kann Message modifizieren.
        MUSS die (modifizierte) Message zurueckgeben."""
        return message

    def post_send(self, message: TaskiqMessage) -> None | Coroutine:
        """Nach dem Senden an den Broker."""

    # --- Worker-Seite ---

    def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage | Coroutine[..., TaskiqMessage]:
        """Vor der Task-Ausfuehrung. Kann Message modifizieren.
        MUSS die (modifizierte) Message zurueckgeben."""
        return message

    def post_execute(self, message: TaskiqMessage, result: TaskiqResult) -> None | Coroutine:
        """Nach der Task-Ausfuehrung."""

    def post_save(self, message: TaskiqMessage, result: TaskiqResult) -> None | Coroutine:
        """Nach dem Speichern des Ergebnisses im Backend."""

    def on_error(self, message: TaskiqMessage, result: TaskiqResult, exception: BaseException) -> None | Coroutine:
        """Bei einem Fehler waehrend der Task-Ausfuehrung."""
```

## 2.2 maybe_awaitable() — Sync/Async-Kompatibilitaet

TaskIQ unterstuetzt sowohl synchrone als auch asynchrone Middleware-Hooks durch die `maybe_awaitable()`-Hilfsfunktion:

```python
import inspect

async def maybe_awaitable(value):
    """Fuehrt einen Wert aus der entweder ein Coroutine oder ein
    normaler Wert sein kann."""
    if inspect.isawaitable(value):
        return await value
    return value
```

**Verwendung im Receiver:**
```python
# Pre-Execute: Message kann modifiziert werden
for middleware in self.broker.middlewares:
    if middleware.__class__.pre_execute != TaskiqMiddleware.pre_execute:
        taskiq_msg = await maybe_awaitable(middleware.pre_execute(taskiq_msg))
```

**Fuer SwarmQ:** Da SwarmQ rein async ist (Python 3.14+), ist `maybe_awaitable()` **nicht noetig**. Alle Hooks koennen als `async def` erzwungen werden — das vereinfacht den Code und eliminiert den `inspect.isawaitable()`-Check im Hot-Path.

## 2.3 Reversed Order fuer post_*-Hooks (Stack-Semantik)

TaskIQ implementiert konsequent Stack-Semantik — `pre_*`-Hooks laufen vorwaerts, `post_*`-Hooks rueckwaerts:

```python
# Pre-Execute: Vorwaerts [A, B, C]
for middleware in self.broker.middlewares:
    if middleware.__class__.pre_execute != TaskiqMiddleware.pre_execute:
        taskiq_msg = await maybe_awaitable(middleware.pre_execute(taskiq_msg))

# Post-Execute: Rueckwaerts [C, B, A]
for middleware in reversed(self.broker.middlewares):
    if middleware.__class__.post_execute != TaskiqMiddleware.post_execute:
        await maybe_awaitable(middleware.post_execute(taskiq_msg, result))

# On-Error: Rueckwaerts [C, B, A]
for middleware in reversed(self.broker.middlewares):
    if middleware.__class__.on_error != TaskiqMiddleware.on_error:
        await maybe_awaitable(middleware.on_error(message, result, exception))

# Post-Save: Rueckwaerts [C, B, A]
for middleware in reversed(self.broker.middlewares):
    if middleware.__class__.post_save != TaskiqMiddleware.post_save:
        await maybe_awaitable(middleware.post_save(taskiq_msg, result))
```

**Stack-Semantik erklaert:**
```
Middleware A.pre_execute()     ←  Zuerst (aeussere Schicht)
  Middleware B.pre_execute()
    Middleware C.pre_execute()
      --- Task-Ausfuehrung ---
    Middleware C.post_execute()
  Middleware B.post_execute()
Middleware A.post_execute()     ←  Zuletzt (aeussere Schicht)
```

Das entspricht dem Onion-Model (wie HTTP-Middleware in ASGI/Starlette) und stellt sicher, dass Middleware die zuerst einrichtet auch zuletzt aufraeumt.

## 2.4 Performance-Optimierung: Nur ueberschriebene Hooks aufrufen

TaskIQ prueft zur Laufzeit ob ein Hook tatsaechlich ueberschrieben wurde:

```python
if middleware.__class__.pre_execute != TaskiqMiddleware.pre_execute:
    taskiq_msg = await maybe_awaitable(middleware.pre_execute(taskiq_msg))
```

Dieser Vergleich prueft ob die Methode auf der Klasse identisch mit der Base-Implementierung ist. Wenn nicht ueberschrieben, wird der Hook komplett uebersprungen — kein Funktionsaufruf, kein `await`, kein Overhead.

**Fuer SwarmQ (Optimierung beim Startup-Caching):**

```python
# Beim Worker-Start einmalig berechnen welche Middleware welche Hooks hat
class MiddlewareChain:
    def __init__(self, middlewares: list[Middleware]) -> None:
        self._pre_enqueue = [m for m in middlewares if type(m).pre_enqueue is not Middleware.pre_enqueue]
        self._post_enqueue = [m for m in reversed(middlewares) if type(m).post_enqueue is not Middleware.post_enqueue]
        self._pre_execute = [m for m in middlewares if type(m).pre_execute is not Middleware.pre_execute]
        self._post_execute = [m for m in reversed(middlewares) if type(m).post_execute is not Middleware.post_execute]
        self._on_error = [m for m in reversed(middlewares) if type(m).on_error is not Middleware.on_error]
        # ... etc.
```

So wird der Check nur einmal beim Start gemacht — nicht bei jedem Task.

## 2.5 Broker-Zugriff

Jede TaskIQ-Middleware hat ueber `self.broker` Zugriff auf den Broker. Das wird beim Hinzufuegen via `set_broker()` gesetzt:

```python
class MyMiddleware(TaskiqMiddleware):
    async def startup(self):
        # Zugriff auf Broker-Konfiguration, andere Middlewares, etc.
        connection = await self.broker.get_connection()
```

---

# 3. Celery: Bootsteps, Signals und Task-Hooks

Celery hat drei verschiedene Erweiterungsmechanismen die sich ergaenzen aber auch ueberlappen.

## 3.1 Bootstep-System (Komplex, aber maechtig)

Celerys Bootstep-System ist **kein Middleware-System**, sondern ein Component-Lifecycle-Manager mit Dependency-Graph:

```python
class Step:
    """Einzelner Boot-Schritt mit Abhaengigkeiten."""
    name: str                        # Qualifizierter Name (auto-generiert)
    requires: tuple[type[Step], ...]  # Abhaengigkeiten (muessen vorher starten)
    last: bool = False               # Letzter Schritt in der Ausfuehrung?
    conditional: bool = False        # Bedingte Inklusion?
    enabled: bool = True             # Standard-Inklusion

    def include_if(self, parent) -> bool: ...
    def include(self, parent) -> bool: ...
    def create(self, parent): ...

class StartStopStep(Step):
    """Step mit managed Service-Lifecycle."""
    def start(self, parent): ...
    def stop(self, parent): ...
    def close(self, parent): ...
    def terminate(self, parent): ...

class ConsumerStep(StartStopStep):
    """Spezialisierter Step fuer Message-Consumer."""
    def get_consumers(self, channel): ...

class Blueprint:
    """Orchestriert Steps via Dependency-Resolution."""
    def apply(self, parent, **kwargs) -> Blueprint: ...
    def start(self, parent): ...
    def stop(self, parent, close=True, terminate=False): ...
    def restart(self, parent, ...): ...
```

**Dependency-Resolution:**
1. `_firstpass(steps)`: Konvertiert String-Requirements zu Step-Klassen
2. Erstellt `DependencyGraph` aus Step-zu-Requirements-Paaren
3. `topsort()` generiert Ausfuehrungsreihenfolge
4. Steps mit `last=True` werden ans Ende gesetzt
5. Stop/Shutdown laeuft in umgekehrter Reihenfolge

**Bewertung fuer SwarmQ:** Das Bootstep-System ist Overkill fuer SwarmQ. Es loest ein Problem das Celery hat (komplexe Worker-Komponenten die in bestimmter Reihenfolge starten muessen), das SwarmQ durch seine einfachere Architektur nicht hat. Die Worker-Lifecycle-Hooks in der Middleware reichen aus.

## 3.2 Task-Basisklasse-Hooks

Celery hat Hooks direkt auf der Task-Klasse — unabhaengig vom Middleware-System:

```python
class Task:
    def before_start(self, task_id: str, args: tuple, kwargs: dict) -> None:
        """Vor dem Task-Start."""

    def on_success(self, retval: Any, task_id: str, args: tuple, kwargs: dict) -> None:
        """Bei erfolgreicher Ausfuehrung."""

    def on_failure(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo) -> None:
        """Bei Fehler."""

    def on_retry(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo) -> None:
        """Bei Retry."""

    def after_return(self, status: str, retval: Any, task_id: str, args: tuple, kwargs: dict, einfo) -> None:
        """Nach dem Return (egal ob Erfolg oder Fehler)."""

    @classmethod
    def on_bound(cls, app) -> None:
        """Wenn der Task an eine App gebunden wird."""
```

**Fuer SwarmQ:** SwarmQ hat bereits `pre_execution()` und `post_execution()` auf der Task-Klasse. Das ist aequivalent zu Celerys `before_start` und `after_return`. Weitere Task-Level-Hooks (on_success, on_failure) sind weniger noetig wenn das Middleware-System gut genug ist.

## 3.3 Celery Signals — Vollstaendiger Katalog

Celery definiert ueber 30 Signals, organisiert in 6 Kategorien:

### Task-Signals
| Signal | Wann | Argumente |
|---|---|---|
| `before_task_publish` | Vor dem Senden an Broker | sender, body, exchange, routing_key, headers, properties, declare, retry_policy |
| `after_task_publish` | Nach dem Senden an Broker | sender, body, exchange, routing_key |
| `task_received` | Worker empfaengt Task | sender (Consumer), request |
| `task_prerun` | Vor der Ausfuehrung | sender (Task), task_id, task, args, kwargs |
| `task_postrun` | Nach der Ausfuehrung | sender, task_id, task, args, kwargs, retval, state |
| `task_success` | Bei Erfolg | sender, result |
| `task_failure` | Bei Fehler | sender, task_id, exception, args, kwargs, traceback, einfo |
| `task_retry` | Bei Retry | sender, request, reason, einfo |
| `task_revoked` | Task widerrufen | sender, request, terminated, signum, expired |
| `task_rejected` | Task abgelehnt | sender, message, exc |
| `task_unknown` | Unbekannter Task empfangen | sender, name, id, message, exc |
| `task_internal_error` | Interner Fehler | sender, task_id, args, kwargs, request, exception, traceback, einfo |

### Worker-Signals
| Signal | Wann |
|---|---|
| `celeryd_init` | Worker-Daemon initialisiert |
| `celeryd_after_setup` | Worker-Daemon nach Setup |
| `worker_init` | Worker-Prozess initialisiert |
| `worker_before_create_process` | Vor dem Erstellen eines Worker-Prozesses |
| `worker_process_init` | Worker-Prozess gestartet |
| `worker_process_shutdown` | Worker-Prozess faehrt herunter |
| `worker_ready` | Worker bereit Tasks zu verarbeiten |
| `worker_shutdown` | Worker faehrt herunter |
| `worker_shutting_down` | Worker beginnt Shutdown |
| `heartbeat_sent` | Heartbeat gesendet |

### Beat-Signals
| Signal | Wann |
|---|---|
| `beat_init` | Beat-Scheduler initialisiert |
| `beat_embedded_init` | Embedded Beat initialisiert |

### Logging-Signals
| Signal | Wann |
|---|---|
| `setup_logging` | Logging-Setup (kann ueberschrieben werden) |
| `after_setup_logger` | Nach Logger-Setup |
| `after_setup_task_logger` | Nach Task-Logger-Setup |

### Program-Signals
| Signal | Wann |
|---|---|
| `user_preload_options` | Benutzer-Preload-Optionen |
| `import_modules` | Module werden importiert |

### Eventlet-Signals
| Signal | Wann |
|---|---|
| `eventlet_pool_started` | Eventlet-Pool gestartet |
| `eventlet_pool_preshutdown` | Vor Eventlet-Pool-Shutdown |
| `eventlet_pool_postshutdown` | Nach Eventlet-Pool-Shutdown |
| `eventlet_pool_apply` | Eventlet-Pool Task-Zuweisung |

---

# 4. Best Practices fuer SwarmQ Middleware

## 4.1 Soll pre_execute eine Task-Ausfuehrung abbrechen koennen?

**Ja, unbedingt.**

Dramatiq loest das elegant mit `SkipMessage`:
```python
def before_process_message(self, broker, message):
    if should_skip(message):
        raise SkipMessage("Grund")
```

**Empfehlung fuer SwarmQ:**

```python
class SkipTask(Exception):
    """Wird von pre_execute geworfen um einen Task zu ueberspringen."""
    def __init__(self, reason: str = "") -> None:
        self.reason = reason

class Middleware:
    async def pre_execute(self, context: TaskContext) -> None:
        """Kann SkipTask werfen um den Task zu ueberspringen.
        In dem Fall wird on_skip() statt post_execute() aufgerufen."""

    async def on_skip(self, context: TaskContext, reason: str) -> None:
        """Wird aufgerufen wenn ein Task via SkipTask uebersprungen wurde."""
```

**Anwendungsfaelle:**
- **AgeLimit:** Task ist zu alt → `raise SkipTask("max_age ueberschritten")`
- **RateLimit:** Zu viele Tasks → Task wird verzoegert oder uebersprungen
- **Deduplication:** Task mit gleichem unique_key laeuft bereits
- **CircuitBreaker:** Backend nicht erreichbar → Tasks ueberspringen

## 4.2 Zugriff auf Task-Instanz oder nur Message?

**Empfehlung: TaskContext-Objekt das beides enthaelt.**

Dramatiq gibt nur `broker` + `message` an Hooks. TaskIQ gibt `message` + `result`. Beides ist limitiert.

```python
@dataclass(frozen=True)
class TaskContext:
    """Kontext der an alle Middleware-Hooks uebergeben wird."""
    message: TaskMessage        # Die Broker-Message
    task_info: TaskInfo         # Statische Task-Konfiguration (gecacht)
    task_name: str              # Task-Name (Shortcut)
    task_id: str                # Task-ID (Shortcut)
    queue: str                  # Queue-Name
    attempt: int                # Aktueller Versuch (1-basiert)
    worker_id: str              # Worker-Identifikation
    metadata: dict[str, Any]    # Middleware-eigene Metadaten (mutable!)
```

**Warum `metadata`?** Middleware muss State zwischen pre/post-Hooks teilen koennen:

```python
class TimingMiddleware(Middleware):
    async def pre_execute(self, context: TaskContext) -> None:
        context.metadata["timing_start"] = time.monotonic()

    async def post_execute(self, context: TaskContext, result: Any) -> None:
        duration = time.monotonic() - context.metadata["timing_start"]
        metrics.observe("task_duration", duration, task=context.task_name)
```

**Warum NICHT die Task-Instanz?** Middleware soll unabhaengig vom Task-Code sein. Zugriff auf die Task-Instanz wuerde Middleware an die Task-Implementierung koppeln. `TaskInfo` (statisch, gecacht) reicht fuer Konfigurations-Zugriff.

## 4.3 Fehlerbehandlung in Middleware

**Kernfrage:** Was passiert wenn eine Middleware-Exception wirft?

| Szenario | Empfehlung | Begruendung |
|---|---|---|
| `pre_execute` wirft | Task wird NICHT ausgefuehrt, Message wird ge-nacked | Middleware hat ggf. eine Precondition die erfuellt sein muss |
| `post_execute` wirft | Exception wird geloggt, Task-Ergebnis bleibt erhalten | post_execute ist Nachbereitung — der Task war erfolgreich |
| `on_error` wirft | Exception wird geloggt und ignoriert | Fehler-Handler duerfen nicht selbst crashen |
| `pre_enqueue` wirft | Message wird NICHT gesendet, Exception propagiert zum Client | Client muss wissen wenn Senden fehlschlaegt |
| `post_enqueue` wirft | Exception wird geloggt, Message wurde bereits gesendet | Message ist raus — nichts mehr zu tun |

**Implementierung:**

```python
async def _run_pre_execute(self, context: TaskContext) -> None:
    """Pre-Execute: Exceptions propagieren (brechen Task ab)."""
    for middleware in self._pre_execute_chain:
        await middleware.pre_execute(context)

async def _run_post_execute(self, context: TaskContext, result: Any) -> None:
    """Post-Execute: Exceptions werden geloggt aber geschluckt."""
    for middleware in self._post_execute_chain:
        try:
            await middleware.post_execute(context, result)
        except Exception:
            logger.exception(
                "Middleware %s.post_execute() hat eine Exception geworfen",
                type(middleware).__name__,
            )
```

## 4.4 Async-only oder auch Sync unterstuetzen?

**Empfehlung: Nur async.**

| Aspekt | Nur async | Sync + Async (TaskIQ-Ansatz) |
|---|---|---|
| API-Klarheit | Eindeutig: `async def` | Verwirrend: Return-Type ist `None | Coroutine` |
| Hot-Path | Kein `inspect.isawaitable()` noetig | Extra Check pro Hook pro Middleware |
| Implementierung | Einfacher | `maybe_awaitable()` noetig |
| Benutzer-Perspektive | Python 3.14+ ist async-first | Sync-Middleware in async Queue ist Antipattern |

```python
# SwarmQ: Alle Hooks sind async
class Middleware:
    async def pre_execute(self, context: TaskContext) -> None: ...
    async def post_execute(self, context: TaskContext, result: Any) -> None: ...
```

Wenn ein Benutzer synchronen Code in einem Hook ausfuehren will, kann er `asyncio.to_thread()` verwenden — das ist explizit und blockiert den Event-Loop nicht.

## 4.5 Hot-Path-Performance

Der Hot-Path ist: Message empfangen → pre_execute → Task ausfuehren → post_execute → Ergebnis speichern.

**Optimierungen:**

1. **Startup-Caching der Middleware-Ketten** (aus TaskIQ uebernommen):
```python
class MiddlewareChain:
    """Wird einmalig beim Worker-Start berechnet."""

    __slots__ = (
        "_pre_enqueue", "_post_enqueue",
        "_pre_execute", "_post_execute",
        "_on_error", "_on_skip", "_on_retry",
    )

    def __init__(self, middlewares: list[Middleware]) -> None:
        # Nur Middlewares die den Hook tatsaechlich ueberschreiben
        self._pre_execute = tuple(
            m for m in middlewares
            if type(m).pre_execute is not Middleware.pre_execute
        )
        self._post_execute = tuple(
            m for m in reversed(middlewares)
            if type(m).post_execute is not Middleware.post_execute
        )
        self._on_error = tuple(
            m for m in reversed(middlewares)
            if type(m).on_error is not Middleware.on_error
        )
        # ... etc.

    async def run_pre_execute(self, context: TaskContext) -> None:
        for m in self._pre_execute:
            await m.pre_execute(context)

    async def run_post_execute(self, context: TaskContext, result: Any) -> None:
        for m in self._post_execute:
            try:
                await m.post_execute(context, result)
            except Exception:
                logger.exception("Middleware %s.post_execute fehlgeschlagen", type(m).__name__)
```

2. **Tuple statt List** — iterieren ueber Tuples ist ~5% schneller in CPython
3. **Keine Middleware = kein Overhead** — wenn `_pre_execute` leer ist, ist `run_pre_execute` eine leere Schleife

## 4.6 Middleware-Ordering: Prioritaetszahlen vs. Dependency-Graph

| Ansatz | Vorteile | Nachteile | Beispiel |
|---|---|---|---|
| **Registrierungsreihenfolge** (Dramatiq/TaskIQ) | Einfach, vorhersehbar | Benutzer muss Reihenfolge kennen | `add_middleware(A()); add_middleware(B())` |
| **before=/after= Parameter** (Dramatiq) | Flexibel, deklarativ | Kann bei vielen Middlewares komplex werden | `add_middleware(B(), after=A)` |
| **Prioritaetszahlen** | Numerisch sortierbar | Magic Numbers, schwer zu debuggen | `add_middleware(A(), priority=10)` |
| **Dependency-Graph** (Celery Bootsteps) | Maechtig, loest Konflikte | Overkill, schwer zu verstehen | `requires = (OtherMiddleware,)` |

**Empfehlung fuer SwarmQ:** Registrierungsreihenfolge als Default + `before=`/`after=`-Parameter (Dramatiq-Ansatz).

```python
class SwarmQ:
    def add_middleware(
        self,
        middleware: Middleware,
        *,
        before: type[Middleware] | None = None,
        after: type[Middleware] | None = None,
    ) -> None: ...
```

Kein Dependency-Graph noetig. Die Built-in-Middlewares haben eine feste Default-Reihenfolge, und Benutzer-Middleware wird standardmaessig am Ende angehaengt.

## 4.7 Sollen Built-in-Features als Middleware implementiert werden?

| Feature | Als Middleware? | Begruendung |
|---|---|---|
| **Retries** | Ja | Cross-Cutting, konfigurierbar, muss nach Task-Ausfuehrung entscheiden |
| **Timeouts** | Ja | Muss vor/nach Ausfuehrung Timer setzen/aufheben |
| **Rate Limiting** | Ja | Muss vor Ausfuehrung pruefen (pre_execute) |
| **Age Limit** | Ja | Muss vor Ausfuehrung pruefen (pre_execute mit SkipTask) |
| **Deduplication** | Ja | Muss vor Ausfuehrung pruefen (pre_execute mit SkipTask) |
| **Prometheus** | Ja | Klassischer Middleware-Anwendungsfall |
| **CurrentMessage** | Ja | Setzt/raeumt contextvars auf |
| **Ack/Nack** | Nein | Fundamentaler Broker-Mechanismus, kein optionales Feature |
| **Result-Speicherung** | Nein | Fundamentaler Backend-Mechanismus |
| **Serialisierung** | Nein | Zu tief im Hot-Path, keine optionale Erweiterung |

**Vorteil der Middleware-Implementierung:** Built-in-Features sind austauschbar. Ein Benutzer kann z.B. die Retry-Middleware durch eine eigene ersetzen die andere Backoff-Strategien implementiert.

---

# 5. Empfohlene Middleware-API fuer SwarmQ

## 5.1 Vollstaendige Basisklasse

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class SkipTask(Exception):
    """Wird von pre_execute geworfen um einen Task zu ueberspringen."""
    def __init__(self, reason: str = "") -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass
class TaskContext:
    """Kontext der an alle Worker-seitige Middleware-Hooks uebergeben wird.
    Wird pro Task-Ausfuehrung erstellt."""
    message: TaskMessage
    task_info: TaskInfo
    task_name: str
    task_id: str
    queue: str
    attempt: int
    worker_id: str
    metadata: dict[str, Any] = field(default_factory=dict)


class Middleware:
    """Basisklasse fuer SwarmQ-Middleware.

    Alle Hooks sind async. Default-Implementierungen sind No-Ops.
    Subklassen ueberschreiben nur die Hooks die sie brauchen.

    Hook-Reihenfolge:
    - pre_*:  Vorwaerts (Registrierungsreihenfolge)
    - post_*: Rueckwaerts (Stack-Semantik)
    - on_*:   Rueckwaerts (Stack-Semantik)
    """

    # --- Worker Lifecycle ---

    async def on_worker_startup(self, swarmq: SwarmQ) -> None:
        """Beim Worker-Start. Zugriff auf die SwarmQ-Instanz
        fuer Konfiguration, Broker, Backend etc."""

    async def on_worker_shutdown(self, swarmq: SwarmQ) -> None:
        """Beim Worker-Shutdown."""

    # --- Client-Seite (Enqueue) ---

    async def pre_enqueue(self, message: TaskMessage) -> TaskMessage:
        """Vor dem Senden an den Broker. Kann Message modifizieren.
        MUSS die (modifizierte) Message zurueckgeben.
        Exception hier verhindert das Senden."""
        return message

    async def post_enqueue(self, message: TaskMessage) -> None:
        """Nach dem erfolgreichen Senden an den Broker."""

    # --- Worker-Seite (Execute) ---

    async def pre_execute(self, context: TaskContext) -> None:
        """Vor der Task-Ausfuehrung.
        Kann SkipTask werfen um den Task zu ueberspringen.
        Kann context.metadata nutzen um State an post_execute zu uebergeben."""

    async def post_execute(self, context: TaskContext, result: Any) -> None:
        """Nach erfolgreicher Task-Ausfuehrung.
        Exceptions werden geloggt aber geschluckt."""

    async def on_error(self, context: TaskContext, exception: BaseException) -> None:
        """Bei Task-Fehler. Exceptions werden geloggt aber geschluckt."""

    async def on_retry(self, context: TaskContext, exception: BaseException, retry_count: int) -> None:
        """Wenn ein Retry ausgeloest wird.
        Exceptions werden geloggt aber geschluckt."""

    async def on_skip(self, context: TaskContext, reason: str) -> None:
        """Wenn ein Task via SkipTask uebersprungen wurde.
        Wird statt post_execute aufgerufen."""

    # --- Result Backend ---

    async def post_save_result(self, context: TaskContext, result: Any) -> None:
        """Nach dem Speichern des Ergebnisses im Backend.
        Exceptions werden geloggt aber geschluckt."""
```

## 5.2 MiddlewareChain — Gecachte Ausfuehrungsketten

```python
from __future__ import annotations

import structlog
from typing import Any

logger = structlog.get_logger()


class MiddlewareChain:
    """Vorab-berechnete Middleware-Ketten fuer minimalen Hot-Path-Overhead.

    Wird einmalig beim Worker-Start erstellt. Prueft welche Middleware
    welche Hooks ueberschrieben hat und speichert nur die relevanten
    Middleware-Instanzen als Tuples.
    """

    __slots__ = (
        "_pre_enqueue", "_post_enqueue",
        "_pre_execute", "_post_execute",
        "_on_error", "_on_retry", "_on_skip",
        "_post_save_result",
        "_on_worker_startup", "_on_worker_shutdown",
    )

    def __init__(self, middlewares: list[Middleware]) -> None:
        base = Middleware

        # Vorwaerts-Ketten (pre_*)
        self._pre_enqueue = tuple(m for m in middlewares if type(m).pre_enqueue is not base.pre_enqueue)
        self._pre_execute = tuple(m for m in middlewares if type(m).pre_execute is not base.pre_execute)

        # Rueckwaerts-Ketten (post_*, on_*)
        self._post_enqueue = tuple(m for m in reversed(middlewares) if type(m).post_enqueue is not base.post_enqueue)
        self._post_execute = tuple(m for m in reversed(middlewares) if type(m).post_execute is not base.post_execute)
        self._on_error = tuple(m for m in reversed(middlewares) if type(m).on_error is not base.on_error)
        self._on_retry = tuple(m for m in reversed(middlewares) if type(m).on_retry is not base.on_retry)
        self._on_skip = tuple(m for m in reversed(middlewares) if type(m).on_skip is not base.on_skip)
        self._post_save_result = tuple(m for m in reversed(middlewares) if type(m).post_save_result is not base.post_save_result)

        # Lifecycle (Reihenfolge weniger kritisch, laeuft nur einmal)
        self._on_worker_startup = tuple(m for m in middlewares if type(m).on_worker_startup is not base.on_worker_startup)
        self._on_worker_shutdown = tuple(m for m in reversed(middlewares) if type(m).on_worker_shutdown is not base.on_worker_shutdown)

    # --- Client-Seite ---

    async def run_pre_enqueue(self, message: TaskMessage) -> TaskMessage:
        """Pre-Enqueue: Exceptions propagieren zum Client."""
        for m in self._pre_enqueue:
            message = await m.pre_enqueue(message)
        return message

    async def run_post_enqueue(self, message: TaskMessage) -> None:
        for m in self._post_enqueue:
            try:
                await m.post_enqueue(message)
            except Exception:
                logger.exception("middleware.post_enqueue fehlgeschlagen", middleware=type(m).__name__)

    # --- Worker-Seite ---

    async def run_pre_execute(self, context: TaskContext) -> None:
        """Pre-Execute: Exceptions propagieren (brechen Task ab).
        SkipTask wird separat behandelt."""
        for m in self._pre_execute:
            await m.pre_execute(context)

    async def run_post_execute(self, context: TaskContext, result: Any) -> None:
        for m in self._post_execute:
            try:
                await m.post_execute(context, result)
            except Exception:
                logger.exception("middleware.post_execute fehlgeschlagen", middleware=type(m).__name__)

    async def run_on_error(self, context: TaskContext, exception: BaseException) -> None:
        for m in self._on_error:
            try:
                await m.on_error(context, exception)
            except Exception:
                logger.exception("middleware.on_error fehlgeschlagen", middleware=type(m).__name__)

    async def run_on_retry(self, context: TaskContext, exception: BaseException, retry_count: int) -> None:
        for m in self._on_retry:
            try:
                await m.on_retry(context, exception, retry_count)
            except Exception:
                logger.exception("middleware.on_retry fehlgeschlagen", middleware=type(m).__name__)

    async def run_on_skip(self, context: TaskContext, reason: str) -> None:
        for m in self._on_skip:
            try:
                await m.on_skip(context, reason)
            except Exception:
                logger.exception("middleware.on_skip fehlgeschlagen", middleware=type(m).__name__)

    async def run_post_save_result(self, context: TaskContext, result: Any) -> None:
        for m in self._post_save_result:
            try:
                await m.post_save_result(context, result)
            except Exception:
                logger.exception("middleware.post_save_result fehlgeschlagen", middleware=type(m).__name__)

    # --- Lifecycle ---

    async def run_worker_startup(self, swarmq: SwarmQ) -> None:
        for m in self._on_worker_startup:
            await m.on_worker_startup(swarmq)

    async def run_worker_shutdown(self, swarmq: SwarmQ) -> None:
        for m in self._on_worker_shutdown:
            try:
                await m.on_worker_shutdown(swarmq)
            except Exception:
                logger.exception("middleware.on_worker_shutdown fehlgeschlagen", middleware=type(m).__name__)
```

## 5.3 Middleware-Registrierung

```python
class SwarmQ:
    def __init__(self, broker, backend, *, middlewares: list[Middleware] | None = None):
        self._middlewares: list[Middleware] = middlewares or self._default_middlewares()
        self._middleware_chain: MiddlewareChain | None = None  # Wird beim Start gebaut

    def _default_middlewares(self) -> list[Middleware]:
        """Standard-Middlewares in fester Reihenfolge."""
        return [
            AgeLimitMiddleware(),
            TimeoutMiddleware(),
            DeduplicationMiddleware(),
            RateLimitMiddleware(),
            CurrentTaskMiddleware(),
            RetryMiddleware(),
        ]

    def add_middleware(
        self,
        middleware: Middleware,
        *,
        before: type[Middleware] | None = None,
        after: type[Middleware] | None = None,
    ) -> None:
        """Fuegt eine Middleware hinzu.

        Args:
            middleware: Die Middleware-Instanz.
            before: Vor dieser Middleware-Klasse einfuegen.
            after: Nach dieser Middleware-Klasse einfuegen.

        Raises:
            ValueError: Wenn before/after nicht gefunden.
            RuntimeError: Wenn der Worker bereits laeuft.
        """
        if self._middleware_chain is not None:
            raise RuntimeError("Middleware kann nicht nach Worker-Start hinzugefuegt werden")

        if before is not None:
            for i, m in enumerate(self._middlewares):
                if isinstance(m, before):
                    self._middlewares.insert(i, middleware)
                    return
            raise ValueError(f"Middleware {before.__name__} nicht gefunden")

        if after is not None:
            for i, m in enumerate(self._middlewares):
                if isinstance(m, after):
                    self._middlewares.insert(i + 1, middleware)
                    return
            raise ValueError(f"Middleware {after.__name__} nicht gefunden")

        self._middlewares.append(middleware)

    def remove_middleware(self, middleware_type: type[Middleware]) -> None:
        """Entfernt eine Middleware anhand ihres Typs."""
        self._middlewares = [m for m in self._middlewares if not isinstance(m, middleware_type)]

    async def _build_middleware_chain(self) -> None:
        """Wird beim Worker-Start aufgerufen. Baut die gecachte MiddlewareChain."""
        self._middleware_chain = MiddlewareChain(self._middlewares)
```

## 5.4 Beispiel-Implementierungen

### RetryMiddleware

```python
class RetryMiddleware(Middleware):
    """Automatische Retries mit konfigurierbarem Backoff.

    Retry-Konfiguration wird aus TaskInfo gelesen:
    - max_retries: Maximale Anzahl Retries
    - retry_backoff: "exponential", "linear", "fixed"
    - retry_exceptions: Nur diese Exceptions loesen Retries aus
    """

    async def on_error(self, context: TaskContext, exception: BaseException) -> None:
        task_info = context.task_info
        if task_info.max_retries == 0:
            return

        # Nur konfigurierte Exceptions?
        if task_info.retry_exceptions is not None:
            if not isinstance(exception, tuple(task_info.retry_exceptions)):
                return

        if context.attempt > task_info.max_retries:
            return  # Alle Retries erschoepft → DLQ (vom Worker behandelt)

        delay = self._calculate_backoff(context.attempt, task_info)
        await context.broker.requeue(context.message, delay=delay)

    async def on_retry(self, context: TaskContext, exception: BaseException, retry_count: int) -> None:
        logger.warning(
            "Task wird erneut versucht",
            task=context.task_name,
            task_id=context.task_id,
            attempt=retry_count,
            max_retries=context.task_info.max_retries,
            exception=str(exception),
        )

    def _calculate_backoff(self, attempt: int, task_info: TaskInfo) -> float:
        match task_info.retry_backoff:
            case "exponential":
                return min(2 ** attempt, 3600)  # Max 1 Stunde
            case "linear":
                return min(attempt * 10, 3600)
            case "fixed":
                return 10
            case _:
                return min(2 ** attempt, 3600)
```

### SentryMiddleware (Benutzer-definiert)

```python
class SentryMiddleware(Middleware):
    """Sendet Task-Fehler an Sentry."""

    async def on_error(self, context: TaskContext, exception: BaseException) -> None:
        sentry_sdk.set_context("swarmq", {
            "task_name": context.task_name,
            "task_id": context.task_id,
            "queue": context.queue,
            "attempt": context.attempt,
        })
        sentry_sdk.capture_exception(exception)

    async def on_worker_startup(self, swarmq: SwarmQ) -> None:
        sentry_sdk.init(dsn=os.environ["SENTRY_DSN"])
```

### TimingMiddleware (Benutzer-definiert)

```python
class TimingMiddleware(Middleware):
    """Misst Task-Dauer und sendet an StatsD."""

    async def pre_execute(self, context: TaskContext) -> None:
        context.metadata["_timing_start"] = time.monotonic()

    async def post_execute(self, context: TaskContext, result: Any) -> None:
        start = context.metadata.get("_timing_start")
        if start is not None:
            duration_ms = (time.monotonic() - start) * 1000
            statsd.timing(f"swarmq.task.{context.task_name}.duration_ms", duration_ms)

    async def on_error(self, context: TaskContext, exception: BaseException) -> None:
        start = context.metadata.get("_timing_start")
        if start is not None:
            duration_ms = (time.monotonic() - start) * 1000
            statsd.timing(f"swarmq.task.{context.task_name}.error_duration_ms", duration_ms)
```

## 5.5 Integration im Worker-Loop

```python
class Worker:
    async def _process_message(self, raw_message: bytes) -> None:
        message = self._deserialize(raw_message)
        task_info = self._registry.get(message.task_name)
        chain = self._swarmq._middleware_chain

        context = TaskContext(
            message=message,
            task_info=task_info,
            task_name=message.task_name,
            task_id=message.task_id,
            queue=message.queue,
            attempt=message.attempt,
            worker_id=self._worker_id,
        )

        try:
            # 1. Pre-Execute (kann SkipTask werfen)
            await chain.run_pre_execute(context)
        except SkipTask as exc:
            await chain.run_on_skip(context, exc.reason)
            await self._ack(raw_message)
            return
        except Exception:
            await self._nack(raw_message)
            return

        try:
            # 2. Task ausfuehren
            task = self._create_task(task_info)
            await task.pre_execution()
            result = await task.run(*message.args, **message.kwargs)
            await task.post_execution()

            # 3. Post-Execute
            await chain.run_post_execute(context, result)

            # 4. Ergebnis speichern
            await self._save_result(context, result)
            await chain.run_post_save_result(context, result)

            # 5. Ack
            await self._ack(raw_message)

        except Exception as exc:
            # 6. Fehlerbehandlung
            await chain.run_on_error(context, exc)

            # 7. Retry-Logik
            if self._should_retry(context, exc):
                await chain.run_on_retry(context, exc, context.attempt)
                await self._requeue(message, context.attempt)
                await self._ack(raw_message)
            else:
                await self._move_to_dlq(message, exc)
                await self._ack(raw_message)
```

---

# Teil 2: Signal/Event-System

# 6. Dramatiq: Kein Signal-System

Dramatiq hat **kein** dediziertes Signal-System. Alle Cross-Cutting Concerns werden ueber Middleware-Hooks geloest.

**Wie das in der Praxis funktioniert:**

```python
# Statt Signal-Handler:
@on_signal(TASK_COMPLETED)
def notify_slack(task_name, task_id, result):
    slack.post(f"Task {task_name} completed")

# Implementiert man eine Middleware:
class SlackNotificationMiddleware(Middleware):
    def after_process_message(self, broker, message, *, result=None, exception=None):
        if exception is None:
            slack.post(f"Task {message.actor_name} completed")
```

**Vorteile:**
- Ein einziger Erweiterungsmechanismus — weniger API-Oberfläche
- Middleware hat Zugriff auf den vollen Kontext (Broker, Message, Result)
- Middleware-Ordering ist klar definiert

**Nachteile:**
- Jeder noch so kleine Handler erfordert eine eigene Klasse
- Keine Ad-hoc-Registrierung (z.B. in Tests: "nur fuer diesen Test auf TASK_FAILED hoeren")
- Keine Filterung nach Task-Name — Middleware bekommt ALLE Tasks

---

# 7. Celery Signals

## 7.1 Signal-Implementierung

Celery verwendet ein eigenes Signal-System (inspiriert von Django-Signals, nicht blinker):

```python
class Signal:
    def __init__(self, providing_args=None, use_caching=False, name=None):
        self.receivers = []          # Liste von (dispatch_uid, receiver)-Paaren
        self.lock = threading.Lock()  # Thread-Safety
        self.use_caching = use_caching
        self.sender_receivers_cache = weakref.WeakKeyDictionary()  # Per-Sender-Cache

    def connect(self, receiver, sender=None, weak=True, dispatch_uid=None, retry=False):
        """Registriert einen Receiver.
        - weak=True: WeakReference (Default, verhindert Memory-Leaks)
        - dispatch_uid: Verhindert doppelte Registrierung
        - retry=True: Automatischer Retry bei Fehler mit Exponential Backoff
        """

    def disconnect(self, receiver=None, sender=None, dispatch_uid=None):
        """Entfernt einen Receiver."""

    def send(self, sender, **kwargs):
        """Sendet Signal an alle verbundenen Receiver.
        Gibt Liste von (receiver, response)-Tuples zurueck.
        Exceptions werden als Response zurueckgegeben, nicht re-raised."""
```

## 7.2 Registrierung

```python
from celery.signals import task_failure, task_success

# Via Decorator
@task_failure.connect
def handle_failure(sender=None, task_id=None, exception=None, **kwargs):
    sentry_sdk.capture_exception(exception)

# Via Decorator mit Sender-Filter
@task_success.connect(sender="myapp.tasks.send_email")
def on_email_sent(sender=None, result=None, **kwargs):
    metrics.increment("emails_sent")

# Via connect()-Methode
task_failure.connect(handle_failure, sender=None, weak=True)
```

## 7.3 Performance-Aspekte

- **Weak References (Default):** Verhindert Memory-Leaks, aber GC kann Handler entfernen
- **Sender-Caching:** `WeakKeyDictionary` cached Receiver-Listen per Sender
- **Lazy Cleanup:** Tote WeakRefs werden nicht sofort bereinigt, sondern bei naechstem `connect()`/`disconnect()`
- **Thread-Lock:** Jede Signal-Operation akquiriert einen Lock — in async-Kontexten problematisch
- **Exception-Handling:** `send()` faengt Exceptions und gibt sie als Response zurueck — kein Handler kann das Signal-Dispatching abbrechen

---

# 8. Huey Signals

## 8.1 Signal-Konstanten

Huey definiert 13 Signals als String-Konstanten:

```python
SIGNAL_CANCELED = "canceled"        # Task wurde gecancelt
SIGNAL_COMPLETE = "complete"        # Task erfolgreich abgeschlossen
SIGNAL_ERROR = "error"              # Task mit Exception fehlgeschlagen
SIGNAL_EXECUTING = "executing"      # Task-Ausfuehrung begonnen
SIGNAL_EXPIRED = "expired"          # Task wegen TTL abgelaufen
SIGNAL_LOCKED = "locked"            # Task wegen Lock uebersprungen
SIGNAL_RETRYING = "retrying"        # Task wird erneut versucht
SIGNAL_REVOKED = "revoked"          # Task widerrufen
SIGNAL_SCHEDULED = "scheduled"      # Task eingeplant
SIGNAL_INTERRUPTED = "interrupted"  # Task unterbrochen
SIGNAL_ENQUEUED = "enqueued"        # Task in Queue eingereiht
SIGNAL_TIMEOUT = "timeout"          # Task-Timeout erreicht
SIGNAL_RATE_LIMITED = "rate_limited" # Task wegen Rate-Limit verzoegert
```

## 8.2 Signal-Klasse

```python
class Signal:
    __slots__ = ("_receivers",)

    def __init__(self):
        self._receivers = {}  # signal_name -> set(handler_functions)

    def connect(self, receiver, *signals):
        """Registriert einen Handler fuer bestimmte Signals.
        Ohne Signals: Handler fuer 'any' (alle Signals)."""
        if not signals:
            signals = ("any",)
        for signal in signals:
            self._receivers.setdefault(signal, set()).add(receiver)

    def disconnect(self, receiver, *signals):
        """Entfernt einen Handler."""
        if not signals:
            signals = list(self._receivers.keys())
        for signal in signals:
            if signal in self._receivers:
                self._receivers[signal].discard(receiver)

    def send(self, signal, task, *args, **kwargs):
        """Sendet Signal an alle passenden Receiver.
        Receiver fuer das spezifische Signal + 'any'-Receiver."""
        receivers = itertools.chain(
            self._receivers.get(signal, ()),
            self._receivers.get("any", ()),
        )
        for receiver in receivers:
            receiver(signal, task, *args, **kwargs)
```

## 8.3 Decorator-Pattern

```python
class Huey:
    def __init__(self):
        self._signal = Signal()

    def signal(self, *signals):
        """Decorator zum Registrieren von Signal-Handlern."""
        def decorator(fn):
            self._signal.connect(fn, *signals)
            return fn
        return decorator

    def _emit(self, signal, task, *args, **kwargs):
        """Internes Signal-Dispatching mit Fehlerbehandlung."""
        try:
            self._signal.send(signal, task, *args, **kwargs)
        except Exception:
            logger.exception('Fehler beim Senden von Signal "%s"', signal)
```

**Verwendung:**

```python
from huey.signals import SIGNAL_COMPLETE, SIGNAL_ERROR

@huey.signal(SIGNAL_COMPLETE)
def on_complete(signal, task, *args, **kwargs):
    print(f"Task {task.name} completed")

@huey.signal(SIGNAL_ERROR)
def on_error(signal, task, exception=None, **kwargs):
    sentry_sdk.capture_exception(exception)

# Wildcard: Alle Signals
@huey.signal()
def log_all_signals(signal, task, *args, **kwargs):
    logger.info(f"Signal: {signal}, Task: {task.name}")
```

## 8.4 Bewertung

**Staerken:**
- Sehr einfache API — Decorator + String-Konstanten
- Wildcard-Listener (`any`) fuer globales Monitoring
- Exception in Handlern crashed nicht den Worker

**Schwaechen:**
- Keine Filterung nach Task-Name (nur global)
- Rein synchron — in async-Kontext problematisch
- Kein Ordering von Handlern
- Handler koennen das Task-Verhalten nicht beeinflussen (rein observierend)

---

# 9. Architektonische Analyse: Middleware vs. Signals

## 9.1 Fundamentale Unterschiede

| Aspekt | Middleware | Signals |
|---|---|---|
| **Primaerer Zweck** | Verhalten aendern | Ueber Ereignisse informieren |
| **Kann Task abbrechen?** | Ja (SkipTask) | Nein |
| **Kann Message modifizieren?** | Ja (pre_enqueue, pre_execute) | Nein |
| **Kann Ergebnis modifizieren?** | Moeglich | Nein |
| **Registrierung** | Klasse + add_middleware() | Funktion/Decorator |
| **Reihenfolge** | Explizit (Stack-Semantik) | Undefiniert |
| **State zwischen Hooks** | Ja (context.metadata) | Nein |
| **Granularitaet** | Alle Tasks | Filterbar nach Task/Signal-Typ |
| **Typischer Benutzer** | Library-Autor, fortgeschritten | Anwendungs-Entwickler |

## 9.2 Wann was verwenden?

### Middleware ist besser fuer:
- **Retry-Logik** — muss Message re-enqueuen (veraendert Verhalten)
- **Rate Limiting** — muss Task ggf. verzoegern/ablehnen
- **Timeouts** — muss Timer setzen und aufheben (State zwischen pre/post)
- **Tracing/OpenTelemetry** — muss Span vor Task starten und nach Task schliessen
- **Deduplication** — muss Task ggf. ablehnen
- **Prometheus** — muss Dauer messen (State zwischen pre/post)

### Signals sind besser fuer:
- **Slack-/Email-Benachrichtigungen** — reine Benachrichtigung, kein Einfluss auf Task
- **Logging in externen Systemen** — z.B. Audit-Log bei TASK_COMPLETED
- **Metrics-Inkrement** — einfacher Counter ohne Dauer-Messung
- **Test-Assertions** — "warte bis TASK_COMPLETED gefeuert wird"
- **Ad-hoc-Monitoring** — "zeige mir alle fehlgeschlagenen Tasks"

### Kernregel:
> **Middleware** wenn du das Task-Verhalten beeinflussen musst.
> **Signals** wenn du nur reagieren/beobachten willst.

## 9.3 Soll das Signal-System separat von Middleware sein?

**Analyse der drei Optionen:**

### Option A: Nur Middleware (Dramatiq-Ansatz)

```python
# Jeder Handler ist eine Middleware-Klasse
class SlackNotifier(Middleware):
    async def on_error(self, context, exception):
        slack.post(f"Task {context.task_name} fehlgeschlagen")

swarmq.add_middleware(SlackNotifier())
```

**Pro:** Ein Mechanismus, einfacheres System
**Contra:** Overhead fuer simple Handler (eigene Klasse noetig), keine Filterung

### Option B: Nur Signals (kein Middleware)

```python
@swarmq.on_signal(Signal.TASK_FAILED)
def on_failure(task_name, task_id, exception):
    slack.post(f"Task {task_name} fehlgeschlagen")
```

**Pro:** Einfach fuer Anwendungsentwickler
**Contra:** Kann Verhalten nicht beeinflussen, kein State, keine Ordering

### Option C: Beides, getrennt (EMPFOHLEN)

Middleware **dispatcht** Signals. Beide Systeme existieren parallel mit klarer Aufgabentrennung:

```python
# Middleware: Fuer Infrastruktur/Cross-Cutting Concerns
class OpenTelemetryMiddleware(Middleware):
    async def pre_execute(self, context):
        context.metadata["span"] = tracer.start_span(context.task_name)

    async def post_execute(self, context, result):
        context.metadata["span"].end()

swarmq.add_middleware(OpenTelemetryMiddleware())

# Signals: Fuer einfache Reaktionen/Benachrichtigungen
@swarmq.on_signal(Signal.TASK_FAILED)
async def notify_ops(event: TaskEvent) -> None:
    await slack.post(f"Task {event.task_name} fehlgeschlagen: {event.exception}")

@swarmq.on_signal(Signal.TASK_COMPLETED, task="send_invoice")
async def on_invoice_sent(event: TaskEvent) -> None:
    await audit_log.write(f"Rechnung gesendet: {event.task_id}")
```

**Warum Option C?**

1. **Middleware dispatcht Signals** — die Signal-Emission passiert am Ende jedes Middleware-Durchlaufs, als Teil des Worker-Loops. Es gibt keine doppelte Iteration.
2. **Klare Verantwortlichkeiten** — Library-Autoren schreiben Middleware, Anwendungsentwickler nutzen Signals.
3. **Signals kosten nichts wenn niemand zuhoert** — leere Receiver-Sets werden beim Startup erkannt.
4. **Task-Filter** — Signals koennen nach Task-Name gefiltert werden, Middleware nicht.
5. **Test-Freundlich** — In Tests kann man auf Signals warten ohne Middleware zu schreiben.

---

# 10. Empfohlenes Signal-System fuer SwarmQ

## 10.1 Signal-Enum

```python
from enum import StrEnum


class Signal(StrEnum):
    """Alle Signal-Typen in SwarmQ."""

    # Task-Lifecycle
    TASK_ENQUEUED = "task.enqueued"
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_RETRYING = "task.retrying"
    TASK_REVOKED = "task.revoked"
    TASK_EXPIRED = "task.expired"
    TASK_LOCKED = "task.locked"
    TASK_RATE_LIMITED = "task.rate_limited"
    TASK_SKIPPED = "task.skipped"

    # Worker-Lifecycle
    WORKER_ONLINE = "worker.online"
    WORKER_SHUTDOWN = "worker.shutdown"
    WORKER_HEARTBEAT = "worker.heartbeat"

    # Scheduler-Lifecycle
    SCHEDULER_LEADER = "scheduler.leader"
    SCHEDULER_STANDBY = "scheduler.standby"
    SCHEDULER_DISPATCH = "scheduler.dispatch"
```

## 10.2 Event-Datenklassen

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TaskEvent:
    """Event-Daten fuer Task-bezogene Signals."""
    signal: Signal
    task_name: str
    task_id: str
    queue: str
    timestamp: float          # time.monotonic()

    # Optional — abhaengig vom Signal
    result: Any = None        # Nur bei TASK_COMPLETED
    exception: BaseException | None = None  # Nur bei TASK_FAILED
    attempt: int = 1          # Aktueller Versuch
    duration_ms: float = 0.0  # Nur bei COMPLETED/FAILED
    reason: str = ""          # Nur bei SKIPPED/EXPIRED


@dataclass(frozen=True, slots=True)
class WorkerEvent:
    """Event-Daten fuer Worker-bezogene Signals."""
    signal: Signal
    worker_id: str
    timestamp: float


@dataclass(frozen=True, slots=True)
class SchedulerEvent:
    """Event-Daten fuer Scheduler-bezogene Signals."""
    signal: Signal
    worker_id: str
    timestamp: float
    schedule_name: str = ""   # Nur bei SCHEDULER_DISPATCH
```

## 10.3 Signal-Dispatcher

```python
from __future__ import annotations

import asyncio
import structlog
from collections import defaultdict
from typing import Any, Callable, Coroutine

logger = structlog.get_logger()

# Type-Alias fuer Signal-Handler
SignalHandler = Callable[..., Coroutine[Any, Any, None]]


class SignalDispatcher:
    """Async Signal-Dispatcher mit Task-Name-Filterung.

    Alle Handler sind async. Dispatching ist fire-and-forget —
    Handler-Exceptions werden geloggt aber beeinflussen den
    Task nicht.
    """

    __slots__ = ("_handlers", "_task_handlers", "_has_handlers")

    def __init__(self) -> None:
        # signal -> list[handler]
        self._handlers: dict[Signal, list[SignalHandler]] = defaultdict(list)
        # (signal, task_name) -> list[handler]
        self._task_handlers: dict[tuple[Signal, str], list[SignalHandler]] = defaultdict(list)
        # Schneller Check ob es ueberhaupt Handler gibt
        self._has_handlers: set[Signal] = set()

    def connect(
        self,
        signal: Signal,
        handler: SignalHandler,
        *,
        task: str | None = None,
    ) -> None:
        """Registriert einen Handler fuer ein Signal.

        Args:
            signal: Das Signal.
            handler: Async-Funktion die aufgerufen wird.
            task: Optional — nur fuer diesen Task-Namen feuern.
        """
        if task is not None:
            self._task_handlers[(signal, task)].append(handler)
        else:
            self._handlers[signal].append(handler)
        self._has_handlers.add(signal)

    def disconnect(
        self,
        signal: Signal,
        handler: SignalHandler,
        *,
        task: str | None = None,
    ) -> None:
        """Entfernt einen Handler."""
        if task is not None:
            key = (signal, task)
            if key in self._task_handlers:
                self._task_handlers[key] = [h for h in self._task_handlers[key] if h is not handler]
        else:
            if signal in self._handlers:
                self._handlers[signal] = [h for h in self._handlers[signal] if h is not handler]

    async def emit(self, event: TaskEvent | WorkerEvent | SchedulerEvent) -> None:
        """Dispatcht ein Event an alle registrierten Handler.

        Handler-Exceptions werden geloggt aber nicht propagiert.
        Wenn keine Handler registriert sind, ist der Overhead minimal
        (ein Set-Lookup).
        """
        signal = event.signal

        # Schneller Bail-Out wenn niemand zuhoert
        if signal not in self._has_handlers:
            return

        # Globale Handler
        handlers = self._handlers.get(signal, ())

        # Task-spezifische Handler (nur fuer TaskEvent)
        task_name = getattr(event, "task_name", None)
        if task_name is not None:
            task_key = (signal, task_name)
            task_specific = self._task_handlers.get(task_key, ())
        else:
            task_specific = ()

        # Alle Handler ausfuehren
        for handler in (*handlers, *task_specific):
            try:
                await handler(event)
            except Exception:
                logger.exception(
                    "Signal-Handler fehlgeschlagen",
                    signal=signal,
                    handler=handler.__qualname__,
                )
```

## 10.4 Decorator-API auf SwarmQ

```python
class SwarmQ:
    def __init__(self, ...):
        self._signals = SignalDispatcher()

    def on_signal(
        self,
        signal: Signal,
        *,
        task: str | None = None,
    ):
        """Decorator zum Registrieren von Signal-Handlern.

        Args:
            signal: Das Signal auf das reagiert werden soll.
            task: Optional — nur fuer diesen Task-Namen.

        Beispiel:
            @swarmq.on_signal(Signal.TASK_FAILED)
            async def on_failure(event: TaskEvent) -> None:
                sentry_sdk.capture_exception(event.exception)

            @swarmq.on_signal(Signal.TASK_COMPLETED, task="send_invoice")
            async def on_invoice_sent(event: TaskEvent) -> None:
                await audit_log.write(f"Rechnung {event.task_id} gesendet")
        """
        def decorator(fn: SignalHandler) -> SignalHandler:
            self._signals.connect(signal, fn, task=task)
            return fn
        return decorator

    def connect_signal(
        self,
        signal: Signal,
        handler: SignalHandler,
        *,
        task: str | None = None,
    ) -> None:
        """Imperatives Registrieren eines Signal-Handlers.
        Nuetzlich fuer dynamische Registrierung und Tests."""
        self._signals.connect(signal, handler, task=task)

    def disconnect_signal(
        self,
        signal: Signal,
        handler: SignalHandler,
        *,
        task: str | None = None,
    ) -> None:
        """Entfernt einen Signal-Handler."""
        self._signals.disconnect(signal, handler, task=task)
```

## 10.5 Signal-Emission im Worker

Signals werden am Ende des Worker-Loops emittiert, nach allen Middleware-Hooks:

```python
class Worker:
    async def _process_message(self, raw_message: bytes) -> None:
        # ... (Middleware pre_execute, Task-Ausfuehrung, Middleware post_execute) ...

        # Signal emittieren — am Schluss, nachdem alles fertig ist
        if self._swarmq._signals._has_handlers:
            await self._swarmq._signals.emit(TaskEvent(
                signal=Signal.TASK_COMPLETED,
                task_name=context.task_name,
                task_id=context.task_id,
                queue=context.queue,
                timestamp=time.monotonic(),
                result=result,
                attempt=context.attempt,
                duration_ms=duration_ms,
            ))
```

## 10.6 Verwendungsbeispiele

### Einfache Benachrichtigung

```python
@swarmq.on_signal(Signal.TASK_FAILED)
async def notify_ops(event: TaskEvent) -> None:
    await slack_client.post_message(
        channel="#alerts",
        text=f"Task `{event.task_name}` fehlgeschlagen nach {event.attempt} Versuchen: {event.exception}",
    )
```

### Task-spezifischer Handler

```python
@swarmq.on_signal(Signal.TASK_COMPLETED, task="process_payment")
async def on_payment_processed(event: TaskEvent) -> None:
    payment_id = event.result.get("payment_id")
    await audit_service.log_payment(payment_id, event.task_id)
```

### Test-Integration

```python
import asyncio

async def test_task_emits_completed_signal(swarmq):
    received_events: list[TaskEvent] = []

    async def capture(event: TaskEvent) -> None:
        received_events.append(event)

    swarmq.connect_signal(Signal.TASK_COMPLETED, capture, task="my_test_task")

    task_id = await swarmq.schedule("my_test_task", args=[42])
    await swarmq.get_result(task_id, timeout=5)

    assert len(received_events) == 1
    assert received_events[0].task_name == "my_test_task"
    assert received_events[0].result == expected_result
```

### Wildcard-Monitoring

```python
# Alle Task-Signals loggen
for signal in Signal:
    if signal.value.startswith("task."):
        swarmq.connect_signal(signal, log_task_event)

async def log_task_event(event: TaskEvent) -> None:
    logger.info(
        "task_signal",
        signal=event.signal,
        task=event.task_name,
        task_id=event.task_id,
        duration_ms=event.duration_ms,
    )
```

---

# 11. Synthese: Gesamtarchitektur Middleware + Signals

## 11.1 Architekturdiagramm

```
                    Client-Seite                    Worker-Seite
                    ────────────                    ────────────

schedule() ──→ pre_enqueue [MW] ──→ Broker ──→ pre_execute [MW]
               post_enqueue [MW]                    │
                                                    │ SkipTask?
                                              ┌─────┴─────┐
                                              │           │
                                           SKIP        EXECUTE
                                              │           │
                                         on_skip [MW]  pre_execution() [Task]
                                         TASK_SKIPPED  run() [Task]
                                         [Signal]      post_execution() [Task]
                                                          │
                                                    ┌─────┴─────┐
                                                    │           │
                                                 SUCCESS     ERROR
                                                    │           │
                                              post_execute  on_error [MW]
                                              [MW]          on_retry [MW]?
                                                    │           │
                                              save_result   DLQ / Requeue
                                                    │
                                              post_save_result [MW]
                                                    │
                                                   ACK
                                                    │
                                              TASK_COMPLETED  /  TASK_FAILED
                                              [Signal]           [Signal]
```

## 11.2 Zusammenfassung der Designentscheidungen

| Entscheidung | Gewaehlt | Begruendung |
|---|---|---|
| Middleware-Basisklasse | Async-only, No-Op Defaults | Python 3.14+, kein maybe_awaitable()-Overhead |
| Hook-Ordering | Vorwaerts (pre), Rueckwaerts (post/on) | Stack-Semantik wie TaskIQ/Dramatiq |
| Middleware-Registrierung | `add_middleware()` + `before=`/`after=` | Dramatiq-Ansatz, einfach und flexibel |
| Performance | Startup-gecachte MiddlewareChain | TaskIQ-Optimierung: nur ueberschriebene Hooks |
| pre_execute kann abbrechen | Ja, via `SkipTask`-Exception | Dramatiq-Pattern, essentiell fuer AgeLimit/Dedup |
| Hook-Parameter | `TaskContext`-Objekt | Besser als lose Parameter, erweiterbar, State via metadata |
| Fehlerbehandlung | pre_*: propagiert, post_*/on_*: geloggt+geschluckt | Dramatiq-Ansatz, sichere Defaults |
| Built-in als Middleware | Ja (Retries, Timeouts, RateLimit, AgeLimit) | Austauschbar, testbar, konsistent |
| Signal-System | Separat von Middleware | Einfache API fuer Anwendungsentwickler |
| Signal-Handler | Async-only, fire-and-forget | Konsistent mit Middleware, keine Event-Loop-Blockierung |
| Task-Filter fuer Signals | Ja, via `task=`-Parameter | Huey hat das nicht — aber es ist essentiell |
| Signal-Dispatching | Sequentiell (nicht parallel) | Vorhersehbar, einfacher zu debuggen |

## 11.3 Abgrenzung: Task-Hooks vs. Middleware vs. Signals

SwarmQ hat drei Erweiterungsebenen, jede mit klarem Zweck:

| Ebene | Was | Wer | Kann aendern? |
|---|---|---|---|
| **Task-Hooks** (`pre_execution`, `post_execution`) | Task-spezifische Vor-/Nachbereitung | Task-Autor | Nur eigenen Task |
| **Middleware** | Cross-Cutting Concerns | Library-/Plattform-Autor | Task-Verhalten (Skip, Retry, Timeout) |
| **Signals** | Observer/Notifications | Anwendungsentwickler | Nichts — rein beobachtend |

**Ausfuehrungsreihenfolge:**
1. Middleware `pre_execute` (kann SkipTask werfen)
2. Task `pre_execution()`
3. Task `run()`
4. Task `post_execution()`
5. Middleware `post_execute`
6. Result-Speicherung
7. Middleware `post_save_result`
8. Signal-Emission (TASK_COMPLETED / TASK_FAILED)

Signals werden **nach** allem anderen emittiert. Sie sind das letzte Glied in der Kette und koennen nichts mehr beeinflussen. Das ist Absicht — Signal-Handler sollen den Task-Flow nicht stoeren koennen.
