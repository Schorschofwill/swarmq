# Recherche: Python-Logging-Bibliotheken fuer SwarmQ

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument fuer die Logging-Architektur von SwarmQ. Vergleich von Logging-Libraries, non-blocking Patterns fuer asyncio, und konkrete Empfehlung.

---

## Inhaltsverzeichnis

1. [structlog (EMPFOHLEN)](#1-structlog-empfohlen)
2. [loguru (NICHT empfohlen)](#2-loguru-nicht-empfohlen)
3. [picologging (NICHT empfohlen)](#3-picologging-nicht-empfohlen)
4. [stdlib logging mit async Handlers](#4-stdlib-logging-mit-async-handlers)
5. [aiologger (aufgegeben)](#5-aiologger-aufgegeben)
6. [eliot (Nische)](#6-eliot-nische)
7. [Patterns fuer non-blocking Logging in asyncio](#7-patterns-fuer-non-blocking-logging-in-asyncio)
8. [Best Practices fuer strukturiertes Logging in Task Queues](#8-best-practices-fuer-strukturiertes-logging-in-task-queues)
9. [Empfehlung fuer SwarmQ](#9-empfehlung-fuer-swarmq)

---

# 1. structlog (EMPFOHLEN)

## 1.1 Status und Community

- **Version:** 25.5.0 (Oktober 2025)
- **Python:** 3.8-3.14
- **GitHub Stars:** ~4.700
- **Maintainer:** Hynek Schlawack (aktiv, professionell)
- **Status:** Production/Stable seit 2013
- **Lizenz:** MIT/Apache 2.0
- **Keine transitiven Abhaengigkeiten**

## 1.2 Architektur

structlog basiert auf einer **Processor-Pipeline**: Log-Events durchlaufen eine konfigurierbare Kette von Funktionen die das Event-Dict anreichern, filtern oder transformieren.

- **Bound Loggers:** Logger mit gebundenen Kontextdaten (`logger.bind(task_id="abc")`)
- **Processors:** Funktionen `(logger, method_name, event_dict) -> event_dict`
- **Wrapper:** Kann stdlib `logging` wrappen oder standalone arbeiten
- **`make_filtering_bound_logger(level)`:** Deaktivierte Level-Methoden sind `return None` — buchstaeblich kein Overhead

## 1.3 Performance

- **Gefiltertes Logging (Level unter Schwelle):** `return None` — ~10-20ns Overhead
- **Aktives Logging mit JSON-Rendering:** ~50-150us je nach Processor-Kette
- **Vergleich mit stdlib:** structlog mit `make_filtering_bound_logger` + `cache_logger_on_first_use=True` ist schneller als stdlib (Level-Filtering VOR Event-Dict-Erzeugung)
- **Benchmark (16-Core ARM, 1000 concurrent asyncio Tasks):**
  - 2.6x hoehere Throughput als stdlib
  - 3.6x niedrigere Latenz
  - stdlib versagt bei >500K Logs/sec wegen GIL-Contention
  - Skaliert linear bis 64 Cores, saturiert bei ~12M Logs/sec/Node
- **`cache_logger_on_first_use=True`:** Logger wird beim ersten Aufruf assembliert und gecacht

## 1.4 Async-Support

**Strategie 1: Async-Methoden mit Thread-Pool (`ainfo`, `awarning`, etc.)**
```python
await logger.ainfo("task_completed", duration_ms=142.7)
```
- Intern: `asyncio.get_running_loop().run_in_executor(None, sync_log_call)`
- Event-Loop blockiert nie
- Hoehere Overhead pro Call (Thread-Kontextwechsel)
- **Nur verwenden wenn Processor-Kette I/O-abhaengig ist**

**Strategie 2: Synchrones Logging mit QueueHandler (BEVORZUGT)**
```
structlog -> stdlib Logger -> QueueHandler (~1us) -> [Queue] -> QueueListener-Thread -> Handler
```
- QueueHandler.emit() ist ~1us (nur queue.put())
- Gesamte I/O im QueueListener-Thread
- Event-Loop wird nie blockiert

## 1.5 contextvars-Integration (erstklassig)

```python
import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars

structlog.configure(
    processors=[
        merge_contextvars,                    # MUSS erster Processor sein
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    cache_logger_on_first_use=True,
)

# Im Worker bei Task-Empfang:
async def process_task(task_msg):
    clear_contextvars()
    bind_contextvars(
        task_id=task_msg.id,
        task_name=task_msg.name,
        queue=task_msg.queue,
        worker_id=worker_id,
    )
    logger = structlog.get_logger()
    logger.info("task_started")
    # Alle weiteren log-Aufrufe enthalten automatisch task_id etc.
```

- **Async-sicher:** contextvars propagieren korrekt ueber `await` und `asyncio.create_task()`
- **Isolation:** Jeder Task hat eigenen Kontext, kein Leaking zwischen Tasks

## 1.6 JSON-Output

Eingebaut via `structlog.processors.JSONRenderer()`. Kein externes Paket noetig.

## 1.7 stdlib-Kompatibilitaet

**structlog als Frontend, stdlib als Backend:**
```python
structlog.configure(
    processors=[
        merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)
```

**stdlib-Logs durch structlog rendern (ProcessorFormatter):**
```python
formatter = structlog.stdlib.ProcessorFormatter(
    processor=structlog.processors.JSONRenderer(),
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.addHandler(handler)
# Jetzt werden auch stdlib-Logs (von aio-pika, valkey-py) als JSON gerendert
```

## 1.8 Thread-Safety und Async-Safety

- Bound Logger sind **immutable** (jeder `bind()` erzeugt neuen Logger) — inherent thread-safe
- contextvars sind per-Task isoliert in asyncio
- Kein globaler Lock wie in stdlib

---

# 2. loguru (NICHT empfohlen)

## 2.1 Status

- **Version:** 0.7.3 (Dezember 2024)
- **GitHub Stars:** ~23.800 (sehr populaer)
- **Maintainer:** Delgan (Einzelperson)

## 2.2 Kritische Probleme fuer asyncio

1. **Unbounded Memory Growth (Issue #1419):** `enqueue=True` hat KEINE Backpressure. Bei langsamen Sinks waechst die Queue unbegrenzt → OOM in langlebigen Workern
2. **Threading-Lock in async-Code (Issue #906):** loguru verwendet `threading.Lock`, was den Event-Loop blockieren kann
3. **`await logger.complete()` Deadlock:** In Kombination mit `enqueue=True` und async Sinks
4. **Kein natives contextvars:** Braucht "patcher" fuer ContextVar-Injection

## 2.3 Bewertung

**Nicht verwenden** wegen:
- OOM-Risiko durch fehlende Backpressure
- Threading-Lock-Probleme in asyncio
- Nicht fuer Hochlast-asyncio konzipiert

---

# 3. picologging (NICHT empfohlen)

- **Version:** 0.9.4 (September 2024), **Beta-Status**
- **Maintainer:** Microsoft
- C-Extension, Drop-in fuer stdlib, behauptet 4-10x schneller
- Kein strukturiertes Logging, kein contextvars, kein async-Support
- Unklar ob Python 3.14 unterstuetzt

**Nicht verwenden** — Beta, keine relevanten Features fuer SwarmQ.

---

# 4. stdlib logging mit async Handlers

## 4.1 QueueHandler + QueueListener Pattern

```python
import logging
import logging.handlers
import queue

log_queue = queue.Queue(maxsize=10_000)  # Bounded fuer Backpressure

queue_handler = logging.handlers.QueueHandler(log_queue)
stream_handler = logging.StreamHandler()

listener = logging.handlers.QueueListener(
    log_queue, stream_handler,
    respect_handler_level=True,
)
listener.start()

root = logging.getLogger()
root.handlers.clear()
root.addHandler(queue_handler)
root.setLevel(logging.DEBUG)
```

- **QueueHandler.emit():** ~1-2us (nur queue.put())
- **Backpressure:** Mit bounded Queue
- **Problem:** Viel Boilerplate, keine eingebaute JSON-Strukturierung, contextvars manuell

## 4.2 contextvars mit Custom Filter

```python
from contextvars import ContextVar

task_id_var: ContextVar[str] = ContextVar("task_id", default="")

class ContextFilter(logging.Filter):
    def filter(self, record):
        record.task_id = task_id_var.get()
        return True

root.addFilter(ContextFilter())
```

## 4.3 Bewertung

Funktioniert, aber am besten in Kombination mit structlog (structlog als Frontend, stdlib als Backend).

---

# 5. aiologger (aufgegeben)

- **Letzte Version:** 0.7.0 (Oktober 2022)
- Keine Aktivitaet seit >3 Jahren
- Nur fuer stdout/stderr wirklich async
- **Nicht verwenden.**

---

# 6. eliot (Nische)

- **Version:** 1.17.5 (Februar 2025)
- Action-basiertes Logging mit Kausalitaets-Tracking
- Interessant fuer Tracing, aber eigenes Oekosystem
- Nicht stdlib-kompatibel
- **Nicht als primaere Loesung**, Kausalitaets-Tracking mit structlog + Correlation-IDs erreichbar.

---

# 7. Patterns fuer non-blocking Logging in asyncio

## 7.1 Pattern 1: stdlib QueueHandler (EMPFOHLEN)

```
Coroutine -> structlog -> stdlib Logger -> QueueHandler -> [Queue]
    -> QueueListener-Thread -> Handler (File/Stream/Network)
```

- QueueHandler.emit() ~1us
- QueueListener in eigenem Thread
- Event-Loop wird nie blockiert

## 7.2 Pattern 2: structlog async-Methoden

```
Coroutine -> await logger.ainfo() -> ThreadPoolExecutor -> Processor-Kette + Handler
```

- Hoehere Overhead pro Call (Thread-Dispatch)
- Nur sinnvoll wenn Processor-Kette I/O macht

## 7.3 uvloop-Auswirkungen

- uvloop aendert nichts am Logging-Verhalten
- Logging-Calls sind synchrone Python-Aufrufe
- QueueHandler-Pattern funktioniert identisch mit uvloop

## 7.4 Was andere Task Queues machen

| Framework | Logging-Ansatz |
|---|---|
| **Celery** | Eigener `celery.task` Logger mit TaskFormatter, task_id/task_name via `get_current_task()` |
| **TaskIQ** | Middleware-Hooks zum Binden von task_id in structlog.contextvars |
| **Dramatiq** | Loggt Task-Argumente per Default, Custom-Logging ueber Middleware |
| **arq** | Einfaches stdlib logging mit Job-ID |

---

# 8. Best Practices fuer strukturiertes Logging in Task Queues

## 8.1 Kontext-Propagation ueber Task-Grenzen

```python
# Producer: Correlation-ID in Task-Message einbetten
correlation_id = str(uuid.uuid4())
await broker.schedule("send_email", args=[...],
                      headers={"correlation_id": correlation_id})

# Worker: Correlation-ID extrahieren und binden
async def process_task(msg):
    clear_contextvars()
    bind_contextvars(
        correlation_id=msg.headers.get("correlation_id", "unknown"),
        task_id=msg.task.id,
        task_name=msg.task.name,
        worker_id=self.worker_id,
    )
```

## 8.2 Log Sampling fuer Hochdurchsatz

```python
import random

def sample_processor(logger, method_name, event_dict):
    if event_dict.get("level") == "debug" and random.random() > 0.1:
        raise structlog.DropEvent
    return event_dict
```

## 8.3 Log-Aggregation

- **ELK-Stack:** JSON-Logs → Filebeat → Elasticsearch → Kibana
- **Grafana Loki:** Lightweight, Label-basiert, ideal fuer Container
- **Empfehlung:** JSON nach stdout, Aggregation extern (Container-native)

---

# 9. Empfehlung fuer SwarmQ

## 9.1 Entscheidung: structlog + stdlib QueueHandler

| Kriterium | structlog + QueueHandler | loguru | picologging | stdlib pur |
|---|---|---|---|---|
| Event-Loop blockiert nie | Ja (~1us) | Problematisch (Lock, OOM) | Wie stdlib | Ja (mit QueueHandler) |
| High Performance | Ja (Filtering = `return None`) | Langsamer | Beta | Ja |
| Strukturiertes Logging | Nativ (Processors) | Moeglich | Nein | Manuell |
| contextvars fuer async | Erstklassig | Patcher noetig | Nein | Manuell |
| stdlib-kompatibel | Vollstaendig | Ersetzt stdlib | Drop-in | Ist stdlib |
| Backpressure | Ja (bounded Queue) | Nein (OOM!) | Wie stdlib | Ja |
| Python 3.14 | Ja | Ja | Unklar | Ja |
| Transitiv Deps | Keine | Keine | Keine | — |

## 9.2 Empfohlene Konfiguration

```python
"""swarmq/logging.py"""

import logging
import logging.handlers
import queue
import sys
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars


def setup_logging(
    log_level: int = logging.INFO,
    json_output: bool = True,
    queue_maxsize: int = 10_000,
) -> logging.handlers.QueueListener:
    """
    Non-blocking async Logging.

    Architektur:
        structlog -> stdlib Logger -> QueueHandler -> [Queue]
            -> QueueListener (eigener Thread) -> Handler(s)
    """
    # 1. Bounded Queue mit Backpressure
    log_queue: queue.Queue[Any] = queue.Queue(maxsize=queue_maxsize)

    # 2. Echte Handler (laufen im QueueListener-Thread)
    if json_output:
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
        )
    else:
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.dev.ConsoleRenderer(),
            ],
        )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    # 3. QueueHandler (non-blocking, ~1us)
    queue_handler = logging.handlers.QueueHandler(log_queue)

    # 4. Root-Logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(log_level)

    # 5. QueueListener (eigener Thread)
    listener = logging.handlers.QueueListener(
        log_queue, stream_handler,
        respect_handler_level=True,
    )
    listener.start()

    # 6. structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return listener


def bind_task_context(
    task_id: str, task_name: str, queue_name: str,
    worker_id: str, attempt: int = 1,
) -> None:
    clear_contextvars()
    bind_contextvars(
        task_id=task_id, task_name=task_name,
        queue=queue_name, worker_id=worker_id, attempt=attempt,
    )


def bind_worker_context(worker_id: str, worker_pid: int) -> None:
    bind_contextvars(worker_id=worker_id, worker_pid=worker_pid)
```

## 9.3 Integration in Worker (Hot-Path)

```python
import structlog
from swarmq.logging import setup_logging, bind_task_context

logger = structlog.get_logger()

class AsyncWorker:
    async def start(self):
        self._listener = setup_logging(log_level=logging.INFO, json_output=True)
        bind_worker_context(self.worker_id, os.getpid())
        logger.info("worker_started", queues=self.queues)

    async def process_task(self, msg):
        bind_task_context(
            task_id=msg.task.id, task_name=msg.task.name,
            queue_name=msg.task.queue, worker_id=self.worker_id,
        )
        start = time.perf_counter()
        logger.info("task_started")

        try:
            result = await task.run(*msg.args, **msg.kwargs)
            duration_ms = (time.perf_counter() - start) * 1000
            logger.info("task_completed", duration_ms=round(duration_ms, 1))
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000
            logger.error("task_failed", duration_ms=round(duration_ms, 1), exc_info=exc)
            raise

    async def shutdown(self):
        logger.info("worker_shutting_down")
        if self._listener:
            self._listener.stop()
```

## 9.4 Beispiel-Output (JSON)

```json
{"worker_id":"worker-3","worker_pid":12345,"level":"info","timestamp":"2026-04-15T10:30:00.123Z","event":"worker_started","queues":["default","high-priority"]}
{"worker_id":"worker-3","task_id":"01926f3a-87c4-7def-8abc-123456789012","task_name":"send_daily_report","queue":"default","attempt":1,"level":"info","timestamp":"2026-04-15T10:30:01.456Z","event":"task_started"}
{"worker_id":"worker-3","task_id":"01926f3a-87c4-7def-8abc-123456789012","task_name":"send_daily_report","queue":"default","attempt":1,"level":"info","timestamp":"2026-04-15T10:30:01.598Z","event":"task_completed","duration_ms":142.7}
```

## 9.5 Warum NICHT `await logger.ainfo()`

Die async-Methoden fuehren die gesamte Processor-Kette im ThreadPoolExecutor aus. SwarmQ's Processor-Kette ist rein CPU-gebunden (~50-150us) — zu kurz fuer Thread-Dispatch-Overhead. **QueueHandler ist besser:** `queue.put()` kostet ~1us, I/O passiert im QueueListener-Thread.

## 9.6 Backpressure

- `queue.Queue(maxsize=10_000)` gibt Backpressure
- Bei 10K Logs/sec: >1 Minute Buffer
- Bei voller Queue blockiert `put()` — besser als OOM (loguru)
- In der Praxis wird die Queue bei normalem Volumen nie voll

## 9.7 Architektur-Zusammenfassung

1. **structlog** — strukturiertes Logging mit Processor-Pipeline und contextvars
2. **stdlib logging** — Backend fuer Handler-Kompatibilitaet (aio-pika, valkey-py Logs werden auch strukturiert)
3. **QueueHandler + QueueListener** — non-blocking I/O, Event-Loop nie blockiert
4. **`make_filtering_bound_logger`** — Zero-Cost-Filtering deaktivierter Levels
5. **`cache_logger_on_first_use=True`** — minimaler Overhead nach Erstaufruf
6. **`ProcessorFormatter`** — ALLE Logs (structlog UND stdlib) einheitlich als JSON
7. **Bounded Queue** — Backpressure statt OOM
