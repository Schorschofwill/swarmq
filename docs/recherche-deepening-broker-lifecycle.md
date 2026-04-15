# Vertiefende Recherche: Broker-Lifecycle, Ack-Semantik, Shutdown, Middleware-Retry-Interaktion, Timeouts

**Erstellt:** 2026-04-15
**Zweck:** Schliesst die kritischen Luecken aus der bisherigen Recherche. Enthaelt produktionsreife Code-Beispiele mit aio-pika 9.6.2, asyncio (Python 3.14+), und valkey-py. Alle Code-Beispiele sind vollstaendig lauffaehig -- kein Pseudocode.

**Abhaengigkeiten:** Baut auf `recherche-aio-pika-rabbitmq.md`, `recherche-retry-dlq.md`, `recherche-middleware-signals.md`, `worker-architecture.md` auf.

---

## Inhaltsverzeichnis

1. [RabbitMQ Queue/Exchange Topology](#1-rabbitmq-queueexchange-topology)
2. [Delivery Mode Semantik -- Exakte Ack-Zeitpunkte](#2-delivery-mode-semantik)
3. [Graceful Shutdown -- Vollstaendiger Flow](#3-graceful-shutdown)
4. [Middleware + Retry + DLQ Interaktion](#4-middleware--retry--dlq-interaktion)
5. [Timeout-Implementierung (soft_timeout + hard_timeout)](#5-timeout-implementierung)

---

# 1. RabbitMQ Queue/Exchange Topology

## 1.1 Entscheidung: Exchange-Topologie

SwarmQ nutzt den **Default Exchange** (`""`) fuer alle Task-Operationen. Routing erfolgt ueber den Queue-Namen als Routing-Key. Das ist der einfachste und performanteste Ansatz -- identisch mit Dramatiq.

**Begruendung gegen Alternativen:**

| Alternative | Warum nicht |
|---|---|
| Ein Direct Exchange "swarmq" | Unnoetige Indirektion. Default Exchange routet direkt nach Queue-Name. |
| Per-Queue Exchanges | Explosionsartige Exchange-Vermehrung bei vielen Queues. Kein Vorteil. |
| Dedizierter DLX Exchange | Unnoetig -- Default Exchange mit `x-dead-letter-routing-key` reicht. |

**Ergebnis:** Null eigene Exchanges. Der Default Exchange (immer vorhanden, nicht explizit deklarierbar) reicht vollstaendig.

## 1.2 Queue-Deklaration: Vollstaendiger Code

```python
"""swarmq/broker/rabbitmq/topology.py — Queue-Topologie fuer SwarmQ."""

from __future__ import annotations

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue


# Konfigurierbare Defaults
DEFAULT_XQ_TTL_MS: int = 604_800_000       # 7 Tage
DEFAULT_DELIVERY_LIMIT: int = 50            # Sicherheitsnetz ueber Application-Level Retries
DEFAULT_PREFETCH_COUNT: int = 10


async def declare_queue_topology(
    channel: AbstractChannel,
    queue_name: str,
    *,
    xq_ttl_ms: int = DEFAULT_XQ_TTL_MS,
    delivery_limit: int = DEFAULT_DELIVERY_LIMIT,
) -> tuple[AbstractQueue, AbstractQueue, AbstractQueue]:
    """Deklariert das 3-Queue-Pattern fuer eine logische Queue.

    Erstellt:
        {queue_name}     — Haupt-Queue (Quorum, DLX → XQ)
        {queue_name}.DQ  — Delay-Queue (Quorum, DLX → Haupt-Queue)
        {queue_name}.XQ  — Dead-Letter-Queue (Quorum, TTL fuer Auto-Cleanup)

    Returns:
        Tuple von (main_queue, delay_queue, dead_letter_queue).
    """
    dq_name = f"{queue_name}.DQ"
    xq_name = f"{queue_name}.XQ"

    # 1. Dead-Letter-Queue (XQ) — Endstation fuer nicht-retry-faehige Messages
    #    Quorum Queue fuer At-Least-Once Dead Lettering.
    #    TTL sorgt fuer automatische Bereinigung nach 7 Tagen.
    xq = await channel.declare_queue(
        xq_name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-message-ttl": xq_ttl_ms,
        },
    )

    # 2. Delay-Queue (DQ) — Warteraum fuer Retry-Messages mit Per-Message TTL.
    #    Wenn die TTL einer Message ablaeuft, dead-lettert RabbitMQ sie
    #    zurueck in die Haupt-Queue (via Default Exchange + Routing-Key).
    #    Die DQ selbst hat KEINEN delivery-limit — Messages sollen nach
    #    TTL-Ablauf immer weitergeleitet werden.
    #    DLX der DQ zeigt auf XQ: falls eine Message in der DQ rejected
    #    wird (sollte nie passieren), landet sie in der XQ statt verloren zu gehen.
    dq = await channel.declare_queue(
        dq_name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": queue_name,
            "x-dead-letter-strategy": "at-least-once",
        },
    )

    # 3. Haupt-Queue — Workers konsumieren von hier.
    #    Bei reject(requeue=False) geht die Message an die XQ.
    #    delivery-limit als Sicherheitsnetz: falls Application-Level
    #    Retry-Logik versagt (Bug), werden Messages nach N Zustellungen
    #    automatisch zur XQ weitergeleitet.
    main_q = await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": xq_name,
            "x-dead-letter-strategy": "at-least-once",
            "x-delivery-limit": delivery_limit,
            "x-overflow": "reject-publish",
        },
    )

    return main_q, dq, xq


async def declare_all_queues(
    channel: AbstractChannel,
    queue_names: list[str],
    *,
    xq_ttl_ms: int = DEFAULT_XQ_TTL_MS,
    delivery_limit: int = DEFAULT_DELIVERY_LIMIT,
) -> dict[str, tuple[AbstractQueue, AbstractQueue, AbstractQueue]]:
    """Deklariert die Topologie fuer mehrere logische Queues.

    Returns:
        Dict von queue_name → (main_queue, delay_queue, dead_letter_queue).
    """
    result: dict[str, tuple[AbstractQueue, AbstractQueue, AbstractQueue]] = {}
    for name in queue_names:
        topology = await declare_queue_topology(
            channel,
            name,
            xq_ttl_ms=xq_ttl_ms,
            delivery_limit=delivery_limit,
        )
        result[name] = topology
    return result
```

## 1.3 Message-Flow-Diagramme

### Normaler Ablauf (Erfolg)

```
Producer                RabbitMQ                    Worker
   |                       |                          |
   |-- publish(body,       |                          |
   |   routing_key=        |                          |
   |   "default") -------->|                          |
   |                       |                          |
   |   <-- Basic.Ack ------|                          |
   |   (Publisher Confirm) |                          |
   |                       |-- deliver(msg) --------->|
   |                       |                          |
   |                       |                          |-- pre_execution()
   |                       |                          |-- run()
   |                       |                          |-- post_execution()
   |                       |                          |-- result → Valkey
   |                       |                          |
   |                       |<---- Basic.Ack ----------|
   |                       |  (Message entfernt)      |
```

### Retry-Flow (Fehlschlag mit Wiederholung)

```
Worker           RabbitMQ "default"    RabbitMQ "default.DQ"    RabbitMQ "default.XQ"
  |                    |                       |                        |
  |<-- deliver(msg) ---|                       |                        |
  |                    |                       |                        |
  |-- run() → Exception                       |                        |
  |                    |                       |                        |
  |-- retry_count < max_retries?               |                        |
  |   JA: neue Message mit TTL                 |                        |
  |        erstellen (gleiche message_id)      |                        |
  |                    |                       |                        |
  |-- publish(new_msg, |                       |                        |
  |   routing_key=     |                       |                        |
  |   "default.DQ") ---|---------------------->|                        |
  |                    |                       |                        |
  |-- ack(original) -->|                       |                        |
  |   (Original entfernt)                      |                        |
  |                    |                       |                        |
  |                    |   (TTL abgelaufen)    |                        |
  |                    |<-- DLX: routing_key = |                        |
  |                    |   "default" ----------|                        |
  |                    |                       |                        |
  |<-- deliver(msg) ---|  (naechster Versuch)  |                        |
```

### DLQ-Flow (Alle Retries erschoepft)

```
Worker           RabbitMQ "default"    RabbitMQ "default.XQ"
  |                    |                       |
  |<-- deliver(msg) ---|                       |
  |                    |                       |
  |-- run() → Exception                       |
  |                    |                       |
  |-- retry_count >= max_retries               |
  |   KEIN Retry mehr.                         |
  |                    |                       |
  |-- reject(          |                       |
  |   requeue=False) ->|                       |
  |                    |                       |
  |                    |-- DLX: routing_key =  |
  |                    |   "default.XQ" ------>|
  |                    |                       |
  |                    |   (Message bleibt     |
  |                    |    7 Tage in XQ)      |
```

### FIRE_AND_FORGET-Flow

```
Worker           RabbitMQ "default"
  |                    |
  |<-- deliver(msg) ---|
  |                    |
  |-- ack(msg) ------->|  (SOFORT, vor Ausfuehrung)
  |   (Message entfernt)
  |                    |
  |-- pre_execution()  |
  |-- run()            |
  |-- post_execution() |
  |                    |
  |   (Bei Crash: Message bereits weg,
  |    kein Retry moeglich)
```

## 1.4 Anzahl RabbitMQ-Objekte bei mehreren Queues

Wenn ein Worker auf `["default", "high-priority", "cpu"]` hoert:

| Objekt-Typ | Anzahl | Details |
|---|---|---|
| **Exchanges** | 0 | Nur Default Exchange (immer vorhanden, nicht deklariert) |
| **Queues** | 9 | 3 pro logische Queue: `default`, `default.DQ`, `default.XQ`, `high-priority`, `high-priority.DQ`, `high-priority.XQ`, `cpu`, `cpu.DQ`, `cpu.XQ` |
| **Bindings** | 0 | Default Exchange bindet implizit nach Queue-Name |
| **Consumer** | 3 | Einer pro Haupt-Queue (`default`, `high-priority`, `cpu`) |
| **Channels** | 3-4 | 1 pro Consumer + 1 fuer Publishing (oder Channel-Pool) |
| **Connections** | 1 | Eine RobustConnection pro Worker-Prozess |

**Gesamt: 9 Queues, 3 Consumer, 1 Connection, 3-4 Channels.**

Bei N logischen Queues: `3*N` Queues, `N` Consumer.

## 1.5 Vollstaendiger Worker-Setup-Code

```python
"""swarmq/broker/rabbitmq/setup.py — Worker-seitiger Broker-Setup."""

from __future__ import annotations

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel

from swarmq.broker.rabbitmq.topology import declare_all_queues, DEFAULT_PREFETCH_COUNT


async def setup_worker_broker(
    amqp_url: str,
    queue_names: list[str],
    prefetch_count: int = DEFAULT_PREFETCH_COUNT,
) -> tuple[AbstractRobustConnection, AbstractRobustChannel, dict]:
    """Stellt die Broker-Verbindung her und deklariert alle Queues.

    Returns:
        Tuple von (connection, channel, queues_dict).
        queues_dict: {queue_name: (main_q, dq, xq)}
    """
    # RobustConnection: Auto-Reconnect, Channel/Queue/Consumer Restore
    connection = await aio_pika.connect_robust(
        amqp_url,
        client_properties={"connection_name": "swarmq-worker"},
    )

    # Ein Channel fuer Consuming — Prefetch begrenzt die Anzahl
    # unacked Messages die RabbitMQ gleichzeitig sendet.
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=prefetch_count)

    # Alle Queues deklarieren (idempotent — sicher bei mehreren Workern)
    queues = await declare_all_queues(channel, queue_names)

    return connection, channel, queues


async def setup_publisher_channel(
    connection: AbstractRobustConnection,
) -> AbstractRobustChannel:
    """Erstellt einen separaten Channel fuer Publishing (Retry-Messages).

    Separater Channel damit ein Publish-Fehler nicht den Consumer-Channel
    schliesst. Publisher Confirms sind per Default aktiv.
    """
    channel = await connection.channel()
    return channel
```

---

# 2. Delivery Mode Semantik

## 2.1 AT_LEAST_ONCE: Exakter Ack-Zeitpunkt

**Ack erfolgt NACH:**
1. `post_execution()` erfolgreich
2. Result in Valkey geschrieben (`SET ... PX <ttl_ms>`)

**Wenn Valkey waehrend Result-Storage nicht erreichbar ist:**
- **Nack mit requeue=True.** Die Task-Ausfuehrung war erfolgreich, aber das Ergebnis konnte nicht gespeichert werden. Der Task wird erneut zugestellt (Idempotenz-Anforderung!).
- Grund: Der Client wartet auf das Ergebnis in Valkey. Ohne Result-Write bekommt der Client nie eine Antwort.

**Wenn der Worker zwischen Task-Completion und Ack crasht:**
- RabbitMQ erkennt den Consumer-Disconnect.
- Alle unacked Messages werden redelivered (an denselben oder anderen Worker).
- `message.redelivered` Flag ist `True` — der Worker kann dies pruefen.

## 2.2 FIRE_AND_FORGET: Sofortiger Ack

**Ack erfolgt SOFORT nach Message-Empfang, VOR jeder Task-Ausfuehrung.**

**Was passiert wenn der Event-Loop busy ist:**
- `ack()` ist ein asynchroner Aufruf — er wird in den Event-Loop eingereiht.
- Wenn der Loop busy ist (z.B. andere Tasks), wird der Ack erst beim naechsten Yield ausgefuehrt.
- Zwischen Empfang und Ack kann eine kurze Verzoegerung entstehen.
- RabbitMQ wartet geduldig (solange Heartbeats ankommen).

**Risiko von Message-Verlust:**
- Wenn der Worker zwischen Message-Empfang und Ack crasht (extrem unwahrscheinlich, da nur wenige Mikrosekunden), wird die Message redelivered.
- Nach dem Ack ist die Message endgueltig weg — bei Crash waehrend der Ausfuehrung kein Replay.

## 2.3 Vollstaendiger Consumer-Code fuer beide Modi

```python
"""swarmq/broker/rabbitmq/consumer.py — Message-Consumer mit Ack-Semantik."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import aio_pika
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustChannel,
)

if TYPE_CHECKING:
    from swarmq.task import Task, TaskInfo
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.middleware import MiddlewareStack
    from swarmq.signals import SignalDispatcher

logger = logging.getLogger("swarmq.consumer")


class TaskConsumer:
    """Konsumiert Messages von RabbitMQ und fuehrt Tasks aus."""

    def __init__(
        self,
        channel: AbstractRobustChannel,
        publish_channel: AbstractRobustChannel,
        backend: ValkeyBackend,
        middleware: MiddlewareStack,
        signals: SignalDispatcher,
        task_registry: dict[str, type[Task]],
        shutdown_event: asyncio.Event,
    ) -> None:
        self._channel = channel
        self._publish_channel = publish_channel
        self._backend = backend
        self._middleware = middleware
        self._signals = signals
        self._registry = task_registry
        self._shutdown = shutdown_event
        self._consumer_tags: list[str] = []
        self._active_tasks: set[asyncio.Task] = set()
        # Messages die prefetched aber noch nicht verarbeitet wurden
        self._prefetched_messages: list[AbstractIncomingMessage] = []

    async def start_consuming(self, queues: dict[str, tuple]) -> None:
        """Startet Consumer fuer alle Haupt-Queues."""
        for queue_name, (main_q, _dq, _xq) in queues.items():
            tag = await main_q.consume(self._on_message)
            self._consumer_tags.append(tag)
            logger.info("Consumer gestartet fuer Queue %r (tag=%s)", queue_name, tag)

    async def stop_consuming(self) -> None:
        """Stoppt alle Consumer. Prefetched Messages bleiben im Buffer."""
        for tag in self._consumer_tags:
            try:
                await self._channel.cancel(tag)
            except Exception:
                logger.warning("Fehler beim Abbestellen von Consumer %s", tag, exc_info=True)
        self._consumer_tags.clear()
        logger.info("Alle Consumer gestoppt")

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """Callback fuer jede empfangene Message."""
        if self._shutdown.is_set():
            # Im Shutdown-Modus: Message merken fuer spaeteres Nack
            self._prefetched_messages.append(message)
            return

        task = asyncio.current_task()
        if task is not None:
            # Erstelle einen neuen Task fuer die Message-Verarbeitung
            coro_task = asyncio.create_task(
                self._process_message(message),
                name=f"swarmq-task-{message.message_id}",
            )
            self._active_tasks.add(coro_task)
            coro_task.add_done_callback(self._active_tasks.discard)
        else:
            await self._process_message(message)

    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        """Verarbeitet eine einzelne Message — Kern der Ack-Semantik."""
        task_name = (message.headers or {}).get("x-task-name", "unknown")
        task_cls = self._registry.get(task_name)

        if task_cls is None:
            logger.error("Unbekannter Task %r — Message wird rejected", task_name)
            await message.reject(requeue=False)
            return

        task_info: TaskInfo = task_cls.task_info
        task_instance: Task = task_cls()

        # ---------------------------------------------------------------
        # FIRE_AND_FORGET: Sofortiger Ack BEVOR irgendwas ausgefuehrt wird
        # ---------------------------------------------------------------
        if task_info.delivery == DeliveryMode.FIRE_AND_FORGET:
            await message.ack()
            logger.debug("FIRE_AND_FORGET: Message %s geackt", message.message_id)
            try:
                await self._middleware.call_pre_execution(task_instance, message)
                await task_instance.pre_execution()
                result = await task_instance.run(
                    *self._deserialize_args(message),
                    **self._deserialize_kwargs(message),
                )
                await task_instance.post_execution()
                await self._middleware.call_post_execution(task_instance, message, result=result)
                # Result-Write ist best-effort bei FIRE_AND_FORGET
                if task_info.result_ttl != 0:
                    try:
                        await self._backend.store_result(
                            message.message_id, result, ttl=task_info.result_ttl
                        )
                    except Exception:
                        logger.warning(
                            "Result-Write fehlgeschlagen fuer FIRE_AND_FORGET Task %s",
                            message.message_id,
                            exc_info=True,
                        )
            except Exception as exc:
                logger.exception("FIRE_AND_FORGET Task %s fehlgeschlagen", task_name)
                await self._middleware.call_on_error(task_instance, message, exc)
            return

        # ---------------------------------------------------------------
        # AT_LEAST_ONCE: Ack erst nach vollstaendiger Verarbeitung + Result-Write
        # ---------------------------------------------------------------
        try:
            await self._middleware.call_pre_execution(task_instance, message)
            await task_instance.pre_execution()

            result = await task_instance.run(
                *self._deserialize_args(message),
                **self._deserialize_kwargs(message),
            )

            await task_instance.post_execution()
            await self._middleware.call_post_execution(task_instance, message, result=result)

            # Result in Valkey speichern — MUSS erfolgreich sein vor Ack
            if task_info.result_ttl != 0:
                await self._backend.store_result(
                    message.message_id, result, ttl=task_info.result_ttl
                )

            # ALLES erfolgreich → Ack
            await message.ack()
            logger.debug("AT_LEAST_ONCE: Message %s geackt", message.message_id)

        except Exception as exc:
            await self._handle_task_failure(task_instance, task_info, message, exc)

    async def _handle_task_failure(
        self,
        task_instance: Task,
        task_info: TaskInfo,
        message: AbstractIncomingMessage,
        exception: Exception,
    ) -> None:
        """Behandelt einen Task-Fehler: Retry oder DLQ.

        Siehe Abschnitt 4 fuer den vollstaendigen Middleware-Interaktions-Flow.
        """
        headers = message.headers or {}
        retry_count = headers.get("x-retry-count", 0)

        # Middleware on_error — kann Exception NICHT verhindern oder aendern
        # (siehe Abschnitt 4 fuer Details)
        try:
            await self._middleware.call_on_error(task_instance, message, exception)
        except Exception:
            logger.exception("Middleware on_error hat selbst eine Exception geworfen")

        # Signal TASK_FAILED oder TASK_RETRYING
        should_retry = self._should_retry(task_info, exception, retry_count)

        if should_retry:
            await self._signals.emit("TASK_RETRYING", task_instance, message, exception)
            await self._middleware.call_on_retry(task_instance, message, exception, retry_count)

            # Neue Message in DQ mit TTL (Backoff-Delay)
            delay_ms = self._compute_backoff(
                retry_count,
                task_info.retry_backoff,
            )
            await self._publish_retry(message, task_info, retry_count, delay_ms, exception)

            # Original-Message ACK (nicht nack! — neue Message wurde erstellt)
            await message.ack()
            logger.info(
                "Task %s retry %d/%d in %dms",
                message.message_id,
                retry_count + 1,
                task_info.max_retries,
                delay_ms,
            )
        else:
            await self._signals.emit("TASK_FAILED", task_instance, message, exception)

            # Reject ohne requeue → DLX → XQ
            await message.reject(requeue=False)
            logger.warning(
                "Task %s endgueltig fehlgeschlagen nach %d Versuchen: %s",
                message.message_id,
                retry_count + 1,
                exception,
            )

    def _should_retry(
        self,
        task_info: TaskInfo,
        exception: Exception,
        retry_count: int,
    ) -> bool:
        """Prueft ob ein Retry durchgefuehrt werden soll."""
        if task_info.max_retries <= 0:
            return False
        if retry_count >= task_info.max_retries:
            return False
        # Exception-Filter: wenn retry_exceptions gesetzt, nur diese retrien
        if task_info.retry_exceptions is not None:
            if not isinstance(exception, tuple(task_info.retry_exceptions)):
                return False
        return True

    def _compute_backoff(self, attempt: int, strategy: str) -> int:
        """Berechnet Backoff-Delay in Millisekunden (Full Jitter)."""
        import secrets

        base_ms = 1_000       # 1 Sekunde
        max_ms = 900_000      # 15 Minuten

        if strategy == "exponential":
            ceiling = min(max_ms, base_ms * (2 ** attempt))
            return secrets.randbelow(ceiling + 1)
        elif strategy == "linear":
            ceiling = min(max_ms, base_ms * (attempt + 1))
            return secrets.randbelow(ceiling + 1)
        elif strategy == "fixed":
            return base_ms
        else:
            ceiling = min(max_ms, base_ms * (2 ** attempt))
            return secrets.randbelow(ceiling + 1)

    async def _publish_retry(
        self,
        original: AbstractIncomingMessage,
        task_info: TaskInfo,
        retry_count: int,
        delay_ms: int,
        exception: Exception,
    ) -> None:
        """Publiziert eine Retry-Message in die Delay-Queue."""
        import traceback as tb

        queue_name = (original.headers or {}).get("x-task-queue", "default")
        dq_name = f"{queue_name}.DQ"

        # Headers aktualisieren: Retry-State reist mit der Message
        updated_headers = dict(original.headers or {})
        updated_headers["x-retry-count"] = retry_count + 1
        updated_headers["x-last-error-type"] = type(exception).__qualname__
        updated_headers["x-last-error-message"] = str(exception)[:500]
        updated_headers["x-last-traceback"] = "".join(
            tb.format_exception(type(exception), exception, exception.__traceback__)
        )[-2000:]  # Max 2000 Zeichen Traceback

        retry_msg = aio_pika.Message(
            body=original.body,
            headers=updated_headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            expiration=delay_ms,           # Per-Message TTL → nach Ablauf DLX → Haupt-Queue
            message_id=original.message_id,  # Gleiche Message-ID beibehalten
            content_type=original.content_type,
            correlation_id=original.correlation_id,
        )

        await self._publish_channel.default_exchange.publish(
            retry_msg,
            routing_key=dq_name,
        )

    async def nack_prefetched_messages(self) -> None:
        """Nacked alle prefetched aber unverarbeiteten Messages (Shutdown)."""
        for msg in self._prefetched_messages:
            try:
                await msg.nack(requeue=True)
                logger.debug("Prefetched Message %s zurueck in Queue", msg.message_id)
            except Exception:
                logger.warning("Nack fehlgeschlagen fuer %s", msg.message_id, exc_info=True)
        self._prefetched_messages.clear()

    def _deserialize_args(self, message: AbstractIncomingMessage) -> tuple:
        """Deserialisiert Task-Argumente aus dem Message-Body."""
        import msgspec
        data = msgspec.json.decode(message.body)
        return tuple(data.get("args", []))

    def _deserialize_kwargs(self, message: AbstractIncomingMessage) -> dict:
        """Deserialisiert Task-Keyword-Argumente aus dem Message-Body."""
        import msgspec
        data = msgspec.json.decode(message.body)
        return data.get("kwargs", {})
```

## 2.4 Manuelles Ack/Nack mit aio-pika IncomingMessage

```python
"""Referenz: Alle Ack/Nack-Varianten mit aio_pika.IncomingMessage."""

import aio_pika
from aio_pika.abc import AbstractIncomingMessage


async def ack_patterns(message: AbstractIncomingMessage) -> None:
    """Demonstriert alle Ack/Nack-Moeglichkeiten."""

    # --- ACK: Message erfolgreich verarbeitet ---
    await message.ack()
    # Nach dem Ack ist die Message endgueltig aus der Queue entfernt.
    # Doppeltes Ack: MessageProcessError (RuntimeError).

    # --- ACK mit multiple=True ---
    await message.ack(multiple=True)
    # Ackt ALLE Messages bis zu dieser delivery_tag.
    # Sinnvoll fuer Batch-Verarbeitung. Gefaehrlich bei gemischten Delivery-Modi.

    # --- NACK: Negative Acknowledgement ---
    await message.nack(requeue=True)
    # requeue=True: Message wird zurueck an den Anfang der Queue gestellt.
    # RabbitMQ liefert sie erneut aus (moeglicherweise an einen anderen Consumer).
    # VORSICHT: Kann Endlosschleife verursachen wenn der Fehler persistent ist!

    await message.nack(requeue=False)
    # requeue=False: Message wird NICHT zurueck in die Queue gestellt.
    # Wenn DLX konfiguriert: Message geht an DLX → DLQ.
    # Wenn kein DLX: Message wird VERWORFEN (unwiderruflich!).

    # --- REJECT: Wie NACK, aber nur fuer eine einzelne Message ---
    await message.reject(requeue=False)
    # Semantisch identisch mit nack(requeue=False) fuer eine Message.
    # AMQP 0-9-1 basic.reject kennt kein multiple-Flag.

    # --- message.process() Context Manager (NICHT fuer SwarmQ) ---
    # SwarmQ verwendet MANUELLES Ack/Nack, NICHT den process() CM.
    # Grund: SwarmQ braucht volle Kontrolle ueber den Ack-Zeitpunkt
    # (nach Result-Write, nach Retry-Publish, etc.).
    #
    # Der process() CM wird hier nur der Vollstaendigkeit halber gezeigt:
    async with message.process(requeue=True, reject_on_redelivered=True):
        # Erfolg → auto-ack
        # Exception → auto-reject(requeue=True)
        # Exception + redelivered → auto-reject(requeue=False)
        pass
```

---

# 3. Graceful Shutdown

## 3.1 Shutdown-Zustandsmaschine

```
RUNNING ──(SIGTERM/SIGINT)──> DRAINING ──(Tasks fertig ODER Soft-Timeout)──> SHUTTING_DOWN ──(Cleanup fertig)──> STOPPED
   │                              │                                                │
   │                              │ Akzeptiert KEINE neuen Messages                │ Nack prefetched
   │                              │ Consumer abbestellt                             │ Locks freigeben
   │                              │ Scheduler-Lock freigegeben                     │ Connections schliessen
   │                              │ Laufende Tasks duerfen fertig werden            │
   │                              │                                                │
   │                              └──(Hard-Timeout)──> SHUTTING_DOWN               │
   │                                  Tasks werden cancelled                       │
   │                                                                               │
   └──(Normalbetrieb)                                                              └──> Prozess endet
       Messages empfangen
       Tasks ausfuehren
       Results speichern
```

### Zustandsuebergaenge im Detail

| Zustand | Eintritt durch | Erlaubte Operationen | Verbotene Operationen |
|---|---|---|---|
| **RUNNING** | Worker-Start | Alles: consume, execute, ack, schedule | — |
| **DRAINING** | SIGTERM/SIGINT empfangen | Laufende Tasks beenden, ack/nack | Neue Messages annehmen, Scheduler-Dispatch |
| **SHUTTING_DOWN** | Soft-Timeout abgelaufen ODER alle Tasks fertig | Task.cancel(), nack prefetched, close connections | Task-Ausfuehrung starten |
| **STOPPED** | Cleanup abgeschlossen | — (Prozess endet) | Alles |

## 3.2 Consumer-Cancellation: aio-pika Details

```python
"""swarmq/worker/shutdown.py — Broker-Interaktion waehrend Shutdown."""

from __future__ import annotations

import asyncio
import logging
from enum import Enum, auto

import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

logger = logging.getLogger("swarmq.shutdown")


class WorkerState(Enum):
    RUNNING = auto()
    DRAINING = auto()
    SHUTTING_DOWN = auto()
    STOPPED = auto()


class GracefulShutdownManager:
    """Verwaltet den vollstaendigen Shutdown-Prozess eines Workers.

    Verantwortlich fuer:
    - Consumer-Cancellation (ohne Message-Verlust)
    - In-Flight-Task-Handling (Soft/Hard Timeout)
    - Scheduler-Lock-Release
    - Connection-Cleanup
    """

    def __init__(
        self,
        consumer: TaskConsumer,
        connection: AbstractRobustConnection,
        channel: AbstractRobustChannel,
        publish_channel: AbstractRobustChannel,
        backend: ValkeyBackend,
        scheduler_lock: SchedulerLock | None,
        *,
        soft_timeout: float = 30.0,
        hard_timeout: float = 5.0,
    ) -> None:
        self._consumer = consumer
        self._connection = connection
        self._channel = channel
        self._publish_channel = publish_channel
        self._backend = backend
        self._scheduler_lock = scheduler_lock
        self._soft_timeout = soft_timeout
        self._hard_timeout = hard_timeout
        self._state = WorkerState.RUNNING

    @property
    def state(self) -> WorkerState:
        return self._state

    async def shutdown(self) -> None:
        """Vollstaendiger Shutdown-Ablauf.

        Reihenfolge ist KRITISCH — Aenderungen nur mit grosser Vorsicht:

        1. Consumer stoppen (keine neuen Messages)
        2. Scheduler-Lock freigeben (kein Dispatch nach Lock-Release)
        3. Auf laufende Tasks warten (Soft-Timeout)
        4. Verbleibende Tasks canceln (Hard-Timeout)
        5. Prefetched Messages nack'en (zurueck in Queue)
        6. Broker-Connection schliessen
        7. Backend-Connection schliessen
        """
        if self._state != WorkerState.RUNNING:
            logger.warning("Shutdown bereits eingeleitet (State: %s)", self._state.name)
            return

        logger.info("Graceful Shutdown eingeleitet")

        # === SCHRITT 1: Consumer stoppen ===
        # queue.cancel(consumer_tag) sendet Basic.Cancel an RabbitMQ.
        # RabbitMQ bestaetigt mit Basic.CancelOk.
        # DANACH werden keine neuen Messages mehr zugestellt.
        # ABER: Messages die bereits im Channel-Buffer sind (prefetched),
        # koennen noch im _on_message Callback ankommen!
        # Deshalb setzt der Consumer sein Shutdown-Flag VOR dem Cancel.
        self._state = WorkerState.DRAINING
        self._consumer._shutdown.set()  # Flag fuer _on_message: prefetch-Buffer fuellen
        await self._consumer.stop_consuming()
        logger.info("[1/7] Consumer gestoppt — keine neuen Messages")

        # === SCHRITT 2: Scheduler-Lock freigeben ===
        # MUSS vor dem Warten auf Tasks passieren, damit ein anderer Worker
        # den Scheduler uebernehmen kann. KEIN Dispatch nach Lock-Release.
        if self._scheduler_lock is not None:
            try:
                await self._scheduler_lock.release()
                logger.info("[2/7] Scheduler-Lock freigegeben")
            except Exception:
                logger.warning("[2/7] Scheduler-Lock-Release fehlgeschlagen", exc_info=True)
        else:
            logger.debug("[2/7] Kein Scheduler-Lock vorhanden — uebersprungen")

        # === SCHRITT 3: Auf laufende Tasks warten (Soft-Timeout) ===
        # Tasks laufen zu Ende. Kein Cancel, kein Interrupt.
        active = self._consumer._active_tasks.copy()
        if active:
            logger.info("[3/7] Warte auf %d laufende Tasks (max %.1fs)", len(active), self._soft_timeout)
            done, pending = await asyncio.wait(active, timeout=self._soft_timeout)
            logger.info(
                "[3/7] %d Tasks abgeschlossen, %d noch laufend",
                len(done),
                len(pending),
            )
        else:
            pending = set()
            logger.info("[3/7] Keine laufenden Tasks — uebersprungen")

        # === SCHRITT 4: Verbleibende Tasks canceln (Hard-Timeout) ===
        # Tasks die nach dem Soft-Timeout noch laufen, werden via
        # asyncio.Task.cancel() abgebrochen. CancelledError wird propagiert.
        # Tasks koennen in finally-Bloecken aufraumen.
        self._state = WorkerState.SHUTTING_DOWN
        if pending:
            logger.warning("[4/7] Cancle %d Tasks (Hard-Timeout %.1fs)", len(pending), self._hard_timeout)
            for task in pending:
                task.cancel()
            # Warten bis CancelledError verarbeitet ist
            done2, still_pending = await asyncio.wait(pending, timeout=self._hard_timeout)
            if still_pending:
                logger.error(
                    "[4/7] %d Tasks ignorieren CancelledError — werden aufgegeben",
                    len(still_pending),
                )
        else:
            logger.debug("[4/7] Keine Tasks zum Canceln — uebersprungen")

        # === SCHRITT 5: Prefetched Messages zurueck in Queue ===
        # Messages die aus dem Channel-Buffer kamen aber nie verarbeitet wurden,
        # werden mit nack(requeue=True) zurueck in die Queue gestellt.
        # RabbitMQ liefert sie an einen anderen Consumer aus.
        prefetch_count = len(self._consumer._prefetched_messages)
        if prefetch_count > 0:
            logger.info("[5/7] Nack fuer %d prefetched Messages", prefetch_count)
            await self._consumer.nack_prefetched_messages()
        else:
            logger.debug("[5/7] Keine prefetched Messages — uebersprungen")

        # === SCHRITT 6: Broker-Connection schliessen ===
        # Schliesst Connection (und damit alle Channels).
        # Unacked Messages (falls noch vorhanden) werden von RabbitMQ
        # automatisch requeued wenn die Connection schliesst.
        try:
            await self._publish_channel.close()
            await self._channel.close()
            await self._connection.close()
            logger.info("[6/7] Broker-Connection geschlossen")
        except Exception:
            logger.warning("[6/7] Fehler beim Schliessen der Broker-Connection", exc_info=True)

        # === SCHRITT 7: Backend-Connection schliessen ===
        try:
            await self._backend.close()
            logger.info("[7/7] Backend-Connection geschlossen")
        except Exception:
            logger.warning("[7/7] Fehler beim Schliessen der Backend-Connection", exc_info=True)

        self._state = WorkerState.STOPPED
        logger.info("Graceful Shutdown abgeschlossen")
```

## 3.3 Was passiert mit unacked Messages nach Consumer-Disconnect?

Drei Szenarien und ihr Verhalten:

| Szenario | RabbitMQ-Verhalten |
|---|---|
| `queue.cancel(tag)` → Channel bleibt offen | Consumer bekommt keine neuen Messages. Bereits geprefetchte Messages bleiben unacked auf dem Channel. Worker muss sie manuell ack/nack. |
| Channel wird geschlossen (explizit oder Exception) | Alle unacked Messages auf diesem Channel werden automatisch requeued (`redelivered=True`). |
| Connection wird geschlossen / Worker crasht | Alle unacked Messages auf allen Channels dieser Connection werden automatisch requeued. |

**Fuer SwarmQ:** Wir nutzen `queue.cancel(tag)` um den Consumer zu stoppen, verarbeiten laufende Tasks zu Ende (mit ack/nack), nack'en prefetched Messages, und schliessen dann Channel und Connection.

## 3.4 Prefetched Messages: Was genau passiert

RabbitMQ sendet bis zu `prefetch_count` Messages an den Consumer, BEVOR der Consumer sie verarbeitet hat. Bei `prefetch_count=10` und 3 laufenden Tasks sind 7 Messages im Channel-Buffer.

```
RabbitMQ Queue: [msg_11] [msg_12] [msg_13] ...
                         │
                    ┌────┘ prefetch_count=10
                    ▼
Channel-Buffer:  [msg_4] [msg_5] [msg_6] [msg_7] [msg_8] [msg_9] [msg_10]
                    │         │
                    │    (diese warten auf Verarbeitung)
                    ▼
Worker Tasks:    [msg_1: running] [msg_2: running] [msg_3: running]
```

Bei `queue.cancel(tag)`:
- Messages 1-3: Laufende Tasks — warten auf Completion, dann ack/nack.
- Messages 4-10: Im Channel-Buffer — koennen noch per Callback ankommen. SwarmQ's `_on_message` erkennt `shutdown.is_set()` und legt sie in `_prefetched_messages`.
- Messages 11+: Bleiben in der RabbitMQ Queue.

Beim Nack von Messages 4-10: `await msg.nack(requeue=True)` → zurueck an die Queue.

## 3.5 Selektives Nack bei gecancelten Tasks

```python
"""Ergaenzung zu consumer.py: CancelledError-Handling in _process_message."""


async def _process_message_with_cancel_handling(
    self,
    message: AbstractIncomingMessage,
) -> None:
    """Erweiterte Message-Verarbeitung mit CancelledError-Handling."""
    try:
        # ... normale Task-Ausfuehrung (wie in _process_message) ...
        await self._execute_task(message)
    except asyncio.CancelledError:
        # Task wurde waehrend der Ausfuehrung cancelled (Hard-Timeout).
        # Message zurueck in die Queue — wird beim naechsten Mal
        # hoffentlich von einem gesunden Worker verarbeitet.
        logger.warning(
            "Task %s cancelled (Shutdown) — Message wird requeued",
            message.message_id,
        )
        try:
            await message.nack(requeue=True)
        except Exception:
            # Connection koennte bereits geschlossen sein.
            # In diesem Fall requeued RabbitMQ die Message automatisch.
            logger.debug("Nack nach Cancel fehlgeschlagen — RabbitMQ requeued automatisch")
        raise  # CancelledError MUSS weitergegeben werden
    except Exception as exc:
        await self._handle_task_failure(self._current_task, self._current_task_info, message, exc)
```

---

# 4. Middleware + Retry + DLQ Interaktion

## 4.1 Exakte Ausfuehrungsreihenfolge bei Task-Fehler

```
Task-Code wirft ConnectionError
  │
  ▼
post_execution() wird NICHT aufgerufen (Task ist fehlgeschlagen)
  │
  ▼
Middleware on_error() feuert (in UMGEKEHRTER Registrierungsreihenfolge)
  │  ├── PrometheusMiddleware.on_error()  — zaehlt Fehler-Metrik
  │  ├── LoggingMiddleware.on_error()     — loggt Exception
  │  └── SentryMiddleware.on_error()      — sendet an Sentry
  │
  ▼
Retry-Entscheidung (NACH on_error, NICHT davor):
  │
  ├── retry_exceptions match? UND retry_count < max_retries?
  │     │
  │     ├── JA:
  │     │     ▼
  │     │   Signal TASK_RETRYING feuert
  │     │     ▼
  │     │   Middleware on_retry() feuert (Vorwaertsreihenfolge)
  │     │     ▼
  │     │   Neue Message in {queue}.DQ mit TTL (Backoff-Delay)
  │     │     ▼
  │     │   Original-Message wird GEACKT (nicht genackt!)
  │     │     ▼
  │     │   Middleware after_ack() feuert
  │     │
  │     └── NEIN:
  │           ▼
  │         Signal TASK_FAILED feuert
  │           ▼
  │         Message wird rejected (requeue=False)
  │           ▼
  │         RabbitMQ DLX → Message in {queue}.XQ
  │           ▼
  │         Middleware after_nack() feuert
```

## 4.2 Offene Fragen: Klaerung

### Frage: Feuert on_error VOR oder NACH der Retry-Entscheidung?

**Antwort: DAVOR.** `on_error` feuert sobald die Exception erkannt wird, BEVOR die Retry-Logik greift. Das gibt Middleware die Moeglichkeit, den Fehler zu loggen/melden unabhaengig davon ob ein Retry stattfindet.

### Frage: Kann on_error-Middleware einen Retry VERHINDERN?

**Antwort: NEIN.** `on_error` ist rein informativ (fire-and-forget). Es gibt keine Moeglichkeit, die Exception zu schlucken oder die Retry-Entscheidung zu beeinflussen. Begruendung: Middleware soll observieren, nicht kontrollieren. Eine Middleware die Retries verhindert waere ein versteckter Kontrollfluss.

Falls eine Middleware Retries beeinflussen MUSS (z.B. Circuit Breaker), sollte sie `before_process_message` verwenden und `SkipMessage` werfen — nicht `on_error`.

### Frage: Kann on_error-Middleware die Exception AENDERN?

**Antwort: NEIN.** Die Exception wird per Referenz uebergeben, aber die Retry-Logik nutzt die Original-Exception. Eine Middleware kann die Exception nicht austauschen. Der `retry_exceptions`-Check verwendet `isinstance()` auf die Original-Exception.

### Frage: Was wenn on_error-Middleware selbst eine Exception wirft?

**Antwort: Die Exception wird geloggt und geschluckt.** Die Retry-Logik laeuft trotzdem weiter. Eine defekte Middleware darf niemals den Worker zum Absturz bringen oder die Retry-Logik blockieren.

```python
"""swarmq/middleware/stack.py — Middleware-Aufruf mit Fehlertoleranz."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("swarmq.middleware")


class MiddlewareStack:
    """Verwaltet die geordnete Liste von Middlewares."""

    def __init__(self, middlewares: list[Middleware] | None = None) -> None:
        self._middlewares: list[Middleware] = middlewares or []

    async def call_on_error(
        self,
        task: Task,
        message: AbstractIncomingMessage,
        exception: Exception,
    ) -> None:
        """Ruft on_error auf allen Middlewares auf (umgekehrte Reihenfolge).

        Fehler in on_error werden geloggt und geschluckt — die Retry-Logik
        laeuft IMMER weiter, unabhaengig davon ob Middleware-on_error crasht.
        """
        for middleware in reversed(self._middlewares):
            try:
                result = middleware.on_error(task, message, exception)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.critical(
                    "Middleware %s.on_error() hat eine Exception geworfen — wird ignoriert",
                    type(middleware).__name__,
                    exc_info=True,
                )

    async def call_on_retry(
        self,
        task: Task,
        message: AbstractIncomingMessage,
        exception: Exception,
        retry_count: int,
    ) -> None:
        """Ruft on_retry auf allen Middlewares auf (Vorwaertsreihenfolge)."""
        for middleware in self._middlewares:
            try:
                result = middleware.on_retry(task, message, exception, retry_count)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.critical(
                    "Middleware %s.on_retry() hat eine Exception geworfen — wird ignoriert",
                    type(middleware).__name__,
                    exc_info=True,
                )

    async def call_pre_execution(self, task: Task, message: Any) -> None:
        """Ruft pre_execution auf allen Middlewares auf (Vorwaertsreihenfolge).

        ACHTUNG: Wenn eine Middleware hier SkipMessage wirft, wird der Task
        uebersprungen und after_skip_message aufgerufen.
        """
        for middleware in self._middlewares:
            result = middleware.pre_execution(task, message)
            if asyncio.iscoroutine(result):
                await result

    async def call_post_execution(
        self,
        task: Task,
        message: Any,
        *,
        result: Any = None,
    ) -> None:
        """Ruft post_execution auf allen Middlewares auf (umgekehrte Reihenfolge)."""
        for middleware in reversed(self._middlewares):
            try:
                res = middleware.post_execution(task, message, result=result)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                logger.critical(
                    "Middleware %s.post_execution() hat eine Exception geworfen — wird ignoriert",
                    type(middleware).__name__,
                    exc_info=True,
                )
```

### Frage: Erstellt der Retry eine NEUE Message oder requeued die ORIGINALE?

**Antwort: NEUE Message.** Das Dramatiq-Pattern wird uebernommen:

1. Eine neue `aio_pika.Message` wird erstellt mit:
   - Gleichem `body` (Task-Argumente)
   - Gleichem `message_id` (Tracking bleibt konsistent)
   - Aktualisierten Headers (`x-retry-count`, `x-last-error-*`, `x-last-traceback`)
   - `expiration=delay_ms` (Per-Message TTL fuer den Backoff-Delay)
2. Die neue Message wird in die `{queue}.DQ` publiziert.
3. Die Original-Message wird **geackt** (nicht genackt!).

**Warum nicht `nack(requeue=True)`:**
- `nack(requeue=True)` stellt die Message SOFORT wieder zu — kein Delay moeglich.
- Kein Platz fuer aktualisierte Headers (retry_count, traceback).
- Kein Backoff-Mechanismus.

**Warum ack statt nack:**
- Die neue Message uebernimmt die Verantwortung. Die Original-Message hat ihren Zweck erfuellt.
- Wenn ack + publish nicht atomar ist (theoretisches Problem): Im schlimmsten Fall wird die Message doppelt verarbeitet (at-least-once), nie verloren.

## 4.3 Sonderfall: RetryAfter und NoRetry Exceptions

```python
"""swarmq/exceptions.py — Task-Steuerungs-Exceptions."""


class RetryAfter(Exception):
    """Task moechte explizit retried werden mit spezifischem Delay.

    Usage:
        raise RetryAfter(delay=60)  # Retry in 60 Sekunden
    """

    def __init__(self, delay: int) -> None:
        self.delay = delay
        super().__init__(f"Retry requested after {delay}s")


class NoRetry(Exception):
    """Task moechte NICHT retried werden, trotz konfiguriertem max_retries.

    Usage:
        raise NoRetry("Permanenter Fehler — Retry sinnlos")
    """
    pass
```

Integration in `_handle_task_failure`:

```python
async def _handle_task_failure(self, task_instance, task_info, message, exception):
    """Erweitert um RetryAfter und NoRetry."""

    # on_error feuert IMMER, unabhaengig vom Exception-Typ
    try:
        await self._middleware.call_on_error(task_instance, message, exception)
    except Exception:
        logger.exception("Middleware on_error hat selbst eine Exception geworfen")

    # NoRetry: Sofort zur DLQ, egal was task_info sagt
    if isinstance(exception, NoRetry):
        await self._signals.emit("TASK_FAILED", task_instance, message, exception)
        await message.reject(requeue=False)
        return

    headers = message.headers or {}
    retry_count = headers.get("x-retry-count", 0)

    # RetryAfter: Retry mit explizitem Delay (ueberschreibt Backoff-Berechnung)
    if isinstance(exception, RetryAfter):
        if retry_count < task_info.max_retries:
            delay_ms = exception.delay * 1000
            await self._signals.emit("TASK_RETRYING", task_instance, message, exception)
            await self._middleware.call_on_retry(task_instance, message, exception, retry_count)
            await self._publish_retry(message, task_info, retry_count, delay_ms, exception)
            await message.ack()
            return
        # max_retries bereits erreicht — trotzdem zur DLQ
        await self._signals.emit("TASK_FAILED", task_instance, message, exception)
        await message.reject(requeue=False)
        return

    # Normaler Retry-Flow (wie in Abschnitt 2.3)
    should_retry = self._should_retry(task_info, exception, retry_count)
    if should_retry:
        delay_ms = self._compute_backoff(retry_count, task_info.retry_backoff)
        await self._signals.emit("TASK_RETRYING", task_instance, message, exception)
        await self._middleware.call_on_retry(task_instance, message, exception, retry_count)
        await self._publish_retry(message, task_info, retry_count, delay_ms, exception)
        await message.ack()
    else:
        await self._signals.emit("TASK_FAILED", task_instance, message, exception)
        await message.reject(requeue=False)
```

---

# 5. Timeout-Implementierung

## 5.1 Architektur-Entscheidung

| Mechanismus | Python-API | Verhalten | Gewaehlt fuer |
|---|---|---|---|
| `asyncio.timeout()` (Python 3.11+) | Context Manager | Wirft `TimeoutError` wenn Deadline ueberschritten | **hard_timeout** |
| `asyncio.create_task()` + Watchdog | Separater Ueberwachungs-Task | Kann beliebige Aktionen ausloesen | **soft_timeout** |
| `asyncio.wait_for()` | Wrapped Coroutine | Cancelt den Task bei Timeout | Nicht gewaehlt (cancelt sofort — keine Soft-Phase) |

**Design:**
- `soft_timeout`: Watchdog-Task der nach Ablauf `SoftTimeoutError` in den laufenden Task injiziert.
- `hard_timeout`: `asyncio.timeout()` Context Manager — cancelt den Task mit `CancelledError`.
- `hard_timeout` MUSS groesser als `soft_timeout` sein (wird bei TaskInfo-Validierung geprueft).

## 5.2 SoftTimeoutError

```python
"""swarmq/exceptions.py — Ergaenzung."""


class SoftTimeoutError(Exception):
    """Wird in einen Task injiziert wenn sein soft_timeout ablaeuft.

    Der Task kann diese Exception fangen und graceful reagieren:

        async def run(self):
            try:
                await long_running_operation()
            except SoftTimeoutError:
                await save_partial_results()
                return partial_result

    Wenn der Task SoftTimeoutError NICHT faengt, wird die Exception
    nach oben propagiert und wie ein normaler Task-Fehler behandelt
    (on_error, Retry-Logik, etc.).
    """
    pass
```

## 5.3 Vollstaendiger Timeout-Code

```python
"""swarmq/worker/timeout.py — Soft- und Hard-Timeout-Implementierung."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Coroutine

logger = logging.getLogger("swarmq.timeout")


class TaskTimeoutManager:
    """Implementiert Soft- und Hard-Timeout fuer Task-Ausfuehrung.

    Soft-Timeout:
        - Injiziert SoftTimeoutError in den laufenden Task.
        - Task kann die Exception fangen und graceful reagieren.
        - Loest Signal TASK_TIMEOUT aus.
        - Loest Middleware on_error() aus (SoftTimeoutError IST eine Exception).

    Hard-Timeout:
        - Cancelt den Task via asyncio.Task.cancel().
        - CancelledError wird propagiert.
        - Task hat eine kurze Frist fuer finally-Bloecke.
        - Message wird nack(requeue=True).
    """

    def __init__(
        self,
        soft_timeout: float | None,
        hard_timeout: float | None,
        signals: SignalDispatcher,
    ) -> None:
        self._soft_timeout = soft_timeout
        self._hard_timeout = hard_timeout
        self._signals = signals

    async def execute_with_timeout(
        self,
        coro: Coroutine[Any, Any, Any],
        task_instance: Any,
        message: Any,
    ) -> Any:
        """Fuehrt eine Coroutine mit Soft- und Hard-Timeout aus.

        Returns:
            Das Ergebnis der Coroutine.

        Raises:
            SoftTimeoutError: Wenn soft_timeout ablaeuft und der Task
                die Exception nicht faengt.
            asyncio.CancelledError: Wenn hard_timeout ablaeuft.
        """
        task = asyncio.current_task()
        assert task is not None

        # Task als eigenstaendigen Task starten damit wir ihn canceln koennen
        execution_task = asyncio.create_task(coro, name="swarmq-task-execution")

        # Watchdog-Tasks fuer Soft- und Hard-Timeout
        soft_watchdog: asyncio.Task | None = None
        hard_watchdog: asyncio.Task | None = None

        try:
            if self._soft_timeout is not None:
                soft_watchdog = asyncio.create_task(
                    self._soft_timeout_watchdog(
                        execution_task, self._soft_timeout, task_instance, message
                    ),
                    name="swarmq-soft-timeout-watchdog",
                )

            if self._hard_timeout is not None:
                hard_watchdog = asyncio.create_task(
                    self._hard_timeout_watchdog(
                        execution_task, self._hard_timeout, task_instance, message
                    ),
                    name="swarmq-hard-timeout-watchdog",
                )

            return await execution_task

        finally:
            # Watchdog-Tasks aufraumen wenn der Haupt-Task fertig ist
            if soft_watchdog is not None and not soft_watchdog.done():
                soft_watchdog.cancel()
                try:
                    await soft_watchdog
                except asyncio.CancelledError:
                    pass

            if hard_watchdog is not None and not hard_watchdog.done():
                hard_watchdog.cancel()
                try:
                    await hard_watchdog
                except asyncio.CancelledError:
                    pass

    async def _soft_timeout_watchdog(
        self,
        execution_task: asyncio.Task,
        timeout: float,
        task_instance: Any,
        message: Any,
    ) -> None:
        """Wartet soft_timeout Sekunden, dann injiziert SoftTimeoutError."""
        await asyncio.sleep(timeout)

        if execution_task.done():
            return

        logger.warning(
            "Soft-Timeout (%.1fs) fuer Task %s abgelaufen — injiziere SoftTimeoutError",
            timeout,
            getattr(message, "message_id", "unknown"),
        )

        # Signal TASK_TIMEOUT feuern
        await self._signals.emit("TASK_TIMEOUT", task_instance, message, timeout)

        # SoftTimeoutError in den Task injizieren.
        # cancel() mit msg-Parameter (Python 3.9+) wuerde CancelledError senden.
        # Stattdessen: Wir nutzen Task.cancel() NICHT — wir werfen die Exception
        # ueber einen Umweg: Wir schreiben ein Flag und der Task muss kooperieren.
        #
        # ABER: Fuer echte Injection brauchen wir einen Trick.
        # Python hat keine native "inject exception into coroutine" API.
        # Die Loesung: Task.cancel() mit einer Custom-Message, dann im
        # Task-Wrapper die CancelledError in SoftTimeoutError umwandeln.
        #
        # ALTERNATIVE (gewaehlt): cancel() den Task und fange CancelledError
        # im Wrapper ab. Wenn es der Soft-Timeout ist, konvertiere zu SoftTimeoutError.

        # Wir setzen ein Flag und canceln den Task
        self._soft_timeout_triggered = True
        execution_task.cancel(msg="soft_timeout")

    async def _hard_timeout_watchdog(
        self,
        execution_task: asyncio.Task,
        timeout: float,
        task_instance: Any,
        message: Any,
    ) -> None:
        """Wartet hard_timeout Sekunden, dann cancelt den Task endgueltig."""
        await asyncio.sleep(timeout)

        if execution_task.done():
            return

        logger.error(
            "Hard-Timeout (%.1fs) fuer Task %s abgelaufen — Task wird gecancelt",
            timeout,
            getattr(message, "message_id", "unknown"),
        )

        # Endgueltiger Cancel — keine Gnade
        execution_task.cancel(msg="hard_timeout")
```

## 5.4 Integration in den Task-Execution-Wrapper

```python
"""swarmq/worker/executor.py — Task-Ausfuehrung mit Timeout und Middleware."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger("swarmq.executor")


class TaskExecutor:
    """Fuehrt einen einzelnen Task mit Timeout, Middleware und Ack-Semantik aus."""

    def __init__(
        self,
        timeout_manager: TaskTimeoutManager,
        middleware: MiddlewareStack,
        signals: SignalDispatcher,
        backend: ValkeyBackend,
        publish_channel: AbstractRobustChannel,
    ) -> None:
        self._timeout = timeout_manager
        self._middleware = middleware
        self._signals = signals
        self._backend = backend
        self._publish_channel = publish_channel

    async def execute(
        self,
        task_instance: Task,
        task_info: TaskInfo,
        message: AbstractIncomingMessage,
        args: tuple,
        kwargs: dict,
    ) -> None:
        """Fuehrt einen Task vollstaendig aus inkl. Timeout und Ack."""

        # FIRE_AND_FORGET: Sofortiger Ack
        if task_info.delivery == DeliveryMode.FIRE_AND_FORGET:
            await message.ack()
            await self._execute_task_body(task_instance, task_info, message, args, kwargs)
            return

        # AT_LEAST_ONCE: Ack nach Completion + Result-Write
        try:
            result = await self._execute_task_body(
                task_instance, task_info, message, args, kwargs
            )

            # Result in Valkey speichern
            if task_info.result_ttl != 0:
                await self._backend.store_result(
                    message.message_id, result, ttl=task_info.result_ttl
                )

            await message.ack()

        except asyncio.CancelledError as exc:
            # Pruefen ob es ein Soft-Timeout oder Hard-Timeout ist
            cancel_msg = getattr(exc, "args", ("",))
            if cancel_msg and cancel_msg[0] == "soft_timeout":
                # Soft-Timeout: Konvertiere zu SoftTimeoutError
                soft_exc = SoftTimeoutError(
                    f"Task {task_info.name} ueberschritt soft_timeout von {task_info.soft_timeout}s"
                )
                soft_exc.__cause__ = exc
                await self._handle_failure(task_instance, task_info, message, soft_exc)
            else:
                # Hard-Timeout oder Shutdown-Cancel: Message zurueck in Queue
                logger.warning("Task %s cancelled — nack(requeue=True)", message.message_id)
                await message.nack(requeue=True)
                raise

        except Exception as exc:
            await self._handle_failure(task_instance, task_info, message, exc)

    async def _execute_task_body(
        self,
        task_instance: Task,
        task_info: TaskInfo,
        message: AbstractIncomingMessage,
        args: tuple,
        kwargs: dict,
    ) -> Any:
        """Fuehrt den Task-Body mit Timeout und Middleware aus.

        Returns:
            Task-Ergebnis.

        Raises:
            SoftTimeoutError, asyncio.CancelledError, oder beliebige Task-Exceptions.
        """
        await self._middleware.call_pre_execution(task_instance, message)

        async def _run_task() -> Any:
            await task_instance.pre_execution()
            try:
                result = await task_instance.run(*args, **kwargs)
            except asyncio.CancelledError:
                raise  # Nicht in post_execution wenn cancelled
            await task_instance.post_execution()
            return result

        # Timeout-Wrapper (soft + hard)
        if task_info.soft_timeout is not None or task_info.hard_timeout is not None:
            timeout_mgr = TaskTimeoutManager(
                soft_timeout=task_info.soft_timeout,
                hard_timeout=task_info.hard_timeout,
                signals=self._signals,
            )
            result = await timeout_mgr.execute_with_timeout(
                _run_task(), task_instance, message
            )
        else:
            result = await _run_task()

        await self._middleware.call_post_execution(task_instance, message, result=result)
        return result

    async def _handle_failure(
        self,
        task_instance: Task,
        task_info: TaskInfo,
        message: AbstractIncomingMessage,
        exception: Exception,
    ) -> None:
        """Delegiert an die Retry/DLQ-Logik aus Abschnitt 4."""
        # Identisch mit _handle_task_failure aus dem Consumer — wird dort aufgerufen
        raise exception  # Propagiert zum Consumer fuer Retry/DLQ-Handling
```

## 5.5 Timeout-Interaktion mit Middleware

| Timeout-Typ | Loest on_error aus? | Loest post_execution (Middleware) aus? | Retry oder DLQ? |
|---|---|---|---|
| **soft_timeout** | **JA** — `SoftTimeoutError` ist eine normale Exception | **NEIN** — Task ist fehlgeschlagen, `post_execution` wird uebersprungen | **Retry** wenn `SoftTimeoutError` in `retry_exceptions` oder `retry_exceptions=None` (alle). Standard: Retry. |
| **hard_timeout** | **NEIN** — `CancelledError` wird direkt als nack(requeue=True) behandelt | **NEIN** | **Weder Retry noch DLQ** — Message wird requeued fuer erneute Zustellung. Kein Retry-Counter-Inkrement. |
| **Kein Timeout** | N/A | JA (bei Erfolg) | Abhaengig von Exception-Typ |

**Begruendung Hard-Timeout → Requeue statt Retry:**
- Hard-Timeout bedeutet: Der Worker ist ueberlastet oder der Task haengt.
- Retry mit Backoff wuerde das Problem nicht loesen — der naechste Worker hat vielleicht mehr Kapazitaet.
- Requeue ist die sicherere Option: Die Message geht sofort zurueck in die Queue.
- Der Retry-Counter wird NICHT erhoeht — es war kein Business-Logik-Fehler.

## 5.6 Alternative Soft-Timeout-Implementierung ohne cancel()

Die `cancel(msg="soft_timeout")`-Methode hat den Nachteil, dass sie die Coroutine unterbricht. Eine kooperative Alternative nutzt ein asyncio.Event:

```python
"""Alternativer Ansatz: Kooperativer Soft-Timeout via Event."""

from __future__ import annotations

import asyncio


class CooperativeSoftTimeout:
    """Kooperativer Soft-Timeout: Setzt ein Flag, unterbricht den Task NICHT.

    Vorteile:
    - Task kann graceful reagieren (z.B. Teilergebnis speichern).
    - Kein CancelledError — Task laeuft weiter.

    Nachteile:
    - Task MUSS das Flag pruefen (`self.context.timeout_exceeded`).
    - Wenn der Task das Flag ignoriert, laeuft er bis zum Hard-Timeout.

    Verwendung im Task:
        async def run(self):
            for chunk in data:
                if self.context.timeout_exceeded:
                    await self.save_partial()
                    return partial_result
                await process(chunk)
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._watchdog: asyncio.Task | None = None

    @property
    def exceeded(self) -> bool:
        """True wenn der Soft-Timeout abgelaufen ist."""
        return self._event.is_set()

    async def start(self, timeout: float) -> None:
        """Startet den Timeout-Watchdog."""
        self._watchdog = asyncio.create_task(self._watch(timeout))

    async def _watch(self, timeout: float) -> None:
        await asyncio.sleep(timeout)
        self._event.set()

    async def cleanup(self) -> None:
        if self._watchdog is not None and not self._watchdog.done():
            self._watchdog.cancel()
            try:
                await self._watchdog
            except asyncio.CancelledError:
                pass
```

**Empfehlung fuer SwarmQ:** Den `cancel(msg="soft_timeout")`-Ansatz als Primaer-Mechanismus nutzen (funktioniert auch wenn der Task nicht kooperiert). Den kooperativen Ansatz als zusaetzliche API bereitstellen (`self.context.timeout_exceeded`) fuer Tasks die graceful mit Soft-Timeout umgehen wollen.

---

## Quellen

### aio-pika / RabbitMQ
- aio-pika Quellcode: github.com/mosquito/aio-pika (v9.6.2)
- aio-pika Dokumentation: aio-pika.readthedocs.io
- RabbitMQ Dead Lettering: rabbitmq.com/docs/dlx
- RabbitMQ Quorum Queues: rabbitmq.com/docs/quorum-queues
- RabbitMQ Consumer Cancellation: rabbitmq.com/docs/consumer-cancel
- RabbitMQ Confirms: rabbitmq.com/docs/confirms

### Dramatiq
- Dramatiq Retries Middleware: github.com/Bogdanp/dramatiq → `dramatiq/middleware/retries.py`
- Dramatiq RabbitmqBroker: `dramatiq/brokers/rabbitmq.py` (Queue-Deklaration)
- Dramatiq common.py: `dramatiq/common.py` (compute_backoff)

### asyncio
- asyncio.timeout (Python 3.11+): docs.python.org/3/library/asyncio-task.html#asyncio.timeout
- asyncio.Task.cancel: docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel
- Task Cancellation (Python 3.9+, msg parameter): docs.python.org/3/whatsnew/3.9.html

### Best Practices
- AWS Architecture Blog: Exponential Backoff And Jitter
- RabbitMQ Best Practices: cloudamqp.com/blog/part1-rabbitmq-best-practice.html
- Graceful Shutdowns with asyncio: roguelynn.com/words/asyncio-graceful-shutdowns/
