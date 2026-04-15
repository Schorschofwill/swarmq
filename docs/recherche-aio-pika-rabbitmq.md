# Recherche: Async Python Libraries fuer RabbitMQ (SwarmQ Phase 1)

Stand: 2026-04-15 | aio-pika 9.6.2 | aiormq 6.9.4 | pamqp 3.3.0

---

## Inhaltsverzeichnis

1. [aio-pika -- Primaerer Kandidat](#1-aio-pika----primaerer-kandidat)
2. [aiormq -- Low-Level Alternative](#2-aiormq----low-level-alternative)
3. [Best Practices fuer RabbitMQ mit async Python](#3-best-practices-fuer-rabbitmq-mit-async-python)
4. [Performance-Ueberlegungen](#4-performance-ueberlegungen)
5. [Haeufige Fallstricke](#5-haeufige-fallstricke)
6. [Python 3.14 Kompatibilitaet](#6-python-314-kompatibilitaet)

---

## 1. aio-pika -- Primaerer Kandidat

### 1.1 Status und Community

| Merkmal | Wert |
|---|---|
| **Aktuelle Version** | 9.6.2 (22.03.2026) |
| **GitHub Stars** | ~1.450 |
| **Contributors** | ~100 |
| **Autor** | Dmitry Orlov (mosquito) |
| **Lizenz** | Apache 2.0 |
| **Abhaengigkeiten** | aiormq, yarl |
| **Python-Support** | 3.9+ (offiziell) |
| **Maintenance** | Aktiv -- regelmaessige Releases (9.5.8 Nov 2025, 9.6.0 Feb 2026, 9.6.2 Maerz 2026) |

**Bewertung:** Gut gepflegt, groesste async AMQP Library fuer Python. Klarer Industriestandard. TaskIQ nutzt ebenfalls aio-pika als Broker-Backend.

### 1.2 Architektur: Schichtenmodell

```
aio-pika        (High-Level: Connection Mgmt, Robust Reconnect, Pooling, Message Abstraktion)
    |
aiormq          (Mid-Level: AMQP 0-9-1 Protokoll, Channel Multiplexing, Frame Handling)
    |
pamqp           (Low-Level: AMQP Frame Marshalling/Unmarshalling, reine Datenstrukturen)
    |
asyncio         (TCP Streams: asyncio.open_connection)
```

**Beziehung aio-pika <-> aiormq:**

- aiormq implementiert das vollstaendige AMQP 0-9-1 Protokoll als pure-Python asyncio Client
- aio-pika ist ein High-Level Wrapper, der aiormq intern nutzt
- aio-pika fuegt hinzu: Auto-Reconnect (RobustConnection), Channel/Connection Pooling, Message-Abstraktion mit `process()` Context Manager, Queue Iterator Pattern, Exchange-Typen als Enum, Callback Collections

**Quellcode-Struktur von aio-pika (aus der Analyse):**

```
aio_pika/
    abc.py              # Abstrakte Basisklassen, Enums (DeliveryMode, ExchangeType)
    connection.py       # Connection + connect()
    robust_connection.py # RobustConnection + connect_robust() (Auto-Reconnect)
    channel.py          # Channel (Basis)
    robust_channel.py   # RobustChannel (Auto-Restore nach Reconnect)
    exchange.py         # Exchange-Abstraktion
    robust_exchange.py  # RobustExchange (speichert Bindings, stellt sie nach Reconnect wieder her)
    queue.py            # Queue + QueueIterator
    robust_queue.py     # RobustQueue + RobustQueueIterator (speichert Consumer/Bindings)
    message.py          # Message + IncomingMessage + ProcessContext
    pool.py             # Pool<T> -- generischer Connection/Channel Pool
    transaction.py      # AMQP Transactions
    exceptions.py       # Alle Exceptions (re-exportiert von aiormq)
    tools.py            # CallbackCollection, OneShotCallback, ensure_awaitable
    patterns/
        master.py       # Master/Worker Pattern
        rpc.py          # RPC Pattern
```

### 1.3 Connection Management

#### Connection vs. RobustConnection

| Merkmal | `Connection` | `RobustConnection` |
|---|---|---|
| Auto-Reconnect | Nein | Ja |
| Channel-Restore | Nein | Ja (alle Channels, Queues, Exchanges, Consumers) |
| `reconnect_interval` | N/A | Default 5 Sekunden |
| `fail_fast` | N/A | Default True -- erster Connect-Fehler wird propagiert |
| Reconnect-Callbacks | Nein | Ja (`reconnect_callbacks`) |
| **Empfehlung fuer SwarmQ** | Nie verwenden | **Immer verwenden** |

**Empfehlung: Immer `connect_robust()` verwenden.**

```python
import aio_pika

# Einfachste Variante
connection = await aio_pika.connect_robust(
    "amqp://guest:guest@localhost/"
)

# Mit allen Optionen
connection = await aio_pika.connect_robust(
    host="rabbitmq.local",
    port=5672,
    login="swarmq",
    password="secret",
    virtualhost="/swarmq",
    timeout=10,                          # Connect-Timeout
    client_properties={
        "connection_name": "swarmq-worker-1"   # Name im RabbitMQ Admin Panel
    },
)

# Connection String Format (alle Optionen als Query-Parameter moeglich)
url = "amqp://user:pass@host:5672/vhost?heartbeat=60&reconnect_interval=5&fail_fast=1"
```

#### Auto-Reconnect Verhalten (aus Quellcode-Analyse)

Die `RobustConnection.__connection_factory()` Methode laeuft als Background-Task:

1. Wartet auf `connection_close_event`
2. Bei Verbindungsverlust: `transport = None`, `connected.clear()`
3. Versucht neue Verbindung via `Connection.connect()`
4. Bei Erfolg: Ruft `_on_connected()` auf, die alle Channels via `channel.restore()` wiederherstellt
5. Bei Fehler: Loggt Warning, wartet `reconnect_interval` Sekunden, versucht erneut
6. `fail_fast=True` (Default): Erster Connect-Fehler wird als Exception propagiert
7. `fail_fast=False`: Auch erster Connect-Fehler fuehrt zu Retry (nuetzlich fuer Startup wenn Broker noch nicht bereit)

**Wichtig fuer SwarmQ:** Bei `fail_fast=False` startet der Worker auch wenn RabbitMQ noch nicht verfuegbar ist -- die Verbindung wird hergestellt sobald der Broker erreichbar ist.

```python
# Worker der auf RabbitMQ wartet
connection = await aio_pika.connect_robust(
    "amqp://guest:guest@localhost/?fail_fast=0&reconnect_interval=5"
)
```

#### Was wird nach Reconnect wiederhergestellt?

1. **Connection** wird neu aufgebaut
2. **Alle Channels** werden via `RobustChannel.restore()` wiederhergestellt
3. **QoS/Prefetch** wird neu gesetzt (gespeichert in `_prefetch_count`, `_prefetch_size`, `_global_qos`)
4. **Alle Exchanges** werden neu deklariert (gespeichert in `_exchanges` Dict)
5. **Alle Queues** werden neu deklariert (gespeichert in `_queues` Dict)
6. **Alle Queue-Bindings** werden neu erstellt (gespeichert in `_bindings` Dict)
7. **Alle Consumer** werden neu registriert (gespeichert in `_consumers` Dict)

**Achtung:** Nur als `robust=True` deklarierte Exchanges/Queues/Bindings/Consumer werden wiederhergestellt. Default ist True.

```python
# Wird nach Reconnect NICHT wiederhergestellt:
queue = await channel.declare_queue("temp", robust=False)

# Wird nach Reconnect wiederhergestellt (Default):
queue = await channel.declare_queue("persistent_queue")
```

### 1.4 Channel Management

#### Channel-Erstellung

```python
# Channel erstellen (NICHT await -- gibt ChannelContext zurueck)
channel = connection.channel(
    channel_number=None,         # Auto-assign (empfohlen)
    publisher_confirms=True,     # Default: Publisher Confirms aktiv
    on_return_raises=False,      # True: DeliveryError bei unzustellbaren Nachrichten
)

# Channel muss initialisiert werden (via await oder async with)
channel = await connection.channel()    # Shortcut: erstellt + initialisiert

# Oder als Context Manager
async with connection.channel() as channel:
    # channel ist offen
    pass
# channel ist geschlossen
```

#### Channel-Pooling mit `aio_pika.Pool`

```python
import aio_pika
from aio_pika.pool import Pool

async def get_connection() -> aio_pika.RobustConnection:
    return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

connection_pool: Pool = Pool(get_connection, max_size=2)

async def get_channel() -> aio_pika.Channel:
    async with connection_pool.acquire() as connection:
        return await connection.channel()

channel_pool: Pool = Pool(get_channel, max_size=10)

# Nutzung:
async with channel_pool.acquire() as channel:
    await channel.default_exchange.publish(
        aio_pika.Message(body=b"Hello"),
        routing_key="test_queue",
    )
```

#### Channel pro Operation vs. Shared Channel

| Strategie | Vorteile | Nachteile | Empfehlung |
|---|---|---|---|
| **Ein Channel pro Consumer** | Isolation, ein Channel-Fehler toetet nicht andere Consumer | Mehr Ressourcen | **Fuer Consumer** |
| **Ein Channel fuer Publishing** | Einfach, weniger Overhead | Channel-Exception schliesst den Channel | **Fuer simple Publisher** |
| **Channel-Pool fuer Publishing** | Fehlerisolation, Parallelitaet | Komplexer | **Fuer High-Throughput Publishing** |
| **Channel pro Publish-Operation** | Maximale Isolation | Zu viel Overhead | Nicht empfohlen |

**Empfehlung fuer SwarmQ:**
- **Worker-Seite:** Ein dedizierter Channel pro Queue-Consumer
- **Publisher-Seite:** Channel-Pool (max_size=5-10 pro Connection) fuer `broker.schedule()`

### 1.5 Publishing

#### Publisher Confirms

Publisher Confirms sind der empfohlene Mechanismus um sicherzustellen, dass Nachrichten den Broker erreicht haben.

```python
# Publisher Confirms sind DEFAULT AN
channel = await connection.channel()  # publisher_confirms=True

exchange = await channel.declare_exchange("tasks", aio_pika.ExchangeType.DIRECT)

# publish() gibt ConfirmationFrame zurueck wenn publisher_confirms=True
confirmation = await exchange.publish(
    aio_pika.Message(
        body=b'{"task": "example"}',
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # delivery_mode=2
        content_type="application/json",
        message_id="unique-id-123",
    ),
    routing_key="task_queue",
    mandatory=True,     # True: Fehler wenn Queue nicht existiert
    timeout=10,         # Timeout fuer Confirm
)
# confirmation ist ein spec.Basic.Ack Frame bei Erfolg
```

#### mandatory Flag und Return Callbacks

```python
# Option 1: on_return_raises=True -- Exception bei unzustellbarer Nachricht
channel = await connection.channel(on_return_raises=True)
exchange = await channel.declare_exchange("tasks")

try:
    await exchange.publish(
        aio_pika.Message(body=b"test"),
        routing_key="nonexistent_queue",
        mandatory=True,
    )
except aio_pika.exceptions.DeliveryError as e:
    print(f"Nachricht konnte nicht zugestellt werden: {e}")

# Option 2: Return Callback -- Nachricht wird zurueckgegeben
channel = await connection.channel(on_return_raises=False)

async def on_return(channel, message):
    print(f"Nachricht zurueckgegeben: {message.body}")

channel.return_callbacks.add(on_return)
```

**Empfehlung fuer SwarmQ:**
- `publisher_confirms=True` (Default) fuer `AT_LEAST_ONCE` Delivery
- `mandatory=True` fuer Task-Nachrichten (Fehler wenn Queue nicht existiert)
- `on_return_raises=True` fuer den Publisher-Channel um sofort zu wissen wenn eine Nachricht nicht zugestellt werden konnte
- Fuer `FIRE_AND_FORGET`: `publisher_confirms=False` moeglich (spart einen Roundtrip)

#### Message Persistence

```python
# PERSISTENT = delivery_mode 2 -- Nachricht ueberlebt Broker-Restart
message = aio_pika.Message(
    body=b'payload',
    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # == 2
)

# NOT_PERSISTENT = delivery_mode 1 (Default!) -- Nachricht geht bei Restart verloren
message = aio_pika.Message(
    body=b'payload',
    delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT,  # == 1
)
```

**WICHTIG:** Eine persistente Nachricht ueberlebt einen Broker-Restart NUR wenn:
1. `delivery_mode=PERSISTENT` (delivery_mode=2)
2. Die Queue als `durable=True` deklariert ist
3. Die Exchange als `durable=True` deklariert ist

**Empfehlung fuer SwarmQ:** Fuer `AT_LEAST_ONCE` IMMER `delivery_mode=PERSISTENT` + `durable=True` Queue + `durable=True` Exchange.

### 1.6 Consuming

#### basic_consume vs. basic_get

| Methode | Verhalten | Performance | Anwendung |
|---|---|---|---|
| `queue.consume(callback)` | Push-basiert, Broker sendet Nachrichten aktiv | Hoch | **Standard fuer Worker** |
| `queue.get()` | Pull-basiert, Client fragt einzelne Nachricht ab | Niedrig | Einmalige Abfragen, Debugging |
| `queue.iterator()` | Async-Iterator ueber consume() | Hoch | Fuer `async for` Pattern |

```python
# === CONSUME (Push-basiert) -- empfohlen fuer Worker ===
async def on_message(message: aio_pika.IncomingMessage) -> None:
    async with message.process():
        # Nachricht wird automatisch geackt nach dem Block
        # Bei Exception: automatisch rejected
        data = message.body
        await process_task(data)

consumer_tag = await queue.consume(on_message)

# Spaeter: Consumer stoppen
await queue.cancel(consumer_tag)


# === QUEUE ITERATOR (Async For) ===
async with queue.iterator() as queue_iter:
    async for message in queue_iter:
        async with message.process():
            print(message.body)


# === BASIC_GET (Pull-basiert) -- nur fuer Spezialfaelle ===
message = await queue.get(fail=False)  # None wenn Queue leer
if message:
    async with message.process():
        print(message.body)
```

#### Prefetch Count / QoS

```python
# Prefetch pro Channel setzen
await channel.set_qos(prefetch_count=10)

# Prefetch global (fuer alle Consumer auf dem Channel)
await channel.set_qos(prefetch_count=100, global_=True)
```

| Szenario | Empfohlener Prefetch | Begruendung |
|---|---|---|
| I/O-bound Tasks (HTTP, DB) | 10-50 | Viele Tasks koennen parallel warten |
| CPU-bound Tasks | 1-2 | Nicht mehr laden als verarbeitet werden kann |
| Gemischt | 5-10 | Kompromiss |
| Quorum Queues | 10-50 | Quorum Queues brauchen hoehere Prefetch-Werte wegen interner Replikation |

**WICHTIG fuer Quorum Queues:** Quorum Queues unterstuetzen kein `global_=True` QoS. Nur per-Consumer Prefetch verwenden! Ein Versuch mit `global_=True` fuehrt zu einem Channel-Fehler.

**WICHTIG:** Prefetch 0 bei Quorum Queues wird auf max. 2000 pro Consumer begrenzt (anders als Classic Queues wo 0 = unlimited).

### 1.7 Queue Declaration

```python
# Durable Queue (ueberlebt Broker-Restart)
task_queue = await channel.declare_queue(
    "swarmq.tasks.default",
    durable=True,           # Queue ueberlebt Restart
    exclusive=False,        # Andere Connections koennen zugreifen
    auto_delete=False,      # Queue bleibt bestehen wenn keine Consumer mehr da
    arguments={
        # Dead Letter Exchange
        "x-dead-letter-exchange": "swarmq.dlx",
        "x-dead-letter-routing-key": "swarmq.tasks.default.dead",

        # TTL (Nachrichten-Ablaufzeit in Millisekunden)
        "x-message-ttl": 3600000,          # 1 Stunde

        # Maximale Queue-Laenge
        "x-max-length": 100000,             # Max 100k Nachrichten
        "x-overflow": "reject-publish",     # Ablehnen statt aelteste loeschen

        # Single Active Consumer (nur ein Consumer gleichzeitig aktiv)
        "x-single-active-consumer": True,

        # Queue-Typ (ab RabbitMQ 3.12)
        "x-queue-type": "quorum",           # oder "classic" (default)
    },
)
```

#### Queue Arguments fuer SwarmQ

| Argument | Wert | Zweck |
|---|---|---|
| `x-dead-letter-exchange` | `"swarmq.dlx"` | DLX fuer fehlgeschlagene Nachrichten |
| `x-dead-letter-routing-key` | `"<queue>.dead"` | Routing-Key fuer DLQ |
| `x-message-ttl` | Konfigurierbar | Ablaufzeit in ms |
| `x-max-length` | Konfigurierbar | Queue-Laenge begrenzen |
| `x-overflow` | `"reject-publish"` | Publisher bekommt Nack bei voller Queue |
| `x-queue-type` | `"quorum"` | **Empfohlen fuer Produktion** |
| `x-single-active-consumer` | Boolean | Fuer geordnete Verarbeitung |
| `x-delivery-limit` | Integer | Max. Redeliveries bevor DLX (Default 20 bei Quorum ab RabbitMQ 4.0) |

### 1.8 Exchange-Typen

```python
from aio_pika import ExchangeType

# === DIRECT Exchange (Default) ===
# Exakter Routing-Key Match
# Empfehlung: Fuer SwarmQ Task-Queues
direct_exchange = await channel.declare_exchange(
    "swarmq.tasks",
    ExchangeType.DIRECT,
    durable=True,
)
await queue.bind(direct_exchange, routing_key="queue_name")

# === TOPIC Exchange ===
# Pattern-basiertes Routing (* = ein Wort, # = null oder mehr Woerter)
# Empfehlung: Fuer Event-Broadcasting mit Filter
topic_exchange = await channel.declare_exchange(
    "swarmq.events",
    ExchangeType.TOPIC,
    durable=True,
)
await queue.bind(topic_exchange, routing_key="task.*.completed")
await queue.bind(topic_exchange, routing_key="task.#")  # Alle Task-Events

# === FANOUT Exchange ===
# Broadcast an alle gebundenen Queues (Routing-Key wird ignoriert)
# Empfehlung: Fuer Broadcast-Notifications
fanout_exchange = await channel.declare_exchange(
    "swarmq.broadcast",
    ExchangeType.FANOUT,
    durable=True,
)

# === HEADERS Exchange ===
# Routing basierend auf Message-Headers statt Routing-Key
# Selten benoetigt, hoeherer Overhead
headers_exchange = await channel.declare_exchange(
    "swarmq.headers",
    ExchangeType.HEADERS,
    durable=True,
)
await queue.bind(headers_exchange, arguments={
    "x-match": "all",
    "priority": "high",
    "type": "payment",
})

# === Default Exchange (Nameless) ===
# Immer vorhanden, Direct-Routing nach Queue-Name
# Perfekt fuer einfache Queue-zu-Queue Kommunikation
await channel.default_exchange.publish(
    message,
    routing_key="queue_name",  # == Queue-Name
)
```

**Empfehlung fuer SwarmQ:**
- **Task Dispatch:** Default Exchange (Routing-Key = Queue-Name) -- am einfachsten
- **Alternative mit explizitem Routing:** Direct Exchange mit Queue-Name als Routing-Key
- **Events/Monitoring:** Topic Exchange fuer Task-Lifecycle Events

### 1.9 Acknowledgment

```python
async def process_message(message: aio_pika.IncomingMessage) -> None:
    # === ACK: Nachricht erfolgreich verarbeitet ===
    await message.ack(multiple=False)
    # multiple=True: Alle Nachrichten bis zu dieser delivery_tag werden geackt

    # === NACK: Negative Acknowledgement (RabbitMQ Extension) ===
    await message.nack(
        multiple=False,
        requeue=True,       # True: Nachricht zurueck in die Queue
    )                       # False: Nachricht geht an DLX (wenn konfiguriert) oder wird verworfen

    # === REJECT: Wie NACK, aber nur fuer eine Nachricht (kein multiple) ===
    await message.reject(requeue=False)

    # === PROCESS Context Manager (empfohlen) ===
    async with message.process(
        requeue=True,                   # Bei Exception: requeue?
        reject_on_redelivered=True,     # Bei Redelivery + Exception: reject statt requeue
        ignore_processed=False,         # True: Kein Fehler wenn schon ack/nack/reject
    ):
        await do_work(message.body)
        # Erfolg -> auto-ack
        # Exception -> auto-reject(requeue=True)
        # Exception + redelivered -> auto-reject(requeue=False)  (verhindert Endlosschleife!)
```

**SwarmQ Delivery Mode Mapping:**

| SwarmQ Mode | Ack-Zeitpunkt | Verhalten bei Fehler |
|---|---|---|
| `AT_LEAST_ONCE` | Nach `post_execution()` | `nack(requeue=False)` -> DLX |
| `FIRE_AND_FORGET` | Sofort bei Empfang (`no_ack=True` oder sofort `ack()`) | Nachricht verloren |

```python
# AT_LEAST_ONCE Implementation
async def handle_task(message: aio_pika.IncomingMessage) -> None:
    try:
        task_data = deserialize(message.body)
        await task.pre_execution()
        result = await task.run(*task_data.args, **task_data.kwargs)
        await task.post_execution()
        await message.ack()
    except Exception:
        # Nicht requeuen -- an DLX senden fuer spaetere Analyse
        await message.reject(requeue=False)

# FIRE_AND_FORGET Implementation
async def handle_task_fire_forget(message: aio_pika.IncomingMessage) -> None:
    # no_ack=True beim consume() -> kein ack noetig
    task_data = deserialize(message.body)
    await task.run(*task_data.args, **task_data.kwargs)
```

### 1.10 Error Handling

#### Exception-Hierarchie (aus Quellcode)

```
AMQPError (aiormq.exceptions)
├── AMQPConnectionError (+ ConnectionError)
│   ├── IncompatibleProtocolError
│   ├── AuthenticationError
│   ├── ProbableAuthenticationError
│   └── ConnectionClosed
│       ├── ConnectionFrameError (501)
│       ├── ConnectionSyntaxError (502)
│       ├── ConnectionCommandInvalid (503)
│       ├── ConnectionChannelError (504)
│       ├── ConnectionUnexpectedFrame (505)
│       ├── ConnectionResourceError (506)
│       ├── ConnectionNotAllowed (530)
│       ├── ConnectionNotImplemented (540)
│       └── ConnectionInternalError (541)
├── AMQPChannelError
│   └── ChannelClosed
│       ├── ChannelAccessRefused (403)
│       ├── ChannelNotFoundEntity (404)
│       ├── ChannelLockedResource (405)
│       └── ChannelPreconditionFailed (406)
├── DeliveryError
│   └── PublishError
├── DuplicateConsumerTag
├── ProtocolSyntaxError
│   └── InvalidFrameError
└── MethodNotImplemented

ChannelInvalidStateError (RuntimeError) -- Channel nicht offen

MessageProcessError (AMQPError) -- aio_pika-spezifisch, Message schon verarbeitet
QueueEmpty (AMQPError + asyncio.QueueEmpty) -- aio_pika-spezifisch
```

#### Welche Exceptions wo fangen

```python
from aio_pika.exceptions import (
    AMQPConnectionError,    # Verbindungsprobleme
    AMQPChannelError,       # Channel-Level Fehler (Channel wird geschlossen!)
    ChannelClosed,          # Channel wurde geschlossen
    ChannelPreconditionFailed,  # Queue/Exchange Deklaration mit falschen Parametern
    ChannelNotFoundEntity,  # Queue/Exchange existiert nicht
    DeliveryError,          # Nachricht konnte nicht zugestellt werden
    PublishError,           # Publish fehlgeschlagen (mandatory + kein Match)
    MessageProcessError,    # Message schon ack/nack/reject
    QueueEmpty,             # basic_get auf leere Queue
    CONNECTION_EXCEPTIONS,  # Tuple aller Verbindungs-Exceptions (fuer except)
)

# Fuer Publishing
try:
    await exchange.publish(message, routing_key="key", mandatory=True)
except PublishError as e:
    # Nachricht konnte nicht geroutet werden (kein passende Queue)
    log.error("Publish failed: %s", e)
except DeliveryError as e:
    # Broker hat die Nachricht abgelehnt (nack)
    log.error("Delivery failed: %s", e)

# Fuer Consuming -- Channel-Exceptions sind KRITISCH
try:
    await channel.declare_queue("test", durable=True)
except ChannelPreconditionFailed:
    # Queue existiert bereits mit anderen Parametern
    # ACHTUNG: Channel ist jetzt GESCHLOSSEN! Neuen Channel oeffnen!
    channel = await connection.channel()
except ChannelNotFoundEntity:
    # Queue existiert nicht (bei passive=True)
    pass
```

**KRITISCH fuer SwarmQ:** Channel-Level Exceptions (4xx) schliessen den Channel! Nach einem `ChannelClosed` Error muss ein neuer Channel geoeffnet werden. `RobustChannel` macht das automatisch.

### 1.11 Connection Loss Handling

```python
connection = await aio_pika.connect_robust("amqp://localhost/")

# Callback wenn Verbindung verloren geht
async def on_close(connection, exc):
    log.warning("Connection lost: %s", exc)

connection.close_callbacks.add(on_close)

# Callback wenn Verbindung wiederhergestellt ist
async def on_reconnect(connection):
    log.info("Reconnected!")

connection.reconnect_callbacks.add(on_reconnect)

# Channel-Reopen Callback
channel = await connection.channel()
async def on_channel_reopen(channel):
    log.info("Channel reopened!")

channel.reopen_callbacks.add(on_channel_reopen)
```

### 1.12 Heartbeat und Timeout-Erkennung

Heartbeats werden auf aiormq-Ebene verwaltet:

```python
# Heartbeat-Konfiguration (Default: 60 Sekunden)
connection = await aio_pika.connect_robust(
    "amqp://localhost/?heartbeat=60"
)
```

Aus der Quellcode-Analyse von `aiormq.Connection`:
- `HEARTBEAT_INTERVAL_MULTIPLIER = 0.5` -- Heartbeat wird alle `heartbeat_timeout / 2` Sekunden gesendet
- `HEARTBEAT_GRACE_MULTIPLIER = 3` -- Nach `heartbeat_timeout * 3` Sekunden ohne Frame wird die Connection als stuck betrachtet
- Bei stuck Connection: Reader-Task wird gecancelt, Connection wird geschlossen
- RobustConnection erkennt dies und startet Reconnect

**Default:** Heartbeat 60s -> Heartbeat alle 30s gesendet -> Connection gilt als stuck nach 183s (61 * 3) ohne Frame.

### 1.13 Transactions vs. Publisher Confirms

```python
# === PUBLISHER CONFIRMS (empfohlen) ===
# Default: Aktiviert
channel = await connection.channel(publisher_confirms=True)
# Jedes publish() wartet auf Broker-Bestaetigung

# === TRANSACTIONS ===
# ACHTUNG: publisher_confirms und transactions sind NICHT kompatibel!
channel_tx = await connection.channel(publisher_confirms=False)
tx = channel_tx.transaction()

async with tx:
    await channel_tx.default_exchange.publish(msg1, "queue1")
    await channel_tx.default_exchange.publish(msg2, "queue2")
    # Commit bei Erfolg, Rollback bei Exception
```

| Merkmal | Publisher Confirms | Transactions |
|---|---|---|
| **Throughput** | Hoch (async, pipelined) | Niedrig (synchron, blockierend) |
| **Latenz** | Niedrig | Hoch (Roundtrip pro Tx) |
| **Batch** | Ja (mehrere publishes, confirms kommen async) | Ja (commit am Ende) |
| **Atomaritaet** | Nein (jede Nachricht einzeln bestaetigt) | Ja (alles oder nichts) |
| **Empfehlung** | **Standard fuer SwarmQ** | Nur wenn Atomaritaet noetig |

**Empfehlung fuer SwarmQ:** Immer Publisher Confirms. Transactions nur falls SwarmQ jemals atomare Batch-Publishes braucht.

### 1.14 SSL/TLS Support

```python
import ssl

# Option 1: SSL Context direkt
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.load_cert_chain("client.pem", "client.key")
ssl_context.load_verify_locations("ca.pem")

connection = await aio_pika.connect_robust(
    "amqps://user:pass@host:5671/",
    ssl_context=ssl_context,
)

# Option 2: SSL via URL Query-Parameter
connection = await aio_pika.connect_robust(
    "amqps://user:pass@host:5671/?cafile=ca.pem&certfile=client.pem&keyfile=client.key"
)

# Option 3: SSL ohne Zertifikats-Verifizierung (nur fuer Dev!)
connection = await aio_pika.connect_robust(
    "amqps://user:pass@host:5671/?no_verify_ssl=1"
)
```

---

## 2. aiormq -- Low-Level Alternative

### 2.1 Wann aiormq direkt vs. aio-pika

| Szenario | Empfehlung |
|---|---|
| Standard Task-Queue | aio-pika |
| Maximale Performance, minimaler Overhead | aiormq |
| Auto-Reconnect benoetigt | aio-pika (aiormq hat kein Auto-Reconnect) |
| Custom AMQP-Protokoll-Features | aiormq |
| Einfache API gewuenscht | aio-pika |

### 2.2 Was aio-pika ueber aiormq hinzufuegt

| Feature | aiormq | aio-pika |
|---|---|---|
| AMQP 0-9-1 Protokoll | Vollstaendig | Via aiormq |
| Auto-Reconnect | Nein | `RobustConnection` |
| Channel/Queue/Exchange Restore | Nein | `RobustChannel/Queue/Exchange` |
| Connection/Channel Pool | Nein | `aio_pika.Pool` |
| Message-Abstraktion | `DeliveredMessage` (raw) | `IncomingMessage` mit `process()`, `ack()`, `nack()` |
| Queue Iterator | Nein | `QueueIterator` fuer `async for` |
| Exchange-Typen Enum | Nein | `ExchangeType` |
| Delivery Mode Enum | Nein | `DeliveryMode` |
| Pattern: Master/Worker | Nein | `aio_pika.patterns.Master` |
| Pattern: RPC | Nein | `aio_pika.patterns.RPC` |
| Transaction Wrapper | Nein | `Transaction` Context Manager |

### 2.3 Performance-Unterschied

aiormq ist schneller weil:
- Kein Abstraktions-Overhead (kein Wrapping von Messages, Channels, etc.)
- Direkter Zugriff auf Frames
- Keine CallbackCollection Verwaltung

**Aber:** Der Unterschied ist in der Praxis gering (< 5% Throughput-Differenz) weil der Bottleneck das Netzwerk und der Broker sind, nicht der Client-Code.

**Empfehlung fuer SwarmQ:** aio-pika verwenden. Der Overhead ist vernachlaessigbar, die Vorteile (Auto-Reconnect, Pooling, Abstraktion) sind erheblich.

### 2.4 aiormq direkt verwenden (Beispiel)

```python
import aiormq

# Verbindung oeffnen
connection = await aiormq.connect("amqp://localhost/")
channel = await connection.channel()

# Publisher Confirms aktivieren
await channel.confirm_delivery()

# Queue deklarieren
await channel.queue_declare("test_queue", durable=True)

# Publish
await channel.basic_publish(
    body=b"Hello",
    exchange="",
    routing_key="test_queue",
    properties=aiormq.spec.Basic.Properties(delivery_mode=2),
)

# Consume
async def on_message(message: aiormq.abc.DeliveredMessage):
    print(message.body)
    await channel.basic_ack(message.delivery_tag)

await channel.basic_consume("test_queue", on_message)

# Kein Auto-Reconnect! Bei Verbindungsverlust -> Exception -> manueller Reconnect noetig
```

---

## 3. Best Practices fuer RabbitMQ mit async Python

### 3.1 Connection pro Prozess

**Regel: Eine Connection pro Prozess. Niemals Connections ueber Prozessgrenzen teilen.**

```python
# RICHTIG: Jeder Worker-Prozess erstellt eigene Connection
async def worker_main():
    connection = await aio_pika.connect_robust("amqp://localhost/")
    channel = await connection.channel()
    # ...

# FALSCH: Connection vor fork() erstellen und in Child-Prozessen nutzen
# connection = await aio_pika.connect_robust(...)
# os.fork()  # VERBOTEN -- Connection ist nicht fork-safe!
```

Bei `multiprocessing` mit `spawn` (Linux Default ab Python 3.14: `fork`):
- Jeder Subprozess erstellt seine eigene Connection im eigenen Event-Loop
- Connection-URLs/Credentials koennen als Argumente uebergeben werden
- Keine Shared-State-Probleme

**Empfehlung fuer SwarmQ:** Jeder Worker-Prozess erstellt bei Start eine eigene `RobustConnection` mit eigenem Channel-Pool.

### 3.2 Channels pro Connection

**RabbitMQ Empfehlung:** Nicht mehr als ~100 Channels pro Connection.

| Zweck | Channels |
|---|---|
| Consumer | 1 Channel pro Queue-Consumer |
| Publisher | Channel-Pool (5-10) |
| Admin (declare, delete) | 1 separater Channel |

```python
# SwarmQ Worker Setup
async def setup_worker(connection):
    # Admin-Channel fuer Queue/Exchange Deklaration
    admin_channel = await connection.channel()
    await setup_queues_and_exchanges(admin_channel)

    # Je ein Consumer-Channel pro Queue
    consumer_channels = {}
    for queue_name in worker_queues:
        ch = await connection.channel()
        await ch.set_qos(prefetch_count=10)
        consumer_channels[queue_name] = ch

    # Publisher-Channel Pool
    publisher_pool = Pool(
        lambda: connection.channel(),
        max_size=5,
    )
```

### 3.3 Dead Letter Exchange Setup -- DLX + DLQ Pattern

```python
async def setup_dlx(channel: aio_pika.Channel):
    # 1. DLX Exchange deklarieren
    dlx_exchange = await channel.declare_exchange(
        "swarmq.dlx",
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )

    # 2. DLQ (Dead Letter Queue) deklarieren
    dlq = await channel.declare_queue(
        "swarmq.tasks.default.dead",
        durable=True,
        arguments={
            # Optional: TTL fuer Dead Letters (z.B. 7 Tage)
            "x-message-ttl": 7 * 24 * 60 * 60 * 1000,
        },
    )

    # 3. DLQ an DLX binden
    await dlq.bind(dlx_exchange, routing_key="swarmq.tasks.default.dead")

    # 4. Task-Queue mit DLX-Konfiguration deklarieren
    task_queue = await channel.declare_queue(
        "swarmq.tasks.default",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "swarmq.dlx",
            "x-dead-letter-routing-key": "swarmq.tasks.default.dead",
            "x-queue-type": "quorum",
            # Ab RabbitMQ 4.0: Default delivery-limit ist 20
            # Explizit setzen fuer Kontrolle
            "x-delivery-limit": 5,
        },
    )

    return task_queue, dlq
```

**Wann gehen Nachrichten an die DLX?**
1. `reject(requeue=False)` oder `nack(requeue=False)`
2. Message TTL abgelaufen
3. Queue-Laenge ueberschritten (`x-max-length` + `x-overflow=reject-publish-dlx`)
4. Delivery-Limit erreicht (Quorum Queues, Default 20 ab RabbitMQ 4.0)

**Wichtig fuer Quorum Queues:** Dead-Lettering in Quorum Queues verwendet intern Publisher Confirms und ist damit at-least-once zuverlaessig. Classic Queues haben keine solche Garantie!

### 3.4 Message TTL

```python
# === Per-Queue TTL (auf alle Nachrichten in der Queue) ===
queue = await channel.declare_queue(
    "temp_queue",
    durable=True,
    arguments={
        "x-message-ttl": 60000,  # 60 Sekunden in Millisekunden
    },
)

# === Per-Message TTL (ueberschreibt Queue-TTL wenn kuerzer) ===
message = aio_pika.Message(
    body=b"data",
    expiration=30,           # 30 Sekunden (aio-pika konvertiert zu ms)
    # oder:
    # expiration=timedelta(seconds=30),
    # expiration=datetime.now() + timedelta(seconds=30),
)
```

**Empfehlung fuer SwarmQ:**
- Per-Queue TTL fuer generelle Queue-Hygiene
- Per-Message TTL fuer Tasks mit individuellem `hard_timeout`

### 3.5 Quorum Queues vs. Classic Queues

| Merkmal | Classic Queue | Quorum Queue |
|---|---|---|
| **Replikation** | Optional (Mirroring, deprecated) | Eingebaut (Raft-basiert) |
| **Datensicherheit** | Gut | **Sehr gut** |
| **Performance** | Hoeher | Etwas niedriger (Replikations-Overhead) |
| **Dead Letter** | Best-effort | At-least-once (mit Publisher Confirms intern) |
| **Global QoS** | Ja | **Nein** (nur per-Consumer) |
| **Priority** | Ja | **Nein** |
| **Lazy Mode** | Konfigurierbar | Immer lazy-artig |
| **Delivery Limit** | Nein | Ja (Default 20 ab RabbitMQ 4.0) |
| **Empfehlung** | Dev/Test | **Produktion** |

```python
# Quorum Queue deklarieren
queue = await channel.declare_queue(
    "swarmq.tasks.default",
    durable=True,  # Muss True sein fuer Quorum Queues
    arguments={
        "x-queue-type": "quorum",
        "x-delivery-limit": 5,
        "x-dead-letter-exchange": "swarmq.dlx",
        "x-dead-letter-routing-key": "swarmq.tasks.default.dead",
        "x-dead-letter-strategy": "at-least-once",  # Explizit
    },
)
```

**Empfehlung fuer SwarmQ:** Quorum Queues als Default in Produktion. Classic Queues fuer Dev/Test (einfacher, kein Cluster noetig).

### 3.6 Graceful Shutdown

```python
import asyncio
import signal
from typing import Set

class SwarmQWorker:
    def __init__(self):
        self.connection: aio_pika.RobustConnection = None
        self.consumer_tags: dict[str, str] = {}  # queue_name -> consumer_tag
        self.active_tasks: Set[asyncio.Task] = set()
        self.shutting_down = False

    async def graceful_shutdown(self):
        """Graceful Shutdown ohne Nachrichtenverlust"""
        self.shutting_down = True

        # 1. Consumer stoppen (keine neuen Nachrichten mehr empfangen)
        for queue_name, consumer_tag in self.consumer_tags.items():
            try:
                queue = self.queues[queue_name]
                await queue.cancel(consumer_tag)
            except Exception as e:
                log.warning("Error cancelling consumer for %s: %s", queue_name, e)

        # 2. Laufende Tasks abschliessen lassen (mit Timeout)
        if self.active_tasks:
            log.info("Waiting for %d active tasks to complete...", len(self.active_tasks))
            done, pending = await asyncio.wait(
                self.active_tasks,
                timeout=30,  # hard_timeout
            )

            # 3. Nicht abgeschlossene Tasks: Nachrichten nack'en
            for task in pending:
                task.cancel()
                # Die Message wird durch den Channel-Close automatisch requeued
                # (unacked messages werden bei Channel-Close requeued)

        # 4. Connection sauber schliessen
        if self.connection:
            await self.connection.close()

    def setup_signals(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self._handle_signal(s)),
            )

    async def _handle_signal(self, sig):
        log.info("Received signal %s, initiating graceful shutdown", sig.name)
        await self.graceful_shutdown()
```

**Wichtige Punkte:**
1. **Consumer canceln** BEVOR Connection geschlossen wird
2. **Laufende Tasks** zu Ende verarbeiten lassen
3. **Unacked Messages** werden bei Channel/Connection-Close automatisch requeued (RabbitMQ-Verhalten)
4. **Timeout** fuer den Shutdown-Prozess setzen

### 3.7 Broker-Restart Handling

`RobustConnection` handelt dies automatisch:
1. Connection-Loss wird erkannt (Heartbeat Timeout oder TCP Error)
2. `reconnect_interval` Sekunden warten (Default: 5s)
3. Neue Connection aufbauen
4. Alle Channels, Queues, Exchanges, Consumer wiederherstellen
5. `reconnect_callbacks` ausfuehren

**Keine manuelle Intervention noetig** wenn `connect_robust()` verwendet wird.

### 3.8 Virtual Hosts

Virtual Hosts (vhosts) sind logische Namespaces innerhalb eines RabbitMQ-Brokers.

**Wann verwenden:**
- Mehrere Umgebungen auf einem Broker (dev, staging, prod)
- Mandantenfaehigkeit (verschiedene Applikationen/Teams)
- Isolation von Queues, Exchanges, Bindings, Permissions

```python
# Verschiedene Vhosts
dev_connection = await aio_pika.connect_robust("amqp://user:pass@host/dev")
staging_connection = await aio_pika.connect_robust("amqp://user:pass@host/staging")
prod_connection = await aio_pika.connect_robust("amqp://user:pass@host/swarmq")

# Default Vhost
default_connection = await aio_pika.connect_robust("amqp://user:pass@host/")
```

**Empfehlung fuer SwarmQ:** Eigener Vhost `/swarmq` (oder konfigurierbar). Ermoeglicht saubere Trennung wenn mehrere Anwendungen denselben Broker nutzen.

---

## 4. Performance-Ueberlegungen

### 4.1 Publisher Confirms vs. Transactions: Throughput

| Methode | Messages/sec (typisch) | Faktor |
|---|---|---|
| Kein Confirm, kein Tx | ~50.000 | 1x |
| Publisher Confirms (async) | ~30.000-40.000 | 0.7x |
| Transactions | ~5.000-10.000 | 0.1-0.2x |

**Publisher Confirms sind 3-8x schneller als Transactions** weil sie asynchron bestaetigt werden (Pipeline), waehrend Transactions einen synchronen Roundtrip pro Commit benoetigen.

### 4.2 Batch Publishing

aio-pika hat kein eingebautes Batch-Publish, aber man kann es mit `asyncio.gather()` nachbauen:

```python
async def batch_publish(
    exchange: aio_pika.Exchange,
    messages: list[tuple[bytes, str]],  # (body, routing_key)
    batch_size: int = 100,
):
    """Batch-Publishing mit Publisher Confirms"""
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        tasks = [
            exchange.publish(
                aio_pika.Message(
                    body=body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=routing_key,
                mandatory=True,
            )
            for body, routing_key in batch
        ]
        await asyncio.gather(*tasks)
```

### 4.3 Channel Multiplexing Limits

- Ein RabbitMQ-Connection multiplext bis zu 65535 Channels ueber eine TCP-Verbindung
- **Praktisches Limit:** ~100 Channels pro Connection (RabbitMQ-Empfehlung)
- Bei >100 Channels: Mehrere Connections verwenden
- Channel-Lock in aiormq kann bei vielen Channels zum Bottleneck werden

### 4.4 Memory High Watermark und Flow Control

RabbitMQ hat mehrere Schutzmechanismen:
1. **Credit-based Flow Control:** Gezielt einzelne schnelle Publisher bremsen
2. **Memory Alarm:** Bei Ueberschreitung des Memory High Watermark (Default 40% RAM) werden ALLE Publisher blockiert
3. **Disk Alarm:** Bei wenig Disk Space werden ALLE Publisher blockiert

aiormq erkennt `Connection.Blocked` und `Connection.Unblocked` Frames und pausiert Publishing automatisch:

```python
# Aus aiormq/connection.py:
async def __handle_connection_blocked(self, frame):
    self.__connection_unblocked.clear()  # Stoppt Publishing

async def __handle_connection_unblocked(self, _):
    self.__connection_unblocked.set()  # Nimmt Publishing wieder auf

# ready() wartet bis Connection unblocked ist:
async def ready(self):
    await self.connected.wait()
    await self.__connection_unblocked.wait()
```

### 4.5 Lazy Queues und Quorum Queues

**Lazy Queues** (Classic):
- Nachrichten werden sofort auf Disk geschrieben statt im RAM gehalten
- Vorteil: Viel groessere Backlogs moeglich, weniger RAM
- Nachteil: Hoeherer Latenz (Disk I/O)
- Konfiguration: `x-queue-mode: lazy`

**Quorum Queues** verhalten sich aehnlich wie Lazy Queues -- Nachrichten werden schnell auf Disk persistiert. Ein expliziter Lazy Mode ist nicht noetig.

```python
# Classic Queue im Lazy Mode
lazy_queue = await channel.declare_queue(
    "large_backlog",
    durable=True,
    arguments={
        "x-queue-mode": "lazy",
    },
)

# Besser: Quorum Queue (implizit lazy-artig + repliziert)
quorum_queue = await channel.declare_queue(
    "reliable_queue",
    durable=True,
    arguments={
        "x-queue-type": "quorum",
    },
)
```

### 4.6 Consumer Concurrency

| Strategie | Throughput | Ordnung | Empfehlung |
|---|---|---|---|
| **Mehrere Consumer auf einer Queue** | Hoch (parallelisiert) | Keine Garantie | Standard fuer SwarmQ |
| **Single Active Consumer** | Niedriger | Garantiert | Fuer geordnete Verarbeitung |
| **Hoeherer Prefetch** | Hoher (Pipelining) | Keine Garantie | Fuer I/O-bound Tasks |

```python
# Mehrere Consumer (verschiedene Worker-Prozesse, gleiche Queue)
# Jeder Worker:
await channel.set_qos(prefetch_count=10)
await queue.consume(handler)

# Single Active Consumer (nur ein Consumer aktiv, Failover bei Ausfall)
queue = await channel.declare_queue(
    "ordered_queue",
    durable=True,
    arguments={"x-single-active-consumer": True},
)
```

---

## 5. Haeufige Fallstricke

### 5.1 Channel-Level Exceptions schliessen den Channel

**Problem:** Jeder AMQP 4xx Fehler (404 Not Found, 406 Precondition Failed, etc.) schliesst den Channel sofort.

**Typische Ursachen:**
- Queue mit anderen Argumenten re-deklarieren (406)
- Publish an nicht existierende Exchange (404)
- Queue.bind an nicht existierende Exchange (404)

```python
# FALSCH: Channel nach Fehler weiterverwenden
try:
    await channel.declare_queue("test", durable=True)
except ChannelPreconditionFailed:
    # Channel ist jetzt GESCHLOSSEN!
    await channel.declare_queue("test", durable=False)  # FEHLER!

# RICHTIG: Neuen Channel oeffnen
try:
    await channel.declare_queue("test", durable=True)
except ChannelPreconditionFailed:
    channel = await connection.channel()  # Neuer Channel!
    await channel.declare_queue("test", durable=False)

# ODER: RobustChannel -- stellt sich automatisch wieder her
# (aber nur bei Connection-Loss, nicht bei programmatischen Fehlern!)
```

**Empfehlung fuer SwarmQ:** Queue/Exchange-Deklarationen beim Worker-Start in einem separaten Admin-Channel durchfuehren. Fehler dort abfangen, neuen Channel oeffnen, Problem beheben.

### 5.2 Unacked Messages und Memory

**Problem:** Unacked Messages verbleiben im Broker-RAM. Bei vielen unacked Messages steigt der Memory-Verbrauch.

**Ursachen:**
- Zu hoher Prefetch Count
- Consumer verarbeitet Nachrichten zu langsam
- Consumer vergisst ack/nack

**Loesung:**
- Prefetch Count angemessen setzen
- Immer `message.process()` Context Manager verwenden (garantiert ack oder reject)
- Monitoring fuer unacked Messages einrichten

### 5.3 Connection Blocked Notifications

**Problem:** Wenn RabbitMQ unter Speicherdruck steht, werden alle Publishing-Connections blockiert.

aiormq handelt dies transparent:
- `Connection.Blocked` Frame -> `__connection_unblocked.clear()`
- Alle `publish()` Aufrufe warten automatisch
- `Connection.Unblocked` Frame -> `__connection_unblocked.set()`
- Publishing wird fortgesetzt

**Fuer SwarmQ:** Kein spezielles Handling noetig, aber Monitoring einrichten um rechtzeitig zu skalieren.

### 5.4 Message Size Limits

- **RabbitMQ Default Maximum:** 128 MiB
- **Empfehlung:** Nachrichten unter 1 MiB halten
- **SwarmQ Task Messages** sollten nur Task-Metadaten enthalten (ID, Name, Args), keine grossen Payloads
- Bei grossen Daten: Referenz auf externen Storage (S3, DB) in der Nachricht

### 5.5 Queue Length Limits und Overflow

```python
# Overflow-Strategien:
queue = await channel.declare_queue(
    "bounded_queue",
    durable=True,
    arguments={
        "x-max-length": 10000,
        # Optionen fuer x-overflow:
        "x-overflow": "drop-head",           # Aelteste Nachricht loeschen (Default)
        # "x-overflow": "reject-publish",     # Publisher bekommt Nack
        # "x-overflow": "reject-publish-dlx", # Publisher Nack + Nachricht an DLX
    },
)
```

**Empfehlung fuer SwarmQ:** `x-overflow: reject-publish` damit der Publisher weiss, dass die Queue voll ist und entsprechend reagieren kann (Backpressure).

---

## 6. Python 3.14 Kompatibilitaet

### 6.1 Bekannte Probleme

**KRITISCH: `asyncio.get_event_loop()` wirft RuntimeError in Python 3.14**

In Python 3.14 wirft `asyncio.get_event_loop()` eine RuntimeError wenn kein laufender Event-Loop existiert (zuvor wurde implizit einer erstellt). Dies betrifft sowohl aio-pika als auch aiormq an mehreren Stellen:

**Betroffene Stellen in aio-pika 9.6.2:**
- `connection.py:103` -- `self.loop = loop or asyncio.get_event_loop()`
- `pool.py:61` -- `self.loop = loop or asyncio.get_event_loop()`
- `tools.py:60` -- `loop = loop or asyncio.get_event_loop()`
- `tools.py:300` -- `self.loop = asyncio.get_event_loop()`
- `abc.py:739` -- `asyncio.get_event_loop().create_future()`
- `patterns/master.py:108` -- `self.loop = asyncio.get_event_loop()`
- `patterns/rpc.py:104` -- `self.loop = asyncio.get_event_loop()`

**Betroffene Stellen in aiormq 6.9.4:**
- `connection.py:167` -- `self._loop = asyncio.get_event_loop()`
- `connection.py:382` -- `loop or asyncio.get_event_loop()`
- `tools.py:70` -- `self.loop = asyncio.get_event_loop()`

**Auswirkung:**
- Wenn aio-pika/aiormq Objekte AUSSERHALB eines laufenden Event-Loops erstellt werden, gibt es einen RuntimeError
- **Innerhalb von `asyncio.run()` oder einer laufenden Coroutine ist es kein Problem** -- dort existiert immer ein laufender Loop
- Das heisst: Solange SwarmQ alle Objekte innerhalb von async Funktionen erstellt (was bei einem async Framework der Fall ist), sollte es funktionieren

**Workaround falls noetig:**

```python
# NICHT mehr moeglich in Python 3.14:
connection = aio_pika.Connection(url)  # RuntimeError!
await connection.connect()

# Stattdessen immer innerhalb async context:
async def main():
    connection = await aio_pika.connect_robust(url)  # OK -- Loop laeuft
```

### 6.2 asyncio Policy System Deprecation

Das gesamte asyncio Policy System ist in Python 3.14 deprecated und wird in 3.16 entfernt. Das betrifft:
- `asyncio.get_event_loop_policy()`
- `asyncio.set_event_loop_policy()`
- Custom Event Loop Policies

**Auswirkung auf SwarmQ:** Gering. SwarmQ sollte `asyncio.run()` verwenden und keine Custom Policies setzen.

### 6.3 Empfehlung

1. **aio-pika 9.6.2 sollte mit Python 3.14 funktionieren** solange alle Objekte innerhalb eines laufenden Event-Loops erstellt werden (Standard-Nutzung)
2. **Monitoring der aio-pika Releases** -- die Maintainer werden wahrscheinlich die `get_event_loop()` Aufrufe durch `get_running_loop()` ersetzen
3. **SwarmQ-Code:** Immer `asyncio.get_running_loop()` statt `asyncio.get_event_loop()` verwenden
4. **Kein `loop` Parameter** an aio-pika Funktionen uebergeben -- der Parameter ist ohnehin deprecated

```python
# SwarmQ -- korrekt fuer Python 3.14+
async def start_worker():
    # Innerhalb einer Coroutine -- Loop laeuft immer
    connection = await aio_pika.connect_robust("amqp://localhost/")
    # ...

if __name__ == "__main__":
    asyncio.run(start_worker())
```

---

## Zusammenfassung: Empfehlungen fuer SwarmQ

### Bibliothek
- **aio-pika 9.6.2** als primaere Abhaengigkeit
- aiormq kommt automatisch als Transitive Dependency

### Connection Management
- `connect_robust()` mit `fail_fast=False` fuer Worker (wartet auf Broker)
- `connect_robust()` mit `fail_fast=True` (Default) fuer Client/Publisher
- Ein Connection pro Worker-Prozess
- `reconnect_interval=5` (Default)
- `heartbeat=60` (Default)
- Eigener Vhost `/swarmq` (konfigurierbar)

### Channel Management
- Ein Channel pro Queue-Consumer
- Channel-Pool (max_size=5-10) fuer Publishing
- Separater Admin-Channel fuer Deklarationen

### Queues
- `durable=True` immer
- `x-queue-type: quorum` fuer Produktion
- DLX + DLQ fuer jede Task-Queue
- `x-delivery-limit: 5` (konfigurierbar)
- `x-overflow: reject-publish`

### Messages
- `delivery_mode=PERSISTENT` fuer AT_LEAST_ONCE
- `publisher_confirms=True` (Default)
- `mandatory=True` fuer Task-Nachrichten
- Per-Message TTL basierend auf `hard_timeout`

### Consuming
- `prefetch_count=10` als Default (konfigurierbar)
- `message.process()` Context Manager fuer sicheres Ack/Nack
- `reject_on_redelivered=True` um Endlosschleifen zu verhindern

### Graceful Shutdown
1. Consumer canceln
2. Laufende Tasks abwarten (mit Timeout)
3. Unacked Messages werden automatisch requeued
4. Connection schliessen

### Exchange-Strategie
- Default Exchange fuer einfaches Task-Routing (Routing-Key = Queue-Name)
- Optional: Direct Exchange fuer explizites Routing

---

## Quellen

- [aio-pika GitHub](https://github.com/mosquito/aio-pika)
- [aio-pika Dokumentation](https://docs.aio-pika.com/)
- [aio-pika PyPI](https://pypi.org/project/aio-pika/)
- [aio-pika Releases](https://github.com/mosquito/aio-pika/releases)
- [aio-pika Package Health](https://snyk.io/advisor/python/aio-pika)
- [RabbitMQ Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)
- [RabbitMQ Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx)
- [RabbitMQ Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch)
- [RabbitMQ Flow Control](https://www.rabbitmq.com/docs/flow-control)
- [RabbitMQ Queue Length Limits](https://www.rabbitmq.com/docs/maxlength)
- [RabbitMQ Best Practices (CloudAMQP)](https://www.cloudamqp.com/blog/part1-rabbitmq-best-practice.html)
- [RabbitMQ 4.0 Quorum Queue Features](https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0)
- [Python 3.14 What's New](https://docs.python.org/3/whatsnew/3.14.html)
- [Python 3.14 asyncio Event Loop Docs](https://docs.python.org/3/library/asyncio-eventloop.html)
- [Asyncio Changes in Python 3.14](https://python-docs.synapse.org/en/latest/explanations/asyncio_in_python_3_14/)
- [cpython Issue #130322 - Re-reverse deprecation of set_event_loop](https://github.com/python/cpython/issues/130322)
