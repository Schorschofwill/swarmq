# Recherche: Retry-Strategien und Dead Letter Queues (SwarmQ)

Stand: 2026-04-15 | RabbitMQ 4.x | aio-pika 9.6.2 | Python 3.14+

---

## Inhaltsverzeichnis

### Teil 1: Retry-Strategien
1. [Dramatiq -- Umfassendste Implementierung](#1-dramatiq----umfassendste-implementierung)
2. [Celery -- Manuelles und automatisches Retry](#2-celery----manuelles-und-automatisches-retry)
3. [TaskIQ -- Simple und Smart Retry Middleware](#3-taskiq----simple-und-smart-retry-middleware)
4. [Procrastinate -- Deklarative RetryStrategy](#4-procrastinate----deklarative-retrystrategy)
5. [arq -- Manuelles Retry via Exception](#5-arq----manuelles-retry-via-exception)
6. [RQ -- Explizite Intervall-Listen](#6-rq----explizite-intervall-listen)
7. [Best Practices: Exponential Backoff und Jitter](#7-best-practices-exponential-backoff-und-jitter)
8. [RabbitMQ-spezifische Retry-Patterns](#8-rabbitmq-spezifische-retry-patterns)
9. [Entscheidungen fuer SwarmQ: Retry](#9-entscheidungen-fuer-swarmq-retry)

### Teil 2: Dead Letter Queue (DLQ)
10. [Dramatiq -- Automatische DLQ (.XQ)](#10-dramatiq----automatische-dlq-xq)
11. [RabbitMQ Dead Letter Exchange (DLX)](#11-rabbitmq-dead-letter-exchange-dlx)
12. [Celery -- Keine native DLQ](#12-celery----keine-native-dlq)
13. [RQ -- FailedJobRegistry](#13-rq----failedjobregistry)
14. [Best Practices: DLQ-Design](#14-best-practices-dlq-design)
15. [RabbitMQ-Implementierung fuer SwarmQ: DLQ](#15-rabbitmq-implementierung-fuer-swarmq-dlq)
16. [Entscheidungen fuer SwarmQ: DLQ](#16-entscheidungen-fuer-swarmq-dlq)
17. [Zusammenfassung und Empfehlungen](#17-zusammenfassung-und-empfehlungen)

---

# Teil 1: Retry-Strategien

## 1. Dramatiq -- Umfassendste Implementierung

Dramatiq hat das ausgereifteste Retry-System aller untersuchten Libraries. Grundannahme: **Alle Actors sind idempotent**, daher wird bei jeder Exception automatisch ein Retry ausgeloest.

### 1.1 Retries-Middleware: Konfiguration

| Parameter | Default | Beschreibung |
|---|---|---|
| `max_retries` | 20 | Maximale Retry-Versuche. `None` = unbegrenzt |
| `min_backoff` | 15.000 ms (15s) | Minimale Wartezeit (muss >100ms sein) |
| `max_backoff` | 604.800.000 ms (7 Tage) | Maximale Wartezeit |
| `retry_when` | `None` | Callable `(retries_so_far, exception) -> bool` -- bedingte Retries |
| `throws` | `None` | Exception-Typen die NICHT retried werden |
| `on_retry_exhausted` | `None` | Callback-Actor der bei erschoepften Retries aufgerufen wird |

```python
# Einfaches Retry-Limit
@dramatiq.actor(max_retries=3)
def count_words(url):
    ...

# Bedingtes Retry mit Praedikat
def should_retry(retries_so_far, exception):
    return retries_so_far < 3 and isinstance(exception, HttpTimeout)

@dramatiq.actor(retry_when=should_retry)
def count_words(url):
    ...

# Exceptions die nicht retried werden
@dramatiq.actor(throws=(ValidationError, NotFoundError))
def process_item(item_id):
    ...
```

### 1.2 Backoff-Formel und Jitter

Die `compute_backoff`-Funktion in `dramatiq/common.py`:

```python
def compute_backoff(attempts, *, factor=5, jitter=True, max_backoff=2000, max_exponent=32):
    """Berechnet Backoff mit optionalem Jitter.

    Returns:
        tuple[int, int]: (naechster_attempt, backoff_ms)
    """
    exponent = min(attempts, max_exponent)
    backoff = factor * (2 ** exponent)

    if jitter:
        backoff = backoff * uniform(1, 2)  # Multiplikativer Jitter: 1x-2x
        if backoff > max_backoff:
            backoff = max_backoff * uniform(0.5, 1.0)  # 50-100% des Maximums
    else:
        backoff = min(backoff, max_backoff)

    return attempts + 1, int(backoff)
```

**Aufruf aus der Retries-Middleware:**
```python
_, delay = compute_backoff(retries, factor=min_backoff, max_backoff=max_backoff)
```

**Wichtig:** Dramatiq nutzt `random.uniform()` fuer den Jitter. Problem: `random.random()` verwendet EPOCH als Seed -- bei gleichzeitig startenden Workern koennen identische Jitter-Werte entstehen (GitHub Issue #273). Fuer SwarmQ sollte `secrets`-Modul oder `random.SystemRandom()` in Betracht gezogen werden.

### 1.3 Interner Retry-Mechanismus: Drei-Queue-Pattern

Dramatiq erstellt fuer jede Queue automatisch drei RabbitMQ-Queues:

```
default        -- Haupt-Queue (Message-Verarbeitung)
default.DQ     -- Delay-Queue (wartende Retries)
default.XQ     -- Dead-Letter-Queue (erschoepfte Retries)
```

**Ablauf bei einem Retry:**

1. Worker empfaengt Message aus `default`
2. Task schlaegt fehl (Exception)
3. Retries-Middleware erhoeht `message.options["retries"]`
4. Traceback und Requeue-Timestamp werden in Message-Options gespeichert
5. Delay wird via `compute_backoff()` berechnet
6. `broker.enqueue(message, delay=delay)` publiziert nach `default.DQ`
7. Message wartet in DQ bis TTL ablaeuft
8. RabbitMQ dead-lettert die Message zurueck nach `default` (via DLX-Konfiguration)
9. Worker empfaengt Message erneut

**Ablauf bei erschoepften Retries:**

1. `retries >= max_retries` → Message wird nach `default.XQ` verschoben
2. Optional: `on_retry_exhausted`-Actor wird aufgerufen
3. Message bleibt 7 Tage in XQ (`x-message-ttl: 604800000`)

**Sonderfall: Retry-Exception mit eigenem Delay:**

```python
from dramatiq import Retry

@dramatiq.actor
def my_task():
    raise Retry(delay=60000)  # Ueberschreibt compute_backoff
```

### 1.4 RabbitMQ-Queue-Deklaration (Dramatiq)

```python
# Haupt-Queue und DQ: Dead-Letter nach XQ
def _build_queue_arguments(self, queue_name):
    return {
        "x-dead-letter-exchange": "",                      # Default Exchange
        "x-dead-letter-routing-key": xq_name(queue_name), # → foo.XQ
    }

# XQ: TTL fuer automatische Bereinigung
def _declare_xq_queue(self, queue_name):
    channel.queue_declare(
        queue=xq_name(queue_name),
        durable=True,
        arguments={"x-message-ttl": 604800000}  # 7 Tage
    )
```

**Beobachtung:** Dramatiq nutzt den Default Exchange (`""`) mit direktem Routing-Key statt eines dedizierten DLX. Das ist einfacher, aber weniger flexibel. Die DQ hat dieselbe DLX-Konfiguration wie die Haupt-Queue -- rejected/expired Messages aus der DQ landen ebenfalls in der XQ.

### 1.5 Retry-State: Wo gespeichert?

Dramatiq speichert den gesamten Retry-State **in den Message-Options (AMQP Headers)**:

| Key | Typ | Beschreibung |
|---|---|---|
| `retries` | `int` | Aktuelle Retry-Nummer |
| `traceback` | `str` | Letzter Traceback (max. 30 Zeilen) |
| `requeue_timestamp` | `int` | Zeitpunkt der letzten Requeue (ms seit Epoch) |
| `eta` | `int` | Fruehester Ausfuehrungszeitpunkt (ms seit Epoch) |

**Vorteil:** Kein externer State-Store noetig. Alles reist mit der Message.
**Nachteil:** Message wird bei jedem Retry groesser (Traceback). Begrenzung auf 30 Zeilen Traceback als Mitigation.

---

## 2. Celery -- Manuelles und automatisches Retry

Celery bietet zwei Retry-Mechanismen: manuelles Retry im Task-Body und deklaratives Auto-Retry.

### 2.1 Manuelles Retry

```python
@app.task(bind=True, max_retries=3)
def send_email(self, recipient):
    try:
        smtp.send(recipient)
    except SMTPConnectionError as exc:
        raise self.retry(exc=exc, countdown=60)
```

| Parameter | Beschreibung |
|---|---|
| `exc` | Original-Exception (fuer Traceback) |
| `countdown` | Sekunden bis zum naechsten Versuch |
| `max_retries` | Ueberschreibt Task-Default |
| `throw` | `True` = wirft `Retry`-Exception, `False` = kehrt zurueck |

### 2.2 Automatisches Retry (autoretry_for)

```python
@app.task(
    autoretry_for=(ConnectionError, TimeoutError),
    retry_backoff=True,       # Exponential Backoff aktivieren
    retry_backoff_max=600,    # Max 10 Minuten (Default: 600s)
    retry_jitter=True,        # Jitter aktivieren (Default: True)
    max_retries=5,
)
def fetch_data(url):
    ...
```

### 2.3 Backoff-Formel

```python
# Celery Backoff-Berechnung:
countdown = retry_backoff * (2 ** retries)
countdown = min(countdown, retry_backoff_max)

# Mit Jitter (retry_jitter=True):
countdown = random.uniform(0, countdown)  # Full Jitter: 0 bis countdown
```

**Besonderheit:** `retry_backoff=True` setzt den Basis-Faktor auf 1 Sekunde. `retry_backoff=30` setzt ihn auf 30 Sekunden. Der Jitter ist **Full Jitter** (Zufallswert zwischen 0 und berechnetem Maximum).

### 2.4 acks_on_failure_or_timeout

| Wert | Verhalten |
|---|---|
| `True` (Default) | Message wird auch bei Failure/Timeout geackt |
| `False` | Message wird bei Failure nicht geackt → RabbitMQ redelivert |

**Warnung:** `acks_on_failure_or_timeout=False` kann Endlosschleifen verursachen wenn keine `max_retries` gesetzt sind.

---

## 3. TaskIQ -- Simple und Smart Retry Middleware

TaskIQ bietet zwei Retry-Middlewares mit unterschiedlicher Komplexitaet.

### 3.1 SimpleRetryMiddleware

Sofortiges Re-Kick ohne Delay. Erzeugt einen **neuen Task** (Re-Kick), nicht ein Requeue der Original-Message.

```python
from taskiq import ZeroMQBroker, SimpleRetryMiddleware

broker = ZeroMQBroker().with_middlewares(
    SimpleRetryMiddleware(default_retry_count=3),
)

@broker.task(retry_on_error=True, max_retries=20)
async def fragile_task():
    raise Exception("Temporaerer Fehler")
```

**Mechanismus:** Bei Exception wird der Task mit identischen Parametern neu eingereicht. Der Retry-Zaehler wird in **Task-Labels** gespeichert (nicht in Message-Headers).

### 3.2 SmartRetryMiddleware

Erweiterte Variante mit Delay, Exponential Backoff und Jitter.

```python
from taskiq.middlewares import SmartRetryMiddleware

broker = ZeroMQBroker().with_middlewares(
    SmartRetryMiddleware(
        default_retry_count=5,
        default_delay=10,          # 10 Sekunden Basis-Delay
        use_jitter=True,           # Zufaellige Delay-Variation
        use_delay_exponent=True,   # Exponentieller Backoff
        max_delay_exponent=120,    # Maximaler Delay: 120 Sekunden
    ),
)

@broker.task(retry_on_error=True, max_retries=10, delay=15)
async def my_task():
    raise Exception("Retry mit 15s Basis-Delay")
```

### 3.3 Kritische Unterschiede zu Dramatiq

| Aspekt | Dramatiq | TaskIQ |
|---|---|---|
| **Retry-Mechanismus** | Requeue der Original-Message via DQ | Neuer Task (Re-Kick) |
| **Delay-Implementierung** | RabbitMQ DLX + TTL | Scheduler/Sleep im Worker |
| **State-Speicherung** | AMQP Message-Headers | Task-Labels |
| **DLQ** | Automatisch (.XQ) | Nicht vorhanden |

**Bewertung fuer SwarmQ:** TaskIQs Re-Kick-Ansatz ist einfacher, hat aber Nachteile: neue Task-ID pro Retry (Tracking schwieriger), kein atomares Requeue, und der Original-Task-Kontext geht verloren.

---

## 4. Procrastinate -- Deklarative RetryStrategy

Procrastinate (PostgreSQL-basiert) hat das sauberste deklarative API:

```python
from procrastinate import RetryStrategy

@app.task(
    retry_strategy=RetryStrategy(
        max_attempts=10,
        wait=5,                  # Konstanter Anteil: 5 Sekunden
        linear_wait=2,           # Linearer Anteil: +2s pro Versuch
        exponential_wait=3,      # Exponentieller Anteil: 3^(attempts+1)
        retry_exceptions={ConnectionError, IOError},
    )
)
async def fragile_task():
    ...
```

### 4.1 Backoff-Formel

```python
total_wait = wait + linear_wait * attempts + exponential_wait ** (attempts + 1)
```

**Beispiel** (wait=5, linear_wait=2, exponential_wait=3):

| Versuch | Berechnung | Wartezeit |
|---|---|---|
| 1 | 5 + 2*1 + 3^2 | 16s |
| 2 | 5 + 2*2 + 3^3 | 36s |
| 3 | 5 + 2*3 + 3^4 | 92s |
| 4 | 5 + 2*4 + 3^5 | 256s |

**Bewertung:** Sehr flexibel durch die Kombination aus konstant + linear + exponentiell. Kein Jitter eingebaut -- muesste manuell hinzugefuegt werden. `retry_exceptions` als Set ist elegant (Procrastinate prueft mit `isinstance`).

---

## 5. arq -- Manuelles Retry via Exception

arq hat den minimalistischsten Ansatz: der Task entscheidet selbst ueber Retries.

```python
from arq import Retry

async def download_file(ctx, url):
    try:
        return await fetch(url)
    except ConnectionError:
        if ctx['job_try'] >= 5:
            raise  # Endgueltig fehlgeschlagen
        raise Retry(defer=ctx['job_try'] * 5)  # 5s, 10s, 15s, 20s
```

| Kontext-Variable | Beschreibung |
|---|---|
| `ctx['job_try']` | Aktuelle Versuchsnummer (startet bei 1) |
| `Retry(defer=N)` | Retry in N Sekunden |

**Bewertung:** Maximale Flexibilitaet, aber jeder Task muss seine eigene Retry-Logik implementieren. Kein deklarativer Ansatz. Gut fuer Prototypen, schlecht fuer einheitliche Retry-Policies.

---

## 6. RQ -- Explizite Intervall-Listen

```python
from rq import Retry

# Sofortiger Retry (3x)
queue.enqueue(my_func, retry=Retry(max=3))

# Fester Intervall (3x, je 60s)
queue.enqueue(my_func, retry=Retry(max=3, interval=60))

# Explizite Intervall-Liste (Pseudo-Exponential)
queue.enqueue(my_func, retry=Retry(max=3, interval=[10, 30, 60]))
```

**Besonderheiten:**
- `interval`-Liste: Letzter Wert wird fuer alle weiteren Retries verwendet
- Erfordert `--with-scheduler` beim Worker-Start
- Kein eingebauter Jitter oder Exponential Backoff
- FailedJobRegistry speichert endgueltig fehlgeschlagene Jobs (Default-TTL: 1 Jahr)
- `requeue_job(job_id)` fuer manuelles Replay aus der FailedJobRegistry

---

## 7. Best Practices: Exponential Backoff und Jitter

### 7.1 Warum Jitter kritisch ist: Thundering Herd

Ohne Jitter waehlen alle Clients denselben Retry-Zeitpunkt:

```
t=0:   100 Clients → Server (Fehler)
t=15s: 100 Clients → Server (Fehler, identischer Backoff)
t=30s: 100 Clients → Server (Fehler, identischer Backoff)
```

Jitter verteilt die Retries zeitlich und verhindert diese synchronisierten Wellen.

### 7.2 Drei Jitter-Strategien (AWS Architecture Blog)

**Basis-Formel (kein Jitter):**

```python
sleep = min(max_backoff, base * 2 ** attempt)
```

**Full Jitter (empfohlen fuer die meisten Faelle):**

```python
sleep = random_between(0, min(max_backoff, base * 2 ** attempt))
```

Sehr aggressives Spreading. Clients verteilen sich maximal. Weniger Gesamtarbeit als andere Strategien, aber etwas laengere Gesamtzeit.

**Equal Jitter:**

```python
v = min(max_backoff, base * 2 ** attempt)
sleep = v / 2 + random_between(0, v / 2)
```

Garantiert mindestens 50% der berechneten Wartezeit. Weniger aggressiv als Full Jitter. **Laut AWS der schlechteste Kompromiss** -- weder schnell noch sparsam.

**Decorrelated Jitter:**

```python
sleep = min(max_backoff, random_between(base, sleep_prev * 3))
```

Basiert auf dem vorherigen Sleep-Wert, nicht auf der Versuchsnummer. Etwas mehr Server-Last als Full Jitter, aber aehnlich gute Verteilung.

### 7.3 Empfehlung fuer SwarmQ

**Full Jitter** implementieren als Default:
- Weniger Gesamtarbeit (Server-Last) als Equal und Decorrelated
- Maximale Verteilung der Retries
- Einfach zu implementieren

```python
import secrets

def compute_backoff(attempt: int, base_ms: int, max_ms: int) -> int:
    """Full Jitter Backoff nach AWS-Empfehlung."""
    exp_backoff = min(max_ms, base_ms * (2 ** attempt))
    # secrets statt random: kryptographisch sicher, kein Seed-Problem
    return secrets.randbelow(exp_backoff + 1)
```

### 7.4 Exception-Filterung

Nicht jede Exception sollte einen Retry ausloesen:

| Retry-wuerdig | Nicht Retry-wuerdig |
|---|---|
| `ConnectionError` | `ValidationError` |
| `TimeoutError` | `ValueError` |
| `OSError` (temporaer) | `TypeError` |
| HTTP 429, 500, 502, 503, 504 | HTTP 400, 401, 403, 404 |
| `asyncio.TimeoutError` | `PermissionError` |

**Pattern aus Dramatiq/Celery:**

```python
# Positiv-Liste: NUR diese Exceptions retrien (Procrastinate, Celery)
retry_exceptions=[ConnectionError, TimeoutError]

# Negativ-Liste: Diese Exceptions NICHT retrien (Dramatiq)
throws=(ValidationError, NotFoundError)
```

**Empfehlung fuer SwarmQ:** Beide Ansaetze unterstuetzen:
- `retry_exceptions` (Positiv-Liste) = sicherer, expliziter
- `no_retry_exceptions` (Negativ-Liste, wie Dramatiq `throws`) = bequemer fuer generische Retries

### 7.5 Retry-Budget

Neben `max_retries` sollte auch eine maximale Gesamt-Retry-Dauer existieren:

```python
class TaskInfo:
    max_retries: int = 3              # Maximale Anzahl Versuche
    max_retry_time: int | None = 3600 # Max. 1 Stunde Gesamt-Retry-Zeit (Sekunden)
```

Dramatiq loest das ueber `max_age` (maximales Message-Alter). Das ist eleganter als ein separates `max_retry_time`, weil es den gesamten Message-Lifecycle abdeckt.

### 7.6 Idempotenz-Anforderungen

Retried Tasks **muessen** idempotent sein. SwarmQ sollte das in der Dokumentation prominent kommunizieren:

- `AT_LEAST_ONCE` Delivery erfordert Idempotenz
- Idempotenz-Keys (z.B. Task-ID) sollten als Pattern dokumentiert werden
- `unique_for` in TaskInfo kann als einfache Idempotenz-Garantie dienen

### 7.7 Retry-Sichtbarkeit im Task

Der Task sollte wissen, in welchem Versuch er sich befindet:

```python
# Pattern: Attempt-Nummer im Task-Kontext
class MyTask(Task):
    async def run(self, *args, **kwargs):
        attempt = self.context.attempt  # 1 = erster Versuch, 2 = erster Retry, ...
        if attempt > 1:
            logger.warning("Retry-Versuch %d", attempt)
```

Alle untersuchten Libraries bieten das: arq (`ctx['job_try']`), Dramatiq (`message.options['retries']`), Celery (`self.request.retries`).

---

## 8. RabbitMQ-spezifische Retry-Patterns

### 8.1 Pattern A: DLX + TTL (Dramatiq-Pattern)

Das einfachste Pattern -- eine Delay-Queue pro Haupt-Queue:

```
                    ┌─────────────────────────────────────┐
                    │                                     │
  Producer → [main.Q] → Worker                           │
                │                                        │
                │ (nack/Fehler)                          │
                ▼                                        │
           [main.DQ]  ──── (TTL abgelaufen) ─────────────┘
                │           DLX → default exchange
                │           routing-key → main.Q
                │
                │ (max_retries erschoepft)
                ▼
           [main.XQ]  ──── (TTL: 7 Tage, dann geloescht)
```

**aio-pika Implementierung:**

```python
import aio_pika

async def declare_retry_topology(channel: aio_pika.Channel, queue_name: str):
    """Deklariert das Drei-Queue-Pattern fuer eine Queue."""

    dq_name = f"{queue_name}.DQ"
    xq_name = f"{queue_name}.XQ"

    # Dead-Letter-Queue (XQ): Messages bleiben 7 Tage
    xq = await channel.declare_queue(
        xq_name,
        durable=True,
        arguments={
            "x-message-ttl": 604_800_000,  # 7 Tage in ms
        },
    )

    # Delay-Queue (DQ): Expired Messages → zurueck zur Haupt-Queue
    dq = await channel.declare_queue(
        dq_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",           # Default Exchange
            "x-dead-letter-routing-key": queue_name, # → zurueck zu main.Q
        },
    )

    # Haupt-Queue: Rejected Messages → XQ
    main_q = await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": xq_name,  # → zur XQ bei reject
        },
    )

    return main_q, dq, xq
```

**Retry-Publish mit Per-Message TTL:**

```python
async def retry_message(
    channel: aio_pika.Channel,
    message: aio_pika.IncomingMessage,
    delay_ms: int,
    queue_name: str,
):
    """Publiziert eine Message in die Delay-Queue mit Per-Message TTL."""

    dq_name = f"{queue_name}.DQ"

    retry_message = aio_pika.Message(
        body=message.body,
        headers={
            **(message.headers or {}),
            "x-retry-count": (message.headers or {}).get("x-retry-count", 0) + 1,
            "x-original-queue": queue_name,
            "x-last-error": "...",  # Traceback (gekuerzt)
        },
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        expiration=delay_ms,  # Per-Message TTL in ms → nach Ablauf → DLX → main.Q
        message_id=message.message_id,
        correlation_id=message.correlation_id,
    )

    default_exchange = channel.default_exchange
    await default_exchange.publish(retry_message, routing_key=dq_name)

    # Original-Message acknowledged (aus der Haupt-Queue entfernt)
    await message.ack()
```

**Wichtig:** Die Original-Message wird **geackt** (nicht rejected), und eine neue Message mit TTL wird in die DQ publiziert. Das ist zuverlaessiger als nack+requeue, weil es variable Delays erlaubt.

### 8.2 Problem: Per-Message TTL und Queue-Ordering

RabbitMQ entfernt expired Messages **nur vom Kopf der Queue**. Eine Message mit 60s TTL blockiert eine nachfolgende Message mit 5s TTL:

```
DQ: [msg_A: TTL=60s] [msg_B: TTL=5s] [msg_C: TTL=10s]
              │
              └── msg_B und msg_C warten, bis msg_A expired ist
```

**Loesungen:**

**Loesung 1: Mehrere Delay-Queues mit Queue-Level TTL (empfohlen)**

```python
# Vordefinierte Delay-Stufen
DELAY_LEVELS = [5_000, 15_000, 60_000, 300_000, 900_000]  # 5s, 15s, 1m, 5m, 15m

async def declare_tiered_delay_queues(channel, queue_name):
    """Deklariert eine Delay-Queue pro Delay-Stufe."""
    for delay_ms in DELAY_LEVELS:
        dq_name = f"{queue_name}.DQ.{delay_ms}"
        await channel.declare_queue(
            dq_name,
            durable=True,
            arguments={
                "x-message-ttl": delay_ms,                  # Queue-Level TTL
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,    # → zurueck zu main.Q
                "x-expires": delay_ms + 60_000,             # Queue loeschen wenn leer + 60s
            },
        )

async def retry_with_tier(channel, message, attempt, queue_name):
    """Waehlt die passende Delay-Queue basierend auf dem Versuch."""
    delay_ms = DELAY_LEVELS[min(attempt, len(DELAY_LEVELS) - 1)]
    dq_name = f"{queue_name}.DQ.{delay_ms}"

    # ... publish nach dq_name (analog zu oben)
```

**Vorteil:** Alle Messages in einer Queue haben dieselbe TTL → kein Blocking-Problem.
**Nachteil:** Mehr Queues (5-7 pro Haupt-Queue), feste Delay-Stufen (kein stufenloses Backoff).

**Loesung 2: Per-Message TTL mit Akzeptanz der Ungenauigkeit**

In der Praxis ist das Blocking-Problem weniger schlimm als es klingt:
- Messages mit kurzem TTL werden erst verzoegert, nicht verloren
- Bei moderatem Retry-Aufkommen stehen Messages selten hintereinander
- Dramatiq nutzt diesen Ansatz und es funktioniert in der Praxis

**Empfehlung fuer SwarmQ:** Per-Message TTL als Default (wie Dramatiq), mit Option fuer tiered Delay-Queues bei hohem Retry-Aufkommen.

### 8.3 RabbitMQ 4.0: Quorum Queues und delivery-limit

Ab RabbitMQ 4.0 haben Quorum Queues ein **Default delivery-limit von 20**. Messages die oefter als 20x zugestellt werden, werden automatisch:
- **Geloescht** (wenn kein DLX konfiguriert)
- **Dead-lettered** (wenn DLX konfiguriert)

```python
# Quorum Queue mit delivery-limit
await channel.declare_queue(
    "my_queue",
    durable=True,
    arguments={
        "x-queue-type": "quorum",
        "x-delivery-limit": 5,                # Ueberschreibt Default (20)
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "my_queue.XQ",
    },
)
```

**Warnung:** Ohne DLX-Konfiguration **gehen Messages bei Erreichen des delivery-limit verloren**. SwarmQ muss **immer** einen DLX konfigurieren.

**Kann delivery-limit das Application-Level Retry-Counting ersetzen?**

| Aspekt | delivery-limit | Application-Level |
|---|---|---|
| Delay/Backoff | Kein Delay (sofortige Redelivery) | Variables Delay moeglich |
| Zaehlung | RabbitMQ zaehlt automatisch | Muss in Message-Headers verwaltet werden |
| Flexibilitaet | Nur max. Zustellungen, keine Bedingungen | `retry_when`, `throws`, Exception-Filterung |
| Sichtbarkeit | x-death Header | Eigene Headers |

**Fazit:** delivery-limit ist ein **Sicherheitsnetz**, ersetzt aber nicht die Application-Level Retry-Logik (kein Backoff, keine Exception-Filterung). SwarmQ sollte **beides** nutzen: eigene Retry-Logik + delivery-limit als Fallback.

### 8.4 Tanzu RabbitMQ 4.2: Native Delayed Queues

Tanzu RabbitMQ (kommerziell, nicht Open Source) fuehrt in 4.2 native Delayed Queues ein:

```python
# NUR Tanzu RabbitMQ -- nicht in Open-Source-Version verfuegbar
await channel.declare_queue(
    "delayed-tasks",
    durable=True,
    arguments={"x-queue-type": "delayed"},
)

# Publish mit Delay-Header
message = aio_pika.Message(
    body=payload,
    headers={"x-delay": 5000},  # 5 Sekunden Delay
)
```

**Fuer SwarmQ nicht relevant:** SwarmQ soll mit Standard-RabbitMQ funktionieren. Das DLX+TTL-Pattern bleibt der Standardansatz.

---

## 9. Entscheidungen fuer SwarmQ: Retry

### 9.1 Requeue Original vs. Neue Message?

| Ansatz | Vorteile | Nachteile |
|---|---|---|
| **Requeue Original** (nack+requeue) | Atomare Operation, gleiche Message-ID | Kein variables Delay moeglich, RabbitMQ stellt sofort zu |
| **Neue Message in DQ** (Dramatiq) | Variables Delay via TTL, Retry-Metadata in Headers | Zwei Operationen (ack + publish), nicht atomar |
| **Re-Kick** (TaskIQ) | Einfach, funktioniert mit jedem Broker | Neue Task-ID, Original-Kontext verloren |

**Empfehlung:** Dramatiq-Pattern (neue Message in DQ mit Per-Message TTL). Behaelt die Original-Message-ID bei, erlaubt variable Delays, und der Retry-State reist in den Headers mit.

### 9.2 Retry-State: Message Headers vs. Valkey?

| Speicherort | Vorteile | Nachteile |
|---|---|---|
| **Message Headers** | Kein externer Zugriff noetig, State reist mit Message | Message wird groesser, State geht verloren bei Message-Loss |
| **Valkey** | Zentral abfragbar, Monitoring, unabhaengig von Message | Extra Roundtrip pro Retry, muss bei Message-Loss aufgeraeumt werden |
| **Beides** | Headers fuer Worker-Logik, Valkey fuer Monitoring | Konsistenz muss sichergestellt werden |

**Empfehlung:** **Message Headers als primaerer Speicherort** (wie Dramatiq). Retry-Count und letzter Fehler werden beim Result-Write auch nach Valkey geschrieben (fuer Monitoring/Observability), aber der Worker liest den State aus den Headers.

### 9.3 Variable Delays mit RabbitMQ?

**Empfehlung:** Per-Message TTL in einer einzelnen DQ pro Queue (wie Dramatiq):

- Einfachste Topologie (3 Queues pro logische Queue statt 3+N)
- Das Head-of-Queue-Blocking ist in der Praxis tolerierbar
- Bei Bedarf spaeter auf Tiered Delay-Queues upgraden

### 9.4 Runtime-Override der Retry-Behavior?

```python
class MyTask(Task):
    task_info = TaskInfo(max_retries=3, retry_backoff="exponential")

    async def run(self, *args, **kwargs):
        try:
            await external_api()
        except RateLimitError as e:
            # Runtime-Override: Retry mit spezifischem Delay
            raise RetryAfter(delay=e.retry_after) from e
        except PermanentError:
            # Runtime-Override: Kein Retry trotz max_retries > 0
            raise NoRetry() from e
```

**Empfehlung:** Ja, durch spezielle Exceptions (`RetryAfter(delay=...)`, `NoRetry()`), analog zu Dramatiq's `Retry(delay=...)` und arq's `Retry(defer=...)`.

---

# Teil 2: Dead Letter Queue (DLQ)

## 10. Dramatiq -- Automatische DLQ (.XQ)

### 10.1 Verhalten

- **Automatisch:** Jede Queue `foo` hat automatisch eine `foo.XQ`
- **Trigger:** Message landet in XQ wenn:
  - Alle Retries erschoepft (`retries >= max_retries`)
  - Message-Alter ueberschritten (`max_age` abgelaufen)
  - Worker die Message rejected (ohne Retry)
- **TTL:** Messages in XQ haben eine TTL von 7 Tagen (`dead_message_ttl`)
- **Metadaten:** Original-Message-Body + Options bleiben erhalten (Traceback, Retry-Count, Timestamps)

### 10.2 DLQ-Message-Format

```json
{
    "queue_name": "default",
    "actor_name": "my_task",
    "args": [1, 2, 3],
    "kwargs": {"key": "value"},
    "options": {
        "retries": 3,
        "traceback": "Traceback (most recent call last):\n  ...",
        "requeue_timestamp": 1713196800000
    }
}
```

### 10.3 CLI-Tooling

Dramatiq selbst hat **kein eingebautes CLI** fuer DLQ-Inspektion. Es gibt Community-Tools und die Empfehlung, das RabbitMQ Management Plugin zu nutzen. Die `dramatiq_dashboard` Library bietet eine Web-UI.

---

## 11. RabbitMQ Dead Letter Exchange (DLX)

### 11.1 Konfiguration

**Queue-Argumente:**

| Argument | Typ | Beschreibung |
|---|---|---|
| `x-dead-letter-exchange` | `string` | Ziel-Exchange fuer dead-lettered Messages |
| `x-dead-letter-routing-key` | `string` | Routing-Key fuer dead-lettered Messages |

**Wann werden Messages dead-lettered?**

| Grund | `reason` in x-death | Beschreibung |
|---|---|---|
| Rejection | `rejected` | Consumer rejected mit `requeue=false` |
| TTL abgelaufen | `expired` | Per-Message oder Per-Queue TTL |
| Queue-Overflow | `maxlen` | `x-max-length` oder `x-max-length-bytes` ueberschritten |
| Delivery-Limit | `delivery_limit` | Quorum Queue `x-delivery-limit` erreicht |

**Ausnahme:** Queue-Expiration (gesamte Queue laeuft ab) dead-lettert **nicht**.

### 11.2 x-death Header

Dead-lettered Messages erhalten einen `x-death` Array-Header:

```python
# Struktur eines x-death Eintrags
{
    "queue": "original-queue",        # Herkunfts-Queue
    "reason": "expired",              # rejected | expired | maxlen | delivery_limit
    "count": 3,                       # Anzahl Dead-Letterings aus dieser Queue
    "time": datetime(2026, 4, 15),    # Zeitpunkt
    "exchange": "my-exchange",        # Original-Exchange
    "routing-keys": ["my-key"],       # Original-Routing-Keys
    "original-expiration": "60000",   # Originale TTL (wird entfernt, um Re-Expiration zu verhindern)
}
```

Zusaetzliche Convenience-Headers:

| Header | Beschreibung |
|---|---|
| `x-first-death-queue` | Queue des ersten Dead-Letterings |
| `x-first-death-reason` | Grund des ersten Dead-Letterings |
| `x-last-death-queue` | Queue des letzten Dead-Letterings |
| `x-last-death-reason` | Grund des letzten Dead-Letterings |

### 11.3 At-Least-Once Dead Lettering

**Standard-Queues:** Dead Lettering erfolgt **ohne Publisher Confirms**. Bei Cluster-Failover koennen Messages verloren gehen.

**Quorum Queues (ab RabbitMQ 3.13+):** Dead Lettering mit internen Publisher Confirms. **Empfohlen fuer SwarmQ** -- garantiert, dass Messages die XQ erreichen.

```python
# Quorum Queue mit At-Least-Once Dead Lettering
await channel.declare_queue(
    "tasks",
    durable=True,
    arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "tasks.XQ",
        "x-dead-letter-strategy": "at-least-once",  # Nur Quorum Queues
        "x-overflow": "reject-publish",               # Backpressure statt Drop
    },
)
```

### 11.4 Overflow-Verhalten

| Policy | Verhalten |
|---|---|
| `drop-head` (Default) | Aelteste Message wird entfernt (dead-lettered wenn DLX konfiguriert) |
| `reject-publish` | Neue Publishes werden rejected (Backpressure zum Producer) |

**Empfehlung fuer SwarmQ:** `reject-publish` fuer Haupt-Queues (Backpressure), `drop-head` fuer XQ (aelteste Fehler zuerst entfernen).

### 11.5 Zykluserkennung

RabbitMQ erkennt DLX-Zyklen: Wenn eine Message zurueck in ihre Ursprungs-Queue dead-lettered wird und **kein Consumer sie dazwischen rejected hat**, wird sie verworfen. Das ist relevant fuer das Retry-Pattern, wo die DQ zurueck zur Haupt-Queue routet -- hier funktioniert es, weil die Message dazwischen von einem Consumer verarbeitet wird.

---

## 12. Celery -- Keine native DLQ

Celery hat **keine eingebaute DLQ**. Workarounds:

1. **Broker-seitig:** RabbitMQ DLX konfigurieren (manuell, nicht durch Celery verwaltet)
2. **task_reject_on_worker_lost=True** + DLX → Messages bei Worker-Crash an DLX
3. **on_failure Signal** → manuell in separate Queue schreiben
4. **acks_on_failure_or_timeout=False** + DLX → ungeackte Messages an DLX (Vorsicht: Endlosschleifen)

**Bewertung:** Ein signifikanter Nachteil von Celery. SwarmQ sollte DLQ als First-Class-Feature haben.

---

## 13. RQ -- FailedJobRegistry

```python
from rq import Queue
from rq.job import Job

queue = Queue(connection=redis)
registry = queue.failed_job_registry

# Alle fehlgeschlagenen Jobs auflisten
failed_job_ids = registry.get_job_ids()

# Job inspizieren
job = Job.fetch(job_id, connection=redis)
print(job.exc_info)  # Exception + Traceback

# Einzelnen Job erneut einreihen
registry.requeue(job_id)
```

| Aspekt | Wert |
|---|---|
| Default-TTL | 1 Jahr |
| Speicherort | Redis (Sorted Set) |
| Replay | `registry.requeue(job_id)` |
| Metadaten | Exception-Typ, Traceback, Original-Args |

---

## 14. Best Practices: DLQ-Design

### 14.1 Metadaten in DLQ-Messages

Jede DLQ-Message sollte folgende Informationen enthalten:

| Feld | Beschreibung | Quelle |
|---|---|---|
| Original-Message-Body | Unveraenderte Task-Argumente | Message Body |
| `task_id` | Task-Identifier | Message Headers |
| `task_name` | Task-Name | Message Headers |
| `original_queue` | Herkunfts-Queue | Message Headers |
| `retry_count` | Anzahl durchgefuehrter Retries | Message Headers |
| `first_failure_at` | Zeitpunkt des ersten Fehlers | Message Headers |
| `last_failure_at` | Zeitpunkt des letzten Fehlers | Message Headers |
| `last_exception` | Exception-Typ und -Message | Message Headers |
| `last_traceback` | Traceback (gekuerzt, max. 30 Zeilen) | Message Headers |
| `retry_history` | Liste aller Retry-Timestamps und Fehler | Optional: Valkey |

### 14.2 DLQ-Message-TTL

| Library | Default-TTL | Einstellbar |
|---|---|---|
| Dramatiq | 7 Tage | Ja (`dead_message_ttl`) |
| RQ | 1 Jahr | Ja (`failure_ttl`) |

**Empfehlung fuer SwarmQ:** 7 Tage als Default (wie Dramatiq). Konfigurierbar global und pro Queue.

### 14.3 DLQ-Monitoring und Alerting

SwarmQ sollte folgende Metriken exponieren:

```python
# Prometheus Metriken
swarmq_dlq_messages_total     # Counter: Gesamt-Anzahl DLQ-Zustellungen
swarmq_dlq_messages_current   # Gauge: Aktuelle Anzahl Messages in DLQ (pro Queue)
swarmq_dlq_replayed_total     # Counter: Anzahl Replays aus DLQ
```

**Alert-Schwellenwerte (Vorschlag):**
- `swarmq_dlq_messages_current > 0` → INFO (jede DLQ-Message ist untersuchenswert)
- `swarmq_dlq_messages_current > 100` → WARNING
- `rate(swarmq_dlq_messages_total[5m]) > 10` → CRITICAL (>10 DLQ/Minute)

### 14.4 Replay-Patterns

| Pattern | Beschreibung | Einsatz |
|---|---|---|
| **Single Replay** | Eine Message erneut in die Haupt-Queue | Bug gefixt, einzelner Task |
| **Bulk Replay** | Alle Messages einer Queue erneut einreihen | Bug gefixt, viele betroffene Tasks |
| **Filtered Replay** | Nur Messages mit bestimmtem Task-Name/Exception | Selektive Wiederverarbeitung |
| **Dry-Run Replay** | Messages anzeigen die replayed wuerden | Vor dem eigentlichen Replay |

**Soll Replay durch die normale Retry-Logik laufen?**

**Ja (empfohlen):** Replayed Messages durchlaufen den normalen Retry-Cycle. Wenn der Bug nicht gefixt ist, landen sie wieder in der DLQ. Der Retry-Counter wird zurueckgesetzt.

### 14.5 DLQ pro Queue vs. Globale DLQ

| Ansatz | Vorteile | Nachteile |
|---|---|---|
| **Pro Queue** (Dramatiq) | Isoliert, gezieltes Replay, Queue-spezifische TTL | Mehr Queues, mehr Overhead |
| **Globale DLQ** | Einfacher, zentrales Monitoring | Kein isoliertes Replay, keine Queue-spezifische TTL |

**Empfehlung:** Pro Queue (wie Dramatiq). Die Anzahl zusaetzlicher Queues ist ueberschaubar (1 XQ pro logische Queue).

### 14.6 Maximale DLQ-Groesse und Overflow

```python
# DLQ mit Groesenlimit und Overflow-Handling
await channel.declare_queue(
    "tasks.XQ",
    durable=True,
    arguments={
        "x-message-ttl": 604_800_000,      # 7 Tage
        "x-max-length": 10_000,            # Maximal 10.000 Messages
        "x-overflow": "drop-head",          # Aelteste Messages zuerst entfernen
    },
)
```

---

## 15. RabbitMQ-Implementierung fuer SwarmQ: DLQ

### 15.1 Queue-Topologie

```
Fuer jede logische Queue "tasks":

[tasks]          Haupt-Queue (Quorum)
    │                x-dead-letter-exchange: ""
    │                x-dead-letter-routing-key: tasks.XQ
    │                x-queue-type: quorum
    │                x-delivery-limit: 100  (Sicherheitsnetz)
    │
[tasks.DQ]       Delay-Queue (Classic, durable)
    │                x-dead-letter-exchange: ""
    │                x-dead-letter-routing-key: tasks  (zurueck zur Haupt-Queue)
    │
[tasks.XQ]       Dead-Letter-Queue (Quorum)
                     x-message-ttl: 604800000  (7 Tage)
                     x-max-length: 10000
                     x-overflow: drop-head
```

### 15.2 Namenskonvention

```python
def delay_queue_name(queue: str) -> str:
    return f"{queue}.DQ"

def dead_letter_queue_name(queue: str) -> str:
    return f"{queue}.XQ"
```

### 15.3 Vollstaendige Queue-Deklaration mit aio-pika

```python
import aio_pika
from aio_pika import Channel, Message, DeliveryMode


DEAD_MESSAGE_TTL = 604_800_000  # 7 Tage in ms
DLQ_MAX_LENGTH = 10_000
DELIVERY_LIMIT_SAFETY = 100     # Sicherheitsnetz fuer Quorum Queues


async def declare_queue_topology(
    channel: Channel,
    queue_name: str,
    *,
    dead_message_ttl: int = DEAD_MESSAGE_TTL,
    dlq_max_length: int = DLQ_MAX_LENGTH,
    use_quorum: bool = True,
) -> tuple[aio_pika.Queue, aio_pika.Queue, aio_pika.Queue]:
    """Deklariert die vollstaendige Drei-Queue-Topologie.

    Returns:
        (main_queue, delay_queue, dead_letter_queue)
    """
    dq_name = f"{queue_name}.DQ"
    xq_name = f"{queue_name}.XQ"

    queue_type_args = {"x-queue-type": "quorum"} if use_quorum else {}

    # 1. Dead-Letter-Queue (XQ)
    xq = await channel.declare_queue(
        xq_name,
        durable=True,
        arguments={
            **queue_type_args,
            "x-message-ttl": dead_message_ttl,
            "x-max-length": dlq_max_length,
            "x-overflow": "drop-head",
        },
    )

    # 2. Delay-Queue (DQ) -- Classic Queue (Per-Message TTL funktioniert besser)
    dq = await channel.declare_queue(
        dq_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": queue_name,  # → zurueck zu main
        },
    )

    # 3. Haupt-Queue
    main_q = await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            **queue_type_args,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": xq_name,      # → rejected nach XQ
            **({"x-delivery-limit": DELIVERY_LIMIT_SAFETY} if use_quorum else {}),
        },
    )

    return main_q, dq, xq
```

### 15.4 DLQ-Inspektion via aio-pika

```python
async def inspect_dlq(
    channel: Channel,
    queue_name: str,
    limit: int = 10,
) -> list[dict]:
    """Liest Messages aus der DLQ ohne sie zu entfernen (peek)."""
    xq_name = f"{queue_name}.XQ"
    messages = []

    # Passive Deklaration (Queue muss existieren)
    xq = await channel.declare_queue(xq_name, passive=True)

    for _ in range(limit):
        msg = await xq.get(no_ack=False, fail=False)
        if msg is None:
            break

        messages.append({
            "message_id": msg.message_id,
            "body": msg.body.decode(),
            "headers": dict(msg.headers or {}),
            "timestamp": msg.timestamp,
            "x_death": (msg.headers or {}).get("x-death", []),
        })

        # Message zurueck in die Queue (nack mit requeue)
        await msg.nack(requeue=True)

    return messages
```

**Achtung:** `get()` + `nack(requeue=True)` ist kein echtes Peek -- es veraendert die Message-Reihenfolge. Fuer echte Inspektion ohne Seiteneffekte ist das RabbitMQ Management API besser geeignet.

### 15.5 DLQ-Replay via aio-pika

```python
async def replay_from_dlq(
    channel: Channel,
    queue_name: str,
    message_id: str | None = None,
    replay_all: bool = False,
) -> int:
    """Replayed Messages aus der DLQ zurueck in die Haupt-Queue.

    Args:
        queue_name: Name der logischen Queue
        message_id: Bestimmte Message-ID (oder None fuer alle)
        replay_all: Alle Messages replayed

    Returns:
        Anzahl replayed Messages
    """
    xq_name = f"{queue_name}.XQ"
    xq = await channel.declare_queue(xq_name, passive=True)
    default_exchange = channel.default_exchange
    replayed = 0

    while True:
        msg = await xq.get(no_ack=False, fail=False)
        if msg is None:
            break

        if not replay_all and message_id and msg.message_id != message_id:
            await msg.nack(requeue=True)
            continue

        # Replay: Neue Message in Haupt-Queue mit zurueckgesetztem Retry-Counter
        replay_headers = dict(msg.headers or {})
        replay_headers["x-retry-count"] = 0
        replay_headers["x-replayed-from-dlq"] = True
        replay_headers["x-replay-timestamp"] = int(
            __import__("time").time() * 1000
        )

        replay_msg = Message(
            body=msg.body,
            headers=replay_headers,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=msg.message_id,
            correlation_id=msg.correlation_id,
        )

        await default_exchange.publish(replay_msg, routing_key=queue_name)
        await msg.ack()  # Aus DLQ entfernen
        replayed += 1

        if not replay_all:
            break

    return replayed
```

---

## 16. Entscheidungen fuer SwarmQ: DLQ

### 16.1 DLQ-Topologie

**Entscheidung:** Pro Queue eine DLQ (wie Dramatiq). Namensschema: `{queue}.XQ`.

**Begruendung:** Isoliertes Replay, Queue-spezifische TTL, klare Zuordnung.

### 16.2 DLQ-Message-Inhalt

Gespeichert in Message-Headers:

```python
REQUIRED_DLQ_HEADERS = {
    "x-swarmq-task-id": str,           # Task-UUID
    "x-swarmq-task-name": str,         # Task-Klassenname
    "x-swarmq-original-queue": str,    # Herkunfts-Queue
    "x-swarmq-retry-count": int,       # Durchgefuehrte Retries
    "x-swarmq-first-failure": int,     # Timestamp erster Fehler (ms)
    "x-swarmq-last-failure": int,      # Timestamp letzter Fehler (ms)
    "x-swarmq-last-exception": str,    # Exception-Typ: "ConnectionError"
    "x-swarmq-last-error-msg": str,    # Exception-Message
    "x-swarmq-last-traceback": str,    # Traceback (max. 30 Zeilen)
}
```

Zusaetzlich in Valkey (fuer CLI/Monitoring):

```python
# Valkey Key: swarmq:dlq:{task_id}
# TTL: gleich wie DLQ-Message-TTL
{
    "task_id": "...",
    "task_name": "...",
    "queue": "...",
    "retry_count": 3,
    "exception": "ConnectionError",
    "traceback": "...",
    "failed_at": 1713196800,
    "args": [...],
    "kwargs": {...},
}
```

### 16.3 DLQ-TTL

- **Default:** 7 Tage (konfigurierbar)
- **Global:** `SwarmQ(dead_message_ttl=604800)` (in Sekunden)
- **Pro Queue:** Spaetere Erweiterung

### 16.4 DLQ-CLI

```bash
swarmq dlq list                          # Alle DLQ-Messages (aus Valkey, schnell)
swarmq dlq list --queue default          # Nur bestimmte Queue
swarmq dlq inspect <task_id>             # Details inkl. Traceback
swarmq dlq retry <task_id>               # Einzelne Message replayed
swarmq dlq retry --all                   # Alle Messages replayed
swarmq dlq retry --all --queue default   # Alle Messages einer Queue
swarmq dlq purge                         # Alle DLQ-Messages loeschen
swarmq dlq purge --queue default         # Nur bestimmte Queue
swarmq dlq count                         # Anzahl Messages pro Queue
```

---

## 17. Zusammenfassung und Empfehlungen

### Retry-Strategie

| Aspekt | Empfehlung | Vorbild |
|---|---|---|
| **Backoff-Algorithmus** | Full Jitter (AWS) | Dramatiq + AWS Blog |
| **Default max_retries** | 3 (konservativ, nicht 20 wie Dramatiq) | Celery |
| **Default min_backoff** | 5 Sekunden | Eigene Wahl |
| **Default max_backoff** | 3600 Sekunden (1 Stunde) | Eigene Wahl |
| **Exception-Filterung** | `retry_exceptions` (Positiv) + `no_retry_exceptions` (Negativ) | Procrastinate + Dramatiq |
| **Runtime-Override** | `RetryAfter(delay=...)` und `NoRetry()` Exceptions | Dramatiq + arq |
| **Retry-State** | Message-Headers (primaer) + Valkey (Monitoring) | Dramatiq |
| **Delay-Mechanismus** | Per-Message TTL in DQ | Dramatiq |
| **Attempt-Sichtbarkeit** | `self.context.attempt` im Task | arq, Dramatiq, Celery |
| **Quorum delivery-limit** | Als Sicherheitsnetz (100), nicht als Retry-Logik | RabbitMQ 4.0 |
| **Jitter-Implementierung** | `secrets.randbelow()` statt `random.uniform()` | Verbesserung gegenueber Dramatiq |

### DLQ-Strategie

| Aspekt | Empfehlung | Vorbild |
|---|---|---|
| **Topologie** | Pro Queue: `{queue}.XQ` | Dramatiq |
| **Queue-Typ** | Quorum Queue (At-Least-Once Dead Lettering) | RabbitMQ 3.13+ |
| **TTL** | 7 Tage (konfigurierbar) | Dramatiq |
| **Max-Groesse** | 10.000 Messages, drop-head Overflow | Eigene Wahl |
| **Metadaten** | In Message-Headers + Valkey-Mirror | Dramatiq + RQ |
| **Replay** | Durch normale Retry-Logik (Counter zurueckgesetzt) | Best Practice |
| **Monitoring** | Prometheus-Metriken + CLI | Eigene Wahl |
| **CLI** | `swarmq dlq list/inspect/retry/purge` | Dramatiq (fehlend!) + RQ |

### Gesamte Queue-Topologie pro logische Queue

```
[tasks]     ← Quorum Queue, DLX → tasks.XQ, delivery-limit: 100
[tasks.DQ]  ← Classic Queue, DLX → tasks (Per-Message TTL fuer variable Delays)
[tasks.XQ]  ← Quorum Queue, TTL: 7d, max-length: 10.000, drop-head
```

**3 RabbitMQ-Queues pro logische SwarmQ-Queue.** Bei 5 logischen Queues = 15 RabbitMQ-Queues. Akzeptabler Overhead.

### Quellen

- Dramatiq Dokumentation: https://dramatiq.io/guide.html
- Dramatiq Retries-Middleware (Quellcode): https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/middleware/retries.py
- Dramatiq RabbitMQ-Broker (Quellcode): https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/brokers/rabbitmq.py
- Celery Tasks-Dokumentation: https://docs.celeryq.dev/en/stable/userguide/tasks.html
- Celery Autoretry (Quellcode): https://github.com/celery/celery/blob/main/celery/app/autoretry.py
- TaskIQ Middlewares: https://taskiq-python.github.io/available-components/middlewares.html
- Procrastinate Retry: https://procrastinate.readthedocs.io/en/stable/howto/advanced/retry.html
- arq Dokumentation: https://arq-docs.helpmanual.io/
- RQ Exceptions & Retries: https://python-rq.org/docs/exceptions/
- AWS Architecture Blog -- Exponential Backoff and Jitter: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
- AWS Builders Library -- Timeouts, Retries and Backoff with Jitter: https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/
- RabbitMQ Dead Letter Exchanges: https://www.rabbitmq.com/docs/dlx
- RabbitMQ Quorum Queues in 4.0: https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0
- RabbitMQ At-Least-Once Dead Lettering: https://www.rabbitmq.com/blog/2022/03/29/at-least-once-dead-lettering
- Tanzu RabbitMQ Delayed Queues: https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-rpm/4-2/tanzu-rabbitmq-rpm-offering/site-delayed-queues.html
- RabbitMQ Exponential Backoff (Brian Storti): https://www.brianstorti.com/rabbitmq-exponential-backoff/
- RabbitMQ Delayed Retry Approaches (Jack Vanlightly): https://jack-vanlightly.com/blog/2017/3/24/rabbitmq-delayed-retry-approaches-that-work
