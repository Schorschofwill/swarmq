# Recherche: Rate Limiting, Task Locking und Deduplizierung

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument fuer die Implementierung von Rate Limiting, Task Locking und Unique-Task-Mechanismen in SwarmQ.

---

## Inhaltsverzeichnis

1. [Rate Limiting](#1-rate-limiting)
   1. [Celery: Token Bucket (Per-Worker)](#11-celery-token-bucket-per-worker)
   2. [Dramatiq: Distributed Rate Limiters](#12-dramatiq-distributed-rate-limiters)
   3. [Faktory Enterprise: Queue Throttling](#13-faktory-enterprise-queue-throttling)
   4. [Algorithmen im Vergleich](#14-algorithmen-im-vergleich)
   5. [Lua-Skripte fuer Valkey/Redis](#15-lua-skripte-fuer-valkeyredis)
   6. [Designentscheidungen fuer SwarmQ](#16-designentscheidungen-fuer-swarmq)
2. [Task Locking (Concurrency Control)](#2-task-locking-concurrency-control)
   1. [Huey: lock_task Decorator](#21-huey-lock_task-decorator)
   2. [Procrastinate: PostgreSQL-basierte Locks](#22-procrastinate-postgresql-basierte-locks)
   3. [Dramatiq: ConcurrentRateLimiter als Lock](#23-dramatiq-concurrentratelimiter-als-lock)
   4. [Faktory Enterprise: Concurrency Throttle](#24-faktory-enterprise-concurrency-throttle)
   5. [Patterns fuer Distributed Locking](#25-patterns-fuer-distributed-locking)
   6. [Lua-Skripte fuer Task Locking](#26-lua-skripte-fuer-task-locking)
   7. [Designentscheidungen fuer SwarmQ](#27-designentscheidungen-fuer-swarmq)
3. [Unique Tasks / Deduplizierung](#3-unique-tasks--deduplizierung)
   1. [Faktory Enterprise: unique_for + unique_until](#31-faktory-enterprise-unique_for--unique_until)
   2. [Procrastinate: queueing_lock](#32-procrastinate-queueing_lock)
   3. [arq: _job_id als Deduplizierungsschluessel](#33-arq-_job_id-als-deduplizierungsschluessel)
   4. [SAQ: Key-basierte Unique Jobs](#34-saq-key-basierte-unique-jobs)
   5. [celery-singleton / celery-heimdall](#35-celery-singleton--celery-heimdall)
   6. [Lua-Skripte fuer Deduplizierung](#36-lua-skripte-fuer-deduplizierung)
   7. [Designentscheidungen fuer SwarmQ](#37-designentscheidungen-fuer-swarmq)
4. [Zusammenfassung und Empfehlungen](#4-zusammenfassung-und-empfehlungen)

---

# 1. Rate Limiting

## 1.1 Celery: Token Bucket (Per-Worker)

### Rate-String-Syntax

Celery definiert Rate Limits als String auf der Task-Klasse:

```python
@app.task(rate_limit='10/s')   # 10 pro Sekunde
@app.task(rate_limit='100/m')  # 100 pro Minute
@app.task(rate_limit='1000/h') # 1000 pro Stunde
```

Die Parsing-Logik (`celery/utils/time.py`):

```python
RATE_MODIFIER_MAP = {
    's': lambda n: n,
    'm': lambda n: n / 60.0,
    'h': lambda n: n / 60.0 / 60.0,
}

def rate(r: str) -> float:
    """Konvertiert Rate-String zu Operations pro Sekunde."""
    if r:
        if isinstance(r, str):
            ops, _, modifier = r.partition('/')
            return RATE_MODIFIER_MAP[modifier or 's'](float(ops)) or 0
        return r or 0
    return 0
```

### Algorithmus: Token Bucket (kombu)

Die eigentliche Implementierung liegt in `kombu/utils/limits.py`:

```python
class TokenBucket:
    """Nicht thread-sicher. Zugriff muss in Critical Sections erfolgen."""

    def __init__(self, fill_rate, capacity=1):
        self.capacity = float(capacity)
        self._tokens = capacity
        self.fill_rate = float(fill_rate)
        self.timestamp = monotonic()
        self.contents = deque()  # Warteschlange fuer gehaltene Tasks

    def can_consume(self, tokens=1):
        if tokens <= self._get_tokens():
            self._tokens -= tokens
            return True
        return False

    def expected_time(self, tokens=1):
        _tokens = self._get_tokens()
        tokens = max(tokens, _tokens)
        return (tokens - _tokens) / self.fill_rate

    def _get_tokens(self):
        if self._tokens < self.capacity:
            now = monotonic()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
        return self._tokens
```

### Per-Worker — nicht global!

**Kritisch:** Celery's Rate Limiting ist **ausschliesslich per-Worker**. Bei 4 Workern mit `rate_limit='10/s'` werden effektiv bis zu 40 Tasks/Sekunde verarbeitet. Es gibt **keinen** eingebauten globalen Rate Limiter.

### Verhalten bei Rate-Limit-Ueberschreitung

Aus `celery/worker/strategy.py`:

```python
if eta and bucket:
    consumer.qos.increment_eventually()
    return call_at(eta, limit_post_eta, (req, bucket, 1), priority=6)

if bucket:
    return limit_task(req, bucket, 1)
```

- Rate-limitierte Tasks werden **im Speicher gehalten** (in der `contents`-Deque des TokenBucket)
- Sie werden **nicht** zurueck an den Broker geschickt
- Ein Timer prueft regelmaessig, ob Tokens verfuegbar sind
- Bei Worker-Neustart gehen gehaltene Tasks verloren (werden durch Broker-Redelivery wiederhergestellt)

### Runtime-Aenderungen

```python
# Via celery control
app.control.rate_limit('myapp.tasks.send_email', '100/m')
```

Intern broadcast diese Methode eine Control-Message an alle (oder bestimmte) Worker:

```python
def rate_limit(self, task_name, rate_limit, destination=None, **kwargs):
    return self.broadcast(
        'rate_limit',
        destination=destination,
        arguments={'task_name': task_name, 'rate_limit': rate_limit},
        **kwargs
    )
```

### Bewertung

| Aspekt | Bewertung |
|---|---|
| Einfachheit | Sehr gut — String-Syntax ist intuitiv |
| Globalitaet | Mangelhaft — nur per-Worker |
| Praezision | Gut fuer moderate Raten, ungenau bei Bursts |
| Ausfallsicherheit | Maessig — Tasks im Speicher bei Crash verloren |

---

## 1.2 Dramatiq: Distributed Rate Limiters

### Architektur

Dramatiq bietet drei Rate-Limiter-Typen, alle mit Redis-Backend fuer verteilte Nutzung:

1. **BucketRateLimiter** — Fixed Window
2. **WindowRateLimiter** — Sliding Window
3. **ConcurrentRateLimiter** — Concurrency Control (kein Rate Limit im klassischen Sinn)

### Verwendungspattern: Context Manager im Task Body

```python
from dramatiq.rate_limits import ConcurrentRateLimiter
from dramatiq.rate_limits.backends import RedisBackend

backend = RedisBackend(url="redis://localhost:6379")
RATE_LIMITER = ConcurrentRateLimiter(backend, "api-limiter", limit=10)

@dramatiq.actor
def call_external_api(url):
    with RATE_LIMITER.acquire(raise_on_failure=False) as acquired:
        if not acquired:
            raise dramatiq.RateLimitExceeded()  # Task wird spaeter retried
        requests.get(url)
```

### RateLimiter-Basisklasse

```python
class RateLimiter:
    def __init__(self, backend, key):
        self.backend = backend
        self.key = key

    @contextmanager
    def acquire(self, raise_on_failure=True):
        acquired = self._acquire()
        try:
            if raise_on_failure and not acquired:
                raise RateLimitExceeded()
            yield acquired
        finally:
            if acquired:
                self._release()
```

### BucketRateLimiter (Fixed Window)

```python
class BucketRateLimiter(RateLimiter):
    def __init__(self, backend, key, limit=10, bucket=10000):
        # limit: max Operationen pro Bucket
        # bucket: Bucket-Intervall in Millisekunden
        ...

    def _acquire(self):
        timestamp = int(time.time() * 1000)
        bucket_timestamp = timestamp - (timestamp % self.bucket)
        bucket_key = f"{self.key}@{bucket_timestamp}"

        if self.backend.add(bucket_key, 1, ttl=self.bucket):
            return True
        return self.backend.incr(bucket_key, 1, self.limit, ttl=self.bucket)

    def _release(self):
        pass  # Kein Release noetig
```

**Warnung:** Anfaellig fuer Burst-Attacken an Bucket-Grenzen. Bei `limit=100, bucket=60000` (100/min) koennen an der Grenze zweier Minuten bis zu 200 Requests in einer Sekunde passieren.

### WindowRateLimiter (Sliding Window)

```python
class WindowRateLimiter(RateLimiter):
    def __init__(self, backend, key, limit=10, window=1):
        # limit: max Operationen im Fenster
        # window: Fenstergroesse in Sekunden
        ...

    def _acquire(self):
        timestamp = int(time.time())
        keys = [f"{self.key}@{timestamp - i}" for i in range(self.window)]
        current_key = keys[0]
        return self.backend.incr_and_sum(
            current_key, keys, 1, self.limit, ttl=self.window * 1000
        )

    def _release(self):
        pass
```

### Redis-Backend (keine Lua-Skripte!)

Ueberraschendes Detail: Dramatiq's Redis-Backend verwendet **keine Lua-Skripte**, sondern Python-seitige Transaktionen mit `WATCH`:

```python
class RedisBackend:
    def add(self, key, value, ttl):
        return bool(self.client.set(key, value, nx=True, px=ttl))

    def incr(self, key, amount, maximum, ttl):
        with self.client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    value = int(pipe.get(key) or b"0")
                    if value + amount > maximum:
                        return False
                    pipe.multi()
                    pipe.incr(key, amount)
                    pipe.pexpire(key, ttl)
                    pipe.execute()
                    return True
                except WatchError:
                    continue
```

**Nachteil:** WATCH-basierte Transaktionen erzeugen unter hoher Last viele Retries (Optimistic Locking). Lua-Skripte waeren effizienter.

### Bewertung

| Aspekt | Bewertung |
|---|---|
| Distributed | Ja — ueber Redis |
| Flexibilitaet | Gut — Context Manager erlaubt beliebige Logik |
| Atomaritaet | Maessig — WATCH statt Lua |
| API-Design | Gut — expliziter Context Manager |

---

## 1.3 Faktory Enterprise: Queue Throttling

Faktory (Enterprise) implementiert Throttling auf Queue-Ebene ueber eine Konfigurationsdatei:

```toml
# conf.d/throttles.toml
instagram = { concurrency = 4, timeout = 60 }
bulk = { worker = 5, timeout = 60 }
```

### Mechanismus

- **Concurrency Throttle:** Begrenzt parallele Jobs ueber den gesamten Worker-Cluster
- **Per-Worker Throttle:** Begrenzt parallele Jobs pro Worker-Prozess
- Locks werden bei `FETCH` akquiriert und bei `ACK`/`FAIL` freigegeben
- **Timeout** verhindert verwaiste Locks bei Worker-Crashes

### Bewertung

Faktory's Ansatz ist primaer Concurrency Control (nicht Rate Limiting im klassischen Sinn). Der Timeout-Mechanismus fuer Crash-Sicherheit ist ein wichtiges Pattern fuer SwarmQ.

---

## 1.4 Algorithmen im Vergleich

### Token Bucket

```
Prinzip: Tokens werden mit konstanter Rate aufgefuellt.
         Jeder Request verbraucht ein Token.
         Burst erlaubt bis zur Bucket-Kapazitaet.

Vorteile:
  + Erlaubt kontrollierte Bursts
  + Glaettet Traffic ueber Zeit
  + Einfach zu implementieren (lokal)

Nachteile:
  - Distributed: erfordert atomaren Zugriff auf Token-Count
  - Kapazitaet muss sinnvoll gewaehlt werden
```

### Fixed Window

```
Prinzip: Zaehler pro Zeitfenster (z.B. pro Minute).
         Zaehler wird am Fensterende zurueckgesetzt.

Vorteile:
  + Einfachste Implementierung (INCR + EXPIRE)
  + Geringer Speicherverbrauch

Nachteile:
  - Burst-Problem an Fenstergrenzen (2x Limit moeglich)
  - Ungenaue Ratensteuerung
```

### Sliding Window Log

```
Prinzip: Jeder Request wird mit Timestamp in Sorted Set gespeichert.
         Fenster gleitet mit der aktuellen Zeit.

Vorteile:
  + Praeziseste Ratenbegrenzung
  + Kein Burst-Problem

Nachteile:
  - Hoher Speicherverbrauch (ein Eintrag pro Request)
  - Langsamer bei hohen Raten (ZRANGEBYSCORE)
```

### Sliding Window Counter (Hybrid)

```
Prinzip: Kombination aus Fixed Window + gewichteter Anteil des vorherigen Fensters.
         Approximiert Sliding Window mit O(1) Speicher.

Formel: count = current_window_count + previous_window_count * overlap_ratio

Vorteile:
  + Guter Kompromiss: fast so praezise wie Sliding Window Log
  + Sehr geringer Speicherverbrauch (nur 2 Zaehler)
  + Schnell (nur GET + INCR)

Nachteile:
  - Approximation, nicht exakt
```

### Leaky Bucket

```
Prinzip: Requests fliessen in einen "Eimer" und werden mit
         konstanter Rate abgearbeitet. Ueberlauf wird abgelehnt.

Vorteile:
  + Garantiert konstante Abarbeitungsrate
  + Kein Burst

Nachteile:
  - Keine Bursts erlaubt (kann ein Nachteil sein)
  - Komplexer zu implementieren (braucht Queue)
```

### Empfehlung fuer Task Queues

**Sliding Window Counter** ist der beste Kompromiss:
- Praezise genug fuer Task-Queue-Anwendungsfaelle
- Sehr geringer Speicher- und CPU-Verbrauch
- Einfach atomar in Lua implementierbar
- Kein Burst-Problem wie bei Fixed Window

Fuer einfache Faelle ist **Token Bucket** ebenfalls gut geeignet, besonders weil kontrollierte Bursts in Task Queues oft wuenschenswert sind (z.B. nach einer Phase ohne Tasks).

---

## 1.5 Lua-Skripte fuer Valkey/Redis

### Token Bucket (Distributed)

```lua
-- KEYS[1] = Rate-Limit-Key (z.B. "sq:rl:send_email")
-- ARGV[1] = max_tokens (Kapazitaet)
-- ARGV[2] = refill_rate (Tokens pro Sekunde)
-- ARGV[3] = tokens_requested (normalerweise 1)
-- ARGV[4] = jetzt (Timestamp in Mikrosekunden)
--
-- Rueckgabe: {erlaubt (0/1), verbleibende_tokens, retry_after_ms}

local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
    -- Erster Zugriff: Bucket voll initialisieren
    tokens = max_tokens
    last_refill = now
end

-- Tokens auffuellen basierend auf vergangener Zeit
local elapsed = (now - last_refill) / 1000000.0  -- Mikrosekunden zu Sekunden
local new_tokens = elapsed * refill_rate
tokens = math.min(max_tokens, tokens + new_tokens)

local allowed = 0
local retry_after = 0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    -- Berechne Wartezeit bis genug Tokens verfuegbar
    retry_after = math.ceil(((requested - tokens) / refill_rate) * 1000)
end

-- Zustand speichern
redis.call('HMSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(now))
-- TTL: doppelte Zeit bis Bucket voll, mindestens 60s
local ttl = math.max(60, math.ceil((max_tokens / refill_rate) * 2))
redis.call('EXPIRE', key, ttl)

return {allowed, tostring(tokens), retry_after}
```

### Fixed Window

```lua
-- KEYS[1] = Rate-Limit-Key (z.B. "sq:rl:send_email:1713200400")
-- ARGV[1] = max_requests
-- ARGV[2] = window_size_seconds
--
-- Rueckgabe: {erlaubt (0/1), aktueller_count}

local key = KEYS[1]
local max_requests = tonumber(ARGV[1])
local window_size = tonumber(ARGV[2])

local current = tonumber(redis.call('GET', key) or '0')

if current >= max_requests then
    return {0, current}
end

current = redis.call('INCR', key)

if current == 1 then
    redis.call('EXPIRE', key, window_size)
end

-- Doppelte Pruefung nach INCR (Race Condition bei erstem INCR)
if current > max_requests then
    return {0, current}
end

return {1, current}
```

### Sliding Window Log (Sorted Set)

```lua
-- KEYS[1] = Rate-Limit-Key (z.B. "sq:rl:send_email:log")
-- ARGV[1] = max_requests
-- ARGV[2] = window_size_ms
-- ARGV[3] = jetzt_ms (Timestamp in Millisekunden)
-- ARGV[4] = eindeutige_id (z.B. Task-ID)
--
-- Rueckgabe: {erlaubt (0/1), aktueller_count}

local key = KEYS[1]
local max_requests = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local unique_id = ARGV[4]

-- Alte Eintraege entfernen
local window_start = now - window_ms
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Aktuelle Anzahl pruefen
local current = redis.call('ZCARD', key)

if current >= max_requests then
    return {0, current}
end

-- Neuen Eintrag hinzufuegen
redis.call('ZADD', key, now, unique_id)
redis.call('PEXPIRE', key, window_ms)

return {1, current + 1}
```

### Sliding Window Counter (Hybrid — Empfohlen)

```lua
-- KEYS[1] = Key fuer aktuelles Fenster (z.B. "sq:rl:send_email:1713200460")
-- KEYS[2] = Key fuer vorheriges Fenster (z.B. "sq:rl:send_email:1713200400")
-- ARGV[1] = max_requests
-- ARGV[2] = window_size_seconds
-- ARGV[3] = jetzt_timestamp (Sekunden als Float)
--
-- Rueckgabe: {erlaubt (0/1), geschaetzter_count, retry_after_ms}

local current_key = KEYS[1]
local previous_key = KEYS[2]
local max_requests = tonumber(ARGV[1])
local window_size = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local current_count = tonumber(redis.call('GET', current_key) or '0')
local previous_count = tonumber(redis.call('GET', previous_key) or '0')

-- Position im aktuellen Fenster berechnen (0.0 bis 1.0)
local window_start = now - (now % window_size)
local elapsed_ratio = (now - window_start) / window_size

-- Gewichteter Count: vorheriges Fenster * verbleibender Anteil + aktuelles Fenster
local estimated_count = previous_count * (1.0 - elapsed_ratio) + current_count

if estimated_count >= max_requests then
    -- Retry-After: geschaetzte Zeit bis ein Slot frei wird
    local retry_after = math.ceil((1.0 - elapsed_ratio) * window_size * 1000)
    return {0, tostring(estimated_count), retry_after}
end

-- Request zaehlen
current_count = redis.call('INCR', current_key)

if current_count == 1 then
    -- TTL = 2x Fenstergroesse (aktuelles Fenster + naechstes braucht den Wert)
    redis.call('EXPIRE', current_key, window_size * 2)
end

return {1, tostring(estimated_count + 1), 0}
```

### Performance-Vergleich

| Algorithmus | Valkey-Befehle pro Check | Speicher pro Key | Praezision |
|---|---|---|---|
| Token Bucket | 2 (HMGET + HMSET) | O(1) — 2 Felder | Gut, erlaubt Bursts |
| Fixed Window | 1-2 (GET oder INCR) | O(1) — 1 Zaehler | Maessig, Burst an Grenzen |
| Sliding Window Log | 3 (ZREMRANGE + ZCARD + ZADD) | O(n) — n = Requests | Exakt |
| Sliding Window Counter | 2-3 (2x GET + INCR) | O(1) — 2 Zaehler | Sehr gut (Approximation) |

**Fazit:** Sliding Window Counter ist am schnellsten fuer den typischen Fall und hat den besten Kompromiss aus Praezision und Performance.

### Clock Drift zwischen Workern

- **Problem:** Worker mit unterschiedlichen Systemuhren erzeugen falsche Fenster-Berechnungen
- **Loesung 1:** Valkey `TIME`-Befehl im Lua-Skript verwenden (Server-seitige Zeit):
  ```lua
  local time = redis.call('TIME')
  local now = tonumber(time[1]) + tonumber(time[2]) / 1000000.0
  ```
- **Loesung 2:** NTP auf allen Workern konfigurieren (ausreichend fuer Sekunden-Granularitaet)
- **Empfehlung:** Valkey TIME im Lua-Skript verwenden. Eliminiert das Problem vollstaendig und ist atomisch.

### Aufraumen veralteter Keys

- Token Bucket: EXPIRE im Lua-Skript gesetzt (selbstreinigend)
- Fixed Window: EXPIRE beim ersten INCR (selbstreinigend)
- Sliding Window Log: PEXPIRE + ZREMRANGEBYSCORE (selbstreinigend)
- Sliding Window Counter: EXPIRE mit 2x Window-Groesse (selbstreinigend)

Alle Lua-Skripte setzen TTLs, daher ist **kein separater Cleanup-Prozess noetig**.

---

## 1.6 Designentscheidungen fuer SwarmQ

### Empfohlene API

```python
# Decorator-basiert (wie Celery, aber global)
@swarmq.task(rate_limit='100/m')
async def send_email(to: str, body: str): ...

# Alternativ: programmatisch
@swarmq.task(rate_limit=RateLimit(limit=100, window=60))
async def send_email(to: str, body: str): ...
```

### Empfohlener Algorithmus

**Primaer: Sliding Window Counter** fuer globale Rate Limits
- Atomar in einem Lua-Skript
- Verwendet Valkey TIME fuer Clock-Drift-Freiheit
- Selbstreinigend durch TTLs

**Sekundaer: Token Bucket** als Alternative fuer Faelle mit gewuenschtem Burst-Verhalten

### Verhalten bei Rate-Limit-Ueberschreitung

| Option | Vorteil | Nachteil | Empfehlung |
|---|---|---|---|
| Requeue mit Delay | Einfach, kein Taskverlust | Erhoehte Broker-Last | Fuer SwarmQ empfohlen |
| Im Speicher halten | Schnelles Retry | Taskverlust bei Crash | Nicht empfohlen |
| Sleep im Worker | Einfach | Blockiert Worker | Nicht empfohlen (async!) |
| Reject + DLQ | Sauber | Task geht in DLQ statt delayed | Nicht empfohlen |

**Empfehlung:** Task mit Delay zurueck in die Queue legen. Der Delay wird aus `retry_after_ms` des Lua-Skripts berechnet. RabbitMQ unterstuetzt delayed Messages nativ ueber `x-delayed-message` Plugin oder per-Message-TTL + Dead-Letter-Exchange.

### Key-Format

```
sq:rl:{task_name}           — fuer Sliding Window Counter (aktuelles Fenster)
sq:rl:{task_name}:{window}  — mit Window-Timestamp-Suffix
```

### Globales vs. Per-Task Rate Limiting

- **Global pro Task-Typ** (empfohlen): Ein Rate Limit gilt fuer alle Instanzen eines Task-Typs ueber alle Worker
- **Per-Queue** (optional): Rate Limit pro Queue fuer Queue-weite Begrenzung
- **Per-Key** (optional, spaeter): Rate Limit mit dynamischem Schluessel, z.B. pro API-Key:
  ```python
  @swarmq.task(rate_limit='100/m', rate_limit_key=lambda to, body: to.split('@')[1])
  async def send_email(to: str, body: str): ...
  ```

---

# 2. Task Locking (Concurrency Control)

## 2.1 Huey: lock_task Decorator

### API

```python
@huey.task()
@huey.lock_task('send-newsletter')
def send_newsletter():
    # Nur eine Instanz dieses Tasks kann gleichzeitig laufen
    ...
```

### Implementierung

```python
class TaskLock:
    def __init__(self, huey, name):
        self._huey = huey
        self._name = name
        self._key = f'{self._huey.name}.lock.{self._name}'
        self._huey._locks.add(self._key)

    def acquire(self):
        if not self._huey.put_if_empty(self._key, '1'):
            raise TaskLockedException(f'unable to acquire lock {self._name}')
        return True

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._huey.delete(self._key)
```

### Kernpunkte

- **Storage:** `put_if_empty` = Redis `SET NX`
- **Lock Release:** Immer im `__exit__` — bei Erfolg UND bei Fehler
- **Bei Lock-Konflikt:** `TaskLockedException` wird geworfen, Task schlaegt fehl
- **Kein Timeout:** Lock bleibt bis zur expliziten Freigabe bestehen
- **Gefahr:** Bei Worker-Crash bleibt Lock bestehen (kein automatischer Ablauf!)

### Bewertung

| Aspekt | Bewertung |
|---|---|
| Einfachheit | Sehr gut |
| Crash-Sicherheit | Mangelhaft — kein TTL |
| Flexibilitaet | Maessig — nur per Lock-Name |
| Verhalten bei Konflikt | Task schlaegt fehl (kein Retry) |

---

## 2.2 Procrastinate: PostgreSQL-basierte Locks

### Zwei Lock-Typen

**queueing_lock** — Verhindert doppeltes Einreihen:
```python
@app.task(queueing_lock="newsletter-send")
async def send_newsletter(): ...
```
- Implementiert als UNIQUE-Constraint auf der `jobs`-Tabelle
- Wirft `AlreadyEnqueued` bei Duplikat
- Lock wird freigegeben, wenn Job die Queue verlaesst (startet oder abgeschlossen)

**lock** (Execution Lock) — Verhindert parallele Ausfuehrung:
```python
@app.task(lock="newsletter-send")
async def send_newsletter(): ...
```
- Implementiert ueber PostgreSQL Advisory Locks
- Verhindert, dass zwei Jobs mit gleichem Lock gleichzeitig laufen
- Lock wird bei Job-Abschluss freigegeben

### Unterschied

| Eigenschaft | queueing_lock | lock (Execution) |
|---|---|---|
| Zeitpunkt | Beim Einreihen | Bei Ausfuehrung |
| Mechanismus | DB UNIQUE Constraint | Advisory Lock |
| Bei Konflikt | AlreadyEnqueued Exception | Job wartet / wird uebersprungen |
| Zweck | Deduplizierung | Concurrency Control |

---

## 2.3 Dramatiq: ConcurrentRateLimiter als Lock

### Mutex-Pattern (limit=1)

```python
MUTEX = ConcurrentRateLimiter(backend, "newsletter-lock", limit=1, ttl=900_000)

@dramatiq.actor
def send_newsletter():
    with MUTEX.acquire(raise_on_failure=True) as acquired:
        if not acquired:
            raise dramatiq.RateLimitExceeded()
        # Geschuetzter Code
```

### Implementierung

```python
class ConcurrentRateLimiter(RateLimiter):
    def __init__(self, backend, key, limit=1, ttl=900000):
        self.limit = limit
        self.ttl = ttl
        ...

    def _acquire(self):
        added = self.backend.add(self.key, 1, self.ttl)
        if added:
            return True
        return self.backend.incr(self.key, 1, self.limit, self.ttl)

    def _release(self):
        self.backend.decr(self.key, 1, 0)
```

### Kernpunkte

- **TTL:** 15 Minuten Standard — automatische Freigabe bei Crash
- **Limit > 1:** Erlaubt N parallele Ausfuehrungen (nicht nur Mutex)
- **Release:** Dekrementiert Counter bei Abschluss
- **Backend:** WATCH-basierte Transaktionen (nicht Lua)

---

## 2.4 Faktory Enterprise: Concurrency Throttle

```toml
instagram = { concurrency = 4, timeout = 60 }
```

- Locks werden bei FETCH akquiriert und bei ACK/FAIL freigegeben
- **Timeout** als Sicherheitsnetz fuer gecrasshte Worker
- Monitoring: `throttle.lock` und `throttle.unlock` Metriken
- "Overage" und "Reclaimed" Zaehler zeigen Lock-Probleme an

---

## 2.5 Patterns fuer Distributed Locking

### SET NX PX — Basis-Pattern

```
SET lock_key unique_value NX PX timeout_ms
```

- `NX`: Nur setzen wenn Key nicht existiert
- `PX`: Ablaufzeit in Millisekunden
- `unique_value`: Identifiziert den Lock-Inhaber (fuer sicheres Release)

### Lock-Granularitaet

| Muster | Key-Format | Anwendungsfall |
|---|---|---|
| Per Task-Name | `sq:lock:{task_name}` | Singleton-Tasks |
| Per Task + Args | `sq:lock:{task_name}:{hash(args)}` | Gleiche Operation auf gleichem Objekt verhindern |
| Per Lock-Name | `sq:lock:{lock_name}` | Benutzerdefinierte Lock-Gruppen |

### Verhalten bei Lock-Konflikt

| Option | Beschreibung | Empfehlung |
|---|---|---|
| Skip (Drop) | Task wird verworfen | Fuer idempotente Periodic Tasks |
| Requeue mit Delay | Task geht zurueck in Queue | Fuer wichtige Tasks |
| Warten (Block) | Worker blockiert bis Lock frei | Nie empfohlen (async!) |
| Exception | TaskLockedException | Wie Huey — einfach aber inflexibel |

### Lock-Timeout

- **Sollte >= Task-Timeout** sein, sonst wird Lock vorzeitig freigegeben
- **Faustformel:** `lock_ttl = task_timeout * 1.5` (Sicherheitspuffer)
- **Alternative:** Lock-Verlaengerung (Lock Extension) waehrend der Ausfuehrung

### Lock Release: Wann?

| Strategie | Beschreibung | Anwendungsfall |
|---|---|---|
| Bei Erfolg + Fehler | Lock wird immer freigegeben | Standard — naechster Task kann sofort starten |
| Nur bei Erfolg | Lock bleibt bei Fehler bestehen | Wenn Retry den Lock braucht |
| Lock bleibt bis TTL | Kein explizites Release | Fuer Deduplizierung (nicht Locking) |

**Empfehlung:** Release bei Erfolg UND Fehler (wie Huey, Dramatiq). Bei Retries soll der Retry-Versuch den Lock neu akquirieren.

### Reentrante Locks

Problem: Ein Task mit Retries muss den eigenen Lock wieder akquirieren koennen.

Loesungen:
1. **Lock wird vor Retry freigegeben** (einfachste Loesung)
2. **Lock-Value = Task-ID**, und der gleiche Task darf den Lock uebernehmen
3. **Kein Problem**, wenn Lock-TTL < Retry-Delay

---

## 2.6 Lua-Skripte fuer Task Locking

### Lock Akquirieren (mit Owner-Tracking)

```lua
-- KEYS[1] = Lock-Key (z.B. "sq:lock:send_newsletter")
-- ARGV[1] = owner_id (z.B. Task-ID)
-- ARGV[2] = ttl_ms
--
-- Rueckgabe: 1 = akquiriert, 0 = Lock belegt

local key = KEYS[1]
local owner = ARGV[1]
local ttl = tonumber(ARGV[2])

-- Pruefen ob Lock existiert
local current_owner = redis.call('GET', key)

if current_owner == false then
    -- Lock frei — akquirieren
    redis.call('SET', key, owner, 'PX', ttl)
    return 1
elseif current_owner == owner then
    -- Reentrant: gleicher Owner, TTL erneuern
    redis.call('PEXPIRE', key, ttl)
    return 1
else
    -- Lock von anderem Owner gehalten
    return 0
end
```

### Lock Freigeben (sicher, nur eigener Lock)

```lua
-- KEYS[1] = Lock-Key
-- ARGV[1] = owner_id
--
-- Rueckgabe: 1 = freigegeben, 0 = nicht eigener Lock

local key = KEYS[1]
local owner = ARGV[1]

local current_owner = redis.call('GET', key)

if current_owner == owner then
    redis.call('DEL', key)
    return 1
else
    -- Lock gehoert jemand anderem oder ist bereits abgelaufen
    return 0
end
```

### Lock Akquirieren mit max_concurrency (Semaphore)

```lua
-- KEYS[1] = Lock-Key (z.B. "sq:lock:send_email")
-- ARGV[1] = owner_id (Task-ID)
-- ARGV[2] = max_concurrency
-- ARGV[3] = ttl_ms
-- ARGV[4] = jetzt_ms (Timestamp)
--
-- Rueckgabe: 1 = akquiriert, 0 = max erreicht

local key = KEYS[1]
local owner = ARGV[1]
local max_conc = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

-- Abgelaufene Eintraege entfernen
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - ttl)

-- Aktuelle Anzahl pruefen
local current = redis.call('ZCARD', key)

if current >= max_conc then
    return 0
end

-- Owner hinzufuegen mit aktuellem Timestamp als Score
redis.call('ZADD', key, now, owner)
redis.call('PEXPIRE', key, ttl)

return 1
```

### Semaphore Release

```lua
-- KEYS[1] = Lock-Key
-- ARGV[1] = owner_id
--
-- Rueckgabe: 1 = freigegeben, 0 = nicht gefunden

local key = KEYS[1]
local owner = ARGV[1]

local removed = redis.call('ZREM', key, owner)
return removed
```

---

## 2.7 Designentscheidungen fuer SwarmQ

### Empfohlene API

```python
# Einfacher Mutex (max 1 parallel)
@swarmq.task(lock='send-newsletter')
async def send_newsletter(): ...

# Lock basierend auf Argumenten
@swarmq.task(lock=lambda user_id: f'sync-user-{user_id}')
async def sync_user(user_id: int): ...

# Concurrency Control (max N parallel)
@swarmq.task(max_concurrency=5)
async def call_external_api(url: str): ...

# Verhalten bei Lock-Konflikt konfigurierbar
@swarmq.task(lock='send-newsletter', on_locked='requeue')  # oder 'skip', 'raise'
async def send_newsletter(): ...
```

### Key-Format

```
sq:lock:{lock_name}                   — Einfacher Lock (Mutex)
sq:lock:{task_name}:{hash(args)}      — Auto-Lock per Task+Args
sq:sem:{task_name}                    — Semaphore (max_concurrency)
```

### Empfohlene Implementierung

1. **Mutex (lock):** `SET NX PX` mit Owner-Tracking (Lua-Skript oben)
2. **Semaphore (max_concurrency):** Sorted Set mit Timestamp-Scores (Lua-Skript oben)
3. **Lock-TTL:** `task_timeout * 1.5`, mindestens 60 Sekunden
4. **Lock-Release:** Bei Erfolg UND Fehler, vor Retry
5. **on_locked Standard:** `'requeue'` mit exponential backoff

### Was passiert bei Lock-Konflikt?

```
on_locked='requeue':  Task wird mit Delay zurueck in die Queue gelegt
                       Delay: min(base_delay * 2^attempt, max_delay)
                       Standard: base_delay=1s, max_delay=60s

on_locked='skip':     Task wird verworfen (ACK ohne Ausfuehrung)
                       Result: TaskSkipped mit Grund

on_locked='raise':    TaskLockedException wird geworfen
                       Normales Error-Handling greift (Retry-Policy etc.)
```

---

# 3. Unique Tasks / Deduplizierung

## 3.1 Faktory Enterprise: unique_for + unique_until

### API

```ruby
# Ruby-Client Beispiel
client.push({
  "jobtype" => "SendEmail",
  "args" => ["user@example.com"],
  "unique_for" => 600,          # 10 Minuten Deduplizierungs-Fenster
  "unique_until" => "success"   # Lock bleibt bis zum Erfolg
})
```

### Unique-Key-Berechnung

Basiert auf drei Faktoren:
- **jobtype** (Task-Name)
- **args** (Argumente)
- **queue** (Queue-Name)

Identische Jobs in unterschiedlichen Queues sind NICHT dedupliziert.

### unique_until Strategien

**"success" (Standard):**
- Lock bleibt bestehen bis Job erfolgreich abgeschlossen
- Bei Fehler + Retry: Lock bleibt aktiv, kein doppeltes Einreihen moeglich
- Lock wird erst bei SUCCESS oder nach Ablauf von `unique_for` freigegeben
- **Sicherste Option**

**"start":**
- Lock wird freigegeben sobald Job-Ausfuehrung beginnt
- Ermoeglicht sofortiges Re-Enqueue eines gleichen Jobs
- Bei Fehler: Job kann waehrend der Ausfuehrung doppelt eingereiht werden
- **Nuetzlich fuer:** Periodic Tasks, wo Re-Enqueue schnell moeglich sein soll

### Lebenszyklus

```
Enqueue ──> Lock gesetzt (unique_for TTL)
   │
   ├── Duplikat versucht ──> Abgelehnt (Lock aktiv)
   │
   ├── unique_until="start":
   │      Start ──> Lock freigegeben
   │
   └── unique_until="success":
          Start ──> Lock bleibt
          Success ──> Lock freigegeben
          Failure ──> Lock bleibt (bis TTL oder naechster Success)
```

---

## 3.2 Procrastinate: queueing_lock

### API

```python
@app.task(queueing_lock="daily-report")
async def generate_daily_report(): ...

# Beim Aufruf:
try:
    await generate_daily_report.defer_async()
except AlreadyEnqueued:
    print("Report ist bereits in der Queue")
```

### Implementierung

- **Mechanismus:** PostgreSQL UNIQUE-Constraint auf `queueing_lock`-Spalte
- **Constraint:** `procrastinate_jobs_queueing_lock_idx_v1`
- **Bei Duplikat:** `UniqueViolation` wird in `AlreadyEnqueued` umgewandelt:

```python
if exc.constraint_name == QUEUEING_LOCK_CONSTRAINT:
    raise exceptions.AlreadyEnqueued(
        f"Job cannot be enqueued: there is already a job in the queue "
        f"with the queueing lock {queueing_lock}"
    ) from exc
```

### Lock-Freigabe

Der `queueing_lock`-Wert wird aus der `jobs`-Tabelle entfernt, wenn der Job:
- Erfolgreich abgeschlossen wird
- Fehlschlaegt (endgueltig, ohne weitere Retries)
- Manuell geloescht wird

### Bewertung

Elegant fuer PostgreSQL-basierte Systeme, aber nicht auf Valkey uebertragbar. Das Konzept (UNIQUE Constraint bei Einreihung) ist aber wichtig.

---

## 3.3 arq: _job_id als Deduplizierungsschluessel

### API

```python
# Automatisch generiert (uuid4)
await arq_redis.enqueue_job('send_email', 'user@example.com')

# Explizite Job-ID fuer Deduplizierung
await arq_redis.enqueue_job('send_email', 'user@example.com',
                             _job_id='send-email-user@example.com')
```

### Implementierung

```python
async def enqueue_job(self, function, *args, _job_id=None, **kwargs):
    job_id = _job_id or uuid4().hex
    job_key = job_key_prefix + job_id

    async with self.pipeline() as pipe:
        await pipe.watch(job_key)

        # Duplikat-Check: Job-Key oder Result-Key vorhanden?
        if await pipe.exists(job_key, result_key_prefix + job_id):
            return None  # Duplikat!

        pipe.multi()
        pipe.psetex(job_key, timeout_ms, serialized_job)
        pipe.zadd(queue_key, {job_key: score})
        try:
            await pipe.execute()
        except WatchError:
            return None  # Race Condition — als Duplikat behandeln
```

### Kernpunkte

- **Deduplizierungs-Key:** `_job_id` (Benutzer-definiert oder UUID)
- **Atomaritaet:** Redis WATCH + MULTI/EXEC (Optimistic Locking)
- **Rueckgabe bei Duplikat:** `None` (kein Fehler, kein Exception)
- **Pruefung:** Sowohl Job-Key als auch Result-Key (verhindert Re-Enqueue abgeschlossener Jobs)
- **TTL:** Job-Key hat TTL, dadurch zeitbasierte Deduplizierung

### Bewertung

| Aspekt | Bewertung |
|---|---|
| Einfachheit | Gut — Benutzer setzt _job_id |
| Atomaritaet | Maessig — WATCH statt Lua |
| Flexibilitaet | Gut — beliebige Key-Generierung |
| Rueckgabe | Sauber — None statt Exception |

---

## 3.4 SAQ: Key-basierte Unique Jobs

### API

```python
job = Job(function="send_email", kwargs={"to": "user@example.com"},
          key="send-email-user@example.com")
await queue.enqueue(job)
```

### Lua-Skript fuer atomisches Enqueue

```lua
if not redis.call('ZSCORE', KEYS[1], KEYS[2]) and
   redis.call('EXISTS', KEYS[4]) == 0 then
    redis.call('SET', KEYS[2], ARGV[1])
    redis.call('ZADD', KEYS[1], ARGV[2], KEYS[2])
    if ARGV[2] == '0' then redis.call('RPUSH', KEYS[3], KEYS[2]) end
    return 1
else
    return nil
end
```

### Kernpunkte

- **Key:** Benutzer-definiert, Standard ist `uuid1()`
- **Duplikat-Check:** Atomar in Lua (ZSCORE auf Incomplete-Set)
- **Bei Duplikat:** `enqueue()` gibt `None` zurueck, bestehender Job wird aktualisiert
- **CronJob:** `unique=True` als Standard — verhindert doppelte Cron-Jobs

---

## 3.5 celery-singleton / celery-heimdall

### celery-singleton

```python
from celery_singleton import Singleton

@app.task(base=Singleton)
def send_email(to):
    ...
```

- **Lock-Key:** Generiert aus `task_name + args + kwargs` (via Hash)
- **Backend:** Redis SET NX
- **Lock-Freigabe:** Nach Task-Abschluss (on_success + on_failure)
- **Bei Duplikat:** Gibt die bestehende Task-ID zurueck

### celery-heimdall

```python
from celery_heimdall import HeimdallTask

@app.task(base=HeimdallTask, unique=True, unique_timeout=600)
def send_email(to):
    ...
```

- **unique_timeout:** Deduplizierungsfenster in Sekunden
- Aehnlicher Ansatz wie celery-singleton
- Zusaetzlich: Rate Limiting und Task-Locking Features

---

## 3.6 Lua-Skripte fuer Deduplizierung

### Unique-Task Enqueue (empfohlen fuer SwarmQ)

```lua
-- KEYS[1] = Unique-Key (z.B. "sq:unique:{task_name}:{args_hash}")
-- ARGV[1] = task_id
-- ARGV[2] = ttl_ms (unique_for Fenster)
-- ARGV[3] = unique_until ("enqueue", "start", "success")
--
-- Rueckgabe: {status, existing_task_id}
--   status: 1 = neu (darf eingereiht werden), 0 = Duplikat
--   existing_task_id: ID des bestehenden Tasks (bei Duplikat)

local key = KEYS[1]
local task_id = ARGV[1]
local ttl = tonumber(ARGV[2])
local unique_until = ARGV[3]

-- Pruefen ob bereits ein Task mit diesem Key existiert
local existing = redis.call('GET', key)

if existing then
    -- Duplikat gefunden
    return {0, existing}
end

-- Unique-Lock setzen
redis.call('SET', key, task_id, 'PX', ttl)

return {1, task_id}
```

### Unique-Lock Freigeben (bei "start" oder "success")

```lua
-- KEYS[1] = Unique-Key
-- ARGV[1] = task_id (nur eigenen Lock freigeben)
--
-- Rueckgabe: 1 = freigegeben, 0 = nicht eigener Lock oder nicht vorhanden

local key = KEYS[1]
local task_id = ARGV[1]

local current = redis.call('GET', key)

if current == task_id then
    redis.call('DEL', key)
    return 1
end

return 0
```

### Hash-Funktion fuer Argumente

```python
import hashlib
import json

def stable_args_hash(*args, **kwargs) -> str:
    """Erzeugt einen stabilen Hash fuer Task-Argumente.

    Stabil ueber Python-Neustarts hinweg (kein hash() verwenden!).
    Deterministisch: gleiche Argumente = gleicher Hash.
    """
    # JSON mit sort_keys fuer deterministische Serialisierung
    serialized = json.dumps(
        {"args": args, "kwargs": kwargs},
        sort_keys=True,
        default=str,  # Fallback fuer nicht-serialisierbare Typen
        ensure_ascii=True,
    )
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]
```

**Wichtig:** Pythons eingebautes `hash()` ist **nicht stabil** ueber Neustarts (PYTHONHASHSEED). Immer `hashlib` verwenden.

### Edge Cases

**Was wenn der originale Task fehlschlaegt?**

| unique_until | Verhalten bei Fehler |
|---|---|
| `"enqueue"` | Lock wurde bei Enqueue gesetzt und laeuft per TTL ab. Kein Problem. |
| `"start"` | Lock wurde bei Start freigegeben. Neuer Task kann sofort eingereiht werden. |
| `"success"` | Lock bleibt bestehen bis TTL ablaeuft. Waehrend dieser Zeit kein Re-Enqueue moeglich. **Achtung:** Bei Retries muss der Lock erhalten bleiben. |

**Was wenn der Worker crasht?**

- TTL sorgt dafuer, dass der Lock spaetestens nach `unique_for` Sekunden ablaeuft
- Ohne TTL: Lock bleibt ewig bestehen (Deadlock!)
- **Empfehlung:** `unique_for` immer setzen, kein unbegrenztes Unique-Fenster erlauben

---

## 3.7 Designentscheidungen fuer SwarmQ

### Empfohlene API

```python
# Einfache Deduplizierung (task_name + args als Key)
@swarmq.task(unique_for=600)  # 10 Minuten
async def send_email(to: str, body: str): ...

# unique_until Strategie
@swarmq.task(unique_for=600, unique_until='success')  # Standard
async def process_payment(order_id: int): ...

@swarmq.task(unique_for=600, unique_until='start')
async def sync_user_data(user_id: int): ...

# Benutzerdefinierter Unique-Key
@swarmq.task(unique_for=600, unique_key=lambda order_id: f'payment-{order_id}')
async def process_payment(order_id: int): ...
```

### Empfohlene Defaults

```python
unique_for: int | None = None          # Kein Unique per Default
unique_until: str = 'success'          # Sicherste Option
unique_key: Callable | None = None     # Auto: task_name + stable_args_hash
```

### Rueckgabe bei Duplikat

```python
# Option A: None zurueckgeben (wie arq)
result = await send_email.enqueue("user@example.com", "Hallo")
if result is None:
    print("Duplikat — Task bereits in Queue")

# Option B: Bestehende Task-ID zurueckgeben (informativer)
result = await send_email.enqueue("user@example.com", "Hallo")
if result.is_duplicate:
    print(f"Duplikat von Task {result.existing_task_id}")
```

**Empfehlung:** Option B — `EnqueueResult` Objekt mit `is_duplicate` Flag und `existing_task_id`. Informativer als `None` und ermoeglicht dem Aufrufer, den bestehenden Task zu tracken.

```python
@dataclass(frozen=True, slots=True)
class EnqueueResult:
    task_id: str
    is_duplicate: bool = False
    queue: str = ''
```

### Key-Format

```
sq:unique:{task_name}:{args_hash}     — Auto-generierter Unique-Key
sq:unique:{custom_key}                — Benutzerdefinierter Key
```

### Unique-Lock Lifecycle

```
enqueue() aufgerufen
  │
  ├── Lua: Unique-Key pruefen
  │     ├── Key existiert ──> EnqueueResult(is_duplicate=True, task_id=existing)
  │     └── Key frei ──> SET key task_id PX unique_for_ms
  │                       Message an RabbitMQ senden
  │                       EnqueueResult(is_duplicate=False, task_id=new_id)
  │
  ├── Worker empfaengt Task
  │     ├── unique_until="start" ──> Lua: Lock freigeben
  │     └── unique_until="success" ──> Lock bleibt
  │
  └── Task abgeschlossen
        ├── Erfolg ──> Lua: Lock freigeben (bei "success")
        └── Fehler ──> Lock bleibt (bei "success"), TTL laeuft ab
                        Lock freigegeben (bei "start", war schon frei)
```

---

# 4. Zusammenfassung und Empfehlungen

## Feature-Matrix: Bestehende Libraries

| Feature | Celery | Dramatiq | Huey | Procrastinate | arq | SAQ | Faktory |
|---|---|---|---|---|---|---|---|
| Rate Limiting | Per-Worker Token Bucket | Distributed (Redis) | — | — | — | — | Queue Throttle |
| Task Lock | — | ConcurrentRateLimiter | lock_task | lock (Advisory) | — | — | Concurrency |
| Unique Tasks | — (Drittanbieter) | — | — | queueing_lock | _job_id | key | unique_for |

## Empfohlene Implementierungsreihenfolge fuer SwarmQ

### Phase 1: Unique Tasks (hoechste Prioritaet)

Am haeufigsten benoetigt. Verhindert doppelte Verarbeitung.

- `unique_for` + `unique_until` (nach Faktory-Vorbild)
- Valkey SET NX PX mit Lua-Skript
- `stable_args_hash` fuer Key-Generierung
- `EnqueueResult` mit Duplikat-Info

### Phase 2: Task Locking

Zweithaeufigstes Feature. Notwendig fuer Singleton-Tasks und Concurrency Control.

- `lock` Parameter (nach Huey/Procrastinate-Vorbild, aber mit TTL)
- `max_concurrency` fuer Semaphore-Verhalten
- `on_locked` Konfiguration (requeue/skip/raise)
- Sorted Set + Lua fuer Semaphore

### Phase 3: Rate Limiting

Komplex, aber wichtig fuer API-Integrationen.

- Sliding Window Counter als Standard-Algorithmus
- Token Bucket als Alternative
- Globales Rate Limiting ueber Valkey (nicht per-Worker wie Celery!)
- `rate_limit` String-Syntax kompatibel zu Celery
- Requeue mit Delay bei Ueberschreitung

## Valkey-Key-Namensraum (gesamt)

```
sq:unique:{task_name}:{args_hash}    — Unique-Task Lock
sq:lock:{lock_name}                  — Task Mutex Lock
sq:sem:{task_name}                   — Task Semaphore (Sorted Set)
sq:rl:{task_name}:{window_ts}       — Rate Limit Counter
sq:rl:{task_name}:prev:{window_ts}  — Rate Limit Previous Window
```

## Offene Fragen

1. **Rate Limit + Unique Task Interaktion:** Wenn ein unique Task rate-limited requeued wird, soll der Unique-Lock bestehen bleiben? (Ja — sonst koennte ein Duplikat eingereiht werden waehrend der Original-Task wartet.)

2. **Lock + Retry Interaktion:** Wenn ein gelockter Task fehlschlaegt und retried wird, soll er den Lock behalten oder neu akquirieren? (Neu akquirieren — Lock wird bei Fehler freigegeben, Retry ist ein neuer Versuch.)

3. **max_concurrency + Prefetch:** Sollte der Worker den Prefetch-Count basierend auf max_concurrency anpassen? (Nein — das wuerde Queue-Mixing verhindern. Besser: Requeue bei Semaphore-Sättigung.)
