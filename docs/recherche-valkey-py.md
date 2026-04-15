# Recherche: Async Python Libraries fuer Valkey (SwarmQ Result Backend + Distributed Lock)

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument fuer die Implementierung des Valkey-Result-Backends und des Distributed-Lock-Mechanismus in SwarmQ.

---

## Inhaltsverzeichnis

1. [valkey-py — Primaerer Kandidat](#1-valkey-py--primaerer-kandidat)
2. [redis-py als Alternative](#2-redis-py-als-alternative)
3. [Best Practices: Valkey als Result Backend](#3-best-practices-valkey-als-result-backend)
4. [Best Practices: Valkey als Distributed Lock](#4-best-practices-valkey-als-distributed-lock)
5. [Performance-Ueberlegungen](#5-performance-ueberlegungen)
6. [Patterns fuer Task-Queue Result-Storage](#6-patterns-fuer-task-queue-result-storage)
7. [Haeufige Fallstricke](#7-haeufige-fallstricke)
8. [Valkey-spezifische Features](#8-valkey-spezifische-features)
9. [Python 3.14 Kompatibilitaet](#9-python-314-kompatibilitaet)
10. [Zusammenfassung und Empfehlungen](#10-zusammenfassung-und-empfehlungen)

---

# 1. valkey-py — Primaerer Kandidat

## 1.1 Ueberblick und Status

- **Aktuelle Version:** 6.1.1 (11. August 2025)
- **Python-Anforderung:** >=3.9
- **Lizenz:** MIT
- **Wartung:** Aktiv gepflegt durch valkey-io Organisation
- **Repository:** https://github.com/valkey-io/valkey-py
- **Dokumentation:** https://valkey-py.readthedocs.io

## 1.2 Beziehung zu redis-py

valkey-py ist ein **direkter Fork von redis-py**. Die API ist nahezu identisch:

| redis-py | valkey-py |
|---|---|
| `redis.Redis` | `valkey.Valkey` |
| `redis.asyncio.Redis` | `valkey.asyncio.Valkey` |
| `redis.ConnectionPool` | `valkey.ConnectionPool` |
| `redis.asyncio.ConnectionPool` | `valkey.asyncio.ConnectionPool` |

Rueckwaertskompatibilitaet wird durch Aliase gewaehrleistet:

```python
# Aus valkey/__init__.py
Redis = Valkey
StrictRedis = StrictValkey
RedisCluster = ValkeyCluster
RedisError = ValkeyError
```

**Jeder redis-py Code kann mit minimalen Aenderungen auf valkey-py migriert werden.**

## 1.3 Async Client: `valkey.asyncio.Valkey`

Basiert auf `asyncio.StreamReader`/`asyncio.StreamWriter`, verwendet `asyncio.Lock` fuer Thread-Sicherheit.

```python
import valkey.asyncio as valkey

# Methode 1: Direkte Erstellung
client = valkey.Valkey(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True,
    socket_timeout=5.0,
    max_connections=20,
)

# Methode 2: Aus URL
client = valkey.Valkey.from_url("valkey://localhost:6379/0")

# Methode 3: Aus bestehendem Pool (empfohlen)
pool = valkey.ConnectionPool(
    host="localhost",
    port=6379,
    max_connections=20,
    decode_responses=True,
)
client = valkey.Valkey.from_pool(pool)

# Context-Manager fuer sauberes Cleanup
async with valkey.Valkey(host="localhost") as client:
    await client.set("key", "value")
    result = await client.get("key")

# Explizites Schliessen
await client.aclose()
```

**Internes Verhalten von `execute_command`:**
1. Holt Connection aus dem Pool (`pool.get_connection()`)
2. Prueft optionalen Client-Side-Cache
3. Sendet Befehl mit automatischem Retry (`conn.retry.call_with_retry()`)
4. Gibt Connection zurueck an den Pool (`pool.release(conn)`)

Gesamter Ablauf ist **non-blocking** — keine Threads.

## 1.4 Connection Pooling

### ConnectionPool (Standard)

```python
pool = valkey.asyncio.ConnectionPool(
    host="localhost",
    port=6379,
    max_connections=20,  # Default: 2**31 (praktisch unbegrenzt — GEFAEHRLICH!)
    decode_responses=True,
)
```

- Verwaltet `_available_connections` (Liste) und `_in_use_connections` (WeakSet)
- Bei Pool-Erschoepfung: wirft sofort `ConnectionError("Too many connections")`
- Default `max_connections` ist `2**31` — **muss immer explizit gesetzt werden!**

### BlockingConnectionPool (empfohlen fuer Produktion)

```python
pool = valkey.asyncio.BlockingConnectionPool(
    host="localhost",
    port=6379,
    max_connections=50,  # Default: 50
    timeout=20,          # Default: 20 Sekunden Wartezeit
    decode_responses=True,
)
```

- Verwendet `asyncio.Condition` statt sofortigem Fehler
- Blockiert aktuellen Task bis Connection verfuegbar wird
- Wirft `ConnectionError("No connection available.")` nach Timeout
- **Empfehlung fuer SwarmQ:** `BlockingConnectionPool` mit `max_connections=20`, `timeout=10`

## 1.5 Pipeline-Unterstuetzung

```python
# Nicht-atomare Pipeline (kein MULTI/EXEC)
async with client.pipeline(transaction=False) as pipe:
    pipe.set("key1", "value1")
    pipe.set("key2", "value2")
    pipe.get("key1")
    results = await pipe.execute()
    # results = [True, True, b"value1"]

# Atomare Pipeline (mit MULTI/EXEC)
async with client.pipeline(transaction=True) as pipe:
    pipe.set("key1", "value1")
    pipe.incr("counter")
    results = await pipe.execute()

# Optimistisches Locking mit WATCH
async def transfer(client, src, dst, amount):
    async with client.pipeline(True) as pipe:
        while True:
            try:
                await pipe.watch(src, dst)
                src_bal = int(await pipe.get(src))
                dst_bal = int(await pipe.get(dst))
                pipe.multi()
                pipe.set(src, src_bal - amount)
                pipe.set(dst, dst_bal + amount)
                await pipe.execute()
                break
            except valkey.WatchError:
                continue
```

Intern: Pipeline packt alle Befehle in `command_stack` und sendet erst bei `execute()` als einen Batch. Bei `transaction=True` werden `MULTI`/`EXEC` automatisch umschlossen.

## 1.6 Lua Scripting

### register_script (empfohlene Methode)

```python
LUA_SET_AND_NOTIFY = client.register_script("""
    redis.call('SET', KEYS[1], ARGV[1])
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    redis.call('PUBLISH', KEYS[2], ARGV[3])
    return 1
""")

result = await LUA_SET_AND_NOTIFY(
    keys=["result:task123", "notify:task123"],
    args=["serialized_result", 3600000, "done"],
)
```

**AsyncScript Verhalten:**
1. SHA1-Hash wird bei Registrierung berechnet
2. Beim Aufruf: versucht zuerst `EVALSHA` (schnell, nur Hash gesendet)
3. Bei `NoScriptError`: automatischer Fallback auf `SCRIPT LOAD` + `EVALSHA`
4. Funktioniert auch in Pipelines

### Direkte Eval-Methoden

```python
# EVAL — sendet Script jedes Mal
await client.eval("return redis.call('GET', KEYS[1])", 1, "mykey")

# EVALSHA — sendet nur Hash
sha = await client.script_load("return redis.call('GET', KEYS[1])")
await client.evalsha(sha, 1, "mykey")
```

## 1.7 Pub/Sub

```python
# Subscriber
pubsub = client.pubsub()
await pubsub.subscribe("result:task123")

# Non-blocking mit Timeout
message = await pubsub.get_message(
    ignore_subscribe_messages=True,
    timeout=1.0,
)

# Als Iterator
async for message in pubsub.listen():
    if message["type"] == "message":
        print(message["data"])
        break

# Callback-basiert
async def handler(message):
    print(f"Ergebnis: {message['data']}")

await pubsub.subscribe(**{"result:task123": handler})

# Publisher
await client.publish("result:task123", "done")

# Aufraeumen (WICHTIG!)
await pubsub.unsubscribe("result:task123")
await pubsub.aclose()
```

**Wichtige Einschraenkungen:**
- Pub/Sub ist **fire-and-forget** — Nachrichten gehen verloren wenn Subscriber nicht verbunden
- Pub/Sub benoetigt **dedizierte Connection** (automatisch verwaltet)
- Geeignet als **Benachrichtigungskanal**, aber **nicht als alleiniger Mechanismus**
- Pattern: SET + PUBLISH als Kombination (Lua-Script fuer Atomaritaet)

## 1.8 Datentypen und async APIs

```python
# Strings
await client.set("key", "value", ex=3600)    # TTL in Sekunden
await client.set("key", "value", px=3600000)  # TTL in Millisekunden
await client.set("key", "value", nx=True)     # nur wenn nicht existiert
await client.get("key")
await client.mset({"k1": "v1", "k2": "v2"})
await client.mget("k1", "k2")

# Hashes
await client.hset("task:123", mapping={
    "status": "completed",
    "result": "serialized_data",
    "finished_at": "2026-04-15T10:00:00",
})
await client.hget("task:123", "result")
await client.hgetall("task:123")

# Listen
await client.lpush("queue:default", "task_data")
result = await client.brpop("queue:default", timeout=5)

# Sorted Sets
await client.zadd("schedule:next_run", {"task1": 1713168000.0})
tasks = await client.zrangebyscore("schedule:next_run", 0, time.time())
```

## 1.9 Streams (XADD/XREAD/XREADGROUP)

Koennte in späterer Phase als Valkey-basierter Broker dienen:

```python
# Stream erstellen / Nachricht hinzufuegen
msg_id = await client.xadd(
    "tasks:default",
    {"task": "example_task", "payload": "..."},
    maxlen=10000,
    approximate=True,
)

# Consumer Group erstellen
await client.xgroup_create(
    "tasks:default", "workers",
    id="0", mkstream=True,
)

# Als Consumer lesen (blockierend)
messages = await client.xreadgroup(
    groupname="workers",
    consumername="worker-1",
    streams={"tasks:default": ">"},
    count=10,
    block=5000,
)

# Bestaetigen
await client.xack("tasks:default", "workers", msg_id)
```

**Relevanz fuer SwarmQ:**
- Streams bieten eingebaute Consumer Groups mit Acknowledgment
- XREADGROUP mit `block` ist async-freundlich
- Koennte spaeter als Valkey-basierter Broker verwendet werden
- Nachteil: Kein DLQ-Mechanismus eingebaut (muesste manuell implementiert werden)

## 1.10 Error Handling

### Exception-Hierarchie

```
ValkeyError (Basis)
  |-- ConnectionError
  |     |-- AuthenticationError
  |     |-- AuthorizationError
  |     |-- BusyLoadingError
  |     |-- MaxConnectionsError
  |-- TimeoutError
  |-- InvalidResponse
  |-- ResponseError
  |     |-- NoScriptError
  |     |-- OutOfMemoryError
  |     |-- ExecAbortError
  |     |-- ReadOnlyError
  |     |-- NoPermissionError
  |     |-- ModuleError
  |-- DataError
  |-- PubSubError
  |-- WatchError
  |-- LockError (auch ValueError)
  |     |-- LockNotOwnedError
  |-- ClusterError
        |-- ClusterDownError
```

### Empfehlung fuer SwarmQ

```python
from valkey.exceptions import (
    ValkeyError, ConnectionError, TimeoutError,
    ResponseError, OutOfMemoryError,
    LockError, LockNotOwnedError, MaxConnectionsError,
)

# Result Backend: Verbindungsfehler behandeln
async def store_result(client, task_id, result):
    try:
        await client.set(f"swarmq:result:{task_id}", result, px=3600000)
    except (ConnectionError, TimeoutError) as e:
        # Retry mit Backoff — transiente Fehler
        logger.warning(f"Valkey-Verbindungsfehler: {e}")
        raise
    except OutOfMemoryError:
        # Valkey voll — kritisch, nicht retrybar
        logger.error("Valkey OOM — maxmemory erreicht!")
        raise

# Lock: Spezialisierte Fehlerbehandlung
async def acquire_scheduler_lock(client, worker_id):
    try:
        lock = client.lock(
            "swarmq:scheduler:lock",
            timeout=30, blocking=False, thread_local=False,
        )
        acquired = await lock.acquire(token=worker_id.encode())
        return lock if acquired else None
    except LockError:
        return None
    except (ConnectionError, TimeoutError):
        logger.warning("Valkey nicht erreichbar bei Lock-Versuch")
        return None
```

## 1.11 Retry/Reconnect-Verhalten

Eingebautes Retry mit konfigurierbarem Backoff:

```python
from valkey.backoff import EqualJitterBackoff
from valkey.asyncio.retry import Retry

client = valkey.asyncio.Valkey(
    host="localhost",
    retry_on_timeout=True,
    retry_on_error=[ConnectionError, TimeoutError],
    retry=Retry(
        backoff=EqualJitterBackoff(cap=0.512, base=0.008),
        retries=3,
    ),
)
```

**Verfuegbare Backoff-Strategien:**
- `NoBackoff()` — sofortiger Retry
- `ConstantBackoff(backoff=0.1)` — fester Abstand
- `ExponentialBackoff(cap=0.512, base=0.008)` — exponentiell
- `FullJitterBackoff(cap=0.512, base=0.008)` — exponentiell + Zufall
- `EqualJitterBackoff(cap=0.512, base=0.008)` — **Default**, halb exponentiell + halb Zufall
- `DecorrelatedJitterBackoff(cap=0.512, base=0.008)` — dekorreliert

**Automatisches Reconnect:** Ja. Connection wird bei Fehler getrennt und bei naechstem Befehl automatisch wiederverbunden.

**Health Checks:**
```python
client = valkey.asyncio.Valkey(health_check_interval=30)  # Sekunden zwischen PINGs
```

## 1.12 SSL/TLS

```python
client = valkey.asyncio.Valkey(
    host="valkey.example.com", port=6380,
    ssl=True,
    ssl_certfile="/path/to/client-cert.pem",
    ssl_keyfile="/path/to/client-key.pem",
    ssl_ca_certs="/path/to/ca-cert.pem",
    ssl_cert_reqs="required",
    ssl_check_hostname=True,
)

# Oder ueber URL
client = valkey.asyncio.Valkey.from_url(
    "valkeys://username:password@valkey.example.com:6380/0"
)
```

## 1.13 Cluster- und Sentinel-Unterstuetzung

```python
# Cluster
from valkey.asyncio import ValkeyCluster
client = ValkeyCluster(host="node1.example.com", port=6379)

# Sentinel
from valkey.asyncio.sentinel import Sentinel
sentinel = Sentinel(
    [("sentinel1", 26379), ("sentinel2", 26379), ("sentinel3", 26379)],
)
master = sentinel.master_for("myservice")
slave = sentinel.slave_for("myservice")
```

---

# 2. redis-py als Alternative

## 2.1 Unterschiede

| Aspekt | valkey-py | redis-py |
|---|---|---|
| Fork-Basis | Fork von redis-py | Original |
| Server-Kompatibilitaet | Valkey + Redis | Redis (+ Valkey via Wire-Kompatibilitaet) |
| Parser | libvalkey (Fork von hiredis) | hiredis |
| URL-Schema | `valkey://` und `redis://` | `redis://` |
| API | Identisch | Identisch |
| Wartung | valkey-io Community | Redis Ltd. |

redis-py funktioniert problemlos mit Valkey-Server (wire-protocol-kompatibel).

**Empfehlung: valkey-py**, weil:
1. Langfristig koennten Valkey-spezifische Features nur in valkey-py landen
2. URL-Schemas `valkey://` sind semantisch korrekter
3. Community fixt valkey-spezifische Bugs schneller

## 2.2 libvalkey Parser

```bash
pip install "valkey[libvalkey]"
```

- C-basierter Parser (Fork von hiredis)
- **Signifikanter Performance-Vorteil bei grossen Antworten** (Multi-Bulk-Replies)
- Minimal bei einfachen GET/SET
- Wird automatisch verwendet wenn installiert
- **Empfehlung: Immer installieren, kein Nachteil**

---

# 3. Best Practices: Valkey als Result Backend

## 3.1 Key-Namenskonventionen

```
swarmq:result:{task_id}          — Task-Ergebnis
swarmq:result:meta:{task_id}     — Metadaten (Status, Timing)
swarmq:notify:{task_id}          — Pub/Sub Kanal fuer Benachrichtigung
swarmq:scheduler:lock            — Scheduler Leader Lock
swarmq:schedule:{name}:last_run  — Letzte Ausfuehrung eines Schedules
swarmq:schedule:{name}:config    — Schedule-Konfiguration
```

**Regeln:**
- Doppelpunkt `:` als Trennzeichen (Valkey-Konvention, Cluster-kompatibel)
- Praefix `swarmq:` fuer Namensraum-Isolation
- Task-ID als UUID7 (zeitbasiert, sortierbar)

## 3.2 TTL-Strategie

```python
RESULT_TTL_MS = 24 * 60 * 60 * 1000       # 24 Stunden (Default)
RESULT_TTL_SHORT_MS = 60 * 60 * 1000       # 1 Stunde (Fire-and-Forget)
RESULT_TTL_LONG_MS = 7 * 24 * 60 * 60 * 1000  # 7 Tage (wichtige Tasks)
```

**Strategie:**
- **Immer TTL setzen** — niemals ohne Ablauf speichern
- TTL bei Schreiben des Ergebnisses setzen (SET + PX in einem Befehl)
- Default: 24h
- Fire-and-Forget Tasks: 1h oder gar kein Result speichern
- Optional: TTL erneuern bei `get_result()`

## 3.3 Serialisierung

```python
import msgpack

serialized = msgpack.packb(result, use_bin_type=True)
result = msgpack.unpackb(raw_data, raw=False)
```

**Empfehlung:** Ergebnis als **einzelner String-Key** mit msgpack-Bytes:
- `SET swarmq:result:{task_id} <msgpack_bytes> PX 86400000`
- Kein Hash (HSET) — unnoetige Komplexitaet fuer einzelnes Ergebnis
- msgpack ist kompakter als JSON und unterstuetzt binaere Daten nativ

## 3.4 Atomares Set-Result + Notify (Lua-Script)

```lua
-- KEYS[1] = result_key, KEYS[2] = notify_channel
-- ARGV[1] = result_data, ARGV[2] = ttl_ms, ARGV[3] = notification
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('PUBLISH', KEYS[2], ARGV[3])
return 1
```

```python
store_result = client.register_script(STORE_RESULT_SCRIPT)
await store_result(
    keys=[f"swarmq:result:{task_id}", f"swarmq:notify:{task_id}"],
    args=[msgpack.packb({"is_error": False, "result": result}), 86400000, "done"],
)
```

## 3.5 Result-Waiting: Pub/Sub + Polling Hybrid

**Warum Hybrid?**
- Reines Polling: Verschwendet Bandbreite, erhoet Latenz
- Reines Pub/Sub: Nachrichten gehen verloren wenn Subscriber nicht verbunden
- Keyspace Notifications: Unzuverlaessig (Timing, CPU-Overhead, fire-and-forget)

```python
async def get_result(client, task_id, timeout=30.0, polling_interval=0.5):
    result_key = f"swarmq:result:{task_id}"
    notify_channel = f"swarmq:notify:{task_id}"

    # 1. Pruefen ob Ergebnis schon da ist
    result = await client.get(result_key)
    if result is not None:
        return result

    # 2. Pub/Sub subscriben
    pubsub = client.pubsub()
    try:
        await pubsub.subscribe(notify_channel)
        deadline = asyncio.get_event_loop().time() + timeout

        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"Result timeout nach {timeout}s")

            # 3. Pub/Sub warten (kurzer Timeout)
            wait_time = min(polling_interval, remaining)
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=wait_time,
            )

            if msg and msg["type"] == "message":
                result = await client.get(result_key)
                if result is not None:
                    return result

            # 4. Fallback-GET
            result = await client.get(result_key)
            if result is not None:
                return result
    finally:
        await pubsub.unsubscribe(notify_channel)
        await pubsub.aclose()
```

## 3.6 Speicher-Management

**maxmemory-policy Empfehlung:**
- `volatile-lru` — evictiert nur Keys mit TTL, LRU-basiert
- **Niemals `allkeys-lru`** — koennte Locks und Schedules evictieren!

```
# valkey.conf
maxmemory 1gb
maxmemory-policy volatile-lru
```

## 3.7 Persistenz-Empfehlung

| Modus | Empfehlung | Grund |
|---|---|---|
| RDB only | Fuer Entwicklung | Snapshots reichen |
| AOF only | Nicht empfohlen | Performance-Overhead |
| RDB + AOF | Fuer Produktion | Bester Kompromiss |
| Keine Persistenz | Akzeptabel | Results haben TTL, Locks sind ephemeral |

SwarmQ-Doku sollte darauf hinweisen: **Keine Persistenz ist akzeptabel**, da Results TTL haben und Locks ephemeral sind. Bei Neustart gehen `last_run`-Timestamps verloren → einmalige Doppelausfuehrung moeglich (akzeptabel bei idempotenten Tasks).

---

# 4. Best Practices: Valkey als Distributed Lock

## 4.1 Eingebaute Lock-Klasse (valkey-py)

valkey-py hat eine vollstaendige `Lock`-Klasse mit Lua-Scripts:

```python
lock = client.lock(
    name="swarmq:scheduler:lock",
    timeout=30,           # Lock-TTL: 30 Sekunden
    sleep=0.1,            # Polling-Intervall beim Warten
    blocking=False,       # nicht warten, sofort zurueckgeben
    blocking_timeout=None,
    thread_local=False,   # WICHTIG: False fuer async!
)

acquired = await lock.acquire(token=worker_id.encode())
if acquired:
    try:
        # ... Scheduler-Arbeit ...
        pass
    finally:
        await lock.release()
```

**Was der eingebaute Lock intern macht:**
- `acquire()`: `SET name token NX PX timeout_ms`
- `release()`: Lua-Script das nur loescht wenn Token stimmt
- `extend()` / `reacquire()`: Lua-Script mit Ownership-Check + PEXPIRE

## 4.2 Eingebaute Lua-Scripts (aus valkey-py Quellcode)

### Lock Release
```lua
local token = redis.call('get', KEYS[1])
if not token or token ~= ARGV[1] then
    return 0
end
redis.call('del', KEYS[1])
return 1
```

### Lock Extend
```lua
local token = redis.call('get', KEYS[1])
if not token or token ~= ARGV[1] then
    return 0
end
local expiration = redis.call('pttl', KEYS[1])
if not expiration then expiration = 0 end
if expiration < 0 then return 0 end
local newttl = ARGV[2]
if ARGV[3] == "0" then
    newttl = ARGV[2] + expiration
end
redis.call('pexpire', KEYS[1], newttl)
return 1
```

### Lock Reacquire (TTL zuruecksetzen)
```lua
local token = redis.call('get', KEYS[1])
if not token or token ~= ARGV[1] then
    return 0
end
redis.call('pexpire', KEYS[1], ARGV[2])
return 1
```

## 4.3 Empfohlene Leader Election fuer SwarmQ

Die **eingebaute Lock-Klasse reicht** — kein eigener Lock-Code noetig:

```python
class SchedulerLeaderElection:
    def __init__(self, client: valkey.asyncio.Valkey, worker_id: str):
        self.client = client
        self.worker_id = worker_id
        self.lock = client.lock(
            name="swarmq:scheduler:lock",
            timeout=30,
            sleep=5,
            blocking=False,
            thread_local=False,
        )
        self.is_leader = False

    async def try_acquire(self) -> bool:
        acquired = await self.lock.acquire(token=self.worker_id.encode())
        self.is_leader = acquired
        return acquired

    async def heartbeat(self) -> bool:
        if not self.is_leader:
            return False
        try:
            await self.lock.reacquire()
            return True
        except LockNotOwnedError:
            self.is_leader = False
            return False
        except (ConnectionError, TimeoutError):
            logger.warning("Heartbeat fehlgeschlagen")
            return False

    async def release(self):
        if self.is_leader:
            try:
                await self.lock.release()
            except LockNotOwnedError:
                pass
            finally:
                self.is_leader = False

    async def run_election_loop(self):
        while True:
            if self.is_leader:
                if not await self.heartbeat():
                    logger.info("Leader-Status verloren")
                await asyncio.sleep(10)
            else:
                if await self.try_acquire():
                    logger.info(f"Worker {self.worker_id} ist neuer Leader")
                await asyncio.sleep(5)
```

## 4.4 Heartbeat vs TTL Ratio

| Parameter | Wert | Begruendung |
|---|---|---|
| Lock-TTL | 30s | Genug Zeit fuer 2 fehlgeschlagene Heartbeats |
| Heartbeat | 10s | 3 Heartbeats pro TTL-Fenster |
| Kandidaten-Check | 5s | Schnelles Failover |

**Faustregel:** TTL >= 3 × Heartbeat-Intervall.

---

# 5. Performance-Ueberlegungen

## 5.1 Connection Pool Groesse

**Empfehlung pro Worker-Prozess:**
- **Result Backend Pool:** 10-20 Connections
- **Lock/Scheduler Pool:** Separater Pool mit 3-5 Connections
- **Pub/Sub:** Dedizierte Connection (automatisch)

```python
result_pool = valkey.asyncio.BlockingConnectionPool(max_connections=20, timeout=10)
lock_pool = valkey.asyncio.BlockingConnectionPool(max_connections=5, timeout=5)
result_client = valkey.asyncio.Valkey.from_pool(result_pool)
lock_client = valkey.asyncio.Valkey.from_pool(lock_pool)
```

## 5.2 Pipeline-Batching

```python
# Schlecht: 10 Roundtrips
for task_id in completed_tasks:
    await client.set(f"swarmq:result:{task_id}", results[task_id])

# Gut: 1 Roundtrip
async with client.pipeline(transaction=False) as pipe:
    for task_id in completed_tasks:
        pipe.set(f"swarmq:result:{task_id}", results[task_id], px=86400000)
    await pipe.execute()
```

## 5.3 Lua Scripts vs Multiple Roundtrips

**Lua bevorzugen wenn:**
- Atomaritaet erforderlich (SET + PUBLISH fuer Result-Notification)
- Bedingte Logik auf dem Server (Lock-Ownership-Check)
- 2-3 Befehle die zusammengehoeren

**Lua vermeiden wenn:**
- Grosse Datenmengen (blockiert Single-Threaded Server)
- Mehr als ~10 Befehle (schwer zu debuggen)

## 5.4 RESP3 Protokoll

```python
client = valkey.asyncio.Valkey(protocol=3)
```

- Typisierte Antworten (Maps, Sets, Doubles)
- Client-Side Caching (Push-Invalidierung)
- Fuer SwarmQ: RESP2 reicht, RESP3 optional

## 5.5 Memory Overhead

- Jeder Key: ~60-80 Bytes Overhead
- 1 Million Results mit je 1KB: ~1.1 GB
- 1 Million Results mit je 100 Bytes: ~170 MB

## 5.6 Grosse Werte (>1MB)

```python
import zlib

threshold = 1024  # ab 1KB komprimieren
serialized = msgpack.packb(result)
if len(serialized) > threshold:
    compressed = zlib.compress(serialized, level=6)
    data = b"\x01" + compressed  # Praefix "komprimiert"
else:
    data = b"\x00" + serialized  # Praefix "unkomprimiert"
```

---

# 6. Patterns fuer Task-Queue Result-Storage

## 6.1 Result-Expiration-Strategie

```python
class TaskResultConfig:
    default_ttl_ms: int = 86400000         # 24h
    fire_and_forget_ttl_ms: int = 0        # kein Speichern
    extend_on_read: bool = False
    extend_ttl_ms: int = 3600000           # +1h bei Abruf
```

## 6.2 Nie abgerufene Results

- TTL sorgt automatisch fuer Bereinigung
- Kein aktives Problem — TTL reicht

## 6.3 Fortschritts-Tracking

```lua
-- Progress-Update Lua-Script
redis.call('HSET', KEYS[1], 'progress', ARGV[1], 'message', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
redis.call('PUBLISH', KEYS[2], ARGV[1])
return 1
```

```python
# Im Task:
async def run(self, *args, **kwargs):
    for i, chunk in enumerate(data_chunks):
        await self.update_progress(
            percent=int((i + 1) / len(data_chunks) * 100),
            message=f"Verarbeite Chunk {i+1}/{len(data_chunks)}"
        )
        await process_chunk(chunk)
```

---

# 7. Haeufige Fallstricke

## 7.1 Connection Pool Exhaustion

**Symptom:** `ConnectionError("Too many connections")` oder `ConnectionError("No connection available.")`

**Ursachen:**
- Vergessenes `await` — Connection wird nie zurueckgegeben
- Lange blockierende Operationen
- Pool zu klein

**Fix:** `BlockingConnectionPool` + angemessenes `max_connections` + Monitoring

## 7.2 Blockierende Befehle im async Kontext

`BLPOP`, `BRPOP`, `SUBSCRIBE` blockieren eine Pool-Connection. Fuer SwarmQ nicht relevant (RabbitMQ ist Broker). Pub/Sub nutzt automatisch dedizierte Connection.

## 7.3 Key-Expiration Race Condition

```python
# SCHLECHT: Race Condition
if await client.exists(f"swarmq:result:{task_id}"):
    result = await client.get(f"swarmq:result:{task_id}")  # koennte None sein!

# GUT: Direkt GET
result = await client.get(f"swarmq:result:{task_id}")
if result is not None:
    return deserialize(result)
```

## 7.4 Memory-Bloat

- Immer TTL setzen
- `INFO memory` regelmaessig pruefen
- `volatile-lru` als Sicherheitsnetz

## 7.5 Pub/Sub Einschraenkungen

- Dedizierte Connection pro `pubsub()`
- Keine Zustellungsgarantie
- Bei vielen Result-Abfragen: viele offene Subscriptions
- **Loesung:** Pub/Sub nur als Optimierung, GET als Fallback

## 7.6 Grosse Lua-Scripts

- Valkey ist Single-Threaded fuer Lua
- Blockiert ALLE anderen Clients
- Default-Timeout: 5s (`lua-time-limit`)
- SwarmQ-Scripts sind kurz (2-3 Befehle) — kein Problem

---

# 8. Valkey-spezifische Features

## 8.1 Features gegenueber Redis

| Feature | Status | Relevanz fuer SwarmQ |
|---|---|---|
| I/O Multithreading (verbessert) | Stabil (v8.0) | Hoeherer Throughput |
| Verbesserte Dictionary-Struktur | Stabil | ~20% weniger Memory |
| RDMA Support | Experimentell | Nicht relevant |
| Per-Slot-Metriken | Stabil | Cluster-Monitoring |

## 8.2 Performance vs Redis

Valkey 8.0 Benchmarks:
- **~3× Throughput** gegenueber Redis 7.2 bei Multi-Core
- **Bis zu 1.19 Millionen Requests/Sekunde** mit I/O Threading
- **~20% weniger Memory** durch optimierte Datenstrukturen

## 8.3 Kompatibilitaetsgarantien

- Wire-protocol-kompatibel mit Redis
- Alle Redis-Clients funktionieren mit Valkey
- `redis.call()` in Lua funktioniert identisch
- URL-Schemas `redis://` und `valkey://` beide unterstuetzt

---

# 9. Python 3.14 Kompatibilitaet

## 9.1 Bekannte Aenderungen

| Aenderung | Auswirkung auf valkey-py |
|---|---|
| `asyncio.get_event_loop()` wirft RuntimeError | valkey-py nutzt `asyncio.get_running_loop()` — **kein Problem** |
| Verschaerftes nest_asyncio | Nicht relevant fuer rein-async Code |
| Free-Threaded Build (PEP 703) | Positiv: bessere Parallelisierung moeglich |

## 9.2 Quellcode-Analyse

valkey-py nutzt korrekte Patterns:
```python
# Aus asyncio/connection.py
if sys.version_info >= (3, 11, 3):
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout
```

Verwendet `asyncio.get_running_loop()`, `asyncio.StreamReader`/`StreamWriter` — alles stabil in 3.14.

**Ergebnis:** valkey-py 6.1.1 sollte mit Python 3.14 **problemlos funktionieren**.

---

# 10. Zusammenfassung und Empfehlungen

| Entscheidung | Empfehlung | Begruendung |
|---|---|---|
| **Client-Library** | `valkey[libvalkey]>=6.0` | Fork von redis-py, aktiv gepflegt, libvalkey fuer Performance |
| **Connection Pool** | `BlockingConnectionPool(max_connections=20, timeout=10)` | Verhindert Pool-Exhaustion |
| **Result Storage** | `SET key value PX ttl_ms` (einzelner String) | Einfach, schnell, TTL in einem Befehl |
| **Serialisierung** | msgpack als Bytes in SET | Kompakt, binaer, konsistent mit Aufgabe |
| **Result Notification** | Lua: `SET + PUBLISH` atomar | Hybrid: Pub/Sub fuer Speed, GET als Fallback |
| **Result Waiting** | Pub/Sub + Polling Hybrid | Zuverlaessig und schnell |
| **Distributed Lock** | Eingebaute `valkey.asyncio.lock.Lock` | Lua-Scripts mit Ownership-Check bereits enthalten |
| **Heartbeat** | `lock.reacquire()` alle 10s | Setzt TTL zurueck, prueft Ownership |
| **TTL Default** | 24h Results, 30s Lock | Sinnvolle Defaults, konfigurierbar |
| **RESP Version** | RESP2 (Default) | RESP3 optional fuer Client-Side-Caching |
| **Persistenz** | Keine / RDB empfehlen | Results sind ephemeral |
| **maxmemory-policy** | `volatile-lru` | Evictiert nur TTL-Keys, schuetzt Locks |

### Wichtigste Erkenntnisse

1. **valkey-py's eingebaute Lock-Klasse** hat bereits alle Lua-Scripts mit Ownership-Check — kein eigener Lock-Code noetig fuer den Scheduler
2. **BlockingConnectionPool** statt Default-Pool — verhindert ungebremste Connection-Erzeugung
3. **Pub/Sub + GET Hybrid** fuer Result-Waiting — zuverlaessiger als reines Pub/Sub, schneller als reines Polling
4. **libvalkey Parser** immer installieren — automatischer Performance-Gewinn
5. **Separate Pools** fuer Result-Backend und Lock/Scheduler — Isolation verhindert gegenseitige Blockade
6. **`thread_local=False`** bei Lock-Erstellung — sonst funktioniert der Lock nicht korrekt im async Kontext
