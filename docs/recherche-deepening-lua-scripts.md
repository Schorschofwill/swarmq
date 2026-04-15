# Recherche Deepening: Vollstaendige Lua-Skripte und Valkey-Patterns fuer SwarmQ

**Erstellt:** 2026-04-15
**Zweck:** Produktionsreife Lua-Skripte und Python-Caller-Code fuer ALLE Valkey-Operationen in SwarmQ. Dieses Dokument ist die definitive Implementierungsreferenz.

---

## Inhaltsverzeichnis

1. [Result Storage Scripts](#1-result-storage-scripts)
2. [Scheduler Lock Scripts](#2-scheduler-lock-scripts)
3. [Rate Limiting Scripts](#3-rate-limiting-scripts)
4. [Task Locking Scripts](#4-task-locking-scripts)
5. [Deduplication Scripts](#5-deduplication-scripts)
6. [Task Cancellation Scripts](#6-task-cancellation-scripts)
7. [Workflow Chord Completion Scripts](#7-workflow-chord-completion-scripts)
8. [Schedule State Scripts](#8-schedule-state-scripts)
9. [Worker Registration Scripts](#9-worker-registration-scripts)
10. [Vollstaendige Python-Integration: ValkeyScripts Klasse](#10-vollstaendige-python-integration)

---

# 1. Result Storage Scripts

## 1.1 Store Result mit TTL + Pub/Sub Notification (atomar)

### Lua-Skript

```lua
-- store_result.lua
-- Speichert ein Task-Ergebnis mit TTL und benachrichtigt Subscriber atomar.
--
-- KEYS[1] = Result-Key (z.B. "sq:result:{task_id}")
-- KEYS[2] = Notification-Channel (z.B. "sq:notify:{task_id}")
-- ARGV[1] = Serialisiertes Ergebnis (bytes)
-- ARGV[2] = TTL in Millisekunden
-- ARGV[3] = Notification-Payload (z.B. Task-Status JSON)
--
-- Rueckgabe: 1 = erfolgreich gespeichert
--
-- Atomaritaet: Alle drei Operationen (SET, PEXPIRE, PUBLISH) laufen
-- in einer einzigen Lua-Ausfuehrung — kein Zwischenzustand sichtbar.
--
-- Edge Cases:
--   - Wenn ARGV[2] <= 0: kein TTL gesetzt (Key lebt ewig — NICHT empfohlen)
--   - Wenn Channel keine Subscriber hat: PUBLISH gibt 0 zurueck, kein Fehler
--   - Wenn Key bereits existiert: wird ueberschrieben (idempotent)
--   - Maximale Value-Groesse: 512 MB (Valkey-Limit)

local result_key = KEYS[1]
local channel = KEYS[2]
local result_data = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local notification = ARGV[3]

-- Ergebnis speichern
redis.call('SET', result_key, result_data)

-- TTL setzen (nur wenn > 0)
if ttl_ms > 0 then
    redis.call('PEXPIRE', result_key, ttl_ms)
end

-- Subscriber benachrichtigen
-- PUBLISH gibt die Anzahl der Subscriber zurueck, die die Nachricht erhalten haben.
-- Bei 0 Subscribern geht die Nachricht verloren — das ist beabsichtigt.
-- Der Client muss ZUERST subscriben und DANN das Ergebnis per GET pruefen,
-- um die Race-Condition "Ergebnis vor Subscribe gespeichert" abzufangen.
redis.call('PUBLISH', channel, notification)

return 1
```

### Python-Caller

```python
import valkey.asyncio as valkey
from typing import Optional
import json
import time


class ResultStorage:
    """Verwaltet Task-Ergebnisse in Valkey."""

    # Lua-Skript: wird einmalig bei __init__ registriert
    _STORE_RESULT_LUA = """
local result_key = KEYS[1]
local channel = KEYS[2]
local result_data = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local notification = ARGV[3]

redis.call('SET', result_key, result_data)

if ttl_ms > 0 then
    redis.call('PEXPIRE', result_key, ttl_ms)
end

redis.call('PUBLISH', channel, notification)

return 1
"""

    def __init__(self, client: valkey.Valkey, key_prefix: str = "sq"):
        self._client = client
        self._prefix = key_prefix
        self._store_result_script = client.register_script(self._STORE_RESULT_LUA)

    async def store_result(
        self,
        task_id: str,
        result: bytes,
        ttl_ms: int,
        status: str = "SUCCESS",
    ) -> None:
        """Speichert ein Task-Ergebnis atomar mit TTL und Pub/Sub-Benachrichtigung.

        Args:
            task_id: Eindeutige Task-ID.
            result: Serialisiertes Ergebnis (z.B. msgpack/JSON bytes).
            ttl_ms: Time-to-Live in Millisekunden. 0 = kein TTL (nicht empfohlen).
            status: Task-Status fuer die Notification (z.B. "SUCCESS", "FAILURE").

        Raises:
            valkey.ConnectionError: Bei Verbindungsproblemen.
            valkey.TimeoutError: Bei Timeout.
        """
        result_key = f"{self._prefix}:result:{task_id}"
        channel = f"{self._prefix}:notify:{task_id}"
        notification = json.dumps({
            "task_id": task_id,
            "status": status,
            "timestamp": time.time(),
        })

        await self._store_result_script(
            keys=[result_key, channel],
            args=[result, ttl_ms, notification],
        )
```

### Edge Cases und Fehlerbehandlung

```
1. Race-Condition bei Pub/Sub:
   Der Client muss ZUERST subscriben, DANN per GET pruefen ob das Ergebnis
   bereits vorliegt. Andernfalls kann das Ergebnis zwischen GET und SUBSCRIBE
   gespeichert werden, und die PUBLISH-Nachricht geht verloren.

   Korrekte Reihenfolge im Client:
   a) SUBSCRIBE sq:notify:{task_id}
   b) GET sq:result:{task_id}
   c) Falls Ergebnis vorhanden -> fertig, UNSUBSCRIBE
   d) Falls nicht -> auf PUBLISH warten

2. Sehr grosse Ergebnisse (> 1 MB):
   Valkey unterstuetzt bis 512 MB pro Value, aber grosse Values erhoehen
   die Latenz fuer ALLE Operationen auf dem Server. Empfehlung:
   - Ergebnisse > 1 MB in externen Speicher (S3, Dateisystem) ablegen
   - In Valkey nur eine Referenz speichern

3. TTL = 0:
   Key lebt ewig. Kann zu Memory-Leaks fuehren. Sollte in der API
   validiert werden (Minimum z.B. 60_000 ms).

4. Doppeltes Speichern (Idempotenz):
   SET ueberschreibt den bestehenden Wert. TTL wird zurueckgesetzt.
   Das ist beabsichtigt — bei Retries soll das neueste Ergebnis gelten.
```

---

## 1.2 Get Result (drei Varianten)

### Plain GET (Standard-Abruf)

```python
async def get_result(self, task_id: str) -> Optional[bytes]:
    """Holt ein Task-Ergebnis ohne Seiteneffekte.

    Returns:
        Serialisiertes Ergebnis oder None wenn nicht vorhanden/abgelaufen.
    """
    return await self._client.get(f"{self._prefix}:result:{task_id}")
```

### GETEX (Sliding TTL Extension)

```python
async def get_result_extend(
    self, task_id: str, extend_ttl_ms: int
) -> Optional[bytes]:
    """Holt ein Task-Ergebnis und verlaengert den TTL.

    Nuetzlich fuer Ergebnisse, die aktiv genutzt werden und nicht
    ablaufen sollen, solange sie abgefragt werden (Sliding Window TTL).

    GETEX ist seit Redis 6.2 / Valkey 7.0 verfuegbar.

    Args:
        task_id: Eindeutige Task-ID.
        extend_ttl_ms: Neuer TTL in Millisekunden.

    Returns:
        Serialisiertes Ergebnis oder None wenn nicht vorhanden.
    """
    return await self._client.getex(
        f"{self._prefix}:result:{task_id}",
        pxat=None,
        exat=None,
        px=extend_ttl_ms,
    )
```

### GETDEL (One-Time Retrieval)

```python
async def get_result_once(self, task_id: str) -> Optional[bytes]:
    """Holt ein Task-Ergebnis und loescht es sofort.

    Fuer Fire-and-Forget Patterns, wo das Ergebnis nur einmal
    abgeholt wird. Reduziert Speicherverbrauch.

    GETDEL ist seit Redis 6.2 / Valkey 7.0 verfuegbar.

    Returns:
        Serialisiertes Ergebnis oder None wenn nicht vorhanden.
    """
    return await self._client.getdel(f"{self._prefix}:result:{task_id}")
```

### Edge Cases

```
1. GETEX auf nicht-existierenden Key: gibt None zurueck, kein Fehler.
2. GETDEL auf nicht-existierenden Key: gibt None zurueck, kein Fehler.
3. GETEX mit TTL auf Key ohne TTL: setzt den TTL (Key hatte vorher keinen).
4. Concurrent GETDEL: Nur ein Caller erhaelt den Wert, alle anderen None.
   Das ist beabsichtigt fuer One-Time-Retrieval.
```

---

## 1.3 Store Progress Update (atomar, mit Pub/Sub)

### Lua-Skript

```lua
-- store_progress.lua
-- Speichert Fortschritts-Update atomar in einem HASH und benachrichtigt Subscriber.
--
-- KEYS[1] = Progress-Key (z.B. "sq:progress:{task_id}")
-- KEYS[2] = Notification-Channel (z.B. "sq:progress:channel:{task_id}")
-- ARGV[1] = current (Fortschrittswert)
-- ARGV[2] = total (Gesamtwert)
-- ARGV[3] = message (Benutzerdefinierte Statusnachricht)
-- ARGV[4] = TTL in Millisekunden
-- ARGV[5] = updated_at (ISO-Timestamp)
-- ARGV[6] = started_at (ISO-Timestamp, nur beim ersten Update gesetzt)
-- ARGV[7] = eta_seconds (geschaetzte Restzeit, kann "" sein)
--
-- Rueckgabe: 1 = erfolgreich
--
-- Warum HASH statt JSON-String:
--   - Einzelne Felder koennen gelesen werden (HGET) ohne Deserialisierung
--   - Atomare Updates einzelner Felder
--   - Effizienterer Speicher bei strukturierten Daten (Redis ziplist Encoding)
--
-- Edge Cases:
--   - Wenn total = 0: Division-by-Zero wird vermieden (percent = 0)
--   - started_at wird nur gesetzt wenn der Key noch nicht existiert
--   - Ueberschreibt bestehende Werte (idempotent)

local progress_key = KEYS[1]
local channel = KEYS[2]
local current = ARGV[1]
local total = ARGV[2]
local message = ARGV[3]
local ttl_ms = tonumber(ARGV[4])
local updated_at = ARGV[5]
local started_at = ARGV[6]
local eta_seconds = ARGV[7]

-- Percent berechnen (Division-by-Zero-Schutz)
local total_num = tonumber(total)
local current_num = tonumber(current)
local percent = "0.0"
if total_num > 0 then
    percent = string.format("%.1f", (current_num / total_num) * 100)
end

-- started_at nur setzen wenn Key noch nicht existiert
local is_new = redis.call('EXISTS', progress_key) == 0

-- Progress-Daten atomar aktualisieren
redis.call('HSET', progress_key,
    'current', current,
    'total', total,
    'percent', percent,
    'message', message,
    'updated_at', updated_at,
    'eta_seconds', eta_seconds
)

-- started_at nur beim ersten Mal setzen
if is_new then
    redis.call('HSET', progress_key, 'started_at', started_at)
end

-- TTL setzen/erneuern
if ttl_ms > 0 then
    redis.call('PEXPIRE', progress_key, ttl_ms)
end

-- Subscriber benachrichtigen (Notification-Payload = gesamter Progress als JSON)
local notification = string.format(
    '{"current":%s,"total":%s,"percent":%s,"message":"%s","eta_seconds":"%s","updated_at":"%s"}',
    current, total, percent, message, eta_seconds, updated_at
)
redis.call('PUBLISH', channel, notification)

return 1
```

### Python-Caller

```python
class ProgressTracker:
    """Verwaltet Task-Fortschritt in Valkey."""

    _STORE_PROGRESS_LUA = """
local progress_key = KEYS[1]
local channel = KEYS[2]
local current = ARGV[1]
local total = ARGV[2]
local message = ARGV[3]
local ttl_ms = tonumber(ARGV[4])
local updated_at = ARGV[5]
local started_at = ARGV[6]
local eta_seconds = ARGV[7]

local total_num = tonumber(total)
local current_num = tonumber(current)
local percent = "0.0"
if total_num > 0 then
    percent = string.format("%.1f", (current_num / total_num) * 100)
end

local is_new = redis.call('EXISTS', progress_key) == 0

redis.call('HSET', progress_key,
    'current', current,
    'total', total,
    'percent', percent,
    'message', message,
    'updated_at', updated_at,
    'eta_seconds', eta_seconds
)

if is_new then
    redis.call('HSET', progress_key, 'started_at', started_at)
end

if ttl_ms > 0 then
    redis.call('PEXPIRE', progress_key, ttl_ms)
end

local notification = string.format(
    '{"current":%s,"total":%s,"percent":%s,"message":"%s","eta_seconds":"%s","updated_at":"%s"}',
    current, total, percent, message, eta_seconds, updated_at
)
redis.call('PUBLISH', channel, notification)

return 1
"""

    def __init__(self, client: valkey.Valkey, key_prefix: str = "sq"):
        self._client = client
        self._prefix = key_prefix
        self._store_progress_script = client.register_script(
            self._STORE_PROGRESS_LUA
        )

    async def store_progress(
        self,
        task_id: str,
        current: int,
        total: int,
        message: str,
        ttl_ms: int,
        started_at: str,
        eta_seconds: Optional[int] = None,
    ) -> None:
        """Speichert ein Fortschritts-Update atomar mit Pub/Sub-Benachrichtigung.

        Args:
            task_id: Eindeutige Task-ID.
            current: Aktueller Fortschrittswert.
            total: Gesamtwert (z.B. Anzahl Items).
            message: Benutzerdefinierte Statusnachricht.
            ttl_ms: Time-to-Live in Millisekunden.
            started_at: ISO-Timestamp des Task-Starts.
            eta_seconds: Geschaetzte Restzeit in Sekunden (optional).
        """
        from datetime import datetime, timezone

        progress_key = f"{self._prefix}:progress:{task_id}"
        channel = f"{self._prefix}:progress:channel:{task_id}"
        updated_at = datetime.now(timezone.utc).isoformat()

        await self._store_progress_script(
            keys=[progress_key, channel],
            args=[
                current,
                total,
                message,
                ttl_ms,
                updated_at,
                started_at,
                str(eta_seconds) if eta_seconds is not None else "",
            ],
        )
```

### Edge Cases fuer Progress-Updates

```
1. Lua string.format mit Sonderzeichen in message:
   Wenn message Anfuehrungszeichen oder Backslashes enthaelt, wird das JSON
   im PUBLISH ungueltig. Loesung: message vor dem Senden in Python sanitizen
   oder das JSON in Python bauen und als ARGV uebergeben.

   VERBESSERUNG (empfohlen fuer Produktion): Notification-Payload als ARGV
   uebergeben statt in Lua zusammenbauen:

   -- ARGV[8] = vollstaendiger JSON-Notification-Payload (in Python gebaut)
   redis.call('PUBLISH', channel, ARGV[8])

2. Concurrent Progress-Updates:
   Wenn zwei Worker (fehlerhaft) den gleichen Task bearbeiten, wird der
   letzte HSET-Aufruf gewinnen. Das ist korrekt — der aktuellste Stand gilt.

3. total = 0:
   Percent wird auf "0.0" gesetzt. Kein Crash.

4. current > total:
   Percent wird > 100%. Sollte in der Python-Schicht validiert werden.
```

---

## 1.4 Get Progress

```python
async def get_progress(self, task_id: str) -> Optional[dict[str, str]]:
    """Holt den aktuellen Fortschritt eines Tasks.

    Returns:
        Dict mit Progress-Feldern oder None wenn kein Progress vorhanden.
        Felder: current, total, percent, message, updated_at, started_at, eta_seconds
    """
    progress_key = f"{self._prefix}:progress:{task_id}"
    data = await self._client.hgetall(progress_key)
    if not data:
        return None
    return data
```

---

# 2. Scheduler Lock Scripts

Der Scheduler-Lock schuetzt die periodische Task-Planung: Nur ein Worker/Scheduler-Prozess darf gleichzeitig den Scheduler ausfuehren.

## 2.1 Lock Acquire (SET NX PX)

Kein Lua-Skript noetig — `SET NX PX` ist bereits atomar:

```python
async def acquire_scheduler_lock(
    self,
    lock_name: str,
    owner_id: str,
    ttl_ms: int,
) -> bool:
    """Versucht den Scheduler-Lock zu akquirieren.

    Verwendet SET NX PX — atomar ohne Lua noetig.

    Args:
        lock_name: Name des Locks (z.B. "scheduler").
        owner_id: Eindeutige ID des Lock-Inhabers (z.B. Worker-ID + PID).
        ttl_ms: Time-to-Live in Millisekunden. Sollte > Scheduler-Intervall sein.

    Returns:
        True wenn Lock akquiriert, False wenn bereits von anderem gehalten.

    Edge Cases:
        - Bei Netzwerk-Partition: Lock laeuft per TTL ab, anderer Worker uebernimmt.
        - owner_id muss pro Prozess eindeutig sein (UUID4 beim Start generieren).
        - Wenn ttl_ms < Scheduler-Intervall: Lock kann zwischen Checks ablaufen!
          Empfehlung: ttl_ms = scheduler_interval_ms * 3
    """
    lock_key = f"{self._prefix}:scheduler:lock:{lock_name}"
    result = await self._client.set(
        lock_key, owner_id, nx=True, px=ttl_ms
    )
    return result is not None  # SET NX gibt None zurueck wenn Key existiert
```

## 2.2 Lock Extend (mit Ownership-Check)

### Lua-Skript

```lua
-- extend_scheduler_lock.lua
-- Verlaengert einen Scheduler-Lock NUR wenn der Caller der aktuelle Owner ist.
--
-- KEYS[1] = Lock-Key (z.B. "sq:scheduler:lock:scheduler")
-- ARGV[1] = owner_id (muss mit gespeichertem Wert uebereinstimmen)
-- ARGV[2] = new_ttl_ms
--
-- Rueckgabe: 1 = verlaengert, 0 = nicht Owner oder Lock abgelaufen
--
-- KRITISCH: Ohne Ownership-Check koennte ein alter Owner den Lock eines
-- neuen Owners verlaengern, nachdem sein eigener Lock per TTL abgelaufen ist.
--
-- Szenario ohne Ownership-Check:
--   t=0:   Worker A akquiriert Lock (TTL=10s)
--   t=12:  Lock abgelaufen, Worker B akquiriert Lock
--   t=13:  Worker A (verzoegert) versucht Lock zu verlaengern -> MUSS fehlschlagen!

local lock_key = KEYS[1]
local expected_owner = ARGV[1]
local new_ttl_ms = tonumber(ARGV[2])

local current_owner = redis.call('GET', lock_key)

if current_owner == expected_owner then
    redis.call('PEXPIRE', lock_key, new_ttl_ms)
    return 1
else
    return 0
end
```

### Python-Caller

```python
_EXTEND_LOCK_LUA = """
local lock_key = KEYS[1]
local expected_owner = ARGV[1]
local new_ttl_ms = tonumber(ARGV[2])

local current_owner = redis.call('GET', lock_key)

if current_owner == expected_owner then
    redis.call('PEXPIRE', lock_key, new_ttl_ms)
    return 1
else
    return 0
end
"""

async def extend_scheduler_lock(
    self,
    lock_name: str,
    owner_id: str,
    ttl_ms: int,
) -> bool:
    """Verlaengert den Scheduler-Lock (nur wenn Owner).

    Muss periodisch aufgerufen werden (Heartbeat), sonst laeuft der Lock ab.
    Empfehlung: Alle ttl_ms / 3 Millisekunden aufrufen.

    Returns:
        True wenn verlaengert, False wenn nicht mehr Owner.
        Bei False MUSS der Scheduler sofort aufhoeren zu arbeiten!
    """
    lock_key = f"{self._prefix}:scheduler:lock:{lock_name}"
    result = await self._extend_lock_script(
        keys=[lock_key],
        args=[owner_id, ttl_ms],
    )
    return result == 1
```

## 2.3 Lock Release (mit Ownership-Check)

### Lua-Skript

```lua
-- release_scheduler_lock.lua
-- Gibt einen Scheduler-Lock frei NUR wenn der Caller der aktuelle Owner ist.
--
-- KEYS[1] = Lock-Key
-- ARGV[1] = owner_id
--
-- Rueckgabe: 1 = freigegeben, 0 = nicht Owner oder Lock bereits abgelaufen
--
-- KRITISCH: Ohne Ownership-Check koennte Worker A den Lock von Worker B loeschen:
--   t=0:   Worker A akquiriert Lock
--   t=10:  Lock laeuft ab
--   t=11:  Worker B akquiriert Lock
--   t=12:  Worker A bemerkt Fehler, ruft release auf -> MUSS Lock von B ignorieren!
--
-- NICHT mit einfachem DEL implementieren! DEL prueft keinen Owner.

local lock_key = KEYS[1]
local expected_owner = ARGV[1]

local current_owner = redis.call('GET', lock_key)

if current_owner == expected_owner then
    redis.call('DEL', lock_key)
    return 1
else
    -- Lock gehoert jemand anderem oder ist bereits abgelaufen.
    -- In beiden Faellen: nichts tun.
    return 0
end
```

### Python-Caller

```python
_RELEASE_LOCK_LUA = """
local lock_key = KEYS[1]
local expected_owner = ARGV[1]

local current_owner = redis.call('GET', lock_key)

if current_owner == expected_owner then
    redis.call('DEL', lock_key)
    return 1
else
    return 0
end
"""

async def release_scheduler_lock(
    self,
    lock_name: str,
    owner_id: str,
) -> bool:
    """Gibt den Scheduler-Lock frei (nur wenn Owner).

    Sollte beim graceful Shutdown aufgerufen werden, damit ein anderer
    Worker den Lock sofort uebernehmen kann statt auf TTL-Ablauf zu warten.

    Returns:
        True wenn freigegeben, False wenn nicht Owner (kein Fehler).
    """
    lock_key = f"{self._prefix}:scheduler:lock:{lock_name}"
    result = await self._release_lock_script(
        keys=[lock_key],
        args=[owner_id],
    )
    return result == 1
```

## 2.4 Lock Status Check

Kein Lua noetig — GET + PTTL in Pipeline:

```python
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True, slots=True)
class LockStatus:
    """Status eines Scheduler-Locks."""
    held: bool
    owner: Optional[str]
    ttl_ms: int  # -2 = Key existiert nicht, -1 = kein TTL, >= 0 = verbleibende ms


async def get_lock_status(self, lock_name: str) -> LockStatus:
    """Prueft den Status eines Scheduler-Locks (nicht-atomar, nur informativ).

    ACHTUNG: Nicht fuer Entscheidungslogik verwenden! Zwischen GET und PTTL
    kann sich der Zustand aendern. Nur fuer Monitoring/Debugging.
    """
    lock_key = f"{self._prefix}:scheduler:lock:{lock_name}"
    async with self._client.pipeline(transaction=False) as pipe:
        pipe.get(lock_key)
        pipe.pttl(lock_key)
        owner, ttl_ms = await pipe.execute()

    return LockStatus(
        held=owner is not None,
        owner=owner.decode() if isinstance(owner, bytes) else owner,
        ttl_ms=ttl_ms,
    )
```

---

# 3. Rate Limiting Scripts

## 3.1 Sliding Window Counter (EMPFOHLEN)

### Lua-Skript

```lua
-- sliding_window_counter.lua
-- Sliding Window Counter fuer globales Rate Limiting.
-- Kombiniert Fixed-Window-Einfachheit mit Sliding-Window-Praezision.
--
-- KEYS[1] = Key fuer aktuelles Fenster (z.B. "sq:rl:send_email:{window_ts}")
-- KEYS[2] = Key fuer vorheriges Fenster (z.B. "sq:rl:send_email:{prev_window_ts}")
-- ARGV[1] = max_requests (Limit pro Fenster)
-- ARGV[2] = window_size_ms (Fenstergroesse in Millisekunden)
-- ARGV[3] = now_ms (Aktueller Timestamp in Millisekunden, Valkey-Server-Zeit empfohlen)
--
-- Rueckgabe: {erlaubt (0/1), verbleibende_requests, retry_after_ms}
--
-- Algorithmus:
--   estimated_count = prev_count * (1 - elapsed_ratio) + current_count
--   Wenn estimated_count >= max_requests: Rate Limited
--
-- Praezision: Approximation mit max ~0.003% Fehlerrate (Cloudflare-Studie).
-- Speicher: O(1) — nur 2 Zaehler pro Task-Typ.
--
-- Clock-Drift: Dieses Skript erwartet now_ms als Argument. Der Caller
-- kann entweder lokale Zeit oder Valkey TIME verwenden.
-- Empfehlung: Valkey TIME im Caller verwenden (siehe Python-Code).
--
-- Edge Cases:
--   - Erster Request: prev_count = 0, estimated = 0 -> erlaubt
--   - Genau an Fenstergrenze: elapsed_ratio = 0, volles Gewicht auf prev_count
--   - window_size_ms = 0: Division by Zero -> Schutz eingebaut
--   - max_requests = 0: Alle Requests abgelehnt (sinnvoll fuer temporaere Sperre)
--   - Concurrent Requests: INCR ist atomar, kein Race-Condition-Problem

local current_key = KEYS[1]
local previous_key = KEYS[2]
local max_requests = tonumber(ARGV[1])
local window_size_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

-- Schutz gegen Division by Zero
if window_size_ms <= 0 then
    return {0, 0, 0}
end

-- Aktuelle Zaehler lesen
local current_count = tonumber(redis.call('GET', current_key) or '0')
local previous_count = tonumber(redis.call('GET', previous_key) or '0')

-- Position im aktuellen Fenster berechnen (0.0 bis 1.0)
local window_start_ms = now_ms - (now_ms % window_size_ms)
local elapsed_ratio = (now_ms - window_start_ms) / window_size_ms

-- Gewichteter Count: vorheriges Fenster * verbleibender Anteil + aktuelles Fenster
local estimated_count = previous_count * (1.0 - elapsed_ratio) + current_count

if estimated_count >= max_requests then
    -- Rate Limited: Berechne Wartezeit
    -- Wartezeit = verbleibende Zeit im aktuellen Fenster
    -- (konservative Schaetzung — nach Fensterrotation sinkt der gewichtete Count)
    local remaining_window_ms = window_size_ms - (now_ms - window_start_ms)
    local retry_after = math.max(1, math.ceil(remaining_window_ms))
    local remaining = math.max(0, math.floor(max_requests - estimated_count))
    return {0, remaining, retry_after}
end

-- Request erlaubt: Zaehler erhoehen
current_count = redis.call('INCR', current_key)

-- TTL setzen: 2x Fenstergroesse (aktuelles Fenster wird im naechsten als "previous" benoetigt)
if current_count == 1 then
    local ttl_ms = window_size_ms * 2
    redis.call('PEXPIRE', current_key, ttl_ms)
end

-- Verbleibende Requests berechnen (nach dem INCR)
local new_estimated = previous_count * (1.0 - elapsed_ratio) + current_count
local remaining = math.max(0, math.floor(max_requests - new_estimated))

return {1, remaining, 0}
```

### Python-Caller

```python
import time


class RateLimiter:
    """Globales Rate Limiting via Sliding Window Counter in Valkey."""

    _SLIDING_WINDOW_LUA = """
local current_key = KEYS[1]
local previous_key = KEYS[2]
local max_requests = tonumber(ARGV[1])
local window_size_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

if window_size_ms <= 0 then
    return {0, 0, 0}
end

local current_count = tonumber(redis.call('GET', current_key) or '0')
local previous_count = tonumber(redis.call('GET', previous_key) or '0')

local window_start_ms = now_ms - (now_ms % window_size_ms)
local elapsed_ratio = (now_ms - window_start_ms) / window_size_ms

local estimated_count = previous_count * (1.0 - elapsed_ratio) + current_count

if estimated_count >= max_requests then
    local remaining_window_ms = window_size_ms - (now_ms - window_start_ms)
    local retry_after = math.max(1, math.ceil(remaining_window_ms))
    local remaining = math.max(0, math.floor(max_requests - estimated_count))
    return {0, remaining, retry_after}
end

current_count = redis.call('INCR', current_key)

if current_count == 1 then
    local ttl_ms = window_size_ms * 2
    redis.call('PEXPIRE', current_key, ttl_ms)
end

local new_estimated = previous_count * (1.0 - elapsed_ratio) + current_count
local remaining = math.max(0, math.floor(max_requests - new_estimated))

return {1, remaining, 0}
"""

    def __init__(self, client: valkey.Valkey, key_prefix: str = "sq"):
        self._client = client
        self._prefix = key_prefix
        self._sliding_window_script = client.register_script(
            self._SLIDING_WINDOW_LUA
        )

    @staticmethod
    def _parse_rate_string(rate_str: str) -> tuple[int, int]:
        """Parst einen Rate-String wie '100/m' in (max_requests, window_ms).

        Unterstuetzte Formate:
          '10/s'    -> (10, 1000)
          '100/m'   -> (100, 60000)
          '1000/h'  -> (1000, 3600000)

        Raises:
            ValueError: Bei ungueltigem Format.
        """
        modifiers = {
            's': 1_000,
            'm': 60_000,
            'h': 3_600_000,
        }
        parts = rate_str.strip().split('/')
        if len(parts) != 2 or parts[1] not in modifiers:
            raise ValueError(
                f"Ungueltiger Rate-String: '{rate_str}'. "
                f"Erwartet: '<n>/<s|m|h>' (z.B. '100/m')"
            )
        try:
            max_requests = int(parts[0])
        except ValueError:
            raise ValueError(f"Ungueltiger Zahlenwert: '{parts[0]}'")

        if max_requests < 0:
            raise ValueError(f"max_requests muss >= 0 sein, war: {max_requests}")

        window_ms = modifiers[parts[1]]
        return max_requests, window_ms

    async def _get_server_time_ms(self) -> int:
        """Holt die aktuelle Serverzeit von Valkey in Millisekunden.

        Eliminiert Clock-Drift-Probleme zwischen Workern.
        """
        t = await self._client.time()
        # time() gibt (seconds, microseconds) zurueck
        return t[0] * 1000 + t[1] // 1000

    async def check_rate_limit(
        self,
        task_name: str,
        max_requests: int,
        window_ms: int,
        *,
        rate_limit_key: Optional[str] = None,
    ) -> tuple[bool, int, int]:
        """Prueft ob ein Request innerhalb des Rate Limits liegt.

        Args:
            task_name: Name des Tasks (fuer Key-Generierung).
            max_requests: Maximale Anzahl Requests pro Fenster.
            window_ms: Fenstergroesse in Millisekunden.
            rate_limit_key: Optionaler benutzerdefinierter Key-Suffix
                (z.B. API-Key fuer Per-Key-Limiting).

        Returns:
            Tuple (allowed, remaining, retry_after_ms):
              - allowed: True wenn erlaubt, False wenn Rate Limited.
              - remaining: Verbleibende Requests im aktuellen Fenster.
              - retry_after_ms: Wartezeit in ms bis zum naechsten erlaubten Request
                (0 wenn allowed=True).
        """
        now_ms = await self._get_server_time_ms()

        # Window-Timestamps berechnen
        window_start_ms = now_ms - (now_ms % window_ms)
        prev_window_start_ms = window_start_ms - window_ms

        # Key-Suffix fuer Per-Key-Limiting
        key_base = f"{self._prefix}:rl:{task_name}"
        if rate_limit_key:
            key_base = f"{key_base}:{rate_limit_key}"

        current_key = f"{key_base}:{window_start_ms}"
        previous_key = f"{key_base}:{prev_window_start_ms}"

        result = await self._sliding_window_script(
            keys=[current_key, previous_key],
            args=[max_requests, window_ms, now_ms],
        )

        allowed = result[0] == 1
        remaining = int(result[1])
        retry_after_ms = int(result[2])

        return allowed, remaining, retry_after_ms

    async def check_rate_limit_str(
        self,
        task_name: str,
        rate_str: str,
        *,
        rate_limit_key: Optional[str] = None,
    ) -> tuple[bool, int, int]:
        """Convenience-Methode mit Rate-String (z.B. '100/m').

        Returns:
            Tuple (allowed, remaining, retry_after_ms).
        """
        max_requests, window_ms = self._parse_rate_string(rate_str)
        return await self.check_rate_limit(
            task_name, max_requests, window_ms,
            rate_limit_key=rate_limit_key,
        )
```

## 3.2 Token Bucket (Alternative)

### Lua-Skript

```lua
-- token_bucket.lua
-- Distributed Token Bucket fuer Rate Limiting mit Burst-Unterstuetzung.
--
-- KEYS[1] = Bucket-Key (z.B. "sq:rl:bucket:send_email")
-- ARGV[1] = max_tokens (Bucket-Kapazitaet)
-- ARGV[2] = refill_rate (Tokens pro Sekunde)
-- ARGV[3] = tokens_requested (normalerweise 1)
-- ARGV[4] = now_us (Timestamp in Mikrosekunden fuer hohe Praezision)
--
-- Rueckgabe: {erlaubt (0/1), verbleibende_tokens_str, retry_after_ms}
--
-- Vorteile gegenueber Sliding Window Counter:
--   - Erlaubt kontrollierte Bursts (bis zur Bucket-Kapazitaet)
--   - Glaettet Traffic ueber Zeit (gleichmaessige Rate nach Burst)
--
-- Nachteile:
--   - 2 HMGET/HMSET Operationen statt 1 GET/INCR
--   - Etwas komplexer zu debuggen
--
-- Edge Cases:
--   - Erster Request: Bucket wird voll initialisiert (Burst moeglich)
--   - Lange Pause (> Bucket-Fuellzeit): Tokens maximal auf max_tokens
--   - Concurrent Requests: HMGET + HMSET ist atomar im Lua-Kontext
--   - refill_rate = 0: Bucket fuellt sich nie auf (einmalige Tokens)
--   - tokens_requested > max_tokens: Immer abgelehnt

local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now_us = tonumber(ARGV[4])

-- Tokens > Kapazitaet kann nie erfuellt werden
if requested > max_tokens then
    return {0, "0", math.ceil((requested / refill_rate) * 1000)}
end

local data = redis.call('HMGET', key, 'tokens', 'last_refill_us')
local tokens = tonumber(data[1])
local last_refill_us = tonumber(data[2])

if tokens == nil then
    -- Erster Zugriff: Bucket voll initialisieren
    tokens = max_tokens
    last_refill_us = now_us
end

-- Tokens auffuellen basierend auf vergangener Zeit
if refill_rate > 0 and now_us > last_refill_us then
    local elapsed_s = (now_us - last_refill_us) / 1000000.0
    local new_tokens = elapsed_s * refill_rate
    tokens = math.min(max_tokens, tokens + new_tokens)
    last_refill_us = now_us
end

local allowed = 0
local retry_after_ms = 0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    -- Berechne Wartezeit bis genug Tokens verfuegbar
    if refill_rate > 0 then
        retry_after_ms = math.ceil(((requested - tokens) / refill_rate) * 1000)
    else
        retry_after_ms = -1  -- Nie moeglich (refill_rate = 0)
    end
end

-- Zustand speichern
redis.call('HMSET', key,
    'tokens', string.format("%.6f", tokens),
    'last_refill_us', tostring(last_refill_us)
)

-- TTL: doppelte Zeit bis Bucket voll, mindestens 60s
local ttl_s
if refill_rate > 0 then
    ttl_s = math.max(60, math.ceil((max_tokens / refill_rate) * 2))
else
    ttl_s = 3600  -- 1 Stunde fuer statische Buckets
end
redis.call('EXPIRE', key, ttl_s)

return {allowed, string.format("%.2f", tokens), retry_after_ms}
```

### Python-Caller

```python
async def check_rate_limit_token_bucket(
    self,
    task_name: str,
    max_tokens: int,
    refill_rate: float,
    tokens_requested: int = 1,
) -> tuple[bool, float, int]:
    """Prueft Rate Limit mit Token Bucket Algorithmus.

    Args:
        task_name: Name des Tasks.
        max_tokens: Maximale Bucket-Kapazitaet (Burst-Limit).
        refill_rate: Tokens pro Sekunde (Langzeit-Rate).
        tokens_requested: Benoetigte Tokens (Standard: 1).

    Returns:
        Tuple (allowed, remaining_tokens, retry_after_ms).
    """
    key = f"{self._prefix}:rl:bucket:{task_name}"

    # Server-Zeit in Mikrosekunden fuer hohe Praezision
    t = await self._client.time()
    now_us = t[0] * 1_000_000 + t[1]

    result = await self._token_bucket_script(
        keys=[key],
        args=[max_tokens, refill_rate, tokens_requested, now_us],
    )

    allowed = result[0] == 1
    remaining_tokens = float(result[1])
    retry_after_ms = int(result[2])

    return allowed, remaining_tokens, retry_after_ms
```

---

# 4. Task Locking Scripts

## 4.1 Task Lock Acquire (Mutex)

### Lua-Skript

```lua
-- task_lock_acquire.lua
-- Akquiriert einen Task-Lock (Mutex) mit Owner-Tracking und Reentranz.
--
-- KEYS[1] = Lock-Key (z.B. "sq:lock:send_newsletter")
-- ARGV[1] = owner_id (z.B. Task-ID oder Worker-ID)
-- ARGV[2] = ttl_ms
--
-- Rueckgabe: 1 = akquiriert, 0 = Lock belegt (von anderem Owner)
--
-- Reentranz: Wenn der gleiche Owner den Lock bereits haelt, wird der TTL
-- erneuert und 1 zurueckgegeben. Das ist wichtig fuer Retry-Szenarien,
-- wo der gleiche Task (gleiche ID) den Lock neu akquirieren will.
--
-- Edge Cases:
--   - Lock abgelaufen: Key existiert nicht -> wird normal akquiriert
--   - Gleicher Owner: TTL wird erneuert (Reentranz)
--   - Anderer Owner: 0 zurueck, kein Seiteneffekt
--   - ttl_ms <= 0: SET ohne PX -> Lock ohne Ablauf (GEFAEHRLICH, vermeiden!)

local key = KEYS[1]
local owner = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

local current_owner = redis.call('GET', key)

if current_owner == false then
    -- Lock frei -> akquirieren
    if ttl_ms > 0 then
        redis.call('SET', key, owner, 'PX', ttl_ms)
    else
        redis.call('SET', key, owner)
    end
    return 1
elseif current_owner == owner then
    -- Reentrant: gleicher Owner, TTL erneuern
    if ttl_ms > 0 then
        redis.call('PEXPIRE', key, ttl_ms)
    end
    return 1
else
    -- Lock von anderem Owner gehalten
    return 0
end
```

### Python-Caller

```python
_TASK_LOCK_ACQUIRE_LUA = """
local key = KEYS[1]
local owner = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

local current_owner = redis.call('GET', key)

if current_owner == false then
    if ttl_ms > 0 then
        redis.call('SET', key, owner, 'PX', ttl_ms)
    else
        redis.call('SET', key, owner)
    end
    return 1
elseif current_owner == owner then
    if ttl_ms > 0 then
        redis.call('PEXPIRE', key, ttl_ms)
    end
    return 1
else
    return 0
end
"""

async def acquire_task_lock(
    self,
    lock_name: str,
    owner_id: str,
    ttl_ms: int,
) -> bool:
    """Akquiriert einen Task-Lock (Mutex).

    Args:
        lock_name: Name des Locks. Kann statisch ("send_newsletter")
            oder dynamisch ("sync_user:42") sein.
        owner_id: Eindeutige ID des Lock-Inhabers (Task-ID empfohlen).
        ttl_ms: Time-to-Live in Millisekunden.
            Empfehlung: task_timeout * 1.5, mindestens 60_000.

    Returns:
        True wenn akquiriert (oder reentrant erneuert), False wenn belegt.
    """
    lock_key = f"{self._prefix}:lock:{lock_name}"
    result = await self._task_lock_acquire_script(
        keys=[lock_key],
        args=[owner_id, ttl_ms],
    )
    return result == 1
```

## 4.2 Task Lock Release (mit Ownership-Check)

### Lua-Skript

```lua
-- task_lock_release.lua
-- Gibt einen Task-Lock frei, NUR wenn der Caller der Owner ist.
--
-- KEYS[1] = Lock-Key
-- ARGV[1] = owner_id
--
-- Rueckgabe: 1 = freigegeben, 0 = nicht Owner oder bereits abgelaufen
--
-- KRITISCH: Ohne Ownership-Check kann folgendes passieren:
--   1. Worker A akquiriert Lock, beginnt Task
--   2. Lock laeuft ab (Task dauert zu lange)
--   3. Worker B akquiriert Lock
--   4. Worker A beendet Task, ruft DEL auf -> Lock von Worker B geloescht!
--
-- Mit Ownership-Check: Schritt 4 gibt 0 zurueck, Lock bleibt erhalten.

local key = KEYS[1]
local expected_owner = ARGV[1]

local current_owner = redis.call('GET', key)

if current_owner == expected_owner then
    redis.call('DEL', key)
    return 1
else
    return 0
end
```

### Python-Caller

```python
_TASK_LOCK_RELEASE_LUA = """
local key = KEYS[1]
local expected_owner = ARGV[1]

local current_owner = redis.call('GET', key)

if current_owner == expected_owner then
    redis.call('DEL', key)
    return 1
else
    return 0
end
"""

async def release_task_lock(
    self,
    lock_name: str,
    owner_id: str,
) -> bool:
    """Gibt einen Task-Lock frei (nur wenn Owner).

    MUSS im finally-Block aufgerufen werden, damit der Lock bei Erfolg
    UND Fehler freigegeben wird.

    Returns:
        True wenn freigegeben, False wenn nicht Owner.
        False ist KEIN Fehler — bedeutet, der Lock wurde bereits
        von jemand anderem uebernommen (TTL abgelaufen).
    """
    lock_key = f"{self._prefix}:lock:{lock_name}"
    result = await self._task_lock_release_script(
        keys=[lock_key],
        args=[owner_id],
    )
    return result == 1
```

## 4.3 Semaphore Acquire (max_concurrency > 1)

### Lua-Skript

```lua
-- semaphore_acquire.lua
-- Akquiriert einen Platz in einem verteilten Semaphore.
-- Verwendet Sorted Set mit score = Ablauf-Timestamp.
--
-- KEYS[1] = Semaphore-Key (z.B. "sq:sem:send_email")
-- ARGV[1] = max_concurrency
-- ARGV[2] = owner_id (Task-ID)
-- ARGV[3] = ttl_ms
-- ARGV[4] = now_ms (Timestamp in Millisekunden)
--
-- Rueckgabe: {status, current_count}
--   status: 1 = akquiriert, 0 = max erreicht
--   current_count: Aktuelle Anzahl gehaltener Slots
--
-- Algorithmus:
--   1. Abgelaufene Eintraege entfernen (Score < now_ms)
--   2. Aktuelle Anzahl pruefen
--   3. Falls < max: Owner mit Ablauf-Timestamp als Score hinzufuegen
--
-- Vorteile gegenueber einfachem INCR/DECR-Counter:
--   - Automatische Bereinigung bei Worker-Crash (abgelaufene Eintraege werden entfernt)
--   - Owner-Tracking (wer haelt welchen Slot)
--   - Kein Zaehlerfehler moeglich (DECR vergessen = Deadlock bei Counter)
--
-- Edge Cases:
--   - Owner haelt bereits einen Slot: Score wird aktualisiert (TTL erneuert)
--     ZADD mit gleichem Member ueberschreibt den Score
--   - Alle Slots abgelaufen: ZREMRANGEBYSCORE raeut auf -> Slots frei
--   - max_concurrency = 1: Funktioniert als Mutex (aber mit Sorted Set)
--     Fuer echten Mutex den einfacheren task_lock_acquire.lua verwenden
--   - max_concurrency = 0: Kein Slot wird je vergeben (Sperre)

local key = KEYS[1]
local max_conc = tonumber(ARGV[1])
local owner = ARGV[2]
local ttl_ms = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

-- Abgelaufene Eintraege entfernen
redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms)

-- Pruefen ob Owner bereits einen Slot haelt
local existing_score = redis.call('ZSCORE', key, owner)
if existing_score then
    -- Owner haelt bereits einen Slot -> TTL erneuern
    local expire_at = now_ms + ttl_ms
    redis.call('ZADD', key, expire_at, owner)
    redis.call('PEXPIRE', key, ttl_ms + 1000)  -- Key-TTL etwas laenger als Slot-TTL
    local current = redis.call('ZCARD', key)
    return {1, current}
end

-- Aktuelle Anzahl pruefen
local current = redis.call('ZCARD', key)

if current >= max_conc then
    -- Max Concurrency erreicht
    return {0, current}
end

-- Slot akquirieren: Score = Ablauf-Timestamp
local expire_at = now_ms + ttl_ms
redis.call('ZADD', key, expire_at, owner)

-- Key-TTL setzen (etwas laenger als der laengste Slot-TTL)
redis.call('PEXPIRE', key, ttl_ms + 1000)

return {1, current + 1}
```

### Python-Caller

```python
_SEMAPHORE_ACQUIRE_LUA = """
local key = KEYS[1]
local max_conc = tonumber(ARGV[1])
local owner = ARGV[2]
local ttl_ms = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms)

local existing_score = redis.call('ZSCORE', key, owner)
if existing_score then
    local expire_at = now_ms + ttl_ms
    redis.call('ZADD', key, expire_at, owner)
    redis.call('PEXPIRE', key, ttl_ms + 1000)
    local current = redis.call('ZCARD', key)
    return {1, current}
end

local current = redis.call('ZCARD', key)

if current >= max_conc then
    return {0, current}
end

local expire_at = now_ms + ttl_ms
redis.call('ZADD', key, expire_at, owner)
redis.call('PEXPIRE', key, ttl_ms + 1000)

return {1, current + 1}
"""

async def acquire_semaphore(
    self,
    task_name: str,
    owner_id: str,
    max_concurrency: int,
    ttl_ms: int,
) -> tuple[bool, int]:
    """Akquiriert einen Slot im verteilten Semaphore.

    Args:
        task_name: Name des Tasks (fuer Key-Generierung).
        owner_id: Eindeutige Task-ID.
        max_concurrency: Maximale Anzahl paralleler Ausfuehrungen.
        ttl_ms: Time-to-Live fuer den Slot in Millisekunden.
            Bei Worker-Crash wird der Slot nach TTL automatisch freigegeben.

    Returns:
        Tuple (acquired, current_count):
          - acquired: True wenn Slot akquiriert.
          - current_count: Aktuelle Anzahl gehaltener Slots.
    """
    key = f"{self._prefix}:sem:{task_name}"
    t = await self._client.time()
    now_ms = t[0] * 1000 + t[1] // 1000

    result = await self._semaphore_acquire_script(
        keys=[key],
        args=[max_concurrency, owner_id, ttl_ms, now_ms],
    )

    acquired = result[0] == 1
    current_count = int(result[1])
    return acquired, current_count
```

## 4.4 Semaphore Release

### Lua-Skript

```lua
-- semaphore_release.lua
-- Gibt einen Slot im verteilten Semaphore frei.
--
-- KEYS[1] = Semaphore-Key
-- ARGV[1] = owner_id
--
-- Rueckgabe: 1 = freigegeben, 0 = Owner hatte keinen Slot
--
-- ZREM ist idempotent: Wenn der Owner keinen Slot haelt (z.B. bereits
-- abgelaufen und per ZREMRANGEBYSCORE entfernt), gibt ZREM 0 zurueck.
-- Das ist kein Fehler.

local key = KEYS[1]
local owner = ARGV[1]

local removed = redis.call('ZREM', key, owner)
return removed
```

### Python-Caller

```python
_SEMAPHORE_RELEASE_LUA = """
local key = KEYS[1]
local owner = ARGV[1]

local removed = redis.call('ZREM', key, owner)
return removed
"""

async def release_semaphore(
    self,
    task_name: str,
    owner_id: str,
) -> bool:
    """Gibt einen Slot im verteilten Semaphore frei.

    MUSS im finally-Block aufgerufen werden.

    Returns:
        True wenn freigegeben, False wenn Owner keinen Slot hatte
        (z.B. bereits per TTL abgelaufen).
    """
    key = f"{self._prefix}:sem:{task_name}"
    result = await self._semaphore_release_script(
        keys=[key],
        args=[owner_id],
    )
    return result == 1
```

---

# 5. Deduplication Scripts

## 5.1 Unique Task Check + Acquire (atomar)

### Lua-Skript

```lua
-- unique_task_acquire.lua
-- Prueft ob ein Task mit diesem Unique-Key bereits existiert.
-- Falls nein: setzt den Unique-Lock atomar.
--
-- KEYS[1] = Unique-Key (z.B. "sq:unique:send_email:a3f8b2c1")
-- ARGV[1] = task_id (ID des neuen Tasks)
-- ARGV[2] = unique_for_ms (Deduplizierungs-Fenster in ms)
--
-- Rueckgabe: {status, task_id}
--   status: 1 = einzigartig (Lock akquiriert), 0 = Duplikat
--   task_id: Bei status=1: neue task_id. Bei status=0: bestehende task_id.
--
-- Atomaritaet: GET + SET NX in einem Lua-Aufruf. Kein Fenster fuer
-- Race-Conditions zwischen Check und Acquire.
--
-- Edge Cases:
--   - Key existiert aber ist abgelaufen: GET gibt nil -> kein Duplikat
--   - unique_for_ms = 0: Key wird ohne TTL gesetzt (NICHT empfohlen)
--   - Zwei identische Tasks gleichzeitig: Einer gewinnt, anderer ist Duplikat
--     (Lua ist single-threaded in Valkey)
--   - task_id identisch mit bestehendem: trotzdem als Duplikat behandelt
--     (der bestehende Task laeuft bereits)

local key = KEYS[1]
local task_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

-- Pruefen ob bereits ein Task mit diesem Key existiert
local existing = redis.call('GET', key)

if existing then
    -- Duplikat: bestehende Task-ID zurueckgeben
    return {0, existing}
end

-- Unique-Lock setzen
if ttl_ms > 0 then
    redis.call('SET', key, task_id, 'PX', ttl_ms)
else
    redis.call('SET', key, task_id)
end

return {1, task_id}
```

### Python-Caller

```python
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class UniqueCheckResult:
    """Ergebnis einer Unique-Task-Pruefung."""
    is_unique: bool
    task_id: str
    existing_task_id: Optional[str] = None


_UNIQUE_ACQUIRE_LUA = """
local key = KEYS[1]
local task_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

local existing = redis.call('GET', key)

if existing then
    return {0, existing}
end

if ttl_ms > 0 then
    redis.call('SET', key, task_id, 'PX', ttl_ms)
else
    redis.call('SET', key, task_id)
end

return {1, task_id}
"""

async def check_unique_task(
    self,
    unique_key: str,
    task_id: str,
    unique_for_ms: int,
) -> UniqueCheckResult:
    """Prueft atomar ob ein Task einzigartig ist und akquiriert den Lock.

    Args:
        unique_key: Voller Unique-Key (z.B. "sq:unique:send_email:a3f8b2c1").
        task_id: ID des neuen Tasks.
        unique_for_ms: Deduplizierungs-Fenster in Millisekunden.

    Returns:
        UniqueCheckResult mit is_unique Flag und Task-IDs.
    """
    result = await self._unique_acquire_script(
        keys=[unique_key],
        args=[task_id, unique_for_ms],
    )

    status = result[0]
    returned_id = result[1].decode() if isinstance(result[1], bytes) else result[1]

    if status == 1:
        return UniqueCheckResult(is_unique=True, task_id=returned_id)
    else:
        return UniqueCheckResult(
            is_unique=False,
            task_id=task_id,
            existing_task_id=returned_id,
        )
```

## 5.2 Unique Task Release

### Lua-Skript

```lua
-- unique_task_release.lua
-- Gibt einen Unique-Lock frei, NUR wenn die task_id uebereinstimmt.
--
-- KEYS[1] = Unique-Key
-- ARGV[1] = task_id (nur eigenen Lock freigeben)
--
-- Rueckgabe: 1 = freigegeben, 0 = nicht eigener Lock oder abgelaufen
--
-- Wird aufgerufen basierend auf unique_until Strategie:
--   "start":   Bei Task-Start (Worker ruft auf bevor task.run())
--   "success": Bei erfolgreichem Abschluss (Worker ruft auf nach task.run())
--
-- Bei unique_until="success" und Task-Fehler: Lock wird NICHT freigegeben.
-- Der Lock laeuft per TTL ab, was korrekt ist — waehrend der TTL-Dauer
-- soll kein Duplikat eingereiht werden koennen.
--
-- Ownership-Check verhindert: Task A laeuft ab (TTL), Task B akquiriert Lock,
-- Task A (verspaetet) gibt Lock von Task B frei.

local key = KEYS[1]
local task_id = ARGV[1]

local current = redis.call('GET', key)

if current == task_id then
    redis.call('DEL', key)
    return 1
end

return 0
```

### Python-Caller

```python
_UNIQUE_RELEASE_LUA = """
local key = KEYS[1]
local task_id = ARGV[1]

local current = redis.call('GET', key)

if current == task_id then
    redis.call('DEL', key)
    return 1
end

return 0
"""

async def release_unique_task(
    self,
    unique_key: str,
    task_id: str,
) -> bool:
    """Gibt einen Unique-Lock frei (nur wenn Owner).

    Wann aufrufen (basierend auf unique_until):
      - "start": Direkt bevor der Task ausgefuehrt wird.
      - "success": Nach erfolgreichem Abschluss des Tasks.
      - Nicht aufrufen bei Fehler + unique_until="success"!

    Returns:
        True wenn freigegeben, False wenn nicht Owner.
    """
    result = await self._unique_release_script(
        keys=[unique_key],
        args=[task_id],
    )
    return result == 1
```

## 5.3 Unique Key Generation (Python)

```python
import hashlib
import json
from typing import Any


def generate_unique_key(
    task_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    key_prefix: str = "sq",
    custom_key_func: Optional[callable] = None,
) -> str:
    """Generiert einen stabilen Unique-Key fuer Task-Deduplizierung.

    Der Key ist deterministisch: gleiche Eingaben erzeugen IMMER den gleichen Key,
    auch ueber Python-Neustarts hinweg.

    Args:
        task_name: Vollqualifizierter Task-Name (z.B. "myapp.tasks.send_email").
        args: Positionale Argumente des Tasks.
        kwargs: Keyword-Argumente des Tasks.
        key_prefix: Praefix fuer den Valkey-Key.
        custom_key_func: Optionale benutzerdefinierte Key-Funktion.
            Wird mit (*args, **kwargs) aufgerufen und muss einen String zurueckgeben.

    Returns:
        Voller Valkey-Key (z.B. "sq:unique:send_email:a3f8b2c1d4e5f6a7").

    ACHTUNG zu hash():
        Pythons eingebautes hash() ist NICHT stabil ueber Neustarts!
        PYTHONHASHSEED randomisiert hash() bei jedem Start.
        IMMER hashlib verwenden!

    Edge Cases:
        - Unhashable Argumente (z.B. dict, list): json.dumps mit default=str
          konvertiert alles in Strings. Reihenfolge von Dicts ist stabil
          durch sort_keys=True.
        - Datetime-Objekte: Werden durch default=str in ISO-Format konvertiert.
        - None-Werte: Korrekt als JSON null serialisiert.
        - Sehr grosse Argumente: SHA256 erzeugt immer 64 Hex-Zeichen,
          wir verwenden die ersten 16 fuer den Key (64 Bit Collision-Resistance).
        - Sets und frozensets: Werden durch default=str in String konvertiert.
          WARNUNG: Set-Reihenfolge ist nicht deterministisch!
          Loesung: Sets vor der Key-Generierung sortieren.
        - Bytes-Argumente: Werden durch default=str als "b'...'" serialisiert.
          Fuer binaere Daten besser base64-encoded uebergeben.
    """
    if custom_key_func is not None:
        custom_part = custom_key_func(*args, **kwargs)
        return f"{key_prefix}:unique:{custom_part}"

    args_hash = stable_args_hash(args, kwargs)
    return f"{key_prefix}:unique:{task_name}:{args_hash}"


def stable_args_hash(args: tuple, kwargs: dict) -> str:
    """Erzeugt einen stabilen Hash fuer Task-Argumente.

    Stabil ueber Python-Neustarts hinweg (kein hash() verwenden!).
    Deterministisch: gleiche Argumente = gleicher Hash.

    Args:
        args: Positionale Argumente.
        kwargs: Keyword-Argumente.

    Returns:
        16-Zeichen Hex-String (64 Bit).
    """
    # Sortierung fuer deterministische Serialisierung
    serialized = json.dumps(
        {"a": _normalize_for_hash(args), "k": _normalize_for_hash(kwargs)},
        sort_keys=True,
        ensure_ascii=True,
        separators=(',', ':'),  # Kompakte Darstellung
    )
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()[:16]


def _normalize_for_hash(obj: Any) -> Any:
    """Normalisiert Objekte fuer deterministische JSON-Serialisierung.

    Wandelt problematische Typen (set, frozenset, bytes) in
    deterministische Darstellungen um.
    """
    if isinstance(obj, (set, frozenset)):
        return sorted(str(item) for item in obj)
    elif isinstance(obj, bytes):
        import base64
        return base64.b64encode(obj).decode('ascii')
    elif isinstance(obj, dict):
        return {str(k): _normalize_for_hash(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, (list, tuple)):
        return [_normalize_for_hash(item) for item in obj]
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        # Fallback fuer unbekannte Typen
        return str(obj)
```

---

# 6. Task Cancellation Scripts

## 6.1 Set Cancellation Flag

### Lua-Skript

```lua
-- cancel_task.lua
-- Setzt das Cancellation-Flag fuer einen Task atomar.
-- Speichert die Task-ID im Cancel-Set und publiziert ein Event.
--
-- KEYS[1] = Cancel-Set-Key (z.B. "sq:cancel")
-- KEYS[2] = Notification-Channel (z.B. "sq:events:{task_id}")
-- ARGV[1] = task_id
-- ARGV[2] = ttl_ms (Lebensdauer des Cancel-Flags)
-- ARGV[3] = cancelled_at (ISO-Timestamp)
--
-- Rueckgabe: 1 = Flag gesetzt (neu), 0 = Flag war bereits gesetzt
--
-- Design: Verwendet ein globales SET statt individueller Keys.
-- Vorteil: SISMEMBER ist O(1), und alle Cancel-Flags koennen mit
-- SMEMBERS auf einmal abgefragt werden (fuer Monitoring).
--
-- TTL-Problem: Redis/Valkey SETs unterstuetzen kein Per-Member TTL.
-- Loesung: Separater Key "sq:cancel:ttl:{task_id}" mit TTL,
-- und periodisches Aufraumen im Cancel-Set.
--
-- Alternative (einfacher, hier implementiert):
-- Individueller Key pro Task statt globalem SET.
-- Vorteil: Natuerlicher TTL per Key. Nachteil: Kein SMEMBERS fuer alle.

local cancel_key = KEYS[1]
local channel = KEYS[2]
local task_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local cancelled_at = ARGV[3]

-- Pruefen ob bereits gecancelt
local already_cancelled = redis.call('EXISTS', cancel_key)

-- Cancel-Flag setzen (individueller Key: "sq:cancel:{task_id}")
redis.call('SET', cancel_key, cancelled_at)

if ttl_ms > 0 then
    redis.call('PEXPIRE', cancel_key, ttl_ms)
end

-- Event publizieren
local notification = string.format(
    '{"task_id":"%s","event":"CANCELLING","cancelled_at":"%s"}',
    task_id, cancelled_at
)
redis.call('PUBLISH', channel, notification)

if already_cancelled == 1 then
    return 0  -- War bereits gecancelt
else
    return 1  -- Neu gecancelt
end
```

### Python-Caller

```python
_CANCEL_TASK_LUA = """
local cancel_key = KEYS[1]
local channel = KEYS[2]
local task_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local cancelled_at = ARGV[3]

local already_cancelled = redis.call('EXISTS', cancel_key)

redis.call('SET', cancel_key, cancelled_at)

if ttl_ms > 0 then
    redis.call('PEXPIRE', cancel_key, ttl_ms)
end

local notification = string.format(
    '{"task_id":"%s","event":"CANCELLING","cancelled_at":"%s"}',
    task_id, cancelled_at
)
redis.call('PUBLISH', channel, notification)

if already_cancelled == 1 then
    return 0
else
    return 1
end
"""

async def cancel_task(
    self,
    task_id: str,
    ttl_ms: int = 86_400_000,  # 24 Stunden Standard
) -> bool:
    """Setzt das Cancellation-Flag fuer einen Task.

    Args:
        task_id: ID des abzubrechenden Tasks.
        ttl_ms: Lebensdauer des Cancel-Flags in ms.
            Muss lang genug sein, damit der Worker das Flag sieht.
            Standard: 24 Stunden.

    Returns:
        True wenn Flag neu gesetzt, False wenn bereits gecancelt.
    """
    from datetime import datetime, timezone

    cancel_key = f"{self._prefix}:cancel:{task_id}"
    channel = f"{self._prefix}:events:{task_id}"
    cancelled_at = datetime.now(timezone.utc).isoformat()

    result = await self._cancel_task_script(
        keys=[cancel_key, channel],
        args=[task_id, ttl_ms, cancelled_at],
    )
    return result == 1
```

## 6.2 Check Cancellation Flag

Kein Lua noetig — einfaches EXISTS:

```python
async def is_cancelled(self, task_id: str) -> bool:
    """Prueft ob ein Task gecancelt wurde.

    Wird vom Worker aufgerufen:
      1. Vor Task-Start (im Message-Handler)
      2. Periodisch waehrend der Ausfuehrung (Cancel-Monitor-Coroutine)

    Returns:
        True wenn gecancelt, False wenn nicht.
    """
    cancel_key = f"{self._prefix}:cancel:{task_id}"
    return await self._client.exists(cancel_key) == 1
```

### Batch-Check fuer mehrere Tasks (optimiert)

```python
async def are_cancelled(self, task_ids: list[str]) -> dict[str, bool]:
    """Prueft Cancellation-Flags fuer mehrere Tasks in einem Roundtrip.

    Verwendet Pipeline fuer N EXISTS-Befehle in einem Netzwerk-Roundtrip.

    Returns:
        Dict {task_id: is_cancelled}.
    """
    if not task_ids:
        return {}

    async with self._client.pipeline(transaction=False) as pipe:
        for task_id in task_ids:
            pipe.exists(f"{self._prefix}:cancel:{task_id}")
        results = await pipe.execute()

    return {
        task_id: bool(result)
        for task_id, result in zip(task_ids, results)
    }
```

## 6.3 Clean Up Cancellation Flag

```python
async def clear_cancellation(self, task_id: str) -> bool:
    """Loescht das Cancellation-Flag nach Task-Abschluss.

    Wird aufgerufen nachdem der Task erfolgreich als CANCELLED markiert wurde.
    Gibt Speicher frei und verhindert, dass ein Retry faelschlicherweise
    als gecancelt erkannt wird.

    Returns:
        True wenn Flag existierte und geloescht wurde.
    """
    cancel_key = f"{self._prefix}:cancel:{task_id}"
    return await self._client.delete(cancel_key) == 1
```

---

# 7. Workflow Chord Completion Scripts

## 7.1 Chord Counter Increment + Completion Check (atomar)

### Lua-Skript

```lua
-- chord_complete.lua
-- Inkrementiert den Chord-Counter, speichert das Teilergebnis, und prueft
-- ob alle Tasks im Chord abgeschlossen sind. ATOMAR.
--
-- KEYS[1] = Chord-Counter-Key (z.B. "sq:chord:counter:{chord_id}")
-- KEYS[2] = Chord-Results-Key (z.B. "sq:chord:results:{chord_id}")
-- KEYS[3] = Notification-Channel (z.B. "sq:chord:notify:{chord_id}")
-- ARGV[1] = total_tasks (Gesamtanzahl Tasks im Chord)
-- ARGV[2] = task_index (0-basierter Index dieses Tasks im Chord)
-- ARGV[3] = serialized_result (Ergebnis dieses Tasks)
-- ARGV[4] = ttl_ms (TTL fuer Counter und Results)
-- ARGV[5] = task_id (fuer Notification)
--
-- Rueckgabe: {is_complete (0/1), current_count}
--   is_complete: 1 wenn ALLE Tasks abgeschlossen (Callback ausfuehren!)
--   current_count: Aktuelle Anzahl abgeschlossener Tasks
--
-- KRITISCH fuer Korrektheit:
--   - INCR ist atomar -> kein doppeltes Zaehlen moeglich
--   - Ergebnis wird per Index gespeichert (HSET) -> Reihenfolge egal
--   - Genau EIN Worker erhaelt is_complete=1 -> genau EIN Callback
--
-- Race-Condition-Analyse:
--   Worker A und Worker B beenden gleichzeitig Tasks im gleichen Chord.
--   Beide fuehren dieses Lua-Skript aus, aber Lua ist single-threaded:
--     Worker A: INCR -> 3, Check: 3 < 5 -> nicht komplett
--     Worker B: INCR -> 4, Check: 4 < 5 -> nicht komplett
--     Worker C: INCR -> 5, Check: 5 == 5 -> KOMPLETT! Callback enqueuen.
--   Kein Worker sieht faelschlicherweise "komplett".
--
-- Edge Cases:
--   - Worker crashed nach INCR aber vor HSET: Counter ist erhoet,
--     aber Ergebnis fehlt. Der Chord wird als "komplett" erkannt,
--     aber ein Ergebnis fehlt. Loesung: Im Callback pruefen ob
--     HLEN(results) == total_tasks. Falls nicht: Fehlerbehandlung.
--   - Doppeltes Ausfuehren (Retry desselben Tasks): INCR zaehlt doppelt!
--     Loesung: Vor INCR pruefen ob das Ergebnis fuer diesen Index
--     bereits existiert (HEXISTS).
--   - total_tasks = 0: Counter wird nie "komplett". Muss in der API validiert werden.
--   - total_tasks = 1: Sofort komplett nach dem einzigen Task.

local counter_key = KEYS[1]
local results_key = KEYS[2]
local channel = KEYS[3]
local total_tasks = tonumber(ARGV[1])
local task_index = ARGV[2]
local result = ARGV[3]
local ttl_ms = tonumber(ARGV[4])
local task_id = ARGV[5]

-- Duplikat-Schutz: Pruefen ob dieses Ergebnis bereits gespeichert wurde
local already_stored = redis.call('HEXISTS', results_key, task_index)
if already_stored == 1 then
    -- Duplikat (Retry) — NICHT erneut zaehlen
    -- Aber Ergebnis aktualisieren (neuestes Ergebnis gewinnt)
    redis.call('HSET', results_key, task_index, result)
    local current_count = tonumber(redis.call('GET', counter_key) or '0')
    return {0, current_count}
end

-- Ergebnis speichern (Index -> Ergebnis)
redis.call('HSET', results_key, task_index, result)

-- Counter atomar inkrementieren
local current_count = redis.call('INCR', counter_key)

-- TTL setzen/erneuern
if ttl_ms > 0 then
    redis.call('PEXPIRE', counter_key, ttl_ms)
    redis.call('PEXPIRE', results_key, ttl_ms)
end

-- Completion-Check
local is_complete = 0
if current_count >= total_tasks then
    is_complete = 1
    -- Notification: Chord ist komplett
    local notification = string.format(
        '{"chord_id":"%s","event":"CHORD_COMPLETE","count":%d,"total":%d}',
        KEYS[1], current_count, total_tasks
    )
    redis.call('PUBLISH', channel, notification)
end

return {is_complete, current_count}
```

### Python-Caller

```python
_CHORD_COMPLETE_LUA = """
local counter_key = KEYS[1]
local results_key = KEYS[2]
local channel = KEYS[3]
local total_tasks = tonumber(ARGV[1])
local task_index = ARGV[2]
local result = ARGV[3]
local ttl_ms = tonumber(ARGV[4])
local task_id = ARGV[5]

local already_stored = redis.call('HEXISTS', results_key, task_index)
if already_stored == 1 then
    redis.call('HSET', results_key, task_index, result)
    local current_count = tonumber(redis.call('GET', counter_key) or '0')
    return {0, current_count}
end

redis.call('HSET', results_key, task_index, result)

local current_count = redis.call('INCR', counter_key)

if ttl_ms > 0 then
    redis.call('PEXPIRE', counter_key, ttl_ms)
    redis.call('PEXPIRE', results_key, ttl_ms)
end

local is_complete = 0
if current_count >= total_tasks then
    is_complete = 1
    local notification = string.format(
        '{"chord_id":"%s","event":"CHORD_COMPLETE","count":%d,"total":%d}',
        KEYS[1], current_count, total_tasks
    )
    redis.call('PUBLISH', channel, notification)
end

return {is_complete, current_count}
"""

@dataclass(frozen=True, slots=True)
class ChordProgress:
    """Fortschritt eines Chord-Workflows."""
    is_complete: bool
    completed_count: int
    total_tasks: int


async def chord_task_complete(
    self,
    chord_id: str,
    task_index: int,
    task_id: str,
    result: bytes,
    total_tasks: int,
    ttl_ms: int,
) -> ChordProgress:
    """Meldet den Abschluss eines Tasks in einem Chord.

    Wird vom Worker nach jedem Task-Abschluss in einem Chord aufgerufen.
    Genau ein Aufruf (der letzte) erhaelt is_complete=True.
    Dieser Worker MUSS dann den Chord-Callback enqueuen.

    Args:
        chord_id: Eindeutige ID des Chords.
        task_index: 0-basierter Index dieses Tasks im Chord.
        task_id: ID des abgeschlossenen Tasks.
        result: Serialisiertes Ergebnis.
        total_tasks: Gesamtanzahl Tasks im Chord.
        ttl_ms: TTL fuer Counter und Results.

    Returns:
        ChordProgress mit Completion-Status.
    """
    counter_key = f"{self._prefix}:chord:counter:{chord_id}"
    results_key = f"{self._prefix}:chord:results:{chord_id}"
    channel = f"{self._prefix}:chord:notify:{chord_id}"

    raw = await self._chord_complete_script(
        keys=[counter_key, results_key, channel],
        args=[total_tasks, task_index, result, ttl_ms, task_id],
    )

    return ChordProgress(
        is_complete=raw[0] == 1,
        completed_count=int(raw[1]),
        total_tasks=total_tasks,
    )
```

## 7.2 Get All Chord Results (nach Completion)

```python
async def get_chord_results(
    self,
    chord_id: str,
    total_tasks: int,
) -> Optional[list[bytes]]:
    """Holt alle Ergebnisse eines abgeschlossenen Chords in korrekter Reihenfolge.

    Die Ergebnisse sind nach task_index sortiert (0, 1, 2, ...).

    Args:
        chord_id: Eindeutige ID des Chords.
        total_tasks: Erwartete Gesamtanzahl (fuer Validierung).

    Returns:
        Liste der Ergebnisse in Index-Reihenfolge, oder None wenn
        nicht alle Ergebnisse vorhanden sind.

    Edge Case:
        Wenn HLEN < total_tasks: Ein Ergebnis fehlt (Worker-Crash zwischen
        INCR und HSET). Gibt None zurueck. Der Caller muss entscheiden
        ob der Chord als fehlgeschlagen gilt.
    """
    results_key = f"{self._prefix}:chord:results:{chord_id}"
    raw = await self._client.hgetall(results_key)

    if len(raw) < total_tasks:
        return None

    # Sortiert nach Index zurueckgeben
    return [raw[str(i).encode()] for i in range(total_tasks)]
```

---

# 8. Schedule State Scripts

## 8.1 Update last_run fuer einen Schedule

### Lua-Skript

```lua
-- update_schedule_last_run.lua
-- Aktualisiert den last_run Timestamp und berechnet den naechsten Lauf.
-- Aktualisiert gleichzeitig den Score im Sorted Set fuer effizientes
-- Abfragen faelliger Schedules.
--
-- KEYS[1] = Schedule-Hash-Key (z.B. "sq:schedule:daily_cleanup")
-- KEYS[2] = Schedules-Sorted-Set (z.B. "sq:schedules")
-- ARGV[1] = last_run_timestamp (Unix-Timestamp in Sekunden, Float)
-- ARGV[2] = next_run_timestamp (Unix-Timestamp in Sekunden, Float)
-- ARGV[3] = schedule_name (Member im Sorted Set)
--
-- Rueckgabe: 1 = erfolgreich
--
-- Das Sorted Set "sq:schedules" hat als:
--   Member: schedule_name
--   Score:  next_run_timestamp
-- Dadurch kann ZRANGEBYSCORE effizient alle faelligen Schedules finden.

local hash_key = KEYS[1]
local sorted_set_key = KEYS[2]
local last_run = ARGV[1]
local next_run = ARGV[2]
local schedule_name = ARGV[3]

-- Hash aktualisieren
redis.call('HSET', hash_key, 'last_run', last_run, 'next_run', next_run)

-- Score im Sorted Set aktualisieren
redis.call('ZADD', sorted_set_key, next_run, schedule_name)

return 1
```

### Python-Caller

```python
_UPDATE_SCHEDULE_LUA = """
local hash_key = KEYS[1]
local sorted_set_key = KEYS[2]
local last_run = ARGV[1]
local next_run = ARGV[2]
local schedule_name = ARGV[3]

redis.call('HSET', hash_key, 'last_run', last_run, 'next_run', next_run)
redis.call('ZADD', sorted_set_key, next_run, schedule_name)

return 1
"""

async def update_schedule_last_run(
    self,
    schedule_name: str,
    last_run: float,
    next_run: float,
) -> None:
    """Aktualisiert den last_run Timestamp eines Schedules.

    Args:
        schedule_name: Name des Schedules (z.B. "daily_cleanup").
        last_run: Unix-Timestamp der letzten Ausfuehrung.
        next_run: Unix-Timestamp der naechsten geplanten Ausfuehrung.
    """
    hash_key = f"{self._prefix}:schedule:{schedule_name}"
    sorted_set_key = f"{self._prefix}:schedules"

    await self._update_schedule_script(
        keys=[hash_key, sorted_set_key],
        args=[last_run, next_run, schedule_name],
    )
```

## 8.2 Get All Due Schedules (atomar)

### Lua-Skript

```lua
-- get_due_schedules.lua
-- Findet alle Schedules die faellig sind und markiert sie als "in Bearbeitung".
--
-- KEYS[1] = Schedules-Sorted-Set (z.B. "sq:schedules")
-- ARGV[1] = now_timestamp (Unix-Timestamp in Sekunden, Float)
-- ARGV[2] = max_results (Limit um Ueberlastung zu vermeiden)
--
-- Rueckgabe: Liste von Schedule-Namen die faellig sind
--
-- Algorithmus:
--   ZRANGEBYSCORE mit score <= now_timestamp findet alle faelligen Schedules.
--   
-- ACHTUNG: Dieses Skript entfernt die Schedules NICHT aus dem Sorted Set.
-- Der Caller muss nach erfolgreicher Enqueue den Score auf next_run aktualisieren
-- (via update_schedule_last_run). Andernfalls werden sie beim naechsten Check
-- erneut gefunden — was als Sicherheitsnetz funktioniert (At-Least-Once).
--
-- Alternative Strategie (aggressiver, At-Most-Once):
-- ZRANGEBYSCORE + ZREM in einem Skript. Aber dann geht ein Schedule verloren
-- wenn der Worker zwischen ZREM und Enqueue crasht.
--
-- Empfehlung: At-Least-Once (ohne ZREM), kombiniert mit Unique-Tasks
-- um Duplikate zu verhindern.

local sorted_set_key = KEYS[1]
local now = ARGV[1]
local max_results = tonumber(ARGV[2])

-- Alle faelligen Schedules finden (Score <= now)
local due = redis.call('ZRANGEBYSCORE', sorted_set_key, '-inf', now, 'LIMIT', 0, max_results)

return due
```

### Python-Caller

```python
_GET_DUE_SCHEDULES_LUA = """
local sorted_set_key = KEYS[1]
local now = ARGV[1]
local max_results = tonumber(ARGV[2])

local due = redis.call('ZRANGEBYSCORE', sorted_set_key, '-inf', now, 'LIMIT', 0, max_results)

return due
"""

async def get_due_schedules(
    self,
    max_results: int = 100,
) -> list[str]:
    """Findet alle Schedules die faellig sind.

    Verwendet Valkey-Serverzeit um Clock-Drift zu vermeiden.

    Args:
        max_results: Maximale Anzahl zurueckgegebener Schedules.

    Returns:
        Liste von Schedule-Namen die ausgefuehrt werden muessen.
    """
    sorted_set_key = f"{self._prefix}:schedules"

    # Server-Zeit verwenden
    t = await self._client.time()
    now = t[0] + t[1] / 1_000_000.0

    result = await self._get_due_schedules_script(
        keys=[sorted_set_key],
        args=[now, max_results],
    )

    return [
        name.decode() if isinstance(name, bytes) else name
        for name in result
    ]
```

---

# 9. Worker Registration Scripts

## 9.1 Register Worker (Heartbeat)

### Lua-Skript

```lua
-- register_worker.lua
-- Registriert einen Worker oder erneuert seinen Heartbeat.
-- Speichert Worker-Metadaten und setzt einen TTL fuer automatische Abmeldung.
--
-- KEYS[1] = Workers-Sorted-Set (z.B. "sq:workers")
-- KEYS[2] = Worker-Hash-Key (z.B. "sq:worker:{worker_id}")
-- ARGV[1] = worker_id
-- ARGV[2] = ttl_ms (Heartbeat-Intervall * 3 empfohlen)
-- ARGV[3] = metadata_json (Worker-Metadaten: hostname, pid, queues, etc.)
-- ARGV[4] = now_ms (Timestamp in Millisekunden)
--
-- Rueckgabe: 1 = neu registriert, 0 = Heartbeat erneuert
--
-- Design:
--   - Sorted Set fuer schnelle Abfrage aktiver Worker (Score = last_seen)
--   - Hash fuer detaillierte Metadaten pro Worker
--   - TTL auf dem Hash fuer automatisches Aufraumen bei Crash
--
-- Edge Cases:
--   - Worker startet nach Crash mit gleicher ID: Metadaten werden ueberschrieben
--   - Worker aendert Queues zur Laufzeit: Metadaten-Update bei naechstem Heartbeat
--   - Sehr viele Worker (> 1000): Sorted Set bleibt effizient (O(log N))

local workers_set = KEYS[1]
local worker_hash = KEYS[2]
local worker_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local metadata = ARGV[3]
local now_ms = tonumber(ARGV[4])

-- Pruefen ob Worker neu ist
local is_new = redis.call('ZSCORE', workers_set, worker_id) == false

-- Score aktualisieren (last_seen Timestamp)
redis.call('ZADD', workers_set, now_ms, worker_id)

-- Metadaten speichern
redis.call('SET', worker_hash, metadata)

-- TTL auf Hash setzen (automatisches Cleanup bei Crash)
if ttl_ms > 0 then
    redis.call('PEXPIRE', worker_hash, ttl_ms)
end

if is_new then
    return 1
else
    return 0
end
```

### Python-Caller

```python
_REGISTER_WORKER_LUA = """
local workers_set = KEYS[1]
local worker_hash = KEYS[2]
local worker_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local metadata = ARGV[3]
local now_ms = tonumber(ARGV[4])

local is_new = redis.call('ZSCORE', workers_set, worker_id) == false

redis.call('ZADD', workers_set, now_ms, worker_id)
redis.call('SET', worker_hash, metadata)

if ttl_ms > 0 then
    redis.call('PEXPIRE', worker_hash, ttl_ms)
end

if is_new then
    return 1
else
    return 0
end
"""

async def register_worker(
    self,
    worker_id: str,
    metadata: dict,
    heartbeat_ttl_ms: int = 30_000,
) -> bool:
    """Registriert einen Worker oder erneuert seinen Heartbeat.

    Sollte alle heartbeat_ttl_ms / 3 Millisekunden aufgerufen werden.

    Args:
        worker_id: Eindeutige Worker-ID (z.B. "{hostname}-{pid}-{uuid4[:8]}").
        metadata: Worker-Metadaten:
            {
                "hostname": "worker-01",
                "pid": 12345,
                "queues": ["default", "high"],
                "concurrency": 10,
                "started_at": "2026-04-15T10:00:00Z",
                "version": "0.1.0",
            }
        heartbeat_ttl_ms: TTL fuer automatische Abmeldung bei Crash.
            Standard: 30 Sekunden. Heartbeat-Intervall sollte ttl/3 sein.

    Returns:
        True wenn Worker neu registriert, False wenn Heartbeat erneuert.
    """
    workers_set = f"{self._prefix}:workers"
    worker_hash = f"{self._prefix}:worker:{worker_id}"

    t = await self._client.time()
    now_ms = t[0] * 1000 + t[1] // 1000

    result = await self._register_worker_script(
        keys=[workers_set, worker_hash],
        args=[
            worker_id,
            heartbeat_ttl_ms,
            json.dumps(metadata),
            now_ms,
        ],
    )
    return result == 1
```

## 9.2 Deregister Worker

### Lua-Skript

```lua
-- deregister_worker.lua
-- Entfernt einen Worker aus der Registrierung (graceful Shutdown).
--
-- KEYS[1] = Workers-Sorted-Set (z.B. "sq:workers")
-- KEYS[2] = Worker-Hash-Key (z.B. "sq:worker:{worker_id}")
-- ARGV[1] = worker_id
--
-- Rueckgabe: 1 = entfernt, 0 = Worker war nicht registriert

local workers_set = KEYS[1]
local worker_hash = KEYS[2]
local worker_id = ARGV[1]

redis.call('ZREM', workers_set, worker_id)
redis.call('DEL', worker_hash)

return 1
```

### Python-Caller

```python
_DEREGISTER_WORKER_LUA = """
local workers_set = KEYS[1]
local worker_hash = KEYS[2]
local worker_id = ARGV[1]

redis.call('ZREM', workers_set, worker_id)
redis.call('DEL', worker_hash)

return 1
"""

async def deregister_worker(self, worker_id: str) -> None:
    """Entfernt einen Worker aus der Registrierung.

    Sollte beim graceful Shutdown aufgerufen werden.
    """
    workers_set = f"{self._prefix}:workers"
    worker_hash = f"{self._prefix}:worker:{worker_id}"

    await self._deregister_worker_script(
        keys=[workers_set, worker_hash],
        args=[worker_id],
    )
```

## 9.3 Get Active Workers

```python
async def get_active_workers(
    self,
    stale_threshold_ms: int = 30_000,
) -> list[dict]:
    """Holt alle aktiven Worker mit ihren Metadaten.

    Entfernt gleichzeitig Worker deren Heartbeat abgelaufen ist
    (Score aelter als stale_threshold_ms).

    Args:
        stale_threshold_ms: Worker aelter als dieser Threshold gelten als tot.

    Returns:
        Liste von Worker-Metadaten-Dicts.
    """
    workers_set = f"{self._prefix}:workers"

    t = await self._client.time()
    now_ms = t[0] * 1000 + t[1] // 1000
    stale_cutoff = now_ms - stale_threshold_ms

    # Schritt 1: Veraltete Worker entfernen
    await self._client.zremrangebyscore(workers_set, '-inf', stale_cutoff)

    # Schritt 2: Aktive Worker-IDs holen (mit Score = last_seen)
    active = await self._client.zrangebyscore(
        workers_set, stale_cutoff, '+inf', withscores=True
    )

    if not active:
        return []

    # Schritt 3: Metadaten per Pipeline holen
    worker_ids = []
    async with self._client.pipeline(transaction=False) as pipe:
        for member, score in active:
            worker_id = member.decode() if isinstance(member, bytes) else member
            worker_ids.append((worker_id, score))
            pipe.get(f"{self._prefix}:worker:{worker_id}")
        metadata_list = await pipe.execute()

    # Schritt 4: Zusammenfuegen
    result = []
    for (worker_id, last_seen_ms), raw_metadata in zip(worker_ids, metadata_list):
        if raw_metadata is None:
            continue  # Hash per TTL abgelaufen, Sorted-Set-Eintrag bald weg
        metadata = json.loads(raw_metadata)
        metadata["worker_id"] = worker_id
        metadata["last_seen_ms"] = last_seen_ms
        result.append(metadata)

    return result
```

---

# 10. Vollstaendige Python-Integration: ValkeyScripts Klasse

```python
"""
swarmq/valkey_scripts.py

Zentrale Klasse fuer alle Lua-Skripte in SwarmQ.
Registriert alle Skripte einmalig beim Start und bietet typisierte Python-Methoden.

Verwendung:
    client = valkey.asyncio.Valkey(host="localhost", port=6379)
    scripts = ValkeyScripts(client)
    # Ab jetzt alle Operationen ueber scripts.*
"""

from __future__ import annotations

import hashlib
import json
import time
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import valkey.asyncio as valkey


# ============================================================================
# Datenklassen
# ============================================================================

@dataclass(frozen=True, slots=True)
class UniqueCheckResult:
    """Ergebnis einer Unique-Task-Pruefung."""
    is_unique: bool
    task_id: str
    existing_task_id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class ChordProgress:
    """Fortschritt eines Chord-Workflows."""
    is_complete: bool
    completed_count: int
    total_tasks: int


@dataclass(frozen=True, slots=True)
class LockStatus:
    """Status eines Locks."""
    held: bool
    owner: Optional[str]
    ttl_ms: int


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    """Ergebnis einer Rate-Limit-Pruefung."""
    allowed: bool
    remaining: int
    retry_after_ms: int


@dataclass(frozen=True, slots=True)
class ProgressInfo:
    """Fortschrittsinformation eines Tasks."""
    current: int
    total: int
    percent: float
    message: str = ""
    eta_seconds: Optional[int] = None
    updated_at: Optional[str] = None
    started_at: Optional[str] = None


# ============================================================================
# Hilfsfunktionen
# ============================================================================

def _normalize_for_hash(obj: Any) -> Any:
    """Normalisiert Objekte fuer deterministische JSON-Serialisierung."""
    if isinstance(obj, (set, frozenset)):
        return sorted(str(item) for item in obj)
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode('ascii')
    elif isinstance(obj, dict):
        return {str(k): _normalize_for_hash(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, (list, tuple)):
        return [_normalize_for_hash(item) for item in obj]
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        return str(obj)


def stable_args_hash(args: tuple, kwargs: dict) -> str:
    """Erzeugt einen stabilen Hash fuer Task-Argumente (16 Hex-Zeichen)."""
    serialized = json.dumps(
        {"a": _normalize_for_hash(args), "k": _normalize_for_hash(kwargs)},
        sort_keys=True,
        ensure_ascii=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()[:16]


def generate_unique_key(
    task_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    key_prefix: str = "sq",
    custom_key_func: Optional[callable] = None,
) -> str:
    """Generiert einen stabilen Unique-Key fuer Task-Deduplizierung."""
    if custom_key_func is not None:
        custom_part = custom_key_func(*args, **kwargs)
        return f"{key_prefix}:unique:{custom_part}"
    args_hash_val = stable_args_hash(args, kwargs)
    return f"{key_prefix}:unique:{task_name}:{args_hash_val}"


def parse_rate_string(rate_str: str) -> tuple[int, int]:
    """Parst '100/m' in (max_requests=100, window_ms=60000)."""
    modifiers = {'s': 1_000, 'm': 60_000, 'h': 3_600_000}
    parts = rate_str.strip().split('/')
    if len(parts) != 2 or parts[1] not in modifiers:
        raise ValueError(
            f"Ungueltiger Rate-String: '{rate_str}'. "
            f"Erwartet: '<n>/<s|m|h>' (z.B. '100/m')"
        )
    max_requests = int(parts[0])
    if max_requests < 0:
        raise ValueError(f"max_requests muss >= 0 sein, war: {max_requests}")
    return max_requests, modifiers[parts[1]]


# ============================================================================
# Lua-Skripte (als Konstanten)
# ============================================================================

_LUA_STORE_RESULT = """
local result_key = KEYS[1]
local channel = KEYS[2]
local result_data = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local notification = ARGV[3]

redis.call('SET', result_key, result_data)
if ttl_ms > 0 then
    redis.call('PEXPIRE', result_key, ttl_ms)
end
redis.call('PUBLISH', channel, notification)
return 1
"""

_LUA_STORE_PROGRESS = """
local progress_key = KEYS[1]
local channel = KEYS[2]
local current = ARGV[1]
local total = ARGV[2]
local message = ARGV[3]
local ttl_ms = tonumber(ARGV[4])
local updated_at = ARGV[5]
local started_at = ARGV[6]
local eta_seconds = ARGV[7]
local notification_json = ARGV[8]

local total_num = tonumber(total)
local current_num = tonumber(current)
local percent = "0.0"
if total_num > 0 then
    percent = string.format("%.1f", (current_num / total_num) * 100)
end

local is_new = redis.call('EXISTS', progress_key) == 0

redis.call('HSET', progress_key,
    'current', current,
    'total', total,
    'percent', percent,
    'message', message,
    'updated_at', updated_at,
    'eta_seconds', eta_seconds
)

if is_new then
    redis.call('HSET', progress_key, 'started_at', started_at)
end

if ttl_ms > 0 then
    redis.call('PEXPIRE', progress_key, ttl_ms)
end

redis.call('PUBLISH', channel, notification_json)
return 1
"""

_LUA_EXTEND_LOCK = """
local lock_key = KEYS[1]
local expected_owner = ARGV[1]
local new_ttl_ms = tonumber(ARGV[2])

local current_owner = redis.call('GET', lock_key)
if current_owner == expected_owner then
    redis.call('PEXPIRE', lock_key, new_ttl_ms)
    return 1
else
    return 0
end
"""

_LUA_RELEASE_LOCK = """
local lock_key = KEYS[1]
local expected_owner = ARGV[1]

local current_owner = redis.call('GET', lock_key)
if current_owner == expected_owner then
    redis.call('DEL', lock_key)
    return 1
else
    return 0
end
"""

_LUA_SLIDING_WINDOW = """
local current_key = KEYS[1]
local previous_key = KEYS[2]
local max_requests = tonumber(ARGV[1])
local window_size_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

if window_size_ms <= 0 then
    return {0, 0, 0}
end

local current_count = tonumber(redis.call('GET', current_key) or '0')
local previous_count = tonumber(redis.call('GET', previous_key) or '0')

local window_start_ms = now_ms - (now_ms % window_size_ms)
local elapsed_ratio = (now_ms - window_start_ms) / window_size_ms
local estimated_count = previous_count * (1.0 - elapsed_ratio) + current_count

if estimated_count >= max_requests then
    local remaining_window_ms = window_size_ms - (now_ms - window_start_ms)
    local retry_after = math.max(1, math.ceil(remaining_window_ms))
    local remaining = math.max(0, math.floor(max_requests - estimated_count))
    return {0, remaining, retry_after}
end

current_count = redis.call('INCR', current_key)
if current_count == 1 then
    redis.call('PEXPIRE', current_key, window_size_ms * 2)
end

local new_estimated = previous_count * (1.0 - elapsed_ratio) + current_count
local remaining = math.max(0, math.floor(max_requests - new_estimated))
return {1, remaining, 0}
"""

_LUA_TOKEN_BUCKET = """
local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now_us = tonumber(ARGV[4])

if requested > max_tokens then
    return {0, "0", math.ceil((requested / refill_rate) * 1000)}
end

local data = redis.call('HMGET', key, 'tokens', 'last_refill_us')
local tokens = tonumber(data[1])
local last_refill_us = tonumber(data[2])

if tokens == nil then
    tokens = max_tokens
    last_refill_us = now_us
end

if refill_rate > 0 and now_us > last_refill_us then
    local elapsed_s = (now_us - last_refill_us) / 1000000.0
    local new_tokens = elapsed_s * refill_rate
    tokens = math.min(max_tokens, tokens + new_tokens)
    last_refill_us = now_us
end

local allowed = 0
local retry_after_ms = 0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    if refill_rate > 0 then
        retry_after_ms = math.ceil(((requested - tokens) / refill_rate) * 1000)
    else
        retry_after_ms = -1
    end
end

redis.call('HMSET', key,
    'tokens', string.format("%.6f", tokens),
    'last_refill_us', tostring(last_refill_us)
)

local ttl_s
if refill_rate > 0 then
    ttl_s = math.max(60, math.ceil((max_tokens / refill_rate) * 2))
else
    ttl_s = 3600
end
redis.call('EXPIRE', key, ttl_s)

return {allowed, string.format("%.2f", tokens), retry_after_ms}
"""

_LUA_TASK_LOCK_ACQUIRE = """
local key = KEYS[1]
local owner = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

local current_owner = redis.call('GET', key)

if current_owner == false then
    if ttl_ms > 0 then
        redis.call('SET', key, owner, 'PX', ttl_ms)
    else
        redis.call('SET', key, owner)
    end
    return 1
elseif current_owner == owner then
    if ttl_ms > 0 then
        redis.call('PEXPIRE', key, ttl_ms)
    end
    return 1
else
    return 0
end
"""

_LUA_SEMAPHORE_ACQUIRE = """
local key = KEYS[1]
local max_conc = tonumber(ARGV[1])
local owner = ARGV[2]
local ttl_ms = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms)

local existing_score = redis.call('ZSCORE', key, owner)
if existing_score then
    local expire_at = now_ms + ttl_ms
    redis.call('ZADD', key, expire_at, owner)
    redis.call('PEXPIRE', key, ttl_ms + 1000)
    local current = redis.call('ZCARD', key)
    return {1, current}
end

local current = redis.call('ZCARD', key)
if current >= max_conc then
    return {0, current}
end

local expire_at = now_ms + ttl_ms
redis.call('ZADD', key, expire_at, owner)
redis.call('PEXPIRE', key, ttl_ms + 1000)
return {1, current + 1}
"""

_LUA_SEMAPHORE_RELEASE = """
local key = KEYS[1]
local owner = ARGV[1]
return redis.call('ZREM', key, owner)
"""

_LUA_UNIQUE_ACQUIRE = """
local key = KEYS[1]
local task_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

local existing = redis.call('GET', key)
if existing then
    return {0, existing}
end

if ttl_ms > 0 then
    redis.call('SET', key, task_id, 'PX', ttl_ms)
else
    redis.call('SET', key, task_id)
end
return {1, task_id}
"""

_LUA_UNIQUE_RELEASE = """
local key = KEYS[1]
local task_id = ARGV[1]

local current = redis.call('GET', key)
if current == task_id then
    redis.call('DEL', key)
    return 1
end
return 0
"""

_LUA_CANCEL_TASK = """
local cancel_key = KEYS[1]
local channel = KEYS[2]
local task_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local cancelled_at = ARGV[3]

local already_cancelled = redis.call('EXISTS', cancel_key)

redis.call('SET', cancel_key, cancelled_at)
if ttl_ms > 0 then
    redis.call('PEXPIRE', cancel_key, ttl_ms)
end

local notification = string.format(
    '{"task_id":"%s","event":"CANCELLING","cancelled_at":"%s"}',
    task_id, cancelled_at
)
redis.call('PUBLISH', channel, notification)

if already_cancelled == 1 then
    return 0
else
    return 1
end
"""

_LUA_CHORD_COMPLETE = """
local counter_key = KEYS[1]
local results_key = KEYS[2]
local channel = KEYS[3]
local total_tasks = tonumber(ARGV[1])
local task_index = ARGV[2]
local result = ARGV[3]
local ttl_ms = tonumber(ARGV[4])
local task_id = ARGV[5]

local already_stored = redis.call('HEXISTS', results_key, task_index)
if already_stored == 1 then
    redis.call('HSET', results_key, task_index, result)
    local current_count = tonumber(redis.call('GET', counter_key) or '0')
    return {0, current_count}
end

redis.call('HSET', results_key, task_index, result)
local current_count = redis.call('INCR', counter_key)

if ttl_ms > 0 then
    redis.call('PEXPIRE', counter_key, ttl_ms)
    redis.call('PEXPIRE', results_key, ttl_ms)
end

local is_complete = 0
if current_count >= total_tasks then
    is_complete = 1
    local notification = string.format(
        '{"chord_id":"%s","event":"CHORD_COMPLETE","count":%d,"total":%d}',
        KEYS[1], current_count, total_tasks
    )
    redis.call('PUBLISH', channel, notification)
end

return {is_complete, current_count}
"""

_LUA_UPDATE_SCHEDULE = """
local hash_key = KEYS[1]
local sorted_set_key = KEYS[2]
local last_run = ARGV[1]
local next_run = ARGV[2]
local schedule_name = ARGV[3]

redis.call('HSET', hash_key, 'last_run', last_run, 'next_run', next_run)
redis.call('ZADD', sorted_set_key, next_run, schedule_name)
return 1
"""

_LUA_GET_DUE_SCHEDULES = """
local sorted_set_key = KEYS[1]
local now = ARGV[1]
local max_results = tonumber(ARGV[2])

return redis.call('ZRANGEBYSCORE', sorted_set_key, '-inf', now, 'LIMIT', 0, max_results)
"""

_LUA_REGISTER_WORKER = """
local workers_set = KEYS[1]
local worker_hash = KEYS[2]
local worker_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local metadata = ARGV[3]
local now_ms = tonumber(ARGV[4])

local is_new = redis.call('ZSCORE', workers_set, worker_id) == false

redis.call('ZADD', workers_set, now_ms, worker_id)
redis.call('SET', worker_hash, metadata)

if ttl_ms > 0 then
    redis.call('PEXPIRE', worker_hash, ttl_ms)
end

if is_new then
    return 1
else
    return 0
end
"""

_LUA_DEREGISTER_WORKER = """
local workers_set = KEYS[1]
local worker_hash = KEYS[2]
local worker_id = ARGV[1]

redis.call('ZREM', workers_set, worker_id)
redis.call('DEL', worker_hash)
return 1
"""


# ============================================================================
# ValkeyScripts Klasse
# ============================================================================

class ValkeyScripts:
    """Alle Lua-Skripte fuer SwarmQ, registriert und gecacht.

    Registriert alle Skripte einmalig bei Instanziierung via register_script().
    valkey-py berechnet SHA1-Hashes und verwendet automatisch EVALSHA
    mit Fallback auf EVAL bei NOSCRIPT-Fehlern.

    Verwendung:
        client = valkey.asyncio.Valkey(host="localhost", port=6379)
        scripts = ValkeyScripts(client)

        # Result speichern
        await scripts.store_result("task-123", b'{"result": 42}', ttl_ms=86400000)

        # Lock akquirieren
        acquired = await scripts.acquire_task_lock("my-lock", "owner-1", ttl_ms=60000)

        # Rate Limit pruefen
        result = await scripts.check_rate_limit("send_email", 100, 60000)
        if not result.allowed:
            # Requeue mit result.retry_after_ms Delay
            ...

    NOSCRIPT-Handling:
        valkey-py's register_script() behandelt NOSCRIPT automatisch:
        1. Erster Aufruf: versucht EVALSHA (nur SHA1-Hash gesendet)
        2. Bei NoScriptError: fuehrt EVAL aus (volles Skript + Hash-Caching)
        3. Folgende Aufrufe: EVALSHA funktioniert (Skript im Server-Cache)
        Kein manuelles Handling noetig.

    Thread-Safety:
        Alle Methoden sind async und thread-safe (asyncio ist single-threaded
        per Event Loop). Mehrere concurrent Tasks koennen die gleiche
        ValkeyScripts-Instanz sicher verwenden.
    """

    def __init__(
        self,
        client: valkey.Valkey,
        key_prefix: str = "sq",
    ) -> None:
        self._client = client
        self._prefix = key_prefix

        # Alle Skripte registrieren
        self._store_result = client.register_script(_LUA_STORE_RESULT)
        self._store_progress = client.register_script(_LUA_STORE_PROGRESS)
        self._extend_lock = client.register_script(_LUA_EXTEND_LOCK)
        self._release_lock = client.register_script(_LUA_RELEASE_LOCK)
        self._sliding_window = client.register_script(_LUA_SLIDING_WINDOW)
        self._token_bucket = client.register_script(_LUA_TOKEN_BUCKET)
        self._task_lock_acquire = client.register_script(_LUA_TASK_LOCK_ACQUIRE)
        self._semaphore_acquire = client.register_script(_LUA_SEMAPHORE_ACQUIRE)
        self._semaphore_release = client.register_script(_LUA_SEMAPHORE_RELEASE)
        self._unique_acquire = client.register_script(_LUA_UNIQUE_ACQUIRE)
        self._unique_release = client.register_script(_LUA_UNIQUE_RELEASE)
        self._cancel_task = client.register_script(_LUA_CANCEL_TASK)
        self._chord_complete = client.register_script(_LUA_CHORD_COMPLETE)
        self._update_schedule = client.register_script(_LUA_UPDATE_SCHEDULE)
        self._get_due_schedules = client.register_script(_LUA_GET_DUE_SCHEDULES)
        self._register_worker = client.register_script(_LUA_REGISTER_WORKER)
        self._deregister_worker = client.register_script(_LUA_DEREGISTER_WORKER)

    # ====================================================================
    # Hilfsmethoden
    # ====================================================================

    async def _server_time_ms(self) -> int:
        """Valkey-Serverzeit in Millisekunden."""
        t = await self._client.time()
        return t[0] * 1000 + t[1] // 1000

    async def _server_time_us(self) -> int:
        """Valkey-Serverzeit in Mikrosekunden."""
        t = await self._client.time()
        return t[0] * 1_000_000 + t[1]

    async def _server_time_float(self) -> float:
        """Valkey-Serverzeit als Float (Sekunden)."""
        t = await self._client.time()
        return t[0] + t[1] / 1_000_000.0

    def _decode(self, value: bytes | str | None) -> Optional[str]:
        """Dekodiert bytes zu str wenn noetig."""
        if isinstance(value, bytes):
            return value.decode()
        return value

    # ====================================================================
    # 1. Result Storage
    # ====================================================================

    async def store_result(
        self,
        task_id: str,
        result: bytes,
        ttl_ms: int,
        status: str = "SUCCESS",
    ) -> None:
        """Speichert ein Task-Ergebnis atomar mit TTL und Pub/Sub-Notification."""
        result_key = f"{self._prefix}:result:{task_id}"
        channel = f"{self._prefix}:notify:{task_id}"
        notification = json.dumps({
            "task_id": task_id,
            "status": status,
            "timestamp": time.time(),
        })
        await self._store_result(
            keys=[result_key, channel],
            args=[result, ttl_ms, notification],
        )

    async def get_result(self, task_id: str) -> Optional[bytes]:
        """Holt ein Task-Ergebnis (Plain GET)."""
        return await self._client.get(f"{self._prefix}:result:{task_id}")

    async def get_result_extend(
        self, task_id: str, extend_ttl_ms: int
    ) -> Optional[bytes]:
        """Holt ein Task-Ergebnis und verlaengert den TTL (Sliding Window)."""
        return await self._client.getex(
            f"{self._prefix}:result:{task_id}", px=extend_ttl_ms
        )

    async def get_result_once(self, task_id: str) -> Optional[bytes]:
        """Holt ein Task-Ergebnis und loescht es sofort (One-Time Retrieval)."""
        return await self._client.getdel(f"{self._prefix}:result:{task_id}")

    # ====================================================================
    # 1b. Progress Tracking
    # ====================================================================

    async def store_progress(
        self,
        task_id: str,
        current: int,
        total: int,
        message: str,
        ttl_ms: int,
        started_at: str,
        eta_seconds: Optional[int] = None,
    ) -> None:
        """Speichert ein Fortschritts-Update atomar mit Pub/Sub."""
        progress_key = f"{self._prefix}:progress:{task_id}"
        channel = f"{self._prefix}:progress:channel:{task_id}"
        updated_at = datetime.now(timezone.utc).isoformat()

        # Percent fuer Notification berechnen
        percent = (current / total * 100) if total > 0 else 0.0
        eta_str = str(eta_seconds) if eta_seconds is not None else ""

        # Notification in Python bauen (vermeidet Lua string.format Probleme
        # mit Sonderzeichen in message)
        notification_json = json.dumps({
            "current": current,
            "total": total,
            "percent": round(percent, 1),
            "message": message,
            "eta_seconds": eta_str,
            "updated_at": updated_at,
        })

        await self._store_progress(
            keys=[progress_key, channel],
            args=[
                current, total, message, ttl_ms,
                updated_at, started_at, eta_str, notification_json,
            ],
        )

    async def get_progress(self, task_id: str) -> Optional[ProgressInfo]:
        """Holt den aktuellen Fortschritt eines Tasks."""
        progress_key = f"{self._prefix}:progress:{task_id}"
        data = await self._client.hgetall(progress_key)
        if not data:
            return None

        def _get(field: str, default: str = "") -> str:
            val = data.get(field) or data.get(field.encode(), b"")
            return val.decode() if isinstance(val, bytes) else (val or default)

        eta_raw = _get("eta_seconds")
        return ProgressInfo(
            current=int(_get("current", "0")),
            total=int(_get("total", "0")),
            percent=float(_get("percent", "0.0")),
            message=_get("message"),
            eta_seconds=int(eta_raw) if eta_raw else None,
            updated_at=_get("updated_at") or None,
            started_at=_get("started_at") or None,
        )

    # ====================================================================
    # 2. Scheduler Lock
    # ====================================================================

    async def acquire_scheduler_lock(
        self, lock_name: str, owner_id: str, ttl_ms: int
    ) -> bool:
        """Akquiriert den Scheduler-Lock (SET NX PX)."""
        lock_key = f"{self._prefix}:scheduler:lock:{lock_name}"
        result = await self._client.set(lock_key, owner_id, nx=True, px=ttl_ms)
        return result is not None

    async def extend_scheduler_lock(
        self, lock_name: str, owner_id: str, ttl_ms: int
    ) -> bool:
        """Verlaengert den Scheduler-Lock (nur wenn Owner)."""
        lock_key = f"{self._prefix}:scheduler:lock:{lock_name}"
        result = await self._extend_lock(
            keys=[lock_key], args=[owner_id, ttl_ms]
        )
        return result == 1

    async def release_scheduler_lock(
        self, lock_name: str, owner_id: str
    ) -> bool:
        """Gibt den Scheduler-Lock frei (nur wenn Owner)."""
        lock_key = f"{self._prefix}:scheduler:lock:{lock_name}"
        result = await self._release_lock(
            keys=[lock_key], args=[owner_id]
        )
        return result == 1

    async def get_scheduler_lock_status(self, lock_name: str) -> LockStatus:
        """Prueft den Status des Scheduler-Locks (nur fuer Monitoring)."""
        lock_key = f"{self._prefix}:scheduler:lock:{lock_name}"
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.get(lock_key)
            pipe.pttl(lock_key)
            owner, ttl_ms = await pipe.execute()
        return LockStatus(
            held=owner is not None,
            owner=self._decode(owner),
            ttl_ms=ttl_ms,
        )

    # ====================================================================
    # 3. Rate Limiting
    # ====================================================================

    async def check_rate_limit(
        self,
        task_name: str,
        max_requests: int,
        window_ms: int,
        *,
        rate_limit_key: Optional[str] = None,
    ) -> RateLimitResult:
        """Prueft Rate Limit via Sliding Window Counter."""
        now_ms = await self._server_time_ms()

        window_start_ms = now_ms - (now_ms % window_ms)
        prev_window_start_ms = window_start_ms - window_ms

        key_base = f"{self._prefix}:rl:{task_name}"
        if rate_limit_key:
            key_base = f"{key_base}:{rate_limit_key}"

        current_key = f"{key_base}:{window_start_ms}"
        previous_key = f"{key_base}:{prev_window_start_ms}"

        result = await self._sliding_window(
            keys=[current_key, previous_key],
            args=[max_requests, window_ms, now_ms],
        )

        return RateLimitResult(
            allowed=result[0] == 1,
            remaining=int(result[1]),
            retry_after_ms=int(result[2]),
        )

    async def check_rate_limit_str(
        self, task_name: str, rate_str: str, **kwargs: Any
    ) -> RateLimitResult:
        """Convenience: Rate-String Syntax (z.B. '100/m')."""
        max_requests, window_ms = parse_rate_string(rate_str)
        return await self.check_rate_limit(
            task_name, max_requests, window_ms, **kwargs
        )

    async def check_rate_limit_token_bucket(
        self,
        task_name: str,
        max_tokens: int,
        refill_rate: float,
        tokens_requested: int = 1,
    ) -> RateLimitResult:
        """Prueft Rate Limit via Token Bucket (fuer Burst-tolerantes Limiting)."""
        key = f"{self._prefix}:rl:bucket:{task_name}"
        now_us = await self._server_time_us()

        result = await self._token_bucket(
            keys=[key],
            args=[max_tokens, refill_rate, tokens_requested, now_us],
        )

        return RateLimitResult(
            allowed=result[0] == 1,
            remaining=int(float(result[1])),
            retry_after_ms=int(result[2]),
        )

    # ====================================================================
    # 4. Task Locking
    # ====================================================================

    async def acquire_task_lock(
        self, lock_name: str, owner_id: str, ttl_ms: int
    ) -> bool:
        """Akquiriert einen Task-Lock (Mutex, reentrant)."""
        lock_key = f"{self._prefix}:lock:{lock_name}"
        result = await self._task_lock_acquire(
            keys=[lock_key], args=[owner_id, ttl_ms]
        )
        return result == 1

    async def release_task_lock(self, lock_name: str, owner_id: str) -> bool:
        """Gibt einen Task-Lock frei (nur wenn Owner)."""
        lock_key = f"{self._prefix}:lock:{lock_name}"
        result = await self._release_lock(
            keys=[lock_key], args=[owner_id]
        )
        return result == 1

    async def extend_task_lock(
        self, lock_name: str, owner_id: str, ttl_ms: int
    ) -> bool:
        """Verlaengert einen Task-Lock (nur wenn Owner)."""
        lock_key = f"{self._prefix}:lock:{lock_name}"
        result = await self._extend_lock(
            keys=[lock_key], args=[owner_id, ttl_ms]
        )
        return result == 1

    async def acquire_semaphore(
        self,
        task_name: str,
        owner_id: str,
        max_concurrency: int,
        ttl_ms: int,
    ) -> tuple[bool, int]:
        """Akquiriert einen Slot im Semaphore (max_concurrency > 1)."""
        key = f"{self._prefix}:sem:{task_name}"
        now_ms = await self._server_time_ms()

        result = await self._semaphore_acquire(
            keys=[key],
            args=[max_concurrency, owner_id, ttl_ms, now_ms],
        )
        return result[0] == 1, int(result[1])

    async def release_semaphore(self, task_name: str, owner_id: str) -> bool:
        """Gibt einen Semaphore-Slot frei."""
        key = f"{self._prefix}:sem:{task_name}"
        result = await self._semaphore_release(
            keys=[key], args=[owner_id]
        )
        return result == 1

    # ====================================================================
    # 5. Deduplication
    # ====================================================================

    async def check_unique_task(
        self, unique_key: str, task_id: str, unique_for_ms: int
    ) -> UniqueCheckResult:
        """Prueft atomar ob ein Task einzigartig ist und akquiriert den Lock."""
        result = await self._unique_acquire(
            keys=[unique_key], args=[task_id, unique_for_ms]
        )
        status = result[0]
        returned_id = self._decode(result[1]) or task_id

        if status == 1:
            return UniqueCheckResult(is_unique=True, task_id=returned_id)
        else:
            return UniqueCheckResult(
                is_unique=False,
                task_id=task_id,
                existing_task_id=returned_id,
            )

    async def release_unique_task(
        self, unique_key: str, task_id: str
    ) -> bool:
        """Gibt einen Unique-Lock frei (nur wenn Owner)."""
        result = await self._unique_release(
            keys=[unique_key], args=[task_id]
        )
        return result == 1

    # ====================================================================
    # 6. Task Cancellation
    # ====================================================================

    async def cancel_task(
        self, task_id: str, ttl_ms: int = 86_400_000
    ) -> bool:
        """Setzt das Cancellation-Flag fuer einen Task."""
        cancel_key = f"{self._prefix}:cancel:{task_id}"
        channel = f"{self._prefix}:events:{task_id}"
        cancelled_at = datetime.now(timezone.utc).isoformat()

        result = await self._cancel_task(
            keys=[cancel_key, channel],
            args=[task_id, ttl_ms, cancelled_at],
        )
        return result == 1

    async def is_cancelled(self, task_id: str) -> bool:
        """Prueft ob ein Task gecancelt wurde."""
        cancel_key = f"{self._prefix}:cancel:{task_id}"
        return await self._client.exists(cancel_key) == 1

    async def are_cancelled(self, task_ids: list[str]) -> dict[str, bool]:
        """Prueft Cancellation-Flags fuer mehrere Tasks (eine Pipeline)."""
        if not task_ids:
            return {}
        async with self._client.pipeline(transaction=False) as pipe:
            for task_id in task_ids:
                pipe.exists(f"{self._prefix}:cancel:{task_id}")
            results = await pipe.execute()
        return {tid: bool(r) for tid, r in zip(task_ids, results)}

    async def clear_cancellation(self, task_id: str) -> bool:
        """Loescht das Cancellation-Flag nach Task-Abschluss."""
        cancel_key = f"{self._prefix}:cancel:{task_id}"
        return await self._client.delete(cancel_key) == 1

    # ====================================================================
    # 7. Workflow Chord
    # ====================================================================

    async def chord_task_complete(
        self,
        chord_id: str,
        task_index: int,
        task_id: str,
        result: bytes,
        total_tasks: int,
        ttl_ms: int,
    ) -> ChordProgress:
        """Meldet den Abschluss eines Tasks in einem Chord (atomar)."""
        counter_key = f"{self._prefix}:chord:counter:{chord_id}"
        results_key = f"{self._prefix}:chord:results:{chord_id}"
        channel = f"{self._prefix}:chord:notify:{chord_id}"

        raw = await self._chord_complete(
            keys=[counter_key, results_key, channel],
            args=[total_tasks, task_index, result, ttl_ms, task_id],
        )
        return ChordProgress(
            is_complete=raw[0] == 1,
            completed_count=int(raw[1]),
            total_tasks=total_tasks,
        )

    async def get_chord_results(
        self, chord_id: str, total_tasks: int
    ) -> Optional[list[bytes]]:
        """Holt alle Chord-Ergebnisse in korrekter Index-Reihenfolge."""
        results_key = f"{self._prefix}:chord:results:{chord_id}"
        raw = await self._client.hgetall(results_key)
        if len(raw) < total_tasks:
            return None
        return [
            raw.get(str(i).encode()) or raw.get(str(i), b"")
            for i in range(total_tasks)
        ]

    # ====================================================================
    # 8. Schedule State
    # ====================================================================

    async def update_schedule_last_run(
        self, schedule_name: str, last_run: float, next_run: float
    ) -> None:
        """Aktualisiert last_run und next_run eines Schedules."""
        hash_key = f"{self._prefix}:schedule:{schedule_name}"
        sorted_set_key = f"{self._prefix}:schedules"
        await self._update_schedule(
            keys=[hash_key, sorted_set_key],
            args=[last_run, next_run, schedule_name],
        )

    async def get_due_schedules(self, max_results: int = 100) -> list[str]:
        """Findet alle faelligen Schedules (next_run <= now)."""
        sorted_set_key = f"{self._prefix}:schedules"
        now = await self._server_time_float()

        result = await self._get_due_schedules(
            keys=[sorted_set_key],
            args=[now, max_results],
        )
        return [self._decode(name) or "" for name in result]

    async def register_schedule(
        self, schedule_name: str, next_run: float, metadata: dict
    ) -> None:
        """Registriert einen neuen Schedule."""
        hash_key = f"{self._prefix}:schedule:{schedule_name}"
        sorted_set_key = f"{self._prefix}:schedules"
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.hset(hash_key, mapping={
                "next_run": str(next_run),
                "last_run": "",
                **{k: str(v) for k, v in metadata.items()},
            })
            pipe.zadd(sorted_set_key, {schedule_name: next_run})
            await pipe.execute()

    # ====================================================================
    # 9. Worker Registration
    # ====================================================================

    async def register_worker(
        self,
        worker_id: str,
        metadata: dict,
        heartbeat_ttl_ms: int = 30_000,
    ) -> bool:
        """Registriert einen Worker oder erneuert seinen Heartbeat."""
        workers_set = f"{self._prefix}:workers"
        worker_hash = f"{self._prefix}:worker:{worker_id}"
        now_ms = await self._server_time_ms()

        result = await self._register_worker(
            keys=[workers_set, worker_hash],
            args=[worker_id, heartbeat_ttl_ms, json.dumps(metadata), now_ms],
        )
        return result == 1

    async def deregister_worker(self, worker_id: str) -> None:
        """Entfernt einen Worker aus der Registrierung."""
        workers_set = f"{self._prefix}:workers"
        worker_hash = f"{self._prefix}:worker:{worker_id}"
        await self._deregister_worker(
            keys=[workers_set, worker_hash],
            args=[worker_id],
        )

    async def get_active_workers(
        self, stale_threshold_ms: int = 30_000
    ) -> list[dict]:
        """Holt alle aktiven Worker mit Metadaten."""
        workers_set = f"{self._prefix}:workers"
        now_ms = await self._server_time_ms()
        stale_cutoff = now_ms - stale_threshold_ms

        # Veraltete Worker entfernen
        await self._client.zremrangebyscore(workers_set, '-inf', stale_cutoff)

        # Aktive Worker holen
        active = await self._client.zrangebyscore(
            workers_set, stale_cutoff, '+inf', withscores=True
        )
        if not active:
            return []

        # Metadaten per Pipeline holen
        worker_ids = []
        async with self._client.pipeline(transaction=False) as pipe:
            for member, score in active:
                wid = self._decode(member) or ""
                worker_ids.append((wid, score))
                pipe.get(f"{self._prefix}:worker:{wid}")
            metadata_list = await pipe.execute()

        result = []
        for (wid, last_seen_ms), raw_metadata in zip(worker_ids, metadata_list):
            if raw_metadata is None:
                continue
            meta = json.loads(raw_metadata)
            meta["worker_id"] = wid
            meta["last_seen_ms"] = last_seen_ms
            result.append(meta)

        return result
```

---

# Anhang: Valkey-Key-Namensraum (Uebersicht)

```
sq:result:{task_id}                      — Task-Ergebnis (STRING)
sq:notify:{task_id}                      — Pub/Sub Channel fuer Ergebnis-Notification
sq:progress:{task_id}                    — Task-Fortschritt (HASH)
sq:progress:channel:{task_id}            — Pub/Sub Channel fuer Progress-Updates
sq:scheduler:lock:{lock_name}            — Scheduler-Lock (STRING)
sq:rl:{task_name}:{window_ts}            — Rate-Limit Sliding Window Counter (STRING)
sq:rl:bucket:{task_name}                 — Rate-Limit Token Bucket (HASH)
sq:lock:{lock_name}                      — Task-Mutex-Lock (STRING)
sq:sem:{task_name}                       — Task-Semaphore (SORTED SET)
sq:unique:{task_name}:{args_hash}        — Unique-Task Lock (STRING)
sq:cancel:{task_id}                      — Cancellation-Flag (STRING)
sq:events:{task_id}                      — Pub/Sub Channel fuer Task-Events
sq:chord:counter:{chord_id}              — Chord-Completion Counter (STRING)
sq:chord:results:{chord_id}              — Chord-Teilergebnisse (HASH)
sq:chord:notify:{chord_id}               — Pub/Sub Channel fuer Chord-Completion
sq:schedule:{schedule_name}              — Schedule-Metadaten (HASH)
sq:schedules                             — Alle Schedules nach next_run (SORTED SET)
sq:workers                               — Aktive Worker nach last_seen (SORTED SET)
sq:worker:{worker_id}                    — Worker-Metadaten (STRING/JSON)
```
