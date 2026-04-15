# Recherche: Workflow-Primitives, Task-Cancellation und Progress-Tracking (SwarmQ)

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument fuer die Implementierung von Workflow-Komposition (chain, group, chord), Task-Abbruch und Fortschritts-Tracking in SwarmQ (async Python 3.14+, RabbitMQ Broker, Valkey Backend).

---

## Inhaltsverzeichnis

1. [Workflow-Primitives: Ueberblick ueber bestehende Implementierungen](#1-workflow-primitives)
2. [Chain: Sequentielle Ausfuehrung mit Resultat-Weiterleitung](#2-chain)
3. [Group: Parallele Ausfuehrung](#3-group)
4. [Chord: Group + Callback](#4-chord)
5. [Weitere Primitives: Starmap, Chunks, Map/Filter](#5-weitere-primitives)
6. [Operator-Syntax und Signature-Pattern](#6-operator-syntax-und-signature-pattern)
7. [Implementierungsmuster fuer SwarmQ: Workflows](#7-implementierungsmuster-workflows)
8. [Task-Cancellation / Revocation](#8-task-cancellation)
9. [Implementierungsmuster fuer SwarmQ: Cancellation](#9-implementierungsmuster-cancellation)
10. [Progress-Tracking](#10-progress-tracking)
11. [Implementierungsmuster fuer SwarmQ: Progress](#11-implementierungsmuster-progress)
12. [Zusammenfassung und Empfehlungen](#12-zusammenfassung)

---

# 1. Workflow-Primitives: Ueberblick ueber bestehende Implementierungen

## 1.1 Vergleich der Libraries

| Feature | Celery Canvas | Dramatiq | TaskIQ Pipelines | arq | SAQ | RQ |
|---|---|---|---|---|---|---|
| Chain / Pipeline | `chain()` | `pipeline()` | `.call_next()` | — | — | Dependency-Chain |
| Group / Parallel | `group()` | `group()` | `.map()` | — | — | — |
| Chord (Group+CB) | `chord()` | `group(completion_callbacks=)` | — | — | — | — |
| Operator-Syntax | `\|` fuer chain | `\|` fuer pipeline | — | — | — | — |
| Signature-Pattern | `task.s()` / `task.si()` | `actor.message()` | `task.kicker()` | — | — | — |
| Serialisierung | JSON/Pickle | JSON | `dumps()`/`loads()` | — | — | — |
| Nested Workflows | Ja (chains of groups) | Nein | Nein | — | — | — |

**Kernbeobachtung:** Celery ist die mit Abstand umfassendste Implementierung. Dramatiq bietet solide Grundlagen. TaskIQ hat einen interessanten Middleware-Ansatz. arq, SAQ und RQ haben keine eingebauten Workflow-Primitives.

---

# 2. Chain: Sequentielle Ausfuehrung mit Resultat-Weiterleitung

## 2.1 Celery: Chain-Implementierung

Celery chains verbinden Tasks sequentiell. Das Ergebnis jedes Tasks wird als erstes Argument an den naechsten weitergereicht.

**Interne Mechanik:**
- `chain.prepare_steps()` verarbeitet Tasks in **umgekehrter Reihenfolge**
- Jeder Task wird mit einer eindeutigen `task_id` "eingefroren" (frozen)
- Tasks werden ueber das `link`-Feld (Protocol v1) oder `chain`-Feld (Protocol v2+) verkettet
- `task.link(prev_task)` verknuepft den vorherigen Task mit dem naechsten
- Das Resultat-Objekt bildet eine Parent-Chain: jedes `AsyncResult.parent` zeigt auf das Ergebnis des vorherigen Tasks

```python
# Celery Chain
from celery import chain

# Explizit
workflow = chain(add.s(2, 2), multiply.s(4), store.s())

# Operator-Syntax (aequivalent)
workflow = add.s(2, 2) | multiply.s(4) | store.s()

# Ausfuehrung: add(2,2) -> 4, multiply(4, 4) -> 16, store(16)
result = workflow.apply_async()
```

**Resultat-Weitergabe:** Der Rueckgabewert von Task N wird als **erstes Argument** von Task N+1 eingefuegt. Bei mutable Signatures (`s()`) werden bestehende Argumente nach rechts verschoben. Bei immutable Signatures (`si()`) wird das Ergebnis **verworfen**.

## 2.2 Dramatiq: Pipeline

```python
import dramatiq

pipe = dramatiq.pipeline([
    add.message(2, 2),
    multiply.message(4),
])
pipe.run()

# Alternativ mit Pipe-Operator
pipe = add.message(2, 2) | multiply.message(4)
```

**Interne Mechanik:**
- Jede Message erhaelt ein `pipe_target`-Feld, das auf die serialisierte naechste Message zeigt
- Nur die **erste Message** wird an den Broker gesendet (`run()` enqueued nur Message 1)
- Nach Abschluss von Task N liest der Worker `pipe_target` und enqueued Task N+1 mit dem Ergebnis
- `get_result()` gibt das Ergebnis der **letzten** Message zurueck
- `get_results()` liefert Ergebnisse aller Messages als Generator

## 2.3 TaskIQ: Pipelines

```python
from taskiq_pipelines import Pipeline

pipe = Pipeline(broker, [])
    .call_next(add, 2, 2)           # Task 1
    .call_next(multiply, param_name=0)  # Task 2, Ergebnis als 1. Argument
    .call_after(log_result)          # Task 3, ohne Ergebnis-Weiterleitung
```

**Interne Mechanik:**
- Pipeline ist ein Wrapper ueber eine Liste von Steps (`AbstractStep`)
- `call_next()`: Ergebnis als positionales Argument (Index) oder Keyword-Argument (String)
- `call_after()`: Naechster Task ohne Ergebnis-Weiterleitung
- Bei `kiq()` werden **alle Task-IDs vorausberechnet**
- Die serialisierte Pipeline wird in den Labels des ersten Tasks gespeichert
- Eine `PipelineMiddleware` deserialisiert nach jedem Task-Abschluss die Pipeline und startet den naechsten Schritt
- Serialisierung: `pipe.dumps()` -> String, `Pipeline.loads(s)` -> Pipeline

**Besonderer Ansatz:** Die gesamte Pipeline-Logik liegt in einer Middleware, nicht im Worker selbst. Das ist elegant, koppelt aber Pipeline-Ausfuehrung an die Middleware-Konfiguration.

---

# 3. Group: Parallele Ausfuehrung

## 3.1 Celery: Group

```python
from celery import group

g = group(add.s(i, i) for i in range(10))
result = g.apply_async()  # GroupResult

# Ergebnisse abfragen
results = result.get()  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
```

**Interne Mechanik:**
- Alle Tasks in der Group erhalten dieselbe `group_id` (via `_freeze_gid()`)
- Jeder Task wird einzeln an den Broker gesendet
- `GroupResult` aggregiert einzelne `AsyncResult`-Objekte
- Completion-Tracking ueber ein `barrier`-Objekt (`ready_barrier`)
- `result.ready()` prueft, ob alle Teilergebnisse vorliegen
- `result.join()` wartet blockierend auf alle Ergebnisse (mit Timeout)
- `result.join_native()` nutzt Backend-optimierte Abfragen (Redis `MGET`)

**Fehlerbehandlung in Groups:**
- Standardmaessig propagiert `result.get()` die erste Exception
- `result.get(propagate=False)` gibt Exceptions als Werte zurueck
- Fehlgeschlagene Tasks verhindern **nicht** die Ausfuehrung anderer Tasks in der Group
- Bei Chords: ein fehlgeschlagener Task kann den Chord-Callback blockieren (konfigurierbar)

## 3.2 Dramatiq: Group

```python
import dramatiq

g = dramatiq.group([
    add.message(1, 2),
    add.message(3, 4),
])
g.run()
g.get_results(block=True)  # [3, 7]
```

**Completion-Tracking:** Dramatiq nutzt eine `Barrier`-Klasse aus dem Rate-Limiting-Backend:
- `Barrier.create(parties=n)` erstellt einen Zaehler mit Anfangswert n
- Jeder abgeschlossene Task ruft `barrier.wait()` auf, was den Zaehler atomar dekrementiert (`backend.decr(key, 1, 1, ttl)`)
- Wenn der Zaehler 0 erreicht, ist die Barrier "cleared"
- Event-basierte Benachrichtigung via `backend.wait_notify(key_events, ttl)`

## 3.3 TaskIQ: Map

```python
pipe = Pipeline(broker, [])
    .call_next(get_user_ids)
    .map(process_user)      # Fuer jedes Element im Ergebnis
    .filter(is_valid_user)  # Filtert basierend auf Rueckgabewert (True/False)
```

TaskIQ implementiert kein eigenstaendiges Group-Primitive, sondern `.map()` als Pipeline-Step, der fuer jedes Element des vorherigen Ergebnisses einen Task startet.

---

# 4. Chord: Group + Callback

## 4.1 Celery: Chord-Implementierung

Ein Chord besteht aus einem Header (Group) und einem Body (Callback). Der Callback wird ausgefuehrt, sobald alle Header-Tasks abgeschlossen sind.

```python
from celery import chord

# Explizit
result = chord(
    [add.s(i, i) for i in range(10)],  # Header (Group)
    sum_results.s()                      # Body (Callback)
).apply_async()

# Operator-Syntax: Group | Task ergibt automatisch einen Chord
result = (group(add.s(i, i) for i in range(10)) | sum_results.s())()
```

### 4.1.1 Chord-Completion-Detection (Redis/Valkey Backend)

Die Erkennung, wann alle Header-Tasks fertig sind, ist das **kritischste Implementierungsdetail**:

**Redis-Backend-Mechanismus:**

Drei Schluessel pro Group:
- `celery-tasksetmeta-{group_id}.t` — Atomarer Zaehler (INCR nach jedem Task)
- `celery-tasksetmeta-{group_id}.s` — Chord-Groesse (erwartete Gesamtanzahl)
- `celery-tasksetmeta-{group_id}.j` — Ergebnis-Set (serialisierte Teilergebnisse)

**Ablauf:**
1. Chord wird gestartet: `set_chord_size()` speichert die Anzahl der Header-Tasks
2. Jeder abgeschlossene Header-Task loest `on_chord_part_return()` aus
3. Dieser Callback fuehrt atomares `INCR` auf den `.t`-Schluessel aus
4. `readycount` (aktueller Zaehler) wird mit `total` (Chord-Groesse) verglichen
5. Wenn `readycount == total`: Ergebnisse werden gesammelt und der Callback wird enqueued

**Atomic Operations:** Celery nutzt Redis Pipelines fuer Batching:
```python
with client.pipeline() as pipe:
    pipe.zadd(key_j, {result: score})
    pipe.zcount(key_j, '-inf', '+inf')  # readycount
    pipe.get(key_s)                      # total
    pipe.execute()
```

**Wichtig:** Es werden **keine Lua-Scripts** verwendet — stattdessen Redis Pipelines. Fuer SwarmQ ist ein Lua-Script jedoch die bessere Wahl (siehe Abschnitt 7).

### 4.1.2 Nested Workflows

Celery unterstuetzt verschachtelte Workflows:
- Chain of Groups: `chain(group1, task, group2)`
- Group of Chains: `group(chain1, chain2, chain3)`
- Chord in Chain: `chain(task1, chord(group, callback), task3)`

Die Methode `_descend()` zaehlt Tasks rekursiv:
- `group`: Summe aller Member-Tasks
- `chain`: Zaehlt das letzte nicht-leere Element
- `chord`: Zaehlt Body-Tasks
- `signature`: Zaehlt als 1

## 4.2 Dramatiq: Group mit Completion-Callbacks

```python
g = dramatiq.group([
    add.message(1, 2),
    add.message(3, 4),
], completion_callbacks=[
    aggregate.message()
])
g.run()
```

**Mechanik:**
- Erfordert die `GroupCallbacks`-Middleware
- Erzeugt eine eindeutige `group_completion_uuid`
- Erstellt ein `Barrier`-Objekt mit der Anzahl der Group-Members
- Jede Message erhaelt `group_completion_uuid` und `group_completion_callbacks` in den Options
- Nach Task-Abschluss (nur bei `exception is None`!) dekrementiert die Middleware den Barrier-Zaehler
- Wenn Barrier cleared: Callback-Messages werden enqueued

**Einschraenkung:** Callbacks werden nur bei erfolgreichen Tasks ausgeloest. Ein fehlgeschlagener Task blockiert den Callback dauerhaft (bis TTL ablaeuft).

---

# 5. Weitere Primitives: Starmap, Chunks, Map/Filter

## 5.1 Celery: Starmap und Chunks

```python
# Starmap: Task auf Liste von Argumenten anwenden
result = add.starmap([(2, 2), (4, 4), (8, 8)])

# Chunks: Arbeit in Batches aufteilen
result = add.chunks(zip(range(100), range(100)), 10)
# Erstellt 10 Tasks mit je 10 Argument-Paaren
```

**Chunks** sind besonders nuetzlich, um den Overhead vieler kleiner Tasks zu reduzieren. Statt 1000 einzelner Messages werden z.B. 10 Messages mit je 100 Argument-Paaren erzeugt.

## 5.2 TaskIQ: Map und Filter

```python
pipe = Pipeline(broker, [])
    .call_next(get_items)
    .map(process_item)    # Parallelisiert ueber alle Items
    .filter(is_valid)     # Behaelt nur True-Ergebnisse
```

`map` und `filter` sind Pipeline-Steps, die auf die Ergebnisliste des vorherigen Steps angewendet werden — analog zu Pythons `map()` und `filter()`, aber verteilt.

---

# 6. Operator-Syntax und Signature-Pattern

## 6.1 Celery: Signature (`s()`, `si()`)

Eine Signature ist ein **serialisierbarer Task-Aufruf** — eine Beschreibung eines Tasks mit Argumenten und Optionen, die noch nicht ausgefuehrt wurde.

```python
# Mutable Signature: Ergebnis des vorherigen Tasks wird eingefuegt
sig = add.s(2, 2)     # Signature(add, args=(2, 2))

# Immutable Signature: Ignoriert eingehende Argumente
sig = add.si(2, 2)    # Signature(add, args=(2, 2), immutable=True)
```

**Internes:** `Signature` ist eine dict-Subklasse mit den Feldern:
- `task`: Task-Name
- `args`: Positionale Argumente
- `kwargs`: Keyword-Argumente
- `options`: Execution-Options (task_id, group_id, countdown, eta, etc.)
- `immutable`: Boolean, ob eingehende Argumente ignoriert werden

**Mutable vs. Immutable:**
- `_merge(new_args, new_kwargs)` bei **mutable**: neue Args werden **vorangestellt**, bestehende Kwargs werden gemerged
- `_merge()` bei **immutable**: gibt gespeicherte Args/Kwargs unveraendert zurueck
- Immutable ist wichtig fuer Callbacks in Groups, die nicht versehentlich Teilergebnisse als Argumente erhalten sollen

## 6.2 Celery: Pipe-Operator (`|`)

```python
# task | task -> chain
add.s(2, 2) | multiply.s(4)  # chain(add.s(2,2), multiply.s(4))

# group | task -> chord (automatisches Upgrade!)
group(add.s(i, i) for i in range(10)) | sum_results.s()
# Wird zu: chord([add.s(i,i)...], sum_results.s())

# chain | group -> chain mit Chord am Ende
(add.s(2, 2) | group(multiply.s(i) for i in range(3)))
```

**Implementierung:** `__or__()` auf der Signature-Klasse:
- Signature | Signature -> `chain`
- Group | Signature -> `chord` (automatische Konvertierung)
- Chain | Group -> Erweitert die Chain, letztes Group-Element wird zum Chord

## 6.3 Fuer SwarmQ: Klassen-basiertes Signature-Pattern

Da SwarmQ klassenbasierte Tasks verwendet, muss das Signature-Pattern angepasst werden:

```python
# Vorschlag fuer SwarmQ
class AddTask(Task):
    async def run(self, a: int, b: int) -> int:
        return a + b

# Signature erstellen
sig = AddTask.s(2, 2)           # Mutable
sig = AddTask.si(2, 2)          # Immutable
sig = AddTask.signature(args=(2, 2), kwargs={}, immutable=False)

# Workflows
workflow = AddTask.s(2, 2) | MultiplyTask.s(4)
workflow = group(AddTask.s(i, i) for i in range(10)) | SumTask.s()
```

Die `s()` und `si()` Methoden waeren Classmethods, die ein `Signature`-Objekt zurueckgeben.

---

# 7. Implementierungsmuster fuer SwarmQ: Workflows

## 7.1 Workflow als First-Class Object in Valkey

**Empfehlung: Ja.** Workflows sollten als eigenstaendige Objekte in Valkey gespeichert werden.

**Gruende:**
- Workflows koennen laenger laufen als einzelne Tasks
- Bei Worker-Restart muss der Workflow-Zustand erhalten bleiben
- Clients muessen den Workflow-Status abfragen koennen
- Debugging und Monitoring erfordern Einblick in den Workflow-Zustand

**Valkey-Schema:**

```
sq:workflow:{workflow_id}           -> HASH
    type: "chain" | "group" | "chord"
    status: "pending" | "running" | "completed" | "failed" | "cancelled"
    created_at: ISO-Timestamp
    updated_at: ISO-Timestamp
    tasks: JSON-Array von Task-IDs (geordnet)
    parent_workflow_id: Optional (fuer verschachtelte Workflows)
    error: Optional (Fehlermeldung bei Failure)
    result: Optional (Endergebnis)

sq:workflow:{workflow_id}:progress  -> HASH
    total: Gesamtanzahl Tasks
    completed: Abgeschlossene Tasks
    failed: Fehlgeschlagene Tasks
```

## 7.2 Chain-Implementierung mit RabbitMQ + Valkey

**Ansatz: Hybrid (Message-Header + Valkey State)**

```python
# Beim Erstellen der Chain:
# 1. Workflow in Valkey speichern
await valkey.hset(f"sq:workflow:{wf_id}", mapping={
    "type": "chain",
    "status": "pending",
    "tasks": json.dumps([task1_id, task2_id, task3_id]),
})

# 2. Ersten Task an RabbitMQ senden mit Chain-Info im Header
message_headers = {
    "sq_workflow_id": wf_id,
    "sq_chain_next": json.dumps([task2_id, task3_id]),  # Verbleibende Tasks
    "sq_chain_next_signatures": json.dumps([sig2.to_dict(), sig3.to_dict()]),
}
await channel.basic_publish(exchange, routing_key, body, properties=pika.BasicProperties(
    headers=message_headers,
))
```

**Nach Task-Abschluss im Worker:**
```python
# Worker erkennt Chain-Header und enqueued naechsten Task
if "sq_chain_next" in message.headers:
    next_signatures = json.loads(message.headers["sq_chain_next_signatures"])
    if next_signatures:
        next_sig = next_signatures[0]
        remaining = next_signatures[1:]
        # Ergebnis als erstes Argument einfuegen (wenn mutable)
        if not next_sig.get("immutable"):
            next_sig["args"] = [task_result] + next_sig.get("args", [])
        # Naechsten Task publizieren mit verbleibender Chain
        next_sig["headers"]["sq_chain_next_signatures"] = json.dumps(remaining)
        await publish_task(next_sig)
```

**Vorteile dieses Ansatzes:**
- Kein zentraler Orchestrator noetig — jeder Worker leitet selbst weiter
- Valkey speichert nur den Gesamtzustand, nicht die Ausfuehrungslogik
- Bei Worker-Crash wird der aktuelle Task via RabbitMQ Nack/Requeue wiederholt

## 7.3 Group-Completion mit Valkey INCR + Lua-Script

**Empfehlung: Lua-Script fuer atomares "Increment and Check"**

Dies ist zuverlaessiger als Celery's Pipeline-Ansatz, da alle Operationen in einem einzigen atomaren Schritt passieren:

```lua
-- Lua-Script: sq_group_complete.lua
-- KEYS[1] = sq:workflow:{wf_id}:counter
-- KEYS[2] = sq:workflow:{wf_id}:results
-- ARGV[1] = task_id
-- ARGV[2] = serialisiertes Ergebnis
-- ARGV[3] = erwartete Gesamtanzahl
-- ARGV[4] = TTL in Sekunden

-- Ergebnis speichern
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])

-- Zaehler atomar inkrementieren
local count = redis.call('INCR', KEYS[1])

-- TTL setzen (nur beim ersten Inkrement)
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[4])
    redis.call('EXPIRE', KEYS[2], ARGV[4])
end

-- Pruefen ob alle Tasks fertig
local total = tonumber(ARGV[3])
if count >= total then
    return 1  -- Group ist komplett -> Callback ausloesen
else
    return 0  -- Noch nicht fertig
end
```

**Verwendung im Worker:**
```python
is_complete = await valkey.evalsha(
    script_sha,
    2,  # Anzahl KEYS
    f"sq:workflow:{wf_id}:counter",
    f"sq:workflow:{wf_id}:results",
    task_id,
    serialized_result,
    str(total_tasks),
    str(ttl_seconds),
)

if is_complete:
    # Alle Ergebnisse sammeln
    results = await valkey.hgetall(f"sq:workflow:{wf_id}:results")
    # Chord-Callback ausloesen (falls vorhanden)
    if chord_callback:
        await publish_chord_callback(chord_callback, results)
```

**Warum Lua-Script statt Pipeline:**
- **Atomizitaet:** INCR + Vergleich in einem Schritt, keine Race-Condition moeglich
- **Effizienz:** Ein Roundtrip statt drei
- **Zuverlaessigkeit:** Kein Fenster zwischen INCR und Check, in dem ein anderer Worker ebenfalls "complete" sehen koennte

## 7.4 Chord-Implementierung

```python
# Chord = Group + Callback
# 1. Group-Tasks an RabbitMQ senden mit Chord-Info
for task_sig in header_tasks:
    headers = {
        "sq_workflow_id": wf_id,
        "sq_chord_id": chord_id,
        "sq_chord_size": len(header_tasks),
        "sq_chord_callback": json.dumps(callback_sig.to_dict()),
    }
    await publish_task(task_sig, extra_headers=headers)

# 2. Worker nach Task-Abschluss: Lua-Script ausfuehren
# 3. Wenn Lua-Script 1 zurueckgibt: Callback enqueuen
```

## 7.5 Resultat-Weitergabe zwischen Tasks

**Zwei Optionen:**

| Ansatz | Via Message Body | Via Valkey |
|---|---|---|
| Mechanik | Ergebnis direkt in den Body der naechsten Message | Ergebnis in Valkey speichern, nur Task-ID weiterreichen |
| Vorteil | Kein zusaetzlicher Valkey-Lookup | Grosse Ergebnisse belasten RabbitMQ nicht |
| Nachteil | RabbitMQ Message-Size-Limit (128MB default) | Zusaetzlicher Roundtrip zu Valkey |
| Empfehlung | **Fuer kleine Ergebnisse (<64KB)** | **Fuer grosse Ergebnisse (>64KB)** |

**Empfehlung fuer SwarmQ:** Hybridansatz mit konfigurierbarem Schwellenwert:
```python
RESULT_INLINE_THRESHOLD = 65536  # 64KB

if len(serialized_result) <= RESULT_INLINE_THRESHOLD:
    # Direkt in Message-Body der naechsten Task
    next_message.body = {"args": [result], ...}
else:
    # In Valkey speichern, Referenz weiterleiten
    await valkey.setex(f"sq:result:{task_id}", ttl, serialized_result)
    next_message.body = {"args": [{"__sq_ref__": task_id}], ...}
```

## 7.6 Fehlerbehandlung in Workflows

**Optionen:**

| Strategie | Beschreibung | Anwendungsfall |
|---|---|---|
| Fail-Fast | Beim ersten Fehler: gesamten Workflow abbrechen | Kritische Pipelines |
| Collect-All-Errors | Fehler sammeln, Workflow laeuft weiter | Batch-Verarbeitung |
| Partial Results | Erfolgreiche Ergebnisse + Fehlerliste an Callback | Chord mit toleranten Callbacks |

**Empfehlung fuer SwarmQ:** Konfigurierbar pro Workflow, Default = Collect-All-Errors fuer Groups, Fail-Fast fuer Chains.

```python
class WorkflowPolicy(Enum):
    FAIL_FAST = "fail_fast"           # Abbruch bei erstem Fehler
    COLLECT_ERRORS = "collect_errors"  # Weiterlaufen, Fehler sammeln
    PARTIAL_RESULTS = "partial"        # Teilergebnisse + Fehler an Callback

# Beispiel
workflow = group(
    [task1.s(), task2.s(), task3.s()],
    error_policy=WorkflowPolicy.COLLECT_ERRORS,
) | aggregate.s()
```

## 7.7 State-Machine fuer Workflow-Ausfuehrung

```
PENDING  ──>  RUNNING  ──>  COMPLETED
                │                 
                ├──>  FAILED      
                │                 
                └──>  CANCELLED   

Zusaetzliche Zustaende fuer Chord:
RUNNING  ──>  HEADER_COMPLETE  ──>  CALLBACK_RUNNING  ──>  COMPLETED
```

Zustandsuebergaenge werden atomar in Valkey gespeichert:
```python
# Atomarer Zustandsuebergang mit Lua-Script
-- KEYS[1] = sq:workflow:{wf_id}
-- ARGV[1] = erwarteter aktueller Status
-- ARGV[2] = neuer Status
local current = redis.call('HGET', KEYS[1], 'status')
if current == ARGV[1] then
    redis.call('HSET', KEYS[1], 'status', ARGV[2], 'updated_at', ARGV[3])
    return 1
else
    return 0  -- Zustandsuebergang abgelehnt
end
```

---

# 8. Task-Cancellation / Revocation

## 8.1 Celery: Revoke

```python
# Einfache Revocation
result.revoke()

# Mit Terminierung eines laufenden Tasks
result.revoke(terminate=True, signal='SIGTERM')

# Batch-Revocation
app.control.revoke([id1, id2, id3])
```

**Interne Mechanik:**
- Revoked Task-IDs werden in `worker_state.revoked` gespeichert (In-Memory Set auf jedem Worker)
- **Zusaetzlich** wird der Task im Backend als `REVOKED` markiert: `backend.mark_as_revoked(task_id)`
- Broadcast-Mechanismus: `revoke`-Command wird an **alle Worker** gesendet
- Worker pruefen vor Task-Ausfuehrung, ob die `task_id` in `revoked` liegt
- Bei `terminate=True`: Laufender Task wird via `request.terminate(signal)` beendet
- Signal-Konvertierung: `_signals.signum('KILL')` wandelt Signal-Namen in Nummern um

**Schwaechen:**
- In-Memory-Set geht bei Worker-Restart verloren (Race-Condition-Fenster)
- Kein RabbitMQ-seitiges Entfernen der Message moeglich
- Broadcast erfordert zusaetzliche Control-Queue

## 8.2 arq: Job Abort

```python
# Worker-Konfiguration
class WorkerSettings:
    allow_abort_jobs = True

# Client-seitig
job = await arq_redis.enqueue_job('my_task', ...)
await job.abort()
```

**Interne Mechanik:**
- Abort-IDs werden in einem Redis Sorted Set `abort_jobs_ss` gespeichert (mit Timestamp)
- Worker prueft in jedem Poll-Zyklus via `_cancel_aborted_jobs()`:
  1. Liest IDs aus `abort_jobs_ss`
  2. Sucht den zugehoerigen `asyncio.Task`
  3. Ruft `task.cancel()` auf — loest `asyncio.CancelledError` aus
- Abgebrochene Jobs werden in `self.aborting_tasks` Set getrackt
- Worker unterscheidet zwischen User-Abort und anderen CancelledErrors
- Aufraeum: Abgelaufene Abort-Eintraege (aelter als `abort_job_max_age`) werden geloescht

**Stärken:**
- Nutzt `asyncio.Task.cancel()` — saubere cooperative Cancellation
- Redis Sorted Set ueberlebt Worker-Restarts
- Einfaches Design

## 8.3 SAQ: Job Abort

```python
job = await queue.job(job_id)
await job.abort("Cancelled by user")
```

**Interne Mechanik:**
- Job wird in Status `ABORTING` versetzt
- Job wird aus den Queued/Incomplete Sets entfernt
- Falls der Job noch nicht gestartet wurde: sofort `ABORTED`
- Falls der Job laeuft: Worker wird via internem Signal unterbrochen
- Falls Retry moeglich: Job wird erneut eingereiht, sonst `ABORTED`
- Abort-ID wird unter `saq:abort:{job_key}` in Redis gespeichert

**Pub/Sub fuer Status-Updates:**
```python
await self.redis.publish(job.id, job.status)
# Client kann auf Status-Aenderungen lauschen
```

## 8.4 RQ: Job Cancellation

```python
from rq import cancel_job
cancel_job(job_id, connection=redis_conn)

# Oder direkt
job = Job.fetch(job_id, connection=redis_conn)
job.cancel()
```

**Interne Mechanik:**
- Job wird zur `CanceledJobRegistry` hinzugefuegt
- Job wird aus der aktuellen Registry entfernt (Queue, Started, Deferred, Scheduled)
- Job-Hash und Dependencies werden aus Redis geloescht
- Fuer **laufende** Jobs: `send_stop_job_command(connection, job_id)` sendet ein Signal an den Worker
- Verwendet Redis WATCH fuer optimistisches Locking bei Concurrent Updates
- Neun Job-Zustaende: CREATED, QUEUED, STARTED, FINISHED, FAILED, DEFERRED, SCHEDULED, STOPPED, CANCELED

## 8.5 RabbitMQ: Nachrichten-Entfernung aus Queue

**Wichtige Erkenntnis:** RabbitMQ unterstuetzt **kein Entfernen einzelner Nachrichten anhand einer ID**. Es gibt keinen `basic.delete_by_id`-Befehl.

**Moegliche Workarounds:**
- `basic.reject(requeue=False)` + Dead Letter Exchange: Consumer muesste die Nachricht erst empfangen
- Consumer, der Messages durchsucht: Ineffizient und nicht atomar
- Loesung: **Nicht auf RabbitMQ-Ebene abbrechen**, sondern Cancellation-Flag in Valkey speichern

---

# 9. Implementierungsmuster fuer SwarmQ: Cancellation

## 9.1 Empfohlene Architektur

**Zwei-Phasen-Cancellation:**

1. **Queued Tasks (noch nicht gestartet):** Worker prueft beim Dequeue das Cancellation-Flag in Valkey. Falls gesetzt: Task sofort als CANCELLED markieren, RabbitMQ Message ACKen.
2. **Running Tasks (in Ausfuehrung):** `asyncio.Task.cancel()` auf den laufenden Task anwenden.

```
Client                    Valkey                    Worker
  │                         │                         │
  │ cancel(task_id)         │                         │
  │────────────────────────>│                         │
  │  SADD sq:cancel task_id │                         │
  │  HSET sq:task:{id}      │                         │
  │    status=CANCELLING    │                         │
  │                         │                         │
  │                         │   poll sq:cancel        │
  │                         │<────────────────────────│
  │                         │   SISMEMBER -> True     │
  │                         │────────────────────────>│
  │                         │                         │
  │                         │   asyncio.Task.cancel() │
  │                         │   (CancelledError)      │
  │                         │                         │
  │                         │   HSET status=CANCELLED │
  │                         │<────────────────────────│
  │                         │   SREM sq:cancel id     │
  │                         │<────────────────────────│
  │                         │   PUBLISH sq:events id  │
  │                         │<────────────────────────│
```

## 9.2 Valkey-Datenstrukturen fuer Cancellation

```python
# Cancellation-Flag setzen (Client-Seite)
async def cancel_task(task_id: str) -> bool:
    """Bricht einen Task ab. Gibt True zurueck wenn erfolgreich."""
    pipe = valkey.pipeline()
    pipe.sadd("sq:cancel", task_id)
    pipe.hset(f"sq:task:{task_id}", mapping={
        "status": "CANCELLING",
        "cancelled_at": datetime.utcnow().isoformat(),
    })
    pipe.publish(f"sq:events:{task_id}", "CANCELLING")
    await pipe.execute()
    return True

# Cancellation pruefen (Worker-Seite, vor Task-Start)
async def is_cancelled(task_id: str) -> bool:
    return await valkey.sismember("sq:cancel", task_id)

# Cancellation pruefen (Worker-Seite, waehrend Ausfuehrung)
# Wird periodisch aufgerufen (alle 1-2 Sekunden)
async def check_cancellation_loop(task_id: str, asyncio_task: asyncio.Task):
    while not asyncio_task.done():
        if await valkey.sismember("sq:cancel", task_id):
            asyncio_task.cancel()
            return
        await asyncio.sleep(1.0)
```

## 9.3 Cooperative vs. Forceful Cancellation

| Methode | Mechanik | Vorteil | Nachteil |
|---|---|---|---|
| `asyncio.Task.cancel()` | CancelledError bei naechstem `await` | Sauber, erlaubt Cleanup | Task muss `await`-Punkte haben |
| Cancellation-Flag pruefen | Task prueft `self.is_cancelled()` | Volle Kontrolle | Task muss aktiv pruefen |
| SIGTERM (Prozess) | Nur bei Subprocess-Workers | Auch blocking I/O | Kein graceful Cleanup |

**Empfehlung fuer SwarmQ:**
- **Primaer:** `asyncio.Task.cancel()` — funktioniert bei allen async Tasks automatisch
- **Sekundaer:** `self.is_cancelled` Property im Task fuer feingranulare Kontrolle
- **Kein SIGTERM:** SwarmQ ist rein async, keine Subprocess-Worker

```python
class Task:
    @property
    def is_cancelled(self) -> bool:
        """Prueft ob der Task abgebrochen werden soll.
        Fuer Tasks mit langen Schleifen ohne await-Punkte."""
        return self._cancel_flag.is_set()

    async def run(self):
        for item in huge_list:
            if self.is_cancelled:
                return  # Graceful exit
            await self.process(item)
```

## 9.4 Race-Conditions

**Problem:** Task startet genau zwischen Cancel-Request und Worker-Poll.

**Loesung:** Worker prueft Cancellation-Flag **immer** direkt vor `task.run()`:

```python
async def execute_task(message):
    task_id = message.headers["task_id"]

    # Check 1: Vor dem Start
    if await is_cancelled(task_id):
        await mark_as_cancelled(task_id)
        await channel.basic_ack(delivery_tag)
        return

    # Task ausfuehren mit Cancellation-Monitor
    asyncio_task = asyncio.create_task(task.run(*args, **kwargs))
    monitor = asyncio.create_task(check_cancellation_loop(task_id, asyncio_task))

    try:
        result = await asyncio_task
    except asyncio.CancelledError:
        await mark_as_cancelled(task_id)
    finally:
        monitor.cancel()
```

## 9.5 Workflow-Cancellation

Beim Abbruch eines Workflows muessen alle zugehoerigen Tasks abgebrochen werden:

```python
async def cancel_workflow(workflow_id: str):
    """Bricht einen gesamten Workflow und alle seine Tasks ab."""
    task_ids = json.loads(await valkey.hget(f"sq:workflow:{workflow_id}", "tasks"))
    pipe = valkey.pipeline()
    for task_id in task_ids:
        pipe.sadd("sq:cancel", task_id)
        pipe.hset(f"sq:task:{task_id}", "status", "CANCELLING")
    pipe.hset(f"sq:workflow:{workflow_id}", "status", "CANCELLED")
    await pipe.execute()
```

## 9.6 Cancelled Tasks und DLQ

**Empfehlung: Cancelled Tasks gehoeren NICHT in die Dead Letter Queue.**

- DLQ ist fuer unerwartete Fehler (Exceptions, Timeouts, Gift-Nachrichten)
- Cancellation ist ein **erwarteter** Zustandsuebergang
- Cancelled Tasks erhalten den Status `CANCELLED` und werden normal in Valkey gespeichert
- Optional: TTL fuer Cancelled-Tasks kuerzer als fuer Failed-Tasks

```python
class TaskStatus(Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    CANCELLED = "CANCELLED"    # Eigener Status, nicht DLQ
    CANCELLING = "CANCELLING"  # Uebergangszustand
```

---

# 10. Progress-Tracking

## 10.1 Celery: update_state()

```python
class MyTask(celery.Task):
    def run(self, total):
        for i in range(total):
            self.update_state(
                state='PROGRESS',
                meta={'current': i, 'total': total, 'percent': i/total*100}
            )
            do_work(i)

# Client-Seite
result = MyTask.delay(100)
print(result.state)  # 'PROGRESS'
print(result.info)   # {'current': 42, 'total': 100, 'percent': 42.0}
```

**Interne Mechanik:**
- `update_state()` ruft `backend.store_result(task_id, meta, state)` auf
- Beliebige Custom-States moeglich (nicht auf eingebaute beschraenkt)
- Client pollt via `result.state` und `result.info`
- Backend speichert Status + Meta als ein Objekt
- **Kein Pub/Sub** — nur Polling

**Eingebaute States:** PENDING, STARTED, SUCCESS, FAILURE, RETRY, REVOKED

## 10.2 SAQ: job.update()

```python
async def my_task(ctx, *, job):
    for i in range(100):
        await job.update(progress=i/100)  # Float 0.0 - 1.0
        await do_work(i)
```

**Interne Mechanik:**
- `progress` ist ein Float-Feld im Job-Dataclass (0.0 bis 1.0)
- `job.update()` serialisiert den gesamten Job und speichert ihn in Redis (`SET job.id serialized_job`)
- **Pub/Sub-Benachrichtigung:** `redis.publish(job.id, job.status)` nach jedem Update
- `PubSubMultiplexer` routet Nachrichten in-process an mehrere Listener
- Pattern-basiertes Subscribing fuer alle Job-Channels

## 10.3 RQ: Heartbeat

```python
class MyJob:
    def run(self):
        # Kein echtes Progress-Tracking, nur Heartbeat
        job = get_current_job()
        job.heartbeat(datetime.utcnow(), 0, 100)
```

RQ hat kein eingebautes Progress-Tracking — nur einen Heartbeat-Mechanismus (`last_heartbeat` Timestamp in Redis Hash), der erkennen laesst, ob ein Job noch aktiv ist.

---

# 11. Implementierungsmuster fuer SwarmQ: Progress

## 11.1 API-Design

```python
class Task:
    async def update_progress(
        self,
        current: int,
        total: int,
        message: str = "",
    ) -> None:
        """Aktualisiert den Fortschritt des Tasks.
        
        Automatisches Throttling: max 1 Update pro 500ms.
        """
        ...

    async def run(self):
        items = await self.fetch_items()
        for i, item in enumerate(items):
            await self.update_progress(i, len(items), f"Verarbeite {item.name}")
            await self.process(item)

# Client-Seite: Polling
progress = await broker.get_progress(task_id)
# -> ProgressInfo(current=42, total=100, percent=42.0, message="Verarbeite Item X", eta_seconds=58)

# Client-Seite: Async Iterator (Pub/Sub)
async for progress in broker.watch_progress(task_id):
    print(f"{progress.percent:.1f}% — {progress.message}")
```

## 11.2 Valkey-Datenstrukturen

```python
# Progress-Daten in Valkey HASH
KEY = f"sq:progress:{task_id}"

# Felder:
{
    "current": "42",
    "total": "100",
    "percent": "42.0",
    "message": "Verarbeite Item X",
    "updated_at": "2026-04-15T10:30:00Z",
    "started_at": "2026-04-15T10:29:00Z",
    "eta_seconds": "58",        # Geschaetzte Restzeit
}

# TTL: Gleich wie Task-Result TTL (z.B. 24h)
```

**Warum HASH statt JSON in STRING:**
- Einzelne Felder koennen gelesen werden ohne Deserialisierung
- Atomare Updates einzelner Felder mit HSET
- Effizienterer Speicher fuer strukturierte Daten

## 11.3 Pub/Sub fuer Echtzeit-Updates

```python
# Worker-Seite: Nach Progress-Update
async def _publish_progress(self, task_id: str, progress: dict):
    await self._valkey.publish(
        f"sq:progress:channel:{task_id}",
        json.dumps(progress),
    )

# Client-Seite: watch_progress als AsyncIterator
async def watch_progress(self, task_id: str) -> AsyncIterator[ProgressInfo]:
    pubsub = self._valkey.pubsub()
    await pubsub.subscribe(f"sq:progress:channel:{task_id}")
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = json.loads(message["data"])
                yield ProgressInfo(**data)
    finally:
        await pubsub.unsubscribe()
        await pubsub.close()
```

## 11.4 Throttling: Rate-Limiting fuer Progress-Updates

**Problem:** Tasks in engen Schleifen koennen Valkey mit Tausenden Updates pro Sekunde ueberfluten.

**Loesung: Client-seitiges Throttling im Task:**

```python
class Task:
    _PROGRESS_MIN_INTERVAL = 0.5  # Sekunden

    async def update_progress(self, current: int, total: int, message: str = ""):
        now = asyncio.get_event_loop().time()
        if now - self._last_progress_update < self._PROGRESS_MIN_INTERVAL:
            # Zu frueh — Update ueberspringen (ausser es ist das letzte)
            if current < total:
                return
        self._last_progress_update = now

        percent = (current / total * 100) if total > 0 else 0
        elapsed = now - self._task_start_time
        eta = (elapsed / current * (total - current)) if current > 0 else None

        progress = {
            "current": str(current),
            "total": str(total),
            "percent": f"{percent:.1f}",
            "message": message,
            "updated_at": datetime.utcnow().isoformat(),
            "eta_seconds": str(int(eta)) if eta else "",
        }

        pipe = self._valkey.pipeline()
        pipe.hset(f"sq:progress:{self._task_id}", mapping=progress)
        pipe.expire(f"sq:progress:{self._task_id}", self._result_ttl)
        pipe.publish(f"sq:progress:channel:{self._task_id}", json.dumps(progress))
        await pipe.execute()
```

**Verhalten:**
- Maximal 2 Updates pro Sekunde (konfigurierbar)
- Das letzte Update (current == total) wird **immer** gesendet
- ETA-Berechnung basierend auf bisheriger Ausfuehrungszeit
- Pipeline fuer HSET + EXPIRE + PUBLISH in einem Roundtrip

## 11.5 ProgressInfo Datenklasse

```python
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True, slots=True)
class ProgressInfo:
    current: int
    total: int
    percent: float
    message: str = ""
    eta_seconds: Optional[int] = None
    updated_at: Optional[str] = None
```

---

# 12. Zusammenfassung und Empfehlungen

## 12.1 Workflow-Primitives

| Entscheidung | Empfehlung | Begruendung |
|---|---|---|
| Workflows in Valkey? | **Ja** | Persistenz, Monitoring, Worker-Restart-Sicherheit |
| Signature-Pattern | `Task.s()` / `Task.si()` als Classmethods | Konsistent mit klassenbasiertem Task-Design |
| Chain-Mechanik | Message-Header mit verbleibenden Signatures | Dezentral, kein Orchestrator noetig |
| Group-Completion | Valkey Lua-Script (INCR + Check) | Atomar, keine Race-Conditions |
| Chord-Completion | Lua-Script gibt 1 zurueck -> Worker enqueued Callback | Einfach, zuverlaessig |
| Resultat-Weitergabe | Hybrid: inline (<64KB) oder Valkey-Referenz | Balance zwischen Latenz und Message-Groesse |
| Error-Policy | Konfigurierbar (Fail-Fast / Collect-Errors) | Flexibel fuer verschiedene Anwendungsfaelle |
| Operator-Syntax | `\|` fuer chain, `group() \| task` fuer chord | Celery-kompatible, intuitive API |

## 12.2 Task-Cancellation

| Entscheidung | Empfehlung | Begruendung |
|---|---|---|
| Cancellation-Mechanik | `asyncio.Task.cancel()` + Valkey Flag | Natuerlich fuer async, persistent |
| Speicherort Flags | Valkey SET `sq:cancel` | Ueberlebt Worker-Restarts |
| Queued Tasks | Flag-Check vor Task-Start | Kein RabbitMQ-seitiges Entfernen moeglich |
| Running Tasks | Cancel-Monitor-Coroutine | Periodischer Check (1s Intervall) |
| Cancelled -> DLQ? | **Nein** | Eigener Status CANCELLED, kein Fehler |
| Workflow-Cancel | Alle Task-IDs des Workflows canceln | Atomare Pipeline-Operation |
| Benachrichtigung | Pub/Sub Event | Client erhaelt sofortige Rueckmeldung |

## 12.3 Progress-Tracking

| Entscheidung | Empfehlung | Begruendung |
|---|---|---|
| Storage | Valkey HASH `sq:progress:{task_id}` | Einzelne Felder aktualisierbar |
| Echtzeit | Pub/Sub Channel pro Task | Sofortige Benachrichtigung |
| Throttling | Max 1 Update / 500ms (konfigurierbar) | Valkey-Last begrenzen |
| Client-API | `get_progress()` + `watch_progress()` AsyncIterator | Polling und Echtzeit |
| Datenstruktur | `{current, total, percent, message, eta_seconds}` | Umfassend, aber kompakt |
| TTL | Gleich wie Task-Result | Konsistentes Cleanup |

## 12.4 Priorisierung der Implementierung

1. **Phase 1 (MVP):** Chain + einfache Cancellation + Progress-Tracking
2. **Phase 2:** Group + Chord (Lua-Script) + Workflow-Cancellation
3. **Phase 3:** Operator-Syntax + Nested Workflows + Starmap/Chunks
