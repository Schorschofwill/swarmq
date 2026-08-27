---
title: SwarmQ 0.2.0 — Verträge, Modulschnitt und Durchspiel
date: 2026-08-27
status: entwurf-zur-umsetzung
plan: docs/plans/2026-08-26-001-refactor-restrukturierung-0-2-0-plan.md
unit: U1
covers: R1, R6, R12, R16; AE1 (Durchspiel-Teil); KTD2–KTD6, KTD12
---

# SwarmQ 0.2.0 — Verträge, Modulschnitt und Durchspiel

## Frage

Welche Broker-/Backend-Verträge, welcher Modulschnitt und welche
Invarianten-Mechanismen tragen die 0.2.0-Restrukturierung — und erfüllt ein
Valkey-Streams-Broker (`aufgabe.md` §2.4, XACK/XCLAIM, kein Nack/Requeue)
jede Kernvertragsmethode, ohne dass eine Erweiterung nötig wird?

Faktenbasis: `src/swarmq/` auf Stand `v0.1.2` (Commit `2c622b7`), verifiziert
am 2026-08-27 durch vollständige Lesung von `broker/rabbitmq.py`,
`backend/valkey.py`, `worker.py`, `client.py`, `scheduler.py`, `locking.py`,
`limits.py`, `workflow_orchestration.py`, `health.py`, `process_manager.py`,
`headers.py`, `serialization.py` und `lua/*.lua`. Zeilenangaben beziehen
sich auf diesen Stand.

Zeitbox: Phase 0 endet mit dem Merge dieses Dokuments (KTD13). Die
Programm-Zeitbox von zwölf Wochen läuft ab dem Tag `v0.1.2`
(gesetzt am 2026-08-27) bis **2026-11-19**.

---

## 1. Befund: Ausgangslage

| Aspekt | Heute (`v0.1.2`) |
|---|---|
| Broker-Vertrag | keiner; `RabbitMQBroker` wird in `Worker.__init__` (worker.py:122), `Client.__init__` (client.py:116), `HealthChecker.__init__` (health.py:66) als Typ referenziert |
| Broker-Verben | `ack(message)` (rabbitmq.py:506), `nack(message, requeue=False)` (:509), `publish_to_delay_queue` (:434), `publish_to_dead_letter_queue` (:449); `publish` gibt `None` zurück (:426) |
| aio_pika im Kern | vier Signaturen in `worker.py` tragen `aio_pika.abc.AbstractIncomingMessage` (:219, :328, :457, :2610); sieben weitere Methoden sind untypisiert, erwarten aber dasselbe Objekt |
| Backend-Vertrag | keiner; 12 Methoden wiederholen das `op_timeout`-Muster mit 14 `wait_for`-Aufrufen, 32× `except (ValkeyError, OSError)`, 39× `raise BackendConnectionError` |
| Rohclient-Durchreichung | `Worker.start` baut `RateLimiter(backend._client)` (:777), `LockManager(backend._client)` (:780), `ChordManager(backend._client)` (:786); `ScheduleStore.__init__` greift auf `backend._client` (scheduler.py:193), `remove_schedule` auf `_client.hexists` (:262); `HealthChecker` auf `_client.ping()` (health.py:107) und `_connection.ready()` (:124) |
| Lua-Eigentum | 8 von 9 Skripten gehören Klassen außerhalb von `backend/` (§7) |
| Queue-Quelle im Worker | ausschließlich `payload["task"]["queue"]` (worker.py:1736); `routing_key` wird nie gelesen — Retry/Delay/DLQ folgen dem Body, nicht dem Zustellungs-Band |
| Terminal-Flags | `_unique_release_pending`/`_schedfire_terminal` mit Default `True` (:1811, :1819), acht paarweise Reset-Stellen (:1830, :1856, :1872, :1890, :2250, :2337, :2406, :2419), Auswertung im `finally` (:2451, :2468) |
| publish-then-mark | Kommentar + Dict-Zuweisung nach `await self._publish(...)` (workflow_orchestration.py:599–600); kein Typ erzwingt die Reihenfolge |

---

## 2. Vertragsvokabular

Konventionen: alle Verträge sind `typing.Protocol` (KTD1), alle Methoden
`async` außer den reinen Namens-/Konstruktionsfunktionen. Kein Methodenname
und kein Parameter nennt AMQP- oder Valkey-Primitive. Für jede Methode
stehen zwei Zeilen: **RMQ** (heutige Erfüllung durch RabbitMQ) und
**Streams** (Erfüllung durch einen Valkey-Streams-Broker; Klasse
„erfüllbar" oder „nur Erweiterung").

### 2.1 `contracts/message.py`

```python
@dataclass(frozen=True, slots=True)
class Delivery:
    body: bytes
    headers: Mapping[str, Any]      # normalisierte Schlüssel, Werte wie zugestellt
    queue: str                      # Queue, aus der zugestellt wurde — inkl. Prioritäts-Suffix
    handle: object                  # broker-privates Zustellungs-Handle (opak für den Kern)

@dataclass(frozen=True, slots=True)
class PublishReceipt:
    queue: str
    message_id: str | None          # broker-seitige Kennung, falls vorhanden
```

- `Delivery.queue` kommt aus der Zustellung (RMQ: `routing_key` des
  Consumers bzw. der Queue-Name, den `consume` registriert hat; Streams:
  der Stream-Key), **nicht** aus dem Body. Das ist die Grundlage der
  einzigen zugelassenen Verhaltenskorrektur (Band-Erhalt beim Retry,
  KTD2, wirksam ab U8).
- `handle` ist opak: der Kern reicht es nur an die Verben zurück. Der
  Broker hält keine Zuordnungstabelle (Entscheidung zur offenen Frage
  „Handle am Objekt oder Zuordnung im Broker" aus dem Plan: **am Objekt**,
  weil es die `id(message)`-Dictionaries des heutigen Workers wie
  `_schedfire_inflight_claims[id(message)]` (worker.py:1391) überflüssig
  macht und der Stub keinen Lebenszyklus verwalten muss).
- `PublishReceipt` ist absichtlich klein: es ist ein *Beweis*, dass ein
  Publish stattgefunden hat (publish-then-mark, §4), kein Confirm-Objekt.

### 2.2 `contracts/broker.py`

**`Publisher`** — der Client braucht nur diesen Teil.

| Methode | Zusage | RMQ | Streams |
|---|---|---|---|
| `connect() -> None` | idempotent; Treiberfehler → `BrokerConnectionError` | `connect_robust` + zwei Kanäle (rabbitmq.py:134) | Client-Aufbau; erfüllbar |
| `close() -> None` | schließt nur Eigenes (geliehene Verbindung bleibt) | rabbitmq.py:512 | erfüllbar |
| `ping() -> None` | wirft `BrokerConnectionError`, wenn nicht verbunden/nicht bereit | `connection.ready()` mit Timeout — heute in health.py:118–124 | `PING`; erfüllbar |
| `declare_queue(queue: str) -> None` | macht `queue` samt Verzögerungs- und Dead-Letter-Ziel zustellbereit; idempotent | Tripel `q`/`q.DQ`/`q.XQ` (rabbitmq.py:388) | `XGROUP CREATE … MKSTREAM` für `q`, Delay-ZSET `q:delayed`, Stream `q:dead`; erfüllbar |
| `publish(queue: str, body: bytes, headers: Mapping[str, Any] \| None = None) -> PublishReceipt` | persistente, bestätigte Veröffentlichung; bei Fehlschlag `BrokerConnectionError`, kein stiller Verlust | `_publish_with_retry` auf dem Confirms-Kanal (rabbitmq.py:263); Receipt trägt `queue` und `message_id` (heute `None`) | `XADD q * body … headers …`; Receipt trägt die Stream-ID; erfüllbar |

Anmerkung zu `publish`: heute akzeptiert `publish(queue, message, **properties)`
beliebige aio_pika-Properties (rabbitmq.py:426). Im Vertrag gibt es nur
`headers`. Die einzigen genutzten Properties im Kern sind `headers` und
`expiration` (letzteres nur intern in `retry_later`); Aufrufer, die heute
`**properties` nutzen, gibt es außerhalb des Brokers nicht.

**`Consumer`** — die Worker-Seite; vier Verben auf `Delivery` (KTD2).

| Methode | Zusage | RMQ | Streams |
|---|---|---|---|
| `consume(queue: str, on_delivery: Callable[[Delivery], Awaitable[None]], *, concurrency: int) -> None` | liefert jede Nachricht genau einmal als `Delivery` an den Callback; höchstens `concurrency` unbestätigte Zustellungen gleichzeitig; der Callback darf nicht blockieren | `set_qos(prefetch_count)` + `queue.consume` (rabbitmq.py:462); Message → `Delivery(body, normalisierte Header, queue, handle=message)` | `XREADGROUP GROUP g consumer COUNT n BLOCK …`; `concurrency` = Fenster unbestätigter Einträge (`XPENDING`-Länge), softwareseitig durchgesetzt; erfüllbar |
| `cancel_consume() -> None` | keine weiteren Zustellungen; bereits zugestellte bleiben bestätigbar | rabbitmq.py:475 | Leseschleife stoppen; Pending bleibt pending; erfüllbar |
| `ack(delivery) -> None` | Zustellung ist erledigt; kein weiteres Verb mehr zulässig | `message.ack()` (:506) | `XACK q g id`; erfüllbar |
| `redeliver(delivery) -> None` | transient: dieselbe Nachricht wird erneut zugestellt (an irgendeinen Consumer), ohne Verzögerung, ohne Header-Änderung | `message.nack(requeue=True)` (:509) | Eintrag bleibt in der PEL; erneute Zustellung über `XAUTOCLAIM`/`XCLAIM` durch denselben oder einen anderen Consumer nach `min-idle-time`; alternativ `XACK` + `XADD` derselben Felder (verliert die Zähler der PEL nicht, weil Streams keine Zähler-Header haben — der Kern nutzt ausschließlich `x-swarmq-*`-Header). Erfüllbar |
| `retry_later(delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None` | veröffentlicht `body` + `headers` an das Verzögerungsziel **der Zustellungs-Queue** (`delivery.queue`), das die Nachricht nach `delay_ms` wieder in genau diese Queue liefert; danach bestätigt es das Original. Schlägt die Veröffentlichung fehl → `BrokerConnectionError`, Original bleibt **unbestätigt** | `publish_to_delay_queue(delivery.queue, body, delay_ms, headers)` mit Per-Message-`expiration` + `ack` (heute worker.py:401–407, :693–699, :2688–2691) | `ZADD q:delayed <now+delay_ms> <payload>`; ein Mover (jeder Worker beim Poll oder ein Leader-Tick) verschiebt fällige Einträge per Lua atomar `ZRANGEBYSCORE` + `XADD` + `ZREM`; dann `XACK`. Erfüllbar — Verzögerung ist ein ZSET, kein Broker-Feature |
| `dead_letter(delivery, *, headers: Mapping[str, Any]) -> None` | veröffentlicht `body` + `headers` an das Dead-Letter-Ziel der Zustellungs-Queue, dann bestätigt es das Original. Publish-Fehler → `BrokerConnectionError`, Original unbestätigt | `publish_to_dead_letter_queue(delivery.queue, …)` + `ack` (heute worker.py:312–315, :522–525, :655–658, :2774–2777) | `XADD q:dead * …` + `XACK`; erfüllbar |

**`Broker`** = `Publisher & Consumer` (ein Protocol, das beide erbt).

**`BrokerAdmin`** — optionaler Erweiterungsvertrag (KTD3). Konsumenten:
`swarmq dlq`, Metrics-Sampler (`worker.py:1054/:1058`,
`process_manager.py:874`), Health. Der Kern referenziert ihn nie.

| Methode | RMQ heute | Streams |
|---|---|---|
| `list_dead_letters(queue, limit) -> tuple[list[dict], int]` | rabbitmq.py:759 | `XRANGE q:dead`; erfüllbar (ist aber Erweiterung — zulässig) |
| `inspect_dead_letter(queue, task_id, scan_limit)` | :791 | `XRANGE` + Filter |
| `retry_dead_letter(queue, task_id, scan_limit) -> tuple[bool, int]` | :812 (strippt `x-swarmq-retry-count`, :720–722) | `XADD q` + `XDEL q:dead` |
| `retry_all_dead_letters(queue, limit) -> tuple[int, int]` | :838 | dito in Schleife |
| `purge_dead_letters(queue) -> int` | :861 | `DEL q:dead` |
| `queue_depth(queue) -> int` | :874 (Main + `.DQ`, ohne `.XQ`) | `XLEN q` + `ZCARD q:delayed` |
| `dead_letter_depth(queue) -> int` | :947 | `XLEN q:dead` |
| `route_via_dlx(delivery) -> None` | `message.nack(requeue=False)` — Broker-Konfiguration leitet nach `q.XQ`, RabbitMQ ergänzt `x-death`, **keine** `x-swarmq-*`-Diagnose | Streams haben kein DLX: die Implementierung wäre `dead_letter(delivery, headers=delivery.headers)` — deshalb liegt das Verb bewusst **außerhalb** des Kernvertrags |

### 2.3 `contracts/backend.py` — `ResultBackend`

Vokabular ist store-neutral; die heutigen Namen bleiben bis U15 als Aliase
auf der `ValkeyBackend`-Fassade (KTD10). Spalte „Alias" = heutiger Name.

| Methode | Zusage | Alias (valkey.py) |
|---|---|---|
| `connect()`, `close()`, `ping()` | idempotent; `ping` vor `connect` wirft `BackendConnectionError`; Treiberfehler gewrappt | :111, :135; `ping` neu (heute health.py:107) |
| `store_result(task_id, data: bytes, ttl_ms, *, op_timeout=None)` | schreibt mit TTL | `store_result` :197 |
| `get_result(task_id) -> bytes \| None` | | `get_result` :219 |
| `get_results(task_ids) -> list[bytes \| None]` | ein Round-Trip | `get_results_batch` :237 |
| `delete_result(task_id)` | | :225 |
| `notify_result(task_id)` | weckt Wartende; Fehler werden geloggt, nie geworfen | `publish_notification` :231 |
| `wait_for_result(task_id, *, timeout: float) -> bytes \| None` | liefert das Ergebnis, sobald vorhanden; wacht bei `notify_result` auf, fällt sonst auf Polling zurück; `None` bei Ablauf. Ersetzt das geleakte Pub/Sub-Objekt | neu — heute `Client.get_result` inline (client.py:460–554) mit hart kodiertem Kanal `sq:notify:{id}` (:503) |
| `set_progress(task_id, fields: Mapping[str,str], ttl_ms, *, op_timeout=None)`, `get_progress(task_id, *, op_timeout=None) -> Mapping[bytes,bytes] \| None` | | :264, :297 |
| `publish_progress(task_id, payload: bytes)` | Fehler geloggt, nie geworfen | :319 |
| `watch_progress(task_id) -> AsyncIterator[bytes]` | liefert Fortschritts-Payloads, bis der Aufrufer abbricht; überlebt Verbindungsabbrüche durch erneutes Abonnieren (toxiproxy-Spike 2026-04-17) | neu — heute `Client.watch_progress` inline (client.py:581–686, Resubscribe :646–661) |
| `set_cancel_flag(task_id, ttl_ms) -> bool`, `is_cancelled(task_id) -> bool`, `clear_cancel_flag(task_id)` | `clear` loggt statt wirft | :338, :369, :608 |
| `store_workflow_state(wf_id, data, ttl_ms)`, `get_workflow_state(wf_id)`, `set_workflow_cancel_flag(wf_id, ttl_ms) -> bool`, `is_workflow_cancelled(wf_id) -> bool`, `notify_workflow(wf_id)`, `wait_for_workflow(wf_id, *, timeout) -> bool` | | :409, :429, :446, :471, `publish_workflow_notification` :489, neu (heute `WorkflowHandle.get` inline, workflow_orchestration.py:174–226) |
| `claim_unique(dedup_hash, owner: str, ttl_ms) -> str \| None` | `None` = beansprucht, sonst der bestehende Owner | `setnx_unique` :506 |
| `release_unique(dedup_hash, owner: str) -> bool` | Owner-Check; Fehler → `False` (heute so, :579–605) | `release_unique` :552 |

Nicht im Kernvertrag, sondern in `ScheduleStore` (§2.4): die 15
Schedule-/Schedfire-Primitive `zadd_schedule` … `get_scheduler_leader`
(valkey.py:640–904). `subscribe_and_wait` (:846) und `zrangebyscore_schedules`
(:656) haben keine Aufrufer in `src/` und entfallen ohne Alias (Bruch
im U14-Inventar, KTD16: nicht public — Changelog-Satz genügt).

**Owner-Check der Dedup-Flags bleibt ein String** (`task_id`), kein
Token-Typ: der Wert steht im Wire-Format (`x-swarmq-unique-hash` im
Header, `task_id` als Valkey-Value) und muss 0.1.2-kompatibel bleiben
(KTD12). Begründete Ausnahme in §4.

### 2.4 `contracts/coordination.py`

**`LockStore`**

```python
@dataclass(frozen=True, slots=True)
class LockToken:
    name: str
    owner: str
    max_concurrency: int
```

| Methode | Zusage | Valkey heute |
|---|---|---|
| `acquire(name, *, owner: str, ttl_ms, max_concurrency) -> LockToken \| None` | `None` bei Konflikt; nur `acquire` erzeugt Tokens | `LockManager.acquire` (locking.py:113) → `lock_acquire.lua` |
| `release(token) -> bool` | Owner-Check | `release` :142 → `lock_release.lua` |
| `renew(token, ttl_ms) -> bool` | nur `max_concurrency == 1` | `renew` :171 → `lock_renew.lua` |
| `force_release(name, *, max_concurrency) -> bool` | Operator-Pfad ohne Owner-Check | heute `release(name, "__force__", …)` |
| `current_owner(name) -> str \| None` | für Health/CLI (Leader-Anzeige) | `get_scheduler_leader` (valkey.py:878) generalisiert |

Owner-Token bleibt der heutige String `host:pid:uuid8`
(scheduler.py:436) bzw. `handler_id`; er ist Lua-ARGV und muss gleich
bleiben. Das Token *kapselt* ihn — der Kern kann `release` nicht mit einem
selbstgebauten Owner aufrufen, nur mit dem Rückgabewert von `acquire`.

**`RateLimitStore`**

```python
@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    remaining: int
    retry_after_ms: int     # ≥ 1 wenn nicht erlaubt, 0 sonst
```

| Methode | Zusage | Valkey heute |
|---|---|---|
| `check(name, *, max_requests, window_ms) -> RateLimitDecision` | zählt den Aufruf, wenn erlaubt; meldet Restwartezeit, wenn nicht | `RateLimiter.check` (limits.py:96) → `sliding_window_counter.lua` |

Die Rate-Limit-Rückstellung selbst ist **Broker-Verhalten** und läuft über
`Consumer.retry_later(delivery, delay_ms=decision.retry_after_ms, …)` —
eine Kernmethode (AE1). Ihr Contract-Test steht in der broker-neutralen
Suite (U3), bevor `BrokerAdmin` entsteht.

**`ChordStore`**

| Methode | Zusage | Valkey heute |
|---|---|---|
| `record_member(wf_id, chord_id, member_id, *, expected: int, ttl_ms) -> bool` | idempotent; `True` bei jedem Aufruf ab Erreichen von `expected` (der One-Shot ist der `step_tasks["/b"]`-Marker, workflow_orchestration.py:729–734) | `ChordManager.record_and_check_complete` (:716) → `chord_complete.lua` |

**`ScheduleStore`** — arbeitet auf `bytes` plus Fire-Zeitpunkt, damit
`backend/` nicht `scheduler.definition` importiert (KTD4).

| Methode | Zusage | Valkey heute |
|---|---|---|
| `add(name, payload: bytes, fire_ts_ms: int \| None) -> bool` | insert-if-absent | `hsetnx_schedule_def` + `zadd_schedule` (scheduler.py:210) |
| `put(name, payload, fire_ts_ms)` | überschreibt | `hset_schedule_def` + `zadd` (:232) |
| `exists(name) -> bool` | | heute `_client.hexists` (:262) |
| `remove(name) -> bool` | | `zrem_schedule` + `hdel_schedule_def` (:250) |
| `get(name) -> bytes \| None`, `list_all() -> Mapping[str, bytes]` | | `hget_schedule_def`, `hgetall_schedule_defs` |
| `disable(name, payload) -> bool` | atomar: Definition ersetzen + aus dem Fire-Index nehmen | `sched_disable.lua` (:275) |
| `poll_due(now_ms, *, limit) -> list[tuple[str, int, bytes]]` | | `sched_poll.lua` (:351) |
| `commit_fire(name, *, next_fire_ts_ms, payload: bytes \| None, owner: str) -> bool` | Owner-Check gegen den Leader-Lock im selben atomaren Schritt; `payload=None` löscht (One-Shot) | `sched_commit.lua` (:384) |
| `mark_fire_completed(fire_key, ttl_ms) -> bool`, `claim_fire(fire_key, ttl_ms) -> bool`, `release_fire_claim(fire_key)`, `is_fire_completed(fire_key) -> bool` | Schedfire-Dedup-Marker | `set_schedfire_dedup` :717, `set_schedfire_inflight` :749, `clear_schedfire_inflight` :776, `is_schedfire_completed` :795 |
| `set_handover_marker(owner, ttl_ms)`, `take_handover_marker() -> bool` | Leader-Übergabe | :809, :827 |

Die msgspec-Kodierung der `ScheduleDefinition` (scheduler.py:87–134)
bleibt in `scheduler/definition.py`; `SCHEDULER_LOCK_NAME`/`SCHEDULER_LOCK_KEY`
(heute scheduler.py:54/:67) ziehen nach `swarmq/locking.py` neben `lock_key`,
womit der Lazy-Import in valkey.py:891 verschwindet (KTD6).

### 2.5 AE1-Zählung

- Kernmethoden (`Publisher`, `Consumer`, `ResultBackend`, vier
  Koordinationsverträge): **alle** in der Klasse „mit XACK/XCLAIM (bzw.
  Valkey-Primitiven) erfüllbar". Klasse „nur Erweiterung": **0**.
- Rate-Limit-Rückstellung: Kernmethode (`retry_later`).
- Methodennamen/Parameter ohne AMQP-/Valkey-Primitive: geprüft — kein
  `nack`, `requeue`, `delivery_tag`, `prefetch`, `x-death`, `zadd`,
  `hset`, `setnx`, `pubsub`, `eval`, Skript- oder Key-Name im Vokabular.
  (`route_via_dlx` und die DLQ-Operationen liegen in `BrokerAdmin`.)

---

## 3. R4.2-Andockstellen

R4.2 (Bulk-Fast-Path) braucht Batch-Publish mit einer Confirms-Pipeline
und gepipelinte Result-Writes. Beides wird als **benannte
Erweiterungsstelle** angelegt, nicht als Kernmethode — kein Broker/Backend
muss sie in 0.2.0 implementieren, und ihr späteres Hinzufügen bricht den
Vertrag nicht:

```python
class BatchPublisher(Protocol):          # contracts/broker.py, Erweiterung von Publisher
    async def publish_many(self, queue: str, items: Sequence[tuple[bytes, Mapping[str, Any] | None]]) -> list[PublishReceipt]: ...

class BatchResultBackend(Protocol):      # contracts/backend.py, Erweiterung von ResultBackend
    async def store_results(self, items: Sequence[tuple[str, bytes, int]], *, op_timeout: float | None = None) -> None: ...
```

- `publish_many` gibt **pro Element** einen `PublishReceipt` zurück, damit
  `schedule_many` (client.py:688) den heutigen Teil-Rollback der
  Dedup-Flags pro fehlgeschlagenem Element behält. Semantik bei
  Teilfehler: die Methode wirft `BrokerConnectionError` mit dem Attribut
  `receipts: list[PublishReceipt]` der erfolgreichen Präfix-Elemente —
  festgelegt hier, damit R4.2 den Vertrag nicht nachträglich ändert.
- Der Client fragt zur Laufzeit `isinstance(broker, BatchPublisher)`
  (Smoke via `runtime_checkable`) und fällt sonst auf eine Schleife über
  `publish` zurück.
- RMQ: `publish_many` = mehrere `basic.publish` auf dem Confirms-Kanal,
  ein gemeinsames Warten; Streams: `MULTI`/Pipeline aus `XADD`.
  `store_results`: Valkey-Pipeline aus `SET … PX` — heute schon das Muster
  von `set_progress` (valkey.py:282).

---

## 4. Invarianten-Tabelle (R6, KTD5)

| Invariante | Heute erzwungen durch | Mechanismus 0.2.0 | Klasse |
|---|---|---|---|
| **Ack-Ordering** — kein Broker-Verb vor dem letzten Pre-Execute-Gate; FAF ackt genau einmal nach den Gates, danach kein Verb mehr; ALO ackt nach Ergebnisspeicherung/Workflow-Advance | Kommentare (worker.py:1860–1863, :472–479, :1894–1904, :2364–2368) und manuelle Reihenfolge in `_handle_message` | **Pipeline-Runner** (`worker/pipeline.py`, U19): Gates sind eine geordnete Liste `Gate.apply(delivery, ctx) -> DeliveryOutcome`; nur der Runner ruft `ack`; nach dem letzten `Continue` schaltet er für FAF in `acked_before_execute`, in dem der Executor kein `Consumer`-Objekt mehr erhält (Typ `None`) — ein Verb nach dem FAF-Ack ist damit ein Attributfehler, kein Kommentarverstoß. Ordnung wird durch einen Unit-Test auf dem Stub gepinnt (U19) | strukturell (Kapselung + Typzustand) |
| **Terminal-Flags** — `release_unique` und `finalize_schedfire` genau nach der terminalen Variante | zwei Booleans, Default `True`, acht paarweise Resets (§1) | **`DeliveryOutcome`** (`worker/outcome.py`, U6): `Continue \| Redelivered \| Terminal(release_unique: bool, finalize_schedfire: bool)`. Die drei heutigen Varianten: `Terminal(True, True)` (Freigabe im `finally`: Erfolg, Fehler, Cancel, Unknown-Task, Middleware-Fail), `Terminal(False, True)` (Lock-Pfade und `_send_to_dlq`, die nach dem Ack inline freigeben; worker.py:525, :567, :658, :2782), `Terminal(False, False)` (Expiry, worker.py:1853–1858: Flag steht bewusst weiter). `Redelivered` = beide `False` plus Löschen des In-Flight-Claims. Ein Gate **muss** ein Outcome zurückgeben; „Reset vergessen" ist kein möglicher Zustand mehr | strukturell (Summentyp) |
| **Owner-Checks (Locks)** — nur der Erwerber gibt frei/erneuert | Owner-String wird durchgereicht; jede Aufrufstelle könnte einen fremden String übergeben | **`LockToken`** nur aus `LockStore.acquire`; `release`/`renew` nehmen ausschließlich das Token. Der Operator-Pfad (`__force__`) ist eine separate Methode `force_release` und nicht über ein gefälschtes Token erreichbar | strukturell (Token-Typ) |
| **Owner-Checks (Dedup-Flag, Schedule-Commit)** | `task_id` als Owner-Value (valkey.py:506), `owner_token` als Lua-ARGV (scheduler.py:407) | **bleibt String, dokumentierte Ausnahme**: beide Werte sind Wire-/Storage-Format (KTD12 — `task_id` steht als Value in `sq:unique:*`, das Leader-Token in `sq:lock:{sched}:leader` und wird von `swarmq inspect`/Health gelesen). Ein Token-Typ würde nur eine Hülle um denselben String legen; der Owner-Check läuft serverseitig in `unique_release.lua`/`sched_commit.lua` und ist durch die Contract-Tests „Dedup-Flag Owner-Check", „Schedule-Commit Owner-Check" (U3) abgesichert | Contract-Test (begründet kommentiert) |
| **publish-then-mark** — ein Workflow-Schritt wird erst als eingereiht markiert, wenn er veröffentlicht wurde | Kommentar + Reihenfolge zweier Zeilen (workflow_orchestration.py:599–600) | **`PublishReceipt`**: `Publisher.publish` ist die einzige Quelle; `WorkflowAdvancer._enqueue_node` ruft `mark_enqueued(step_path, receipt)`, dessen Parameter `PublishReceipt` verlangt (mypy) und zur Laufzeit `isinstance` prüft (`TypeError`, AE5). Ohne Publish gibt es kein Receipt, also keine Markierung (U20) | strukturell (Token-Typ) |
| Cancel-Flag erst nach gespeichertem Ergebnis löschen (worker.py:2350) | Kommentar | Teil des Ergebnis-Tails `worker/results.py` (U20), Reihenfolge in einem einzigen Modul mit Unit-Test auf dem Stub; kein eigener Typ (Verstoß wäre eine Umsortierung innerhalb einer 30-Zeilen-Funktion, die der Test sofort zeigt) | Kapselung + Test |
| Signal nach Ack (`TASK_DLQ`, `TASK_CANCELLED` pre-start; worker.py:2785, :1596) | Kommentar | Kapselung in `retry.py`/`gates/cancel.py`; Test „Signal erst nach `ack`" auf dem Stub (Aufrufreihenfolge) | Kapselung + Test |

Am Programmende hat jede tragende Invariante einen Eintrag; keine bleibt
nur kommentiert, ohne dass die Begründung hier steht (R6).

### 4.1 Zustandsmaschine des Runners

```
Decoded ─► Gate₁ ─Continue─► Gate₂ … Gateₙ ─Continue─► [FAF: ack, Modus acked_before_execute] ─► Execute
   │            │ Redelivered / Terminal(...)                                             │
   │            └────────────────────────────► finally: Outcome auswerten ◄──────────────┘
   └ Decode-Fehler / Body-Cap / Unknown-Task → BrokerAdmin.route_via_dlx (falls verfügbar) sonst dead_letter(delivery, headers=delivery.headers)
```

Gate-Reihenfolge (heute worker.py:1770–1918, bleibt): Schedfire-Dedup →
Pre-Start-Cancel → Semaphore → Shutdown-Guard → Registry → Expiry →
Rate-Limit → Lock → Dedup-Start-Release → FAF-Ack. Unbehandelte Exception
aus der Ausführung → `Terminal(True, True)` (heutiges Verhalten: Flags
bleiben `True`).

---

## 5. Ziel-Modulschnitt und Extraktionsreihenfolge (KTD6)

Pakete und Dateien wie im Plan (Output Structure). Zuordnung des heutigen
Codes (Zeilen `v0.1.2`):

| Zielmodul | Heutiger Code | Unit |
|---|---|---|
| `broker/rabbitmq/connection.py` | rabbitmq.py:53–262 (`__init__`, `connect`, `_ensure_*_channel`, `_ensure_channel_tracked`), :512–549 (`close`) | U5 |
| `broker/rabbitmq/delivery.py` | :263–325 (`_publish_with_retry`), :366–474 (`declare_queue`, Tripel, `publish`, `publish_to_delay_queue`, `publish_to_dead_letter_queue`, `consume`), :475–511 (`cancel_consume`, `ack`, `nack` → Verben) | U5 |
| `broker/rabbitmq/admin.py` | :551–947 (DLQ-Operationen, `get_queue_depth`, `get_dlq_depth`, `route_via_dlx`) | U5 |
| `backend/valkey/connection.py` | valkey.py:22–142 plus **ein** `op_timeout`-Wrapper für die 12 Wiederholungen | U17 |
| `backend/valkey/results.py` | :144–634 (Ergebnisse, Notify, Fortschritt, Cancel, Workflow, Unique) + `wait_for_result`/`watch_progress`/`wait_for_workflow` aus client.py:460–686 und workflow_orchestration.py:174–226 | U17 |
| `backend/valkey/locks.py` | locking.py:91–208 (`LockManager`) | U18 |
| `backend/valkey/rate_limit.py` | limits.py:84–127 (`RateLimiter`) | U18 |
| `backend/valkey/chords.py` | workflow_orchestration.py:704–740 (`ChordManager`) | U18 |
| `backend/valkey/schedules.py` | scheduler.py:173–421 (`ScheduleStore`) + valkey.py:640–904 (Schedule-/Schedfire-Primitive, `get_scheduler_leader`) | U18 |
| `locking.py` (bleibt) | `lock_key`, `compute_lock_ttl_ms` + `SCHEDULER_LOCK_NAME`/`_KEY` aus scheduler.py:54/:67 | U18 |
| `worker/outcome.py` | neu | U6 |
| `worker/pipeline.py` | worker.py komplett (U6), dann Runner `_handle_message` :1714–2537 mit Outcome (U19) | U6, U19 |
| `worker/gates/{schedfire,cancel,expiry,rate_limit,lock}.py` | :1285–1432, :1433–1609, :217–323, :326–453, :455–740 | U7 |
| `worker/execution.py` | :1953–2173 (Ausführung, Watchdog :2539–2556, Cancel-Monitor :2558–2606) | U8 |
| `worker/retry.py` | :2608–2705 (`_evaluate_retry`), :2707–2808 (`_send_to_dlq`) | U8 |
| `worker/results.py` | :2256–2441 (Ergebnis-Tail, Workflow-Seam) + Adapter :1696–1712 | U20 |
| `worker/lifecycle.py` | :769–1273 (`start`, Burst-Monitor, Metrics-Sampler, Shutdown, Teardown) | U9 |
| `client/{schedule,results,workflows}.py` | client.py:157–459 + :688–1130, :460–687 + :955–986, :1131–1262 | U10 |
| `scheduler/{definition,loop,sync}.py` | scheduler.py:54–165, :449–1002 + :1134–1176, :1003–1133 | U11 |
| `process_manager/{config,supervisor,entry,sampler,check,run}.py` | process_manager.py (Sampler :845–888 erhält `Callable[[], BrokerAdmin]`) | U21 |

Extraktionsreihenfolge (Abhängigkeits-DAG des Plans): U5 → U17 → U18 →
U6 → U19 → U7 → U8 → U20 → U9; U10, U11, U21 parallel nach U18.
Begründung der Worker-Reihenfolge: Outcome-Typ vor Runner (der Runner
braucht ihn), Runner vor Gates (Gates geben Outcomes an den Runner),
Gates vor Ausführung/Retry (Retry nutzt `retry_later`/`dead_letter`
dieselben Verben wie die Gates), Ergebnis-Tail vor Lifecycle (Lifecycle
verdrahtet nur noch).

Fassaden re-exportieren keine heute gepatchten Namen — Inventar der
Patch-Ziele in `tests/` (Stand `v0.1.2`): `swarmq.worker.logger` (3),
`swarmq.worker.decode_json` (1), `swarmq.client.asyncio.sleep` (3),
`swarmq.scheduler.logger` (1), `swarmq.process_manager.os.execvpe` (7),
`.run_multi_process_worker` (6), `._validate_module_importable` (5),
`.setup_logging` (5), `.ProcessManager` (4), `.os.kill` (1+1),
`.logger` (1), `swarmq.broker.rabbitmq.aio_pika.connect_robust` (3),
`.logger` (2), `swarmq.backend.valkey.valkey.asyncio.Valkey.from_url` (5),
`.valkey.asyncio.Valkey` (2). Jede Extraktions-MR stellt diese Pfade im
selben MR um (KTD14); der Patch-Pfad-Guard (U2) fängt Nachzügler.

---

## 6. Kompatibilitätsauflage 0.1.2 → 0.2.0 (KTD12)

Unverändert bleiben; jede Abweichung ist ein benannter Bruch mit
Migrationshinweis im Changelog. Golden-Fixtures (U17) halten es prüfbar.

**Valkey-Keys** (`keys.json`): `sq:result:{task_id}` STRING ·
`sq:cancel:{task_id}` STRING · `sq:progress:{task_id}` HASH ·
`sq:wf:{wf_id}` STRING · `sq:wf:cancel:{wf_id}` STRING ·
`sq:wf:chord:{wf_id}:{chord_id}` SET · `sq:unique:{sha256hex}` STRING
(Value = `task_id`) · `sq:lock:{lock_name}` STRING (Mutex) oder ZSET
(Semaphore, `max_concurrency > 1`) · `sq:lock:{sched}:leader` STRING ·
`sq:{sched}:zset` ZSET · `sq:{sched}:defs` HASH ·
`sq:sched:fired:{name}:{ts}` STRING · `sq:sched:inflight:{name}:{ts}`
STRING · `sq:sched:handover:{owner}` STRING · `sq:rl:{task_name}:{window_ts}`
STRING. Hash-Tags `{sched}` und `{task_name}` bleiben; Einzelschlüssel-Marker
tragen bewusst keinen Tag (valkey.py:150–159).

**Values** (`values/`): msgpack-Ext-Codes 1 datetime · 2 date · 3 time ·
4 Decimal · 5 UUID · 6 set · 7 frozenset (serialization.py:156–162);
Result-Envelope-Felder aus `build_result_dict`/`build_error_result_dict`;
Workflow-Zustand `v, workflow_id, status, topology, final_result_task_id,
error, step_tasks` (workflow_orchestration.py:63–90);
`ScheduleDefinition` als msgspec-JSON mit Feldreihenfolge
`schedule_name, task_name, args, kwargs, queue, cron, fire_ts_ms, source,
disabled, last_fire_at, dedup_hash, unique_until` (scheduler.py:87–134);
Progress-HASH-Felder `current, total, percent, message, updated_at,
started_at, eta_seconds` als `str`; Flag-Values `"1"`.

**Kanäle** (`channels.txt`): `sq:notify:{task_id}` Payload `done` ·
`sq:progress:channel:{task_id}` Payload JSON der HASH-Felder ·
`sq:wf:notify:{wf_id}` Payload `done`.

**Lua** (`lua_contract.txt`): `chord_complete` 1/3 · `lock_acquire` 1/4 ·
`lock_release` 1/2 · `lock_renew` 1/2 · `sched_commit` 3/4 ·
`sched_disable` 2/2 · `sched_poll` 2/2 · `sliding_window_counter` 2/3 ·
`unique_release` 1/1 (KEYS/ARGV); Slot-Gleichheit der Mehrschlüssel-Skripte
über die Hash-Tags.

**RabbitMQ** (`queue_args.json`): Main `durable`, `x-queue-type`
(Default `quorum`), `x-dead-letter-exchange: ""`,
`x-dead-letter-routing-key: {q}.XQ`, `x-delivery-limit: 20` **nur** bei
quorum; `{q}.DQ` mit `x-dead-letter-routing-key: {q}`; `{q}.XQ` nur
`x-queue-type`. Verzögerung über Per-Message-`expiration` (Sekunden-Float,
rabbitmq.py:444), **kein** `x-message-ttl`. Prioritäts-Bänder als
Suffix-Queues `{q}.high`/`{q}`/`{q}.low` mit vollständigem Tripel je Band;
kein `x-max-priority`. Body = JSON `{"task": {"id", "name", "queue",
"args", "kwargs", …}}`; `delivery_mode` persistent.

**Header** (`headers.txt`): `x-swarmq-unique-hash`, `x-swarmq-unique-until`,
`x-swarmq-enqueued-at`, `x-swarmq-workflow-id`, `x-swarmq-workflow-step`
(headers.py) sowie die heute nur als Literale gesetzten
`x-swarmq-schedfire`, `x-swarmq-retry-count`, `x-swarmq-traceback`,
`x-swarmq-rate-limit-count`, `x-swarmq-lock-conflict-count`,
`x-swarmq-lock-name`, `x-swarmq-lock-render-error`. Die Literale werden
im Zuge der Extraktion nach `headers.py` gezogen (Name unverändert —
keine Wire-Änderung). Doku-Drift: `website/reference/agent/message-formats.md:47–49`
nennt `x-swarmq-expired`, `x-swarmq-lock-error`, `x-swarmq-error`, die im
Code nicht existieren, und lässt sechs reale Header aus — Korrektur in U15
(R11), kein Bruch.

**Ausdrücklich erlaubte Sichtbarkeitsänderung:** `PublishReceipt.message_id`
ist für RabbitMQ `None`; kein Wire-Feld.

---

## 7. Dead-Letter-Fallbacks je Aufrufstelle (KTD3)

Diese Tabelle friert das heutige Verhalten ein; Phase 2 ändert es nicht
(R9). U5 pinnt es per Characterization-Test, U14 entscheidet über eine
spätere Vereinheitlichung (0.2.x).

| Stufe (worker.py) | Primärpfad | Fallback bei Publish-/Ack-Fehler | Verb 0.2.0 |
|---|---|---|---|
| Body-Cap :1728, Decode :1758, Unknown-Task :1840, Middleware-Factory :1942 | `nack(requeue=False)` → DLX | — | `route_via_dlx` (BrokerAdmin) — terminaler Pfad ohne Task-ID |
| Schedfire Backend-Fehler :1328/:1372, Cancel Backend-Fehler :1472/:1497, Shutdown :1832, Lock-Acquire-Fehler :613, Rate-Limit-Check-Fehler :368, `store_result`-Fehler :2339, Workflow-Advance-Fehler :2408/:2421 | `nack(requeue=True)` | — | `redeliver` → `Redelivered` |
| Pre-Start-Cancel :1582 | store + `ack` | Result-Write-Fehler → `nack(requeue=True)` :1593 | `ack` / `redeliver` |
| Expiry :312–315 | DLQ-Publish + `ack` | `BrokerConnectionError` → `nack(requeue=True)` :319 | `dead_letter` / `redeliver`; Outcome `Terminal(False, False)` |
| Rate-Limit begrenzt :401–407 | Delay-Publish + `ack` | `nack(requeue=True)` :420 | `retry_later` / `redeliver` |
| Lock Render-Fehler :522, Leer-Render :564, Konflikt am Cap :655 | DLQ-Publish (+Diagnose-Header) + `ack` + Release | `nack(requeue=False)` :541/:578/:683 → DLX **ohne** Diagnose-Header, danach Release | `dead_letter` / `route_via_dlx`; `Terminal(False, True)` |
| Lock-Konflikt unter Cap :693 | Delay-Publish + `ack` | `nack(requeue=True)` :712 | `retry_later` / `redeliver` |
| Retry :2688 | Delay-Publish + `ack` | `nack(requeue=True)` :2695 | `retry_later` / `redeliver` |
| `_send_to_dlq` :2774 | store + fail-mark + DLQ-Publish + `ack` + Release + `TASK_DLQ` | `nack(requeue=True)` :2802, **kein** Release, **kein** Signal | `dead_letter` / `redeliver` |

Muster: terminale Lock-/Render-Pfade fallen auf DLX (`requeue=False`)
zurück, Expiry/Rate-Limit/Retry/DLQ auf Redelivery (`requeue=True`).

**`x-death`-Differenz.** `x-death` wird im Produktionscode nirgends gelesen
oder gesetzt. Weg A (`route_via_dlx`): RabbitMQ leitet über die
Queue-Argumente nach `{q}.XQ` und ergänzt `x-death` (reason `rejected`);
es fehlen `x-swarmq-traceback` und im Lock-Fallback
`x-swarmq-lock-*`, außerdem Error-Result, `TASK_DLQ`-Signal,
Workflow-Fail-Mark und (bei Body-Cap/Decode) Unique-Release, weil die
Task-ID unbekannt ist. Weg B (`dead_letter`): direkter Publish nach
`{q}.XQ` **ohne** `x-death`, aber mit vollständiger `x-swarmq-*`-Diagnose
und allen Nebenwirkungen. Der Stub macht beide Wege inspizierbar
(Dead-Letter-Liste mit Grund `routed` vs. `published`, U4), damit die
Characterization in U5 auf Stub und RabbitMQ gleich liest. Wie ein
Streams-Broker Weg A abbildet (er hat kein DLX; `route_via_dlx` wäre dort
`dead_letter` mit den Original-Headern), ist genau der Grund, warum
`route_via_dlx` **nicht** im Kernvertrag steht.

---

## 8. Konsequenz

1. **AE1 (Durchspiel-Teil) ist ohne Befund**: null Kernmethoden „nur
   Erweiterung", Rate-Limit-Rückstellung ist Kernmethode, kein
   AMQP-/Valkey-Primitiv im Vokabular, R4.2 hat zwei benannte
   Erweiterungsstellen (§3).
2. **U3** schreibt die Protocols exakt nach §2 und die Contract-Basisklassen
   aus den Zusagen-Spalten (KTD9-Pflichtfälle: ack entfernt; redeliver
   liefert dieselbe Zustellung; retry_later hält Verzögerung ein und
   trifft die Zustellungs-Queue; dead_letter inspizierbar mit
   Original-Headern; beide werfen bei Publish-Fehler und lassen
   unbestätigt; Treiberfehler gewrappt; connect idempotent; ping vor
   connect wirft; TTL-Ablauf; wait_for_result wacht auf/fällt zurück;
   Owner-Checks; Restwartezeit; Chord-Idempotenz; Schedule-Commit mit
   Owner-Check und `exists`).
3. **Zeitmodell des Stubs** (offene Frage aus dem Plan): eine injizierbare
   monotone Fake-Clock (`tests/support/inmemory/clock.py`) steuert
   `retry_later`-Fälligkeit, TTLs (Ergebnisse, Flags, Locks) und die
   Sliding-Window-Fenster; die Contract-Basisklassen erhalten einen
   `clock`-Hook, der beim RabbitMQ-/Valkey-Binding echte Wartezeit ist.
4. **Verhaltenskorrektur Band-Erhalt** (KTD2): wird durch `Delivery.queue`
   möglich, aber erst in U8 wirksam (Wechsel auf `retry_later(delivery, …)`),
   mit e2e-Test und Changelog-Eintrag. Bis dahin lesen Gates die Queue
   weiter aus dem Body — sonst wäre die Korrektur eine unbenannte
   Nebenwirkung von U7.

## Offene Punkte (Deferred to Implementation)

- Ob `Consumer.consume` `concurrency` selbst als Prefetch setzt oder der
  Worker die Semaphore behält: beides — der Broker begrenzt unbestätigte
  Zustellungen (Prefetch), der Worker begrenzt gleichzeitige Ausführung
  (Semaphore). Entscheidet U19 nicht neu, ist heute schon so.
- Exakte Fehlerklasse für Teil-Fehler in `publish_many` (§3) — Attribut
  `receipts` an `BrokerConnectionError` oder Subklasse; R4.2 entscheidet.
- Ob `BrokerAdmin.route_via_dlx` beim Stub `x-death` nachahmt: nein — der
  Stub markiert Grund `routed`; RabbitMQ-Contract-Tests prüfen `x-death`
  nur in der RabbitMQ-Erweiterungssuite.
